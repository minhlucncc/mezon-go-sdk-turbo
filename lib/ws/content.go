package ws

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
)

// Mezon renders URLs as clickable when the content blob carries "mk" entities
// with type "lk" over the URL text. Markdown links [name](url) are NOT
// rendered by Mezon clients; senders must flatten them to bare-URL text before
// calling SendText.
var urlRE = regexp.MustCompile(`https?://[^\s<>\[\]()]+`)

const urlTrailingPunct = ".,;:!?"

type linkSpan struct {
	S int `json:"s"`
	E int `json:"e"`
}

type markSpan struct {
	S    int    `json:"s"`
	E    int    `json:"e"`
	Type string `json:"type"`
}

// Bold with a non-empty, single-line body free of nested asterisks; inline
// code on one line. Mirrors the Python worker's mezon_format.markdown_entities.
var (
	boldRE       = regexp.MustCompile(`\*\*([^*\n]+?)\*\*`)
	inlineCodeRE = regexp.MustCompile("`[^`\n]+`")
	fenceLineRE  = regexp.MustCompile("(?m)^[ \t]*```")
)

// BuildContent wraps text in Mezon's content blob: {"t": ...} plus "mk"
// entities (bold/code/link). Clients render rich text from entity spans, not
// markdown syntax; ** markers are stripped here while backtick spans keep their
// backticks for the client to strip. Link spans are computed on the CLEANED
// text since marker stripping shifts offsets.
func BuildContent(text string) string {
	clean, marks := markdownSpans(text)
	links := linkSpans(clean)
	out := map[string]any{"t": clean}
	if len(links) > 0 {
		for _, link := range links {
			marks = append(marks, markSpan{S: link.S, E: link.E, Type: "lk"})
		}
	}
	if len(marks) > 0 {
		out["mk"] = marks
	}
	blob, _ := json.Marshal(out)
	return string(blob)
}

type region struct{ s, e int }

// markdownSpans converts the markdown Mezon can't parse into mk entity spans
// over the returned clean text (UTF-16 offsets):
//   - fenced code blocks → type "t", span INCLUDING the fences, body untouched
//   - inline code → type "s", span including the backticks
//   - **bold** → type "b" over the unwrapped text; the ** markers are removed
func markdownSpans(text string) (string, []markSpan) {
	if text == "" {
		return text, nil
	}
	fences := fenceRegions(text)
	var inline []region
	for _, loc := range inlineCodeRE.FindAllStringIndex(text, -1) {
		if !inside(loc[0], fences) {
			inline = append(inline, region{loc[0], loc[1]})
		}
	}
	masked := append(append([]region{}, fences...), inline...)

	type event struct {
		s, e int
		kind string
		body string
	}
	var events []event
	for _, f := range fences {
		events = append(events, event{f.s, f.e, "t", text[f.s:f.e]})
	}
	for _, c := range inline {
		events = append(events, event{c.s, c.e, "s", text[c.s:c.e]})
	}
	for _, loc := range boldRE.FindAllStringSubmatchIndex(text, -1) {
		if inside(loc[0], masked) || inside(loc[1]-1, masked) {
			continue
		}
		events = append(events, event{loc[0], loc[1], "b", text[loc[2]:loc[3]]})
	}
	if len(events) == 0 {
		return text, nil
	}
	sort.Slice(events, func(i, j int) bool { return events[i].s < events[j].s })

	// Drop any event that starts before the previous one ended.
	//
	// The endpoint checks above skip a bold whose START or whose LAST CHAR
	// sits inside a masked region — but not one that CONTAINS a masked region
	// outright, which `**see `x` here**` does. Two overlapping events then
	// walk `pos` past the next event's start, and `text[pos:ev.s]` panics with
	// an inverted slice. That took the whole bridge down for every bot on it,
	// because one bot's reply happened to contain backticks inside bold.
	//
	// The nested event is dropped rather than the outer one: the outer span
	// still covers that text, so the client renders it — one entity instead of
	// two, which is a rendering difference, where the alternative was a crash.
	kept := events[:0]
	end := -1
	for _, ev := range events {
		if ev.s < end {
			continue
		}
		kept = append(kept, ev)
		end = ev.e
	}
	events = kept

	var b strings.Builder
	var spans []markSpan
	outU16, pos := 0, 0
	emit := func(chunk string) {
		b.WriteString(chunk)
		outU16 += utf16Len(chunk)
	}
	for _, ev := range events {
		emit(text[pos:ev.s])
		start := outU16
		emit(ev.body)
		spans = append(spans, markSpan{S: start, E: outU16, Type: ev.kind})
		pos = ev.e
	}
	emit(text[pos:])
	return b.String(), spans
}

// fenceRegions returns char spans of CLOSED fenced code blocks, fences
// included. An unclosed fence is left alone — the client slices 3 chars off
// both ends of a "t" span, which would corrupt unfenced text.
func fenceRegions(text string) []region {
	var regions []region
	offset := 0
	open := -1
	for _, line := range strings.Split(text, "\n") {
		if fenceLineRE.MatchString(line) {
			if open < 0 {
				open = offset
			} else {
				regions = append(regions, region{open, offset + len(line)})
				open = -1
			}
		}
		offset += len(line) + 1 // the split newline
	}
	return regions
}

func inside(pos int, regions []region) bool {
	for _, r := range regions {
		if r.s <= pos && pos < r.e {
			return true
		}
	}
	return false
}

// linkSpans returns the UTF-16 code-unit spans of bare URLs in text. Mezon
// clients are JS, so offsets must count UTF-16 units (astral chars like emoji
// take 2); for Vietnamese/BMP text they equal rune indices. Trailing sentence
// punctuation is excluded from the span.
func linkSpans(text string) []linkSpan {
	var spans []linkSpan
	for _, loc := range urlRE.FindAllStringIndex(text, -1) {
		url := strings.TrimRight(text[loc[0]:loc[1]], urlTrailingPunct)
		if url == "" {
			continue
		}
		start := utf16Len(text[:loc[0]])
		spans = append(spans, linkSpan{S: start, E: start + utf16Len(url)})
	}
	return spans
}

func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		if r > 0xFFFF {
			n += 2
		} else {
			n++
		}
	}
	return n
}
