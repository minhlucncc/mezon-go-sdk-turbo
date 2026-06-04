package ws

import (
	"encoding/json"
	"regexp"
	"strings"
)

// Mezon renders URLs as clickable only when the content blob carries "lk"
// entities — UTF-16 (start, end) spans into the "t" text marking each bare
// URL. Markdown links [name](url) are NOT rendered by Mezon clients; senders
// must flatten them to bare-URL text before calling SendText.
var urlRE = regexp.MustCompile(`https?://[^\s<>\[\]()]+`)

const urlTrailingPunct = ".,;:!?"

type linkSpan struct {
	S int `json:"s"`
	E int `json:"e"`
}

// BuildContent wraps text in Mezon's content blob: {"t": ...} plus "lk" link
// entities for every bare URL so clients render them clickable.
func BuildContent(text string) string {
	spans := linkSpans(text)
	if len(spans) == 0 {
		blob, _ := json.Marshal(map[string]string{"t": text})
		return string(blob)
	}
	blob, _ := json.Marshal(struct {
		T  string     `json:"t"`
		Lk []linkSpan `json:"lk"`
	}{T: text, Lk: spans})
	return string(blob)
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
