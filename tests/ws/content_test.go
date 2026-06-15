package ws_test

import (
	"encoding/json"
	"testing"
	"unicode/utf16"

	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

type contentBlob struct {
	T  string `json:"t"`
	Mk []struct {
		S    int    `json:"s"`
		E    int    `json:"e"`
		Type string `json:"type"`
	} `json:"mk"`
}

func decodeBlob(tb testing.TB, raw string) contentBlob {
	tb.Helper()
	var blob contentBlob
	if err := json.Unmarshal([]byte(raw), &blob); err != nil {
		tb.Fatalf("content blob is not JSON: %v (%q)", err, raw)
	}
	return blob
}

// sliceUTF16 extracts [s:e) from text counting UTF-16 code units — exactly how
// Mezon's JS clients interpret lk offsets.
func sliceUTF16(text string, s, e int) string {
	units := utf16.Encode([]rune(text))
	return string(utf16.Decode(units[s:e]))
}

func TestBuildContentPlainTextNoLk(t *testing.T) {
	blob := decodeBlob(t, ws.BuildContent("Chào bạn! Mình sẵn sàng hỗ trợ."))
	if blob.T != "Chào bạn! Mình sẵn sàng hỗ trợ." {
		t.Fatalf("text mangled: %q", blob.T)
	}
	if len(blob.Mk) != 0 {
		t.Fatalf("no mk expected for plain text: %+v", blob.Mk)
	}
}

func TestBuildContentMarksURLSpans(t *testing.T) {
	text := "Nguồn tham khảo:\n- Quy chế Đào tạo cho SE - v1: https://meknow.mezon.vn/references/abc"
	blob := decodeBlob(t, ws.BuildContent(text))
	if len(blob.Mk) != 1 || blob.Mk[0].Type != "lk" {
		t.Fatalf("expected 1 lk mk span, got %+v", blob.Mk)
	}
	if got := sliceUTF16(blob.T, blob.Mk[0].S, blob.Mk[0].E); got != "https://meknow.mezon.vn/references/abc" {
		t.Fatalf("lk span does not slice back to the URL: %q", got)
	}
}

func TestBuildContentUTF16OffsetsAfterEmoji(t *testing.T) {
	// 😊 is an astral char: 2 UTF-16 units. The span must still slice cleanly.
	text := "Chúc bạn học tốt 😊 https://funix.edu.vn"
	blob := decodeBlob(t, ws.BuildContent(text))
	if len(blob.Mk) != 1 || blob.Mk[0].Type != "lk" {
		t.Fatalf("expected 1 lk mk span, got %+v", blob.Mk)
	}
	if got := sliceUTF16(blob.T, blob.Mk[0].S, blob.Mk[0].E); got != "https://funix.edu.vn" {
		t.Fatalf("lk span shifted by emoji: %q", got)
	}
}

func TestBuildContentTrimsTrailingPunctuation(t *testing.T) {
	blob := decodeBlob(t, ws.BuildContent("Xem https://funix.edu.vn/guide."))
	if got := sliceUTF16(blob.T, blob.Mk[0].S, blob.Mk[0].E); got != "https://funix.edu.vn/guide" {
		t.Fatalf("trailing punctuation must be outside the span: %q", got)
	}
}

func TestBuildContentMultipleURLs(t *testing.T) {
	text := "- A: https://x.vn/a\n- B: https://x.vn/b"
	blob := decodeBlob(t, ws.BuildContent(text))
	if len(blob.Mk) != 2 {
		t.Fatalf("expected 2 lk mk spans, got %+v", blob.Mk)
	}
	want := []string{"https://x.vn/a", "https://x.vn/b"}
	for i, span := range blob.Mk {
		if span.Type != "lk" {
			t.Fatalf("span %d type = %q, want lk", i, span.Type)
		}
		if got := sliceUTF16(blob.T, span.S, span.E); got != want[i] {
			t.Fatalf("span %d wrong: %q", i, got)
		}
	}
}

// ── mk markdown entities (mirrors Python mezon_format.markdown_entities) ────

func TestBuildContentBoldStrippedWithSpan(t *testing.T) {
	blob := decodeBlob(t, ws.BuildContent("**Quy định chung:** áp dụng cho Fresher."))
	if blob.T != "Quy định chung: áp dụng cho Fresher." {
		t.Fatalf("markers not stripped: %q", blob.T)
	}
	if len(blob.Mk) != 1 || blob.Mk[0].Type != "b" {
		t.Fatalf("want one bold span, got %+v", blob.Mk)
	}
	if got := sliceUTF16(blob.T, blob.Mk[0].S, blob.Mk[0].E); got != "Quy định chung:" {
		t.Fatalf("bold span covers %q", got)
	}
}

func TestBuildContentInlineCodeKeepsBackticks(t *testing.T) {
	blob := decodeBlob(t, ws.BuildContent("Chạy `uv sync` trước."))
	if blob.T != "Chạy `uv sync` trước." {
		t.Fatalf("text mutated: %q", blob.T)
	}
	if len(blob.Mk) != 1 || blob.Mk[0].Type != "s" {
		t.Fatalf("want one single-backtick span, got %+v", blob.Mk)
	}
	if got := sliceUTF16(blob.T, blob.Mk[0].S, blob.Mk[0].E); got != "`uv sync`" {
		t.Fatalf("code span covers %q", got)
	}
}

func TestBuildContentFenceMasksBoldAndSpansFences(t *testing.T) {
	text := "Xem:\n```python\nx = '**not bold**'\n```\nHết."
	blob := decodeBlob(t, ws.BuildContent(text))
	if blob.T != text {
		t.Fatalf("fenced text mutated: %q", blob.T)
	}
	if len(blob.Mk) != 1 || blob.Mk[0].Type != "t" {
		t.Fatalf("want one triple span, got %+v", blob.Mk)
	}
}

func TestBuildContentBoldThenLinkOffsetsCompose(t *testing.T) {
	blob := decodeBlob(t, ws.BuildContent("**Nguồn:** https://meknow.mezon.vn/references/abc"))
	if len(blob.Mk) != 2 {
		t.Fatalf("want 1 bold mk + 1 link mk, got %+v", blob.Mk)
	}
	if blob.Mk[1].Type != "lk" {
		t.Fatalf("second mark should be lk, got %+v", blob.Mk)
	}
	if got := sliceUTF16(blob.T, blob.Mk[1].S, blob.Mk[1].E); got != "https://meknow.mezon.vn/references/abc" {
		t.Fatalf("lk span on cleaned text covers %q", got)
	}
}

func TestBuildContentBoldAfterEmojiUTF16(t *testing.T) {
	blob := decodeBlob(t, ws.BuildContent("😊 **chú ý**"))
	if blob.T != "😊 chú ý" {
		t.Fatalf("text: %q", blob.T)
	}
	// emoji = 2 UTF-16 units + space → bold starts at 3.
	if blob.Mk[0].S != 3 {
		t.Fatalf("bold span start = %d, want 3", blob.Mk[0].S)
	}
}

func TestBuildContentUnbalancedBoldUntouched(t *testing.T) {
	blob := decodeBlob(t, ws.BuildContent("Giá **chưa chốt"))
	if blob.T != "Giá **chưa chốt" || len(blob.Mk) != 0 {
		t.Fatalf("unbalanced bold mutated: %q %+v", blob.T, blob.Mk)
	}
}
