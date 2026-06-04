package ws_test

import (
	"encoding/json"
	"testing"
	"unicode/utf16"

	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

type contentBlob struct {
	T  string `json:"t"`
	Lk []struct {
		S int `json:"s"`
		E int `json:"e"`
	} `json:"lk"`
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
	if len(blob.Lk) != 0 {
		t.Fatalf("no lk expected for plain text: %+v", blob.Lk)
	}
}

func TestBuildContentMarksURLSpans(t *testing.T) {
	text := "Nguồn tham khảo:\n- Quy chế Đào tạo cho SE - v1: https://meknow.mezon.vn/references/abc"
	blob := decodeBlob(t, ws.BuildContent(text))
	if len(blob.Lk) != 1 {
		t.Fatalf("expected 1 lk span, got %+v", blob.Lk)
	}
	if got := sliceUTF16(blob.T, blob.Lk[0].S, blob.Lk[0].E); got != "https://meknow.mezon.vn/references/abc" {
		t.Fatalf("lk span does not slice back to the URL: %q", got)
	}
}

func TestBuildContentUTF16OffsetsAfterEmoji(t *testing.T) {
	// 😊 is an astral char: 2 UTF-16 units. The span must still slice cleanly.
	text := "Chúc bạn học tốt 😊 https://funix.edu.vn"
	blob := decodeBlob(t, ws.BuildContent(text))
	if len(blob.Lk) != 1 {
		t.Fatalf("expected 1 lk span, got %+v", blob.Lk)
	}
	if got := sliceUTF16(blob.T, blob.Lk[0].S, blob.Lk[0].E); got != "https://funix.edu.vn" {
		t.Fatalf("lk span shifted by emoji: %q", got)
	}
}

func TestBuildContentTrimsTrailingPunctuation(t *testing.T) {
	blob := decodeBlob(t, ws.BuildContent("Xem https://funix.edu.vn/guide."))
	if got := sliceUTF16(blob.T, blob.Lk[0].S, blob.Lk[0].E); got != "https://funix.edu.vn/guide" {
		t.Fatalf("trailing punctuation must be outside the span: %q", got)
	}
}

func TestBuildContentMultipleURLs(t *testing.T) {
	text := "- A: https://x.vn/a\n- B: https://x.vn/b"
	blob := decodeBlob(t, ws.BuildContent(text))
	if len(blob.Lk) != 2 {
		t.Fatalf("expected 2 lk spans, got %+v", blob.Lk)
	}
	want := []string{"https://x.vn/a", "https://x.vn/b"}
	for i, span := range blob.Lk {
		if got := sliceUTF16(blob.T, span.S, span.E); got != want[i] {
			t.Fatalf("span %d wrong: %q", i, got)
		}
	}
}
