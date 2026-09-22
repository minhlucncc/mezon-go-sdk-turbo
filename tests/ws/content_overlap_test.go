package ws_test

import (
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

// A reply whose bold CONTAINS inline code used to panic with an inverted
// slice (`slice bounds out of range [161:137]`) and take the whole Mezon
// bridge down — every bot on it, not just the one that replied. It was
// reached by an ordinary marking reply: Vietnamese prose with `**bold**`
// around a backticked word.
//
// The endpoint checks skip a bold that STARTS or ENDS inside a masked region.
// They never skipped one that swallowed a region whole.
func TestBoldContainingInlineCodeDoesNotPanic(t *testing.T) {
	for _, in := range []string{
		"**sửa `reduce` thành `reduces`**",
		"Lỗi: **dùng `deserve` thay vì `deserves`** nhé.",
		"**a `b` c** and **d `e` f**",
		"**```\ncode\n```**",
		"**điểm `Task Response` 6.0** — cần dẫn chứng 😅",
	} {
		blob := decodeBlob(t, ws.BuildContent(in))
		if blob.T == "" {
			t.Fatalf("input %q produced no text", in)
		}
	}
}

// Whatever survives must still describe the text it is attached to: a span
// running past the end of the string is a client-side crash instead of a
// server-side one. Offsets are UTF-16, and the marking replies are Vietnamese
// with emoji, so this is exactly where it would go wrong.
func TestEverySpanStaysInsideTheText(t *testing.T) {
	for _, in := range []string{
		"**sửa `reduce` thành `reduces`**",
		"plain text with no markup at all",
		"**bold** then `code` then **more bold**",
		"**điểm `Task Response` 6.0** — cần dẫn chứng 😅",
	} {
		blob := decodeBlob(t, ws.BuildContent(in))
		limit := len(utf16.Encode([]rune(blob.T)))
		for _, sp := range blob.Mk {
			if sp.S < 0 || sp.E > limit || sp.S > sp.E {
				t.Fatalf("input %q: span %+v outside [0,%d] of %q", in, sp, limit, blob.T)
			}
		}
	}
}

// Non-overlapping markup is untouched — the fix must not change what already
// worked, which the goldens also cover.
func TestOrdinaryMarkupIsUnchanged(t *testing.T) {
	blob := decodeBlob(t, ws.BuildContent("**bold** and `code`"))
	if strings.Contains(blob.T, "**") {
		t.Fatalf("bold markers should be stripped: %q", blob.T)
	}
	if len(blob.Mk) != 2 {
		t.Fatalf("expected a bold span and a code span, got %d: %+v", len(blob.Mk), blob.Mk)
	}
}
