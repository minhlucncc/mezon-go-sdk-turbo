package ws_test

import (
	"os"
	"testing"

	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

// The exact reply that took the live bridge down on 2026-09-22: an IELTS
// marking in Vietnamese, with `**bold**` wrapped around backticked words.
//
// Kept as a fixture rather than paraphrased inline, because the thing that
// broke was the interaction between real prose, real markup and UTF-16
// offsets — a shortened version of it would not have crashed.
func TestTheRealMarkingReplyBuilds(t *testing.T) {
	raw, err := os.ReadFile("testdata/marking_reply.txt")
	if err != nil {
		t.Fatalf("fixture missing: %v", err)
	}
	if blob := ws.BuildContent(string(raw)); blob == "" {
		t.Fatal("no content built")
	}
}
