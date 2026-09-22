package ws_test

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

// Field 17 of ChannelMessage is MessageAttachmentList. Field numbers inside
// MessageAttachment come from the official mezon-sdk's generated encoder
// (api.MessageAttachment.encode): 1 filename, 2 size, 3 url, 4 filetype.
//
// This matters because an essay long enough to be worth marking usually
// arrives as a file. The decoder used to walk past field 17, which meant the
// submission never reached the platform at all.
func attachmentFrame(tb testing.TB, filename, url, filetype string, size uint64) []byte {
	tb.Helper()
	str := func(b []byte, num protowire.Number, v string) []byte {
		return protowire.AppendString(protowire.AppendTag(b, num, protowire.BytesType), v)
	}
	var att []byte
	att = str(att, 1, filename)
	att = protowire.AppendVarint(protowire.AppendTag(att, 2, protowire.VarintType), size)
	att = str(att, 3, url)
	att = str(att, 4, filetype)
	list := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), att)

	cm := liveChannelMessage(tb)
	cm = protowire.AppendBytes(protowire.AppendTag(cm, 17, protowire.BytesType), list)
	return protowire.AppendBytes(protowire.AppendTag(nil, 6, protowire.BytesType), cm)
}

func TestDecodeChannelMessageCarriesAttachments(t *testing.T) {
	msg, ok, err := ws.DecodeChannelMessage(
		attachmentFrame(t, "essay.docx", "https://cdn.mezon.ai/a.docx", "application/docx", 4096),
	)
	if err != nil || !ok {
		t.Fatalf("expected a channel message, ok=%v err=%v", ok, err)
	}

	var got []struct {
		Filename string `json:"filename"`
		URL      string `json:"url"`
		Filetype string `json:"filetype"`
		Size     int32  `json:"size"`
	}
	if err := json.Unmarshal([]byte(msg.Attachments), &got); err != nil {
		t.Fatalf("attachments must be the REST JSON dialect, got %q: %v", msg.Attachments, err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one attachment, got %d", len(got))
	}
	if got[0].Filename != "essay.docx" || got[0].URL != "https://cdn.mezon.ai/a.docx" {
		t.Fatalf("attachment decoded wrong: %+v", got[0])
	}
	if got[0].Size != 4096 || got[0].Filetype != "application/docx" {
		t.Fatalf("size/filetype decoded wrong: %+v", got[0])
	}

	// Everything else still decodes: adding a field must not shift the rest.
	if msg.SenderID != "555" || msg.Content != `{"t":"hi"}` {
		t.Fatalf("adding attachments disturbed the rest: %+v", msg)
	}
}

func TestAMessageWithNoAttachmentsCarriesNone(t *testing.T) {
	// Empty rather than "[]": downstream treats the empty string as "none",
	// and an empty JSON array would read as a message that had attachments.
	msg, _, _ := ws.DecodeChannelMessage(channelMsgFrame(t))
	if msg.Attachments != "" {
		t.Fatalf("expected no attachments, got %q", msg.Attachments)
	}
}
