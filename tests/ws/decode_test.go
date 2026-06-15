package ws_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/nccasia/mezon-go-sdk/mezon-protobuf/mezon/v2/common/rtapi"

	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

// liveChannelMessage builds a ChannelMessage with the LIVE server's field
// numbers (mezon-sdk python protobuf, 2026-06): the pinned Go codegen
// (mezon-go-sdk v0.0.34) still has create_time/update_time at 9/10, which the
// server has since removed — every field >= 9 shifted down by two, so frames
// must be decoded against the live layout, not the pinned model.
func liveChannelMessage(tb testing.TB) []byte {
	tb.Helper()
	str := func(b []byte, num protowire.Number, v string) []byte {
		return protowire.AppendString(protowire.AppendTag(b, num, protowire.BytesType), v)
	}
	vint := func(b []byte, num protowire.Number, v uint64) []byte {
		return protowire.AppendVarint(protowire.AppendTag(b, num, protowire.VarintType), v)
	}
	var cm []byte
	cm = vint(cm, 1, 111)             // clan_id (int64 varint live!)
	cm = vint(cm, 2, 222)             // channel_id
	cm = vint(cm, 3, 333)             // message_id
	cm = vint(cm, 5, 555)             // sender_id
	cm = str(cm, 6, "alice")          // username
	cm = str(cm, 8, `{"t":"hi"}`)     // content
	cm = str(cm, 9, "general")        // channel_label (pinned model: create_time!)
	cm = str(cm, 12, "Alice")         // display_name
	cm = str(cm, 13, "Al")            // clan_nick
	// mentions/references are NESTED PROTO LISTS on the wire (the decoder
	// converts them to the REST JSON dialect downstream code expects)
	mention := vint(nil, 2, 777)            // MessageMention.user_id
	mention = vint(mention, 7, 0)           // s (omitted: zero)
	mention = vint(mention, 8, 4)           // e
	mlist := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), mention)
	cm = protowire.AppendBytes(protowire.AppendTag(cm, 16, protowire.BytesType), mlist)
	cm = vint(cm, 20, 1770000000)     // create_time_seconds (skipped)
	cm = vint(cm, 22, 2)              // mode
	cm = vint(cm, 24, 1)              // is_public
	return cm
}

func channelMsgFrame(tb testing.TB) []byte {
	tb.Helper()
	return protowire.AppendBytes(protowire.AppendTag(nil, 6, protowire.BytesType), liveChannelMessage(tb))
}

func TestDecodeChannelMessage(t *testing.T) {
	msg, ok, err := ws.DecodeChannelMessage(channelMsgFrame(t))
	if err != nil || !ok {
		t.Fatalf("expected a channel message, ok=%v err=%v", ok, err)
	}
	if msg.MessageID != "333" || msg.SenderID != "555" || msg.ChannelID != "222" ||
		msg.ClanID != "111" || msg.Content != `{"t":"hi"}` || msg.DisplayName != "Alice" ||
		msg.Mode != 2 || !msg.IsPublic {
		t.Fatalf("decoded fields wrong: %+v", msg)
	}
	if msg.Username != "alice" || msg.ClanNick != "Al" || msg.ChannelLabel != "general" {
		t.Fatalf("shifted string fields wrong: %+v", msg)
	}
	if msg.Mentions != `[{"user_id":"777","s":0,"e":4}]` || msg.References != "" {
		t.Fatalf("mention/reference blobs wrong: %+v", msg)
	}
	if strings.TrimSpace(msg.Content) == "" {
		t.Fatal("content should be preserved")
	}
}

func TestDecodeIgnoresNonChannelMessage(t *testing.T) {
	env := &rtapi.Envelope{Message: &rtapi.Envelope_Ping{Ping: &rtapi.Ping{}}}
	b, _ := proto.Marshal(env)
	if _, ok, err := ws.DecodeChannelMessage(b); ok || err != nil {
		t.Fatalf("ping frame should be ignored (ok=false, no err); got ok=%v err=%v", ok, err)
	}
}

func TestDecodeMalformedDoesNotPanic(t *testing.T) {
	for _, junk := range [][]byte{{0xff, 0xff, 0xff}, {0x32}, {}, {0x08}} {
		_, _, _ = ws.DecodeChannelMessage(junk) // must not panic
	}
}
