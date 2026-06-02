package ws_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/nccasia/mezon-go-sdk/mezon-protobuf/mezon/v2/common/api"
	"github.com/nccasia/mezon-go-sdk/mezon-protobuf/mezon/v2/common/rtapi"

	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

func channelMsgFrame(tb testing.TB) []byte {
	env := &rtapi.Envelope{
		Cid: "abc",
		Message: &rtapi.Envelope_ChannelMessage{ChannelMessage: &api.ChannelMessage{
			MessageId:   "m1",
			SenderId:    "u1",
			ChannelId:   "c1",
			ClanId:      "clan1",
			Content:     `{"t":"hi"}`,
			DisplayName: "Alice",
			Mode:        2,
			IsPublic:    true,
		}},
	}
	b, err := proto.Marshal(env)
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

func TestDecodeChannelMessage(t *testing.T) {
	msg, ok, err := ws.DecodeChannelMessage(channelMsgFrame(t))
	if err != nil || !ok {
		t.Fatalf("expected a channel message, ok=%v err=%v", ok, err)
	}
	if msg.MessageID != "m1" || msg.SenderID != "u1" || msg.ChannelID != "c1" ||
		msg.ClanID != "clan1" || msg.Content != `{"t":"hi"}` || msg.DisplayName != "Alice" ||
		msg.Mode != 2 || !msg.IsPublic {
		t.Fatalf("decoded fields wrong: %+v", msg)
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
