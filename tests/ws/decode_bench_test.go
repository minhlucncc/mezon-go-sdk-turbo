package ws_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/nccasia/mezon-go-sdk/mezon-protobuf/mezon/v2/common/rtapi"

	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

// BenchmarkPartialDecode is our hot path: extract channel_message only.
func BenchmarkPartialDecode(b *testing.B) {
	frame := channelMsgFrame(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, err := ws.DecodeChannelMessage(frame); !ok || err != nil {
			b.Fatal("decode failed")
		}
	}
}

// BenchmarkFullEnvelopeUnmarshal is what the official SDK does per frame.
func BenchmarkFullEnvelopeUnmarshal(b *testing.B) {
	frame := channelMsgFrame(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var env rtapi.Envelope
		if err := proto.Unmarshal(frame, &env); err != nil {
			b.Fatal(err)
		}
		_ = env.GetChannelMessage()
	}
}
