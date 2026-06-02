package ws

import (
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/nccasia/mezon-go-sdk/mezon-protobuf/mezon/v2/common/api"

	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

// fieldChannelMessage is the Envelope oneof field number for channel_message
// (see realtime.pb.go: `protobuf:"bytes,6,...,name=channel_message"`).
const fieldChannelMessage = 6

// DecodeChannelMessage scans a raw Envelope frame and, if it carries a
// channel_message (field 6), decodes ONLY that submessage — never allocating the
// ~60-field Envelope nor the other oneof cases (pings, pongs, presence events,
// streaming, etc.). ok=false means the frame was something we don't handle.
//
// This is the hot-path CPU/GC win over the official SDK, which does a full
// proto.Unmarshal(&rtapi.Envelope{}) on every frame.
func DecodeChannelMessage(frame []byte) (types.Message, bool, error) {
	b := frame
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return types.Message{}, false, protowire.ParseError(n)
		}
		b = b[n:]

		if num == fieldChannelMessage && typ == protowire.BytesType {
			sub, sn := protowire.ConsumeBytes(b)
			if sn < 0 {
				return types.Message{}, false, protowire.ParseError(sn)
			}
			var cm api.ChannelMessage
			if err := proto.Unmarshal(sub, &cm); err != nil {
				return types.Message{}, false, err
			}
			return fromProto(&cm), true, nil
		}

		// Skip every other field without decoding it.
		vn := protowire.ConsumeFieldValue(num, typ, b)
		if vn < 0 {
			return types.Message{}, false, protowire.ParseError(vn)
		}
		b = b[vn:]
	}
	return types.Message{}, false, nil
}

func fromProto(cm *api.ChannelMessage) types.Message {
	return types.Message{
		ChannelID:    cm.GetChannelId(),
		ClanID:       cm.GetClanId(),
		MessageID:    cm.GetMessageId(),
		SenderID:     cm.GetSenderId(),
		Content:      cm.GetContent(),
		Username:     cm.GetUsername(),
		DisplayName:  cm.GetDisplayName(),
		ClanNick:     cm.GetClanNick(),
		ChannelLabel: cm.GetChannelLabel(),
		Mode:         cm.GetMode(),
		IsPublic:     cm.GetIsPublic(),
	}
}
