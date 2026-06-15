package ws

import (
	"encoding/json"
	"strconv"

	"google.golang.org/protobuf/encoding/protowire"

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
			msg, err := decodeChannelMessageFields(sub)
			if err != nil {
				return types.Message{}, false, err
			}
			return msg, true, nil
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

// decodeChannelMessageFields walks a ChannelMessage with the LIVE server's
// field numbers (mezon-sdk python protobuf, 2026-06). The pinned Go codegen
// (mezon-go-sdk v0.0.34) still carries create_time/update_time as fields 9/10
// — the server removed them and every later field shifted down by two, so a
// codegen proto.Unmarshal fails ("cannot parse invalid wire-format data") and
// silently drops EVERY message. Hand-walking just the needed fields keeps the
// decoder correct against the live schema and resilient to future additions.
//
// Live layout (ids are int64 VARINTS): 1 clan_id, 2 channel_id, 3 message_id, 4 code, 5 sender_id,
// 6 username, 7 avatar, 8 content, 9 channel_label, 10 clan_logo,
// 11 category_name, 12 display_name, 13 clan_nick, 14 clan_avatar,
// 15 reactions, 16 mentions, 17 attachments, 18 references,
// 19 referenced_message, 20/21 create/update_time_seconds, 22 mode,
// 23 hide_editted, 24 is_public, 25 topic_id.
func decodeChannelMessageFields(b []byte) (types.Message, error) {
	var msg types.Message
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return msg, protowire.ParseError(n)
		}
		b = b[n:]

		if typ == protowire.BytesType {
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return msg, protowire.ParseError(n)
			}
			b = b[n:]
			switch num {
			case 6:
				msg.Username = v
			case 7:
				msg.Avatar = v
			case 8:
				msg.Content = v
			case 9:
				msg.ChannelLabel = v
			case 12:
				msg.DisplayName = v
			case 13:
				msg.ClanNick = v
			case 16:
				msg.Mentions = mentionsToJSON([]byte(v))
			case 18:
				msg.References = refsToJSON([]byte(v))
			}
			continue
		}
		if typ == protowire.VarintType {
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return msg, protowire.ParseError(n)
			}
			b = b[n:]
			switch num {
			// ids are int64 varints live (decimal-snowflake strings everywhere
			// above the wire — the locked cross-SDK contract).
			case 1:
				msg.ClanID = strconv.FormatUint(v, 10)
			case 2:
				msg.ChannelID = strconv.FormatUint(v, 10)
			case 3:
				msg.MessageID = strconv.FormatUint(v, 10)
			case 5:
				msg.SenderID = strconv.FormatUint(v, 10)
			case 22:
				msg.Mode = int32(v)
			case 24:
				msg.IsPublic = v != 0
			}
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return msg, protowire.ParseError(n)
		}
		b = b[n:]
	}
	return msg, nil
}


// Over the WS wire, ChannelMessage.mentions/references are NESTED PROTOBUFS
// (MessageMentionList / MessageRefList) — unlike the REST list-messages API,
// which returns them as JSON strings. Downstream consumers (the addressed
// gate, context capture) speak the REST JSON dialect, so the decoder converts:
// the wire format is an implementation detail that must not leak upward.

// mentionsToJSON converts MessageMentionList{1: repeated MessageMention{2
// user_id i64, 7 s i32, 8 e i32}} to `[{"user_id":"...","s":N,"e":N}]`.
func mentionsToJSON(b []byte) string {
	type mention struct {
		UserID string `json:"user_id"`
		S      int32  `json:"s"`
		E      int32  `json:"e"`
	}
	var out []mention
	walkWire(b, 1, func(item []byte) {
		var m mention
		walkVarints(item, func(num protowire.Number, v uint64, sv []byte) {
			switch num {
			case 2:
				m.UserID = strconv.FormatUint(v, 10)
			case 7:
				m.S = int32(v)
			case 8:
				m.E = int32(v)
			}
		})
		out = append(out, m)
	})
	if len(out) == 0 {
		return ""
	}
	j, _ := json.Marshal(out)
	return string(j)
}

// refsToJSON converts MessageRefList{1: repeated MessageRef{1 message_id i64,
// 3 content str, 6 message_sender_id i64}} to the REST JSON dialect.
func refsToJSON(b []byte) string {
	type ref struct {
		MessageID string `json:"message_id"`
		SenderID  string `json:"message_sender_id"`
		Content   string `json:"content"`
	}
	var out []ref
	walkWire(b, 1, func(item []byte) {
		var r ref
		walkVarints(item, func(num protowire.Number, v uint64, sv []byte) {
			switch num {
			case 1:
				r.MessageID = strconv.FormatUint(v, 10)
			case 3:
				r.Content = string(sv)
			case 6:
				r.SenderID = strconv.FormatUint(v, 10)
			}
		})
		out = append(out, r)
	})
	if len(out) == 0 {
		return ""
	}
	j, _ := json.Marshal(out)
	return string(j)
}

// walkWire visits every length-delimited occurrence of field `want`.
func walkWire(b []byte, want protowire.Number, visit func([]byte)) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]
		if num == want && typ == protowire.BytesType {
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return
			}
			visit(v)
			b = b[vn:]
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return
		}
		b = b[n:]
	}
}

// walkVarints visits every field, passing varints via v and bytes via sv.
func walkVarints(b []byte, visit func(protowire.Number, uint64, []byte)) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]
		switch typ {
		case protowire.VarintType:
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return
			}
			visit(num, v, nil)
			b = b[vn:]
		case protowire.BytesType:
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return
			}
			visit(num, 0, v)
			b = b[vn:]
		default:
			vn := protowire.ConsumeFieldValue(num, typ, b)
			if vn < 0 {
				return
			}
			b = b[vn:]
		}
	}
}
