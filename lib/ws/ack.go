package ws

import (
	"strconv"

	"google.golang.org/protobuf/encoding/protowire"
)

// Request/acknowledge: a frame sent with a cid (Envelope field 1) is answered
// by the server with an Envelope carrying the same cid and either a
// channel_message_ack (field 7) or an error (field 12). The ack is the only
// place the server tells us the id of a message we sent — which is what a
// later edit of that message needs.

const (
	envChannelMessageAck    = 7
	envChannelMessageUpdate = 9
)

// Ack is one server reply to a cid'd request.
type Ack struct {
	Cid       uint64
	MessageID string // the sent/edited message's id, when the server gave one
	Err       bool   // the server answered with an Error envelope
}

// DecodeAck reports whether frame is a reply to a cid'd request. It reads only
// the Envelope's cid, channel_message_ack and error fields.
//
// Live ChannelMessageAck: 1 channel_id, 2 message_id. Ids are int64 varints on
// the live server (see wire.go); a string-encoded id is accepted too, so a
// server that moves to the codegen's string ids does not silently lose acks.
func DecodeAck(frame []byte) (Ack, bool) {
	var a Ack
	var hasCid, hasReply bool
	walkVarints(frame, func(num protowire.Number, v uint64, sub []byte) {
		switch {
		case num == 1 && sub == nil:
			a.Cid, hasCid = v, true
		case num == 1 && sub != nil:
			if n, err := strconv.ParseUint(string(sub), 10, 64); err == nil {
				a.Cid, hasCid = n, true
			}
		case num == envChannelMessageAck && sub != nil:
			hasReply = true
			walkVarints(sub, func(f protowire.Number, fv uint64, fs []byte) {
				if f != 2 {
					return
				}
				if fs == nil {
					a.MessageID = strconv.FormatUint(fv, 10)
				} else {
					a.MessageID = string(fs)
				}
			})
		case num == envError && sub != nil:
			hasReply, a.Err = true, true
		}
	})
	return a, hasCid && hasReply
}

// WithCid prefixes an envelope with a request cid, so the server acknowledges
// it. The envelopes built in wire.go carry no cid; an Envelope's fields may
// appear in any order, so prefixing is equivalent to setting field 1.
func WithCid(cid uint64, env []byte) []byte {
	return append(appendVarintField(nil, 1, cid), env...)
}

// BuildUpdateEnvelope edits a message this bot sent earlier.
//
// Live ChannelMessageUpdate: 1 clan_id i64, 2 channel_id i64, 3 message_id
// i64, 4 content str, 7 mode i32, 8 is_public bool, 9 hide_editted bool.
// hide_editted is set: the edit replaces a "working on it" placeholder with
// the answer it promised, and an "(edited)" badge on that would be noise.
func BuildUpdateEnvelope(channelID, clanID, messageID string, mode int32, isPublic bool, text string) []byte {
	var msg []byte
	msg = appendVarintField(msg, 1, id(clanID))
	msg = appendVarintField(msg, 2, id(channelID))
	msg = appendVarintField(msg, 3, id(messageID))
	msg = appendStringField(msg, 4, BuildContent(text))
	msg = appendVarintField(msg, 7, uint64(uint32(mode)))
	if isPublic {
		msg = appendVarintField(msg, 8, 1)
	}
	msg = appendVarintField(msg, 9, 1)
	return appendMessageField(nil, envChannelMessageUpdate, msg)
}
