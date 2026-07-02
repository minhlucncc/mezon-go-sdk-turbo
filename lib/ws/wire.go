package ws

import (
	"strconv"

	"google.golang.org/protobuf/encoding/protowire"
)

// This file hand-encodes the outgoing envelopes against the LIVE server
// schema (verified against the mezon-sdk python protobuf, 2026-06). The
// pinned Go codegen (mezon-go-sdk v0.0.34) has drifted in two fatal ways:
//   - ids (clan/channel/message/sender) are int64 varints live, strings in
//     the codegen — the server rejects string-encoded frames with
//     "can not unmarshal." (observed on ClanJoin), so nothing ever flows;
//   - ChannelMessage lost two mid-message Timestamp fields, shifting every
//     field >= 9 (handled in decode.go).
// protowire keeps the wire shape explicit and drift-visible.

// Envelope oneof field numbers (live rtapi.Envelope).
const (
	envClanJoin           = 3
	envChannelMessage     = 6 // inbound; see decode.go
	envChannelMessageSend = 8
	envError              = 12
	envPing               = 22
	envTyping             = 24
	envMessageReactionEvent = 26
)

// Mention is one mention entity span over the FINAL text (UTF-16 units).
type Mention struct {
	UserID string // Mezon user id (decimal snowflake)
	S, E   int32
}

// Ref is a reply-as-reference to an earlier message. Field set mirrors the
// Python SDK's Message.reply() exactly (mezonsim differential finding: the
// official Go codegen used message_id/field 1, but production replies carry
// message_ref_id/field 2 — the wire shape Mezon clients actually render).
type Ref struct {
	RefMessageID   string // the replied-to message id (decimal snowflake)
	SenderID       string // its sender (decimal snowflake)
	SenderUsername string // clan_nick || display_name || username of the sender
	SenderAvatar   string
	Content        string // its raw content blob
}

// SendOpts carries the optional delivery decorations of an outgoing message:
// a reply reference, mention entity spans (computed over the FINAL text —
// prefixes must be applied before markdown stripping shifts offsets), and the
// channel-wide mention_everyone flag used for "@here" deliveries.
type SendOpts struct {
	Ref             *Ref
	Mentions        []Mention
	MentionEveryone bool
}

// id encodes a decimal snowflake as the varint the live schema expects.
// Empty / non-numeric (e.g. "" for DMs) encodes as 0.
func id(s string) uint64 {
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}

func appendVarintField(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b // proto3 default — omit
	}
	return protowire.AppendVarint(protowire.AppendTag(b, num, protowire.VarintType), v)
}

func appendStringField(b []byte, num protowire.Number, v string) []byte {
	if v == "" {
		return b
	}
	return protowire.AppendString(protowire.AppendTag(b, num, protowire.BytesType), v)
}

func appendMessageField(b []byte, num protowire.Number, msg []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(b, num, protowire.BytesType), msg)
}

// BuildSendEnvelope assembles the ChannelMessageSend envelope: text is wrapped
// in Mezon's {"t": ...} content blob (markdown → mk entities, bare URLs → lk
// entities), opts attach references/mentions. Pure — exists so the wire shape
// is testable without a socket.
//
// Live ChannelMessageSend: 1 clan_id i64, 2 channel_id i64, 3 content str,
// 4 mentions []MessageMention, 6 references []MessageRef, 7 mode i32,
// 9 mention_everyone bool, 11 is_public bool.
func BuildSendEnvelope(channelID, clanID string, mode int32, isPublic bool, text string, opts SendOpts) []byte {
	return BuildSendEnvelopeRaw(channelID, clanID, mode, isPublic, BuildContent(text), opts)
}

// BuildSendEnvelopeRaw is BuildSendEnvelope with a PRE-RENDERED content blob
// (no markdown→entity pass) — the seam the cross-SDK wire goldens lock.
func BuildSendEnvelopeRaw(channelID, clanID string, mode int32, isPublic bool, content string, opts SendOpts) []byte {
	var msg []byte
	msg = appendVarintField(msg, 1, id(clanID))
	msg = appendVarintField(msg, 2, id(channelID))
	msg = appendStringField(msg, 3, content)
	for _, m := range opts.Mentions {
		// live MessageMention: 2 user_id i64, 7 s i32, 8 e i32
		var mm []byte
		mm = appendVarintField(mm, 2, id(m.UserID))
		mm = appendVarintField(mm, 7, uint64(uint32(m.S)))
		mm = appendVarintField(mm, 8, uint64(uint32(m.E)))
		msg = appendMessageField(msg, 4, mm)
	}
	if opts.Ref != nil {
		// live MessageRef (python-parity field set): 2 message_ref_id i64,
		// 3 content str, 6 message_sender_id i64, 7 message_sender_username,
		// 8 message_sender_avatar
		var mr []byte
		mr = appendVarintField(mr, 2, id(opts.Ref.RefMessageID))
		mr = appendStringField(mr, 3, opts.Ref.Content)
		mr = appendVarintField(mr, 6, id(opts.Ref.SenderID))
		mr = appendStringField(mr, 7, opts.Ref.SenderUsername)
		mr = appendStringField(mr, 8, opts.Ref.SenderAvatar)
		msg = appendMessageField(msg, 6, mr)
	}
	msg = appendVarintField(msg, 7, uint64(uint32(mode)))
	if opts.MentionEveryone {
		msg = appendVarintField(msg, 9, 1)
	}
	if isPublic {
		msg = appendVarintField(msg, 11, 1)
	}
	return appendMessageField(nil, envChannelMessageSend, msg)
}

// BuildClanJoinEnvelope assembles the ClanJoin envelope (live: clan_id i64).
func BuildClanJoinEnvelope(clanID string) []byte {
	inner := appendVarintField(nil, 1, id(clanID))
	return appendMessageField(nil, envClanJoin, inner)
}

// BuildTypingEnvelope assembles a MessageTypingEvent envelope.
// Live: 1 clan_id i64, 2 channel_id i64, 3 sender_id i64, 4 mode i32, 5 is_public bool.
func BuildTypingEnvelope(channelID, clanID, senderID string, mode int32, isPublic bool) []byte {
	var msg []byte
	msg = appendVarintField(msg, 1, id(clanID))
	msg = appendVarintField(msg, 2, id(channelID))
	msg = appendVarintField(msg, 3, id(senderID))
	msg = appendVarintField(msg, 4, uint64(uint32(mode)))
	if isPublic {
		msg = appendVarintField(msg, 5, 1)
	}
	return appendMessageField(nil, envTyping, msg)
}

// BuildReactionEnvelope assembles a MessageReactionEvent envelope (field 26)
// for adding a reaction to a message.  emojiID is the numeric ID for a custom
// clan emoji (empty for Unicode emojis).  emoji is either a Unicode glyph or a
// custom shortcode (e.g. "pepe_joy").  Action true = add, false = remove.
//
// Live MessageReaction: 1 id str, 2 emoji_id i64, 3 emoji str, 4 sender_id i64,
// 5 sender_name str, 6 sender_avatar str, 7 action bool, 8 count i32,
// 9 channel_id i64, 10 message_id i64, 11 clan_id i64, 12 mode i32,
// 13 message_sender_id i64, 14 is_public bool.
func BuildReactionEnvelope(clanID, channelID, messageID, emojiID, emoji string, action bool, mode int32, isPublic bool) []byte {
	var msg []byte
	if emojiID != "" {
		msg = appendVarintField(msg, 2, id(emojiID))
	}
	msg = appendStringField(msg, 3, emoji)
	if action {
		msg = appendVarintField(msg, 7, 1) // action=true
	}
	msg = appendVarintField(msg, 9, id(channelID))
	msg = appendVarintField(msg, 10, id(messageID))
	msg = appendVarintField(msg, 11, id(clanID))
	msg = appendVarintField(msg, 12, uint64(uint32(mode)))
	if isPublic {
		msg = appendVarintField(msg, 14, 1)
	}
	return appendMessageField(nil, envMessageReactionEvent, msg)
}

// BuildStickerEnvelope assembles a ChannelMessageSend envelope (field 8) with
// an ATTACHMENT carrying the sticker image URL and shortname.  Stickers are
// sent as messages with a single MessageAttachment whose Filetype is "sticker".
//
// The attachment layout mirrors what the live server expects for sticker sends:
// 1 filename (shortcode), 3 url (sticker CDN src), 4 filetype ("sticker").
func BuildStickerEnvelope(channelID, clanID string, mode int32, isPublic bool, stickerShortcode, stickerURL string) []byte {
	// Build a single MessageAttachment.
	var att []byte
	att = appendStringField(att, 1, stickerShortcode)
	att = appendStringField(att, 3, stickerURL)
	att = appendStringField(att, 4, "sticker")

	var msg []byte
	msg = appendVarintField(msg, 1, id(clanID))
	msg = appendVarintField(msg, 2, id(channelID))
	msg = appendMessageField(msg, 5, att) // attachments field
	msg = appendVarintField(msg, 7, uint64(uint32(mode)))
	if isPublic {
		msg = appendVarintField(msg, 11, 1)
	}
	return appendMessageField(nil, envChannelMessageSend, msg)
}

// BuildPingEnvelope assembles a keepalive ping (empty Ping message, field 22).
func BuildPingEnvelope() []byte {
	return appendMessageField(nil, envPing, nil)
}

// BuildPingEnvelopeWithCid is BuildPingEnvelope with a request cid (Envelope
// field 1, int32 live). The server only ACKNOWLEDGES cid'd pings (pong) and
// reaps sockets whose pings are cid-less after exactly 120s — every bot went
// deaf on a 2-minute cycle until pings carried cids (probe-verified live,
// matching the Python SDK's _send_with_cid heartbeat).
func BuildPingEnvelopeWithCid(cid uint64) []byte {
	env := appendVarintField(nil, 1, cid)
	return append(env, appendMessageField(nil, envPing, nil)...)
}
