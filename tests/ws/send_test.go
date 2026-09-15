package ws_test

import (
	"encoding/json"
	"testing"
	"unicode/utf16"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

// sendWire is the decoded live-schema ChannelMessageSend (Envelope field 8).
// LIVE types (mezon-sdk python protobuf, 2026-06): ids are int64 varints —
// the pinned Go codegen still encodes them as strings, which the server
// rejects with "can not unmarshal.".
type sendWire struct {
	clanID, channelID uint64
	content           string
	mode              int32
	isPublic          bool
	mentionEveryone   bool
	mentions          []mentionWire
	refs              []refWire
}

type mentionWire struct {
	userID uint64
	s, e   int32
}

type refWire struct {
	refMessageID, senderID uint64
	content                string
}

func parseSendEnvelope(tb testing.TB, env []byte) sendWire {
	tb.Helper()
	var out sendWire
	found := false
	walk(tb, env, func(num protowire.Number, typ protowire.Type, v []byte, u uint64) {
		if num != 8 || typ != protowire.BytesType {
			return
		}
		found = true
		walk(tb, v, func(num protowire.Number, typ protowire.Type, v []byte, u uint64) {
			switch num {
			case 1:
				out.clanID = u
			case 2:
				out.channelID = u
			case 3:
				out.content = string(v)
			case 4:
				m := mentionWire{}
				walk(tb, v, func(num protowire.Number, _ protowire.Type, v []byte, u uint64) {
					switch num {
					case 2:
						m.userID = u
					case 7:
						m.s = int32(u)
					case 8:
						m.e = int32(u)
					}
				})
				out.mentions = append(out.mentions, m)
			case 6:
				r := refWire{}
				walk(tb, v, func(num protowire.Number, _ protowire.Type, v []byte, u uint64) {
					switch num {
					case 2:
						r.refMessageID = u
					case 3:
						r.content = string(v)
					case 6:
						r.senderID = u
					}
				})
				out.refs = append(out.refs, r)
			case 7:
				out.mode = int32(u)
			case 9:
				out.mentionEveryone = u != 0
			case 11:
				out.isPublic = u != 0
			}
		})
	})
	if !found {
		tb.Fatalf("envelope carries no channel_message_send (field 8)")
	}
	return out
}

// walk visits every field; varints pass u, bytes pass v.
func walk(tb testing.TB, b []byte, visit func(protowire.Number, protowire.Type, []byte, uint64)) {
	tb.Helper()
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			tb.Fatalf("bad tag")
		}
		b = b[n:]
		switch typ {
		case protowire.VarintType:
			u, n := protowire.ConsumeVarint(b)
			if n < 0 {
				tb.Fatalf("bad varint")
			}
			visit(num, typ, nil, u)
			b = b[n:]
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				tb.Fatalf("bad bytes")
			}
			visit(num, typ, v, 0)
			b = b[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				tb.Fatalf("bad field")
			}
			b = b[n:]
		}
	}
}

func TestBuildSendEnvelopePlain(t *testing.T) {
	env := ws.BuildSendEnvelope("123456", "789", 2, true, "hello", ws.SendOpts{})
	out := parseSendEnvelope(t, env)
	if out.channelID != 123456 || out.clanID != 789 {
		t.Fatalf("channel/clan routing wrong: %+v", out)
	}
	if out.mode != 2 || !out.isPublic {
		t.Fatalf("mode/is_public wrong: %+v", out)
	}
	if blob := decodeBlob(t, out.content); blob.T != "hello" {
		t.Fatalf("content text = %q, want hello", blob.T)
	}
	if len(out.mentions) != 0 || out.mentionEveryone || len(out.refs) != 0 {
		t.Fatalf("plain send must carry no decorations: %+v", out)
	}
}

func TestBuildSendEnvelopeReplyRef(t *testing.T) {
	env := ws.BuildSendEnvelope("11", "22", 2, true, "answer",
		ws.SendOpts{Ref: &ws.Ref{RefMessageID: "987", SenderID: "654", SenderUsername: "minh", Content: `{"t":"q"}`}})
	out := parseSendEnvelope(t, env)
	if len(out.refs) != 1 || out.refs[0].refMessageID != 987 || out.refs[0].senderID != 654 ||
		out.refs[0].content != `{"t":"q"}` {
		t.Fatalf("reference not attached: %+v", out.refs)
	}
}

// Creator-mention delivery (scheduled tasks): the text is prefixed with a
// display token and the mention span covers it in UTF-16 units, matching the
// Python worker's ApiMessageMention(user_id, s=0, e=len(prefix)).
func TestBuildSendEnvelopeCreatorMention(t *testing.T) {
	prefix := "@bạn"
	text := prefix + " Daily digest ready."
	env := ws.BuildSendEnvelope("11", "22", 2, true, text, ws.SendOpts{
		Mentions: []ws.Mention{{UserID: "42", S: 0, E: int32(len(utf16.Encode([]rune(prefix))))}},
	})
	out := parseSendEnvelope(t, env)
	if len(out.mentions) != 1 {
		t.Fatalf("mention not attached: %+v", out.mentions)
	}
	got := out.mentions[0]
	if got.userID != 42 || got.s != 0 || got.e != 4 {
		t.Fatalf("mention span wrong: %+v", got)
	}
	if blob := decodeBlob(t, out.content); blob.T != text {
		t.Fatalf("content text = %q, want %q", blob.T, text)
	}
	if out.mentionEveryone {
		t.Fatal("creator mention must not set mention_everyone")
	}
}

func TestBuildSendEnvelopeMentionEveryone(t *testing.T) {
	env := ws.BuildSendEnvelope("11", "22", 2, true, "@here Daily digest ready.",
		ws.SendOpts{MentionEveryone: true})
	out := parseSendEnvelope(t, env)
	if !out.mentionEveryone {
		t.Fatal("mention_everyone not set")
	}
	if len(out.mentions) != 0 {
		t.Fatalf("@here uses the flag, not mention entities: %+v", out.mentions)
	}
}

// Markdown markers in the body must still become mk entities — the mention
// span sits at offset 0 (before any stripped markers) so it stays valid.
func TestBuildSendEnvelopeMentionWithMarkdownBody(t *testing.T) {
	env := ws.BuildSendEnvelope("11", "22", 2, true, "@bạn **bold** body", ws.SendOpts{})
	out := parseSendEnvelope(t, env)
	blob := decodeBlob(t, out.content)
	if blob.T != "@bạn bold body" {
		t.Fatalf("markdown markers not stripped: %q", blob.T)
	}
	if len(blob.Mk) != 1 || blob.Mk[0].Type != "b" {
		t.Fatalf("bold entity missing: %+v", blob.Mk)
	}
}

// The clan-join frame must carry the clan id as an int64 varint — string
// encoding gets the live server's "can not unmarshal." rejection.
func TestBuildClanJoinEnvelope(t *testing.T) {
	env := ws.BuildClanJoinEnvelope("1780431535405535232")
	var clanID uint64
	walk(t, env, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) {
		if num != 3 || typ != protowire.BytesType { // Envelope.clan_join
			t.Fatalf("unexpected envelope field %d", num)
		}
		walk(t, v, func(num protowire.Number, _ protowire.Type, _ []byte, u uint64) {
			if num == 1 {
				clanID = u
			}
		})
	})
	if clanID != 1780431535405535232 {
		t.Fatalf("clan id = %d", clanID)
	}
}

func TestBuildTypingEnvelope(t *testing.T) {
	env := ws.BuildTypingEnvelope("22", "11", "33", "meknow", "MeKnow Bot", 2, true)
	var got struct {
		clan, chan_, sender, mode, public uint64
		username, displayName             string
	}
	walk(t, env, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) {
		if num != 24 || typ != protowire.BytesType { // Envelope.message_typing_event
			t.Fatalf("unexpected envelope field %d", num)
		}
		walk(t, v, func(num protowire.Number, _ protowire.Type, b []byte, u uint64) {
			switch num {
			case 1:
				got.clan = u
			case 2:
				got.chan_ = u
			case 3:
				got.sender = u
			case 4:
				got.mode = u
			case 5:
				got.public = u
			case 6:
				got.username = string(b)
			case 7:
				got.displayName = string(b)
			}
		})
	})
	if got.clan != 11 || got.chan_ != 22 || got.sender != 33 || got.mode != 2 || got.public != 1 {
		t.Fatalf("typing event wrong: %+v", got)
	}
	// Clients render "<sender_display_name || sender_username> is typing" and
	// fall back to the raw sender id when both are empty (mezon web ChatContext).
	if got.username != "meknow" || got.displayName != "MeKnow Bot" {
		t.Fatalf("typing sender name wrong: %+v", got)
	}
}

// A bot with no display name still shows a name: display name falls back to
// the username (mezon-ios writeMessageTyping parity).
func TestBuildTypingEnvelopeDisplayNameFallsBackToUsername(t *testing.T) {
	env := ws.BuildTypingEnvelope("22", "11", "33", "meknow", "", 2, true)
	var displayName string
	walk(t, env, func(_ protowire.Number, _ protowire.Type, v []byte, _ uint64) {
		walk(t, v, func(num protowire.Number, _ protowire.Type, b []byte, _ uint64) {
			if num == 7 {
				displayName = string(b)
			}
		})
	})
	if displayName != "meknow" {
		t.Fatalf("display name = %q, want username fallback", displayName)
	}
}

// DMs have no clan — clan id "" / "0" must encode as 0, not error out.
func TestBuildSendEnvelopeDMClan(t *testing.T) {
	env := ws.BuildSendEnvelope("123", "", 4, false, "dm reply", ws.SendOpts{})
	out := parseSendEnvelope(t, env)
	if out.clanID != 0 || out.channelID != 123 || out.mode != 4 || out.isPublic {
		t.Fatalf("dm envelope wrong: %+v", out)
	}
}

// keep linters honest about the helper types
var _ = json.Marshal

// Keepalive pings must carry an incrementing cid (Envelope field 1, int32) —
// live finding (2026-06-06): the server only acknowledges cid'd pings (pong,
// fields [1 23]) and reaps sockets with cid-less pings after exactly 120s,
// turning every bot deaf on a 2-minute cycle.
func TestBuildPingEnvelopeWithCid(t *testing.T) {
	env := ws.BuildPingEnvelopeWithCid(7)
	var cid uint64
	sawPing := false
	walk(t, env, func(num protowire.Number, typ protowire.Type, _ []byte, u uint64) {
		switch {
		case num == 1 && typ == protowire.VarintType:
			cid = u
		case num == 22 && typ == protowire.BytesType:
			sawPing = true
		}
	})
	if cid != 7 || !sawPing {
		t.Fatalf("ping envelope wrong: cid=%d ping=%v", cid, sawPing)
	}
}
