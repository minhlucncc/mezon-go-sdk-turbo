package ws_test

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

// Cross-SDK wire goldens: every frame in testdata/wire_golden.json was
// encoded by the PYTHON SDK's protobuf — the pin that tracks the live Mezon
// server. The Go encoder/decoder must agree byte-for-byte. This is the test
// that turns silent schema drift (2026-06-06: int64 ids + removed fields →
// "can not unmarshal." on every ClanJoin, every inbound message dropped)
// into a loud CI failure. Regenerate with:
//
//	uv --directory archive/worker-mezon run python \
//	    ../../apps/worker-mezon-go/scripts/gen_wire_golden.py
type wireGolden struct {
	Inbound []struct {
		Name     string `json:"name"`
		FrameB64 string `json:"frame_b64"`
		Expect   struct {
			ChannelID    string `json:"channel_id"`
			ClanID       string `json:"clan_id"`
			MessageID    string `json:"message_id"`
			SenderID     string `json:"sender_id"`
			Username     string `json:"username"`
			Content      string `json:"content"`
			ChannelLabel string `json:"channel_label"`
			DisplayName  string `json:"display_name"`
			ClanNick     string `json:"clan_nick"`
			Mentions     string `json:"mentions"`
			References   string `json:"references"`
			Mode         int32  `json:"mode"`
			IsPublic     bool   `json:"is_public"`
		} `json:"expect"`
	} `json:"inbound"`
	Outbound []struct {
		Name      string `json:"name"`
		Kind      string `json:"kind"`
		ChannelID string `json:"channel_id"`
		ClanID    string `json:"clan_id"`
		SenderID  string `json:"sender_id"`
		Mode      int32  `json:"mode"`
		IsPublic  bool   `json:"is_public"`
		Content   string `json:"content"`
		Mention   *struct {
			UserID string `json:"user_id"`
			S      int32  `json:"s"`
			E      int32  `json:"e"`
		} `json:"mention"`
		Ref *struct {
			RefMessageID   string `json:"ref_message_id"`
			SenderID       string `json:"sender_id"`
			SenderUsername string `json:"sender_username"`
			Content        string `json:"content"`
		} `json:"ref"`
		MentionEveryone bool   `json:"mention_everyone"`
		ExpectB64       string `json:"expect_b64"`
	} `json:"outbound"`
}

func loadWireGolden(t *testing.T) wireGolden {
	t.Helper()
	raw, err := os.ReadFile("testdata/wire_golden.json")
	if err != nil {
		t.Fatalf("wire goldens missing (run gen_wire_golden.py): %v", err)
	}
	var g wireGolden
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatalf("bad goldens: %v", err)
	}
	if len(g.Inbound) == 0 || len(g.Outbound) == 0 {
		t.Fatal("empty goldens")
	}
	return g
}

func TestGoldenInboundDecode(t *testing.T) {
	for _, c := range loadWireGolden(t).Inbound {
		t.Run(c.Name, func(t *testing.T) {
			frame, err := base64.StdEncoding.DecodeString(c.FrameB64)
			if err != nil {
				t.Fatal(err)
			}
			msg, ok, err := ws.DecodeChannelMessage(frame)
			if err != nil || !ok {
				t.Fatalf("python-encoded frame must decode: ok=%v err=%v", ok, err)
			}
			e := c.Expect
			if msg.ChannelID != e.ChannelID || msg.ClanID != e.ClanID ||
				msg.MessageID != e.MessageID || msg.SenderID != e.SenderID {
				t.Fatalf("ids wrong: %+v want %+v", msg, e)
			}
			if msg.Content != e.Content || msg.Username != e.Username ||
				msg.DisplayName != e.DisplayName || msg.ClanNick != e.ClanNick ||
				msg.ChannelLabel != e.ChannelLabel {
				t.Fatalf("strings wrong: %+v want %+v", msg, e)
			}
			if msg.Mentions != e.Mentions || msg.References != e.References {
				t.Fatalf("blobs wrong: %+v want %+v", msg, e)
			}
			if msg.Mode != e.Mode || msg.IsPublic != e.IsPublic {
				t.Fatalf("mode/public wrong: %+v want %+v", msg, e)
			}
		})
	}
}

func TestGoldenOutboundEncode(t *testing.T) {
	for _, c := range loadWireGolden(t).Outbound {
		t.Run(c.Name, func(t *testing.T) {
			want, err := base64.StdEncoding.DecodeString(c.ExpectB64)
			if err != nil {
				t.Fatal(err)
			}
			var got []byte
			switch c.Kind {
			case "clan_join":
				got = ws.BuildClanJoinEnvelope(c.ClanID)
			case "typing":
				got = ws.BuildTypingEnvelope(c.ChannelID, c.ClanID, c.SenderID, "", "", c.Mode, c.IsPublic)
			case "send":
				opts := ws.SendOpts{MentionEveryone: c.MentionEveryone}
				if c.Mention != nil {
					opts.Mentions = []ws.Mention{{UserID: c.Mention.UserID, S: c.Mention.S, E: c.Mention.E}}
				}
				if c.Ref != nil {
					opts.Ref = &ws.Ref{
						RefMessageID: c.Ref.RefMessageID, SenderID: c.Ref.SenderID,
						SenderUsername: c.Ref.SenderUsername, Content: c.Ref.Content,
					}
				}
				got = ws.BuildSendEnvelopeRaw(c.ChannelID, c.ClanID, c.Mode, c.IsPublic, c.Content, opts)
			default:
				t.Fatalf("unknown kind %q", c.Kind)
			}
			if string(got) != string(want) {
				t.Fatalf("wire bytes differ from the python encoding\n got %x\nwant %x", got, want)
			}
		})
	}
}

// TestLiveCapturedFrame decodes a frame CAPTURED FROM THE LIVE SERVER
// (wsprobe self-test, 2026-06-06 08:45:22 — sock.mezon.ai pushed the bot's
// own message back). This is real production bytes, not a fixture anyone
// synthesized — the strongest possible decode lock.
func TestLiveCapturedFrame(t *testing.T) {
	raw, err := os.ReadFile("testdata/live_channel_message.b64")
	if err != nil {
		t.Fatalf("live capture missing: %v", err)
	}
	frame, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	msg, ok, err := ws.DecodeChannelMessage(frame)
	if err != nil || !ok {
		t.Fatalf("live frame must decode: ok=%v err=%v", ok, err)
	}
	if msg.ChannelID != "2061345416766558208" || msg.ClanID != "2061345416372293632" ||
		msg.SenderID != "2062754877070643200" {
		t.Fatalf("live ids wrong: %+v", msg)
	}
	if msg.Content != `{"t":"wsprobe self-test 08:45:22"}` {
		t.Fatalf("live content wrong: %q", msg.Content)
	}
	if msg.Mode != 2 || !msg.IsPublic {
		t.Fatalf("live mode/public wrong: %+v", msg)
	}
}
