package ws_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

func ackFrame(cid uint64, messageID uint64) []byte {
	var ack []byte
	ack = protowire.AppendTag(ack, 2, protowire.VarintType)
	ack = protowire.AppendVarint(ack, messageID)
	var env []byte
	env = protowire.AppendTag(env, 1, protowire.VarintType)
	env = protowire.AppendVarint(env, cid)
	env = protowire.AppendTag(env, 7, protowire.BytesType)
	return protowire.AppendBytes(env, ack)
}

func errorFrame(cid uint64) []byte {
	var env []byte
	env = protowire.AppendTag(env, 1, protowire.VarintType)
	env = protowire.AppendVarint(env, cid)
	env = protowire.AppendTag(env, 12, protowire.BytesType)
	return protowire.AppendBytes(env, []byte{})
}

func TestDecodeAckReadsCidAndMessageID(t *testing.T) {
	a, ok := ws.DecodeAck(ackFrame(7, 1234567890123))
	if !ok || a.Cid != 7 || a.MessageID != "1234567890123" || a.Err {
		t.Fatalf("got %+v ok=%v", a, ok)
	}
	a, ok = ws.DecodeAck(errorFrame(9))
	if !ok || a.Cid != 9 || !a.Err {
		t.Fatalf("error frame: got %+v ok=%v", a, ok)
	}
	// A pong carries a cid but no reply: not an ack.
	var pong []byte
	pong = protowire.AppendTag(pong, 1, protowire.VarintType)
	pong = protowire.AppendVarint(pong, 3)
	pong = protowire.AppendTag(pong, 23, protowire.BytesType)
	pong = protowire.AppendBytes(pong, nil)
	if _, ok := ws.DecodeAck(pong); ok {
		t.Fatal("a pong must not decode as an ack")
	}
}

func TestBuildUpdateEnvelopeLayout(t *testing.T) {
	env := ws.BuildUpdateEnvelope("22", "11", "33", 2, true, "hello")
	num, typ, n := protowire.ConsumeTag(env)
	if num != 9 || typ != protowire.BytesType {
		t.Fatalf("envelope field = %d/%d, want channel_message_update (9)", num, typ)
	}
	body, _ := protowire.ConsumeBytes(env[n:])
	got := map[protowire.Number]uint64{}
	var content string
	for len(body) > 0 {
		f, ft, tn := protowire.ConsumeTag(body)
		body = body[tn:]
		if ft == protowire.VarintType {
			v, vn := protowire.ConsumeVarint(body)
			got[f] = v
			body = body[vn:]
		} else {
			v, vn := protowire.ConsumeBytes(body)
			if f == 4 {
				content = string(v)
			}
			body = body[vn:]
		}
	}
	want := map[protowire.Number]uint64{1: 11, 2: 22, 3: 33, 7: 2, 8: 1, 9: 1}
	for f, v := range want {
		if got[f] != v {
			t.Errorf("field %d = %d, want %d", f, got[f], v)
		}
	}
	if !strings.Contains(content, "hello") {
		t.Errorf("content %q does not carry the text", content)
	}
}

// fakeAcker answers every cid'd frame: sends get an ack with a message id,
// updates get an ack or (reject=true) an error.
func fakeAcker(t *testing.T, reject bool) *httptest.Server {
	up := websocket.Upgrader{Subprotocols: []string{"protobuf"}}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			_, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			num, typ, n := protowire.ConsumeTag(data)
			if num != 1 || typ != protowire.VarintType {
				continue
			}
			cid, cn := protowire.ConsumeVarint(data[n:])
			rest := data[n+cn:]
			kind, _, _ := protowire.ConsumeTag(rest)
			var reply []byte
			switch {
			case kind == 9 && reject:
				reply = errorFrame(cid)
			case kind == 8 || kind == 9:
				reply = ackFrame(cid, 555)
			default:
				continue
			}
			_ = c.WriteMessage(websocket.BinaryMessage, reply)
		}
	}))
}

func dial(t *testing.T, srv *httptest.Server) *ws.Conn {
	host := strings.TrimPrefix(srv.URL, "http://")
	c, err := ws.Dial(host, false, "tok", "bot", nil, func(types.Message) {}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestSendTextAckReturnsTheNewMessageID(t *testing.T) {
	srv := fakeAcker(t, false)
	defer srv.Close()
	c := dial(t, srv)
	id, err := c.SendTextAck("22", "11", 2, true, "working on it", ws.SendOpts{}, 2*time.Second)
	if err != nil || id != "555" {
		t.Fatalf("id=%q err=%v", id, err)
	}
	if err := c.UpdateText("22", "11", id, 2, true, "the answer", 2*time.Second); err != nil {
		t.Fatalf("update: %v", err)
	}
}

func TestUpdateTextReportsRejection(t *testing.T) {
	srv := fakeAcker(t, true)
	defer srv.Close()
	c := dial(t, srv)
	err := c.UpdateText("22", "11", "555", 2, true, "x", 2*time.Second)
	if !errors.Is(err, ws.ErrRejected) {
		t.Fatalf("err = %v, want ErrRejected", err)
	}
}

func TestSendTextAckTimesOutWithoutAnAck(t *testing.T) {
	up := websocket.Upgrader{Subprotocols: []string{"protobuf"}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	c := dial(t, srv)
	if _, err := c.SendTextAck("22", "11", 2, true, "x", ws.SendOpts{}, 200*time.Millisecond); !errors.Is(err, ws.ErrNoAck) {
		t.Fatalf("err = %v, want ErrNoAck", err)
	}
}
