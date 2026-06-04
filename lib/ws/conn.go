// Package ws is a lean Mezon WebSocket client for the hot tier. Versus the
// official SDK it uses small (1 KB) buffers, a SHARED ping goroutine (see
// PingWheel) instead of one ticker per connection, and a partial protobuf
// decoder (decode.go) that skips everything but channel_message.
package ws

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/nccasia/mezon-go-sdk/mezon-protobuf/mezon/v2/common/api"
	"github.com/nccasia/mezon-go-sdk/mezon-protobuf/mezon/v2/common/rtapi"

	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

// leanDialer keeps per-connection buffers small — bot messages are tiny, so the
// gorilla default 4 KB read/write buffers are pure waste at thousands of sockets.
var leanDialer = &websocket.Dialer{
	ReadBufferSize:    1024,
	WriteBufferSize:   1024,
	EnableCompression: false,
	HandshakeTimeout:  15 * time.Second,
	Proxy:             http.ProxyFromEnvironment,
}

// Conn is one hot bot socket.
type Conn struct {
	ws        *websocket.Conn
	botUserID string
	onMessage func(types.Message)
	writeMu   sync.Mutex
	closed    atomic.Bool
}

// Dial opens a lean WebSocket as a bot identity and starts the read loop. The
// caller registers the returned Conn with a PingWheel for keepalive.
func Dial(host string, ssl bool, token, botUserID string, clanIDs []string, onMessage func(types.Message)) (*Conn, error) {
	scheme := "wss"
	if !ssl {
		scheme = "ws"
	}
	endpoint := fmt.Sprintf("%s://%s/ws?lang=en&status=true&token=%s&format=protobuf",
		scheme, host, url.QueryEscape(token))
	raw, _, err := leanDialer.Dial(endpoint, nil)
	if err != nil {
		return nil, err
	}
	c := &Conn{ws: raw, botUserID: botUserID, onMessage: onMessage}
	for _, id := range clanIDs {
		_ = c.send(&rtapi.Envelope{Message: &rtapi.Envelope_ClanJoin{ClanJoin: &rtapi.ClanJoin{ClanId: id}}})
	}
	go c.readLoop()
	return c, nil
}

func (c *Conn) readLoop() {
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			c.closed.Store(true)
			return
		}
		msg, ok, derr := DecodeChannelMessage(data)
		if derr != nil || !ok {
			continue // ping/pong/presence/etc. — or a malformed frame
		}
		if msg.SenderID == c.botUserID {
			continue // never react to our own messages
		}
		c.onMessage(msg)
	}
}

func (c *Conn) send(env *rtapi.Envelope) error {
	if c.closed.Load() {
		return websocket.ErrCloseSent
	}
	data, err := proto.Marshal(env)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.ws.WriteMessage(websocket.BinaryMessage, data)
}

// SendText sends a reply. content is plain text; it is wrapped in Mezon's
// {"t": ...} content blob, with "lk" entities marking bare URLs so they
// render clickable. ref (optional) makes it a reply-as-reference.
func (c *Conn) SendText(channelID, clanID string, mode int32, isPublic bool, text string, ref *api.MessageRef) error {
	out := &rtapi.ChannelMessageSend{
		ClanId:    clanID,
		ChannelId: channelID,
		Content:   BuildContent(text),
		Mode:      mode,
		IsPublic:  isPublic,
	}
	if ref != nil {
		out.References = []*api.MessageRef{ref}
	}
	return c.send(&rtapi.Envelope{Message: &rtapi.Envelope_ChannelMessageSend{ChannelMessageSend: out}})
}

// SendTyping emits a typing indicator.
func (c *Conn) SendTyping(channelID, clanID, senderID string, mode int32, isPublic bool) error {
	return c.send(&rtapi.Envelope{Message: &rtapi.Envelope_MessageTypingEvent{
		MessageTypingEvent: &rtapi.MessageTypingEvent{
			ClanId: clanID, ChannelId: channelID, SenderId: senderID, Mode: mode, IsPublic: isPublic,
		},
	}})
}

// ping sends a keepalive (called by the PingWheel).
func (c *Conn) ping() error {
	return c.send(&rtapi.Envelope{Message: &rtapi.Envelope_Ping{Ping: &rtapi.Ping{}}})
}

// Close tears the socket down.
func (c *Conn) Close() error {
	c.closed.Store(true)
	return c.ws.Close()
}

// Closed reports whether the socket has been closed (or its read loop died).
func (c *Conn) Closed() bool { return c.closed.Load() }
