// Package ws is a lean Mezon WebSocket client for the hot tier. Versus the
// official SDK it uses small (1 KB) buffers, a SHARED ping goroutine (see
// PingWheel) instead of one ticker per connection, and a partial protobuf
// decoder (decode.go) that skips everything but channel_message.
package ws

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

// writeBufferPool is shared across all sockets so a generous write buffer costs
// memory only during an actual write (returned to the pool after), not
// per-connection — keeping the lean-socket memory profile at thousands of bots.
var writeBufferPool = &sync.Pool{}

// leanDialer keeps the per-connection READ buffer small (inbound bot messages are
// tiny). The WRITE buffer, however, must be large enough to hold a whole outbound
// message in ONE WebSocket frame: gorilla fragments any message larger than
// WriteBufferSize into continuation frames, and the Mezon gateway silently DROPS
// fragmented messages — so a long grounded answer (>1 KB) never arrived while
// short greetings did. A shared pool gives the headroom without per-socket cost.
// Subprotocol "protobuf" matches the Python SDK's handshake (probe-verified on
// the live server alongside the format=protobuf query param).
var leanDialer = &websocket.Dialer{
	ReadBufferSize:    1024,
	WriteBufferSize:   65536,
	WriteBufferPool:   writeBufferPool,
	EnableCompression: false,
	HandshakeTimeout:  15 * time.Second,
	Proxy:             http.ProxyFromEnvironment,
	Subprotocols:      []string{"protobuf"},
}

// Conn is one hot bot socket.
type Conn struct {
	ws        *websocket.Conn
	botUserID string
	onMessage func(types.Message)
	onClose   func()
	writeMu   sync.Mutex
	closed    atomic.Bool
	pingCid   atomic.Uint64 // request ids: keepalives and acknowledged sends share one sequence

	ackMu   sync.Mutex
	pending map[uint64]chan Ack // cid'd requests waiting for the server's reply

	joinMu sync.Mutex
	joined map[string]bool // clans this socket has sent ClanJoin for
}

// Dial opens a lean WebSocket as a bot identity and starts the read loop. The
// caller registers the returned Conn with a PingWheel for keepalive. onClose
// (may be nil) fires exactly once when the read loop ends — the OWNER must
// evict the conn and re-dial, or the bot goes permanently deaf while still
// looking hot (the 2026-06-06 "works once then dies" regression).
func Dial(host string, ssl bool, token, botUserID string, clanIDs []string, onMessage func(types.Message), onClose func()) (*Conn, error) {
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
	c := &Conn{ws: raw, botUserID: botUserID, onMessage: onMessage, onClose: onClose,
		joined: make(map[string]bool, len(clanIDs))}
	for _, id := range clanIDs {
		_ = c.JoinClan(id)
	}
	go c.readLoop()
	return c, nil
}

func (c *Conn) readLoop() {
	defer func() {
		c.closed.Store(true)
		if c.onClose != nil {
			c.onClose()
		}
	}()
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		if c.hasPending() {
			if a, ok := DecodeAck(data); ok {
				c.resolve(a)
				continue
			}
		}
		msg, ok, derr := DecodeChannelMessage(data)
		if derr != nil || !ok {
			continue // ping/pong/presence/etc. — or a malformed frame
		}
		if msg.SenderID == c.botUserID {
			continue // never react to our own messages
		}
		c.emit(msg)
	}
}

func (c *Conn) emit(msg types.Message) {
	defer func() { _ = recover() }()
	if c.onMessage != nil {
		c.onMessage(msg)
	}
}

func (c *Conn) send(env []byte) error {
	if c.closed.Load() {
		return websocket.ErrCloseSent
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.ws.WriteMessage(websocket.BinaryMessage, env)
}

// JoinClan subscribes the live socket to a clan the bot joined after dialing;
// without it the socket stays silent for that clan until the next redial.
// Joining a clan twice is a no-op.
func (c *Conn) JoinClan(clanID string) error {
	c.joinMu.Lock()
	defer c.joinMu.Unlock()
	if c.joined[clanID] {
		return nil
	}
	if err := c.send(BuildClanJoinEnvelope(clanID)); err != nil {
		return err
	}
	c.joined[clanID] = true
	return nil
}

// HasJoined reports whether this socket has joined the clan.
func (c *Conn) HasJoined(clanID string) bool {
	c.joinMu.Lock()
	defer c.joinMu.Unlock()
	return c.joined[clanID]
}

// SendText sends a reply. content is plain text; it is wrapped in Mezon's
// {"t": ...} content blob, with "lk" entities marking bare URLs so they
// render clickable. ref (optional) makes it a reply-as-reference.
func (c *Conn) SendText(channelID, clanID string, mode int32, isPublic bool, text string, ref *Ref) error {
	return c.SendTextOpts(channelID, clanID, mode, isPublic, text, SendOpts{Ref: ref})
}

// SendTextOpts is SendText with full delivery decorations (mentions,
// mention_everyone) — used by scheduled-task outbound deliveries.
func (c *Conn) SendTextOpts(channelID, clanID string, mode int32, isPublic bool, text string, opts SendOpts) error {
	return c.send(BuildSendEnvelope(channelID, clanID, mode, isPublic, text, opts))
}

// ErrNoAck means the server did not answer a cid'd request in time. The frame
// may still have been applied; callers treat it as "unconfirmed", not "failed".
var ErrNoAck = errors.New("ws: no acknowledgement from server")

// ErrRejected means the server answered a cid'd request with an Error.
var ErrRejected = errors.New("ws: request rejected by server")

// SendTextAck sends like SendTextOpts, then waits up to timeout for the
// server's acknowledgement and returns the id it gave the new message.
func (c *Conn) SendTextAck(channelID, clanID string, mode int32, isPublic bool, text string, opts SendOpts, timeout time.Duration) (string, error) {
	a, err := c.request(BuildSendEnvelope(channelID, clanID, mode, isPublic, text, opts), timeout)
	if err != nil {
		return "", err
	}
	if a.MessageID == "" {
		return "", ErrNoAck
	}
	return a.MessageID, nil
}

// UpdateText replaces the content of a message this bot sent, and waits up to
// timeout for the server to confirm the edit.
func (c *Conn) UpdateText(channelID, clanID, messageID string, mode int32, isPublic bool, text string, timeout time.Duration) error {
	_, err := c.request(BuildUpdateEnvelope(channelID, clanID, messageID, mode, isPublic, text), timeout)
	return err
}

func (c *Conn) request(env []byte, timeout time.Duration) (Ack, error) {
	cid := c.pingCid.Add(1)
	ch := make(chan Ack, 1)
	c.ackMu.Lock()
	if c.pending == nil {
		c.pending = make(map[uint64]chan Ack)
	}
	c.pending[cid] = ch
	c.ackMu.Unlock()
	defer func() {
		c.ackMu.Lock()
		delete(c.pending, cid)
		c.ackMu.Unlock()
	}()
	if err := c.send(WithCid(cid, env)); err != nil {
		return Ack{}, err
	}
	select {
	case a := <-ch:
		if a.Err {
			return a, ErrRejected
		}
		return a, nil
	case <-time.After(timeout):
		return Ack{}, ErrNoAck
	}
}

func (c *Conn) hasPending() bool {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()
	return len(c.pending) > 0
}

func (c *Conn) resolve(a Ack) {
	c.ackMu.Lock()
	ch := c.pending[a.Cid]
	c.ackMu.Unlock()
	if ch != nil {
		select {
		case ch <- a:
		default:
		}
	}
}

// SendTyping emits a typing indicator. senderUsername/senderDisplayName are
// what clients show ("<name> is typing"); blank names make them show the id.
func (c *Conn) SendTyping(channelID, clanID, senderID, senderUsername, senderDisplayName string, mode int32, isPublic bool) error {
	return c.send(BuildTypingEnvelope(channelID, clanID, senderID, senderUsername, senderDisplayName, mode, isPublic))
}

// SendReaction adds (or removes) an emoji reaction on a message.
// emoji is a Unicode glyph or a custom shortcode (e.g. "pepe_joy").
// emojiID is the numeric ID for custom clan emojis (empty for Unicode).
// action=true adds the reaction; action=false removes it.
func (c *Conn) SendReaction(clanID, channelID, messageID, emojiID, emoji string, action bool, mode int32, isPublic bool) error {
	return c.send(BuildReactionEnvelope(clanID, channelID, messageID, emojiID, emoji, action, mode, isPublic))
}

// SendSticker sends a sticker as a message with an attachment.
// stickerShortcode is the sticker's shortcode (e.g. "froge_no").
// stickerURL is the CDN URL for the sticker image.
func (c *Conn) SendSticker(channelID, clanID string, mode int32, isPublic bool, stickerShortcode, stickerURL string) error {
	return c.send(BuildStickerEnvelope(channelID, clanID, mode, isPublic, stickerShortcode, stickerURL))
}

// ping sends a keepalive (called by the PingWheel). Pings carry an
// incrementing cid — the live server reaps sockets with cid-less pings.
func (c *Conn) ping() error {
	return c.send(BuildPingEnvelopeWithCid(c.pingCid.Add(1)))
}

// Close tears the socket down.
func (c *Conn) Close() error {
	c.closed.Store(true)
	return c.ws.Close()
}

// Closed reports whether the socket has been closed (or its read loop died).
func (c *Conn) Closed() bool { return c.closed.Load() }
