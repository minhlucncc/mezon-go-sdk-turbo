// Package turbo is the top-level engine of mezon-go-sdk-turbo. It ties together
// the lean WebSocket client (hot tier), the cursor-based REST poller (warm/cold
// tiers), the Redis-offloaded per-bot state, and the hot/warm/cold tiering
// manager. Consumers register bots, supply an OnMessage callback, and the engine
// transparently runs sockets or polls per the resource budget.
package turbo

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nccasia/mezon-go-sdk/mezon-protobuf/mezon/v2/common/api"

	"github.com/mezon/mezon-go-sdk-turbo/lib/poller"
	"github.com/mezon/mezon-go-sdk-turbo/lib/state"
	"github.com/mezon/mezon-go-sdk-turbo/lib/tier"
	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

// Config tunes the engine.
type Config struct {
	WSHost        string // Mezon WS host (e.g. "api.mezon.ai")
	WSSSL         bool
	Tier          tier.Config
	PollRPS       int
	PollWorkers   int
	PollPageLimit int32
	StateTTL      time.Duration
	DedupCap      int64
	PingInterval  time.Duration
}

// Engine is the resource-aware Mezon client.
type Engine struct {
	cfg       Config
	store     state.Store
	poller    *poller.Poller
	mgr       *tier.Manager
	pw        *ws.PingWheel
	onMessage func(types.BotRef, types.Message)

	mu  sync.Mutex
	hot map[string]*ws.Conn
}

// New builds an engine. lister is the REST capability (rest.New(apiBase)
// satisfies it); onMessage receives every new inbound message (from a socket or
// a poll) — it is the consumer's turn entrypoint.
func New(cfg Config, rdb redis.Cmdable, lister poller.Lister, onMessage func(types.BotRef, types.Message)) *Engine {
	e := &Engine{
		cfg:       cfg,
		store:     state.NewRedisStore(rdb, cfg.StateTTL, cfg.DedupCap),
		pw:        ws.NewPingWheel(cfg.PingInterval),
		onMessage: onMessage,
		hot:       make(map[string]*ws.Conn),
	}
	e.poller = poller.New(lister, e.store, e.onPollMessage, cfg.PollRPS, cfg.PollWorkers, cfg.PollPageLimit)
	e.mgr = tier.New(cfg.Tier, e) // Engine implements tier.Actuator
	return e
}

// Register adds a bot (starts Cold; the manager promotes it on activity).
func (e *Engine) Register(bot types.BotRef) { e.mgr.Add(bot) }

// AddChannel registers a channel id to poll for a bot. Channels are normally
// learned from inbound messages; seed them explicitly for a cold bot that must
// be polled before any live activity (e.g. from tenant_clans.indexed_channel_ids).
func (e *Engine) AddChannel(keyID, channelID string) error {
	return e.store.AddChannel(context.Background(), keyID, channelID)
}

// Deregister removes a bot and closes its socket if hot.
func (e *Engine) Deregister(keyID string) {
	e.mgr.Remove(keyID)
	e.CloseHot(keyID)
}

// Run starts the ping-wheel and the tiering loop; blocks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) {
	go e.pw.Run(ctx)
	e.mgr.Run(ctx)
}

// ── inbound delivery ────────────────────────────────────────────────────────

// onHotMessage handles a socket frame — it must dedup (the socket has no prior
// dedup), then deliver.
func (e *Engine) onHotMessage(bot types.BotRef, msg types.Message) {
	if seen, _ := e.store.Seen(context.Background(), bot.KeyID, msg.MessageID); seen {
		return
	}
	e.deliver(bot, msg)
}

// onPollMessage handles a polled message — already deduped by the poller.
func (e *Engine) onPollMessage(bot types.BotRef, msg types.Message) {
	e.deliver(bot, msg)
}

func (e *Engine) deliver(bot types.BotRef, msg types.Message) {
	ctx := context.Background()
	_ = e.store.AddChannel(ctx, bot.KeyID, msg.ChannelID) // learn channels to poll
	e.mgr.Touch(bot.KeyID)                                // bump activity -> maybe promote
	if e.onMessage != nil {
		e.onMessage(bot, msg)
	}
}

// ── tier.Actuator ───────────────────────────────────────────────────────────

// OpenHot opens a live socket for a bot (non-blocking enough for the manager;
// on dial failure the bot is demoted back to Warm).
func (e *Engine) OpenHot(bot types.BotRef) {
	e.mu.Lock()
	_, exists := e.hot[bot.KeyID]
	e.mu.Unlock()
	if exists {
		return
	}
	conn, err := ws.Dial(e.cfg.WSHost, e.cfg.WSSSL, bot.BotToken, bot.BotUserID, nil,
		func(m types.Message) { e.onHotMessage(bot, m) })
	if err != nil {
		e.mgr.ForceTier(bot.KeyID, types.Warm)
		return
	}
	e.pw.Add(conn)
	e.mu.Lock()
	e.hot[bot.KeyID] = conn
	e.mu.Unlock()
}

// CloseHot tears down a bot's live socket.
func (e *Engine) CloseHot(keyID string) {
	e.mu.Lock()
	conn := e.hot[keyID]
	delete(e.hot, keyID)
	e.mu.Unlock()
	if conn != nil {
		e.pw.Remove(conn)
		_ = conn.Close()
	}
}

// Poll runs one REST poll for a warm/cold bot.
func (e *Engine) Poll(bot types.BotRef) {
	e.poller.Submit(context.Background(), bot, bot.BotToken, nil)
}

// ── outbound (used by the consumer's turn handler) ──────────────────────────

// Send replies to a message. If the bot is hot it goes over the live socket;
// otherwise a transient lean socket is opened to send, then closed.
func (e *Engine) Send(bot types.BotRef, in types.Message, text string, asReply bool) error {
	var ref *api.MessageRef
	if asReply && !in.IsDM() {
		ref = &api.MessageRef{MessageId: in.MessageID, MessageSenderId: in.SenderID, Content: in.Content}
	}
	e.mu.Lock()
	conn := e.hot[bot.KeyID]
	e.mu.Unlock()
	if conn != nil && !conn.Closed() {
		return conn.SendText(in.ChannelID, in.ClanID, in.Mode, in.IsPublic, text, ref)
	}
	// Fallback: transient socket (rare — only when hot slots are saturated).
	tmp, err := ws.Dial(e.cfg.WSHost, e.cfg.WSSSL, bot.BotToken, bot.BotUserID, nil, func(types.Message) {})
	if err != nil {
		return err
	}
	defer tmp.Close()
	return tmp.SendText(in.ChannelID, in.ClanID, in.Mode, in.IsPublic, text, ref)
}

// SendTyping emits a typing indicator if the bot is hot (no-op otherwise).
func (e *Engine) SendTyping(bot types.BotRef, in types.Message) {
	e.mu.Lock()
	conn := e.hot[bot.KeyID]
	e.mu.Unlock()
	if conn != nil && !conn.Closed() {
		_ = conn.SendTyping(in.ChannelID, in.ClanID, bot.BotUserID, in.Mode, in.IsPublic)
	}
}
