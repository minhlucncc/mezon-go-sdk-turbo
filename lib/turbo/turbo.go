// Package turbo is the top-level engine of mezon-go-sdk-turbo. It ties together
// the lean WebSocket client (hot tier), the cursor-based REST poller (warm/cold
// tiers), the Redis-offloaded per-bot state, and the hot/warm/cold tiering
// manager. Consumers register bots, supply an OnMessage callback, and the engine
// transparently runs sockets or polls per the resource budget.
package turbo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mezon/mezon-go-sdk-turbo/lib/poller"
	"github.com/mezon/mezon-go-sdk-turbo/lib/rest"
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

// Authenticator exchanges a bot's API key for a session (rest.Client
// satisfies it). Mezon's WS handshake and message-list endpoints only accept
// SESSION tokens — the raw key fails with "bad handshake"/401.
type Authenticator interface {
	Authenticate(ctx context.Context, appID, apiKey string) (rest.Session, error)
}

// ClanLister fetches the clans a bot has joined (rest.Client satisfies it).
// The socket only delivers messages for clans the connection explicitly
// JOINS after dialing — without ClanJoin frames a connected bot hears nothing.
type ClanLister interface {
	ListClanIDs(ctx context.Context, baseURL, sessionToken string) ([]string, error)
}

// ClanDirectory lists a bot's clans with names, and a clan's text channels
// (rest.Client satisfies it). It backs SyncClans.
type ClanDirectory interface {
	ListClans(ctx context.Context, baseURL, sessionToken string) ([]rest.Clan, error)
	ListChannels(ctx context.Context, baseURL, sessionToken, clanID string) ([]rest.Channel, error)
}

// ClanSnapshot is one clan the bot has joined, with its text channels.
// ChannelsKnown is false when the channel listing failed — Channels is then
// empty for that reason, not because the clan has none.
type ClanSnapshot struct {
	ID            string
	Name          string
	Channels      []rest.Channel
	ChannelsKnown bool
}

// AccountFetcher fetches the bot's own account (rest.Client satisfies it). The
// session carries only the user id, but the typing indicator must carry the
// bot's username/display name or clients render the raw id.
type AccountFetcher interface {
	GetAccount(ctx context.Context, baseURL, sessionToken string) (rest.Account, error)
}

// channelsBackoff is how long a bot whose channel listing was refused (403)
// goes without asking again.
const channelsBackoff = 6 * time.Hour

// sessionTTL bounds how long an exchanged session token is reused before
// re-authenticating. Dial failures also drop the cached session immediately.
const sessionTTL = 30 * time.Minute

type cachedSession struct {
	token   string
	wsHost  string   // session-provided socket host (e.g. sock.mezon.ai)
	apiURL  string   // session-provided REST host
	clanIDs []string // joined clans + "0" (DM space) — ClanJoin targets
	// bot's own account names (typing indicator); empty if the lookup failed
	username    string
	displayName string
	fetched     time.Time
}

// Engine is the resource-aware Mezon client.
type Engine struct {
	cfg       Config
	store     state.Store
	poller    *poller.Poller
	mgr       *tier.Manager
	pw        *ws.PingWheel
	onMessage func(types.BotRef, types.Message)
	auth      Authenticator  // nil → raw tokens (tests/legacy)
	clans     ClanLister     // nil → dial joins no clans
	dir       ClanDirectory  // nil → SyncClans unsupported
	accounts  AccountFetcher // nil → typing carries only names set on BotRef

	mu sync.Mutex
	// noChannels holds bots whose token may not list channels (a 403) and when
	// that was learned; SyncClans skips the listing for channelsBackoff.
	noChannels map[string]time.Time
	hot        map[string]*ws.Conn
	opening    map[string]struct{}
	sessions   map[string]cachedSession // keyID → exchanged session
}

// New builds an engine. lister is the REST capability (rest.New(apiBase)
// satisfies it); onMessage receives every new inbound message (from a socket or
// a poll) — it is the consumer's turn entrypoint. When lister also implements
// Authenticator (rest.Client does), bot API keys are exchanged for session
// tokens before any dial or poll.
func New(cfg Config, rdb redis.Cmdable, lister poller.Lister, onMessage func(types.BotRef, types.Message)) *Engine {
	e := &Engine{
		cfg:        cfg,
		store:      state.NewRedisStore(rdb, cfg.StateTTL, cfg.DedupCap),
		pw:         ws.NewPingWheel(cfg.PingInterval),
		onMessage:  onMessage,
		hot:        make(map[string]*ws.Conn),
		noChannels: make(map[string]time.Time),
		opening:    make(map[string]struct{}),
		sessions:   make(map[string]cachedSession),
	}
	if auth, ok := lister.(Authenticator); ok {
		e.auth = auth
	}
	if cl, ok := lister.(ClanLister); ok {
		e.clans = cl
	}
	if d, ok := lister.(ClanDirectory); ok {
		e.dir = d
	}
	if af, ok := lister.(AccountFetcher); ok {
		e.accounts = af
	}
	e.poller = poller.New(lister, e.store, e.onPollMessage, cfg.PollRPS, cfg.PollWorkers, cfg.PollPageLimit)
	e.mgr = tier.New(cfg.Tier, e) // Engine implements tier.Actuator
	return e
}

// session returns the bot's session token and the WS host to dial, exchanging
// (and caching) its API key on first use or after expiry. The session response
// ROUTES the socket: live, gw.mezon.ai authenticates but sock.mezon.ai serves
// the WS — dialing the auth host gets "bad handshake". Falls back to the raw
// token + configured host when no Authenticator is wired or the exchange
// fails, so the subsequent dial fails loudly instead of silently dropping.
func (e *Engine) session(bot types.BotRef) (token, wsHost string, clanIDs []string) {
	if e.auth == nil {
		return bot.BotToken, e.cfg.WSHost, nil
	}
	e.mu.Lock()
	s, ok := e.sessions[bot.KeyID]
	e.mu.Unlock()
	if ok && time.Since(s.fetched) < sessionTTL {
		return s.token, s.wsHost, s.clanIDs
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sess, err := e.auth.Authenticate(ctx, bot.BotUserID, bot.BotToken)
	if err != nil {
		log.Printf("authenticate failed (bot=%s key=%s): %v — using raw token", bot.BotUserID, bot.KeyID, err)
		return bot.BotToken, e.cfg.WSHost, nil
	}
	wsHost = strings.TrimPrefix(strings.TrimPrefix(sess.WSURL, "wss://"), "ws://")
	if wsHost == "" {
		wsHost = e.cfg.WSHost
	}
	// "0" is the DM space (official SDK convention); clan messages need an
	// explicit ClanJoin per joined clan or the socket stays silent.
	clanIDs = []string{"0"}
	if e.clans != nil {
		ids, err := e.clans.ListClanIDs(ctx, sess.APIURL, sess.Token)
		if err != nil {
			log.Printf("list clans failed (bot=%s key=%s): %v — joining DM space only", bot.BotUserID, bot.KeyID, err)
		} else {
			clanIDs = append(clanIDs, ids...)
		}
	}
	cs := cachedSession{token: sess.Token, wsHost: wsHost, apiURL: sess.APIURL, clanIDs: clanIDs, fetched: time.Now()}
	if e.accounts != nil {
		acc, err := e.accounts.GetAccount(ctx, sess.APIURL, sess.Token)
		if err != nil {
			log.Printf("get account failed (bot=%s key=%s): %v — typing will show the bot id", bot.BotUserID, bot.KeyID, err)
		} else {
			cs.username, cs.displayName = acc.Username, acc.DisplayName
		}
	}
	e.mu.Lock()
	e.sessions[bot.KeyID] = cs
	e.mu.Unlock()
	return sess.Token, wsHost, clanIDs
}

// SyncClans lists the clans the bot has joined, with each clan's text
// channels, and joins any clan the live socket has not joined yet.
//
// The clan list is otherwise read once, at dial: a clan the bot is added to
// while its socket is open delivers nothing until a redial — so it is never
// answered in and never enrolled. Calling this periodically closes that gap;
// the snapshot is what the consumer enrolls.
func (e *Engine) SyncClans(ctx context.Context, bot types.BotRef) ([]ClanSnapshot, error) {
	if e.dir == nil || e.auth == nil {
		return nil, fmt.Errorf("sync clans: no clan directory configured")
	}
	e.session(bot) // ensure a cached session exists
	e.mu.Lock()
	s, ok := e.sessions[bot.KeyID]
	e.mu.Unlock()
	if !ok || s.apiURL == "" {
		return nil, fmt.Errorf("sync clans: no session for bot %s", bot.BotUserID)
	}
	clans, err := e.dir.ListClans(ctx, s.apiURL, s.token)
	if err != nil {
		return nil, fmt.Errorf("sync clans: %w", err)
	}

	e.mu.Lock()
	forbiddenAt, forbidden := e.noChannels[bot.KeyID]
	e.mu.Unlock()
	listChannels := !forbidden || time.Since(forbiddenAt) > channelsBackoff

	out := make([]ClanSnapshot, 0, len(clans))
	for _, cl := range clans {
		snap := ClanSnapshot{ID: cl.ID, Name: cl.Name}
		if listChannels {
			chans, err := e.dir.ListChannels(ctx, s.apiURL, s.token, cl.ID)
			var herr *rest.HTTPError
			switch {
			case err == nil:
				snap.Channels, snap.ChannelsKnown = chans, true
			case errors.As(err, &herr) && herr.Status == http.StatusForbidden:
				// Live, bot tokens may not list channels at all. Stop asking —
				// for every clan now and for a while — rather than log a 403
				// per clan per pass; channels are then learned from messages.
				log.Printf("channel listing not permitted (bot=%s) — channels will be learned from messages", bot.BotUserID)
				e.mu.Lock()
				e.noChannels[bot.KeyID] = time.Now()
				e.mu.Unlock()
				listChannels = false
			default:
				log.Printf("list channels failed (bot=%s clan=%s): %v", bot.BotUserID, cl.ID, err)
			}
		}
		out = append(out, snap)
	}

	// Remember every clan so a redial inside the session TTL joins it, and
	// join on the live socket whatever it has not joined yet.
	e.mu.Lock()
	if cur, ok := e.sessions[bot.KeyID]; ok {
		known := make(map[string]bool, len(cur.clanIDs))
		for _, id := range cur.clanIDs {
			known[id] = true
		}
		for _, cl := range clans {
			if !known[cl.ID] {
				cur.clanIDs = append(cur.clanIDs, cl.ID)
			}
		}
		e.sessions[bot.KeyID] = cur
	}
	conn := e.hot[bot.KeyID]
	e.mu.Unlock()
	if conn != nil && !conn.Closed() {
		for _, cl := range clans {
			if conn.HasJoined(cl.ID) {
				continue
			}
			if err := conn.JoinClan(cl.ID); err != nil {
				log.Printf("join clan failed (bot=%s clan=%s): %v", bot.BotUserID, cl.ID, err)
			} else {
				log.Printf("joined new clan (bot=%s clan=%s)", bot.BotUserID, cl.ID)
			}
		}
	}
	return out, nil
}

// dropSession forgets a bot's cached session (e.g. after a failed dial — the
// token may have expired server-side before our TTL).
func (e *Engine) dropSession(keyID string) {
	e.mu.Lock()
	delete(e.sessions, keyID)
	e.mu.Unlock()
}

// Register adds a bot. New bots are treated as just-active so the next
// rebalance opens their socket (budget permitting) — see Manager.Add for the
// cold-start rationale; idle bots then decay to warm/cold polling.
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
		defer func() { _ = recover() }()
		e.onMessage(bot, msg)
	}
}

// ── tier.Actuator ───────────────────────────────────────────────────────────

// OpenHot opens a live socket for a bot (non-blocking enough for the manager;
// on dial failure the bot is demoted back to Warm).
func (e *Engine) OpenHot(bot types.BotRef) {
	e.mu.Lock()
	if existing := e.hot[bot.KeyID]; existing != nil && !existing.Closed() {
		e.mu.Unlock()
		return
	}
	if _, exists := e.opening[bot.KeyID]; exists {
		e.mu.Unlock()
		return
	}
	e.opening[bot.KeyID] = struct{}{}
	e.mu.Unlock()

	go func() {
		token, wsHost, clanIDs := e.session(bot)
		// onClose evicts the dead conn and re-arms the tier manager so the
		// next rebalance re-dials — without it the bot stays "Hot" with a
		// zombie socket and is permanently deaf (silent: read loops don't log).
		onClose := func() {
			// Distinguish UNEXPECTED death (server drop, network blip — the
			// conn is still registered) from an intentional CloseHot
			// (deregister/demote already removed it): only the former re-arms,
			// otherwise a demoted bot Touch-refreshes itself and oscillates.
			e.mu.Lock()
			c := e.hot[bot.KeyID]
			unexpected := c != nil && c.Closed()
			if unexpected {
				delete(e.hot, bot.KeyID)
				e.pw.Remove(c)
			}
			e.mu.Unlock()
			if !unexpected {
				return
			}
			log.Printf("hot socket closed (bot=%s key=%s) — re-dialing", bot.BotUserID, bot.KeyID)
			e.dropSession(bot.KeyID) // session may have been invalidated server-side
			e.mgr.ForceTier(bot.KeyID, types.Warm)
			e.mgr.Touch(bot.KeyID) // fresh activity → immediate re-promotion → re-dial
		}
		conn, err := ws.Dial(wsHost, e.cfg.WSSSL, token, bot.BotUserID, clanIDs,
			func(m types.Message) { e.onHotMessage(bot, m) }, onClose)
		if err != nil {
			// Loud on purpose: a bad token or unreachable WS host otherwise
			// looks identical to a healthy idle bot from the outside.
			log.Printf("hot dial failed (bot=%s key=%s host=%s): %v — demoting to warm",
				bot.BotUserID, bot.KeyID, wsHost, err)
			e.dropSession(bot.KeyID) // may have expired server-side
			e.mu.Lock()
			delete(e.opening, bot.KeyID)
			e.mu.Unlock()
			e.mgr.ForceTier(bot.KeyID, types.Warm)
			return
		}
		log.Printf("hot socket open (bot=%s key=%s clans=%v)", bot.BotUserID, bot.KeyID, clanIDs)

		e.mu.Lock()
		if _, stillWanted := e.opening[bot.KeyID]; !stillWanted {
			e.mu.Unlock()
			_ = conn.Close()
			return
		}
		delete(e.opening, bot.KeyID)
		if existing := e.hot[bot.KeyID]; existing != nil && !existing.Closed() {
			e.mu.Unlock()
			_ = conn.Close()
			return
		}
		e.hot[bot.KeyID] = conn
		e.pw.Add(conn)
		e.mu.Unlock()
	}()
}

// CloseHot tears down a bot's live socket.
func (e *Engine) CloseHot(keyID string) {
	e.mu.Lock()
	conn := e.hot[keyID]
	delete(e.hot, keyID)
	delete(e.opening, keyID)
	e.mu.Unlock()
	if conn != nil {
		e.pw.Remove(conn)
		_ = conn.Close()
	}
}

// Poll runs one REST poll for a warm/cold bot.
func (e *Engine) Poll(bot types.BotRef) {
	token, _, _ := e.session(bot)
	e.poller.Submit(context.Background(), bot, token, nil)
}

// ── outbound (used by the consumer's turn handler) ──────────────────────────

// Send replies to a message. If the bot is hot it goes over the live socket;
// otherwise a transient lean socket is opened to send, then closed.
func (e *Engine) Send(bot types.BotRef, in types.Message, text string, asReply bool) error {
	var ref *ws.Ref
	if asReply && !in.IsDM() {
		username := in.ClanNick
		if username == "" {
			username = in.DisplayName
		}
		if username == "" {
			username = in.Username
		}
		ref = &ws.Ref{
			RefMessageID: in.MessageID, SenderID: in.SenderID,
			SenderUsername: username, SenderAvatar: in.Avatar, Content: in.Content,
		}
	}
	return e.SendTo(bot, in.ChannelID, in.ClanID, in.Mode, in.IsPublic, text, ws.SendOpts{Ref: ref})
}

// SendTo posts to an explicit channel — no inbound message required. Used for
// scheduler-produced outbound deliveries, where the caller supplies the
// channel coordinates (and mode/is_public from the channel-meta cache) plus
// any mention decorations. Hot socket when available, transient otherwise.
func (e *Engine) SendTo(bot types.BotRef, channelID, clanID string, mode int32, isPublic bool, text string, opts ws.SendOpts) error {
	e.mu.Lock()
	conn := e.hot[bot.KeyID]
	e.mu.Unlock()
	if conn != nil && !conn.Closed() {
		return conn.SendTextOpts(channelID, clanID, mode, isPublic, text, opts)
	}
	// Fallback: transient socket (rare — only when hot slots are saturated).
	token, wsHost, clanIDs := e.session(bot)
	tmp, err := ws.Dial(wsHost, e.cfg.WSSSL, token, bot.BotUserID, clanIDs, func(types.Message) {}, nil)
	if err != nil {
		return err
	}
	defer tmp.Close()
	return tmp.SendTextOpts(channelID, clanID, mode, isPublic, text, opts)
}

// SendAck replies like Send and returns the id the server gave the new
// message, so the caller can edit it later (a "working on it" placeholder
// that becomes the answer). Waits up to timeout for the acknowledgement.
func (e *Engine) SendAck(bot types.BotRef, in types.Message, text string, asReply bool, timeout time.Duration) (string, error) {
	var ref *ws.Ref
	if asReply && !in.IsDM() {
		username := in.ClanNick
		if username == "" {
			username = in.DisplayName
		}
		if username == "" {
			username = in.Username
		}
		ref = &ws.Ref{
			RefMessageID: in.MessageID, SenderID: in.SenderID,
			SenderUsername: username, SenderAvatar: in.Avatar, Content: in.Content,
		}
	}
	var id string
	err := e.withConn(bot, func(c *ws.Conn) error {
		var err error
		id, err = c.SendTextAck(in.ChannelID, in.ClanID, in.Mode, in.IsPublic, text, ws.SendOpts{Ref: ref}, timeout)
		return err
	})
	return id, err
}

// SendToAck posts to an explicit channel like SendTo, then waits up to timeout
// for the server's acknowledgement and returns the new message's id. A delivery
// that must not be counted until Mezon has it (outbound streams) uses this:
// ws.ErrNoAck means unconfirmed, ws.ErrRejected means refused.
func (e *Engine) SendToAck(bot types.BotRef, channelID, clanID string, mode int32, isPublic bool, text string, opts ws.SendOpts, timeout time.Duration) (string, error) {
	var id string
	err := e.withConn(bot, func(c *ws.Conn) error {
		var err error
		id, err = c.SendTextAck(channelID, clanID, mode, isPublic, text, opts, timeout)
		return err
	})
	return id, err
}

// UpdateTo replaces the content of a message the bot sent earlier and waits up
// to timeout for the server to confirm. ws.ErrNoAck means unconfirmed.
func (e *Engine) UpdateTo(bot types.BotRef, channelID, clanID, messageID string, mode int32, isPublic bool, text string, timeout time.Duration) error {
	return e.withConn(bot, func(c *ws.Conn) error {
		return c.UpdateText(channelID, clanID, messageID, mode, isPublic, text, timeout)
	})
}

// withConn runs fn on the bot's hot socket, or on a transient one when the bot
// is not hot — the same fallback SendTo uses. A transient socket stays open
// until fn returns, so an acknowledgement it is waiting for can arrive.
func (e *Engine) withConn(bot types.BotRef, fn func(*ws.Conn) error) error {
	e.mu.Lock()
	conn := e.hot[bot.KeyID]
	e.mu.Unlock()
	if conn != nil && !conn.Closed() {
		return fn(conn)
	}
	token, wsHost, clanIDs := e.session(bot)
	tmp, err := ws.Dial(wsHost, e.cfg.WSSSL, token, bot.BotUserID, clanIDs, func(types.Message) {}, nil)
	if err != nil {
		return err
	}
	defer tmp.Close()
	return fn(tmp)
}

// SendTyping emits a typing indicator if the bot is hot (no-op otherwise).
// The indicator carries the bot's name — BotRef's when set, else the account
// names cached with the session (a hot bot always has one: OpenHot resolved it).
func (e *Engine) SendTyping(bot types.BotRef, in types.Message) {
	e.mu.Lock()
	conn := e.hot[bot.KeyID]
	s := e.sessions[bot.KeyID]
	e.mu.Unlock()
	if conn == nil || conn.Closed() {
		return
	}
	username, displayName := bot.BotUsername, bot.BotDisplayName
	if username == "" {
		username = s.username
	}
	if displayName == "" {
		displayName = s.displayName
	}
	_ = conn.SendTyping(in.ChannelID, in.ClanID, bot.BotUserID, username, displayName, in.Mode, in.IsPublic)
}

// SendReaction adds (or removes) an emoji reaction on a message. emoji is a
// Unicode glyph or a custom shortcode (e.g. "pepe_joy"). emojiID is the numeric
// ID for custom clan emojis (empty for Unicode). action=true adds, false removes.
func (e *Engine) SendReaction(bot types.BotRef, in types.Message, emojiID, emoji string, action bool) error {
	e.mu.Lock()
	conn := e.hot[bot.KeyID]
	e.mu.Unlock()
	if conn != nil && !conn.Closed() {
		return conn.SendReaction(in.ClanID, in.ChannelID, in.MessageID, emojiID, emoji, action, in.Mode, in.IsPublic)
	}
	// Fallback: transient socket.
	token, wsHost, clanIDs := e.session(bot)
	tmp, err := ws.Dial(wsHost, e.cfg.WSSSL, token, bot.BotUserID, clanIDs, func(types.Message) {}, nil)
	if err != nil {
		return err
	}
	defer tmp.Close()
	return tmp.SendReaction(in.ClanID, in.ChannelID, in.MessageID, emojiID, emoji, action, in.Mode, in.IsPublic)
}

// SendSticker sends a sticker as a message attachment. stickerShortcode is the
// sticker's shortcode (e.g. "froge_no"). stickerURL is the CDN image URL.
func (e *Engine) SendSticker(bot types.BotRef, in types.Message, stickerShortcode, stickerURL string) error {
	e.mu.Lock()
	conn := e.hot[bot.KeyID]
	e.mu.Unlock()
	if conn != nil && !conn.Closed() {
		return conn.SendSticker(in.ChannelID, in.ClanID, in.Mode, in.IsPublic, stickerShortcode, stickerURL)
	}
	// Fallback: transient socket.
	token, wsHost, clanIDs := e.session(bot)
	tmp, err := ws.Dial(wsHost, e.cfg.WSSSL, token, bot.BotUserID, clanIDs, func(types.Message) {}, nil)
	if err != nil {
		return err
	}
	defer tmp.Close()
	return tmp.SendSticker(in.ChannelID, in.ClanID, in.Mode, in.IsPublic, stickerShortcode, stickerURL)
}
