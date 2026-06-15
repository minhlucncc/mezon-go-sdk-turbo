package turbo_test

import (
	"context"
	"encoding/base64"
	"sync/atomic"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/mezon/mezon-go-sdk-turbo/lib/rest"
	"github.com/mezon/mezon-go-sdk-turbo/lib/tier"
	turbo "github.com/mezon/mezon-go-sdk-turbo/lib/turbo"
	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

// TestFullConnectionLifecycle is the integration test that would have caught
// every bug in the 2026-06-06 "connected but silent" outage. A fake Mezon
// server enforces the REAL handshake order and wire format:
//
//  1. the engine must EXCHANGE the API key for a session
//     (POST /v2/apps/authenticate/token, Basic appid:key) — not dial raw;
//  2. it must dial the SESSION-provided ws host (ws_url), with the session
//     token — gw (the auth host) rejects sockets;
//  3. it must fetch the clan list (Bearer session) and send ClanJoin frames
//     with INT64 clan ids — string ids get "can not unmarshal." and the
//     socket stays silent forever;
//  4. it must do all of this for a freshly REGISTERED bot with zero traffic —
//     the cold-start case (no Touch, no activity);
//  5. inbound frames use the LIVE ChannelMessage schema (int64 ids, shifted
//     field numbers) and must reach onMessage decoded;
//  6. the reply must be a live-schema ChannelMessageSend (int64 ids).
type fakeMezon struct {
	t        *testing.T
	apiKey   string
	appID    string
	session  string
	clanID   string
	upgrader websocket.Upgrader

	mu        sync.Mutex
	authBasic string
	authAppID string
	wsToken   string
	joins     []uint64
	replies   [][]byte
	replyCh   chan []byte
	conn      *websocket.Conn
}

func (f *fakeMezon) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/apps/authenticate/token", f.handleAuth)
	mux.HandleFunc("/mezon.api.Mezon/ListClanDescs", f.handleClans)
	mux.HandleFunc("/ws", f.handleWS)
	return mux
}

func (f *fakeMezon) handleAuth(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Account struct {
			AppID string `json:"appid"`
			Token string `json:"token"`
		} `json:"account"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.authBasic = r.Header.Get("Authorization")
	f.authAppID = body.Account.AppID
	f.mu.Unlock()
	if body.Account.Token != f.apiKey {
		http.Error(w, `{"code":3,"message":"Invalid App Id."}`, http.StatusBadRequest)
		return
	}
	host := r.Host // session ROUTES the socket — same fake server here
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"token":  f.session,
		"user_id": f.appID,
		"api_url": "http://" + host,
		"ws_url":  host,
	})
}

func (f *fakeMezon) handleClans(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+f.session {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	id, _ := protowireParseUint(f.clanID)
	// live ClanDesc: field 1 = creator_id (a DIFFERENT user id — joining it is
	// the bug this guards), field 5 = clan_id.
	clan := protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 999000111)
	clan = protowire.AppendVarint(protowire.AppendTag(clan, 5, protowire.VarintType), id)
	resp := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), clan)
	w.Header().Set("Content-Type", "application/proto")
	_, _ = w.Write(resp)
}

func (f *fakeMezon) handleWS(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.wsToken = r.URL.Query().Get("token")
	f.mu.Unlock()
	if r.URL.Query().Get("token") != f.session {
		http.Error(w, "bad handshake", http.StatusUnauthorized) // raw key → rejected, like live
		return
	}
	conn, err := f.upgrader.Upgrade(w, r, nil)
	if err != nil {
		f.t.Errorf("upgrade: %v", err)
		return
	}
	f.mu.Lock()
	f.conn = conn
	f.mu.Unlock()
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if id, ok := parseClanJoin(data); ok {
				f.mu.Lock()
				f.joins = append(f.joins, id)
				f.mu.Unlock()
				continue
			}
			if isSendEnvelope(data) {
				f.mu.Lock()
				f.replies = append(f.replies, data)
				f.mu.Unlock()
				f.replyCh <- data
			}
		}
	}()
}

// push delivers a live-schema ChannelMessage frame to the connected socket.
func (f *fakeMezon) push(frameB64 string) error {
	frame, err := base64.StdEncoding.DecodeString(frameB64)
	if err != nil {
		return err
	}
	f.mu.Lock()
	conn := f.conn
	f.mu.Unlock()
	return conn.WriteMessage(websocket.BinaryMessage, frame)
}

func parseClanJoin(b []byte) (uint64, bool) {
	num, typ, n := protowire.ConsumeTag(b)
	if n < 0 || num != 3 || typ != protowire.BytesType { // Envelope.clan_join
		return 0, false
	}
	inner, n2 := protowire.ConsumeBytes(b[n:])
	if n2 < 0 {
		return 0, false
	}
	if len(inner) == 0 {
		return 0, true // clan "0" (DM space) — zero omitted in proto3
	}
	fn, ft, fn2 := protowire.ConsumeTag(inner)
	if fn2 < 0 || fn != 1 || ft != protowire.VarintType { // int64 clan_id — the live contract
		return 0, false
	}
	v, _ := protowire.ConsumeVarint(inner[fn2:])
	return v, true
}

func isSendEnvelope(b []byte) bool {
	num, typ, n := protowire.ConsumeTag(b)
	return n > 0 && num == 8 && typ == protowire.BytesType // Envelope.channel_message_send
}

func protowireParseUint(s string) (uint64, error) {
	var v uint64
	for _, r := range s {
		v = v*10 + uint64(r-'0')
	}
	return v, nil
}

func TestFullConnectionLifecycle(t *testing.T) {
	fake := &fakeMezon{
		t:       t,
		apiKey:  "raw-api-key",
		appID:   "2062754877070643200",
		session: "session-jwt-token",
		clanID:  "1780431535405535232",
		replyCh: make(chan []byte, 4),
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	type delivery struct {
		bot types.BotRef
		msg types.Message
	}
	delivered := make(chan delivery, 4)

	cfg := turbo.Config{
		WSHost:      "wrong-host.invalid", // must be OVERRIDDEN by the session ws_url
		WSSSL:       false,
		Tier:        tier.Config{MaxHot: 5, Tick: 20 * time.Millisecond},
		PollRPS:     100,
		PollWorkers: 2,
		StateTTL:    time.Hour,
		DedupCap:    128,
	}
	engine := turbo.New(cfg, rdb, rest.New(srv.URL), func(b types.BotRef, m types.Message) {
		delivered <- delivery{b, m}
	})

	ctx, cancel := testContext(t)
	defer cancel()
	go engine.Run(ctx)

	// COLD-START: register with zero traffic, never Touch — the engine must
	// still authenticate + dial + join on its own.
	bot := types.BotRef{KeyID: "k1", BotUserID: fake.appID, BotToken: fake.apiKey, TenantID: "t1"}
	engine.Register(bot)

	// The socket must come up with both joins (DM space 0 + the real clan).
	waitFor(t, 5*time.Second, "clan joins", func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return len(fake.joins) >= 2
	})
	fake.mu.Lock()
	if fake.authAppID != fake.appID {
		t.Fatalf("authenticate must carry the appid, got %q", fake.authAppID)
	}
	if fake.wsToken != fake.session {
		t.Fatalf("socket must dial with the SESSION token, got %q", fake.wsToken)
	}
	joinedReal := false
	for _, j := range fake.joins {
		if j == 1780431535405535232 {
			joinedReal = true
		}
	}
	fake.mu.Unlock()
	if !joinedReal {
		t.Fatalf("real clan never joined (int64): %v", fake.joins)
	}

	// Push a live-schema inbound frame (from the cross-SDK goldens — encoded
	// by the python protobuf, the live source of truth).
	golden := loadGoldenFrame(t, "clan channel message with mention")
	if err := fake.push(golden); err != nil {
		t.Fatalf("push: %v", err)
	}
	var got delivery
	select {
	case got = <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("inbound message never reached onMessage — the 'connected but silent' regression")
	}
	if got.msg.ChannelID != "1780431535405535233" || got.msg.SenderID != "1784059393956909056" ||
		got.msg.Content != `{"t":"@Local hello"}` {
		t.Fatalf("decoded message wrong: %+v", got.msg)
	}

	// Reply over the live socket — the server must receive a live-schema send.
	if err := engine.Send(got.bot, got.msg, "hello back", true); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case <-fake.replyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("reply never reached the server")
	}
}

func loadGoldenFrame(t *testing.T, name string) string {
	t.Helper()
	raw, err := readWireGolden()
	if err != nil {
		t.Fatalf("wire goldens: %v", err)
	}
	var g struct {
		Inbound []struct {
			Name     string `json:"name"`
			FrameB64 string `json:"frame_b64"`
		} `json:"inbound"`
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	for _, c := range g.Inbound {
		if c.Name == name {
			return c.FrameB64
		}
	}
	t.Fatalf("golden frame %q not found", name)
	return ""
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.After(d)
	for !cond() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for " + what)
		case <-time.After(10 * time.Millisecond):
		}
	}
}


func testContext(t *testing.T) (ctx context.Context, cancel context.CancelFunc) {
	t.Helper()
	return context.WithCancel(context.Background())
}

func readWireGolden() ([]byte, error) {
	return os.ReadFile("../ws/testdata/wire_golden.json")
}

// TestSocketReconnectsAfterServerClose locks the "works once then dies"
// regression (2026-06-06): when a hot socket's read loop ends (server close,
// network blip), the engine must EVICT the dead conn and re-dial — a zombie
// left in the hot map makes the bot permanently deaf while looking Hot.
func TestSocketReconnectsAfterServerClose(t *testing.T) {
	fake := &fakeMezon{
		t:       t,
		apiKey:  "raw-api-key",
		appID:   "600",
		session: "session-jwt-token",
		clanID:  "777",
		replyCh: make(chan []byte, 4),
	}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	delivered := make(chan types.Message, 8)
	cfg := turbo.Config{
		WSSSL:       false,
		Tier:        tier.Config{MaxHot: 5, Tick: 20 * time.Millisecond},
		PollRPS:     100,
		PollWorkers: 2,
		StateTTL:    time.Hour,
		DedupCap:    128,
	}
	engine := turbo.New(cfg, rdb, rest.New(srv.URL), func(_ types.BotRef, m types.Message) {
		delivered <- m
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go engine.Run(ctx)

	engine.Register(types.BotRef{KeyID: "k1", BotUserID: "600", BotToken: "raw-api-key", TenantID: "t1"})
	waitFor(t, 5*time.Second, "first connect", func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.conn != nil
	})

	// Server drops the connection (deploy, LB restart, idle reap…).
	fake.mu.Lock()
	first := fake.conn
	fake.conn = nil
	fake.mu.Unlock()
	_ = first.Close()

	// The engine must come back: a NEW socket, joins re-sent.
	waitFor(t, 10*time.Second, "re-dial after close", func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.conn != nil
	})

	// And the re-dialed socket must DELIVER — the actual regression.
	st := Step{Channel: "11", Clan: "777", Sender: "900", MessageID: "5002",
		Username: "alice", Text: "after reconnect", Mode: 2, IsPublic: true}
	frame := buildTestChannelMessage(st)
	fake.mu.Lock()
	conn := fake.conn
	fake.mu.Unlock()
	if err := conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		t.Fatalf("push after reconnect: %v", err)
	}
	select {
	case m := <-delivered:
		if m.Content != `{"t":"after reconnect"}` {
			t.Fatalf("wrong delivery: %+v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message after reconnect never delivered — zombie hot socket")
	}
}

// Step + buildTestChannelMessage: minimal live-schema frame builder for tests.
type Step struct {
	Channel, Clan, Sender, MessageID, Username, Text string
	Mode                                             int
	IsPublic                                         bool
}

func buildTestChannelMessage(st Step) []byte {
	vint := func(b []byte, num protowire.Number, v uint64) []byte {
		if v == 0 {
			return b
		}
		return protowire.AppendVarint(protowire.AppendTag(b, num, protowire.VarintType), v)
	}
	str := func(b []byte, num protowire.Number, v string) []byte {
		if v == "" {
			return b
		}
		return protowire.AppendString(protowire.AppendTag(b, num, protowire.BytesType), v)
	}
	parse := func(s string) uint64 {
		var v uint64
		for _, r := range s {
			v = v*10 + uint64(r-'0')
		}
		return v
	}
	var cm []byte
	cm = vint(cm, 1, parse(st.Clan))
	cm = vint(cm, 2, parse(st.Channel))
	cm = vint(cm, 3, parse(st.MessageID))
	cm = vint(cm, 5, parse(st.Sender))
	cm = str(cm, 6, st.Username)
	cm = str(cm, 8, `{"t":"`+st.Text+`"}`)
	cm = vint(cm, 22, uint64(st.Mode))
	if st.IsPublic {
		cm = vint(cm, 24, 1)
	}
	return protowire.AppendBytes(protowire.AppendTag(nil, 6, protowire.BytesType), cm)
}

// Deregister (bot removed / config rotation) closes the socket INTENTIONALLY —
// onClose must not fight it by re-arming the tier manager, or removed bots
// flap back open (hot/close oscillation).
func TestDeregisterDoesNotRedial(t *testing.T) {
	fake := &fakeMezon{
		t:       t,
		apiKey:  "raw-api-key",
		appID:   "600",
		session: "session-jwt-token",
		clanID:  "777",
		replyCh: make(chan []byte, 4),
	}
	var dials atomic.Int64
	inner := fake.handler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" {
			dials.Add(1)
		}
		inner.ServeHTTP(w, r)
	}))
	defer srv.Close()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	cfg := turbo.Config{
		WSSSL:       false,
		Tier:        tier.Config{MaxHot: 5, Tick: 20 * time.Millisecond},
		PollRPS:     100,
		PollWorkers: 2,
		StateTTL:    time.Hour,
		DedupCap:    128,
	}
	engine := turbo.New(cfg, rdb, rest.New(srv.URL), func(types.BotRef, types.Message) {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go engine.Run(ctx)

	engine.Register(types.BotRef{KeyID: "k1", BotUserID: "600", BotToken: "raw-api-key", TenantID: "t1"})
	waitFor(t, 5*time.Second, "connect", func() bool { return dials.Load() >= 1 })

	engine.Deregister("k1")
	before := dials.Load()
	time.Sleep(500 * time.Millisecond) // several manager ticks
	if after := dials.Load(); after != before {
		t.Fatalf("deregistered bot re-dialed: %d → %d", before, after)
	}
}
