package turbo_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/mezon/mezon-go-sdk-turbo/lib/rest"
	"github.com/mezon/mezon-go-sdk-turbo/lib/tier"
	turbo "github.com/mezon/mezon-go-sdk-turbo/lib/turbo"
	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

// onceLister returns one message on the first call (empty cursor), nothing after
// — so a second poll is "caught up".
type onceLister struct{ msg types.Message }

func (l *onceLister) ListSince(_ context.Context, _, channelID, _, cursor string, _ int32) ([]types.Message, string, error) {
	if cursor != "" || channelID != l.msg.ChannelID {
		return nil, cursor, nil
	}
	return []types.Message{l.msg}, l.msg.MessageID, nil
}

func newEngine(t *testing.T, lister *onceLister, onMessage func(types.BotRef, types.Message)) *turbo.Engine {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	now := time.Unix(1000, 0)
	cfg := turbo.Config{
		// MaxHot:0 so the activity bump never tries a real ws.Dial in the test.
		Tier:        tier.Config{MaxHot: 0, ColdPoll: time.Hour, Now: func() time.Time { return now }},
		PollRPS:     1000,
		PollWorkers: 2,
		StateTTL:    time.Hour,
		DedupCap:    2048,
	}
	return turbo.New(cfg, rdb, lister, onMessage)
}

func TestEnginePollDeliversAndDedups(t *testing.T) {
	delivered := make(chan types.Message, 8)
	lister := &onceLister{msg: types.Message{MessageID: "m1", SenderID: "u1", ChannelID: "c1", Content: `{"t":"hi"}`}}
	e := newEngine(t, lister, func(_ types.BotRef, m types.Message) { delivered <- m })

	bot := types.BotRef{KeyID: "k1", BotUserID: "bot", TenantID: "t1"}
	e.Register(bot)
	if err := e.AddChannel(bot.KeyID, "c1"); err != nil { // seed the channel to poll
		t.Fatal(err)
	}

	e.Poll(bot) // public poll (warm/cold path) — async
	select {
	case m := <-delivered:
		if m.MessageID != "m1" {
			t.Fatalf("delivered wrong message: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected a delivered message from poll")
	}

	// Cursor advanced -> a second poll delivers nothing (no duplicate).
	e.Poll(bot)
	select {
	case m := <-delivered:
		t.Fatalf("second poll should be caught up; got duplicate %+v", m)
	case <-time.After(300 * time.Millisecond):
		// expected: no further delivery
	}
}

func TestEngineContainsConsumerPanic(t *testing.T) {
	lister := &onceLister{msg: types.Message{MessageID: "m1", SenderID: "u1", ChannelID: "c1", Content: `{"t":"hi"}`}}
	e := newEngine(t, lister, func(types.BotRef, types.Message) {
		panic("turn handler bug")
	})

	bot := types.BotRef{KeyID: "k1", BotUserID: "bot", TenantID: "t1"}
	e.Register(bot)
	if err := e.AddChannel(bot.KeyID, "c1"); err != nil {
		t.Fatal(err)
	}

	e.Poll(bot)
	time.Sleep(300 * time.Millisecond)
}

// authLister records which token each poll used and serves Authenticate —
// polls (and dials) must use the exchanged SESSION token, never the raw API
// key (Mezon rejects raw keys with "bad handshake"/401).
type authLister struct {
	mu         sync.Mutex
	pollTokens []string
	authCalls  int
}

func (l *authLister) ListSince(_ context.Context, token, _, _, cursor string, _ int32) ([]types.Message, string, error) {
	l.mu.Lock()
	l.pollTokens = append(l.pollTokens, token)
	l.mu.Unlock()
	return nil, cursor, nil
}

func (l *authLister) Authenticate(_ context.Context, _, apiKey string) (rest.Session, error) {
	l.mu.Lock()
	l.authCalls++
	l.mu.Unlock()
	return rest.Session{Token: "session-for-" + apiKey, UserID: "123"}, nil
}

func TestPollUsesSessionTokenAndCachesIt(t *testing.T) {
	lister := &authLister{}
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	now := time.Unix(1000, 0)
	cfg := turbo.Config{
		Tier:        tier.Config{MaxHot: 0, ColdPoll: time.Hour, Now: func() time.Time { return now }},
		PollRPS:     1000,
		PollWorkers: 2,
		StateTTL:    time.Hour,
		DedupCap:    2048,
	}
	e := turbo.New(cfg, rdb, lister, func(types.BotRef, types.Message) {})

	bot := types.BotRef{KeyID: "k1", BotUserID: "123", BotToken: "raw-api-key"}
	e.Register(bot)
	if err := e.AddChannel("k1", "chan-1"); err != nil {
		t.Fatal(err)
	}

	e.Poll(bot)
	e.Poll(bot)
	deadline := time.After(2 * time.Second)
	for {
		lister.mu.Lock()
		n := len(lister.pollTokens)
		lister.mu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("polls never ran")
		case <-time.After(5 * time.Millisecond):
		}
	}

	lister.mu.Lock()
	defer lister.mu.Unlock()
	for _, tok := range lister.pollTokens {
		if tok != "session-for-raw-api-key" {
			t.Fatalf("poll used %q, want the exchanged session token", tok)
		}
	}
	if lister.authCalls != 1 {
		t.Fatalf("session must be cached across polls, auth calls = %d", lister.authCalls)
	}
}
