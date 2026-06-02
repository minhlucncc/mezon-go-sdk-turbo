package turbo_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

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
