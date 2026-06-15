package poller_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/mezon/mezon-go-sdk-turbo/lib/poller"
	"github.com/mezon/mezon-go-sdk-turbo/lib/state"
	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

// fakeLister returns a fixed set of messages once, then advances the cursor so a
// second poll returns nothing (mimics catch-up semantics).
type fakeLister struct {
	mu     sync.Mutex
	byChan map[string][]types.Message
	calls  int
}

func (f *fakeLister) ListSince(_ context.Context, _, channelID, _, cursor string, _ int32) ([]types.Message, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if cursor != "" {
		return nil, cursor, nil // already caught up
	}
	msgs := f.byChan[channelID]
	newCursor := ""
	if n := len(msgs); n > 0 {
		newCursor = msgs[n-1].MessageID
	}
	return msgs, newCursor, nil
}

func newStore(t *testing.T) state.Store {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	return state.NewRedisStore(redis.NewClient(&redis.Options{Addr: mr.Addr()}), time.Hour, 2048)
}

func TestPollOnceEmitsNewThenCatchesUp(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	bot := types.BotRef{KeyID: "k1", BotUserID: "bot", TenantID: "t1"}
	st.AddChannel(ctx, "k1", "c1")

	lister := &fakeLister{byChan: map[string][]types.Message{
		"c1": {
			{MessageID: "m1", SenderID: "u1", ChannelID: "c1", Content: `{"t":"hi"}`},
			{MessageID: "m2", SenderID: "bot", ChannelID: "c1"}, // own message, skipped
			{MessageID: "m3", SenderID: "u2", ChannelID: "c1"},
		},
	}}

	var got []string
	p := poller.New(lister, st, func(_ types.BotRef, m types.Message) { got = append(got, m.MessageID) }, 1000, 4, 20)

	n, err := p.PollOnce(ctx, bot, "tok")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || len(got) != 2 || got[0] != "m1" || got[1] != "m3" {
		t.Fatalf("expected m1,m3 (own message skipped); n=%d got=%v", n, got)
	}

	n2, _ := p.PollOnce(ctx, bot, "tok")
	if n2 != 0 {
		t.Fatalf("second poll should be caught up, got %d", n2)
	}
}

func TestPollOnceDedupsAcrossCalls(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	bot := types.BotRef{KeyID: "k1", BotUserID: "bot"}
	st.AddChannel(ctx, "k1", "c1")
	lister := &fakeLister{byChan: map[string][]types.Message{
		"c1": {{MessageID: "m1", SenderID: "u1", ChannelID: "c1"}},
	}}
	count := 0
	p := poller.New(lister, st, func(types.BotRef, types.Message) { count++ }, 1000, 4, 20)
	p.PollOnce(ctx, bot, "tok")
	st.SetCursor(ctx, "k1", "c1", "") // force re-fetch
	p.PollOnce(ctx, bot, "tok")
	if count != 1 {
		t.Fatalf("dedup failed: emitted %d times", count)
	}
}

func TestPollOnceContainsCallbackPanic(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	bot := types.BotRef{KeyID: "k1", BotUserID: "bot"}
	st.AddChannel(ctx, "k1", "c1")
	lister := &fakeLister{byChan: map[string][]types.Message{
		"c1": {{MessageID: "m1", SenderID: "u1", ChannelID: "c1"}},
	}}
	p := poller.New(lister, st, func(types.BotRef, types.Message) {
		panic("consumer bug")
	}, 1000, 4, 20)

	n, err := p.PollOnce(ctx, bot, "tok")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("message should still be counted despite callback panic, got %d", n)
	}
}

type failingStore struct {
	state.Store
	failCursor    bool
	failSeen      bool
	failSetCursor bool
}

func (s failingStore) Cursor(ctx context.Context, keyID, channelID string) (string, error) {
	if s.failCursor {
		return "", errors.New("cursor unavailable")
	}
	return s.Store.Cursor(ctx, keyID, channelID)
}

func (s failingStore) Seen(ctx context.Context, keyID, msgID string) (bool, error) {
	if s.failSeen {
		return false, errors.New("dedup unavailable")
	}
	return s.Store.Seen(ctx, keyID, msgID)
}

func (s failingStore) SetCursor(ctx context.Context, keyID, channelID, cursor string) error {
	if s.failSetCursor {
		return errors.New("cursor write failed")
	}
	return s.Store.SetCursor(ctx, keyID, channelID, cursor)
}

func TestPollOnceFailsClosedWhenCacheUnavailable(t *testing.T) {
	ctx := context.Background()

	for name, st := range map[string]func(state.Store) state.Store{
		"cursor":     func(base state.Store) state.Store { return failingStore{Store: base, failCursor: true} },
		"dedup":      func(base state.Store) state.Store { return failingStore{Store: base, failSeen: true} },
		"set_cursor": func(base state.Store) state.Store { return failingStore{Store: base, failSetCursor: true} },
	} {
		t.Run(name, func(t *testing.T) {
			base := newStore(t)
			bot := types.BotRef{KeyID: "k1", BotUserID: "bot"}
			if err := base.AddChannel(ctx, "k1", "c1"); err != nil {
				t.Fatal(err)
			}
			lister := &fakeLister{byChan: map[string][]types.Message{
				"c1": {{MessageID: "m1", SenderID: "u1", ChannelID: "c1"}},
			}}
			delivered := 0
			p := poller.New(lister, st(base), func(types.BotRef, types.Message) {
				delivered++
			}, 1000, 4, 20)
			if _, err := p.PollOnce(ctx, bot, "tok"); err == nil {
				t.Fatal("expected cache error")
			}
			if name != "set_cursor" && delivered != 0 {
				t.Fatalf("must not deliver without cache safety, delivered=%d", delivered)
			}
		})
	}
}
