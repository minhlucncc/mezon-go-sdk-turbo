package state_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/mezon/mezon-go-sdk-turbo/lib/state"
	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

func newStore(t *testing.T, dedupCap int64) *state.RedisStore {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return state.NewRedisStore(rdb, time.Hour, dedupCap)
}

func TestSeenDedups(t *testing.T) {
	s := newStore(t, 2048)
	ctx := context.Background()
	if seen, _ := s.Seen(ctx, "k1", "m1"); seen {
		t.Fatal("first sight should be unseen")
	}
	if seen, _ := s.Seen(ctx, "k1", "m1"); !seen {
		t.Fatal("second sight should be seen")
	}
	if seen, _ := s.Seen(ctx, "k2", "m1"); seen {
		t.Fatal("dedup must be per-bot")
	}
}

func TestSeenTrimsToCap(t *testing.T) {
	s := newStore(t, 3)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c", "d"} { // evicts "a"
		s.Seen(ctx, "k1", id)
	}
	if seen, _ := s.Seen(ctx, "k1", "a"); seen {
		t.Fatal("oldest id should have been trimmed")
	}
}

func TestCursorRoundTrip(t *testing.T) {
	s := newStore(t, 16)
	ctx := context.Background()
	if c, _ := s.Cursor(ctx, "k1", "chan1"); c != "" {
		t.Fatalf("empty cursor expected, got %q", c)
	}
	if err := s.SetCursor(ctx, "k1", "chan1", "m9"); err != nil {
		t.Fatal(err)
	}
	if c, _ := s.Cursor(ctx, "k1", "chan1"); c != "m9" {
		t.Fatalf("cursor = %q, want m9", c)
	}
}

func TestChannelsAndActivityAndTier(t *testing.T) {
	s := newStore(t, 16)
	ctx := context.Background()

	s.AddChannel(ctx, "k1", "c1")
	s.AddChannel(ctx, "k1", "c2")
	chans, _ := s.Channels(ctx, "k1")
	if len(chans) != 2 {
		t.Fatalf("expected 2 channels, got %v", chans)
	}

	now := time.Unix(1_700_000_000, 0)
	s.BumpActivity(ctx, "k1", now)
	s.BumpActivity(ctx, "k1", now)
	last, count, _ := s.Activity(ctx, "k1")
	if last != now.Unix() || count != 2 {
		t.Fatalf("activity last=%d count=%d", last, count)
	}

	if tr, _ := s.Tier(ctx, "k1"); tr != types.Cold {
		t.Fatalf("default tier should be cold, got %s", tr)
	}
	s.SetTier(ctx, "k1", types.Hot)
	if tr, _ := s.Tier(ctx, "k1"); tr != types.Hot {
		t.Fatalf("tier = %s, want hot", tr)
	}
}
