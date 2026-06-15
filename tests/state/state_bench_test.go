package state_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/mezon/mezon-go-sdk-turbo/lib/state"
	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

func benchStore(b *testing.B, dedupCap int64) *state.RedisStore {
	b.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return state.NewRedisStore(rdb, time.Hour, dedupCap)
}

func BenchmarkRedisSeenNew(b *testing.B) {
	ctx := context.Background()
	s := benchStore(b, 4096)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := s.Seen(ctx, "k1", "m"+strconv.Itoa(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisSeenDuplicate(b *testing.B) {
	ctx := context.Background()
	s := benchStore(b, 4096)
	if _, err := s.Seen(ctx, "k1", "m1"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := s.Seen(ctx, "k1", "m1"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisStateRoundTrip(b *testing.B) {
	ctx := context.Background()
	s := benchStore(b, 4096)
	now := time.Unix(1_700_000_000, 0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := s.AddChannel(ctx, "k1", "c1"); err != nil {
			b.Fatal(err)
		}
		if err := s.SetCursor(ctx, "k1", "c1", "m1"); err != nil {
			b.Fatal(err)
		}
		if err := s.BumpActivity(ctx, "k1", now); err != nil {
			b.Fatal(err)
		}
		if err := s.SetTier(ctx, "k1", types.Hot); err != nil {
			b.Fatal(err)
		}
	}
}
