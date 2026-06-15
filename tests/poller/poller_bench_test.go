package poller_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/mezon/mezon-go-sdk-turbo/lib/poller"
	"github.com/mezon/mezon-go-sdk-turbo/lib/state"
	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

func newStoreForBench(b *testing.B) state.Store {
	b.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(mr.Close)
	return state.NewRedisStore(redis.NewClient(&redis.Options{Addr: mr.Addr()}), time.Hour, 4096)
}

func BenchmarkPollOnceDedupedPage(b *testing.B) {
	ctx := context.Background()
	st := newStoreForBench(b)
	bot := types.BotRef{KeyID: "k1", BotUserID: "bot"}
	if err := st.AddChannel(ctx, "k1", "c1"); err != nil {
		b.Fatal(err)
	}
	msgs := make([]types.Message, 20)
	for i := range msgs {
		msgs[i] = types.Message{MessageID: "m" + strconv.Itoa(i), SenderID: "u1", ChannelID: "c1"}
	}
	lister := &fakeLister{byChan: map[string][]types.Message{"c1": msgs}}
	p := poller.New(lister, st, nil, 1_000_000, 8, 20)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := st.SetCursor(ctx, "k1", "c1", ""); err != nil {
			b.Fatal(err)
		}
		if _, err := p.PollOnce(ctx, bot, "tok"); err != nil {
			b.Fatal(err)
		}
	}
}
