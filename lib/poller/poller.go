// Package poller drives the warm/cold tiers: it polls each bot's channels for
// new messages via REST (cursor-based), dedups, advances cursors, and emits new
// messages to the engine. All REST calls pass through a single global token
// bucket so total Mezon API load is capped regardless of bot count.
package poller

import (
	"context"
	"time"

	"golang.org/x/time/rate"

	"github.com/mezon/mezon-go-sdk-turbo/lib/state"
	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

// Lister is the REST capability the poller needs (rest.Client satisfies it).
type Lister interface {
	ListSince(ctx context.Context, token, channelID, clanID, cursor string, limit int32) ([]types.Message, string, error)
}

// Poller polls bots on demand, bounded by a worker pool and a global rate limit.
type Poller struct {
	lister    Lister
	store     state.Store
	onMessage func(types.BotRef, types.Message)
	limiter   *rate.Limiter
	sem       chan struct{}
	pageLimit int32
}

// New builds a poller. rps caps total REST calls/sec; workers bounds concurrent
// poll jobs; pageLimit is the per-channel message page size.
func New(lister Lister, store state.Store, onMessage func(types.BotRef, types.Message), rps, workers int, pageLimit int32) *Poller {
	if workers <= 0 {
		workers = 8
	}
	if rps <= 0 {
		rps = 50
	}
	if pageLimit <= 0 {
		pageLimit = 20
	}
	return &Poller{
		lister:    lister,
		store:     store,
		onMessage: onMessage,
		limiter:   rate.NewLimiter(rate.Limit(rps), rps),
		sem:       make(chan struct{}, workers),
		pageLimit: pageLimit,
	}
}

// PollOnce polls every channel of one bot once, emitting new messages. Returns
// the number of new (deduped) messages found. Token is the bot's REST access
// token. Each REST call waits on the global rate limiter.
func (p *Poller) PollOnce(ctx context.Context, bot types.BotRef, token string) (int, error) {
	channels, err := p.store.Channels(ctx, bot.KeyID)
	if err != nil {
		return 0, err
	}
	found := 0
	for _, ch := range channels {
		if err := p.limiter.Wait(ctx); err != nil {
			return found, err
		}
		cursor, err := p.store.Cursor(ctx, bot.KeyID, ch)
		if err != nil {
			return found, err
		}
		msgs, newCursor, err := p.lister.ListSince(ctx, token, ch, "", cursor, p.pageLimit)
		if err != nil {
			continue // transient; try again next poll
		}
		for _, m := range msgs {
			if m.SenderID == bot.BotUserID {
				continue
			}
			seen, err := p.store.Seen(ctx, bot.KeyID, m.MessageID)
			if err != nil {
				return found, err
			}
			if seen {
				continue
			}
			found++
			if p.onMessage != nil {
				p.emit(bot, m)
			}
		}
		if newCursor != "" && newCursor != cursor {
			if err := p.store.SetCursor(ctx, bot.KeyID, ch, newCursor); err != nil {
				return found, err
			}
		}
	}
	if found > 0 {
		_ = p.store.BumpActivity(ctx, bot.KeyID, time.Now())
	}
	return found, nil
}

func (p *Poller) emit(bot types.BotRef, msg types.Message) {
	defer func() { _ = recover() }()
	if p.onMessage != nil {
		p.onMessage(bot, msg)
	}
}

// Submit runs PollOnce on a bounded worker; done (may be nil) is called with the
// new-message count when the poll finishes (the engine uses it to promote).
func (p *Poller) Submit(ctx context.Context, bot types.BotRef, token string, done func(int)) {
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	go func() {
		defer func() { <-p.sem }()
		n, _ := p.PollOnce(ctx, bot, token)
		if done != nil {
			done(n)
		}
	}()
}
