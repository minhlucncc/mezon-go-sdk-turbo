// Package state offloads per-bot connection state to Redis so warm/cold bots
// hold ~0 Go heap. It carries the message-dedup set, per-channel read cursors,
// the channel list to poll, activity counters, and the bot's tier.
package state

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

// Store is the per-bot connection-state backend.
type Store interface {
	Seen(ctx context.Context, keyID, msgID string) (bool, error)
	Channels(ctx context.Context, keyID string) ([]string, error)
	AddChannel(ctx context.Context, keyID, channelID string) error
	Cursor(ctx context.Context, keyID, channelID string) (string, error)
	SetCursor(ctx context.Context, keyID, channelID, cursor string) error
	BumpActivity(ctx context.Context, keyID string, at time.Time) error
	Activity(ctx context.Context, keyID string) (lastUnix, count int64, err error)
	Tier(ctx context.Context, keyID string) (types.Tier, error)
	SetTier(ctx context.Context, keyID string, t types.Tier) error
}

// RedisStore is the Redis-backed Store.
type RedisStore struct {
	rdb      redis.Cmdable
	ttl      time.Duration
	dedupCap int64
}

// NewRedisStore builds a store. ttl bounds idle bot state; dedupCap is the max
// remembered message ids per bot (FIFO by score).
func NewRedisStore(rdb redis.Cmdable, ttl time.Duration, dedupCap int64) *RedisStore {
	if ttl <= 0 {
		ttl = 30 * 24 * time.Hour
	}
	if dedupCap <= 0 {
		dedupCap = 2048
	}
	return &RedisStore{rdb: rdb, ttl: ttl, dedupCap: dedupCap}
}

func (s *RedisStore) k(keyID, suffix string) string { return "mzbot:" + keyID + ":" + suffix }

// Seen records msgID and reports whether it was already present (dedup). The set
// is trimmed to dedupCap newest entries.
func (s *RedisStore) Seen(ctx context.Context, keyID, msgID string) (bool, error) {
	if msgID == "" {
		return false, nil
	}
	key := s.k(keyID, "dedup")
	score := float64(time.Now().UnixNano())
	added, err := s.rdb.ZAddNX(ctx, key, redis.Z{Score: score, Member: msgID}).Result()
	if err != nil {
		return false, err
	}
	if added == 0 {
		return true, nil // already seen
	}
	// New id: trim oldest beyond the cap and refresh TTL.
	pipe := s.rdb.Pipeline()
	pipe.ZRemRangeByRank(ctx, key, 0, -s.dedupCap-1)
	pipe.Expire(ctx, key, s.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	return false, nil
}

// Channels returns the channel ids this bot should poll.
func (s *RedisStore) Channels(ctx context.Context, keyID string) ([]string, error) {
	return s.rdb.SMembers(ctx, s.k(keyID, "chan")).Result()
}

// AddChannel records a channel id for polling.
func (s *RedisStore) AddChannel(ctx context.Context, keyID, channelID string) error {
	if channelID == "" {
		return nil
	}
	pipe := s.rdb.Pipeline()
	pipe.SAdd(ctx, s.k(keyID, "chan"), channelID)
	pipe.Expire(ctx, s.k(keyID, "chan"), s.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// Cursor returns the last-seen message id for a channel ("" if none).
func (s *RedisStore) Cursor(ctx context.Context, keyID, channelID string) (string, error) {
	v, err := s.rdb.HGet(ctx, s.k(keyID, "cur"), channelID).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

// SetCursor advances the last-seen message id for a channel.
func (s *RedisStore) SetCursor(ctx context.Context, keyID, channelID, cursor string) error {
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, s.k(keyID, "cur"), channelID, cursor)
	pipe.Expire(ctx, s.k(keyID, "cur"), s.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// BumpActivity records that the bot just handled a message.
func (s *RedisStore) BumpActivity(ctx context.Context, keyID string, at time.Time) error {
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, s.k(keyID, "meta"), "last", at.Unix())
	pipe.HIncrBy(ctx, s.k(keyID, "meta"), "count", 1)
	pipe.Expire(ctx, s.k(keyID, "meta"), s.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// Activity returns the last-activity unix time and lifetime message count.
func (s *RedisStore) Activity(ctx context.Context, keyID string) (int64, int64, error) {
	vals, err := s.rdb.HMGet(ctx, s.k(keyID, "meta"), "last", "count").Result()
	if err != nil {
		return 0, 0, err
	}
	return asInt(vals[0]), asInt(vals[1]), nil
}

// Tier returns the bot's persisted tier (defaults to Cold).
func (s *RedisStore) Tier(ctx context.Context, keyID string) (types.Tier, error) {
	v, err := s.rdb.HGet(ctx, s.k(keyID, "meta"), "tier").Result()
	if err == redis.Nil {
		return types.Cold, nil
	}
	if err != nil {
		return types.Cold, err
	}
	switch v {
	case "hot":
		return types.Hot, nil
	case "warm":
		return types.Warm, nil
	default:
		return types.Cold, nil
	}
}

// SetTier persists the bot's tier.
func (s *RedisStore) SetTier(ctx context.Context, keyID string, t types.Tier) error {
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, s.k(keyID, "meta"), "tier", t.String())
	pipe.Expire(ctx, s.k(keyID, "meta"), s.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func asInt(v any) int64 {
	switch x := v.(type) {
	case string:
		var n int64
		_, _ = fmt.Sscan(x, &n)
		return n
	case int64:
		return x
	default:
		return 0
	}
}
