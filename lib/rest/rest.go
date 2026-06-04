// Package rest is a thin, host-configurable wrapper over the official SDK's
// codegen REST client (mezon-api). It powers the warm/cold poll path
// (ListChannelMessages with a cursor) and bot authentication. Unlike the SDK's
// WebSocket layer, the codegen Configuration.BasePath is settable, so the host
// is configurable per environment.
package rest

import (
	"context"

	"github.com/antihax/optional"
	mezonapi "github.com/nccasia/mezon-go-sdk/mezon-api"

	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

// directionAfter requests messages newer than the cursor. The exact enum value
// is confirmed against a live endpoint in the shadow phase; 1 = "after" here.
const directionAfter = int32(1)

// Session is the result of authenticating a bot.
type Session struct {
	Token        string
	RefreshToken string
	UserID       string
}

// Client wraps a single Mezon API base URL. A per-bot access token is supplied
// per call (bots authenticate independently), so Client itself is stateless and
// safe to share across goroutines.
type Client struct {
	api *mezonapi.APIClient
}

// New builds a REST client against basePath (e.g. "https://api.mezon.ai").
func New(basePath string) *Client {
	cfg := mezonapi.NewConfiguration()
	if basePath != "" {
		cfg.BasePath = basePath
	}
	return &Client{api: mezonapi.NewAPIClient(cfg)}
}

func authCtx(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return context.WithValue(ctx, mezonapi.ContextAccessToken, token)
}

// ListSince fetches messages newer than cursor for a channel and returns the new
// cursor to persist. An empty cursor fetches the most recent page.
func (c *Client) ListSince(
	ctx context.Context, token, channelID, clanID, cursor string, limit int32,
) ([]types.Message, string, error) {
	opts := &mezonapi.MezonListChannelMessagesOpts{Direction: optional.NewInt32(directionAfter)}
	if clanID != "" {
		opts.ClanId = optional.NewString(clanID)
	}
	if cursor != "" {
		opts.MessageId = optional.NewString(cursor)
	}
	if limit > 0 {
		opts.Limit = optional.NewInt32(limit)
	}

	res, _, err := c.api.MezonApi.MezonListChannelMessages(authCtx(ctx, token), channelID, opts)
	if err != nil {
		return nil, cursor, err
	}

	out := make([]types.Message, 0, len(res.Messages))
	for i := range res.Messages {
		out = append(out, fromAPI(&res.Messages[i]))
	}

	newCursor := cursor
	if res.LastSeenMessage != nil && res.LastSeenMessage.Id != "" {
		newCursor = res.LastSeenMessage.Id
	} else if n := len(res.Messages); n > 0 {
		newCursor = res.Messages[n-1].MessageId
	}
	return out, newCursor, nil
}

// fromAPI normalizes a REST message into the neutral type.
func fromAPI(m *mezonapi.ApiChannelMessage) types.Message {
	return types.Message{
		ChannelID:    m.ChannelId,
		ClanID:       m.ClanId,
		MessageID:    m.MessageId,
		SenderID:     m.SenderId,
		Content:      m.Content,
		Mentions:     m.Mentions,
		References:   m.References,
		Username:     m.Username,
		DisplayName:  m.DisplayName,
		ClanNick:     m.ClanNick,
		ChannelLabel: m.ChannelLabel,
		Mode:         m.Mode,
		IsPublic:     m.IsPublic,
	}
}
