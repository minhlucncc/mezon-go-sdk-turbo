// Package rest is a thin, host-configurable wrapper over the official SDK's
// codegen REST client (mezon-api). It powers the warm/cold poll path
// (ListChannelMessages with a cursor) and bot authentication. Unlike the SDK's
// WebSocket layer, the codegen Configuration.BasePath is settable, so the host
// is configurable per environment.
package rest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/antihax/optional"
	mezonapi "github.com/nccasia/mezon-go-sdk/mezon-api"

	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

// directionAfter requests messages newer than the cursor. The exact enum value
// is confirmed against a live endpoint in the shadow phase; 1 = "after" here.
const directionAfter = int32(1)

// Session is the result of authenticating a bot. Beyond credentials it also
// ROUTES the client: WSURL is the host the socket must dial (live: it differs
// from the auth host — gw.mezon.ai authenticates, sock.mezon.ai serves WS) and
// APIURL is the message REST base.
type Session struct {
	Token        string
	RefreshToken string
	UserID       string
	APIURL       string
	WSURL        string
}

// Client wraps a single Mezon API base URL. A per-bot access token is supplied
// per call (bots authenticate independently), so Client itself is stateless and
// safe to share across goroutines.
type Client struct {
	api      *mezonapi.APIClient
	basePath string
	http     *http.Client
}

// New builds a REST client against basePath (e.g. "https://api.mezon.ai").
func New(basePath string) *Client {
	cfg := mezonapi.NewConfiguration()
	if basePath != "" {
		cfg.BasePath = basePath
	}
	return &Client{
		api:      mezonapi.NewAPIClient(cfg),
		basePath: strings.TrimRight(cfg.BasePath, "/"),
		http:     &http.Client{Timeout: 15 * time.Second},
	}
}

// Authenticate exchanges a bot's App ID + API key for a session. The WS
// handshake and the message-list endpoints only accept SESSION tokens —
// dialing with the raw key fails with "bad handshake". Wire format mirrors
// the Python SDK's SessionManager.authenticate (the production-proven shape):
// standard HTTP Basic appid:key, body {"account":{"appid","token"}}. The
// appid IS the bot's Mezon user id (worker bot keys use them interchangeably);
// omitting it gets {"code":3,"message":"Invalid App Id."}.
func (c *Client) Authenticate(ctx context.Context, appID, apiKey string) (Session, error) {
	body, err := json.Marshal(map[string]any{"account": map[string]string{"appid": appID, "token": apiKey}})
	if err != nil {
		return Session{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.basePath+"/v2/apps/authenticate/token", bytes.NewReader(body))
	if err != nil {
		return Session{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(appID+":"+apiKey)))

	res, err := c.http.Do(req)
	if err != nil {
		return Session{}, err
	}
	defer res.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode != http.StatusOK {
		return Session{}, fmt.Errorf("authenticate: HTTP %d: %s", res.StatusCode, payload)
	}
	// Live responses are snake_case (refresh_token, user_id, ws_url); the
	// official SDK's codegen model claims camelCase — accept both.
	var sess struct {
		Token         string `json:"token"`
		RefreshToken  string `json:"refresh_token"`
		RefreshTokenC string `json:"refreshToken"`
		UserID        string `json:"user_id"`
		UserIDC       string `json:"userId"`
		APIURL        string `json:"api_url"`
		WSURL         string `json:"ws_url"`
	}
	if err := json.Unmarshal(payload, &sess); err != nil {
		return Session{}, fmt.Errorf("authenticate: bad response: %w", err)
	}
	if sess.Token == "" {
		return Session{}, errors.New("authenticate: response carried no session token")
	}
	if sess.RefreshToken == "" {
		sess.RefreshToken = sess.RefreshTokenC
	}
	if sess.UserID == "" {
		sess.UserID = sess.UserIDC
	}
	return Session{
		Token: sess.Token, RefreshToken: sess.RefreshToken, UserID: sess.UserID,
		APIURL: sess.APIURL, WSURL: sess.WSURL,
	}, nil
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
		Attachments:  m.Attachments,
		References:   m.References,
		Username:     m.Username,
		DisplayName:  m.DisplayName,
		ClanNick:     m.ClanNick,
		ChannelLabel: m.ChannelLabel,
		Mode:         m.Mode,
		IsPublic:     m.IsPublic,
	}
}
