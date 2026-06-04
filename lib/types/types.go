// Package types holds the leaf data types shared across mezon-go-sdk-turbo
// (rest, ws, poller, tier, engine) so subpackages never import each other.
package types

// Tier is a bot's resource tier. Higher = more resources / lower latency.
type Tier int8

const (
	// Cold: no socket, polled slowly via REST; state lives in Redis.
	Cold Tier = iota
	// Warm: no socket, polled frequently via REST.
	Warm
	// Hot: live WebSocket, lowest latency, capped by the memory budget.
	Hot
)

func (t Tier) String() string {
	switch t {
	case Hot:
		return "hot"
	case Warm:
		return "warm"
	default:
		return "cold"
	}
}

// ChannelTrigger is a per-channel answer rule the consumer applies before
// handling a clan-channel message. Matcher is a Mezon channel id, "dm" or "*"
// (global). Mode: "mention" (default), "pattern", or "all".
type ChannelTrigger struct {
	Matcher     string
	ChannelType string
	Mode        string
	Patterns    []string
}

// BotRef identifies a bot and carries the bits the engine needs to connect and
// route turns. It is a value type — cheap to copy, ~100 bytes.
type BotRef struct {
	KeyID        string // backend bot-key id (unique per listener)
	TenantID     string
	BotUserID    string // Mezon bot identity
	BotToken     string // Mezon credential (WS token / REST auth)
	WorkspaceID  string
	BotVersionID string
	Plan         string // tenant plan: starter | pro | enterprise (priority weight)
	// Per-channel answer triggers (portal channel settings); empty → the
	// consumer's default gate applies.
	ChannelTriggers []ChannelTrigger
}

// Message is the normalized inbound message, populated by either the WS partial
// decoder (protobuf api.ChannelMessage) or the REST poller (ApiChannelMessage),
// so consumers never depend on the wire format.
type Message struct {
	ChannelID    string
	ClanID       string
	MessageID    string
	SenderID     string
	Content      string // raw Mezon content blob (JSON {"t": "..."} typically)
	Mentions     string // raw mentions blob (JSON [{"user_id": ...}, ...])
	References   string // raw references blob (JSON [{"message_sender_id": ...}, ...])
	Username     string
	DisplayName  string
	ClanNick     string
	ChannelLabel string
	Mode         int32
	IsPublic     bool
}

// IsDM reports whether the message is a direct message (no clan).
func (m Message) IsDM() bool {
	return m.ClanID == "" || m.ClanID == "0"
}
