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
	// Optional bot name shown on the typing indicator. When empty the engine
	// resolves it from the bot's Mezon account (GetAccount).
	BotUsername    string
	BotDisplayName string
	// Per-channel answer triggers (portal channel settings); empty → the
	// consumer's default gate applies.
	ChannelTriggers []ChannelTrigger
	// CommandPrefixes are the message prefixes this bot's TASKS declare as
	// their trigger (`*chambai`). A message starting with one is addressed to
	// the bot by definition — it named a command the bot owns — so it passes
	// the mention gate without an @mention.
	//
	// Derived from the tasks themselves, never configured separately: a task
	// that says `*chambai` fires it, and a gate that had to be told the same
	// thing again in the portal is a second place to look for what a bot
	// answers, and the two would drift.
	CommandPrefixes []string
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
	// Attachments is the raw attachments blob, in the same REST JSON dialect
	// as Mentions: `[{"filename":..,"url":..,"filetype":..,"size":N}]`.
	// A student sending an essay as a file is an ordinary message with one of
	// these on it, so dropping them at the wire would lose the submission.
	Attachments  string
	References   string // raw references blob (JSON [{"message_sender_id": ...}, ...])
	Username     string
	Avatar       string
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
