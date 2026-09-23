package rest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

// Clan is a clan the bot has joined.
type Clan struct {
	ID   string
	Name string
}

// Channel is a text channel of a clan.
type Channel struct {
	ID    string
	Label string
}

// ListClanIDs returns the clan ids the bot has joined — ListClans without the
// names.
func (c *Client) ListClanIDs(ctx context.Context, baseURL, sessionToken string) ([]string, error) {
	clans, err := c.ListClans(ctx, baseURL, sessionToken)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(clans))
	for _, cl := range clans {
		ids = append(ids, cl.ID)
	}
	return ids, nil
}

// ListClans returns the clans the bot has joined, via the RPC-style protobuf
// endpoint POST /mezon.api.Mezon/ListClanDescs (session Bearer, empty request
// = server defaults). baseURL is the session's api_url — the auth gateway 404s
// this path.
//
// The response is walked with protowire instead of the codegen model: the
// pinned mezon-protobuf's ClanDesc has drifted from the live server schema
// (live: 1 creator_id, 2 clan_name, 5 clan_id; codegen: field 1 = creator_id
// string). Decode exactly the fields needed and stay drift-proof.
func (c *Client) ListClans(ctx context.Context, baseURL, sessionToken string) ([]Clan, error) {
	payload, err := c.rpc(ctx, baseURL, sessionToken, "ListClanDescs", nil)
	if err != nil {
		return nil, fmt.Errorf("list clan descs: %w", err)
	}
	var clans []Clan
	err = eachRepeated(payload, func(desc []byte) {
		var cl Clan
		walk(desc, func(num protowire.Number, v uint64, b []byte) {
			switch num {
			case 2:
				cl.Name = string(b)
			case 5: // NOT 1 — that is creator_id, also an int64 user id
				cl.ID = strconv.FormatUint(v, 10)
			}
		})
		if cl.ID != "" && cl.ID != "0" {
			clans = append(clans, cl)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("list clan descs: %w", err)
	}
	return clans, nil
}

// ListChannels returns a clan's text channels via POST
// /mezon.api.Mezon/ListChannelDescs. Live wire (cmd/wsprobe): request
// 4 clan_id, 5 channel_type; response repeated 1 ChannelDescription
// {3 channel_id, 6 type, 8 label}.
func (c *Client) ListChannels(ctx context.Context, baseURL, sessionToken, clanID string) ([]Channel, error) {
	cid, err := strconv.ParseUint(clanID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("list channel descs: bad clan id %q", clanID)
	}
	req := protowire.AppendVarint(protowire.AppendTag(nil, 4, protowire.VarintType), cid)
	req = protowire.AppendVarint(protowire.AppendTag(req, 5, protowire.VarintType), channelTypeText)
	payload, err := c.rpc(ctx, baseURL, sessionToken, "ListChannelDescs", req)
	if err != nil {
		return nil, fmt.Errorf("list channel descs: %w", err)
	}
	var chans []Channel
	err = eachRepeated(payload, func(desc []byte) {
		var ch Channel
		typ := uint64(channelTypeText)
		walk(desc, func(num protowire.Number, v uint64, b []byte) {
			switch num {
			case 3:
				ch.ID = strconv.FormatUint(v, 10)
			case 6:
				typ = v
			case 8:
				ch.Label = string(b)
			}
		})
		// The server may ignore the type filter; keep text channels only.
		if ch.ID != "" && ch.ID != "0" && typ == channelTypeText {
			chans = append(chans, ch)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("list channel descs: %w", err)
	}
	return chans, nil
}

// channelTypeText is CHANNEL_TYPE_CHANNEL.
const channelTypeText = 1

func (c *Client) rpc(ctx context.Context, baseURL, sessionToken, method string, body []byte) ([]byte, error) {
	base := strings.TrimRight(baseURL, "/")
	if base == "" {
		base = c.basePath
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/mezon.api.Mezon/"+method, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Accept", "application/proto")
	req.Header.Set("Authorization", "Bearer "+sessionToken)

	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", res.StatusCode, payload)
	}
	return payload, nil
}

// eachRepeated calls fn with every top-level field-1 message in b.
func eachRepeated(b []byte, fn func([]byte)) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return fmt.Errorf("bad wire tag")
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			msg, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return fmt.Errorf("bad message bytes")
			}
			b = b[n:]
			fn(msg)
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return fmt.Errorf("bad field %d", num)
		}
		b = b[n:]
	}
	return nil
}

// walk visits the varint and bytes fields of one message; it stops quietly on
// malformed input (a half-decoded desc is dropped by its caller's id check).
func walk(b []byte, fn func(num protowire.Number, v uint64, bytes []byte)) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]
		switch typ {
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return
			}
			fn(num, v, nil)
			b = b[n:]
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return
			}
			fn(num, 0, v)
			b = b[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return
			}
			b = b[n:]
		}
	}
}
