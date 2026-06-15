package rest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

// ListClanIDs returns the clan ids the bot has joined, via the RPC-style
// protobuf endpoint POST /mezon.api.Mezon/ListClanDescs (session Bearer,
// empty request = server defaults). baseURL is the session's api_url — the
// auth gateway 404s this path.
//
// The response is walked with protowire instead of the codegen model: the
// pinned mezon-protobuf's ClanDesc has drifted from the live server schema
// (live: field 1 = clan id varint; codegen: field 1 = creator_id string).
// Only the clan id is needed, so decode exactly that and stay drift-proof.
func (c *Client) ListClanIDs(ctx context.Context, baseURL, sessionToken string) ([]string, error) {
	base := strings.TrimRight(baseURL, "/")
	if base == "" {
		base = c.basePath
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/mezon.api.Mezon/ListClanDescs", nil)
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
		return nil, fmt.Errorf("list clan descs: HTTP %d: %s", res.StatusCode, payload)
	}
	return clanIDsFromWire(payload)
}

// clanIDsFromWire extracts ClanDescList.clandesc[].clan_id from the raw wire:
// top-level repeated field 1 (bytes) = ClanDesc; its field 5 (varint) = clan
// id. NOT field 1 — that is creator_id (also an int64 user id, so a wrong
// walker "works" and joins the creator instead of the clan; locked by
// TestListClanIDsGolden against the python-encoded fixture).
func clanIDsFromWire(b []byte) ([]string, error) {
	var ids []string
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, fmt.Errorf("list clan descs: bad wire tag")
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			clan, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return nil, fmt.Errorf("list clan descs: bad clandesc bytes")
			}
			b = b[n:]
			if id := clanIDFromDesc(clan); id != "" {
				ids = append(ids, id)
			}
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return nil, fmt.Errorf("list clan descs: bad field %d", num)
		}
		b = b[n:]
	}
	return ids, nil
}

func clanIDFromDesc(b []byte) string {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return ""
		}
		b = b[n:]
		if num == 5 && typ == protowire.VarintType { // ClanDesc.clan_id
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return ""
			}
			return strconv.FormatUint(v, 10)
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return ""
		}
		b = b[n:]
	}
	return ""
}
