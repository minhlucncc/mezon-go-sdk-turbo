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

// Account is the subset of the bot's own Mezon account the engine needs: the
// names clients show on its typing indicator.
type Account struct {
	UserID      string
	Username    string
	DisplayName string
}

// GetAccount fetches the session owner's account via the RPC-style protobuf
// endpoint POST /mezon.api.Mezon/GetAccount (session Bearer, empty request —
// the same call mezon-js/ios/desktop make). baseURL is the session's api_url.
// Like ListClanIDs, the response is walked with protowire rather than the
// drifted codegen model.
func (c *Client) GetAccount(ctx context.Context, baseURL, sessionToken string) (Account, error) {
	base := strings.TrimRight(baseURL, "/")
	if base == "" {
		base = c.basePath
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/mezon.api.Mezon/GetAccount", nil)
	if err != nil {
		return Account{}, err
	}
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Accept", "application/proto")
	req.Header.Set("Authorization", "Bearer "+sessionToken)

	res, err := c.http.Do(req)
	if err != nil {
		return Account{}, err
	}
	defer res.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode != http.StatusOK {
		return Account{}, fmt.Errorf("get account: HTTP %d: %s", res.StatusCode, payload)
	}
	return accountFromWire(payload)
}

// accountFromWire extracts live Account.user (field 1) → User 1 id i64,
// 2 username, 3 display_name.
func accountFromWire(b []byte) (Account, error) {
	var acc Account
	err := walkFields(b, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) {
		if num != 1 || typ != protowire.BytesType {
			return
		}
		_ = walkFields(v, func(num protowire.Number, typ protowire.Type, v []byte, u uint64) {
			switch {
			case num == 1 && typ == protowire.VarintType:
				acc.UserID = strconv.FormatUint(u, 10)
			case num == 2 && typ == protowire.BytesType:
				acc.Username = string(v)
			case num == 3 && typ == protowire.BytesType:
				acc.DisplayName = string(v)
			}
		})
	})
	if err != nil {
		return Account{}, fmt.Errorf("get account: %w", err)
	}
	return acc, nil
}

// walkFields visits each top-level field: bytes fields get v, varints get u.
func walkFields(b []byte, visit func(protowire.Number, protowire.Type, []byte, uint64)) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return fmt.Errorf("bad wire tag")
		}
		b = b[n:]
		switch typ {
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return fmt.Errorf("bad bytes field %d", num)
			}
			visit(num, typ, v, 0)
			b = b[n:]
		case protowire.VarintType:
			u, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return fmt.Errorf("bad varint field %d", num)
			}
			visit(num, typ, nil, u)
			b = b[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return fmt.Errorf("bad field %d", num)
			}
			b = b[n:]
		}
	}
	return nil
}
