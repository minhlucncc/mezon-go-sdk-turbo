package rest_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/mezon/mezon-go-sdk-turbo/lib/rest"
)

// Mezon bots authenticate by exchanging their API key for a session token
// (POST /v2/apps/authenticate/token) — the WS handshake and REST polls only
// accept SESSION tokens, never the raw key. Wire format mirrors the official
// SDK's getAuthenticate (nccasia/mezon-go-sdk client.go).
func TestAuthenticateExchangesAPIKeyForSession(t *testing.T) {
	var gotAuth, gotBody, gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		gotAuth = r.Header.Get("Authorization")
		var body struct {
			Account struct {
				AppID string `json:"appid"`
				Token string `json:"token"`
			} `json:"account"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotBody = body.Account.AppID + "|" + body.Account.Token
		w.Header().Set("Content-Type", "application/json")
		// Live-confirmed response shape (snake_case): the session also routes
		// the client — ws_url is where the socket must dial (sock.mezon.ai).
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token": "session-jwt", "refresh_token": "refresh-jwt", "user_id": "2062754877070643200",
			"api_url": "https://api.mezon.ai", "ws_url": "sock.mezon.ai",
		})
	}))
	defer srv.Close()

	c := rest.New(srv.URL)
	sess, err := c.Authenticate(t.Context(), "2062754877070643200", "my-api-key")
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if gotMethod != "POST" || gotPath != "/v2/apps/authenticate/token" {
		t.Fatalf("wrong endpoint: %s %s", gotMethod, gotPath)
	}
	// Standard HTTP Basic with appid:key — the Python SDK's proven format
	// (the appid-less official-Go format gets {"code":3,"message":"Invalid App Id."}).
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("2062754877070643200:my-api-key"))
	if gotAuth != want {
		t.Fatalf("Authorization = %q, want %q", gotAuth, want)
	}
	if gotBody != "2062754877070643200|my-api-key" {
		t.Fatalf("body appid|token = %q", gotBody)
	}
	if sess.Token != "session-jwt" || sess.RefreshToken != "refresh-jwt" || sess.UserID != "2062754877070643200" {
		t.Fatalf("session mismatch: %+v", sess)
	}
	if sess.WSURL != "sock.mezon.ai" || sess.APIURL != "https://api.mezon.ai" {
		t.Fatalf("session routing not parsed: %+v", sess)
	}
}

func TestAuthenticateSurfacesHTTPErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"invalid key"}`, http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := rest.New(srv.URL)
	if _, err := c.Authenticate(t.Context(), "123", "bad-key"); err == nil {
		t.Fatal("expected error for 401")
	}
}

func TestAuthenticateRejectsEmptySessionToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := rest.New(srv.URL)
	if _, err := c.Authenticate(t.Context(), "123", "key"); err == nil {
		t.Fatal("a 200 with no token must still error — the WS dial would just fail later")
	}
}

// The socket only delivers messages for clans the connection has JOINED
// (ClanJoin frames after dial). The clan list comes from the RPC-style
// protobuf endpoint POST /mezon.api.Mezon/ListClanDescs with the session
// Bearer — live-confirmed wire shape: repeated field 1 (clandesc), whose
// field 1 is the clan id varint (the pinned codegen model has drifted from
// the server schema, so the client walks the wire directly).
func TestListClanIDs(t *testing.T) {
	// live ClanDesc: 1 creator_id, 2 clan_name, 5 clan_id
	clan := protowire.AppendVarint(
		protowire.AppendTag(nil, 1, protowire.VarintType), 1780431535405535232)
	clan = protowire.AppendString(
		protowire.AppendTag(clan, 2, protowire.BytesType), "Demo Bot")
	clan = protowire.AppendVarint(
		protowire.AppendTag(clan, 5, protowire.VarintType), 2061345416372293632)
	resp := protowire.AppendBytes(
		protowire.AppendTag(nil, 1, protowire.BytesType), clan)

	var gotAuth, gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mezon.api.Mezon/ListClanDescs" || r.Method != "POST" {
			http.NotFound(w, r)
			return
		}
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		w.Header().Set("Content-Type", "application/proto")
		_, _ = w.Write(resp)
	}))
	defer srv.Close()

	c := rest.New(srv.URL)
	ids, err := c.ListClanIDs(t.Context(), srv.URL, "session-jwt")
	if err != nil {
		t.Fatalf("list clans: %v", err)
	}
	if gotAuth != "Bearer session-jwt" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotCT != "application/proto" {
		t.Fatalf("Content-Type = %q", gotCT)
	}
	if len(ids) != 1 || ids[0] != "2061345416372293632" {
		t.Fatalf("clan ids = %v", ids)
	}
}

func TestListClanIDsSurfacesErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusUnauthorized)
	}))
	defer srv.Close()
	c := rest.New(srv.URL)
	if _, err := c.ListClanIDs(t.Context(), srv.URL, "tok"); err == nil {
		t.Fatal("expected error for 401")
	}
}

// Golden: the ListClanDescs response encoded by the PYTHON protobuf (the live
// pin). Live ClanDesc: field 1 is creator_id; clan_id is FIELD 5 — a walker
// reading field 1 joins the creator's user id instead of the clan and the
// socket stays silent (the 2026-06-06 follow-up bug).
func TestListClanIDsGolden(t *testing.T) {
	raw, err := os.ReadFile("../ws/testdata/wire_golden.json")
	if err != nil {
		t.Fatalf("wire goldens missing (run gen_wire_golden.py): %v", err)
	}
	var g struct {
		ClanDescList struct {
			FrameB64      string   `json:"frame_b64"`
			ExpectClanIDs []string `json:"expect_clan_ids"`
		} `json:"clan_desc_list"`
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	frame, err := base64.StdEncoding.DecodeString(g.ClanDescList.FrameB64)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/proto")
		_, _ = w.Write(frame)
	}))
	defer srv.Close()

	ids, err := rest.New(srv.URL).ListClanIDs(t.Context(), srv.URL, "tok")
	if err != nil {
		t.Fatalf("list clans: %v", err)
	}
	if len(ids) != len(g.ClanDescList.ExpectClanIDs) {
		t.Fatalf("clan ids = %v, want %v", ids, g.ClanDescList.ExpectClanIDs)
	}
	for i, want := range g.ClanDescList.ExpectClanIDs {
		if ids[i] != want {
			t.Fatalf("clan ids = %v, want %v", ids, g.ClanDescList.ExpectClanIDs)
		}
	}
}
