package rest_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/mezon/mezon-go-sdk-turbo/lib/rest"
)

// A clan the bot joined is enrolled under its real name, not its id: the
// live ClanDesc carries 2 clan_name next to 5 clan_id.
func TestListClansCarriesNames(t *testing.T) {
	desc := func(id uint64, name string) []byte {
		d := protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 42) // creator
		d = protowire.AppendString(protowire.AppendTag(d, 2, protowire.BytesType), name)
		return protowire.AppendVarint(protowire.AppendTag(d, 5, protowire.VarintType), id)
	}
	var resp []byte
	resp = protowire.AppendBytes(protowire.AppendTag(resp, 1, protowire.BytesType), desc(111, "FUNiX"))
	resp = protowire.AppendBytes(protowire.AppendTag(resp, 1, protowire.BytesType), desc(222, "IELTS"))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(resp)
	}))
	defer srv.Close()

	clans, err := rest.New(srv.URL).ListClans(t.Context(), srv.URL, "tok")
	if err != nil {
		t.Fatalf("list clans: %v", err)
	}
	want := []rest.Clan{{ID: "111", Name: "FUNiX"}, {ID: "222", Name: "IELTS"}}
	if len(clans) != len(want) || clans[0] != want[0] || clans[1] != want[1] {
		t.Fatalf("clans = %+v, want %+v", clans, want)
	}
}

// A newly detected clan gets its text channels listed so they can be created
// under it. Live wire (cmd/wsprobe): request 4 clan_id, 5 channel_type;
// response repeated 1 ChannelDescription{3 channel_id, 6 type, 8 label}.
func TestListChannelsOfClan(t *testing.T) {
	ch := func(id uint64, typ uint64, label string) []byte {
		d := protowire.AppendVarint(protowire.AppendTag(nil, 3, protowire.VarintType), id)
		d = protowire.AppendVarint(protowire.AppendTag(d, 6, protowire.VarintType), typ)
		return protowire.AppendString(protowire.AppendTag(d, 8, protowire.BytesType), label)
	}
	var resp []byte
	resp = protowire.AppendBytes(protowire.AppendTag(resp, 1, protowire.BytesType), ch(7, 1, "general"))
	resp = protowire.AppendBytes(protowire.AppendTag(resp, 1, protowire.BytesType), ch(8, 4, "voice"))
	resp = protowire.AppendBytes(protowire.AppendTag(resp, 1, protowire.BytesType), ch(9, 1, "hỏi-đáp"))

	var gotClan, gotType uint64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mezon.api.Mezon/ListChannelDescs" {
			http.NotFound(w, r)
			return
		}
		b, _ := io.ReadAll(r.Body)
		for len(b) > 0 {
			num, _, n := protowire.ConsumeTag(b)
			b = b[n:]
			v, m := protowire.ConsumeVarint(b)
			b = b[m:]
			switch num {
			case 4:
				gotClan = v
			case 5:
				gotType = v
			}
		}
		_, _ = w.Write(resp)
	}))
	defer srv.Close()

	chans, err := rest.New(srv.URL).ListChannels(t.Context(), srv.URL, "tok", "555")
	if err != nil {
		t.Fatalf("list channels: %v", err)
	}
	if gotClan != 555 || gotType != 1 {
		t.Fatalf("request clan=%d type=%d, want 555/1", gotClan, gotType)
	}
	want := []rest.Channel{{ID: "7", Label: "general"}, {ID: "9", Label: "hỏi-đáp"}}
	if len(chans) != 2 || chans[0] != want[0] || chans[1] != want[1] {
		t.Fatalf("channels = %+v, want %+v (text channels only)", chans, want)
	}
}
