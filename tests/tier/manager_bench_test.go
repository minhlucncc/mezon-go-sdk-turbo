package tier_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/mezon/mezon-go-sdk-turbo/lib/tier"
	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

type noopAct struct{}

func (noopAct) OpenHot(types.BotRef) {}
func (noopAct) CloseHot(string)      {}
func (noopAct) Poll(types.BotRef)    {}

func BenchmarkRebalanceTenThousandBots(b *testing.B) {
	now := time.Unix(1_700_000_000, 0)
	m := tier.New(tier.Config{
		MaxHot:       200,
		HotPerTenant: 5,
		ColdPoll:     time.Hour,
		Now:          func() time.Time { return now },
	}, noopAct{})
	for i := 0; i < 10_000; i++ {
		m.Add(types.BotRef{
			KeyID:     "k" + strconv.Itoa(i),
			TenantID:  "tenant-" + strconv.Itoa(i%100),
			BotUserID: "bot-" + strconv.Itoa(i),
			Plan:      "pro",
		})
		if i%20 == 0 {
			m.Touch("k" + strconv.Itoa(i))
		}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Rebalance()
	}
}
