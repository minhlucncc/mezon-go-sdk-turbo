package tier_test

import (
	"sync"
	"testing"
	"time"

	"github.com/mezon/mezon-go-sdk-turbo/lib/tier"
	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

type fakeAct struct {
	mu             sync.Mutex
	opened, closed []string
	polled         []string
}

func (f *fakeAct) OpenHot(b types.BotRef) {
	f.mu.Lock()
	f.opened = append(f.opened, b.KeyID)
	f.mu.Unlock()
}
func (f *fakeAct) CloseHot(keyID string) {
	f.mu.Lock()
	f.closed = append(f.closed, keyID)
	f.mu.Unlock()
}
func (f *fakeAct) Poll(b types.BotRef) {
	f.mu.Lock()
	f.polled = append(f.polled, b.KeyID)
	f.mu.Unlock()
}

func bot(key, tenant, plan string) types.BotRef {
	return types.BotRef{KeyID: key, TenantID: tenant, BotUserID: key, Plan: plan}
}

func TestPromoteRespectsMaxHot(t *testing.T) {
	act := &fakeAct{}
	now := time.Unix(1000, 0)
	m := tier.New(tier.Config{MaxHot: 2, Now: func() time.Time { return now }}, act)
	for _, k := range []string{"a", "b", "c"} {
		m.Add(bot(k, "t1", "pro"))
	}
	for _, k := range []string{"a", "b", "c"} {
		m.Touch(k)
	}
	if got := m.HotCount(); got != 2 {
		t.Fatalf("MaxHot=2 should cap hot at 2, got %d", got)
	}
}

func TestPerTenantFairness(t *testing.T) {
	act := &fakeAct{}
	now := time.Unix(1000, 0)
	m := tier.New(tier.Config{MaxHot: 10, HotPerTenant: 1, Now: func() time.Time { return now }}, act)
	m.Add(bot("a", "t1", "pro"))
	m.Add(bot("b", "t1", "pro")) // same tenant
	m.Add(bot("c", "t2", "pro"))
	m.Touch("a")
	m.Touch("b")
	m.Touch("c")
	if got := m.HotCount(); got != 2 {
		t.Fatalf("fairness: at most 1 hot/tenant over 2 tenants => 2 hot, got %d", got)
	}
}

func TestIdleDemotion(t *testing.T) {
	act := &fakeAct{}
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	m := tier.New(tier.Config{MaxHot: 5, HotIdle: 2 * time.Minute, WarmIdle: 10 * time.Minute, Now: clock}, act)
	m.Add(bot("a", "t1", "pro"))
	m.Touch("a")
	if m.TierOf("a") != types.Hot {
		t.Fatal("active bot should be hot")
	}
	now = now.Add(3 * time.Minute) // past HotIdle
	m.Rebalance()
	if m.TierOf("a") != types.Warm {
		t.Fatalf("idle past HotIdle should demote to warm, got %s", m.TierOf("a"))
	}
	now = now.Add(11 * time.Minute) // past WarmIdle
	m.Rebalance()
	if m.TierOf("a") != types.Cold {
		t.Fatalf("idle past WarmIdle should demote to cold, got %s", m.TierOf("a"))
	}
}

func TestMemoryWatermarkEvicts(t *testing.T) {
	act := &fakeAct{}
	now := time.Unix(1000, 0)
	var mem uint64 = 10 // start healthy
	m := tier.New(tier.Config{
		MaxHot:         3,
		PressureHotCap: 1,
		MemHighMB:      100,
		Now:            func() time.Time { return now },
		MemMB:          func() uint64 { return mem },
		PlanWeight:     map[string]float64{"enterprise": 3, "pro": 2, "starter": 1},
	}, act)
	m.Add(bot("a", "t1", "enterprise"))
	m.Add(bot("b", "t2", "pro"))
	m.Add(bot("c", "t3", "starter"))
	for _, k := range []string{"a", "b", "c"} {
		m.Touch(k)
	}
	if m.HotCount() != 3 {
		t.Fatalf("all 3 should be hot under healthy memory, got %d", m.HotCount())
	}
	mem = 200 // cross the high watermark
	m.Rebalance()
	if m.HotCount() != 1 {
		t.Fatalf("pressure should cut hot to PressureHotCap=1, got %d", m.HotCount())
	}
	if m.TierOf("a") != types.Hot {
		t.Fatalf("highest-priority (enterprise) bot should survive eviction, got %s", m.TierOf("a"))
	}
}

func TestColdBotsArePolled(t *testing.T) {
	act := &fakeAct{}
	now := time.Unix(1000, 0)
	m := tier.New(tier.Config{MaxHot: 5, ColdPoll: time.Minute, Now: func() time.Time { return now }}, act)
	m.Add(bot("a", "t1", "pro")) // never touched -> cold
	m.Rebalance()
	act.mu.Lock()
	polled := len(act.polled)
	act.mu.Unlock()
	if polled != 1 {
		t.Fatalf("cold bot should be polled once when due, got %d", polled)
	}
	m.Rebalance()
	act.mu.Lock()
	polled2 := len(act.polled)
	act.mu.Unlock()
	if polled2 != 1 {
		t.Fatalf("cold bot should not be re-polled before interval, got %d", polled2)
	}
}
