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
	// Decay is pressure-driven: demotion applies only when the hot budget is
	// CONTENDED (a fresher bot wants the slot). Spare capacity keeps idle
	// bots hot — see TestSpareHotCapacityKeepsIdleBotsHot.
	act := &fakeAct{}
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	m := tier.New(tier.Config{MaxHot: 1, HotIdle: 2 * time.Minute, WarmIdle: 10 * time.Minute, Now: clock}, act)
	m.Add(bot("a", "t1", "pro"))
	m.Touch("a")
	if m.TierOf("a") != types.Hot {
		t.Fatal("active bot should be hot")
	}
	now = now.Add(3 * time.Minute) // a past HotIdle
	m.Add(bot("b", "t2", "pro"))   // fresh rival contends for the only slot
	m.Touch("b")
	if m.TierOf("a") != types.Warm {
		t.Fatalf("idle past HotIdle should yield under pressure, got %s", m.TierOf("a"))
	}
	now = now.Add(11 * time.Minute) // a past WarmIdle; keep b fresh
	m.Touch("b")
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
	m := tier.New(tier.Config{
		MaxHot: 1, ColdPoll: time.Minute,
		HotIdle: time.Minute, WarmIdle: 5 * time.Minute,
		Now: func() time.Time { return now },
	}, act)
	// New bots start hot (cold-start fix); decay needs CONTENTION now, so a
	// fresher rival takes the only slot and "a" falls through warm to cold.
	m.Add(bot("a", "t1", "pro"))
	m.Rebalance()
	now = now.Add(6 * time.Minute) // a past WarmIdle
	m.Add(bot("rival", "t2", "pro"))
	m.Touch("rival") // takes the slot; a decays cold + first due poll
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

// A freshly-added bot must be promoted to Hot on the next rebalance (budget
// permitting) WITHOUT waiting for inbound activity. A new bot has no known
// channels, so warm/cold REST polling is a no-op for it — if registration
// doesn't open the socket, the bot can never see its first message and is
// dead forever (cold-start deadlock). Mirrors the Python worker, which opened
// one listener per key immediately.
func TestNewBotPromotedToHotOnAdd(t *testing.T) {
	act := &fakeAct{}
	now := time.Unix(1000, 0)
	m := tier.New(tier.Config{MaxHot: 10, Now: func() time.Time { return now }}, act)

	m.Add(bot("fresh", "t1", "pro"))
	m.Rebalance()

	act.mu.Lock()
	opened := append([]string(nil), act.opened...)
	act.mu.Unlock()
	if len(opened) != 1 || opened[0] != "fresh" {
		t.Fatalf("new bot must open a hot socket on the first rebalance, got opened=%v", opened)
	}
}

// The promotion is budget-bound: with no hot slots free, a new bot still must
// not sit Cold (it would deadlock) — it lands Warm at worst.
func TestNewBotBeyondBudgetStaysOutOfCold(t *testing.T) {
	act := &fakeAct{}
	now := time.Unix(1000, 0)
	m := tier.New(tier.Config{MaxHot: 1, Now: func() time.Time { return now }}, act)
	m.Add(bot("a", "t1", "pro"))
	m.Touch("a") // occupies the only hot slot with a higher rate score
	m.Add(bot("fresh", "t2", "pro"))
	m.Rebalance()

	if got := m.HotCount(); got != 1 {
		t.Fatalf("budget must hold, hot=%d", got)
	}
}

// Idle decay still applies UNDER PRESSURE: a never-active bot yields its hot
// slot to a fresher rival once the budget is contended.
func TestNewBotDecaysWhenIdle(t *testing.T) {
	act := &fakeAct{}
	now := time.Unix(1000, 0)
	m := tier.New(tier.Config{
		MaxHot: 1, HotIdle: time.Minute, WarmIdle: 10 * time.Minute,
		Now: func() time.Time { return now },
	}, act)
	m.Add(bot("fresh", "t1", "pro"))
	m.Rebalance()

	now = now.Add(11 * time.Minute) // past WarmIdle
	m.Add(bot("rival", "t2", "pro"))
	m.Touch("rival") // contends for the only slot

	act.mu.Lock()
	closed := append([]string(nil), act.closed...)
	act.mu.Unlock()
	if len(closed) != 1 || closed[0] != "fresh" {
		t.Fatalf("idle bot must release its hot socket under pressure, closed=%v", closed)
	}
}

// Idle decay must be PRESSURE-DRIVEN, not absolute: with spare hot budget an
// idle bot keeps its socket. (2026-06-06: a single registered bot was demoted
// every HotIdle=2m, closing a perfectly healthy socket — "works once then
// dies" from the worker's own tier manager; warm REST polling can't cover a
// bot whose channel list is still empty.)
func TestSpareHotCapacityKeepsIdleBotsHot(t *testing.T) {
	act := &fakeAct{}
	now := time.Unix(1000, 0)
	m := tier.New(tier.Config{
		MaxHot: 5, HotIdle: time.Minute, WarmIdle: 5 * time.Minute,
		Now: func() time.Time { return now },
	}, act)
	m.Add(bot("solo", "t1", "pro"))
	m.Rebalance()

	// way past HotIdle AND WarmIdle — budget is empty, so it must stay hot
	now = now.Add(30 * time.Minute)
	m.Rebalance()

	act.mu.Lock()
	closed := append([]string(nil), act.closed...)
	act.mu.Unlock()
	if len(closed) != 0 {
		t.Fatalf("idle bot with spare hot capacity must keep its socket, closed=%v", closed)
	}
	if m.TierOf("solo") != types.Hot {
		t.Fatalf("tier = %s, want hot", m.TierOf("solo"))
	}
}

// Under contention the activity-based decay still applies: the active bot
// keeps the only slot, the idle one demotes.
func TestIdleDecayUnderPressure(t *testing.T) {
	act := &fakeAct{}
	now := time.Unix(1000, 0)
	m := tier.New(tier.Config{
		MaxHot: 1, HotIdle: time.Minute, WarmIdle: 5 * time.Minute,
		Now: func() time.Time { return now },
	}, act)
	m.Add(bot("idle", "t1", "pro"))
	m.Rebalance() // idle takes the slot first

	now = now.Add(2 * time.Minute) // idle past HotIdle
	m.Add(bot("active", "t2", "pro"))
	m.Touch("active")

	if m.TierOf("active") != types.Hot {
		t.Fatalf("active bot must win the contended slot, got %s", m.TierOf("active"))
	}
	if m.TierOf("idle") == types.Hot {
		t.Fatal("idle bot must yield the slot under pressure")
	}
}
