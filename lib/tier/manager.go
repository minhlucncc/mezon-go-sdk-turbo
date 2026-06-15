// Package tier is the resource manager: it assigns each bot a hot/warm/cold tier
// from a priority score (activity + tenant plan), capped by a hot-socket budget,
// per-tenant fairness, and a memory watermark. It actuates tier changes through
// an Actuator (open/close sockets, trigger polls) supplied by the engine.
package tier

import (
	"container/heap"
	"sort"
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
)

// Actuator performs the side effects of a tier decision. All methods MUST be
// non-blocking (the engine kicks off dials/polls asynchronously); the manager
// calls them outside its lock.
type Actuator interface {
	OpenHot(bot types.BotRef) // open a live socket
	CloseHot(keyID string)    // close a live socket
	Poll(bot types.BotRef)    // run one REST poll (warm/cold discovery)
}

// Config tunes the manager.
type Config struct {
	MaxHot         int                // hot-socket cap (memory budget)
	PressureHotCap int                // hot cap while above the memory high-watermark
	HotPerTenant   int                // fairness cap (0 = unlimited)
	WarmPoll       time.Duration      // warm poll interval
	ColdPoll       time.Duration      // cold poll interval
	HotIdle        time.Duration      // hot->warm after this much idle
	WarmIdle       time.Duration      // warm->cold after this much idle
	Tick           time.Duration      // rebalance cadence
	PlanWeight     map[string]float64 // plan -> weight (e.g. starter1 pro2 enterprise3)
	MemHighMB      uint64             // heap above this => pressure (0 = disabled)
	MemLowMB       uint64             // (reserved for hysteresis)

	// Now and MemMB are injectable for tests; nil uses wall clock / runtime heap.
	Now   func() time.Time
	MemMB func() uint64
}

func (c *Config) withDefaults() {
	if c.MaxHot <= 0 {
		c.MaxHot = 200
	}
	if c.WarmPoll <= 0 {
		c.WarmPoll = 20 * time.Second
	}
	if c.ColdPoll <= 0 {
		c.ColdPoll = 3 * time.Minute
	}
	if c.HotIdle <= 0 {
		c.HotIdle = 2 * time.Minute
	}
	if c.WarmIdle <= 0 {
		c.WarmIdle = 10 * time.Minute
	}
	if c.Tick <= 0 {
		c.Tick = 2 * time.Second
	}
	if c.PlanWeight == nil {
		c.PlanWeight = map[string]float64{"starter": 1, "pro": 2, "enterprise": 3}
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.MemMB == nil {
		c.MemMB = heapMB
	}
}

func heapMB() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc / (1024 * 1024)
}

type entry struct {
	bot          types.BotRef
	tier         types.Tier
	lastActivity time.Time
	rate         float64 // EWMA messages/tick
	score        float64
	nextPoll     time.Time
}

// Manager owns the tier state. Safe for concurrent use.
type Manager struct {
	cfg  Config
	act  Actuator
	mu   sync.Mutex
	bots map[string]*entry
}

// New builds a manager.
func New(cfg Config, act Actuator) *Manager {
	cfg.withDefaults()
	return &Manager{cfg: cfg, act: act, bots: make(map[string]*entry)}
}

// Add registers a bot. Idempotent. The bot starts with FRESH activity so the
// next rebalance promotes it toward Hot (budget permitting): a new bot has no
// known channels, so warm/cold REST polling is a no-op for it — without an
// initial socket it could never see its first message and would sit Cold
// forever (cold-start deadlock). If it stays idle it decays to Warm/Cold
// through the normal windows.
func (m *Manager) Add(bot types.BotRef) {
	m.mu.Lock()
	if _, ok := m.bots[bot.KeyID]; !ok {
		m.bots[bot.KeyID] = &entry{
			bot: bot, tier: types.Cold, nextPoll: m.cfg.Now(), lastActivity: m.cfg.Now(),
		}
	}
	m.mu.Unlock()
}

// Remove deregisters a bot, closing its socket if hot.
func (m *Manager) Remove(keyID string) {
	m.mu.Lock()
	e, ok := m.bots[keyID]
	if ok {
		delete(m.bots, keyID)
	}
	m.mu.Unlock()
	if ok && e.tier == types.Hot {
		m.act.CloseHot(keyID)
	}
}

// Touch records that a bot just handled a message (from socket or poll) and
// immediately rebalances so it can be promoted toward Hot.
func (m *Manager) Touch(keyID string) {
	now := m.cfg.Now()
	m.mu.Lock()
	if e, ok := m.bots[keyID]; ok {
		e.lastActivity = now
		e.rate = e.rate*0.7 + 1.0 // EWMA bump
	}
	acts := m.rebalanceLocked(now)
	m.mu.Unlock()
	m.apply(acts)
}

// Run rebalances on a ticker until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	t := time.NewTicker(m.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := m.cfg.Now()
			m.mu.Lock()
			acts := m.rebalanceLocked(now)
			m.mu.Unlock()
			m.apply(acts)
		}
	}
}

// TierOf returns a bot's current tier (for tests/introspection).
func (m *Manager) TierOf(keyID string) types.Tier {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.bots[keyID]; ok {
		return e.tier
	}
	return types.Cold
}

// HotCount returns how many bots are currently hot.
func (m *Manager) HotCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, e := range m.bots {
		if e.tier == types.Hot {
			n++
		}
	}
	return n
}

type actKind int

const (
	actOpenHot actKind = iota
	actCloseHot
	actPoll
)

type action struct {
	kind  actKind
	bot   types.BotRef
	keyID string
}

// rebalanceLocked recomputes desired tiers and returns the side effects to run
// after the lock is released. Caller holds m.mu.
func (m *Manager) rebalanceLocked(now time.Time) []action {
	// 1. Score every bot and compute its activity-desired tier.
	want := make(map[string]types.Tier, len(m.bots))
	candidates := &scoreHeap{}
	for _, e := range m.bots {
		age := now.Sub(e.lastActivity)
		e.rate *= 0.9 // decay
		recency := 1 - age.Seconds()/m.cfg.HotIdle.Seconds()
		if recency < 0 {
			recency = 0
		}
		e.score = m.cfg.PlanWeight[e.bot.Plan]*100 + recency*10 + e.rate

		switch {
		case e.lastActivity.IsZero() || age > m.cfg.WarmIdle:
			want[e.bot.KeyID] = types.Cold
		case age > m.cfg.HotIdle:
			want[e.bot.KeyID] = types.Warm
		default:
			want[e.bot.KeyID] = types.Hot
			candidates.Push(e)
		}
	}

	// 2. Hot budget, reduced under memory pressure.
	effHot := m.cfg.MaxHot
	if m.cfg.MemHighMB > 0 && m.cfg.MemMB() >= m.cfg.MemHighMB {
		effHot = m.cfg.PressureHotCap
	}

	// 3. Select the highest-score hot candidates within budget + fairness.
	selected := make(map[string]bool, effHot)
	perTenant := make(map[string]int)
	heap.Init(candidates)
	for candidates.Len() > 0 && len(selected) < effHot {
		e := heap.Pop(candidates).(*entry)
		if m.cfg.HotPerTenant > 0 && perTenant[e.bot.TenantID] >= m.cfg.HotPerTenant {
			continue
		}
		selected[e.bot.KeyID] = true
		perTenant[e.bot.TenantID]++
	}

	// 3b. Spare-capacity fill: idle decay is PRESSURE-driven, not absolute.
	// With hot budget left over, idle bots keep (or get) sockets — closing a
	// healthy socket saves nothing and makes fresh bots (empty channel list,
	// so warm polling covers nothing) deaf on an Idle-period cycle. Highest
	// score first, same fairness cap.
	if len(selected) < effHot {
		spare := make([]*entry, 0, len(m.bots))
		for _, e := range m.bots {
			if !selected[e.bot.KeyID] {
				spare = append(spare, e)
			}
		}
		sort.Slice(spare, func(i, j int) bool { return spare[i].score > spare[j].score })
		for _, e := range spare {
			if len(selected) >= effHot {
				break
			}
			if m.cfg.HotPerTenant > 0 && perTenant[e.bot.TenantID] >= m.cfg.HotPerTenant {
				continue
			}
			selected[e.bot.KeyID] = true
			perTenant[e.bot.TenantID]++
			want[e.bot.KeyID] = types.Hot
		}
	}

	// 4. Reconcile each bot's actual tier with the decision.
	var acts []action
	for _, e := range m.bots {
		target := want[e.bot.KeyID]
		if target == types.Hot && !selected[e.bot.KeyID] {
			target = types.Warm // wanted hot but no slot/fairness
		}
		prev := e.tier
		if target == types.Hot {
			e.tier = types.Hot
			if prev != types.Hot {
				acts = append(acts, action{kind: actOpenHot, bot: e.bot})
			}
		} else {
			if prev == types.Hot {
				acts = append(acts, action{kind: actCloseHot, keyID: e.bot.KeyID})
			}
			e.tier = target // Warm or Cold
		}

		// 5. Schedule due polls for non-hot bots.
		if e.tier != types.Hot && !now.Before(e.nextPoll) {
			interval := m.cfg.WarmPoll
			if e.tier == types.Cold {
				interval = m.cfg.ColdPoll
			}
			e.nextPoll = now.Add(interval)
			acts = append(acts, action{kind: actPoll, bot: e.bot})
		}
	}
	return acts
}

func (m *Manager) apply(acts []action) {
	for _, a := range acts {
		switch a.kind {
		case actOpenHot:
			m.act.OpenHot(a.bot)
		case actCloseHot:
			m.act.CloseHot(a.keyID)
		case actPoll:
			m.act.Poll(a.bot)
		}
	}
}

// Rebalance runs one rebalance pass now (used by tests; Run does this on a tick).
func (m *Manager) Rebalance() {
	now := m.cfg.Now()
	m.mu.Lock()
	acts := m.rebalanceLocked(now)
	m.mu.Unlock()
	m.apply(acts)
}

// ForceTier lets the engine demote a bot (e.g. when a hot dial fails).
func (m *Manager) ForceTier(keyID string, t types.Tier) {
	m.mu.Lock()
	if e, ok := m.bots[keyID]; ok {
		e.tier = t
	}
	m.mu.Unlock()
}
