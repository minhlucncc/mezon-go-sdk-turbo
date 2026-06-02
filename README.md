# mezon-go-sdk-turbo

A resource-optimized Mezon client for running **thousands of bots on a single small
machine**. It replaces the official [`mezon-go-sdk`](https://github.com/mezonai/mezon-go-sdk)
connection runtime — which spends **2 goroutines per bot**, does a **full `Envelope`
unmarshal on every frame**, uses **4 KB buffers**, and **hardcodes the host** — with a
lean, tiered engine, while reusing the official SDK's generated protobuf types and
codegen REST client for wire compatibility.

- [Why](#why) · [Features](#features) · [Architecture](#architecture) ·
  [Usage](#usage) · [Configuration](#configuration) · [Scalability](#scalability) ·
  [Build & test](#build--test) · [Shadow validation](#validate-before-production-shadow)

## Why

A Mezon bot only receives messages over a live WebSocket. Holding one socket per bot
is the cost wall: each connection is 2 goroutines + buffers + per-bot heap, and the
server caps connections. But **most bots are idle most of the time** — they don't need
a live socket. `mezon-go-sdk-turbo` keeps live sockets only for the *active* bots and
serves the long idle tail by **polling REST with a cursor**, so the footprint tracks
*active* bots, not *total* bots.

## Features

- **Hot / warm / cold tiering** with a priority queue. Each bot sits in one tier:
  | Tier | Transport | Latency | Cost |
  |------|-----------|---------|------|
  | **Hot** | live lean WebSocket | lowest | 1 socket + small heap; capped by a memory budget |
  | **Warm** | REST poll ~15–30s | seconds | no socket; state in Redis |
  | **Cold** | REST poll ~2–5min | minutes | no socket; ~0 Go heap |
- **Smart resource management.** A manager scores every bot by **activity recency/rate
  + tenant plan**, and assigns hot slots by score under two constraints: a
  **per-tenant fairness cap** (no tenant hogs hot slots) and a **memory watermark**
  that demotes the lowest-priority hot sockets when the heap crosses a threshold.
- **Lean WebSocket client.** 1 KB buffers, a **single shared ping-wheel goroutine** for
  *all* sockets (not one ticker per bot), and a **partial protobuf decoder** that pulls
  only `channel_message` out of a frame and skips the ~60-case `Envelope`.
- **Cursor-based REST poller** with a **global rate budget** (token bucket) so total
  Mezon API load is capped regardless of bot count.
- **Redis-offloaded per-bot state** (dedup set, channel read-cursors, tier) — warm/cold
  bots cost ~0 Go heap; the in-memory index is ~100 B/bot.
- **Configurable host** (the official SDK hardcodes `api.mezon.ai`).
- **Transparent send** — `Send`/`SendTyping` route over the live socket when hot, or a
  transient socket otherwise; the caller never thinks about tiers.

## Architecture

```
                         ┌──────────────────────── turbo.Engine ───────────────────────┐
   backend bot-keys ───▶ │  Register(BotRef)                              OnMessage(cb) │ ──▶ your turn handler
   (supervisor)          │       │                                             ▲        │      (chat pipeline)
                         │       ▼                                             │        │
                         │  ┌─────────┐  promote/demote   ┌───────────────┐    │        │
                         │  │  tier/  │ ◀───────────────▶ │  state/ (Redis)│   │        │
                         │  │ manager │  score+heap       │ dedup·cursor·  │   │        │
                         │  │ +watermk│  fairness·budget  │ tier·channels  │   │        │
                         │  └────┬────┘                   └───────────────┘    │        │
                         │       │ actuate (Actuator)                          │        │
                         │   ┌───┴───────────────┬───────────────────┐        │        │
                         │   ▼                   ▼                   ▼         │        │
                         │ OpenHot/CloseHot     Poll              (Send)       │        │
                         │   │                   │                             │        │
                         │   ▼                   ▼                             │        │
                         │ ┌──────┐ pingwheel  ┌────────┐ rate budget          │        │
                         │ │ ws/  │──(1 gor.)  │poller/ │──(token bucket)──▶ Mezon REST │
                         │ │ lean │            │ batched│   ListChannelMessages(cursor)  │
                         │ │socket│──partial──▶│        │──────────────────────┘        │
                         │ └──┬───┘   decode   └────────┘                               │
                         └────┼──────────────────────────────────────────────────────┘
                              ▼ Mezon WebSocket (hot bots only)
```

### Layout

Source lives under `lib/`, tests under `tests/` (one black-box `*_test` package
per source package):

```
lib/
  turbo/    Engine — wires everything; implements tier.Actuator; Register/Run/Send/AddChannel
  ws/       lean WebSocket: Dial, PingWheel (shared keepalive), DecodeChannelMessage (partial decode)
  rest/     host-configurable REST: ListSince (cursor poll); satisfies poller.Lister
  poller/   batched cursor polling + global rate budget; emits new messages
  state/    Store (Redis): per-bot dedup, channel cursors, channel set, activity, tier
  tier/     Manager — priority scoring, hot budget, fairness, memory watermark, poll scheduling
  types/    leaf BotRef, Message, Tier (no inter-package cycles)
tests/
  turbo/ ws/ rest/ poller/ state/ tier/    black-box tests + benchmarks
```

Import paths are `github.com/mezon/mezon-go-sdk-turbo/lib/<pkg>`.

### Inbound flow

- **Hot bot:** socket frame → `ws` partial decode → `Engine.onHotMessage` → dedup
  (Redis) → `deliver` → `Touch` (bump priority) → `OnMessage` callback.
- **Warm/cold bot:** manager schedules a poll → `poller` `ListSince(cursor)` → dedup
  → advance cursor → `Engine.onPollMessage` → `deliver` → **`Touch` promotes the bot
  toward Hot**, so the reply and the rest of the conversation run on a live socket.

### Outbound flow

`OnMessage` (your handler) calls `Engine.Send(bot, msg, text, asReply)`. If the bot is
hot it goes over the live socket; otherwise a transient lean socket is opened to send
and closed (rare — only when hot slots are saturated). `SendTyping` is a no-op unless hot.

### Tier state machine

```
   activity (Touch)                 activity                 activity
COLD ───────────────▶ WARM ───────────────▶ HOT ◀── (memory watermark / no slot)
   ▲   idle > WarmIdle   ▲   idle > HotIdle    │            demotes lowest-priority
   └─────────────────────┴─────────────────────┘
   Hot is granted to the highest-score bots within MaxHot and HotPerTenant.
```

## Usage

```go
import (
    turbo "github.com/mezon/mezon-go-sdk-turbo/lib/turbo"
    "github.com/mezon/mezon-go-sdk-turbo/lib/rest"
    "github.com/mezon/mezon-go-sdk-turbo/lib/tier"
    "github.com/mezon/mezon-go-sdk-turbo/lib/types"
    "github.com/redis/go-redis/v9"
)

rdb := redis.NewClient(redisOpt)
restClient := rest.New("https://api.mezon.ai") // configurable host

eng := turbo.New(turbo.Config{
    WSHost: "api.mezon.ai", WSSSL: true,
    Tier: tier.Config{
        MaxHot:       200,            // hot-socket budget
        HotPerTenant: 5,              // fairness
        WarmPoll:     20 * time.Second,
        ColdPoll:     3 * time.Minute,
        MemHighMB:    1500,           // demote hot above this heap
        PressureHotCap: 50,
        PlanWeight: map[string]float64{"starter": 1, "pro": 2, "enterprise": 3},
    },
    PollRPS:  50,                     // global REST rate budget
    StateTTL: 30 * 24 * time.Hour,
    DedupCap: 2048,
}, rdb, restClient, func(bot types.BotRef, msg types.Message) {
    // Your turn pipeline. Reply via the engine:
    eng.SendTyping(bot, msg)
    eng.Send(bot, msg, "answer text", !msg.IsDM())
})

// Register bots (from your backend); the manager starts them Cold and promotes on activity.
eng.Register(types.BotRef{
    KeyID: "key-1", BotUserID: "12345", BotToken: "…",
    TenantID: "t-1", WorkspaceID: "w-1", Plan: "pro",
})

eng.Run(ctx) // starts the ping-wheel + tiering loop; blocks until ctx is cancelled
```

### This is a library, not a service

`mezon-go-sdk-turbo` is a **library only** — it has no `main`/`cmd` and is not
deployable on its own. The deployable consumer is **`apps/worker-mezon-go`**: a Go
worker that imports this SDK (via a local `replace`), supplies the backend-bridge
glue (a bot-key supervisor + a chat turn handler wired to `OnMessage`), and is what
actually runs (docker-compose service `worker-mezon-go`, opt-in `mezon-go` profile).

## Configuration

### `turbo.Config`

| Field | Default | Purpose |
|---|---|---|
| `WSHost` / `WSSSL` | — / — | Mezon WebSocket host and TLS (hot tier). |
| `PollRPS` | 50 | Global REST calls/sec budget (token bucket). |
| `PollWorkers` | 8 | Concurrent poll jobs. |
| `PollPageLimit` | 20 | Messages fetched per channel per poll. |
| `StateTTL` | 30d | TTL on Redis bot state. |
| `DedupCap` | 2048 | Remembered message ids per bot. |
| `PingInterval` | 10s | Shared ping-wheel cadence. |

### `tier.Config`

| Field | Default | Purpose |
|---|---|---|
| `MaxHot` | 200 | Hot-socket cap (memory budget). |
| `PressureHotCap` | — | Hot cap while above the memory watermark. |
| `HotPerTenant` | 0 (∞) | Per-tenant fairness cap on hot slots. |
| `WarmPoll` / `ColdPoll` | 20s / 3m | Poll cadence per tier. |
| `HotIdle` / `WarmIdle` | 2m / 10m | Idle thresholds for demotion. |
| `Tick` | 2s | Rebalance cadence. |
| `PlanWeight` | starter1 pro2 enterprise3 | Priority weight by tenant plan. |
| `MemHighMB` | 0 (off) | Heap above this triggers watermark eviction. |
| `Now` / `MemMB` | wall clock / heap | Injectable for tests. |

The reference worker exposes these as env vars: `MEZON_API_BASE`, `MEZON_WS_HOST`,
`MEZON_WS_SSL`, `MEZON_MAX_HOT`, `MEZON_HOT_PER_TENANT`, `MEZON_WARM_POLL`,
`MEZON_COLD_POLL`, `MEZON_REST_RPS`, `MEZON_MEM_HIGH_MB`, `MEZON_PRESSURE_HOT`.

## Scalability

**The cost model.** With the official SDK, footprint ≈ `total_bots × (2 goroutines +
~8 KB buffers + per-bot heap)`. With turbo it's:

```
RAM ≈ hot_bots × (1 socket + small heap)        // capped by MaxHot
    + total_bots × ~100 B in-memory index
    + (warm+cold state lives in Redis, not Go heap)
REST load ≈ min(PollRPS, Σ channels / poll_interval)   // hard-capped
```

So a box sized for, say, **200 hot sockets** can *register* tens of thousands of bots;
the inactive majority sit cold at ~100 B each in Go + a few KB each in Redis, polled
within the global rate budget. Hot membership floats to wherever the activity is.

**Levers:**
- `MaxHot` — the memory budget knob (each hot socket is the dominant per-bot cost).
- `MemHighMB` + `PressureHotCap` — hard ceiling: under heap pressure the manager sheds
  the lowest-priority hot sockets automatically.
- `PollRPS` — caps total Mezon API load; raise for lower warm/cold latency.
- `WarmPoll` / `ColdPoll` — latency vs REST load for the polled tiers.
- `HotPerTenant` — keeps one busy tenant from starving others.

**Decoder win** (`go test -bench=Decode|Unmarshal -benchmem ./ws`):

```
BenchmarkPartialDecode          416 B/op    7 allocs/op   (~0.58 µs/op)
BenchmarkFullEnvelopeUnmarshal  504 B/op   10 allocs/op   (~0.75 µs/op)
```

…on a channel-message frame. For the **common** traffic (pings, pongs, presence,
typing, reactions) the partial decoder skips the frame at near-zero allocations, while
the official SDK unmarshals the whole `Envelope` every time — so the real-world CPU/GC
gap is much wider than the table above.

**Goroutines:** official SDK = `2 × bots`; turbo = `~1 × hot_bots` (read loops) + 1
ping-wheel + `PollWorkers` + the tiering loop. At 10k registered bots with 200 hot,
that's ~200 + a handful, versus ~20,000.

## Build & test

Requires **Go 1.24**.

```bash
go build ./lib/... && go vet ./... && go test ./...
go test -bench=. -benchmem ./tests/ws   # decoder benchmark
```

State and poller tests run against an in-process Redis (`miniredis`); no external
services needed.

## Validate before production (shadow)

Verified offline (build/vet/test/bench). These depend on a live Mezon endpoint and
should be confirmed in the shadow phase:

- Whether the bot token is the WS/REST token directly, or needs `MezonAuthenticate` →
  `ApiSession` exchange (the `rest` client accepts a token per call, so either fits).
- The real WS host / TLS for the target environment.
- **Cold-bot channel enumeration.** Channels are learned from inbound messages today;
  a never-active cold bot has nothing to poll until seeded — wire its channels from
  `tenant_clans.indexed_channel_ids` if needed.
- Exact `ListChannelMessages` cursor / `Direction` semantics.
- Clan webhooks as a future, lower-latency cold path (push instead of poll).
