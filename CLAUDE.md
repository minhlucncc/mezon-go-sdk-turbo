# CLAUDE.md — mezon-go-sdk-turbo (Go 1.24)

A **resource-optimized Mezon bot client library** built to run thousands of
mostly-idle bots on one small machine. It replaces the official SDK's connection
runtime (per-bot goroutines, full `Envelope` unmarshal, hardcoded host) while
**reusing the official `nccasia/mezon-go-sdk` generated protobuf types + codegen
REST client** for wire compatibility. **Library only — no `main`/service.** The
deployable consumer is `apps/worker-mezon-go`.

**Specs:** openspec/specs/go-backend/spec.md (support role)  ·  **Rationale:** docs/design/0014-architecture-v2.md  ·  **Workflow:** /DEVELOPMENT.md

Standalone Go module (`github.com/mezon/mezon-go-sdk-turbo`, own go.mod, **go 1.24**,
no go.work). Imported by `apps/worker-mezon-go` via `replace ../../packages/mezon-go-sdk-turbo`.

## Layout
| Path | What |
|---|---|
| `lib/turbo/turbo.go` | Top-level **`Engine`** wiring ws+poller+state+tier. `New`, `Register`/`Deregister`, `AddChannel`, `Run`, `OpenHot`/`CloseHot`/`Poll`, `Send`/`SendTo`/`SendTyping`. Implements `tier.Actuator`. |
| `lib/tier/manager.go` | Resource `Manager`: priority scoring (activity recency/rate + tenant plan weight), hot-socket budget (`MaxHot`), per-tenant fairness (`HotPerTenant`), memory-watermark demotion (`MemHighMB`/`PressureHotCap`). |
| `lib/tier/pq.go` | `scoreHeap` max-heap (`container/heap`) selecting highest-priority bots into scarce hot slots. |
| `lib/ws/conn.go` | Lean WebSocket client (1 KB buffers, shared write-buffer pool). |
| `lib/ws/decode.go` | **Partial protobuf decoder** — extracts only `channel_message` (Envelope field 6), skips the ~60-field Envelope (~1.5x faster than full unmarshal). |
| `lib/ws/wire.go` | Hand-encoded outgoing envelopes via `protowire` against the **live** server schema (codegen v0.0.34 drifted: int64 ids, shifted fields). |
| `lib/ws/content.go` | `BuildContent` — URLs→Mezon "lk" link entities, markdown flattening, **UTF-16 offset** mk/lk spans. |
| `lib/ws/pingwheel.go` | `PingWheel` — single shared goroutine keeping all hot connections alive (vs one ticker per bot). |
| `lib/rest/{rest,clans}.go` | Host-configurable wrapper over the official codegen REST client; `Authenticate` (API-key→session), `ListSince` (cursor poll, satisfies `poller.Lister`), `ListClanIDs`. |
| `lib/poller/poller.go` | Warm/cold cursor-based REST polling + dedup + cursor advance; all calls through one global token bucket (`PollRPS`). |
| `lib/state/state.go` | Redis-offloaded per-bot state: dedup set, channel cursors, channel set, activity counters, tier. |
| `lib/types/types.go` | Leaf shared types (`BotRef`, `Message`, `Tier` enum) so subpackages never import each other (no cycles). |
| `bench/` | Scale-footprint probes (`run_scale_footprint.py`, `footprint_go.go`, `python_sdk_footprint.py`) → `bench/results/scale_footprint.{md,json}`. |
| `tests/` | Black-box `*_test` packages + microbenchmarks + wire goldens. |

## Commands
```bash
# from this module dir (packages/mezon-go-sdk-turbo)
go build ./... && go vet ./... && go test -race ./...
# coverage
go test -race -coverprofile=cover.out ./... && go tool cover -func=cover.out | tail -1
# microbenchmarks
go test -bench=. -benchmem ./tests/...
# scale footprint (Python vs go-sdk vs turbo)
python3 bench/scale/run_scale_footprint.py
```
CI tests this module as part of the **go-worker** job (alongside its consumer
`apps/worker-mezon-go`). Toolchain pin **go 1.24**.

## Tests
Black-box `package <pkg>_test` under `tests/` (none under `lib/` or `bench/`):
- `tests/ws/` — `decode_test.go`, `content_test.go`, `send_test.go`, and
  `wire_golden_test.go` (golden inbound decode + outbound encode against a
  **live-captured frame**, `tests/ws/testdata/{live_channel_message.b64,wire_golden.json}`).
- `tests/turbo/` — `turbo_test.go`, `lifecycle_test.go` (connection lifecycle,
  reconnect after server close, deregister-does-not-redial). **These are the
  protocol-drift guards** the worker's BEHAVIOR_PARITY relies on.
- `tests/tier/` — 10 manager tests (fairness, idle demotion, memory eviction, promotion).
- `tests/poller/`, `tests/rest/`, `tests/state/` — polling/dedup, auth+clan listing, Redis state.
- `*_bench_test.go` per package; Redis tests use `miniredis` (regression guards, not prod latency).

## Benchmarks
Microbenchmarks in `tests/*/*_bench_test.go` (PartialDecode vs FullEnvelopeUnmarshal,
BuildContent, Redis state round-trips, `RebalanceTenThousandBots`). Cross-SDK scale
footprint (construct-idle RSS/heap/goroutines for python-sdk / go-sdk / turbo at
1/10/100 bots) via `bench/scale/run_scale_footprint.py`. See BENCHMARKS.md.

## Invariants & patterns (module-specific)
- **Wire compatibility is golden-locked.** The codegen types (v0.0.34) drifted from
  the live server (int64 ids vs strings, shifted ChannelMessage fields), so inbound
  decode and outbound encode are **hand-rolled with `protowire`** and pinned by
  `tests/ws/wire_golden_test.go`. Never trust codegen field layout — change wire code
  only with a regenerated golden.
- **No import cycles:** subpackages depend only on `lib/types`; keep new shared types there.
- **Hot/Warm/Cold tiering** is the whole point — hot = live lean socket (capped),
  warm/cold = REST poll with state in Redis. Respect the hot budget + per-tenant
  fairness + memory watermark when touching `lib/tier`.
- **mk/lk spans are UTF-16 offset-based** (shared concern with worker-mezon-go's format port).
- Library only — do not add a `cmd/`/`main`; the consumer supplies the supervisor + turn handler.

## Where to look first
- `lib/turbo/turbo.go` — the `Engine` entrypoint; everything else hangs off it.
- `lib/ws/wire.go` + `decode.go` + the wire goldens — where protocol drift bites.
- `lib/tier/manager.go` — the scarce-hot-slot scheduling logic.
- README.md (~18 KB) has the full API + config (`turbo.Config`, `tier.Config`) reference.
