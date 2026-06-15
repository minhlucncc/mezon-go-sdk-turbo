# Benchmarks

Command:

```bash
go test -bench=. -benchmem ./tests/...
```

Environment:

- `goos`: `darwin`
- `goarch`: `amd64`
- CPU: `VirtualApple @ 2.50GHz`

Results:

| Package | Benchmark | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| `tests/ws` | `BenchmarkPartialDecode` | 388.7 | 416 | 7 |
| `tests/ws` | `BenchmarkFullEnvelopeUnmarshal` | 581.9 | 504 | 10 |
| `tests/ws` | `BenchmarkBuildContentPlain` | 6,255 | 621 | 10 |
| `tests/ws` | `BenchmarkBuildContentRich` | 8,690 | 2,644 | 35 |
| `tests/state` | `BenchmarkRedisSeenNew` | 585,485 | 137,846 | 95 |
| `tests/state` | `BenchmarkRedisSeenDuplicate` | 36,750 | 1,058 | 37 |
| `tests/state` | `BenchmarkRedisStateRoundTrip` | 206,406 | 5,477 | 249 |
| `tests/poller` | `BenchmarkPollOnceDedupedPage` | 887,213 | 24,660 | 905 |
| `tests/tier` | `BenchmarkRebalanceTenThousandBots` | 2,412,407 | 614,515 | 190 |

Interpretation:

- Partial frame decode is about 1.5x faster than full `Envelope` unmarshal on
  channel-message frames, with fewer bytes and allocations.
- URL/link content generation stays under 10 us for rich reply text on this
  machine.
- Tier rebalance over 10,000 registered bots completes in about 2.5 ms.
- Redis benchmarks use `miniredis`; they are regression guards for command
  shape and cache behavior, not production Redis latency predictions.

## SDK scale footprint

Command:

```bash
python3 bench/scale/run_scale_footprint.py
```

Generated artifacts:

- `bench/results/scale_footprint.md`
- `bench/results/scale_footprint.json`

Latest local construct-idle results:

| SDK | 1 bot RSS | 10 bots RSS | 100 bots RSS | 100 bots heap |
|---|---:|---:|---:|---:|
| `python-mezon-sdk` | 86.98 MiB | 85.89 MiB | 87.73 MiB | 28.80 MiB |
| `go-sdk` | 12.25 MiB | 12.19 MiB | 12.20 MiB | 0.61 MiB |
| `go-turbo-sdk` | 12.58 MiB | 12.56 MiB | 12.56 MiB | 0.60 MiB |

Notes:

- This is a no-network baseline: no helper logs into Mezon or opens production
  sockets.
- Python measures `mezon-sdk==1.8.1` `MezonClient(...)` construction, including
  cache managers and SQLite `MessageDB` objects.
- Standard Go measures the generated REST API client stack in
  `github.com/nccasia/mezon-go-sdk@v0.0.34`; its published constructor and
  socket path hardcode `api.mezon.ai`, so local fake connected sockets require
  patching the dependency.
- Turbo measures one engine with N registered cold bots, Redis-backed state
  client, tier manager, poller, and ping wheel objects. It does not open hot
  sockets in this benchmark.
