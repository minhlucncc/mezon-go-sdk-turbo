# SDK Scale Footprint

Generated: 2026-06-05T22:50:32.925856+00:00

Mode: construct-idle. No helper opens real Mezon sockets or logs into production.

| SDK | Bots | RSS MiB | RSS delta MiB | Heap MiB | Heap delta MiB | Goroutines/Threads | Startup ms | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| python-mezon-sdk | 1 | 86.98 | 0.00 | 28.00 | 0.01 | 1 | 0.40 | Constructor only; no login or websocket. Includes MezonClient cache managers and SQLite MessageDB objects. |
| go-sdk | 1 | 12.25 | 1.34 | 0.56 | 0.00 | 1 | 0.00 | Generated REST API client skeleton only; NewClient/CreateSocket require real api.mezon.ai auth and websocket. |
| go-turbo-sdk | 1 | 12.58 | 1.77 | 0.58 | 0.02 | 2 | 0.55 | Turbo engine plus Redis-backed state client and registered cold bots; no sockets opened. |
| python-mezon-sdk | 10 | 85.89 | 0.11 | 28.08 | 0.09 | 1 | 1.20 | Constructor only; no login or websocket. Includes MezonClient cache managers and SQLite MessageDB objects. |
| go-sdk | 10 | 12.19 | 1.34 | 0.56 | 0.00 | 1 | 0.01 | Generated REST API client skeleton only; NewClient/CreateSocket require real api.mezon.ai auth and websocket. |
| go-turbo-sdk | 10 | 12.56 | 1.86 | 0.58 | 0.02 | 2 | 0.24 | Turbo engine plus Redis-backed state client and registered cold bots; no sockets opened. |
| python-mezon-sdk | 100 | 87.73 | 1.28 | 28.80 | 0.81 | 1 | 15.15 | Constructor only; no login or websocket. Includes MezonClient cache managers and SQLite MessageDB objects. |
| go-sdk | 100 | 12.20 | 1.38 | 0.61 | 0.05 | 1 | 0.03 | Generated REST API client skeleton only; NewClient/CreateSocket require real api.mezon.ai auth and websocket. |
| go-turbo-sdk | 100 | 12.56 | 1.77 | 0.60 | 0.05 | 2 | 0.33 | Turbo engine plus Redis-backed state client and registered cold bots; no sockets opened. |

## Per-Bot Delta

| SDK | Bots | RSS delta KiB/bot | Heap delta KiB/bot |
| --- | ---: | ---: | ---: |
| python-mezon-sdk | 1 | 0.0 | 8.1 |
| go-sdk | 1 | 1376.0 | 0.0 |
| go-turbo-sdk | 1 | 1808.0 | 18.6 |
| python-mezon-sdk | 10 | 11.2 | 9.0 |
| go-sdk | 10 | 137.6 | 0.3 |
| go-turbo-sdk | 10 | 190.4 | 2.1 |
| python-mezon-sdk | 100 | 13.1 | 8.3 |
| go-sdk | 100 | 14.1 | 0.5 |
| go-turbo-sdk | 100 | 18.1 | 0.5 |

## Interpretation

- `python-mezon-sdk` measures `MezonClient(...)` construction, including cache managers and SQLite `MessageDB` objects. It does not call `login()`.
- `go-sdk` measures the generated REST API client stack used by `github.com/nccasia/mezon-go-sdk`; the published SDK hardcodes `api.mezon.ai`, so local fake connected sockets are not benchmarkable without patching the dependency.
- `go-turbo-sdk` measures one turbo engine with N registered cold bots, Redis-backed state client, tier manager, poller, and ping wheel objects. Sockets are not opened.
- Use this as a baseline footprint comparison. For live socket scale, run against a staging Mezon gateway with real bot tokens or patch the standard Go SDK to accept an API/WS host override.
