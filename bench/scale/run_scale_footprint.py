#!/usr/bin/env python3
"""Run SDK footprint comparison for 1, 10, and 100 idle bots."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCALE_DIR = ROOT / "bench" / "scale"
OUT_DIR = ROOT / "bench" / "results"
COUNTS = [1, 10, 100]


def run_json(cmd: list[str], cwd: Path) -> dict:
    proc = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=True)
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"no JSON output from {' '.join(cmd)}\nstderr={proc.stderr}")
    return json.loads(lines[-1])


def per_bot(value: float, bots: int) -> float:
    return value / bots if bots else 0


def markdown(rows: list[dict]) -> str:
    lines = [
        "# SDK Scale Footprint",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "Mode: construct-idle. No helper opens real Mezon sockets or logs into production.",
        "",
        "| SDK | Bots | RSS MiB | RSS delta MiB | Heap MiB | Heap delta MiB | Goroutines/Threads | Startup ms | Notes |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for r in rows:
        concurrency = r.get("goroutines", r.get("threads", 0))
        lines.append(
            "| {sdk} | {bots} | {rss:.2f} | {rss_delta:.2f} | {heap:.2f} | {heap_delta:.2f} | {conc} | {elapsed:.2f} | {notes} |".format(
                sdk=r["sdk"],
                bots=r["bots"],
                rss=r["rss_kib"] / 1024,
                rss_delta=r["rss_delta_kib"] / 1024,
                heap=r["heap_bytes"] / 1024 / 1024,
                heap_delta=r["heap_delta_bytes"] / 1024 / 1024,
                conc=concurrency,
                elapsed=r["elapsed_ms"],
                notes=r["notes"].replace("|", "/"),
            )
        )
    lines.extend(
        [
            "",
            "## Per-Bot Delta",
            "",
            "| SDK | Bots | RSS delta KiB/bot | Heap delta KiB/bot |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for r in rows:
        lines.append(
            f"| {r['sdk']} | {r['bots']} | {per_bot(r['rss_delta_kib'], r['bots']):.1f} | {per_bot(r['heap_delta_bytes'] / 1024, r['bots']):.1f} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- `python-mezon-sdk` measures `MezonClient(...)` construction, including cache managers and SQLite `MessageDB` objects. It does not call `login()`.",
            "- `go-sdk` measures the generated REST API client stack used by `github.com/nccasia/mezon-go-sdk`; the published SDK hardcodes `api.mezon.ai`, so local fake connected sockets are not benchmarkable without patching the dependency.",
            "- `go-turbo-sdk` measures one turbo engine with N registered cold bots, Redis-backed state client, tier manager, poller, and ping wheel objects. Sockets are not opened.",
            "- Use this as a baseline footprint comparison. For live socket scale, run against a staging Mezon gateway with real bot tokens or patch the standard Go SDK to accept an API/WS host override.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    go_bin = OUT_DIR / "footprint_go"
    subprocess.run(
        ["/opt/homebrew/bin/go", "build", "-o", str(go_bin), "./bench/scale/footprint_go.go"],
        cwd=ROOT,
        check=True,
    )

    rows: list[dict] = []
    for bots in COUNTS:
        rows.append(
            run_json(
                [
                    "uv",
                    "run",
                    "--project",
                    str(ROOT.parents[1] / "apps" / "worker-mezon"),
                    "python",
                    str(SCALE_DIR / "python_sdk_footprint.py"),
                    "--bots",
                    str(bots),
                ],
                ROOT,
            )
        )
        rows.append(run_json([str(go_bin), "--sdk", "go", "--bots", str(bots)], ROOT))
        rows.append(run_json([str(go_bin), "--sdk", "turbo", "--bots", str(bots)], ROOT))

    raw_path = OUT_DIR / "scale_footprint.json"
    md_path = OUT_DIR / "scale_footprint.md"
    raw_path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")
    md_path.write_text(markdown(rows))
    print(md_path)
    print(raw_path)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(exc.stdout or "", file=sys.stderr)
        print(exc.stderr or "", file=sys.stderr)
        raise
