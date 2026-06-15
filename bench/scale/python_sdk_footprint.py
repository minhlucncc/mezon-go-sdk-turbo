#!/usr/bin/env python3
"""Measure local Python mezon-sdk footprint for N idle bot clients.

This intentionally does not call login() or open real Mezon sockets. It captures
the constructor/runtime footprint, including the SDK's per-client SQLite
MessageDB object and cache managers, without needing credentials or network.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import resource
import sys
import threading
import time
import tracemalloc
from pathlib import Path


def rss_kib() -> int:
    try:
        with os.popen(f"ps -o rss= -p {os.getpid()}") as f:
            return int((f.read() or "0").strip() or "0")
    except Exception:
        return 0


def max_rss_kib() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return int(value / 1024)
    return int(value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bots", type=int, required=True)
    parser.add_argument("--hold-ms", type=int, default=250)
    args = parser.parse_args()

    cache_dir = Path.cwd() / ".scale-cache"
    cache_dir.mkdir(exist_ok=True)
    os.chdir(cache_dir)

    tracemalloc.start()
    import mezon  # noqa: PLC0415

    gc.collect()
    before_rss = rss_kib()
    before_heap, _ = tracemalloc.get_traced_memory()
    start = time.perf_counter()

    clients = [
        mezon.MezonClient(
            client_id=184000000000 + i,
            api_key=f"token-{i}",
            host="127.0.0.1",
            enable_logging=False,
        )
        for i in range(args.bots)
    ]

    elapsed_ms = (time.perf_counter() - start) * 1000
    gc.collect()
    if args.hold_ms > 0:
        time.sleep(args.hold_ms / 1000)
    heap_current, heap_peak = tracemalloc.get_traced_memory()

    print(
        json.dumps(
            {
                "sdk": "python-mezon-sdk",
                "version": getattr(mezon, "__version__", "unknown"),
                "mode": "construct-idle",
                "bots": args.bots,
                "elapsed_ms": elapsed_ms,
                "rss_kib": rss_kib(),
                "rss_delta_kib": max(0, rss_kib() - before_rss),
                "max_rss_kib": max_rss_kib(),
                "heap_bytes": heap_current,
                "heap_delta_bytes": max(0, heap_current - before_heap),
                "heap_peak_bytes": heap_peak,
                "threads": threading.active_count(),
                "objects_kept": len(clients),
                "notes": "Constructor only; no login or websocket. Includes MezonClient cache managers and SQLite MessageDB objects.",
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
