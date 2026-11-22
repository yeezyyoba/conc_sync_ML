"""
Task 4 - Reliability stress test for the full pipeline
======================================================

Heavy multi-threaded load: 36 clients, mixed hit/miss traffic, bursty
arrivals, and a mid-run shutdown. Repeated for every combination of cache
locking mode and batch size so each code path is exercised.

For every run it verifies:
    * no deadlock: all clients and workers finish (watchdog dumps stacks if not)
    * accepted == completed + rejected_on_shutdown   (no lost requests)
    * client-side view matches server-side counters
    * no request completed twice
    * LRU cache verify_integrity() passes, size <= capacity
    * queue never exceeded its capacity
    * zero lock-order violations (RankedLock would raise)

Run:  python stress_test.py      (report -> results/stress_report.txt)
"""

import faulthandler
import os
import sys
import time

from lock_hierarchy import HIERARCHY, RankedLock
from pipeline import InferencePipeline
from workload import run_clients

HERE = os.path.dirname(os.path.abspath(__file__))
out = []


def say(s=""):
    print(s)
    out.append(s)


def one_run(cache_mode, max_batch, shutdown_at, seed):
    p = InferencePipeline(cache_mode=cache_mode, max_batch_size=max_batch,
                          batch_timeout_s=0.005, n_workers=4, queue_capacity=32,
                          cache_capacity=64)
    # Watchdog: if anything hangs for 90 s, dump every thread's stack and exit.
    faulthandler.dump_traceback_later(90, exit=True)
    c = run_clients(p, n_clients=36, n_requests=60, hot_ratio=0.6, seed=seed,
                    shutdown_at=shutdown_at)
    if shutdown_at is None:
        c["shutdown_clean"] = p.shutdown()
    faulthandler.cancel_dump_traceback_later()
    r = p.report(c["wall_s"])

    integrity_ok = True
    try:
        p.cache.verify_integrity()
    except AssertionError as exc:
        integrity_ok = False
        say(f"    integrity error: {exc}")

    checks = {
        "no deadlock / hung threads": not c["hung_clients"] and c["shutdown_clean"],
        "accepted == completed + rejected_on_shutdown":
            r["accepted"] == r["completed"] + r["rejected_on_shutdown"],
        "client view == server view":
            c["ok"] == r["completed"] and c["rejected"] == r["rejected_on_shutdown"],
        "no timeouts / failures": c["timeouts"] == 0 and r["failed"] == 0 and c["errors"] == 0,
        "no duplicate completions": r["duplicate_completions"] == 0,
        "cache integrity + size <= capacity": integrity_ok and len(p.cache) <= p.cache.capacity,
        "queue size <= capacity": r["queue_max_size_seen"] <= r["queue_capacity"],
    }
    return r, c, checks


def main():
    say("Task 4 stress test - full pipeline under heavy load")
    say("Lock hierarchy (acquire only in increasing rank; scopes kept disjoint):")
    for rank, name, what in HIERARCHY:
        say(f"  rank {rank:<3} {name:<13} {what}")
    say("Load: 36 clients x 60 requests, 60% hot prompts, bursty, 4 workers, "
        "queue capacity 32, cache capacity 64")

    runs = []
    seed = 100
    for cache_mode in ("coarse", "scoped"):
        for max_batch in (1, 8):
            for shutdown_at in (None, 0.6):
                seed += 1
                runs.append((cache_mode, max_batch, shutdown_at, seed))
