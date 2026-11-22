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
