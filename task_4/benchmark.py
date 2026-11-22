"""
Task 4 - Benchmarks
===================

Same closed-loop workload (32 clients x 40 requests, 70% hot prompts,
bursty) against four pipeline configurations:

                     max_batch_size = 1     max_batch_size = 8
    coarse cache     coarse / unbatched     coarse / batched
    scoped cache     scoped / unbatched     scoped / batched

Comparison 1 (cache locking): coarse vs scoped, both with batching (batch=8).
Comparison 2 (batching):      batch=1 vs batch=8, both with scoped locking.

Simulated costs: embed() 5 ms, forward pass 20 ms + 1 ms per item,
4 inference workers, batch timeout 5 ms. Each config runs 3 times; the
median run (by throughput) is reported.

Metrics: throughput (req/s), average and p95 end-to-end latency (ms),
cache hit rate (hits / lookups), batch utilization (mean batch / max batch).

Run:  python benchmark.py [--repeats 3]
Output: results/benchmark_results.md, results/benchmark_results.json,
        results/benchmark_chart.png (if matplotlib is installed)
"""

import argparse
import json
import os

from pipeline import InferencePipeline
from workload import run_clients

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "results")

CONFIGS = [
    ("coarse", 1), ("coarse", 8),
    ("scoped", 1), ("scoped", 8),
]
WORKLOAD = dict(n_clients=32, n_requests=40, hot_ratio=0.7)
PIPELINE = dict(n_workers=4, queue_capacity=64, cache_capacity=256, embed_s=0.005,
                forward_base_s=0.020, forward_per_item_s=0.001, batch_timeout_s=0.005)


def run_config(cache_mode, max_batch, seed):
    p = InferencePipeline(cache_mode=cache_mode, max_batch_size=max_batch, **PIPELINE)
    c = run_clients(p, seed=seed, **WORKLOAD)
    clean = p.shutdown()
    r = p.report(c["wall_s"])
    r["clean"] = clean and not c["hung_clients"] and r["completed"] == r["accepted"]
    return r
