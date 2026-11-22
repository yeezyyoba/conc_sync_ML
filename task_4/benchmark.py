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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=3)
    args = ap.parse_args()

    rows = {}
    for cache_mode, max_batch in CONFIGS:
        runs = []
        for rep in range(args.repeats):
            r = run_config(cache_mode, max_batch, seed=500 + rep)   # same seeds per config
            runs.append(r)
            print(f"{cache_mode:<6} batch={max_batch}  rep {rep + 1}: "
                  f"{r['throughput_rps']:7.1f} req/s  avg {r['latency_avg_ms']:6.1f} ms  "
                  f"p95 {r['latency_p95_ms']:6.1f} ms  hit {r['cache_hit_rate']:.2f}  "
                  f"util {r['batch_utilization']:.2f}")
        runs.sort(key=lambda x: x["throughput_rps"])
        med = runs[len(runs) // 2]
        med["throughput_spread"] = [round(x["throughput_rps"], 1) for x in runs]
        med["all_clean"] = all(x["clean"] for x in runs)
        rows[f"{cache_mode}/b{max_batch}"] = med

    def line(key, label):
        r = rows[key]
        return (f"| {label} | {r['throughput_rps']:.1f} | {r['latency_avg_ms']:.1f} | "
                f"{r['latency_p95_ms']:.1f} | {r['cache_hit_rate']:.2f} | "
                f"{r['mean_batch_size']:.2f} / {r['batch_utilization']:.2f} | "
                f"{'yes' if r['all_clean'] else 'NO'} |")

    header = ("| Configuration | Throughput (req/s) | Avg latency (ms) | p95 latency (ms) | "
              "Cache hit rate | Mean batch / utilization | Clean (no lost) |\n"
              "|---|---|---|---|---|---|---|")
    md = [
        "# Task 4 benchmark results",
        "",
        f"Workload: {WORKLOAD['n_clients']} closed-loop clients x {WORKLOAD['n_requests']} requests, "
        f"{int(WORKLOAD['hot_ratio'] * 100)}% hot prompts, bursty arrivals. "
        f"embed() {PIPELINE['embed_s'] * 1000:.0f} ms, forward pass "
        f"{PIPELINE['forward_base_s'] * 1000:.0f} ms + {PIPELINE['forward_per_item_s'] * 1000:.0f} ms/item, "
        f"{PIPELINE['n_workers']} workers, batch timeout {PIPELINE['batch_timeout_s'] * 1000:.0f} ms. "
        f"Median of {args.repeats} runs.",
        "",
        "## Comparison 1: coarse vs scoped locking on the LRU cache (batch = 8)",
        "", header, line("coarse/b8", "Coarse lock"), line("scoped/b8", "Scoped lock"), "",
        "## Comparison 2: unbatched vs dynamically batched (scoped cache)",
        "", header, line("scoped/b1", "Unbatched (max_batch_size=1)"),
        line("scoped/b8", "Batched (max_batch_size=8)"), "",
        "## Full grid",
        "", header,
        line("coarse/b1", "coarse / batch 1"), line("coarse/b8", "coarse / batch 8"),
        line("scoped/b1", "scoped / batch 1"), line("scoped/b8", "scoped / batch 8"), "",
        "Throughput of each repeat (sorted): " +
        "; ".join(f"{k}: {v['throughput_spread']}" for k, v in rows.items()),
    ]
    os.makedirs(RESULTS, exist_ok=True)
    with open(os.path.join(RESULTS, "benchmark_results.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")
    slim = {k: {kk: (round(vv, 3) if isinstance(vv, float) else vv) for kk, vv in v.items()}
            for k, v in rows.items()}
    with open(os.path.join(RESULTS, "benchmark_results.json"), "w", encoding="utf-8") as f:
        json.dump(slim, f, indent=2)
    print("\n" + "\n".join(md))
    make_chart(rows)
