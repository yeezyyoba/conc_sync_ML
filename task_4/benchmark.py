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
