# Task 4: Integration, stress test and benchmark results

## 1. Pipeline

```
Client threads ─► Router ─► ThreadSafeLRUCache ─► DynamicBatchQueue ─► Inference worker pool
 (36 / 32)        (runs on    (task_2, cache_lock)   (task_3, queue_lock)  (4 threads, get_batch,
                 client thread)                                              one forward pass/batch)
                                                                         └─► ResultSlot wakes client
```

## 2. Global lock hierarchy

| Rank | Lock | Protects | Taken by |
|---|---|---|---|
| 10 | `cache_lock` | LRU dict + doubly linked list + in-flight set | Router (client thread) |
| 20 | `queue_lock` | ring buffer, `not_full` / `not_empty` | Router (`put`), workers (`get_batch`) |
| 30 | `metrics_lock` | accepted/completed/rejected, latencies, batch sizes | Router, workers |
| 40 | `result_lock` | one per request: result slot | worker (`set_result`), client (`wait`) |

Rules:
1. If two locks are ever nested, take them in increasing rank (`cache_lock` before `queue_lock`). This breaks the circular-wait Coffman condition.
2. In practice scopes are **disjoint**: the router releases `cache_lock` before `put()`, and workers release `queue_lock` before the forward pass, metrics and results. No thread holds two locks, so hold-and-wait never happens either.
3. Every lock is a `RankedLock` (`lock_hierarchy.py`) that checks the rule at runtime and raises `LockOrderViolation` on a wrong-order acquire. The stress test reports 0 violations.
4. Slow work (`embed()`, forward pass) runs with no lock held, except in coarse cache mode, which is the deliberate baseline.

`deadlock_demo.py` shows the cache-vs-queue circular wait from the training: it deadlocks with opposite lock orders and runs cleanly with ordering or disjoint scopes.

## 3. Reliability (`stress_test.py` → `results/stress_report.txt`)

36 clients × 60 requests, 60% hot prompts, bursty arrivals, queue capacity 32, cache capacity 64. Run in all 8 combinations of {coarse, scoped} × {batch 1, batch 8} × {full run, shutdown at 0.6 s}.

| Check | Result |
|---|---|
| Deadlocks / hung threads | 0 in all 8 runs (faulthandler watchdog would dump stacks) |
| `accepted == completed + rejected_on_shutdown` | holds in all 8 runs, lost = 0 |
| Client-side counts == server-side counts | yes |
| Duplicate completions | 0 |
| `verify_integrity()` on the cache after load | pass, size ≤ capacity |
| Queue max size ≤ capacity | yes (max 32/32) |
| Lock-order violations | 0 |

## 4. Benchmarks (`benchmark.py` → `results/benchmark_results.md`, `benchmark_chart.png`)

Workload: 32 closed-loop clients × 40 requests, 70% hot prompts, bursty. `embed()` = 5 ms, forward pass = 20 ms + 1 ms per item, 4 workers, batch timeout 5 ms. Median of 3 runs.

| Configuration | Throughput (req/s) | Avg latency (ms) | p95 latency (ms) | Cache hit rate | Mean batch | Batch utilization |
|---|---|---|---|---|---|---|
| coarse lock, batch 1 | 188.1 | 166.9 | 170.1 | 0.68 | 1.00 | 1.00 |
| coarse lock, batch 8 | 553.6 | 55.0 | 82.1 | 0.69 | 4.10 | 0.51 |
| scoped lock, batch 1 | 187.5 | 167.1 | 172.2 | 0.68 | 1.00 | 1.00 |
| **scoped lock, batch 8** | **882.3** | **32.7** | **43.3** | 0.71 | 6.70 | 0.84 |

### Comparison 1: coarse vs scoped locking (batch 8)

Scoped locking gives **+59% throughput** (554 → 882 req/s), **−41% average latency** (55 → 33 ms) and **−47% p95** (82 → 43 ms).

Why: about 30% of requests miss, so coarse mode holds `cache_lock` for 5 ms per miss. At ~550 req/s that is ~165 misses/s × 5 ms ≈ 0.83 s of lock time every second, so the cache lock is ~83% busy and becomes the bottleneck. Every request queues behind it, **cache hits included**, even though a hit needs only microseconds. Requests then reach the batch queue one at a time, so batches stay small (mean 4.1 of 8, utilization 0.51) and the GPU's fixed 20 ms cost is spread over fewer items. Scoped mode releases the lock during `embed()`: misses compute in parallel, hits never wait, batches fill up (6.7 of 8), and the tail latency halves.
