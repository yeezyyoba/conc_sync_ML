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
