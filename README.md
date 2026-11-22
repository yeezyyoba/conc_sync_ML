# Concurrent ML Inference Server

A concurrent ML inference server built from scratch in Python with `threading.Lock` and
`threading.Condition` only. No `queue.Queue`, no `functools.lru_cache`, no `collections.OrderedDict`.
iCog Labs training task: Concurrency & Synchronization in AI/ML.

Requirements: Python 3.9+ and only the standard library (`matplotlib` is optional, used for one chart).

```
conc_sync/
├── task_1/   Broken baseline server: expose real race conditions
├── task_2/   O(1) thread-safe LRU embedding cache (coarse vs scoped locking)
├── task_3/   Bounded dynamic inference batching queue (Lock + Condition)
└── task_4/   Integrated pipeline: stress test, deadlock demo, benchmarks
```
