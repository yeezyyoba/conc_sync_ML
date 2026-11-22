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

## Task 1: Expose the bugs in a broken baseline server

```bash
cd task_1
python stress_test.py           # broken router: every race reproduced
python stress_test.py --fixed   # fixed router: every count is 0
```

* `broken_server.py`: unsynchronized router (dict cache, list queue, telemetry counters); every bug is tagged `# [BUG-n]`
* `fixed_server.py`: same API with each bug fixed, for comparison
* `stress_test.py`: 30 client threads + 4 workers, 3 seeded trials, maps each symptom to its line
* `BUG_REPORT.md`: symptom → exact line → why → fix
