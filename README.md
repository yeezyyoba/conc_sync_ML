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

## Task 2: O(1) thread-safe LRU embedding cache

```bash
cd task_2
python test_lru_cache.py   # 20 unittest cases: single-threaded + concurrent, both modes
python demo_lru.py         # linked-list walkthrough + quick coarse vs scoped timing
```

* `lru_cache.py`: `ThreadSafeLRUCache`, a doubly linked list with HEAD/TAIL sentinels plus a dict, so get/put/evict are O(1)
  * `mode="coarse"` holds the lock across `embed()`; `mode="scoped"` releases it during `embed()` and re-checks after
  * `single_flight=True` (scoped): concurrent misses on the same key wait instead of embedding twice
  * `verify_integrity()` checks prev/next symmetry, cycles, list length == dict size, `0 <= size <= capacity`
* `test_lru_cache.py`: hit-heavy, miss-heavy and eviction-heavy concurrent tests with a live integrity checker
* `results/`: test log and demo output

## Task 3: Bounded dynamic inference batching queue

```bash
cd task_3
python test_batch_queue.py   # 15 unittest cases: backpressure, batching, deadline, shutdown
python stress_harness.py     # steady load, mid-run shutdown, idle-CPU busy-spin check
```

* `batch_queue.py`: `DynamicBatchQueue`, a ring buffer plus one `Lock` shared by two `Condition`s (`not_full`, `not_empty`)
  * `put()` blocks in `while count >= capacity` (backpressure); after `shutdown()` it raises `QueueShutdown`
  * `get_batch(max_batch_size, timeout_s)` returns a full batch, a partial batch after the monotonic deadline, or drains on shutdown
  * `shutdown()` calls `notify_all()` on both conditions, drains the remaining items, and leaves no thread hanging
* `stress_harness.py`: checks `attempts == accepted + rejected`, `accepted == consumed`, no duplicates, no overflow, no hung threads
* `results/`: test log and stress report
