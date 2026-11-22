"""
Task 4 - Global lock hierarchy (deadlock prevention)
====================================================

A deadlock needs all four Coffman conditions at once:
    1. mutual exclusion   - a lock is held by one thread at a time
    2. hold and wait      - a thread holds one lock while waiting for another
    3. no preemption      - a lock cannot be taken away from its holder
    4. circular wait      - T1 waits for T2's lock while T2 waits for T1's lock

We break condition 4. Every lock in the pipeline gets a RANK, and a thread
may only acquire locks in strictly increasing rank order:

    rank 10  CACHE_LOCK    LRU cache map + linked list        (task_2)
    rank 20  QUEUE_LOCK    DynamicBatchQueue buffer + conds   (task_3)
    rank 30  METRICS_LOCK  pipeline telemetry counters
    rank 40  RESULT_LOCK   per-request result slot

If every thread climbs the ranks in the same direction, a cycle of waiting
threads is impossible: the thread holding the highest-ranked lock in any
would-be cycle can always proceed.

In the actual pipeline the scopes are also DISJOINT: no code path holds two
of these locks at once (the router releases the cache lock before touching
the queue, workers release the queue lock before recording metrics). The
ranks are a second line of defence: RankedLock checks the order at runtime
and raises LockOrderViolation immediately, instead of letting a future code
change hang the server.

RankedLock is a drop-in Lock: it works with `with`, acquire/release, and as
the underlying lock of threading.Condition (wait() releases and re-acquires
it through the same checked path).
"""

import threading

CACHE_LOCK = 10
QUEUE_LOCK = 20
METRICS_LOCK = 30
RESULT_LOCK = 40

HIERARCHY = [
    (CACHE_LOCK, "cache_lock", "LRU embedding cache (map + doubly linked list)"),
    (QUEUE_LOCK, "queue_lock", "DynamicBatchQueue ring buffer + not_full/not_empty"),
    (METRICS_LOCK, "metrics_lock", "pipeline counters, latencies, batch sizes"),
    (RESULT_LOCK, "result_lock", "per-request result slot (client wake-up)"),
]

_held = threading.local()           # per-thread stack of held RankedLocks


class LockOrderViolation(RuntimeError):
    """Raised when a thread acquires locks out of hierarchy order."""


def held_locks():
    """Names of the RankedLocks the current thread holds (outermost first)."""
    return [lk.name for lk in getattr(_held, "stack", [])]


class RankedLock:
    violations = 0                   # global counter, for the stress test report
    _count_lock = threading.Lock()

    def __init__(self, rank, name):
        self.rank = rank
        self.name = name
        self._lock = threading.Lock()
        self._owner = None

    def _check(self):
        stack = getattr(_held, "stack", None)
        if stack:
            top = stack[-1]
            if self.rank <= top.rank:
                with RankedLock._count_lock:
                    RankedLock.violations += 1
                raise LockOrderViolation(
                    f"acquiring {self.name}(rank {self.rank}) while holding "
                    f"{top.name}(rank {top.rank}); locks must be taken in "
                    f"increasing rank order: {[n for _, n, _ in HIERARCHY]}")
