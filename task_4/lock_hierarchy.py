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
