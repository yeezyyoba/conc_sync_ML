"""
Task 4 - Deadlock demo: circular wait and how lock ordering cures it
====================================================================

Scenario from the training: a ROUTER thread updates the cache and then
enqueues (cache_lock -> queue_lock), while a BATCH WORKER pops from the
queue and then writes back to the cache (queue_lock -> cache_lock).

    Part 1  opposite order  -> circular wait -> DEADLOCK (detected with timeouts)
    Part 2  global order    -> both take cache_lock then queue_lock -> completes
    Part 3  disjoint scopes -> never hold both at once (what pipeline.py does)
    Part 4  RankedLock      -> wrong order raises LockOrderViolation instantly

Run:  python deadlock_demo.py      (output -> results/deadlock_demo.txt)
"""

import os
import threading
import time

from lock_hierarchy import CACHE_LOCK, QUEUE_LOCK, LockOrderViolation, RankedLock

HERE = os.path.dirname(os.path.abspath(__file__))
out = []
_print_lock = threading.Lock()


def say(s=""):
    with _print_lock:
        print(s)
        out.append(s)
