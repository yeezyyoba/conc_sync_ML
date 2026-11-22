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


def part1_deadlock():
    say("PART 1 - opposite lock order (router: cache->queue, worker: queue->cache)")
    cache_lock, queue_lock = threading.Lock(), threading.Lock()
    both_hold_first = threading.Barrier(2)       # make the bad interleaving certain
    both_decided = threading.Barrier(2)          # nobody releases until both gave up
    status = {}

    def router():
        with cache_lock:
            say("  router : holds cache_lock, wants queue_lock")
            both_hold_first.wait()
            if queue_lock.acquire(timeout=2.0):  # timeout only so the demo can end
                queue_lock.release()
                status["router"] = "finished"
            else:
                status["router"] = "STUCK waiting for queue_lock (held by worker)"
            both_decided.wait()

    def worker():
        with queue_lock:
            say("  worker : holds queue_lock, wants cache_lock")
            both_hold_first.wait()
            if cache_lock.acquire(timeout=2.0):
                cache_lock.release()
                status["worker"] = "finished"
            else:
                status["worker"] = "STUCK waiting for cache_lock (held by router)"
            both_decided.wait()

    t0 = time.perf_counter()
    ts = [threading.Thread(target=router), threading.Thread(target=worker)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    for who in ("router", "worker"):
        say(f"  {who:<7}: {status[who]}")
    deadlocked = all("STUCK" in v for v in status.values())
    say(f"  -> {'DEADLOCK: circular wait, neither thread could ever proceed' if deadlocked else 'no deadlock this time'}"
        f" (gave up after {time.perf_counter() - t0:.1f}s)")
    say("  Coffman conditions present: mutual exclusion, hold-and-wait, "
        "no preemption, circular wait")
    return deadlocked


def _run_iterations(router_fn, worker_fn, iterations=2000, threads_each=4):
    ts = ([threading.Thread(target=router_fn, args=(iterations,)) for _ in range(threads_each)] +
          [threading.Thread(target=worker_fn, args=(iterations,)) for _ in range(threads_each)])
    t0 = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=20)
    hung = sum(t.is_alive() for t in ts)
    return hung, time.perf_counter() - t0
