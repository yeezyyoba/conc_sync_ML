"""
Task 3 - Stress harness for DynamicBatchQueue
=============================================

Scenario A  steady load : 16 bursty producers, 4 workers, shutdown after the
                          producers finish -> every item must be processed once.
Scenario B  mid-run stop: shutdown() fires while producers are still pushing
                          -> attempts == accepted + rejected, and every accepted
                          item is still drained and processed.
Scenario C  idle CPU    : workers wait on an empty queue for 1 s. Condition-based
                          waiting should use ~0 CPU; a naive polling loop burns
                          a full core. Shows what "zero busy-spinning" means.

Every thread is joined with a timeout: a thread still alive = hang/deadlock.

Run:  python stress_harness.py      (report -> results/stress_report.txt)
"""

import os
import random
import threading
import time

from batch_queue import DynamicBatchQueue, QueueShutdown

HERE = os.path.dirname(os.path.abspath(__file__))
out = []


def say(s=""):
    print(s)
    out.append(s)


def run_load(n_producers=16, items_each=150, n_workers=4, capacity=32,
             max_batch=8, timeout_s=0.01, shutdown_after=None, seed=7):
    q = DynamicBatchQueue(capacity)
    lock = threading.Lock()
    accepted, rejected, consumed, batch_sizes = [], [], [], []
    attempts = [0]
    start = threading.Barrier(n_producers + n_workers)

    def producer(pid):
        rng = random.Random(seed * 100 + pid)
        start.wait()
        for i in range(items_each):
            if rng.random() < 0.1:                     # occasional idle gap
                time.sleep(rng.uniform(0.005, 0.02))
            item = (pid, i)
            with lock:
                attempts[0] += 1
            try:
                q.put(item)
                with lock:
                    accepted.append(item)
            except QueueShutdown:
                with lock:
                    rejected.append(item)
                    # count the rest of this producer's items as rejected too
                    remaining = items_each - i - 1
                    attempts[0] += remaining
                    rejected.extend((pid, j) for j in range(i + 1, items_each))
                return

    def worker():
        start.wait()
        while True:
            batch = q.get_batch(max_batch, timeout_s)
            if not batch:
                return                                 # shut down and drained
            time.sleep(0.002 + 0.0003 * len(batch))     # simulated forward pass
            with lock:
                consumed.extend(batch)
                batch_sizes.append(len(batch))

    ps = [threading.Thread(target=producer, args=(p,)) for p in range(n_producers)]
    ws = [threading.Thread(target=worker) for _ in range(n_workers)]
    cpu0, t0 = time.process_time(), time.perf_counter()
    for t in ps + ws:
        t.start()
    if shutdown_after is not None:
        time.sleep(shutdown_after)
        q.shutdown()
    for t in ps:
        t.join(timeout=30)
    if shutdown_after is None:
        q.shutdown()
    for t in ws:
        t.join(timeout=30)
    wall = time.perf_counter() - t0
    cpu = time.process_time() - cpu0
    hung = [t.name for t in ps + ws if t.is_alive()]
    s = q.stats()
    return {
        "wall_s": wall, "cpu_s": cpu, "hung_threads": hung,
        "attempts": attempts[0], "accepted": len(accepted), "rejected": len(rejected),
        "consumed": len(consumed), "unique_consumed": len(set(consumed)),
        "lost": len(set(accepted) - set(consumed)),
        "duplicates": len(consumed) - len(set(consumed)),
        "mean_batch": sum(batch_sizes) / len(batch_sizes) if batch_sizes else 0,
        "stats": s,
    }


def report(name, r, capacity=32):
    s = r["stats"]
    say(f"\n=== {name} ===")
    say(f"wall {r['wall_s']:.2f}s  cpu {r['cpu_s']:.2f}s  hung threads: {r['hung_threads'] or 'none'}")
    say(f"attempts {r['attempts']}  accepted {r['accepted']}  rejected_on_shutdown {r['rejected']}  "
        f"consumed {r['consumed']}")
    say(f"lost {r['lost']}  duplicates {r['duplicates']}  max_size_seen {s['max_size_seen']} / "
        f"capacity {capacity}")
    say(f"batches {s['batches']} (full {s['full_batches']}, timeout {s['timeout_batches']}, "
        f"drain {s['drain_batches']})  mean batch {r['mean_batch']:.2f}")
    say(f"producer waits (backpressure) {s['producer_waits']}  consumer wakeups {s['consumer_wakeups']}")
    checks = {
        "no hung threads (clean shutdown)": not r["hung_threads"],
        "attempts == accepted + rejected": r["attempts"] == r["accepted"] + r["rejected"],
        "accepted == consumed (nothing lost)": r["accepted"] == r["consumed"] and r["lost"] == 0,
        "no duplicates": r["duplicates"] == 0,
        "no capacity overflow": s["max_size_seen"] <= capacity,
    }
    for k, ok in checks.items():
        say(f"  [{'PASS' if ok else 'FAIL'}] {k}")
    return all(checks.values())


def idle_cpu(kind, seconds=1.0, n_workers=4):
    """CPU seconds burned by idle workers over `seconds` of wall time."""
    q = DynamicBatchQueue(8)
    stop = threading.Event()
    lock, buf = threading.Lock(), []

    def condition_worker():
        while q.get_batch(8, 0.05):
            pass

    def polling_worker():                       # the anti-pattern from the slides
        while not stop.is_set():
            with lock:
                if buf:
                    buf.pop()

    target = condition_worker if kind == "condition" else polling_worker
    ws = [threading.Thread(target=target) for _ in range(n_workers)]
    for t in ws:
        t.start()
    time.sleep(0.1)
    c0 = time.process_time()
    time.sleep(seconds)
    cpu = time.process_time() - c0
    stop.set()
    q.shutdown()
    for t in ws:
        t.join(5)
    return cpu, q.stats()["consumer_wakeups"]
