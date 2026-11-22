"""
Task 2 - Demo for the presentation
==================================

Part 1 shows the linked list after each operation (MRU on the left).
Part 2 runs the same concurrent miss-heavy workload in coarse and scoped
mode so you can see, before the full Task 4 benchmark, why holding the lock
across embed() hurts.

Run:  python demo_lru.py        (output also saved to results/demo_output.txt)
"""

import os
import threading
import time

from lru_cache import ThreadSafeLRUCache

HERE = os.path.dirname(os.path.abspath(__file__))
out = []


def say(s=""):
    print(s)
    out.append(s)


def show(cache, action):
    chain = " <-> ".join(str(k) for k in cache.keys_mru_to_lru())
    say(f"{action:<18} HEAD <-> {chain or '(empty)'} <-> TAIL   size={len(cache)}")


def part1():
    say("PART 1 - LRU behaviour (capacity 3)")
    c = ThreadSafeLRUCache(3)
    for k in ("A", "B", "C"):
        c.put(k, k.lower())
        show(c, f"put({k})")
    c.get("A")
    show(c, "get(A)  hit")
    c.put("D", "d")
    show(c, "put(D)  evicts B")
    c.get("B")
    show(c, "get(B)  miss")
    c.put("C", "c2")
    show(c, "put(C)  update")
    c.verify_integrity()
    say(f"verify_integrity() -> OK, stats: {c.stats()}")


def timed_run(mode, threads=16, keys_per_thread=10, embed_s=0.005):
    def embed(k):
        time.sleep(embed_s)          # simulated model call
        return f"vec({k})"
    cache = ThreadSafeLRUCache(64, embed_fn=embed, mode=mode)
    cache.put("hot", "vec(hot)")
    hit_lat = []
    lat_lock = threading.Lock()

    def client(tid):
        for i in range(keys_per_thread):
            cache.get_or_compute(f"t{tid}-k{i}")           # always a miss
            t0 = time.perf_counter()
            cache.get_or_compute("hot")                    # always a hit
            with lat_lock:
                hit_lat.append((time.perf_counter() - t0) * 1000)

    ts = [threading.Thread(target=client, args=(t,)) for t in range(threads)]
    t0 = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    wall = time.perf_counter() - t0
    cache.verify_integrity()
    hit_lat.sort()
    return wall, sum(hit_lat) / len(hit_lat), hit_lat[int(0.95 * len(hit_lat))]


def part2():
    say("\nPART 2 - coarse vs scoped locking (16 threads, 160 misses of 5 ms)")
    say(f"{'mode':<8} {'wall time':>10} {'avg hit latency':>17} {'p95 hit latency':>17}")
    for mode in ("coarse", "scoped"):
        wall, avg, p95 = timed_run(mode)
        say(f"{mode:<8} {wall:>9.2f}s {avg:>15.3f}ms {p95:>15.3f}ms")
    say("coarse: every miss holds the lock for 5 ms, so even cache HITS queue behind it.")
    say("scoped: embeds run in parallel outside the lock; hits only wait microseconds.")


if __name__ == "__main__":
    part1()
    part2()
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    with open(os.path.join(HERE, "results", "demo_output.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
