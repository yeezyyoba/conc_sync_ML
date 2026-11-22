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
