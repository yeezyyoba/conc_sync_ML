"""
Task 2 - Tests for the O(1) thread-safe LRU cache
=================================================

Run:  python test_lru_cache.py          (plain unittest, no extra packages)

Single-threaded tests prove the LRU logic. Multi-threaded tests hammer the
cache with hits, misses and evictions from many threads while a checker
thread keeps calling verify_integrity(), in BOTH locking modes.
"""

import random
import threading
import time
import unittest

from lru_cache import CacheCorruptionError, ThreadSafeLRUCache, _Node


def slow_embed(key, delay=0.002):
    time.sleep(delay)
    return f"vec({key})"


class SingleThreadedLRUTests(unittest.TestCase):
    def test_get_missing_returns_none(self):
        c = ThreadSafeLRUCache(2)
        self.assertIsNone(c.get("x"))
        self.assertEqual(c.misses, 1)

    def test_put_then_get(self):
        c = ThreadSafeLRUCache(2)
        c.put("a", 1)
        self.assertEqual(c.get("a"), 1)
        self.assertEqual(c.hits, 1)

    def test_evicts_least_recently_used(self):
        c = ThreadSafeLRUCache(2)
        c.put("a", 1)
        c.put("b", 2)
        c.get("a")              # a becomes most recent, b is now LRU
        c.put("c", 3)           # evicts b
        self.assertIsNone(c.get("b"))
        self.assertEqual(c.get("a"), 1)
        self.assertEqual(c.get("c"), 3)
        self.assertEqual(c.evictions, 1)

    def test_order_mru_to_lru(self):
        c = ThreadSafeLRUCache(3)
        for k in "abc":
            c.put(k, k)
        c.get("a")
        self.assertEqual(c.keys_mru_to_lru(), ["a", "c", "b"])

    def test_update_existing_key_does_not_grow(self):
        c = ThreadSafeLRUCache(2)
        c.put("a", 1)
        c.put("a", 99)
        self.assertEqual(len(c), 1)
        self.assertEqual(c.get("a"), 99)
