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

    def test_capacity_one(self):
        c = ThreadSafeLRUCache(1)
        c.put("a", 1)
        c.put("b", 2)
        self.assertEqual(len(c), 1)
        self.assertIsNone(c.get("a"))
        self.assertTrue(c.verify_integrity())

    def test_invalid_arguments(self):
        with self.assertRaises(ValueError):
            ThreadSafeLRUCache(0)
        with self.assertRaises(ValueError):
            ThreadSafeLRUCache(2, mode="medium")

    def test_size_never_exceeds_capacity(self):
        c = ThreadSafeLRUCache(5)
        for i in range(100):
            c.put(i, i)
            self.assertLessEqual(len(c), 5)
        self.assertEqual(c.keys_mru_to_lru(), [99, 98, 97, 96, 95])
        self.assertTrue(c.verify_integrity())

    def test_get_or_compute_both_modes(self):
        for mode in ("coarse", "scoped"):
            c = ThreadSafeLRUCache(2, embed_fn=lambda k: k * 2, mode=mode)
            self.assertEqual(c.get_or_compute(3), (6, False))
            self.assertEqual(c.get_or_compute(3), (6, True))
            self.assertEqual(c.embed_calls, 1)

    def test_embed_error_releases_inflight(self):
        def bad(_):
            raise RuntimeError("model down")
        c = ThreadSafeLRUCache(2, embed_fn=bad, mode="scoped")
        with self.assertRaises(RuntimeError):
            c.get_or_compute("k")
        c.embed_fn = lambda k: "ok"
        self.assertEqual(c.get_or_compute("k"), ("ok", False))  # no hang

    def test_verify_integrity_detects_corruption(self):
        c = ThreadSafeLRUCache(3)
        for k in "abc":
            c.put(k, k)
        c._head.next.next.prev = _Node("intruder")      # break a back-pointer
        with self.assertRaises(CacheCorruptionError):
            c.verify_integrity()


class ConcurrentLRUTests(unittest.TestCase):
    def _hammer(self, mode, capacity, key_space, threads=24, ops=400):
        cache = ThreadSafeLRUCache(capacity, embed_fn=lambda k: slow_embed(k, 0.0005),
                                   mode=mode)
        errors, stop = [], threading.Event()
        wrong_values = []
        start = threading.Barrier(threads + 1)

        def client(tid):
            rng = random.Random(tid)
            start.wait()
            try:
                for _ in range(ops):
                    k = rng.randrange(key_space)
                    r = rng.random()
                    if r < 0.6:
                        v, _ = cache.get_or_compute(k)
                        if v != f"vec({k})":
                            wrong_values.append((k, v))
                    elif r < 0.8:
                        v = cache.get(k)
                        if v is not None and v != f"vec({k})":
                            wrong_values.append((k, v))
                    else:
                        cache.put(k, f"vec({k})")
            except Exception as exc:          # pragma: no cover - should not happen
                errors.append(exc)

        def checker():
            start.wait()
            while not stop.is_set():
                try:
                    cache.verify_integrity()
                except Exception as exc:
                    errors.append(exc)
                    return
                time.sleep(0.001)

        ts = [threading.Thread(target=client, args=(i,)) for i in range(threads)]
        chk = threading.Thread(target=checker)
        for t in ts + [chk]:
            t.start()
        for t in ts:
            t.join(timeout=60)
            self.assertFalse(t.is_alive(), "client thread hung (deadlock?)")
        stop.set()
        chk.join(timeout=10)
        self.assertEqual(errors, [])
        self.assertEqual(wrong_values, [])
        self.assertTrue(cache.verify_integrity())
        s = cache.stats()
        self.assertLessEqual(s["size"], capacity)
        return cache, s

    def test_eviction_heavy_scoped(self):
        _, s = self._hammer("scoped", capacity=16, key_space=200)
        self.assertGreater(s["evictions"], 100)

    def test_eviction_heavy_coarse(self):
        _, s = self._hammer("coarse", capacity=16, key_space=200, ops=150)
        self.assertGreater(s["evictions"], 50)

    def test_hit_heavy_scoped(self):
        _, s = self._hammer("scoped", capacity=64, key_space=32)
        self.assertEqual(s["evictions"], 0)
        # Each key is embedded at most once (some keys arrive via put() first).
        self.assertLessEqual(s["embed_calls"], 32)
        self.assertGreater(s["hit_rate"], 0.9)

    def test_hit_heavy_coarse(self):
        _, s = self._hammer("coarse", capacity=64, key_space=32, ops=150)
        self.assertLessEqual(s["embed_calls"], 32)

    def test_mixed_hit_miss(self):
        for mode in ("coarse", "scoped"):
            _, s = self._hammer(mode, capacity=32, key_space=48, ops=150)
            self.assertGreater(s["hits"], 0)
            self.assertGreater(s["misses"], 0)

    def _concurrent_same_key(self, **kw):
        cache = ThreadSafeLRUCache(8, embed_fn=lambda k: slow_embed(k, 0.02), **kw)
        start = threading.Barrier(20)

        def worker():
            start.wait()
            cache.get_or_compute("hot-prompt")
        ts = [threading.Thread(target=worker) for _ in range(20)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        return cache

    def test_single_flight_prevents_stampede(self):
        cache = self._concurrent_same_key(mode="scoped", single_flight=True)
        self.assertEqual(cache.embed_calls, 1)
        self.assertEqual(cache.hits, 19)

    def test_without_single_flight_stampede_is_visible(self):
        cache = self._concurrent_same_key(mode="scoped", single_flight=False)
        self.assertGreater(cache.embed_calls, 1)     # duplicate work, but safe
        self.assertTrue(cache.verify_integrity())    # structure still intact

    def _max_parallel_embeds(self, mode):
        active, peak, lock = [0], [0], threading.Lock()

        def tracked_embed(k):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            time.sleep(0.01)
            with lock:
                active[0] -= 1
            return k
        cache = ThreadSafeLRUCache(64, embed_fn=tracked_embed, mode=mode)
        ts = [threading.Thread(target=cache.get_or_compute, args=(i,)) for i in range(16)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        return peak[0]

    def test_coarse_mode_serializes_embed(self):
        self.assertEqual(self._max_parallel_embeds("coarse"), 1)

    def test_scoped_mode_runs_embeds_in_parallel(self):
        self.assertGreater(self._max_parallel_embeds("scoped"), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
