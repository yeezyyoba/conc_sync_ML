"""
Task 3 - Tests for DynamicBatchQueue
====================================

Run:  python test_batch_queue.py
"""

import threading
import time
import unittest

from batch_queue import DynamicBatchQueue, QueueShutdown


def run_in_thread(fn, *args):
    box = {}

    def target():
        t0 = time.monotonic()
        try:
            box["result"] = fn(*args)
        except Exception as exc:
            box["error"] = exc
        box["elapsed"] = time.monotonic() - t0
    t = threading.Thread(target=target, daemon=True)
    t.start()
    return t, box


class BasicTests(unittest.TestCase):
    def test_fifo_order(self):
        q = DynamicBatchQueue(5)
        for i in range(5):
            q.put(i)
        self.assertEqual(q.get_batch(5, 0.1), [0, 1, 2, 3, 4])

    def test_ring_buffer_wraps_around(self):
        q = DynamicBatchQueue(3)
        out = []
        for i in range(10):
            q.put(i)
            out += q.get_batch(1, 0.01)
        self.assertEqual(out, list(range(10)))
        self.assertEqual(len(q), 0)

    def test_invalid_arguments(self):
        with self.assertRaises(ValueError):
            DynamicBatchQueue(0)
        with self.assertRaises(ValueError):
            DynamicBatchQueue(2).get_batch(0, 0.1)


class BackpressureTests(unittest.TestCase):
    def test_put_blocks_when_full_until_space(self):
        q = DynamicBatchQueue(2)
        q.put("a")
        q.put("b")
        t, box = run_in_thread(q.put, "c")
        time.sleep(0.1)
        self.assertTrue(t.is_alive(), "put should block while full")
        self.assertEqual(q.get_batch(1, 0.01), ["a"])
        t.join(1)
        self.assertFalse(t.is_alive())
        self.assertTrue(box["result"])
        self.assertEqual(q.get_batch(2, 0.01), ["b", "c"])
        self.assertGreaterEqual(q.stats()["producer_waits"], 1)

    def test_put_timeout_returns_false(self):
        q = DynamicBatchQueue(1)
        q.put(1)
        t0 = time.monotonic()
        self.assertFalse(q.put(2, timeout=0.1))
        self.assertGreaterEqual(time.monotonic() - t0, 0.09)
        self.assertEqual(len(q), 1)
