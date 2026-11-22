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


class BatchingTests(unittest.TestCase):
    def test_full_batch_returns_immediately(self):
        q = DynamicBatchQueue(16)
        for i in range(10):
            q.put(i)
        t0 = time.monotonic()
        self.assertEqual(q.get_batch(8, 5.0), list(range(8)))
        self.assertLess(time.monotonic() - t0, 0.05)

    def test_partial_batch_after_deadline(self):
        q = DynamicBatchQueue(16)
        q.put("x")
        q.put("y")
        t0 = time.monotonic()
        batch = q.get_batch(8, 0.2)
        elapsed = time.monotonic() - t0
        self.assertEqual(batch, ["x", "y"])
        self.assertGreaterEqual(elapsed, 0.19)
        self.assertLess(elapsed, 0.5)
        self.assertEqual(q.stats()["timeout_batches"], 1)

    def test_batch_fills_while_waiting(self):
        q = DynamicBatchQueue(16)
        t, box = run_in_thread(q.get_batch, 4, 5.0)
        for i in range(4):
            time.sleep(0.02)
            q.put(i)
        t.join(1)
        self.assertEqual(box["result"], [0, 1, 2, 3])
        self.assertLess(box["elapsed"], 1.0)        # did not wait for 5 s deadline

    def test_empty_after_deadline_waits_for_first_item(self):
        q = DynamicBatchQueue(4)
        t, box = run_in_thread(q.get_batch, 8, 0.05)
        time.sleep(0.3)                            # deadline long gone, still empty
        self.assertTrue(t.is_alive())
        q.put("late")
        t.join(1)
        self.assertEqual(box["result"], ["late"])

    def test_consumers_do_not_busy_spin(self):
        q = DynamicBatchQueue(4)
        threads = [run_in_thread(q.get_batch, 8, 0.01)[0] for _ in range(4)]
        time.sleep(0.5)                            # idle period, nothing to do
        wakeups = q.stats()["consumer_wakeups"]
        # Each worker wakes once when its 10 ms deadline passes, then blocks.
        self.assertLessEqual(wakeups, 8)
        q.shutdown()
        for t in threads:
            t.join(1)
            self.assertFalse(t.is_alive())


class ShutdownTests(unittest.TestCase):
    def test_shutdown_wakes_blocked_consumer(self):
        q = DynamicBatchQueue(4)
        t, box = run_in_thread(q.get_batch, 8, 10.0)
        time.sleep(0.05)
        q.shutdown()
        t.join(1)
        self.assertFalse(t.is_alive())
        self.assertEqual(box["result"], [])

    def test_shutdown_wakes_blocked_producer(self):
        q = DynamicBatchQueue(1)
        q.put(1)
        t, box = run_in_thread(q.put, 2)
        time.sleep(0.05)
        q.shutdown()
        t.join(1)
        self.assertFalse(t.is_alive())
        self.assertIsInstance(box.get("error"), QueueShutdown)

    def test_put_after_shutdown_rejected(self):
        q = DynamicBatchQueue(4)
        q.shutdown()
        with self.assertRaises(QueueShutdown):
            q.put("late")
        self.assertEqual(q.stats()["rejected_puts"], 1)

    def test_shutdown_drains_remaining_items(self):
        q = DynamicBatchQueue(10)
        for i in range(7):
            q.put(i)
        q.shutdown()
        self.assertEqual(q.get_batch(4, 1.0), [0, 1, 2, 3])
        self.assertEqual(q.get_batch(4, 1.0), [4, 5, 6])
        self.assertEqual(q.get_batch(4, 1.0), [])
