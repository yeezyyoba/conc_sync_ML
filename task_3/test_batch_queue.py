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
