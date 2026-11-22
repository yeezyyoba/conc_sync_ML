"""
Task 1 - Fixed Router (for comparison only)
===========================================

Same public API as BrokenInferenceRouter, with each bug fixed:

    BUG-1/2/4  telemetry   -> one metrics_lock around every read-modify-write
    BUG-3      cache       -> scoped locking + "single flight": the first thread
                              that misses marks the key as in-flight; others wait
                              on a Condition instead of calling embed() again.
                              embed() itself runs OUTSIDE the lock.
    BUG-5/6    queue       -> the capacity check and the append (or the
                              not-empty check and the pop) happen under ONE lock,
                              so check-then-act becomes a single atomic step.

Lock order (never nested here, but documented anyway):
    cache_cond  ->  queue_lock  ->  metrics_lock
"""

import threading

from broken_server import _race_window, fake_embed


class FixedInferenceRouter:
    def __init__(self, queue_capacity=10):
        self.cache = {}
        self.inflight = set()
        self.cache_cond = threading.Condition(threading.Lock())

        self.queue = []
        self.queue_capacity = queue_capacity
        self.queue_lock = threading.Lock()

        self.metrics_lock = threading.Lock()
        self.total_requests = 0
        self.cache_hits = 0
        self.total_tokens = 0

        self.embed_calls = []
        self.max_queue_len_seen = 0
        self.capacity_violations = []
        self.processed = []
        self.worker_errors = []
        self.rejected = 0
