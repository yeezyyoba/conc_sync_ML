"""
Task 4 - Integrated Concurrent ML Inference Pipeline
====================================================

    Client threads
         |  submit(prompt)
         v
    Router ---------> ThreadSafeLRUCache  (task_2)  embedding lookup / embed()
         |                [cache_lock, rank 10]  released before next step
         v
    DynamicBatchQueue (task_3)            bounded buffer, backpressure
         |                [queue_lock, rank 20]
         v
    Inference worker pool                 get_batch() -> one forward pass per batch
         |                [metrics_lock rank 30, result_lock rank 40]
         v
    ResultSlot.wait() wakes the client

Lock discipline (see lock_hierarchy.py)
    * Ranks: cache_lock(10) -> queue_lock(20) -> metrics_lock(30) -> result_lock(40)
    * Scopes are disjoint: no thread ever holds two of them at once.
    * Slow work (embed(), forward pass) never runs under any lock in
      scoped mode. Coarse cache mode holds cache_lock across embed() on
      purpose, as the baseline to benchmark against.

Request accounting (checked by stress_test.py)
    accepted             every request that entered Router.submit()
    completed            got a result from a worker
    rejected_on_shutdown refused because shutdown had started
    invariant            accepted == completed + rejected_on_shutdown
"""

import os
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "task_2"))
sys.path.insert(0, os.path.join(HERE, "..", "task_3"))

from batch_queue import DynamicBatchQueue, QueueShutdown  # noqa: E402
from lock_hierarchy import (CACHE_LOCK, METRICS_LOCK, QUEUE_LOCK,  # noqa: E402
                            RESULT_LOCK, RankedLock)
from lru_cache import ThreadSafeLRUCache  # noqa: E402


class RequestRejected(Exception):
    """The pipeline was shutting down and did not accept the request."""


class ResultSlot:
    """One-shot result holder a client blocks on (Lock + Condition)."""

    def __init__(self):
        self._cond = threading.Condition(RankedLock(RESULT_LOCK, "result_lock"))
        self._done = False
        self._value = None
        self._error = None

    def set_result(self, value):
        with self._cond:
            self._value, self._done = value, True
            self._cond.notify_all()

    def set_error(self, exc):
        with self._cond:
            self._error, self._done = exc, True
            self._cond.notify_all()

    def wait(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while not self._done:                      # while, never if
                if deadline is None:
                    self._cond.wait()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("no result (possible deadlock / lost request)")
                    self._cond.wait(remaining)
            if self._error is not None:
                raise self._error
            return self._value


class Request:
    __slots__ = ("rid", "prompt", "embedding", "cache_hit", "t_submit", "slot")

    def __init__(self, rid, prompt, embedding, cache_hit, t_submit):
        self.rid = rid
        self.prompt = prompt
        self.embedding = embedding
        self.cache_hit = cache_hit
        self.t_submit = t_submit
        self.slot = ResultSlot()
