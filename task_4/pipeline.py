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


class Metrics:
    def __init__(self):
        self._lock = RankedLock(METRICS_LOCK, "metrics_lock")
        self.accepted = 0
        self.completed = 0
        self.rejected_on_shutdown = 0
        self.failed = 0
        self.latencies_ms = []
        self.batch_sizes = []
        self.completed_ids = set()
        self.duplicate_completions = 0

    def on_accept(self):
        with self._lock:
            self.accepted += 1
            return self.accepted            # also used as the request id

    def on_reject(self):
        with self._lock:
            self.rejected_on_shutdown += 1

    def on_failure(self):
        with self._lock:
            self.failed += 1

    def on_batch(self, requests, now):
        with self._lock:
            self.batch_sizes.append(len(requests))
            for r in requests:
                if r.rid in self.completed_ids:
                    self.duplicate_completions += 1
                self.completed_ids.add(r.rid)
                self.completed += 1
                self.latencies_ms.append((now - r.t_submit) * 1000.0)

    def snapshot(self):
        with self._lock:
            return {
                "accepted": self.accepted,
                "completed": self.completed,
                "rejected_on_shutdown": self.rejected_on_shutdown,
                "failed": self.failed,
                "duplicate_completions": self.duplicate_completions,
                "latencies_ms": list(self.latencies_ms),
                "batch_sizes": list(self.batch_sizes),
            }


def make_embed(embed_s):
    def embed(prompt):
        time.sleep(embed_s)                       # simulated embedding model
        return (len(prompt), sum(map(ord, prompt)) % 9973)
    return embed


class Router:
    """Runs on the client's own thread: cache lookup, then enqueue."""

    def __init__(self, cache, queue, metrics):
        self.cache = cache
        self.queue = queue
        self.metrics = metrics
        self._closing = False

    def close(self):
        self._closing = True

    def submit(self, prompt):
        rid = self.metrics.on_accept()
        t0 = time.perf_counter()
        if self._closing:                          # fast reject at the gate
            self.metrics.on_reject()
            raise RequestRejected(f"request {rid}: pipeline shutting down")
        # Step 1: cache (cache_lock is taken and RELEASED inside the cache).
        embedding, hit = self.cache.get_or_compute(prompt)
        req = Request(rid, prompt, embedding, hit, t0)
        # Step 2: queue (queue_lock only; no other lock is held here).
        try:
            self.queue.put(req)
        except QueueShutdown:
            self.metrics.on_reject()
            raise RequestRejected(f"request {rid}: queue shut down") from None
        return req.slot


class InferencePipeline:
    def __init__(self, cache_mode="scoped", max_batch_size=8, batch_timeout_s=0.005,
                 n_workers=4, queue_capacity=64, cache_capacity=256,
                 embed_s=0.005, forward_base_s=0.020, forward_per_item_s=0.001):
        self.max_batch_size = max_batch_size
        self.batch_timeout_s = batch_timeout_s
        self.forward_base_s = forward_base_s
        self.forward_per_item_s = forward_per_item_s

        self.cache = ThreadSafeLRUCache(
            cache_capacity, embed_fn=make_embed(embed_s), mode=cache_mode,
            lock=RankedLock(CACHE_LOCK, "cache_lock"))
        self.queue = DynamicBatchQueue(queue_capacity,
                                       lock=RankedLock(QUEUE_LOCK, "queue_lock"))
        self.metrics = Metrics()
        self.router = Router(self.cache, self.queue, self.metrics)
        self.worker_errors = []
        self._workers = [threading.Thread(target=self._worker_loop, name=f"worker-{i}",
                                          daemon=True) for i in range(n_workers)]
        for w in self._workers:
            w.start()

    # -- public API ----------------------------------------------------- #
    def submit(self, prompt):
        return self.router.submit(prompt)

    def infer(self, prompt, timeout=None):
        return self.submit(prompt).wait(timeout)

    def shutdown(self, join_timeout=10.0):
        """Stop accepting, drain the queue, join workers. Returns True if clean."""
        self.router.close()
        self.queue.shutdown()
        for w in self._workers:
            w.join(join_timeout)
        return not any(w.is_alive() for w in self._workers)

    # -- worker --------------------------------------------------------- #
    def _forward(self, batch):
        # One "GPU" call for the whole batch: fixed overhead + small per-item cost.
        time.sleep(self.forward_base_s + self.forward_per_item_s * len(batch))
        return [f"answer[{r.prompt[:24]}|emb={r.embedding[1]}]" for r in batch]

    def _worker_loop(self):
        while True:
            batch = self.queue.get_batch(self.max_batch_size, self.batch_timeout_s)
            if not batch:
                return                                   # shut down + drained
            try:
                outputs = self._forward(batch)           # no lock held
            except Exception as exc:                     # pragma: no cover
                self.worker_errors.append(repr(exc))
                for r in batch:
                    self.metrics.on_failure()
                    r.slot.set_error(exc)
                continue
            self.metrics.on_batch(batch, time.perf_counter())   # metrics_lock only
            for r, out in zip(batch, outputs):
                r.slot.set_result(out)                   # result_lock only
