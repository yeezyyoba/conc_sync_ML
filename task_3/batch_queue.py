"""
Task 3 - Bounded Dynamic Inference Batching Queue
=================================================

Producers (client/router threads) call put(); inference workers call
get_batch() to pull up to max_batch_size requests at once, because a GPU
forward pass on 8 requests costs much less than 8 separate passes.

Storage
-------
A fixed-size ring buffer (circular array) built from a plain list:
    head  -> index of the oldest item
    count -> number of items stored
    enqueue at (head + count) % capacity, dequeue at head.
Both operations are O(1) and the buffer can never physically hold more
than `capacity` items. (No queue.Queue, no collections.deque.)

Synchronization
---------------
One Lock, two Conditions that share it:
    not_full   - producers wait here while the buffer is full (backpressure)
    not_empty  - workers wait here for items / a full batch / shutdown
Every wait() sits inside a `while` loop that re-checks the condition,
because another thread can grab the slot or the items between the
notify and the moment the woken thread re-acquires the lock.
Nobody ever sleeps or polls: threads block in wait() using 0% CPU.

get_batch(max_batch_size, timeout_s) returns when ANY of these is true:
    1. at least max_batch_size items are queued        -> full batch
    2. the monotonic deadline has passed and >= 1 item -> partial batch
    3. shutdown() was called                           -> drain what is left
If the deadline passes while the queue is empty, the worker keeps waiting
(without a timeout) for the first item and returns it as soon as it lands.
time.monotonic() is used because it never jumps when the wall clock changes.

shutdown() sets a flag and notify_all()s both conditions:
    * blocked and future put() calls raise QueueShutdown (rejected)
    * workers keep receiving the remaining items (clean drain)
    * once the buffer is empty, get_batch() returns [] -> worker exits
"""

import threading
import time


class QueueShutdown(Exception):
    """Raised by put() when the queue has been shut down."""


class DynamicBatchQueue:
    def __init__(self, capacity, lock=None):
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self._buf = [None] * capacity
        self._head = 0
        self._count = 0
        self._shutdown = False

        self._lock = lock if lock is not None else threading.Lock()
        self._not_full = threading.Condition(self._lock)
        self._not_empty = threading.Condition(self._lock)

        # Statistics (all updated under the lock)
        self.total_put = 0
        self.total_got = 0
        self.batches = 0
        self.rejected_puts = 0
        self.max_size_seen = 0
        self.producer_waits = 0      # times a producer had to block (backpressure)
        self.consumer_wakeups = 0    # times a worker returned from wait()
        self.full_batches = 0
        self.timeout_batches = 0
        self.drain_batches = 0

    # ------------------------------------------------------------------ #
    # Ring buffer primitives (CALLER MUST HOLD self._lock)
    # ------------------------------------------------------------------ #
    def _enqueue(self, item):
        tail = (self._head + self._count) % self.capacity
        self._buf[tail] = item
        self._count += 1
        if self._count > self.max_size_seen:
            self.max_size_seen = self._count

    def _dequeue(self):
        item = self._buf[self._head]
        self._buf[self._head] = None          # drop the reference
        self._head = (self._head + 1) % self.capacity
        self._count -= 1
        return item

    # ------------------------------------------------------------------ #
    # Producer side
    # ------------------------------------------------------------------ #
    def put(self, item, timeout=None):
        """Block while full. Returns True, or False if `timeout` expired.
        Raises QueueShutdown if the queue is (or becomes) shut down."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._not_full:
            while self._count >= self.capacity and not self._shutdown:
                self.producer_waits += 1
                if deadline is None:
                    self._not_full.wait()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._not_full.wait(remaining)
            if self._shutdown:
                self.rejected_puts += 1
                raise QueueShutdown("queue is shut down")
            self._enqueue(item)
            self.total_put += 1
            self._not_empty.notify_all()          # wake waiting workers
            return True

    # ------------------------------------------------------------------ #
    # Consumer side
    # ------------------------------------------------------------------ #
    def get_batch(self, max_batch_size, timeout_s):
        """Return a list of 1..max_batch_size items, or [] after shutdown
        once the queue is fully drained."""
        if max_batch_size < 1:
            raise ValueError("max_batch_size must be >= 1")
        deadline = time.monotonic() + timeout_s
        with self._not_empty:
            while True:
                if self._count >= max_batch_size:
                    reason = "full"
                    break
                if self._shutdown:
                    reason = "drain"
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if self._count > 0:
                        reason = "timeout"
                        break
                    self._not_empty.wait()            # deadline gone, queue empty
                else:
                    self._not_empty.wait(remaining)   # wait for items or deadline
                self.consumer_wakeups += 1

            n = min(self._count, max_batch_size)
            batch = [self._dequeue() for _ in range(n)]
            if batch:
                self.total_got += n
                self.batches += 1
                if reason == "full":
                    self.full_batches += 1
                elif reason == "timeout":
                    self.timeout_batches += 1
                else:
                    self.drain_batches += 1
                self._not_full.notify_all()           # space opened up
            if self._count >= max_batch_size or (self._shutdown and self._count):
                self._not_empty.notify_all()          # let another worker continue
            return batch

    # ------------------------------------------------------------------ #
    # Lifecycle / inspection
    # ------------------------------------------------------------------ #
    def shutdown(self):
        with self._lock:
            self._shutdown = True
            self._not_full.notify_all()
            self._not_empty.notify_all()

    @property
    def is_shutdown(self):
        with self._lock:
            return self._shutdown

    def __len__(self):
        with self._lock:
            return self._count
