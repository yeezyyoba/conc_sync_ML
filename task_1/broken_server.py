"""
Task 1 - Broken Baseline Inference Router (INTENTIONALLY UNSAFE)
=================================================================

This router is shared by many client threads and many worker threads but
uses NO locks at all. It has three kinds of shared state:

    1. self.cache        - plain dict          (prompt -> embedding)
    2. self.queue        - plain Python list   (bounded "request queue")
    3. telemetry         - total_requests, cache_hits, total_tokens

Every place where shared state is touched without synchronization is
tagged with a  # [BUG-n]  marker. stress_test.py finds those markers
automatically so the bug report always points to the exact line.

The _race_window() calls are tiny sleeps (time.sleep) that widen the gap
between "read" and "write". They do not create the bug - the bug is the
missing lock - they only make the OS scheduler switch threads at the
worst possible moment more often, so the bug shows up on every run.

DO NOT use this design. See fixed_server.py for the corrected version.
"""

import random
import threading
import time


def _race_window(max_s=0.0005):
    """Sleep a random tiny amount so another thread can run in between."""
    time.sleep(random.uniform(0, max_s))


def fake_embed(prompt):
    """Simulated slow embedding model (e.g. a remote embedding API)."""
    time.sleep(0.002)
    return [float(ord(c) % 7) for c in prompt[:8]]


class BrokenInferenceRouter:
    def __init__(self, queue_capacity=10):
        self.cache = {}                 # shared, unprotected
        self.queue = []                 # shared, unprotected
        self.queue_capacity = queue_capacity

        # Telemetry counters (shared, unprotected)
        self.total_requests = 0
        self.cache_hits = 0
        self.total_tokens = 0

        # Instrumentation used ONLY to detect the bugs (written by many
        # threads; list.append is a single bytecode op so these logs are
        # reliable enough for evidence collection).
        self.embed_calls = []           # one entry per embed() call
        self.max_queue_len_seen = 0
        self.capacity_violations = []   # queue lengths seen above capacity
        self.processed = []             # request ids processed by workers
        self.worker_errors = []         # exceptions raised in workers
        self.rejected = 0

    # ------------------------------------------------------------------ #
    # Client side
    # ------------------------------------------------------------------ #
    def handle_request(self, request_id, prompt):
        # --- telemetry: read-modify-write without a lock ----------------
        current = self.total_requests          # [BUG-1] read
        _race_window()
        self.total_requests = current + 1      # [BUG-1] write (lost update)

        tokens = len(prompt.split())
        current_tokens = self.total_tokens     # [BUG-2] read
        _race_window()
        self.total_tokens = current_tokens + tokens   # [BUG-2] write

        # --- cache: check-then-act without a lock -----------------------
        if prompt not in self.cache:           # [BUG-3] CHECK
            _race_window()
            vec = fake_embed(prompt)
            self.embed_calls.append(prompt)
            self.cache[prompt] = vec           # [BUG-3] ACT (stampede / overwrite)
        else:
            hits = self.cache_hits             # [BUG-4] read
            _race_window()
            self.cache_hits = hits + 1         # [BUG-4] write

        # --- bounded queue: capacity check-then-append without a lock ---
        if len(self.queue) < self.queue_capacity:   # [BUG-5] CHECK capacity
            _race_window()
            self.queue.append(request_id)            # [BUG-5] ACT (overflow)
            n = len(self.queue)
            if n > self.max_queue_len_seen:
                self.max_queue_len_seen = n
            if n > self.queue_capacity:
                self.capacity_violations.append(n)
            return True
        self.rejected += 1
        return False

    # ------------------------------------------------------------------ #
    # Worker side
    # ------------------------------------------------------------------ #
    def worker_step(self):
        """Take one request from the front of the list queue."""
        if self.queue:                         # [BUG-6] CHECK not empty
            _race_window()
            try:
                item = self.queue[0]           # [BUG-6] peek (two workers see same item)
                _race_window()
                del self.queue[0]              # [BUG-6] remove (may delete a DIFFERENT item)
                self.processed.append(item)
            except IndexError as exc:          # queue emptied by another worker
                self.worker_errors.append(repr(exc))
            return True
        return False
