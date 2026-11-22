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
