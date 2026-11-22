"""
Task 4 - Shared client workload (used by stress_test.py and benchmark.py)
=========================================================================

Each client thread sends `n_requests` prompts:
    * hot_ratio of them come from a small HOT set (repeat prompts -> cache hits)
    * the rest are unique prompts (always cache misses -> embed())
    * bursty arrivals: a burst of back-to-back requests, then a short pause
Each client is closed-loop: it waits for its answer before sending the next.
"""

import random
import threading
import time

from pipeline import RequestRejected

HOT_PROMPTS = [f"hot prompt #{i}: summarize the ethiopian fintech report" for i in range(20)]


def run_clients(pipeline, n_clients=32, n_requests=40, hot_ratio=0.7, seed=1,
                burst=(4, 12), pause_s=(0.002, 0.02), shutdown_at=None,
                result_timeout=30.0):
    """Run client threads against `pipeline`. Returns a dict of client-side
    counts. If `shutdown_at` is set, pipeline.shutdown() is called that many
    seconds after start, while clients are still sending."""
    lock = threading.Lock()
    client = {"ok": 0, "rejected": 0, "timeouts": 0, "errors": 0, "answers": []}
    start = threading.Barrier(n_clients + 1)
