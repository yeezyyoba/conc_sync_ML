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
