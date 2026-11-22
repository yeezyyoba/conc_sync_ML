"""
Task 1 - Reproducible stress script
===================================

Fires 30 concurrent client threads (>= 25 required) at the router while
4 worker threads drain the list queue, then checks the results against
what MUST be true if the code were correct.

Usage:
    python stress_test.py                 # broken router, 3 trials
    python stress_test.py --fixed         # fixed router (all checks pass)
    python stress_test.py --trials 5 --clients 40

Output: printed report + results/task1_<broken|fixed>_report.txt
"""

import argparse
import os
import random
import re
import threading
import time
from collections import Counter

from broken_server import BrokenInferenceRouter
from fixed_server import FixedInferenceRouter

HERE = os.path.dirname(os.path.abspath(__file__))

PROMPTS = [
    "summarize the quarterly sales report",
    "translate hello world to amharic",
    "classify this review as positive or negative",
    "what is the capital of ethiopia",
    "explain gradient descent simply",
    "write a haiku about coffee",
    "detect fraud in this transaction",
    "extract entities from the news article",
    "recommend a movie like inception",
    "generate sql for top customers",
    "describe a deadlock in one sentence",
    "rank these search results",
]


def bug_line_map(path=os.path.join(HERE, "broken_server.py")):
    """Return {'BUG-1': [line numbers], ...} by scanning the source markers."""
    lines = {}
    with open(path, encoding="utf-8") as f:
        for no, text in enumerate(f, start=1):
            for tag in re.findall(r"\[(BUG-\d+)\]", text):
                lines.setdefault(tag, []).append(no)
    return lines


def run_trial(router_cls, n_clients, reqs_per_client, n_workers, seed):
    random.seed(seed)
    router = router_cls(queue_capacity=10)
    accepted_ids, expected_tokens = [], [0]
    book_lock = threading.Lock()        # protects the test's OWN bookkeeping
    clients_done = threading.Event()
    start_gate = threading.Barrier(n_clients + n_workers)

    def client(cid):
        rng = random.Random(seed * 1000 + cid)
        mine, toks = [], 0
        start_gate.wait()               # everybody starts at the same instant
        for i in range(reqs_per_client):
            prompt = rng.choice(PROMPTS)
            rid = f"c{cid}-r{i}"
            toks += len(prompt.split())
            if router.handle_request(rid, prompt):
                mine.append(rid)
            time.sleep(rng.uniform(0, 0.001))
        with book_lock:
            accepted_ids.extend(mine)
            expected_tokens[0] += toks

    def worker():
        start_gate.wait()
        idle_after_done = 0
        while True:
            did = router.worker_step()
            if not did:
                if clients_done.is_set():
                    idle_after_done += 1
                    if idle_after_done > 50:   # queue stayed empty: finished
                        return
                time.sleep(0.0005)
            else:
                idle_after_done = 0
