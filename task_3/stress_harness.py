"""
Task 3 - Stress harness for DynamicBatchQueue
=============================================

Scenario A  steady load : 16 bursty producers, 4 workers, shutdown after the
                          producers finish -> every item must be processed once.
Scenario B  mid-run stop: shutdown() fires while producers are still pushing
                          -> attempts == accepted + rejected, and every accepted
                          item is still drained and processed.
Scenario C  idle CPU    : workers wait on an empty queue for 1 s. Condition-based
                          waiting should use ~0 CPU; a naive polling loop burns
                          a full core. Shows what "zero busy-spinning" means.

Every thread is joined with a timeout: a thread still alive = hang/deadlock.

Run:  python stress_harness.py      (report -> results/stress_report.txt)
"""

import os
import random
import threading
import time

from batch_queue import DynamicBatchQueue, QueueShutdown

HERE = os.path.dirname(os.path.abspath(__file__))
out = []


def say(s=""):
    print(s)
    out.append(s)
