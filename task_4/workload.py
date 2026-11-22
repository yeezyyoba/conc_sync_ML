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

    def one_client(cid):
        rng = random.Random(seed * 10_000 + cid)
        start.wait()
        left_in_burst = rng.randint(*burst)
        for i in range(n_requests):
            if rng.random() < hot_ratio:
                prompt = rng.choice(HOT_PROMPTS)
            else:
                prompt = f"unique prompt c{cid} r{i} seed{seed}"
            try:
                answer = pipeline.submit(prompt).wait(result_timeout)
                with lock:
                    client["ok"] += 1
                    client["answers"].append(answer)
            except RequestRejected:
                with lock:
                    client["rejected"] += 1
            except TimeoutError:
                with lock:
                    client["timeouts"] += 1
            except Exception:                         # pragma: no cover
                with lock:
                    client["errors"] += 1
            left_in_burst -= 1
            if left_in_burst == 0:                    # end of burst: idle a bit
                time.sleep(rng.uniform(*pause_s))
                left_in_burst = rng.randint(*burst)

    threads = [threading.Thread(target=one_client, args=(c,), name=f"client-{c}", daemon=True)
               for c in range(n_clients)]
    for t in threads:
        t.start()
    t0 = time.perf_counter()
    start.wait()
    if shutdown_at is not None:
        time.sleep(shutdown_at)
        client["shutdown_clean"] = pipeline.shutdown()
    for t in threads:
        t.join(result_timeout + 30)
    client["wall_s"] = time.perf_counter() - t0
    client["hung_clients"] = [t.name for t in threads if t.is_alive()]
    return client
