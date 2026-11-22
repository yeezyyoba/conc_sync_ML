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

    clients = [threading.Thread(target=client, args=(c,)) for c in range(n_clients)]
    workers = [threading.Thread(target=worker) for _ in range(n_workers)]
    t0 = time.perf_counter()
    for t in clients + workers:
        t.start()
    for t in clients:
        t.join()
    clients_done.set()
    for t in workers:
        t.join()
    elapsed = time.perf_counter() - t0

    total = n_clients * reqs_per_client
    used_prompts = len(set(router.embed_calls)) or 1
    processed_counts = Counter(router.processed)
    duplicates = sum(c - 1 for c in processed_counts.values() if c > 1)
    lost = len(set(accepted_ids) - set(processed_counts))
    embed_counts = Counter(router.embed_calls)
    misses = len(router.embed_calls)

    return {
        "elapsed_s": elapsed,
        "expected_total_requests": total,
        "observed_total_requests": router.total_requests,
        "lost_request_updates": total - router.total_requests,
        "expected_total_tokens": expected_tokens[0],
        "observed_total_tokens": router.total_tokens,
        "lost_token_updates": expected_tokens[0] - router.total_tokens,
        "unique_prompts": used_prompts,
        "embed_calls": misses,
        "duplicate_embed_calls": misses - used_prompts,
        "worst_prompt_embeds": max(embed_counts.values()) if embed_counts else 0,
        "expected_cache_hits": total - misses,
        "observed_cache_hits": router.cache_hits,
        "lost_hit_updates": (total - misses) - router.cache_hits,
        "queue_capacity": router.queue_capacity,
        "max_queue_len_seen": router.max_queue_len_seen,
        "capacity_violations": len(router.capacity_violations),
        "accepted_into_queue": len(accepted_ids),
        "rejected_queue_full": router.rejected,
        "items_processed_twice": duplicates,
        "items_lost_never_processed": lost,
        "worker_index_errors": len(router.worker_errors),
    }


CHECKS = [
    ("BUG-1", "Lost total_requests updates", "lost_request_updates"),
    ("BUG-2", "Lost total_tokens updates", "lost_token_updates"),
    ("BUG-3", "Duplicate embed() calls (cache stampede)", "duplicate_embed_calls"),
    ("BUG-4", "Lost cache_hits updates", "lost_hit_updates"),
    ("BUG-5", "Queue capacity violations", "capacity_violations"),
    ("BUG-6", "Items processed twice (corruption)", "items_processed_twice"),
    ("BUG-6", "Items lost / never processed", "items_lost_never_processed"),
    ("BUG-6", "Worker IndexError crashes", "worker_index_errors"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixed", action="store_true", help="test the fixed router")
    ap.add_argument("--clients", type=int, default=30)
    ap.add_argument("--requests", type=int, default=40, help="requests per client")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--trials", type=int, default=3)
    args = ap.parse_args()

    cls = FixedInferenceRouter if args.fixed else BrokenInferenceRouter
    name = "fixed" if args.fixed else "broken"
    lines = bug_line_map()
    out = []
    say = lambda s="": (print(s), out.append(s))

    say(f"=== Task 1 stress test: {name.upper()} router ===")
    say(f"clients={args.clients}  requests/client={args.requests}  "
        f"workers={args.workers}  trials={args.trials}  queue_capacity=10")
    results = []
    for t in range(args.trials):
        r = run_trial(cls, args.clients, args.requests, args.workers, seed=42 + t)
        results.append(r)
        say(f"\n--- trial {t + 1} (seed {42 + t}, {r['elapsed_s']:.2f}s) ---")
        say(f"total_requests : expected {r['expected_total_requests']:>6}  "
            f"observed {r['observed_total_requests']:>6}  lost {r['lost_request_updates']}")
        say(f"total_tokens   : expected {r['expected_total_tokens']:>6}  "
            f"observed {r['observed_total_tokens']:>6}  lost {r['lost_token_updates']}")
        say(f"cache_hits     : expected {r['expected_cache_hits']:>6}  "
            f"observed {r['observed_cache_hits']:>6}  lost {r['lost_hit_updates']}")
        say(f"embed() calls  : {r['embed_calls']} for {r['unique_prompts']} unique prompts "
            f"-> {r['duplicate_embed_calls']} duplicates (worst prompt embedded "
            f"{r['worst_prompt_embeds']}x)")
        say(f"queue          : capacity {r['queue_capacity']}, max length seen "
            f"{r['max_queue_len_seen']}, violations {r['capacity_violations']}")
        say(f"workers        : accepted {r['accepted_into_queue']}, processed twice "
            f"{r['items_processed_twice']}, lost {r['items_lost_never_processed']}, "
            f"IndexErrors {r['worker_index_errors']}")

    say("\n=== Summary: symptom -> unsynchronized code line (broken_server.py) ===")
    say(f"{'bug':<6} {'symptom':<42} {'lines':<12} {'per-trial counts'}")
    all_clean = True
    for tag, label, key in CHECKS:
        counts = [r[key] for r in results]
        all_clean &= all(c == 0 for c in counts)
        say(f"{tag:<6} {label:<42} {str(lines.get(tag, [])):<12} {counts}")
    say("\nRESULT: " + ("all invariants hold (no races observed)" if all_clean
                       else "races reproduced - see counts above"))

    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    path = os.path.join(HERE, "results", f"task1_{name}_report.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    print(f"\nreport written to {path}")


if __name__ == "__main__":
    main()
