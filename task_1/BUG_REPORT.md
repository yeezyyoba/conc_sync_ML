# Task 1 – Bug Report: Broken Baseline Inference Router

**Reproduce:** `python stress_test.py` (30 clients × 40 requests, 4 workers, 3 seeded trials).
**Compare with the fix:** `python stress_test.py --fixed` → every count is 0.
Full logs: `results/task1_broken_report.txt`, `results/task1_fixed_report.txt`.

## Observed symptoms (committed run, `results/task1_broken_report.txt`)

| Bug | Symptom | Trial 1 | Trial 2 | Trial 3 |
|---|---|---|---|---|
| BUG-1 | `total_requests` lost updates (expected 1200) | 1027 | 1022 | 1028 |
| BUG-2 | `total_tokens` lost updates | 5444 | 5457 | 5441 |
| BUG-3 | Duplicate `embed()` calls for 12 unique prompts | 21 | 27 | 28 |
| BUG-4 | `cache_hits` lost updates | 990 | 991 | 988 |
| BUG-5 | Queue length above capacity 10 (max seen ≈ 20) | 216 | 223 | 240 |
| BUG-6 | Requests processed twice | 192 | 179 | 203 |
| BUG-6 | Requests lost (accepted, never processed) | 192 | 179 | 203 |
| BUG-6 | Worker `IndexError` crashes | 6 | 5 | 5 |

Exact numbers change from run to run (that is what a race is), but every run shows every symptom.

## Symptom → exact line in `broken_server.py`

| Bug | Line(s) | Code | Race type | Why it breaks |
|---|---|---|---|---|
| BUG-1 | 66 → 68 | `current = self.total_requests` … `self.total_requests = current + 1` | read-modify-write | Two threads read the same value, both write value+1, one increment vanishes. This is exactly what `+=` does in bytecode (LOAD → ADD → STORE). |
| BUG-2 | 71 → 73 | same pattern on `total_tokens` | read-modify-write | Same lost update, but each lost write drops several tokens at once. |
| BUG-3 | 76 → 80 | `if prompt not in self.cache:` … `self.cache[prompt] = vec` | check-then-act | Many threads see the miss before the first one writes back, so all of them call the slow `embed()` (cache stampede) and overwrite each other's entry. |
| BUG-4 | 82 → 84 | `hits = self.cache_hits` … `self.cache_hits = hits + 1` | read-modify-write | Hit counter loses updates, so the hit rate is wrong. |
| BUG-5 | 87 → 89 | `if len(self.queue) < self.queue_capacity:` … `self.queue.append(...)` | check-then-act | Several producers pass the capacity check while the queue holds 9 items, then all append: the "bounded" queue reaches ~20. |
| BUG-6 | 104 → 107 → 109 | `if self.queue:` … `item = self.queue[0]` … `del self.queue[0]` | check-then-act + non-atomic peek/remove | Two workers peek the same front item (processed twice), then both delete, so the next item is removed without ever being processed (lost). When the list empties between check and peek, `IndexError`. |
