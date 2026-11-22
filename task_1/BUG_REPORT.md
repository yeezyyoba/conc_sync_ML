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
