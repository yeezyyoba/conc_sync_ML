# Task 4 benchmark results

Workload: 32 closed-loop clients x 40 requests, 70% hot prompts, bursty arrivals. embed() 5 ms, forward pass 20 ms + 1 ms/item, 4 workers, batch timeout 5 ms. Median of 3 runs.

## Comparison 1: coarse vs scoped locking on the LRU cache (batch = 8)

| Configuration | Throughput (req/s) | Avg latency (ms) | p95 latency (ms) | Cache hit rate | Mean batch / utilization | Clean (no lost) |
|---|---|---|---|---|---|---|
| Coarse lock | 553.6 | 55.0 | 82.1 | 0.69 | 4.10 / 0.51 | yes |
| Scoped lock | 882.3 | 32.7 | 43.3 | 0.71 | 6.70 / 0.84 | yes |

## Comparison 2: unbatched vs dynamically batched (scoped cache)

| Configuration | Throughput (req/s) | Avg latency (ms) | p95 latency (ms) | Cache hit rate | Mean batch / utilization | Clean (no lost) |
|---|---|---|---|---|---|---|
| Unbatched (max_batch_size=1) | 187.5 | 167.1 | 172.2 | 0.68 | 1.00 / 1.00 | yes |
| Batched (max_batch_size=8) | 882.3 | 32.7 | 43.3 | 0.71 | 6.70 / 0.84 | yes |

## Full grid

| Configuration | Throughput (req/s) | Avg latency (ms) | p95 latency (ms) | Cache hit rate | Mean batch / utilization | Clean (no lost) |
|---|---|---|---|---|---|---|
| coarse / batch 1 | 188.1 | 166.9 | 170.1 | 0.68 | 1.00 / 1.00 | yes |
| coarse / batch 8 | 553.6 | 55.0 | 82.1 | 0.69 | 4.10 / 0.51 | yes |
| scoped / batch 1 | 187.5 | 167.1 | 172.2 | 0.68 | 1.00 / 1.00 | yes |
| scoped / batch 8 | 882.3 | 32.7 | 43.3 | 0.71 | 6.70 / 0.84 | yes |

Throughput of each repeat (sorted): coarse/b1: [187.5, 188.1, 188.1]; coarse/b8: [541.9, 553.6, 589.4]; scoped/b1: [187.5, 187.5, 188.1]; scoped/b8: [878.5, 882.3, 888.7]
