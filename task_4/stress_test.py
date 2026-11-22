"""
Task 4 - Reliability stress test for the full pipeline
======================================================

Heavy multi-threaded load: 36 clients, mixed hit/miss traffic, bursty
arrivals, and a mid-run shutdown. Repeated for every combination of cache
locking mode and batch size so each code path is exercised.

For every run it verifies:
    * no deadlock: all clients and workers finish (watchdog dumps stacks if not)
    * accepted == completed + rejected_on_shutdown   (no lost requests)
    * client-side view matches server-side counters
    * no request completed twice
    * LRU cache verify_integrity() passes, size <= capacity
    * queue never exceeded its capacity
    * zero lock-order violations (RankedLock would raise)

Run:  python stress_test.py      (report -> results/stress_report.txt)
"""
