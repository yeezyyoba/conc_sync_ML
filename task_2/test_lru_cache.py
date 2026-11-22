"""
Task 2 - Tests for the O(1) thread-safe LRU cache
=================================================

Run:  python test_lru_cache.py          (plain unittest, no extra packages)

Single-threaded tests prove the LRU logic. Multi-threaded tests hammer the
cache with hits, misses and evictions from many threads while a checker
thread keeps calling verify_integrity(), in BOTH locking modes.
"""
