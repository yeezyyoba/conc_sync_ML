"""
Task 2 - O(1) Thread-Safe LRU Embedding Cache
=============================================

Data structure
--------------
    Hash map (dict):   key -> Node                  O(1) lookup
    Doubly linked list with two sentinel nodes:

        HEAD <-> [most recent] <-> ... <-> [least recent] <-> TAIL

    * get / hit      : find node in dict, unlink it, relink right after HEAD
    * put / insert   : new node right after HEAD
    * evict          : remove the node right before TAIL (the LRU entry)
    All of these are a fixed number of pointer changes -> O(1).
    The sentinels mean there is never a "first node" / "last node" special
    case, so prev/next are never None for real nodes.

Locking modes
-------------
    mode="coarse"  one lock held for the WHOLE get_or_compute call, including
                   the slow embed() call. Simple and obviously correct, but
                   every miss blocks every other thread (hits included).

    mode="scoped"  the lock is held only for the fast dict/pointer work.
                   It is RELEASED while embed() runs, then re-acquired to
                   insert the result. Because the world may have changed while
                   we were unlocked, we RE-CHECK the map before inserting
                   (same idea as dict.setdefault in the slides).

    single_flight=True (scoped mode only): the first thread that misses a key
    marks it "in-flight"; other threads asking for the same key wait on a
    Condition (inside a while loop) instead of calling embed() again. This
    removes the cache stampede from Task 1 while still keeping embed()
    outside the lock.

No functools.lru_cache, no collections.OrderedDict - built from scratch.
"""
