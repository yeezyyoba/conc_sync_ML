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

import threading


class CacheCorruptionError(AssertionError):
    """Raised by verify_integrity() when an invariant is broken."""


class _Node:
    __slots__ = ("key", "value", "prev", "next")

    def __init__(self, key=None, value=None):
        self.key = key
        self.value = value
        self.prev = None
        self.next = None


class ThreadSafeLRUCache:
    def __init__(self, capacity, embed_fn=None, mode="scoped",
                 single_flight=True, lock=None):
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        if mode not in ("coarse", "scoped"):
            raise ValueError("mode must be 'coarse' or 'scoped'")
        self.capacity = capacity
        self.embed_fn = embed_fn
        self.mode = mode
        self.single_flight = single_flight and mode == "scoped"

        # One lock protects map + list + counters. The Condition shares that
        # lock and is used only by single-flight waiters.
        self._lock = lock if lock is not None else threading.Lock()
        self._cond = threading.Condition(self._lock)

        self._map = {}
        self._head = _Node("<HEAD>")
        self._tail = _Node("<TAIL>")
        self._head.next = self._tail
        self._tail.prev = self._head
        self._inflight = set()

        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.embed_calls = 0

    # ------------------------------------------------------------------ #
    # Linked-list primitives (CALLER MUST HOLD self._lock)
    # ------------------------------------------------------------------ #
    def _unlink(self, node):
        node.prev.next = node.next
        node.next.prev = node.prev
        node.prev = node.next = None

    def _push_front(self, node):
        first = self._head.next
        node.prev = self._head
        node.next = first
        self._head.next = node
        first.prev = node

    def _move_to_front(self, node):
        self._unlink(node)
        self._push_front(node)

    def _evict_if_needed(self):
        while len(self._map) > self.capacity:
            lru = self._tail.prev            # least recently used
            self._unlink(lru)
            del self._map[lru.key]
            self.evictions += 1

    def _insert_or_update(self, key, value):
        node = self._map.get(key)
        if node is not None:                 # already present: update + refresh
            node.value = value
            self._move_to_front(node)
            return
        node = _Node(key, value)
        self._map[key] = node
        self._push_front(node)
        self._evict_if_needed()

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def get(self, key):
        """Return the cached value (and mark it most-recent) or None."""
        with self._lock:
            node = self._map.get(key)
            if node is None:
                self.misses += 1
                return None
            self.hits += 1
            self._move_to_front(node)
            return node.value

    def put(self, key, value):
        with self._lock:
            self._insert_or_update(key, value)

    def get_or_compute(self, key):
        """Return (value, was_hit). On a miss, calls embed_fn(key)."""
        if self.embed_fn is None:
            raise ValueError("embed_fn is required for get_or_compute")
        if self.mode == "coarse":
            return self._get_or_compute_coarse(key)
        return self._get_or_compute_scoped(key)

    def _get_or_compute_coarse(self, key):
        with self._lock:                     # held across embed() on purpose
            node = self._map.get(key)
            if node is not None:
                self.hits += 1
                self._move_to_front(node)
                return node.value, True
            self.misses += 1
            value = self.embed_fn(key)       # SLOW WORK UNDER THE LOCK
            self.embed_calls += 1
            self._insert_or_update(key, value)
            return value, False
