"""
Task 4 - Integrated Concurrent ML Inference Pipeline
====================================================

    Client threads
         |  submit(prompt)
         v
    Router ---------> ThreadSafeLRUCache  (task_2)  embedding lookup / embed()
         |                [cache_lock, rank 10]  released before next step
         v
    DynamicBatchQueue (task_3)            bounded buffer, backpressure
         |                [queue_lock, rank 20]
         v
    Inference worker pool                 get_batch() -> one forward pass per batch
         |                [metrics_lock rank 30, result_lock rank 40]
         v
    ResultSlot.wait() wakes the client

Lock discipline (see lock_hierarchy.py)
    * Ranks: cache_lock(10) -> queue_lock(20) -> metrics_lock(30) -> result_lock(40)
    * Scopes are disjoint: no thread ever holds two of them at once.
    * Slow work (embed(), forward pass) never runs under any lock in
      scoped mode. Coarse cache mode holds cache_lock across embed() on
      purpose, as the baseline to benchmark against.

Request accounting (checked by stress_test.py)
    accepted             every request that entered Router.submit()
    completed            got a result from a worker
    rejected_on_shutdown refused because shutdown had started
    invariant            accepted == completed + rejected_on_shutdown
"""
