"""
Task 1 - Fixed Router (for comparison only)
===========================================

Same public API as BrokenInferenceRouter, with each bug fixed:

    BUG-1/2/4  telemetry   -> one metrics_lock around every read-modify-write
    BUG-3      cache       -> scoped locking + "single flight": the first thread
                              that misses marks the key as in-flight; others wait
                              on a Condition instead of calling embed() again.
                              embed() itself runs OUTSIDE the lock.
    BUG-5/6    queue       -> the capacity check and the append (or the
                              not-empty check and the pop) happen under ONE lock,
                              so check-then-act becomes a single atomic step.

Lock order (never nested here, but documented anyway):
    cache_cond  ->  queue_lock  ->  metrics_lock
"""
