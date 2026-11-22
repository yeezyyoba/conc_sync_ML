"""
Task 3 - Bounded Dynamic Inference Batching Queue
=================================================

Producers (client/router threads) call put(); inference workers call
get_batch() to pull up to max_batch_size requests at once, because a GPU
forward pass on 8 requests costs much less than 8 separate passes.

Storage
-------
A fixed-size ring buffer (circular array) built from a plain list:
    head  -> index of the oldest item
    count -> number of items stored
    enqueue at (head + count) % capacity, dequeue at head.
Both operations are O(1) and the buffer can never physically hold more
than `capacity` items. (No queue.Queue, no collections.deque.)

Synchronization
---------------
One Lock, two Conditions that share it:
    not_full   - producers wait here while the buffer is full (backpressure)
    not_empty  - workers wait here for items / a full batch / shutdown
Every wait() sits inside a `while` loop that re-checks the condition,
because another thread can grab the slot or the items between the
notify and the moment the woken thread re-acquires the lock.
Nobody ever sleeps or polls: threads block in wait() using 0% CPU.

get_batch(max_batch_size, timeout_s) returns when ANY of these is true:
    1. at least max_batch_size items are queued        -> full batch
    2. the monotonic deadline has passed and >= 1 item -> partial batch
    3. shutdown() was called                           -> drain what is left
If the deadline passes while the queue is empty, the worker keeps waiting
(without a timeout) for the first item and returns it as soon as it lands.
time.monotonic() is used because it never jumps when the wall clock changes.

shutdown() sets a flag and notify_all()s both conditions:
    * blocked and future put() calls raise QueueShutdown (rejected)
    * workers keep receiving the remaining items (clean drain)
    * once the buffer is empty, get_batch() returns [] -> worker exits
"""
