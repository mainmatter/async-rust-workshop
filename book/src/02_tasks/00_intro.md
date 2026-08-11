# Tasks

A task is a lightweight, non-blocking unit of execution, and its job is to drive one future to
completion. Lightweight because it is a small heap allocation rather than a thread; non-blocking
because it occupies a worker thread only for the length of a single poll. `tokio::spawn` creates one
from a future:

```rust
let handle = tokio::spawn(async move {
    store.get(&bucket, &key).cloned()
});

let value = handle.await.unwrap();
```

Three things change the moment you spawn.

**It runs whether or not you await it.** A future sitting in a variable is inert. A spawned task is
in the runtime's queue, and it makes progress as soon as there is a thread free. Awaiting the
`JoinHandle` waits for the result; dropping the handle does not stop the task.

**It has to be `Send + 'static`.** The task may be picked up by any worker thread, and it may outlive
whatever spawned it, so it cannot borrow from the caller and cannot hold anything that is not `Send`
across an `.await`. `Rc`, `RefCell`, and `MutexGuard` from `std` all fall foul of this, and the
compiler's error will point at the `.await` rather than at the value, which takes some getting used
to.

**It can fail on its own.** `handle.await` returns a `Result`, and the error case means the task
panicked or was aborted. A panicking task does not bring the process down; it quietly stops
existing, and the only way anybody finds out is by looking at its handle. Chapter 7 is about what to
do with that.

## Tasks are not threads

A task is a heap allocation and a state machine. Spawning one costs a few hundred bytes and no
system call, which is why a Tokio server can have a hundred thousand of them and would fall over
with a hundred thousand threads.

The tradeoff is that tasks are cooperative. A task only yields at an `.await`, so a task that does
not await does not give anything else a turn. On a multi-threaded runtime that means one worker
thread is stuck; if enough tasks do it, the whole runtime is. The next two exercises are the two
halves of that: how to get concurrency when you want it, and what to do with work that cannot yield.
