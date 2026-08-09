# Cancellation

In most languages, cancelling work means asking it to stop and hoping it checks. In Rust it means
dropping a future, and it happens immediately and everywhere:

```rust
tokio::select! {
    response = store.apply(request) => response,
    _ = sleep(limit) => Response::Error("busy".to_owned()),
}
```

When the sleep wins, the other future is dropped where it stands. Not signalled. Not asked. Dropped,
mid-await, with whatever it was holding.

This is the best and the sharpest thing about async Rust. Best, because cancellation is free and
composable: `timeout`, `select!`, and dropping a `JoinHandle`'s task all work on any future, without
that future having been written to support them. Sharpest, because a future that is dropped at an
awkward moment leaves the world in whatever state it had reached.

## Where the state goes

Dropping a future runs `Drop` for everything the state machine holds, so memory and locks and file
handles are all released correctly. What is not automatic is anything that had already reached the
outside world:

- Bytes already written to a socket have been written. Half a response is a real thing a client can
  receive.
- A row already inserted stays inserted. There is no rollback unless you wrote one.
- Bytes already read out of a socket into a buffer inside the dropped future are gone, and this is
  the one that catches people. It is the subject of the second exercise.

## The three tools

**`timeout(duration, future)`** wraps a future and drops it if it takes too long. The result is a
`Result<T, Elapsed>`, and it is worth being precise about what `Err(Elapsed)` means: it means _you
stopped waiting_, not that the work stopped happening. If the work was a message to another task,
that task is still going to do it.

**`select!`** polls several futures and takes the first one that finishes, dropping the rest. Every
branch is a cancellation point for the others.

**`CancellationToken`** from `tokio-util` is cooperative cancellation for cases where dropping is not
enough, typically because you want many tasks to stop at a point of their own choosing. Chapter 7
uses it.

## Cancel safety

A future is **cancel safe** if dropping it part-way loses nothing that cannot be recovered by calling
it again. This is a property of the API you are calling, not of your code, and Tokio documents it
per method under a "Cancel safety" heading.

`AsyncBufReadExt::next_line` is cancel safe: the bytes it has read live in the `BufReader`, which is
yours and outlives the call. A read loop that accumulates into a local `Vec` is not: the `Vec` is
inside the future, and dropping the future drops the bytes.

The rule to take home: before putting a call in a `select!` branch, check whether its documentation
says it is cancel safe. If it does not say, or if you wrote it, assume it is not.
