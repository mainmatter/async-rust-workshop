# Admission control

The mailbox is bounded and the number of connections is not. Every accepted socket is a task, a
buffer, and a file descriptor, and `serve` will happily accept ten thousand of them.

```rust
let permits = Arc::new(Semaphore::new(max_connections));

loop {
    let permit = Arc::clone(&permits).acquire_owned().await.expect("never closed");
    let (stream, _) = listener.accept().await?;
    let store = store.clone();

    tokio::spawn(async move {
        let _permit = permit;
        let _ = handle_connection(stream, &store, IDLE_LIMIT).await;
    });
}
```

`Semaphore` holds a fixed number of permits. Take one before serving a connection, hold it until the
connection ends, and the count of live connections cannot exceed the limit.

## Where you acquire it is the design

Acquire **before** `accept`, as above, and the listener stops taking connections off the kernel's
backlog when it is at capacity. A client that cannot be served yet waits in the backlog queue,
which is the operating system's memory rather than yours, and if the backlog fills the kernel refuses
the connection outright. That is a fast, cheap "no" that never reaches your process.

Acquire **after** `accept` and you have accepted a connection you cannot serve. Now it is your
socket, your task, and your memory, waiting for a permit, and you have moved the queue from the
kernel into your heap. That is the version that looks fine and falls over.

The general shape: **refuse work as early as you can**, at the outermost edge where you still know
enough to refuse it.

## Why `acquire_owned`

`Semaphore::acquire` returns a guard that borrows the semaphore, which cannot be moved into a
spawned task. `acquire_owned` takes an `Arc<Semaphore>` and returns a permit that owns its share, so
it can go into the `async move` block.

`let _permit = permit;` is the whole release mechanism. The permit lives as long as the task, and its
`Drop` gives it back, so a connection that ends any way at all, including by panicking, returns its
permit. Binding it to `_permit` rather than `_` matters: `let _ = permit;` drops it immediately, and
the limit quietly stops existing.

## Choosing the number

`MAX_CONNECTIONS = 128` is, again, a guess with a meaning. Multiply it by the per-connection memory
(a buffer, a task, and whatever the handler allocates) to get the worst case, and check that against
the memory you have. Do the same for file descriptors, because `ulimit -n` is often 1024 and
`accept` failing with `EMFILE` is a spectacularly confusing failure mode.

The limits in this chapter compose into a pattern worth naming. Bound the number of connections, so
memory is bounded. Bound the mailbox, so queueing delay is bounded. Bound the time per request, so
no single one can hold a slot forever. Any one of those alone leaves a way to fall over.
