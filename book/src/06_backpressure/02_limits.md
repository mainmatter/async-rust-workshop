# Admission control

The mailbox is bounded and the number of connections is not. Every accepted socket is a task, a
buffer, and a file descriptor, and `serve` will happily accept ten thousand of them. Nothing in the
loop counts:

```rust
let (stream, _) = listener.accept().await?;   // ... and again, and again
```

`Semaphore` holds a fixed number of permits:

```rust
Semaphore::new(limit);                            // an Arc<Semaphore> is what tasks share
Arc::clone(&permits).acquire_owned().await;       // -> Result<OwnedSemaphorePermit, AcquireError>
```

Take one before serving a connection, hold it until the connection ends, and the count of live
connections cannot exceed the limit. `acquire_owned` rather than `acquire` because the permit has to
be moved into the task, and the permit is released by its own `Drop` rather than by any call.

## Where you acquire it is the design

Acquire **before** `accept` and the listener stops taking connections off the kernel's
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

The permit's `Drop` is the whole release mechanism. There is no `release` call, so the slot comes
back exactly when the permit dies, which means a connection that ends any way at all, including by
panicking, returns its permit.

That makes where the permit lives the only thing that matters, so hand it to `handle_connection`:

```rust
pub async fn handle_connection<S>(
    stream: S,
    store: &StoreHandle,
    idle: Duration,
    _permit: OwnedSemaphorePermit,   // held for the length of the call, dropped when it returns
) -> io::Result<()>
```

Nothing in the body uses it, hence the underscore prefix, which silences the warning while leaving
it an ordinary binding. Written this way the signature states the rule the code was only implying,
and a caller that forgets the permit is a compile error rather than a limit that quietly does
nothing.

The alternative is to park it in the spawned block with `let _permit = permit;`, which works and is
one character away from not working: `let _ = permit;` is not a binding at all, so it drops the
permit on the spot and the limit stops existing silently.

## Choosing the number

`MAX_CONNECTIONS = 128` is, again, a guess with a meaning. Multiply it by the per-connection memory
(a buffer, a task, and whatever the handler allocates) to get the worst case, and check that against
the memory you have. Do the same for file descriptors, because `ulimit -n` is often 1024 and
`accept` failing with `EMFILE` is a spectacularly confusing failure mode.

The limits in this chapter compose into a pattern worth naming. Bound the number of connections, so
memory is bounded. Bound the mailbox, so queueing delay is bounded. Bound the time per request, so
no single one can hold a slot forever. Any one of those alone leaves a way to fall over.
