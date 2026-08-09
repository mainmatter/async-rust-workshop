# Work that will not yield

Async Rust is cooperative. A task keeps its worker thread until it hits an `.await` that returns
`Pending`, and a task that never does that never gives the thread back:

```rust
pub fn checksum(store: &Store) -> u64 {
    // a few hundred milliseconds of pure computation, no awaits anywhere
}
```

Call that from an async function and the worker thread running it stops polling anything else for
as long as it takes. On the current-thread runtime, that is the entire server. On the multi-threaded
runtime it is one worker out of however many cores you have, which is worse in a way, because it
shows up as a service that is fine in testing and stalls under load.

The symptom is distinctive: everything gets slower at once, including work that has nothing to do
with the slow part, and a heartbeat task that should tick every ten milliseconds stops ticking.

## `spawn_blocking`

Tokio keeps a second, much larger pool of threads for exactly this:

```rust
tokio::task::spawn_blocking(move || /* the work that will not yield */)
    .await   // -> Result<T, JoinError>
```

The closure runs on a blocking thread, the calling task awaits the result and yields while it waits,
and the runtime's workers keep serving everybody else. The pool is large (512 threads by default)
because those threads are expected to be blocked most of the time.

`spawn_blocking` needs an owned, `'static` closure, so the store arrives as an `Arc<Store>` again.

## What counts as blocking

Anything that can take more than a moment and does not `.await`:

- CPU-bound work: hashing, compression, serialising something large, image processing.
- Synchronous file I/O. This is why `tokio::fs` exists, and why it is a wrapper around
  `spawn_blocking` rather than anything cleverer. Chapter 9 comes back to that.
- Any library that talks to the network or the disk without being async, which is most C bindings.
- `std::thread::sleep`, which is never what you want inside an async function. `tokio::time::sleep`
  is.

## The rules of thumb

A rough line: if a piece of work can take longer than about a hundred microseconds and cannot yield,
it does not belong on a runtime thread.

For genuinely CPU-heavy work that is the _point_ of your service, `spawn_blocking` is a blunt
instrument, and a dedicated `rayon` pool with a channel back into async code is the usual answer. The
principle is the same either way: the runtime's threads exist to poll futures, and anything else you
ask them to do is time they are not doing that.
