# Draining

`serve` today is a loop with no way out of it. It accepts, it spawns, it goes round again, and the
only thing that ever ends it is the process ending:

```rust
loop {
    let (stream, _) = listener.accept().await?;   // nothing here is watching for a reason to stop
    // ...
}
```

Graceful shutdown is three mechanisms doing three different jobs.

**Stop taking new work**, by racing the accept against the token:

```rust
shutdown.cancelled().await;   // resolves once anybody has called cancel(), and stays resolved
```

`accept` is cancel safe, so losing that future when the token wins costs nothing: a connection that
had not been accepted yet simply stays in the kernel's backlog, and closing the listener means the
client gets a connection refused and can go elsewhere.

**Wait for the work in flight**, by spawning through a tracker instead of through `tokio::spawn`:

```rust
connections.spawn(future);    // same as tokio::spawn, but counted
connections.close();          // no more will be added
connections.wait().await;     // resolves when every counted task has finished
```

`close()` says no more tasks will be added; `wait()` resolves when every task spawned through it has
finished. Without the `close()`, `wait()` never returns, which is the single most common way to get
this wrong.

## Every await in the loop, not just the interesting one

The accept is not the only place this loop stops. Chapter 6 put a `Semaphore` in front of it, and at
capacity the loop parks on the acquire instead, where nothing is watching the token. Cancel then, and
the server carries on waiting for a permit; when one frees, `select!` picks between a ready
`cancelled()` and a ready `accept()` at random, so a server that was told to stop accepts roughly
half the clients that turn up anyway. One of the tests says so, with a limit of zero, because a
server that has no permits to hand out can only leave that loop by looking at the token.

The general point is worth more than the fix: **cancellation is a property of the whole loop, not of
one await in it.** Every point where an iteration can block is a point where a shutdown can be
missed, and the only way to find them is to go through them one at a time and ask what is watching.

## What "finish what they are doing" means

The connection tasks are not cancelled, but they are told. `handle_connection` takes the token too,
and its `select!` gains one arm:

```rust
let line = tokio::select! {
    line = requests.next_line() => line?,
    _ = housekeeping.tick() => continue,
    _ = &mut idle_deadline => return Ok(()),
    _ = shutdown.cancelled() => return Ok(()),   // stop waiting for another request
};
```

Which await that arm cancels is the whole design. It cancels the wait for the _next_ request, and
nothing else. A request already parsed is applied and answered below the `select!`, untouched, so a
client mid-request still gets its answer. What it does not get is another turn.

Without that arm, the unit of work is the connection, and a client that sends a request every twenty
seconds keeps the drain going forever. With it, the unit of work is the request, and the drain is
bounded by the slowest single request rather than by client behaviour. That is what HTTP servers do
when they answer `Connection: close` instead of reusing a keep-alive connection.

Then bound the waiting anyway. `GRACE` is the constant, `timeout` is the tool, and after it expires
whatever is left is dropped on the floor. Every await in this exercise is already bounded, so
nothing should reach that deadline; the case it exists for is the one nobody bounded, such as
`write_all` to a client that has stopped reading, where the kernel's send buffer fills and the write
never completes. Kubernetes gives a pod `terminationGracePeriodSeconds`, thirty by default, before
`SIGKILL`, so your own grace period wants to sit comfortably under whatever that is set to.

Note which way round the three mechanisms go. The token stops the loop taking new connections and
stops each connection taking new requests; the deadline stops the drain taking forever. A shutdown
with only the first hangs on its slowest client, and one with only the last cuts off work it had
already accepted.

## Ordering

The order matters and follows the data:

1. Stop accepting.
2. Drain the connections. They are the only things that talk to the store.
3. Then the store task, which ends by itself once every `StoreHandle` has been dropped, because the
   channel closes when the last sender goes.

That last point is worth dwelling on. The store task's lifetime is managed entirely by ownership of
its senders. Hold a `StoreHandle` in a global, or in a struct that outlives the drain, and the store
task will never see its channel close and shutdown will hang. When shutdown hangs, look for the
handle nobody dropped.

## Where the signal comes from

In production the token gets cancelled by a signal handler:

```rust
tokio::spawn(async move {
    let _ = tokio::signal::ctrl_c().await;
    shutdown.cancel();
});
```

`tokio::signal` also has `unix::signal(SignalKind::terminate())` for `SIGTERM`, which is what
orchestrators actually send. The exercise passes the token in from the test instead, which is the
same thing with a more convenient source.
