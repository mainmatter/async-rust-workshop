# Draining

`serve` today is a loop with no way out of it. It accepts, it spawns, it goes round again, and the
only thing that ever ends it is the process ending:

```rust
loop {
    let (stream, _) = listener.accept().await?;   // nothing here is watching for a reason to stop
    // ...
}
```

Graceful shutdown is two mechanisms doing two different jobs.

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

## Draining is not cancelling

Notice that the connection tasks are not cancelled. They are left alone to finish what they are
doing, and the process waits for them. A client mid-request gets its answer.

Which is right until one connection decides to stay for an hour. Real shutdown has a deadline:

```rust
let _ = timeout(GRACE, connections.wait()).await;
```

and after the grace period, whatever is left is dropped on the floor. Kubernetes gives a pod
`terminationGracePeriodSeconds` (30 by default) before `SIGKILL`, so your own grace period should be
comfortably under whatever that is set to. This exercise leaves the deadline out to keep the test
honest about what it is testing; adding it is three lines and worth doing in anything real.

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
