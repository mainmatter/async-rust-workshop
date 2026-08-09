# Draining

Graceful shutdown is two mechanisms doing two different jobs.

**Stop taking new work**, by racing the accept against the token:

```rust
let accepted = tokio::select! {
    _ = shutdown.cancelled() => break,
    accepted = listener.accept() => accepted?,
};
```

`accept` is cancel safe, so losing that future when the token fires costs nothing: a connection that
had not been accepted yet simply stays in the kernel's backlog, and closing the listener means the
client gets a connection refused and can go elsewhere.

**Wait for the work in flight**, by spawning through a tracker:

```rust
connections.spawn(async move { ... });

// after the loop
connections.close();
connections.wait().await;

Ok(())
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
