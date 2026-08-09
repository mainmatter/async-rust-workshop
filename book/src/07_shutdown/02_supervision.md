# Supervision

The store task is the one task in `minidb` that nothing can replace. If it dies, every handle to it
answers `ERR the store is gone`, forever, and the server keeps accepting connections so that it can
keep saying so.

A process that is up but cannot do anything is worse than one that is down. Nothing restarts it, and
every liveness check that only asks whether the port is open reports success.

The accept loop you wrote in the last exercise watches two things, and the store is not one of them:

```rust
tokio::select! {
    _ = shutdown.cancelled() => /* ... */,
    accepted = listener.accept() => /* ... */,
}
```

`StoreHandle` now exposes the third thing it could be watching:

```rust
store.closed().await;   // mpsc::Sender::closed: resolves once the Receiver has been dropped
```

The receiver is dropped when the store task ends, whether it returned normally or panicked, so this
is one await that answers "is the thing I depend on still alive". What `serve` should do when it
resolves is the exercise.

## Dying on purpose

`serve` returns an error, `main` returns it, the process exits non-zero, and systemd or Kubernetes
or a shell loop starts it again. That is supervision: not a framework, just a decision about which
failures are fatal and something outside the process that notices.

Choosing to die is a real answer and usually the right one when the state that was lost lived in the
task that died. `minidb` could not restart the store task with the data intact even if it tried,
because the data was in it. After chapter 9 it could: the log on disk is what makes a restart
recover rather than forget.

## Restarting instead

When the dead task is stateless or its state is recoverable, supervise it by holding the handle:

```rust
loop {
    match handle.await {
        Ok(()) => break,
        Err(error) if error.is_panic() => {
            warn!(?error, "worker panicked, restarting");
            handle = tokio::spawn(worker());
        }
        Err(_) => break,   // aborted
    }
}
```

`JoinError` distinguishes a panic from an abort, which is the difference between "something broke"
and "we asked it to stop". Restart loops need a backoff and a give-up count, or a task that panics
on startup becomes a busy loop that panics several thousand times a second.

## Panics in connection tasks

Those are fine to swallow, and the semaphore permit from chapter 6 is why: it is released by `Drop`,
so a panicking connection gives its permit back on the way out. That is the general defence. Anything
that must happen when a task ends should hang off a destructor rather than off the last line of the
task body, because the last line is exactly what a panic skips.

If you want a service to be loud about it, `std::panic::set_hook` at startup gets you one place where
every panic in the process is logged, with a backtrace, before unwinding starts.
