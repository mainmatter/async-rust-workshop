# Shutdown and supervision

A server that cannot be stopped cleanly is a server that loses a request on every deploy. This
chapter is about the two ends of a process's life going wrong: being asked to stop, and having
something stop without being asked.

## Being asked to stop

`SIGTERM` arrives, from Kubernetes or systemd or somebody's Ctrl-C. What should happen is:

1. Stop accepting new connections.
2. Let the connections in flight finish, within reason.
3. Let the store task drain its mailbox.
4. Exit.

What happens by default is that `main` returns and the process ends, taking every task with it,
mid-request. The runtime does not wait for spawned tasks and does not run their destructors.

Two tools from `tokio-util` do most of the work:

**`CancellationToken`** is a broadcast "please stop". It is cheap to clone, `cancelled()` is a future
any task can select on, and cancelling is idempotent. It is cooperative: it asks, and each task
decides where it is safe to stop.

**`TaskTracker`** is a `JoinSet` that does not own the results. Spawn through it, then `close()` it
and `wait()` for everything spawned to finish. That is the draining step, and doing it by hand with a
`Vec<JoinHandle>` is where the bugs live.

Between them they cover both halves: cancellation ends the accept loop, and the tracker drains what
is already running. Neither knows about the other, which is why the ordering is yours to get right.

## Something stopping without being asked

A panicking task does not bring the process down. It unwinds, its `JoinHandle` starts returning
`Err(JoinError)`, and if nobody is holding that handle, nothing anywhere notices.

For a connection task that is exactly right: one client's bad day is not everybody's. For the store
task it is a disaster, because `minidb`'s entire state was in it. What is left is a process that
still accepts connections, still answers, and answers `ERR the store is gone` to every request
forever. Every health check that asks "is the port open" says yes.

Rust has no supervisor tree, so somebody has to await the thing that matters and decide what its
death means. Here, dying is the right answer: exit, and let whatever supervises the process do its
job. `mpsc::Sender::closed` is how you find out, because the receiver is dropped when the store task
ends, whether it returned or panicked.

## The question to ask about every task

For each `tokio::spawn` in a codebase: **who finds out if this dies, and what do they do about it?**

Three answers are legitimate. Nobody needs to know, and the work is genuinely optional. Someone holds
the handle and restarts it. Or its death is fatal and the process should end. What is not legitimate
is not having asked, which is the default for every spawn ever written in a hurry.
