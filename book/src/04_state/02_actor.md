# An actor

Stop sharing the store. Give it to a task, and hand everybody else a way to ask. The two types the
exercise ships say the whole design:

```rust
#[derive(Clone)]
pub struct StoreHandle {
    commands: mpsc::Sender<Command>,
}

pub struct Command {
    pub request: Request,
    pub reply: oneshot::Sender<Response>,
}
```

The task behind that sender takes the store by value and holds `&mut` to it for as long as it likes,
because nothing else in the process can reach it. No lock, no `Arc`, no guard to think about.

## The two channels

**`mpsc`** carries requests in. Many senders, one receiver, and the receiver is the store task. Its
capacity is a decision with consequences, which is chapter 6.

**`oneshot`** carries the answer back. One value, one direction, allocated per request. The caller
makes the pair, sends the sending half away inside the command, and awaits the other half.

Four calls, and what each of them reports when things go wrong:

```rust
mpsc::channel(capacity);   // -> (Sender<T>, Receiver<T>); the Sender is Clone
inbox.recv().await;        // -> Option<T>; None once the last Sender has been dropped
oneshot::channel();        // -> (Sender<T>, Receiver<T>) for exactly one value
reply.send(value);         // -> Result<(), T>; Err when nobody is waiting any more
```

Two of those can tell you the store task is gone: sending a command can fail because the channel is
closed, and awaiting the answer can fail because the task died holding the `oneshot` sender. Both
mean the same thing, neither can be ignored, and the answer to both is to tell the client the truth.
Chapter 7 asks whether the server should stay up at all in that state.

## What changed for the caller

Nothing, which is the point. `handle_connection` calls `store.apply(request).await` and gets a
`Response`, exactly as it called `apply` under a lock. The handle is `Clone`, cheaply, because
cloning an `mpsc::Sender` is what "give this task access" means.

## Measuring it

The exercise ships a criterion benchmark comparing the mutex and the actor under contention:

```bash
cargo bench
```

It is not graded, and the number is not the lesson. What is worth doing is changing the shape of the
load and watching which way the answer moves. `TASKS` and `REQUESTS` at the top of
`benches/store.rs` are the two knobs: raise the first for more contention, the second for a longer
run per task. The actor pays a message round trip per request and wins when the alternative is tasks
queueing on a lock; the mutex wins when the critical section is tiny and contention is low.

Expect the mutex to win as it ships, because this workload is the one that suits it: the critical
section is a single `HashMap` insert, which is about as small as a critical section gets. That is a
real result rather than a rigged one, and it is why "use the actor" is not the moral of this chapter.

Benchmarks in async code are easy to get wrong. `criterion` measures wall-clock time around a block
you give it, so the block has to include the runtime work you care about and nothing else. Note what
the benchmark therefore does _not_ do: the store, and the task that owns it, are built once outside
the timed block, because a benchmark that allocates a channel and spawns a task on every iteration is
partly measuring how fast Tokio can start things. The fan-out stays inside, because that is the
workload. Treat the result as a direction, not a fact.
