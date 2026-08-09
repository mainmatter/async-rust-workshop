# An actor

Stop sharing the store. Give it to a task, and hand everybody else a way to ask:

```rust
#[derive(Clone)]
pub struct StoreHandle {
    commands: mpsc::Sender<Command>,
}

pub struct Command {
    pub request: Request,
    pub reply: oneshot::Sender<Response>,
}

async fn run(mut store: Store, mut inbox: mpsc::Receiver<Command>) {
    while let Some(Command { request, reply }) = inbox.recv().await {
        let _ = reply.send(apply(request, &mut store));
    }
}
```

The loop owns `store` by value and holds `&mut` to it for as long as it likes, because nothing else
in the process can reach it. No lock, no `Arc`, no guard to think about.

## The two channels

**`mpsc`** carries requests in. Many senders, one receiver, and the receiver is the store task. Its
capacity is a decision with consequences, which is chapter 6.

**`oneshot`** carries the answer back. One value, one direction, allocated per request. The caller
holds the receiving half and awaits it:

```rust
pub async fn apply(&self, request: Request) -> Response {
    let (reply, answer) = oneshot::channel();

    if self.commands.send(Command { request, reply }).await.is_err() {
        return Response::Error("the store is gone".to_owned());
    }

    answer
        .await
        .unwrap_or_else(|_| Response::Error("the store is gone".to_owned()))
}
```

Both error paths mean the same thing: the store task is not there any more, either because the
channel is closed or because it died holding the `oneshot` sender. Neither can be ignored, and the
answer to both is to tell the client the truth. Chapter 7 asks whether the server should stay up at
all in that state.

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
load, more tasks, longer critical sections, a mix of reads and writes, and watching which way the
answer moves. The actor pays a message round trip per request and wins when the alternative is
tasks queueing on a lock; the mutex wins when the critical section is tiny and contention is low.

Benchmarks in async code are easy to get wrong. `criterion` measures wall-clock time around a block
you give it, so the block has to include the runtime work you care about and nothing else, and a
benchmark that spawns tasks is measuring the scheduler as much as your code. Treat the result as a
direction, not a fact.
