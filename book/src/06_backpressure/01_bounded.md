# Refusing rather than queueing

Waiting for a full mailbox is the right default, and it is the wrong answer for a request that has a
deadline anyway. `REQUEST_LIMIT` is two seconds; if the queue is already full of work that will take
longer than that, the client is better off being told now.

```rust
pub async fn try_apply(&self, request: Request) -> Response {
    let (reply, answer) = oneshot::channel();

    match self.commands.try_send(Command { request, reply }) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => return Response::Error("busy".to_owned()),
        Err(TrySendError::Closed(_)) => {
            return Response::Error("the store is gone".to_owned());
        }
    }

    answer
        .await
        .unwrap_or_else(|_| Response::Error("the store is gone".to_owned()))
}
```

`try_send` returns instead of waiting, and its two errors mean opposite things. **Full** is temporary
and the client should try again. **Closed** means the store task is gone and trying again will not
help anybody. Collapsing them into one answer is the kind of shortcut that turns a five-minute
incident into an hour of confusion.

## Shedding is a real decision

`ERR busy` is the same string the timeout produces, and deliberately so: from the client's side both
mean "the store is not keeping up". What is different is the cost. A shed request costs nothing, and
a timed-out one costs two seconds of a connection task's time and, as the last chapter showed, gets
applied anyway.

The trade is worth stating plainly. Shedding early keeps latency bounded for the requests you do
accept, and it converts a queue that would have absorbed a burst into errors the client can see. A
store that only ever sheds is a store that is too small, and a store that never sheds under any load
has a queue that is too long.

Whatever you choose, count it. A shed request that is not in a metric is a service that looks
healthy while refusing half its traffic.

## Testing something that only happens under load

The test spawns eight requests at a store with room for one and half a second per request, and
asserts that some were refused and some were accepted:

```rust
let store = StoreHandle::spawn_with_capacity(Store::new(), SLOW, 1);
```

The assertion is deliberately not "exactly three were shed". With a paused clock the scheduling is
deterministic today, and pinning the exact number would make the test a hostage to Tokio's internal
ordering. Asserting the property, that shedding happens and that it does not shed everything, is
what you actually mean.
