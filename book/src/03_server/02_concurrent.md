# One task per connection

The loop you have serves one client at a time, because it waits for the connection it just accepted
before it accepts another:

```rust
let (stream, _) = listener.accept().await?;
handle_connection(stream, &mut store).await?;   // nothing else happens until this returns
```

Serving two clients at once is a two-line change, and the tool is the one from chapter 2:

```rust
tokio::spawn(future);   // -> JoinHandle<T>, and `future` must be Send + 'static
```

The accept loop then goes straight back to accepting, and each connection makes progress on a task
of its own. This is the shape of essentially every Tokio server, from this one to `hyper`.

Notice what spawning bought for free. An error on one connection now ends that task and nothing
else, because the `Result` is swallowed at the boundary rather than propagated into `serve`. That is
the right default: a connection's problems belong to that connection.

## And what it took away

`handle_connection` cannot borrow the store any more. `tokio::spawn` requires `Send + 'static`, and a
`&mut Store` is neither. There is no way to spawn a task that borrows a local, no matter how
obviously the local outlives it, because the compiler cannot see that and the runtime does not
promise it.

So this exercise ships without a store at all. The server parses requests, answers `ERR` to
everything, and proves that two clients are being read from at the same time. That is a deliberately
useless server, and its uselessness is precisely the question chapter 4 answers: **who owns the
state when every connection is its own task?**

## Interleaving, on purpose

The test drives two clients by hand:

```rust
first.send("SET users alice hello").await;
second.send("SET users bob hi").await;
first.response().await;
second.response().await;
```

Sequential code would deadlock here: the first client's response is not read until after the second
client's request has been sent, so a server that finishes one connection before starting the next
never gets there. This is how to test for concurrency rather than hope for it, and the test fails by
timing out rather than by hanging forever, which is a courtesy worth extending to your own test
suites.
