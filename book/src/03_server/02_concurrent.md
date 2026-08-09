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

So for now each connection gets a `Store` of its own. That compiles, and it is worse than it looks:
`SET users alice hello` is answered `OK`, and the next client to ask for `alice` is told `NIL`,
because the write went into a `HashMap` that dies with the connection that made it. A server that
loses your data and says `OK` is worth meeting once, deliberately, and it is precisely the question
chapter 4 answers: **who owns the state when every connection is its own task?**

## Proving it, rather than hoping

The test opens a second connection while the first one is still open, and puts a deadline on the
answer:

```rust
let answered = timeout(Duration::from_secs(5), second.request("SET users bob hi")).await;
```

The one-client-at-a-time server from the last exercise never answers that, because it is still
inside `handle_connection` for the first client and stays there until that client hangs up. This is
how to test for concurrency rather than hope for it, and the deadline is the part worth copying: the
test fails in five seconds with a message rather than hanging forever, which is a courtesy worth
extending to your own suites.
