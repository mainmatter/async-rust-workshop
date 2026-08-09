# An idle timeout

A connection that says nothing is not free. It holds a task, a buffer, a file descriptor, and a slot
in whatever limit you set later. The loop you have waits for the client for as long as the client
feels like taking:

```rust
requests.next_line().await?   // -> Some(line), eventually, or never
```

Servers close those connections, and the tool for it is a macro that waits for the first of several
futures rather than for one:

```rust
tokio::select! {
    a = first  => /* first finished, `second` was dropped where it stood */,
    b = second => /* and the other way round */,
}
```

`select!` polls every branch and takes whichever finishes first. The branch that did not win has its
future dropped, which is the entire subject of this chapter: a read that loses to a timer is a read
that never happened.

## The two ways a connection ends

They are different and both have to be handled:

- `next_line()` returns `Ok(None)`: the client hung up politely.
- The sleep fires: the client is still connected and has said nothing for `idle`.

Returning `Ok(())` for both is right. Neither is an error, and neither deserves a log line above
DEBUG.

## `select!` is a macro with sharp edges

Three things about it are worth knowing before you need them:

**Every branch is polled every time.** That means every branch's expression is evaluated on every
iteration, including `sleep(idle)`, which builds a new timer each time round the loop. Sometimes that
is what you want; for a deadline it is a bug, which the next exercise makes you fix.

**A branch that is not taken has its future dropped.** See the previous page, and the next one.

**Branch order does not decide the winner.** `select!` polls in a random order by default,
specifically so that a branch that is always ready cannot starve the others. If you need
priority, `biased;` turns the randomisation off, and then a hot branch really can starve the rest.

## Testing it without waiting

The limit is thirty seconds and the test suite runs in milliseconds:

```rust
#[tokio::test(start_paused = true)]
async fn a_client_that_says_nothing_is_hung_up_on() {
```

With the clock paused, the runtime advances time itself as soon as every task is waiting on a timer.
Thirty seconds pass instantly, and `Instant::now()` inside the test agrees that they did. Chapter 8
is about this and what else it makes possible.

One habit from these tests: wrap the thing you expect to finish in a `timeout` anyway.

```rust
timeout(IDLE * 2, client.lines.next_line()).await.expect("the server never hung up")
```

A test that fails by hanging tells you nothing and blocks CI. A test that fails with a message tells
you what it wanted.
