# Testing async code

Async code is hard to test for three reasons, and Tokio has an answer to each of them.

## It takes time

The idle timeout is thirty seconds and no suite can wait. Pause the clock:

```rust
#[tokio::test(start_paused = true)]
async fn a_client_that_says_nothing_is_hung_up_on() {
```

With time paused, the runtime auto-advances: whenever every task is blocked on a timer, it jumps
straight to the earliest deadline. Thirty seconds pass in microseconds, `Instant::now()` agrees they
did, and the assertion can be `==` rather than a tolerance window.

`tokio::time::advance(duration)` moves the clock by hand when you want the deadline to arrive at a
particular point in your test rather than as soon as possible.

The one thing that surprises everybody: **a paused clock does not notice work**. Time only moves when
the runtime decides it should, so a loop burning two million iterations takes exactly zero
nanoseconds as far as `Instant` is concerned. That is not a limitation, it is the property that makes
these tests deterministic. It also means you cannot use a paused clock to measure anything, only to
test behaviour that depends on time passing.

Anything that waits by _not_ awaiting a timer is invisible to all of this: `std::thread::sleep`
inside `spawn_blocking` sleeps for real. Which is one more reason to make waiting explicit.

## It needs a peer

For a server, the peer is a socket. Two ways to avoid the pain:

**Port zero.** `TcpListener::bind("127.0.0.1:0")` and then `local_addr()` gives a hermetic test that
can run in parallel with itself.

**No socket at all.** `tokio::io::duplex(1024)` returns two connected in-memory pipes implementing
`AsyncRead + AsyncWrite`. Hand one half to `handle_connection` and keep the other:

```rust
let (client, server) = tokio::io::duplex(1024);
tokio::spawn(async move { handle_connection(server, &store, IDLE).await });
```

This is why `handle_connection` is generic over its stream, and it is worth noticing that the
generic was not added for reuse: it was added for testability, and it is the reason the
cancel-safety test in chapter 5 could send twelve bytes, wait, and send the rest.

## It is concurrent

Which is to say the failure happens once in fifty runs on your machine and every time on
somebody else's. Most of the fix is in the design rather than the test: a single task owning the
store is testable in a way that a lock held across an await is not, because there is one place where
ordering is decided.

What helps in the tests themselves:

- Assert on properties, not on schedules. "Some were shed and some were accepted" survives a Tokio
  upgrade; "exactly three were shed" does not.
- Use `#[tokio::test]`'s default current-thread runtime when you want deterministic ordering, and
  `#[tokio::test(flavor = "multi_thread")]` when the thing you are testing is that it works when it
  is not.
- Bound anything that could hang with a `timeout`, so a broken implementation fails with a message
  instead of stopping CI.
- `yield_now().await` in a loop is a blunt but honest way to let spawned tasks reach their next
  await before you assert.
