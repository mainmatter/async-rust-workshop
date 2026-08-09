# Retrying, and testing that it waited

The client connects once, and if nobody is listening it gives up. Every real client retries, and
every retry that does not back off turns one restarting server into a thundering herd.

```rust
pub async fn with_backoff<O, F, T, E>(attempts: u32, base: Duration, mut operation: O) -> Result<T, E>
where
    O: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
{
    let mut attempt = 1;

    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if attempt >= attempts => return Err(error),
            Err(_) => {
                sleep(base * 2u32.pow(attempt - 1)).await;
                attempt += 1;
            }
        }
    }
}
```

## Why the argument is a closure

Not `FnMut() -> Result<T, E>`, and not a single future.

A future runs once. Awaiting it consumes it, and there is no way to rewind it, so retrying means
asking for a _new_ one each time. That is what `FnMut() -> F` expresses: a thing that makes futures.

This shape turns up all over async Rust, in retry helpers, connection pools, anything that supervises,
and it is worth being able to write from memory. Note also that `F` is one type parameter, so every
call must produce the same future type, which an `async` block or a direct call satisfies and a
`match` returning two different futures does not. `Box::pin` is the escape hatch.

## The tests are the chapter

Four tests assert the exact elapsed time of a retry sequence, up to a second and a half of it, and
the suite finishes in microseconds:

```rust
assert_eq!(started.elapsed(), BASE_DELAY * 15);   // 100 + 200 + 400 + 800
```

No tolerance window, no `sleep` in the test, no flake on a loaded laptop. That is the deal a paused
clock offers: code that waits by awaiting a timer is testable to the millisecond.

Note what the tests pin down beyond the total. That the first attempt is immediate. That the wait
doubles rather than being constant. That there is no sleep after the last attempt, which is the
detail everybody's first implementation gets wrong and nobody's first test catches, because a
stray hundred milliseconds at the end of a retry loop is invisible unless you are measuring.

## What is missing from this backoff

Two things, deliberately, and both are worth adding in production code:

**Jitter.** A thousand clients that all fail at the same moment and all back off by exactly 100ms
retry at exactly the same moment. Randomising the delay (`base * 2^n * random(0.5..1.5)`, or the
full-jitter variant that picks uniformly from the whole interval) spreads the retry storm out.

**A cap.** Doubling forever reaches absurd delays quickly, and `2u32.pow(attempt - 1)` overflows at
33 attempts. Real backoff clamps to a maximum, usually tens of seconds.

Neither is in the exercise, because both make the arithmetic in the tests less obvious, which is a
trade worth understanding: this implementation is exactly testable because it is exactly
predictable, and adding jitter means the test has to assert on a range. That is a fair price, but it
should be a deliberate one.
