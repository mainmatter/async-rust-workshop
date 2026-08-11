# What the runtime actually does

This chapter has no code to write. It exists so that the vocabulary the rest of the day leans on
means the same thing to everyone in the room.

## A future is inert

An `async fn` does not run anything when you call it. It builds a value, and until something polls
that value, nothing happens at all:

```rust
let future = touch(&counter);   // counter is still 0
future.await;                   // now it is 1
```

This is the first thing that surprises people arriving from JavaScript, C#, or Python, where calling
an async function starts the work. In Rust the work is a value you own, and you decide when and
where it runs. Everything else in this chapter follows from that.

## `.await` is a suspension point

`.await` is not a call. It is a point at which this function is willing to be put down and picked up
again later, possibly on another thread, possibly minutes later, possibly never.

Everything you are holding when you reach an `.await` is held across that gap. That is where most of
the surprises in this workshop come from: a lock held across an await is a lock held for as long as
the wait takes, and a `!Send` value held across an await makes the whole future `!Send`.

## Concurrency is not parallelism

Two futures awaited one after the other take as long as both:

```rust
let first = slow_get(&store, &users, &alice).await;    // 50ms
let second = slow_get(&store, &users, &bob).await;     // 50ms, so 100ms in total
```

The same two handed to `join!` take as long as the slower one:

```rust
let (first, second) = tokio::join!(
    slow_get(&store, &users, &alice),
    slow_get(&store, &users, &bob),
);                                                     // 50ms
```

No threads were involved. Both futures are polled by the same task on the same thread, and while one
is waiting the other makes progress.

The saving comes out of the waiting, which is also where it stops. `join!` interleaves polls, it does
not add threads: two futures that compute for 50ms each and never suspend still take 100ms under it,
and the second one is not polled at all until the first returns. Concurrency is about structure,
parallelism is about hardware, and async Rust gives you the first whether or not you have the second.
Work that does not wait needs the second, which is the next chapter.

## The machinery, once

A `Future` is a trait with one method:

```rust
fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
```

The runtime calls `poll`. The future either returns `Ready(value)` and is done, or returns `Pending`
after arranging for `cx.waker()` to be called when there is a point in trying again. A future that
returns `Pending` without keeping the waker will never be polled again, which is the single most
common way to write a future that hangs.

`Pin` is there because an `async fn` compiles to a state machine that can hold references into
itself. Pinning is the promise that the value will not move, which is what makes those references
sound.

The exercise contains a `Future` implemented by hand, so the protocol is twenty lines you can read
rather than something you take on trust. Put an `eprintln!` in its `poll` and run
`cargo test -- --nocapture` if you would rather watch it than read it. That is the only `poll` in
this workshop. From here on, the runtime does it, and the day is about the
decisions you still have to make: who owns the state, what happens when a future is dropped
halfway, and what your server does when it cannot keep up.

## Where the work runs

A **task** is a lightweight, non-blocking unit of execution that drives one future to completion.
`tokio::spawn` makes one, and from that moment it makes progress whether or not anybody awaits it. A
**future** is just a value. The next chapter is about the difference.
