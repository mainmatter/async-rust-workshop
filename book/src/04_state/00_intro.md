# Who owns the state

Every connection is a task, and every task wants the same `Store`. Rust will not let more than one
of them have `&mut Store`, and it is right not to. There are two answers, and this chapter builds
both.

## Share it: `Arc<Mutex<Store>>`

```rust
let store = Arc::new(Mutex::new(Store::new()));

let mut guard = store.lock().await;
guard.insert(bucket, key, value);
```

`Arc` gives every task a handle to the same allocation; the mutex makes sure only one of them is
inside at a time. It is the direct translation of what you would write with threads, and for a lot
of services it is the correct answer.

The trap is which mutex. `std::sync::Mutex` is fine to use in async code as long as its guard never
crosses an `.await`, and using it that way is often the fastest option, since the lock is
uncontended and never held for long. Hold that guard across an await and you have a problem the
compiler will describe badly: `MutexGuard` is `!Send`, so the whole future becomes `!Send`, so
`tokio::spawn` refuses it, and the error points at the spawn rather than at the lock.

`tokio::sync::Mutex` is the one whose guard may be held across an await. It is slower, because
waiting on it means parking a task rather than spinning, and reaching for it is often a sign that
the critical section is bigger than it should be.

The other trap has nothing to do with types. A lock held while doing I/O serialises every task in
the process on that I/O, and no amount of async makes that faster.

## Give it away: the actor

The alternative is to stop sharing. One task owns the `Store` outright, and everybody else sends it
messages:

```rust
pub struct StoreHandle {
    commands: mpsc::Sender<Command>,
}

struct Command {
    request: Request,
    reply: oneshot::Sender<Response>,
}
```

There is no lock, because there is nothing to lock: exactly one task ever touches the data, so it can
hold `&mut Store` for as long as it likes. Callers get a cheap `Clone` handle and `await` their
answer.

What this buys is more than tidiness:

- **The queue is a place to put policy.** A bounded mailbox is backpressure (chapter 6), the depth
  is a metric worth watching, and a request can be refused before it is queued.
- **It is testable.** The store task has one input and one output, and both are channels.
- **It composes with cancellation.** Dropping the caller drops the `oneshot`, and the store task can
  see that nobody is listening any more.

What it costs is a round trip per request, an allocation per reply, and a single task that is now a
bottleneck and a single point of failure. Chapter 7 is about the second of those.

## Which one

Use the mutex when the critical section is small, synchronous, and contention is low. Use the actor
when the state has behaviour of its own, when you want a queue you can reason about, or when the
work under the lock would otherwise involve an `.await`.

The exercises build both, and the chapter ends with a criterion benchmark that measures them under
contention, because the honest answer to which is faster is "measure it, and the answer will change
with the shape of your load".
