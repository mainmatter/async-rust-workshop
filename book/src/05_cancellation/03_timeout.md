# Bounding the work

The idle timeout protects against a client that says nothing. It does nothing about a store that
takes too long to answer:

```rust
let response = match Request::parse(&line) {
    Ok(request) => match timeout(REQUEST_LIMIT, store.apply(request)).await {
        Ok(response) => response,
        Err(_) => Response::Error("busy".to_owned()),
    },
    Err(error) => Response::Error(error.to_string()),
};
```

Now every request has an upper bound, and a client gets an answer either way. That is worth
something on its own: a client waiting forever cannot retry, cannot fail over, and usually cannot
tell the difference between slow and dead.

## What the timeout did not do

It did not stop the work.

`store.apply(request)` sent a message to the store task and waited for the reply. The timeout drops
the waiting, so the `oneshot` receiver goes away, but the `Command` is still in the mailbox and the
store task is still going to apply it. The client is told `ERR busy`, and the write happens anyway,
a moment later, with nobody listening.

The exercise ships a test that asserts exactly this, because it is the sort of thing that is obvious
once stated and invisible otherwise:

```rust
// the client was told "busy", and the value is in the store regardless
```

This is not a flaw in `timeout`. It is what cancellation means when the work is happening somewhere
else: you can stop waiting for a message, but you cannot un-send it.

## What to do about it

Three honest options, and the right one depends on what the work is:

**Accept it.** For an idempotent write like `SET`, applying it late is harmless. This is what
`minidb` does, and for a key-value store it is defensible.

**Do not queue it in the first place.** If the mailbox is full, refuse before sending. That is load
shedding, and it is the next chapter.

**Make the work itself cancellable.** Pass something the worker can check, a `CancellationToken`
alongside the request, and have the store task drop work whose caller has gone. `oneshot::Sender`
even has `is_closed` and `closed`, so the store task can ask whether anybody is still waiting before
starting anything expensive.

## Timeouts are a system property

One last thing worth saying out loud, because it is where timeouts usually go wrong in production:
your timeout should be shorter than your caller's. If a client gives up after two seconds and your
server gives up after ten, you spend eight seconds doing work for somebody who has already left, and
under load that is most of what you do. Timeouts that are not ordered across a call chain amplify an
overload instead of shedding it.
