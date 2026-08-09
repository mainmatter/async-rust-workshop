# Write ahead

The store task now owns a `Wal` as well as a `Store`, and `run` logs before it applies:

```rust
if let Err(error) = log(&mut wal, &request).await {
    let _ = reply.send(Response::Error(format!("not logged: {error}")));
    continue;
}

let _ = reply.send(apply(request, &mut store));
```

Two decisions live in `log`.

## What gets logged

A `GET` changes nothing, so writing it down would cost a disk sync to record that nothing happened.
`SET` and `DEL` are the log, and `matches!(request, Request::Get { .. })` is how you say so.

`Wal` gives you two calls, and the difference between them is the whole chapter:

```rust
wal.append(&request).await;   // -> io::Result<()>, into a buffer, not onto a disk
wal.sync().await;             // -> io::Result<()>, and back only once the disk agrees
```

## When it is safe to say yes

`append` hands the bytes to a buffer, so a reply sent after the append and before the sync is a
promise you have not kept. The reply goes out after the sync, and only if the sync said it worked.

The exercise's second test is the one that pins this down: a store whose log cannot be written must
refuse the change, not apply it and hope. A server that acknowledges a write it could not record is
the failure mode this entire chapter exists to prevent, and it is invisible until the day the disk is
full.

## The asymmetry

Say it in both directions, because only one of them is a problem.

The log may contain changes the store never applied, because the process can die between the sync and
the apply. That is fine. Replaying a change that already happened sets the same key to the same
value, which is why replay wants idempotent records.

A store that is ahead of its log is data loss, and no amount of replaying fixes it. Every rule in
this chapter follows from that one asymmetry.

## What this costs

One sync per request, which is the slowest thing this server now does. A spinning disk does on the
order of a hundred of those a second; an SSD does more, and still fewer than the store task could
apply. From here on, durability is the bottleneck, and the next exercise is about the standard way
of making it a smaller one.
