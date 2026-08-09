# Group commit

One sync per request is correct and slow. A sync is a round trip to a physical device, and
`tokio::fs` runs it on the blocking pool, so a hundred requests a second is a hundred trips to the
disk and a hundred trips to the pool, whether or not those requests arrived together.

What the last exercise left you with pays that cost once per request, however many are waiting:

```rust
wal.append(request).await?;
wal.sync().await          // one trip to the disk, for one change
```

They usually did arrive together. That is what a mailbox is.

```rust
let mut batch = Vec::with_capacity(BATCH);

while inbox.recv_many(&mut batch, BATCH).await > 0 {
    if let Err(error) = commit(&mut wal, &batch).await {
        // refuse the whole batch
    }

    for Command { request, reply } in batch.drain(..) {
        let _ = reply.send(apply(request, &mut store));
    }
}
```

`recv_many` takes everything that is waiting, up to a limit, in one call. Sixteen requests that
arrived while the last sync was in flight become one `Vec` rather than sixteen turns of the loop.
That loop is written for you; `commit` is not. It gets the whole batch, and the same two calls as
before, and has to decide how many times to make each of them.

Two things fall out of that. Appending is per record and syncing is not, so the count of `append`
calls and the count of `sync` calls are different numbers. And a batch that was all reads syncs
nothing at all, which is the same decision as the last exercise applied to a set, so `commit` has to
know whether the batch changed anything before it decides to sync.

## Why this is not cheating

Durability is only promised to a client that has been answered, and nothing in the batch is answered
until the sync returns. The sixteenth client waits no longer than it would have; the first one waits
slightly longer than it would have. Every one of them gets exactly the same promise as before, and
the disk did one trip instead of sixteen.

This is **group commit**, and every database you have used does it. Postgres calls it commit
delay, and will even wait a moment before syncing to let more transactions join the batch, trading
latency for throughput on purpose.

The idea generalises well past disks: **when work has a fixed cost per trip, the thing to batch is
the trip.** Network round trips, syscalls, lock acquisitions, writes to a metrics backend, all the
same shape.

## Testing a performance property

The test spawns sixteen requests at once and counts the syncs:

```rust
assert!(syncs <= 4, "sixteen requests that arrived together cost {syncs} syncs");
```

Counting a side effect is what makes this testable at all. A wall-clock assertion would be a flake
waiting for a slower machine; the number of times `sync` was called is exact, and it is what you
actually mean by "batched".

The bound is `<= 4` rather than `== 1` deliberately. Batching sixteen into one is what happens today
on a current-thread runtime, and pinning that exactly would make the test a hostage to the
scheduler's arrival timing. Four is comfortably below sixteen, and no implementation that syncs per
request can sneak past it.
