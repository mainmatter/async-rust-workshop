# Surviving a restart

`minidb` keeps everything in a `HashMap`, so a restart loses the lot. The fix is the oldest idea in
databases: before you change anything, write down what you are about to do, somewhere that outlives
the process.

That is a **write-ahead log**, and the two words are the whole idea. Write it _ahead_ of the change,
because a log written afterwards is missing exactly the records you needed.

## The format is already here

Every mutating request is a line of the wire protocol:

```text
SET users alice hello
DEL users alice
```

So the log is a transcript of what clients asked for, and replaying it is running those requests
again in order. `Request` has `parse` and `Display` and a round-trip test from chapter 3, so the log
writer and the log reader were finished before this chapter started.

That is not a trick to save time in a workshop. Reusing the wire format as the log format is what
gives you a log you can read with `cat`, and it means one round-trip test covers both.

The thing to be careful about is that the log records _requests_, not results. `SET users alice hello` replays to the same state every time. `INCR users counter` would not, and a log of
non-deterministic operations replays into a different database than the one you had. If a command
can produce a different result on a different day, log its effect rather than the command.

## `write_all` is not durability

```rust
self.file.write_all(format!("{request}\n").as_bytes()).await
```

hands the bytes to `tokio::fs`, which hands them to the operating system, which puts them in a cache
and says it is done. A process crash is survivable at that point. A power cut is not.

`sync_all` is the call that waits for the disk, and it is expensive, which is why the next two
exercises are about _when_ to call it rather than whether.

There are two buffers in the way, and both have to be emptied:

```rust
pub async fn sync(&mut self) -> io::Result<()> {
    self.file.flush().await?;      // tokio's own buffer
    self.file.sync_all().await     // the operating system's
}
```

## Tokio's file I/O is not async

There is no portable way to await a disk, so `tokio::fs` wraps the blocking calls in
`spawn_blocking`. Chapter 2, in other words, with the trip to the blocking pool already written for
you.

Two consequences follow. Every `append` costs a trip to that pool, which is another reason to do more
per trip. And `write_all` returns before the write has been attempted, so a disk that refuses it says
nothing until the buffer is flushed. The test for a log that cannot be written is where that shows
up, and it is the reason the durability check in the next exercise has to look at what `sync`
returned.

## Where this chapter goes

Write the record before applying the change. Batch the syncs so a busy server does not make one trip
to the disk per request. Replay the log on startup so the restart is invisible to whoever reconnects.
