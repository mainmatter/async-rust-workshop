# Replay

The log has been correct since the start of this chapter and has never once been read. Reading it is
the last thing `minidb` needs:

```rust
pub async fn replay(path: &Path) -> io::Result<Store> {
    let mut store = Store::new();

    let file = match File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(store),
        Err(error) => return Err(error),
    };

    let mut records = BufReader::new(file).lines();

    while let Some(record) = records.next_line().await? {
        let request = Request::parse(&record)
            .map_err(|error| io::Error::other(format!("{record}: {error}")))?;

        apply(request, &mut store);
    }

    Ok(store)
}
```

Replaying is running the requests again, in order, against an empty store. The parser and `apply`
already existed; this function is glue, and that is the point of having chosen the wire format as the
log format.

## Two cases worth deciding on purpose

**No log at all.** `ErrorKind::NotFound` is not a failure, it is what a first start looks like. Any
other error from `File::open` is a real problem and belongs to the caller: a log that exists and
cannot be opened is not the same as no log, and starting empty in that case would silently discard a
database.

**A line that does not parse.** Refuse to start. A server that skips records it cannot read comes up
quietly holding a database that is missing writes it acknowledged, and nobody finds out until much
later.

There is a more sophisticated version of that second rule, and it is what real systems do. A crash
mid-write leaves a _torn_ last record, so the convention is to accept a truncated final line, discard
it, and refuse anything malformed in the middle. Doing it properly means a checksum per record, so
that a record which is complete but corrupt is detected rather than replayed.

## Proving it by hand

```bash
cargo run                              # terminal one
cargo run --bin client                 # terminal two
SET users alice hello
```

Ctrl-C the server, start it again, ask for the key back, and it is there. The log is a text file in
the working directory; `cat minidb.wal` shows exactly what was recorded.

## Where to go next

What you have is a real write-ahead log with a real weakness: it grows forever, and a restart takes
as long as the entire history of the database.

- **Checkpointing.** Periodically write the current state out in full, then truncate the log up to
  that point. Restart cost becomes the size of the data rather than the size of its history.
- **Segments.** One file per span of the log, so old segments can be deleted or archived without
  rewriting anything.
- **Checksums.** Per record, so corruption is detected rather than replayed.
- **`fdatasync` and `O_DIRECT`.** The next layer of the durability story, and the point where the
  answers become filesystem-specific.
- **Group commit with a delay.** Wait a moment before syncing so more requests can join the batch,
  trading a little latency for throughput.

Those are the next things to build, and they are all yours. The book stays where you left it.
