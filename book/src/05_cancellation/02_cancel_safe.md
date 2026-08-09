# Cancel safety

`handle_connection` has grown a housekeeping branch that wakes up every `TICK` and goes straight
back to waiting, and it reads its lines by hand, a byte at a time, into a buffer of its own:

```rust
async fn read_line_by_hand<R>(reader: &mut R) -> io::Result<Option<String>> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).await?;
        if byte[0] == b'\n' {
            return Ok(Some(String::from_utf8_lossy(&line).into_owned()));
        }
        line.push(byte[0]);
    }
}
```

Both of those are reasonable-looking code. Together they lose data.

A client sends `GET users al`, pauses, and the tick fires. `select!` drops the read future, and the
`Vec` holding those twelve bytes goes with it. The client sends `ice\n`, the next read starts from
scratch, and the server answers with a complaint about a verb called `ICE`.

Nothing was written down anywhere that survives the drop, which is the whole definition of a future
that is not cancel safe.

## Why `next_line` is different

```rust
let mut requests = BufReader::new(reader).lines();
let line = requests.next_line().await?;
```

The bytes read so far live in the `BufReader`, which you own, and which is alive for the whole
connection. Dropping the `next_line` future loses nothing, because the partial line is not in the
future. Call it again and it picks up where it stopped.

That is the general shape of the fix. **Move the state out of the future and into something that
outlives it.** Every cancel-safe API in Tokio does this, and it is how to make your own: take
`&mut self` on a type that holds the partial state, rather than accumulating into a local.

## The deadline has the same disease

Look at the third branch:

```rust
_ = sleep(idle) => return Ok(()),
```

`select!` evaluates every branch expression on every iteration, so this builds a _new_ thirty-second
sleep each time round the loop. With a tick every five seconds, the deadline restarts before it can
ever fire, and the idle timeout from the previous exercise silently stops working. No error, no
warning, and the only reason you know is that a test says so.

A future that has to outlive the iteration has to be built outside it, and then polled in place
rather than consumed. Two things make that possible:

```rust
tokio::pin!(deadline);                          // Sleep -> Pin<&mut Sleep>, and it cannot move again
deadline.as_mut().reset(Instant::now() + d);    // pushes it out, without allocating a new timer
```

`tokio::pin!` puts the sleep somewhere it cannot move, which is what lets a branch poll it by `&mut`
instead of taking ownership of it. `Sleep::reset` is what you reach for when the deadline should
start again, and the question the exercise asks is when that is.

This is the one place in the workshop where `Pin` shows up in code you write, and the reason is
exactly the one from chapter 1: a future may hold references into itself, so polling it repeatedly
requires promising it will not move.

## The checklist

Before a call goes in a `select!` branch:

1. Does its documentation have a "Cancel safety" section, and what does it say?
2. If it is yours, where does the partial state live? In the future, or in something that outlives
   it?
3. Is the future being _created_ in the branch, or polled there? A deadline created in the branch is
   not a deadline.
