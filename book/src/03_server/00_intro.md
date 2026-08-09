# A server

Everything so far has been `minidb` in one process talking to itself. From here it is a server, and
it stays one for the rest of the day.

## The protocol

One request per line, one response per line, all of it text:

```text
SET users alice hello        ->  OK
GET users alice              ->  VALUE hello
GET users bob                ->  NIL
DEL users alice              ->  OK
PING                         ->  ERR unknown verb PING
```

A text protocol is a workshop's best friend. You can drive the whole server with `nc localhost 7878`
and read every byte that goes past, which means a failure is something you can look at rather than
something you have to instrument. Real systems pick differently: length-prefixed binary framing
avoids the delimiter problem entirely, and gRPC or Postgres wire format arrive with tooling. The
concurrency lessons are identical either way.

The one thing a line protocol needs from its types is a guarantee that a value cannot contain the
delimiter, which is why `Value::parse` rejects newlines and anything over 4 KiB. Parse at the edge
and the rest of the program cannot produce a line that does not round trip. `src/protocol.rs` has
the test that proves it.

## Reading lines

```rust
let (reader, mut writer) = tokio::io::split(stream);
let mut requests = BufReader::new(reader).lines();

while let Some(line) = requests.next_line().await? {
    // ...
    writer.write_all(format!("{response}\n").as_bytes()).await?;
}
```

`BufReader` matters for more than speed here. Without it, every `read` is a syscall, and with a
`lines()` wrapper on top it is a syscall per byte. It matters again in chapter 5 for a reason that
has nothing to do with performance: the buffer is what makes `next_line` safe to cancel.

`tokio::io::split` gives you a reader half and a writer half of the same stream, so a future reading
and a future writing can exist at the same time. For a `TcpStream` specifically, `into_split` gives
you owned halves that can be moved into separate tasks.

## The binaries

From this chapter on, every exercise carries two of them:

```bash
cargo run                 # the server, on 127.0.0.1:7878
cargo run --bin client    # a second terminal
```

The client sends one line per request and prints what comes back. It is worth actually running: the
tests tell you the behaviour is right, and typing at the thing tells you what it feels like.
