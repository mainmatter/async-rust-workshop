# The accept loop

`minidb` is not a server yet. `serve` and `handle_connection` are both `todo!()`, and this is the
exercise that makes them real.

A TCP server is a loop around one call:

```rust
listener.accept().await;   // -> io::Result<(TcpStream, SocketAddr)>
```

`accept` yields until a client turns up, hands you a `TcpStream` and the address it came from, and is
ready to be called again. Around it goes a loop; inside it goes `handle_connection`, which reads
lines with the three calls from the previous page, parses each one into a `Request`, applies it to
the store, and writes the `Response` back.

## One at a time

Awaiting `handle_connection` inside the loop serves exactly one client. The second one connects, the
kernel holds it in the backlog queue, and nothing else happens until the first one hangs up.

That is not a bug in this exercise, it is the exercise. The single-client version is short enough to
hold in your head, and it makes the next step, one task per connection, a change of two lines rather
than a rewrite. It also makes the ownership obvious: with one client at a time, `&mut Store` is
enough, and the moment there are two clients it is not.

## Errors that are not errors

A client that goes away is not a failure of your server. `next_line` returning `Ok(None)` is a clean
end of stream, and a write failing with `BrokenPipe` means somebody closed a laptop. Ending the
connection quietly is the right response to both.

What you must not do is let one connection's error end the accept loop. In this exercise
`handle_connection` returns its error to `serve`, which returns it to `main`, which means a client
that disconnects rudely takes the server down with it. The next exercise fixes that as a side effect
of spawning, and chapter 7 is about telling the difference between an error that should end a
connection and one that should end the process.

## Ports in tests

The tests bind `127.0.0.1:0` and ask the listener which port it got:

```rust
let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
let addr = listener.local_addr().unwrap();
```

Port zero means "whatever is free". A test suite with a hardcoded port fails when two of its own
tests run at once, and fails again on the machine where something else already has 7878. This is the
cheapest habit in this chapter and the one most often skipped.
