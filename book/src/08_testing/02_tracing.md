# Seeing inside

`minidb` handles hundreds of connections at once and says nothing about any of them. When one client
in a hundred gets `ERR busy`, there is no way to find out which, when, or what it had asked for.

`println!` does not scale to this. Interleaved output from a hundred tasks is a soup, because a line
of text carries no record of which piece of work produced it.

## Spans and events

`tracing` splits it in two. A **span** is a period of work with a beginning and an end. An **event**
is a moment. Every event records which spans it happened inside, so the context travels with the
data instead of being copied into every message.

```rust
#[instrument(name = "connection", skip_all)]
pub async fn handle_connection<S>(stream: S, store: &StoreHandle, idle: Duration) -> io::Result<()> {
    // ...
    info!(request = %line, response = %response, "handled");
}
```

`#[instrument]` opens a span for the whole function, including across every `.await` in it, which is
the part that matters: the span is attached to the _future_, so it is entered and exited every time
the task is polled and the context survives suspension. This is why `tracing` and not `log`.

Two details from the attribute:

- **`skip_all`.** `#[instrument]` records every argument as a field and therefore requires them all
  to be `Debug`. A generic stream and a `StoreHandle` are not. `skip_all` records none of them, and
  `fields(...)` adds back the ones worth having.
- **`name = "connection"`.** A span name is something an operator reads. A function name is an
  implementation detail that will be refactored next month.

## Fields, not sentences

```rust
info!(request = %line, response = %response, "handled");   // yes
info!("handled {line} -> {response}");                     // no
```

Both print about the same thing. Only the first can be indexed, so that a collector can answer "how
many `ERR busy` in the last hour" without anybody writing a regex. `%` records the `Display` output,
`?` records `Debug`, and a bare name records the value.

Levels are part of the same discipline. A client sending nonsense is a `warn!` at most: it is not
your server malfunctioning, and it must not be able to fill your error budget by typing badly.

## Testing instrumentation

The exercise asserts on spans and fields directly, with a `Layer` of its own that collects events
into a `Vec` rather than printing them:

```rust
let captured = Captured::default();
let _guard = tracing::subscriber::set_default(Registry::default().with(captured.clone()));
```

That is the part worth taking home. Instrumentation is structured data, so it can be tested like
data, and a log line your alerting depends on deserves a test as much as any other behaviour does.
`set_default` scopes the subscriber to the current thread, which is exactly right under
`#[tokio::test]`'s single-threaded runtime.

## In the binaries

```rust
tracing_subscriber::fmt::init();
```

one line in `main`, and `RUST_LOG=info cargo run` prints the events. In production the same events go
to a JSON layer, or to OpenTelemetry via `tracing-opentelemetry`, where the spans become distributed
traces. None of the instrumentation changes; only the subscriber does.
