# Fetching things at the same time

Reading N keys in a loop takes N times as long as reading one:

```rust
for key in keys {
    values.push(slow_get(&store, &bucket, &key).await);
}
```

Every `.await` here is a full stop. Nothing else in this task happens until that lookup comes back,
and the next one has not started.

## Doing them at once

The fix is to have all the work in flight before awaiting any of it. `JoinSet` is the tool when the
work is a set of tasks of the same shape:

```rust
let mut set = JoinSet::new();

set.spawn(async move { /* one piece of the work */ });   // Send + 'static, like any spawn
set.join_next().await;                                   // -> Option<Result<T, JoinError>>
```

`JoinSet` hands you results in completion order, not in the order you spawned them, which is exactly
what you want when you are aggregating and exactly what you must not forget when the order matters.
The exercise wants the values back in the order the keys came in, so it is worth deciding early
whether to spawn with an index or to collect the handles in order instead.

## Which tool for which shape

- **`join!`** for a fixed, small number of futures of different types. No spawning, no allocation,
  all on the current task, so no `Send` requirement.
- **`JoinSet`** for a dynamic number of tasks of the same type, when you want results as they
  arrive and the ability to abort them all at once.
- **`FuturesUnordered`** for the same thing without spawning, so the futures stay on this task.
  Cheaper, but no parallelism and no isolation from a panic.
- **A `Vec<JoinHandle<T>>`** when you want results strictly in the order you started the work.

## What it costs

Spawning means the work can move to another thread, so everything it touches has to be `Send + 'static`. That is why the store arrives as an `Arc<Store>` here rather than a `&Store`: the task may
outlive the function that spawned it, and the compiler will not let you promise otherwise.

The other cost is that N concurrent lookups are N concurrent lookups. Turning a sequential loop into
an unbounded fan-out is how a service that was polite to its database becomes the reason the
database is down. Chapter 6 puts a limit on it.
