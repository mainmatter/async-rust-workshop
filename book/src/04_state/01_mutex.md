# A store every connection can reach

The server from chapter 3 gives every connection a store of its own, so no client can see what any
other one wrote. Give them one store between them:

```rust
pub async fn serve(listener: TcpListener, store: Arc<Mutex<Store>>) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let store = Arc::clone(&store);

        tokio::spawn(async move {
            let _ = handle_connection(stream, &store).await;
        });
    }
}
```

`Arc::clone` before the `async move` block is the idiom, and it reads oddly until you have written it
twenty times. The clone is what the task takes ownership of; the original stays behind for the next
turn of the loop.

## Inside the connection

`serve` is written for you. `handle_connection` is not, and it now has a shared store where it used
to have an exclusive one. One call gets you in:

```rust
store.lock().await;   // -> MutexGuard<'_, Store>, once whoever holds it lets go
```

Where that call goes is the decision. Take the lock once per request rather than once per
connection, and be finished with the guard before the write, so the lock is held for the length of a
`HashMap` operation rather than for the length of a network write to a client that may be on the
other side of the world.

That is the whole discipline with a shared mutex, and it is worth stating as a rule: **take the lock,
do the work on the data, drop the guard, then do the I/O.** A server that writes to a socket while
holding the store lock has one client's connection speed setting the throughput of every other
client.

## The dereference

`apply` wants a `&mut Store`, and `lock().await` gives you a `MutexGuard<Store>`, so `&mut` on it is
a `&mut MutexGuard` and the types do not line up. `&mut *` is the operator that derefs through the
guard. The compiler's message is clear enough once you have seen it once, which is the point of
meeting it here.

## Why `tokio::sync::Mutex` here

The guard crosses an `.await` in this design, so it has to be Tokio's. If you restructure to drop the
guard first, `std::sync::Mutex` works and is faster. Both are defensible; what is not defensible is
`std::sync::Mutex` with the guard alive across an await, and the exercise ships a `compile_fail`
doctest showing exactly what the compiler says when you try it.
