//! # Exercise
//!
//! A future only gives the runtime a chance to run something else when it hits an `.await`. Code
//! that computes for a while without awaiting cannot be preempted: no timer fires, no socket is
//! read, no other task moves, because there is nobody to move them. On a current-thread runtime
//! that is the whole runtime stopped. On a multi-thread one it is a worker thread gone, and there
//! are only as many of those as you have cores.
//!
//! `checksum` is that kind of code. It touches no I/O, it just computes, and it holds the thread
//! for as long as it takes.
//!
//! Implement `checksum_async` so it returns the same number without stopping the runtime. The tool
//! is `tokio::task::spawn_blocking`, which hands the closure to a separate pool that is allowed to
//! block, and gives you back a `JoinHandle` to await. That pool is large (512 threads by default)
//! precisely because the work on it is expected to sit still.
//!
//! The test runs on a single-threaded runtime on purpose, and counts how often a second task gets
//! to run while the checksum is being computed. Called directly, the answer is zero.
//!
//! The rule of thumb worth taking home: anything that might take longer than about 100 microseconds
//! and does not await belongs on the blocking pool. That includes the obvious CPU work, and also
//! `std::fs`, `std::net`, and any C library that talks to a disk.

use std::{
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    sync::Arc,
    time::Duration,
};

use tokio::{task::JoinHandle, time::sleep};

const MAX_NAME_LENGTH: usize = 64;
const ROUNDS: u32 = 200_000;
const MAX_VALUE_LENGTH: usize = 4096;

/// An in-memory key-value store, partitioned into named buckets.
pub struct Store {
    buckets: HashMap<Bucket, HashMap<Key, Value>>,
}

impl Store {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self {
            buckets: HashMap::new(),
        }
    }

    /// Inserts a value, returning the value it replaced, if any.
    pub fn insert(&mut self, bucket: Bucket, key: Key, value: Value) -> Option<Value> {
        self.buckets.entry(bucket).or_default().insert(key, value)
    }

    /// Looks up a value.
    pub fn get(&self, bucket: &Bucket, key: &Key) -> Option<&Value> {
        self.buckets.get(bucket)?.get(key)
    }

    /// Removes a value, returning it if it was there.
    pub fn remove(&mut self, bucket: &Bucket, key: &Key) -> Option<Value> {
        self.buckets.get_mut(bucket)?.remove(key)
    }

    /// Lists the buckets, including any that have been emptied.
    pub fn buckets(&self) -> impl Iterator<Item = &Bucket> {
        self.buckets.keys()
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

/// The name of a bucket.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Bucket(String);

impl Bucket {
    /// Parses a bucket name, rejecting anything `minidb` cannot store.
    pub fn parse(raw: &str) -> Result<Self, NameError> {
        parse_name(raw).map(Self)
    }

    /// Borrows the bucket name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The name of a value within a bucket.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key(String);

impl Key {
    /// Parses a key, rejecting anything `minidb` cannot store.
    pub fn parse(raw: &str) -> Result<Self, NameError> {
        parse_name(raw).map(Self)
    }

    /// Borrows the key.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A value held in the store.
#[derive(Clone, PartialEq, Eq)]
pub struct Value(String);

impl Value {
    /// Parses a value, rejecting anything that could not survive a round trip over the wire.
    pub fn parse(raw: &str) -> Result<Self, ValueError> {
        if raw.len() > MAX_VALUE_LENGTH {
            return Err(ValueError::TooLong { length: raw.len() });
        }

        match raw.char_indices().find(|(_, c)| matches!(c, '\n' | '\r')) {
            Some((index, _)) => Err(ValueError::Newline { index }),
            None => Ok(Self(raw.to_owned())),
        }
    }

    /// Borrows the value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Value(<redacted, {} bytes>)", self.0.len())
    }
}

/// What can go wrong when parsing a bucket name or a key.
#[derive(Debug, PartialEq, Eq)]
pub enum NameError {
    Empty,
    TooLong { length: usize },
    InvalidCharacter { character: char, index: usize },
}

/// What can go wrong when parsing a value.
#[derive(Debug, PartialEq, Eq)]
pub enum ValueError {
    TooLong { length: usize },
    Newline { index: usize },
}

/// Computes the checksum without holding on to the runtime.
pub async fn checksum_async(store: Arc<Store>) -> u64 {
    tokio::task::spawn_blocking(move || checksum(&store))
        .await
        .expect("the checksum did not panic")
}

/// Computes a checksum over the whole store, using nothing but the CPU and taking its time.
pub fn checksum(store: &Store) -> u64 {
    let mut sum = 0u64;

    for _ in 0..ROUNDS {
        for bucket in store.buckets() {
            let bytes = bucket.as_str().bytes().map(u64::from).sum::<u64>();
            sum = sum.wrapping_mul(31).wrapping_add(bytes);
        }
    }

    sum
}

/// Looks a value up on a task of its own, which is why it takes the store by value.
pub fn spawn_lookup(store: Store, bucket: Bucket, key: Key) -> JoinHandle<Option<Value>> {
    tokio::spawn(async move { slow_get(&store, &bucket, &key).await })
}

/// Looks a value up, pretending the data is somewhere slower than memory.
pub async fn slow_get(store: &Store, bucket: &Bucket, key: &Key) -> Option<Value> {
    sleep(Duration::from_millis(50)).await;
    store.get(bucket, key).cloned()
}

fn parse_name(raw: &str) -> Result<String, NameError> {
    if raw.is_empty() {
        return Err(NameError::Empty);
    }

    if raw.len() > MAX_NAME_LENGTH {
        return Err(NameError::TooLong { length: raw.len() });
    }

    match raw.char_indices().find(|(_, c)| !is_valid_char(*c)) {
        Some((index, character)) => Err(NameError::InvalidCharacter { character, index }),
        None => Ok(raw.to_owned()),
    }
}

fn is_valid_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/')
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    use tokio::task::yield_now;

    use crate::{Bucket, Key, Store, Value, checksum, checksum_async};

    #[tokio::test]
    async fn the_runtime_keeps_running() {
        let ticks = Arc::new(AtomicU32::new(0));

        let ticker = tokio::spawn({
            let ticks = Arc::clone(&ticks);
            async move {
                loop {
                    ticks.fetch_add(1, Ordering::SeqCst);
                    yield_now().await;
                }
            }
        });

        checksum_async(Arc::new(store_with_buckets())).await;
        ticker.abort();

        assert!(
            ticks.load(Ordering::SeqCst) > 0,
            "nothing else ran while the checksum was being computed"
        );
    }

    #[tokio::test]
    async fn it_is_still_the_same_checksum() {
        let store = Arc::new(store_with_buckets());

        assert_eq!(checksum_async(Arc::clone(&store)).await, checksum(&store));
    }

    fn store_with_buckets() -> Store {
        let mut store = Store::new();

        for name in ["users", "sessions", "audit-log"] {
            store.insert(
                Bucket::parse(name).unwrap(),
                Key::parse("seed").unwrap(),
                Value::parse(name).unwrap(),
            );
        }

        store
    }
}
