//! # Chapter 2: tasks
//!
//! Nothing to write here either. This one is about the difference between a future and a task,
//! because the rest of the day is spent spawning things.
//!
//! **A future is a value. A task is a future the runtime owns.** `join!` runs futures concurrently
//! inside one task: they take turns at the same await points, on the same thread, and if that task
//! goes away they all go with it. `spawn` hands a future to the runtime as an independent unit,
//! scheduled on its own, and on a multi-thread runtime free to move between threads.
//!
//! **That independence is what the bounds pay for.** A spawned future must be `Send`, because the
//! runtime may move it to another thread, and `'static`, because the runtime cannot promise to
//! finish it before anything you lent it goes away. `spawn_lookup` below therefore takes its `Store`
//! by value where `slow_get` took it by reference.
//!
//! `Send` is a question about await points, not about the call: a value that is not `Send` is fine
//! inside a spawned future as long as it is gone before the next `.await`. Held across one, it is a
//! compile error:
//!
//! ```compile_fail,E0277
//! use std::rc::Rc;
//! use std::time::Duration;
//!
//! async fn held_across_an_await() {
//!     let shared = Rc::new(0);
//!
//!     tokio::spawn(async move {
//!         tokio::time::sleep(Duration::from_millis(1)).await;
//!         println!("{shared}");
//!     });
//! }
//! ```
//!
//! **Tasks are cheap.** Not free, but a spawn is an allocation and a queue push, not a thread. One
//! of the tests below spawns a thousand and does not notice.
//!
//! **A task can outlive your interest in it.** `spawn` returns a `JoinHandle`, awaiting it gives a
//! `Result<T, JoinError>`, and dropping the handle does not stop the task, it only stops you hearing
//! how it went. Chapter 7 is about what that costs.

use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::sleep;

const MAX_NAME_LENGTH: usize = 64;
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::time::sleep;

    use crate::{Bucket, Key, Store, Value, spawn_lookup};

    #[tokio::test(start_paused = true)]
    async fn a_task_is_joined_for_its_result() {
        let handle = spawn_lookup(
            store_with_alice(),
            Bucket::parse("users").unwrap(),
            Key::parse("alice").unwrap(),
        );

        let found = handle.await.expect("the task did not panic");
        assert_eq!(found.unwrap().as_str(), "Alice");
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_handle_does_not_stop_the_task() {
        let done = Arc::new(AtomicBool::new(false));

        let handle = tokio::spawn({
            let done = Arc::clone(&done);
            async move {
                sleep(Duration::from_millis(10)).await;
                done.store(true, Ordering::SeqCst);
            }
        });

        drop(handle);
        sleep(Duration::from_millis(50)).await;

        assert!(done.load(Ordering::SeqCst), "the task ran anyway");
    }

    #[tokio::test]
    async fn a_thousand_tasks_is_nothing() {
        let handles = (0..1000).map(|i| tokio::spawn(async move { i })).collect::<Vec<_>>();

        let mut total = 0;
        for handle in handles {
            total += handle.await.unwrap();
        }

        assert_eq!(total, 499_500);
    }

    fn store_with_alice() -> Store {
        let mut store = Store::new();

        store.insert(
            Bucket::parse("users").unwrap(),
            Key::parse("alice").unwrap(),
            Value::parse("Alice").unwrap(),
        );

        store
    }
}
