//! # Chapter 4: who owns the store
//!
//! Nothing to write here. Two ways to share one `Store` between many tasks, and the trade they
//! represent.
//!
//! **`Arc<Mutex<Store>>` is the obvious one**, and there are two mutexes to choose between.
//! `std::sync::Mutex` is faster and perfectly fine in async code, as long as the guard is gone
//! before the next `.await`. Hold it across one inside a spawned task and it does not compile,
//! because the guard is not `Send` and the future inherits that:
//!
//! ```compile_fail,E0277
//! use std::sync::{Arc, Mutex};
//! use std::time::Duration;
//!
//! use state_intro::{Bucket, Key, Store, Value};
//!
//! async fn broken(store: Arc<Mutex<Store>>) {
//!     tokio::spawn(async move {
//!         let mut store = store.lock().unwrap();
//!
//!         tokio::time::sleep(Duration::from_millis(1)).await;
//!
//!         store.insert(
//!             Bucket::parse("users").unwrap(),
//!             Key::parse("alice").unwrap(),
//!             Value::parse("hello").unwrap(),
//!         );
//!     });
//! }
//! ```
//!
//! That error is a good one. It is the compiler noticing that a lock held across a suspension point
//! is held for an unbounded time, because the task may not be polled again for a while.
//!
//! **`tokio::sync::Mutex` is the one you may hold across an await**, and `shared_set` below does
//! exactly that. It costs more than the `std` one, and it is the right choice only when the critical
//! section genuinely has to await.
//!
//! The thing both have in common is the point of the chapter: whoever holds the lock, everybody else
//! waits. A hundred connections and one mutex is a hundred connections taking turns, and the
//! contention does not show up in any type. The next two exercises are those two designs, in the
//! server, one after the other.

pub mod protocol;

use std::collections::HashMap;
use std::sync::Arc;
use std::fmt::{self, Debug, Formatter};

use tokio::sync::Mutex;

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

/// Writes a value into a store that several tasks share.
pub async fn shared_set(store: &Arc<Mutex<Store>>, bucket: Bucket, key: Key, value: Value) {
    store.lock().await.insert(bucket, key, value);
}

/// Reads a value out of a store that several tasks share, cloning it so the lock can be released.
pub async fn shared_get(store: &Arc<Mutex<Store>>, bucket: &Bucket, key: &Key) -> Option<Value> {
    store.lock().await.get(bucket, key).cloned()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Mutex;

    use crate::{Bucket, Key, Store, Value, shared_get, shared_set};

    #[tokio::test]
    async fn a_hundred_writers_and_one_store() {
        let store = Arc::new(Mutex::new(Store::new()));
        let users = Bucket::parse("users").unwrap();

        let writers = (0..100)
            .map(|i| {
                let (store, users) = (Arc::clone(&store), users.clone());
                tokio::spawn(async move {
                    let key = Key::parse(&format!("user-{i}")).unwrap();
                    shared_set(&store, users, key, Value::parse("hello").unwrap()).await;
                })
            })
            .collect::<Vec<_>>();

        for writer in writers {
            writer.await.unwrap();
        }

        let key = Key::parse("user-42").unwrap();
        let found = shared_get(&store, &users, &key).await;

        assert_eq!(found.unwrap().as_str(), "hello");
    }

    #[tokio::test]
    async fn what_one_task_writes_another_can_read() {
        let store = Arc::new(Mutex::new(Store::new()));
        let users = Bucket::parse("users").unwrap();
        let alice = Key::parse("alice").unwrap();

        shared_set(
            &store,
            users.clone(),
            alice.clone(),
            Value::parse("hello").unwrap(),
        )
        .await;

        let reader = tokio::spawn({
            let (store, users, alice) = (Arc::clone(&store), users.clone(), alice.clone());
            async move { shared_get(&store, &users, &alice).await }
        });

        assert_eq!(reader.await.unwrap().unwrap().as_str(), "hello");
    }
}
