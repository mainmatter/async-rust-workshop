//! # Exercise
//!
//! `slow_get` takes 50 milliseconds, because pretending to reach a disk is what it is for. Looking
//! up four keys with four `.await`s therefore takes 200 milliseconds, and the store was idle for
//! nearly all of it.
//!
//! Implement `get_all` so the four lookups happen at the same time. The test says 50 milliseconds
//! for four keys, and it says the results come back in the order the keys were given.
//!
//! Two things fall out of chapter 2 here. Each lookup has to go on its own task, so each one needs
//! to own what it touches: hence `Arc<Store>`, which is how you lend the same immutable thing to
//! several tasks at once. And `JoinSet` hands you results in the order they *finish*, which is not
//! the order you were asked for, so either keep the handles in a `Vec` or carry the index along.
//!
//! `Arc` is enough here only because nothing writes. Chapter 4 is what happens when something does.

use std::{
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    sync::Arc,
    time::Duration,
};

use tokio::{task::JoinHandle, time::sleep};

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

/// Looks up every key at once, returning the values in the order the keys were given.
pub async fn get_all(store: Arc<Store>, bucket: Bucket, keys: Vec<Key>) -> Vec<Option<Value>> {
    todo!("one task per key, then collect the results in order")
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
    use std::{sync::Arc, time::Duration};

    use tokio::time::Instant;

    use crate::{Bucket, Key, Store, Value, get_all};

    #[tokio::test(start_paused = true)]
    async fn four_lookups_take_as_long_as_one() {
        let keys = ["alice", "bob", "carol", "dave"]
            .map(|key| Key::parse(key).unwrap())
            .to_vec();

        let started = Instant::now();
        let found = get_all(
            Arc::new(store_with_four_users()),
            Bucket::parse("users").unwrap(),
            keys,
        )
        .await;

        assert_eq!(
            started.elapsed(),
            Duration::from_millis(50),
            "the lookups ran one after another"
        );
        assert_eq!(found.len(), 4);
    }

    #[tokio::test]
    async fn the_results_keep_the_order_of_the_keys() {
        let keys = ["dave", "nobody", "alice"]
            .map(|key| Key::parse(key).unwrap())
            .to_vec();

        let found = get_all(
            Arc::new(store_with_four_users()),
            Bucket::parse("users").unwrap(),
            keys,
        )
        .await;

        let names = found
            .iter()
            .map(|value| value.as_ref().map(|value| value.as_str()))
            .collect::<Vec<_>>();

        assert_eq!(names, [Some("Dave"), None, Some("Alice")]);
    }

    fn store_with_four_users() -> Store {
        let users = Bucket::parse("users").unwrap();
        let mut store = Store::new();

        for (key, name) in [
            ("alice", "Alice"),
            ("bob", "Bob"),
            ("carol", "Carol"),
            ("dave", "Dave"),
        ] {
            store.insert(
                users.clone(),
                Key::parse(key).unwrap(),
                Value::parse(name).unwrap(),
            );
        }

        store
    }
}
