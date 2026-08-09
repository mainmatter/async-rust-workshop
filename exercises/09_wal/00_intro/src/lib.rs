//! # Chapter 9: surviving a restart
//!
//! There is nothing to write in this exercise. Read `src/wal.rs`, run `wr`, and it passes.
//!
//! `minidb` keeps everything in a `HashMap`, so a restart loses the lot. The fix is the oldest one
//! in databases: before you change anything, write down what you are about to do, somewhere that
//! outlives the process. That is a **write-ahead log**, and the two words are the whole idea. Write
//! it _ahead_ of the change, because a log written afterwards is a log that is missing exactly the
//! records you needed.
//!
//! The format is free: `minidb` already has one. Every mutating request is a line of the wire
//! protocol, so the log is a transcript of what clients asked for, and replaying it is running
//! those requests again in order. `Request` implements both `parse` and `Display`, so the log
//! writer and the log reader already exist.
//!
//! **`write_all` is not durability.** It hands your bytes to the operating system, which puts them
//! in a cache and tells you it is done. A process crash is survivable at that point; a power cut is
//! not. `sync_all` is the call that waits for the disk, and it is expensive, which is why the next
//! exercises are about when to make it rather than whether to.
//!
//! **Tokio's file I/O is not async.** There is no portable way to await a disk, so `tokio::fs`
//! wraps the blocking calls in `spawn_blocking`. Every `append` costs a trip to the blocking pool,
//! which is another reason to do more per trip. It also means `write_all` returns before the write
//! has been attempted, so a disk that refuses it says nothing until the buffer is flushed. The last
//! test is where that shows up.
//!
//! The rest of the chapter: write the record before applying the change, batch the syncs so a busy
//! server does not sync once per request, and replay the log on startup so the restart is invisible
//! to whoever reconnects.

pub mod actor;
pub mod protocol;
pub mod retry;
pub mod server;
pub mod wal;

use std::{
    collections::HashMap,
    fmt::{self, Debug, Formatter},
};

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
