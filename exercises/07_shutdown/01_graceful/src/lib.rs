//! # Exercise
//!
//! `serve` runs until the listener errors, which in practice means until the process is killed. A
//! client mid-request loses its answer, and anything the store had not applied yet is gone.
//!
//! Make it stop properly. `serve` now takes a `CancellationToken`, and it should:
//!
//! - stop accepting new connections as soon as the token is cancelled,
//! - let the request each connection is already applying finish,
//! - stop those connections taking a new request, and hang up,
//! - and return only once they have.
//!
//! `select!` on `token.cancelled()` and `listener.accept()` gives you the first half. For the
//! second, spawn connections through a `TaskTracker` and `close()` then `wait()` for it. Doing the
//! two in the wrong order hangs: a tracker that is not closed never finishes waiting, because more
//! tasks could always arrive.
//!
//! The third bullet needs the token in `handle_connection` too, so it takes one now. Add an arm to
//! the `select!` that waits for the next line: when the token wins, the connection returns and the
//! socket closes. Only that wait is cancelled. A request already being applied is outside the
//! `select!` and finishes normally, which is what makes this graceful rather than abrupt.
//!
//! Then bound the waiting anyway. Every await in this exercise is bounded, so a connection ends
//! within `REQUEST_LIMIT` of the cancel, but `write_all` to a client that has stopped reading is
//! not bounded by anything, and neither is the next thing somebody adds. Wait politely for `GRACE`,
//! then stop waiting: `tokio::time::timeout` around the `wait()` is all that takes, and whatever is
//! still running is left where it stands. No test forces that path, which is the nature of a
//! backstop.
//!
//! Pick the grace period to sit comfortably under whatever kills you if you overrun it. Kubernetes
//! gives a pod `terminationGracePeriodSeconds`, thirty by default, before `SIGKILL`.

pub mod actor;
pub mod protocol;
pub mod server;

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
