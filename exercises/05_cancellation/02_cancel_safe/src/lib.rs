//! # Exercise
//!
//! `handle_connection` has grown a housekeeping branch: every `TICK` it wakes up to do whatever a
//! server does periodically, and goes straight back to waiting. It also reads its lines with
//! `read_line_by_hand`, which does the obvious thing, one byte at a time into a buffer of its own.
//!
//! Those two are incompatible, and the test says how: a client that sends `GET users al`, pauses,
//! and then sends `ice` gets an error about a verb called `ICE`. The dozen bytes it sent first are
//! gone.
//!
//! They are gone because `select!` dropped the read future when the tick fired, and that future
//! owned the buffer holding `GET users al`. Nothing was written down anywhere that survives.
//!
//! That is what **cancel safety** means, and it is a property of the API you call, not of your
//! code. `AsyncBufReadExt::next_line` is cancel safe: the bytes it has read live in the
//! `BufReader`, which you own and which outlives any individual call, so dropping the future loses
//! nothing and you can call it again. `read_line_by_hand` is not, and no type says so.
//!
//! Fix `handle_connection` by reading with `next_line` again, and delete `read_line_by_hand`.
//!
//! Then look at the third branch, because the tick broke that one too, for a related reason.
//! `sleep(idle)` is built inside the `select!`, so every tick throws the deadline away and starts a
//! fresh thirty seconds. With a tick every five, the idle timeout you wrote in the last exercise
//! can never fire, and one of the tests says so.
//!
//! A future that has to outlive the iteration has to live outside it. Build the sleep once, pin it
//! with `tokio::pin!` so the branch can poll it by `&mut` in place, and push the deadline forward
//! with `Sleep::reset` once a line has arrived.
//!
//! The habit to take home: before putting a call in a `select!` branch, look up whether its docs
//! say it is cancel safe. Tokio documents this per method, under "Cancel safety". If it does not
//! say, or if you wrote it yourself, assume it is not. And whatever you build inside the `select!`
//! starts over every time round the loop, which is occasionally what you want and is never what
//! you want from a deadline.

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
