//! # Exercise
//!
//! `minidb` now handles hundreds of connections at once, and says nothing about any of them. When
//! one client in a hundred gets `ERR busy`, there is no way to find out which, or when, or what it
//! had asked for.
//!
//! `println!` will not save you here. Interleaved output from a hundred tasks is a soup, because a
//! line of text carries no record of which piece of work produced it. That is the problem `tracing`
//! solves: a **span** is a period of work, an **event** is a moment inside it, and every event
//! remembers which spans it happened in.
//!
//! Instrument `handle_connection` in `src/server.rs`:
//!
//! - Wrap it in a span called `connection`. `#[tracing::instrument]` does that, and it wants every
//!   argument to be `Debug` unless you tell it otherwise, which a generic stream and a
//!   `StoreHandle` are not: reach for `skip_all`. The span is named after the function by default,
//!   and `name = "connection"` overrides that, because a span name is something an operator reads
//!   and a function name is not.
//! - Log every request at INFO with `request` and `response` as **fields**, not as text spliced
//!   into the message: `info!(request = %line, response = %response, "handled")`. Fields are what
//!   make a log searchable. A collector can index `response` and answer "how many `ERR busy` in the
//!   last hour" without anybody writing a regex.
//! - Log a request the parser refused at WARN, with the error in a field called `error`. Something
//!   a client got wrong is not something an operator should be woken up for.
//!
//! `tests/instrumentation.rs` asserts on the spans and fields directly, through a `Layer` of its
//! own, rather than by matching on printed output. That is the part worth taking home:
//! instrumentation is structured data, so it can be tested like data, and a log line your alerting
//! depends on deserves a test as much as any other behaviour does.
//!
//! `src/bin/server.rs` already installs a subscriber, so `RUST_LOG=info cargo run` shows you the
//! same events the tests are reading.

pub mod actor;
pub mod protocol;
pub mod retry;
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
