//! # Exercise
//!
//! The log has been correct since the first exercise of this chapter and has never once been read.
//! Write `Wal::replay` in `src/wal.rs`, and the day is done: `minidb` comes back up with everything
//! it had when it went down.
//!
//! Replaying is running the requests again. Open the file, read it a line at a time with
//! `BufReader::lines`, `Request::parse` each line, and hand it to `apply` with a fresh `Store`. The
//! log was written by `Display` and is read back by `parse`, which is the round trip
//! `src/protocol.rs` has had a test for since chapter 3.
//!
//! Two cases the tests care about:
//!
//! **No log at all.** `ErrorKind::NotFound` is not a failure, it is a first start. Anything else
//! from `File::open` is a failure and belongs to the caller.
//!
//! **A line that does not parse.** Refuse to start. A server that skips the records it cannot read
//! comes up quietly holding a database that is missing writes it acknowledged, and nobody finds out
//! until much later.
//!
//! `src/bin/server.rs` already replays before it serves, so you can prove the whole thing by hand:
//! start the server, `SET users alice hello` from the client, stop the server with Ctrl-C, start it
//! again, and ask for the key back.
//!
//! ## Where to go next
//!
//! What you have is a real write-ahead log with a real weakness: it grows forever, and a restart
//! takes as long as the entire history. Databases answer that with **checkpointing**, writing the
//! current state out in full and truncating the log up to that point, and with **segments** rather
//! than a single file. The log also records requests rather than results, which is fine here
//! because `SET` is deterministic and would not be if it could say "add one to this counter".
//!
//! Those are the next things to build, and they are all yours: the book stays where you left it.

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
