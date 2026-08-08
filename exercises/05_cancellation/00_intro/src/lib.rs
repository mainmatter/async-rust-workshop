//! # Chapter 5: cancellation
//!
//! Nothing to write here. This chapter is the one that bites people in production, so it is worth
//! being slow about the vocabulary.
//!
//! **Cancelling a future is dropping it.** There is no signal, no unwind, no handler. Whatever it
//! was in the middle of simply stops, at the last await point it reached, and everything it owned
//! is dropped. `with_limit` below is `tokio::time::timeout`, and all `timeout` does when it fires
//! is drop the future it was given.
//!
//! That means cancellation is silent by construction: no code of yours runs to notice it, unless
//! you put it in a `Drop` impl. Chapter 7 is where that becomes a design problem.
//!
//! **Cancellation stops your waiting, not the work.** This is the one that surprises people, and
//! the last test says it out loud. A request that has already been handed to the store task is
//! going to be applied whether or not anybody is still listening for the answer. `timeout` gave you
//! back control; it did not reach into another task and undo anything.
//!
//! **Cancel safety is a property of an API, not of your code.** A future is cancel safe when
//! dropping it mid-flight loses nothing. `AsyncBufReadExt::next_line` is: the bytes it has read so
//! far live in the `BufReader`, which you still own, so a dropped `next_line` can be retried and
//! nothing is lost. A hand-rolled read loop that buffers into a local variable is not, because that
//! variable dies with the future. The docs say which is which, per method, and there is no way to
//! tell by looking at the type. Exercise 02 is that difference, in the debugger rather than in the
//! abstract.
//!
//! **One refactor comes with this chapter.** `handle_connection` no longer insists on a
//! `TcpStream`: it takes anything that can be read and written, and splits it with
//! `tokio::io::split`. That is how the next exercise can drive it from a test with an in-memory
//! pipe and control exactly when each byte arrives, and it is how chapter 8 tests the whole server
//! without a socket.

pub mod actor;
pub mod protocol;
pub mod server;

use std::{
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::time::{sleep, timeout};

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

/// Runs `work`, giving up on it after `limit`.
pub async fn with_limit<F>(limit: Duration, work: F) -> Option<F::Output>
where
    F: Future,
{
    timeout(limit, work).await.ok()
}

/// Work that takes its time, and records whether it was allowed to finish.
pub async fn slow_work(took: Duration, finished: Arc<AtomicBool>) {
    sleep(took).await;
    finished.store(true, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU32, Ordering},
        },
        time::Duration,
    };

    use tokio::{task::yield_now, time::sleep};

    use crate::{
        Bucket, Key, Store, Value,
        actor::StoreHandle,
        protocol::{Request, Response},
        slow_work, with_limit,
    };

    #[tokio::test(start_paused = true)]
    async fn a_timeout_drops_the_work_it_was_given() {
        let finished = Arc::new(AtomicBool::new(false));

        let outcome = with_limit(
            Duration::from_millis(50),
            slow_work(Duration::from_secs(1), Arc::clone(&finished)),
        )
        .await;

        assert!(outcome.is_none());
        assert!(!finished.load(Ordering::SeqCst), "the work was dropped");
    }

    #[tokio::test(start_paused = true)]
    async fn an_aborted_task_stops_where_it_stood() {
        let ticks = Arc::new(AtomicU32::new(0));

        let ticker = tokio::spawn({
            let ticks = Arc::clone(&ticks);
            async move {
                loop {
                    ticks.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(10)).await;
                }
            }
        });

        sleep(Duration::from_millis(100)).await;
        ticker.abort();

        let when_stopped = ticks.load(Ordering::SeqCst);
        sleep(Duration::from_millis(100)).await;

        assert_eq!(ticks.load(Ordering::SeqCst), when_stopped);
    }

    #[tokio::test(start_paused = true)]
    async fn giving_up_on_a_request_does_not_call_it_back() {
        let store = StoreHandle::spawn_slow(Store::new(), Duration::from_millis(100));

        let outcome = with_limit(Duration::from_millis(10), store.apply(set())).await;
        assert!(outcome.is_none(), "the request timed out");

        sleep(Duration::from_millis(500)).await;
        yield_now().await;

        assert_eq!(
            store.apply(get()).await,
            Response::Value(Value::parse("hello").unwrap()),
            "the store applied it anyway, because nothing told it not to"
        );
    }

    fn get() -> Request {
        Request::Get {
            bucket: Bucket::parse("users").unwrap(),
            key: Key::parse("alice").unwrap(),
        }
    }

    fn set() -> Request {
        Request::Set {
            bucket: Bucket::parse("users").unwrap(),
            key: Key::parse("alice").unwrap(),
            value: Value::parse("hello").unwrap(),
        }
    }
}
