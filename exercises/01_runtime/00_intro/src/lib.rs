//! # Chapter 1: what the runtime actually does
//!
//! There is nothing to write in this exercise. Read it, run `wr`, and the tests pass on arrival.
//! The point is to agree on vocabulary before the rest of the day leans on it.
//!
//! **A future is inert.** An `async fn` does not run anything when you call it. It builds a value
//! that knows how to run, and until something polls that value, nothing at all happens. `touch`
//! below proves it: the counter stays at zero until the `.await`.
//!
//! **`.await` is a suspension point.** It is not a call into the future, it is a point at which
//! this function is willing to be put down and picked up again later. Everything you hold across an
//! `.await` is held across that gap, which is where most of the surprises in this workshop come
//! from.
//!
//! **Concurrency is not parallelism.** Two futures awaited one after the other take as long as
//! both. The same two handed to `join!` take as long as the slower one, on a single thread, with no
//! parallelism anywhere, because `slow_get` spends its 50 milliseconds suspended and that is time
//! the other future can use. The saving comes out of the waiting, not out of the work: `join!`
//! interleaves polls, it does not add threads, so two futures that compute for 50 milliseconds each
//! without ever suspending still take 100 under it.
//!
//! **The clock in these tests is a fake.** `start_paused = true` freezes time, `tokio::time::Instant`
//! reads that frozen clock, and whenever every task is parked the runtime jumps straight to the next
//! deadline. Hence exactly 100 and exactly 50 below, and instantly. Chapter 8 teaches the trick.
//!
//! **The machinery, once, and then never again.** `PollCounter` is a `Future` written by hand. It
//! returns `Poll::Pending` until it has been polled often enough, and wakes itself so the runtime
//! knows to come back. That is the whole protocol: poll, get `Pending`, wait for the waker, poll
//! again. `Pin` is what makes it sound for futures that hold references into themselves.
//!
//! You will not write another `poll` today. From here on the runtime does it, and the workshop is
//! about the decisions you still have to make.

use std::{
    cell::Cell,
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

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

/// A future that becomes ready after a given number of polls, and outputs that number.
pub struct PollCounter {
    polls: u32,
    ready_after: u32,
}

impl PollCounter {
    /// Creates a future that returns `Pending` until it has been polled `ready_after` times.
    pub fn new(ready_after: u32) -> Self {
        Self {
            polls: 0,
            ready_after,
        }
    }
}

impl Future for PollCounter {
    type Output = u32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.polls += 1;

        if this.polls >= this.ready_after {
            Poll::Ready(this.polls)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
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

/// Increments the counter. Building this future does not: awaiting it does.
pub async fn touch(counter: &Cell<u32>) {
    counter.set(counter.get() + 1);
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
    use std::{cell::Cell, time::Duration};

    use tokio::time::Instant;

    use crate::{Bucket, Key, PollCounter, Store, Value, slow_get, touch};

    #[tokio::test]
    async fn a_future_does_nothing_until_it_is_awaited() {
        let counter = Cell::new(0);

        let future = touch(&counter);
        assert_eq!(counter.get(), 0, "building a future must not run it");

        future.await;
        assert_eq!(counter.get(), 1);
    }

    #[tokio::test]
    async fn pending_means_come_back_later() {
        assert_eq!(PollCounter::new(3).await, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn awaiting_in_sequence_adds_the_waits_up() {
        let store = store_with_two_users();
        let users = Bucket::parse("users").unwrap();
        let (alice, bob) = (Key::parse("alice").unwrap(), Key::parse("bob").unwrap());

        let started = Instant::now();
        let first = slow_get(&store, &users, &alice).await;
        let second = slow_get(&store, &users, &bob).await;

        assert_eq!(started.elapsed(), Duration::from_millis(100));
        assert!(first.is_some() && second.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn joining_them_does_not() {
        let store = store_with_two_users();
        let users = Bucket::parse("users").unwrap();
        let (alice, bob) = (Key::parse("alice").unwrap(), Key::parse("bob").unwrap());

        let started = Instant::now();
        let (first, second) = tokio::join!(
            slow_get(&store, &users, &alice),
            slow_get(&store, &users, &bob)
        );

        assert_eq!(started.elapsed(), Duration::from_millis(50));
        assert!(first.is_some() && second.is_some());
    }

    fn store_with_two_users() -> Store {
        let mut store = Store::new();
        let users = Bucket::parse("users").unwrap();

        store.insert(
            users.clone(),
            Key::parse("alice").unwrap(),
            Value::parse("Alice").unwrap(),
        );
        store.insert(
            users,
            Key::parse("bob").unwrap(),
            Value::parse("Bob").unwrap(),
        );

        store
    }
}
