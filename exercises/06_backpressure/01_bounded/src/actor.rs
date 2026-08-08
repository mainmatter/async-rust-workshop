//! One task owns the store. Everybody else asks it nicely.

use std::time::Duration;

use tokio::{
    sync::{mpsc, oneshot},
    time::sleep,
};

use crate::{
    Store,
    protocol::{Request, Response},
    server::apply,
};

/// How many requests may be waiting to be applied.
pub const MAILBOX: usize = 32;

/// A handle to the task that owns the store. Cloning it is how a connection gets access.
#[derive(Clone)]
pub struct StoreHandle {
    commands: mpsc::Sender<Command>,
}

impl StoreHandle {
    /// Hands the store to a task of its own and returns a handle to it.
    pub fn spawn(store: Store) -> Self {
        Self::spawn_slow(store, Duration::ZERO)
    }

    /// The same, but pretending each request takes `delay` to apply, which the tests need.
    pub fn spawn_slow(store: Store, delay: Duration) -> Self {
        Self::spawn_with_capacity(store, delay, MAILBOX)
    }

    /// The same again, with room for `capacity` requests waiting to be applied.
    pub fn spawn_with_capacity(store: Store, delay: Duration, capacity: usize) -> Self {
        let (commands, inbox) = mpsc::channel(capacity);

        tokio::spawn(run(store, inbox, delay));

        Self { commands }
    }

    /// Applies one request, refusing rather than waiting when the mailbox is full.
    pub async fn try_apply(&self, request: Request) -> Response {
        todo!("`try_send` rather than `send`, and `ERR busy` when there is no room")
    }

    /// Applies one request and waits for the answer.
    pub async fn apply(&self, request: Request) -> Response {
        let (reply, answer) = oneshot::channel();

        if self
            .commands
            .send(Command { request, reply })
            .await
            .is_err()
        {
            return Response::Error("the store is gone".to_owned());
        }

        answer
            .await
            .unwrap_or_else(|_| Response::Error("the store is gone".to_owned()))
    }
}

/// A request, plus somewhere to put the answer.
pub struct Command {
    pub request: Request,
    pub reply: oneshot::Sender<Response>,
}

async fn run(mut store: Store, mut inbox: mpsc::Receiver<Command>, delay: Duration) {
    while let Some(Command { request, reply }) = inbox.recv().await {
        if !delay.is_zero() {
            sleep(delay).await;
        }

        let _ = reply.send(apply(request, &mut store));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::{
        Bucket, Key, Store, Value,
        actor::StoreHandle,
        protocol::{Request, Response},
    };

    const SLOW: Duration = Duration::from_millis(500);

    #[tokio::test(start_paused = true)]
    async fn a_full_mailbox_is_answered_rather_than_queued() {
        let store = StoreHandle::spawn_with_capacity(Store::new(), SLOW, 1);

        let senders = (0..8)
            .map(|i| {
                let store = store.clone();
                tokio::spawn(async move { store.try_apply(set(&format!("user-{i}"))).await })
            })
            .collect::<Vec<_>>();

        let mut answers = Vec::new();
        for sender in senders {
            answers.push(sender.await.unwrap());
        }

        let shed = answers
            .iter()
            .filter(|answer| **answer == Response::Error("busy".to_owned()))
            .count();

        assert!(
            shed > 0,
            "eight requests at {SLOW:?} each, with room for one: some should have been refused"
        );
        assert!(
            answers.iter().any(|answer| *answer == Response::Ok),
            "and some should have been accepted"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_store_that_is_keeping_up_answers_normally() {
        let store = StoreHandle::spawn(Store::new());

        assert_eq!(store.try_apply(set("alice")).await, Response::Ok);
        assert_eq!(
            store.try_apply(get("alice")).await,
            Response::Value(Value::parse("hello").unwrap())
        );
    }

    fn get(key: &str) -> Request {
        Request::Get {
            bucket: Bucket::parse("users").unwrap(),
            key: Key::parse(key).unwrap(),
        }
    }

    fn set(key: &str) -> Request {
        Request::Set {
            bucket: Bucket::parse("users").unwrap(),
            key: Key::parse(key).unwrap(),
            value: Value::parse("hello").unwrap(),
        }
    }
}
