//! One task owns the store. Everybody else asks it nicely.

use std::time::Duration;

use tokio::{
    sync::{mpsc, mpsc::error::TrySendError, oneshot},
    time::sleep,
};

use crate::{
    Store,
    protocol::{Request, Response},
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
        let (reply, answer) = oneshot::channel();

        match self.commands.try_send(Command { request, reply }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => return Response::Error("busy".to_owned()),
            Err(TrySendError::Closed(_)) => {
                return Response::Error("the store is gone".to_owned());
            }
        }

        answer
            .await
            .unwrap_or_else(|_| Response::Error("the store is gone".to_owned()))
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

/// Applies a request to the store.
pub fn apply(request: Request, store: &mut Store) -> Response {
    match request {
        Request::Get { bucket, key } => match store.get(&bucket, &key) {
            Some(value) => Response::Value(value.clone()),
            None => Response::Nil,
        },

        Request::Set { bucket, key, value } => {
            store.insert(bucket, key, value);
            Response::Ok
        }

        Request::Del { bucket, key } => match store.remove(&bucket, &key) {
            Some(_) => Response::Ok,
            None => Response::Nil,
        },
    }
}

async fn run(mut store: Store, mut inbox: mpsc::Receiver<Command>, delay: Duration) {
    while let Some(Command { request, reply }) = inbox.recv().await {
        if !delay.is_zero() {
            sleep(delay).await;
        }

        let _ = reply.send(apply(request, &mut store));
    }
}
