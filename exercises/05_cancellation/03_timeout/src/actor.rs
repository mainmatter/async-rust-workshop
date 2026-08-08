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
        let (commands, inbox) = mpsc::channel(32);

        tokio::spawn(run(store, inbox, delay));

        Self { commands }
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
