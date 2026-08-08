//! One task owns the store. Everybody else asks it nicely.

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;

use crate::Store;
use crate::protocol::{Request, Response};
use crate::server::apply;

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

    /// A store task that panics as soon as it is asked to do anything, which the tests need.
    pub fn spawn_doomed() -> Self {
        let (commands, mut inbox) = mpsc::channel(MAILBOX);

        tokio::spawn(async move {
            let _ = inbox.recv().await;
            panic!("the store task fell over");
        });

        Self { commands }
    }

    /// Resolves when the store task is gone, for whatever reason.
    pub async fn closed(&self) {
        self.commands.closed().await;
    }

    /// Applies one request and waits for the answer.
    pub async fn apply(&self, request: Request) -> Response {
        let (reply, answer) = oneshot::channel();

        if self.commands.send(Command { request, reply }).await.is_err() {
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use tokio::time::{sleep, timeout};
    use tokio_util::sync::CancellationToken;
    use tokio_util::task::TaskTracker;

    use crate::Store;
    use crate::actor::StoreHandle;
    use crate::protocol::{Request, Response};
    use crate::{Bucket, Key};

    #[tokio::test(start_paused = true)]
    async fn a_token_is_how_you_ask_a_task_to_stop() {
        let token = CancellationToken::new();
        let ticks = Arc::new(AtomicU32::new(0));

        let worker = tokio::spawn({
            let (token, ticks) = (token.clone(), Arc::clone(&ticks));
            async move {
                loop {
                    tokio::select! {
                        _ = token.cancelled() => return,
                        _ = sleep(Duration::from_millis(10)) => {
                            ticks.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
        });

        sleep(Duration::from_millis(100)).await;
        token.cancel();

        timeout(Duration::from_secs(1), worker)
            .await
            .expect("the worker ignored the token")
            .unwrap();

        assert!(ticks.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_tracker_is_how_you_wait_for_all_of_them() {
        let tracker = TaskTracker::new();
        let done = Arc::new(AtomicU32::new(0));

        for i in 0..3 {
            let done = Arc::clone(&done);
            tracker.spawn(async move {
                sleep(Duration::from_millis(10 * (i + 1))).await;
                done.fetch_add(1, Ordering::SeqCst);
            });
        }

        tracker.close();
        tracker.wait().await;

        assert_eq!(done.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_task_that_panics_takes_nothing_else_with_it() {
        let store = StoreHandle::spawn_doomed();

        assert_eq!(
            store.apply(get()).await,
            Response::Error("the store is gone".to_owned()),
            "the panic was captured, and the process carried on"
        );

        timeout(Duration::from_secs(1), store.closed())
            .await
            .expect("the store task is gone and the handle can tell");
    }

    fn get() -> Request {
        Request::Get {
            bucket: Bucket::parse("users").unwrap(),
            key: Key::parse("alice").unwrap(),
        }
    }
}
