//! One task owns the store, and writes down what it is about to do before it does it.

use std::{io, time::Duration};

use tokio::{
    sync::{mpsc, mpsc::error::TrySendError, oneshot},
    time::sleep,
};

use crate::{
    Store,
    protocol::{Request, Response},
    server::apply,
    wal::Wal,
};

/// How many requests may be waiting to be applied.
pub const MAILBOX: usize = 32;

/// How many of them may be logged and applied as one batch.
pub const BATCH: usize = 32;

/// A handle to the task that owns the store. Cloning it is how a connection gets access.
#[derive(Clone)]
pub struct StoreHandle {
    commands: mpsc::Sender<Command>,
}

impl StoreHandle {
    /// Hands the store and its log to a task of their own and returns a handle to it.
    pub fn spawn(store: Store, wal: Wal) -> Self {
        Self::spawn_slow(store, wal, Duration::ZERO)
    }

    /// The same, but pretending each request takes `delay` to apply, which the tests need.
    pub fn spawn_slow(store: Store, wal: Wal, delay: Duration) -> Self {
        Self::spawn_with_capacity(store, wal, delay, MAILBOX)
    }

    /// The same again, with room for `capacity` requests waiting to be applied.
    pub fn spawn_with_capacity(store: Store, wal: Wal, delay: Duration, capacity: usize) -> Self {
        let (commands, inbox) = mpsc::channel(capacity);

        tokio::spawn(run(store, inbox, wal, delay));

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

async fn run(mut store: Store, mut inbox: mpsc::Receiver<Command>, mut wal: Wal, delay: Duration) {
    let mut batch = Vec::with_capacity(BATCH);

    while inbox.recv_many(&mut batch, BATCH).await > 0 {
        if let Err(error) = commit(&mut wal, &batch).await {
            for Command { reply, .. } in batch.drain(..) {
                let _ = reply.send(Response::Error(format!("not logged: {error}")));
            }

            continue;
        }

        for Command { request, reply } in batch.drain(..) {
            if !delay.is_zero() {
                sleep(delay).await;
            }

            let _ = reply.send(apply(request, &mut store));
        }
    }
}

/// Makes a whole batch of changes durable, before any one of them is applied.
async fn commit(wal: &mut Wal, batch: &[Command]) -> io::Result<()> {
    let mut changed = false;

    for Command { request, .. } in batch {
        if matches!(request, Request::Get { .. }) {
            continue;
        }

        wal.append(request).await?;
        changed = true;
    }

    if changed { wal.sync().await } else { Ok(()) }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use tempfile::{TempDir, tempdir};
    use tokio::fs::read_to_string;

    use crate::{
        Bucket, Key, Store, Value,
        actor::StoreHandle,
        protocol::{Request, Response},
        wal::{PATH, Wal},
    };

    #[tokio::test]
    async fn a_burst_is_one_trip_to_the_disk_rather_than_sixteen() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(PATH);

        let wal = Wal::open(&path).await.unwrap();
        let syncs = wal.syncs();
        let store = StoreHandle::spawn(Store::new(), wal);

        let senders = (0..16)
            .map(|i| {
                let store = store.clone();
                tokio::spawn(async move { store.apply(set(&format!("user-{i}"), "hello")).await })
            })
            .collect::<Vec<_>>();

        for sender in senders {
            assert_eq!(sender.await.unwrap(), Response::Ok);
        }

        assert_eq!(
            read_to_string(&path).await.unwrap().lines().count(),
            16,
            "every change still has to be in the log"
        );

        let syncs = syncs.load(Ordering::Relaxed);
        assert!(
            syncs >= 1,
            "a batch that was never synced was never durable"
        );
        assert!(
            syncs <= 4,
            "sixteen requests that arrived together cost {syncs} syncs"
        );
    }

    #[tokio::test]
    async fn a_change_is_written_down_and_a_read_is_not() {
        let directory = tempdir().unwrap();
        let store = spawn(&directory).await;

        assert_eq!(store.apply(set("alice", "hello")).await, Response::Ok);
        assert_eq!(
            store.apply(get("alice")).await,
            Response::Value(Value::parse("hello").unwrap())
        );
        assert_eq!(store.apply(del("alice")).await, Response::Ok);

        assert_eq!(
            read_to_string(directory.path().join(PATH)).await.unwrap(),
            "SET users alice hello\nDEL users alice\n",
            "the log holds the changes, in order, and nothing else"
        );
    }

    #[tokio::test]
    async fn a_change_that_cannot_be_written_down_does_not_happen() {
        let directory = tempdir().unwrap();
        let wal = Wal::read_only(&directory.path().join(PATH)).await.unwrap();
        let store = StoreHandle::spawn(Store::new(), wal);

        assert!(
            matches!(store.apply(set("alice", "hello")).await, Response::Error(_)),
            "the client was told the write had happened"
        );
        assert_eq!(
            store.apply(get("alice")).await,
            Response::Nil,
            "the change was applied even though it was never logged"
        );
    }

    async fn spawn(directory: &TempDir) -> StoreHandle {
        let wal = Wal::open(&directory.path().join(PATH)).await.unwrap();

        StoreHandle::spawn(Store::new(), wal)
    }

    fn get(key: &str) -> Request {
        Request::Get {
            bucket: Bucket::parse("users").unwrap(),
            key: Key::parse(key).unwrap(),
        }
    }

    fn set(key: &str, value: &str) -> Request {
        Request::Set {
            bucket: Bucket::parse("users").unwrap(),
            key: Key::parse(key).unwrap(),
            value: Value::parse(value).unwrap(),
        }
    }

    fn del(key: &str) -> Request {
        Request::Del {
            bucket: Bucket::parse("users").unwrap(),
            key: Key::parse(key).unwrap(),
        }
    }
}
