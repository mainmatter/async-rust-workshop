//! One task owns the store, and writes down what it is about to do before it does it.

use std::{io, time::Duration};

use tokio::{
    sync::{mpsc, mpsc::error::TrySendError, oneshot},
    time::sleep,
};

use crate::{
    Store,
    protocol::{Request, Response},
    wal::Wal,
};

/// How many requests may be waiting to be applied.
pub const MAILBOX: usize = 32;

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

async fn run(mut store: Store, mut inbox: mpsc::Receiver<Command>, mut wal: Wal, delay: Duration) {
    while let Some(Command { request, reply }) = inbox.recv().await {
        if !delay.is_zero() {
            sleep(delay).await;
        }

        if let Err(error) = log(&mut wal, &request).await {
            let _ = reply.send(Response::Error(format!("not logged: {error}")));
            continue;
        }

        let _ = reply.send(apply(request, &mut store));
    }
}

/// Makes a change durable. Called before the change is applied, which is the whole point.
async fn log(wal: &mut Wal, request: &Request) -> io::Result<()> {
    if matches!(request, Request::Get { .. }) {
        return Ok(());
    }

    wal.append(request).await?;
    wal.sync().await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tempfile::{TempDir, tempdir};
    use tokio::{fs::read_to_string, time::timeout};

    use crate::{
        Bucket, Key, Store, Value,
        actor::StoreHandle,
        protocol::{Request, Response},
        wal::{PATH, Wal},
    };

    #[tokio::test]
    async fn a_change_is_written_down_and_a_read_is_not() {
        let directory = tempdir().unwrap();
        let store = spawn(&directory).await;

        assert_eq!(
            answered(store.apply(set("alice", "hello"))).await,
            Response::Ok
        );
        assert_eq!(
            answered(store.apply(get("alice"))).await,
            Response::Value(Value::parse("hello").unwrap())
        );
        assert_eq!(answered(store.apply(del("alice"))).await, Response::Ok);

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
            matches!(
                answered(store.apply(set("alice", "hello"))).await,
                Response::Error(_)
            ),
            "the client was told the write had happened"
        );
        assert_eq!(
            answered(store.apply(get("alice"))).await,
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

    async fn answered<F>(round_trip: F) -> Response
    where
        F: Future<Output = Response>,
    {
        timeout(Duration::from_secs(5), round_trip)
            .await
            .expect("the store never answered")
    }
}
