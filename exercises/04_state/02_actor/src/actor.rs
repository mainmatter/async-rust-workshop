//! One task owns the store. Everybody else asks it nicely.

use tokio::sync::{mpsc, oneshot};

use crate::{
    Store,
    protocol::{Request, Response},
};

/// A handle to the task that owns the store. Cloning it is how a connection gets access.
#[derive(Clone)]
pub struct StoreHandle {
    commands: mpsc::Sender<Command>,
}

impl StoreHandle {
    /// Hands the store to a task of its own and returns a handle to it.
    pub fn spawn(store: Store) -> Self {
        let (commands, inbox) = mpsc::channel(32);

        tokio::spawn(run(store, inbox));

        Self { commands }
    }

    /// Applies one request and waits for the answer.
    pub async fn apply(&self, request: Request) -> Response {
        todo!("send the request with a reply channel, then wait on it")
    }
}

/// A request, plus somewhere to put the answer.
pub struct Command {
    pub request: Request,
    pub reply: oneshot::Sender<Response>,
}

async fn run(store: Store, mut inbox: mpsc::Receiver<Command>) {
    todo!("take commands one at a time, apply them, and answer")
}

#[cfg(test)]
mod tests {
    use crate::{
        Bucket, Key, Store, Value,
        actor::StoreHandle,
        protocol::{Request, Response},
    };

    #[tokio::test]
    async fn the_handle_answers() {
        let store = StoreHandle::spawn(Store::new());

        assert_eq!(store.apply(set("alice", "hello")).await, Response::Ok);
        assert_eq!(
            store.apply(get("alice")).await,
            Response::Value(Value::parse("hello").unwrap())
        );
        assert_eq!(store.apply(get("bob")).await, Response::Nil);
    }

    #[tokio::test]
    async fn every_clone_talks_to_the_same_store() {
        let store = StoreHandle::spawn(Store::new());
        let writer = store.clone();

        writer.apply(set("alice", "hello")).await;

        assert_eq!(
            store.apply(get("alice")).await,
            Response::Value(Value::parse("hello").unwrap())
        );
    }

    #[tokio::test]
    async fn a_hundred_writers_are_serialised_without_a_lock() {
        let store = StoreHandle::spawn(Store::new());

        let writers = (0..100)
            .map(|i| {
                let store = store.clone();
                tokio::spawn(async move { store.apply(set(&format!("user-{i}"), "hello")).await })
            })
            .collect::<Vec<_>>();

        for writer in writers {
            assert_eq!(writer.await.unwrap(), Response::Ok);
        }

        for i in 0..100 {
            assert_eq!(
                store.apply(get(&format!("user-{i}"))).await,
                Response::Value(Value::parse("hello").unwrap())
            );
        }
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
}
