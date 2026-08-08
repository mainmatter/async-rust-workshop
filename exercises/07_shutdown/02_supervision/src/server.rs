//! The socket half of `minidb`.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio::time::{interval, sleep, timeout};

use crate::Store;
use crate::actor::StoreHandle;
use crate::protocol::{Request, Response};

/// How long a connection may say nothing before it is closed.
pub const IDLE_LIMIT: Duration = Duration::from_secs(30);

/// How many clients may be served at the same time.
pub const MAX_CONNECTIONS: usize = 128;

/// How often the connection wakes up to do its housekeeping.
pub const TICK: Duration = Duration::from_secs(5);

/// How long a single request may take before the client is told the store is busy.
pub const REQUEST_LIMIT: Duration = Duration::from_secs(2);

/// Serves clients, at most `max_connections` of them at a time.
pub async fn serve(
    listener: TcpListener,
    store: StoreHandle,
    max_connections: usize,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let permits = Arc::new(Semaphore::new(max_connections));
    let connections = TaskTracker::new();

    loop {
        let permit = Arc::clone(&permits)
            .acquire_owned()
            .await
            .expect("never closed");

        let accepted = tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => accepted?,
        };

        let (stream, _) = accepted;
        let store = store.clone();

        connections.spawn(async move {
            let _permit = permit;
            let _ = handle_connection(stream, &store, IDLE_LIMIT).await;
        });
    }

    connections.close();
    connections.wait().await;

    Ok(())
}

/// Talks to one client until it goes away or stops saying anything.
pub async fn handle_connection<S>(stream: S, store: &StoreHandle, idle: Duration) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut requests = BufReader::new(reader).lines();
    let mut housekeeping = interval(TICK);

    loop {
        let line = tokio::select! {
            line = requests.next_line() => line?,
            _ = housekeeping.tick() => continue,
            _ = sleep(idle) => return Ok(()),
        };

        let Some(line) = line else {
            return Ok(());
        };

        let response = match Request::parse(&line) {
            Ok(request) => match timeout(REQUEST_LIMIT, store.apply(request)).await {
                Ok(response) => response,
                Err(_) => Response::Error("busy".to_owned()),
            },
            Err(error) => Response::Error(error.to_string()),
        };

        writer.write_all(format!("{response}\n").as_bytes()).await?;
    }
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

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use crate::Store;
    use crate::actor::StoreHandle;
    use crate::server::serve;

    #[tokio::test]
    async fn a_dead_store_takes_the_server_with_it() {
        let (addr, _shutdown, server) = spawn_server(StoreHandle::spawn_doomed()).await;

        let mut client = TestClient::connect(addr).await;
        assert_eq!(
            client.request("GET users alice").await,
            "ERR the store is gone"
        );

        let outcome = timeout(Duration::from_secs(5), server)
            .await
            .expect("serve kept running without a store")
            .unwrap();

        assert!(outcome.is_err(), "serve should report why it stopped");
    }

    #[tokio::test]
    async fn a_healthy_store_keeps_the_server_up() {
        let (addr, shutdown, mut server) = spawn_server(StoreHandle::spawn(Store::new())).await;

        let mut client = TestClient::connect(addr).await;
        assert_eq!(client.request("SET users alice hello").await, "OK");

        assert!(
            timeout(Duration::from_millis(300), &mut server).await.is_err(),
            "nothing is wrong and the server stopped anyway"
        );

        drop(client);
        shutdown.cancel();

        timeout(Duration::from_secs(5), server)
            .await
            .expect("serve never returned")
            .unwrap()
            .unwrap();
    }

    struct TestClient {
        lines: Lines<BufReader<OwnedReadHalf>>,
        writer: OwnedWriteHalf,
    }

    impl TestClient {
        async fn connect(addr: SocketAddr) -> Self {
            let (reader, writer) = TcpStream::connect(addr).await.unwrap().into_split();

            Self {
                lines: BufReader::new(reader).lines(),
                writer,
            }
        }

        async fn request(&mut self, request: &str) -> String {
            self.writer
                .write_all(format!("{request}\n").as_bytes())
                .await
                .unwrap();

            self.lines
                .next_line()
                .await
                .unwrap()
                .expect("the server hung up")
        }
    }

    async fn spawn_server(
        store: StoreHandle,
    ) -> (SocketAddr, CancellationToken, JoinHandle<std::io::Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();

        let server = tokio::spawn(serve(listener, store, 128, shutdown.clone()));

        (addr, shutdown, server)
    }
}
