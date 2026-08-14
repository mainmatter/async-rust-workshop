//! The socket half of `minidb`.

use std::{io, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, interval, sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::{
    actor::StoreHandle,
    protocol::{Request, Response},
};

/// How long a connection may say nothing before it is closed.
pub const IDLE_LIMIT: Duration = Duration::from_secs(30);

/// How many clients may be served at the same time.
pub const MAX_CONNECTIONS: usize = 128;

/// How often the connection wakes up to do its housekeeping.
pub const TICK: Duration = Duration::from_secs(5);

/// How long a shutdown waits for the connections still in flight before it stops waiting.
pub const GRACE: Duration = Duration::from_secs(5);

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

    loop {
        let permit = Arc::clone(&permits)
            .acquire_owned()
            .await
            .expect("never closed");
        let (stream, _) = listener.accept().await?;
        let store = store.clone();

        tokio::spawn(async move {
            let _ = handle_connection(stream, &store, IDLE_LIMIT, permit).await;
        });
    }
}

/// Talks to one client until it goes away or stops saying anything, holding its connection
/// slot for as long as it does.
pub async fn handle_connection<S>(
    stream: S,
    store: &StoreHandle,
    idle: Duration,
    _permit: OwnedSemaphorePermit,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut requests = BufReader::new(reader).lines();
    let mut housekeeping = interval(TICK);

    let idle_deadline = sleep(idle);
    tokio::pin!(idle_deadline);

    loop {
        let line = tokio::select! {
            line = requests.next_line() => line?,
            _ = housekeeping.tick() => continue,
            _ = &mut idle_deadline => return Ok(()),
        };

        let Some(line) = line else {
            return Ok(());
        };

        idle_deadline.as_mut().reset(Instant::now() + idle);

        let response = match Request::parse(&line) {
            Ok(request) => match timeout(REQUEST_LIMIT, store.try_apply(request)).await {
                Ok(response) => response,
                Err(_) => Response::Error("busy".to_owned()),
            },
            Err(error) => Response::Error(error.to_string()),
        };

        writer.write_all(format!("{response}\n").as_bytes()).await?;
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, time::Duration};

    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
        net::{
            TcpListener, TcpStream,
            tcp::{OwnedReadHalf, OwnedWriteHalf},
        },
        task::JoinHandle,
        time::timeout,
    };
    use tokio_util::sync::CancellationToken;

    use crate::{
        Store,
        actor::StoreHandle,
        server::{GRACE, serve},
    };

    type Server = (
        SocketAddr,
        CancellationToken,
        JoinHandle<std::io::Result<()>>,
    );

    #[tokio::test]
    async fn cancelling_the_token_stops_the_server() {
        let (addr, shutdown, server) = spawn_server().await;

        let mut client = TestClient::connect(addr).await;
        assert_eq!(client.request("SET users alice hello").await, "OK");
        drop(client);

        shutdown.cancel();

        timeout(Duration::from_secs(5), server)
            .await
            .expect("serve never returned")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn a_connection_in_flight_is_waited_for() {
        let (addr, shutdown, mut server) = spawn_server().await;

        let mut client = TestClient::connect(addr).await;
        assert_eq!(client.request("SET users alice hello").await, "OK");

        shutdown.cancel();

        assert!(
            timeout(Duration::from_millis(300), &mut server)
                .await
                .is_err(),
            "serve returned while a client was still connected"
        );

        assert_eq!(client.request("GET users alice").await, "VALUE hello");
        drop(client);

        timeout(Duration::from_secs(5), server)
            .await
            .expect("serve never returned once the client had gone")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn nothing_new_is_accepted_after_the_cancel() {
        let (addr, shutdown, server) = spawn_server().await;

        shutdown.cancel();
        timeout(Duration::from_secs(5), server)
            .await
            .expect("serve never returned")
            .unwrap()
            .unwrap();

        let served = match TcpStream::connect(addr).await {
            Err(_) => false,
            Ok(stream) => {
                let (reader, mut writer) = stream.into_split();
                let mut lines = BufReader::new(reader).lines();

                writer.write_all(b"GET users alice\n").await.is_ok()
                    && timeout(Duration::from_millis(300), lines.next_line())
                        .await
                        .is_ok_and(|line| matches!(line, Ok(Some(_))))
            }
        };

        assert!(!served, "the server answered after it had shut down");
    }

    #[tokio::test]
    async fn a_connection_that_will_not_end_is_not_waited_for_forever() {
        let (addr, shutdown, server) = spawn_server().await;

        let mut silent = TestClient::connect(addr).await;
        assert_eq!(silent.request("SET users alice hello").await, "OK");

        shutdown.cancel();

        timeout(GRACE * 2, server)
            .await
            .expect("a client that says nothing kept the shutdown waiting past the grace period")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn a_server_with_no_permits_left_still_shuts_down() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let shutdown = CancellationToken::new();

        let server = tokio::spawn(serve(
            listener,
            StoreHandle::spawn(Store::new()),
            0,
            shutdown.clone(),
        ));

        shutdown.cancel();

        timeout(Duration::from_secs(5), server)
            .await
            .expect("the loop was waiting for a permit and never looked at the token")
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

            timeout(Duration::from_secs(5), self.lines.next_line())
                .await
                .expect("the server never answered")
                .unwrap()
                .expect("the server hung up")
        }
    }

    async fn spawn_server() -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();

        let server = tokio::spawn(serve(
            listener,
            StoreHandle::spawn(Store::new()),
            128,
            shutdown.clone(),
        ));

        (addr, shutdown, server)
    }
}
