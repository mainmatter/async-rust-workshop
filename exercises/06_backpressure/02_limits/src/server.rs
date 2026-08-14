//! The socket half of `minidb`.

use std::{io, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
    time::{Instant, interval, sleep, timeout},
};

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

/// How long a single request may take before the client is told the store is busy.
pub const REQUEST_LIMIT: Duration = Duration::from_secs(2);

/// Serves clients, at most `max_connections` of them at a time.
pub async fn serve(
    listener: TcpListener,
    store: StoreHandle,
    max_connections: usize,
) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let store = store.clone();

        tokio::spawn(async move {
            let _ = handle_connection(stream, &store, IDLE_LIMIT).await;
        });
    }
}

/// Talks to one client until it goes away or stops saying anything.
pub async fn handle_connection<S>(stream: S, store: &StoreHandle, idle: Duration) -> io::Result<()>
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
        time::timeout,
    };

    use crate::{Store, actor::StoreHandle, server::serve};

    const LIMIT: usize = 2;

    #[tokio::test]
    async fn the_first_clients_are_served() {
        let addr = spawn_server().await;

        let mut first = TestClient::connect(addr).await;
        let mut second = TestClient::connect(addr).await;

        assert_eq!(first.request("SET users alice hello").await, "OK");
        assert_eq!(second.request("GET users alice").await, "VALUE hello");
    }

    #[tokio::test]
    async fn the_one_over_the_limit_waits_its_turn() {
        let addr = spawn_server().await;

        let mut first = TestClient::connect(addr).await;
        let mut second = TestClient::connect(addr).await;
        assert_eq!(first.request("SET users alice hello").await, "OK");
        assert_eq!(second.request("SET users bob hello").await, "OK");

        let mut third = TestClient::connect(addr).await;
        third.send("GET users alice").await;

        assert!(
            timeout(Duration::from_millis(300), third.response())
                .await
                .is_err(),
            "the server is already serving its maximum"
        );

        drop(first);

        let answer = timeout(Duration::from_secs(5), third.response())
            .await
            .expect("a permit was freed and nobody used it");

        assert_eq!(answer, "VALUE hello");
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

        async fn send(&mut self, request: &str) {
            self.writer
                .write_all(format!("{request}\n").as_bytes())
                .await
                .unwrap();
        }

        async fn request(&mut self, request: &str) -> String {
            self.send(request).await;
            self.response().await
        }

        async fn response(&mut self) -> String {
            timeout(Duration::from_secs(5), self.lines.next_line())
                .await
                .expect("the server never answered")
                .unwrap()
                .expect("the server hung up")
        }
    }

    async fn spawn_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(serve(listener, StoreHandle::spawn(Store::new()), LIMIT));

        addr
    }
}
