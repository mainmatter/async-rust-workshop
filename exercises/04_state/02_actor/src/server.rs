//! The socket half of `minidb`.

use std::io;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

use crate::{
    actor::StoreHandle,
    protocol::{Request, Response},
};

/// Serves every client that turns up, each on a task of its own, all talking to one store task.
pub async fn serve(listener: TcpListener, store: StoreHandle) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let store = store.clone();

        tokio::spawn(async move {
            let _ = handle_connection(stream, &store).await;
        });
    }
}

/// Talks to one client until it goes away.
pub async fn handle_connection(stream: TcpStream, store: &StoreHandle) -> io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut requests = BufReader::new(reader).lines();

    while let Some(line) = requests.next_line().await? {
        let response = match Request::parse(&line) {
            Ok(request) => store.apply(request).await,
            Err(error) => Response::Error(error.to_string()),
        };

        writer.write_all(format!("{response}\n").as_bytes()).await?;
    }

    Ok(())
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

    #[tokio::test]
    async fn what_one_client_writes_another_one_reads() {
        let addr = spawn_server().await;

        let mut first = TestClient::connect(addr).await;
        let mut second = TestClient::connect(addr).await;

        assert_eq!(first.request("SET users alice hello").await, "OK");
        assert_eq!(second.request("GET users alice").await, "VALUE hello");

        assert_eq!(second.request("DEL users alice").await, "OK");
        assert_eq!(first.request("GET users alice").await, "NIL");
    }

    #[tokio::test]
    async fn clients_are_still_served_at_the_same_time() {
        let addr = spawn_server().await;

        let mut first = TestClient::connect(addr).await;
        assert_eq!(first.request("SET users alice hello").await, "OK");

        let mut second = TestClient::connect(addr).await;
        let answered = timeout(Duration::from_secs(5), second.request("GET users alice")).await;

        assert_eq!(
            answered.expect("the first connection is holding the store"),
            "VALUE hello"
        );
    }

    #[tokio::test]
    async fn a_hundred_clients_do_not_lose_a_write() {
        let addr = spawn_server().await;

        let writers = (0..100)
            .map(|i| {
                tokio::spawn(async move {
                    let mut client = TestClient::connect(addr).await;
                    client.request(&format!("SET users user-{i} hello")).await
                })
            })
            .collect::<Vec<_>>();

        for writer in writers {
            assert_eq!(writer.await.unwrap(), "OK");
        }

        let mut client = TestClient::connect(addr).await;
        for i in 0..100 {
            assert_eq!(
                client.request(&format!("GET users user-{i}")).await,
                "VALUE hello"
            );
        }
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

    async fn spawn_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(serve(listener, StoreHandle::spawn(Store::new())));

        addr
    }
}
