//! The socket half of `minidb`.

use std::io;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::Store;
use crate::protocol::{Request, Response};

/// Serves every client that turns up, each on a task of its own.
pub async fn serve(listener: TcpListener) -> io::Result<()> {
    todo!("accept, spawn, and get straight back to accepting")
}

/// Talks to one client until it goes away.
pub async fn handle_connection(stream: TcpStream, store: &mut Store) -> io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut requests = BufReader::new(reader).lines();

    while let Some(line) = requests.next_line().await? {
        let response = match Request::parse(&line) {
            Ok(request) => apply(request, store),
            Err(error) => Response::Error(error.to_string()),
        };

        writer.write_all(format!("{response}\n").as_bytes()).await?;
    }

    Ok(())
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
    use tokio::time::timeout;

    use crate::server::serve;

    #[tokio::test]
    async fn a_slow_client_does_not_hold_up_the_next_one() {
        let addr = spawn_server().await;

        let mut first = TestClient::connect(addr).await;
        assert_eq!(first.request("SET users alice hello").await, "OK");

        let mut second = TestClient::connect(addr).await;
        let answered = timeout(Duration::from_secs(5), second.request("SET users bob hi")).await;

        assert_eq!(
            answered.expect("the server is still busy with the first client"),
            "OK"
        );
    }

    #[tokio::test]
    async fn but_every_connection_has_its_own_store_for_now() {
        let addr = spawn_server().await;

        let mut first = TestClient::connect(addr).await;
        let mut second = TestClient::connect(addr).await;

        assert_eq!(first.request("SET users alice hello").await, "OK");
        assert_eq!(
            second.request("GET users alice").await,
            "NIL",
            "two clients, two stores, which is what chapter 4 is for"
        );
    }

    #[tokio::test]
    async fn a_bad_request_is_answered_not_punished() {
        let mut client = TestClient::connect(spawn_server().await).await;

        assert_eq!(client.request("PING").await, "ERR unknown verb PING");
        assert_eq!(client.request("SET users alice hello").await, "OK");
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

    async fn spawn_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(serve(listener));

        addr
    }
}
