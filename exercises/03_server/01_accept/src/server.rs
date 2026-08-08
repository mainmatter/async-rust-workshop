//! The socket half of `minidb`.

use std::io;

use tokio::net::{TcpListener, TcpStream};

use crate::Store;
use crate::protocol::{Request, Response};

/// Serves clients, one at a time, until the listener gives up.
pub async fn serve(listener: TcpListener, store: Store) -> io::Result<()> {
    todo!("accept a connection, hand it to `handle_connection`, then accept the next one")
}

/// Talks to one client until it goes away.
pub async fn handle_connection(stream: TcpStream, store: &mut Store) -> io::Result<()> {
    todo!("a line in, a response out, until `next_line` returns `None`")
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

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
    use tokio::net::TcpStream;
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

    use crate::Store;
    use crate::server::serve;

    #[tokio::test]
    async fn a_value_written_can_be_read_back() {
        let mut client = TestClient::connect(spawn_server().await).await;

        assert_eq!(client.request("SET users alice hello").await, "OK");
        assert_eq!(client.request("GET users alice").await, "VALUE hello");
        assert_eq!(client.request("GET users bob").await, "NIL");
        assert_eq!(client.request("DEL users alice").await, "OK");
        assert_eq!(client.request("GET users alice").await, "NIL");
    }

    #[tokio::test]
    async fn a_value_may_contain_spaces() {
        let mut client = TestClient::connect(spawn_server().await).await;

        assert_eq!(client.request("SET users alice a b c").await, "OK");
        assert_eq!(client.request("GET users alice").await, "VALUE a b c");
    }

    #[tokio::test]
    async fn a_bad_request_is_answered_not_punished() {
        let mut client = TestClient::connect(spawn_server().await).await;

        assert_eq!(client.request("PING").await, "ERR unknown verb PING");
        assert_eq!(
            client.request("SET users alice hello").await,
            "OK",
            "the connection stayed open"
        );
    }

    #[tokio::test]
    async fn the_next_client_is_served_once_the_first_hangs_up() {
        let addr = spawn_server().await;

        let mut first = TestClient::connect(addr).await;
        assert_eq!(first.request("SET users alice hello").await, "OK");
        drop(first);

        let mut second = TestClient::connect(addr).await;
        assert_eq!(second.request("GET users alice").await, "VALUE hello");
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(serve(listener, Store::new()));

        addr
    }
}
