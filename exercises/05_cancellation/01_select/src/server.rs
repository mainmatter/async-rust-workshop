//! The socket half of `minidb`.

use std::{io, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
};

use crate::{
    actor::StoreHandle,
    protocol::{Request, Response},
};

/// How long a connection may say nothing before it is closed.
pub const IDLE_LIMIT: Duration = Duration::from_secs(30);

/// Serves every client that turns up, each on a task of its own, all talking to one store task.
pub async fn serve(listener: TcpListener, store: StoreHandle) -> io::Result<()> {
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
    use std::time::Duration;

    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines, ReadHalf, WriteHalf},
        time::{sleep, timeout},
    };

    use crate::{Store, actor::StoreHandle, server::handle_connection};

    const IDLE: Duration = Duration::from_secs(30);

    #[tokio::test(start_paused = true)]
    async fn a_client_that_says_nothing_is_hung_up_on() {
        let mut client = connect();

        let hung_up = timeout(IDLE * 2, client.lines.next_line())
            .await
            .expect("the server never closed the connection")
            .unwrap();

        assert_eq!(hung_up, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_client_that_keeps_talking_is_left_alone() {
        let mut client = connect();

        assert_eq!(client.request("SET users alice hello").await, "OK");

        sleep(IDLE - Duration::from_secs(1)).await;
        assert_eq!(client.request("GET users alice").await, "VALUE hello");

        sleep(IDLE - Duration::from_secs(1)).await;
        assert_eq!(client.request("GET users alice").await, "VALUE hello");
    }

    #[tokio::test(start_paused = true)]
    async fn the_limit_is_measured_from_the_last_request() {
        let mut client = connect();

        assert_eq!(client.request("SET users alice hello").await, "OK");

        let hung_up = timeout(IDLE * 2, client.lines.next_line())
            .await
            .expect("one request does not buy an eternal connection")
            .unwrap();

        assert_eq!(hung_up, None);
    }

    struct TestClient {
        lines: Lines<BufReader<ReadHalf<DuplexStream>>>,
        writer: WriteHalf<DuplexStream>,
    }

    impl TestClient {
        async fn request(&mut self, request: &str) -> String {
            self.writer
                .write_all(format!("{request}\n").as_bytes())
                .await
                .unwrap();

            timeout(Duration::from_secs(120), self.lines.next_line())
                .await
                .expect("the server never answered")
                .unwrap()
                .expect("the server hung up")
        }
    }

    fn connect() -> TestClient {
        let (client, server) = tokio::io::duplex(1024);
        let store = StoreHandle::spawn(Store::new());

        tokio::spawn(async move { handle_connection(server, &store, IDLE).await });

        let (reader, writer) = tokio::io::split(client);

        TestClient {
            lines: BufReader::new(reader).lines(),
            writer,
        }
    }
}
