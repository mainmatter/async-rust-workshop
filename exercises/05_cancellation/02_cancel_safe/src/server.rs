//! The socket half of `minidb`.

use std::{io, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
    time::{interval, sleep},
};

use crate::{
    Store,
    actor::StoreHandle,
    protocol::{Request, Response},
};

/// How long a connection may say nothing before it is closed.
pub const IDLE_LIMIT: Duration = Duration::from_secs(30);

/// How often the connection wakes up to do its housekeeping.
pub const TICK: Duration = Duration::from_secs(5);

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
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut housekeeping = interval(TICK);

    loop {
        let line = tokio::select! {
            line = read_line_by_hand(&mut reader) => line?,
            _ = housekeeping.tick() => continue,
            _ = sleep(idle) => return Ok(()),
        };

        let Some(line) = line else {
            return Ok(());
        };

        let response = match Request::parse(&line) {
            Ok(request) => store.apply(request).await,
            Err(error) => Response::Error(error.to_string()),
        };

        writer.write_all(format!("{response}\n").as_bytes()).await?;
    }
}

/// Reads one line, a byte at a time, into a buffer of its own.
async fn read_line_by_hand<R>(reader: &mut R) -> io::Result<Option<String>>
where
    R: AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let mut line = Vec::new();

    loop {
        let mut byte = [0u8; 1];

        if reader.read_exact(&mut byte).await.is_err() {
            return Ok(None);
        }

        if byte[0] == b'\n' {
            return Ok(Some(String::from_utf8_lossy(&line).into_owned()));
        }

        line.push(byte[0]);
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
    use std::time::Duration;

    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines, ReadHalf, WriteHalf},
        task::yield_now,
        time::{advance, timeout},
    };

    use crate::{
        Store,
        actor::StoreHandle,
        server::{TICK, handle_connection},
    };

    const IDLE: Duration = Duration::from_secs(30);

    #[tokio::test(start_paused = true)]
    async fn a_request_split_across_a_tick_still_arrives() {
        let mut client = connect();

        client.send("GET users al").await;
        settle().await;

        advance(TICK * 2).await;
        settle().await;

        client.send("ice\n").await;

        assert_eq!(
            client.response().await,
            "NIL",
            "the first twelve bytes were dropped with the read future"
        );
    }

    #[tokio::test]
    async fn a_whole_request_still_works() {
        let mut client = connect();

        assert_eq!(client.request("SET users alice hello").await, "OK");
        assert_eq!(client.request("GET users alice").await, "VALUE hello");
    }

    #[tokio::test(start_paused = true)]
    async fn a_client_that_says_nothing_is_still_hung_up_on() {
        let mut client = connect();

        let hung_up = timeout(IDLE * 2, client.lines.next_line())
            .await
            .expect("the deadline is thrown away and rebuilt on every tick")
            .unwrap();

        assert_eq!(hung_up, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_client_that_keeps_talking_is_left_alone() {
        let mut client = connect();

        for _ in 0..3 {
            assert_eq!(client.request("SET users alice hello").await, "OK");

            advance(IDLE - Duration::from_secs(1)).await;
            settle().await;
        }

        assert_eq!(client.request("GET users alice").await, "VALUE hello");
    }

    #[tokio::test(start_paused = true)]
    async fn ticks_do_not_close_the_connection() {
        let mut client = connect();

        advance(TICK * 3).await;
        settle().await;

        assert_eq!(client.request("SET users alice hello").await, "OK");
    }

    struct TestClient {
        lines: Lines<BufReader<ReadHalf<DuplexStream>>>,
        writer: WriteHalf<DuplexStream>,
    }

    impl TestClient {
        async fn send(&mut self, bytes: &str) {
            self.writer.write_all(bytes.as_bytes()).await.unwrap();
        }

        async fn request(&mut self, request: &str) -> String {
            self.send(&format!("{request}\n")).await;
            self.response().await
        }

        async fn response(&mut self) -> String {
            timeout(Duration::from_secs(120), self.lines.next_line())
                .await
                .expect("the server never answered")
                .unwrap()
                .expect("the server hung up")
        }
    }

    async fn settle() {
        for _ in 0..16 {
            yield_now().await;
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
