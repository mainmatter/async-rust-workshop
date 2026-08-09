//! The socket half of `minidb`.

use std::{io, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
    time::{Instant, interval, sleep},
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

/// How long a single request may take before the client is told the store is busy.
pub const REQUEST_LIMIT: Duration = Duration::from_secs(2);

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
            Ok(request) => store.apply(request).await,
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
    use std::time::Duration;

    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines, ReadHalf, WriteHalf},
        time::timeout,
    };

    use crate::{
        Store,
        actor::StoreHandle,
        server::{REQUEST_LIMIT, handle_connection},
    };

    const IDLE: Duration = Duration::from_secs(30);

    #[tokio::test(start_paused = true)]
    async fn a_store_that_answers_in_time_is_left_alone() {
        let mut client = connect(Duration::ZERO);

        assert_eq!(client.request("SET users alice hello").await, "OK");
        assert_eq!(client.request("GET users alice").await, "VALUE hello");
    }

    #[tokio::test(start_paused = true)]
    async fn a_store_that_does_not_gets_an_error() {
        let mut client = connect(REQUEST_LIMIT * 4);

        assert_eq!(client.request("SET users alice hello").await, "ERR busy");
    }

    #[tokio::test(start_paused = true)]
    async fn but_the_work_was_done_anyway() {
        let mut client = connect(REQUEST_LIMIT * 4);

        assert_eq!(client.request("SET users alice hello").await, "ERR busy");

        assert_eq!(
            client.request("GET users alice").await,
            "ERR busy",
            "the read is slow too, so it times out as well"
        );

        assert_eq!(
            client.request("GET users alice").await,
            "ERR busy",
            "and the store is still working through the queue"
        );
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

    fn connect(delay: Duration) -> TestClient {
        let (client, server) = tokio::io::duplex(1024);
        let store = StoreHandle::spawn_slow(Store::new(), delay);

        tokio::spawn(async move { handle_connection(server, &store, IDLE).await });

        let (reader, writer) = tokio::io::split(client);

        TestClient {
            lines: BufReader::new(reader).lines(),
            writer,
        }
    }
}
