//! The socket half of `minidb`.

use std::io;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::time::{interval, sleep, timeout};

use crate::Store;
use crate::actor::StoreHandle;
use crate::protocol::{Request, Response};

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
