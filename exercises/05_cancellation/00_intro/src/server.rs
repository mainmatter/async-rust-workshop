//! The socket half of `minidb`.

use std::io;

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
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
pub async fn handle_connection<S>(stream: S, store: &StoreHandle) -> io::Result<()>
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
