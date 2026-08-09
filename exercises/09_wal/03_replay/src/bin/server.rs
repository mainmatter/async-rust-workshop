use std::{io, path::Path};

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use wal_replay::{
    actor::StoreHandle,
    server::{MAX_CONNECTIONS, serve},
    wal::{PATH, Wal},
};

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    let path = Path::new(PATH);
    let store = Wal::replay(path).await?;
    let wal = Wal::open(path).await?;

    let listener = TcpListener::bind("127.0.0.1:7878").await?;
    println!("minidb is listening on {}", listener.local_addr()?);

    serve(
        listener,
        StoreHandle::spawn(store, wal),
        MAX_CONNECTIONS,
        CancellationToken::new(),
    )
    .await
}
