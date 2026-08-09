use std::{io, path::Path};

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use wal_append::{
    Store,
    actor::StoreHandle,
    server::{MAX_CONNECTIONS, serve},
    wal::{PATH, Wal},
};

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    let wal = Wal::open(Path::new(PATH)).await?;

    let listener = TcpListener::bind("127.0.0.1:7878").await?;
    println!("minidb is listening on {}", listener.local_addr()?);

    serve(
        listener,
        StoreHandle::spawn(Store::new(), wal),
        MAX_CONNECTIONS,
        CancellationToken::new(),
    )
    .await
}
