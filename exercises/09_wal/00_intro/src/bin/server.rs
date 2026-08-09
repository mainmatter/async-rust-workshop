use std::io;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use wal_intro::{
    Store,
    actor::StoreHandle,
    server::{MAX_CONNECTIONS, serve},
};

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    let listener = TcpListener::bind("127.0.0.1:7878").await?;
    println!("minidb is listening on {}", listener.local_addr()?);

    serve(
        listener,
        StoreHandle::spawn(Store::new()),
        MAX_CONNECTIONS,
        CancellationToken::new(),
    )
    .await
}
