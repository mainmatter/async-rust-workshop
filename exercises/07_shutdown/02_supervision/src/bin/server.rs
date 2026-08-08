use std::io;

use shutdown_supervision::Store;
use shutdown_supervision::actor::StoreHandle;
use shutdown_supervision::server::{MAX_CONNECTIONS, serve};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> io::Result<()> {
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
