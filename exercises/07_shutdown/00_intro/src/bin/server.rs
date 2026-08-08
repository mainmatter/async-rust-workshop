use std::io;

use shutdown_intro::Store;
use shutdown_intro::actor::StoreHandle;
use shutdown_intro::server::{MAX_CONNECTIONS, serve};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:7878").await?;
    println!("minidb is listening on {}", listener.local_addr()?);

    serve(listener, StoreHandle::spawn(Store::new()), MAX_CONNECTIONS).await
}
