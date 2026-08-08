use std::io;

use cancellation_select::Store;
use cancellation_select::actor::StoreHandle;
use cancellation_select::server::serve;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:7878").await?;
    println!("minidb is listening on {}", listener.local_addr()?);

    serve(listener, StoreHandle::spawn(Store::new())).await
}
