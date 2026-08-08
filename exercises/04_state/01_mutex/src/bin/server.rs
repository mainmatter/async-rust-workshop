use std::{io, sync::Arc};

use state_mutex::{Store, server::serve};
use tokio::{net::TcpListener, sync::Mutex};

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:7878").await?;
    println!("minidb is listening on {}", listener.local_addr()?);

    serve(listener, Arc::new(Mutex::new(Store::new()))).await
}
