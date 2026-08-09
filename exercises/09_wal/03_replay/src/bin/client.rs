use std::{env, io};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin},
    net::TcpStream,
};
use wal_replay::retry::{ATTEMPTS, BASE_DELAY, with_backoff};

#[tokio::main]
async fn main() -> io::Result<()> {
    let addr = env::args().nth(1).unwrap_or("127.0.0.1:7878".to_owned());

    let stream = with_backoff(ATTEMPTS, BASE_DELAY, || TcpStream::connect(addr.as_str())).await?;
    let (reader, mut writer) = stream.into_split();

    let mut responses = BufReader::new(reader).lines();
    let mut requests = BufReader::new(stdin()).lines();

    println!("connected to {addr}, one request per line, Ctrl-D to quit");

    while let Some(request) = requests.next_line().await? {
        if request.trim().is_empty() {
            continue;
        }

        writer.write_all(format!("{request}\n").as_bytes()).await?;

        match responses.next_line().await? {
            Some(response) => println!("{response}"),
            None => break,
        }
    }

    Ok(())
}
