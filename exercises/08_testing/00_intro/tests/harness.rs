//! The three tools this workshop has been quietly using all day, in one place.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use testing_intro::{
    Store,
    actor::StoreHandle,
    server::{IDLE_LIMIT, MAX_CONNECTIONS, handle_connection, serve},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, advance, timeout},
};
use tokio_util::sync::CancellationToken;

/// A fake socket: two connected pipes, no networking, no port, no operating system.
#[tokio::test]
async fn duplex_is_a_connection_without_a_network() {
    let (client, server) = tokio::io::duplex(1024);
    let store = StoreHandle::spawn(Store::new());

    tokio::spawn(async move {
        handle_connection(
            server,
            &store,
            IDLE_LIMIT,
            &CancellationToken::new(),
            permit(),
        )
        .await
    });

    let (reader, mut writer) = tokio::io::split(client);
    let mut responses = BufReader::new(reader).lines();

    writer.write_all(b"SET users alice hello\n").await.unwrap();
    assert_eq!(responses.next_line().await.unwrap().unwrap(), "OK");
}

/// Port zero means "whatever is free", which is what makes the suite safe to run in parallel.
#[tokio::test]
async fn port_zero_is_the_only_port_a_test_may_ask_for() {
    let (addr, shutdown) = spawn_server().await;
    assert_ne!(addr.port(), 0, "the kernel hands out a real port");

    let mut client = connect(addr).await;
    assert_eq!(client.request("SET users alice hello").await, "OK");
    assert_eq!(client.request("GET users alice").await, "VALUE hello");

    shutdown.cancel();
}

/// A paused clock skips ahead as soon as every task is waiting for it, so a timeout costs nothing.
#[tokio::test(start_paused = true)]
async fn a_paused_clock_makes_the_idle_timeout_free() {
    let (client, server) = tokio::io::duplex(1024);
    let store = StoreHandle::spawn(Store::new());

    tokio::spawn(async move {
        handle_connection(
            server,
            &store,
            IDLE_LIMIT,
            &CancellationToken::new(),
            permit(),
        )
        .await
    });

    let (reader, _writer) = tokio::io::split(client);
    let mut responses = BufReader::new(reader).lines();

    let started = Instant::now();
    let hung_up = timeout(IDLE_LIMIT * 2, responses.next_line())
        .await
        .expect("the server never hung up")
        .unwrap();

    assert_eq!(hung_up, None);
    assert!(started.elapsed() >= IDLE_LIMIT);
}

/// `advance` moves the clock by hand, for when you want the deadline to arrive on your terms.
#[tokio::test(start_paused = true)]
async fn advance_moves_the_clock_without_waiting_for_it() {
    let started = Instant::now();

    advance(Duration::from_secs(60 * 60 * 24)).await;

    assert_eq!(started.elapsed(), Duration::from_secs(60 * 60 * 24));
}

/// The surprise: the clock measures waiting, not working. Nothing else would be deterministic.
#[tokio::test(start_paused = true)]
async fn a_paused_clock_does_not_notice_work() {
    let started = Instant::now();

    let mut checksum = 0u64;
    for i in 0..2_000_000u64 {
        checksum = checksum.wrapping_add(i).rotate_left(1);
    }

    assert_ne!(checksum, 0);
    assert_eq!(started.elapsed(), Duration::ZERO);
}

struct TestClient {
    lines: tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TestClient {
    async fn request(&mut self, request: &str) -> String {
        self.writer
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();

        timeout(Duration::from_secs(5), self.lines.next_line())
            .await
            .expect("the server never answered")
            .unwrap()
            .expect("the server hung up")
    }
}

async fn spawn_server() -> (SocketAddr, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let store = StoreHandle::spawn(Store::new());

    tokio::spawn(serve(listener, store, MAX_CONNECTIONS, shutdown.clone()));

    (addr, shutdown)
}

async fn connect(addr: SocketAddr) -> TestClient {
    let (reader, writer) = TcpStream::connect(addr).await.unwrap().into_split();

    TestClient {
        lines: BufReader::new(reader).lines(),
        writer,
    }
}

/// A permit from a semaphore of its own, because `handle_connection` holds a connection slot.
fn permit() -> OwnedSemaphorePermit {
    Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .expect("a fresh semaphore has a permit")
}
