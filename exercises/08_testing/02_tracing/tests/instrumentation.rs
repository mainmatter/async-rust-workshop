//! Asserting on what the server said about itself.

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex},
    time::Duration,
};

use testing_tracing::{
    Store,
    actor::StoreHandle,
    server::{IDLE_LIMIT, handle_connection},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::yield_now,
    time::timeout,
};
use tracing::{
    Event, Level, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    Registry,
    layer::{Context, Layer, SubscriberExt},
    registry::LookupSpan,
};

/// Every request is reported at INFO, from inside the span that belongs to the connection.
#[tokio::test]
async fn a_request_is_logged_inside_the_connection_span() {
    let captured = Captured::default();
    let _guard = tracing::subscriber::set_default(Registry::default().with(captured.clone()));

    let mut client = connect();
    assert_eq!(client.request("SET users alice hello").await, "OK");
    settle().await;

    let record = captured
        .with_field("request", "SET users alice hello")
        .expect("no event carried the request as a field");

    assert_eq!(record.level, Level::INFO);
    assert_eq!(
        record.span.as_deref(),
        Some("connection"),
        "the event happened outside any span, or inside one by another name"
    );
}

/// The response is a field of its own, not something buried in a message.
#[tokio::test]
async fn the_response_is_a_field_too() {
    let captured = Captured::default();
    let _guard = tracing::subscriber::set_default(Registry::default().with(captured.clone()));

    let mut client = connect();
    assert_eq!(client.request("SET users alice hello").await, "OK");
    assert_eq!(client.request("GET users alice").await, "VALUE hello");
    settle().await;

    let record = captured
        .with_field("request", "GET users alice")
        .expect("no event carried the request as a field");

    assert_eq!(
        record.fields.get("response").map(String::as_str),
        Some("VALUE hello")
    );
}

/// A client that talks nonsense is worth a warning, and nothing louder.
#[tokio::test]
async fn a_rejected_request_is_a_warning() {
    let captured = Captured::default();
    let _guard = tracing::subscriber::set_default(Registry::default().with(captured.clone()));

    let mut client = connect();
    assert_eq!(client.request("PING").await, "ERR unknown verb PING");
    settle().await;

    let record = captured
        .with_field("error", "unknown verb PING")
        .expect("nothing was logged about the request that was refused");

    assert_eq!(record.level, Level::WARN);
    assert_eq!(record.span.as_deref(), Some("connection"));
}

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<Record>>>);

impl Captured {
    fn with_field(&self, name: &str, value: &str) -> Option<Record> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|record| record.fields.get(name).is_some_and(|found| found == value))
            .cloned()
    }
}

impl<S> Layer<S> for Captured
where
    S: Subscriber,
    S: for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
        let mut fields = HashMap::new();
        event.record(&mut Fields(&mut fields));

        self.0.lock().unwrap().push(Record {
            level: *event.metadata().level(),
            span: context.event_span(event).map(|span| span.name().to_owned()),
            fields,
        });
    }
}

#[derive(Clone)]
struct Record {
    level: Level,
    span: Option<String>,
    fields: HashMap<String, String>,
}

struct Fields<'a>(&'a mut HashMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

struct TestClient {
    lines: tokio::io::Lines<BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
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

fn connect() -> TestClient {
    let (client, server) = tokio::io::duplex(1024);
    let store = StoreHandle::spawn(Store::new());

    tokio::spawn(async move { handle_connection(server, &store, IDLE_LIMIT, permit()).await });

    let (reader, writer) = tokio::io::split(client);

    TestClient {
        lines: BufReader::new(reader).lines(),
        writer,
    }
}

async fn settle() {
    for _ in 0..16 {
        yield_now().await;
    }
}

/// A permit from a semaphore of its own, because `handle_connection` holds a connection slot.
fn permit() -> OwnedSemaphorePermit {
    Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .expect("a fresh semaphore has a permit")
}
