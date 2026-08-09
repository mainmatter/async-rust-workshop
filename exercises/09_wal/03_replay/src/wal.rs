//! The write-ahead log: what the store was told to do, in the order it was told.

use std::{
    io::{self, ErrorKind},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};

use crate::{Store, actor::apply, protocol::Request};

/// Where the log lives unless somebody says otherwise.
pub const PATH: &str = "minidb.wal";

/// An append-only record of every change, as lines of the wire protocol.
pub struct Wal {
    file: File,
    syncs: Arc<AtomicUsize>,
}

impl Wal {
    /// Opens the log, creating it if it is not there, and writes to the end of it.
    pub async fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;

        Ok(Self {
            file,
            syncs: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// A log that refuses every write, which the tests need.
    pub async fn read_only(path: &Path) -> io::Result<Self> {
        File::create(path).await?;

        let file = OpenOptions::new().read(true).open(path).await?;

        Ok(Self {
            file,
            syncs: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Counts the syncs this log has done, which is what the tests are watching.
    pub fn syncs(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.syncs)
    }

    /// Rebuilds a store by replaying every record in the log, in order.
    ///
    /// A log that is not there is not an error: it is what a first start looks like.
    pub async fn replay(path: &Path) -> io::Result<Store> {
        let mut store = Store::new();

        let file = match File::open(path).await {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(store),
            Err(error) => return Err(error),
        };

        let mut records = BufReader::new(file).lines();

        while let Some(record) = records.next_line().await? {
            let request = Request::parse(&record)
                .map_err(|error| io::Error::other(format!("{record}: {error}")))?;

            apply(request, &mut store);
        }

        Ok(store)
    }

    /// Hands one record to `tokio::fs`, which is not the same as handing it to the disk.
    pub async fn append(&mut self, request: &Request) -> io::Result<()> {
        self.file.write_all(format!("{request}\n").as_bytes()).await
    }

    /// Waits until everything appended so far is on the disk, and reports anything that went wrong
    /// on the way there.
    ///
    /// There are two buffers between `append` and the platter: the one `tokio::fs::File` keeps so
    /// that `write_all` can return before the blocking pool has run, and the operating system's own
    /// cache. `flush` empties the first, which is also where a failed write is finally reported,
    /// and `sync_all` empties the second.
    pub async fn sync(&mut self) -> io::Result<()> {
        self.syncs.fetch_add(1, Ordering::Relaxed);

        self.file.flush().await?;
        self.file.sync_all().await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::{
        fs::{read_to_string, write},
        time::timeout,
    };

    use crate::{
        Bucket, Key, Store, Value,
        actor::StoreHandle,
        protocol::{Request, Response},
        wal::{PATH, Wal},
    };

    #[tokio::test]
    async fn a_log_that_is_not_there_is_a_store_with_nothing_in_it() {
        let directory = tempdir().unwrap();

        let store = Wal::replay(&directory.path().join(PATH)).await.unwrap();

        assert_eq!(store.buckets().count(), 0);
    }

    #[tokio::test]
    async fn replaying_runs_the_requests_again_in_order() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(PATH);

        write(
            &path,
            "SET users alice hello\nSET users bob hi\nSET users alice goodbye\nDEL users bob\n",
        )
        .await
        .unwrap();

        let store = Wal::replay(&path).await.unwrap();

        assert_eq!(
            store.get(&users(), &Key::parse("alice").unwrap()),
            Some(&Value::parse("goodbye").unwrap()),
            "the later write has to win, which is what in order means"
        );
        assert_eq!(store.get(&users(), &Key::parse("bob").unwrap()), None);
    }

    #[tokio::test]
    async fn a_log_nobody_can_parse_is_not_quietly_ignored() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(PATH);

        write(&path, "SET users alice hello\nHELLO?\n")
            .await
            .unwrap();

        assert!(
            Wal::replay(&path).await.is_err(),
            "starting up with half a database is worse than not starting up"
        );
    }

    #[tokio::test]
    async fn a_restart_is_invisible_to_whoever_reconnects() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(PATH);

        let store = StoreHandle::spawn(Store::new(), Wal::open(&path).await.unwrap());
        assert_eq!(
            answered(store.apply(set("alice", "hello"))).await,
            Response::Ok
        );
        assert_eq!(answered(store.apply(set("bob", "hi"))).await, Response::Ok);
        assert_eq!(answered(store.apply(del("bob"))).await, Response::Ok);
        drop(store);

        let store = StoreHandle::spawn(
            Wal::replay(&path).await.unwrap(),
            Wal::open(&path).await.unwrap(),
        );

        assert_eq!(
            answered(store.apply(get("alice"))).await,
            Response::Value(Value::parse("hello").unwrap())
        );
        assert_eq!(answered(store.apply(get("bob"))).await, Response::Nil);
    }

    #[tokio::test]
    async fn a_record_is_a_line_of_the_wire_protocol() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(PATH);

        let mut wal = Wal::open(&path).await.unwrap();
        wal.append(&set("alice", "hello")).await.unwrap();
        wal.append(&del("alice")).await.unwrap();
        wal.sync().await.unwrap();

        assert_eq!(
            read_to_string(&path).await.unwrap(),
            "SET users alice hello\nDEL users alice\n"
        );
    }

    #[tokio::test]
    async fn opening_a_log_that_exists_adds_to_it() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(PATH);

        let mut wal = Wal::open(&path).await.unwrap();
        wal.append(&set("alice", "hello")).await.unwrap();
        wal.sync().await.unwrap();
        drop(wal);

        let mut wal = Wal::open(&path).await.unwrap();
        wal.append(&set("bob", "hi")).await.unwrap();
        wal.sync().await.unwrap();

        assert_eq!(
            read_to_string(&path).await.unwrap(),
            "SET users alice hello\nSET users bob hi\n"
        );
    }

    #[tokio::test]
    async fn a_log_that_cannot_be_written_says_so_by_the_time_it_is_synced() {
        let directory = tempdir().unwrap();
        let path = directory.path().join(PATH);

        let mut wal = Wal::read_only(&path).await.unwrap();

        let appended = wal.append(&set("alice", "hello")).await;
        let synced = wal.sync().await;

        assert!(
            appended.is_err() || synced.is_err(),
            "a write nobody accepted was reported as fine"
        );
        assert!(synced.is_err(), "the failure has to surface by the sync");
    }

    fn get(key: &str) -> Request {
        Request::Get {
            bucket: users(),
            key: Key::parse(key).unwrap(),
        }
    }

    fn set(key: &str, value: &str) -> Request {
        Request::Set {
            bucket: users(),
            key: Key::parse(key).unwrap(),
            value: Value::parse(value).unwrap(),
        }
    }

    fn del(key: &str) -> Request {
        Request::Del {
            bucket: users(),
            key: Key::parse(key).unwrap(),
        }
    }

    fn users() -> Bucket {
        Bucket::parse("users").unwrap()
    }

    async fn answered<F>(round_trip: F) -> Response
    where
        F: Future<Output = Response>,
    {
        timeout(Duration::from_secs(5), round_trip)
            .await
            .expect("the store never answered")
    }
}
