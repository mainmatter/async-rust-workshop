//! The write-ahead log: what the store was told to do, in the order it was told.

use std::{io, path::Path};

use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};

use crate::protocol::Request;

/// Where the log lives unless somebody says otherwise.
pub const PATH: &str = "minidb.wal";

/// An append-only record of every change, as lines of the wire protocol.
pub struct Wal {
    file: File,
}

impl Wal {
    /// Opens the log, creating it if it is not there, and writes to the end of it.
    pub async fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;

        Ok(Self { file })
    }

    /// A log that refuses every write, which the tests need.
    pub async fn read_only(path: &Path) -> io::Result<Self> {
        File::create(path).await?;

        let file = OpenOptions::new().read(true).open(path).await?;

        Ok(Self { file })
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
        self.file.flush().await?;
        self.file.sync_all().await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use tokio::fs::read_to_string;

    use crate::{
        Bucket, Key, Value,
        protocol::Request,
        wal::{PATH, Wal},
    };

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

    fn set(key: &str, value: &str) -> Request {
        Request::Set {
            bucket: Bucket::parse("users").unwrap(),
            key: Key::parse(key).unwrap(),
            value: Value::parse(value).unwrap(),
        }
    }

    fn del(key: &str) -> Request {
        Request::Del {
            bucket: Bucket::parse("users").unwrap(),
            key: Key::parse(key).unwrap(),
        }
    }
}
