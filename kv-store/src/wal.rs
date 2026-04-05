//! Write-ahead log (WAL) for the kv-store.
//!
//! # Format
//!
//! Each entry is a fixed-width binary frame:
//!
//! ```text
//! [1 byte: op] [4 bytes: key_len u32 LE] [4 bytes: val_len u32 LE] [key bytes] [val bytes]
//! ```
//!
//! op = 0x01 (Put) | 0x02 (Delete). Delete always has val_len = 0.
//!
//! # Durability
//!
//! `append` calls `flush()` after every write, draining the userspace buffer
//! to the OS page cache. This survives `kill -9` but not a kernel crash or
//! power failure. To upgrade to power-loss durability, add:
//!
//! ```rust,ignore
//! writer.get_ref().sync_data().await?;
//! ```
//!
//! after `flush()`. (`BufWriter` doesn't expose `sync_data` directly.)
//!
//! # Partial-write recovery
//!
//! If the process dies mid-append, the trailing bytes of the file may be
//! an incomplete frame. `open` detects this during replay (short-read on
//! header or body) and truncates the file to the last complete entry. That
//! entry was never acknowledged to the client, so discarding it is correct.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

const OP_PUT: u8 = 0x01;
const OP_DELETE: u8 = 0x02;
const HEADER_LEN: usize = 9; // 1 + 4 + 4

pub enum WalEntry {
    Put { key: String, value: String },
    Delete { key: String },
}

pub struct Wal {
    writer: BufWriter<File>,
    pub path: PathBuf,
}

// ---------------------------------------------------------------------------
// Encoding / decoding
// ---------------------------------------------------------------------------

fn encode_entry(entry: &WalEntry) -> Vec<u8> {
    match entry {
        WalEntry::Put { key, value } => {
            let klen = key.len() as u32;
            let vlen = value.len() as u32;
            let mut buf = Vec::with_capacity(HEADER_LEN + key.len() + value.len());
            buf.push(OP_PUT);
            buf.extend_from_slice(&klen.to_le_bytes());
            buf.extend_from_slice(&vlen.to_le_bytes());
            buf.extend_from_slice(key.as_bytes());
            buf.extend_from_slice(value.as_bytes());
            buf
        }
        WalEntry::Delete { key } => {
            let klen = key.len() as u32;
            let mut buf = Vec::with_capacity(HEADER_LEN + key.len());
            buf.push(OP_DELETE);
            buf.extend_from_slice(&klen.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes()); // val_len = 0
            buf.extend_from_slice(key.as_bytes());
            buf
        }
    }
}

/// Decode all complete entries from `buf`.
///
/// Returns the decoded entries and the byte offset of the first incomplete
/// or unrecognised entry (i.e. the safe truncation point). If all entries
/// decoded successfully, the offset equals `buf.len()`.
fn decode_entries(buf: &[u8]) -> (Vec<WalEntry>, usize) {
    let mut entries = Vec::new();
    let mut pos = 0;

    loop {
        let entry_start = pos;

        // Need at least a full header.
        if buf.len() - pos < HEADER_LEN {
            return (entries, entry_start);
        }

        let op = buf[pos];
        let key_len = u32::from_le_bytes(buf[pos + 1..pos + 5].try_into().unwrap()) as usize;
        let val_len = u32::from_le_bytes(buf[pos + 5..pos + 9].try_into().unwrap()) as usize;
        pos += HEADER_LEN;

        let body_len = key_len + val_len;
        if buf.len() - pos < body_len {
            // Partial body — truncate back to before this header.
            return (entries, entry_start);
        }

        let key_bytes = &buf[pos..pos + key_len];
        let val_bytes = &buf[pos + key_len..pos + key_len + val_len];
        pos += body_len;

        // UTF-8 failures are treated as corruption: stop replay here.
        let key = match std::str::from_utf8(key_bytes) {
            Ok(s) => s.to_owned(),
            Err(_) => return (entries, entry_start),
        };

        let entry = match op {
            OP_PUT => {
                let value = match std::str::from_utf8(val_bytes) {
                    Ok(s) => s.to_owned(),
                    Err(_) => return (entries, entry_start),
                };
                WalEntry::Put { key, value }
            }
            OP_DELETE => WalEntry::Delete { key },
            _ => {
                // Unknown op code — treat as corruption.
                return (entries, entry_start);
            }
        };

        entries.push(entry);
    }
}

// ---------------------------------------------------------------------------
// Wal impl
// ---------------------------------------------------------------------------

impl Wal {
    /// Open (or create) the WAL at `path`, replay it into a HashMap, truncate
    /// any partial trailing entry, and return both the Wal and the rebuilt map.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<(Wal, HashMap<String, String>)> {
        let path = path.as_ref().to_path_buf();

        // Clean up a leftover compact file from a previous crashed compaction.
        // The active log is authoritative; the .compact file is stale.
        let compact_path = compact_path(&path);
        if compact_path.exists() {
            tokio::fs::remove_file(&compact_path).await?;
        }

        // Read the existing log (or start empty).
        let raw = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => vec![],
            Err(e) => return Err(e),
        };

        let (entries, good_offset) = decode_entries(&raw);

        // Rebuild state from the decoded entries.
        let mut map = HashMap::new();
        for entry in entries {
            match entry {
                WalEntry::Put { key, value } => {
                    map.insert(key, value);
                }
                WalEntry::Delete { key } => {
                    map.remove(&key);
                }
            }
        }

        // Open the file for writing (create if not present).
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?;

        // Truncate any partial trailing entry detected during replay.
        if good_offset < raw.len() {
            file.set_len(good_offset as u64).await?;
        }

        // Seek to end so new appends follow existing content.
        file.seek(io::SeekFrom::End(0)).await?;

        let wal = Wal {
            writer: BufWriter::new(file),
            path,
        };
        Ok((wal, map))
    }

    /// Append one entry to the log and flush to the OS page cache.
    pub async fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
        let bytes = encode_entry(entry);
        self.writer.write_all(&bytes).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Rewrite the log to contain exactly one Put per live key in `snapshot`,
    /// then atomically replace the active log via rename.
    ///
    /// The caller must hold the WAL lock for the full duration and must have
    /// taken the HashMap snapshot while holding the db lock (then released it)
    /// before acquiring the WAL lock — never hold both simultaneously.
    pub async fn compact(&mut self, snapshot: &HashMap<String, String>) -> io::Result<()> {
        let compact = compact_path(&self.path);

        // Write the compact file from scratch.
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&compact)
            .await?;
        let mut writer = BufWriter::new(file);

        for (key, value) in snapshot {
            let bytes = encode_entry(&WalEntry::Put {
                key: key.clone(),
                value: value.clone(),
            });
            writer.write_all(&bytes).await?;
        }
        writer.flush().await?;
        // fsync before rename so the compact file is durable on disk.
        writer.get_ref().sync_data().await?;
        drop(writer);

        // Atomic swap: rename compact → active.
        tokio::fs::rename(&compact, &self.path).await?;

        // Reopen the active log for appending.
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.path)
            .await?;
        file.seek(io::SeekFrom::End(0)).await?;
        self.writer = BufWriter::new(file);

        Ok(())
    }
}

fn compact_path(wal_path: &Path) -> PathBuf {
    wal_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("wal.log.compact")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &str, value: &str) -> WalEntry {
        WalEntry::Put { key: key.into(), value: value.into() }
    }

    fn del(key: &str) -> WalEntry {
        WalEntry::Delete { key: key.into() }
    }

    fn roundtrip(entry: &WalEntry) -> Vec<u8> {
        encode_entry(entry)
    }

    // Encode a sequence of entries into one buffer.
    fn encode_all(entries: &[WalEntry]) -> Vec<u8> {
        entries.iter().flat_map(encode_entry).collect()
    }

    #[test]
    fn encode_decode_put() {
        let buf = roundtrip(&put("hello", "world"));
        let (entries, offset) = decode_entries(&buf);
        assert_eq!(entries.len(), 1);
        assert_eq!(offset, buf.len());
        match &entries[0] {
            WalEntry::Put { key, value } => {
                assert_eq!(key, "hello");
                assert_eq!(value, "world");
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn encode_decode_delete() {
        let buf = roundtrip(&del("foo"));
        let (entries, offset) = decode_entries(&buf);
        assert_eq!(entries.len(), 1);
        assert_eq!(offset, buf.len());
        match &entries[0] {
            WalEntry::Delete { key } => assert_eq!(key, "foo"),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn decode_multiple_entries() {
        let buf = encode_all(&[put("a", "1"), put("b", "2"), del("a")]);
        let (entries, offset) = decode_entries(&buf);
        assert_eq!(entries.len(), 3);
        assert_eq!(offset, buf.len());
    }

    #[test]
    fn decode_empty_buf() {
        let (entries, offset) = decode_entries(&[]);
        assert!(entries.is_empty());
        assert_eq!(offset, 0);
    }

    #[test]
    fn partial_header_truncated() {
        let buf = roundtrip(&put("key", "val"));
        // Truncate mid-header.
        let partial = &buf[..4];
        let (entries, offset) = decode_entries(partial);
        assert!(entries.is_empty());
        assert_eq!(offset, 0);
    }

    #[test]
    fn partial_body_truncated() {
        let buf = roundtrip(&put("key", "val"));
        // Keep header, drop half the body.
        let partial = &buf[..HEADER_LEN + 1];
        let (entries, offset) = decode_entries(partial);
        assert!(entries.is_empty());
        assert_eq!(offset, 0);
    }

    #[test]
    fn partial_second_entry_ignored() {
        let first = roundtrip(&put("a", "1"));
        let second = roundtrip(&put("b", "2"));
        let mut buf = first.clone();
        buf.extend_from_slice(&second[..HEADER_LEN]); // only the header of second
        let (entries, offset) = decode_entries(&buf);
        assert_eq!(entries.len(), 1);
        assert_eq!(offset, first.len()); // truncation point is after first entry
    }

    #[test]
    fn empty_key_and_value() {
        let buf = roundtrip(&put("", ""));
        let (entries, offset) = decode_entries(&buf);
        assert_eq!(entries.len(), 1);
        assert_eq!(offset, buf.len());
    }

    #[tokio::test]
    async fn open_nonexistent_file_returns_empty_map() {
        let dir = tempdir();
        let path = dir.path().join("wal.log");
        let (_, map) = Wal::open(&path).await.unwrap();
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn append_then_replay() {
        let dir = tempdir();
        let path = dir.path().join("wal.log");
        {
            let (mut wal, _) = Wal::open(&path).await.unwrap();
            wal.append(&put("x", "10")).await.unwrap();
            wal.append(&put("y", "20")).await.unwrap();
            wal.append(&del("x")).await.unwrap();
        }
        let (_, map) = Wal::open(&path).await.unwrap();
        assert_eq!(map.get("y"), Some(&"20".to_owned()));
        assert!(!map.contains_key("x"));
        assert_eq!(map.len(), 1);
    }

    #[tokio::test]
    async fn replay_truncates_partial_entry() {
        let dir = tempdir();
        let path = dir.path().join("wal.log");

        // Write one good entry then a partial frame.
        {
            let (mut wal, _) = Wal::open(&path).await.unwrap();
            wal.append(&put("good", "val")).await.unwrap();
        }
        // Corrupt: append a partial frame directly to the file.
        {
            use tokio::io::AsyncWriteExt;
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .await
                .unwrap();
            file.write_all(&[OP_PUT, 0, 0, 0, 5, 0, 0, 0, 3]) // header only, no body
                .await
                .unwrap();
        }

        let (_, map) = Wal::open(&path).await.unwrap();
        assert_eq!(map.get("good"), Some(&"val".to_owned()));
        assert_eq!(map.len(), 1);

        // File should be truncated to just the good entry.
        let size = tokio::fs::metadata(&path).await.unwrap().len();
        let expected = encode_entry(&put("good", "val")).len() as u64;
        assert_eq!(size, expected);
    }

    #[tokio::test]
    async fn compact_reduces_file() {
        let dir = tempdir();
        let path = dir.path().join("wal.log");
        {
            let (mut wal, _) = Wal::open(&path).await.unwrap();
            for i in 0..100 {
                wal.append(&put("k", &i.to_string())).await.unwrap();
            }
        }
        let size_before = tokio::fs::metadata(&path).await.unwrap().len();

        {
            let (mut wal, map) = Wal::open(&path).await.unwrap();
            wal.compact(&map).await.unwrap();
        }
        let size_after = tokio::fs::metadata(&path).await.unwrap().len();

        // After compaction, only one entry for "k" should remain.
        assert!(size_after < size_before, "compacted file should be smaller");

        let (_, map) = Wal::open(&path).await.unwrap();
        assert_eq!(map.get("k"), Some(&"99".to_owned()));
    }

    // Minimal temp-dir helper that cleans up on drop.
    struct TempDir(PathBuf);
    impl TempDir {
        fn path(&self) -> &Path { &self.0 }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn tempdir() -> TempDir {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(0);
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("kv_wal_test_{n}"));
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }
}
