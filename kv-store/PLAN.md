# Project 2 — Persistent Key-Value Store: Implementation Plan

## Overview

Add durability to the existing in-memory store via a write-ahead log (WAL):
an append-only file where every write is recorded before being applied to the
HashMap. On startup, replay the log to reconstruct state. Periodic compaction
rewrites the log to its minimal form.

---

## File Structure

```
src/
  main.rs    -- AppState, axum setup, handler wiring, compaction task
  wal.rs     -- Wal struct, encode/decode, replay, compaction
```

No Cargo.toml changes needed — `tokio = { features = ["full"] }` already
includes `tokio::fs`, `tokio::io::BufWriter`, `tokio::sync::Mutex`,
and `tokio::time`.

---

## 1. WAL Entry Format

Binary, fixed-width header — no third-party crates needed.

```
[1 byte: op_type] [4 bytes: key_len u32 LE] [4 bytes: val_len u32 LE] [key bytes] [val bytes]
```

- `0x01` = Put, `0x02` = Delete
- Delete always has `val_len = 0`
- Total frame: `9 + key_len + val_len` bytes

**Partial write detection on replay:** read the 9-byte header; if fewer bytes
remain, stop. Then read `key_len + val_len` bytes; if fewer remain, stop and
truncate the file to the offset of the start of that entry. A partial entry
means the write was never acknowledged to the client, so discarding it is
correct.

No checksum — a process kill (`kill -9`) is the tested failure mode, not bit
rot. Note this as a deliberate tradeoff.

---

## 2. Types

### `src/wal.rs`

```rust
pub enum WalEntry {
    Put { key: String, value: String },
    Delete { key: String },
}

pub struct Wal {
    writer: BufWriter<tokio::fs::File>,
    path: PathBuf,
}

impl Wal {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<(Wal, HashMap<String, String>)>
    pub async fn append(&mut self, entry: &WalEntry) -> io::Result<()>
    pub async fn compact(&mut self, snapshot: &HashMap<String, String>) -> io::Result<()>
}

fn encode_entry(entry: &WalEntry) -> Vec<u8>
fn decode_entries(buf: &[u8]) -> (Vec<WalEntry>, usize)  // usize = last good byte offset
```

### `src/main.rs`

```rust
#[derive(Clone)]
struct AppState {
    db:  Arc<std::sync::Mutex<HashMap<String, String>>>,
    wal: Arc<tokio::sync::Mutex<Wal>>,
}
```

`db` uses `std::sync::Mutex` (never held across `.await`).
`wal` uses `tokio::sync::Mutex` because `append` and `compact` both contain
`.await` points — holding a `std::sync::Mutex` guard across `.await` can
deadlock the tokio scheduler.

---

## 3. Flush Strategy

Call `writer.flush().await?` after every `append`. This drains the userspace
buffer to the OS page cache, which survives a process kill. It does NOT
survive a kernel crash or power failure (that requires `sync_data()`).

This satisfies the done-condition: "kill the process mid-write, restart,
all committed writes survive."

To upgrade to power-loss durability later:
```rust
writer.flush().await?;
writer.get_ref().sync_data().await?;  // BufWriter doesn't expose sync_data directly
```

---

## 4. Startup Replay (`Wal::open`)

1. `OpenOptions::new().read(true).write(true).create(true).open(&path)`
2. Read entire file into `Vec<u8>` (`tokio::fs::read`)
3. Walk the buffer with `decode_entries`:
   - Record `pos` before reading each header
   - If header read fails (< 9 bytes): break
   - If body read fails: break, this is the truncation point
   - UTF-8 decode failure: treat as corruption, break
   - Apply entries to a local `HashMap` (insert for Put, remove for Delete)
4. If replay stopped early: `file.set_len(last_good_offset).await?`
5. `file.seek(SeekFrom::End(0)).await?`
6. Wrap in `BufWriter`, return `(Wal, HashMap)`

On startup, also delete any leftover `wal.log.compact` file — it is evidence
of a crashed previous compaction; `wal.log` is authoritative.

---

## 5. Handler Flow (WAL before HashMap — always)

```rust
async fn put_key(Path(key): Path<String>, State(state): State<AppState>, body: String)
    -> impl IntoResponse
{
    // 1. Write WAL (async — holds tokio mutex across .await)
    let entry = WalEntry::Put { key: key.clone(), value: body.clone() };
    {
        let mut wal = state.wal.lock().await;
        if let Err(_) = wal.append(&entry).await {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }
    // 2. Update HashMap (sync — std mutex, no .await)
    state.db.lock().unwrap().insert(key, body);
    StatusCode::OK.into_response()
}
```

If the process dies between step 1 and step 2: replay re-applies the entry.
If the process dies during step 1: the partial entry is truncated on replay.
Either way, the client never received a 200, so no acknowledged write is lost.

For `delete_key`: check the HashMap first; return 404 early if the key does
not exist. Only write the WAL entry if the key is present (avoids useless
Delete entries inflating the log).

`get_key` is unchanged — reads never touch the WAL.

---

## 6. Compaction

**Strategy: two-file atomic swap.**

1. Snapshot the HashMap under `db` lock, then release it.
2. Acquire WAL lock.
3. Write `wal.log.compact` — one Put entry per live key in the snapshot.
4. `flush()` + `sync_data()` the compact file (fsync before rename).
5. `tokio::fs::rename("wal.log.compact", "wal.log")` — atomic on POSIX,
   same filesystem required.
6. Reopen `wal.log` with append semantics, replace `self.writer`.

**Crash safety:**
- Crash during step 3/4: `wal.log.compact` is partial; `wal.log` untouched.
  Startup deletes the leftover `.compact` file (step above).
- Crash after step 5: `wal.log` is the compacted file; replay succeeds.

**Locking discipline:** never hold `db` lock and `wal` lock simultaneously
(take snapshot while holding `db`, release it, then acquire `wal`). Violating
this order risks deadlock.

**When to trigger:** background tokio task on a 60-second interval:

```rust
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        let snapshot = state.db.lock().unwrap().clone();
        let mut wal = state.wal.lock().await;
        if let Err(e) = wal.compact(&snapshot).await {
            eprintln!("compaction error: {e}");
        }
    }
});
```

Known limitation: the WAL lock blocks all writes during compaction. Acceptable
for this project; production systems use a separate active segment.

---

## 7. Implementation Order

1. **`wal.rs` encode/decode only** — `WalEntry`, `encode_entry`, `decode_entries`.
   No server changes yet. Unit-test round-trip and truncated-buffer handling.

2. **`Wal::open` with replay** — open file, decode, build HashMap, truncate if
   partial, seek to end. Test manually with raw byte files.

3. **`Wal::append`** — encode + write + flush. Test by open → append → open again
   and verifying HashMap contents.

4. **Restructure `AppState`, wire handlers** — replace `type Db` with `AppState`,
   update `main` to call `Wal::open`, update `put_key` and `delete_key`.

5. **Verify done-condition** — PUT keys, `kill -9`, restart, GET keys.

6. **`Wal::compact`** — write compact file, fsync, rename, reopen writer.

7. **Background compaction task** — spawn in `main`, timer-based.

8. **Startup cleanup** — delete leftover `wal.log.compact` at open time.

---

## 8. Key Edge Cases

| Case | Handling |
|---|---|
| Partial write at crash | Detect by header/body short-read; truncate to last good offset |
| Non-UTF-8 key or value | Treat as corruption; stop replay at that entry |
| Crash during compaction | Startup deletes leftover `.compact`; `wal.log` is authoritative |
| Delete of non-existent key | Return 404 before writing WAL — no log entry for no-op |
| `sync_data()` on BufWriter | Must use `writer.get_ref().sync_data()` — BufWriter doesn't expose it directly |
| `.compact` file path | Use `parent().join("wal.log.compact")` explicitly; `with_extension` replaces only the last extension and may surprise |
