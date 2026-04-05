use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use kv_store::wal::{Wal, WalEntry};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type Db = Arc<Mutex<HashMap<String, String>>>;

fn seeded_db(n: usize) -> Db {
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let mut map = db.lock().unwrap();
    for i in 0..n {
        map.insert(format!("key{i}"), "value".to_string());
    }
    drop(map);
    db
}

// Minimal temp-dir that cleans up on drop.
struct TempDir(PathBuf);
impl TempDir {
    fn path(&self) -> &Path { &self.0 }
}
impl Drop for TempDir {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
}
fn tempdir() -> TempDir {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CTR: AtomicU64 = AtomicU64::new(0);
    let n = CTR.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("kv_bench_{n}"));
    std::fs::create_dir_all(&path).unwrap();
    TempDir(path)
}

// ---------------------------------------------------------------------------
// In-memory storage benchmarks (Project 1 baseline)
// These remain useful for comparing lock strategies (Mutex vs RwLock vs DashMap)
// as the project evolves.
// ---------------------------------------------------------------------------

fn bench_put(c: &mut Criterion) {
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let mut i = 0u64;
    c.bench_function("mem/put", |b| {
        b.iter(|| {
            db.lock().unwrap().insert(format!("k{i}"), "v".to_string());
            i += 1;
        });
    });
}

fn bench_get_hit(c: &mut Criterion) {
    let db = seeded_db(1000);
    let mut i = 0u64;
    c.bench_function("mem/get/hit", |b| {
        b.iter(|| {
            let _ = db.lock().unwrap().get(&format!("key{}", i % 1000));
            i += 1;
        });
    });
}

fn bench_get_miss(c: &mut Criterion) {
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let mut i = 0u64;
    c.bench_function("mem/get/miss", |b| {
        b.iter(|| {
            let _ = db.lock().unwrap().get(&format!("nope{i}"));
            i += 1;
        });
    });
}

fn bench_delete_hit(c: &mut Criterion) {
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let mut i = 0u64;
    c.bench_function("mem/delete/hit", |b| {
        b.iter(|| {
            let key = format!("k{i}");
            db.lock().unwrap().insert(key.clone(), "v".to_string());
            db.lock().unwrap().remove(&key);
            i += 1;
        });
    });
}

fn bench_mixed(c: &mut Criterion) {
    let db = seeded_db(1000);
    let mut i = 0u64;
    c.bench_function("mem/mixed", |b| {
        b.iter(|| {
            let key = format!("key{}", i % 1000);
            if i % 3 == 0 {
                db.lock().unwrap().insert(key, "v".to_string());
            } else {
                let _ = db.lock().unwrap().get(&key);
            }
            i += 1;
        });
    });
}

fn bench_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("mem/mutex_contention");
    for n_threads in [1usize, 2, 4, 8] {
        group.bench_with_input(BenchmarkId::from_parameter(n_threads), &n_threads, |b, &n| {
            let db = seeded_db(1000);
            b.iter(|| {
                let handles: Vec<_> = (0..n)
                    .map(|t| {
                        let db = db.clone();
                        std::thread::spawn(move || {
                            let _ = db.lock().unwrap().get(&format!("key{}", t * 7 % 1000));
                        })
                    })
                    .collect();
                for h in handles { h.join().unwrap(); }
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// WAL benchmarks (Project 2)
//
// These use iter_custom to run async code without criterion's async executor
// overhead: we block on a tokio runtime and measure only the inner loop.
// ---------------------------------------------------------------------------

fn bench_wal_append(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempdir();
    let path = dir.path().join("wal.log");

    // Open the WAL once; all iterations append to the same growing file.
    // This matches real server behaviour: one persistent log, many appends.
    let wal = rt.block_on(async {
        let (wal, _) = Wal::open(&path).await.unwrap();
        Arc::new(tokio::sync::Mutex::new(wal))
    });

    c.bench_function("wal/append", |b| {
        b.iter_custom(|iters| {
            let wal = wal.clone();
            let entry = WalEntry::Put { key: "k".into(), value: "v".into() };
            rt.block_on(async move {
                let mut wal = wal.lock().await;
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    wal.append(&entry).await.unwrap();
                }
                start.elapsed()
            })
        });
    });
}

// How long does startup replay take as the log grows?
// Key question for Project 2: does replay stay fast enough to be acceptable?
fn bench_wal_replay(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("wal/replay");

    for n_entries in [100usize, 1_000, 10_000] {
        let dir = tempdir();
        let path = dir.path().join("wal.log");

        // Pre-seed the log with n_entries unique keys.
        rt.block_on(async {
            let (mut wal, _) = Wal::open(&path).await.unwrap();
            for i in 0..n_entries {
                wal.append(&WalEntry::Put { key: format!("k{i}"), value: "v".into() })
                    .await.unwrap();
            }
        });

        group.bench_with_input(
            BenchmarkId::from_parameter(n_entries),
            &path,
            |b, path| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            let _ = Wal::open(path).await.unwrap();
                        }
                        start.elapsed()
                    })
                });
            },
        );
    }
    group.finish();
}

// How long does compacting N keys take?
// Compaction writes N Put entries to a new file and renames it — I/O bound.
fn bench_wal_compact(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("wal/compact");

    for n_keys in [100usize, 1_000, 10_000] {
        let snapshot: HashMap<String, String> = (0..n_keys)
            .map(|i| (format!("k{i}"), "v".to_owned()))
            .collect();

        group.bench_with_input(
            BenchmarkId::from_parameter(n_keys),
            &snapshot,
            |b, snapshot| {
                b.iter_custom(|iters| {
                    let dir = tempdir();
                    let path = dir.path().join("wal.log");
                    rt.block_on(async {
                        let (mut wal, _) = Wal::open(&path).await.unwrap();
                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            wal.compact(snapshot).await.unwrap();
                        }
                        start.elapsed()
                    })
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    mem_benches,
    bench_put,
    bench_get_hit,
    bench_get_miss,
    bench_delete_hit,
    bench_mixed,
    bench_contention,
);
criterion_group!(wal_benches, bench_wal_append, bench_wal_replay, bench_wal_compact);
criterion_main!(mem_benches, wal_benches);
