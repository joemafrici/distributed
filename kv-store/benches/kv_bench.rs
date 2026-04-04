use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

// --- single-operation benchmarks ---

fn bench_put(c: &mut Criterion) {
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let mut i = 0u64;
    c.bench_function("put", |b| {
        b.iter(|| {
            db.lock().unwrap().insert(format!("k{i}"), "v".to_string());
            i += 1;
        });
    });
}

fn bench_get_hit(c: &mut Criterion) {
    let db = seeded_db(1000);
    let mut i = 0u64;
    c.bench_function("get/hit", |b| {
        b.iter(|| {
            let _ = db.lock().unwrap().get(&format!("key{}", i % 1000));
            i += 1;
        });
    });
}

fn bench_get_miss(c: &mut Criterion) {
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let mut i = 0u64;
    c.bench_function("get/miss", |b| {
        b.iter(|| {
            let _ = db.lock().unwrap().get(&format!("nope{i}"));
            i += 1;
        });
    });
}

fn bench_delete_hit(c: &mut Criterion) {
    // Repopulate each batch so deletes always hit.
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let mut i = 0u64;
    c.bench_function("delete/hit", |b| {
        b.iter(|| {
            let key = format!("k{i}");
            {
                db.lock().unwrap().insert(key.clone(), "v".to_string());
            }
            db.lock().unwrap().remove(&key);
            i += 1;
        });
    });
}

// --- mixed workload (1/3 writes, 2/3 reads) ---

fn bench_mixed(c: &mut Criterion) {
    let db = seeded_db(1000);
    let mut i = 0u64;
    c.bench_function("mixed", |b| {
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

// --- mutex contention: same key across N threads ---
// This is the benchmark most likely to change when switching from
// Mutex<HashMap> to RwLock<HashMap> or a lock-free structure.

fn bench_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("mutex_contention");
    for n_threads in [1usize, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_threads),
            &n_threads,
            |b, &n| {
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
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_put,
    bench_get_hit,
    bench_get_miss,
    bench_delete_hit,
    bench_mixed,
    bench_contention
);
criterion_main!(benches);
