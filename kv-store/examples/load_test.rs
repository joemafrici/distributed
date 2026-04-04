/// HTTP load tester for kv-store.
///
/// Usage:
///   load_test [--host 127.0.0.1:3000] [--concurrency 20] [--requests 10000]
///             [--workload write|read|mixed]
///
/// Each worker maintains a single persistent HTTP/1.1 connection. On error it
/// reconnects and continues. The reported latencies exclude failed requests.

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::client::conn::http1;
use hyper::Request;
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Workload {
    Write,
    Read,
    Mixed,
}

impl std::str::FromStr for Workload {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "write" => Ok(Workload::Write),
            "read" => Ok(Workload::Read),
            "mixed" => Ok(Workload::Mixed),
            other => Err(format!("unknown workload '{other}' — use write|read|mixed")),
        }
    }
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Workload::Write => "write",
            Workload::Read => "read",
            Workload::Mixed => "mixed",
        }
    }
}

#[derive(Clone)]
struct Config {
    addr: SocketAddr,
    concurrency: usize,
    requests: usize, // total across all workers
    workload: Workload,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            addr: "127.0.0.1:3000".parse().unwrap(),
            concurrency: 20,
            requests: 10_000,
            workload: Workload::Mixed,
        }
    }
}

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut cfg = Config::default();
    let mut i = 1;
    while i < args.len() {
        let key = args[i].as_str();
        let val = || args.get(i + 1).map(|s| s.as_str()).unwrap_or("");
        match key {
            "--host" => {
                cfg.addr = val().parse().expect("--host must be <ip>:<port>");
                i += 2;
            }
            "--concurrency" | "-c" => {
                cfg.concurrency = val().parse().expect("--concurrency must be a number");
                i += 2;
            }
            "--requests" | "-n" => {
                cfg.requests = val().parse().expect("--requests must be a number");
                i += 2;
            }
            "--workload" | "-w" => {
                cfg.workload = val().parse().expect("bad --workload");
                i += 2;
            }
            _ => i += 1,
        }
    }
    cfg
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

struct WorkerResult {
    latencies: Vec<Duration>,
    errors: u64,
}

async fn connect(addr: SocketAddr) -> Option<http1::SendRequest<Full<Bytes>>> {
    let stream = TcpStream::connect(addr).await.ok()?;
    let io = TokioIo::new(stream);
    let (sender, conn) = http1::handshake(io).await.ok()?;
    tokio::spawn(async move { let _ = conn.await; });
    Some(sender)
}

async fn run_worker(cfg: Arc<Config>, worker_id: usize, per_worker: usize) -> WorkerResult {
    let host = cfg.addr.to_string();
    let mut sender = match connect(cfg.addr).await {
        Some(s) => s,
        None => {
            eprintln!("worker {worker_id}: failed to connect");
            return WorkerResult {
                latencies: vec![],
                errors: per_worker as u64,
            };
        }
    };

    let mut latencies = Vec::with_capacity(per_worker);
    let mut errors = 0u64;

    for i in 0..per_worker {
        // Distribute keys across a fixed-size keyspace so read workloads can
        // find keys written by the warm-up pass.
        let key = format!("k{}", (worker_id * per_worker + i) % 10_000);

        let req = build_request(cfg.workload, &key, &host, i);

        let t0 = Instant::now();
        match sender.send_request(req).await {
            Ok(resp) => {
                // Drain the body so the connection can be reused.
                let _ = resp.into_body().collect().await;
                latencies.push(t0.elapsed());
            }
            Err(_) => {
                errors += 1;
                // Attempt to reconnect once.
                if let Some(new_sender) = connect(cfg.addr).await {
                    sender = new_sender;
                }
            }
        }
    }

    WorkerResult { latencies, errors }
}

fn build_request(
    workload: Workload,
    key: &str,
    host: &str,
    seq: usize,
) -> Request<Full<Bytes>> {
    match workload {
        Workload::Write => put(key, host),
        Workload::Read => get(key, host),
        Workload::Mixed => {
            if seq % 3 == 0 {
                put(key, host)
            } else {
                get(key, host)
            }
        }
    }
}

fn put(key: &str, host: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .method("PUT")
        .uri(format!("/key/{key}"))
        .header("host", host)
        .body(Full::new(Bytes::from("value")))
        .unwrap()
}

fn get(key: &str, host: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .method("GET")
        .uri(format!("/key/{key}"))
        .header("host", host)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let cfg = Arc::new(parse_args());
    let per_worker = (cfg.requests / cfg.concurrency).max(1);
    let total = cfg.concurrency * per_worker;

    println!(
        "workload={:<6}  concurrency={:<4}  requests={}",
        cfg.workload.name(),
        cfg.concurrency,
        total
    );

    let t0 = Instant::now();

    let handles: Vec<_> = (0..cfg.concurrency)
        .map(|id| {
            let cfg = cfg.clone();
            tokio::spawn(run_worker(cfg, id, per_worker))
        })
        .collect();

    let mut all_latencies: Vec<Duration> = Vec::with_capacity(total);
    let mut total_errors = 0u64;

    for handle in handles {
        let r = handle.await.unwrap();
        all_latencies.extend(r.latencies);
        total_errors += r.errors;
    }

    let elapsed = t0.elapsed();
    let successful = (total as u64).saturating_sub(total_errors);
    let throughput = successful as f64 / elapsed.as_secs_f64();

    all_latencies.sort_unstable();

    println!("duration    {:.3}s", elapsed.as_secs_f64());
    println!("throughput  {:.0} req/s", throughput);
    println!("errors      {total_errors}");
    if !all_latencies.is_empty() {
        println!(
            "latency     p50={:.2}ms  p95={:.2}ms  p99={:.2}ms  max={:.2}ms",
            ms(percentile(&all_latencies, 50.0)),
            ms(percentile(&all_latencies, 95.0)),
            ms(percentile(&all_latencies, 99.0)),
            ms(*all_latencies.last().unwrap()),
        );
    }
}
