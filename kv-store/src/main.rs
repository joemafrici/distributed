use kv_store::wal::{Wal, WalEntry};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, put},
    Router,
};

#[derive(Clone)]
struct AppState {
    db:  Arc<Mutex<HashMap<String, String>>>,
    wal: Arc<tokio::sync::Mutex<Wal>>,
}

#[tokio::main]
async fn main() {
    let wal_path = std::env::var("KV_WAL_PATH").unwrap_or_else(|_| "wal.log".to_owned());

    let (wal, map) = Wal::open(&wal_path).await.expect("failed to open WAL");
    let state = AppState {
        db:  Arc::new(Mutex::new(map)),
        wal: Arc::new(tokio::sync::Mutex::new(wal)),
    };

    // Background compaction: every 60 seconds, rewrite the log to its minimal form.
    tokio::spawn({
        let state = state.clone();
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                // Snapshot the map while holding db lock, then release before
                // acquiring the WAL lock — never hold both simultaneously.
                let snapshot = state.db.lock().unwrap().clone();
                let mut wal = state.wal.lock().await;
                if let Err(e) = wal.compact(&snapshot).await {
                    eprintln!("compaction error: {e}");
                }
            }
        }
    });

    let app = Router::new()
        .route("/key/{k}", get(get_key))
        .route("/key/{k}", put(put_key))
        .route("/key/{k}", delete(delete_key))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_key(
    Path(key): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let db = state.db.lock().unwrap();
    match db.get(&key) {
        Some(value) => (StatusCode::OK, value.clone()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn put_key(
    Path(key): Path<String>,
    State(state): State<AppState>,
    body: String,
) -> impl IntoResponse {
    let entry = WalEntry::Put { key: key.clone(), value: body.clone() };

    // WAL must be written before the HashMap is updated. If the process dies
    // between the two steps, replay will reconstruct the correct state.
    {
        let mut wal = state.wal.lock().await; // tokio mutex — safe to hold across .await
        if let Err(e) = wal.append(&entry).await {
            eprintln!("WAL append error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    state.db.lock().unwrap().insert(key, body);
    StatusCode::OK.into_response()
}

async fn delete_key(
    Path(key): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // Check existence before writing WAL — no log entry for a no-op delete.
    if !state.db.lock().unwrap().contains_key(&key) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let entry = WalEntry::Delete { key: key.clone() };
    {
        let mut wal = state.wal.lock().await;
        if let Err(e) = wal.append(&entry).await {
            eprintln!("WAL append error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    state.db.lock().unwrap().remove(&key);
    StatusCode::OK.into_response()
}
