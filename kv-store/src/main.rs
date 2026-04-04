use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, put},
    Router,
};

type Db = Arc<Mutex<HashMap<String, String>>>;

#[tokio::main]
async fn main() {
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let app = Router::new()
        .route("/key/{k}", get(get_key))
        .route("/key/{k}", put(put_key))
        .route("/key/{k}", delete(delete_key))
        .with_state(db);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_key(
    Path(key): Path<String>,
    State(db): State<Db>,
) -> impl IntoResponse {
    let db = db.lock().unwrap();
    match db.get(&key) {
        Some(value) => (StatusCode::OK, value.clone()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn put_key(
    Path(key): Path<String>,
    State(db): State<Db>,
    body: String,
) -> impl IntoResponse {
    let mut db = db.lock().unwrap();
    db.insert(key, body);
    StatusCode::OK
}

async fn delete_key(
    Path(key): Path<String>,
    State(db): State<Db>,
) -> impl IntoResponse {
    let mut db = db.lock().unwrap();
    match db.remove(&key) {
        Some(_) => StatusCode::OK,
        None => StatusCode::NOT_FOUND,
    }
}
