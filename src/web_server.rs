use axum::{
    extract::State,
    response::{Html, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use dashmap::DashMap;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::info;

#[derive(Clone, Serialize, Debug)]
pub struct MarketData {
    pub id: String,
    pub name: String,
    pub countdown: String,
    pub yes_price: Option<f64>,
    pub no_price: Option<f64>,
    pub sum: Option<f64>,
    pub diff: Option<f64>,
    pub update_time: i64,
}

// Shared state for controlling the bot
#[derive(Clone)]
pub struct AppState {
    pub is_running: Arc<AtomicBool>,
    pub market_data: Arc<DashMap<String, MarketData>>,
}

#[derive(Serialize)]
struct StatusResponse {
    running: bool,
}

#[derive(Deserialize)]
struct ControlRequest {
    action: String, // "start" or "stop"
}

pub async fn start_server(is_running: Arc<AtomicBool>, market_data: Arc<DashMap<String, MarketData>>) {
    let state = AppState { is_running, market_data };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/status", get(status_handler))
        .route("/api/control", post(control_handler))
        .route("/api/logs", get(logs_handler))
        .route("/api/trades", get(trades_handler))
        .route("/api/markets", get(markets_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = "0.0.0.0:3000";
    info!("🚀 Control server listening on http://{}", addr);

    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn index_handler() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    let running = state.is_running.load(Ordering::Relaxed);
    Json(StatusResponse { running })
}

async fn markets_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut markets: Vec<MarketData> = state.market_data.iter().map(|r| r.value().clone()).collect();
    // 按更新时间倒序排序
    markets.sort_by(|a, b| b.update_time.cmp(&a.update_time));
    Json(markets)
}

async fn logs_handler() -> impl IntoResponse {
    use crate::utils::logger::LOG_BUFFER;
    
    let logs: Vec<String> = if let Ok(buffer) = LOG_BUFFER.lock() {
        buffer.iter().cloned().collect::<Vec<_>>()
    } else {
        vec!["无法获取日志锁".to_string()]
    };
    
    Json(logs)
}

async fn trades_handler() -> impl IntoResponse {
    use crate::utils::trade_history;
    Json(trade_history::get_trades())
}

async fn control_handler(
    State(state): State<AppState>,
    Json(payload): Json<ControlRequest>,
) -> impl IntoResponse {
    match payload.action.as_str() {
        "start" => {
            state.is_running.store(true, Ordering::Relaxed);
            info!("▶️ Bot started via web interface");
        }
        "stop" => {
            state.is_running.store(false, Ordering::Relaxed);
            info!("⏸️ Bot stopped via web interface");
        }
        _ => {}
    }

    let running = state.is_running.load(Ordering::Relaxed);
    Json(StatusResponse { running })
}
