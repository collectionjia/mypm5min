use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use dashmap::DashMap;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::trading::TradingExecutor;
use crate::utils::balance_checker::get_usdc_balance;
use poly_5min_bot::positions::{get_positions, Position};

#[derive(Clone, Serialize, Debug)]
pub struct MarketData {
    pub id: String,
    pub name: String,
    pub category: String,
    pub countdown: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub yes_price: Option<f64>,
    pub no_price: Option<f64>,
    pub price_to_beat: Option<f64>,
    pub sum: Option<f64>,
    pub diff: Option<f64>,
    pub update_time: i64,
}

// Shared state for controlling the bot
#[derive(Clone)]
pub struct AppState {
    pub is_running: Arc<AtomicBool>,
    pub market_data: Arc<DashMap<String, MarketData>>,
    pub executor: Option<Arc<TradingExecutor>>,
}

#[derive(Serialize)]
struct StatusResponse {
    running: bool,
}

#[derive(Serialize)]
struct CloseAllResponse {
    success: bool,
    message: String,
    positions_closed: usize,
}

#[derive(Deserialize)]
struct BuyRequest {
    market_id: String,
    side: String,
    qty: Option<f64>,
}

#[derive(Deserialize)]
struct TradesQueryParams {
    page: Option<usize>,
    page_size: Option<usize>,
}

#[derive(Serialize)]
struct TradesResponse {
    trades: Vec<crate::utils::trade_history::TradeRecord>,
    total: usize,
    page: usize,
    page_size: usize,
    total_pages: usize,
}

#[derive(Serialize)]
struct BuyResponse {
    success: bool,
    message: String,
    order_id: Option<String>,
}

#[derive(Serialize)]
struct PositionView {
    pub asset: String,
    pub title: String,
    pub size: Decimal,
    pub cur_price: Decimal,
}

#[derive(Serialize)]
struct PortfolioResponse {
    balance: Option<String>,
    positions: Vec<PositionView>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct ControlRequest {
    action: String, // "start" or "stop"
}

pub async fn start_server(
    is_running: Arc<AtomicBool>,
    market_data: Arc<DashMap<String, MarketData>>,
    executor: Option<Arc<TradingExecutor>>,
) {
    let state = AppState {
        is_running,
        market_data,
        executor,
    };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/status", get(status_handler))
        .route("/api/control", post(control_handler))
        .route("/api/close_all", post(close_all_handler))
        .route("/api/buy", post(buy_handler))
        .route("/api/logs", get(logs_handler))
        .route("/api/trades", get(trades_handler))
        .route("/api/trades/clear", post(clear_trades_handler))
        .route("/api/markets", get(markets_handler))
        .route("/api/portfolio", get(portfolio_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = "0.0.0.0:3001";
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

async fn portfolio_handler() -> Json<PortfolioResponse> {
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            return Json(PortfolioResponse {
                balance: None,
                positions: vec![],
                error: Some(format!("配置加载失败: {}", e)),
            });
        }
    };

    let proxy_address = match config.proxy_address {
        Some(addr) => addr,
        None => {
            return Json(PortfolioResponse {
                balance: None,
                positions: vec![],
                error: Some("未配置 POLYMARKET_PROXY_ADDRESS".to_string()),
            });
        }
    };

    // 获取余额
    let balance = match get_usdc_balance(proxy_address).await {
        Ok(b) => Some(b.to_string()),
        Err(e) => {
            warn!("获取余额失败: {}", e);
            None
        }
    };

    // 获取持仓
    let positions = match get_positions().await {
        Ok(p) => p
            .into_iter()
            .map(|pos| PositionView {
                asset: pos.asset.to_string(),
                title: pos.title,
                size: pos.size,
                cur_price: pos.cur_price,
            })
            .collect(),
        Err(e) => {
            warn!("获取持仓失败: {}", e);
            vec![]
        }
    };

    Json(PortfolioResponse {
        balance,
        positions,
        error: None,
    })
}

async fn close_all_handler(State(state): State<AppState>) -> impl IntoResponse {
    let executor = match &state.executor {
        Some(exec) => exec,
        None => {
            return Json(CloseAllResponse {
                success: false,
                message: "Executor not initialized".to_string(),
                positions_closed: 0,
            })
        }
    };

    info!("🛑 收到Web控制台平仓指令，开始执行平仓...");

    let mut closed_count = 0;

    // 获取当前持仓
    match get_positions().await {
        Ok(positions) => {
            let active_positions: Vec<_> =
                positions.iter().filter(|p| p.size > dec!(0.01)).collect();
            if active_positions.is_empty() {
                info!("✅ 当前无剩余持仓");
                return Json(CloseAllResponse {
                    success: true,
                    message: "当前无剩余持仓".to_string(),
                    positions_closed: 0,
                });
            }

            info!("🔍 发现 {} 个持仓需要平仓", active_positions.len());

            for pos in active_positions {
                let size_floor = (pos.size * dec!(100)).floor() / dec!(100);
                if size_floor < dec!(0.01) {
                    continue;
                }

                let sell_price = dec!(0.01);

                match executor
                    .sell_at_price(pos.asset, sell_price, size_floor)
                    .await
                {
                    Ok(_) => {
                        info!(
                            "✅ 已下卖单 | token_id={:#x} | 数量:{} | 价格:{:.4}",
                            pos.asset, size_floor, sell_price
                        );
                        closed_count += 1;
                    }
                    Err(e) => {
                        error!("❌ 平仓失败 | token_id={:#x} | 错误:{}", pos.asset, e);
                    }
                }
            }
        }
        Err(e) => {
            error!("❌ 获取持仓失败: {}", e);
            return Json(CloseAllResponse {
                success: false,
                message: format!("获取持仓失败: {}", e),
                positions_closed: 0,
            });
        }
    }

    Json(CloseAllResponse {
        success: true,
        message: format!("已触发平仓 {} 个持仓", closed_count),
        positions_closed: closed_count,
    })
}

async fn buy_handler(
    State(state): State<AppState>,
    Json(payload): Json<BuyRequest>,
) -> impl IntoResponse {
    let executor = match &state.executor {
        Some(exec) => exec.clone(),
        None => {
            return Json(BuyResponse {
                success: false,
                message: "Executor not initialized".to_string(),
                order_id: None,
            });
        }
    };

    let market = match state.market_data.get(&payload.market_id) {
        Some(m) => m.clone(),
        None => {
            return Json(BuyResponse {
                success: false,
                message: format!("未找到市场: {}", payload.market_id),
                order_id: None,
            });
        }
    };

    let side = payload.side.trim().to_uppercase();
    let (token_id_str, price_f64_opt) = match side.as_str() {
        "YES" => (market.yes_token_id, market.yes_price),
        "NO" => (market.no_token_id, market.no_price),
        _ => {
            return Json(BuyResponse {
                success: false,
                message: "side 仅支持 YES 或 NO".to_string(),
                order_id: None,
            });
        }
    };

    let price_f64 = match price_f64_opt {
        Some(p) => p,
        None => {
            return Json(BuyResponse {
                success: false,
                message: format!("当前无 {} 价格数据，无法下单", side),
                order_id: None,
            });
        }
    };

    let token_id = match polymarket_client_sdk::types::U256::from_str(&token_id_str) {
        Ok(v) => v,
        Err(e) => {
            return Json(BuyResponse {
                success: false,
                message: format!("token_id 解析失败: {}", e),
                order_id: None,
            });
        }
    };

    let price = match Decimal::try_from(price_f64).map(|p| p.round_dp(2)) {
        Ok(p) => p,
        Err(e) => {
            return Json(BuyResponse {
                success: false,
                message: format!("价格解析失败: {}", e),
                order_id: None,
            });
        }
    };

    let usd_amount = dec!(1.01).round_dp(2);
    let mut size = (usd_amount / price).round_dp(0);
    if size < dec!(1) {
        size = dec!(1);
    }

    info!(
        "🛒 Web手动买入(市价意图) | market_id={} | side={} | token_id={} | ask={:.4} | usd={} | size={}",
        payload.market_id, side, token_id_str, price_f64, usd_amount, size
    );

    if !state.is_running.load(Ordering::Relaxed) {
        use crate::utils::trade_history::{add_trade, TradeRecord};
        use chrono::Utc;
        use rust_decimal::prelude::ToPrimitive;
        let sim_order_id = format!("SIM-{}", Utc::now().timestamp_millis());

        add_trade(TradeRecord {
            id: sim_order_id.clone(),
            market_id: payload.market_id.clone(),
            market_slug: market.name.clone(),
            side: side.clone(),
            order_price: price.to_f64().unwrap_or(0.0),
            price: price.to_f64().unwrap_or(0.0),
            size: size.to_f64().unwrap_or(0.0),
            timestamp: Utc::now().timestamp(),
            status: "SimBought".to_string(),
            profit: None,
        });

        return Json(BuyResponse {
            success: true,
            message: format!("模拟下单成功: {}", sim_order_id),
            order_id: Some(sim_order_id),
        });
    }

    match executor.buy_market_usd(token_id, price, usd_amount).await {
        Ok(resp) => {
            use crate::utils::trade_history::{add_trade, TradeRecord};
            use chrono::Utc;
            use rust_decimal::prelude::ToPrimitive;

            add_trade(TradeRecord {
                id: resp.order_id.clone(),
                market_id: payload.market_id.clone(),
                market_slug: market.name.clone(),
                side: side.clone(),
                order_price: price.to_f64().unwrap_or(0.0),
                price: price.to_f64().unwrap_or(0.0),
                size: size.to_f64().unwrap_or(0.0),
                timestamp: Utc::now().timestamp(),
                status: "Bought".to_string(),
                profit: None,
            });

            Json(BuyResponse {
                success: true,
                message: format!("下单成功: {}", resp.order_id),
                order_id: Some(resp.order_id),
            })
        }
        Err(e) => Json(BuyResponse {
            success: false,
            message: format!("下单失败: {}", e),
            order_id: None,
        }),
    }
}

async fn markets_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut markets: Vec<MarketData> = state
        .market_data
        .iter()
        .map(|r| r.value().clone())
        .collect();
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

async fn trades_handler(
    State(state): State<AppState>,
    Query(params): Query<TradesQueryParams>,
) -> impl IntoResponse {
    use crate::utils::trade_history;
    let mut trades = trade_history::get_trades();

    for trade in &mut trades {
        if trade.status == "Won"
            || trade.status == "Lost"
            || trade.status == "SimSkipped"
            || trade.status == "SimPosted"
        {
            continue;
        }

        let (yes_price, no_price) = state
            .market_data
            .get(&trade.market_id)
            .map(|m| (m.yes_price, m.no_price))
            .unwrap_or((None, None));

        let mut new_status: Option<&'static str> = None;
        if trade.side == "YES" {
            if yes_price.is_none() || yes_price.unwrap_or(0.0) > 0.99 {
                new_status = Some("Won");
            } else if yes_price.unwrap_or(1.0) < 0.01 {
                new_status = Some("Lost");
            }
        } else if trade.side == "NO" {
            if no_price.is_none() || no_price.unwrap_or(0.0) > 0.99 {
                new_status = Some("Won");
            } else if no_price.unwrap_or(1.0) < 0.01 {
                new_status = Some("Lost");
            }
        }

        if let Some(s) = new_status {
            trade.status = s.to_string();
            trade_history::update_trade_status(&trade.id, s);
        }
    }

    let total = trades.len();
    let page_size = params.page_size.unwrap_or(10).max(1).min(100);
    let page = params.page.unwrap_or(1).max(1);
    let start = (page - 1) * page_size;
    let end = start + page_size;

    let paged_trades: Vec<_> = trades.into_iter().skip(start).take(page_size).collect();
    let total_pages = (total + page_size - 1) / page_size;

    Json(TradesResponse {
        trades: paged_trades,
        total,
        page,
        page_size,
        total_pages,
    })
}

async fn clear_trades_handler() -> impl IntoResponse {
    use crate::utils::trade_history;
    trade_history::clear_trades();
    Json(serde_json::json!({ "success": true }))
}

async fn control_handler(
    State(state): State<AppState>,
    Json(payload): Json<ControlRequest>,
) -> impl IntoResponse {
    match payload.action.as_str() {
        "start" => {
            state.is_running.store(true, Ordering::Relaxed);
            info!("▶️ 已开启真实投注（web控制台）");
        }
        "stop" => {
            state.is_running.store(false, Ordering::Relaxed);
            info!("🧪 已切换为模拟交易（web控制台）");
            if let Some(exec) = state.executor.clone() {
                tokio::spawn(async move {
                    if let Err(e) = exec.cancel_all_orders().await {
                        warn!(error = %e, "模拟交易切换：取消所有挂单失败");
                    } else {
                        info!("模拟交易切换：已取消所有挂单");
                    }
                });
            }
        }
        _ => {}
    }

    let running = state.is_running.load(Ordering::Relaxed);
    Json(StatusResponse { running })
}
