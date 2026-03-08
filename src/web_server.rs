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
use tracing::{info, warn, error};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::trading::TradingExecutor;
use poly_5min_bot::positions::{get_positions, Position};
use crate::config::Config;
use crate::merge;
use crate::utils::balance_checker::get_usdc_balance;
use alloy::primitives::Address;
use std::str::FromStr;

#[derive(Clone, Serialize, Debug)]
pub struct MarketData {
    pub id: String,
    pub name: String,
    pub category: String,
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

#[derive(Serialize)]
struct RedeemResponse {
    success: bool,
    message: String,
    tx_hashes: Vec<String>,
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
    let state = AppState { is_running, market_data, executor };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/status", get(status_handler))
        .route("/api/control", post(control_handler))
        .route("/api/close_all", post(close_all_handler))
        .route("/api/redeem", post(redeem_handler))
        .route("/api/logs", get(logs_handler))
        .route("/api/trades", get(trades_handler))
        .route("/api/markets", get(markets_handler))
        .route("/api/portfolio", get(portfolio_handler))
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
        Ok(p) => p.into_iter().map(|pos| PositionView {
            asset: pos.asset,
            title: pos.title,
            size: pos.size,
            cur_price: pos.cur_price,
        }).collect(),
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
        None => return Json(CloseAllResponse {
            success: false,
            message: "Executor not initialized".to_string(),
            positions_closed: 0,
        }),
    };

    info!("🛑 收到Web控制台平仓指令，开始执行平仓...");

    // 之前会暂停Bot，现在根据需求移除暂停逻辑
    // state.is_running.store(false, Ordering::Relaxed);
    // info!("⏸️ 已暂停Bot自动交易，防止新开仓位");

    let mut closed_count = 0;
    
    // 获取当前持仓
    match get_positions().await {
        Ok(positions) => {
            let active_positions: Vec<_> = positions.iter().filter(|p| p.size > dec!(0.01)).collect();
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
                
                // 尝试以 0.05 卖出 (市价单效果，比0.01稍微高一点避免极端情况，但实际上0.01最稳妥能成交)
                // 这里使用0.01确保只要有买单就能成交
                let sell_price = dec!(0.01);
                
                match executor.sell_at_price(pos.asset, sell_price, size_floor).await {
                    Ok(_) => {
                        info!("✅ 已下卖单 | token_id={:#x} | 数量:{} | 价格:{:.4}", pos.asset, size_floor, sell_price);
                        closed_count += 1;
                    },
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

async fn redeem_handler() -> impl IntoResponse {
    info!("🎁 收到Web控制台领取奖励指令，开始执行 Merge...");
    
    // 加载配置
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            error!("❌ 无法加载配置: {}", e);
            return Json(RedeemResponse {
                success: false,
                message: format!("配置加载失败: {}", e),
                tx_hashes: vec![],
            });
        }
    };

    let proxy_address = match config.proxy_address {
        Some(addr) => addr,
        None => {
            return Json(RedeemResponse {
                success: false,
                message: "未配置 POLYMARKET_PROXY_ADDRESS，无法执行 Merge".to_string(),
                tx_hashes: vec![],
            });
        }
    };

    let private_key = config.private_key.clone();
    let mut tx_hashes = Vec::new();

    // 获取持仓
    match get_positions().await {
        Ok(positions) => {
            // 找到所有 YES+NO 双边都有持仓的市场
            // 先按 condition_id 分组
            let mut markets: std::collections::HashMap<String, (Decimal, Decimal)> = std::collections::HashMap::new();
            
            for pos in positions {
                // pos.condition_id 是 B256 转 hex 字符串
                // pos.outcome_index: 0=YES, 1=NO
                let condition_id_str = pos.condition_id.to_string();
                let entry = markets.entry(condition_id_str).or_insert((dec!(0), dec!(0)));
                if pos.outcome_index == 0 {
                    entry.0 = pos.size;
                } else if pos.outcome_index == 1 {
                    entry.1 = pos.size;
                }
            }

            // 筛选出双边都有持仓的市场
            let mergeable_markets: Vec<String> = markets
                .into_iter()
                .filter(|(_, (yes, no))| *yes > dec!(0.000001) && *no > dec!(0.000001))
                .map(|(cid, _)| cid)
                .collect();

            if mergeable_markets.is_empty() {
                info!("✅ 没有可领取的奖励（无双边持仓）");
                return Json(RedeemResponse {
                    success: true,
                    message: "没有可领取的奖励".to_string(),
                    tx_hashes: vec![],
                });
            }

            info!("🔍 发现 {} 个市场可执行 Merge", mergeable_markets.len());

            for cid_str in mergeable_markets {
                if let Ok(condition_id) = alloy::primitives::B256::from_str(&cid_str) {
                    info!("🔄 正在 Merge 市场: {}", cid_str);
                    match merge::merge_max(condition_id, proxy_address, &private_key, None).await {
                        Ok(tx) => {
                            info!("✅ Merge 成功: {}", tx);
                            tx_hashes.push(tx);
                        },
                        Err(e) => {
                            error!("❌ Merge 失败 {}: {}", cid_str, e);
                        }
                    }
                }
            }
        }
        Err(e) => {
            error!("❌ 获取持仓失败: {}", e);
            return Json(RedeemResponse {
                success: false,
                message: format!("获取持仓失败: {}", e),
                tx_hashes: vec![],
            });
        }
    }

    Json(RedeemResponse {
        success: true,
        message: format!("已执行 {} 笔 Merge 交易", tx_hashes.len()),
        tx_hashes,
    })
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

async fn trades_handler(State(state): State<AppState>) -> impl IntoResponse {
    use crate::utils::trade_history;
    let mut trades = trade_history::get_trades();
    
    // 更新交易状态（基于最新市场价格）
    for trade in &mut trades {
        // 如果状态不是 Pending，则跳过
        if trade.status != "Pending" {
            continue;
        }

        if let Some(market) = state.market_data.get(&trade.market_id) {
            // YES方向
            if trade.side == "YES" {
                if let Some(yes_price) = market.yes_price {
                    if yes_price >= 0.99 {
                        trade.status = "Won".to_string();
                    } else if yes_price <= 0.01 {
                        trade.status = "Lost".to_string();
                    }
                }
            } 
            // NO方向
            else if trade.side == "NO" {
                if let Some(no_price) = market.no_price {
                    if no_price >= 0.99 {
                        trade.status = "Won".to_string();
                    } else if no_price <= 0.01 {
                        trade.status = "Lost".to_string();
                    }
                }
            }
        }
    }
    
    Json(trades)
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
