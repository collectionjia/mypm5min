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
use tokio::sync::RwLock;
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
    pub countdown_settings: Arc<RwLock<CountdownSettings>>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CountdownSettings {
    pub side: String,
    pub multiplier: f64,
}

impl Default for CountdownSettings {
    fn default() -> Self {
        Self {
            side: "YES".to_string(),
            multiplier: 2.0,
        }
    }
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

#[derive(Deserialize)]
struct BuyRequest {
    market_id: String,
    side: String,
    qty: Option<f64>,
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
    countdown_settings: Arc<RwLock<CountdownSettings>>,
) {
    let state = AppState { is_running, market_data, executor, countdown_settings };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/status", get(status_handler))
        .route("/api/control", post(control_handler))
        .route("/api/countdown_settings", get(get_countdown_settings_handler).post(set_countdown_settings_handler))
        .route("/api/close_all", post(close_all_handler))
        .route("/api/redeem", post(redeem_handler))
        .route("/api/buy", post(buy_handler))
        .route("/api/logs", get(logs_handler))
        .route("/api/trades", get(trades_handler))
        .route("/api/trades/clear", post(clear_trades_handler))
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

async fn get_countdown_settings_handler(State(state): State<AppState>) -> impl IntoResponse {
    let settings = state.countdown_settings.read().await.clone();
    Json(settings)
}

async fn set_countdown_settings_handler(
    State(state): State<AppState>,
    Json(payload): Json<CountdownSettings>,
) -> impl IntoResponse {
    let side = payload.side.trim().to_uppercase();
    let side = if side == "YES" || side == "NO" { side } else { "YES".to_string() };
    let multiplier = if payload.multiplier.is_finite() && payload.multiplier >= 1.0 {
        payload.multiplier
    } else {
        2.0
    };

    let updated = CountdownSettings { side, multiplier };
    *state.countdown_settings.write().await = updated.clone();
    Json(updated)
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
            asset: pos.asset.to_string(),
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

    let qty = if let Some(q) = payload.qty {
        match Decimal::try_from(q) {
            Ok(d) => d,
            Err(e) => {
                return Json(BuyResponse {
                    success: false,
                    message: format!("数量解析失败: {}", e),
                    order_id: None,
                });
            }
        }
    } else {
        let target_qty = Config::from_env()
            .ok()
            .and_then(|c| Decimal::try_from(c.max_order_size_usdc).ok())
            .unwrap_or(dec!(5));
        let mut q = (target_qty * dec!(100)).floor() / dec!(100);
        if q < dec!(5) {
            q = dec!(5);
        }
        q
    };

    info!(
        "🛒 Web手动买入 | market_id={} | side={} | token_id={} | price={:.4} | qty={}",
        payload.market_id,
        side,
        token_id_str,
        price_f64,
        qty
    );

    if !state.is_running.load(Ordering::Relaxed) {
        use crate::utils::trade_history::{add_trade, TradeRecord};
        use chrono::Utc;
        use rust_decimal::prelude::ToPrimitive;
        let sim_order_id = format!("SIM-{}", Utc::now().timestamp_millis());
        let buy_countdown = Some(market.countdown.clone());

        add_trade(TradeRecord {
            id: sim_order_id.clone(),
            market_id: payload.market_id.clone(),
            market_slug: market.name.clone(),
            side: side.clone(),
            price: price.to_f64().unwrap_or(0.0),
            size: qty.to_f64().unwrap_or(0.0),
            timestamp: Utc::now().timestamp(),
            status: "SimBought".to_string(),
            profit: None,
            buy_countdown,
            sell_countdown: None,
        });

        return Json(BuyResponse {
            success: true,
            message: format!("模拟下单成功: {}", sim_order_id),
            order_id: Some(sim_order_id),
        });
    }

    match executor.buy_at_price(token_id, price, qty).await {
        Ok(resp) => {
            use crate::utils::trade_history::{add_trade, TradeRecord};
            use chrono::Utc;
            use rust_decimal::prelude::ToPrimitive;
            let buy_countdown = Some(market.countdown.clone());

            add_trade(TradeRecord {
                id: resp.order_id.clone(),
                market_id: payload.market_id.clone(),
                market_slug: market.name.clone(),
                side: side.clone(),
                price: price.to_f64().unwrap_or(0.0),
                size: qty.to_f64().unwrap_or(0.0),
                timestamp: Utc::now().timestamp(),
                status: "Bought".to_string(),
                profit: None,
                buy_countdown,
                sell_countdown: None,
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
        if trade.status == "Won"
            || trade.status == "Lost"
            || trade.status == "SimSkipped"
            || trade.status == "SimPosted"
        {
            continue;
        }

        if let Some(market) = state.market_data.get(&trade.market_id) {
            let yes_price = market.yes_price;
            let no_price = market.no_price;

            if trade.side == "YES" {
                if let (Some(yp), Some(np)) = (yes_price, no_price) {
                    if yp >= 0.99 {
                        trade.status = "Won".to_string();
                    } else if yp <= 0.01 {
                        trade.status = "Lost".to_string();
                    }
                }
            } else if trade.side == "NO" {
                if let (Some(yp), Some(np)) = (yes_price, no_price) {
                    if np >= 0.99 {
                        trade.status = "Won".to_string();
                    } else if np <= 0.01 {
                        trade.status = "Lost".to_string();
                    }
                }
            }
        }
    }
    
    Json(trades)
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
