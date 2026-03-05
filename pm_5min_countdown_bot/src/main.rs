mod config;
mod market { pub mod discoverer; }
mod monitor { pub mod orderbook; }
mod trading { pub mod executor; }
mod utils { pub mod logger; }

use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use futures::StreamExt;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn, debug};
use polymarket_client_sdk::types::{B256};
use std::sync::Arc;

use config::Config;
use market::discoverer::{MarketDiscoverer, MarketInfo, FIVE_MIN_SECS};
use monitor::orderbook::OrderBookMonitor;
use trading::executor::TradingExecutor;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().expect("Failed to install rustls crypto provider");
    utils::logger::init_logger()?;
    let cfg = Config::from_env()?;

    let executor = Arc::new(TradingExecutor::new(
        cfg.private_key.clone(),
        cfg.max_order_size_usdc,
        cfg.proxy_address,
        cfg.gtd_expiration_secs,
        cfg.arbitrage_order_type.clone(),
    ).await?);

    let discoverer = MarketDiscoverer::new(cfg.crypto_symbols.clone());

    loop {
        let ts = MarketDiscoverer::calculate_current_window_timestamp(Utc::now());
        let markets = discoverer.get_markets_for_timestamp(ts).await?;
        if markets.is_empty() {
            warn!("未找到市场，等待5秒后重试");
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        let mut monitor = OrderBookMonitor::new();
        for m in &markets { let _ = monitor.subscribe_market(m); }
        let mut stream = match monitor.create_orderbook_stream() { Ok(s) => s, Err(e) => { warn!("创建订单簿流失败: {}", e); sleep(Duration::from_secs(3)).await; continue; } };

        let window_end = chrono::DateTime::from_timestamp(ts + FIVE_MIN_SECS, 0).unwrap_or_else(|| Utc::now());
        let one_dollar_attempted: Arc<DashMap<B256, (polymarket_client_sdk::types::U256, Decimal, Decimal, bool)>> = Arc::new(DashMap::new());

        info!(count = markets.len(), "开始倒计时策略监控");

        // 简单的WS重连机制：当出现连接错误时，尝试最多3次重建流
        let mut reconnect_attempts = 0usize;

        while let Some(update) = stream.next().await {
            match update {
                Ok(book) => {
                    reconnect_attempts = 0; // 有数据即归零重试计数
                    if let Some(pair) = monitor.handle_book_update(book) {
                        let yes_ask_vol: Decimal = pair.yes_book.asks.iter().map(|o| o.size).sum();
                        let yes_bid_vol: Decimal = pair.yes_book.bids.iter().map(|o| o.size).sum();
                        let yes_total_vol = yes_ask_vol + yes_bid_vol;
                        let no_ask_vol: Decimal = pair.no_book.asks.iter().map(|o| o.size).sum();
                        let no_bid_vol: Decimal = pair.no_book.bids.iter().map(|o| o.size).sum();
                        let no_total_vol = no_ask_vol + no_bid_vol;
                        let yes_best_ask = pair.yes_book.asks.last().map(|a| (a.price, a.size));
                        let no_best_ask = pair.no_book.asks.last().map(|a| (a.price, a.size));

                        let now = Utc::now();
                        let sec_to_end = (window_end - now).num_seconds();
                        let countdown_active = sec_to_end <= 10 && sec_to_end >= 5;
                        let sec_to_end_nonneg = sec_to_end.max(0);
                        let countdown_minutes = sec_to_end_nonneg / 60;
                        let countdown_seconds = sec_to_end_nonneg % 60;

                        let market_symbol = markets.iter().find(|m| m.market_id == pair.market_id).map(|m| m.crypto_symbol.as_str()).unwrap_or("");
                        let market_display = if !market_symbol.is_empty() { format!("{}预测市场", market_symbol) } else { markets.iter().find(|m| m.market_id == pair.market_id).map(|m| m.title.clone()).unwrap_or_else(|| "未知市场".to_string()) };

                        if countdown_active {
                            if let Some(entry) = one_dollar_attempted.get(&pair.market_id) {
                                let (token_id, entry_price, qty, active) = entry.value().clone();
                                if active {
                                    let best_bid = if token_id == pair.yes_book.asset_id { pair.yes_book.bids.first().map(|b| b.price) } else { pair.no_book.bids.first().map(|b| b.price) };
                                    if let Some(bid_price) = best_bid {
                                        if bid_price <= entry_price * dec!(0.9) {
                                            let _ = one_dollar_attempted.insert(pair.market_id, (token_id, entry_price, qty, false));
                                            let exec2 = executor.clone();
                                            tokio::spawn(async move { let _ = exec2.sell_at_price(token_id, bid_price, qty).await; });
                                        }
                                    }
                                }
                            }
                            info!("倒计时:{}分{:02}秒 | 市场:{}", countdown_minutes, countdown_seconds, market_display);
                            if one_dollar_attempted.get(&pair.market_id).is_none() {
                                if let (Some((y_price, _)), Some((n_price, _))) = (yes_best_ask, no_best_ask) {
                                    if y_price >= dec!(0.99) || n_price >= dec!(0.99) {
                                        debug!("价格>=0.99，倒计时策略跳过 | 市场:{}", market_display);
                                    } else {
                                        let (chosen_token, chosen_price, side_str, vol_ok) = if y_price >= n_price {
                                            (pair.yes_book.asset_id, y_price, "YES", yes_total_vol > no_total_vol)
                                        } else {
                                            (pair.no_book.asset_id, n_price, "NO", no_total_vol > yes_total_vol)
                                        };
                                        if !vol_ok { debug!("量价不匹配，跳过 | 市场:{}", market_display); } else { let mut qty = (dec!(1.0) / chosen_price) * dec!(100.0);
                                        qty = qty.floor() / dec!(100.0);
                                        if qty < dec!(5.0) { qty = dec!(5.0); }
                                        info!("⏱️ 倒计时策略下单 | 市场:{} | 方向:{} | 价格:{:.4} | 份额:{:.2}", market_display, side_str, chosen_price, qty);
                                        let exec = executor.clone();
                                        let token = chosen_token; let price = chosen_price; let q = qty;
                                        let map = one_dollar_attempted.clone();
                                        map.insert(pair.market_id, (token, price, q, true));
                                        tokio::spawn(async move {
                                            match exec.buy_at_price(token, price, q).await { Ok(_) => { info!("倒计时策略下单成功"); }, Err(e) => { warn!("倒计时策略下单失败: {}", e); } }
                                        }); }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("订单簿更新错误: {}", e);
                    reconnect_attempts += 1;
                    if reconnect_attempts > 3 {
                        warn!("WS连接多次失败，结束本窗口监控并刷新下一轮");
                        break;
                    }
                    let backoff = Duration::from_secs(2 * reconnect_attempts as u64);
                    warn!("准备重建订单簿连接，等待{:?}", backoff);
                    sleep(backoff).await;
                    match monitor.create_orderbook_stream() {
                        Ok(new_stream) => { stream = new_stream; continue; }
                        Err(err) => { warn!("重建订单簿流失败: {}", err); break; }
                    }
                }
            }
        }

        info!("当前窗口结束，刷新下一轮");
    }
}
