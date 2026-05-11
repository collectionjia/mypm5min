mod config;
mod market;
mod monitor;
mod risk;
mod trading;
mod utils;
mod web_server;

use anyhow::Result;
use alloy::signers::Signer;
use dashmap::DashMap;
use futures::StreamExt;
use polymarket_client_sdk::types::{B256, U256};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::market::{MarketDiscoverer, MarketInfo, MarketScheduler};
use crate::monitor::OrderBookMonitor;
use crate::risk::positions::PositionTracker;
use crate::risk::RiskManager;
use crate::trading::TradingExecutor;

#[tokio::main]
async fn main() -> Result<()> {
    // Install rustls crypto provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // 初始化日志
    utils::logger::init_logger()?;

    tracing::info!("Polymarket 套利机器人启动");

    // 许可证校验：须存在有效 license.key，删除许可证将无法运行
    // poly_5min_bot::trial::check_license()?;

    // 加载配置
    let config = Config::from_env()?;
    tracing::info!("配置加载完成");

    // 初始化组件
    let _discoverer = MarketDiscoverer::new(config.crypto_symbols.clone());
    let _scheduler = MarketScheduler::new(_discoverer, config.market_refresh_advance_secs);

    // 验证私钥格式
    info!("正在验证私钥格式...");
    use alloy::signers::local::LocalSigner;
    use polymarket_client_sdk::POLYGON;
    use std::str::FromStr;

    let _signer_test = LocalSigner::from_str(&config.private_key)
        .map_err(|e| anyhow::anyhow!("私钥格式无效: {}", e))?;
    info!("私钥格式验证通过");

    // 初始化交易执行器（需要认证）
    info!("正在初始化交易执行器（需要API认证）...");
    if let Some(ref proxy) = config.proxy_address {
        info!(proxy_address = %proxy, "使用Proxy签名类型（Email/Magic或Browser Wallet）");
    } else {
        info!("使用EOA签名类型（直接交易）");
    }
    info!("注意：如果看到'Could not create api key'警告，这是正常的。SDK会先尝试创建新API key，失败后会自动使用派生方式，认证仍然会成功。");
    let executor = match TradingExecutor::new(
        config.private_key.clone(),
        config.max_order_size_usdc,
        config.proxy_address.clone(),
        config.slippage,
        config.gtd_expiration_secs,
        config.arbitrage_order_type.clone(),
        config.split_order_count,
        config.split_order_interval_ms,
    )
    .await
    {
        Ok(exec) => {
            info!("交易执行器认证成功（可能使用了派生API key）");
            Arc::new(exec)
        }
        Err(e) => {
            error!(error = %e, "交易执行器认证失败！无法继续运行。");
            error!("请检查：");
            error!("  1. POLYMARKET_PRIVATE_KEY 环境变量是否正确设置");
            error!("  2. 私钥格式是否正确（应该是64字符的十六进制字符串，不带0x前缀）");
            error!("  3. 网络连接是否正常");
            error!("  4. Polymarket API服务是否可用");
            return Err(anyhow::anyhow!("认证失败，程序退出: {}", e));
        }
    };

    // 创建CLOB客户端用于风险管理（需要认证）
    info!("正在初始化风险管理客户端（需要API认证）...");
    use polymarket_client_sdk::clob::types::SignatureType;
    use polymarket_client_sdk::clob::{Client, Config as ClobConfig};

    let signer_for_risk = LocalSigner::from_str(&config.private_key)?.with_chain_id(Some(POLYGON));
    let clob_config = ClobConfig::builder().use_server_time(true).build();
    let mut auth_builder_risk = Client::new("https://clob.polymarket.com", clob_config)?
        .authentication_builder(&signer_for_risk);

    // 如果提供了proxy_address，设置funder和signature_type
    if let Some(funder) = config.proxy_address.clone() {
        auth_builder_risk = auth_builder_risk
            .funder(funder)
            .signature_type(SignatureType::Proxy);
    }

    let clob_client = match auth_builder_risk.authenticate().await {
        Ok(client) => {
            info!("风险管理客户端认证成功（可能使用了派生API key）");
            client
        }
        Err(e) => {
            error!(error = %e, "风险管理客户端认证失败！无法继续运行。");
            error!("请检查：");
            error!("  1. POLYMARKET_PRIVATE_KEY 环境变量是否正确设置");
            error!("  2. 私钥格式是否正确");
            error!("  3. 网络连接是否正常");
            error!("  4. Polymarket API服务是否可用");
            return Err(anyhow::anyhow!("认证失败，程序退出: {}", e));
        }
    };

    let _risk_manager = Arc::new(RiskManager::new(clob_client.clone(), &config));

    // 验证认证是否真的成功 - 尝试一个简单的API调用
    info!("正在验证认证状态（通过API调用测试）...");
    match executor.verify_authentication().await {
        Ok(_) => {
            info!("✅ 认证验证成功，API调用正常");

            // 检查余额和授权
            info!("正在检查USDC余额和CTF Exchange授权...");
            let wallet_to_check = if let Some(proxy) = config.proxy_address.clone() {
                proxy
            } else {
                _signer_test.address()
            };

            if let Err(e) =
                crate::utils::balance_checker::check_balance_and_allowance(wallet_to_check).await
            {
                warn!("余额/授权检查失败（非致命错误）: {}", e);
            }
            if let Err(e) =
                crate::utils::balance_checker::check_conditional_token_approval(wallet_to_check)
                    .await
            {
                warn!("ConditionalTokens 授权检查失败（非致命错误）: {}", e);
            }
        }
        Err(e) => {
            error!(error = %e, "❌ 认证验证失败！虽然authenticate()没有报错，但API调用失败。");
            error!("这表明认证实际上没有成功，可能是：");
            error!("  1. API密钥创建失败（看到'Could not create api key'警告）");
            error!("  2. 私钥对应的账户可能没有在Polymarket上注册");
            error!("  3. 账户可能被限制或暂停");
            error!("  4. 网络连接问题");
            error!("程序将退出，请解决认证问题后再运行。");
            return Err(anyhow::anyhow!("认证验证失败: {}", e));
        }
    }

    info!("✅ 所有组件初始化完成，认证验证通过");

    // 启动 Web 控制服务器
    let is_running = Arc::new(AtomicBool::new(false));
    let market_data = Arc::new(DashMap::new());
    let countdown_settings = Arc::new(tokio::sync::RwLock::new(
        web_server::CountdownSettings::default(),
    ));
    let is_running_server = is_running.clone();
    let market_data_server = market_data.clone();
    let executor_server = Some(executor.clone());
    let countdown_settings_server = countdown_settings.clone();

    tokio::spawn(async move {
        web_server::start_server(
            is_running_server,
            market_data_server,
            executor_server,
            countdown_settings_server,
        )
        .await;
    });

    info!("🌐 Web控制台已启动: http://localhost:3000");
    info!("🧪 默认模拟交易：在 Web 控制台可切换真实投注开关");

    // 主循环
    loop {
        // 获取当前窗口的市场，如果失败则等待下一个窗口
        let markets = match _scheduler.get_markets_immediately_or_wait().await {
            Ok(markets) => markets,
            Err(e) => {
                error!(error = %e, "获取市场失败");
                sleep(Duration::from_secs(60)).await;
                continue;
            }
        };

        if markets.is_empty() {
            warn!("未找到任何市场，跳过当前窗口");
            continue;
        }

        // 新一轮开始：重置风险敞口
        _risk_manager.position_tracker().reset_exposure();

        // 初始化订单簿监控器
        let mut monitor = OrderBookMonitor::new();

        // 订阅所有市场
        for market in &markets {
            if let Err(e) = monitor.subscribe_market(market) {
                error!(error = %e, market_id = %market.market_id, "订阅市场失败");
            }
        }

        info!("start jiajia  split ");
        // 如果启用了 1 美元 split 订单策略，直接下单（跳过订单簿等待）
        if config.enable_split_order_strategy {
            info!("🎯 1美元 Split 订单策略已启用，直接下单...");

            for market in &markets {
                let market_display = if !market.crypto_symbol.is_empty() {
                    market.crypto_symbol.clone()
                } else {
                    market.title.clone()
                };

                // 使用固定价格下单（0.5）
                let yes_price = dec!(0.5);
                let no_price = dec!(0.5);

                let yes_token_id = market.yes_token_id;
                let no_token_id = market.no_token_id;

                info!(
                    "📤 执行 1 美元 Split 订单 | {} | YES {:.4} | NO {:.4}",
                    market_display,
                    yes_price,
                    no_price
                );

                match executor.execute_split_order(
                    yes_token_id,
                    no_token_id,
                    yes_price,
                    no_price,
                ).await {
                    Ok(result) => {
                        if result.success {
                            info!("✅ Split 订单成功 | {}", market_display);
                        } else {
                            warn!("⚠️ Split 订单部分成交 | {}", market_display);
                        }
                    }
                    Err(e) => {
                        error!(error = %e, market = %market_display, "Split 订单失败");
                    }
                }

                // 每个市场间隔 1 秒
                sleep(Duration::from_secs(1)).await;
            }
            info!("🎯 1美元 Split 订单策略执行完毕");
        }

        // 创建订单簿流
        let mut stream = match monitor.create_orderbook_stream() {
            Ok(stream) => stream,
            Err(e) => {
                error!(error = %e, "创建订单簿流失败");
                continue;
            }
        };

        info!(market_count = markets.len(), "开始监控订单簿");

        // 记录当前窗口的时间戳，用于检测周期切换
        use crate::market::discoverer::FIVE_MIN_SECS;
        use chrono::Utc;
        let current_window_timestamp =
            MarketDiscoverer::calculate_current_window_timestamp(Utc::now());
        let window_end =
            chrono::DateTime::from_timestamp(current_window_timestamp + FIVE_MIN_SECS, 0)
                .unwrap_or_else(|| Utc::now());

        // 创建市场ID到市场信息的映射
        let market_map: HashMap<B256, &MarketInfo> =
            markets.iter().map(|m| (m.market_id, m)).collect();

        // 按市场记录上一拍卖一价，用于计算涨跌方向
        let last_prices: DashMap<B256, (Decimal, Decimal)> = DashMap::new();

        // 监控订单簿更新
        loop {
            tokio::select! {
                // 处理订单簿更新
                book_result = stream.next() => {
                    match book_result {
                        Some(Ok(book)) => {
                            if let Some(pair) = monitor.handle_book_update(book) {
                                // 注意：asks 最后一个为卖一价
                                let yes_best_ask = pair.yes_book.asks.last().map(|a| (a.price, a.size));
                                let no_best_ask = pair.no_book.asks.last().map(|a| (a.price, a.size));
                                let total_ask_price = yes_best_ask.and_then(|(p, _)| no_best_ask.map(|(np, _)| p + np));

                                let market_id = pair.market_id;

                                // 与上一拍比较得到涨跌方向
                                let (yes_dir, no_dir) = match (yes_best_ask, no_best_ask) {
                                    (Some((yp, _)), Some((np, _))) => {
                                        let prev = last_prices.get(&market_id).map(|r| (r.0, r.1));
                                        let (y_dir, n_dir) = prev
                                            .map(|(ly, ln)| (
                                                if yp > ly { "↑" } else if yp < ly { "↓" } else { "−" },
                                                if np > ln { "↑" } else if np < ln { "↓" } else { "−" },
                                            ))
                                            .unwrap_or(("", ""));
                                        last_prices.insert(market_id, (yp, np));
                                        (y_dir, n_dir)
                                    }
                                    _ => ("", ""),
                                };

                                let market_info = market_map.get(&pair.market_id);
                                let market_title = market_info.map(|m| m.title.as_str()).unwrap_or("未知市场");
                                let market_symbol = market_info.map(|m| m.crypto_symbol.as_str()).unwrap_or("");
                                let price_to_beat = market_info.and_then(|m| m.price_to_beat);
                                let market_display = if !market_symbol.is_empty() {
                                    format!("{}", market_symbol)
                                } else {
                                    market_title.to_string()
                                };

                                let market_category = if !market_symbol.is_empty() {
                                    market_symbol.to_string()
                                } else {
                                    "其他".to_string()
                                };

                                let now = Utc::now();
                                let sec_to_end = (window_end - now).num_seconds();
                                let sec_to_end_nonneg = sec_to_end.max(0);
                                let countdown_minutes = sec_to_end_nonneg / 60;
                                let countdown_seconds = sec_to_end_nonneg % 60;

                                // 更新 Web 控制台数据
                                {
                                    use rust_decimal::prelude::ToPrimitive;
                                    let yes_f64 = yes_best_ask.map(|(p, _)| p.to_f64().unwrap_or(0.0));
                                    let no_f64 = no_best_ask.map(|(p, _)| p.to_f64().unwrap_or(0.0));
                                    let sum_val = if let (Some(y), Some(n)) = (yes_f64, no_f64) { Some(y + n) } else { None };
                                    let diff_val = if let (Some(y), Some(n)) = (yes_f64, no_f64) { Some((y - n).abs()) } else { None };

                                    let entry = web_server::MarketData {
                                        id: market_id.to_string(),
                                        name: market_display.clone(),
                                        category: market_category.clone(),
                                        countdown: format!("{:02}:{:02}", countdown_minutes, countdown_seconds),
                                        yes_token_id: format!("{:#x}", pair.yes_book.asset_id),
                                        no_token_id: format!("{:#x}", pair.no_book.asset_id),
                                        yes_price: yes_f64,
                                        no_price: no_f64,
                                        price_to_beat,
                                        sum: sum_val,
                                        diff: diff_val,
                                        update_time: Utc::now().timestamp(),
                                    };
                                    market_data.insert(market_id.to_string(), entry);
                                }

                                // 检测套利机会
                                if let Some(total) = total_ask_price {
                                    let (prefix, spread_info) = if total < dec!(1.0) {
                                        let profit_pct = (dec!(1.0) - total) * dec!(100.0);
                                        ("🚨套利机会", format!("总价:{:.4} 利润:{:.2}%", total, profit_pct))
                                    } else {
                                        ("📊", format!("总价:{:.4} ", total))
                                    };


                                     

                                    let yes_info = match yes_best_ask {
                                        Some((ap, asz)) => format!("Yes:A{:.4}({:.2}){}", ap, asz, yes_dir),
                                        None => "Yes:A:无".to_string(),
                                    };
                                    let no_info = match no_best_ask {
                                        Some((ap, asz)) => format!("No:A{:.4}({:.2}){}", ap, asz, no_dir),
                                        None => "No:A:无".to_string(),
                                    };

                                    info!(
                                        "{} | {}分{:02}秒 | {} | {} | {}",
                                        market_display,
                                        countdown_minutes,
                                        countdown_seconds,
                                        prefix,
                                        yes_info,
                                        no_info
                                    );

                                    // 写入日志文件
                                    let log_line = format!(
                                        "{} | {}分{:02}秒 | {} | {} | {}\n",
                                        market_display,
                                        countdown_minutes,
                                        countdown_seconds,
                                        spread_info,
                                        yes_info,
                                        no_info
                                    );
                                    let now = std::time::SystemTime::now();
                                    let timestamp = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
                                    let five_min_interval = (timestamp / 300) * 300;
                                    let time_str = if let Some(dt) = chrono::NaiveDateTime::from_timestamp_opt(five_min_interval as i64, 0) {
                                        dt.format("%Y%m%d_%H%M").to_string()
                                    } else {
                                        format!("{}", five_min_interval)
                                    };
                                    
                                    let safe_market_name = market_display.replace(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-', "_");
                                    let file_name = format!("market_files/market_{}_{}.txt", safe_market_name, time_str);
                                    if let Ok(mut file) = OpenOptions::new()
                                        .create(true)
                                        .append(true)
                                        .open(file_name)
                                    {
                                        let _ = file.write_all(log_line.as_bytes());
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "订单簿更新错误");
                            break;
                        }
                        None => {
                            warn!("订单簿流结束，重新创建");
                            break;
                        }
                    }
                }

                // 定期检查：是否进入新的5分钟窗口
                _ = sleep(Duration::from_secs(1)) => {
                    let now = Utc::now();
                    let new_window_timestamp = MarketDiscoverer::calculate_current_window_timestamp(now);

                    if new_window_timestamp != current_window_timestamp {
                        info!(
                            old_window = current_window_timestamp,
                            new_window = new_window_timestamp,
                            "检测到新的5分钟窗口，准备切换到新窗口"
                        );
                        
                        drop(stream);
                        monitor.clear();
                        break;
                    }
                }
            }
        }

        info!("当前窗口监控结束，刷新市场进入下一轮");
    }
}
