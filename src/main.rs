mod config;
mod market;
mod monitor;
mod risk;
mod strategy;
mod trading;
mod utils;
mod web_server;

use poly_5min_bot::merge;
use poly_5min_bot::positions::{get_positions, Position};

use anyhow::Result;
use dashmap::DashMap;
use futures::StreamExt;
use polymarket_client_sdk::types::{Address, B256, U256};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};



use crate::config::Config;
use crate::market::{MarketDiscoverer, MarketInfo, MarketScheduler};
use crate::monitor::OrderBookMonitor;
use crate::risk::{PositionBalancer, RiskManager};
use crate::trading::TradingExecutor;
 

#[derive(Clone, Debug)]
enum CountdownOnceState {
    Idle,
    Buying {
        market_id: B256,
        token_id: U256,
        qty: Decimal,
        side: String,
    },
    Bought {
        market_id: B256,
        token_id: U256,
        qty: Decimal,
        entry_price: Decimal,
        side: String,
    },
    Selling {
        market_id: B256,
        token_id: U256,
        qty: Decimal,
        side: String,
    },
}


#[tokio::main]
async fn main() -> Result<()> {
    // Install rustls crypto provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // 初始化日志
    utils::logger::init_logger()?;

    tracing::info!("5分钟");

    // 许可证校验：须存在有效 license.key，删除许可证将无法运行
    // poly_5min_bot::trial::check_license()?;

    // 加载配置
    let config = Config::from_env()?;
    tracing::info!("配置加载完成");

    // 初始化组件（暂时不使用，主循环已禁用）
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
        config.proxy_address,
        config.slippage,
        config.gtd_expiration_secs,
        config.arbitrage_order_type.clone(),
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
    use alloy::signers::Signer;
    use polymarket_client_sdk::clob::types::SignatureType;
    use polymarket_client_sdk::clob::{Client, Config as ClobConfig};

    let signer_for_risk = LocalSigner::from_str(&config.private_key)?.with_chain_id(Some(POLYGON));
    let clob_config = ClobConfig::builder()
        .use_server_time(true)
        .build();
    
    // 先创建一个认证构建器，用于检查配置是否正确
    let mut auth_builder_risk = Client::new("https://clob.polymarket.com", clob_config.clone())?
        .authentication_builder(&signer_for_risk);

    // 如果提供了proxy_address，设置funder和signature_type
    if let Some(funder) = config.proxy_address {
        auth_builder_risk = auth_builder_risk
            .funder(funder)
            .signature_type(SignatureType::Proxy);
    }

    let clob_client = {
        let max_retries = 5; // 增加重试次数
        let mut last_error: Option<anyhow::Error> = None;
        let mut result: Option<polymarket_client_sdk::clob::Client<polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>>> = None;

        for attempt in 1..=max_retries {
            // 每次重试都重新创建认证构建器
            let mut auth_builder_risk = Client::new("https://clob.polymarket.com", clob_config.clone())?
                .authentication_builder(&signer_for_risk);

            // 如果提供了proxy_address，设置funder和signature_type
            if let Some(funder) = config.proxy_address {
                auth_builder_risk = auth_builder_risk
                    .funder(funder)
                    .signature_type(SignatureType::Proxy);
            }

            match auth_builder_risk.authenticate().await {
                Ok(client) => {
                    info!("风险管理客户端认证成功 (尝试 {} / {})", attempt, max_retries);
                    result = Some(client);
                    break;
                }
                Err(e) => {
                    // 检查是否是网络错误
                    let error_str = e.to_string();
                    let is_network_error = error_str.contains("error sending request") || 
                                         error_str.contains("timeout") ||
                                         error_str.contains("connection") ||
                                         error_str.contains("network");
                    
                    last_error = Some(anyhow::anyhow!(
                        "风险管理客户端认证失败 (尝试 {} / {}): {}",
                        attempt, max_retries, e
                    ));
                    
                    if is_network_error {
                        error!(
                            "网络错误 (尝试 {} / {}): {}. 正在重试...",
                            attempt, max_retries, e
                        );
                    } else {
                        error!(
                            "风险管理客户端认证失败 (尝试 {} / {}): {}",
                            attempt, max_retries, e
                        );
                    }
                    
                    if attempt < max_retries {
                        let delay = attempt * 2; // 增加延迟时间
                        info!("{}秒后重试...", delay);
                        tokio::time::sleep(tokio::time::Duration::from_secs(delay as u64)).await;
                    }
                }
            }
        }

        match result {
            Some(client) => client,
            None => {
                let error = last_error.unwrap_or_else(|| anyhow::anyhow!("风险管理客户端认证失败：达到最大重试次数"));
                error!(error = %error, "风险管理客户端认证失败！无法继续运行。");
                error!("请检查：");
                error!("  1. POLYMARKET_PRIVATE_KEY 环境变量是否正确设置");
                error!("  2. 私钥格式是否正确");
                error!("  3. 网络连接是否正常");
                error!("  4. Polymarket API服务是否可用");
                error!("  5. 尝试使用代理服务器或VPN");
                return Err(anyhow::anyhow!("认证失败，程序退出: {}", error));
            }
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
            let wallet_to_check = if let Some(proxy) = config.proxy_address {
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
    
    let countdown_in_progress = Arc::new(AtomicBool::new(false));
    let marketrecord: Arc<DashMap<B256, bool>> = Arc::new(DashMap::new());


    // 主循环已启用，开始监控和交易
    #[allow(unreachable_code)]
    loop {
        // 立即获取当前窗口的市场，如果失败则等待下一个窗口
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

        // 新一轮开始：重置风险敞口，使本轮从 0 敞口重新累计
        _risk_manager.position_tracker().reset_exposure();
 

        // 初始化订单簿监控器
        let mut monitor = OrderBookMonitor::new();

        // 订阅所有市场
        for market in &markets {
            if let Err(e) = monitor.subscribe_market(market) {
                error!(error = %e, market_id = %market.market_id, "订阅市场失败");
            }
        }

        // 创建订单簿流
        let stream_result = monitor.create_orderbook_stream().await;
        let mut stream = match stream_result {
            Ok(stream) => stream,
            Err(e) => {
                error!(error = %e, "创建订单簿流失败");
                continue;
            }
        };

        info!(market_count = markets.len(), "开始监控订单簿");

        // 记录当前窗口的时间戳，用于检测周期切换与收尾触发
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

        // 按市场记录上一拍卖一价，用于计算涨跌方向（仅一次 HashMap 读写，不影响监控性能）
        let last_prices: DashMap<B256, (Decimal, Decimal)> = DashMap::new();
        let yes_greater_than_no_counters: DashMap<B256, u32> = DashMap::new();
        let last_check_timestamps: DashMap<B256, i64> = DashMap::new();
        let current_larger_side: DashMap<B256, Option<bool>> = DashMap::new(); // None: no data, Some(true): yes larger, Some(false): no larger
        //订单存储(市场id,是否下单,下单yes/no,下单的tokenid,未下单的tokenid,下单的数量)
        let order_status: Arc<Mutex<HashMap<String, (bool, String, String, String, String, String)>>> = Arc::new(Mutex::new(HashMap::new()));
        let firstorder: DashMap<B256, i64> = DashMap::new();
        // 较大边下单计数
        let larger_side_order_counters: DashMap<B256, u32> = DashMap::new();
        
        // 购买历史记录结构
        #[derive(Clone)]
        struct PurchaseRecord {
            side: String,
            price: Decimal,
            size: Decimal,
            total_cost: Decimal,
        }
        
        // 单边购买历史映射：市场ID -> (较大边购买记录, 较小边购买记录)
        let purchase_history: DashMap<B256, (Vec<PurchaseRecord>, Vec<PurchaseRecord>)> = DashMap::new();

        #[derive(Clone)]
        struct SimOrderInfo {
            market_id: B256,
            side_key: u8,
            limit_price: Decimal,
            size: Decimal,
            on_filled_state: u8,
            clear_first_leg_price: bool,
        }

        // 计算投注金额的函数
        fn calculate_order_price(market: &str) -> Decimal {
            dec!(2)
        }

        // 监控订单簿更新
        let mut reconnect_attempts = 0;
        const MAX_RECONNECT_ATTEMPTS: u32 = 5;
        loop {
            let now_all = Utc::now();
            let seconds_until_end_all = (window_end - now_all).num_seconds();
            tokio::select! {
                // 处理订单簿更新
                book_result = stream.next() => {
                    match book_result {
                        Some(Ok(book)) => {
                            // 重置重连计数器
                            reconnect_attempts = 0;
                            // 然后处理订单簿更新（book会被move）
                            if let Some(pair) = monitor.handle_book_update(book) {
                                // 注意：asks 最后一个为卖一价
                                let yes_best_ask = pair.yes_book.asks.last().map(|a| (a.price, a.size));
                                let no_best_ask = pair.no_book.asks.last().map(|a| (a.price, a.size));
                                let total_ask_price = yes_best_ask.and_then(|(p, _)| no_best_ask.map(|(np, _)| p + np));

                                let market_id = pair.market_id;
                                // info!(market_id = %market_id, "处理市场数据");

                                // 与上一拍比较得到涨跌方向（↑涨 ↓跌 −平），首拍无箭头
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
                                let now_countdown = Utc::now();
                                let sec_to_end = (window_end - now_countdown).num_seconds();
                                let force_close_window_active = sec_to_end <= 15 && sec_to_end >= 0;
                                countdown_in_progress.store(force_close_window_active, Ordering::Relaxed);
                                let sec_to_end_nonneg = sec_to_end.max(0);
                                let _sec_since_start = now_countdown.timestamp() - current_window_timestamp;
                                let _sec_since_start_nonneg = _sec_since_start.max(0);
                                let countdown_minutes = sec_to_end_nonneg / 60;
                                let countdown_seconds = sec_to_end_nonneg % 60;
                                let countdown_str = format!("{:02}:{:02}", countdown_minutes, countdown_seconds);

                                

                                // 1、获取yes和no的实时卖价并进行比较，选择较大的一边，并记录是yes还是no，本轮市场进行计数，每秒加1，过程中较大的一边比较小的一边小时，计数置零，如果计数和等于60的时候，进行下单购买,并计数置零。

                                // 获取当前时间戳（秒）
                                let current_timestamp = Utc::now().timestamp();
                                {
                                    let mut last_check_ts = last_check_timestamps.entry(market_id).or_insert(0);
                                    if current_timestamp > *last_check_ts {
                                        // 更新最后检查时间戳
                                        *last_check_ts = current_timestamp;

                                // 检查yes和no的卖价
                                if let (Some((yes_price, _)), Some((no_price, _))) = (yes_best_ask, no_best_ask) {
                                    let is_yes_larger = yes_price > no_price;
                                    let is_no_larger = no_price > yes_price;
                                    
                                    // 确定当前较大的一边
                                    let current_larger = if is_yes_larger {
                                        Some(true) // yes较大
                                    } else if is_no_larger {
                                        Some(false) // no较大
                                    } else {
                                        None // 价格相等
                                    };

                                    // 读取并更新计数器（短生命周期引用，立即释放）
                                    {
                                        let mut counter = yes_greater_than_no_counters.entry(market_id).or_insert(0);
                                        let mut larger_side = current_larger_side.entry(market_id).or_insert(None);

                                        if *larger_side != current_larger {
                                            // 较大的一边发生变化，重置计数器
                                            *counter = 0;
                                            *larger_side = current_larger;
                                            info!(
                                                "{} | 较大边变化 | 新较大边: {:?} | Yes:A{:.4} No:A{:.4}",
                                                market_display, current_larger, yes_price, no_price
                                            );
                                        } else if current_larger.is_some() {
                                            // 较大的一边未变化，计数加1
                                            if *counter == 60 {
                                                *counter = 0;
                                            }
                                            *counter += 1;
                                            let side_str = if current_larger == Some(true) { "Yes" } else { "No" };
                                            error!(
                                                "{}分{:02}秒 | {} | {}价格大于另一方 | 计数: {} | Yes:A{:.4} No:A{:.4}",
                                               countdown_minutes,countdown_seconds, market_display, side_str, *counter, yes_price, no_price
                                            );
                                        }
                                        } // counter 和 larger_side 引用在此释放
 

                                        // 确定要购买的token和价格
                                        let (token_id, price, side_name) = if current_larger == Some(true) {
                                            (pair.yes_book.asset_id, yes_price, "Yes")
                                        } else {
                                            (pair.no_book.asset_id, no_price, "No")
                                        };


                                        // 确定小的一边要购买的token和价格
                                        let (low_token_id, low_price, low_side_name) = if current_larger == Some(false) {
                                            (pair.yes_book.asset_id, yes_price, "Yes")
                                        } else {
                                            (pair.no_book.asset_id, no_price, "No")
                                        };


                                        let countdown_within_180 = sec_to_end_nonneg <= 180 && sec_to_end_nonneg > 30;
                                        let countdown_within_30 = sec_to_end_nonneg <= 30 && sec_to_end_nonneg > 0;
                                        
                                        let price_greater_than_07 = price > dec!(0.7);
                                        let price_less_than_98 = price < dec!(0.98);

                                        
                                        let counter_val = yes_greater_than_no_counters.get(&market_id).map(|r| *r).unwrap_or(0);
                                        let price_greater_count = counter_val == 60;
                                        let default = (false, "".to_string(),"".to_string(),"".to_string(),"".to_string(),"".to_string());
                                        let order_status_lock = order_status.lock().await;
                                        let (is_ordered, order_side_name,order_token_id,unorder_token_id,ordered_size,order_price) = order_status_lock.get(&market_display).unwrap_or(&default).clone();
                                        //如果有订单,而且订单中购买的token这边价格卖价在0.97,对订单进行清仓

                                    if  countdown_within_180  && price_greater_count && price_less_than_98{
                                        // 较大边下单进行计数,当下单次数等于2,对较小边下单,次数并重置成0
                                        let mut counter = larger_side_order_counters.entry(market_id).or_insert(0);
                                        *counter += 1;
                                        
                                        // 较大边下单
                                        let (buy_token_id, buy_price, buy_side_name) = if current_larger == Some(true) {
                                            (pair.yes_book.asset_id, yes_price * dec!(1.02), "Yes")
                                        } else {
                                            (pair.no_book.asset_id, no_price * dec!(1.02), "No")
                                        };
                                        
                                        let order_size = dec!(5);
                                        let order_total_cost = buy_price * order_size;
                                        
                                        // 记录较大边购买历史
                                        {
                                            let mut history = purchase_history.entry(market_id).or_insert((Vec::new(), Vec::new()));
                                            history.0.push(PurchaseRecord {
                                                side: buy_side_name.to_string(),
                                                price: buy_price,
                                                size: order_size,
                                                total_cost: order_total_cost,
                                            });
                                        }
                                        
                                        // 计算并打印较大边累计信息和利润
                                        {
                                            if let Some(history) = purchase_history.get(&market_id) {
                                                let larger_records = &history.0;
                                                let smaller_records = &history.1;
                                                let larger_total_qty: Decimal = larger_records.iter().map(|r| r.size).sum();
                                                let larger_total_cost: Decimal = larger_records.iter().map(|r| r.total_cost).sum();
                                                let larger_avg_price = if larger_total_qty > dec!(0) { larger_total_cost / larger_total_qty } else { dec!(0) };
                                                
                                                let opposite_total_cost: Decimal = smaller_records.iter().map(|r| r.total_cost).sum();
                                                
                                                let gross_profit = (dec!(1) - larger_avg_price) * larger_total_qty;
                                                let net_profit = gross_profit - opposite_total_cost;
                                                
                                                info!(
                                                    "{} | 较大边下单 | 方向: {} | 本单单价:{:.4}*本单数量:{:.2}=本单成本:{:.4} | 累计均价:{:.4}*累计数量:{:.2}=累计成本:{:.4} | 毛利润:{:.4} | 纯利润:{:.4}",
                                                    market_display, buy_side_name, buy_price, order_size, order_total_cost, larger_avg_price, larger_total_qty, larger_total_cost, gross_profit, net_profit
                                                );
                                            }
                                        }
                                        
                                        // 这里应该调用实际的下单函数
                                        // executor.buy_at_price(buy_token_id, buy_price, order_size).await;
                                        
                                        // 这里应该调用实际的下单函数
                                        // executor.buy_at_price(buy_token_id, buy_price, order_size).await;
                                        
                                        // 当下单次数等于2时，对较小边下单
                                        if *counter == 2 {
                                            // 确定较小边的token和价格
                                            let (low_token_id, low_price, low_side_name) = if current_larger == Some(false) {
                                                (pair.yes_book.asset_id, yes_price * dec!(0.99), "Yes")
                                            } else {
                                                (pair.no_book.asset_id, no_price * dec!(0.99), "No")
                                            };
                                            
                                            // 执行较小边下单
                                            let order_size = dec!(5);
                                            let order_total_cost = low_price * order_size;
                                            
                                            // 记录较小边购买历史
                                            {
                                                let mut history = purchase_history.entry(market_id).or_insert((Vec::new(), Vec::new()));
                                                history.1.push(PurchaseRecord {
                                                    side: low_side_name.to_string(),
                                                    price: low_price,
                                                    size: order_size,
                                                    total_cost: order_total_cost,
                                                });
                                            }
                                            
                                            // 计算并打印较小边累计信息和利润
                                            {
                                                if let Some(history) = purchase_history.get(&market_id) {
                                                    let smaller_records = &history.1;
                                                    let larger_records = &history.0;
                                                    let smaller_total_qty: Decimal = smaller_records.iter().map(|r| r.size).sum();
                                                    let smaller_total_cost: Decimal = smaller_records.iter().map(|r| r.total_cost).sum();
                                                    let smaller_avg_price = if smaller_total_qty > dec!(0) { smaller_total_cost / smaller_total_qty } else { dec!(0) };
                                                    
                                                    let opposite_total_cost: Decimal = larger_records.iter().map(|r| r.total_cost).sum();
                                                    
                                                    let gross_profit = (dec!(1) - smaller_avg_price) * smaller_total_qty;
                                                    let net_profit = gross_profit - opposite_total_cost;
                                                    
                                                    info!(
                                                        "{} | 较小边下单 | 方向: {} | 本单单价:{:.4}*本单数量:{:.2}=本单成本:{:.4} | 累计均价:{:.4}*累计数量:{:.2}=累计成本:{:.4} | 毛利润:{:.4} | 纯利润:{:.4}",
                                                        market_display, low_side_name, low_price, order_size, order_total_cost, smaller_avg_price, smaller_total_qty, smaller_total_cost, gross_profit, net_profit
                                                    );
                                                }
                                            }
                                            
                                            // 这里应该调用实际的下单函数
                                            // executor.buy_at_price(low_token_id, low_price, order_size).await;
                                            
                                            // 重置计数为0
                                            *counter = 0;
                                            info!(
                                                "{} | 计数重置 | 已对较小边下单，计数重置为0",
                                                market_display
                                            );
                                        }
                                    }else{
                                        //根据firstorder判断,是否有订单,如果有,则不下单,如果没有,则下单,并firstorder插入记录,价格大的边下单价格*1.02,价格小的下单价格*0.99,下单用限价单下单
                                        let has_first_order = firstorder.contains_key(&market_id);
                                        if !has_first_order {
                                            // 计算下单价格
                                            let (buy_token_id, buy_price, buy_side_name) = if current_larger == Some(true) {
                                                (pair.yes_book.asset_id, yes_price * dec!(1.02), "Yes")
                                            } else {
                                                (pair.no_book.asset_id, no_price * dec!(1.02), "No")
                                            };
                                            
                                            let (low_buy_token_id, low_buy_price, low_buy_side_name) = if current_larger == Some(false) {
                                                (pair.yes_book.asset_id, yes_price * dec!(0.99), "Yes")
                                            } else {
                                                (pair.no_book.asset_id, no_price * dec!(0.99), "No")
                                            };
                                            
                                            // 执行下单操作
                                            let order_size = dec!(5); // 订单大小，可根据实际情况调整
                                            
                                            // 下单价格大的一边
                                            let order_total_cost = buy_price * order_size;
                                            
                                            // 记录较大边购买历史
                                            {
                                                let mut history = purchase_history.entry(market_id).or_insert((Vec::new(), Vec::new()));
                                                history.0.push(PurchaseRecord {
                                                    side: buy_side_name.to_string(),
                                                    price: buy_price,
                                                    size: order_size,
                                                    total_cost: order_total_cost,
                                                });
                                            }
                                            
                                            // 计算并打印较大边累计信息和利润
                                            {
                                                if let Some(history) = purchase_history.get(&market_id) {
                                                    let larger_records = &history.0;
                                                    let smaller_records = &history.1;
                                                    let larger_total_qty: Decimal = larger_records.iter().map(|r| r.size).sum();
                                                    let larger_total_cost: Decimal = larger_records.iter().map(|r| r.total_cost).sum();
                                                    let larger_avg_price = if larger_total_qty > dec!(0) { larger_total_cost / larger_total_qty } else { dec!(0) };
                                                    
                                                    let opposite_total_cost: Decimal = smaller_records.iter().map(|r| r.total_cost).sum();
                                                    
                                                    let gross_profit = (dec!(1) - larger_avg_price) * larger_total_qty;
                                                    let net_profit = gross_profit - opposite_total_cost;
                                                    
                                                    info!(
                                                        "{} | 较大边下单 | 方向: {} | 本单单价:{:.4}*本单数量:{:.2}=本单成本:{:.4} | 累计均价:{:.4}*累计数量:{:.2}=累计成本:{:.4} | 毛利润:{:.4} | 纯利润:{:.4}",
                                                        market_display, buy_side_name, buy_price, order_size, order_total_cost, larger_avg_price, larger_total_qty, larger_total_cost, gross_profit, net_profit
                                                    );
                                                }
                                            }
                                            
                                            // 这里应该调用实际的下单函数
                                            // executor.buy_at_price(buy_token_id, buy_price, order_size).await;
                                            
                                            // 下单价格小的一边
                                            let low_order_total_cost = low_buy_price * order_size;
                                            
                                            // 记录较小边购买历史
                                            {
                                                let mut history = purchase_history.entry(market_id).or_insert((Vec::new(), Vec::new()));
                                                history.1.push(PurchaseRecord {
                                                    side: low_buy_side_name.to_string(),
                                                    price: low_buy_price,
                                                    size: order_size,
                                                    total_cost: low_order_total_cost,
                                                });
                                            }
                                            
                                            // 计算并打印较小边累计信息和利润
                                            {
                                                if let Some(history) = purchase_history.get(&market_id) {
                                                    let smaller_records = &history.1;
                                                    let larger_records = &history.0;
                                                    let smaller_total_qty: Decimal = smaller_records.iter().map(|r| r.size).sum();
                                                    let smaller_total_cost: Decimal = smaller_records.iter().map(|r| r.total_cost).sum();
                                                    let smaller_avg_price = if smaller_total_qty > dec!(0) { smaller_total_cost / smaller_total_qty } else { dec!(0) };
                                                    
                                                    let opposite_total_cost: Decimal = larger_records.iter().map(|r| r.total_cost).sum();
                                                    
                                                    let gross_profit = (dec!(1) - smaller_avg_price) * smaller_total_qty;
                                                    let net_profit = gross_profit - opposite_total_cost;
                                                    
                                                    info!(
                                                        "{} | 较小边下单 | 方向: {} | 本单单价:{:.4}*本单数量:{:.2}=本单成本:{:.4} | 累计均价:{:.4}*累计数量:{:.2}=累计成本:{:.4} | 毛利润:{:.4} | 纯利润:{:.4}",
                                                        market_display, low_buy_side_name, low_buy_price, order_size, low_order_total_cost, smaller_avg_price, smaller_total_qty, smaller_total_cost, gross_profit, net_profit
                                                    );
                                                }
                                            }
                                            
                                            // 这里应该调用实际的下单函数
                                            // executor.buy_at_price(low_buy_token_id, low_buy_price, order_size).await;
                                            
                                            // 记录firstorder
                                            firstorder.insert(market_id, current_timestamp);
                                            
                                           
                                        }
                                    }
                                
                                }
                                } // end last_check_ts block

                                }

                                let (prefix, spread_info) = total_ask_price
                                    .map(|t| {
                                        if t < dec!(1.0) {
                                            let profit_pct = (dec!(1.0) - t) * dec!(100.0);
                                            ("🚨套利机会", format!("总价:{:.4} 利润:{:.2}%", t, profit_pct))
                                        } else {
                                            ("📊", format!("总价:{:.4} ", t))
                                        }
                                    })
                                    .unwrap_or_else(|| ("📊", "无数据".to_string()));

                                // 涨跌箭头仅在套利机会时显示
                                let is_arbitrage = prefix == "🚨套利机会";
                                let yes_best_bid = pair
                                    .yes_book
                                    .bids
                                    .last()
                                    .map(|b| (b.price, b.size));
                                let no_best_bid = pair.no_book.bids.last().map(|b| (b.price, b.size));

                                let yes_arrow = if is_arbitrage && !yes_dir.is_empty() {
                                    format!(" {}", yes_dir)
                                } else {
                                    String::new()
                                };
                                let no_arrow = if is_arbitrage && !no_dir.is_empty() {
                                    format!(" {}", no_dir)
                                } else {
                                    String::new()
                                };

                                let yes_info = match yes_best_ask {
                                    Some((ap, asz)) => format!("Yes:A{:.4}({:.2}){}", ap, asz, yes_arrow),
                                    None => "Yes:A:无".to_string(),
                                };
                                let no_info = match no_best_ask {
                                    Some((ap, asz)) => format!("No:A{:.4}({:.2}){}", ap, asz, no_arrow),
                                    None => "No:A:无".to_string(),
                                };
                                

                                let order_status_lock = order_status.lock().await;
                                //获取订单的第二个参数
                                let order_second_param = order_status_lock
                                    .get(&market_display)
                                    .map(|(_, side, _, _, _, _)| side.clone())
                                    .unwrap_or_else(|| "未下单".to_string());
                                
                                // info!("{} {} | {}分{:02}秒 | {} | {} | {}|{}",
                                //     prefix,
                                //     market_display,
                                //     countdown_minutes,
                                //     countdown_seconds,
                                //     yes_info,
                                //     no_info,
                                //     spread_info,
                                //     order_second_param
                                // );

                                // 保留原有的结构化日志用于调试（可选）
                                // debug!(
                                //     market_id = %pair.market_id,
                                //     yes_token = %pair.yes_book.asset_id,
                                //     no_token = %pair.no_book.asset_id,
                                //     "订单簿对详细信息"
                                // );
                            }
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "订单簿更新错误");
                            // 流错误，重新创建流
                            break;
                        }
                        None => {
                            warn!("订单簿流结束，重新创建");
                            break;
                        }
                    }
                }
            }
        }
        // monitor 会在循环结束时自动 drop，无需手动清理
        info!("当前窗口监控结束，刷新市场进入下一轮");
    }
}
