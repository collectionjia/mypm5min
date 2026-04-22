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
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};



use crate::config::Config;
use crate::market::{MarketDiscoverer, MarketInfo, MarketScheduler};
use crate::monitor::OrderBookMonitor;
use crate::risk::positions::PositionTracker;
use crate::risk::{PositionBalancer, RiskManager};
use crate::trading::TradingExecutor;

/// 从持仓中筛出 **YES 和 NO 都持仓** 的 condition_id，仅这些市场才能 merge；单边持仓直接跳过。
/// Data API 可能返回 outcome_index 0/1（0=Yes, 1=No）或 1/2（与 CTF index_set 一致），两种都支持。
fn condition_ids_with_both_sides(positions: &[Position]) -> Vec<B256> {
    let mut by_condition: HashMap<B256, HashSet<i32>> = HashMap::new();
    for p in positions {
        if p.size <= dec!(0) {
            continue;
        }
        by_condition
            .entry(p.condition_id)
            .or_default()
            .insert(p.outcome_index);
    }
    by_condition
        .into_iter()
        .filter(|(_, indices)| {
            (indices.contains(&0) && indices.contains(&1))
                || (indices.contains(&1) && indices.contains(&2))
        })
        .map(|(c, _)| c)
        .collect()
}

/// 从持仓中构建 condition_id -> (yes_token_id, no_token_id, merge_amount)，用于 merge 成功后扣减敞口。
/// 支持 outcome_index 0/1（0=Yes, 1=No）与 1/2（CTF 约定）。
fn merge_info_with_both_sides(positions: &[Position]) -> HashMap<B256, (U256, U256, Decimal)> {
    // outcome_index -> (asset, size) 按 condition 分组
    let mut by_condition: HashMap<B256, HashMap<i32, (U256, Decimal)>> = HashMap::new();
    for p in positions {
        if p.size <= dec!(0) {
            continue;
        }
        by_condition
            .entry(p.condition_id)
            .or_default()
            .insert(p.outcome_index, (p.asset, p.size));
    }
    by_condition
        .into_iter()
        .filter_map(|(c, map)| {
            // 优先使用 CTF 约定 1=Yes, 2=No；否则使用 0=Yes, 1=No
            if let (Some((yes_token, yes_size)), Some((no_token, no_size))) =
                (map.get(&1).copied(), map.get(&2).copied())
            {
                return Some((c, (yes_token, no_token, yes_size.min(no_size))));
            }
            if let (Some((yes_token, yes_size)), Some((no_token, no_size))) =
                (map.get(&0).copied(), map.get(&1).copied())
            {
                return Some((c, (yes_token, no_token, yes_size.min(no_size))));
            }
            None
        })
        .collect()
}
 

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

/// 定时 Merge 任务：每 interval_minutes 分钟拉取**持仓**，仅对 YES+NO 双边都持仓的市场 **串行**执行 merge_max，
/// 单边持仓跳过；每笔之间间隔、对 RPC 限速做一次重试。Merge 成功后扣减 position_tracker 的持仓与敞口。
/// 首次执行前短暂延迟，避免与订单簿监听的启动抢占同一 runtime，导致阻塞 stream。
async fn run_merge_task(
    interval_minutes: u64,
    proxy: Address,
    private_key: String,
    position_tracker: Arc<PositionTracker>,
    wind_down_in_progress: Arc<AtomicBool>,
    countdown_in_progress: Arc<AtomicBool>,
) {
    let interval = Duration::from_secs(interval_minutes * 60);
    /// 每笔 merge 之间间隔，降低 RPC  bursts
    const DELAY_BETWEEN_MERGES: Duration = Duration::from_secs(30);
    /// 遇限速时等待后重试的时长（略大于 "retry in 10s"）
    const RATE_LIMIT_BACKOFF: Duration = Duration::from_secs(12);
    /// 首次执行前延迟，让主循环先完成订单簿订阅并进入 select!，避免 merge 阻塞 stream
    const INITIAL_DELAY: Duration = Duration::from_secs(10);

    // 先让主循环完成 get_markets、创建 stream 并进入订单簿监听，再执行第一次 merge
    sleep(INITIAL_DELAY).await;

    loop {
        if wind_down_in_progress.load(Ordering::Relaxed)
            || countdown_in_progress.load(Ordering::Relaxed)
        {
            info!("收尾进行中，本轮回 merge 跳过");
            sleep(interval).await;
            continue;
        }
        let (condition_ids, merge_info) = match get_positions().await {
            Ok(positions) => (
                condition_ids_with_both_sides(&positions),
                merge_info_with_both_sides(&positions),
            ),
            Err(e) => {
                warn!(error = %e, "❌ 获取持仓失败，跳过本轮回 merge");
                sleep(interval).await;
                continue;
            }
        };

        if condition_ids.is_empty() {
            debug!("🔄 本轮回 merge: 无满足 YES+NO 双边持仓的市场");
        } else {
            info!(
                count = condition_ids.len(),
                "🔄 本轮回 merge: 共 {} 个市场满足 YES+NO 双边持仓",
                condition_ids.len()
            );
        }

        for (i, &condition_id) in condition_ids.iter().enumerate() {
            // 第 2 个及以后的市场：先等 30 秒再 merge，避免与上一笔链上处理重叠
            if i > 0 {
                info!(
                    "本轮回 merge: 等待 30 秒后合并下一市场 (第 {}/{} 个)",
                    i + 1,
                    condition_ids.len()
                );
                sleep(DELAY_BETWEEN_MERGES).await;
            }
            let mut result = merge::merge_max(condition_id, proxy, &private_key, None).await;
            if result.is_err() {
                let msg = result.as_ref().unwrap_err().to_string();
                if msg.contains("rate limit") || msg.contains("retry in") {
                    warn!(condition_id = %condition_id, "⏳ RPC 限速，等待 {}s 后重试一次", RATE_LIMIT_BACKOFF.as_secs());
                    sleep(RATE_LIMIT_BACKOFF).await;
                    result = merge::merge_max(condition_id, proxy, &private_key, None).await;
                }
            }
            match result {
                Ok(tx) => {
                    info!("✅ Merge 完成 | condition_id={:#x}", condition_id);
                    info!("  📝 tx={}", tx);
                    // Merge 成功：扣减持仓与风险敞口（先扣敞口再扣持仓，保证 update_exposure_cost 读到的是合并前持仓）
                    if let Some((yes_token, no_token, merge_amt)) = merge_info.get(&condition_id) {
                        position_tracker.update_exposure_cost(*yes_token, dec!(0), -*merge_amt);
                        position_tracker.update_exposure_cost(*no_token, dec!(0), -*merge_amt);
                        position_tracker.update_position(*yes_token, -*merge_amt);
                        position_tracker.update_position(*no_token, -*merge_amt);
                        info!(
                            "💰 Merge 已扣减敞口 | condition_id={:#x} | 数量:{}",
                            condition_id, merge_amt
                        );
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("无可用份额") {
                        debug!(condition_id = %condition_id, "⏭️ 跳过 merge: 无可用份额");
                    } else {
                        warn!(condition_id = %condition_id, error = %e, "❌ Merge 失败");
                    }
                }
            }
            tokio::task::yield_now().await;
        }

        sleep(interval).await;
    }
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
    let clob_config = ClobConfig::builder().use_server_time(true).build();
    let mut auth_builder_risk = Client::new("https://clob.polymarket.com", clob_config)?
        .authentication_builder(&signer_for_risk);

    // 如果提供了proxy_address，设置funder和signature_type
    if let Some(funder) = config.proxy_address {
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
    info!("🧪 默认模拟交易：在 Web 控制台可切换真实投注开关");

  

    // 收尾进行中标志：定时 merge 会检查并跳过，避免与收尾 merge 竞争
    let wind_down_in_progress = Arc::new(AtomicBool::new(false));
    let countdown_in_progress = Arc::new(AtomicBool::new(false));

    let marketrecord: Arc<DashMap<B256, bool>> = Arc::new(DashMap::new());//是否下过单
    let allowmarket: Arc<DashMap<B256, bool>> = Arc::new(DashMap::new());//允许下单

    // 定时 Merge：每 N 分钟根据持仓执行 merge，仅对 YES+NO 双边都持仓的市场
    // let merge_interval = config.merge_interval_minutes;
     let merge_interval = 0;
    if merge_interval > 0 {
        if let Some(proxy) = config.proxy_address {
            let private_key = config.private_key.clone();
            let position_tracker = _risk_manager.position_tracker().clone();
            let wind_down_flag = wind_down_in_progress.clone();
            let countdown_flag_merge = countdown_in_progress.clone();
            tokio::spawn(async move {
                run_merge_task(
                    merge_interval,
                    proxy,
                    private_key,
                    position_tracker,
                    wind_down_flag,
                    countdown_flag_merge,
                )
                .await;
            });
            info!(
                interval_minutes = merge_interval,
                "已启动定时 Merge 任务，每 {} 分钟根据持仓执行（仅 YES+NO 双边）", merge_interval
            );
        } else {
            warn!(
                "MERGE_INTERVAL_MINUTES={} 但未设置 POLYMARKET_PROXY_ADDRESS，定时 Merge 已禁用",
                merge_interval
            );
        }
    } else {
        info!("定时 Merge 未启用（MERGE_INTERVAL_MINUTES=0），如需启用请在 .env 中设置 MERGE_INTERVAL_MINUTES 为正数，例如 5 或 15");
    }

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

        let position_tracker = _risk_manager.position_tracker().clone();

        // 初始化订单簿监控器
        let mut monitor = OrderBookMonitor::new();

        // 订阅所有市场
        for market in &markets {
            if let Err(e) = monitor.subscribe_market(market) {
                error!(error = %e, market_id = %market.market_id, "订阅市场失败");
            }
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

        // 记录当前窗口的时间戳，用于检测周期切换与收尾触发
        use crate::market::discoverer::FIVE_MIN_SECS;
        use chrono::Utc;
        let current_window_timestamp =
            MarketDiscoverer::calculate_current_window_timestamp(Utc::now());
        let window_end =
            chrono::DateTime::from_timestamp(current_window_timestamp + FIVE_MIN_SECS, 0)
                .unwrap_or_else(|| Utc::now());
        let mut wind_down_done = false;
        let mut post_end_claim_done = false;
        let mut force_close_cancel_done = false;
        let mut first_order = false;
        let mut end_order = false;
        let mut end_30 = false;
        let mut is_small = false;
        

        // 创建市场ID到市场信息的映射
        let market_map: HashMap<B256, &MarketInfo> =
            markets.iter().map(|m| (m.market_id, m)).collect();

        // 按市场记录上一拍卖一价，用于计算涨跌方向（仅一次 HashMap 读写，不影响监控性能）
        let last_prices: DashMap<B256, (Decimal, Decimal)> = DashMap::new();
        let strategy_state: DashMap<B256, u8> = DashMap::new();
        let first_leg_price_map: Arc<DashMap<B256, Decimal>> = Arc::new(DashMap::new());
        let first_leg_qty_map: Arc<DashMap<B256, Decimal>> = Arc::new(DashMap::new());
        let first_leg_side_key_map: Arc<DashMap<B256, u8>> = Arc::new(DashMap::new());
        let yes_greater_than_no_counters: DashMap<B256, u32> = DashMap::new();
        let last_check_timestamps: DashMap<B256, i64> = DashMap::new();
        let current_larger_side: DashMap<B256, Option<bool>> = DashMap::new(); // None: no data, Some(true): yes larger, Some(false): no larger
        let larger_side_order_counters: DashMap<B256, u32> = DashMap::new(); // 较大边订单计数器
        //订单存储(市场id,是否下单,下单yes/no,下单的tokenid,未下单的tokenid,下单的数量)
        let order_status: Arc<Mutex<HashMap<String, (bool, String, String, String, String, String)>>> = Arc::new(Mutex::new(HashMap::new()));
        let drawdown_trigger_mask: Arc<DashMap<B256, u8>> = Arc::new(DashMap::new());
        
        #[derive(Clone)]
        struct SideOrderInfo {
            price: Decimal,
            size: Decimal,
            total_qty: Decimal,
            total_cost: Decimal,
            avg_price: Decimal,
            gross_profit: Decimal,
            net_profit: Decimal,
        }

        #[derive(Clone)]
        struct UpDownOrderInfo {
            up_price: Decimal,
            up_size: Decimal,
            up_total_qty: Decimal,
            up_total_cost: Decimal,
            up_avg_price: Decimal,
            up_gross_profit: Decimal,
            up_net_profit: Decimal,
            up_order_count: u32,
            down_price: Decimal,
            down_size: Decimal,
            down_total_qty: Decimal,
            down_total_cost: Decimal,
            down_avg_price: Decimal,
            down_gross_profit: Decimal,
            down_net_profit: Decimal,
            down_order_count: u32,
            yes_final_price: Decimal,
            no_final_price: Decimal,
        }
        let up_down_history: Arc<DashMap<String, UpDownOrderInfo>> = Arc::new(DashMap::new());
        let final_prices: Arc<DashMap<String, (Decimal, Decimal)>> = Arc::new(DashMap::new());

     

        // 监控订单簿更新
        loop {
            let now_all = Utc::now();
            let seconds_until_end_all = (window_end - now_all).num_seconds();
            tokio::select! {
                // 处理订单簿更新
                book_result = stream.next() => {
                    match book_result {
                        Some(Ok(book)) => {
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
                                let price_to_beat = market_info.and_then(|m| m.price_to_beat);
                                let market_display = if !market_symbol.is_empty() {
                                    format!("{}", market_symbol)
                                } else {
                                    market_title.to_string()
                                };
                                if let (Some((yes_p, _)), Some((no_p, _))) = (yes_best_ask, no_best_ask) {
                                    final_prices.insert(market_display.clone(), (yes_p, no_p));
                                }
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
                                            if first_order==true {
                                                is_small=true;
                                            }
                                            // 较大的一边发生变化，重置计数器
                                            *counter = 0;
                                            *larger_side = current_larger;
                                            info!(
                                                "{} | jiajiajia较大边变化 | 新较大边: {:?} | Yes:A{:.4} No:A{:.4}",
                                                market_display, current_larger, yes_price, no_price
                                            );
                                            
                                            // 确定当前较小的一边
                                            let current_smaller = if yes_price < no_price {
                                                Some(true) // yes较小
                                            } else if no_price < yes_price {
                                                Some(false) // no较小
                                            } else {
                                                None // 价格相等
                                            };
                                            
                                            // 确定当前较小的一边的价格
                                            let smaller_price = if yes_price < no_price {
                                                yes_price
                                            } else if no_price < yes_price {
                                                no_price
                                            } else {
                                                yes_price // 价格相等时默认使用yes价格
                                            };
                                            
                                        } else if current_larger.is_some() {
                                            // 较大的一边未变化，计数加1
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

                                        let countdown_within_180 = sec_to_end_nonneg <= 250 && sec_to_end_nonneg > 60;
                                        let countdown_within_30 = sec_to_end_nonneg <= 60 && sec_to_end_nonneg > 0;
                                        
                                        let price_greater_than_07 = price > dec!(0.7);
                                        let price_greater_than_97 = price < dec!(0.97);

                                        
                                        // 先检查计数器值
                                        let price_greater_count = yes_greater_than_no_counters.get(&market_id).map(|r| *r == 60).unwrap_or(false);
                                        
                                        // 计数达到60后重置
                                        if price_greater_count {
                                            yes_greater_than_no_counters.insert(market_id, 0);
                                        }

                                        //如果有订单,而且订单中购买的token这边价格卖价在0.97,对订单进行清仓
                                        //jiajiatodo
                                        if countdown_within_180  {
                                            if first_order==false {
                                             
                                                    //加个条件，如果价格小于0.90，才下单
                                            //up和down分别根据当前价格下单限价单
                                            if let (Some((yes_price, _)), Some((no_price, _))) = (yes_best_ask, no_best_ask) {
                                                // 为up和down方向分别下单限价单
                                                let up_price = yes_price * dec!(1.02); // 上涨方向价格
                                                let down_price = no_price * dec!(0.99); // 下跌方向价格
                                                
                                                let (up_size, down_size) = if (up_price >= dec!(0.5) && up_price <= dec!(0.6)) || (down_price >= dec!(0.5) && down_price <= dec!(0.6)) {
                                                    if up_price <= down_price {
                                                        (dec!(5), dec!(10))
                                                    } else {
                                                        (dec!(10), dec!(5))
                                                    }
                                                } else if (up_price >= dec!(0.7) && up_price <= dec!(0.8)) || (down_price >= dec!(0.7) && down_price <= dec!(0.8)) {
                                                    if up_price <= down_price {
                                                        (dec!(10), dec!(5))
                                                    } else {
                                                        (dec!(5), dec!(10))
                                                    }
                                                } else {
                                                    (dec!(0), dec!(0))
                                                };

                                           if up_size > dec!(0) && down_size > dec!(0) {
                                            first_order=true;
                                                let up_total_cost_single = up_price * up_size;
                                                let down_total_cost_single = down_price * down_size;
                                                
                                                let market_key = market_display.clone();
                                                
                                                let (up_avg_price, up_total_qty, up_total_cost_acc, up_last_profit, up_last_net_profit) = if let Some(history) = up_down_history.get(&market_key) {
                                                    let prev_up_price = history.up_avg_price;
                                                    let prev_up_size = history.up_total_qty;
                                                    let prev_up_total_cost = history.up_total_cost;
                                                    let prev_down_total_cost = history.down_total_cost;

                                                    let new_up_avg_price = (prev_up_price * prev_up_size + up_price * up_size) / (prev_up_size + up_size);
                                                    let new_up_total_qty = prev_up_size + up_size;
                                                    let new_up_total_cost = prev_up_total_cost + up_total_cost_single;
                                                    let new_up_gross_profit = (dec!(1) - new_up_avg_price) * new_up_total_qty;
                                                    let new_up_net_profit = new_up_gross_profit - prev_down_total_cost;

                                                    (new_up_avg_price, new_up_total_qty, new_up_total_cost, new_up_gross_profit, new_up_net_profit)
                                                } else {
                                                    let up_gross_profit = (dec!(1) - up_price) * up_size;
                                                    (up_price, up_size, up_total_cost_single, up_gross_profit, up_gross_profit)
                                                };

                                                let (down_avg_price, down_total_qty, down_total_cost_acc, down_last_profit, down_last_net_profit) = if let Some(history) = up_down_history.get(&market_key) {
                                                    let prev_up_total_cost = history.up_total_cost;
                                                    let prev_down_price = history.down_avg_price;
                                                    let prev_down_size = history.down_total_qty;
                                                    let prev_down_total_cost = history.down_total_cost;

                                                    let new_down_avg_price = (prev_down_price * prev_down_size + down_price * down_size) / (prev_down_size + down_size);
                                                    let new_down_total_qty = prev_down_size + down_size;
                                                    let new_down_total_cost = prev_down_total_cost + down_total_cost_single;
                                                    let new_down_gross_profit = (dec!(1) - new_down_avg_price) * new_down_total_qty;
                                                    let new_down_net_profit = new_down_gross_profit - prev_up_total_cost;

                                                    (new_down_avg_price, new_down_total_qty, new_down_total_cost, new_down_gross_profit, new_down_net_profit)
                                                } else {
                                                    let down_gross_profit = (dec!(1) - down_price) * down_size;
                                                    (down_price, down_size, down_total_cost_single, down_gross_profit, down_gross_profit)
                                                };
                                                
                                                // 更新历史记录
                                                let existing_history = up_down_history.get(&market_key)
                                                    .map(|r| r.clone())
                                                    .unwrap_or(UpDownOrderInfo {
                                                        up_price: dec!(0),
                                                        up_size: dec!(0),
                                                        up_total_qty: dec!(0),
                                                        up_total_cost: dec!(0),
                                                        up_avg_price: dec!(0),
                                                        up_gross_profit: dec!(0),
                                                        up_net_profit: dec!(0),
                                                        up_order_count: 0,
                                                        down_price: dec!(0),
                                                        down_size: dec!(0),
                                                        down_total_qty: dec!(0),
                                                        down_total_cost: dec!(0),
                                                        down_avg_price: dec!(0),
                                                        down_gross_profit: dec!(0),
                                                        down_net_profit: dec!(0),
                                                        down_order_count: 0,
                                                        yes_final_price: dec!(0),
                                                        no_final_price: dec!(0),
                                                    });
                                                up_down_history.insert(market_key.clone(), UpDownOrderInfo {
                                                    // 更新UP和DOWN方向数据
                                                    up_price: up_avg_price,
                                                    up_size: up_total_qty,
                                                    up_total_qty: up_total_qty,
                                                    up_total_cost: up_total_cost_acc,
                                                    up_avg_price,
                                                    up_gross_profit: up_last_profit,
                                                    up_net_profit: up_last_profit-down_total_cost_acc,
                                                    up_order_count: existing_history.up_order_count + 1,
                                                    down_price: down_avg_price,
                                                    down_size: down_total_qty,
                                                    down_total_qty: down_total_qty,
                                                    down_total_cost: down_total_cost_acc,
                                                    down_avg_price,
                                                    down_gross_profit: down_last_profit,
                                                    down_net_profit: down_last_profit-up_total_cost_acc,
                                                    down_order_count: existing_history.down_order_count + 1,
                                                    yes_final_price: existing_history.yes_final_price,
                                                    no_final_price: existing_history.no_final_price,
                                                });
                                                
                                                let up_order_cnt = existing_history.up_order_count + 1;
                                                let down_order_cnt = existing_history.down_order_count + 1;
                                                
                                                info!("{} | UP方向下单 | 单边成交单价={:.4} | 单边成交数量={:.0} | 单边总成本={:.4} | 单边平均价={:.4} | 单边总数量={:.0} | 单边毛利润={:.4} | 单边纯利润={:.4} | 下单次数={}",
                                                    market_display, up_price, up_size, up_total_cost_acc, up_avg_price, up_total_qty, up_last_profit, up_last_net_profit-down_total_cost_acc, up_order_cnt);
                                                info!("{} | DOWN方向下单 | 单边成交单价={:.4} | 单边成交数量={:.0} | 单边总成本={:.4} | 单边平均价={:.4} | 单边总数量={:.0} | 单边毛利润={:.4} | 单边纯利润={:.4} | 下单次数={}",
                                                    market_display, down_price, down_size, down_total_cost_acc, down_avg_price, down_total_qty, down_last_profit, down_last_net_profit-up_total_cost_acc, down_order_cnt);
                                                
                                                let is_live = is_running.load(Ordering::Relaxed);
                                                if is_live {
                                                    let exec = executor.clone();
                                                    let up_token = pair.yes_book.asset_id;
                                                    let up_p = up_price;
                                                    let up_s = up_size;
                                                    tokio::spawn(async move {
                                                        match exec.buy_at_price(up_token, up_p, up_s).await {
                                                            Ok(_) => info!("UP方向限价单买入成功: YES token={:#x} price={:.4} size={:.0}", up_token, up_p, up_s),
                                                            Err(e) => error!("UP方向限价单买入失败: {}", e),
                                                        }
                                                    });
                                                    
                                                    let exec2 = executor.clone();
                                                    let down_token = pair.no_book.asset_id;
                                                    let down_p = down_price;
                                                    let down_s = down_size;
                                                    tokio::spawn(async move {
                                                        match exec2.buy_at_price(down_token, down_p, down_s).await {
                                                            Ok(_) => info!("DOWN方向限价单买入成功: NO token={:#x} price={:.4} size={:.0}", down_token, down_p, down_s),
                                                            Err(e) => error!("DOWN方向限价单买入失败: {}", e),
                                                        }
                                                    });
                                                }
                                                }
                                            }
                                            }
                                                 

                                         //首单下完了
                                        // 判断哪边已下数量小，只有数量小的那边才下单，两边相等则不下单
                                        let low_side_qty = if let Some(history) = up_down_history.get(&market_display.clone()) {
                                            if low_side_name == "Yes" { history.up_total_qty } else { history.down_total_qty }
                                        } else { dec!(0) };
                                        let high_side_qty = if let Some(history) = up_down_history.get(&market_display.clone()) {
                                            if low_side_name == "Yes" { history.down_total_qty } else { history.up_total_qty }
                                        } else { dec!(0) };
                                        let lowest_price_threshold = if let Some(history) = up_down_history.get(&market_display.clone()) {
                                            if low_side_name == "Yes" { history.up_avg_price - dec!(0.2) } else { history.down_avg_price - dec!(0.2) }
                                        } else { dec!(0) };
                                        if low_price <= lowest_price_threshold && low_side_qty < high_side_qty && first_order==true {
                                            is_small=false;
                                            let small_order_size = dec!(5);
                                            let small_total_cost = low_price * small_order_size;
                                            let market_key = market_display.clone();
                                            
                                            let (small_avg_price, small_total_qty, small_total_cost_sum, small_gross_profit, small_net_profit, small_order_count) = if let Some(history) = up_down_history.get(&market_key) {
                                                let prev_small_avg_price = if low_side_name == "Yes" { history.up_avg_price } else { history.down_avg_price };
                                                let prev_small_size = if low_side_name == "Yes" { history.up_total_qty } else { history.down_total_qty };
                                                let prev_small_cost = if low_side_name == "Yes" { history.up_total_cost } else { history.down_total_cost };
                                                let prev_small_order_count = if low_side_name == "Yes" { history.up_order_count } else { history.down_order_count };
                                                let other_side_total_cost = if low_side_name == "Yes" { history.down_total_cost } else { history.up_total_cost };

                                                let new_small_avg_price = (prev_small_avg_price * prev_small_size + low_price * small_order_size) / (prev_small_size + small_order_size);
                                                let new_small_total_qty = prev_small_size + small_order_size;
                                                let new_small_total_cost = prev_small_cost + small_total_cost;
                                                let new_small_gross_profit = (dec!(1) - new_small_avg_price) * new_small_total_qty;
                                                let new_small_net_profit = new_small_gross_profit - other_side_total_cost;

                                                (new_small_avg_price, new_small_total_qty, new_small_total_cost, new_small_gross_profit, new_small_net_profit, prev_small_order_count + 1)
                                            } else {
                                                let small_gross_profit = (dec!(1) - low_price) * small_order_size;
                                                let small_net_profit = small_gross_profit - small_total_cost;
                                                (low_price, small_order_size, small_total_cost, small_gross_profit, small_net_profit, 1)
                                            };
                                            
                                            let side_label = if low_side_name == "Yes" { "UP" } else { "DOWN" };
                                            info!("{} | {}方向(小边)下单 | 单边成交单价={:.4} | 单边成交数量={:.0} | 单边总成本={:.4} | 单边平均价={:.4} | 单边总数量={:.0} | 单边毛利润={:.4} | 单边纯利润={:.4} | 下单次数={}", 
                                                market_display, side_label, low_price, small_order_size, small_total_cost, small_avg_price, small_total_qty, small_gross_profit, small_net_profit, small_order_count);
                                            
                                            // 更新历史记录
                                            if let Some(history) = up_down_history.get(&market_key) {
                                                let existing = history.clone();
                                                drop(history);
                                                if low_side_name == "Yes" {
                                                    up_down_history.insert(market_key.clone(), UpDownOrderInfo {
                                                        up_price: small_avg_price,
                                                        up_size: small_total_qty,
                                                        up_total_qty: small_total_qty,
                                                        up_total_cost: small_total_cost_sum,
                                                        up_avg_price: small_avg_price,
                                                        up_gross_profit: small_gross_profit,
                                                        up_net_profit: small_net_profit,
                                                        up_order_count: small_order_count,
                                                        down_price: existing.down_price,
                                                        down_size: existing.down_size,
                                                        down_total_qty: existing.down_total_qty,
                                                        down_total_cost: existing.down_total_cost,
                                                        down_avg_price: existing.down_avg_price,
                                                        down_gross_profit: existing.down_gross_profit,
                                                        down_net_profit: existing.down_net_profit,
                                                        down_order_count: existing.down_order_count,
                                                        yes_final_price: existing.yes_final_price,
                                                        no_final_price: existing.no_final_price,
                                                    });
                                                } else {
                                                    up_down_history.insert(market_key.clone(), UpDownOrderInfo {
                                                        up_price: existing.up_price,
                                                        up_size: existing.up_size,
                                                        up_total_qty: existing.up_total_qty,
                                                        up_total_cost: existing.up_total_cost,
                                                        up_avg_price: existing.up_avg_price,
                                                        up_gross_profit: existing.up_gross_profit,
                                                        up_net_profit: existing.up_net_profit,
                                                        up_order_count: existing.up_order_count,
                                                        down_price: small_avg_price,
                                                        down_size: small_total_qty,
                                                        down_total_qty: small_total_qty,
                                                        down_total_cost: small_total_cost_sum,
                                                        down_avg_price: small_avg_price,
                                                        down_gross_profit: small_gross_profit,
                                                        down_net_profit: small_net_profit,
                                                        down_order_count: small_order_count,
                                                        yes_final_price: existing.yes_final_price,
                                                        no_final_price: existing.no_final_price,
                                                    });
                                                }
                                            } else {
                                                if low_side_name == "Yes" {
                                                    up_down_history.insert(market_key.clone(), UpDownOrderInfo {
                                                        up_price: small_avg_price,
                                                        up_size: small_total_qty,
                                                        up_total_qty: small_total_qty,
                                                        up_total_cost: small_total_cost_sum,
                                                        up_avg_price: small_avg_price,
                                                        up_gross_profit: small_gross_profit,
                                                        up_net_profit: small_net_profit,
                                                        up_order_count: small_order_count,
                                                        down_price: dec!(0),
                                                        down_size: dec!(0),
                                                        down_total_qty: dec!(0),
                                                        down_total_cost: dec!(0),
                                                        down_avg_price: dec!(0),
                                                        down_gross_profit: dec!(0),
                                                        down_net_profit: dec!(0),
                                                        down_order_count: 0,
                                                        yes_final_price: dec!(0),
                                                        no_final_price: dec!(0),
                                                    });
                                                } else {
                                                    up_down_history.insert(market_key.clone(), UpDownOrderInfo {
                                                        up_price: dec!(0),
                                                        up_size: dec!(0),
                                                        up_total_qty: dec!(0),
                                                        up_total_cost: dec!(0),
                                                        up_avg_price: dec!(0),
                                                        up_gross_profit: dec!(0),
                                                        up_net_profit: dec!(0),
                                                        up_order_count: 0,
                                                        down_price: small_avg_price,
                                                        down_size: small_total_qty,
                                                        down_total_qty: small_total_qty,
                                                        down_total_cost: small_total_cost_sum,
                                                        down_avg_price: small_avg_price,
                                                        down_gross_profit: small_gross_profit,
                                                        down_net_profit: small_net_profit,
                                                        down_order_count: small_order_count,
                                                        yes_final_price: dec!(0),
                                                        no_final_price: dec!(0),
                                                    });
                                                }
                                            }
                                            
                                            let is_live = is_running.load(Ordering::Relaxed);
                                            if is_live {
                                                let exec = executor.clone();
                                                let small_token = if low_side_name == "Yes" { pair.yes_book.asset_id } else { pair.no_book.asset_id };
                                                let small_p = low_price;
                                                let small_s = small_order_size;
                                                let side_str = side_label.to_string();
                                                let market_str = market_display.clone();
                                                tokio::spawn(async move {
                                                    match exec.buy_at_price(small_token, small_p, small_s).await {
                                                        Ok(_) => info!("{} | {}方向(小边)限价单买入成功: token={:#x} price={:.4} size={:.0}", market_str, side_str, small_token, small_p, small_s),
                                                        Err(e) => error!("{} | {}方向(小边)限价单买入失败: {}", market_str, side_str, e),
                                                    }
                                                });
                                            }
                                        }

                                }else if countdown_within_30 { 


                                       let low_side_qty = if let Some(history) = up_down_history.get(&market_display.clone()) {
                                            if low_side_name == "Yes" { history.up_total_qty } else { history.down_total_qty }
                                        } else { dec!(0) };
                                        let high_side_qty = if let Some(history) = up_down_history.get(&market_display.clone()) {
                                            if low_side_name == "Yes" { history.down_total_qty } else { history.up_total_qty }
                                        } else { dec!(0) };

                                            //首单下完了
                                            // 判断哪边价格偏大，选择价格偏大的那边下单
                                            let high_price_side = if yes_price > no_price { "Yes" } else { "No" };
                                            let high_price = if high_price_side == "Yes" { yes_price } else { no_price };
                                            let low_price_side = if high_price_side == "Yes" { "No" } else { "Yes" };
                                            let low_price_val = if low_price_side == "Yes" { yes_price } else { no_price };

                                            // 检查是否满足下单条件
                                            if low_side_qty > dec!(10) || high_side_qty > dec!(10) {
                                                is_small=false;
                                                let order_size = dec!(10);
                                                // 确定市场方向
                                                let market_direction = if high_price_side == "Yes" { "UP" } else { "DOWN" };
                                                // 根据market_direction选择购买价格和资产
                                                let buy_price = high_price;
                                                let total_cost = buy_price * order_size;
                                                let market_key = market_display.clone();
                                                
                                                let (avg_price, total_qty, total_cost_sum, gross_profit, net_profit, order_count) = if let Some(history) = up_down_history.get(&market_key) {
                                                    let prev_avg_price = if high_price_side == "Yes" { history.up_avg_price } else { history.down_avg_price };
                                                    let prev_size = if high_price_side == "Yes" { history.up_total_qty } else { history.down_total_qty };
                                                    let prev_cost = if high_price_side == "Yes" { history.up_total_cost } else { history.down_total_cost };
                                                    let prev_order_count = if high_price_side == "Yes" { history.up_order_count } else { history.down_order_count };
                                                    let other_side_total_cost = if high_price_side == "Yes" { history.down_total_cost } else { history.up_total_cost };

                                                    let new_avg_price = (prev_avg_price * prev_size + buy_price * order_size) / (prev_size + order_size);
                                                    let new_total_qty = prev_size + order_size;
                                                    let new_total_cost = prev_cost + total_cost;
                                                    let new_gross_profit = (dec!(1) - new_avg_price) * new_total_qty;
                                                    let new_net_profit = new_gross_profit - other_side_total_cost;

                                                    (new_avg_price, new_total_qty, new_total_cost, new_gross_profit, new_net_profit, prev_order_count + 1)
                                                } else {
                                                    let gross_profit = (dec!(1) - buy_price) * order_size;
                                                    let net_profit = gross_profit - total_cost;
                                                    (buy_price, order_size, total_cost, gross_profit, net_profit, 1)
                                                };
                                                
                                                let side_label = market_direction;
                                                info!("{} | {}方向(价格偏大边)下单 | 单边成交单价={:.4} | 单边成交数量={:.0} | 单边总成本={:.4} | 单边平均价={:.4} | 单边总数量={:.0} | 单边毛利润={:.4} | 单边纯利润={:.4} | 下单次数={}", 
                                                    market_display, side_label, buy_price, order_size, total_cost, avg_price, total_qty, gross_profit, net_profit, order_count);
                                                
                                                // 更新历史记录
                                                if let Some(history) = up_down_history.get(&market_key) {
                                                    let existing = history.clone();
                                                    drop(history);
                                                    if high_price_side == "Yes" {
                                                        up_down_history.insert(market_key.clone(), UpDownOrderInfo {
                                                            up_price: avg_price,
                                                            up_size: total_qty,
                                                            up_total_qty: total_qty,
                                                            up_total_cost: total_cost_sum,
                                                            up_avg_price: avg_price,
                                                            up_gross_profit: gross_profit,
                                                            up_net_profit: net_profit,
                                                            up_order_count: order_count,
                                                            down_price: existing.down_price,
                                                            down_size: existing.down_size,
                                                            down_total_qty: existing.down_total_qty,
                                                            down_total_cost: existing.down_total_cost,
                                                            down_avg_price: existing.down_avg_price,
                                                            down_gross_profit: existing.down_gross_profit,
                                                            down_net_profit: existing.down_net_profit,
                                                            down_order_count: existing.down_order_count,
                                                            yes_final_price: existing.yes_final_price,
                                                            no_final_price: existing.no_final_price,
                                                        });
                                                    } else {
                                                        up_down_history.insert(market_key.clone(), UpDownOrderInfo {
                                                            up_price: existing.up_price,
                                                            up_size: existing.up_size,
                                                            up_total_qty: existing.up_total_qty,
                                                            up_total_cost: existing.up_total_cost,
                                                            up_avg_price: existing.up_avg_price,
                                                            up_gross_profit: existing.up_gross_profit,
                                                            up_net_profit: existing.up_net_profit,
                                                            up_order_count: existing.up_order_count,
                                                            down_price: avg_price,
                                                            down_size: total_qty,
                                                            down_total_qty: total_qty,
                                                            down_total_cost: total_cost_sum,
                                                            down_avg_price: avg_price,
                                                            down_gross_profit: gross_profit,
                                                            down_net_profit: net_profit,
                                                            down_order_count: order_count,
                                                            yes_final_price: existing.yes_final_price,
                                                            no_final_price: existing.no_final_price,
                                                        });
                                                    }
                                                } else {
                                                    if high_price_side == "Yes" {
                                                        up_down_history.insert(market_key.clone(), UpDownOrderInfo {
                                                            up_price: avg_price,
                                                            up_size: total_qty,
                                                            up_total_qty: total_qty,
                                                            up_total_cost: total_cost_sum,
                                                            up_avg_price: avg_price,
                                                            up_gross_profit: gross_profit,
                                                            up_net_profit: net_profit,
                                                            up_order_count: order_count,
                                                            down_price: dec!(0),
                                                            down_size: dec!(0),
                                                            down_total_qty: dec!(0),
                                                            down_total_cost: dec!(0),
                                                            down_avg_price: dec!(0),
                                                            down_gross_profit: dec!(0),
                                                            down_net_profit: dec!(0),
                                                            down_order_count: 0,
                                                            yes_final_price: dec!(0),
                                                            no_final_price: dec!(0),
                                                        });
                                                    } else {
                                                        up_down_history.insert(market_key.clone(), UpDownOrderInfo {
                                                            up_price: dec!(0),
                                                            up_size: dec!(0),
                                                            up_total_qty: dec!(0),
                                                            up_total_cost: dec!(0),
                                                            up_avg_price: dec!(0),
                                                            up_gross_profit: dec!(0),
                                                            up_net_profit: dec!(0),
                                                            up_order_count: 0,
                                                            down_price: avg_price,
                                                            down_size: total_qty,
                                                            down_total_qty: total_qty,
                                                            down_total_cost: total_cost_sum,
                                                            down_avg_price: avg_price,
                                                            down_gross_profit: gross_profit,
                                                            down_net_profit: net_profit,
                                                            down_order_count: order_count,
                                                            yes_final_price: dec!(0),
                                                            no_final_price: dec!(0),
                                                        });
                                                    }
                                                }
                                                
                                                let is_live = is_running.load(Ordering::Relaxed);
                                                if is_live {
                                                    let exec = executor.clone();
                                                    let buy_token = if high_price_side == "Yes" { pair.yes_book.asset_id } else { pair.no_book.asset_id };
                                                    let buy_p = high_price;
                                                    let buy_s = order_size;
                                                    let side_str = side_label.to_string();
                                                    let market_str = market_display.clone();
                                                    tokio::spawn(async move {
                                                        match exec.buy_at_price(buy_token, buy_p, buy_s).await {
                                                            Ok(_) => info!("{} | {}方向(价格偏大边)限价单买入成功: token={:#x} price={:.4} size={:.0}", market_str, side_str, buy_token, buy_p, buy_s),
                                                            Err(e) => error!("{} | {}方向(价格偏大边)限价单买入失败: {}", market_str, side_str, e),
                                                        }
                                                    });
                                                }
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

                                //如果有订单,订单中的方向和市场结果比对,如果一直就赢了,如果不一致就输了
                                let (result_info, pnl_info) = match (yes_best_ask, no_best_ask) {
                                    (Some(_), None) => {
                                        // No赢
                                        ("【No赢】".to_string(), String::new())
                                    }
                                    (None, Some(_)) => {
                                        // Yes赢
                                        ("【Yes赢】".to_string(), String::new())
                                    }
                                    (Some(_), Some(_)) => (String::new(), String::new()), // 两边都有价，市场未结算
                                    (None, None) => (String::new(), String::new()),
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
                                
                                let log_line = format!(
                                    "{} | {}分{:02}秒 | {} | {}\n",
                                    market_display,
                                    countdown_minutes,
                                    countdown_seconds,
                                    yes_info,
                                    no_info
                                );
                                // 生成5分钟间隔的时间戳
                                let now = std::time::SystemTime::now();
                                let timestamp = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
                                let five_min_interval = (timestamp / 300) * 300; // 5分钟间隔
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
 

                // 定期检查：1) 是否进入新的5分钟窗口 2) 收尾触发（5分钟窗口需更频繁检查）
                _ = sleep(Duration::from_secs(1)) => {
                    let now = Utc::now();
                    let sec_to_end = (window_end - now).num_seconds();
                    let cancel_window_active = sec_to_end <= 15 && sec_to_end >= 0;
                    countdown_in_progress.store(cancel_window_active, Ordering::Relaxed);

                    if cancel_window_active && !force_close_cancel_done {
                        force_close_cancel_done = true;
                        let is_live = is_running.load(Ordering::Relaxed);
                        if is_live {
                            let exec_cancel = executor.clone();
                            tokio::spawn(async move {
                                match exec_cancel.cancel_all_orders().await {
                                    Ok(_) => info!("🧯 倒计时15秒强制平仓：已取消所有挂单"),
                                    Err(e) => warn!(error = %e, "🧯 倒计时15秒强制平仓：取消所有挂单失败"),
                                }
                            });
                        }
                    }

                    let new_window_timestamp = MarketDiscoverer::calculate_current_window_timestamp(now);

                    // 如果当前窗口时间戳与记录的不同，说明已经进入新窗口
                    if new_window_timestamp != current_window_timestamp {
                        info!(
                            old_window = current_window_timestamp,
                            new_window = new_window_timestamp,
                            "检测到新的5分钟窗口，准备取消旧订阅并切换到新窗口"
                        );
                        
                        // 输出UP和DOWN方向的最终状态
                        for entry in up_down_history.iter() {
                            let market_key = entry.key();
                            let history = entry.value();
                            
                            let (yes_final_price, no_final_price) = final_prices.get(market_key).map(|r| (r.0, r.1)).unwrap_or((dec!(0), dec!(0)));
                            
                            // 判断哪边赢：根据市场实时价格判断，YES价格=0.99则UP赢，NO价格=0.99则DOWN赢
                            let winner = if yes_final_price == dec!(0.99) {
                                "UP"
                            } else if no_final_price == dec!(0.99) {
                                "DOWN"
                            } else if yes_final_price == dec!(0) {
                                "DOWN"
                            } else if no_final_price == dec!(0) {
                                "UP"
                            } else if yes_final_price < no_final_price {
                                "DOWN"
                            } else {
                                "UP"
                            };
                            
                            // 使用存储的累计数据输出日志
                            let win_profit = if winner == "UP" { history.up_gross_profit - history.down_total_cost } else { history.down_gross_profit - history.up_total_cost };
                            info!("{} | UP方向 | 单边成交单价={:.4} | 单边成交数量={:.0} | 单边总成本={:.4} | 单边平均价={:.4} | 单边总数量={:.0} | 单边毛利润={:.4} | 单边纯利润={:.4} | 下单次数={}",
                                market_key, history.up_price, history.up_size, history.up_total_cost, history.up_avg_price, history.up_total_qty, history.up_gross_profit, history.up_net_profit, history.up_order_count);
                            info!("{} | DOWN方向 | 单边成交单价={:.4} | 单边成交数量={:.0} | 单边总成本={:.4} | 单边平均价={:.4} | 单边总数量={:.0} | 单边毛利润={:.4} | 单边纯利润={:.4} | 下单次数={}",
                                market_key, history.down_price, history.down_size, history.down_total_cost, history.down_avg_price, history.down_total_qty, history.down_gross_profit, history.down_net_profit, history.down_order_count);
                            info!("{} | 本局结束 | YES价格={:.4} | NO价格={:.4} | 获胜方: {} | 纯利润: {:.4}", market_key, yes_final_price, no_final_price, winner, win_profit);
                        }

                        // 获取上一轮的市场信息列表 (condition_id, yes_token, no_token)
                        let prev_round_markets: Vec<(B256, U256, U256)> = market_map.values()
                            .map(|m| (m.market_id, m.yes_token_id, m.no_token_id))
                            .collect();
                        let pt = _risk_manager.position_tracker();
                        let proxy_addr = config.proxy_address.clone();
                        let priv_key = config.private_key.clone();

                        // 启动异步任务：在下一轮开始后10秒，对上一轮市场执行平仓（Merge/Redeem）
                        if  proxy_addr.is_some() {
                            let proxy = proxy_addr.unwrap();
                            let settle_delay_secs = 40u64;
                            info!(
                                "🕒 已安排{}秒后对 {} 个上一轮市场执行平仓（Merge/Redeem）检查",
                                settle_delay_secs,
                                prev_round_markets.len()
                            );

                            tokio::spawn(async move {
                                // sleep(Duration::from_secs(settle_delay_secs)).await;
                                // info!("⏰ 开始对上一轮市场执行平仓（Merge/Redeem）检查...");

                                // // 1. 先尝试 Merge 所有市场（无需等待决议，立刻执行）
                                // for (condition_id, _, _) in &prev_round_markets {
                                //     match merge::merge_max(*condition_id, proxy, &priv_key, None).await {
                                //         Ok(tx) => info!("✅ Merge 成功 | condition_id={} | tx={}", condition_id, tx),
                                //         Err(e) => {
                                //             if !e.to_string().contains("无可用份额") {
                                //                 debug!("Merge 跳过: {}", e);
                                //             }
                                //         }
                                //     }
                                // }

                                // 2. 根据用户当前持仓进行 Redeem（需等待决议，支持重试）- 已禁用，改为手动触发
                                use poly_5min_bot::positions::get_positions;
                                
                                let positions = match get_positions().await {
                                    Ok(pos) => pos,
                                    Err(e) => {
                                        warn!("获取持仓失败：{}", e);
                                        Vec::new()
                                    }
                                };
                                
                                let mut condition_ids: HashSet<B256> = positions.iter()
                                    .map(|p| p.condition_id)
                                    .collect();
                                    
                                if condition_ids.is_empty() {
                                    info!("🏁 当前无持仓，无需 Redeem");
                                } else if condition_ids.len() >= 1 {
                                    info!("📋 当前持仓市场数：{}，开始 Redeem", condition_ids.len());
                                    let mut completed = Vec::new();
                                    for condition_id in &condition_ids {
                                        match merge::redeem_max(*condition_id, proxy, &priv_key, None).await {
                                            Ok(tx) => {
                                                info!(condition_id = %condition_id, tx = %tx, "✅ Redeem 成功");
                                                completed.push(*condition_id);
                                            },
                                            Err(e) => {
                                                info!(condition_id = %condition_id, e = %e, "Redeem 失败");
                                                let err_msg = e.to_string();
                                                if err_msg.contains("无持仓") {
                                                    debug!("Redeem 跳过：无持仓 | condition_id={}", condition_id);
                                                    completed.push(*condition_id);
                                                } else {
                                                    warn!("⚠️ Redeem 暂未成功 (可能未决议) | condition_id={} | error={}", condition_id, err_msg);
                                                }
                                            }
                                        }
                                    }
                                    
                                    for c in completed {
                                        condition_ids.remove(&c);
                                    }
                                    
                                    if !condition_ids.is_empty() {
                                        warn!("仍有 {} 个市场未完成 Redeem", condition_ids.len());
                                    }
                                } else {
                                    info!("📋 当前持仓市场数：{} ≤ 10，跳过 Redeem", condition_ids.len());
                                }
                            });
                        }

                        // 先drop stream和resolutions_stream以释放对monitor的借用，然后清理旧的订阅
                        drop(stream);
                        monitor.clear();
                        break;
                    }
                }
            }
        }

        // monitor 会在循环结束时自动 drop，无需手动清理
        info!("当前窗口监控结束，刷新市场进入下一轮");
    }
}
