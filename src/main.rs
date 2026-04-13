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

    let lostcount: Arc<DashMap<String, u64>> = Arc::new(DashMap::new()); // 记录每个市场的输的次数
    let wincount: Arc<DashMap<String, u64>> = Arc::new(DashMap::new());   // 记录每个市场的赢的次数
    let loststate: Arc<DashMap<String, u64>> = Arc::new(DashMap::new());  // 记录输了几次后进行赢的
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
        //订单存储(市场id,是否下单,下单yes/no,下单的tokenid,未下单的tokenid,下单的数量)
        let order_status: Arc<Mutex<HashMap<String, (bool, String, String, String, String, String)>>> = Arc::new(Mutex::new(HashMap::new()));

        #[derive(Clone)]
        struct SimOrderInfo {
            market_id: B256,
            side_key: u8,
            limit_price: Decimal,
            size: Decimal,
            on_filled_state: u8,
            clear_first_leg_price: bool,
        }
        let sim_open_orders: Arc<DashMap<String, SimOrderInfo>> = Arc::new(DashMap::new());
        let drawdown_trigger_mask: Arc<DashMap<B256, u8>> = Arc::new(DashMap::new());

        // 计算投注金额的函数
        fn calculate_order_price(market: &str, lostcount: &Arc<DashMap<String, u64>>, wincount: &Arc<DashMap<String, u64>>, loststate: &Arc<DashMap<String, u64>>) -> Decimal {
            let lc = lostcount.get(market).map(|v| *v).unwrap_or(0);
            let wc = wincount.get(market).map(|v| *v).unwrap_or(0);
            let ls = loststate.get(market).map(|v| *v).unwrap_or(0);
            info!("market: {}, lostcount: {}, wincount: {}, loststate: {}", market, lc, wc, ls);
            
            // loststate 为 0 时
            if ls == 0 {
                if wc > 0 {
                    dec!(2)
                } else {
                    dec!(2)
                }
            } else {
                // loststate 不为 0，检查 win_count
                if wc == 0 {
                    // 连续输的情况
                    match ls {
                        1 => dec!(10),
                        2 => dec!(30),
                        _ => dec!(30)
                    }
                } else {
                    // loststate 不为 0，win_count 不为 0
                    match ls {
                        1 => {
                            match wc {
                                1 => dec!(5),
                                2 => dec!(2),
                                3..=7 => dec!(1),
                                _ => dec!(1)
                            }
                        },
                        2 => {
                            match wc {
                                1 => dec!(20),
                                2 => dec!(10),
                                3 => dec!(5),
                                4 => dec!(1),
                                5 => dec!(1),
                                6..=9 => dec!(1),
                                _ => dec!(1)
                            }
                        },
                        _ => {
                            match wc {
                                1 => dec!(30),
                                2 => dec!(10),
                                3 => dec!(5),
                                4 => dec!(1),
                                5 => dec!(1),
                                _ => dec!(1)
                            }
                        }
                    }
                }
            }
        }

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

                                {
                                    let is_live = is_running.load(Ordering::Relaxed);
                                  

                                    if !is_live {
                                        let mut to_fill: Vec<String> = Vec::new();
                                        for e in sim_open_orders.iter() {
                                            let info = e.value();
                                            if info.market_id != market_id {
                                                continue;
                                            }
                                            let can_fill = if info.side_key == 0 {
                                                yes_best_ask
                                                    .as_ref()
                                                    .map(|(p, _)| *p <= info.limit_price)
                                                    .unwrap_or(false)
                                            } else {
                                                no_best_ask
                                                    .as_ref()
                                                    .map(|(p, _)| *p <= info.limit_price)
                                                    .unwrap_or(false)
                                            };
                                            if can_fill {
                                                to_fill.push(e.key().clone());
                                            }
                                        }
                                        for id in to_fill {
                                            if let Some((_k, info)) = sim_open_orders.remove(&id) {
                                                crate::utils::trade_history::update_trade_status(&id, "SimBought");
                                                if info.on_filled_state == 1u8 || info.on_filled_state == 2u8 {
                                                    first_leg_qty_map.insert(info.market_id, info.size);
                                                    first_leg_side_key_map.insert(info.market_id, info.side_key);
                                                    drawdown_trigger_mask.remove(&info.market_id);
                                                }
                                                strategy_state.insert(info.market_id, info.on_filled_state);
                                                if info.clear_first_leg_price {
                                                    first_leg_price_map.remove(&info.market_id);
                                                    first_leg_qty_map.remove(&info.market_id);
                                                    first_leg_side_key_map.remove(&info.market_id);
                                                    drawdown_trigger_mask.remove(&info.market_id);
                                                }
                                            }
                                        }
                                    }
                                
                                 }

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
                                        let default = (false, "".to_string(),"" .to_string(),"" .to_string(),"" .to_string(),"" .to_string());

                                        if *larger_side != current_larger {
                                            // 较大的一边发生变化，重置计数器
                                            *counter = 0;
                                            *larger_side = current_larger;
                                            info!(
                                                "{} | jiajiajia较大边变化 | 新较大边: {:?} | Yes:A{:.4} No:A{:.4}",
                                                market_display, current_larger, yes_price, no_price
                                            );
                                            
                                            // 检查是否已经建仓，如果是则卖出
                                            let order_status_lock = order_status.lock().await;
                                            let (is_ordered, order_side_name, order_token_id, unorder_token_id, ordered_size, order_price) = order_status_lock.get(&market_display).unwrap_or(&default).clone();
                                            
                                            if is_ordered {
                                                error!("{} | 价格出现反转，已建仓，执行卖出操作", market_display);
                                                let sell_token_id = U256::from_str(&order_token_id).unwrap_or(U256::ZERO);
                                                let sell_size = ordered_size.parse::<Decimal>().unwrap_or(dec!(0));
                                                let sell_price = Decimal::from_str(&order_price).unwrap_or(dec!(0));
                                                let countdown_for_trade = countdown_str.clone();
                                                let market_id_str = market_id.to_string();
                                                
                                                // 异步执行卖出操作
                                                tokio::spawn({
                                                    let executor = executor.clone();
                                                    let market_display = market_display.clone();
                                                    let order_status = order_status.clone();
                                                    let countdown_for_trade = countdown_for_trade;
                                                    let market_id_str = market_id_str;
                                                    let order_side_name = order_side_name;
                                                    let is_running_clone = is_running.clone();
                                                    async move {
                                                        let is_live = is_running_clone.load(Ordering::Relaxed);
                                                        if is_live {
                                                            match executor.sell_at_price(sell_token_id, sell_price, sell_size).await {
                                                                Ok(response) => {
                                                                    let mut order_status_map = order_status.lock().await;
                                                                    order_status_map.insert(market_display.clone(), (false, "".to_string(), "".to_string(), "".to_string(), "".to_string(), "".to_string()));
                                                                    use crate::utils::trade_history::{add_trade, TradeRecord};
                                                                    use chrono::Utc;
                                                                    let order_id = if response.order_id.is_empty() {
                                                                        format!("LIVE-{}", Utc::now().timestamp_millis())
                                                                    } else {
                                                                        response.order_id.clone()
                                                                    };
                                                                    add_trade(TradeRecord {
                                                                        id: order_id,
                                                                        market_id: market_id_str,
                                                                        market_slug: market_display.clone(),
                                                                        side: "Sell".to_string(),
                                                                        price: sell_price.to_string().parse().unwrap_or(0.0),
                                                                        order_price: sell_price.to_string().parse().unwrap_or(0.0),
                                                                        size: sell_size.to_f64().unwrap_or(0.0),
                                                                        timestamp: Utc::now().timestamp(),
                                                                        status: "Sold".to_string(),
                                                                        profit: None,
                                                                        buy_countdown: None,
                                                                        sell_countdown: Some(countdown_for_trade.clone()),
                                                                    });
                                                                    error!("{} | 价格反转，卖出操作成功 | 订单ID: {:?} | 卖出: {} | 数量: {:.2}", market_display, response.order_id, order_side_name, sell_size);
                                                                }
                                                                Err(e) => {
                                                                    error!("{} | 价格反转，卖出操作失败: {:?}", market_display, e);
                                                                }
                                                            }
                                                        } else {
                                                            error!("{} | 价格反转，执行模拟卖出操作 | 卖出: {} | 数量: {:.2} | 价格: {:.4}", market_display, order_side_name, sell_size, sell_price);
                                                            let mut order_status_map = order_status.lock().await;
                                                            order_status_map.insert(market_display.clone(), (false, "".to_string(), "".to_string(), "".to_string(), "".to_string(), "".to_string()));
                                                            use crate::utils::trade_history::{add_trade, TradeRecord};
                                                            use chrono::Utc;
                                                            let sim_order_id = format!("SIM-{}", Utc::now().timestamp_millis());
                                                            add_trade(TradeRecord {
                                                                id: sim_order_id,
                                                                market_id: market_id_str,
                                                                market_slug: market_display.clone(),
                                                                side: "Sell".to_string(),
                                                                price: sell_price.to_string().parse().unwrap_or(0.0),
                                                                order_price: sell_price.to_string().parse().unwrap_or(0.0),
                                                                size: sell_size.to_f64().unwrap_or(0.0),
                                                                timestamp: Utc::now().timestamp(),
                                                                status: "SimSold".to_string(),
                                                                profit: None,
                                                                buy_countdown: None,
                                                                sell_countdown: Some(countdown_for_trade.clone()),
                                                            });
                                                        }
                                                    }
                                                });
                                            }
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
                                        let price_greater_than_97 = price > dec!(0.97);

                                        
                                        let counter_val = yes_greater_than_no_counters.get(&market_id).map(|r| *r).unwrap_or(0);
                                        let price_greater_count = counter_val == 60;
                                        let default = (false, "".to_string(),"".to_string(),"".to_string(),"".to_string(),"".to_string());
                                        let order_status_lock = order_status.lock().await;
                                        let (is_ordered, order_side_name,order_token_id,unorder_token_id,ordered_size,order_price) = order_status_lock.get(&market_display).unwrap_or(&default).clone();

                                        //如果有订单,而且订单中购买的token这边价格卖价在0.97,对订单进行清仓
                                    if  price_greater_than_07 {
                                        error!("{} | 倒计时120秒内 | 计数: {} | Yes:A{:.4} No:A{:.4}", market_display, counter_val, yes_price, no_price);

                                        if price_greater_than_97 {//大于0.97的那侧
                                            info!("止盈策略执行,卖掉当前的订单,当前价格为{}", price);
                                            
                                            let is_running_clone = is_running.clone();
                                            let market_id_str = market_id.to_string();
                                            let countdown_for_trade = countdown_str.clone();
                                            let is_live = is_running_clone.load(Ordering::Relaxed);
                                                    if is_live {
                                                            if is_ordered {//如果已经建仓,价格发生了反转,则先卖后买
                                                            error!("{} | 价格大于0.97，已建仓，执行卖出操作", market_display);
                                                            let sell_token_id = U256::from_str(&order_token_id).unwrap_or(U256::ZERO);
                                                            let sell_size = ordered_size.parse::<Decimal>().unwrap_or(dec!(0));
                                                            let sell_price = Decimal::from_str(&order_price).unwrap_or(dec!(0));
                                                            
                                                            match executor.sell_at_price(sell_token_id, sell_price, sell_size).await {
                                                                Ok(response) => {
                                                                    let mut order_status_map = order_status.lock().await;
                                                                    order_status_map.insert(market_display.clone(), (false, "".to_string(), "".to_string(), "".to_string(), "".to_string(), "".to_string()));
                                                                    use crate::utils::trade_history::{add_trade, TradeRecord};
                                                                    use chrono::Utc;
                                                                    let order_id = if response.order_id.is_empty() {
                                                                        format!("LIVE-{}", Utc::now().timestamp_millis())
                                                                    } else {
                                                                        response.order_id.clone()
                                                                    };
                                                                    add_trade(TradeRecord {
                                                                        id: order_id,
                                                                        market_id: market_id_str,
                                                                        market_slug: market_display.clone(),
                                                                        side: "Sell".to_string(),
                                                                        price: sell_price.to_string().parse().unwrap_or(0.0),
                                                                        order_price: sell_price.to_string().parse().unwrap_or(0.0),
                                                                        size: sell_size.to_f64().unwrap_or(0.0),
                                                                        timestamp: Utc::now().timestamp(),
                                                                        status: "Sold".to_string(),
                                                                        profit: None,
                                                                        buy_countdown: None,
                                                                        sell_countdown: Some(countdown_for_trade.clone()),
                                                                    });
                                                                    error!("{} | 价格大于0.97，卖出操作成功 | 订单ID: {:?} | 卖出: {} | 数量: {:.2}", market_display, response.order_id, order_side_name, sell_size);
                                                                }
                                                                Err(e) => {
                                                                    error!("{} | 价格大于0.97，卖出操作失败: {:?}", market_display, e);
                                                                }
                                                            }
                                                            }
                                                        }
                                        }else{//大于0.7小于0.97的那侧

                                        if price_greater_count && price_greater_than_97 {
                                            let order_price_usd = calculate_order_price(&market_display, &lostcount, &wincount, &loststate);
                                            let order_size = (order_price_usd / price).round_dp(0).to_string().parse::<f64>().unwrap_or(0.0);
                                            let countdown_for_trade = countdown_str.clone();
                                            tokio::spawn({
                                                let executor = executor.clone();
                                                let market_display = market_display.clone();
                                                let side_name = side_name.to_string();
                                                let order_status = order_status.clone();
                                                let token_id = token_id;
                                                let low_token_id = low_token_id;
                                                let is_running_clone = is_running.clone();
                                                let mid = market_id;
                                                let counters = yes_greater_than_no_counters.clone();
                                                let order_size_str = order_size.clone();
                                                let order_price = order_price_usd;
                                                let countdown_for_trade = countdown_for_trade.clone();
                                                let market_id_str = market_id.to_string();
                                                let order_size_dec = order_size.to_string().parse::<Decimal>().unwrap_or(dec!(0));
                                                async move {
                                                    let is_live = is_running_clone.load(Ordering::Relaxed);
                                                    if is_live {
                                                        if is_ordered {//如果已经建仓,价格发生了反转,则先卖后买
                                                            info!("{} | 下单条件在180秒内，价格大于0.7，计数60次 已建仓,不购买了", market_display);    
                                                        }else{
                                                            match executor.buy_market_usd(token_id, price, order_price_usd).await {
                                                                Ok(response) => {
                                                                    order_status.lock().await.insert(market_display.clone(), (true, side_name.clone(),token_id.to_string(),low_token_id.to_string(),order_size_str.to_string(),price.to_string().into()));
                                                                    use crate::utils::trade_history::{add_trade, TradeRecord};
                                                                    use chrono::Utc;
                                                                    let order_id = if response.order_id.is_empty() {
                                                                        format!("LIVE-{}", Utc::now().timestamp_millis())
                                                                    } else {
                                                                        response.order_id.clone()
                                                                    };
                                                                    let size = (order_price_usd / price).to_f64().unwrap_or(0.0);
                                                                    add_trade(TradeRecord {
                                                                        id: order_id,
                                                                        market_id: market_id_str,
                                                                        market_slug: market_display.clone(),
                                                                        side: side_name.clone(),
                                                                        order_price: order_price_usd.to_string().parse().unwrap_or(0.0),
                                                                        price: price.to_string().parse().unwrap_or(0.0),
                                                                        size: size,
                                                                        timestamp: Utc::now().timestamp(),
                                                                        status: "Bought".to_string(),
                                                                        profit: None,
                                                                        buy_countdown: Some(countdown_for_trade.clone()),
                                                                        sell_countdown: None,
                                                                    });
                                                                    info!("{} | 下单条件在180秒内，价格大于0.7，计数60次，订单成功 | 订单ID: {:?} | 购买: {} | 金额: {:.2}美元", market_display, response.order_id, side_name, order_size);
                                                                }
                                                                Err(e) => {
                                                                    error!("{} | 下单条件在180秒内，价格大于0.7，计数60次，订单下单失败: {:?} | 购买: {}", market_display, e, side_name);
                                                                }
                                                            }
                                                        }
                                                    } else {
                                                          if is_ordered {//如果已经建仓,价格发生了反转,则先卖后买
                                                                info!("{} | 模拟下单条件在180秒内，价格大于0.7，计数60次 已建仓,不购买了", market_display);    
                                                          }else{
                                                            info!("{} | 模拟下单条件在180秒内，价格大于0.7，计数60次 | 购买: {} | 金额: {:.2}美元", market_display, side_name, order_size);
                                                            if let Some(mut c) = counters.get_mut(&mid) { *c = 0; }
                                                            order_status.lock().await.insert(market_display.clone(), (true, side_name.clone(),token_id.to_string(),low_token_id.to_string(),order_size_str.to_string(),price.to_string().into()));
                                                            use crate::utils::trade_history::{add_trade, TradeRecord};
                                                            use chrono::Utc;
                                                            let sim_order_id = format!("SIM-{}", Utc::now().timestamp_millis());
                                                            let size = (order_price_usd / price).to_f64().unwrap_or(0.0);
                                                            add_trade(TradeRecord {
                                                                id: sim_order_id,
                                                                market_id: market_id_str,
                                                                market_slug: market_display.clone(),
                                                                side: side_name.clone(),
                                                                order_price: order_price_usd.to_string().parse().unwrap_or(0.0),
                                                                price: price.to_string().parse().unwrap_or(0.0),
                                                                size: size,
                                                                timestamp: Utc::now().timestamp(),
                                                                status: "SimBought".to_string(),
                                                                profit: None,
                                                                buy_countdown: Some(countdown_for_trade.clone()),
                                                                sell_countdown: None,
                                                            });
                                                          }
                                                        
                                                    }
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
                                        // No赢，判断下单盈亏
                                        let order_status_lock = order_status.lock().await;
                                   
                                        if let Some((is_ordered, order_side_name, _, _, ordered_size, _)) = order_status_lock.get(&market_display) {
                                            // info!("xxxxxxxxxxxx111111");
                                            if *is_ordered {
                                                // info!("xxxxxxxxxxxx222222");
                                                let my_result = if order_side_name.contains("No") { "盈" } else { "亏" };
                                                // info!("No赢分支 | market: {} | order_side: {} | my_result: {}", market_display, order_side_name, my_result);
                                                if my_result == "盈" {
                                                    // info!("xxxxxxxxxxxx333333");

                                                //先进行市场id,在marketrecord查询,如果为true,就不更新记录,否则更新记录,并且将marketrecord中的值设为true
                                           
                                                        let current_wincount = wincount.get(&market_display).map(|v| *v).unwrap_or(0);
                                                             info!("{} | 订单状态: {:?}", market_display, order_status_lock);
                                                          info!("No赢-赢 | market: {} | old_wincount: {} | new_wincount: {}", market_display, current_wincount, current_wincount + 1);
                                                 if let Some(record) = marketrecord.get(&market_id) {
                                                    if *record {
                                                        continue;
                                                    }
                                                }else {
                                                    wincount.insert(market_display.clone(), current_wincount + 1);
                                                    lostcount.insert(market_display.clone(), 0);
                                                    marketrecord.insert(market_id.clone(), true);
                                                }
                                                    // 添加结算记录到交易历史
                                                    use crate::utils::trade_history::{add_trade, TradeRecord};
                                                    use chrono::Utc;
                                                    add_trade(TradeRecord {
                                                        id: format!("SETTLE-{}", Utc::now().timestamp_millis()),
                                                        market_id: market_id.to_string(),
                                                        market_slug: market_display.clone(),
                                                        side: "No赢-赢".to_string(),
                                                        order_price: 0.0,
                                                        price: 0.0,
                                                        size: 0.0,
                                                        timestamp: Utc::now().timestamp(),
                                                        status: "Won".to_string(),
                                                        profit: Some(1.0),
                                                        buy_countdown: None,
                                                        sell_countdown: None,
                                                    });
                                                

                                                }else{

                                         
                                                   let current_lostcount = lostcount.get(&market_display).map(|v| *v).unwrap_or(0);
                                                  info!("No赢-亏 | market: {} | old_lostcount: {}", market_display, current_lostcount);
                                                     if let Some(record) = marketrecord.get(&market_id) {
                                                    if *record {
                                                        continue;
                                                    }
                                                }else {
                                                  lostcount.insert(market_display.clone(), current_lostcount + 1);
                                                  wincount.insert(market_display.clone(), 0);
                                                  loststate.insert(market_display.clone(), current_lostcount + 1);
                                                    marketrecord.insert(market_id.clone(), true);
                                                }
                                                    // 添加结算记录到交易历史
                                                    use crate::utils::trade_history::{add_trade, TradeRecord};
                                                    use chrono::Utc;
                                                    add_trade(TradeRecord {
                                                        id: format!("SETTLE-{}", Utc::now().timestamp_millis()),
                                                        market_id: market_id.to_string(),
                                                        market_slug: market_display.clone(),
                                                        side: "No赢-亏".to_string(),
                                                        order_price: 0.0,
                                                        price: 0.0,
                                                        size: 0.0,
                                                        timestamp: Utc::now().timestamp(),
                                                        status: "Lost".to_string(),
                                                        profit: Some(-1.0),
                                                        buy_countdown: None,
                                                        sell_countdown: None,
                                                    });
                                             


                                                }
                                                let pnl = format!("(我{})", my_result);
                                                ("【No赢】".to_string(), pnl)
                                            } else {
                                                ("【No赢】".to_string(), String::new())
                                            }
                                        } else {
                                            ("【No赢】".to_string(), String::new())
                                        }
                                    }
                                    (None, Some(_)) => {
                                        // Yes赢，判断下单盈亏
                                        let order_status_lock = order_status.lock().await;
                                    
                                        if let Some((is_ordered, order_side_name, _, _, ordered_size, _)) = order_status_lock.get(&market_display) {
                                            // info!("xxxxxxxxxxxx444444");
                                            if *is_ordered  {
                                                // info!("xxxxxxxxxxxx555555");
                                                let my_result = if order_side_name.contains("Yes") { "盈" } else { "亏" };
                                                // info!("Yes赢分支 | market: {} | order_side: {} | my_result: {}", market_display, order_side_name, my_result);
                                                if my_result == "盈" {
                                                    // info!("xxxxxxxxxxxx666666");
                                          
                                                   let current_wincount = wincount.get(&market_display).map(|v| *v).unwrap_or(0);
                                                       info!("{} | 订单状态: {:?}", market_display, order_status_lock);
                                                  info!("Yes赢-赢 | market: {} | old_wincount: {} | new_wincount: {}", market_display, current_wincount, current_wincount + 1);
                                                        if let Some(record) = marketrecord.get(&market_id) {
                                                    if *record {
                                                        continue;
                                                    }
                                                }else {
                                                  wincount.insert(market_display.clone(), current_wincount + 1);
                                                  lostcount.insert(market_display.clone(), 0);

                                                    marketrecord.insert(market_id.clone(), true);
                                                }
                                                    // 添加结算记录到交易历史
                                                    use crate::utils::trade_history::{add_trade, TradeRecord};
                                                    use chrono::Utc;
                                                    add_trade(TradeRecord {
                                                        id: format!("SETTLE-{}", Utc::now().timestamp_millis()),
                                                        market_id: market_id.to_string(),
                                                        market_slug: market_display.clone(),
                                                        side: "Yes赢-赢".to_string(),
                                                        order_price: 0.0,
                                                        price: 0.0,
                                                        size: 0.0,
                                                        timestamp: Utc::now().timestamp(),
                                                        status: "Won".to_string(),
                                                        profit: Some(1.0),
                                                        buy_countdown: None,
                                                        sell_countdown: None,
                                                    });
                                                }else{
                                                    let current_lostcount = lostcount.get(&market_display).map(|v| *v).unwrap_or(0);
                                                  info!("Yes赢-亏 | market: {} | old_lostcount: {}", market_display, current_lostcount);
                                                   if let Some(record) = marketrecord.get(&market_id) {
                                                    if *record {
                                                        continue;
                                                    }
                                                }else {
                                                  lostcount.insert(market_display.clone(), current_lostcount + 1);
                                                  wincount.insert(market_display.clone(), 0);
                                                  loststate.insert(market_display.clone(), current_lostcount + 1);
                                                    marketrecord.insert(market_id.clone(), true);
                                                }
                                                    // 添加结算记录到交易历史
                                                    use crate::utils::trade_history::{add_trade, TradeRecord};
                                                    use chrono::Utc;
                                                    add_trade(TradeRecord {
                                                        id: format!("SETTLE-{}", Utc::now().timestamp_millis()),
                                                        market_id: market_id.to_string(),
                                                        market_slug: market_display.clone(),
                                                        side: "Yes赢-亏".to_string(),
                                                        order_price: 0.0,
                                                        price: 0.0,
                                                        size: 0.0,
                                                        timestamp: Utc::now().timestamp(),
                                                        status: "Lost".to_string(),
                                                        profit: Some(-1.0),
                                                        buy_countdown: None,
                                                        sell_countdown: None,
                                                    });
                                               

                                                }
                                                let pnl = format!("(我{})", my_result);
                                                ("【Yes赢】".to_string(), pnl)
                                            } else {
                                                ("【Yes赢】".to_string(), String::new())
                                            }
                                        } else {
                                            ("【Yes赢】".to_string(), String::new())
                                        }
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
                                
                                // info!("{} {} | {}分{:02}秒 | {} | {} | {}{}{}|{}",
                                //     prefix,
                                //     market_display,
                                //     countdown_minutes,
                                //     countdown_seconds,
                                //     yes_info,
                                //     no_info,
                                //     result_info,
                                //     pnl_info,
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
