mod config;
mod market;
mod monitor;
mod risk;
mod trading;
mod utils;
mod web_server;

use poly_5min_bot::merge;
use poly_5min_bot::positions::{get_positions, Position};

use anyhow::Result;
use dashmap::DashMap;
use futures::StreamExt;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use polymarket_client_sdk::types::{Address, B256, U256};

use crate::config::Config;
use crate::market::{MarketDiscoverer, MarketInfo, MarketScheduler};
use crate::monitor::{ArbitrageDetector, OrderBookMonitor};
use crate::risk::positions::PositionTracker;
use crate::risk::{HedgeMonitor, PositionBalancer, RiskManager};
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
            (indices.contains(&0) && indices.contains(&1)) || (indices.contains(&1) && indices.contains(&2))
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
        if wind_down_in_progress.load(Ordering::Relaxed) || countdown_in_progress.load(Ordering::Relaxed) {
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
                info!("本轮回 merge: 等待 30 秒后合并下一市场 (第 {}/{} 个)", i + 1, condition_ids.len());
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

    tracing::info!("Polymarket 5分钟套利机器人启动");

    // 许可证校验：须存在有效 license.key，删除许可证将无法运行
    poly_5min_bot::trial::check_license()?;

    // 加载配置
    let config = Config::from_env()?;
    tracing::info!("配置加载完成");

    // 初始化组件（暂时不使用，主循环已禁用）
    let _discoverer = MarketDiscoverer::new(config.crypto_symbols.clone());
    let _scheduler = MarketScheduler::new(_discoverer, config.market_refresh_advance_secs);
    let _detector = ArbitrageDetector::new(config.min_profit_threshold);
    
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
    ).await {
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
    use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
    use polymarket_client_sdk::clob::types::SignatureType;

    let signer_for_risk = LocalSigner::from_str(&config.private_key)?
        .with_chain_id(Some(POLYGON));
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
    
    // 创建对冲监测器（传入PositionTracker的Arc引用以更新风险敞口）
    // 对冲策略已暂时关闭，但保留hedge_monitor变量以备将来使用
    let position_tracker = _risk_manager.position_tracker();
    let hedge_monitor = Arc::new(HedgeMonitor::new(
        clob_client.clone(),
        config.private_key.clone(),
        config.proxy_address.clone(),
        position_tracker,
    ));

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
            
            if let Err(e) = crate::utils::balance_checker::check_balance_and_allowance(wallet_to_check).await {
                warn!("余额/授权检查失败（非致命错误）: {}", e);
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
    let is_running = Arc::new(AtomicBool::new(false)); // 默认为停止状态，等待网页启动
    let market_data = Arc::new(DashMap::new());
    let is_running_server = is_running.clone();
    let market_data_server = market_data.clone();
    tokio::spawn(async move {
        web_server::start_server(is_running_server, market_data_server).await;
    });

    info!("🌐 Web控制台已启动: http://localhost:3000");
    info!("⏸️ 等待在Web控制台点击启动...");

    // 等待启动信号
    while !is_running.load(Ordering::Relaxed) {
        sleep(Duration::from_millis(100)).await;
    }
    info!("▶️ Bot已启动运行！");


    // 创建仓位平衡器
    let position_balancer = Arc::new(PositionBalancer::new(
        clob_client.clone(),
        _risk_manager.position_tracker(),
        &config,
    ));

    // 定时持仓同步任务：每N秒从API获取最新持仓，覆盖本地缓存
    let position_sync_interval = config.position_sync_interval_secs;
    if position_sync_interval > 0 {
        let position_tracker_sync = _risk_manager.position_tracker();
        tokio::spawn(async move {
            let interval = Duration::from_secs(position_sync_interval);
            loop {
                match position_tracker_sync.sync_from_api().await {
                    Ok(_) => {
                        // 持仓信息已在 sync_from_api 中打印
                    }
                    Err(e) => {
                        warn!(error = %e, "持仓同步失败，将在下次循环重试");
                    }
                }
                sleep(interval).await;
            }
        });
        info!(
            interval_secs = position_sync_interval,
            "已启动定时持仓同步任务，每 {} 秒从API获取最新持仓覆盖本地缓存",
            position_sync_interval
        );
    } else {
        warn!("POSITION_SYNC_INTERVAL_SECS=0，持仓同步已禁用");
    }

    // 定时仓位平衡任务：每N秒检查持仓和挂单，取消多余挂单
    // 注意：由于需要市场映射，平衡任务将在主循环中调用
    let balance_interval = config.position_balance_interval_secs;
    if balance_interval > 0 {
        info!(
            interval_secs = balance_interval,
            "仓位平衡任务将在主循环中每 {} 秒执行一次",
            balance_interval
        );
    } else {
        info!("定时仓位平衡未启用（POSITION_BALANCE_INTERVAL_SECS=0）");
    }

    // 收尾进行中标志：定时 merge 会检查并跳过，避免与收尾 merge 竞争
    let wind_down_in_progress = Arc::new(AtomicBool::new(false));
    let countdown_in_progress = Arc::new(AtomicBool::new(false));

    // 两次套利交易之间的最小间隔
    const MIN_TRADE_INTERVAL: Duration = Duration::from_secs(3);
    let last_trade_time: Arc<tokio::sync::Mutex<Option<Instant>>> = Arc::new(tokio::sync::Mutex::new(None));

    // 定时 Merge：每 N 分钟根据持仓执行 merge，仅对 YES+NO 双边都持仓的市场
    let merge_interval = config.merge_interval_minutes;
    if merge_interval > 0 {
        if let Some(proxy) = config.proxy_address {
            let private_key = config.private_key.clone();
            let position_tracker = _risk_manager.position_tracker().clone();
            let wind_down_flag = wind_down_in_progress.clone();
                let countdown_flag_merge = countdown_in_progress.clone();
                tokio::spawn(async move {
                    run_merge_task(merge_interval, proxy, private_key, position_tracker, wind_down_flag, countdown_flag_merge).await;
                });
            info!(
                interval_minutes = merge_interval,
                "已启动定时 Merge 任务，每 {} 分钟根据持仓执行（仅 YES+NO 双边）",
                merge_interval
            );
        } else {
            warn!("MERGE_INTERVAL_MINUTES={} 但未设置 POLYMARKET_PROXY_ADDRESS，定时 Merge 已禁用", merge_interval);
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
        use chrono::Utc;
        use crate::market::discoverer::FIVE_MIN_SECS;
        let current_window_timestamp = MarketDiscoverer::calculate_current_window_timestamp(Utc::now());
        let window_end = chrono::DateTime::from_timestamp(current_window_timestamp + FIVE_MIN_SECS, 0)
            .unwrap_or_else(|| Utc::now());
        let mut wind_down_done = false;
        let mut post_end_claim_done = false;

        // 创建市场ID到市场信息的映射
        let market_map: HashMap<B256, &MarketInfo> = markets.iter()
            .map(|m| (m.market_id, m))
            .collect();

        // 创建市场映射（condition_id -> (yes_token_id, no_token_id)）用于仓位平衡
        let market_token_map: HashMap<B256, (U256, U256)> = markets.iter()
            .map(|m| (m.market_id, (m.yes_token_id, m.no_token_id)))
            .collect();

        // 创建定时仓位平衡定时器
        let balance_interval = config.position_balance_interval_secs;
        let mut balance_timer = if balance_interval > 0 {
            let mut timer = tokio::time::interval(Duration::from_secs(balance_interval));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            timer.tick().await; // 立即触发第一次
            Some(timer)
        } else {
            None
        };

        // 按市场记录上一拍卖一价，用于计算涨跌方向（仅一次 HashMap 读写，不影响监控性能）
        let last_prices: DashMap<B256, (Decimal, Decimal)> = DashMap::new();
        // 倒计时策略：每个市场仅投注一次的标记，记录 (token_id, entry_price, is_active)
        let one_dollar_attempted: DashMap<B256, (U256, Decimal, bool)> = DashMap::new();

        // 监控订单簿更新
        loop {
            let now_all = Utc::now();
            let seconds_until_end_all = (window_end - now_all).num_seconds();
            if seconds_until_end_all <= 0 && !post_end_claim_done {
                post_end_claim_done = true;
                let config_claim = config.clone();
                tokio::spawn(async move {
                    if let Some(proxy) = config_claim.proxy_address {
                        match get_positions().await {
                            Ok(positions) => {
                                let condition_ids = condition_ids_with_both_sides(&positions);
                                let n = condition_ids.len();
                                for (i, condition_id) in condition_ids.iter().enumerate() {
                                    match merge::merge_max(*condition_id, proxy, &config_claim.private_key, None).await {
                                        Ok(tx) => {
                                            info!("🎁 自动领取：Merge 完成 | condition_id={:#x} | tx={}", condition_id, tx);
                                        }
                                        Err(e) => {
                                            warn!(condition_id = %condition_id, error = %e, "自动领取：Merge 失败");
                                        }
                                    }
                                    if i + 1 < n {
                                        sleep(Duration::from_secs(30)).await;
                                    }
                                }
                            }
                            Err(e) => { warn!(error = %e, "自动领取：获取持仓失败，跳过"); }
                        }
                    } else {
                        warn!("自动领取：未配置 POLYMARKET_PROXY_ADDRESS，跳过");
                    }
                });
            }
            // 收尾检查：距窗口结束 <= N 分钟时执行一次收尾（不跳出，继续监控直到窗口结束由下方「新窗口检测」自然切换）
            // 使用秒级精度，5分钟窗口下 num_minutes() 截断可能导致漏检
            if config.wind_down_before_window_end_minutes > 0 && !wind_down_done {
                let now = Utc::now();
                let seconds_until_end = (window_end - now).num_seconds();
                let threshold_seconds = config.wind_down_before_window_end_minutes as i64 * 60;
                
                // 如果时间到了，或者已经过期（比如窗口已经结束），都应该触发收尾
                // 注意：如果窗口已经结束(seconds_until_end <= 0)，也应该执行收尾
                if seconds_until_end <= threshold_seconds {
                    info!("🛑 触发收尾 | 距窗口结束 {} 秒", seconds_until_end);
                    wind_down_done = true;
                    wind_down_in_progress.store(true, Ordering::Relaxed);

                    // 收尾在独立任务中执行，不阻塞订单簿；各市场 merge 之间间隔 30 秒
                    let executor_wd = executor.clone();
                    let config_wd = config.clone();
                    let risk_manager_wd = _risk_manager.clone();
                    let wind_down_flag = wind_down_in_progress.clone();
                    
                    // 克隆 one_dollar_attempted 以便在收尾时清理倒计时策略的持仓
                    // 注意：DashMap本身是并发安全的，但这里我们需要它的引用，而 tokio::spawn 需要 'static
                    // 所以我们需要将 one_dollar_attempted 包装在 Arc 中，或者在 main 函数开始时就用 Arc<DashMap>
                    // 由于目前 one_dollar_attempted 是局部变量，我们无法直接传给 'static 任务
                    // 临时的解决方案：我们在收尾任务中不直接操作 one_dollar_attempted，
                    // 而是通过 get_positions() 获取所有持仓并卖出，这自然涵盖了倒计时策略的持仓。
                    // 下面的代码已经包含了 "3. 市价卖出剩余单腿持仓"，这应该已经满足了需求。
                    
                    tokio::spawn(async move {
                        const MERGE_INTERVAL: Duration = Duration::from_secs(30);

                        // 1. 取消所有挂单
                        if let Err(e) = executor_wd.cancel_all_orders().await {
                            warn!(error = %e, "收尾：取消所有挂单失败，继续执行 Merge 与卖出");
                        } else {
                            info!("✅ 收尾：已取消所有挂单");
                        }

                        // 取消后等 10 秒再 Merge，避免取消前刚成交的订单尚未上链更新持仓
                        const DELAY_AFTER_CANCEL: Duration = Duration::from_secs(10);
                        sleep(DELAY_AFTER_CANCEL).await;

                        // 2. Merge 双边持仓（每完成一个市场后等 30 秒再合并下一个）并更新敞口
                        let position_tracker = risk_manager_wd.position_tracker();
                        let mut did_any_merge = false;
                        if let Some(proxy) = config_wd.proxy_address {
                            match get_positions().await {
                                Ok(positions) => {
                                    let condition_ids = condition_ids_with_both_sides(&positions);
                                    let merge_info = merge_info_with_both_sides(&positions);
                                    let n = condition_ids.len();
                                    for (i, condition_id) in condition_ids.iter().enumerate() {
                                        match merge::merge_max(*condition_id, proxy, &config_wd.private_key, None).await {
                                            Ok(tx) => {
                                                did_any_merge = true;
                                                info!("✅ 收尾：Merge 完成 | condition_id={:#x} | tx={}", condition_id, tx);
                                                if let Some((yes_token, no_token, merge_amt)) = merge_info.get(condition_id) {
                                                    position_tracker.update_exposure_cost(*yes_token, dec!(0), -*merge_amt);
                                                    position_tracker.update_exposure_cost(*no_token, dec!(0), -*merge_amt);
                                                    position_tracker.update_position(*yes_token, -*merge_amt);
                                                    position_tracker.update_position(*no_token, -*merge_amt);
                                                    info!("💰 收尾：Merge 已扣减敞口 | condition_id={:#x} | 数量:{}", condition_id, merge_amt);
                                                }
                                            }
                                            Err(e) => {
                                                warn!(condition_id = %condition_id, error = %e, "收尾：Merge 失败");
                                            }
                                        }
                                        // 每完成一个市场的 merge 后等 30 秒再处理下一个，给链上时间
                                        if i + 1 < n {
                                            info!("收尾：等待 30 秒后合并下一市场");
                                            sleep(MERGE_INTERVAL).await;
                                        }
                                    }
                                }
                                Err(e) => { warn!(error = %e, "收尾：获取持仓失败，跳过 Merge"); }
                            }
                        } else {
                            warn!("收尾：未配置 POLYMARKET_PROXY_ADDRESS，跳过 Merge");
                        }

                        // 若有执行过 Merge，等半分钟再卖出单腿，给链上处理时间；无 Merge 则不等
                        if did_any_merge {
                            sleep(MERGE_INTERVAL).await;
                        }

                        // 3. 市价卖出剩余单腿持仓（这也将覆盖倒计时策略建立的持仓）
                        // 倒计时策略在窗口结束前5-10秒建立持仓，而收尾逻辑通常在窗口结束前执行（如果配置了 wind_down_before_window_end_minutes）
                        // 如果 wind_down_before_window_end_minutes 设置为0，则需要在窗口切换时执行卖出。
                        // 当前代码是在 wind_down_before_window_end_minutes > 0 时触发。
                        // 如果用户希望在"市场结束后"卖出，意味着需要在窗口时间结束后。
                        // 但实际上，市场结束后（窗口结束后），Gamma API可能不再返回该市场，或者市场已经结算。
                        // 如果是结算，则不需要卖出，等待结算即可（Winning side 兑换 $1，Losing side 归零）。
                        // 用户的需求可能是：在市场即将结束前（比如最后几秒或者刚结束时）卖出，以避免进入结算流程（结算可能需要时间且有不确定性，或者为了快速回笼资金）。
                        // 或者用户是指：倒计时策略博的是最后几秒的波动，如果没赢（或者无论输赢），都在最后时刻平仓。
                        
                        // 既然用户说"市场结束后，进行卖掉"，考虑到Polymarket的机制，市场结束后进入结算。
                        // 如果是指"倒计时结束（即窗口结束）后"，那么此时往往无法交易了（市场关闭）。
                        // 所以最合理的解释是：在市场即将结束的最后时刻（收尾阶段），把倒计时策略买入的仓位也卖掉。
                        // 现有的收尾逻辑（步骤3）已经会卖出所有剩余的单腿持仓。
                        // 我们只需要确保收尾逻辑被正确执行，并且卖出价格合适。
                        
                        let wind_down_sell_price = Decimal::try_from(config_wd.wind_down_sell_price).unwrap_or(dec!(0.01));
                        info!("🧹 收尾：开始卖出所有剩余持仓（包括倒计时策略持仓） | 价格:{:.4}", wind_down_sell_price);
                        
                        // 循环尝试卖出，最多重试 3 次，每次获取最新持仓
                        for retry in 0..3 {
                            if retry > 0 {
                                info!("收尾：第 {} 次重试卖出持仓...", retry);
                                sleep(Duration::from_secs(2)).await;
                            }
                            match get_positions().await {
                                Ok(positions) => {
                                    let active_positions: Vec<_> = positions.iter().filter(|p| p.size > dec!(0.01)).collect();
                                    if active_positions.is_empty() {
                                        info!("✅ 收尾：当前无剩余持仓");
                                        break;
                                    }

                                    for pos in active_positions {
                                        let size_floor = (pos.size * dec!(100)).floor() / dec!(100);
                                        if size_floor < dec!(0.01) {
                                            debug!(token_id = %pos.asset, size = %pos.size, "收尾：持仓过小，跳过卖出");
                                            continue;
                                        }
                                        // 尝试以 wind_down_sell_price 卖出
                                        // 注意：如果市场价格高于 wind_down_sell_price，会以市价成交（更好）
                                        // 如果市场价格低于 wind_down_sell_price，则会挂单（可能无法成交）
                                        // 为了确保卖出，wind_down_sell_price 应该设置得足够低（如 0.01 或 0.05）
                                        if let Err(e) = executor_wd.sell_at_price(pos.asset, wind_down_sell_price, size_floor).await {
                                            warn!(token_id = %pos.asset, size = %pos.size, error = %e, "收尾：卖出单腿失败");
                                        } else {
                                            info!("✅ 收尾：已下卖单 | token_id={:#x} | 数量:{} | 价格:{:.4}", pos.asset, size_floor, wind_down_sell_price);
                                            
                                            // 更新本地持仓追踪（虽然之后会重置，但为了日志准确性）
                                            position_tracker.update_position(pos.asset, -size_floor);
                                        }
                                    }
                                }
                                Err(e) => { warn!(error = %e, "收尾：获取持仓失败，跳过卖出"); }
                            }
                        }

                        info!("🛑 收尾完成，继续监控至窗口结束");
                        wind_down_flag.store(false, Ordering::Relaxed);
                    });
                }
            }

            tokio::select! {
                // 处理订单簿更新
                book_result = stream.next() => {
                    // 检查 Web 控制台停止信号
                    if !is_running.load(Ordering::Relaxed) {
                        info!("⏸️ 收到停止信号，Bot 已暂停运行...");
                        // 取消所有挂单
                        if let Err(e) = executor.cancel_all_orders().await {
                            warn!("暂停时取消挂单失败: {}", e);
                        }
                        // 等待重新启动
                        while !is_running.load(Ordering::Relaxed) {
                            sleep(Duration::from_millis(500)).await;
                        }
                        info!("▶️ Bot 已恢复运行！");
                        // 重新初始化某些状态可能需要（视情况而定），这里简单继续循环
                        // continue; // 在 select! 中 continue 会跳过本次处理，但我们需要重新进入 loop
                    }

                    match book_result {
                        Some(Ok(book)) => {
                            // 检查止盈止损（使用 clone 的 book）
                            let book_clone = book.clone();
                            let monitor_clone = hedge_monitor.clone();
                            tokio::spawn(async move {
                                if let Err(e) = monitor_clone.check_and_execute(&book_clone).await {
                                    warn!("HedgeMonitor check error: {}", e);
                                }
                            });

                            // 然后处理订单簿更新（book会被move）
                            if let Some(pair) = monitor.handle_book_update(book) {
                                // 注意：asks 最后一个为卖一价
                                let yes_best_ask = pair.yes_book.asks.last().map(|a| (a.price, a.size));
                                let no_best_ask = pair.no_book.asks.last().map(|a| (a.price, a.size));
                                let total_ask_price = yes_best_ask.and_then(|(p, _)| no_best_ask.map(|(np, _)| p + np));

                                let market_id = pair.market_id;
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
                                let now_countdown = Utc::now();
                                let sec_to_end = (window_end - now_countdown).num_seconds();
            let countdown_active = sec_to_end <= config.countdown_window_max_sec && sec_to_end >= config.countdown_window_min_sec;
            countdown_in_progress.store(countdown_active, Ordering::Relaxed);
                                let sec_to_end_nonneg = sec_to_end.max(0);
                                let countdown_minutes = sec_to_end_nonneg / 60;
                                let countdown_seconds = sec_to_end_nonneg % 60;

                                // 计算订单总量（Ask + Bid）
                                let yes_ask_vol: Decimal = pair.yes_book.asks.iter().map(|o| o.size).sum();
                                let yes_bid_vol: Decimal = pair.yes_book.bids.iter().map(|o| o.size).sum();
                                let yes_total_vol = yes_ask_vol + yes_bid_vol;

                                let no_ask_vol: Decimal = pair.no_book.asks.iter().map(|o| o.size).sum();
                                let no_bid_vol: Decimal = pair.no_book.bids.iter().map(|o| o.size).sum();
                                let no_total_vol = no_ask_vol + no_bid_vol;

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
                                        countdown: format!("{:02}:{:02}", countdown_minutes, countdown_seconds),
                                        yes_price: yes_f64,
                                        no_price: no_f64,
                                        sum: sum_val,
                                        diff: diff_val,
                                        update_time: Utc::now().timestamp(),
                                    };
                                    market_data.insert(market_id.to_string(), entry);
                                }

                                // 倒计时策略：在距窗口结束 10-5 秒之间，下注较大一边 $1（数量=1/单价，向下取两位），仅投注一次；若任一侧价格>=0.99则跳过
                                {
                                    /*
                                    // 检查止损：如果已有持仓且亏损超过 10%
                                    // 已移除：现在统一使用 HedgeMonitor 进行止盈止损监控
                                    */

                                    if countdown_active {
                                        if !one_dollar_attempted.contains_key(&market_id) {
                                            if let (Some((y_price, _)), Some((n_price, _))) = (yes_best_ask, no_best_ask) {
                                                let max_price = Decimal::try_from(config.countdown_max_price).unwrap_or(dec!(0.99));
                                                let min_price = Decimal::try_from(config.countdown_min_price).unwrap_or(dec!(0.0));
                                                
                                                if y_price >= max_price || n_price >= max_price {
                                                    debug!("⏸️ 价格>={:.2}，倒计时策略跳过 | 市场:{}", max_price, market_display);
                                                    // 不标记已尝试，允许重试
                                                } else {
                                                    // 选择价格较大的一边
                                                    let (chosen_token, chosen_price, side_str, is_yes) = if y_price >= n_price {
                                                        (pair.yes_book.asset_id, y_price, "YES", true)
                                                    } else {
                                                        (pair.no_book.asset_id, n_price, "NO", false)
                                                    };
                                                    
                                                    let opposite_token = if is_yes {
                                                        pair.no_book.asset_id
                                                    } else {
                                                        pair.yes_book.asset_id
                                                    };
                                                    
                                                    // 检查是否低于最小价格阈值
                                                    if chosen_price < min_price {
                                                        debug!("⏸️ 价格<{:.2}，倒计时策略跳过 | 市场:{} | 价格:{:.4}", min_price, market_display, chosen_price);
                                                        // 不标记已尝试，允许重试
                                                    } else {
                                                        // 检查数量是否也偏大（量价齐升）
                                                        let volume_condition_met = if is_yes {
                                                            yes_total_vol > no_total_vol
                                                        } else {
                                                            no_total_vol > yes_total_vol
                                                        };

                                                        if !volume_condition_met {
                                                            debug!("⏸️ 量价不匹配（价格大但数量小），倒计时策略跳过 | 市场:{} | 方向:{} | YesVol:{:.0} | NoVol:{:.0}", 
                                                                market_display, side_str, yes_total_vol, no_total_vol);
                                                            // 不标记已尝试，允许重试
                                                        } else {
                                                            let mut qty = (dec!(1.0) / chosen_price) * dec!(100.0);
                                                            qty = qty.floor() / dec!(100.0);
                                                            if qty < dec!(5.0) {
                                                                qty = dec!(5.0);
                                                            }

                                                            one_dollar_attempted.insert(market_id, (chosen_token, chosen_price, true));
                                                            info!("⏱️ 倒计时策略下单 | 市场:{} | 方向:{} | 价格:{:.4} | 份额:{:.2} | YesVol:{:.0} | NoVol:{:.0}", 
                                                                market_display, side_str, chosen_price, qty, yes_total_vol, no_total_vol);
                                                            let executor_clone = executor.clone();
                                                            let pt = _risk_manager.position_tracker();
                                                            let market_id_str = market_id.to_string();
                                                            let market_display_str = market_display.clone();
                                                            let side_string = side_str.to_string();
                                                            use rust_decimal::prelude::ToPrimitive;
                                                            let price_f64 = chosen_price.to_f64().unwrap_or(0.0);
                                                            let size_f64 = qty.to_f64().unwrap_or(0.0);

                                                            let hedge_monitor_clone = hedge_monitor.clone();
                                                            let tp_pct = Decimal::try_from(config.hedge_take_profit_pct).unwrap_or(dec!(0.05));
                                                            let sl_pct = Decimal::try_from(config.hedge_stop_loss_pct).unwrap_or(dec!(0.05));
                                                            let opposite_token_id = opposite_token;

                                                            tokio::spawn(async move {
                                                                match executor_clone.buy_at_price(chosen_token, chosen_price, qty).await {
                                                                    Ok(resp) => {
                                                                        info!("✅ 倒计时策略下单成功 | order_id={}", resp.order_id);
                                                                        pt.update_exposure_cost(chosen_token, chosen_price, qty);
                                                                        pt.update_position(chosen_token, qty);
                                                                        
                                                                        // Add to hedge monitor
                                                                        use crate::risk::recovery::RecoveryAction;
                                                                        let action = RecoveryAction::MonitorForExit {
                                                                            token_id: chosen_token,
                                                                            opposite_token_id: opposite_token_id,
                                                                            amount: qty,
                                                                            entry_price: chosen_price,
                                                                            take_profit_pct: tp_pct,
                                                                            stop_loss_pct: sl_pct,
                                                                            pair_id: market_id_str.clone(),
                                                                            market_display: market_display_str.clone(),
                                                                        };
                                                                        if let Err(e) = hedge_monitor_clone.add_position(&action) {
                                                                            warn!("❌ 添加对冲监控失败: {}", e);
                                                                        } else {
                                                                            info!("🛡️ 已添加对冲监控 | 止盈:{:.2}% | 止损:{:.2}%", tp_pct * dec!(100), sl_pct * dec!(100));
                                                                        }
                                                                        
                                                                        // 记录交易历史
                                                                        use crate::utils::trade_history::{add_trade, TradeRecord};
                                                                        use chrono::Utc;
                                                                        
                                                                        add_trade(TradeRecord {
                                                                            id: resp.order_id,
                                                                            market_id: market_id_str,
                                                                            market_slug: market_display_str,
                                                                            side: side_string,
                                                                            price: price_f64,
                                                                            size: size_f64,
                                                                            timestamp: Utc::now().timestamp(),
                                                                            status: "Pending".to_string(),
                                                                            profit: None,
                                                                        });
                                                                    }
                                                                    Err(e) => {
                                                                        warn!("❌ 倒计时策略下单失败: {}", e);
                                                                    }
                                                                }
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    }
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
                                let yes_info = yes_best_ask
                                    .map(|(p, _s)| {
                                        if is_arbitrage && !yes_dir.is_empty() {
                                            format!("Yes:{:.4} {}", p, yes_dir)
                                        } else {
                                            format!("Yes:{:.4}", p)
                                        }
                                    })
                                    .unwrap_or_else(|| "Yes:无".to_string());
                                let no_info = no_best_ask
                                    .map(|(p, _s)| {
                                        if is_arbitrage && !no_dir.is_empty() {
                                            format!("No:{:.4} {}", p, no_dir)
                                        } else {
                                            format!("No:{:.4}", p)
                                        }
                                    })
                                    .unwrap_or_else(|| "No:无".to_string());

                                info!(
                                    "{} {} | {}分{:02}秒 | {} | {} | {}",
                                    prefix,
                                    market_display,
                                    countdown_minutes,
                                    countdown_seconds,
                                    yes_info,
                                    no_info,
                                    spread_info
                                );

                                if countdown_active {
                                    continue;
                                }
                                
                                // 保留原有的结构化日志用于调试（可选）
                                debug!(
                                    market_id = %pair.market_id,
                                    yes_token = %pair.yes_book.asset_id,
                                    no_token = %pair.no_book.asset_id,
                                    "订单簿对详细信息"
                                );
                                
                                // 暂时禁用常规套利策略，仅保留倒计时策略
                                continue;

                                // 检测套利机会（监控阶段：只有当总价 <= 1 - 套利执行价差 时才执行套利）
                                use rust_decimal::Decimal;
                                let execution_threshold = dec!(1.0) - Decimal::try_from(config.arbitrage_execution_spread)
                                    .unwrap_or(dec!(0.01));
                                if let Some(total_price) = total_ask_price {
                                    if total_price <= execution_threshold {
                                        if let Some(opp) = _detector.check_arbitrage(
                                            &pair.yes_book,
                                            &pair.no_book,
                                            &pair.market_id,
                                        ) {
                                            // 检查 YES 价格是否达到阈值
                                            if config.min_yes_price_threshold > 0.0 {
                                                use rust_decimal::Decimal;
                                                let min_yes_price_decimal = Decimal::try_from(config.min_yes_price_threshold)
                                                    .unwrap_or(dec!(0.0));
                                                if opp.yes_ask_price < min_yes_price_decimal {
                                                    debug!(
                                                        "⏸️ YES价格未达到阈值，跳过套利执行 | 市场:{} | YES价格:{:.4} | 阈值:{:.4}",
                                                        market_display,
                                                        opp.yes_ask_price,
                                                        config.min_yes_price_threshold
                                                    );
                                                    continue; // 跳过这个套利机会
                                                }
                                            }
                                            
                                            // 检查 NO 价格是否达到阈值
                                            if config.min_no_price_threshold > 0.0 {
                                                use rust_decimal::Decimal;
                                                let min_no_price_decimal = Decimal::try_from(config.min_no_price_threshold)
                                                    .unwrap_or(dec!(0.0));
                                                if opp.no_ask_price < min_no_price_decimal {
                                                    debug!(
                                                        "⏸️ NO价格未达到阈值，跳过套利执行 | 市场:{} | NO价格:{:.4} | 阈值:{:.4}",
                                                        market_display,
                                                        opp.no_ask_price,
                                                        config.min_no_price_threshold
                                                    );
                                                    continue; // 跳过这个套利机会
                                                }
                                            }
                                            
                                            // 检查是否接近市场结束时间（如果配置了停止时间）
                                            // 使用秒级精度，5分钟市场下 num_minutes() 截断可能导致漏检
                                            if config.stop_arbitrage_before_end_minutes > 0 {
                                                if let Some(market_info) = market_map.get(&pair.market_id) {
                                                    use chrono::Utc;
                                                    let now = Utc::now();
                                                    let time_until_end = market_info.end_date.signed_duration_since(now);
                                                    let seconds_until_end = time_until_end.num_seconds();
                                                    let threshold_seconds = config.stop_arbitrage_before_end_minutes as i64 * 60;
                                                    
                                                    if seconds_until_end <= threshold_seconds {
                                                        debug!(
                                                            "⏰ 接近市场结束时间，跳过套利执行 | 市场:{} | 距离结束:{}秒 | 停止阈值:{}分钟",
                                                            market_display,
                                                            seconds_until_end,
                                                            config.stop_arbitrage_before_end_minutes
                                                        );
                                                        continue; // 跳过这个套利机会
                                                    }
                                                }
                                            }

                                            // 新增逻辑：倒数1分钟价格差值检查
                                            // 在倒数60秒到31秒之间进行计算，只要小于0.15，则当前不投注
                                            if let Some(market_info) = market_map.get(&pair.market_id) {
                                                use chrono::Utc;
                                                let now = Utc::now();
                                                let time_until_end = market_info.end_date.signed_duration_since(now);
                                                let seconds_until_end = time_until_end.num_seconds();
                                                
                                                // 如果剩余时间在 [31, 60] 秒之间
                                                if seconds_until_end >= 31 && seconds_until_end <= 60 {
                                                    use rust_decimal::Decimal;
                                                    // 计算价格差值：大值 - 小值 (abs(yes - no))
                                                    let price_diff = (opp.yes_ask_price - opp.no_ask_price).abs();
                                                    
                                                    let diff_threshold = dec!(0.15);
                                                    if price_diff < diff_threshold {
                                                        info!(
                                                            "🛑 倒数[31-60]秒内价格差值过小，跳过套利 | 市场:{} | 剩余:{}秒 | Yes:{:.4} | No:{:.4} | 差值:{:.4} | 阈值:{:.4}",
                                                            market_display,
                                                            seconds_until_end,
                                                            opp.yes_ask_price,
                                                            opp.no_ask_price,
                                                            price_diff,
                                                            diff_threshold
                                                        );
                                                        continue; // 跳过这个套利机会
                                                    }
                                                }
                                            }
                                            
                                            // 计算订单成本（USD）
                                            // 使用套利机会中的实际可用数量，但不超过配置的最大订单大小
                                            use rust_decimal::Decimal;
                                            let max_order_size = Decimal::try_from(config.max_order_size_usdc).unwrap_or(dec!(100.0));
                                            let order_size = opp.yes_size.min(opp.no_size).min(max_order_size);
                                            let yes_cost = opp.yes_ask_price * order_size;
                                            let no_cost = opp.no_ask_price * order_size;
                                            let total_cost = yes_cost + no_cost;
                                            
                                            // 检查风险敞口限制
                                            let position_tracker = _risk_manager.position_tracker();
                                            let current_exposure = position_tracker.calculate_exposure();
                                            
                                            if position_tracker.would_exceed_limit(yes_cost, no_cost) {
                                                warn!(
                                                    "⚠️ 风险敞口超限，拒绝执行套利交易 | 市场:{} | 当前敞口:{:.2} USD | 订单成本:{:.2} USD | 限制:{:.2} USD",
                                                    market_display,
                                                    current_exposure,
                                                    total_cost,
                                                    position_tracker.max_exposure()
                                                );
                                                continue; // 跳过这个套利机会
                                            }
                                            
                                            // 检查持仓平衡（使用本地缓存，零延迟）
                                            if position_balancer.should_skip_arbitrage(opp.yes_token_id, opp.no_token_id) {
                                                warn!(
                                                    "⚠️ 持仓已严重不平衡，跳过套利执行 | 市场:{}",
                                                    market_display
                                                );
                                                continue; // 跳过这个套利机会
                                            }
                                            
                                            // 检查交易间隔：两次交易间隔不少于 3 秒
                                            {
                                                let mut guard = last_trade_time.lock().await;
                                                let now = Instant::now();
                                                if let Some(last) = *guard {
                                                    if now.saturating_duration_since(last) < MIN_TRADE_INTERVAL {
                                                        let elapsed = now.saturating_duration_since(last).as_secs_f32();
                                                        debug!(
                                                            "⏱️ 交易间隔不足 3 秒，跳过 | 市场:{} | 距上次:{}秒",
                                                            market_display,
                                                            elapsed
                                                        );
                                                        continue; // 跳过此套利机会
                                                    }
                                                }
                                                *guard = Some(now);
                                            }

                                            info!(
                                                "⚡ 执行套利交易 | 市场:{} | 利润:{:.2}% | 下单数量:{}份 | 订单成本:{:.2} USD | 当前敞口:{:.2} USD",
                                                market_display,
                                                opp.profit_percentage,
                                                order_size,
                                                total_cost,
                                                current_exposure
                                            );
                                            // 简化敞口：只要执行套利就增加敞口，不管是否成交
                                            let _pt = _risk_manager.position_tracker();
                                            _pt.update_exposure_cost(opp.yes_token_id, opp.yes_ask_price, order_size);
                                            _pt.update_exposure_cost(opp.no_token_id, opp.no_ask_price, order_size);
                                            
                                            // 套利执行：只要总价 <= 阈值即执行，不因涨跌组合跳过；涨跌仅用于滑点分配（仅下降=second，上涨与持平=first）
                                            // 克隆需要的变量到独立任务中（涨跌方向用于按方向分配滑点）
                                            let executor_clone = executor.clone();
                                            let risk_manager_clone = _risk_manager.clone();
                                            let opp_clone = opp.clone();
                                            let yes_dir_s = yes_dir.to_string();
                                            let no_dir_s = no_dir.to_string();
                                            
                                            // 使用 tokio::spawn 异步执行套利交易，不阻塞订单簿更新处理
                                            tokio::spawn(async move {
                                                // 执行套利交易（滑点：仅下降=second，上涨与持平=first）
                                                match executor_clone.execute_arbitrage_pair(&opp_clone, &yes_dir_s, &no_dir_s).await {
                                                    Ok(result) => {
                                                        // 先保存 pair_id，因为 result 会被移动
                                                        let pair_id = result.pair_id.clone();
                                                        
                                                        // 注册到风险管理器（传入价格信息以计算风险敞口）
                                                        risk_manager_clone.register_order_pair(
                                                            result,
                                                            opp_clone.market_id,
                                                            opp_clone.yes_token_id,
                                                            opp_clone.no_token_id,
                                                            opp_clone.yes_ask_price,
                                                            opp_clone.no_ask_price,
                                                        );

                                                        // 处理风险恢复
                                                        // 对冲策略已暂时关闭，买进单边不做任何处理
                                                        match risk_manager_clone.handle_order_pair(&pair_id).await {
                                                            Ok(action) => {
                                                                // 对冲策略已关闭，不再处理MonitorForExit和SellExcess
                                                                match action {
                                                                    crate::risk::recovery::RecoveryAction::None => {
                                                                        // 正常情况，无需处理
                                                                    }
                                                                    crate::risk::recovery::RecoveryAction::MonitorForExit { .. } => {
                                                                        info!("单边成交，但对冲策略已关闭，不做处理");
                                                                    }
                                                                    crate::risk::recovery::RecoveryAction::SellExcess { .. } => {
                                                                        info!("部分成交不平衡，但对冲策略已关闭，不做处理");
                                                                    }
                                                                    crate::risk::recovery::RecoveryAction::ManualIntervention { reason } => {
                                                                        warn!("需要手动干预: {}", reason);
                                                                    }
                                                                }
                                                            }
                                                            Err(e) => {
                                                                error!("风险处理失败: {}", e);
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        // 错误详情已在executor中记录，这里只记录简要信息
                                                        let error_msg = e.to_string();
                                                        // 提取简化的错误信息
                                                        if error_msg.contains("套利失败") {
                                                            // 错误信息已经格式化好了，直接使用
                                                            error!("{}", error_msg);
                                                        } else {
                                                            error!("执行套利交易失败: {}", error_msg);
                                                        }
                                                    }
                                                }
                                            });
                                        }
                                    }
                                }
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

                // 定时仓位平衡任务
                _ = async {
                    if let Some(ref mut timer) = balance_timer {
                        timer.tick().await;
                        if !countdown_in_progress.load(Ordering::Relaxed) {
                            if let Err(e) = position_balancer.check_and_balance_positions(&market_token_map).await {
                                warn!(error = %e, "仓位平衡检查失败");
                            }
                        }
                    } else {
                        futures::future::pending::<()>().await;
                    }
                } => {
                    // 仓位平衡任务已执行
                }

                // 定期检查：1) 是否进入新的5分钟窗口 2) 收尾触发（5分钟窗口需更频繁检查）
                _ = sleep(Duration::from_secs(1)) => {
                    let now = Utc::now();
                    let new_window_timestamp = MarketDiscoverer::calculate_current_window_timestamp(now);

                    // 如果当前窗口时间戳与记录的不同，说明已经进入新窗口
                    if new_window_timestamp != current_window_timestamp {
                        info!(
                            old_window = current_window_timestamp,
                            new_window = new_window_timestamp,
                            "检测到新的5分钟窗口，准备取消旧订阅并切换到新窗口"
                        );
                        // 先drop stream以释放对monitor的借用，然后清理旧的订阅
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
