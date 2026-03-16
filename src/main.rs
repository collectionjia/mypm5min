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
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use polymarket_client_sdk::types::{Address, B256, U256};

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

fn adjust_order_size_for_fee(entry_price: Decimal, size: Decimal) -> Decimal {
    use rust_decimal::prelude::ToPrimitive;

    if size <= dec!(0) {
        return dec!(0);
    }

    let p = entry_price.to_f64().unwrap_or(0.0);
    let base = p * (1.0 - p);
    let fee_value = 100.0 * 0.25 * base.powf(2.0);
    let fee_decimal = Decimal::try_from(fee_value).unwrap_or(dec!(0));

    let available_amount = if fee_decimal >= dec!(100.0) {
        dec!(0.01)
    } else {
        size * (dec!(100.0) - fee_decimal) / dec!(100.0)
    };

    let floored_size = (available_amount * dec!(100.0)).floor() / dec!(100.0);
    if floored_size.is_zero() {
        dec!(0.01)
    } else {
        floored_size
    }
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
            if let Err(e) = crate::utils::balance_checker::check_conditional_token_approval(wallet_to_check).await {
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
    let countdown_settings = Arc::new(tokio::sync::RwLock::new(web_server::CountdownSettings::default()));
    let is_running_server = is_running.clone();
    let market_data_server = market_data.clone();
    let executor_server = Some(executor.clone());
    let countdown_settings_server = countdown_settings.clone();
    tokio::spawn(async move {
        web_server::start_server(is_running_server, market_data_server, executor_server, countdown_settings_server).await;
    });

    info!("🌐 Web控制台已启动: http://localhost:3000");
    info!("🧪 默认模拟交易：在 Web 控制台可切换真实投注开关");

    // 启动时自动检查并领取所有已决议市场的奖励（防止重启后无法领取）
    {
        info!("🚀 启动自检：检查是否有未领取的奖励...");
        if let Some(proxy) = config.proxy_address {
            let priv_key = config.private_key.clone();
            // 获取所有持仓
            match _risk_manager.position_tracker().sync_from_api().await {
                Ok(positions) => {
                    // 聚合 condition_id -> outcome_indexes
                    let mut conditions_map: std::collections::HashMap<B256, std::collections::HashSet<i32>> = std::collections::HashMap::new();
                    for p in &positions {
                        conditions_map
                            .entry(p.condition_id)
                            .or_default()
                            .insert(p.outcome_index);
                    }
                    
                    if !conditions_map.is_empty() {
                        info!("🔍 发现 {} 个相关市场，尝试执行 Redeem...", conditions_map.len());
                        // 启动一个异步任务来执行 Redeem，避免阻塞主线程
                        let priv_key_clone = priv_key.clone();
                        tokio::spawn(async move {
                            for (condition_id, indexes_set) in conditions_map {
                                let indexes: Vec<i32> = indexes_set.into_iter().collect();
                                // 对每个市场尝试 Redeem（如果未决议会失败，忽略即可）
                                match crate::merge::redeem_outcomes(condition_id, proxy, &priv_key_clone, &indexes, None).await {
                                    Ok(tx) => info!("✅ 启动自动领取成功 | condition_id={} | tx={}", condition_id, tx),
                                    Err(e) => {
                                        // 大多数失败是因为市场未决议，这是正常的，使用 debug 日志
                                        debug!("启动自动领取跳过 (可能未决议/无获胜持仓): {} | condition_id={}", e, condition_id);
                                    }
                                }
                                // 稍微间隔一下，避免请求过于频繁，且等待 nonce 更新
                                sleep(Duration::from_millis(1000)).await;
                            }
                            info!("✅ 启动自动领取检查完成");
                        });
                    } else {
                        info!("🔍 当前无持仓，无需 Redeem");
                    }
                }
                Err(e) => warn!("⚠️ 启动自检失败：无法获取持仓 ({})", e),
            }
        }
    }


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
        let mut force_close_cancel_done = false;

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
        let strategy_state: DashMap<B256, u8> = DashMap::new();
        let first_leg_price_map: Arc<DashMap<B256, Decimal>> = Arc::new(DashMap::new());
        #[derive(Clone)]
        struct SimOrderInfo {
            market_id: B256,
            side_key: u8,
            limit_price: Decimal,
            on_filled_state: u8,
            clear_first_leg_price: bool,
        }
        let sim_open_orders: Arc<DashMap<String, SimOrderInfo>> = Arc::new(DashMap::new());

        // 监控订单簿更新
        loop {
            let now_all = Utc::now();
            let seconds_until_end_all = (window_end - now_all).num_seconds();
            // 在窗口结束前1分钟开始尝试领取，而不是等到完全结束
            if seconds_until_end_all <= 60 && !post_end_claim_done {
                post_end_claim_done = true;
                let config_claim = config.clone();
                tokio::spawn(async move {
                    if let Some(proxy) = config_claim.proxy_address {
                        // 循环尝试领取，直到窗口结束一段时间后，以确保所有结算都完成
                        // 尝试5次，每次间隔30秒
                        for i in 0..5 {
                            if i > 0 {
                                info!("自动领取：第 {} 次尝试...", i + 1);
                            }
                            match get_positions().await {
                                Ok(positions) => {
                                    let condition_ids = condition_ids_with_both_sides(&positions);
                                    if condition_ids.is_empty() {
                                        if i == 0 {
                                            info!("自动领取：当前无双边持仓可领取");
                                        }
                                    } else {
                                        info!("自动领取：发现 {} 个市场可领取", condition_ids.len());
                                        let n = condition_ids.len();
                                        for (j, condition_id) in condition_ids.iter().enumerate() {
                                            match merge::merge_max(*condition_id, proxy, &config_claim.private_key, None).await {
                                                Ok(tx) => {
                                                    info!("🎁 自动领取：Merge 完成 | condition_id={:#x} | tx={}", condition_id, tx);
                                                }
                                                Err(e) => {
                                                    warn!(condition_id = %condition_id, error = %e, "自动领取：Merge 失败");
                                                }
                                            }
                                            if j + 1 < n {
                                                sleep(Duration::from_secs(10)).await;
                                            }
                                        }
                                    }
                                }
                                Err(e) => { warn!(error = %e, "自动领取：获取持仓失败，跳过"); }
                            }
                            sleep(Duration::from_secs(30)).await;
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
                        // 已根据需求移除：只保留 Merge 操作，不进行强制卖出
                        info!("🧹 收尾：已完成 Merge 操作，跳过单腿卖出（根据策略配置）");

                        info!("🛑 收尾完成，继续监控至窗口结束");
                        wind_down_flag.store(false, Ordering::Relaxed);
                    });
                }
            }

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
                                let sec_since_start = now_countdown.timestamp() - current_window_timestamp;
                                let sec_since_start_nonneg = sec_since_start.max(0);
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
                                    let entry_trigger_secs_to_end: i64 = 60;
                                    let total_price_cap = dec!(0.95);
                                    let first_leg_min_price = dec!(0.60);
                                    let second_leg_fixed_price = dec!(0.05);

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
                                                strategy_state.insert(info.market_id, info.on_filled_state);
                                                if info.clear_first_leg_price {
                                                    first_leg_price_map.remove(&info.market_id);
                                                }
                                            }
                                        }
                                    }

                                    let state = strategy_state.get(&market_id).map(|v| *v).unwrap_or(0);
                                    if sec_to_end > 0 {
                                        if state == 0 {
                                            let yes_ask = yes_best_ask.map(|(p, _)| p.round_dp(2));
                                            let no_ask = no_best_ask.map(|(p, _)| p.round_dp(2));

                                            if sec_to_end_nonneg <= entry_trigger_secs_to_end {
                                                let (side_key, token_id, side_name, limit_price) = match (yes_ask, no_ask) {
                                                    (Some(yp), Some(np)) => {
                                                        if yp >= np {
                                                            (0u8, pair.yes_book.asset_id, "YES".to_string(), yp)
                                                        } else {
                                                            (1u8, pair.no_book.asset_id, "NO".to_string(), np)
                                                        }
                                                    }
                                                    (Some(yp), None) => (0u8, pair.yes_book.asset_id, "YES".to_string(), yp),
                                                    (None, Some(np)) => (1u8, pair.no_book.asset_id, "NO".to_string(), np),
                                                    (None, None) => {
                                                        continue;
                                                    }
                                                };

                                                if limit_price < first_leg_min_price {
                                                    if !is_live {
                                                        use crate::utils::trade_history::{add_trade, TradeRecord};
                                                        use chrono::Utc;
                                                        use rust_decimal::prelude::ToPrimitive;
                                                        add_trade(TradeRecord {
                                                            id: format!("SIM-{}", uuid::Uuid::new_v4()),
                                                            market_id: market_id.to_string(),
                                                            market_slug: market_display.clone(),
                                                            side: side_name,
                                                            price: limit_price.to_f64().unwrap_or(0.0),
                                                            size: 0.0,
                                                            timestamp: Utc::now().timestamp(),
                                                            status: "SimSkipped".to_string(),
                                                            profit: None,
                                                            buy_countdown: Some(countdown_str.clone()),
                                                            sell_countdown: Some("价格过低".to_string()),
                                                        });
                                                        strategy_state.insert(market_id, 4);
                                                    }
                                                    continue;
                                                }

                                                if limit_price >= total_price_cap {
                                                    if !is_live {
                                                        use crate::utils::trade_history::{add_trade, TradeRecord};
                                                        use chrono::Utc;
                                                        use rust_decimal::prelude::ToPrimitive;
                                                        add_trade(TradeRecord {
                                                            id: format!("SIM-{}", uuid::Uuid::new_v4()),
                                                            market_id: market_id.to_string(),
                                                            market_slug: market_display.clone(),
                                                            side: side_name,
                                                            price: limit_price.to_f64().unwrap_or(0.0),
                                                            size: 0.0,
                                                            timestamp: Utc::now().timestamp(),
                                                            status: "SimSkipped".to_string(),
                                                            profit: None,
                                                            buy_countdown: Some(countdown_str.clone()),
                                                            sell_countdown: Some("价格过高".to_string()),
                                                        });
                                                        strategy_state.insert(market_id, 4);
                                                    }
                                                    continue;
                                                }
                                                let second_leg_candidate = (total_price_cap - limit_price).round_dp(2);
                                                if second_leg_candidate < second_leg_fixed_price {
                                                    if !is_live {
                                                        use crate::utils::trade_history::{add_trade, TradeRecord};
                                                        use chrono::Utc;
                                                        use rust_decimal::prelude::ToPrimitive;
                                                        add_trade(TradeRecord {
                                                            id: format!("SIM-{}", uuid::Uuid::new_v4()),
                                                            market_id: market_id.to_string(),
                                                            market_slug: market_display.clone(),
                                                            side: side_name,
                                                            price: limit_price.to_f64().unwrap_or(0.0),
                                                            size: 0.0,
                                                            timestamp: Utc::now().timestamp(),
                                                            status: "SimSkipped".to_string(),
                                                            profit: None,
                                                            buy_countdown: Some(countdown_str.clone()),
                                                            sell_countdown: Some(format!("总价>0.95(P2:{:.2})", second_leg_candidate.to_f64().unwrap_or(0.0))),
                                                        });
                                                        strategy_state.insert(market_id, 4);
                                                    }
                                                    continue;
                                                }

                                                let qty = dec!(5.0);
                                                let max_qty = Decimal::try_from(config.max_order_size_usdc).unwrap_or(dec!(100.0));
                                                let qty = if qty > max_qty { max_qty } else { qty };

                                                info!(
                                                    "⏱️ 倒计时策略入场买入 | 市场:{} | 倒数<=60s | {} 价格更高 | 价格:{:.4} | 份额:{:.2}",
                                                    market_display, side_name, limit_price, qty
                                                );

                                                if is_live {
                                                    strategy_state.insert(market_id, 9);
                                                    let executor_clone = executor.clone();
                                                    let pt = _risk_manager.position_tracker();
                                                    let state_map = strategy_state.clone();
                                                    let price_map = first_leg_price_map.clone();
                                                    let market_id_key = market_id;
                                                    let next_state = if side_key == 0 { 1u8 } else { 2u8 };
                                                    let market_id_str = market_id.to_string();
                                                    let market_display_str = market_display.clone();
                                                    let buy_countdown = Some(countdown_str.clone());
                                                    use rust_decimal::prelude::ToPrimitive;
                                                    let price_f64 = limit_price.to_f64().unwrap_or(0.0);
                                                    tokio::spawn(async move {
                                                        match executor_clone.buy_at_price(token_id, limit_price, qty).await {
                                                            Ok(resp) => {
                                                                pt.update_exposure_cost(token_id, limit_price, qty);
                                                                pt.update_position(token_id, qty);
                                                                use crate::utils::trade_history::{add_trade, TradeRecord};
                                                                use chrono::Utc;
                                                                add_trade(TradeRecord {
                                                                    id: resp.order_id.clone(),
                                                                    market_id: market_id_str,
                                                                    market_slug: market_display_str,
                                                                    side: side_name,
                                                                    price: price_f64,
                                                                    size: qty.to_f64().unwrap_or(0.0),
                                                                    timestamp: Utc::now().timestamp(),
                                                                    status: "Bought".to_string(),
                                                                    profit: None,
                                                                    buy_countdown,
                                                                    sell_countdown: None,
                                                                });
                                                                price_map.insert(market_id_key, limit_price);
                                                                state_map.insert(market_id_key, next_state);
                                                            }
                                                            Err(e) => {
                                                                warn!("❌ 倒计时策略第一腿下单失败: {}", e);
                                                                price_map.remove(&market_id_key);
                                                                state_map.insert(market_id_key, 0u8);
                                                            }
                                                        }
                                                    });
                                                } else {
                                                    use crate::utils::trade_history::{add_trade, TradeRecord};
                                                    use chrono::Utc;
                                                    use rust_decimal::prelude::ToPrimitive;
                                                    let order_id = format!("SIM-{}", uuid::Uuid::new_v4());
                                                    let buy_countdown = Some(countdown_str.clone());
                                                    add_trade(TradeRecord {
                                                        id: order_id,
                                                        market_id: market_id.to_string(),
                                                        market_slug: market_display.clone(),
                                                        side: side_name,
                                                        price: limit_price.to_f64().unwrap_or(0.0),
                                                        size: qty.to_f64().unwrap_or(0.0),
                                                        timestamp: Utc::now().timestamp(),
                                                        status: "SimPosted".to_string(),
                                                        profit: None,
                                                        buy_countdown,
                                                        sell_countdown: None,
                                                    });
                                                    first_leg_price_map.insert(market_id, limit_price);
                                                    sim_open_orders.insert(
                                                        order_id.clone(),
                                                        SimOrderInfo {
                                                            market_id,
                                                            side_key,
                                                            limit_price,
                                                            on_filled_state: if side_key == 0 { 1 } else { 2 },
                                                            clear_first_leg_price: false,
                                                        },
                                                    );
                                                    strategy_state.insert(market_id, 9);
                                                }
                                            }
                                        } else if state == 1 || state == 2 {
                                            if first_leg_price_map.get(&market_id).is_none() {
                                                strategy_state.insert(market_id, 0u8);
                                                continue;
                                            }

                                            let qty2 = dec!(5.0);
                                            let max_qty = Decimal::try_from(config.max_order_size_usdc).unwrap_or(dec!(100.0));
                                            let qty2 = if qty2 > max_qty { max_qty } else { qty2 };

                                            let (second_leg_token_id, second_leg_side) = if state == 1 {
                                                (pair.no_book.asset_id, "NO".to_string())
                                            } else {
                                                (pair.yes_book.asset_id, "YES".to_string())
                                            };

                                            info!(
                                                "⏱️ 倒计时策略第二腿下单 | 市场:{} | {} 挂0.05 | 份额:{:.2}",
                                                market_display, second_leg_side, qty2
                                            );

                                            if is_live {
                                                strategy_state.insert(market_id, 10);
                                                let exec = executor.clone();
                                                let pt = _risk_manager.position_tracker();
                                                let state_map = strategy_state.clone();
                                                let price_map = first_leg_price_map.clone();
                                                let market_id_key = market_id;
                                                let market_id_str = market_id.to_string();
                                                let market_display_str = market_display.clone();
                                                let buy_countdown = Some(countdown_str.clone());
                                                use rust_decimal::prelude::ToPrimitive;
                                                let price_f64 = second_leg_fixed_price.to_f64().unwrap_or(0.0);
                                                let side_for_record = second_leg_side.clone();
                                                tokio::spawn(async move {
                                                    let res = exec.buy_at_price(second_leg_token_id, second_leg_fixed_price, qty2).await;
                                                    let ok = res.is_ok();

                                                    use crate::utils::trade_history::{add_trade, TradeRecord};
                                                    use chrono::Utc;

                                                    if let Ok(resp) = res {
                                                        pt.update_exposure_cost(second_leg_token_id, second_leg_fixed_price, qty2);
                                                        pt.update_position(second_leg_token_id, qty2);
                                                        add_trade(TradeRecord {
                                                            id: resp.order_id.clone(),
                                                            market_id: market_id_str.clone(),
                                                            market_slug: market_display_str.clone(),
                                                            side: side_for_record,
                                                            price: price_f64,
                                                            size: qty2.to_f64().unwrap_or(0.0),
                                                            timestamp: Utc::now().timestamp(),
                                                            status: "Bought".to_string(),
                                                            profit: None,
                                                            buy_countdown: buy_countdown.clone(),
                                                            sell_countdown: None,
                                                        });
                                                    }
                                                    if ok {
                                                        state_map.insert(market_id_key, 3u8);
                                                        price_map.remove(&market_id_key);
                                                    } else {
                                                        state_map.insert(market_id_key, 0u8);
                                                        price_map.remove(&market_id_key);
                                                    }
                                                });
                                            } else {
                                                use crate::utils::trade_history::{add_trade, TradeRecord};
                                                use chrono::Utc;
                                                use rust_decimal::prelude::ToPrimitive;
                                                let order_id = format!("SIM-{}", uuid::Uuid::new_v4());
                                                let buy_countdown = Some(countdown_str.clone());
                                                add_trade(TradeRecord {
                                                    id: order_id.clone(),
                                                    market_id: market_id.to_string(),
                                                    market_slug: market_display.clone(),
                                                    side: second_leg_side,
                                                    price: second_leg_fixed_price.to_f64().unwrap_or(0.0),
                                                    size: qty2.to_f64().unwrap_or(0.0),
                                                    timestamp: Utc::now().timestamp(),
                                                    status: "SimPosted".to_string(),
                                                    profit: None,
                                                    buy_countdown,
                                                    sell_countdown: None,
                                                });
                                                sim_open_orders.insert(
                                                    order_id.clone(),
                                                    SimOrderInfo {
                                                        market_id,
                                                        side_key: if state == 1 { 1 } else { 0 },
                                                        limit_price: second_leg_fixed_price,
                                                        on_filled_state: 3,
                                                        clear_first_leg_price: true,
                                                    },
                                                );
                                                strategy_state.insert(market_id, 10);
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

                                // 保留原有的结构化日志用于调试（可选）
                                debug!(
                                    market_id = %pair.market_id,
                                    yes_token = %pair.yes_book.asset_id,
                                    no_token = %pair.no_book.asset_id,
                                    "订单簿对详细信息"
                                );
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
                        if !prev_round_markets.is_empty() && proxy_addr.is_some() {
                            let proxy = proxy_addr.unwrap();
                            let settle_delay_secs = 10u64;
                            info!(
                                "🕒 已安排{}秒后对 {} 个上一轮市场执行平仓（Merge/Redeem）检查",
                                settle_delay_secs,
                                prev_round_markets.len()
                            );
                            
                            tokio::spawn(async move {
                                sleep(Duration::from_secs(settle_delay_secs)).await;
                                info!("⏰ 开始对上一轮市场执行平仓（Merge/Redeem）检查...");
                                
                                // 1. 先尝试 Merge 所有市场（无需等待决议，立刻执行）
                                for (condition_id, _, _) in &prev_round_markets {
                                    match merge::merge_max(*condition_id, proxy, &priv_key, None).await {
                                        Ok(tx) => info!("✅ Merge 成功 | condition_id={} | tx={}", condition_id, tx),
                                        Err(e) => {
                                            if !e.to_string().contains("无可用份额") {
                                                debug!("Merge 跳过: {}", e);
                                            }
                                        }
                                    }
                                }

                                // 2. 循环尝试 Redeem（需等待决议，支持重试）
                                let mut pending_markets: HashSet<B256> = prev_round_markets.iter().map(|(c, _, _)| *c).collect();
                                let max_retries = 20; // 20 * 30s = 约10分钟
                                
                                for i in 0..max_retries {
                                    if pending_markets.is_empty() {
                                        break;
                                    }
                                    
                                    if i > 0 {
                                        info!("Redeem 重试 {}/{} | 剩余 {} 个市场等待决议...", i, max_retries, pending_markets.len());
                                        sleep(Duration::from_secs(30)).await;
                                    }

                                    let mut completed = Vec::new();
                                    
                                    for (condition_id, yes_token, no_token) in &prev_round_markets {
                                        if !pending_markets.contains(condition_id) {
                                            continue;
                                        }

                                        match merge::redeem_max(*condition_id, proxy, &priv_key, None).await {
                                            Ok(tx) => {
                                                info!("✅ Redeem 成功 | condition_id={} | tx={}", condition_id, tx);
                                                // 更新本地持仓为0
                                                let yes_bal = pt.get_position(*yes_token);
                                                let no_bal = pt.get_position(*no_token);
                                                pt.update_position(*yes_token, -yes_bal);
                                                pt.update_position(*no_token, -no_bal);
                                                completed.push(*condition_id);
                                            },
                                            Err(e) => {
                                                let err_msg = e.to_string();
                                                if err_msg.contains("无持仓") {
                                                    debug!("Redeem 跳过: 无持仓 | condition_id={}", condition_id);
                                                    completed.push(*condition_id);
                                                } else {
                                                    // 其他错误（如未决议），保留重试
                                                    // 仅在第一次或每5次打印警告，避免刷屏
                                                    if i % 5 == 0 {
                                                        warn!("⚠️ Redeem 暂未成功 (可能未决议) | condition_id={} | error={}", condition_id, err_msg);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                    for c in completed {
                                        pending_markets.remove(&c);
                                    }
                                }
                                
                                if !pending_markets.is_empty() {
                                    warn!("⚠️ 部分市场 Redeem 超时未完成: {:?}", pending_markets);
                                } else {
                                    info!("🏁 上一轮市场平仓任务全部完成");
                                }
                            });
                        }

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
