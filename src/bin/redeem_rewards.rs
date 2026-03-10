use anyhow::{Context, Result};
use dotenvy::dotenv;
use poly_5min_bot::merge::redeem_max;
use poly_5min_bot::positions::get_positions;
use polymarket_client_sdk::types::{Address, B256};
use std::collections::HashSet;
use std::env;
use std::str::FromStr;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    info!("🚀 启动奖励领取脚本 (Redeem Rewards Script)");

    // 加载 .env
    dotenv().ok();

    // 检查环境变量
    let private_key = env::var("POLYMARKET_PRIVATE_KEY").context("POLYMARKET_PRIVATE_KEY 未设置")?;
    let proxy_address_str =
        env::var("POLYMARKET_PROXY_ADDRESS").context("POLYMARKET_PROXY_ADDRESS 未设置")?;
    let proxy_address = Address::from_str(&proxy_address_str).context("POLYMARKET_PROXY_ADDRESS 格式无效")?;

    info!("👤 代理地址: {:?}", proxy_address);

    // 获取持仓
    info!("🔄 正在获取持仓信息...");
    let positions = match get_positions().await {
        Ok(p) => p,
        Err(e) => {
            error!("❌ 获取持仓失败: {}", e);
            return Ok(());
        }
    };

    if positions.is_empty() {
        info!("ℹ️ 当前无持仓，无需领取。");
        return Ok(());
    }

    info!("✅ 获取到 {} 个持仓记录，开始检查可领取的奖励...", positions.len());

    let mut processed_conditions = HashSet::new();
    let mut success_count = 0;
    let mut fail_count = 0;

    for position in positions {
        // 去重 condition_id
        if processed_conditions.contains(&position.condition_id) {
            continue;
        }
        processed_conditions.insert(position.condition_id);

        let condition_id = position.condition_id;
        info!("🔍 检查市场 Condition ID: {:?}", condition_id);

        // 尝试 Redeem
        match redeem_max(condition_id, proxy_address, &private_key, None).await {
            Ok(tx_hash) => {
                info!("🎉 领取成功! Condition: {:?} | Tx: {}", condition_id, tx_hash);
                success_count += 1;
            }
            Err(e) => {
                let msg = e.to_string();
                // 过滤常见的非错误情况（如未决议）
                if msg.contains("execution reverted") || msg.contains("revert") {
                    info!("ℹ️ 跳过 (可能未决议或无获胜份额): {:?}", condition_id);
                } else if msg.contains("无持仓可 Redeem") {
                    info!("ℹ️ 跳过 (无获胜份额): {:?}", condition_id);
                } else {
                    warn!("⚠️ 领取失败 Condition: {:?} | Error: {}", condition_id, e);
                    fail_count += 1;
                }
            }
        }
    }

    info!("🏁 脚本运行结束。");
    info!("📊 统计: 成功领取 {} 个, 失败/跳过 {} 个", success_count, fail_count);

    Ok(())
}
