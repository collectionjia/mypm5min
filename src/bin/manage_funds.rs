use anyhow::{Context, Result};
use dotenvy::dotenv;
use poly_5min_bot::funds::check_and_manage_funds;
use polymarket_client_sdk::types::Address;
use std::env;
use std::str::FromStr;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    info!("🚀 启动资金管理脚本 (Manage Funds Script)");

    // 加载 .env
    dotenv().ok();

    // 检查环境变量
    let proxy_private_key = env::var("POLYMARKET_PRIVATE_KEY").context("POLYMARKET_PRIVATE_KEY 未设置")?;
    let proxy_address_str =
        env::var("POLYMARKET_PROXY_ADDRESS").context("POLYMARKET_PROXY_ADDRESS 未设置")?;
    let proxy_address = Address::from_str(&proxy_address_str).context("POLYMARKET_PROXY_ADDRESS 格式无效")?;

    // 新增：Target Wallet 配置
    // 用户需要在 .env 中配置 TARGET_WALLET_ADDRESS 和 TARGET_WALLET_PRIVATE_KEY
    let target_wallet_str = env::var("TARGET_WALLET_ADDRESS").context("TARGET_WALLET_ADDRESS 未设置 (用于接收提现和来源充值)")?;
    let target_wallet = Address::from_str(&target_wallet_str).context("TARGET_WALLET_ADDRESS 格式无效")?;
    
    // 如果没有配置 TARGET_WALLET_PRIVATE_KEY，充值功能将无法工作，但提现仍可尝试
    let target_private_key = env::var("TARGET_WALLET_PRIVATE_KEY").unwrap_or_else(|_| "".to_string());

    info!("👤 Proxy 地址: {:?}", proxy_address);
    info!("🎯 Target 地址: {:?}", target_wallet);

    if target_private_key.is_empty() {
        info!("⚠️ TARGET_WALLET_PRIVATE_KEY 未设置，自动充值功能将被禁用 (仅支持提现)");
    }

    match check_and_manage_funds(
        proxy_address,
        target_wallet,
        &target_private_key,
        &proxy_private_key,
        None
    ).await {
        Ok(_) => info!("✅ 资金检查/操作完成"),
        Err(e) => error!("❌ 资金操作失败: {}", e),
    }

    Ok(())
}
