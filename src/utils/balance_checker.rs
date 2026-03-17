use alloy::primitives::Address;
use alloy::providers::ProviderBuilder;
use alloy::sol;
use anyhow::Result;
use polymarket_client_sdk::{contract_config, POLYGON};
use rust_decimal::Decimal;
use std::str::FromStr;
use tracing::{info, warn};

// Polygon USDC Contract (Bridged USDC.e)
const USDC_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
// Polymarket CTF Exchange Contract
const CTF_EXCHANGE_ADDRESS: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
    }
}

sol! {
    #[sol(rpc)]
    interface IERC1155 {
        function isApprovedForAll(address account, address operator) external view returns (bool);
    }
}

pub async fn get_usdc_balance(wallet_address: Address) -> Result<Decimal> {
    let rpc_url = std::env::var("RPC_URL")
        .unwrap_or_else(|_| "https://polygon-bor.publicnode.com".to_string())
        .parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let usdc_addr = Address::from_str(USDC_ADDRESS)?;
    let contract = IERC20::new(usdc_addr, provider);

    let balance = contract.balanceOf(wallet_address).call().await?;
    // USDC has 6 decimals
    let balance_dec =
        Decimal::from_str(&balance.to_string()).unwrap_or_default() / Decimal::from(1_000_000);

    Ok(balance_dec)
}

pub async fn check_balance_and_allowance(wallet_address: Address) -> Result<()> {
    // 使用公共RPC节点，或者尝试从环境变量获取
    let rpc_url = std::env::var("RPC_URL")
        .unwrap_or_else(|_| "https://polygon-bor.publicnode.com".to_string())
        .parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let usdc_addr = Address::from_str(USDC_ADDRESS)?;
    let exchange_addr = Address::from_str(CTF_EXCHANGE_ADDRESS)?;
    // 使用 new 方法，但不需要泛型参数，provider 已经包含了网络信息
    let contract = IERC20::new(usdc_addr, provider.clone());

    // 查询余额
    let balance_call = contract.balanceOf(wallet_address).call().await;
    match balance_call {
        Ok(result) => {
            let balance = result;
            // USDC有6位小数
            let balance_dec = Decimal::from_str(&balance.to_string()).unwrap_or_default()
                / Decimal::from(1_000_000);

            info!("当前钱包 ({}) USDC余额: {}", wallet_address, balance_dec);

            if balance_dec < Decimal::from(1) {
                warn!("⚠️  USDC余额不足 1 USDC，可能导致无法下单！请充值。");
            }
        }
        Err(e) => {
            warn!("查询余额失败: {}", e);
        }
    }

    // 查询授权额度
    let allowance_call = contract
        .allowance(wallet_address, exchange_addr)
        .call()
        .await;
    match allowance_call {
        Ok(result) => {
            let allowance = result;
            let allowance_dec = Decimal::from_str(&allowance.to_string()).unwrap_or_default()
                / Decimal::from(1_000_000);

            info!(
                "对CTF Exchange ({}) 的授权额度: {}",
                exchange_addr, allowance_dec
            );

            if allowance_dec < Decimal::from(10) {
                warn!("⚠️  对CTF Exchange的授权额度不足 ({})，可能导致下单失败！请前往Polymarket官网或使用区块浏览器进行Approve操作。", allowance_dec);
            }
        }
        Err(e) => {
            warn!("查询授权额度失败: {}", e);
        }
    }

    Ok(())
}

pub async fn check_conditional_token_approval(wallet_address: Address) -> Result<()> {
    let rpc_url = std::env::var("RPC_URL")
        .unwrap_or_else(|_| "https://polygon-bor.publicnode.com".to_string())
        .parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let exchange_addr = Address::from_str(CTF_EXCHANGE_ADDRESS)?;
    let cfg = contract_config(POLYGON, false)
        .ok_or_else(|| anyhow::anyhow!("不支持的 chain_id: {}", POLYGON))?;

    let conditional_tokens = cfg.conditional_tokens;
    let ct = IERC1155::new(conditional_tokens, provider);

    let approved = ct
        .isApprovedForAll(wallet_address, exchange_addr)
        .call()
        .await
        .unwrap_or(false);
    if approved {
        info!(
            "ConditionalTokens 已授权给 CTF Exchange ({})",
            exchange_addr
        );
    } else {
        warn!(
            "⚠️ ConditionalTokens 未授权给 CTF Exchange ({})，卖出可能失败。请在 Polymarket 官网进行一次 SELL/Approve 以完成授权。",
            exchange_addr
        );
    }
    Ok(())
}
