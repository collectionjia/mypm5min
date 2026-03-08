use alloy::primitives::Address;
use alloy::providers::{ProviderBuilder, Provider};
use alloy::sol;
use anyhow::Result;
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

pub async fn get_usdc_balance(wallet_address: Address) -> Result<Decimal> {
    let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| "https://polygon-rpc.com".to_string()).parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let usdc_addr = Address::from_str(USDC_ADDRESS)?;
    let contract = IERC20::new(usdc_addr, provider);

    let balance = contract.balanceOf(wallet_address).call().await?;
    // USDC has 6 decimals
    let balance_dec = Decimal::from_str(&balance.to_string()).unwrap_or_default() / Decimal::from(1_000_000);
    
    Ok(balance_dec)
}

pub async fn check_balance_and_allowance(wallet_address: Address) -> Result<()> {
    // 使用公共RPC节点，或者尝试从环境变量获取
    let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| "https://polygon-rpc.com".to_string()).parse()?;
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
            let balance_dec = Decimal::from_str(&balance.to_string()).unwrap_or_default() / Decimal::from(1_000_000);
            
            info!("当前钱包 ({}) USDC余额: {}", wallet_address, balance_dec);

            if balance_dec < Decimal::from(1) {
                warn!("⚠️  USDC余额不足 1 USDC，可能导致无法下单！请充值。");
            }
        },
        Err(e) => {
            warn!("查询余额失败: {}", e);
        }
    }

    // 查询授权额度
    let allowance_call = contract.allowance(wallet_address, exchange_addr).call().await;
    match allowance_call {
        Ok(result) => {
            let allowance = result;
            let allowance_dec = Decimal::from_str(&allowance.to_string()).unwrap_or_default() / Decimal::from(1_000_000);
            
            info!("对CTF Exchange ({}) 的授权额度: {}", exchange_addr, allowance_dec);

            if allowance_dec < Decimal::from(10) {
                warn!("⚠️  对CTF Exchange的授权额度不足 ({})，可能导致下单失败！请前往Polymarket官网或使用区块浏览器进行Approve操作。", allowance_dec);
            }
        },
        Err(e) => {
            warn!("查询授权额度失败: {}", e);
        }
    }

    Ok(())
}
