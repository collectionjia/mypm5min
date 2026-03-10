use crate::merge::{
    build_hmac_signature, create_struct_hash, eip191_hash, encode_proxy_call, get_relay_payload,
    to_hex_0x, PROXY_DEFAULT_GAS, PROXY_FACTORY, RELAYER_SUBMIT, RELAYER_URL_DEFAULT, RELAY_HUB,
};
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::LocalSigner;
use alloy::signers::Signer as _;
use alloy::sol_types::SolCall;
use anyhow::{Context, Result};
use base64::Engine;
use polymarket_client_sdk::types::address;
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::env;
use std::str::FromStr;
use tracing::{info, warn};

use alloy::sol;
sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function transfer(address to, uint256 amount) external returns (bool);
        function decimals() external view returns (uint8);
    }
}

const RPC_URL_DEFAULT: &str = "https://polygon-bor-rpc.publicnode.com";
const USDC_POLYGON: Address = address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

async fn relayer_execute_transfer(
    transfer_calldata: &[u8],
    proxy_wallet: Address,
    signer: &impl alloy::signers::Signer,
    builder_key: &str,
    builder_secret: &str,
    builder_passphrase: &str,
    relayer_url: &str,
) -> Result<String> {
    let client = reqwest::Client::new();
    let eoa = signer.address();
    let base = relayer_url.trim_end_matches('/');

    let (relay, nonce) = get_relay_payload(&client, base, eoa).await?;
    // USDC transfer via Proxy:
    // Proxy.proxy([ { to: USDC, value: 0, data: transfer_calldata } ])
    let proxy_data = encode_proxy_call(USDC_POLYGON, transfer_calldata);
    
    let gas_limit: u64 = env::var("MERGE_PROXY_GAS_LIMIT")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(PROXY_DEFAULT_GAS);

    let to = PROXY_FACTORY;
    let struct_hash = create_struct_hash(eoa, to, &proxy_data, 0, 0, gas_limit, &nonce, RELAY_HUB, relay);
    let to_sign = eip191_hash(struct_hash);
    let sig = signer.sign_hash(&to_sign).await.map_err(|e| anyhow::anyhow!("EOA 签名失败: {}", e))?;
    let mut sig_bytes = sig.as_bytes().to_vec();
    if sig_bytes.len() == 65 && (sig_bytes[64] == 0 || sig_bytes[64] == 1) {
        sig_bytes[64] += 27;
    }
    let signature_hex = to_hex_0x(&sig_bytes);

    let signature_params = serde_json::json!({
        "gasPrice": "0",
        "gasLimit": gas_limit.to_string(),
        "relayerFee": "0",
        "relayHub": format!("{:#x}", RELAY_HUB),
        "relay": format!("{:#x}", relay)
    });
    let body = serde_json::json!({
        "from": format!("{:#x}", eoa),
        "to": format!("{:#x}", to),
        "proxyWallet": format!("{:#x}", proxy_wallet),
        "data": to_hex_0x(&proxy_data),
        "nonce": nonce,
        "signature": signature_hex,
        "signatureParams": signature_params,
        "type": "PROXY",
        "metadata": "Withdraw USDC"
    });
    let body_str = serde_json::to_string(&body)?;

    let path = RELAYER_SUBMIT;
    let method = "POST";
    let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64;
    let secret_b64 = builder_secret.trim().replace('-', "+").replace('_', "/");
    let secret_bytes = base64::engine::general_purpose::STANDARD
        .decode(&secret_b64)
        .map_err(|e| anyhow::anyhow!("POLY_BUILDER_SECRET base64 解码失败: {}", e))?;
    let sig_hmac = build_hmac_signature(&secret_bytes, timestamp, method, path, &body_str);

    let url = format!("{}{}", base, path);
    let resp = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("POLY_BUILDER_API_KEY", builder_key)
        .header("POLY_BUILDER_TIMESTAMP", timestamp.to_string())
        .header("POLY_BUILDER_PASSPHRASE", builder_passphrase)
        .header("POLY_BUILDER_SIGNATURE", sig_hmac)
        .body(body_str)
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("Relayer 请求失败 status={} body={}", status, text);
    }
    let json: serde_json::Value = serde_json::from_str(&text)?;
    let hash = json
        .get("transactionHash")
        .or_else(|| json.get("transaction_hash"))
        .and_then(|v| v.as_str())
        .map(String::from);
    Ok(hash.unwrap_or_else(|| text))
}

/// 检查代理钱包的 USDC 余额，并根据规则执行转账
///
/// 规则：
/// 1. 余额 > 20 USDC -> 转出所有余额到 target_wallet
/// 2. 余额 < 5 USDC -> 从 target_wallet 转入 10 USDC 到 proxy_wallet (需要 target_wallet 的私钥)
///
/// 注意：这里的 "target_wallet" 是用户的个人钱包。
/// - 提现时：Proxy -> Target (需要 Proxy 权限，通常是 Relayer 或 Gnosis Safe)
/// - 充值时：Target -> Proxy (需要 Target 私钥)
pub async fn check_and_manage_funds(
    proxy_address: Address,
    target_wallet: Address,
    target_private_key: &str, // 用于充值
    proxy_private_key: &str,  // 用于提现 (如果是 EOA 代理) 或 Relayer 配置
    rpc_url: Option<&str>,
) -> Result<()> {
    let rpc = rpc_url.unwrap_or(RPC_URL_DEFAULT);
    let provider = ProviderBuilder::new().connect(rpc).await?;
    let usdc = IERC20::new(USDC_POLYGON, provider.clone());

    // 1. 查询 Proxy 余额
    let balance_u256 = usdc.balanceOf(proxy_address).call().await?;
    let decimals = usdc.decimals().call().await?;
    let balance_decimal = Decimal::from_i128_with_scale(balance_u256.to_string().parse::<i128>()?, decimals as u32);

    info!("💰 当前 Proxy 余额: {} USDC", balance_decimal);

    if balance_decimal > dec!(20) {
        info!("🚀 余额 > 20 USDC，执行提现...");
        // 提现：Proxy -> Target
        let transfer_amount = balance_u256;
        let transfer_calldata = usdc.transfer(target_wallet, transfer_amount).calldata().to_vec();
        
        // 尝试判断是否是 EOA (通过代码长度)
        let code = provider.get_code_at(proxy_address).await?;
        if code.is_empty() {
            // EOA: 直接用 proxy_private_key 签名发送
            let signer = LocalSigner::from_str(proxy_private_key)?.with_chain_id(Some(POLYGON));
            let wallet_provider = ProviderBuilder::new().wallet(signer).connect(rpc).await?;
            let usdc_wallet = IERC20::new(USDC_POLYGON, wallet_provider);
            
            let tx = usdc_wallet.transfer(target_wallet, transfer_amount).send().await?;
            info!("✅ 提现成功 (EOA): Tx {}", tx.tx_hash());
        } else {
             // 合约钱包 (Magic/Email): 需要 Relayer
             info!("ℹ️ 检测到合约钱包 (Magic/Email)，尝试通过 Relayer 提现...");
             
             let builder_key = env::var("POLY_BUILDER_API_KEY").ok();
             let builder_secret = env::var("POLY_BUILDER_SECRET").ok();
             let builder_passphrase = env::var("POLY_BUILDER_PASSPHRASE").ok();
             let relayer_url = env::var("RELAYER_URL").unwrap_or_else(|_| RELAYER_URL_DEFAULT.to_string());
             
             // 对于 Relayer 模式，proxy_private_key 实际上是 EOA 的私钥
             let signer = LocalSigner::from_str(proxy_private_key)?.with_chain_id(Some(POLYGON));

             match (builder_key.as_deref(), builder_secret.as_deref(), builder_passphrase.as_deref()) {
                (Some(k), Some(s), Some(p)) => {
                    let out = relayer_execute_transfer(
                        &transfer_calldata, 
                        proxy_address, 
                        &signer, 
                        k, s, p, 
                        &relayer_url
                    ).await?;
                    info!("✅ Relayer 已提交提现 tx: {}", out);
                }
                _ => anyhow::bail!("Magic/Email 提现需配置 POLY_BUILDER_API_KEY 等。"),
            }
        }

    } else if balance_decimal < dec!(5) {
        info!("📉 余额 < 5 USDC，执行充值 (10 USDC)...");
        // 充值：Target -> Proxy
        // 使用 target_private_key
        let signer = LocalSigner::from_str(target_private_key)?.with_chain_id(Some(POLYGON));
        let wallet_provider = ProviderBuilder::new().wallet(signer).connect(rpc).await?;
        let usdc_wallet = IERC20::new(USDC_POLYGON, wallet_provider);
        
        let amount_to_deposit = U256::from(10) * U256::from(10).pow(U256::from(decimals));
        
        // 检查 Target 余额
        let target_balance = usdc.balanceOf(target_wallet).call().await?;
        if target_balance < amount_to_deposit {
            anyhow::bail!("❌ 目标钱包余额不足，无法充值 (需 {} USDC)", Decimal::from(10));
        }
        
        let tx = usdc_wallet.transfer(proxy_address, amount_to_deposit).send().await?;
        info!("✅ 充值成功: Tx {}", tx.tx_hash());
    } else {
        info!("✅ 余额在正常范围内 (5 - 20 USDC)，无需操作。");
    }

    Ok(())
}
