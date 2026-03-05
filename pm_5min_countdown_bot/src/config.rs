use anyhow::Result;
use polymarket_client_sdk::clob::types::OrderType;
use polymarket_client_sdk::types::Address;
use std::env;

fn parse_slippage(s: &str) -> [f64; 2] {
    let parts: Vec<f64> = s.split(',').map(|x| x.trim().parse().unwrap_or(0.0)).collect();
    match parts.len() { 0 => [0.0, 0.01], 1 => [parts[0], parts[0]], _ => [parts[0], parts[1]] }
}

fn parse_order_type(s: &str) -> OrderType {
    match s.trim().to_uppercase().as_str() { "GTC" => OrderType::GTC, "GTD" => OrderType::GTD, "FOK" => OrderType::FOK, "FAK" => OrderType::FAK, _ => OrderType::GTC }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub private_key: String,
    pub proxy_address: Option<Address>,
    pub crypto_symbols: Vec<String>,
    pub max_order_size_usdc: f64,
    pub slippage: [f64; 2],
    pub gtd_expiration_secs: u64,
    pub arbitrage_order_type: OrderType,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();
        Ok(Self {
            private_key: env::var("POLYMARKET_PRIVATE_KEY").expect("POLYMARKET_PRIVATE_KEY must be set"),
            proxy_address: env::var("POLYMARKET_PROXY_ADDRESS").ok().and_then(|addr| addr.parse().ok()),
            crypto_symbols: env::var("CRYPTO_SYMBOLS").unwrap_or_else(|_| "btc,eth,xrp,sol".to_string()).split(',').map(|s| s.trim().to_lowercase()).collect(),
            max_order_size_usdc: env::var("MAX_ORDER_SIZE_USDC").unwrap_or_else(|_| "5.0".to_string()).parse().unwrap_or(5.0),
            slippage: parse_slippage(&env::var("SLIPPAGE").unwrap_or_else(|_| "0.0,0.01".to_string())),
            gtd_expiration_secs: env::var("GTD_EXPIRATION_SECS").unwrap_or_else(|_| "300".to_string()).parse().unwrap_or(300),
            arbitrage_order_type: parse_order_type(&env::var("ARBITRAGE_ORDER_TYPE").unwrap_or_else(|_| "GTC".to_string())),
        })
    }
}
