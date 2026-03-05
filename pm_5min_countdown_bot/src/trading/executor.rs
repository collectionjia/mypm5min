use anyhow::Result;
use alloy::signers::Signer;
use alloy::signers::local::LocalSigner;
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::clob::types::{OrderType, Side, SignatureType};
use polymarket_client_sdk::types::{Address, Decimal, U256};
use polymarket_client_sdk::POLYGON;
use rust_decimal_macros::dec;
use std::str::FromStr;

pub struct TradingExecutor {
    client: Client<polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>>,
    private_key: String,
    max_order_size: Decimal,
    gtd_expiration_secs: u64,
    arbitrage_order_type: OrderType,
}

impl TradingExecutor {
    pub async fn new(private_key: String, max_order_size_usdc: f64, proxy_address: Option<Address>, gtd_expiration_secs: u64, arbitrage_order_type: OrderType) -> Result<Self> {
        let signer = LocalSigner::from_str(&private_key).map_err(|e| anyhow::anyhow!("私钥格式无效: {}", e))?.with_chain_id(Some(POLYGON));
        let config = Config::builder().use_server_time(false).build();
        let mut auth_builder = Client::new("https://clob.polymarket.com", config)?.authentication_builder(&signer);
        if let Some(funder) = proxy_address { auth_builder = auth_builder.funder(funder).signature_type(SignatureType::Proxy); }
        let client = match auth_builder.authenticate().await {
            Ok(c) => c,
            Err(e) => {
                if proxy_address.is_some() {
                    let config2 = Config::builder().use_server_time(false).build();
                    let auth_builder2 = Client::new("https://clob.polymarket.com", config2)?.authentication_builder(&signer);
                    auth_builder2.authenticate().await?
                } else {
                    return Err(e.into());
                }
            }
        };
        Ok(Self { client, private_key, proxy_address, max_order_size: Decimal::try_from(max_order_size_usdc).unwrap_or(dec!(5.0)), gtd_expiration_secs, arbitrage_order_type })
    }
    pub async fn buy_at_price(&self, token_id: U256, price: Decimal, size: Decimal) -> Result<polymarket_client_sdk::clob::types::response::PostOrderResponse> {
        let signer = LocalSigner::from_str(&self.private_key)?.with_chain_id(Some(POLYGON));
        let mut builder = self.client.limit_order().token_id(token_id).side(Side::Buy).price(price).size(size).order_type(self.arbitrage_order_type.clone());
        if let Some(funder) = self.proxy_address { builder = builder.funder(funder).maker(funder).signature_type(SignatureType::Proxy); }
        let order = builder.build().await?;
        let signed = self.client.sign(&signer, order).await?;
        Ok(self.client.post_order(signed).await?)
    }
    pub async fn sell_at_price(&self, token_id: U256, price: Decimal, size: Decimal) -> Result<polymarket_client_sdk::clob::types::response::PostOrderResponse> {
        let signer = LocalSigner::from_str(&self.private_key)?.with_chain_id(Some(POLYGON));
        let mut builder = self.client.limit_order().token_id(token_id).side(Side::Sell).price(price).size(size).order_type(self.arbitrage_order_type.clone());
        if let Some(funder) = self.proxy_address { builder = builder.funder(funder).maker(funder).signature_type(SignatureType::Proxy); }
        let order = builder.build().await?;
        let signed = self.client.sign(&signer, order).await?;
        Ok(self.client.post_order(signed).await?)
    }
}
