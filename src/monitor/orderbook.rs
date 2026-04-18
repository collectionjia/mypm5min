use anyhow::Result;
use dashmap::DashMap;
use futures::Stream;
use futures::StreamExt;
use polymarket_client_sdk::clob::ws::{types::response::{BookUpdate, MarketResolved}, Client as WsClient};
use polymarket_client_sdk::types::{B256, U256};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio::sync::RwLock;
use tracing::{debug, info, warn, error};

use crate::market::MarketInfo;

/// 缩短 B256 用于日志：保留 0x + 前 8 位 hex，如 0xb91126b7..
#[inline]
fn short_b256(b: &B256) -> String {
    let s = format!("{b}");
    if s.len() > 12 {
        format!("{}..", &s[..10])
    } else {
        s
    }
}

/// 缩短 U256 用于日志：保留末尾 8 位，如 ..67033653
#[inline]
fn short_u256(u: &U256) -> String {
    let s = format!("{u}");
    if s.len() > 12 {
        format!("..{}", &s[s.len().saturating_sub(8)..])
    } else {
        s
    }
}

pub struct OrderBookMonitor {
    ws_url: String,
    ws_client: Arc<RwLock<Option<WsClient>>>,
    books: DashMap<U256, BookUpdate>,
    market_map: HashMap<B256, (U256, U256)>, // market_id -> (yes_token_id, no_token_id)
    is_connected: Arc<AtomicBool>,
    reconnect_attempts: Arc<std::sync::atomic::AtomicU32>,
}

pub struct OrderBookPair {
    pub yes_book: BookUpdate,
    pub no_book: BookUpdate,
    pub market_id: B256,
}

impl OrderBookMonitor {
    pub fn new() -> Self {
        let ws_url = std::env::var("WS_PROXY_URL")
            .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com".to_string());
        info!(ws_url = %ws_url, "WebSocket 客户端连接地址");

        let ws_client = WsClient::new(
            &ws_url,
            polymarket_client_sdk::ws::config::Config::default(),
        )
        .expect("创建 WsClient 失败");

        Self {
            ws_url,
            ws_client: Arc::new(RwLock::new(Some(ws_client))),
            books: DashMap::new(),
            market_map: HashMap::new(),
            is_connected: Arc::new(AtomicBool::new(true)),
            reconnect_attempts: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
    }
    
    /// 尝试重新连接WebSocket
    pub async fn reconnect(&self) -> Result<()> {
        let max_attempts = 10;
        let base_delay = Duration::from_secs(1);
        
        loop {
            let attempts = self.reconnect_attempts.load(Ordering::SeqCst);
            if attempts >= max_attempts {
                error!("WebSocket重连失败，已达到最大尝试次数 {}，将持续重试", max_attempts);
                sleep(Duration::from_secs(5)).await;
                self.reconnect_attempts.store(0, Ordering::SeqCst);
                continue;
            }
            
            let delay = base_delay * (2_u64.pow(attempts.min(5) as u32)) as u32;
            warn!("WebSocket连接断开，{} 秒后尝试重连 (尝试 {}/{})", delay.as_secs(), attempts + 1, max_attempts);
            sleep(delay).await;
            
            self.reconnect_attempts.fetch_add(1, Ordering::SeqCst);
            
            match WsClient::new(&self.ws_url, polymarket_client_sdk::ws::config::Config::default()) {
                Ok(new_client) => {
                    let mut client_guard = self.ws_client.write().await;
                    *client_guard = Some(new_client);
                    self.is_connected.store(true, Ordering::SeqCst);
                    self.reconnect_attempts.store(0, Ordering::SeqCst);
                    info!("WebSocket重连成功");
                    return Ok(());
                }
                Err(e) => {
                    error!("WebSocket重连失败: {}", e);
                    continue;
                }
            }
        }
    }
    
    /// 检查连接状态
    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::SeqCst)
    }
    
    /// 标记连接断开
    pub fn mark_disconnected(&self) {
        self.is_connected.store(false, Ordering::SeqCst);
    }

    /// 订阅新市场
    pub fn subscribe_market(&mut self, market: &MarketInfo) -> Result<()> {
        // 记录市场映射
        self.market_map
            .insert(market.market_id, (market.yes_token_id, market.no_token_id));

        info!(
            market_id = short_b256(&market.market_id),
            yes = short_u256(&market.yes_token_id),
            no = short_u256(&market.no_token_id),
            "订阅市场订单簿"
        );

        Ok(())
    }

    /// 创建订单簿订阅流
    ///
    /// 注意：订单簿订阅使用未认证的 WebSocket 客户端，因为订单簿数据是公开的。
    /// 只有订阅用户相关数据（如用户订单状态、交易历史等）才需要认证。
    pub async fn create_orderbook_stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<BookUpdate>> + Send + '_>>> {
        let client_guard = self.ws_client.read().await;
        let client = client_guard.as_ref().ok_or_else(|| anyhow::anyhow!("WebSocket客户端未初始化"))?;
        
        // 收集所有需要订阅的token_id
        let token_ids: Vec<U256> = self
            .market_map
            .values()
            .flat_map(|(yes, no)| [*yes, *no])
            .collect();

        if token_ids.is_empty() {
            return Err(anyhow::anyhow!("没有市场需要订阅"));
        }

        info!(token_count = token_ids.len(), "创建订单簿订阅流（未认证）");

        // subscribe_orderbook 不需要认证，使用未认证客户端即可
        let stream = client.subscribe_orderbook(token_ids)?;
        // 将 SDK 的 Error 转换为 anyhow::Error
        let stream = stream.map(|result| result.map_err(|e| anyhow::anyhow!("{}", e)));
        Ok(Box::pin(stream))
    }

    /// 创建市场解决事件订阅流
    ///
    /// 当市场 resolved 时推送 "market_resolved" 事件，包含 winning_outcome ("Yes" 或 "No")
    /// 需要 `custom_features_enabled` 标志
    pub async fn create_market_resolutions_stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<MarketResolved>> + Send + '_>>> {
        let client_guard = self.ws_client.read().await;
        let client = client_guard.as_ref().ok_or_else(|| anyhow::anyhow!("WebSocket客户端未初始化"))?;
        
        // 收集所有需要订阅的 token_id
        let token_ids: Vec<U256> = self
            .market_map
            .values()
            .flat_map(|(yes, no)| [*yes, *no])
            .collect();

        if token_ids.is_empty() {
            return Err(anyhow::anyhow!("没有市场需要订阅"));
        }

        info!(token_count = token_ids.len(), "创建市场解决事件订阅流");

        // subscribe_market_resolutions 需要 custom_features_enabled
        let stream = client.subscribe_market_resolutions(token_ids)?;
        let stream = stream.map(|result| result.map_err(|e| anyhow::anyhow!("{}", e)));
        Ok(Box::pin(stream))
    }

    /// 处理订单簿更新
    pub fn handle_book_update(&self, book: BookUpdate) -> Option<OrderBookPair> {
        // 打印前5档买卖价格（用于调试）
        if !book.bids.is_empty() {
            let top_bids: Vec<String> = book
                .bids
                .iter()
                .take(5)
                .map(|b| format!("{}@{}", b.size, b.price))
                .collect();
            debug!(
                asset_id = %book.asset_id,
                "买盘前5档: {}",
                top_bids.join(", ")
            );
        }
        if !book.asks.is_empty() {
            let top_asks: Vec<String> = book
                .asks
                .iter()
                .take(5)
                .map(|a| format!("{}@{}", a.size, a.price))
                .collect();
            debug!(
                asset_id = short_u256(&book.asset_id),
                "卖盘前5档: {}",
                top_asks.join(", ")
            );
        }

        // 更新订单簿缓存
        self.books.insert(book.asset_id, book.clone());

        // 查找这个 token 属于哪个市场；任一侧（YES 或 NO）更新都返回 OrderBookPair，以便及时反应套利
        for (market_id, (yes_token, no_token)) in &self.market_map {
            if book.asset_id == *yes_token {
                if let Some(no_book) = self.books.get(no_token) {
                    return Some(OrderBookPair {
                        yes_book: book.clone(),
                        no_book: no_book.clone(),
                        market_id: *market_id,
                    });
                }
            } else if book.asset_id == *no_token {
                if let Some(yes_book) = self.books.get(yes_token) {
                    return Some(OrderBookPair {
                        yes_book: yes_book.clone(),
                        no_book: book.clone(),
                        market_id: *market_id,
                    });
                }
            }
        }

        None
    }

    /// 获取订单簿（如果存在）
    pub fn get_book(&self, token_id: U256) -> Option<BookUpdate> {
        self.books.get(&token_id).map(|b| b.clone())
    }

    /// 清除所有订阅
    pub fn clear(&mut self) {
        self.books.clear();
        self.market_map.clear();
    }
}
