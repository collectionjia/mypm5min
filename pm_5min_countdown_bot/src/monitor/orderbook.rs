use dashmap::DashMap;
use futures::Stream;
use futures::StreamExt;
use polymarket_client_sdk::clob::ws::{Client as WsClient, types::response::BookUpdate};
use polymarket_client_sdk::types::{B256, U256};
use std::collections::HashMap;
use std::pin::Pin;
use tracing::info;

use crate::market::discoverer::MarketInfo;

pub struct OrderBookMonitor {
    ws_client: WsClient,
    books: DashMap<U256, BookUpdate>,
    market_map: HashMap<B256, (U256, U256)>,
}

pub struct OrderBookPair { pub yes_book: BookUpdate, pub no_book: BookUpdate, pub market_id: B256 }

impl OrderBookMonitor {
    pub fn new() -> Self { Self { ws_client: WsClient::default(), books: DashMap::new(), market_map: HashMap::new() } }
    pub fn subscribe_market(&mut self, market: &MarketInfo) -> anyhow::Result<()> {
        self.market_map.insert(market.market_id, (market.yes_token_id, market.no_token_id));
        info!("订阅市场订单簿");
        Ok(())
    }
    pub fn create_orderbook_stream(&self) -> anyhow::Result<Pin<Box<dyn Stream<Item = anyhow::Result<BookUpdate>> + Send + '_>>> {
        let token_ids: Vec<U256> = self.market_map.values().flat_map(|(yes, no)| [*yes, *no]).collect();
        if token_ids.is_empty() { return Err(anyhow::anyhow!("没有市场需要订阅")); }
        let stream = self.ws_client.subscribe_orderbook(token_ids)?;
        let stream = stream.map(|result| result.map_err(|e| anyhow::anyhow!("{}", e)));
        Ok(Box::pin(stream))
    }
    pub fn handle_book_update(&self, book: BookUpdate) -> Option<OrderBookPair> {
        self.books.insert(book.asset_id, book.clone());
        let mut yes: Option<BookUpdate> = None; let mut no: Option<BookUpdate> = None; let mut market_id: Option<B256> = None;
        for (mid, (yt, nt)) in self.market_map.iter() { if *yt == book.asset_id { yes = Some(book.clone()); market_id = Some(*mid); } if *nt == book.asset_id { no = Some(book.clone()); market_id = Some(*mid); } }
        match (yes, no, market_id) { (Some(y), Some(n), Some(m)) => Some(OrderBookPair { yes_book: y, no_book: n, market_id: m }), _ => None }
    }
}
