use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use lazy_static::lazy_static;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub id: String,
    pub market_id: String,
    pub market_slug: String, // 市场名称/显示名
    pub side: String,        // "YES" or "NO"
    pub price: f64,
    pub size: f64,
    pub timestamp: i64,      // Unix timestamp
    pub status: String,      // "Pending", "Won", "Lost"
    pub profit: Option<f64>, // 盈亏金额
    #[serde(default)]
    pub buy_countdown: Option<String>,
    #[serde(default)]
    pub sell_countdown: Option<String>,
}

lazy_static! {
    pub static ref TRADE_HISTORY: Arc<Mutex<VecDeque<TradeRecord>>> = Arc::new(Mutex::new(VecDeque::with_capacity(1000)));
}

pub fn add_trade(record: TradeRecord) {
    if let Ok(mut history) = TRADE_HISTORY.lock() {
        history.push_front(record);
        // Keep last 1000 records
        if history.len() > 1000 {
            history.pop_back();
        }
    }
}

pub fn get_trades() -> Vec<TradeRecord> {
    if let Ok(history) = TRADE_HISTORY.lock() {
        history.iter().cloned().collect()
    } else {
        Vec::new()
    }
}
