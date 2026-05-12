use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub id: String,
    pub market_id: String,
    pub market_slug: String,
    pub side: String,
    pub order_price: f64,
    pub price: f64,
    pub size: f64,
    pub timestamp: i64,
    pub status: String,
    pub profit: Option<f64>,
}

lazy_static! {
    pub static ref TRADE_HISTORY: Arc<Mutex<VecDeque<TradeRecord>>> =
        Arc::new(Mutex::new(VecDeque::with_capacity(1000)));
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

pub fn clear_trades() {
    if let Ok(mut history) = TRADE_HISTORY.lock() {
        history.clear();
    }
}

pub fn update_trade_status(id: &str, status: &str) {
    if let Ok(mut history) = TRADE_HISTORY.lock() {
        for rec in history.iter_mut() {
            if rec.id == id {
                rec.status = status.to_string();
                break;
            }
        }
    }
}
