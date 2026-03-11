use dashmap::{DashMap, DashSet};
use reqwest::Client;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpotTrend {
    Up,
    Down,
    Flat,
}

#[derive(Clone)]
pub struct SpotOracle {
    client: Client,
    history: DashMap<String, VecDeque<(i64, f64)>>,
}

impl SpotOracle {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            history: DashMap::new(),
        }
    }

    pub async fn run(self: Arc<Self>, active_symbols: Arc<DashSet<String>>) {
        loop {
            let symbols: Vec<String> = active_symbols.iter().map(|s| s.key().clone()).collect();
            for sym in symbols {
                if let Some(price) = self.fetch_price(&sym).await {
                    let ts = chrono::Utc::now().timestamp();
                    self.push_price(&sym, ts, price);
                }
            }
            sleep(Duration::from_secs(2)).await;
        }
    }

    pub fn trend(&self, symbol: &str) -> Option<SpotTrend> {
        let hist = self.history.get(symbol)?;
        if hist.len() < 8 {
            return None;
        }

        let first = hist.front()?.1;
        let last = hist.back()?.1;
        if first <= 0.0 || last <= 0.0 {
            return None;
        }

        let pct_change = (last - first) / first;

        let mut running_max = first;
        let mut max_drawdown = 0.0;
        for (_, p) in hist.iter() {
            if *p > running_max {
                running_max = *p;
            } else if running_max > 0.0 {
                let dd = (running_max - *p) / running_max;
                if dd > max_drawdown {
                    max_drawdown = dd;
                }
            }
        }

        let mut running_min = first;
        let mut max_drawup = 0.0;
        for (_, p) in hist.iter() {
            if *p < running_min {
                running_min = *p;
            } else if running_min > 0.0 {
                let du = (*p - running_min) / running_min;
                if du > max_drawup {
                    max_drawup = du;
                }
            }
        }

        let up = pct_change >= 0.0015 && max_drawdown <= 0.0006;
        let down = pct_change <= -0.0015 && max_drawup <= 0.0006;

        if up {
            Some(SpotTrend::Up)
        } else if down {
            Some(SpotTrend::Down)
        } else {
            Some(SpotTrend::Flat)
        }
    }

    pub fn latest_price(&self, symbol: &str) -> Option<f64> {
        self.history.get(symbol).and_then(|h| h.back().map(|x| x.1))
    }

    fn push_price(&self, symbol: &str, ts: i64, price: f64) {
        let mut entry = self.history.entry(symbol.to_string()).or_insert_with(VecDeque::new);
        entry.push_back((ts, price));
        while entry.len() > 40 {
            entry.pop_front();
        }
    }

    async fn fetch_price(&self, symbol: &str) -> Option<f64> {
        if let Some(p) = self.fetch_binance(symbol).await {
            return Some(p);
        }
        self.fetch_coinbase(symbol).await
    }

    async fn fetch_binance(&self, symbol: &str) -> Option<f64> {
        #[derive(serde::Deserialize)]
        struct Resp {
            price: String,
        }

        let sym = format!("{}USDT", symbol.to_uppercase());
        let url = format!("https://api.binance.com/api/v3/ticker/price?symbol={}", sym);
        let resp = self.client.get(url).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let body: Resp = resp.json().await.ok()?;
        body.price.parse::<f64>().ok()
    }

    async fn fetch_coinbase(&self, symbol: &str) -> Option<f64> {
        #[derive(serde::Deserialize)]
        struct Resp {
            price: String,
        }

        let product = format!("{}-USD", symbol.to_uppercase());
        let url = format!("https://api.exchange.coinbase.com/products/{}/ticker", product);
        let resp = self.client.get(url).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let body: Resp = resp.json().await.ok()?;
        body.price.parse::<f64>().ok()
    }
}

