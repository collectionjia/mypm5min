//! 5 分钟 BTC 二进制做市商策略
//! 
//! 策略逻辑：
//! 1. 规则 1: 开盘试探 - 双面挂单测试流动性
//! 2. 规则 2: 趋势跟随 - 价格移动>5% 时顺势加仓
//! 3. 规则 3: 反转对冲 - 检测到反转信号时反向小仓位对冲
//! 4. 规则 4: 时间停止 - 距离收盘 5 分钟前 30 秒停止下单
//! 5. 规则 5: 止损检查 - 亏损>100 美元时强平
//! 6. 规则 6: 止盈检查 - 盈利>60 美元时部分止盈

use crate::strategy::{MarketMakerConfig, MarketMakerState, TradeRecord};
use crate::trading::TradingExecutor;
use polymarket_client_sdk::types::B256;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{error, info};

/// 做市商订单
#[derive(Debug, Clone)]
pub struct MarketMakerOrder {
    /// 交易方向 (0=Up/YES, 1=Down/NO)
    pub side: u8,
    /// 订单大小
    pub size: Decimal,
    /// 订单价格
    pub price: Decimal,
}

/// 做市商策略管理器
pub struct MarketMakerManager {
    config: MarketMakerConfig,
    states: HashMap<B256, MarketMakerState>,
    trading_executor: Arc<TradingExecutor>,
}

impl MarketMakerManager {
    pub fn new(trading_executor: Arc<TradingExecutor>) -> Self {
        Self {
            config: MarketMakerConfig::default(),
            states: HashMap::new(),
            trading_executor,
        }
    }

    fn get_or_create_state(&mut self, market_id: &B256) -> &mut MarketMakerState {
        self.states.entry(*market_id).or_insert_with(MarketMakerState::new)
    }

    /// 规则 1: 开盘试探
    fn rule_1_initial_probe(
        config: &MarketMakerConfig,
        state: &MarketMakerState,
        up_price: Decimal,
        down_price: Decimal,
    ) -> Vec<MarketMakerOrder> {
        let mut orders = Vec::new();

        if up_price <= config.max_price {
            orders.push(MarketMakerOrder {
                side: 0,
                size: config.size,
                price: (up_price * dec!(1.02)).round_dp(2),
            });
        }

        if down_price <= config.max_price {
            orders.push(MarketMakerOrder {
                side: 1,
                size: config.size,
                price: (down_price * dec!(1.02)).round_dp(2),
            });
        }

        if !orders.is_empty() {
            info!("[模拟] UP 边第{}单 | 价格:{:.3}", orders.len(), up_price * dec!(1.02));
            info!("[模拟] DOWN 边第{}单 | 价格:{:.3}", orders.len(), down_price * dec!(1.02));
        }

        orders
    }

    /// 规则 2: 趋势跟随
    fn rule_2_trend_follow(
        config: &MarketMakerConfig,
        last_trade: &TradeRecord,
        new_price: Decimal,
    ) -> Option<MarketMakerOrder> {
        if last_trade.price == dec!(0) {
            return None;
        }

        let price_change = (new_price - last_trade.price) / last_trade.price;

        if price_change > dec!(0.05) && last_trade.side == 0 {
            return Some(MarketMakerOrder {
                side: 0,
                size: config.size,
                price: (new_price * dec!(1.01)).round_dp(2),
            });
        }

        if price_change < dec!(-0.05) && last_trade.side == 1 {
            return Some(MarketMakerOrder {
                side: 1,
                size: config.size,
                price: (new_price * dec!(0.99)).round_dp(2),
            });
        }

        None
    }

    /// 规则 3: 反转对冲
    fn rule_3_reversal_hedge(
        config: &MarketMakerConfig,
        state: &MarketMakerState,
    ) -> Option<MarketMakerOrder> {
        if state.trades.len() < 3 {
            return None;
        }

        let recent = &state.trades[state.trades.len() - 3..];
        let outcomes: Vec<u8> = recent.iter().map(|t| t.side).collect();

        if outcomes.len() >= 3 && outcomes[0] != outcomes[1] && outcomes[1] == outcomes[2] {
            let new_dir = if outcomes[2] == 0 { 1 } else { 0 };
            return Some(MarketMakerOrder {
                side: new_dir,
                size: config.size / dec!(2),
                price: dec!(0.50),
            });
        }

        None
    }

    /// 执行单个市场的做市商策略
    pub async fn run_market(
        &mut self,
        market_id: B256,
        market_title: &str,
        end_timestamp: i64,
    ) -> Decimal {
        info!("\n========== 开始市场：{} ==========", market_title);
        info!("市场 ID: {:#x}", market_id);

        {
            let state = self.get_or_create_state(&market_id);
            state.market_end_time = Some(end_timestamp);
            state.running = true;
            state.trades.clear();
        }

        let mut trades_count = 0;
        let config = self.config.clone();

        info!("[阶段 1] 开盘试探...");
        let up_price = dec!(0.50);
        let down_price = dec!(0.50);
        
        let initial_orders = {
            let state = self.get_or_create_state(&market_id);
            Self::rule_1_initial_probe(&config, state, up_price, down_price)
        };
        
        for order in initial_orders {
            if let Err(e) = self.execute_order(market_id, &order).await {
                error!("开盘试探订单失败：{}", e);
                continue;
            }

            {
                let state = self.get_or_create_state(&market_id);
                state.add_trade(TradeRecord {
                    side: order.side,
                    size: order.size,
                    price: order.price,
                    cost: order.size * order.price,
                    timestamp: chrono::Utc::now().timestamp(),
                });
            }
            trades_count += 1;
            sleep(Duration::from_secs(2)).await;
        }

        loop {
            let (should_stop, time_stop, stop_loss, take_profit) = {
                let state = self.get_or_create_state(&market_id);
                (
                    !state.running || trades_count >= config.max_trades_per_market,
                    state.check_time_stop(),
                    state.check_stop_loss(&config),
                    state.check_take_profit(&config),
                )
            };
            
            if should_stop {
                break;
            }
            
            if time_stop {
                info!("触发时间停止条件，退出主循环");
                break;
            }
            
            if stop_loss {
                info!("触发止损条件，平仓退出");
                {
                    let state = self.get_or_create_state(&market_id);
                    state.clear_all();
                }
                break;
            }
            
            if take_profit {
                info!("触发止盈条件，平半仓");
                {
                    let state = self.get_or_create_state(&market_id);
                    state.close_half_position();
                }
                break;
            }

            let trend_order = {
                let state = self.get_or_create_state(&market_id);
                if let Some(last_trade) = state.trades.last().cloned() {
                    let current_price = dec!(0.50);
                    Self::rule_2_trend_follow(&config, &last_trade, current_price)
                } else {
                    None
                }
            };
            
            if let Some(order) = trend_order {
                if let Err(e) = self.execute_order(market_id, &order).await {
                    error!("趋势跟随订单失败：{}", e);
                } else {
                    {
                        let state = self.get_or_create_state(&market_id);
                        state.add_trade(TradeRecord {
                            side: order.side,
                            size: order.size,
                            price: order.price,
                            cost: order.size * order.price,
                            timestamp: chrono::Utc::now().timestamp(),
                        });
                    }
                    trades_count += 1;
                }
            }

            let hedge_order = {
                let state = self.get_or_create_state(&market_id);
                Self::rule_3_reversal_hedge(&config, state)
            };
            
            if let Some(order) = hedge_order {
                if let Err(e) = self.execute_order(market_id, &order).await {
                    error!("反转对冲订单失败：{}", e);
                } else {
                    {
                        let state = self.get_or_create_state(&market_id);
                        state.add_trade(TradeRecord {
                            side: order.side,
                            size: order.size,
                            price: order.price,
                            cost: order.size * order.price,
                            timestamp: chrono::Utc::now().timestamp(),
                        });
                    }
                    trades_count += 1;
                }
            }

            sleep(Duration::from_millis(config.check_interval_ms)).await;
        }

        info!("等待市场结算...");
        sleep(Duration::from_secs(10)).await;

        let pnl = {
            let state = self.get_or_create_state(&market_id);
            state.calculate_pnl(&config)
        };
        
        info!("========== 市场结束，PnL: ${} ==========\n", pnl);

        pnl
    }

    async fn execute_order(&self, market_id: B256, order: &MarketMakerOrder) -> Result<(), anyhow::Error> {
        // 隐藏模拟下单日志
        Ok(())
    }
}

/// 运行所有 5 分钟 BTC 市场
pub async fn run_all_markets(
    trading_executor: Arc<TradingExecutor>,
    markets: Vec<(B256, String, i64)>,
) {
    info!("找到 {} 个 5 分钟 BTC 市场", markets.len());

    let mut manager = MarketMakerManager::new(trading_executor);
    let mut total_pnl = dec!(0);

    for (market_id, title, end_timestamp) in markets {
        let pnl = manager.run_market(market_id, &title, end_timestamp).await;
        total_pnl += pnl;
        sleep(Duration::from_secs(10)).await;
    }

    info!("所有市场结束，总 PnL: ${}", total_pnl);
}
