use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use tracing::info;

/// 交易方向
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Up,
    Down,
}

impl Side {
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Up => "Up",
            Side::Down => "Down",
        }
    }
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// 订单结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub outcome: Side,
    pub size: Decimal,
    pub price: Decimal,
    pub cost: Decimal,
    pub timestamp: i64,
}

/// 交易记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub outcome: Side,
    pub size: Decimal,
    pub price: Decimal,
    pub cost: Decimal,
    pub time: i64,
}

/// 市场状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub condition_id: String,
    pub slug: String,
    pub question: String,
    pub up_price: Decimal,
    pub down_price: Decimal,
    pub result: Option<Side>,
    pub end_time: Option<i64>,
}

/// 规则配置
#[derive(Debug, Clone)]
pub struct RuleConfig {
    pub size: Decimal,
    pub max_trades_per_market: i32,
    pub max_cost_per_market: Decimal,
    pub min_price: Decimal,
    pub max_price: Decimal,
    pub price_step: Decimal,
    pub stop_loss: Decimal,
    pub take_profit: Decimal,
    pub max_position: Decimal,
    pub check_interval: i64,
    pub max_hold_time: i64,
    pub trend_threshold: Decimal,
}

impl Default for RuleConfig {
    fn default() -> Self {
        Self {
            size: dec!(5),
            max_trades_per_market: 50,
            max_cost_per_market: dec!(2000),
            min_price: dec!(0.20),
            max_price: dec!(0.80),
            price_step: dec!(0.01),
            stop_loss: dec!(-100),
            take_profit: dec!(200),
            max_position: dec!(1000),
            check_interval: 500,
            max_hold_time: 300,
            trend_threshold: dec!(0.05),
        }
    }
}

/// 规则1: 开盘试探
/// 双面挂单测试流动性
pub fn rule_1_initial_probe(market: &Market, config: &RuleConfig) -> Vec<Order> {
    let mut orders = Vec::new();

    // 如果一边价格超过0.98，就不进行投注了
    if market.up_price > dec!(0.98) || market.down_price > dec!(0.98) {
        info!(
            "[规则1] 价格超过0.98，不进行投注: UP@{:.2}, DOWN@{:.2}",
            market.up_price, market.down_price
        );
        return orders;
    }

    // 确定价格较小的一边
    let low_side = if market.up_price < market.down_price {
        Side::Up
    } else {
        Side::Down
    };

    let low_price = if low_side == Side::Up {
        market.up_price
    } else {
        market.down_price
    };

    // 只下较小的一边
    if low_price <= config.max_price {
        let order_price = (low_price * dec!(1.02)).round_dp(2);
        
        orders.push(Order {
            outcome: low_side,
            size: dec!(5), // size都是5个
            price: order_price,
            cost: dec!(5) * order_price,
            timestamp: 0,
        });
        
        info!(
            "[规则1] 只下较小的一边: {:?}=@{:.2}",
            low_side, order_price
        );
    }

    orders
}

/// 规则2: 对边下单
/// 对边如果小于较小的一边就下单
pub fn rule_2_trend_follow(
    market: &Market,
    trades: &[Trade],
    config: &RuleConfig,
) -> Option<Order> {
    // 确定价格较小的一边
    let low_side = if market.up_price < market.down_price {
        Side::Up
    } else {
        Side::Down
    };
    
    let low_price = if low_side == Side::Up {
        market.up_price
    } else {
        market.down_price
    };
    
    // 确定对边
    let opposite_side = if low_side == Side::Up {
        Side::Down
    } else {
        Side::Up
    };
    
    let opposite_price = if opposite_side == Side::Up {
        market.up_price
    } else {
        market.down_price
    };
    
    // 对边如果小于较小的一边就下单
    if opposite_price < low_price {
        let order_price = (opposite_price * dec!(0.99)).round_dp(2);
        
        let order = Order {
            outcome: opposite_side,
            size: dec!(5), // size都是5个
            price: order_price,
            cost: dec!(5) * order_price,
            timestamp: 0,
        };
        
        info!(
            "[规则2] 对边下单: {:?}=@{:.2} (较小边: {:?}=@{:.2})",
            opposite_side, order_price, low_side, low_price
        );
        return Some(order);
    }

    None
}

/// 规则3: 反转对冲
/// 检测到反转信号时反向小仓位对冲
pub fn rule_3_reversal_hedge(market: &Market, trades: &[Trade], config: &RuleConfig) -> Option<Order> {
    if trades.len() < 3 {
        return None;
    }

    let outcomes: Vec<_> = trades.iter().rev().take(3).map(|t| t.outcome).collect();
    if outcomes.len() < 3 {
        return None;
    }

    if outcomes[0] != outcomes[1] && outcomes[1] == outcomes[2] {
        let new_dir = if outcomes[2] == Side::Down {
            Side::Up
        } else {
            Side::Down
        };

        let price = match new_dir {
            Side::Up => market.up_price,
            Side::Down => market.down_price,
        };

        let order = Order {
            outcome: new_dir,
            size: config.size / dec!(2),
            price,
            cost: (config.size / dec!(2)) * price,
            timestamp: 0,
        };
        info!("[规则3] 反转对冲: {}", new_dir);
        return Some(order);
    }

    None
}

/// 规则4: 时间停止检查
/// 距离开盘5分钟前30秒停止下单
pub fn rule_4_time_stop(remaining_secs: i64, time_stop_logged: &mut bool) -> bool {
    if remaining_secs <= 30 {
        if !*time_stop_logged {
            info!("[5规则策略] 时间到,停止下单,剩余{}秒", remaining_secs);
            *time_stop_logged = true;
        }
        return true;
    }
    false
}

/// 规则5: 止损检查
/// 亏损>100美元时强平
pub fn rule_5_stop_loss(pnl: Decimal, config: &RuleConfig) -> bool {
    if pnl <= config.stop_loss {
        info!("[规则5] 触发止损: ${:.2}", pnl);
        return true;
    }
    false
}

/// 规则6: 止盈检查
/// 盈利>200美元时部分止盈
pub fn rule_6_take_profit(pnl: Decimal, config: &RuleConfig) -> bool {
    if pnl >= config.take_profit {
        info!("[规则6] 触发止盈: +${:.2}", pnl);
        return true;
    }
    false
}

/// 计算当前盈亏
pub fn calculate_pnl(trades: &[Trade], result: Option<Side>) -> Decimal {
    let total_cost: Decimal = trades.iter().map(|t| t.cost).sum();

    let up_shares: Decimal = trades
        .iter()
        .filter(|t| t.outcome == Side::Up)
        .map(|t| t.size)
        .sum();
    let down_shares: Decimal = trades
        .iter()
        .filter(|t| t.outcome == Side::Down)
        .map(|t| t.size)
        .sum();

    let result = match result {
        Some(r) => r,
        None => return dec!(0),
    };

    if up_shares > down_shares {
        let settlement = if result == Side::Up { dec!(1) } else { dec!(0) };
        up_shares * settlement - total_cost
    } else {
        let settlement = if result == Side::Down { dec!(1) } else { dec!(0) };
        down_shares * settlement - total_cost
    }
}

/// 5规则策略状态
#[derive(Debug, Clone)]
pub struct RuleStrategyState {
    pub trades: Vec<Trade>,
    pub trades_count: i32,
    pub total_cost: Decimal,
    pub start_time: i64,
    pub time_stop_logged: bool, // 标记是否已打印时间停止日志
    pub max_trades_logged: bool, // 标记是否已打印最大交易次数停止日志
    pub last_rule2_time: i64, // 上一次执行规则2的时间（秒）
}

impl RuleStrategyState {
    pub fn new() -> Self {
        Self {
            trades: Vec::new(),
            trades_count: 0,
            total_cost: dec!(0),
            start_time: 0,
            time_stop_logged: false,
            max_trades_logged: false,
            last_rule2_time: 0,
        }
    }

    pub fn reset(&mut self) {
        self.trades.clear();
        self.trades_count = 0;
        self.total_cost = dec!(0);
        self.start_time = 0;
        self.time_stop_logged = false;
        self.max_trades_logged = false;
        self.last_rule2_time = 0;
    }

    pub fn add_trade(&mut self, trade: Trade) {
        self.total_cost += trade.cost;
        self.trades.push(trade);
        self.trades_count += 1;
    }
}

impl Default for RuleStrategyState {
    fn default() -> Self {
        Self::new()
    }
}

/// 执行5规则策略主循环
/// 
/// 参数:
/// - market: 市场信息
/// - config: 规则配置
/// - state: 策略状态（会被修改）
/// - up_price: 当前Up价格
/// - down_price: 当前Down价格
/// - elapsed_secs: 已运行秒数
/// - remaining_secs: 剩余秒数
/// 
/// 返回: (should_continue, orders_to_execute)
/// - should_continue: 是否继续运行
/// - orders_to_execute: 需要执行的订单列表
pub fn execute_rules(
    market: &Market,
    config: &RuleConfig,
    state: &mut RuleStrategyState,
    up_price: Decimal,
    down_price: Decimal,
    elapsed_secs: i64,
    remaining_secs: i64,
) -> (bool, Vec<Order>) {
    let mut orders_to_execute = Vec::new();

    // 更新市场价格
    let current_market = Market {
        condition_id: market.condition_id.clone(),
        slug: market.slug.clone(),
        question: market.question.clone(),
        up_price,
        down_price,
        result: None,
        end_time: None,
    };

    // 规则6: 止盈检查
    let current_pnl = calculate_pnl(&state.trades, None);
    if rule_6_take_profit(current_pnl, config) {
        info!("[5规则策略] 触发止盈，停止交易");
        return (false, orders_to_execute);
    }

    // 检查最大交易次数
    if state.trades_count >= config.max_trades_per_market {
        if !state.max_trades_logged {
            info!("[5规则策略] 达到最大交易次数，停止交易");
            state.max_trades_logged = true;
        }
        return (false, orders_to_execute);
    }

    // 检查最大投入
    if state.total_cost >= config.max_cost_per_market {
        if !state.max_trades_logged {
            info!("[5规则策略] 达到最大投入金额，停止交易  {} USDT", config.max_cost_per_market);
            state.max_trades_logged = true;
        }
        return (false, orders_to_execute);
    }

    // 首次运行：规则1 开盘试探
    if state.trades.is_empty() {
        info!("[5规则策略] 首次运行，执行规则1: 开盘试探");
        let initial_orders = rule_1_initial_probe(&current_market, config);
        for order in initial_orders {
            state.add_trade(Trade {
                outcome: order.outcome,
                size: order.size,
                price: order.price,
                cost: order.cost,
                time: elapsed_secs,
            });
            orders_to_execute.push(order);
        }
        
        // 计算并打印统计信息
        let total_size: Decimal = state.trades.iter().map(|t| t.size).sum();
        let total_cost = state.total_cost;
        let avg_cost = if total_size > dec!(0) {
            total_cost / total_size
        } else {
            dec!(0)
        };
        info!("[5规则策略] 统计信息: 总成本: ${:.2}, 平均成本: ${:.4}, 总数量: {:.2}", 
            total_cost, avg_cost, total_size);
            
        return (true, orders_to_execute);
    }

    // 规则2: 趋势跟随（每秒钟最多执行一次）
    if elapsed_secs > state.last_rule2_time {
        if let Some(order) = rule_2_trend_follow(&current_market, &state.trades, config) {
            state.add_trade(Trade {
                outcome: order.outcome,
                size: order.size,
                price: order.price,
                cost: order.cost,
                time: elapsed_secs,
            });
            orders_to_execute.push(order);
            state.last_rule2_time = elapsed_secs;
        }
    }



    (true, orders_to_execute)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_1_initial_probe() {
        let config = RuleConfig::default();
        let market = Market {
            condition_id: "0xtest".to_string(),
            slug: "btc-updown-5m-test".to_string(),
            question: "Bitcoin Up or Down?".to_string(),
            up_price: dec!(0.50),
            down_price: dec!(0.50),
            result: None,
            end_time: None,
        };

        let orders = rule_1_initial_probe(&market, &config);
        assert_eq!(orders.len(), 2);
    }

    #[test]
    fn test_rule_5_stop_loss() {
        let config = RuleConfig::default();
        assert!(rule_5_stop_loss(dec!(-150), &config));
        assert!(!rule_5_stop_loss(dec!(-50), &config));
    }

    #[test]
    fn test_rule_6_take_profit() {
        let config = RuleConfig::default();
        assert!(rule_6_take_profit(dec!(250), &config));
        assert!(!rule_6_take_profit(dec!(100), &config));
    }
}