use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::info;

use crate::pol_rule::{calculate_pnl, rule_1_initial_probe, rule_2_trend_follow, rule_3_reversal_hedge, rule_4_time_stop, Market, Order, RuleConfig, Side, Trade};

/// 模拟器状态
pub struct Simulator {
    pub trades: Vec<Trade>,
    pub total_pnl: Decimal,
    pub trades_count: i32,
    pub running: bool,
}

impl Simulator {
    pub fn new() -> Self {
        Self {
            trades: Vec::new(),
            total_pnl: dec!(0),
            trades_count: 0,
            running: false,
        }
    }

    pub fn reset(&mut self) {
        self.trades.clear();
        self.total_pnl = dec!(0);
        self.trades_count = 0;
        self.running = false;
    }
}

impl Default for Simulator {
    fn default() -> Self {
        Self::new()
    }
}

/// 运行模拟市场
pub fn run_simulated_market(
    market: &Market,
    config: &RuleConfig,
    price_series: &[(Decimal, Decimal)],
) -> Decimal {
    let mut sim = Simulator::new();
    sim.running = true;

    info!("\n========== 开始模拟市场: {} ==========", market.slug);
    info!("问题: {}", market.question);
    info!("价格序列长度: {}", price_series.len());

    let mut step: usize = 0;
    let remaining_time = 300;

    // 规则1: 开盘试探
    let initial_orders = rule_1_initial_probe(market, config);
    for order in &initial_orders {
        sim.trades.push(Trade {
            outcome: order.outcome,
            size: order.size,
            price: order.price,
            cost: order.cost,
            time: step as i64,
        });
        sim.trades_count += 1;
        info!(
            "[模拟] 下单: {} @ ${:.2}, 金额: ${:.2}",
            order.outcome, order.price, order.cost
        );
    }

    step += 1;

    // 主循环模拟
    while sim.running && sim.trades_count < config.max_trades_per_market as i32 && step < price_series.len() {
        // 规则4: 时间停止
        let remaining = (remaining_time as i64) - (step as i64 * 5);
        let mut time_stop_logged = false;
        if rule_4_time_stop(remaining as i64, &mut time_stop_logged) {
            info!("[模拟] 时间到,退出循环");
            break;
        }

        // 获取当前价格
        let (up_price, down_price) = price_series[step];
        let current_market = Market {
            condition_id: market.condition_id.clone(),
            slug: market.slug.clone(),
            question: market.question.clone(),
            up_price,
            down_price,
            result: None,
            end_time: None,
        };

        // 规则5: 止损检查 (假设当前无结果,跳过PnL计算)
        // 规则6: 止盈检查

        // 规则2: 趋势跟随
        if let Some(order) = rule_2_trend_follow(&current_market, &sim.trades, config) {
            sim.trades.push(Trade {
                outcome: order.outcome,
                size: order.size,
                price: order.price,
                cost: order.cost,
                time: step as i64,
            });
            sim.trades_count += 1;
            info!(
                "[模拟] 趋势跟随: {} @ ${:.2}, 金额: ${:.2}",
                order.outcome, order.price, order.cost
            );
        }

        // 规则3: 反转对冲
        if sim.trades.len() >= 3 {
            if let Some(order) = rule_3_reversal_hedge(&current_market, &sim.trades, config) {
                sim.trades.push(Trade {
                    outcome: order.outcome,
                    size: order.size,
                    price: order.price,
                    cost: order.cost,
                    time: step as i64,
                });
                sim.trades_count += 1;
                info!(
                    "[模拟] 反转对冲: {} @ ${:.2}, 金额: ${:.2}",
                    order.outcome, order.price, order.cost
                );
            }
        }

        step += 1;
    }

    // 计算模拟盈亏 (假设未知结果)
    let pnl = calculate_pnl(&sim.trades, None);
    sim.total_pnl = pnl;

    info!("========== 模拟结束 ==========");
    info!("总交易次数: {}", sim.trades_count);
    info!("当前模拟PnL: ${:.2}", pnl);
    info!("\n");

    pnl
}

/// 生成模拟价格序列
pub fn generate_price_series(
    initial_up: Decimal,
    initial_down: Decimal,
    steps: usize,
    volatility: f64,
    trend: f64,
) -> Vec<(Decimal, Decimal)> {
    let mut prices = Vec::with_capacity(steps);
    let mut up = initial_up.to_string().parse::<f64>().unwrap_or(0.5);
    let mut down = 1.0 - up;

    for i in 0..steps {
        let random_change = (rand_simple(i) - 0.5) * volatility;
        let trend_change = trend * (i as f64 / steps as f64);

        up = (up + random_change + trend_change).clamp(0.01, 0.99);
        down = 1.0 - up;

        let up_dec = Decimal::try_from(up).unwrap_or(dec!(0.5));
        let down_dec = Decimal::try_from(down).unwrap_or(dec!(0.5));

        prices.push((up_dec, down_dec));
    }

    prices
}

/// 简单伪随机数生成
fn rand_simple(seed: usize) -> f64 {
    let x = ((seed as u64).wrapping_mul(1103515245).wrapping_add(12345)) % (1 << 31);
    (x as f64) / (1u64 << 31) as f64
}

/// 运行测试用例
pub fn run_test_simulation() {
    info!("\n======================================");
    info!("       运行5规则模拟测试");
    info!("======================================\n");

    let config = RuleConfig::default();

    // 测试市场
    let market = Market {
        condition_id: "0xtest123".to_string(),
        slug: "btc-updown-5m-test".to_string(),
        question: "Bitcoin will go up in next 5 minutes?".to_string(),
        up_price: dec!(0.50),
        down_price: dec!(0.50),
        result: None,
        end_time: None,
    };

    // 上涨趋势价格序列
    let up_trend = generate_price_series(dec!(0.50), dec!(0.50), 20, 0.02, 0.01);
    let pnl_up = run_simulated_market(&market, &config, &up_trend);
    info!("[测试] 上涨趋势 PnL: ${:.2}", pnl_up);

    // 震荡价格序列  
    let sideway = generate_price_series(dec!(0.50), dec!(0.50), 20, 0.05, 0.0);
    let pnl_sideway = run_simulated_market(&market, &config, &sideway);
    info!("[测试] 震荡趋势 PnL: ${:.2}", pnl_sideway);

    // 下跌趋势价格序列
    let down_trend = generate_price_series(dec!(0.50), dec!(0.50), 20, 0.02, -0.01);
    let pnl_down = run_simulated_market(&market, &config, &down_trend);
    info!("[测试] 下跌趋势 PnL: ${:.2}", pnl_down);

    info!("\n======================================");
    info!("          测试完成");
    info!("======================================\n");
}