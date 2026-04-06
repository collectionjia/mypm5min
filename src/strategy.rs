use polymarket_client_sdk::types::B256;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::{debug, info, warn};
use chrono;

/// 策略状态机 —— 每个市场独立维护一份状态
///
/// - `0` 未操作（初始态）
/// - `1` 已买入第一腿 YES
/// - `2` 已买入第一腿 NO
/// - `4` 已卖出（终态，本轮不再操作）
/// - `9` 第一腿买入中（中间态）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum StrategyState {
    #[default]
    Idle = 0,
    FirstLegYes = 1,
    FirstLegNo = 2,
    Sold = 4,
    Buying = 9,
}

impl From<u8> for StrategyState {
    fn from(v: u8) -> Self {
        match v {
            1 => StrategyState::FirstLegYes,
            2 => StrategyState::FirstLegNo,
            4 => StrategyState::Sold,
            9 => StrategyState::Buying,
            _ => StrategyState::Idle,
        }
    }
}

impl From<StrategyState> for u8 {
    fn from(s: StrategyState) -> Self {
        s as u8
    }
}

// ---------------------------------------------------------------------------
// 策略配置参数
// ---------------------------------------------------------------------------

/// 倒计时策略的全部可调参数，集中在一处方便后续做 web 热更新。
#[derive(Debug, Clone)]
pub struct StrategyConfig {
    /// 入场触发：距窗口结束的最大秒数（如 60）
    pub entry_trigger_secs_max: i64,
    /// 入场触发：距窗口结束的最小秒数（如 40）
    pub entry_trigger_secs_min: i64,
    /// YES+NO 总价上限（过滤掉总价过高的套利机会，如 0.95）
    pub total_price_cap: Decimal,
    /// 第一腿最低触发价格（如 0.80）
    pub first_leg_trigger_price: Decimal,
    /// 止盈价格阈值（当前价 >= 此值则卖出，如 0.98）
    pub take_profit_price: Decimal,
    /// 高价格区第一腿下限（如 0.90）
    pub high_first_leg_min: Decimal,
    /// 高价格区第一腿上限（如 0.97）
    pub high_first_leg_max: Decimal,
    /// 第二腿低价区（如 0.05）
    pub second_leg_low_price: Decimal,
    /// 第二腿高价区（如 0.01）
    pub second_leg_high_price: Decimal,
    /// 每次下单最小份额
    pub min_order_size: Decimal,
    /// 默认下单金额(USDC)
    pub default_usd_amount: Decimal,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            entry_trigger_secs_max: 60,
            entry_trigger_secs_min: 40,
            total_price_cap: dec!(0.95),
            first_leg_trigger_price: dec!(0.80),
            take_profit_price: dec!(0.98),
            high_first_leg_min: dec!(0.90),
            high_first_leg_max: dec!(0.97),
            second_leg_low_price: dec!(0.05),
            second_leg_high_price: dec!(0.01),
            min_order_size: dec!(0.01),
            default_usd_amount: dec!(1.0),
        }
    }
}

// ---------------------------------------------------------------------------
// 第一腿持仓记录
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct FirstLegRecord {
    /// 买入价格
    pub buy_price: Decimal,
    /// 买入数量
    pub qty: Decimal,
    /// 买入方向：0 = YES, 1 = NO
    pub side_key: u8,
}

// ---------------------------------------------------------------------------
// 回撤触发遮罩
// ---------------------------------------------------------------------------

/// 在特定秒数节点（55/50/45/40）各触发一次回撤检查，已触发则标记不再重复。
/// bit0=55s, bit1=50s, bit2=45s, bit3=40s
pub struct DrawdownMask(u8);

impl DrawdownMask {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn from_u8(v: u8) -> Self {
        Self(v)
    }

    pub fn to_u8(&self) -> u8 {
        self.0
    }

    pub fn mark_triggered(&mut self, threshold_secs: i64) {
        let bit = match threshold_secs {
            55 => 1u8,
            50 => 2,
            45 => 4,
            40 => 8,
            _ => return,
        };
        self.0 |= bit;
    }

    pub fn is_triggered(&self, threshold_secs: i64) -> bool {
        let bit = match threshold_secs {
            55 => 1u8,
            50 => 2,
            45 => 4,
            40 => 8,
            _ => return true,
        };
        self.0 & bit != 0
    }

    /// 检查是否在 prev_sec -> cur_sec 跨过了某个检查点，返回 (should_check, new_mask)
    pub fn check_transition(prev_sec: i64, cur_sec: i64, current_mask: u8) -> (bool, u8) {
        let mut mask = current_mask;
        let mut should_check = false;
        for (t, bit) in [(55i64, 1u8), (50i64, 2u8), (45i64, 4u8), (40i64, 8u8)] {
            if mask & bit != 0 {
                continue;
            }
            if prev_sec > t && cur_sec <= t {
                mask |= bit;
                should_check = true;
            }
        }
        (should_check, mask)
    }
}

// ---------------------------------------------------------------------------
// 策略引擎：集中管理所有市场的策略状态
// ---------------------------------------------------------------------------

pub struct StrategyEngine {
    /// 每个市场的策略状态
    pub state: HashMap<B256, StrategyState>,
    /// 第一腿持仓信息
    pub first_leg: HashMap<B256, FirstLegRecord>,
    /// 回撤触发遮罩
    pub drawdown_mask: HashMap<B256, u8>,
    /// 上一拍的秒数（用于检测穿越检查点）
    pub drawdown_last_sec: HashMap<B256, i64>,
    /// 连续亏损计数
    pub losing_streak_count: std::sync::atomic::AtomicU64,
}

impl StrategyEngine {
    pub fn new() -> Self {
        Self {
            state: HashMap::new(),
            first_leg: HashMap::new(),
            drawdown_mask: HashMap::new(),
            drawdown_last_sec: HashMap::new(),
            losing_streak_count: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// 获取市场策略状态
    pub fn get_state(&self, market_id: &B256) -> StrategyState {
        self.state
            .get(market_id)
            .copied()
            .unwrap_or(StrategyState::Idle)
    }

    /// 设置市场策略状态
    pub fn set_state(&mut self, market_id: B256, state: StrategyState) {
        self.state.insert(market_id, state);
    }

    /// 记录第一腿买入信息
    pub fn record_first_leg(
        &mut self,
        market_id: B256,
        price: Decimal,
        qty: Decimal,
        side_key: u8,
    ) {
        self.first_leg.insert(
            market_id,
            FirstLegRecord {
                buy_price: price,
                qty,
                side_key,
            },
        );
    }

    /// 获取第一腿持仓信息
    pub fn get_first_leg(&self, market_id: &B256) -> Option<&FirstLegRecord> {
        self.first_leg.get(market_id)
    }

    /// 清除第一腿记录
    pub fn clear_first_leg(&mut self, market_id: &B256) {
        self.first_leg.remove(market_id);
        self.drawdown_mask.remove(market_id);
        self.drawdown_last_sec.remove(market_id);
    }

    /// 更新回撤遮罩并检查是否需要触发回撤检查
    /// 返回 (should_check, new_mask)
    pub fn update_drawdown(
        &mut self,
        market_id: B256,
        cur_sec: i64,
    ) -> (bool, u8) {
        let prev = self
            .drawdown_last_sec
            .insert(market_id, cur_sec)
            .unwrap_or(cur_sec + 1000);
        let mask = self
            .drawdown_mask
            .get(&market_id)
            .copied()
            .unwrap_or(0u8);
        let (should_check, new_mask) = DrawdownMask::check_transition(prev, cur_sec, mask);
        if new_mask != mask {
            self.drawdown_mask.insert(market_id, new_mask);
        }
        (should_check, new_mask)
    }

    /// 重置连输计数为 1（赢后重置）
    pub fn reset_losing_streak(&self) {
        self.losing_streak_count
            .store(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// 递增连输计数
    pub fn increment_losing_streak(&self) {
        self.losing_streak_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// 获取连输计数（至少返回 1）
    pub fn get_losing_streak(&self) -> u64 {
        self.losing_streak_count
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(1)
    }

    /// 计算本次下单金额 = 连输计数(USDC)，最低 $1
    pub fn calc_order_usd(&self) -> Decimal {
        let streak = self.get_losing_streak();
        Decimal::from(streak).max(dec!(1))
    }

    /// 标记市场已卖出并清理持仓
    pub fn mark_sold(&mut self, market_id: &B256) {
        self.state.insert(*market_id, StrategyState::Sold);
        self.clear_first_leg(market_id);
    }
}

// ---------------------------------------------------------------------------
// 入场信号判定
// ---------------------------------------------------------------------------

/// 根据当前 YES/NO 卖一价，判断是否触发入场信号。
/// 返回 `Some(side_key)`: 0=YES, 1=NO; `None`: 未触发。
pub fn check_entry_signal(
    yes_ask: Option<Decimal>,
    no_ask: Option<Decimal>,
    trigger_price: Decimal,
) -> Option<u8> {
    match (yes_ask, no_ask) {
        (Some(yp), Some(np)) => {
            let yes_hit = yp >= trigger_price;
            let no_hit = np >= trigger_price;
            if yes_hit && no_hit {
                Some(if yp >= np { 0u8 } else { 1u8 })
            } else if yes_hit {
                Some(0u8)
            } else if no_hit {
                Some(1u8)
            } else {
                None
            }
        }
        (Some(yp), None) => {
            if yp >= trigger_price {
                Some(0u8)
            } else {
                None
            }
        }
        (None, Some(np)) => {
            if np >= trigger_price {
                Some(1u8)
            } else {
                None
            }
        }
        (None, None) => None,
    }
}

// ---------------------------------------------------------------------------
// 止盈 / 回撤判定
// ---------------------------------------------------------------------------

/// 止盈 / 回撤卖出判定结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SellSignal {
    /// 止盈卖出
    TakeProfit,
    /// 回撤卖出
    Drawdown,
}

/// 根据买入价和当前价，判断是否应该卖出
pub fn check_sell_signal(
    buy_price: Decimal,
    current_price: Decimal,
    take_profit_price: Decimal,
) -> Option<SellSignal> {
    let should_take_profit = current_price >= take_profit_price;
    let should_drawdown = current_price < buy_price;
    if should_take_profit {
        Some(SellSignal::TakeProfit)
    } else if should_drawdown {
        Some(SellSignal::Drawdown)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

/// 买入份额计算：(usd / price * 100).floor() / 100，最低 min_size
pub fn calc_buy_size(usd_amount: Decimal, price: Decimal, min_size: Decimal) -> Decimal {
    let size = (usd_amount / price * dec!(100.0)).floor() / dec!(100.0);
    size.max(min_size)
}

/// 获取买入方向名称
pub fn side_key_name(side_key: u8) -> &'static str {
    if side_key == 0 {
        "YES"
    } else {
        "NO"
    }
}

// ============================================================================
// 5 分钟 BTC 二进制做市商策略
// ============================================================================

/// 做市商策略配置
#[derive(Debug, Clone)]
pub struct MarketMakerConfig {
    /// 标准份额 (USDC)
    pub size: Decimal,
    /// 单市场最大交易次数
    pub max_trades_per_market: usize,
    /// 单市场最大投入 (USDC)
    pub max_cost_per_market: Decimal,
    /// 最低买入价格
    pub min_price: Decimal,
    /// 最高买入价格
    pub max_price: Decimal,
    /// 价格步长
    pub price_step: Decimal,
    /// 单市场止损 (USDC)
    pub stop_loss: Decimal,
    /// 单市场止盈 (USDC)
    pub take_profit: Decimal,
    /// 总持仓上限 (USDC)
    pub max_position: Decimal,
    /// 检查间隔 (毫秒)
    pub check_interval_ms: u64,
    /// 最大持仓时间 (秒)
    pub max_hold_time_secs: i64,
}

impl Default for MarketMakerConfig {
    fn default() -> Self {
        Self {
            size: dec!(40),
            max_trades_per_market: 30,
            max_cost_per_market: dec!(500),
            min_price: dec!(0.30),
            max_price: dec!(0.70),
            price_step: dec!(0.01),
            stop_loss: dec!(-100),
            take_profit: dec!(60),
            max_position: dec!(1000),
            check_interval_ms: 500,
            max_hold_time_secs: 300,
        }
    }
}

/// 交易记录
#[derive(Debug, Clone)]
pub struct TradeRecord {
    /// 交易方向 (0=YES/Up, 1=NO/Down)
    pub side: u8,
    /// 交易数量
    pub size: Decimal,
    /// 成交价格
    pub price: Decimal,
    /// 成本
    pub cost: Decimal,
    /// 交易时间戳 (秒)
    pub timestamp: i64,
}

/// 做市商策略状态
#[derive(Debug, Clone, Default)]
pub struct MarketMakerState {
    /// 交易记录
    pub trades: Vec<TradeRecord>,
    /// 总盈亏
    pub total_pnl: Decimal,
    /// 今日投入
    pub daily_cost: Decimal,
    /// 是否运行中
    pub running: bool,
    /// 市场结束时间戳
    pub market_end_time: Option<i64>,
    /// 市场结果 (0=Up, 1=Down, -1=未结算)
    pub market_result: i32,
}

impl MarketMakerState {
    pub fn new() -> Self {
        Self {
            trades: Vec::new(),
            total_pnl: dec!(0),
            daily_cost: dec!(0),
            running: false,
            market_end_time: None,
            market_result: -1,
        }
    }

    /// 计算当前盈亏
    pub fn calculate_pnl(&self, config: &MarketMakerConfig) -> Decimal {
        let total_cost: Decimal = self.trades.iter().map(|t| t.cost).sum();
        
        // 统计各方向的持仓
        let up_shares: Decimal = self.trades
            .iter()
            .filter(|t| t.side == 0)
            .map(|t| t.size)
            .sum();
        let down_shares: Decimal = self.trades
            .iter()
            .filter(|t| t.side == 1)
            .map(|t| t.size)
            .sum();
        
        // 根据持仓方向和市场结果计算盈亏
        if up_shares > down_shares {
            // 看涨方向
            let settlement = if self.market_result == 0 { dec!(1) } else { dec!(0) };
            let value = up_shares * settlement;
            value - total_cost
        } else if down_shares > up_shares {
            // 看跌方向
            let settlement = if self.market_result == 1 { dec!(1) } else { dec!(0) };
            let value = down_shares * settlement;
            value - total_cost
        } else {
            // 持仓相等或为空
            -total_cost
        }
    }

    /// 检查是否触发止损
    pub fn check_stop_loss(&self, config: &MarketMakerConfig) -> bool {
        let current_pnl = self.calculate_pnl(config);
        current_pnl <= config.stop_loss
    }

    /// 检查是否触发止盈
    pub fn check_take_profit(&self, config: &MarketMakerConfig) -> bool {
        let current_pnl = self.calculate_pnl(config);
        current_pnl >= config.take_profit
    }

    /// 检查是否应该停止下单（时间条件）
    pub fn check_time_stop(&self) -> bool {
        if let Some(end_time) = self.market_end_time {
            let now = chrono::Utc::now().timestamp();
            let remaining = end_time - now;
            remaining <= 30
        } else {
            false
        }
    }

    /// 获取总交易次数
    pub fn trade_count(&self) -> usize {
        self.trades.len()
    }

    /// 获取总成本
    pub fn total_cost(&self) -> Decimal {
        self.trades.iter().map(|t| t.cost).sum()
    }

    /// 添加交易记录
    pub fn add_trade(&mut self, trade: TradeRecord) {
        self.trades.push(trade);
    }

    /// 清空所有持仓
    pub fn clear_all(&mut self) {
        self.trades.clear();
    }

    /// 平掉一半仓位
    pub fn close_half_position(&mut self) {
        let half_len = self.trades.len() / 2;
        self.trades.truncate(half_len);
    }
}

/// 做市商策略引擎
pub struct MarketMakerEngine {
    /// 配置
    pub config: MarketMakerConfig,
    /// 各市场状态
    pub states: std::collections::HashMap<B256, MarketMakerState>,
}

impl MarketMakerEngine {
    pub fn new(config: MarketMakerConfig) -> Self {
        Self {
            config,
            states: std::collections::HashMap::new(),
        }
    }

    /// 获取或创建市场状态
    pub fn get_or_create_state(&mut self, market_id: &B256) -> &mut MarketMakerState {
        use std::collections::hash_map::Entry;
        match self.states.entry(*market_id) {
            Entry::Vacant(e) => e.insert(MarketMakerState::new()),
            Entry::Occupied(e) => e.into_mut(),
        }
    }

    /// 规则 1: 开盘试探
    /// 双面挂单测试流动性
    pub fn rule_1_initial_probe(
        &self,
        state: &MarketMakerState,
        up_price: Decimal,
        down_price: Decimal,
    ) -> Vec<MarketMakerOrder> {
        let mut orders = Vec::new();

        // 挂 UP 单
        if up_price <= self.config.max_price {
            orders.push(MarketMakerOrder {
                side: 0, // Up/YES
                size: self.config.size,
                price: (up_price * dec!(1.02)).round_dp(2), // 稍微提价优先成交
            });
        }

        // 挂 DOWN 单
        if down_price <= self.config.max_price {
            orders.push(MarketMakerOrder {
                side: 1, // Down/NO
                size: self.config.size,
                price: (down_price * dec!(1.02)).round_dp(2),
            });
        }

        if !orders.is_empty() {
            info!(
                "[规则 1] 开盘试探：UP@{}, DOWN@{}",
                up_price, down_price
            );
        }

        orders
    }

    /// 规则 2: 趋势跟随
    /// 价格移动>5% 时顺势加仓
    pub fn rule_2_trend_follow(
        &self,
        state: &MarketMakerState,
        last_trade: &TradeRecord,
        new_price: Decimal,
    ) -> Option<MarketMakerOrder> {
        if last_trade.price == dec!(0) {
            return None;
        }

        let price_change = (new_price - last_trade.price) / last_trade.price;

        // 价格上涨且持仓 UP → 加仓 UP
        if price_change > dec!(0.05) && last_trade.side == 0 {
            info!("[规则 2] 趋势 UP: +{:.1}%", price_change * dec!(100));
            return Some(MarketMakerOrder {
                side: 0,
                size: self.config.size,
                price: (new_price * dec!(1.01)).round_dp(2),
            });
        }

        // 价格下跌且持仓 DOWN → 加仓 DOWN
        if price_change < dec!(-0.05) && last_trade.side == 1 {
            info!("[规则 2] 趋势 DOWN: {:.1}%", price_change * dec!(100));
            return Some(MarketMakerOrder {
                side: 1,
                size: self.config.size,
                price: (new_price * dec!(0.99)).round_dp(2),
            });
        }

        None
    }

    /// 规则 3: 反转对冲
    /// 检测到反转信号时反向小仓位对冲
    pub fn rule_3_reversal_hedge(
        &self,
        state: &MarketMakerState,
    ) -> Option<MarketMakerOrder> {
        if state.trades.len() < 3 {
            return None;
        }

        // 获取最近 3 笔
        let recent = &state.trades[state.trades.len() - 3..];
        let outcomes: Vec<u8> = recent.iter().map(|t| t.side).collect();

        // 连续同方向 3 笔后反转
        if outcomes[0] != outcomes[1] && outcomes[1] == outcomes[2] {
            let new_dir = if outcomes[2] == 0 { 1 } else { 0 };
            let dir_name = if new_dir == 0 { "Up" } else { "Down" };
            
            info!("[规则 3] 反转对冲：{}", dir_name);
            return Some(MarketMakerOrder {
                side: new_dir,
                size: self.config.size / dec!(2), // 半仓对冲
                price: dec!(0.50), // 需要传入实际价格
            });
        }

        None
    }

    /// 规则 4: 时间停止
    /// 距离收盘 5 分钟前 30 秒停止下单
    pub fn rule_4_time_stop(&self, state: &MarketMakerState) -> bool {
        if state.check_time_stop() {
            info!("[规则 4] 时间到，停止下单");
            true
        } else {
            false
        }
    }

    /// 规则 5: 止损检查
    /// 亏损>100 美元时强平
    pub fn rule_5_stop_loss(&self, state: &MarketMakerState) -> bool {
        if state.check_stop_loss(&self.config) {
            let pnl = state.calculate_pnl(&self.config);
            info!("[规则 5] 触发止损：${}", pnl);
            true
        } else {
            false
        }
    }

    /// 规则 6: 止盈检查
    /// 盈利>60 美元时部分止盈
    pub fn rule_6_take_profit(&self, state: &MarketMakerState) -> bool {
        if state.check_take_profit(&self.config) {
            let pnl = state.calculate_pnl(&self.config);
            info!("[规则 6] 触发止盈：+${}", pnl);
            true
        } else {
            false
        }
    }
}

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
