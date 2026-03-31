use polymarket_client_sdk::types::B256;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::{debug, info, warn};

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
