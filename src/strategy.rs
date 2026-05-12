use polymarket_client_sdk::types::B256;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// 追踪止损策略状态机
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum StrategyState {
    /// 初始状态，等待入场
    #[default]
    Idle = 0,
    /// 第一单 split 已下单（初始入场）
    FirstLeg = 1,
    /// 第二单已下单（跌到 DROP_THRESHOLD 时）
    SecondLeg = 2,
    /// 触发止盈（涨到 TAKE_PROFIT_THRESHOLD 时）
    TakeProfit = 3,
    /// 准备平仓（4分钟时）
    WindDown = 4,
    /// 策略结束
    Done = 5,
}

impl From<u8> for StrategyState {
    fn from(v: u8) -> Self {
        match v {
            0 => StrategyState::Idle,
            1 => StrategyState::FirstLeg,
            2 => StrategyState::SecondLeg,
            3 => StrategyState::TakeProfit,
            4 => StrategyState::WindDown,
            5 => StrategyState::Done,
            _ => StrategyState::Idle,
        }
    }
}

impl From<StrategyState> for u8 {
    fn from(s: StrategyState) -> Self {
        s as u8
    }
}

/// 持仓记录
#[derive(Debug, Clone, Copy)]
pub struct PositionRecord {
    pub yes_buy_price: Decimal,
    pub yes_qty: Decimal,
    pub yes_token_id: B256,
    pub no_buy_price: Decimal,
    pub no_qty: Decimal,
    pub no_token_id: B256,
    pub second_leg_side: Option<u8>, // Some(0)=YES追加, Some(1)=NO追加, None=未追加
}

/// 策略配置常量
pub const DROP_THRESHOLD: i64 = 20;           // 跌到此价格时追加
pub const TAKE_PROFIT_THRESHOLD: i64 = 60;    // 涨到此价格时止盈
pub const WIND_DOWN_MINUTES: i64 = 4;         // 4分钟时平仓低价值
pub const INITIAL_AMOUNT: Decimal = dec!(1.0); // 初始下单金额
