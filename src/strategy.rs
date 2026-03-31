use dashmap::DashMap;
use polymarket_client_sdk::types::U256;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

pub struct CountdownStrategy {
    yes_states: Arc<DashMap<String, CountdownSideState>>,
    no_states: Arc<DashMap<String, CountdownSideState>>,
    yes_cross_0_5_triggered: Arc<DashMap<String, bool>>,
    no_cross_0_5_triggered: Arc<DashMap<String, bool>>,
}

#[derive(Clone, Debug)]
pub struct CountdownSideState {
    pub step: u8,
    pub main_leg_executed: bool,
    pub main_leg_price: Option<Decimal>,
    pub main_leg_time: Option<Instant>,
    pub secondary_check_done: bool,
}

impl CountdownStrategy {
    pub fn new() -> Self {
        Self {
            yes_states: Arc::new(DashMap::new()),
            no_states: Arc::new(DashMap::new()),
            yes_cross_0_5_triggered: Arc::new(DashMap::new()),
            no_cross_0_5_triggered: Arc::new(DashMap::new()),
        }
    }

    pub fn reset(&self, market_id: &str) {
        self.yes_states.remove(market_id);
        self.no_states.remove(market_id);
        self.yes_cross_0_5_triggered.remove(market_id);
        self.no_cross_0_5_triggered.remove(market_id);
    }

    pub fn reset_all(&self) {
        self.yes_states.clear();
        self.no_states.clear();
        self.yes_cross_0_5_triggered.clear();
        self.no_cross_0_5_triggered.clear();
    }

    pub fn check_yes_side(
        &self,
        market_id: &str,
        market_display: &str,
        yes_price: Decimal,
        no_price: Decimal,
        sec_to_end: i64,
        yes_token_id: U256,
        no_token_id: U256,
    ) -> Option<StrategyAction> {
        let now = Instant::now();
        let five_secs = std::time::Duration::from_secs(5);

        let triggered = self.yes_cross_0_5_triggered.get(market_id).map(|v| *v).unwrap_or(false);
        
        if !triggered && yes_price >= dec!(0.5) {
            self.yes_cross_0_5_triggered.insert(market_id.to_string(), true);
            info!(
                "⏱️ 倒计时策略 YES侧 - 主信号触发 | 市场:{} | 秒:{} | YES价格:{:.2}",
                market_display, sec_to_end, yes_price
            );
            return Some(StrategyAction::WaitAndConfirm {
                side: "YES".to_string(),
                market_id: market_id.to_string(),
                market_display: market_display.to_string(),
                confirm_price: dec!(0.55),
                confirm_side: "YES".to_string(),
                wait_secs: 5,
                main_leg_amount: dec!(8.0),
            });
        }

        if triggered {
            let state = self.yes_states.get(market_id).map(|s| s.clone());
            
            match state {
                None => {
                    let new_state = CountdownSideState {
                        step: 1,
                        main_leg_executed: false,
                        main_leg_price: None,
                        main_leg_time: Some(now),
                        secondary_check_done: false,
                    };
                    self.yes_states.insert(market_id.to_string(), new_state);
                    return Some(StrategyAction::WaitAndConfirm {
                        side: "YES".to_string(),
                        market_id: market_id.to_string(),
                        market_display: market_display.to_string(),
                        confirm_price: dec!(0.55),
                        confirm_side: "YES".to_string(),
                        wait_secs: 5,
                        main_leg_amount: dec!(8.0),
                    });
                }
                Some(s) => {
                    if s.step == 1 && !s.main_leg_executed {
                        if let Some(exec_time) = s.main_leg_time {
                            if now.duration_since(exec_time) >= five_secs {
                                if yes_price >= dec!(0.55) {
                                    let mut updated = s.clone();
                                    updated.main_leg_executed = true;
                                    updated.main_leg_price = Some(yes_price);
                                    updated.main_leg_time = Some(now);
                                    self.yes_states.insert(market_id.to_string(), updated);
                                    
                                    info!(
                                        "⏱️ 倒计时策略 YES侧 - Step1确认通过 | 市场:{} | 秒:{} | YES价格:{:.2} | 买入$8",
                                        market_display, sec_to_end, yes_price
                                    );
                                    return Some(StrategyAction::ExecuteMainLeg {
                                        side: "YES".to_string(),
                                        market_id: market_id.to_string(),
                                        market_display: market_display.to_string(),
                                        token_id: yes_token_id,
                                        amount: dec!(8.0),
                                        price: yes_price,
                                        sec_to_end,
                                    });
                                } else {
                                    info!(
                                        "⏱️ 倒计时策略 YES侧 - Step1确认失败(YES<0.55) | 市场:{} | 重置",
                                        market_display
                                    );
                                    self.reset(market_id);
                                    return None;
                                }
                            }
                        }
                    } else if s.main_leg_executed && !s.secondary_check_done {
                        if let Some(exec_time) = s.main_leg_time {
                            if now.duration_since(exec_time) >= five_secs {
                                let in_range = no_price >= dec!(0.2) && no_price <= dec!(0.3);
                                let mut updated = s.clone();
                                updated.secondary_check_done = true;
                                self.yes_states.insert(market_id.to_string(), updated);
                                
                                if in_range {
                                    info!(
                                        "⏱️ 倒计时策略 YES侧 - Step2确认通过 | 市场:{} | 秒:{} | NO价格:{:.2} 在[0.2,0.3] | 买入$2",
                                        market_display, sec_to_end, no_price
                                    );
                                    return Some(StrategyAction::ExecuteSecondaryLeg {
                                        side: "NO".to_string(),
                                        market_id: market_id.to_string(),
                                        market_display: market_display.to_string(),
                                        token_id: no_token_id,
                                        amount: dec!(2.0),
                                        price: no_price,
                                        sec_to_end,
                                    });
                                } else {
                                    info!(
                                        "⏱️ 倒计时策略 YES侧 - Step2条件不满足(NO不在[0.2,0.3]) | 市场:{} | NO价格:{:.2} | 重置",
                                        market_display, no_price
                                    );
                                    self.reset(market_id);
                                    return None;
                                }
                            }
                        }
                    } else if s.secondary_check_done {
                        info!(
                            "⏱️ 倒计时策略 YES侧 - 流程完成 | 市场:{} | 重置",
                            market_display
                        );
                        self.reset(market_id);
                        return None;
                    }
                }
            }
        }
        None
    }

    pub fn check_no_side(
        &self,
        market_id: &str,
        market_display: &str,
        yes_price: Decimal,
        no_price: Decimal,
        sec_to_end: i64,
        yes_token_id: U256,
        no_token_id: U256,
    ) -> Option<StrategyAction> {
        let now = Instant::now();
        let five_secs = std::time::Duration::from_secs(5);

        let triggered = self.no_cross_0_5_triggered.get(market_id).map(|v| *v).unwrap_or(false);
        
        if !triggered && no_price >= dec!(0.5) {
            self.no_cross_0_5_triggered.insert(market_id.to_string(), true);
            info!(
                "⏱️ 倒计时策略 NO侧 - 主信号触发 | 市场:{} | 秒:{} | NO价格:{:.2}",
                market_display, sec_to_end, no_price
            );
            return Some(StrategyAction::WaitAndConfirm {
                side: "NO".to_string(),
                market_id: market_id.to_string(),
                market_display: market_display.to_string(),
                confirm_price: dec!(0.55),
                confirm_side: "YES".to_string(),
                wait_secs: 5,
                main_leg_amount: dec!(8.0),
            });
        }

        if triggered {
            let state = self.no_states.get(market_id).map(|s| s.clone());
            
            match state {
                None => {
                    let new_state = CountdownSideState {
                        step: 1,
                        main_leg_executed: false,
                        main_leg_price: None,
                        main_leg_time: Some(now),
                        secondary_check_done: false,
                    };
                    self.no_states.insert(market_id.to_string(), new_state);
                    return Some(StrategyAction::WaitAndConfirm {
                        side: "NO".to_string(),
                        market_id: market_id.to_string(),
                        market_display: market_display.to_string(),
                        confirm_price: dec!(0.55),
                        confirm_side: "YES".to_string(),
                        wait_secs: 5,
                        main_leg_amount: dec!(8.0),
                    });
                }
                Some(s) => {
                    if s.step == 1 && !s.main_leg_executed {
                        if let Some(exec_time) = s.main_leg_time {
                            if now.duration_since(exec_time) >= five_secs {
                                if yes_price >= dec!(0.55) {
                                    let mut updated = s.clone();
                                    updated.main_leg_executed = true;
                                    updated.main_leg_price = Some(no_price);
                                    updated.main_leg_time = Some(now);
                                    self.no_states.insert(market_id.to_string(), updated);
                                    
                                    info!(
                                        "⏱️ 倒计时策略 NO侧 - Step1确认通过 | 市场:{} | 秒:{} | YES价格:{:.2} >= 0.55 | 买入NO $8",
                                        market_display, sec_to_end, yes_price
                                    );
                                    return Some(StrategyAction::ExecuteMainLeg {
                                        side: "NO".to_string(),
                                        market_id: market_id.to_string(),
                                        market_display: market_display.to_string(),
                                        token_id: no_token_id,
                                        amount: dec!(8.0),
                                        price: no_price,
                                        sec_to_end,
                                    });
                                } else {
                                    info!(
                                        "⏱️ 倒计时策略 NO侧 - Step1确认失败(YES<0.55) | 市场:{} | 重置",
                                        market_display
                                    );
                                    self.reset(market_id);
                                    return None;
                                }
                            }
                        }
                    } else if s.main_leg_executed && !s.secondary_check_done {
                        if let Some(exec_time) = s.main_leg_time {
                            if now.duration_since(exec_time) >= five_secs {
                                let in_range = yes_price >= dec!(0.2) && yes_price <= dec!(0.3);
                                let mut updated = s.clone();
                                updated.secondary_check_done = true;
                                self.no_states.insert(market_id.to_string(), updated);
                                
                                if in_range {
                                    info!(
                                        "⏱️ 倒计时策略 NO侧 - Step2确认通过 | 市场:{} | 秒:{} | YES价格:{:.2} 在[0.2,0.3] | 买入YES $2",
                                        market_display, sec_to_end, yes_price
                                    );
                                    return Some(StrategyAction::ExecuteSecondaryLeg {
                                        side: "YES".to_string(),
                                        market_id: market_id.to_string(),
                                        market_display: market_display.to_string(),
                                        token_id: yes_token_id,
                                        amount: dec!(2.0),
                                        price: yes_price,
                                        sec_to_end,
                                    });
                                } else {
                                    info!(
                                        "⏱️ 倒计时策略 NO侧 - Step2条件不满足(YES不在[0.2,0.3]) | 市场:{} | YES价格:{:.2} | 重置",
                                        market_display, yes_price
                                    );
                                    self.reset(market_id);
                                    return None;
                                }
                            }
                        }
                    } else if s.secondary_check_done {
                        info!(
                            "⏱️ 倒计时策略 NO侧 - 流程完成 | 市场:{} | 重置",
                            market_display
                        );
                        self.reset(market_id);
                        return None;
                    }
                }
            }
        }
        None
    }
}

#[derive(Clone, Debug)]
pub enum StrategyAction {
    WaitAndConfirm {
        side: String,
        market_id: String,
        market_display: String,
        confirm_price: Decimal,
        confirm_side: String,
        wait_secs: u64,
        main_leg_amount: Decimal,
    },
    ExecuteMainLeg {
        side: String,
        market_id: String,
        market_display: String,
        token_id: U256,
        amount: Decimal,
        price: Decimal,
        sec_to_end: i64,
    },
    ExecuteSecondaryLeg {
        side: String,
        market_id: String,
        market_display: String,
        token_id: U256,
        amount: Decimal,
        price: Decimal,
        sec_to_end: i64,
    },
}
