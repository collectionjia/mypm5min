use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use anyhow::Result;
use chrono::Utc;
use polymarket_client_sdk_v2::clob::types::request::OrderBookSummaryRequest;
use polymarket_client_sdk_v2::clob::types::{OrderType, Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::types::{Address, U256};
use polymarket_client_sdk_v2::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::str::FromStr;
use std::time::Instant;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::monitor::arbitrage::ArbitrageOpportunity;

pub struct OrderPairResult {
    pub pair_id: String,
    pub yes_order_id: String,
    pub no_order_id: String,
    pub yes_filled: Decimal,
    pub no_filled: Decimal,
    pub yes_size: Decimal,
    pub no_size: Decimal,
    pub success: bool,
}

pub struct TradingExecutor {
    client: Client<
        polymarket_client_sdk_v2::auth::state::Authenticated<polymarket_client_sdk_v2::auth::Normal>,
    >,
    private_key: String,
    max_order_size: Decimal,
    slippage: [Decimal; 2], // [first, second]，仅下降侧用 second，上涨与持平用 first
    gtd_expiration_secs: u64,
    arbitrage_order_type: OrderType,
    // 分割单配置
    split_order_count: usize,      // 分割成几个小单
    split_order_interval_ms: u64,  // 下单间隔（毫秒）
}

impl TradingExecutor {
    pub async fn new(
        private_key: String,
        max_order_size_usdc: f64,
        proxy_address: Option<Address>,
        slippage: [f64; 2],
        gtd_expiration_secs: u64,
        arbitrage_order_type: OrderType,
        split_order_count: usize,
        split_order_interval_ms: u64,
    ) -> Result<Self> {
        // 验证私钥格式
        let signer = LocalSigner::from_str(&private_key)
            .map_err(|e| {
                anyhow::anyhow!(
                    "私钥格式无效: {}. 请确保私钥是64字符的十六进制字符串（不带0x前缀）",
                    e
                )
            })?
            .with_chain_id(Some(POLYGON));

        let config = Config::builder().use_server_time(false).build();
        let clob_url = std::env::var("CLOB_URL")
            .unwrap_or_else(|_| "https://clob.polymarket.com".to_string());
        let mut auth_builder = Client::new(&clob_url, config)
            .map_err(|e| anyhow::anyhow!("创建CLOB客户端失败: {}", e))?
            .authentication_builder(&signer);

        // 如果提供了proxy_address，设置funder和signature_type（按照Python SDK模式）
        if let Some(funder) = proxy_address {
            auth_builder = auth_builder
                .funder(funder)
                .signature_type(SignatureType::Poly1271);
        }

        let client = auth_builder.authenticate().await.map_err(|e| {
            anyhow::anyhow!(
                "API认证失败: {}. 可能的原因：1) 私钥无效 2) 网络问题 3) Polymarket API服务不可用",
                e
            )
        })?;

        Ok(Self {
            client,
            private_key,
            max_order_size: Decimal::try_from(max_order_size_usdc)
                .unwrap_or(rust_decimal_macros::dec!(100.0)),
            slippage: [
                Decimal::try_from(slippage[0]).unwrap_or(dec!(0.0)),
                Decimal::try_from(slippage[1]).unwrap_or(dec!(0.01)),
            ],
            gtd_expiration_secs,
            arbitrage_order_type,
            split_order_count,
            split_order_interval_ms,
        })
    }

    /// 验证认证是否真的成功 - 按照官方示例使用 api_keys() 来验证
    pub async fn verify_authentication(&self) -> Result<()> {
        // 按照官方示例，使用 api_keys() 来验证认证状态
        self.client
            .api_keys()
            .await
            .map_err(|e| anyhow::anyhow!("认证验证失败: API调用返回错误: {}", e))?;
        Ok(())
    }

    /// 取消该账户所有挂单（收尾时使用）
    pub async fn cancel_all_orders(
        &self,
    ) -> Result<polymarket_client_sdk_v2::clob::types::response::CancelOrdersResponse> {
        self.client
            .cancel_all_orders()
            .await
            .map_err(|e| anyhow::anyhow!("取消所有挂单失败: {}", e))
    }

    /// 以指定价格下 GTC 卖单（收尾时市价意图卖出单腿持仓）
    pub async fn sell_at_price(
        &self,
        token_id: U256,
        price: Decimal,
        size: Decimal,
    ) -> Result<polymarket_client_sdk_v2::clob::types::response::PostOrderResponse> {
        let price = price.round_dp(2);
        info!(
            "🧾 下单参数详情 | action=sell_at_price | token_id={} | side=SELL | order_type=GTC | price={} | size={}",
            token_id, price, size
        );
        let signer = LocalSigner::from_str(&self.private_key)?.with_chain_id(Some(POLYGON));
        let order = self
            .client
            .limit_order()
            .token_id(token_id)
            .side(Side::Sell)
            .price(price)
            .size(size)
            .order_type(OrderType::GTC)
            .build()
            .await?;
        let signed = self.client.sign(&signer, order).await?;
        self.client
            .post_order(signed)
            .await
            .map_err(|e| {
                if e.to_string().contains("not enough balance / allowance") {
                    anyhow::anyhow!("卖出订单提交失败: 余额不足或未授权（可能是份额不足，或 ConditionalTokens/USDC 未授权）。原始错误: {}", e)
                } else {
                    anyhow::anyhow!("卖出订单提交失败: {}", e)
                }
            })
    }

    /// 以指定价格下 GTC 买单（用于策略性单边下单）
    pub async fn buy_at_price(
        &self,
        token_id: U256,
        price: Decimal,
        size: Decimal,
    ) -> Result<polymarket_client_sdk_v2::clob::types::response::PostOrderResponse> {
        let price = price.round_dp(2);
        info!(
            "🧾 下单参数详情 | action=buy_at_price | token_id={} | side=BUY | order_type=GTC | price={} | size={}",
            token_id, price, size
        );
        let signer = LocalSigner::from_str(&self.private_key)?.with_chain_id(Some(POLYGON));
        let order = self
            .client
            .limit_order()
            .token_id(token_id)
            .side(Side::Buy)
            .price(price)
            .size(size)
            .order_type(OrderType::GTC)
            .build()
            .await?;
        let signed = self.client.sign(&signer, order).await?;
        self.client
            .post_order(signed)
            .await
            .map_err(|e| {
                if e.to_string().contains("not enough balance / allowance") {
                    anyhow::anyhow!("买入订单提交失败: 余额不足或未授权 (USDC)。请检查钱包余额及对CTF Exchange的授权。原始错误: {}", e)
                } else {
                    anyhow::anyhow!("买入订单提交失败: {}", e)
                }
            })
    }

    pub async fn buy_market_usd(
        &self,
        token_id: U256,
        reference_ask: Decimal,
        usd_amount: Decimal,
    ) -> Result<polymarket_client_sdk_v2::clob::types::response::PostOrderResponse> {
        if usd_amount <= dec!(0) {
            return Err(anyhow::anyhow!("usd_amount 必须大于 0"));
        }
        if reference_ask <= dec!(0) {
            return Err(anyhow::anyhow!("reference_ask 必须大于 0"));
        }

        let reference_ask = reference_ask.round_dp(2);
        let mut size = (usd_amount / reference_ask).round_dp(0);
        if size < dec!(1) {
            size = dec!(1);
        }

        let price = dec!(0.99);
        let min_size_for_order_price = (dec!(1) / price).ceil();
        if size < min_size_for_order_price {
            size = min_size_for_order_price;
        }
        let order_amount = price * size;
        info!(
            "🧾 下单参数详情 | action=buy_market_usd | token_id={} | side=BUY | order_type=FOK | reference_ask={} | usd_amount={} | price={} | min_size_for_price={} | computed_size={} | order_amount={}",
            token_id, reference_ask, usd_amount, price, min_size_for_order_price, size, order_amount
        );
        let signer = LocalSigner::from_str(&self.private_key)?.with_chain_id(Some(POLYGON));
        let order = self
            .client
            .limit_order()
            .token_id(token_id)
            .side(Side::Buy)
            .price(reference_ask)
            .size(size)
            .order_type(OrderType::FOK)
            .build()
            .await?;
        let signed = self.client.sign(&signer, order).await?;
        self.client
            .post_order(signed)
            .await
            .map_err(|e| {
                if e.to_string().contains("not enough balance / allowance") {
                    anyhow::anyhow!("买入订单提交失败: 余额不足或未授权 (USDC)。请检查钱包余额及对CTF Exchange的授权。原始错误: {}", e)
                } else {
                    anyhow::anyhow!("买入订单提交失败: {}", e)
                }
            })
    }

    /// 按方向取滑点：仅下降(↓)用 second，上涨(↑)和持平(−/空)用 first
    fn slippage_for_direction(&self, dir: &str) -> Decimal {
        if dir == "↓" {
            self.slippage[1]
        } else {
            self.slippage[0]
        }
    }

    /// 执行套利交易（分割单模式：将订单分成多个小单分批下单）
    /// yes_dir / no_dir：涨跌方向 "↑" "↓" "−" 或 ""，用于按方向分配滑点（仅下降=second，上涨与持平=first）
    pub async fn execute_arbitrage_pair(
        &self,
        opp: &ArbitrageOpportunity,
        yes_dir: &str,
        no_dir: &str,
    ) -> Result<OrderPairResult> {
        // 性能计时：总开始时间
        let total_start = Instant::now();

        // 计算实际下单数量（考虑最大订单限制）
        let yes_token_id = U256::from_str(&opp.yes_token_id.to_string())?;
        let no_token_id = U256::from_str(&opp.no_token_id.to_string())?;

        let total_order_size = opp.yes_size.min(opp.no_size).min(self.max_order_size);

        // 生成订单对ID
        let pair_id = Uuid::new_v4().to_string();

        // 计算过期时间：当前时间 + 配置的过期时间
        let expiration = Utc::now() + chrono::Duration::seconds(self.gtd_expiration_secs as i64);

        // 滑点按涨跌方向分配：上涨=first，下降/持平=second
        let yes_slippage_apply = self.slippage_for_direction(yes_dir);
        let no_slippage_apply = self.slippage_for_direction(no_dir);
        let yes_price_with_slippage = (opp.yes_ask_price + yes_slippage_apply).min(dec!(1.0));
        let no_price_with_slippage = (opp.no_ask_price + no_slippage_apply).min(dec!(1.0));

        // 分割单配置
        let split_count = self.split_order_count;
        let split_interval_ms = self.split_order_interval_ms;
        
        // 计算每个小单的大小
        let split_size = (total_order_size / Decimal::from(split_count)).round_dp(0);
        if split_size < dec!(1) {
            warn!(
                "⏭️ 跳过下单 | 分割后单笔大小:{:.2} < $1（交易所最小下单金额）",
                split_size
            );
            return Err(anyhow::anyhow!(
                "分割单后单笔金额不满足交易所最小要求: {:.2} USD",
                split_size
            ));
        }

        info!(
            "🧾 分割单参数 | pair_id={} | market_id={} | 总数量={} | 分割数={} | 单笔={} | 间隔={}ms",
            &pair_id[..8],
            opp.market_id,
            total_order_size,
            split_count,
            split_size,
            split_interval_ms
        );
        info!(
            "📋 选档 | YES {:.4}×{:.2} NO {:.4}×{:.2}",
            yes_price_with_slippage, total_order_size, no_price_with_slippage, total_order_size
        );

        // 创建signer
        let signer = LocalSigner::from_str(&self.private_key)?.with_chain_id(Some(POLYGON));

        // 累计成交数量
        let mut total_yes_filled = dec!(0);
        let mut total_no_filled = dec!(0);
        let mut last_yes_order_id: Option<String> = None;
        let mut last_no_order_id: Option<String> = None;
        let mut all_success = true;
        let mut error_msg = String::new();

        // 分割单循环：分批下单 YES 和 NO
        for i in 0..split_count {
            let split_start = Instant::now();
            
            // 构建并发送 YES 和 NO 小单（并行）
            let (yes_result, no_result) = tokio::join!(
                async {
                    let order = self
                        .client
                        .limit_order()
                        .token_id(yes_token_id)
                        .side(Side::Buy)
                        .price(yes_price_with_slippage)
                        .size(split_size)
                        .order_type(self.arbitrage_order_type.clone());
                    let order = if matches!(&self.arbitrage_order_type, OrderType::GTD) {
                        order.expiration(expiration).build().await
                    } else {
                        order.build().await
                    }?;
                    let signed = self.client.sign(&signer, order).await?;
                    self.client.post_order(signed).await
                },
                async {
                    let order = self
                        .client
                        .limit_order()
                        .token_id(no_token_id)
                        .side(Side::Buy)
                        .price(no_price_with_slippage)
                        .size(split_size)
                        .order_type(self.arbitrage_order_type.clone());
                    let order = if matches!(&self.arbitrage_order_type, OrderType::GTD) {
                        order.expiration(expiration).build().await
                    } else {
                        order.build().await
                    }?;
                    let signed = self.client.sign(&signer, order).await?;
                    self.client.post_order(signed).await
                }
            );

            let split_elapsed = split_start.elapsed().as_millis();

            // 处理 YES 订单结果
            match yes_result {
                Ok(r) => {
                    total_yes_filled += r.taking_amount;
                    last_yes_order_id = Some(r.order_id);
                    if !r.success {
                        all_success = false;
                        if let Some(ref msg) = r.error_msg {
                            error_msg = format!("YES-{}: {}", i + 1, msg);
                        }
                    }
                }
                Err(e) => {
                    all_success = false;
                    error_msg = format!("YES-{}-API: {}", i + 1, e);
                }
            }

            // 处理 NO 订单结果
            match no_result {
                Ok(r) => {
                    total_no_filled += r.taking_amount;
                    last_no_order_id = Some(r.order_id);
                    if !r.success {
                        all_success = false;
                        if let Some(ref msg) = r.error_msg {
                            error_msg = format!("NO-{}: {}", i + 1, msg);
                        }
                    }
                }
                Err(e) => {
                    all_success = false;
                    error_msg = format!("NO-{}-API: {}", i + 1, e);
                }
            }

            info!(
                "📤 分割单[{}/{}] | YES {:.4}×{} | NO {:.4}×{} | 耗时{}ms | 累计YES={:.2} NO={:.2}",
                i + 1,
                split_count,
                yes_price_with_slippage,
                split_size,
                no_price_with_slippage,
                split_size,
                split_elapsed,
                total_yes_filled,
                total_no_filled
            );

            // 如果不是最后一单，等待间隔后再下下一单
            if i < split_count - 1 && split_interval_ms > 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(split_interval_ms)).await;
            }
        }

        let total_elapsed = total_start.elapsed().as_millis();

        // 根据成交情况打印日志
        if total_yes_filled > dec!(0) && total_no_filled > dec!(0) {
            info!(
                "✅ 套利交易成功 | 订单对ID:{} | 分割数:{}/{} | YES成交:{:.2}份 | NO成交:{:.2}份 | 总成交:{:.2}份 | 总耗时:{}ms",
                &pair_id[..8],
                split_count,
                split_count,
                total_yes_filled,
                total_no_filled,
                total_yes_filled.min(total_no_filled),
                total_elapsed
            );
        } else if total_yes_filled > dec!(0) || total_no_filled > dec!(0) {
            let side = if total_yes_filled > dec!(0) { "YES" } else { "NO" };
            let filled = if total_yes_filled > dec!(0) {
                total_yes_filled
            } else {
                total_no_filled
            };
            let other_side = if total_yes_filled > dec!(0) { "NO" } else { "YES" };
            warn!(
                "⚠️ 单边成交 | {} | {} 成交 {:.2} 份，{} 未成交 | 错误:{}",
                &pair_id[..8],
                side,
                filled,
                other_side,
                error_msg
            );
        } else {
            error!(
                "❌ 套利失败 | 订单对ID:{} | YES和NO都未成交 | 错误:{}",
                &pair_id[..8],
                error_msg
            );
            return Err(anyhow::anyhow!("套利失败: {}", error_msg));
        }

        Ok(OrderPairResult {
            pair_id,
            yes_order_id: last_yes_order_id.unwrap_or_default(),
            no_order_id: last_no_order_id.unwrap_or_default(),
            yes_filled: total_yes_filled,
            no_filled: total_no_filled,
            yes_size: total_order_size,
            no_size: total_order_size,
            success: all_success,
        })
    }

    /// 执行 1 美元 split 订单（进入市场时使用）
    /// 同时以指定价格买入 YES 和 NO，各 1 美元
    pub async fn execute_split_order(
        &self,
        yes_token_id: U256,
        no_token_id: U256,
        yes_price: Decimal,
        no_price: Decimal,
    ) -> Result<OrderPairResult> {
        let total_start = Instant::now();
        let order_amount = dec!(1.0); // 固定 1 美元

        // 计算每边份额（1美元 / 价格），最小为 1 份
        let yes_size = (order_amount / yes_price).round_dp(0).max(dec!(1));
        let no_size = (order_amount / no_price).round_dp(0).max(dec!(1));

        let pair_id = Uuid::new_v4().to_string();

        info!(
            "📋 1美元 Split 订单 | pair_id={} | YES {:.4}×{} | NO {:.4}×{}",
            &pair_id[..8],
            yes_price, yes_size,
            no_price, no_size
        );

        let signer = LocalSigner::from_str(&self.private_key)?.with_chain_id(Some(POLYGON));

        // 通过 REST API 获取订单簿以获取最新价格和版本
        let yes_req = OrderBookSummaryRequest::builder()
            .token_id(yes_token_id)
            .build();
        let no_req = OrderBookSummaryRequest::builder()
            .token_id(no_token_id)
            .build();
        let yes_book = self.client.order_book(&yes_req).await?;
        let no_book = self.client.order_book(&no_req).await?;

        // 获取卖一价（asks 最后一档）
        let actual_yes_price = yes_book.asks.last()
            .map(|a| a.price)
            .unwrap_or(yes_price);
        let actual_no_price = no_book.asks.last()
            .map(|a| a.price)
            .unwrap_or(no_price);

        // 如果实际价格与预期差异过大，使用实际价格
        let final_yes_price = if (actual_yes_price - yes_price).abs() < dec!(0.1) {
            yes_price
        } else {
            actual_yes_price
        };
        let final_no_price = if (actual_no_price - no_price).abs() < dec!(0.1) {
            no_price
        } else {
            actual_no_price
        };

        // 重新计算份额
        let final_yes_size = (order_amount / final_yes_price).round_dp(0).max(dec!(1));
        let final_no_size = (order_amount / final_no_price).round_dp(0).max(dec!(1));

        info!(
            "📋 实际价格 | YES {:.4}×{} | NO {:.4}×{}",
            final_yes_price, final_yes_size,
            final_no_price, final_no_size
        );

        // 并行下单 YES 和 NO
        let (yes_result, no_result) = tokio::join!(
            async {
                let order = self
                    .client
                    .limit_order()
                    .token_id(yes_token_id)
                    .side(Side::Buy)
                    .price(final_yes_price)
                    .size(final_yes_size)
                    .order_type(OrderType::FOK)
                    .build()
                    .await?;
                let signed = self.client.sign(&signer, order).await?;
                self.client.post_order(signed).await
            },
            async {
                let order = self
                    .client
                    .limit_order()
                    .token_id(no_token_id)
                    .side(Side::Buy)
                    .price(final_no_price)
                    .size(final_no_size)
                    .order_type(OrderType::FOK)
                    .build()
                    .await?;
                let signed = self.client.sign(&signer, order).await?;
                self.client.post_order(signed).await
            }
        );

        let total_elapsed = total_start.elapsed().as_millis();

        let (yes_filled, no_filled) = match (yes_result, no_result) {
            (Ok(y), Ok(n)) => (y.taking_amount, n.taking_amount),
            (Err(e), _) => {
                error!("❌ YES 下单失败: {}", e);
                return Err(anyhow::anyhow!("YES 下单失败: {}", e));
            }
            (_, Err(e)) => {
                error!("❌ NO 下单失败: {}", e);
                return Err(anyhow::anyhow!("NO 下单失败: {}", e));
            }
        };

        let success = yes_filled > dec!(0) && no_filled > dec!(0);

        if success {
            info!(
                "✅ 1美元 Split 订单成功 | pair_id={} | YES成交:{:.2} | NO成交:{:.2} | 耗时:{}ms",
                &pair_id[..8], yes_filled, no_filled, total_elapsed
            );
        } else {
            warn!(
                "⚠️ 1美元 Split 订单部分成交 | YES:{:.2} | NO:{:.2}",
                yes_filled, no_filled
            );
        }

        Ok(OrderPairResult {
            pair_id,
            yes_order_id: String::new(),
            no_order_id: String::new(),
            yes_filled,
            no_filled,
            yes_size: final_yes_size,
            no_size: final_no_size,
            success,
        })
    }
}
