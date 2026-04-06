#!/usr/bin/env python3
"""
vidarx_polymarket_bot.py
=====================
 Polymarket 5分钟BTC二进制做市机器人
 
 策略: 双面挂单 + 趋势跟随 + 赚取spread和maker费
 适合: Bitcoin 5分钟 Up/Down 市场
"""

import requests
import time
import threading
from datetime import datetime
from typing import Optional, Dict, List
import json

# ====================
# 配置参数
# ====================
CONFIG = {
    # 仓位
    "SIZE": 40,                    # 标准份额
    "MAX_TRADES_PER_MARKET": 30,      # 单市场最大交易次数
    "MAX_COST_PER_MARKET": 500,     # 单市场最大投入$
    
    # 价格参数  
    "MIN_PRICE": 0.30,            # 最低买入价格
    "MAX_PRICE": 0.70,             # 最高买入价格
    "PRICE_STEP": 0.01,            # 价格步长
    
    # 风控
    "STOP_LOSS": -100,              # 单市场止损$
    "TAKE_PROFIT": 60,             # 单市场止盈$
    "MAX_POSITION": 1000,           # 总持仓上限
    
    # 时间
    "CHECK_INTERVAL": 0.5,         # 检查间隔(秒)
    "MAX_HOLD_TIME": 300,           # 最大持仓5分钟
    
    # API
    "API_KEY": "YOUR_API_KEY",       # Polymarket API Key
    "PROXY_WALLET": "0x...",      # 代理钱包地址
}

# 全局状态
STATE = {
    "positions": {},               # 当前持仓
    "trades": [],                 # 交易记录
    "total_pnl": 0,             # 总盈亏
    "daily_cost": 0,              # 今日投入
    "running": False,
    "market_end_time": None,
}

# ====================
# API 函数
# ====================

def get_market_price(condition_id: str, outcome: str) -> float:
    """获取市场价格"""
    url = f"https://clob.polymarket.com/price/{condition_id}/{outcome}"
    try:
        resp = requests.get(url, timeout=5)
        return float(resp.json()["price"])
    except:
        return 0.5

def place_order(condition_id: str, outcome: str, size: int, price: float) -> Dict:
    """下单"""
    url = "https://clob.polymarket.com/order"
    data = {
        "condition_id": condition_id,
        "outcome": outcome,
        "size": size,
        "price": price,
        "proxy_wallet": CONFIG["PROXY_WALLET"],
    }
    # 实际需要签名
    # resp = requests.post(url, json=data)
    # return resp.json()
    return {"status": "simulated", "size": size, "price": price}

def get_market_info(slug: str) -> Dict:
    """获取市场信息"""
    url = f"https://gamma-api.polymarket.com/markets/{slug}"
    resp = requests.get(url)
    return resp.json()

# ====================
# 核心策略规则
# ====================

def rule_1_initial_probe(market: Dict, side: str) -> List[Dict]:
    """
    规则1: 开盘试探
    双面挂单测试流动性
    """
    orders = []
    
    # 基础价格
    up_price = get_market_price(market["condition_id"], "Up")
    down_price = get_market_price(market["condition_id"], "Down")
    
    # 挂UP单
    if up_price <= CONFIG["MAX_PRICE"]:
        orders.append({
            "outcome": "Up",
            "size": CONFIG["SIZE"],
            "price": round(up_price * 1.02, 2),  # 稍微提价优先成交
        })
    
    # 挂DOWN单  
    if down_price <= CONFIG["MAX_PRICE"]:
        orders.append({
            "outcome": "Down", 
            "size": CONFIG["SIZE"],
            "price": round(down_price * 1.02, 2),
        })
    
    print(f"[规则1] 开盘试探: UP@{up_price}, DOWN@{down_price}")
    return orders


def rule_2_trend_follow(market: Dict, last_trade: Dict, new_price: float) -> Optional[Dict]:
    """
    规则2: 趋势跟随
    价格移动>5%时顺势加仓
    """
    if not last_trade:
        return None
    
    price_change = (new_price - last_trade["price"]) / last_trade["price"]
    
    # 价格上涨且持仓UP → 加仓UP
    if price_change > 0.05 and last_trade["outcome"] == "Up":
        order = {
            "outcome": "Up",
            "size": CONFIG["SIZE"],
            "price": round(new_price * 1.01, 2),
        }
        print(f"[规则2] 趋势UP: +{price_change*100:.1f}%")
        return order
    
    # 价格下跌且持仓DOWN → 加仓DOWN
    if price_change < -0.05 and last_trade["outcome"] == "Down":
        order = {
            "outcome": "Down", 
            "size": CONFIG["SIZE"],
            "price": round(new_price * 0.99, 2),
        }
        print(f"[规则2] 趋势DOWN: {price_change*100:.1f}%")
        return order
    
    return None


def rule_3_reversal_hedge(market: Dict, trades: List[Dict]) -> Optional[Dict]:
    """
    规则3: 反转对冲
    检测到反转信号时反向小仓位对冲
    """
    if len(trades) < 3:
        return None
    
    # 获取最近3笔
    recent = trades[-3:]
    outcomes = [t["outcome"] for t in recent]
    
    # 连续同方向3笔后反转
    if outcomes[0] != outcomes[1] == outcomes[2]:
        new_dir = "Up" if outcomes[2] == "Down" else "Down"
        price = get_market_price(market["condition_id"], new_dir)
        
        order = {
            "outcome": new_dir,
            "size": CONFIG["SIZE"] // 2,    # 半仓对冲
            "price": price,
        }
        print(f"[规则3] 反转对冲: {new_dir}")
        return order
    
    return None


def rule_4_time_stop(market: Dict) -> bool:
    """
    规则4: 时间停止
    距离开盘5分钟前30秒停止下单
    """
    now = datetime.now()
    
    if STATE["market_end_time"]:
        remaining = (STATE["market_end_time"] - now).total_seconds()
        if remaining <= 30:
            print(f"[规则4] 时间到,停止下单,剩余{int(remaining)}秒")
            return True
    
    return False


def rule_5_stop_loss(market: Dict) -> bool:
    """
    规则5: 止损检查
    亏损>100美元时强平
    """
    current_pnl = calculate_pnl(market)
    
    if current_pnl <= CONFIG["STOP_LOSS"]:
        print(f"[规则5] 触发止损: ${current_pnl}")
        close_all_position(market)
        return True
    
    return False


def rule_6_take_profit(market: Dict) -> bool:
    """
    规则6: 止盈检查  
    盈利>60美元时部分止盈
    """
    current_pnl = calculate_pnl(market)
    
    if current_pnl >= CONFIG["TAKE_PROFIT"]:
        print(f"[规则6] 触发止盈: +${current_pnl}")
        # 卖掉一半仓位
        close_half_position(market)
        return True
    
    return False


# ====================
# 辅助函数
# ====================

def calculate_pnl(market: Dict) -> float:
    """计算当前盈亏"""
    total_cost = sum(t["cost"] for t in STATE["trades"])
    
    # 假设UP方向结算
    up_shares = sum(t["size"] for t in STATE["trades"] if t["outcome"] == "Up")
    down_shares = sum(t["size"] for t in STATE["trades"] if t["outcome"] == "Down")
    
    if up_shares > down_shares:
        # 看涨方向
        settlement = 1 if market["result"] == "Up" else 0
        value = up_shares * settlement
        pnl = value - total_cost
    else:
        settlement = 1 if market["result"] == "Down" else 0
        value = down_shares * settlement
        pnl = value - total_cost
    
    return pnl


def close_all_position(market: Dict):
    """平所有仓"""
    print("[风控] 全部平仓")
    STATE["trades"].clear()


def close_half_position(market: Dict):
    """平一半仓"""
    print("[风控] 平半仓")
    STATE["trades"] = STATE["trades"][:len(STATE["trades"])//2]


# ====================
# 主循环
# ====================

def run_market_bot(market: Dict):
    """
    执行单个市场交易
    """
    condition_id = market["condition_id"]
    slug = market["slug"]
    
    print(f"\n========== 开始市场: {slug} ==========")
    print(f"问题: {market['question']}")
    
    # 重置状态
    STATE["trades"].clear()
    trades_count = 0
    
    # 1. 开盘试探
    initial_orders = rule_1_initial_probe(market, "Up")
    for order in initial_orders:
        place_order(condition_id, order["outcome"], order["size"], order["price"])
        STATE["trades"].append({
            "outcome": order["outcome"],
            "size": order["size"],
            "price": order["price"],
            "cost": order["size"] * order["price"],
            "time": datetime.now(),
        })
        trades_count += 1
        time.sleep(2)  # 等待成交
    
    # 2. 主循环
    while STATE["running"] and trades_count < CONFIG["MAX_TRADES_PER_MARKET"]:
        
        # 检查时间
        if rule_4_time_stop(market):
            break
            
        # 检查止损止盈
        if rule_5_stop_loss(market) or rule_6_take_profit(market):
            break
        
        # 获取当前价格
        up_price = get_market_price(condition_id, "Up")
        
        # 规则2: 趋势跟随
        if STATE["trades"]:
            last = STATE["trades"][-1]
            order = rule_2_trend_follow(market, last, up_price)
            if order:
                place_order(condition_id, order["outcome"], order["size"], order["price"])
                STATE["trades"].append({**order, "cost": order["size"] * order["price"], "time": datetime.now()})
                trades_count += 1
        
        # 规则3: 反转对冲
        if len(STATE["trades"]) >= 3:
            order = rule_3_reversal_hedge(market, STATE["trades"])
            if order:
                place_order(condition_id, order["outcome"], order["size"], order["price"])
                STATE["trades"].append({**order, "cost": order["size"] * order["price"], "time": datetime.now()})
                trades_count += 1
        
        # 等待下次检查
        time.sleep(CONFIG["CHECK_INTERVAL"])
    
    # 3. 等待结算
    print("等待市场结算...")
    time.sleep(10)
    
    pnl = calculate_pnl(market)
    print(f"========== 市场结束, PnL: ${pnl} ==========\n")
    
    return pnl


def run_all_markets():
    """运行所有5分钟BTC市场"""
    STATE["running"] = True
    
    # 获取当前有效的5分钟市场
    url = "https://gamma-api.polymarket.com/markets?closed=false"
    resp = requests.get(url)
    markets = resp.json()
    
    # 筛选5分钟BTC市场
    btc_5m_markets = [
        m for m in markets 
        if "btc-updown-5m" in m.get("slug", "") and not m.get("closed")
    ]
    
    print(f"找到 {len(btc_5m_markets)} 个5分钟BTC市场")
    
    for market in btc_5m_markets:
        try:
            run_market_bot(market)
            time.sleep(10)  # 市场间隔
        except Exception as e:
            print(f"错误: {e}")
            continue
    
    STATE["running"] = False


# ====================
# 启动
# ====================

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════╗
    ║   vidarx Polymarket Bot v1.0   ║
    ║   5分钟BTC做市商策略           ║
    ╚══════════════════════════════════╝
    """)
    
    # 测试模式
    print("[测试] 运行测试市场...")
    test_market = {
        "condition_id": "0xtest",
        "slug": "btc-updown-5m-test",
        "question": "Bitcoin Up or Down - Test",
    }
    # run_market_bot(test_market)
    
    print("配置完成! 设置API_KEY后运行 run_all_markets()")
