#!/usr/bin/env python3
"""
WebSocket 连接测试：直接测试 Polymarket WebSocket 连接。
用法：python3 test_polymarket_ws.py
"""
import asyncio
import aiohttp

async def test_polymarket_ws():
    """测试 Polymarket WebSocket 连接"""
    ws_url = "wss://ws-subscriptions-clob.polymarket.com"
    proxy = "http://127.0.0.1:7890"
    
    print(f"[*] 尝试连接到 Polymarket WebSocket: {ws_url}")
    print(f"[*] 使用代理: {proxy}")
    
    session = aiohttp.ClientSession()
    try:
        # 尝试连接到 Polymarket WebSocket
        print("[*] 正在建立连接...")
        ws = await session.ws_connect(
            ws_url,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=15)
        )
        
        print("[+] 成功连接到 Polymarket WebSocket")
        
        # 发送订阅消息（订单簿订阅）
        subscribe_msg = {
            "type": "subscribe",
            "channel": "orderbook",
            "args": {
                "tokens": ["99263239165416166061956377610105824497590489806431019530751797297324658291943"]
            }
        }
        
        print("[*] 发送订阅消息...")
        await ws.send_json(subscribe_msg)
        
        # 等待响应
        print("[*] 等待响应...")
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                print(f"[+] 收到消息: {msg.data[:200]}...")
                # 只接收一条消息就退出
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                print(f"[-] WebSocket 错误: {msg.data}")
                break
                
    except Exception as e:
        print(f"[-] 连接失败: {type(e).__name__}: {e}")
    finally:
        await session.close()
        print("[*] 测试完成")

if __name__ == "__main__":
    asyncio.run(test_polymarket_ws())