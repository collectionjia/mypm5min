#!/usr/bin/env python3
"""
WebSocket 代理测试：测试 WebSocket 代理服务器的连接。
用法：python3 test_ws_proxy_full.py
"""
import asyncio
import websockets
import aiohttp

async def test_polymarket_ws_direct():
    """直接测试 Polymarket WebSocket 连接"""
    print("=== 直接测试 Polymarket WebSocket ===")
    ws_url = "wss://ws-subscriptions-clob.polymarket.com"
    
    session = aiohttp.ClientSession()
    try:
        print(f"尝试连接: {ws_url}")
        ws = await session.ws_connect(
            ws_url,
            timeout=aiohttp.ClientTimeout(total=10)
        )
        print("✅ 直接连接成功")
        await ws.close()
    except Exception as e:
        print(f"❌ 直接连接失败: {type(e).__name__}: {e}")
    finally:
        await session.close()

async def test_polymarket_ws_with_proxy():
    """通过代理测试 Polymarket WebSocket 连接"""
    print("\n=== 通过代理测试 Polymarket WebSocket ===")
    ws_url = "wss://ws-subscriptions-clob.polymarket.com"
    proxy = "http://127.0.0.1:7890"
    
    session = aiohttp.ClientSession()
    try:
        print(f"尝试连接: {ws_url} (via {proxy})")
        ws = await session.ws_connect(
            ws_url,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=15)
        )
        print("✅ 代理连接成功")
        await ws.close()
    except Exception as e:
        print(f"❌ 代理连接失败: {type(e).__name__}: {e}")
    finally:
        await session.close()

async def test_local_proxy():
    """测试本地 WebSocket 代理服务器"""
    print("\n=== 测试本地 WebSocket 代理 ===")
    ws_url = "ws://localhost:8443"
    
    try:
        print(f"尝试连接: {ws_url}")
        async with websockets.connect(ws_url) as ws:
            print("✅ 本地代理连接成功")
            # 发送测试消息
            await ws.send("{\"type\": \"ping\"}")
            print("发送测试消息成功")
    except Exception as e:
        print(f"❌ 本地代理连接失败: {type(e).__name__}: {e}")

async def main():
    """主测试函数"""
    await test_polymarket_ws_direct()
    await test_polymarket_ws_with_proxy()
    await test_local_proxy()

if __name__ == "__main__":
    asyncio.run(main())