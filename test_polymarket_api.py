#!/usr/bin/env python3
"""
Polymarket API 测试：测试 Polymarket API 连接。
用法：python3 test_polymarket_api.py
"""
import aiohttp
import asyncio

async def test_polymarket_api():
    """测试 Polymarket API 连接"""
    api_url = "https://clob.polymarket.com"
    proxy = "http://127.0.0.1:7890"
    
    print(f"[*] 测试 Polymarket API: {api_url}")
    print(f"[*] 使用代理: {proxy}")
    
    session = aiohttp.ClientSession()
    try:
        # 测试 API 健康检查
        health_url = f"{api_url}/health"
        print(f"[*] 测试健康检查: {health_url}")
        
        async with session.get(
            health_url,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as response:
            status = response.status
            print(f"[+] 健康检查成功！状态码: {status}")
            content = await response.text()
            print(f"[+] 响应: {content}")
            
        # 测试获取市场信息
        markets_url = f"{api_url}/markets"
        print(f"[*] 测试获取市场信息: {markets_url}")
        
        async with session.get(
            markets_url,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as response:
            status = response.status
            print(f"[+] 获取市场信息成功！状态码: {status}")
            content = await response.text()
            print(f"[+] 响应长度: {len(content)} 字符")
            print(f"[+] 响应前 200 字符: {content[:200]}...")
            
    except Exception as e:
        print(f"[-] API 测试失败: {type(e).__name__}: {e}")
    finally:
        await session.close()

if __name__ == "__main__":
    asyncio.run(test_polymarket_api())