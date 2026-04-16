#!/usr/bin/env python3
"""
HTTP 代理测试：测试代理服务器是否正常工作。
用法：python3 test_http_proxy.py
"""
import aiohttp
import asyncio

async def test_http_proxy():
    """测试 HTTP 代理连接"""
    test_url = "https://www.google.com"
    proxy = "http://127.0.0.1:7890"
    
    print(f"[*] 测试 HTTP 代理: {proxy}")
    print(f"[*] 测试目标: {test_url}")
    
    session = aiohttp.ClientSession()
    try:
        async with session.get(
            test_url,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as response:
            status = response.status
            print(f"[+] HTTP 代理测试成功！状态码: {status}")
            content = await response.text()
            print(f"[+] 响应长度: {len(content)} 字符")
            print(f"[+] 响应前 100 字符: {content[:100]}...")
    except Exception as e:
        print(f"[-] HTTP 代理测试失败: {type(e).__name__}: {e}")
    finally:
        await session.close()

if __name__ == "__main__":
    asyncio.run(test_http_proxy())