#!/usr/bin/env python3
"""
WebSocket 客户端测试：连接到本地代理服务器并测试连接。
用法：python3 test_ws_proxy.py
"""
import asyncio
import websockets

async def test_ws_proxy():
    """测试 WebSocket 代理连接"""
    ws_url = "ws://localhost:8443"
    print(f"[*] 尝试连接到 WebSocket 代理: {ws_url}")
    
    try:
        async with websockets.connect(ws_url) as ws:
            print("[+] 成功连接到 WebSocket 代理")
            
            # 发送一个简单的消息
            test_message = "{\"type\": \"ping\"}"
            print(f"[*] 发送测试消息: {test_message}")
            await ws.send(test_message)
            
            # 等待响应
            try:
                response = await asyncio.wait_for(ws.recv(), timeout=5.0)
                print(f"[+] 收到响应: {response}")
            except asyncio.TimeoutError:
                print("[-] 未收到响应 (5秒超时)")
                
            print("[*] 测试完成，关闭连接")
    except Exception as e:
        print(f"[-] 连接失败: {type(e).__name__}: {e}")

if __name__ == "__main__":
    asyncio.run(test_ws_proxy())