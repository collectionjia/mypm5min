"""
WebSocket 代理：通过 HTTP CONNECT 隧道转发 WebSocket 连接。
用法：python3 ws_proxy.py
会在本地 8443 端口启动 WebSocket 服务，通过代理转发到 Polymarket。
"""
import asyncio
import websockets
from websockets.asyncio.client import connect

POLYMARKET_WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"
LISTEN_PORT = 8443
PROXY = "http://127.0.0.1:7890"

async def proxy_handler(client_ws):
    """接收本地客户端连接，转发到远程 WebSocket。"""
    # SDK 会访问 /ws/market 或 /ws/user 等路径
    path = client_ws.request.path
    remote_url = f"{POLYMARKET_WS_BASE}{path}"
    print(f"[*] 新连接路径: {path} -> {remote_url}")
    try:
        remote_ws = await connect(remote_url, proxy=PROXY)
        print(f"[+] 远程连接成功: {remote_url}")
    except Exception as e:
        print(f"[-] 远程连接失败: {e}")
        await client_ws.close()
        return

    async def forward(src, dst, label):
        try:
            async for msg in src:
                await dst.send(msg)
        except websockets.ConnectionClosed:
            pass
        finally:
            print(f"[*] {label} 方向连接关闭")

    await asyncio.gather(
        forward(client_ws, remote_ws, "客户端->远程"),
        forward(remote_ws, client_ws, "远程->客户端"),
    )

async def main():
    print(f"[*] WebSocket 代理监听 ws://0.0.0.0:{LISTEN_PORT}")
    print(f"[*] 转发目标: {POLYMARKET_WS_BASE} (via {PROXY})")
    async with websockets.serve(proxy_handler, "0.0.0.0", LISTEN_PORT):
        await asyncio.Future()  # 永久运行

if __name__ == "__main__":
    asyncio.run(main())
