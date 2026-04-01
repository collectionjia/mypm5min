"""
WebSocket 代理：通过 HTTP CONNECT 隧道转发 WebSocket 连接。
用法：python3 ws_proxy.py
会在本地 8443 端口启动 WebSocket 服务，通过代理转发到 Polymarket。
"""
import asyncio
import aiohttp
import websockets

POLYMARKET_WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"
LISTEN_PORT = 8443
PROXY = "http://127.0.0.1:7890"
CONNECT_TIMEOUT = 15  # 远程连接超时（秒）


async def proxy_handler(client_ws):
    """接收本地客户端连接，转发到远程 WebSocket。"""
    path = client_ws.request.path
    remote_url = f"{POLYMARKET_WS_BASE}{path}"
    print(f"[*] 新连接路径: {path} -> {remote_url}")

    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=CONNECT_TIMEOUT))
    try:
        remote_ws = await session.ws_connect(remote_url, proxy=PROXY)
        print(f"[+] 远程连接成功: {remote_url}")
    except asyncio.TimeoutError:
        print(f"[-] 远程连接超时 ({CONNECT_TIMEOUT}s): {remote_url}")
        await client_ws.close(1014, "upstream connect timeout")
        await session.close()
        return
    except Exception as e:
        print(f"[-] 远程连接失败: {type(e).__name__}: {e}")
        await client_ws.close(1014, f"upstream connect failed: {e}")
        await session.close()
        return

    async def client_to_remote():
        """本地客户端 -> 远程服务器"""
        try:
            async for msg in client_ws:
                if msg == "ping":
                    await remote_ws.ping()
                elif remote_ws.closed:
                    break
                else:
                    await remote_ws.send_str(msg)
        except websockets.ConnectionClosed:
            pass
        finally:
            print("[*] 客户端->远程 连接关闭")
            if not remote_ws.closed:
                await remote_ws.close()

    async def remote_to_client():
        """远程服务器 -> 本地客户端"""
        try:
            async for msg in remote_ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await client_ws.send(msg.data)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    await client_ws.send(msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
                elif msg.type == aiohttp.WSMsgType.PING:
                    await remote_ws.pong()
        except websockets.ConnectionClosed:
            pass
        finally:
            print("[*] 远程->客户端 连接关闭")

    try:
        await asyncio.gather(client_to_remote(), remote_to_client())
    finally:
        if not remote_ws.closed:
            await remote_ws.close()
        await session.close()


async def main():
    print(f"[*] WebSocket 代理监听 ws://0.0.0.0:{LISTEN_PORT}")
    print(f"[*] 转发目标: {POLYMARKET_WS_BASE} (via {PROXY})")
    async with websockets.serve(
        proxy_handler, "0.0.0.0", LISTEN_PORT,
        ping_interval=None, ping_timeout=None,
    ):
        await asyncio.Future()  # 永久运行


if __name__ == "__main__":
    asyncio.run(main())
