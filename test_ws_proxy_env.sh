#!/bin/bash
"""
WebSocket 代理测试脚本：设置环境变量并运行 Rust 程序测试 WebSocket 连接。
用法：bash test_ws_proxy_env.sh
"""

echo "=== WebSocket 代理测试 ==="
echo "1. 启动 WebSocket 代理服务器"

# 启动 WebSocket 代理服务器
python3 ws_proxy.py &
PROXY_PID=$!
echo "   代理服务器进程 ID: $PROXY_PID"
echo "   等待 3 秒启动..."
sleep 3

# 检查代理服务器是否正在运行
if ps -p $PROXY_PID > /dev/null; then
    echo "   ✅ 代理服务器启动成功"
else
    echo "   ❌ 代理服务器启动失败"
    exit 1
fi

echo "2. 设置环境变量"
export WS_PROXY_URL="ws://localhost:8443"
echo "   WS_PROXY_URL: $WS_PROXY_URL"

echo "3. 运行 Rust 程序测试 WebSocket 连接"
echo "   注意：程序会在 10 秒后自动退出"

# 运行 Rust 程序，设置超时
timeout 10s cargo run --bin poly_5min_bot

# 停止代理服务器
echo "4. 停止代理服务器"
kill $PROXY_PID
echo "   代理服务器已停止"

echo "=== 测试完成 ==="