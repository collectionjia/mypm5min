#!/bin/bash

# ====================== 配置项（可修改）======================
PROGRAM="./target/debug/poly_5min_bot"  # 你的程序路径
LOG_FILE="ab.log"                       # 日志文件
PROCESS_KEY="poly_5min_bot"             # 进程关键字（精准匹配）
# =============================================================

echo "===== 开始停止旧进程 ====="

# 查找所有匹配的进程 PID（排除 grep 自身）
PIDS=$(ps -ef | grep "$PROCESS_KEY" | grep -v grep | awk '{print $2}')

if [ -n "$PIDS" ]; then
    echo "找到旧进程 PID: $PIDS"
    # 强制杀死进程
    kill -9 $PIDS
    echo "已杀死旧进程"
    sleep 1  # 等待进程释放资源
else
    echo "未找到运行中的 $PROCESS_KEY 进程"
fi

echo -e "\n===== 启动新进程 ====="

# 后台启动程序，输出日志
nohup $PROGRAM > $LOG_FILE 2>&1 &

# 等待 1 秒确保进程启动
sleep 1

# 检查是否启动成功
NEW_PID=$(ps -ef | grep "$PROCESS_KEY" | grep -v grep | awk '{print $2}')
if [ -n "$NEW_PID" ]; then
    echo "✅ 启动成功！新进程 PID: $NEW_PID"
else
    echo "❌ 启动失败！请检查程序或日志"
    exit 1
fi

echo -e "\n===== 实时查看日志 ====="
tail -f $LOG_FILE
