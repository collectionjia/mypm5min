#!/bin/bash

# 精准查找 poly 进程，排除 grep 自身
PIDS=$(ps -ef | grep poly | grep -v grep | awk '{print $2}')

# 如果有进程就杀死
if [ -n "$PIDS" ]; then
    echo "正在杀死进程: $PIDS"
    kill -9 $PIDS
    echo "杀死完成"
else
    echo "未找到 poly 相关进程"
fi
