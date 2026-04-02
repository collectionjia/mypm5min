#!/bin/bash

# 脚本名称: poly_bot_start.sh
# 功能: 自动化部署并启动 poly_5min_bot 程序
# 作者: 自定义
# 日期: 2026

# ======================== 配置项 ========================
# 日志文件路径
LOG_FILE="ab8.log"
# 程序名称
APP_NAME="poly_5min_bot"
# 程序编译路径
APP_PATH="./target/debug/${APP_NAME}"
# ======================== 工具函数 ========================

# 彩色输出函数
function echo_color() {
    case $1 in
        red)     echo -e "\033[31m$2\033[0m" ;;
        green)   echo -e "\033[32m$2\033[0m" ;;
        yellow)  echo -e "\033[33m$2\033[0m" ;;
        blue)    echo -e "\033[34m$2\033[0m" ;;
        purple)  echo -e "\033[35m$2\033[0m" ;;
        cyan)    echo -e "\033[36m$2\033[0m" ;;
        *)       echo "$2" ;;
    esac
}

# 检查命令执行是否成功
function check_success() {
    if [ $? -ne 0 ]; then
        echo_color red "❌ 错误: $1 执行失败，脚本退出"
        exit 1
    fi
}

# 检查并杀死已运行的进程
function kill_existing_process() {
    echo_color blue "🔍 检查是否有已运行的 ${APP_NAME} 进程..."
    # 查找进程ID（排除grep自身）
    PID=$(ps aux | grep "${APP_PATH}" | grep -v grep | awk '{print $2}')
    
    if [ -n "${PID}" ]; then
        echo_color yellow "⚠️  发现已运行的进程，PID: ${PID}，正在终止..."
        kill -9 "${PID}"
        check_success "终止旧进程"
        echo_color green "✅ 旧进程已成功终止"
    else
        echo_color green "✅ 未发现已运行的 ${APP_NAME} 进程"
    fi
}

# ======================== 主程序 ========================
clear
echo_color cyan "============================================="
echo_color cyan "        开始部署并启动 ${APP_NAME}        "
echo_color cyan "============================================="

# 1. 拉取最新代码
echo_color blue "\n📥 正在拉取最新代码..."
git pull
check_success "git pull"

# 2. 编译项目
echo_color blue "\n🔨 正在编译项目 (cargo build)..."
cargo build
check_success "cargo build"

# 3. 检查编译产物是否存在
if [ ! -f "${APP_PATH}" ]; then
    echo_color red "❌ 编译产物不存在: ${APP_PATH}"
    exit 1
fi

# 4. 终止已有进程
kill_existing_process

# 5. 备份旧日志（可选）
if [ -f "${LOG_FILE}" ]; then
    echo_color blue "\n📋 备份旧日志文件..."
    mv "${LOG_FILE}" "${LOG_FILE}.$(date +%Y%m%d_%H%M%S).bak"
    check_success "备份日志"
fi

# 6. 启动程序
echo_color blue "\n🚀 启动 ${APP_NAME} 程序..."
nohup "${APP_PATH}" > "${LOG_FILE}" 2>&1 &
check_success "启动程序"

# 获取新进程ID
NEW_PID=$!
echo_color green "✅ 程序已成功启动，PID: ${NEW_PID}"
echo_color blue "📝 日志文件: ${LOG_FILE}"

# 7. 实时查看日志（可选，按 Ctrl+C 退出查看但程序仍在后台运行）
echo_color cyan "\n============================================="
echo_color cyan "          实时查看日志 (Ctrl+C 退出)          "
echo_color cyan "============================================="
tail -f "${LOG_FILE}"
