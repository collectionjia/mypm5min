#!/bin/bash
# 代理服务器启动脚本

# 配置参数
LISTEN_PORT=8888
UPSTREAM_PROXY=""
UPSTREAM_PORT=7890

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== 代理转发服务器启动脚本 ===${NC}"

# 检查Python3是否可用
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}错误: 未找到Python3，请先安装Python3${NC}"
    exit 1
fi

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -l|--listen)
            LISTEN_PORT="$2"
            shift 2
            ;;
        -p|--proxy)
            UPSTREAM_PROXY="$2"
            shift 2
            ;;
        -t|--tport)
            UPSTREAM_PORT="$2"
            shift 2
            ;;
        -h|--help)
            echo "用法: $0 [选项]"
            echo "选项:"
            echo "  -l, --listen <端口>     本地监听端口 (默认: 8888)"
            echo "  -p, --proxy <地址>      上游代理地址 (可选)"
            echo "  -t, --tport <端口>      上游代理端口 (默认: 7890)"
            echo "  -h, --help              显示帮助信息"
            exit 0
            ;;
        *)
            echo -e "${RED}未知选项: $1${NC}"
            exit 1
            ;;
    esac
done

# 启动命令
CMD="python3 proxy_server.py -l $LISTEN_PORT"

if [ -n "$UPSTREAM_PROXY" ]; then
    CMD="$CMD -p $UPSTREAM_PROXY -t $UPSTREAM_PORT"
fi

echo -e "${YELLOW}启动代理服务器...${NC}"
echo "监听端口: $LISTEN_PORT"
if [ -n "$UPSTREAM_PROXY" ]; then
    echo "上游代理: $UPSTREAM_PROXY:$UPSTREAM_PORT"
else
    echo "上游代理: 无 (直接连接)"
fi
echo ""

# 执行启动命令
eval $CMD
