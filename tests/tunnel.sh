#!/bin/bash
# SSH 隧道管理腳本
# 用於啟動、停止和檢查 SSH 隧道狀態

set -e

# 載入 .env 文件
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "❌ 找不到 .env 文件！"
    echo "請先執行: cp .env.example .env"
    exit 1
fi

# 顏色定義
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 隧道配置
MSSQL_PORT=1433
MARIADB_PORT=3306
SSH_CONNECTION="${SSH_USER}@${REMOTE_HOST}"

# 函式：顯示狀態
show_status() {
    echo -e "${BLUE}==============================================================${NC}"
    echo -e "${BLUE}  SSH 隧道狀態檢查${NC}"
    echo -e "${BLUE}==============================================================${NC}"
    
    # 檢查 SSH 進程
    if ps aux | grep -q "[s]sh.*${SSH_CONNECTION}"; then
        echo -e "${GREEN}✅ SSH 隧道進程正在運行${NC}"
        echo ""
        ps aux | grep "[s]sh.*${SSH_CONNECTION}" | head -n 1
        echo ""
    else
        echo -e "${RED}❌ 未找到 SSH 隧道進程${NC}"
        echo ""
    fi
    
    # 檢查端口
    echo -e "${BLUE}連接埠狀態:${NC}"
    
    if lsof -i :${MSSQL_PORT} > /dev/null 2>&1; then
        echo -e "  MSSQL (${MSSQL_PORT}):   ${GREEN}✅ 正在監聽${NC}"
    else
        echo -e "  MSSQL (${MSSQL_PORT}):   ${RED}❌ 未監聽${NC}"
    fi
    
    if lsof -i :${MARIADB_PORT} > /dev/null 2>&1; then
        echo -e "  MariaDB (${MARIADB_PORT}): ${GREEN}✅ 正在監聽${NC}"
    else
        echo -e "  MariaDB (${MARIADB_PORT}): ${RED}❌ 未監聽${NC}"
    fi
    
    echo ""
}

# 函式：啟動隧道
start_tunnel() {
    echo -e "${BLUE}==============================================================${NC}"
    echo -e "${BLUE}  啟動 SSH 隧道${NC}"
    echo -e "${BLUE}==============================================================${NC}"
    
    # 檢查是否已經運行
    if ps aux | grep -q "[s]sh.*${SSH_CONNECTION}"; then
        echo -e "${YELLOW}⚠️  SSH 隧道已經在運行！${NC}"
        show_status
        return
    fi
    
    echo -e "正在建立 SSH 隧道到 ${GREEN}${SSH_CONNECTION}${NC}..."
    echo -e "轉發端口: ${GREEN}${MSSQL_PORT}${NC} (MSSQL), ${GREEN}${MARIADB_PORT}${NC} (MariaDB)"
    echo ""
    
    # 啟動 SSH 隧道
    ssh -f -N -L ${MSSQL_PORT}:localhost:${MSSQL_PORT} -L ${MARIADB_PORT}:localhost:${MARIADB_PORT} ${SSH_CONNECTION}
    
    # 等待一下確保連接建立
    sleep 2
    
    echo -e "${GREEN}✅ SSH 隧道已啟動！${NC}"
    echo ""
    show_status
}

# 函式：停止隧道
stop_tunnel() {
    echo -e "${BLUE}==============================================================${NC}"
    echo -e "${BLUE}  停止 SSH 隧道${NC}"
    echo -e "${BLUE}==============================================================${NC}"
    
    # 找到並終止進程
    if ps aux | grep -q "[s]sh.*${SSH_CONNECTION}"; then
        echo -e "正在終止 SSH 隧道進程..."
        pkill -f "ssh.*${SSH_CONNECTION}" || true
        sleep 1
        echo -e "${GREEN}✅ SSH 隧道已停止${NC}"
    else
        echo -e "${YELLOW}⚠️  未找到運行中的 SSH 隧道${NC}"
    fi
    
    echo ""
    show_status
}

# 函式：重啟隧道
restart_tunnel() {
    echo -e "${BLUE}==============================================================${NC}"
    echo -e "${BLUE}  重啟 SSH 隧道${NC}"
    echo -e "${BLUE}==============================================================${NC}"
    
    stop_tunnel
    sleep 1
    start_tunnel
}

# 函式：顯示幫助
show_help() {
    echo -e "${BLUE}==============================================================${NC}"
    echo -e "${BLUE}  SSH 隧道管理工具${NC}"
    echo -e "${BLUE}==============================================================${NC}"
    echo ""
    echo "用法: $0 {start|stop|restart|status|help}"
    echo ""
    echo "命令:"
    echo "  start   - 啟動 SSH 隧道"
    echo "  stop    - 停止 SSH 隧道"
    echo "  restart - 重啟 SSH 隧道"
    echo "  status  - 顯示隧道狀態"
    echo "  help    - 顯示此幫助訊息"
    echo ""
    echo "配置:"
    echo "  遠端主機:   ${REMOTE_HOST}"
    echo "  SSH 使用者: ${SSH_USER}"
    echo "  MSSQL 端口:   ${MSSQL_PORT}"
    echo "  MariaDB 端口: ${MARIADB_PORT}"
    echo ""
    echo "範例:"
    echo "  $0 start   # 啟動隧道"
    echo "  $0 status  # 檢查狀態"
    echo "  $0 stop    # 停止隧道"
    echo ""
}

# 主程式
case "${1:-}" in
    start)
        start_tunnel
        ;;
    stop)
        stop_tunnel
        ;;
    restart)
        restart_tunnel
        ;;
    status)
        show_status
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo -e "${RED}❌ 未知的命令: ${1:-}${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac
