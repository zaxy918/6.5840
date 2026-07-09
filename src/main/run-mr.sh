#!/bin/bash

# ========== 配置项 ==========
SOCK_NAME="sock123"       # 套接字文件名
WORKER_COUNT=3            # 启动的 Worker 数量
PLUGIN_FILE="wc.so"       # 插件文件名
INPUT_FILES="pg*.txt"     # 输入文件匹配规则
# ============================

# 中断时自动清理所有子进程和套接字
trap 'echo -e "\n检测到中断，正在清理进程..."; pkill -P $$ 2>/dev/null; rm -f $SOCK_NAME; exit 1' INT TERM

echo "====== 步骤1：清理旧环境 ======"
rm -f $SOCK_NAME
rm -rf ./logs
rm -mr-out-*
mkdir ./logs
# 杀掉残留的相关进程，避免端口/套接字占用
pkill -f "mrcoordinator.go" 2>/dev/null
pkill -f "mrworker.go" 2>/dev/null
sleep 0.5
echo "✅ 旧环境清理完成"

echo "====== 步骤2：编译插件 ======"
go build -buildmode=plugin ../mrapps/wc.go
if [ $? -ne 0 ]; then
    echo "❌ 插件编译失败，脚本终止"
    exit 1
fi
echo "✅ 插件编译完成"

echo "====== 步骤3：启动 Coordinator（日志输出到终端） ======"
# 后台运行，不重定向输出，日志直接打印到当前终端
go run mrcoordinator.go "$SOCK_NAME" $INPUT_FILES &
COORD_PID=$!
echo "Coordinator 进程ID: $COORD_PID"

# 等待协调器初始化完成
sleep 1

# 检查协调器是否正常启动
if ! kill -0 "$COORD_PID" 2>/dev/null; then
    echo "❌ Coordinator 启动失败"
    exit 1
fi
echo "✅ Coordinator 启动成功，后续日志将直接输出到终端"
echo "----------------------------------------"

echo ""
echo "====== 步骤4：启动 $WORKER_COUNT 个 Worker（日志写入文件） ======"
for ((i=1; i<=WORKER_COUNT; i++)); do
    # Worker 输出全部重定向到独立日志文件，不污染终端
    go run mrworker.go "$PLUGIN_FILE" "$SOCK_NAME" > "./logs/worker-$i.log" 2>&1 &
    echo "Worker $i 启动，进程ID: $!，日志文件: worker-$i.log"
done

echo ""
echo "====== 等待所有任务执行完成 ======"
# 等待所有后台进程退出
wait

echo ""
echo "----------------------------------------"
echo "====== 全部执行结束 ======"
rm -f "$SOCK_NAME"
echo "套接字已清理"
echo "Worker 日志可查看：worker-1.log ~ worker-$WORKER_COUNT.log"