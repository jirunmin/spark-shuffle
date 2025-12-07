#!/bin/bash
set -e

echo "==== Spark 全节点清理脚本 ===="

# Worker 列表
WORKERS=("spark-worker1" "spark-worker2")

echo ">>> 清理 Master 节点..."

# 1. 删除结果目录
rm -rf /home/spark/results
mkdir -p /home/spark/results

# 2. 删除 Master 数据集
echo " - 删除 Master 数据集"
rm -f /home/spark/data/data_*.csv

# 3. 删除 Spark work 临时文件
rm -rf /home/spark/spark-1.6.3/work/*

# 4. 删除 /tmp 的临时文件
rm -rf /tmp/spark* /tmp/*eventlog* 2>/dev/null || true

echo ">>> Master 清理完成"

# ======================== Worker 清理 =========================
for w in "${WORKERS[@]}"; do
    echo ">>> 清理 Worker: $w ..."

    # 删除 Worker 的数据集
    ssh spark@$w "rm -f /home/spark/data/data_*.csv"

    # 清理 Spark work
    ssh spark@$w "rm -rf /home/spark/spark-1.6.3/work/*"

    # 清理临时文件
    ssh spark@$w "rm -rf /tmp/spark* /tmp/*eventlog* 2>/dev/null || true"

    echo ">>> Worker $w 清理完成"
done

echo "==== 清理完成！===="
echo "请执行 df -h 查看空间情况"
