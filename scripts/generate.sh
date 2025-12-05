#!/usr/bin/env bash
set -e

DATA_DIR=/home/spark/data
mkdir -p "$DATA_DIR"

# 行数与文件名映射
SIZES=(
  "8000000:8m"
  "16000000:16m"
  "32000000:32m"
  "64000000:64m"
  "128000000:128m"
  "256000000:256m"
)

for ITEM in "${SIZES[@]}"; do
    N="${ITEM%%:*}"         # 取冒号左边 → 行数
    LABEL="${ITEM##*:}"     # 取冒号右边 → 文件标签

    OUTFILE="$DATA_DIR/data_${LABEL}.csv"

    echo ">>> Generating $OUTFILE ..."
    python3 generate.py "$N" "$OUTFILE"
    echo ">>> Done: $OUTFILE"
done

echo ">>> All datasets generated under $DATA_DIR"
