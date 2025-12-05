#!/usr/bin/env bash
set -e

DATA_DIR=/home/spark/data
WORKERS=("spark-worker1" "spark-worker2")

echo ">>> Checking local data directory..."
ls -lh $DATA_DIR

echo ">>> Distributing data files to workers..."

for W in "${WORKERS[@]}"; do
    echo ">>> Sending files to $W ..."
    ssh spark@$W "mkdir -p $DATA_DIR"
    scp $DATA_DIR/*.csv spark@$W:$DATA_DIR/
    echo ">>> Done sending to $W"
done

echo ">>> Verify remote directories"
for W in "${WORKERS[@]}"; do
    echo "--- $W:"
    ssh spark@$W "ls -lh $DATA_DIR"
done

echo ">>> Distribution completed."
