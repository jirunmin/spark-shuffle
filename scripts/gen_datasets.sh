#!/usr/bin/env bash
set -e

DATA_DIR=/home/spark/data

mkdir -p "$DATA_DIR"

gen_file () {
  local n_records=$1
  local filename=$2

  echo "Generating $n_records records into $filename ..."

  python3 - "$n_records" "$filename" << 'PYEOF'
import sys, random

n = int(sys.argv[1])
path = sys.argv[2]

random.seed(42)

with open(path, 'w') as f:
    for _ in range(n):
        x = random.randint(0, 10**9 - 1)
        y = random.randint(0, 10**9 - 1)
        z = random.randint(0, 10**9 - 1)
        f.write("{},{},{}\n".format(x, y, z))
PYEOF
}

gen_file  2000000  "$DATA_DIR/data_2m.csv"
gen_file  4000000  "$DATA_DIR/data_4m.csv"
gen_file  8000000  "$DATA_DIR/data_8m.csv"
gen_file 16000000  "$DATA_DIR/data_16m.csv"
gen_file 32000000  "$DATA_DIR/data_32m.csv"
gen_file 64000000  "$DATA_DIR/data_64m.csv"

echo "All datasets generated under $DATA_DIR"
