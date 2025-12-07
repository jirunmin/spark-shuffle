#!/bin/bash

cd /home/spark/shuffle-app

sizes="2m 4m 8m 16m 32m 64m"
shuffles="hash sort"
workloads="reduce sort sort-skew"

for sz in $sizes; do
  for shuffle in $shuffles; do
    for workload in $workloads; do

      echo "==== Running ${shuffle}-${workload}-${sz} ===="

      out="/home/spark/results/${shuffle}_${workload}_${sz}"
      rm -rf $out

      spark-submit \
        --class ShuffleTest \
        --master spark://spark-master:7077 \
        target/scala-2.10/shuffle-test_2.10-0.1.jar \
        file:/home/spark/data/data_${sz}.csv \
        file:${out} \
        ${shuffle} \
        ${workload} \
        ${sz}

    done
  done
done
