#!/usr/bin/env python3
import requests
import sys
import pandas as pd

HISTORY_SERVER = "http://spark-master:18080/api/v1/applications"


def fetch_apps():
    apps = requests.get(HISTORY_SERVER).json()
    result = []
    for a in apps:
        if a["name"].startswith("ShuffleTest"):
            result.append((a["id"], a["name"]))
    return result[-36:]   # 只取最新36个


# ------------------------------------------------------------
# 获取某个 app 的 spill 指标
# ------------------------------------------------------------
def fetch_spill_for_app(app_id):
    stages_url = f"{HISTORY_SERVER}/{app_id}/stages"

    try:
        stages = requests.get(stages_url).json()
    except:
        return None

    total_mem_spill = 0
    total_disk_spill = 0

    # 遍历 Stage，进一步访问任务级 API
    for st in stages:
        sid = st["stageId"]
        attempt = st["attemptId"]

        detail_url = f"{HISTORY_SERVER}/{app_id}/stages/{sid}/{attempt}"

        try:
            stage_detail = requests.get(detail_url).json()
        except:
            continue

        # tasks 含有溢写指标
        tasks = stage_detail.get("tasks", {})
        for _, t in tasks.items():
            metrics = t.get("taskMetrics", {})
            total_mem_spill += metrics.get("memoryBytesSpilled", 0)
            total_disk_spill += metrics.get("diskBytesSpilled", 0)

    return total_mem_spill, total_disk_spill


# ------------------------------------------------------------
def main(output_csv):
    apps = fetch_apps()
    print(f"Found {len(apps)} ShuffleTest apps (latest 36)")

    rows = []

    for app_id, name in apps:
        print(f"Processing {app_id} ...")
        res = fetch_spill_for_app(app_id)

        if res is None:
            print(f"  ⚠️ {app_id}: cannot read stage data, skip")
            continue

        mem_spill, disk_spill = res

        # parse name: ShuffleTest-hash-sort-32m
        parts = name.split("-")
        manager = parts[1]
        workload = parts[2]
        size = parts[3]

        rows.append({
            "app_id": app_id,
            "name": name,
            "manager": manager,
            "workload": workload,
            "size": size,
            "spill_memory_MB": mem_spill / (1024 * 1024),
            "spill_disk_MB": disk_spill / (1024 * 1024),
        })

    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False)
    print("\n=== Saved to", output_csv, "===\n")
    print(df)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 extract_shuffle_spill.py <output.csv>")
        sys.exit(1)

    main(sys.argv[1])
