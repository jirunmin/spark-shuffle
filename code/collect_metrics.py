import requests
import pandas as pd

HISTORY = "http://spark-master:18080/api/v1/applications"

# 解析 Workload 名
def parse_app_name(name):
    # 例如 ShuffleTest-hash-sort-32m → ("hash","sort","32m")
    name = name.replace("ShuffleTest-", "")
    parts = name.split("-")
    return parts[0], parts[1], parts[2]

apps = requests.get(HISTORY).json()
records = []

for app in apps:
    app_id = app["id"]
    name = app["name"]
    start = app["attempts"][0].get("startTime")
    end = app["attempts"][0].get("endTime")

    try:
        duration = (pd.to_datetime(end) - pd.to_datetime(start)).total_seconds()
    except:
        duration = None

    # === 获取 Stage 级指标 ===
    stages = requests.get(f"{HISTORY}/{app_id}/stages").json()

    shuffle_read = 0
    shuffle_write = 0
    spill_mem = 0
    spill_disk = 0
    fetch_wait = 0
    cpu_time = 0
    gc_time = 0
    run_time = 0

    for s in stages:
        print(s)
        shuffle_read += s.get("shuffleReadBytes", 0)
        shuffle_write += s.get("shuffleWriteBytes", 0)

        spill_mem += s.get("memoryBytesSpilled", 0)
        spill_disk += s.get("diskBytesSpilled", 0)

        fetch_wait += sum([t.get("fetchWaitTime", 0) for t in s.get("tasks", {}).values()])
        cpu_time += sum([t.get("executorCpuTime", 0) for t in s.get("tasks", {}).values()])
        gc_time += sum([t.get("jvmGcTime", 0) for t in s.get("tasks", {}).values()])
        run_time += sum([t.get("executorRunTime", 0) for t in s.get("tasks", {}).values()])

    mode1, mode2, size = parse_app_name(name)

    records.append({
        "app": name,
        "mode1": mode1,
        "mode2": mode2,
        "size": size,
        "duration_s": duration,
        "shuffle_read": shuffle_read,
        "shuffle_write": shuffle_write,
        "spill_mem": spill_mem,
        "spill_disk": spill_disk,
        "fetch_wait": fetch_wait,
        "cpu_time": cpu_time,
        "gc_time": gc_time,
        "run_time": run_time
    })

df = pd.DataFrame(records)
df.to_csv("spark_metrics.csv", index=False)
print("\n=== Saved to spark_metrics.csv ===\n")
print(df)
