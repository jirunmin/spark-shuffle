import requests
import pandas as pd
from datetime import datetime

API = "http://spark-master:18080/api/v1/applications"

# 只取前 36 个实验
MAX_COUNT = 36

def parse_name(app_name):
    """
    ShuffleTest-hash-reduce-64m
    返回 (manager, workload, size)
    """
    parts = app_name.split("-")
    return parts[1], parts[2], parts[3]

def parse_time(t):
    # Spark 1.6 timestamp example: 2025-12-07T08:06:56.686GMT
    t = t.replace("GMT", "")
    return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%f")

apps = requests.get(API).json()

rows = []

for app in apps[:MAX_COUNT]:
    app_id = app["id"]
    name = app["name"]

    manager, workload, size = parse_name(name)

    att = app["attempts"][0]
    start = parse_time(att["startTime"])
    end = parse_time(att["endTime"])
    runtime = (end - start).total_seconds()

    rows.append({
        "app_id": app_id,
        "name": name,
        "manager": manager,
        "workload": workload,
        "size": size,
        "start": att["startTime"],
        "end": att["endTime"],
        "runtime_sec": runtime
    })

df = pd.DataFrame(rows)
df.to_csv("metrics_runtime_only.csv", index=False)

print(df)
print("\nSaved to metrics_runtime_only.csv")
