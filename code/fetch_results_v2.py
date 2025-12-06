import requests
import pandas as pd
from datetime import datetime

HISTORY_SERVER = "http://spark-master:18080"

def parse_time(t):
    t = t.replace("GMT", "")
    return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%f")

def parse_app_name(name):
    # ShuffleTest-hash-reduce-16m
    parts = name.split("-")
    manager = parts[1]
    workload = parts[2]
    size = int(parts[3].replace("m", ""))
    return manager, workload, size

apps = requests.get(f"{HISTORY_SERVER}/api/v1/applications").json()

records = []

for app in apps:
    name = app["name"]
    manager, workload, size = parse_app_name(name)

    for att in app["attempts"]:
        start = att.get("startTime")
        end = att.get("endTime")

        if start and end:
            t_start = parse_time(start)
            t_end = parse_time(end)
            duration_s = (t_end - t_start).total_seconds()
        else:
            duration_s = None

        records.append({
            "manager": manager,
            "workload": workload,
            "size_m": size,
            "duration_s": duration_s,
            "app": name,
        })

df = pd.DataFrame(records)

# 按 size 排序，再按 manager/workload 排序
df = df.sort_values(["workload", "manager", "size_m"])

df.to_csv("spark_results.csv", index=False)
print(df)
