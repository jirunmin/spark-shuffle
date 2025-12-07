#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import sys

HISTORY_SERVER = "http://spark-master:18080"

# --------------------------------------------
# Parse ShuffleTest name → manager / workload / size
# --------------------------------------------
def parse_name(name):
    parts = name.split("-")
    manager = parts[1]      # hash / sort
    workload = parts[2]     # sort / reduce
    tail = parts[3:]

    # detect skew workload
    if tail[0] == "skew":
        workload = workload + "-skew"
        size = tail[1]
    else:
        size = tail[0]

    return manager, workload, size


# --------------------------------------------
# Fetch only the *latest 36 ShuffleTest apps*
# --------------------------------------------
def fetch_latest_36():
    print(f"Fetching applications from {HISTORY_SERVER}/api/v1/applications")

    apps = requests.get(f"{HISTORY_SERVER}/api/v1/applications").json()

    # keep only ShuffleTest apps
    apps = [a for a in apps if a.get("name", "").startswith("ShuffleTest")]

    # sort by time — Spark API already returns newest first
    apps = apps[:36]  # KEEP ONLY FIRST 36

    print(f"Found {len(apps)} ShuffleTest apps (using latest 36)")

    rows = []
    for app in apps:
        app_id = app["id"]
        name = app["name"]

        manager, workload, size = parse_name(name)

        rows.append({
            "app_id": app_id,
            "name": name,
            "manager": manager,
            "workload": workload,
            "size": size
        })

    return pd.DataFrame(rows)


# --------------------------------------------
# Fetch shuffle metrics for a single app_id
# --------------------------------------------
def fetch_shuffle(app_id):
    url = f"{HISTORY_SERVER}/api/v1/applications/{app_id}/stages"
    resp = requests.get(url)

    if resp.status_code != 200:
        print(f"  ⚠️  {app_id}: HTTP {resp.status_code}, skip")
        return 0, 0

    try:
        stages = resp.json()
    except:
        print(f"  ⚠️  {app_id}: Invalid JSON, skip")
        return 0, 0

    read_total = 0
    write_total = 0

    for st in stages:
        read_total += st.get("shuffleReadBytes", 0)
        write_total += st.get("shuffleWriteBytes", 0)

    return read_total, write_total


# --------------------------------------------
# Main
# --------------------------------------------
def main(out_csv):
    df_apps = fetch_latest_36()

    results = []

    for _, row in df_apps.iterrows():
        app_id = row["app_id"]
        print(f"Processing {app_id} ...")

        read_b, write_b = fetch_shuffle(app_id)

        results.append({
            "app_id": app_id,
            "name": row["name"],
            "manager": row["manager"],
            "workload": row["workload"],
            "size": row["size"],
            "shuffle_read_MB": round(read_b / (1024 * 1024), 3),
            "shuffle_write_MB": round(write_b / (1024 * 1024), 3),
        })

    df = pd.DataFrame(results)
    df.to_csv(out_csv, index=False)

    print(f"\n=== Saved to {out_csv} ===")
    print(df)


if __name__ == "__main__":
    main(sys.argv[1])
