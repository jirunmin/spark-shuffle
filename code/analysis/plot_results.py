import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# --------------------------------------------------------
# 1. Load CSV
# --------------------------------------------------------
df = pd.read_csv("metrics_runtime_only.csv")

# --------------------------------------------------------
# 2. Fix wrong labeling
# --------------------------------------------------------
mask_skew = df["size"] == "skew"

df.loc[mask_skew, "workload"] = "sort-skew"
df.loc[mask_skew, "size"] = df.loc[mask_skew, "name"].str.split("-").str[-1]

def normalize_size(x):
    return int(x.replace("m", ""))

df["size_num"] = df["size"].apply(normalize_size)

# --------------------------------------------------------
# 3. Pretty colors
# --------------------------------------------------------
manager_colors = {
    "hash": "#1f77b4",   # blue
    "sort": "#ff7f0e",   # orange
}

manager_markers = {
    "hash": "o",
    "sort": "s",
}

# --------------------------------------------------------
# 4. Plot helper
# --------------------------------------------------------
def plot_workload(df, workload_name, output_file):
    sub = df[df["workload"] == workload_name]

    if sub.empty:
        print(f"[Skip] No data for workload = {workload_name}")
        return

    managers = ["hash", "sort"]

    plt.figure(figsize=(8, 5))

    for mgr in managers:
        mgr_df = sub[sub["manager"] == mgr].sort_values("size_num")

        if mgr_df.empty:
            continue

        plt.plot(
            mgr_df["size_num"],
            mgr_df["runtime_sec"],
            marker=manager_markers[mgr],
            markersize=8,
            linewidth=2.2,
            color=manager_colors[mgr],
            label=f"{mgr}"
        )

    plt.xlabel("Dataset Size (millions of rows)", fontsize=12)
    plt.ylabel("Runtime (sec)", fontsize=12)
    plt.title(f"Runtime Comparison – workload = {workload_name}", fontsize=14)
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(output_file, dpi=200)
    plt.close()
    print(f"[Saved] {output_file}")

# --------------------------------------------------------
# 5. Plot reduce / sort / sort-skew ONLY
# --------------------------------------------------------
plot_workload(df, "reduce", "plot_reduce.png")
plot_workload(df, "sort", "plot_sort.png")
plot_workload(df, "sort-skew", "plot_sort-skew.png")

print("All individual plots saved (overall plot removed).")
