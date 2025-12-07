import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv("metrics_runtime_only.csv")

# 修正错误：size=skew → workload=sort-skew + 修正size
mask_skew = df["size"] == "skew"
df.loc[mask_skew, "workload"] = "sort-skew"
df.loc[mask_skew, "size"] = df.loc[mask_skew, "name"].str.split("-").str[-1]

def normalize_size(x):
    return int(x.replace("m", ""))

df["size_num"] = df["size"].apply(normalize_size)

# 按 workload pivot 成表格 (size × workload)
pivot = df.pivot_table(
    index="size_num",
    columns="workload",
    values="runtime_sec"
).sort_index()

print(pivot)

# 画 grouped bar chart
plt.figure(figsize=(12, 6))

sizes = pivot.index
workloads = pivot.columns

bar_width = 0.25
positions = np.arange(len(sizes))

for i, w in enumerate(workloads):
    plt.bar(
        positions + i * bar_width,
        pivot[w],
        width=bar_width,
        label=w
    )

plt.xticks(positions + bar_width, sizes)
plt.xlabel("Dataset Size (millions of rows)")
plt.ylabel("Runtime (sec)")
plt.title("Runtime Comparison (Grouped Bar Chart)")
plt.legend()
plt.grid(axis="y", linestyle="--", alpha=0.5)

plt.tight_layout()
plt.savefig("plot_bar.png")
plt.close()

print("[Saved] plot_bar.png")
