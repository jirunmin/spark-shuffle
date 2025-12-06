import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("spark_metrics.csv")
df = df.sort_values("size")

sizes = [2,4,8,16,32,64]
workloads = ["hash-reduce", "hash-sort", "sort-reduce", "sort-sort"]

# 把数据整理成 workload 形式
df["w"] = df["mode1"] + "-" + df["mode2"]
df["size_num"] = df["size"].str.replace("m","").astype(int)

def plot_metric(metric, title, ylabel):
    plt.figure(figsize=(10,6))
    for w in workloads:
        sub = df[df["w"] == w]
        plt.plot(sub["size_num"], sub[metric], marker="o", label=w)
    plt.xlabel("Data Size (million rows)")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend()
    plt.savefig(f"{metric}.png", dpi=160)
    plt.close()

plot_metric("duration_s", "Execution Time", "Seconds")
plot_metric("shuffle_write", "Shuffle Write Volume", "Bytes")
plot_metric("shuffle_read", "Shuffle Read Volume", "Bytes")
plot_metric("spill_disk", "Disk Spill Volume", "Bytes")
plot_metric("fetch_wait", "Task Fetch Wait Time", "Milliseconds")
plot_metric("gc_time", "GC Time", "Milliseconds")

print("=== Charts generated ===")
