import pandas as pd
import matplotlib.pyplot as plt

# 读结果 CSV
df = pd.read_csv("spark_results.csv")

# 确保根据 size 排序
df = df.sort_values(by="size_m")

# 为了更好看，我们把每条曲线拆出来
configs = [
    ("hash", "reduce"),
    ("sort", "reduce"),
    ("hash", "sort"),
    ("sort", "sort"),
]

plt.figure(figsize=(10, 6))

for manager, workload in configs:
    sub = df[(df["manager"] == manager) & (df["workload"] == workload)]
    
    # 画折线
    plt.plot(
        sub["size_m"],
        sub["duration_s"],
        marker="o",
        linewidth=2,
        label=f"{manager}-{workload}"
    )

# 图形格式美化
plt.xlabel("Data Size (million rows)", fontsize=12)
plt.ylabel("Duration (seconds)", fontsize=12)
plt.title("Spark Shuffle Performance (Hash vs Sort, Reduce vs Sort)", fontsize=14)

plt.xticks(df["size_m"])
plt.grid(True, linestyle="--", alpha=0.5)
plt.legend()

# 输出图像
plt.savefig("duration_plot.png", dpi=200)
print("Saved: duration_plot.png")
