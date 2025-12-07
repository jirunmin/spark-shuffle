import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv("shuffle_rest.csv")

def size_to_num(s):
    return int(s.replace("m", ""))
df["size_num"] = df["size"].apply(size_to_num)

def fix_missing_zero(series, sizes):
    values = series.values.astype(float)
    filled = np.zeros(len(values), dtype=bool)

    for i in range(len(values)):
        if values[i] == 0:
            left = right = None
            for j in range(i-1, -1, -1):
                if values[j] > 0:
                    left = j
                    break
            for j in range(i+1, len(values)):
                if values[j] > 0:
                    right = j
                    break
            if left is not None and right is not None:
                x0, y0 = sizes[left], values[left]
                x1, y1 = sizes[right], values[right]
                xi = sizes[i]
                yi = y0 + (y1 - y0) * (xi - x0) / (x1 - x0)
                values[i] = yi
                filled[i] = True
    return values, filled

# -------- 改善可读性的参数 --------
line_styles = ["-", "--", "-.", ":", "-", "--"]
offset_step = 3  # 让每条曲线微微错开

plt.figure(figsize=(12,6))

groups = list(df.groupby(["manager","workload"]))

for idx, ((mgr, w), sub) in enumerate(groups):
    sub = sub.sort_values("size_num")
    x = sub["size_num"].values
    y_fixed, mask = fix_missing_zero(sub["shuffle_write_MB"], x)

    # 添加轻微错位：避免曲线重合
    y_shifted = y_fixed + idx * offset_step

    plt.plot(
        x, y_shifted,
        linestyle=line_styles[idx % len(line_styles)],
        linewidth=2,
        marker="o",
        label=f"{mgr}-{w}"
    )

    # 标记插值点
    plt.scatter(
        x[mask], y_shifted[mask],
        facecolors="none",
        edgecolors="red",
        s=110,
        linewidths=2
    )

plt.xlabel("Dataset Size (million rows)")
plt.ylabel("Shuffle Write (MB)")
plt.title("Shuffle Write Comparison (Improved Readability)")
plt.grid(True, linestyle="--", alpha=0.5)
plt.legend()
plt.tight_layout()
plt.savefig("shuffle_write_improved.png")
plt.close()

print("Saved shuffle_write_improved.png")
