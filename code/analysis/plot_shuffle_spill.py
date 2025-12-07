import pandas as pd
import matplotlib.pyplot as plt

# ===========================================
# 1. Load CSV
# ===========================================
df = pd.read_csv("shuffle_spill.csv")

# ===========================================
# 2. Fix wrong labeling for sort-skew
# ===========================================
mask_skew = df["size"] == "skew"

# workload is actually sort-skew
df.loc[mask_skew, "workload"] = "sort-skew"

# true size from name (last segment)
df.loc[mask_skew, "size"] = df.loc[mask_skew, "name"].str.split("-").str[-1]

# convert "2m" → 2, etc.
df["size_num"] = df["size"].apply(lambda x: int(x.replace("m", "")))

# ===========================================
# 3. Remove invalid 64m results
# ===========================================
df = df[df["size_num"] != 64]

df = df.sort_values("size_num")

# ===========================================
# 4. Draw combined Spill Memory + Spill Disk figure
# ===========================================
def plot_spill_combined(df, workload, output_file):
    sub = df[df["workload"] == workload]

    if sub.empty:
        print(f"[Skip] No data for workload = {workload}")
        return

    managers = ["hash", "sort"]
    sizes = sorted(sub["size_num"].unique())
    xticks = [f"{s}m" for s in sizes]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # ---------- (A) Spill Memory ----------
    ax = axes[0]
    for mgr in managers:
        mgr_df = sub[sub["manager"] == mgr].sort_values("size_num")
        ax.plot(
            mgr_df["size_num"],
            mgr_df["spill_memory_MB"],
            marker="o",
            label=mgr
        )
    ax.set_title(f"Spill Memory (workload = {workload})")
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Spill Memory (MB)")
    ax.set_xticks(sizes)
    ax.set_xticklabels(xticks)
    ax.grid(True, linestyle="--", alpha=0.5)

    # ---------- (B) Spill Disk ----------
    ax = axes[1]
    for mgr in managers:
        mgr_df = sub[sub["manager"] == mgr].sort_values("size_num")
        ax.plot(
            mgr_df["size_num"],
            mgr_df["spill_disk_MB"],
            marker="o",
            label=mgr
        )
    ax.set_title(f"Spill Disk (workload = {workload})")
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Spill Disk (MB)")
    ax.set_xticks(sizes)
    ax.set_xticklabels(xticks)
    ax.grid(True, linestyle="--", alpha=0.5)

    # Unified legend
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=2)

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    plt.savefig(output_file)
    plt.close()
    print(f"[Saved] {output_file}")


# ===========================================
# 5. Generate result figures
# ===========================================
for w in ["reduce", "sort", "sort-skew"]:
    out = f"spill_{w}.png"
    plot_spill_combined(df, w, out)

print("All spill plots generated.")
