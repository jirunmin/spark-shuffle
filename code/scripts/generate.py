import sys
import random

if len(sys.argv) != 3:
    print("Usage: python3 generate.py <num_records> <output_file>")
    sys.exit(1)

N = int(sys.argv[1])
out_path = sys.argv[2]

random.seed(42)

print(f"Generating {N} lines to {out_path} ...")

with open(out_path, "w") as f:
    for i in range(N):
        id_val = i                    # 第一列递增
        value = random.randint(0, 1000000)  # 第二列均匀随机
        
        r = random.random()

        # 80% → 单热点 skew=1（极端倾斜）
        if r < 0.80:
            skew = 1

        # 15% → 中度热点 skew∈[1..10]
        elif r < 0.95:
            skew = random.randint(1, 10)

        # 5% → 稀疏随机 skew
        else:
            skew = random.randint(1, 1000000)


        f.write(f"{id_val},{value},{skew}\n")

print("Done.")

