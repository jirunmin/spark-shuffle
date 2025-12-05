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
        f.write(f"{i},{random.randint(0, 1000000)},{random.randint(0, 1000000)}\n")

print("Done.")
