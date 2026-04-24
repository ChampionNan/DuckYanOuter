import csv
import random
import os
import argparse
from typing import Optional

# ==========================================
# CONFIGURATION
# ==========================================
TOTAL_IDS = 1_000_000


def generate_csvs(k: int, total_ids: int = TOTAL_IDS, output_dir: Optional[str] = None, seed: Optional[int] = None):
    if k < 1:
        raise ValueError("k must be >= 1")

    if seed is not None:
        random.seed(seed)

    # Default output dir name: R1R{k}_data
    if output_dir is None:
        output_dir = f"R1R{k}_data"

    # 1. Setup Output Directory
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory: {output_dir}")

    # 2. File Handles for R1..Rk
    tables = [f"R{i}" for i in range(1, k + 1)]
    files: dict[str, object] = {}
    writers: dict[str, csv.writer] = {}

    def chain_keys(i: int):
        """
        Create a deterministic chain of keys for R1..Rk.
        For a "perfect chain":
          Rj: (src=keys[j-1], dst=keys[j]) for j=1..k
        """
        base = i * 10
        keys = [base + j * 100 for j in range(k + 1)]  # length k+1
        return keys

    try:
        for t in tables:
            f = open(f"{output_dir}/{t}.csv", "w", newline="")
            files[t] = f
            w = csv.writer(f)
            writers[t] = w
            # w.writerow(["src", "dst"])

        print(f"Generating {total_ids} IDs for tables: {', '.join(tables)}")
        print("Patterns: broken chains, partial chains, complete chains, duplicate keys")

        for i in range(1, total_ids + 1):
            # Force first 30 IDs to cover all edge cases deterministically
            if i <= 30:
                signature = i % 16
            else:
                signature = random.randint(0, 15)

            keys = chain_keys(i)

            # Strategy 1: BROKEN CHAINS (30%)
            if i % 10 < 3:
                # Create edges, but break links by pointing to non-existent "next src"
                for j in range(1, k + 1):
                    if signature & (1 << ((j - 1) % 4)):  # repeat bitmask over k
                        src = keys[j - 1]
                        dst = keys[j] + 1_000_000_000 + j * 17  # unlikely to match any src
                        writers[f"R{j}"].writerow([src, dst])

                # Also add some isolated/orphan rows
                if signature & 8:
                    j = random.randint(1, k)
                    writers[f"R{j}"].writerow([keys[0] + 4_000_000_000, keys[0] + 5_000_000_000])

            # Strategy 2: PARTIAL CHAINS (40%)
            elif i % 10 < 7:
                # Create a chain but skip a random table/link to force NULL padding in outer joins
                break_at = random.randint(1, k)  # stop generating after this table
                for j in range(1, k + 1):
                    if j > break_at:
                        continue
                    # Randomly skip an intermediate link sometimes
                    if 1 < j < k and random.random() < 0.25:
                        continue
                    writers[f"R{j}"].writerow([keys[j - 1], keys[j]])

            # Strategy 3: COMPLETE CHAINS (20%)
            elif i % 10 < 9:
                for j in range(1, k + 1):
                    writers[f"R{j}"].writerow([keys[j - 1], keys[j]])

            # Strategy 4: DUPLICATE KEYS (10%)
            else:
                bucket = (i // 100) * 100
                keys2 = chain_keys(bucket)

                # Pick a couple tables to duplicate rows in
                dup_tables = {random.randint(1, k)}
                if k >= 2 and random.random() < 0.5:
                    dup_tables.add(random.randint(1, k))

                for j in range(1, k + 1):
                    row = [keys2[j - 1], keys2[j]]
                    writers[f"R{j}"].writerow(row)
                    if j in dup_tables:
                        writers[f"R{j}"].writerow(row)  # duplicate

    finally:
        for f in files.values():
            f.close()

    print("\nData Generation Complete!")
    print(f"Files saved to '{output_dir}/' (tables R1..R{k})")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate join-stress CSVs for R1..Rk.")
    parser.add_argument(
        "--k",
        type=int,
        default=7,  # default number of tables
        help="Number of tables to generate (R1..Rk). Default: 4.",
    )
    parser.add_argument(
        "--total-ids",
        type=int,
        default=TOTAL_IDS,
        help=f"Number of IDs to generate. Default: {TOTAL_IDS}.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory. Default: R1R{k}_data (derived from --k).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility. Default: None (no fixed seed).",
    )
    args = parser.parse_args()

    generate_csvs(k=args.k, total_ids=args.total_ids, output_dir=args.output_dir, seed=args.seed)
