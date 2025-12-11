import argparse
from math import nan
import plots.common as common
import pandas as pd

def analyze(csv_path: str) -> tuple[float, float]:
    df = pd.read_csv(csv_path, dtype="float32")
    df.dropna(inplace=True)
    # df = common.fill_clean(df, trim=len(df))
    df = df[(df["Total"] < 25) & (df["Total"] >= 0)].copy()
    # df = df[(df["Total"] < 500) & (df["Total"] >= 5)].copy()
    # df = df[(df["Total"] < 500) & (df["Total"] >= 5)].copy()
    df['time'] = pd.to_datetime(df['time'])
    df = df.set_index('time')
    # if df.shape[1] == 3:
    #     return nan, nan
    # else:
        # df = df[(df["load-left-Node3L"] < 500) & (df["load-left-Node3L"] >= 5)].copy()
        # df = df[(df["load-right-Node3R"] < 500) & (df["load-right-Node3R"] >= 5)].copy()
    rolling_avg = df['Total'].rolling('100ms').mean()
    rolling_avg = rolling_avg.iloc[100:].dropna()
    # df["total_smoothed"] = savgol_filter(df["Total"], window_length=101, polyorder=3)
    # df["load-right-Node3R"] = df["load-right-Node3R"].astype(float)
    return rolling_avg.min(), rolling_avg.max()

import argparse
import os
from pathlib import Path
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--root-dir", default="results")
    p.add_argument("--jobs", type=int, default=os.cpu_count() or 1)
    return p.parse_args()


def find_csv_files(root: Path):
    return list(root.rglob("powersensor3.csv"))


def analyze_file(fpath: Path):
    """Wrapper so ProcessPoolExecutor can call it."""
    try:
        minv, maxv = analyze(str(fpath))
        if minv is nan or maxv is nan:
            return (str(fpath), None, None)
        return (str(fpath), float(minv), float(maxv))
    except Exception as e:
        return (str(fpath), None, None, str(e))


def main():
    args = parse_args()
    root = Path(args.root_dir)

    csv_files = find_csv_files(root)
    if not csv_files:
        print(f"No rapl.csv found under {root}")
        return

    tmpdir = Path(tempfile.mkdtemp(prefix="min_max_"))
    results_file = tmpdir / "results.csv"
    fail_file = tmpdir / "failed.txt"

    results = []
    failures = []

    with ProcessPoolExecutor(max_workers=args.jobs) as pool:
        futures = {pool.submit(analyze_file, f): f for f in csv_files}

        for fut in as_completed(futures):
            out = fut.result()

            if len(out) == 3:
                fpath, minv, maxv = out
                if minv is not None or maxv is not None:
                    results.append((fpath, minv, maxv))
            else:
                fpath, _, _, err = out
                failures.append((fpath, err))

    with results_file.open("w") as f:
        for path, mn, mx in results:
            f.write(f"{path},{mn},{mx}\n")

    with fail_file.open("w") as f:
        for path, err in failures:
            f.write(f"{path}: {err}\n")

    global_min = min(mn for _, mn, _ in results)
    global_max = max(mx for _, _, mx in results)

    print(f"Global smallest value: {global_min}")
    print(f"Global largest value:  {global_max}")

    print(f"Files processed successfully: {len(results)}")
    print(f"Files failed: {len(failures)} (see {fail_file})")
    print(f"Per-file results saved to: {results_file}")


if __name__ == "__main__":
    main()