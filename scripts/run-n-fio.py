import subprocess
import argparse
import sys
import shutil
import re
import json
from datetime import datetime
from pathlib import Path

CONFIGS = [256]

FIO_BASE_CMD = [
    "numactl",
    "--cpunodebind=1",
    "--membind=1",
    "../fio/fio",
    "--filename=/dev/nvme1n1",
    "--direct=1",
    "--bs=4k",
    "--ioengine=io_uring",
    "--time_based=1",
    "--iodepth=256",
    "--output-format=json+",
    "--log_avg_msec=10",
    "--runtime=30s",
    "--ramp_time=15s",
    "--rw=randread"
]

ENERGY_BENCHMARK_CMD = [
    "numactl",
    "--cpunodebind=0",
    "--membind=0",
    "target/release/nvme-energy-bench",
    "--no-progress",
    "bench",
    "--skip-plot"
]

CGROUP_HELPER_CMD = [
    "target/release/cgroup-helper",
    "-c",
    "cgroup.yaml",
    "--name",
    "nvme-energy-bench",
    "--device",
    "/dev/nvme1n1",
    "--copies"
]

def run_config(config_value: int, use_cgroups: bool):
    print(f"=== Running config: {config_value} ===")

    now = datetime.now()
    base_dir = (
        f"results/"
        f"cgroups-{now.year}-"
        f"{now.month:02d}-"
        f"{now.day:02d}-"
        f"{now.hour:02d}-"
        f"{now.minute:02d}-"
        f"{now.second:02d}"
    )
    data_dir = Path(base_dir) / "data" / "read-ps0-i0-0"
    data_dir.mkdir(parents=True, exist_ok=True)

    if use_cgroups:
        cgroup_helper_cmd = CGROUP_HELPER_CMD + [str(config_value)]
        print(f"Running cgroup-helper: {' '.join(cgroup_helper_cmd)}")

        ret = subprocess.run(cgroup_helper_cmd).returncode
        if ret != 0:
            print(f"ERROR: cgroup-helper failed with exit code {ret}. Aborting config.")
            return ret, base_dir

        print("cgroup-helper completed successfully.")

    processes = []
    energy_cmd = ENERGY_BENCHMARK_CMD + [f"--use-dir={base_dir}"]
    print(f"Starting nvme-energy-bench: {' '.join(energy_cmd)}")
    energy_proc = subprocess.Popen(energy_cmd)
    processes.append(("nvme-energy-bench", energy_proc))

    for i in range(config_value):
        fio_cmd = (
            FIO_BASE_CMD
            + [
                f"--output={data_dir}/results-{i}.json",
                f"--write_bw_log={data_dir}/log-{i}",
                f"--write_lat_log={data_dir}/log-{i}",
                "--name=cgroups"
            ]
        )

        if use_cgroups:
            fio_cmd.insert(4, f"--cgroup=nvme-energy-bench-{i}")

        print(f"Starting fio instance {i}: {' '.join(fio_cmd)}")
        p = subprocess.Popen(fio_cmd)
        processes.append((f"fio-{i}", p))

    exit_code = 0
    for name, proc in processes:
        ret = proc.wait()
        print(f"Process {name} exited with code {ret}")
        if ret != 0:
            exit_code = ret

    print(f"=== Finished config: {config_value} (exit_code={exit_code}) ===\n")
    return exit_code, base_dir


def nat_key(s: str):
    return [int(t) if t.isdigit() else t for t in re.split(r'(\d+)', s)]

def find_jobs_key(obj):
    if isinstance(obj, dict):
        if "jobs" in obj and isinstance(obj["jobs"], list):
            return "jobs"
        if "job" in obj and isinstance(obj["job"], list):
            return "job"
    return None

def load_json(p: Path):
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def merge_one_dir(d: Path) -> bool:
    base = d / "results-1.json"
    if not base.exists():
        print(f"[skip] {d}: missing results-1.json")
        return False

    all_parts = sorted((p for p in d.glob("results-*.json") if p.name != "results-1.json"), key=lambda p: nat_key(p.name))
    try:
        base_obj = load_json(base)
    except Exception as e:
        print(f"[warn] {d}: failed to parse {base.name}: {e}")
        return False

    base_key = find_jobs_key(base_obj)
    if not base_key:
        print(f"[warn] {d}: base file does not contain 'jobs' or 'job' array; skipping.")
        return False

    merged = list(base_obj[base_key])

    for part in all_parts:
        try:
            obj = load_json(part)
        except Exception as e:
            print(f"[warn] {d}: cannot parse {part.name}: {e}; skipping this file.")
            continue
        k = find_jobs_key(obj)
        if not k:
            print(f"[warn] {d}: {part.name} has no 'jobs'/'job'; skipping.")
            continue
        if not isinstance(obj[k], list):
            print(f"[warn] {d}: {part.name} {k} is not a list; skipping.")
            continue
        merged.extend(obj[k])

    out = d / "results.json"
    base_obj[base_key] = merged
    with out.open("w", encoding="utf-8") as f:
        json.dump(base_obj, f, indent=2, sort_keys=False)
    print(f"[ok]  {d}: wrote {out.name} ({len(merged)} items in {base_key})")
    return True

def merge_results(all_new_results: list[str]) -> int:
    if len(all_new_results) > len(CONFIGS):
        all_new_results = all_new_results[-len(CONFIGS):]

    if len(all_new_results) == 0:
        print("No results to merge. Exiting.")
        return 0

    iter0_dir = Path(all_new_results[0]).resolve()
    iter0_data = iter0_dir / "data"
    iter0_data.mkdir(parents=True, exist_ok=True)

    print(f"  Base (iteration 0) directory: {iter0_dir}")

    base_sub0 = iter0_data / "read-ps0-i0-0"
    if not base_sub0.is_dir():
        print(f"  NOTE: {base_sub0} not found in base; creating empty directory.")
        base_sub0.mkdir(parents=True, exist_ok=True)

    k = 1
    for src in all_new_results[1:]:
        src_dir = Path(src).resolve()
        src_sub = src_dir / "data" / "read-ps0-i0-0"
        dest_sub = iter0_data / f"read-ps0-i0-{k}"

        if not src_sub.is_dir():
            print(f"  WARNING: {src_sub} does not exist; skipping (k={k}).")
            k += 1
            continue

        if dest_sub.exists():
            print(
                f"  ERROR: destination {dest_sub} already exists; "
                f"refusing to overwrite. Skipping (k={k})."
            )
            k += 1
            continue

        print(f"  Moving: {src_sub} -> {dest_sub}")
        shutil.move(str(src_sub), str(dest_sub))
        k += 1

    print(f"Merge complete. Final contents under: {iter0_data}")
    print("Compacting per-directory results JSON files into a single results.json...")

    targets = sorted((p for p in iter0_data.glob("read-ps0-i0-*") if p.is_dir()), key=lambda p: nat_key(p.name))
    if not targets:
        print(f"[warn] No read-ps0-i0-* directories under {iter0_data}")
        return

    merged_any = False
    for d in targets:
        merged_any |= merge_one_dir(d)
    if not merged_any:
        print("[info] Nothing merged.")
    for d in iter0_data.glob("read-ps0-i0-*"):
        if not d.is_dir():
            continue

        for file in d.glob("log-*_bw.*.log"):
            if not file.is_file():
                continue
            m = re.match(r"log-(\d+)_bw\.(\d+)\.log$", file.name)
            if not m:
                continue
            num1 = m.group(1)
            newname = f"log_bw.{num1}.log"
            newpath = file.with_name(newname)
            print(f"Renaming: {file.name} → {newname}")
            file.rename(newpath)

    print("All done.")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Run energy benchmark with optional cgroups.")
    parser.add_argument(
        "--use-cgroups",
        action="store_true",
        help="Enable cgroup-helper and pass --cgroup=nvme-energy-bench-{i} to fio."
    )
    args = parser.parse_args()
    overall_exit = 0
    all_new_results: list[str] = []

    for cfg in CONFIGS:
        ret, base_dir = run_config(cfg, args.use_cgroups)
        all_new_results.append(base_dir)
        if ret != 0:
            overall_exit = ret

    merge_ret = merge_results(all_new_results)
    if merge_ret != 0:
        overall_exit = merge_ret

    sys.exit(overall_exit)


if __name__ == "__main__":
    main()
