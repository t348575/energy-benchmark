import argparse
import json
import re
from pathlib import Path

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

def main():
    ap = argparse.ArgumentParser(
        description="Combine results-{i}.json into results.json by appending to the base jobs array."
    )
    ap.add_argument(
        "iteration0_dir",
        type=Path,
        help="Path to the base (iteration 0) results folder (the directory under results/* that contains data/)."
    )
    args = ap.parse_args()

    iter0 = args.iteration0_dir
    data_dir = iter0 / "data"
    if not data_dir.is_dir():
        print(f"[error] {data_dir} not found. Point me at the iteration-0 directory that contains 'data/'.")
        raise SystemExit(2)

    targets = sorted((p for p in data_dir.glob("read-ps0-i0-*") if p.is_dir()), key=lambda p: nat_key(p.name))
    if not targets:
        print(f"[warn] No read-ps0-i0-* directories under {data_dir}")
        return

    merged_any = False
    for d in targets:
        merged_any |= merge_one_dir(d)

    if not merged_any:
        print("[info] Nothing merged.")

if __name__ == "__main__":
    main()
