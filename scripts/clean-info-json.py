import argparse
import json
from pathlib import Path
from typing import Any, Dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Remove entries in 'param_map' whose args.request_sizes contains '1m' "
            "from a JSON file and write the result back to disk."
        )
    )
    parser.add_argument(
        "json_path",
        type=Path,
        help="Path to the JSON file to modify",
    )
    return parser.parse_args()


def load_json(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise SystemExit(f"Error: file not found: {path}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"Error: failed to parse JSON in {path}: {e}")


def filter_param_map(data: Dict[str, Any]) -> int:
    param_map = data.get("param_map")
    if not isinstance(param_map, dict):
        raise SystemExit("Error: top-level 'param_map' is missing or not an object")

    keys_to_remove = []

    for key, value in param_map.items():
        args = value.get("args") if isinstance(value, dict) else None
        if not isinstance(args, dict):
            continue

        request_sizes = args.get("request_sizes")
        if isinstance(request_sizes, list) and "1m" in request_sizes:
            keys_to_remove.append(key)

    for key in keys_to_remove:
        del param_map[key]

    return len(keys_to_remove)


def write_json(path: Path, data: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=False)
        f.write("\n")


def main() -> None:
    args = parse_args()
    data = load_json(args.json_path)

    removed = filter_param_map(data)
    write_json(args.json_path, data)

    print(f"Done. Removed {removed} entr{'y' if removed == 1 else 'ies'} with request_sizes containing '1m'.")


if __name__ == "__main__":
    main()
