#!/bin/bash
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <dirs-file>"
    exit 1
fi

mapfile -t dirs < <(grep -v '^[[:space:]]*$' "$1")

get_nproc() { command -v nproc >/dev/null && nproc || sysctl -n hw.ncpu; }
PARALLELISM="4"

printf '%s\0' "${dirs[@]}" \
| xargs -0 -I{} -n1 -P "$PARALLELISM" \
  env ONLY_PROCESS=1 RUST_LOG=debug target/release/nvme-energy-bench -l common plot -f "{}"
