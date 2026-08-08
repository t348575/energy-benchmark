#!/bin/bash
set -euo pipefail

CONFIGS=(1 2 4 8 16 32 64 128 256)

FIO_BIN="../fio/fio"
DEVICE="/dev/nvme1n1"
ENERGY_CMD=(numactl --cpunodebind=0 --membind=0 target/release/energy-benchmark bench --skip-plot)

FIO_FIXED_OPTS=(
  --filename="$DEVICE"
  --direct=1
  --bs=4k
  --ioengine=io_uring
  --time_based=1
  --iodepth=1
  --output-format=json+
  --log_avg_msec=10
  --runtime=30s
  --ramp_time=15s
  --rw=randread
  --numa_cpu_nodes=1
  --numa_mem_policy=local
  --cgroup=energy-benchmark
  --name=read
)

sleep_until() {
  local target="$1"
  local now remaining
  now="$(date +%s.%N)"
  remaining="$(awk -v t="$target" -v n="$now" 'BEGIN{d=t-n; if (d<0) d=0; print d}')"
  sleep "$remaining"
}

RUN_MARKER="$(mktemp -p . .run_marker.XXXXXX)"
touch "$RUN_MARKER"

ITER=0
for N in "${CONFIGS[@]}"; do
  ITER=$((ITER + 1))
  echo "=== Iteration $ITER (N=$N) ==="

  START_AT="$(awk -v now="$(date +%s)" 'BEGIN{print now+5}')"
  ENERGY_START="$(awk -v t="$START_AT" 'BEGIN{print t-1}')"
  echo "  Scheduling energy-bench at $ENERGY_START, fio at $START_AT"

  MARKER="$(mktemp -p . .energy_start_marker.XXXXXX)"
  touch -d "@$ENERGY_START" "$MARKER"

  bash -c '
    set -euo pipefail
    sleep_until() {
      local target="$1"
      local now remaining
      now="$(date +%s.%N)"
      remaining="$(awk -v t="$target" -v n="$now" "BEGIN{d=t-n; if (d<0) d=0; print d}")"
      sleep "$remaining"
    }
    sleep_until '"$ENERGY_START"';
    "$@" &
    wait $!
  ' _ "${ENERGY_CMD[@]}" &
  energy_pid=$!

  echo "  Waiting for energy-bench to create a new results directory..."
  NEW_DIR=""
  for _ in {1..60}; do
    sleep 0.5
    NEW_DIR="$(find results -mindepth 1 -maxdepth 1 -type d -newer "$MARKER" -printf '%T@ %p\n' 2>/dev/null | sort -nr | awk 'NR==1{print $2}')"
    [[ -n "${NEW_DIR:-}" ]] && break
  done
  rm -f "$MARKER"

  if [[ -z "${NEW_DIR:-}" ]]; then
    echo "  ERROR: No new results directory detected after ENERGY_START." >&2
    wait "$energy_pid" || true
    exit 1
  fi

  RESULTS_ROOT="${NEW_DIR%/}/data/read-ps0-i0-0"
  mkdir -p "$RESULTS_ROOT"
  echo "  Using RESULTS_ROOT = $RESULTS_ROOT"

  pids=()
  for i in $(seq 1 "$N"); do
    OUT_JSON="$RESULTS_ROOT/results-$i.json"
    LOG_PREFIX="$RESULTS_ROOT/log-$i"

    CMD=(sudo "$FIO_BIN" "${FIO_FIXED_OPTS[@]}"
        --output="$OUT_JSON"
        --write_bw_log="$LOG_PREFIX")

    bash -c '
      set -euo pipefail
      sleep_until() {
        local target="$1"
        local now remaining
        now="$(date +%s.%N)"
        remaining="$(awk -v t="$target" -v n="$now" "BEGIN{d=t-n; if (d<0) d=0; print d}")"
        sleep "$remaining"
      }
      sleep_until "$1"
      shift
      exec "$@"
    ' _ "$START_AT" "${CMD[@]}" &
    pids+=("$!")
  done

  fail=0
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      echo "  WARNING: a fio job exited non-zero."
      fail=1
    fi
  done

  echo "  Waiting for energy-bench (PID $energy_pid) to finish..."
  wait "$energy_pid" || true

  echo "=== Iteration $ITER complete (N=$N) ==="
  [[ $fail -eq 0 ]] || echo "  (Some fio jobs failed)"
done

echo "All configurations complete."

echo "Merging the last ${#CONFIGS[@]} results folders created during this run..."

mapfile -t ALL_NEW_RESULTS < <(
  find results -mindepth 1 -maxdepth 1 -type d -newer "$RUN_MARKER" -printf '%T@ %p\n' \
    | sort -n \
    | awk '{print $2}'
)

rm -f "$RUN_MARKER"

if ((${#ALL_NEW_RESULTS[@]} > ${#CONFIGS[@]})); then
  ALL_NEW_RESULTS=("${ALL_NEW_RESULTS[@]: -${#CONFIGS[@]}}")
fi

if ((${#ALL_NEW_RESULTS[@]} == 0)); then
  echo "No results to merge. Exiting."
  exit 0
fi

ITER0_DIR="${ALL_NEW_RESULTS[0]}"
ITER0_DATA="${ITER0_DIR%/}/data"
mkdir -p "$ITER0_DATA"

echo "  Base (iteration 0) directory: $ITER0_DIR"

if [[ ! -d "$ITER0_DATA/read-ps0-i0-0" ]]; then
  echo "  NOTE: $ITER0_DATA/read-ps0-i0-0 not found in base; creating empty directory."
  mkdir -p "$ITER0_DATA/read-ps0-i0-0"
fi

k=1
for src in "${ALL_NEW_RESULTS[@]:1}"; do
  src_sub="${src%/}/data/read-ps0-i0-0"
  dest_sub="$ITER0_DATA/read-ps0-i0-$k"

  if [[ ! -d "$src_sub" ]]; then
    echo "  WARNING: $src_sub does not exist; skipping (k=$k)."
    ((k++))
    continue
  fi

  if [[ -e "$dest_sub" ]]; then
    echo "  ERROR: destination $dest_sub already exists; refusing to overwrite. Skipping (k=$k)."
    ((k++))
    continue
  fi

  echo "  Moving: $src_sub -> $dest_sub"
  mv "$src_sub" "$dest_sub"

  ((k++))
done

echo "Merge complete. Final contents under: $ITER0_DATA"

echo "Compacting per-directory results JSON files into a single results.json..."

# Need jq
if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: 'jq' is required for JSON merging. Please install jq and re-run the compact step." >&2
  exit 1
fi

python compact_fio_json.py $ITER0_DATA

for d in "$ITER0_DATA"/read-ps0-i0-*; do
  pushd $d
  for file in log-*_bw.*.log; do
    [[ -e "$file" ]] || continue

    prefix="log"
    num1=$(echo "$file" | sed -E 's/log-([0-9]+)_bw\.[0-9]+\.log/\1/')
    num2=$(echo "$file" | sed -E 's/log-[0-9]+_bw\.([0-9]+)\.log/\1/')

    newname="${prefix}_bw.${num1}.log"

    echo "Renaming: $file → $newname"
    mv "$file" "$newname"
  done
  popd
done

echo "All done."