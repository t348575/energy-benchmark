#!/usr/bin/env bash
set -euo pipefail

artifact_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dataset="${AE_DATASET_DIR:-}"
output="${AE_OUTPUT_DIR:-${artifact_root}/output}"
image="${AE_IMAGE:-nvme-energy-bench-ae}"
results_subdir=""
skip_nvme_plots=0
skip_nvme_energy_bench=0
output_marker=".nvme-energy-bench-ae-output"

prepare_output_dir() {
    local candidate="$1"
    local first_entry=""

    if [[ "${candidate}" == "/" ]]; then
        echo "ERROR: refusing to use the filesystem root as output." >&2
        return 2
    fi
    if [[ -e "${candidate}" && ! -d "${candidate}" ]]; then
        echo "ERROR: output exists but is not a directory: ${candidate}" >&2
        return 2
    fi
    if [[ -d "${candidate}" && ! -f "${candidate}/${output_marker}" ]]; then
        first_entry="$(find "${candidate}" -mindepth 1 -maxdepth 1 -print -quit)"
        if [[ -n "${first_entry}" ]]; then
            echo "ERROR: output is nonempty and is not marked as an AE output directory: ${candidate}" >&2
            return 2
        fi
    fi

    if ! mkdir -p "${candidate}"; then
        echo "ERROR: could not create output directory: ${candidate}" >&2
        return 2
    fi
    if ! : > "${candidate}/${output_marker}"; then
        echo "ERROR: could not write the output-directory marker: ${candidate}" >&2
        return 2
    fi
    if ! realpath -e -- "${candidate}"; then
        echo "ERROR: could not resolve output directory: ${candidate}" >&2
        return 2
    fi
}

usage() {
    cat <<EOF
Usage: $0 --dataset DIR [--output DIR] [options]

Options:
  --dataset DIR          Extracted dataset root (or set AE_DATASET_DIR)
  --output DIR           Output directory (default: artifact/output)
  --results-subdir DIR   Results path relative to the dataset root
  --image NAME           Docker image built by setup-replay.sh
  --skip-nvme-plots      Write nvme-energy-bench plot-data JSON/spec files, but
                         skip its bundled Python plot rendering
  --skip-nvme-energy-bench
                         Reuse each run's existing plots/ directory from the
                         dataset instead of running nvme-energy-bench

The full replay runs nvme-energy-bench plot for every experiment directory
referenced by fig.py, then runs fig.py to generate paper figures.
The dataset is mounted read-only. Generated files are written only to output.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset) dataset="${2:?missing value for --dataset}"; shift 2 ;;
        --output) output="${2:?missing value for --output}"; shift 2 ;;
        --results-subdir) results_subdir="${2:?missing value for --results-subdir}"; shift 2 ;;
        --image) image="${2:?missing value for --image}"; shift 2 ;;
        --skip-nvme-plots) skip_nvme_plots=1; shift ;;
        --skip-nvme-energy-bench) skip_nvme_energy_bench=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: Docker is required but was not found in PATH." >&2
    exit 2
fi
if ! docker image inspect "${image}" >/dev/null 2>&1; then
    echo "ERROR: Docker image ${image} is missing; run artifact/scripts/setup-replay.sh first." >&2
    exit 2
fi
if ! command -v realpath >/dev/null 2>&1; then
    echo "ERROR: realpath is required on the Linux host." >&2
    exit 2
fi

output="$(realpath -m -- "${output}")"

if [[ -z "${dataset}" ]]; then
    echo "ERROR: provide --dataset DIR or set AE_DATASET_DIR." >&2
    exit 2
fi
if [[ ! -d "${dataset}" ]]; then
    echo "ERROR: dataset directory does not exist: ${dataset}" >&2
    exit 2
fi

dataset="$(realpath -e -- "${dataset}")"
case "${output}/" in
    "${dataset}/"*)
        echo "ERROR: output must not equal or be inside the dataset directory." >&2
        exit 2
        ;;
esac
case "${dataset}/" in
    "${output}/"*)
        echo "ERROR: output must not contain the dataset directory." >&2
        exit 2
        ;;
esac

output="$(prepare_output_dir "${output}")"

docker_args=(
    run --rm
    --network none
    --read-only
    --tmpfs /tmp:rw,nosuid,nodev,size=2g
    --env MPLCONFIGDIR=/tmp/matplotlib
    --volume "${dataset}:/dataset:ro"
    --volume "${output}:/output:rw"
)
if command -v id >/dev/null 2>&1 && [[ "$(uname -s)" == "Linux" ]]; then
    docker_args+=(--user "$(id -u):$(id -g)")
fi
docker_args+=(
    "${image}"
    --dataset /dataset
    --output /output
)
if [[ -n "${results_subdir}" ]]; then
    docker_args+=(--results-subdir "${results_subdir}")
fi
if [[ ${skip_nvme_plots} -eq 1 ]]; then
    docker_args+=(--skip-nvme-plots)
fi
if [[ ${skip_nvme_energy_bench} -eq 1 ]]; then
    docker_args+=(--skip-nvme-energy-bench)
fi

docker "${docker_args[@]}"
