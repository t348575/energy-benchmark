#!/usr/bin/env bash
set -euo pipefail

artifact_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repository_root="$(cd "${artifact_root}/.." && pwd)"
image="${AE_IMAGE:-nvme-energy-bench-ae}"
download_dir=""
extract_dir=""
zenodo_record="${ZENODO_RECORD:-}"
zenodo_downloader_image="python:3.12.11-slim-bookworm@sha256:519591d6871b7bc437060736b9f7456b8731f1499a57e22e6c285135ae657bf7"
skip_download=0

usage() {
    cat <<EOF
Usage: $0 [--image NAME]
          [--download-dataset BAG_DIRECTORY [--zenodo-record RECORD]
           [--extract-dataset DATASET_DIRECTORY] [--skip-download]]

Build the Linux/x86-64 IISWC 2026 analysis container.
It does not download the research dataset unless --download-dataset is specified.

RECORD may be a Zenodo record ID or DOI. By default, setup downloads the
published BagIt archive into BAG_DIRECTORY.
--skip-download instead reuses an existing BAG_DIRECTORY.
Extraction occurs only when --extract-dataset is given
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --image)
            image="${2:?missing value for --image}"
            shift 2
            ;;
        --download-dataset)
            download_dir="${2:?missing destination directory}"
            shift 2
            ;;
        --extract-dataset)
            extract_dir="${2:?missing extracted dataset directory}"
            shift 2
            ;;
        --skip-download)
            skip_download=1
            shift
            ;;
        --zenodo-record)
            zenodo_record="${2:?missing Zenodo record ID, DOI, or URL}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "${download_dir}" && ( -n "${zenodo_record}" || -n "${extract_dir}" || ${skip_download} -eq 1 ) ]]; then
    echo "ERROR: --zenodo-record, --extract-dataset, and --skip-download require --download-dataset." >&2
    exit 2
fi
if [[ -n "${download_dir}" && ${skip_download} -eq 0 && -z "${zenodo_record}" ]]; then
    echo "ERROR: --download-dataset requires --zenodo-record unless --skip-download is used." >&2
    exit 2
fi
if [[ ${skip_download} -eq 1 && -n "${zenodo_record}" ]]; then
    echo "ERROR: --skip-download cannot be combined with --zenodo-record." >&2
    exit 2
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: Docker is required but was not found in PATH." >&2
    exit 2
fi
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is installed, but its daemon is not reachable." >&2
    exit 2
fi

docker build \
    --platform linux/amd64 \
    --tag "${image}" \
    --file "${artifact_root}/container/Dockerfile" \
    "${repository_root}"
docker image inspect "${image}" >/dev/null

echo "Built ${image}"
if [[ -n "${download_dir}" ]]; then
    download_dir="$(realpath -m -- "${download_dir}")"
    if [[ "${download_dir}" == "/" ]]; then
        echo "ERROR: refusing to use the filesystem root for the dataset BagIt directory." >&2
        exit 2
    fi
    if [[ ${skip_download} -eq 1 && ! -d "${download_dir}" ]]; then
        echo "ERROR: existing dataset BagIt directory not found: ${download_dir}" >&2
        exit 2
    fi
    if [[ -n "${extract_dir}" ]]; then
        extract_dir="$(realpath -m -- "${extract_dir}")"
        if [[ "${extract_dir}" == "/" ]]; then
            echo "ERROR: refusing to extract the dataset into the filesystem root." >&2
            exit 2
        fi
        case "${extract_dir}/" in
            "${download_dir}/"*|"${download_dir}")
                echo "ERROR: --extract-dataset must be outside --download-dataset." >&2
                exit 2
                ;;
        esac
        if [[ -e "${extract_dir}" || -L "${extract_dir}" ]]; then
            echo "ERROR: extracted dataset output already exists: ${extract_dir}" >&2
            exit 2
        fi
        mkdir -p -- "$(dirname -- "${extract_dir}")"
    fi

    if [[ ${skip_download} -eq 0 ]]; then
        mkdir -p -- "${download_dir}"
    fi

    if [[ ${skip_download} -eq 0 || -n "${extract_dir}" ]]; then
        extract_name=""
        if [[ -n "${extract_dir}" ]]; then
            extract_name="$(basename -- "${extract_dir}")"
            extraction_jobs="${EXTRACTION_JOBS:-$(nproc)}"
        fi
        docker_args=(
            run --rm
            --env HOME=/tmp
            --env HOST_UID="$(id -u)"
            --env HOST_GID="$(id -g)"
            --env DO_DOWNLOAD="$((1 - skip_download))"
            --env ZENODO_RECORD="${zenodo_record}"
            --env ZENODO_RECORD_IS_DOI="$([[ "${zenodo_record}" == 10.* ]] && printf 1 || printf 0)"
            --env EXTRACT_DIRECTORY="${extract_name}"
            --mount "type=bind,source=${download_dir},target=/bag"
        )
        if [[ -n "${extract_dir}" ]]; then
            docker_args+=(
                --env EXTRACTION_JOBS="${extraction_jobs}"
                --mount "type=bind,source=$(dirname -- "${extract_dir}"),target=/extracted-parent"
                --mount "type=bind,source=${artifact_root}/scripts/extract-dataset.sh,target=/opt/ae/extract-dataset.sh,readonly"
            )
        fi
        docker_args+=(
            "${zenodo_downloader_image}"
            sh -ec '
                trap "chown -R \"$HOST_UID:$HOST_GID\" /bag 2>/dev/null || true; [ -z \"$EXTRACT_DIRECTORY\" ] || chown -R \"$HOST_UID:$HOST_GID\" \"/extracted-parent/$EXTRACT_DIRECTORY\" 2>/dev/null || true" 0
                apt-get update
                apt-get install --yes --no-install-recommends \
                    bash coreutils findutils gawk tar zstd
                rm -rf /var/lib/apt/lists/*
                if [ "$DO_DOWNLOAD" = 1 ]; then
                    pip install --no-cache-dir --user zenodo-get
                    if [ "$ZENODO_RECORD_IS_DOI" = 1 ]; then
                        "$HOME/.local/bin/zenodo_get" -d "$ZENODO_RECORD" -o /bag
                    else
                        "$HOME/.local/bin/zenodo_get" "$ZENODO_RECORD" -o /bag
                    fi
                fi
                if [ -n "$EXTRACT_DIRECTORY" ]; then
                    bash /opt/ae/extract-dataset.sh /bag "/extracted-parent/$EXTRACT_DIRECTORY"
                fi
            '
        )
        docker "${docker_args[@]}"
    fi

    if [[ ${skip_download} -eq 1 ]]; then
        echo "Dataset downloaded: reused ${download_dir}"
    else
        echo "Dataset downloaded: ${download_dir}"
    fi
    if [[ -n "${extract_dir}" ]]; then
        echo "Dataset extracted: ${extract_dir}"
    else
        echo "Dataset extracted: no"
    fi
else
    echo "Dataset downloaded: no"
    echo "Dataset extracted: no"
fi
echo "AE setup: PASS"
