#!/usr/bin/env bash
set -euo pipefail

ZENODO_API_BASE="https://zenodo.org/api"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=yoda_dataset_common.sh
source "$SCRIPT_DIR/yoda_dataset_common.sh"

usage() {
    cat <<'EOF'
Usage:
  upload_zenodo_dataset.sh \
    --dataset /path/to/dataset-bag \
    --token-file /path/to/zenodo-token.txt \
    --deposition-id DRAFT_ID

Uploads a BagIt dataset to an existing Zenodo draft. The root README.md is
deliberately skipped because it must be uploaded manually through the Zenodo UI.
Because Zenodo filenames cannot contain directory separators, nested BagIt paths
are uploaded using their basenames. The script rejects basename collisions.
Files already listed in the Zenodo draft are skipped on reruns.
The script does not publish the draft.
EOF
}

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

DATASET_DIRECTORY=""
TOKEN_FILE=""
DEPOSITION_ID=""

while (($# > 0)); do
    case "$1" in
        --dataset)
            (($# >= 2)) || die "--dataset requires a value"
            DATASET_DIRECTORY="$2"
            shift 2
            ;;
        --token-file)
            (($# >= 2)) || die "--token-file requires a value"
            TOKEN_FILE="$2"
            shift 2
            ;;
        --deposition-id)
            (($# >= 2)) || die "--deposition-id requires a value"
            DEPOSITION_ID="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "Unknown argument: $1"
            ;;
    esac
done

[[ -n "$DATASET_DIRECTORY" ]] || die "Missing --dataset"
[[ -n "$TOKEN_FILE" ]] || die "Missing --token-file"
[[ "$DEPOSITION_ID" =~ ^[0-9]+$ ]] || die "--deposition-id must be the numeric Zenodo draft ID"

yd_require_commands curl find jq realpath sha256sum sort comm mktemp sed uniq \
    awk grep cmp wc tr || exit

[[ -d "$DATASET_DIRECTORY" ]] || die "Dataset directory does not exist: $DATASET_DIRECTORY"
[[ -f "$TOKEN_FILE" ]] || die "Token file does not exist: $TOKEN_FILE"

DATASET_DIRECTORY="$(realpath "$DATASET_DIRECTORY")"
TOKEN_FILE="$(realpath "$TOKEN_FILE")"

# yd_validate_ready_bag covers READY, INCOMPLETE, both manifests, the package
# membership file, and it verifies the tag manifest. README.md is required by
# this script alone: make_yoda_dataset.sh does not create one.
yd_validate_ready_bag "$DATASET_DIRECTORY" || exit
[[ -f "$DATASET_DIRECTORY/README.md" ]] || die "Missing root README.md"

EMPTY_FILES="$(find "$DATASET_DIRECTORY" -type f -empty -printf '%P\n')"
if [[ -n "$EMPTY_FILES" ]]; then
    printf 'Zenodo does not accept these empty files:\n%s\n' "$EMPTY_FILES" >&2
    die "Replace or remove empty files and regenerate tagmanifest-sha256.txt before uploading"
fi

TOKEN="$(tr -d '\r\n' < "$TOKEN_FILE")"
[[ -n "$TOKEN" ]] || die "Token file is empty"

UPLOAD_WORK_DIRECTORY="$(mktemp -d)"
cleanup() {
    rm -rf -- "$UPLOAD_WORK_DIRECTORY"
}
trap cleanup EXIT

CURL_CONFIG="$UPLOAD_WORK_DIRECTORY/curl.conf"
umask 077
printf 'header = "Authorization: Bearer %s"\n' "$TOKEN" > "$CURL_CONFIG"
unset TOKEN

printf 'Verifying BagIt payload manifest...\n'
yd_verify_payload_manifest "$DATASET_DIRECTORY" || exit

LOCAL_FILE_LIST="$UPLOAD_WORK_DIRECTORY/local-files.txt"
EXPECTED_REMOTE_FILE_LIST="$UPLOAD_WORK_DIRECTORY/expected-remote-files.txt"
REMOTE_FILE_LIST="$UPLOAD_WORK_DIRECTORY/remote-files.txt"
REMOTE_FILES_JSON="$UPLOAD_WORK_DIRECTORY/remote-files.json"
find "$DATASET_DIRECTORY" -type f -printf '%P\n' | LC_ALL=C sort > "$LOCAL_FILE_LIST"

# Zenodo exposes a flat file namespace. All basenames in this Bag must therefore
# be unique so that the original layout can be reconstructed after download.
sed 's|.*/||' "$LOCAL_FILE_LIST" | LC_ALL=C sort > "$EXPECTED_REMOTE_FILE_LIST"
DUPLICATE_REMOTE_NAMES="$(uniq -d "$EXPECTED_REMOTE_FILE_LIST")"
if [[ -n "$DUPLICATE_REMOTE_NAMES" ]]; then
    printf 'These BagIt paths collide after flattening for Zenodo:\n%s\n' \
        "$DUPLICATE_REMOTE_NAMES" >&2
    die "Dataset filenames are not unique after removing directory paths"
fi

FILE_COUNT="$(wc -l < "$LOCAL_FILE_LIST")"
((FILE_COUNT <= 100)) || die "Dataset has $FILE_COUNT files; Zenodo permits at most 100"

printf 'Dataset contains %s files: 1 manual README and %s automated uploads.\n' \
    "$FILE_COUNT" "$((FILE_COUNT - 1))"

DEPOSITION_JSON="$UPLOAD_WORK_DIRECTORY/deposition.json"
curl --fail-with-body --show-error --silent \
    --config "$CURL_CONFIG" \
    "$ZENODO_API_BASE/deposit/depositions/$DEPOSITION_ID" \
    --output "$DEPOSITION_JSON"

BUCKET_URL="$(jq -er '.links.bucket' "$DEPOSITION_JSON")" \
    || die "Could not obtain the upload bucket URL for draft $DEPOSITION_ID"
BUCKET_URL="${BUCKET_URL%/}"

curl --fail-with-body --show-error --silent \
    --config "$CURL_CONFIG" \
    "$ZENODO_API_BASE/deposit/depositions/$DEPOSITION_ID/files" \
    --output "$REMOTE_FILES_JSON"

declare -A REMOTE_FILES=()
while IFS= read -r REMOTE_FILENAME; do
    [[ -n "$REMOTE_FILENAME" ]] || continue
    REMOTE_FILES["$REMOTE_FILENAME"]=1
done < <(jq -r '.[].filename' "$REMOTE_FILES_JSON")

printf 'Uploading files to Zenodo draft %s...\n' "$DEPOSITION_ID"
UPLOAD_RESPONSE="$UPLOAD_WORK_DIRECTORY/upload-response.json"
while IFS= read -r RELATIVE_PATH; do
    [[ -n "$RELATIVE_PATH" ]] || continue

    if [[ "$RELATIVE_PATH" == "README.md" ]]; then
        printf 'Skipping README.md; upload it manually through the Zenodo UI.\n'
        continue
    fi

    REMOTE_FILENAME="${RELATIVE_PATH##*/}"
    case "$REMOTE_FILENAME" in
        *[!A-Za-z0-9._-]*)
            die "Unsupported character in dataset path: $RELATIVE_PATH"
            ;;
    esac

    if [[ -n "${REMOTE_FILES[$REMOTE_FILENAME]+present}" ]]; then
        printf 'Already uploaded: %s\n' "$REMOTE_FILENAME"
        continue
    fi

    if [[ "$RELATIVE_PATH" == "$REMOTE_FILENAME" ]]; then
        printf 'Uploading: %s\n' "$REMOTE_FILENAME"
    else
        printf 'Uploading: %s (from %s)\n' "$REMOTE_FILENAME" "$RELATIVE_PATH"
    fi
    if ! curl --fail-with-body --show-error --progress-bar \
        --retry 5 --retry-connrefused \
        --config "$CURL_CONFIG" \
        --upload-file "$DATASET_DIRECTORY/$RELATIVE_PATH" \
        "$BUCKET_URL/$REMOTE_FILENAME" \
        --output "$UPLOAD_RESPONSE"; then
        if [[ -s "$UPLOAD_RESPONSE" ]]; then
            printf 'Zenodo response:\n' >&2
            jq . "$UPLOAD_RESPONSE" >&2 2>/dev/null \
                || sed -n '1,80p' "$UPLOAD_RESPONSE" >&2
        fi
        die "Upload failed: $RELATIVE_PATH"
    fi
done < "$LOCAL_FILE_LIST"

printf 'Checking the files currently present in the draft...\n'
curl --fail-with-body --show-error --silent \
    --config "$CURL_CONFIG" \
    "$ZENODO_API_BASE/deposit/depositions/$DEPOSITION_ID/files" \
    | jq -r '.[].filename' \
    | LC_ALL=C sort \
    > "$REMOTE_FILE_LIST"

if ! grep -Fxq 'README.md' "$REMOTE_FILE_LIST"; then
    die "README.md is not in the draft. Upload it manually in the Zenodo UI, then rerun this script to verify the complete draft."
fi

if ! cmp -s "$EXPECTED_REMOTE_FILE_LIST" "$REMOTE_FILE_LIST"; then
    printf 'The local and remote file lists differ:\n' >&2
    comm -3 "$EXPECTED_REMOTE_FILE_LIST" "$REMOTE_FILE_LIST" >&2
    die "Zenodo draft file verification failed"
fi

printf 'Zenodo draft verification: PASS (%s files).\n' "$FILE_COUNT"
printf 'The draft has not been published. Review its metadata and files in the Zenodo UI.\n'
