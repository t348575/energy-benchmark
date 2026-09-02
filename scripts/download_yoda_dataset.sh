#!/usr/bin/env bash
set -euo pipefail

umask 077
export LC_ALL=C

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=yoda_dataset_common.sh
source "$SCRIPT_DIR/yoda_dataset_common.sh"

usage() {
	cat >&2 <<EOF
Usage:
  $0 IRODS_SOURCE OUTPUT_DIR [BASE_DIRECTORY ...]
  $0 --extract-all IRODS_SOURCE OUTPUT_DIR
  $0 --local BAG_DIR OUTPUT_DIR [BASE_DIRECTORY ...]
EOF
	exit 1
}

LOCAL_MODE=0
EXTRACT_ALL=0
while (($# > 0)); do
	case "$1" in
	--local)
		((LOCAL_MODE == 0)) || usage
		LOCAL_MODE=1
		shift
		;;
	--extract-all)
		((EXTRACT_ALL == 0)) || usage
		EXTRACT_ALL=1
		shift
		;;
	--)
		shift
		break
		;;
	-*) usage ;;
	*) break ;;
	esac
done

(($# >= 2)) || usage
SOURCE="$1"
OUTPUT_DIR="$2"
shift 2
SELECTORS=("$@")

((LOCAL_MODE == 0 || EXTRACT_ALL == 0)) || usage
((EXTRACT_ALL == 0 || ${#SELECTORS[@]} == 0)) ||
	yd_fail "--extract-all cannot be combined with base-directory selectors." || exit
[[ ! -e "$OUTPUT_DIR" && ! -L "$OUTPUT_DIR" ]] ||
	yd_fail "OUTPUT_DIR already exists: $OUTPUT_DIR" || exit
OUTPUT_PARENT="$(dirname -- "$OUTPUT_DIR")"
[[ -d "$OUTPUT_PARENT" ]] ||
	yd_fail "Parent directory of OUTPUT_DIR does not exist: $OUTPUT_PARENT" || exit
OUTPUT_PARENT="$(cd -- "$OUTPUT_PARENT" && pwd -P)"
OUTPUT_DIR="$OUTPUT_PARENT/$(basename -- "$OUTPUT_DIR")"

load_all_selectors() {
	local bag_dir="$1" output_name="$2"
	local list="$bag_dir/metadata/base_directories.txt"
	local -n output_ref="$output_name"

	[[ -f "$list" ]] || yd_fail "Missing base-directory list: $list" || return
	mapfile -t output_ref <"$list"
	((${#output_ref[@]} > 0)) || yd_fail "Base-directory list is empty: $list" || return
}

if ((LOCAL_MODE)); then
	[[ -d "$SOURCE" ]] || yd_fail "Local bag directory does not exist: $SOURCE" || exit
	LOCAL_BAG="$(cd -- "$SOURCE" && pwd -P)"
	case "$OUTPUT_DIR/" in
	"$LOCAL_BAG/"*) yd_fail "OUTPUT_DIR must be outside the local bag." || exit ;;
	esac

	yd_require_commands sha256sum awk sort tar zstd find grep cmp
	yd_validate_ready_bag "$LOCAL_BAG"
	LOCAL_ALL=0
	if ((${#SELECTORS[@]} == 0)); then
		LOCAL_ALL=1
		load_all_selectors "$LOCAL_BAG" SELECTORS
	fi
	yd_extract_selected_packages "$LOCAL_BAG" "$OUTPUT_DIR" "${SELECTORS[@]}"
	if ((LOCAL_ALL)); then
		printf 'Extracted and verified complete local bag at %s\n' "$OUTPUT_DIR"
	else
		printf 'Extracted and verified %s selected director%s from local bag at %s\n' \
			"${#SELECTORS[@]}" \
			"$([[ ${#SELECTORS[@]} -eq 1 ]] && printf y || printf ies)" "$OUTPUT_DIR"
	fi
	exit 0
fi

IRODS_SOURCE="$SOURCE"
[[ "$IRODS_SOURCE" == /* && "$IRODS_SOURCE" != "/" ]] ||
	yd_fail "IRODS_SOURCE must be an absolute iRODS collection path." || exit
IRODS_SOURCE="${IRODS_SOURCE%/}"

yd_require_commands ils iget sha256sum awk sort tar zstd find grep cmp
ils "$IRODS_SOURCE" >/dev/null

if ((${#SELECTORS[@]} == 0 && EXTRACT_ALL == 0)); then
	iget -r "$IRODS_SOURCE" "$OUTPUT_DIR"
	yd_validate_ready_bag "$OUTPUT_DIR"
	yd_verify_payload_manifest "$OUTPUT_DIR"
	printf 'Downloaded and verified complete bag at %s\n' "$OUTPUT_DIR"
	exit 0
fi

STAGING="$(mktemp -d "$OUTPUT_PARENT/.yoda-download.XXXXXX")"
trap 'rc=$?; if (( rc != 0 )); then printf "Download failed; staging retained at %s\\n" "$STAGING" >&2; fi' EXIT
mkdir -p -- "$STAGING/data/packages"
iget -r "$IRODS_SOURCE/metadata" "$STAGING/metadata"
for tag_file in bagit.txt bag-info.txt manifest-sha256.txt tagmanifest-sha256.txt READY; do
	iget "$IRODS_SOURCE/$tag_file" "$STAGING/$tag_file"
done
yd_validate_ready_bag "$STAGING"

if ((EXTRACT_ALL)); then
	load_all_selectors "$STAGING" SELECTORS
fi

declare -a PACKAGES=()
yd_resolve_packages "$STAGING" PACKAGES "${SELECTORS[@]}"
for package in "${PACKAGES[@]}"; do
	iget "$IRODS_SOURCE/data/packages/$package" "$STAGING/data/packages/$package"
done

yd_extract_selected_packages "$STAGING" "$OUTPUT_DIR" "${SELECTORS[@]}"
rm -rf -- "$STAGING"
trap - EXIT
if ((EXTRACT_ALL)); then
	printf 'Downloaded, extracted, and verified complete dataset at %s\n' "$OUTPUT_DIR"
else
	printf 'Downloaded and verified %s selected director%s at %s\n' \
		"${#SELECTORS[@]}" \
		"$([[ ${#SELECTORS[@]} -eq 1 ]] && printf y || printf ies)" "$OUTPUT_DIR"
fi
