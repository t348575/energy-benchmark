#!/usr/bin/env bash
set -euo pipefail

umask 077
export LC_ALL=C

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=yoda_dataset_common.sh
source "$SCRIPT_DIR/yoda_dataset_common.sh"

usage() {
	echo "Usage: $0 BAG_DIR IRODS_DEST [BASE_DIRECTORY ...]" >&2
	exit 1
}

(($# >= 2)) || usage
BAG_DIR="$1"
IRODS_DEST="$2"
shift 2
SELECTORS=("$@")

[[ "$IRODS_DEST" == /* && "$IRODS_DEST" != "/" ]] ||
	yd_fail "IRODS_DEST must be an absolute iRODS collection path." || exit
IRODS_DEST="${IRODS_DEST%/}"
BAG_DIR="$(cd -- "$BAG_DIR" && pwd -P)" ||
	yd_fail "Bag directory does not exist: $BAG_DIR" || exit
BAG_NAME="${BAG_DIR##*/}"
REMOTE_BAG="$IRODS_DEST/$BAG_NAME"

yd_require_commands ils imkdir iput sha256sum awk sort
yd_validate_ready_bag "$BAG_DIR"

declare -a PACKAGES=()
if ((${#SELECTORS[@]} == 0)); then
	yd_verify_payload_manifest "$BAG_DIR"
	imkdir -p "$IRODS_DEST"
	ils "$IRODS_DEST" >/dev/null
	iput -r -f "$BAG_DIR" "$IRODS_DEST"
	printf 'Uploaded complete bag to %s\n' "$REMOTE_BAG"
	exit 0
fi

yd_resolve_packages "$BAG_DIR" PACKAGES "${SELECTORS[@]}"
yd_verify_packages "$BAG_DIR" "${PACKAGES[@]}"
imkdir -p "$REMOTE_BAG" "$REMOTE_BAG/data/packages"
ils "$REMOTE_BAG" >/dev/null
iput -r -f "$BAG_DIR/metadata" "$REMOTE_BAG"

while IFS= read -r tag_file; do
	iput -f "$BAG_DIR/$tag_file" "$REMOTE_BAG/$tag_file"
done < <(find "$BAG_DIR" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' | sort)

for package in "${PACKAGES[@]}"; do
	iput -f "$BAG_DIR/data/packages/$package" "$REMOTE_BAG/data/packages/$package"
done

printf 'Uploaded %s package archive(s) for %s selected director%s to %s\n' \
	"${#PACKAGES[@]}" "${#SELECTORS[@]}" \
	"$([[ ${#SELECTORS[@]} -eq 1 ]] && printf y || printf ies)" "$REMOTE_BAG"
