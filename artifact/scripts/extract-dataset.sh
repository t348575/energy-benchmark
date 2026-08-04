#!/usr/bin/env bash
set -euo pipefail

umask 077
export LC_ALL=C

fail() {
	echo "ERROR: $*" >&2
	return 1
}

require_commands() {
	local command_name
	for command_name in "$@"; do
		command -v "$command_name" >/dev/null 2>&1 ||
			fail "Required command is missing: $command_name" || return
	done
}

validate_ready_bag() {
	local bag_dir="$1"
	[[ -f "$bag_dir/READY" ]] || fail "Bag is not marked READY: $bag_dir" || return
	[[ ! -e "$bag_dir/INCOMPLETE" ]] || fail "Bag is marked INCOMPLETE: $bag_dir" || return
	[[ -f "$bag_dir/metadata/package_members.tsv" ]] ||
		fail "Missing package membership file: $bag_dir/metadata/package_members.tsv" || return
	[[ -f "$bag_dir/manifest-sha256.txt" ]] ||
		fail "Missing payload manifest: $bag_dir/manifest-sha256.txt" || return
	[[ -f "$bag_dir/tagmanifest-sha256.txt" ]] ||
		fail "Missing tag manifest: $bag_dir/tagmanifest-sha256.txt" || return
	(cd -- "$bag_dir" && sha256sum --check --strict tagmanifest-sha256.txt >/dev/null) ||
		fail "Tag manifest verification failed: $bag_dir/tagmanifest-sha256.txt"
}

load_all_selectors() {
	local bag_dir="$1" output_name="$2"
	local list="$bag_dir/metadata/base_directories.txt"
	local -n output_ref="$output_name"
	[[ -f "$list" ]] || fail "Missing base-directory list: $list" || return
	mapfile -t output_ref <"$list"
	((${#output_ref[@]} > 0)) || fail "Base-directory list is empty: $list" || return
}

resolve_packages() {
	local bag_dir="$1" output_name="$2"
	shift 2
	local members="$bag_dir/metadata/package_members.tsv"
	local selector package
	local -a resolved=()
	local -n output_ref="$output_name"

	for selector in "$@"; do
		[[ -n "$selector" && "$selector" != *$'\t'* && "$selector" != *$'\n'* && "$selector" != *$'\r'* ]] ||
			fail "Invalid base directory selector." || return
		package="$(awk -F '\t' -v wanted="$selector" \
			'NR > 1 && $2 == wanted { print $1; found = 1 } END { if (!found) exit 1 }' \
			"$members")" || fail "Unknown base directory: $selector" || return
		while IFS= read -r package; do
			[[ "$package" =~ ^base_[0-9]{6}\.tar\.zst$ ]] ||
				fail "Invalid package name in $members: $package" || return
			resolved+=("$package")
		done <<<"$package"
	done
	mapfile -t output_ref < <(printf '%s\n' "${resolved[@]}" | sort -u)
}

verify_package() {
	local bag_dir="$1" package="$2" expected actual
	local manifest="$bag_dir/manifest-sha256.txt"

	[[ -f "$bag_dir/data/packages/$package" ]] ||
		fail "Missing package archive: $bag_dir/data/packages/$package" || return
	expected="$(awk -v path="data/packages/$package" \
		'$2 == path { print $1; found = 1 } END { if (!found) exit 1 }' "$manifest")" ||
		fail "Package is absent from payload manifest: $package" || return
	[[ "$expected" =~ ^[0-9a-f]{64}$ ]] || fail "Invalid SHA-256 value for package: $package" || return
	actual="$(sha256sum -- "$bag_dir/data/packages/$package")"
	actual="${actual%% *}"
	[[ "$actual" == "$expected" ]] || fail "SHA-256 mismatch for package: $package"
}

validate_archive_members() {
	local archive="$1"
	shift
	local member normalized component top allowed_dir allowed
	local -a allowed_dirs=("$@") components=()

	((${#allowed_dirs[@]} > 0)) || fail "No archive member roots were allowed." || return
	if ! tar -I zstd -tvf "$archive" |
		awk 'substr($1, 1, 1) != "-" && substr($1, 1, 1) != "d" { exit 1 }'; then
		fail "Archive contains a symbolic link or special file: $archive"
		return
	fi
	while IFS= read -r member; do
		[[ -n "$member" ]] || continue
		[[ "$member" != /* ]] || fail "Archive contains an absolute path: $member" || return
		normalized="$member"
		while [[ "$normalized" == ./* ]]; do normalized="${normalized#./}"; done
		[[ -n "$normalized" && "$normalized" != "." ]] || continue
		IFS='/' read -r -a components <<<"$normalized"
		for component in "${components[@]}"; do
			[[ "$component" != ".." ]] || fail "Archive contains a parent-directory path: $member" || return
		done
		top="${normalized%%/*}"
		allowed=0
		for allowed_dir in "${allowed_dirs[@]}"; do
			if [[ "$top" == "$allowed_dir" ]]; then
				allowed=1
				break
			fi
		done
		((allowed == 1)) || fail "Archive contains an unexpected top-level directory: $member" || return
	done < <(tar -I zstd -tf "$archive")
}

extract_package() {
	local bag_dir="$1" output_dir="$2" package="$3"
	shift 3
	local stem selector package_dir raw_manifest filtered_manifest expected_paths actual_paths
	local line manifest_line path normalized top
	local -a selectors=("$@") package_dirs=() selected_paths=()

	verify_package "$bag_dir" "$package" || return
	mapfile -t package_dirs < <(awk -F '\t' -v wanted="$package" \
		'NR > 1 && $1 == wanted { print $2 }' "$bag_dir/metadata/package_members.tsv")
	validate_archive_members "$bag_dir/data/packages/$package" "${package_dirs[@]}" || return
	for selector in "${selectors[@]}"; do
		for package_dir in "${package_dirs[@]}"; do
			if [[ "$package_dir" == "$selector" ]]; then
				selected_paths+=("./$selector")
				break
			fi
		done
	done
	((${#selected_paths[@]} > 0)) || fail "Package has no selected directories: $package" || return
	tar -I zstd -xf "$bag_dir/data/packages/$package" -C "$output_dir" -- "${selected_paths[@]}" ||
		{ fail "Could not extract package: $package; partial output retained at $output_dir"; return; }

	stem="${package%.tar.zst}"
	raw_manifest="$bag_dir/metadata/per_package_file_manifests/${stem}.raw-sha256.txt"
	[[ -f "$raw_manifest" ]] ||
		{ fail "Missing raw checksum manifest: $raw_manifest; partial output retained at $output_dir"; return; }
	filtered_manifest="$output_dir/.${stem}.selected-sha256.txt"
	expected_paths="$output_dir/.${stem}.expected-paths.txt"
	actual_paths="$output_dir/.${stem}.actual-paths.txt"
	: >"$filtered_manifest"
	: >"$expected_paths"
	while IFS= read -r line; do
		manifest_line="$line"
		[[ "$line" != \\* ]] || line="${line:1}"
		[[ "$line" == *"  "* ]] ||
			{ fail "Invalid raw checksum record in: $raw_manifest"; return; }
		path="${line#*  }"
		[[ "$manifest_line" != \\* ]] || path="${path//\\\\/\\}"
		normalized="${path#./}"
		top="${normalized%%/*}"
		for selector in "${selectors[@]}"; do
			if [[ "$top" == "$selector" ]]; then
				printf '%s\n' "$manifest_line" >>"$filtered_manifest"
				printf './%s\n' "$normalized" >>"$expected_paths"
				break
			fi
		done
	done <"$raw_manifest"
	sort -u -o "$expected_paths" "$expected_paths"
	(
		cd -- "$output_dir"
		for path in "${selected_paths[@]}"; do find "$path" -type f -print; done | sort -u
	) >"$actual_paths"
	cmp -s -- "$expected_paths" "$actual_paths" ||
		{
			awk 'NR == FNR { actual[$0] = 1; next }
				!($0 in actual) { print "Missing extracted file: " $0; if (++shown == 5) exit }' \
				"$actual_paths" "$expected_paths" >&2
			awk 'NR == FNR { expected[$0] = 1; next }
				!($0 in expected) { print "Unexpected extracted file: " $0; if (++shown == 5) exit }' \
				"$expected_paths" "$actual_paths" >&2
			fail "Extracted file set differs from raw manifest for package: $package; diagnostics retained at $output_dir"
			return
		}
	(cd -- "$output_dir" && sha256sum --check --strict "${filtered_manifest##*/}" >/dev/null) ||
		{ fail "Extracted file verification failed for package: $package"; return; }
	rm -- "$filtered_manifest" "$expected_paths" "$actual_paths"
}

extract_selected_packages() {
	local bag_dir="$1" output_dir="$2"
	shift 2
	local package selector pid extraction_failed=0
	local extraction_jobs="${EXTRACTION_JOBS:-1}"
	local -a selectors=("$@") packages=() pids=()

	[[ ! -e "$output_dir" && ! -L "$output_dir" ]] || fail "Output path already exists: $output_dir" || return
	[[ "$extraction_jobs" =~ ^[0-9]+$ && ${#extraction_jobs} -le 3 ]] &&
		((10#$extraction_jobs >= 1 && 10#$extraction_jobs <= 256)) ||
		fail "EXTRACTION_JOBS must be an integer between 1 and 256." || return
	resolve_packages "$bag_dir" packages "${selectors[@]}" || return
	mapfile -t selectors < <(printf '%s\n' "${selectors[@]}" | sort -u)
	mkdir -- "$output_dir"
	printf 'Extracting %s package(s) with %s worker(s)...\n' "${#packages[@]}" "$extraction_jobs"

	for package in "${packages[@]}"; do
		extract_package "$bag_dir" "$output_dir" "$package" "${selectors[@]}" &
		pids+=("$!")
		if ((${#pids[@]} >= 10#$extraction_jobs)); then
			for pid in "${pids[@]}"; do wait "$pid" || extraction_failed=1; done
			pids=()
			((extraction_failed == 0)) || break
		fi
	done
	for pid in "${pids[@]}"; do wait "$pid" || extraction_failed=1; done
	((extraction_failed == 0)) ||
		fail "One or more package extractions failed; partial output retained at $output_dir" || return
	for selector in "${selectors[@]}"; do
		[[ -d "$output_dir/$selector" ]] ||
			fail "Selected directory was absent after extraction: $selector" || return
	done
}

usage() {
	echo "Usage: $0 BAG_DIR OUTPUT_DIR [BASE_DIRECTORY ...]" >&2
	exit 1
}

(($# >= 2)) || usage
BAG_DIR="$1"
OUTPUT_DIR="$2"
shift 2
SELECTORS=("$@")

require_commands sha256sum awk sort tar zstd find cmp
[[ -d "$BAG_DIR" ]] || fail "Bag directory does not exist: $BAG_DIR" || exit
BAG_DIR="$(cd -- "$BAG_DIR" && pwd -P)"
OUTPUT_PARENT="$(dirname -- "$OUTPUT_DIR")"
[[ -d "$OUTPUT_PARENT" ]] || fail "Parent directory of OUTPUT_DIR does not exist: $OUTPUT_PARENT" || exit
OUTPUT_PARENT="$(cd -- "$OUTPUT_PARENT" && pwd -P)"
OUTPUT_DIR="$OUTPUT_PARENT/$(basename -- "$OUTPUT_DIR")"
case "$OUTPUT_DIR/" in
"$BAG_DIR/"*) fail "OUTPUT_DIR must be outside the bag."; exit ;;
esac

validate_ready_bag "$BAG_DIR"
EXTRACT_ALL=0
if ((${#SELECTORS[@]} == 0)); then
	EXTRACT_ALL=1
	load_all_selectors "$BAG_DIR" SELECTORS
fi
extract_selected_packages "$BAG_DIR" "$OUTPUT_DIR" "${SELECTORS[@]}"
if ((EXTRACT_ALL)); then
	printf 'Extracted and verified complete dataset at %s\n' "$OUTPUT_DIR"
else
	printf 'Extracted and verified %s selected director%s at %s\n' \
		"${#SELECTORS[@]}" "$([[ ${#SELECTORS[@]} -eq 1 ]] && printf y || printf ies)" "$OUTPUT_DIR"
fi
