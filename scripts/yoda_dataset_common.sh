#!/usr/bin/env bash

# Shared functions for upload_yoda_dataset.sh and download_yoda_dataset.sh.
# The caller is responsible for enabling strict shell options.

export LC_ALL=C

yd_fail() {
	echo "ERROR: $*" >&2
	return 1
}

yd_require_commands() {
	local command_name
	for command_name in "$@"; do
		command -v "$command_name" >/dev/null 2>&1 ||
			yd_fail "Required command is missing: $command_name" || return
	done
}

yd_validate_ready_bag() {
	local bag_dir="$1"
	[[ -d "$bag_dir" ]] || yd_fail "Bag directory does not exist: $bag_dir" || return
	[[ -f "$bag_dir/READY" ]] || yd_fail "Bag is not marked READY: $bag_dir" || return
	[[ ! -e "$bag_dir/INCOMPLETE" ]] ||
		yd_fail "Bag is marked INCOMPLETE: $bag_dir" || return
	[[ -f "$bag_dir/metadata/package_members.tsv" ]] ||
		yd_fail "Missing package membership file: $bag_dir/metadata/package_members.tsv" || return
	[[ -f "$bag_dir/manifest-sha256.txt" ]] ||
		yd_fail "Missing payload manifest: $bag_dir/manifest-sha256.txt" || return
	[[ -f "$bag_dir/tagmanifest-sha256.txt" ]] ||
		yd_fail "Missing tag manifest: $bag_dir/tagmanifest-sha256.txt" || return

	(cd -- "$bag_dir" && sha256sum --check --strict tagmanifest-sha256.txt >/dev/null) ||
		yd_fail "Tag manifest verification failed: $bag_dir/tagmanifest-sha256.txt"
}

yd_resolve_packages() {
	local bag_dir="$1" output_name="$2"
	shift 2
	local members="$bag_dir/metadata/package_members.tsv"
	local selector package
	local -a resolved=()
	local -n output_ref="$output_name"

	[[ -f "$members" ]] || yd_fail "Missing package membership file: $members" || return
	(($# > 0)) || yd_fail "At least one base directory selector is required." || return

	for selector in "$@"; do
		[[ -n "$selector" && "$selector" != *$'\t'* && "$selector" != *$'\n'* && "$selector" != *$'\r'* ]] ||
			yd_fail "Invalid base directory selector." || return
		package="$(awk -F '\t' -v wanted="$selector" \
			'NR > 1 && $2 == wanted { print $1; found = 1 } END { if (!found) exit 1 }' \
			"$members")" ||
			yd_fail "Unknown base directory: $selector" || return
		while IFS= read -r package; do
			[[ "$package" =~ ^base_[0-9]{6}\.tar\.zst$ ]] ||
				yd_fail "Invalid package name in $members: $package" || return
			resolved+=("$package")
		done <<<"$package"
	done

	mapfile -t output_ref < <(printf '%s\n' "${resolved[@]}" | sort -u)
}

yd_verify_payload_manifest() {
	local bag_dir="$1"
	local manifest="$bag_dir/manifest-sha256.txt"

	[[ -f "$manifest" ]] || yd_fail "Missing payload manifest: $manifest" || return
	[[ -s "$manifest" ]] || return 0
	(cd -- "$bag_dir" && sha256sum --check --strict manifest-sha256.txt >/dev/null) ||
		yd_fail "Payload manifest verification failed: $manifest"
}

yd_verify_packages() {
	local bag_dir="$1"
	shift
	local package expected actual
	local manifest="$bag_dir/manifest-sha256.txt"

	[[ -f "$manifest" ]] || yd_fail "Missing payload manifest: $manifest" || return
	(($# > 0)) || yd_fail "No packages were selected for verification." || return

	for package in "$@"; do
		[[ "$package" =~ ^base_[0-9]{6}\.tar\.zst$ ]] ||
			yd_fail "Invalid package name: $package" || return
		[[ -f "$bag_dir/data/packages/$package" ]] ||
			yd_fail "Missing package archive: $bag_dir/data/packages/$package" || return
		expected="$(awk -v path="data/packages/$package" '$2 == path { print $1; found = 1 } END { if (!found) exit 1 }' "$manifest")" ||
			yd_fail "Package is absent from payload manifest: $package" || return
		[[ "$expected" =~ ^[0-9a-f]{64}$ ]] ||
			yd_fail "Invalid SHA-256 value for package: $package" || return
		actual="$(sha256sum -- "$bag_dir/data/packages/$package")"
		actual="${actual%% *}"
		[[ "$actual" == "$expected" ]] ||
			yd_fail "SHA-256 mismatch for package: $package" || return
	done
}

yd_validate_archive_members() {
	local archive="$1"
	shift
	local member normalized component top allowed_dir allowed
	local -a allowed_dirs=("$@")

	((${#allowed_dirs[@]} > 0)) || yd_fail "No archive member roots were allowed." || return
	if tar -I zstd -tvf "$archive" | awk 'substr($1, 1, 1) != "-" && substr($1, 1, 1) != "d" { exit 1 }'; then
		:
	else
		yd_fail "Archive contains a symbolic link or special file: $archive"
		return
	fi
	while IFS= read -r member; do
		[[ -n "$member" ]] || continue
		[[ "$member" != /* ]] || yd_fail "Archive contains an absolute path: $member" || return
		normalized="$member"
		while [[ "$normalized" == ./* ]]; do normalized="${normalized#./}"; done
		[[ -n "$normalized" && "$normalized" != "." ]] || continue
		IFS='/' read -r -a components <<<"$normalized"
		for component in "${components[@]}"; do
			[[ "$component" != ".." ]] ||
				yd_fail "Archive contains a parent-directory path: $member" || return
		done
		top="${normalized%%/*}"
		allowed=0
		for allowed_dir in "${allowed_dirs[@]}"; do
			if [[ "$top" == "$allowed_dir" ]]; then
				allowed=1
				break
			fi
		done
		((allowed == 1)) ||
			yd_fail "Archive contains an unexpected top-level directory: $member" || return
	done < <(tar -I zstd -tf "$archive")
}

yd_extract_package() {
	local bag_dir="$1" output_dir="$2" package="$3"
	shift 3
	local stem selector package_dir raw_manifest filtered_manifest expected_paths actual_paths
	local line manifest_line path normalized top
	local -a selectors=("$@") package_dirs=() selected_paths=()

	yd_verify_packages "$bag_dir" "$package" || return
	mapfile -t package_dirs < <(awk -F '\t' -v wanted="$package" \
		'NR > 1 && $1 == wanted { print $2 }' "$bag_dir/metadata/package_members.tsv")
	yd_validate_archive_members "$bag_dir/data/packages/$package" "${package_dirs[@]}" || return
	for selector in "${selectors[@]}"; do
		for package_dir in "${package_dirs[@]}"; do
			if [[ "$package_dir" == "$selector" ]]; then
				selected_paths+=("./$selector")
				break
			fi
		done
	done
	((${#selected_paths[@]} > 0)) || yd_fail "Package has no selected directories: $package" || return
	tar -I zstd -xf "$bag_dir/data/packages/$package" -C "$output_dir" -- "${selected_paths[@]}" ||
		{
			yd_fail "Could not extract package: $package; partial output retained at $output_dir"
			return
		}

	stem="${package%.tar.zst}"
	raw_manifest="$bag_dir/metadata/per_package_file_manifests/${stem}.raw-sha256.txt"
	[[ -f "$raw_manifest" ]] ||
		{
			yd_fail "Missing raw checksum manifest: $raw_manifest; partial output retained at $output_dir"
			return
		}
	filtered_manifest="$output_dir/.${stem}.selected-sha256.txt"
	expected_paths="$output_dir/.${stem}.expected-paths.txt"
	actual_paths="$output_dir/.${stem}.actual-paths.txt"
	: >"$filtered_manifest"
	: >"$expected_paths"
	while IFS= read -r line; do
		manifest_line="$line"
		if [[ "$line" == \\* ]]; then
			line="${line:1}"
		fi
		[[ "$line" == *"  "* ]] ||
			{
				yd_fail "Invalid raw checksum record in: $raw_manifest; partial output retained at $output_dir"
				return
			}
		path="${line#*  }"
		if [[ "$manifest_line" == \\* ]]; then
			path="${path//\\\\/\\}"
		fi
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
		cd -- "$output_dir" || exit
		for path in "${selected_paths[@]}"; do
			find "$path" -type f -print
		done | sort -u
	) >"$actual_paths"
	cmp -s -- "$expected_paths" "$actual_paths" ||
		{
			awk 'NR == FNR { actual[$0] = 1; next }
				!($0 in actual) { print "Missing extracted file: " $0; if (++shown == 5) exit }' \
				"$actual_paths" "$expected_paths" >&2
			awk 'NR == FNR { expected[$0] = 1; next }
				!($0 in expected) { print "Unexpected extracted file: " $0; if (++shown == 5) exit }' \
				"$expected_paths" "$actual_paths" >&2
			yd_fail "Extracted file set differs from raw manifest for package: $package; partial output and path lists retained at $output_dir"
			return
		}
	(cd -- "$output_dir" && sha256sum --check --strict "${filtered_manifest##*/}" >/dev/null) ||
		{
			yd_fail "Extracted file verification failed for package: $package; partial output retained at $output_dir"
			return
		}
	rm -- "$filtered_manifest" "$expected_paths" "$actual_paths"
}

yd_extract_selected_packages() {
	local bag_dir="$1" output_dir="$2"
	shift 2
	local package selector pid extraction_failed=0
	local extraction_jobs="${EXTRACTION_JOBS:-1}"
	local -a selectors=("$@") packages=() pids=()

	[[ ! -e "$output_dir" && ! -L "$output_dir" ]] ||
		yd_fail "Output path already exists: $output_dir" || return
	[[ -d "$(dirname -- "$output_dir")" ]] ||
		yd_fail "Parent directory of output path does not exist: $(dirname -- "$output_dir")" || return
	[[ "$extraction_jobs" =~ ^[0-9]+$ && ${#extraction_jobs} -le 3 ]] &&
		((10#$extraction_jobs >= 1 && 10#$extraction_jobs <= 256)) ||
		yd_fail "EXTRACTION_JOBS must be an integer between 1 and 256." || return

	yd_resolve_packages "$bag_dir" packages "${selectors[@]}" || return
	mapfile -t selectors < <(printf '%s\n' "${selectors[@]}" | sort -u)
	mkdir -- "$output_dir" || return
	printf 'Extracting %s package(s) with %s worker(s)...\n' "${#packages[@]}" "$extraction_jobs"

	for package in "${packages[@]}"; do
		yd_extract_package "$bag_dir" "$output_dir" "$package" "${selectors[@]}" &
		pids+=("$!")
		if ((${#pids[@]} >= 10#$extraction_jobs)); then
			for pid in "${pids[@]}"; do
				wait "$pid" || extraction_failed=1
			done
			pids=()
			((extraction_failed == 0)) || break
		fi
	done
	for pid in "${pids[@]}"; do
		wait "$pid" || extraction_failed=1
	done
	((extraction_failed == 0)) ||
		yd_fail "One or more package extractions failed; partial output retained at $output_dir" || return

	for selector in "${selectors[@]}"; do
		[[ -d "$output_dir/$selector" ]] ||
			{
				yd_fail "Selected directory was absent after extraction: $selector; partial output retained at $output_dir"
				return
			}
	done
}
