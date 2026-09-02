#!/usr/bin/env bash
set -euo pipefail

# Independent base directories can be archived concurrently with JOBS workers.
umask 077
export LC_ALL=C
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

usage() {
  echo "Usage: $0 [--modified-after YYYY-MM-DD] [--modified-before YYYY-MM-DD] [--whitelist FILE] [--exclude-pattern REGEX] [--exclude-file PATTERN]... RAW_DATASET_ROOT OUTPUT_BAG_DIR [DATASET_NAME]" >&2
  exit 1
}

MODIFIED_AFTER=""
MODIFIED_BEFORE=""
WHITELIST_FILE=""
EXCLUDE_PATTERN=""
EXCLUDED_FILE_PATTERNS=()
while (( $# > 0 )); do
  case "$1" in
    --modified-after)
      [[ $# -ge 2 && -z "$MODIFIED_AFTER" ]] || usage
      MODIFIED_AFTER="$2"
      shift 2
      ;;
    --modified-before)
      [[ $# -ge 2 && -z "$MODIFIED_BEFORE" ]] || usage
      MODIFIED_BEFORE="$2"
      shift 2
      ;;
    --whitelist)
      [[ $# -ge 2 && -z "$WHITELIST_FILE" ]] || usage
      WHITELIST_FILE="$2"
      shift 2
      ;;
    --exclude-pattern)
      [[ $# -ge 2 && -z "$EXCLUDE_PATTERN" ]] || usage
      EXCLUDE_PATTERN="$2"
      shift 2
      ;;
    --exclude-file)
      [[ $# -ge 2 && -n "$2" ]] || usage
      EXCLUDED_FILE_PATTERNS+=("$2")
      shift 2
      ;;
    --)
      shift
      break
      ;;
    -*) usage ;;
    *) break ;;
  esac
done

if [[ $# -lt 2 || $# -gt 3 ]]; then
  usage
fi

RAW_ROOT="$1"
BAG_DIR="$2"
DATASET_NAME="${3:-research-dataset}"
DATE_UTC="$(date -u +%Y-%m-%d)"

JOBS="${JOBS:-1}"
VALIDATION_JOBS="${VALIDATION_JOBS:-1}"
# Group base directories by this raw apparent-size target. Compressed archive
# sizes vary with data, so they are approximate rather than capped.
TARGET_RAW_SIZE_GIB="${TARGET_RAW_SIZE_GIB:-4}"
ZSTD_LEVEL="${ZSTD_LEVEL:-19}"
ZSTD_LONG="${ZSTD_LONG:-27}"
# When JOBS is greater than one and this is unset, use an even share of CPUs.
ZSTD_THREADS="${ZSTD_THREADS:-}"
VALIDATION_PYTHON="${VALIDATION_PYTHON:-python3}"
SHA256_PASSTHROUGH="$SCRIPT_DIR/sha256_passthrough.py"
PACKAGE_VALIDATOR="$SCRIPT_DIR/validate_yoda_package.py"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

require_uint_range() {
  local name="$1" value="$2" minimum="$3" maximum="$4"

  if [[ ! "$value" =~ ^[0-9]+$ ]] || [[ ${#value} -gt 3 ]] \
    || (( 10#$value < minimum || 10#$value > maximum )); then
    fail "$name must be an integer between $minimum and $maximum."
  fi
}

if [[ ! -d "$RAW_ROOT" ]]; then
  fail "RAW_DATASET_ROOT does not exist: $RAW_ROOT"
fi
RAW_ROOT="$(cd -- "$RAW_ROOT" && pwd -P)"

whitelist_entries=()
if [[ -n "$WHITELIST_FILE" ]]; then
  [[ -f "$WHITELIST_FILE" && ! -L "$WHITELIST_FILE" ]] \
    || fail "Whitelist must be a regular file: $WHITELIST_FILE"
  while IFS= read -r whitelist_entry || [[ -n "$whitelist_entry" ]]; do
    whitelist_entry="${whitelist_entry%$'\r'}"
    [[ -n "$whitelist_entry" ]] || continue
    if [[ "$whitelist_entry" == "." || "$whitelist_entry" == ".." \
      || "$whitelist_entry" == */* || "$whitelist_entry" == *$'\t'* ]]; then
      fail "Invalid whitelist entry: $whitelist_entry"
    fi
    printf '%s' "$whitelist_entry" | iconv -f UTF-8 -t UTF-8 >/dev/null 2>&1 \
      || fail "Whitelist entry is not valid UTF-8."
    [[ -d "$RAW_ROOT/$whitelist_entry" && ! -L "$RAW_ROOT/$whitelist_entry" ]] \
      || fail "Whitelisted directory does not exist: $whitelist_entry"
    whitelist_entries+=("$whitelist_entry")
  done < "$WHITELIST_FILE"
  (( ${#whitelist_entries[@]} > 0 )) || fail "Whitelist contains no directory names."
fi

# Do not create parent directories. It avoids a check-then-create race and lets us
# reject an output path nested in the source before writing anything.
if [[ -e "$BAG_DIR" || -L "$BAG_DIR" ]]; then
  fail "OUTPUT_BAG_DIR already exists: $BAG_DIR"
fi
BAG_PARENT="$(dirname -- "$BAG_DIR")"
BAG_LEAF="$(basename -- "$BAG_DIR")"
if [[ ! -d "$BAG_PARENT" ]]; then
  fail "Parent directory of OUTPUT_BAG_DIR does not exist: $BAG_PARENT"
fi
BAG_PARENT="$(cd -- "$BAG_PARENT" && pwd -P)"
BAG_DIR="$BAG_PARENT/$BAG_LEAF"

case "$BAG_DIR/" in
  "$RAW_ROOT/"*) fail "OUTPUT_BAG_DIR must be outside RAW_DATASET_ROOT." ;;
esac

dataset_name_pattern='^[A-Za-z0-9][A-Za-z0-9._ -]*$'
if [[ ! "$DATASET_NAME" =~ $dataset_name_pattern ]]; then
  fail "DATASET_NAME may contain only letters, digits, spaces, periods, underscores, and hyphens."
fi

require_uint_range "JOBS" "$JOBS" 1 256
require_uint_range "VALIDATION_JOBS" "$VALIDATION_JOBS" 1 256
require_uint_range "TARGET_RAW_SIZE_GIB" "$TARGET_RAW_SIZE_GIB" 1 999

require_uint_range "ZSTD_LEVEL" "$ZSTD_LEVEL" 1 22
require_uint_range "ZSTD_LONG" "$ZSTD_LONG" 10 31

command -v "$VALIDATION_PYTHON" >/dev/null 2>&1 \
  || fail "Required validation Python command is missing: $VALIDATION_PYTHON"
[[ -f "$SHA256_PASSTHROUGH" ]] || fail "SHA-256 passthrough is missing: $SHA256_PASSTHROUGH"
[[ -f "$PACKAGE_VALIDATOR" ]] || fail "Package validator is missing: $PACKAGE_VALIDATOR"

for tool in tar zstd sha256sum find sort xargs stat du iconv date nproc numfmt; do
  command -v "$tool" >/dev/null 2>&1 || fail "Required command is missing: $tool"
done

if [[ -z "$ZSTD_THREADS" ]]; then
  if (( JOBS == 1 )); then
    ZSTD_THREADS=0
  else
    cpu_count="$(nproc)"
    ZSTD_THREADS=$(( cpu_count / JOBS ))
    (( ZSTD_THREADS > 0 )) || ZSTD_THREADS=1
  fi
fi
require_uint_range "ZSTD_THREADS" "$ZSTD_THREADS" 0 256
if (( JOBS > 1 && ZSTD_THREADS == 0 )); then
  fail "ZSTD_THREADS=0 cannot be used with JOBS greater than one; set a finite thread count."
fi
if (( JOBS > 1 && (BASH_VERSINFO[0] < 5 || (BASH_VERSINFO[0] == 5 && BASH_VERSINFO[1] < 1)) )); then
  fail "JOBS greater than one requires Bash 5.1 or newer."
fi

parse_modified_date() {
  local option="$1" value="$2" output_variable="$3" epoch
  local date_pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}$'

  if [[ ! "$value" =~ $date_pattern ]]; then
    fail "$option must use YYYY-MM-DD."
  fi
  if ! epoch="$(date -u -d "$value 00:00:00" +%s 2>/dev/null)" \
    || [[ "$(date -u -d "$value 00:00:00" +%F 2>/dev/null)" != "$value" ]]; then
    fail "$option is not a valid calendar date: $value"
  fi
  printf -v "$output_variable" '%s' "$epoch"
}

MODIFIED_AFTER_EPOCH=""
MODIFIED_BEFORE_EPOCH=""
if [[ -n "$MODIFIED_AFTER" ]]; then
  parse_modified_date "--modified-after" "$MODIFIED_AFTER" MODIFIED_AFTER_EPOCH
fi
if [[ -n "$MODIFIED_BEFORE" ]]; then
  parse_modified_date "--modified-before" "$MODIFIED_BEFORE" MODIFIED_BEFORE_EPOCH
fi
if [[ -n "$MODIFIED_AFTER" && -n "$MODIFIED_BEFORE" ]] \
  && (( MODIFIED_AFTER_EPOCH >= MODIFIED_BEFORE_EPOCH )); then
  fail "--modified-before must be later than --modified-after."
fi
if [[ -n "$EXCLUDE_PATTERN" ]]; then
  [[ "$EXCLUDE_PATTERN" != *$'\t'* && "$EXCLUDE_PATTERN" != *$'\n'* \
    && "$EXCLUDE_PATTERN" != *$'\r'* ]] \
    || fail "--exclude-pattern must be a single-line regular expression."
  exclude_regex_status=0
  [[ "" =~ $EXCLUDE_PATTERN ]] || exclude_regex_status=$?
  (( exclude_regex_status != 2 )) || fail "--exclude-pattern is not a valid regular expression."
fi
excluded_file_predicates=()
for excluded_file_pattern in "${EXCLUDED_FILE_PATTERNS[@]}"; do
  if [[ "$excluded_file_pattern" == */* || "$excluded_file_pattern" == *$'\t'* \
    || "$excluded_file_pattern" == *$'\n'* || "$excluded_file_pattern" == *$'\r'* ]]; then
    fail "--exclude-file must be a single basename pattern without slashes."
  fi
  [[ "config.yaml" != $excluded_file_pattern ]] \
    || fail "--exclude-file may not match required config.yaml files."
  (( ${#excluded_file_predicates[@]} == 0 )) || excluded_file_predicates+=(-o)
  excluded_file_predicates+=(-name "$excluded_file_pattern")
done

# Date filters use the YYYY-MM-DD_HH-MM-SS suffix of each direct child
# directory. Filesystem modification times are deliberately ignored.
result_timestamp_pattern='([0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2})$'
base_dirs=()
missing_dirs=()
missing_requirements=()
invalid_dirs=()
skipped_by_date=0
skipped_by_pattern=0
skipped_invalid_timestamp=0
while IFS= read -r -d '' src_dir; do
  base_name="${src_dir##*/}"
  is_whitelisted=0
  for whitelist_entry in "${whitelist_entries[@]}"; do
    if [[ "$base_name" == "$whitelist_entry" ]]; then
      is_whitelisted=1
      break
    fi
  done
  if [[ -n "$EXCLUDE_PATTERN" && "$base_name" =~ $EXCLUDE_PATTERN ]]; then
    skipped_by_pattern=$((skipped_by_pattern + 1))
    continue
  fi
  if (( ! is_whitelisted )) && [[ -n "$MODIFIED_AFTER" || -n "$MODIFIED_BEFORE" ]]; then
    if [[ ! "$base_name" =~ $result_timestamp_pattern ]]; then
      printf 'WARNING: Ignoring directory without YYYY-MM-DD_HH-MM-SS suffix: %q\n' \
        "$src_dir" >&2
      skipped_invalid_timestamp=$((skipped_invalid_timestamp + 1))
      continue
    fi
    result_timestamp="${BASH_REMATCH[1]}"
    result_datetime="${result_timestamp:0:10} ${result_timestamp:11:2}:${result_timestamp:14:2}:${result_timestamp:17:2}"
    if ! result_epoch="$(date -u -d "$result_datetime" +%s 2>/dev/null)" \
      || [[ "$(date -u -d "@$result_epoch" +%Y-%m-%d_%H-%M-%S)" != "$result_timestamp" ]]; then
      printf 'WARNING: Ignoring directory with invalid YYYY-MM-DD_HH-MM-SS suffix: %q\n' \
        "$src_dir" >&2
      skipped_invalid_timestamp=$((skipped_invalid_timestamp + 1))
      continue
    fi
    if [[ -n "$MODIFIED_AFTER" ]] && (( result_epoch <= MODIFIED_AFTER_EPOCH )); then
      skipped_by_date=$((skipped_by_date + 1))
      continue
    fi
    if [[ -n "$MODIFIED_BEFORE" ]] && (( result_epoch >= MODIFIED_BEFORE_EPOCH )); then
      skipped_by_date=$((skipped_by_date + 1))
      continue
    fi
  fi

  missing=()
  [[ -f "$src_dir/config.yaml" && ! -L "$src_dir/config.yaml" ]] || missing+=("regular config.yaml")
  [[ -d "$src_dir/data" && ! -L "$src_dir/data" ]] || missing+=("real data/")

  if [[ "$base_name" == *$'\t'* || "$base_name" == *$'\n'* || "$base_name" == *$'\r'* ]]; then
    invalid_dirs+=("$src_dir (tab, carriage return, or line feed in base directory name)")
  elif ! printf '%s' "$base_name" | iconv -f UTF-8 -t UTF-8 >/dev/null 2>&1; then
    invalid_dirs+=("$src_dir (base directory name is not valid UTF-8)")
  elif (( ${#missing[@]} > 0 )); then
    missing_summary=""
    for missing_item in "${missing[@]}"; do
      missing_summary+="${missing_summary:+, }$missing_item"
    done
    missing_dirs+=("$base_name")
    missing_requirements+=("$missing_summary")
  else
    base_dirs+=("$src_dir")
  fi
done < <(find "$RAW_ROOT" -mindepth 1 -maxdepth 1 -type d -print0 | sort -z)

# Raw checksum manifests are UTF-8, line-oriented GNU sha256sum files. Check
# only selected trees, so unrelated older root entries do not block an export.
for src_dir in "${base_dirs[@]}"; do
  if find "$src_dir" \( -name $'*\n*' -o -name $'*\r*' \) -print -quit | grep -q .; then
    invalid_dirs+=("$src_dir (contains a carriage return or line feed in a name)")
  elif ! find "$src_dir" -print0 | iconv -f UTF-8 -t UTF-8 >/dev/null 2>&1; then
    invalid_dirs+=("$src_dir (contains a name that is not valid UTF-8)")
  elif find "$src_dir" -type l -print -quit | grep -q .; then
    invalid_dirs+=("$src_dir (contains a symbolic link)")
  elif find "$src_dir" \( -type b -o -type c -o -type p -o -type s \) -print -quit | grep -q .; then
    invalid_dirs+=("$src_dir (contains a device, FIFO, or socket)")
  fi
done

if (( ${#invalid_dirs[@]} > 0 )); then
  echo "ERROR: Refusing to omit invalid selected top-level directories:" >&2
  printf '  %q\n' "${invalid_dirs[@]}" >&2
  exit 1
fi

TOTAL="${#base_dirs[@]}"
if (( TOTAL == 0 && ${#missing_dirs[@]} == 0 )); then
  if [[ -n "$MODIFIED_AFTER" && -n "$MODIFIED_BEFORE" ]]; then
    fail "No valid base directories matched the result timestamp filters."
  elif [[ -n "$MODIFIED_AFTER" ]]; then
    fail "No valid base directories have a name timestamp after $MODIFIED_AFTER."
  elif [[ -n "$MODIFIED_BEFORE" ]]; then
    fail "No valid base directories have a name timestamp before $MODIFIED_BEFORE."
  elif [[ -n "$EXCLUDE_PATTERN" ]]; then
    fail "No valid base directories remained after applying --exclude-pattern."
  fi
  fail "No valid base directories found."
fi

TARGET_RAW_BYTES=$(( 10#$TARGET_RAW_SIZE_GIB * 1024 * 1024 * 1024 ))
base_raw_bytes=()
group_starts=()
group_ends=()
group_raw_bytes=()
PACKAGE_TOTAL=0

if (( TOTAL > 0 )); then
  printf 'Sizing %s base directories for an approximate %s raw-size archive target...\n' \
    "$TOTAL" "$(numfmt --to=iec-i --suffix=B "$TARGET_RAW_BYTES")"
  for ((i = 0; i < TOTAL; i++)); do
    base_raw_bytes[i]="$(du -sb -- "${base_dirs[i]}" | awk '{print $1}')"
  done

  group_start=0
  group_bytes=0
  for ((i = 0; i < TOTAL; i++)); do
    if (( group_bytes > 0 && group_bytes + base_raw_bytes[i] > TARGET_RAW_BYTES )); then
      group_starts+=("$group_start")
      group_ends+=("$((i - 1))")
      group_raw_bytes+=("$group_bytes")
      group_start=$i
      group_bytes=0
    fi
    group_bytes=$(( group_bytes + base_raw_bytes[i] ))
  done
  group_starts+=("$group_start")
  group_ends+=("$((TOTAL - 1))")
  group_raw_bytes+=("$group_bytes")
  PACKAGE_TOTAL="${#group_starts[@]}"
fi

bag_created=0
on_exit() {
  local rc=$?
  if (( bag_created && rc != 0 )); then
    printf 'Bag creation stopped with exit status %s on %s. Do not upload this directory.\n' \
      "$rc" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$BAG_DIR/INCOMPLETE" 2>/dev/null || true
  fi
}
trap on_exit EXIT

# mkdir without -p is atomic for the final path.
mkdir -- "$BAG_DIR"
bag_created=1
mkdir -p -- \
  "$BAG_DIR/data/packages" \
  "$BAG_DIR/metadata/per_package_file_manifests" \
  "$BAG_DIR/metadata/per_package_records" \
  "$BAG_DIR/metadata/per_package_tar_stream_sha256"
printf 'Bag creation is in progress. Do not upload this directory.\n' > "$BAG_DIR/INCOMPLETE"

printf 'Raw dataset root: %s\n' "$RAW_ROOT"
printf 'Output BagIt dir:  %s\n' "$BAG_DIR"
printf 'Dataset name:      %s\n' "$DATASET_NAME"
printf 'Package workers:   %s\n' "$JOBS"
printf 'Validation workers: %s\n' "$VALIDATION_JOBS"
printf 'Raw target/package: %s\n' "$(numfmt --to=iec-i --suffix=B "$TARGET_RAW_BYTES")"
printf 'Compression:       zstd -T%s -%s --long=%s\n' \
  "$ZSTD_THREADS" "$ZSTD_LEVEL" "$ZSTD_LONG"
if [[ -n "$MODIFIED_AFTER" ]]; then
  printf 'Result-name after:  %s 00:00:00 (benchmark host time)\n' "$MODIFIED_AFTER"
fi
if [[ -n "$MODIFIED_BEFORE" ]]; then
  printf 'Result-name before: %s 00:00:00 (benchmark host time)\n' "$MODIFIED_BEFORE"
fi
if [[ -n "$MODIFIED_AFTER" || -n "$MODIFIED_BEFORE" ]]; then
  printf 'Name-timestamp-filtered root entries skipped: %s\n' "$skipped_by_date"
  printf 'Invalid/missing timestamp root entries ignored: %s\n' "$skipped_invalid_timestamp"
fi
if [[ -n "$WHITELIST_FILE" ]]; then
  printf 'Date-filter whitelist: %s entries\n' "${#whitelist_entries[@]}"
fi
if [[ -n "$EXCLUDE_PATTERN" ]]; then
  printf 'Excluded-name regex: %s (skipped root entries: %s)\n' \
    "$EXCLUDE_PATTERN" "$skipped_by_pattern"
fi
if (( ${#EXCLUDED_FILE_PATTERNS[@]} > 0 )); then
  printf 'Excluded file basename patterns: %s\n' "${#EXCLUDED_FILE_PATTERNS[@]}"
fi
printf 'Valid base directories found: %s\n' "$TOTAL"
printf 'Package archives planned: %s\n' "$PACKAGE_TOTAL"
printf 'Root entries skipped for missing required items: %s\n\n' "${#missing_dirs[@]}"

cat > "$BAG_DIR/bagit.txt" <<EOF
BagIt-Version: 1.0
Tag-File-Character-Encoding: UTF-8
EOF

cat > "$BAG_DIR/metadata/dataset_description.yaml" <<EOF
dataset_name: "$DATASET_NAME"
created_utc: "$DATE_UTC"
description: "Compressed BagIt package for Yoda/VU research data archiving."
original_structure: "Each base directory contains config.yaml and data/; info.json is optional."
packaging:
  outer_format: "BagIt v1.0"
  payload_format: "tar.zst archives"
  checksum_algorithm: "sha256"
  raw_size_target_bytes: "$TARGET_RAW_BYTES"
  raw_size_target_gib: "$TARGET_RAW_SIZE_GIB"
  planned_archives: "$PACKAGE_TOTAL"
compression:
  algorithm: "zstd"
  backend: "cpu"
  level: "$ZSTD_LEVEL"
  long_window: "$ZSTD_LONG"
  threads_per_worker: "$ZSTD_THREADS"
EOF
cat >> "$BAG_DIR/metadata/dataset_description.yaml" <<EOF
validation:
  mode: "streaming CPU decompression"
  decompressor: "standard zstd"
  tar_stream_checksum: "sha256"
  file_checksum: "sha256"
parallelism:
  package_workers: "$JOBS"
  validation_workers: "$VALIDATION_JOBS"
notes:
  - "Each package archive contains one or more original base directories."
  - "Base directories are greedily grouped by raw apparent size."
  - "Source data must remain unchanged until packaging completes."
  - "Per-package raw manifests can be used to verify files after unpacking."
EOF

result_timestamp_after="none"
result_timestamp_before="none"
excluded_name_regex="none"
[[ -z "$MODIFIED_AFTER" ]] || result_timestamp_after="${MODIFIED_AFTER}_00-00-00"
[[ -z "$MODIFIED_BEFORE" ]] || result_timestamp_before="${MODIFIED_BEFORE}_00-00-00"
[[ -z "$EXCLUDE_PATTERN" ]] || excluded_name_regex="$EXCLUDE_PATTERN"
whitelist_metadata="none"
if (( ${#whitelist_entries[@]} > 0 )); then
  whitelist_metadata="metadata/date_filter_whitelist.txt"
  printf '%s\n' "${whitelist_entries[@]}" | sort -u > "$BAG_DIR/$whitelist_metadata"
fi
excluded_file_metadata="none"
if (( ${#EXCLUDED_FILE_PATTERNS[@]} > 0 )); then
  excluded_file_metadata="metadata/excluded_file_patterns.txt"
  printf '%s\n' "${EXCLUDED_FILE_PATTERNS[@]}" | sort -u > "$BAG_DIR/$excluded_file_metadata"
fi
if [[ -n "$MODIFIED_AFTER" || -n "$MODIFIED_BEFORE" || -n "$WHITELIST_FILE" \
  || -n "$EXCLUDE_PATTERN" || ${#EXCLUDED_FILE_PATTERNS[@]} -gt 0 ]]; then
  printf 'result_timestamp_after: %s\nresult_timestamp_before: %s\ntimestamp_source: result directory name suffix YYYY-MM-DD_HH-MM-SS\ntimestamp_timezone: benchmark host local time\ndate_filter_whitelist: %s\nwhitelist_scope: bypasses date filters only\nexcluded_name_regex: %s\nexcluded_file_patterns: %s\nfile_pattern_scope: regular-file basenames at any depth\nfilter_scope: direct children of RAW_DATASET_ROOT only\nskipped_by_date: %s\nskipped_invalid_timestamps: %s\nskipped_name_matches: %s\nskipped_root_entries: %s\n' \
    "$result_timestamp_after" "$result_timestamp_before" "$whitelist_metadata" \
    "$excluded_name_regex" "$excluded_file_metadata" \
    "$skipped_by_date" "$skipped_invalid_timestamp" "$skipped_by_pattern" \
    "$((skipped_by_date + skipped_invalid_timestamp + skipped_by_pattern))" \
    > "$BAG_DIR/metadata/selection_filter.txt"
else
  printf 'result_timestamp_after: none\nresult_timestamp_before: none\ntimestamp_source: result directory name suffix YYYY-MM-DD_HH-MM-SS\ntimestamp_timezone: benchmark host local time\ndate_filter_whitelist: none\nwhitelist_scope: bypasses date filters only\nexcluded_name_regex: none\nexcluded_file_patterns: none\nfile_pattern_scope: regular-file basenames at any depth\nfilter_scope: none\nskipped_invalid_timestamps: 0\n' \
    > "$BAG_DIR/metadata/selection_filter.txt"
fi

MISSING_REPORT="$BAG_DIR/metadata/missing_required_items.txt"
{
  printf '# Base directory (shell-escaped)\tMissing required items\n'
  for ((i = 0; i < ${#missing_dirs[@]}; i++)); do
    printf '%q\t%s\n' "${missing_dirs[i]}" "${missing_requirements[i]}"
  done
} > "$MISSING_REPORT"

printf 'tar: %s\nzstd: %s\nsha256sum: %s\n' \
  "$(tar --version | head -n1)" \
  "$(zstd --version | head -n1)" \
  "$(sha256sum --version | head -n1)" > "$BAG_DIR/metadata/tool_versions.txt"
printf 'validation_python: %s\n' "$("$VALIDATION_PYTHON" --version 2>&1)" \
  >> "$BAG_DIR/metadata/tool_versions.txt"

BASE_DIR_LIST="$BAG_DIR/metadata/base_directories.txt"
: > "$BASE_DIR_LIST"
if (( TOTAL > 0 )); then
  printf '%s\n' "${base_dirs[@]##*/}" > "$BASE_DIR_LIST"
fi

printf 'raw_size_target_bytes: %s\nraw_size_target_gib: %s\ngrouping: greedy sorted base-directory order\nplanned_archives: %s\n' \
  "$TARGET_RAW_BYTES" "$TARGET_RAW_SIZE_GIB" "$PACKAGE_TOTAL" \
  > "$BAG_DIR/metadata/package_grouping.txt"

PACKAGE_MEMBERS="$BAG_DIR/metadata/package_members.tsv"
printf 'package\tbase_directory\n' > "$PACKAGE_MEMBERS"
for ((group_index = 0; group_index < PACKAGE_TOTAL; group_index++)); do
  archive_name="$(printf 'base_%06d.tar.zst' "$((group_index + 1))")"
  for ((base_index = group_starts[group_index]; base_index <= group_ends[group_index]; base_index++)); do
    printf '%s\t%s\n' "$archive_name" "${base_dirs[base_index]##*/}" >> "$PACKAGE_MEMBERS"
  done
done

PACKAGE_MANIFEST="$BAG_DIR/metadata/package_manifest.tsv"
PACKAGE_RECORD_DIR="$BAG_DIR/metadata/per_package_records"
TAR_DIGEST_DIR="$BAG_DIR/metadata/per_package_tar_stream_sha256"
printf 'package\tsha256\tarchive_bytes\tbase_directory_count\tfile_count\traw_bytes\ttar_stream_sha256\n' \
  > "$PACKAGE_MANIFEST"

package_group() {
  local index="$1" start="$2" end="$3" raw_bytes="$4"
  local base_index base_name base_rel archive_stem archive_name archive_path raw_manifest package_record
  local file_count package_sha package_bytes base_count tar_digest_file tar_stream_sha
  local base_paths=()

  for ((base_index = start; base_index <= end; base_index++)); do
    base_name="${base_dirs[base_index]##*/}"
    base_paths+=("./$base_name")
  done
  base_count=$(( end - start + 1 ))
  archive_stem="$(printf 'base_%06d' "$index")"
  archive_name="${archive_stem}.tar.zst"
  archive_path="$BAG_DIR/data/packages/$archive_name"
  raw_manifest="$BAG_DIR/metadata/per_package_file_manifests/${archive_stem}.raw-sha256.txt"
  package_record="$PACKAGE_RECORD_DIR/${archive_stem}.tsv"
  tar_digest_file="$TAR_DIGEST_DIR/${archive_stem}.sha256"

  # The leading ./ makes leading-dash base directory names safe for find,
  # tar, and sha256sum.
  (
    cd -- "$RAW_ROOT"
    {
      for base_rel in "${base_paths[@]}"; do
        if (( ${#excluded_file_predicates[@]} > 0 )); then
          find "$base_rel" -type f ! \( "${excluded_file_predicates[@]}" \) -print0
        else
          find "$base_rel" -type f -print0
        fi
      done
    } | sort -z | xargs -r -0 sha256sum --
  ) > "$raw_manifest"

  file_count="$(wc -l < "$raw_manifest" | tr -d ' ')"

  (
    cd -- "$RAW_ROOT"
    {
      for base_rel in "${base_paths[@]}"; do
        if (( ${#excluded_file_predicates[@]} > 0 )); then
          find "$base_rel" ! \( -type f \( "${excluded_file_predicates[@]}" \) \) -print0
        else
          find "$base_rel" -print0
        fi
      done
    } | sort -z \
      | tar --sort=name --xattrs --acls --null --no-recursion -T - -cf - \
      | "$VALIDATION_PYTHON" "$SHA256_PASSTHROUGH" --digest-file "$tar_digest_file" \
      | zstd -T"$ZSTD_THREADS" -"$ZSTD_LEVEL" --long="$ZSTD_LONG" -c \
        > "$archive_path"
  )

  IFS= read -r tar_stream_sha < "$tar_digest_file"
  [[ "$tar_stream_sha" =~ ^[0-9a-f]{64}$ ]] \
    || fail "Invalid tar-stream SHA-256 for package: $archive_name"

  package_sha="$(sha256sum -- "$archive_path" | awk '{print $1}')"
  package_bytes="$(stat -c '%s' -- "$archive_path")"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$archive_name" "$package_sha" "$package_bytes" "$base_count" "$file_count" "$raw_bytes" \
    "$tar_stream_sha" \
    > "$package_record"
}

worker_pids=()
worker_indices=()
worker_failed=0
wait_any_worker() {
  local completed_pid="" index="" archive_bytes="" raw_bytes="" worker_ok=0
  local position=-1 i
  local remaining_pids=() remaining_indices=()

  if (( JOBS == 1 )); then
    completed_pid="${worker_pids[0]}"
    if wait "$completed_pid"; then
      worker_ok=1
    fi
  elif wait -n -p completed_pid "${worker_pids[@]}"; then
    worker_ok=1
  fi

  for ((i = 0; i < ${#worker_pids[@]}; i++)); do
    if [[ "${worker_pids[i]}" == "$completed_pid" ]]; then
      position=$i
      index="${worker_indices[i]}"
    else
      remaining_pids+=("${worker_pids[i]}")
      remaining_indices+=("${worker_indices[i]}")
    fi
  done
  (( position >= 0 )) || fail "Could not identify the completed package worker."
  worker_pids=("${remaining_pids[@]}")
  worker_indices=("${remaining_indices[@]}")

  if (( worker_ok )); then
    IFS=$'\t' read -r _ _ archive_bytes _ _ raw_bytes _ \
      < "$PACKAGE_RECORD_DIR/$(printf 'base_%06d.tsv' "$index")"
    printf '[%s/%s] Complete: Raw size: %s; Archive size: %s\n' \
      "$index" "$PACKAGE_TOTAL" \
      "$(numfmt --to=iec-i --suffix=B "$raw_bytes")" \
      "$(numfmt --to=iec-i --suffix=B "$archive_bytes")"
  else
    echo "ERROR: Package worker failed for package_${index}." >&2
    worker_failed=1
  fi
}

for ((group_index = 0; group_index < PACKAGE_TOTAL; group_index++)); do
  start="${group_starts[group_index]}"
  end="${group_ends[group_index]}"
  raw_bytes="${group_raw_bytes[group_index]}"
  index="$((group_index + 1))"
  first_base="${base_dirs[start]##*/}"
  last_base="${base_dirs[end]##*/}"
  base_count=$(( end - start + 1 ))
  printf '[%s/%s] Packaging: %s through %s (%s base directories)\n' \
    "$index" "$PACKAGE_TOTAL" "$first_base" "$last_base" "$base_count"
  package_group "$index" "$start" "$end" "$raw_bytes" &
  worker_pids+=("$!")
  worker_indices+=("$index")

  if (( ${#worker_pids[@]} >= JOBS )); then
    wait_any_worker
    if (( worker_failed )); then
      break
    fi
  fi
done

while (( ${#worker_pids[@]} > 0 )); do
  wait_any_worker
done
if (( worker_failed )); then
  fail "One or more package workers failed; the output remains marked INCOMPLETE."
fi

for ((group_index = 0; group_index < PACKAGE_TOTAL; group_index++)); do
  archive_stem="$(printf 'base_%06d' "$((group_index + 1))")"
  IFS= read -r package_record_line < "$PACKAGE_RECORD_DIR/${archive_stem}.tsv"
  printf '%s\n' "$package_record_line" >> "$PACKAGE_MANIFEST"
done

VALIDATION_REPORT="$BAG_DIR/metadata/validation_report.tsv"
printf 'package\tarchive_sha256\texpected_tar_sha256\tactual_tar_sha256\texpected_files\tverified_files\tstatus\n' \
  > "$VALIDATION_REPORT"
printf 'Validating all package archives with CPU zstd...\n'
validate_package() {
  local group_index="$1" archive_stem archive_name package_sha package_bytes
  local base_count file_count raw_bytes tar_stream_sha validation_output
  local actual_tar_sha expected_files verified_files validation_root
  local validation_roots=() validation_args=()

  archive_stem="$(printf 'base_%06d' "$((group_index + 1))")"
  IFS=$'\t' read -r archive_name package_sha package_bytes base_count file_count raw_bytes tar_stream_sha \
    < "$PACKAGE_RECORD_DIR/${archive_stem}.tsv"
  mapfile -t validation_roots < <(
    awk -F '\t' -v wanted="$archive_name" 'NR > 1 && $1 == wanted { print $2 }' \
      "$PACKAGE_MEMBERS"
  )
  (( ${#validation_roots[@]} > 0 )) \
    || fail "No expected roots found for validation: $archive_name"
  validation_args=(
    "$PACKAGE_VALIDATOR"
    --manifest "$BAG_DIR/metadata/per_package_file_manifests/${archive_stem}.raw-sha256.txt"
    --expected-tar-sha256 "$tar_stream_sha"
  )
  for validation_root in "${validation_roots[@]}"; do
    validation_args+=(--allowed-root "$validation_root")
  done

  printf '[%s/%s] Validating: %s\n' "$((group_index + 1))" "$PACKAGE_TOTAL" "$archive_name"
  if ! validation_output="$(
    zstd -q -dc -- "$BAG_DIR/data/packages/$archive_name" \
      | "$VALIDATION_PYTHON" "${validation_args[@]}"
  )"; then
    echo "ERROR: CPU validation failed for package: $archive_name" >&2
    return 1
  fi
  IFS=$'\t' read -r actual_tar_sha expected_files verified_files <<< "$validation_output"
  [[ "$actual_tar_sha" == "$tar_stream_sha" ]] \
    || {
      echo "ERROR: Tar-stream SHA-256 mismatch for package: $archive_name" >&2
      return 1
    }
  [[ "$expected_files" == "$file_count" && "$verified_files" == "$file_count" ]] \
    || {
      echo "ERROR: Validated file count mismatch for package: $archive_name" >&2
      return 1
    }
  printf '%s\t%s\t%s\t%s\t%s\t%s\tverified\n' \
    "$archive_name" "$package_sha" "$tar_stream_sha" "$actual_tar_sha" \
    "$expected_files" "$verified_files" \
    > "$PACKAGE_RECORD_DIR/${archive_stem}.validation.tsv"
}

validation_pids=()
validation_failed=0
wait_validation_batch() {
  local pid
  for pid in "${validation_pids[@]}"; do
    wait "$pid" || validation_failed=1
  done
  validation_pids=()
}

for ((group_index = 0; group_index < PACKAGE_TOTAL; group_index++)); do
  validate_package "$group_index" &
  validation_pids+=("$!")
  if (( ${#validation_pids[@]} >= VALIDATION_JOBS )); then
    wait_validation_batch
    (( validation_failed == 0 )) || break
  fi
done
wait_validation_batch
(( validation_failed == 0 )) \
  || fail "One or more package validations failed; the output remains marked INCOMPLETE."

for ((group_index = 0; group_index < PACKAGE_TOTAL; group_index++)); do
  archive_stem="$(printf 'base_%06d' "$((group_index + 1))")"
  cat -- "$PACKAGE_RECORD_DIR/${archive_stem}.validation.tsv" >> "$VALIDATION_REPORT"
  rm -- "$PACKAGE_RECORD_DIR/${archive_stem}.validation.tsv"
done
printf 'All package archives passed CPU validation.\n'

printf 'Creating BagIt payload manifest...\n'
(
  cd -- "$BAG_DIR"
  find data -type f -print0 \
    | sort -z \
    | xargs -r -0 sha256sum --
) > "$BAG_DIR/manifest-sha256.txt"

payload_bytes="$(find "$BAG_DIR/data" -type f -printf '%s\n' | awk '{s+=$1} END{print s+0}')"
payload_files="$(find "$BAG_DIR/data" -type f | wc -l | tr -d ' ')"

cat > "$BAG_DIR/bag-info.txt" <<EOF
Bag-Software-Agent: make_yoda_dataset.sh
Bagging-Date: $DATE_UTC
External-Identifier: $DATASET_NAME
Payload-Oxum: ${payload_bytes}.${payload_files}
EOF

# READY is included in the tag manifest and must be nonempty because Zenodo
# rejects empty files. If any later validation fails, the EXIT trap creates
# INCOMPLETE as the authoritative signal not to upload this bag.
printf 'Bag creation completed successfully on %s.\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$BAG_DIR/READY"
rm -- "$BAG_DIR/INCOMPLETE"
printf 'Creating BagIt tag manifest...\n'
(
  cd -- "$BAG_DIR"
  find . -path './data' -prune -o \
    -type f ! -name 'tagmanifest-sha256.txt' \
    -printf '%P\0' \
    | sort -z \
    | xargs -r -0 sha256sum --
) > "$BAG_DIR/tagmanifest-sha256.txt"

printf 'Validating BagIt checksums...\n'
(
  cd -- "$BAG_DIR"
  if (( payload_files > 0 )); then
    sha256sum --check --strict manifest-sha256.txt >/dev/null
  fi
  sha256sum --check --strict tagmanifest-sha256.txt >/dev/null
)

printf '\nDone.\n'
printf 'Created BagIt dataset: %s\n' "$BAG_DIR"
printf 'Base directories packaged: %s\n' "$TOTAL"
printf 'Package archives created: %s\n' "$PACKAGE_TOTAL"
printf 'Payload files: %s\n' "$payload_files"
printf 'Payload bytes: %s\n' "$payload_bytes"
printf '\nVerify later with:\n'
printf '  cd "%s"\n' "$BAG_DIR"
printf '  sha256sum --check --strict manifest-sha256.txt\n'
printf '  sha256sum --check --strict tagmanifest-sha256.txt\n'
