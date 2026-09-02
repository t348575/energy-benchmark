#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

(($# <= 1)) || { echo "Usage: $0 [BAG_DIR]" >&2; exit 1; }
cd -P -- "${1:-.}"
[[ -d data && -f bagit.txt && -f manifest-sha256.txt ]] ||
  { echo "ERROR: Not a Yoda dataset root: $PWD" >&2; exit 1; }


sha256sum --check --strict manifest-sha256.txt

trap 'rm -f -- "$PWD/tagmanifest-sha256.txt.new"' EXIT

find . -path './data' -prune -o \
  -type f ! -name 'tagmanifest-sha256.txt' ! -name 'tagmanifest-sha256.txt.new' \
  -printf '%P\0' | sort -z | xargs -r -0 sha256sum -- >tagmanifest-sha256.txt.new
mv -- tagmanifest-sha256.txt.new tagmanifest-sha256.txt
trap - EXIT

sha256sum --check --strict tagmanifest-sha256.txt
echo "Recreated $PWD/tagmanifest-sha256.txt"
