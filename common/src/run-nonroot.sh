#!/bin/bash
set -euo pipefail
sudo -u $SUDO_USER -E env "PATH=$PATH" "LD_LIBRARY_PATH=$LD_LIBRARY_PATH" "$@"