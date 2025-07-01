#!/bin/bash
set -euo pipefail
mkdir -p ~/.ssh && chmod 700 ~/.ssh
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -N "" &>/dev/null
    echo "Generated SSH key in tpcc-host"
fi