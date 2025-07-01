#!/bin/bash
set -euo pipefail
for i in $(seq 1 "$1"); do
    ssh-keyscan -H tpcc-$i >> /root/.ssh/known_hosts
done
chmod 644 /root/.ssh/known_hosts