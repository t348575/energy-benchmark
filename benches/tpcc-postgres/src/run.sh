#!/bin/bash
set -euo pipefail
cd /benchhelpers/tpcc/postgres
./run_postgres.sh --config /tpcc-config.xml --hosts /hosts "$@"