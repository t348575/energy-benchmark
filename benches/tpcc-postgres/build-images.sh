#!/bin/bash
set -euo pipefail
docker build -t tpcc-host:latest -f tpcc-host.Dockerfile .
docker build -t tpcc-client:latest -f tpcc-client.Dockerfile .