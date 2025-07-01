#!/bin/bash
set -euo pipefail
mkdir -p /root/.ssh && chmod 700 /root/.ssh
cat >> /root/.ssh/authorized_keys < /tmp/id_rsa.pub
rm /tmp/id_rsa.pub