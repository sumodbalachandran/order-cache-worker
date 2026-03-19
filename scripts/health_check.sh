#!/bin/bash
# health_check.sh — Check the readiness endpoint and return exit 0 on success
set -euo pipefail

HEALTH_PORT=${HEALTH_PORT:-8001}
URL="http://localhost:${HEALTH_PORT}/health/ready"

response=$(curl -sf -o /dev/null -w "%{http_code}" "${URL}" 2>/dev/null || echo "000")

if [ "${response}" = "200" ]; then
  echo "READY"
  exit 0
else
  echo "NOT READY (HTTP ${response})"
  exit 1
fi
