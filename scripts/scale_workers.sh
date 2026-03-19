#!/bin/bash
# scale_workers.sh — Enable N order-cache-worker instances with offset ports
# Usage: ./scale_workers.sh <N>
set -euo pipefail

N=${1:-1}

if ! [[ "$N" =~ ^[1-9][0-9]*$ ]]; then
  echo "Usage: $0 <number_of_workers>"
  exit 1
fi

BASE_METRICS_PORT=8000
BASE_HEALTH_PORT=8001
BASE_DEBUG_PORT=8002
PORT_STRIDE=10

echo "==> Scaling to ${N} worker(s)"

for i in $(seq 1 "$N"); do
  WORKER_NAME="worker-${i}"
  METRICS_PORT=$((BASE_METRICS_PORT + (i - 1) * PORT_STRIDE))
  HEALTH_PORT=$((BASE_HEALTH_PORT + (i - 1) * PORT_STRIDE))
  DEBUG_PORT=$((BASE_DEBUG_PORT + (i - 1) * PORT_STRIDE))

  ENV_FILE="/etc/order-cache-worker/${WORKER_NAME}.env"
  mkdir -p /etc/order-cache-worker

  # Write per-worker env override
  cat > "${ENV_FILE}" <<EOF
CONSUMER_NAME=${WORKER_NAME}
METRICS_PORT=${METRICS_PORT}
HEALTH_PORT=${HEALTH_PORT}
DEBUG_PORT=${DEBUG_PORT}
EOF

  systemctl enable --now "order-cache-worker@${WORKER_NAME}.service"
  echo "    Started order-cache-worker@${WORKER_NAME}  metrics=${METRICS_PORT} health=${HEALTH_PORT}"
done

echo "==> Done. ${N} worker(s) running."
