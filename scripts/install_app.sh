#!/bin/bash
# install_app.sh — Deploy / update the Order Cache Worker application
set -euo pipefail

APP_USER="ocw"
APP_DIR="/opt/order-cache-worker"
VENV_DIR="$APP_DIR/venv"
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> Syncing application files to ${APP_DIR}"
rsync -av --exclude='.git' --exclude='venv' --exclude='__pycache__' \
  "${SOURCE_DIR}/" "${APP_DIR}/"

echo "==> Creating/updating Python virtual environment"
python3.11 -m venv "${VENV_DIR}"
"${VENV_DIR}/bin/pip" install --upgrade pip wheel
"${VENV_DIR}/bin/pip" install -r "${APP_DIR}/requirements.txt"

echo "==> Installing configuration files"
cp "${APP_DIR}/conf/prometheus.yml" /etc/prometheus/prometheus.yml
cp "${APP_DIR}/conf/tempo.yaml" /etc/tempo/tempo.yaml

echo "==> Installing systemd units"
cp "${APP_DIR}/systemd/order-cache-worker@.service" /etc/systemd/system/
cp "${APP_DIR}/systemd/prometheus.service" /etc/systemd/system/
cp "${APP_DIR}/systemd/tempo.service" /etc/systemd/system/
systemctl daemon-reload

echo "==> Enabling and starting services"
systemctl enable --now prometheus.service
systemctl enable --now tempo.service
systemctl enable --now "order-cache-worker@worker-1.service"

echo "==> Setting ownership"
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}"

echo "==> Waiting for health endpoint..."
for i in {1..15}; do
  if curl -sf http://localhost:8001/health >/dev/null 2>&1; then
    echo "    Health endpoint OK"
    break
  fi
  echo "    Attempt ${i}/15 — waiting 2s"
  sleep 2
done

echo "==> Application deployment complete."
