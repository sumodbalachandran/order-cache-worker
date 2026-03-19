#!/bin/bash
# install_server.sh — Full bare-metal Ubuntu/Debian server setup for Order Cache Worker
set -euo pipefail

TEMPO_VERSION="2.4.1"
PROMETHEUS_VERSION="2.51.0"
APP_USER="ocw"
APP_DIR="/opt/order-cache-worker"
VENV_DIR="$APP_DIR/venv"
LOG_DIR="/var/log/order-cache-worker"
DATA_DIR="/var/lib/order-cache-worker"

echo "==> Updating system packages"
apt-get update -y
apt-get upgrade -y
apt-get install -y \
  curl wget git build-essential \
  software-properties-common \
  ufw \
  python3.11 python3.11-venv python3.11-dev python3-pip \
  redis-server \
  apt-transport-https gnupg2

echo "==> Installing Grafana OSS"
wget -q -O /usr/share/keyrings/grafana.key https://apt.grafana.com/gpg.key
echo "deb [signed-by=/usr/share/keyrings/grafana.key] https://apt.grafana.com stable main" \
  > /etc/apt/sources.list.d/grafana.list
apt-get update -y
apt-get install -y grafana

echo "==> Installing Prometheus ${PROMETHEUS_VERSION}"
cd /tmp
wget -q "https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VERSION}/prometheus-${PROMETHEUS_VERSION}.linux-amd64.tar.gz"
tar xzf "prometheus-${PROMETHEUS_VERSION}.linux-amd64.tar.gz"
cp "prometheus-${PROMETHEUS_VERSION}.linux-amd64/prometheus" /usr/local/bin/
cp "prometheus-${PROMETHEUS_VERSION}.linux-amd64/promtool" /usr/local/bin/
mkdir -p /etc/prometheus /var/lib/prometheus
cp "prometheus-${PROMETHEUS_VERSION}.linux-amd64/consoles" /etc/prometheus/ -r
cp "prometheus-${PROMETHEUS_VERSION}.linux-amd64/console_libraries" /etc/prometheus/ -r
rm -rf "prometheus-${PROMETHEUS_VERSION}.linux-amd64"*

echo "==> Installing Grafana Tempo ${TEMPO_VERSION}"
cd /tmp
wget -q "https://github.com/grafana/tempo/releases/download/v${TEMPO_VERSION}/tempo_${TEMPO_VERSION}_linux_amd64.tar.gz"
tar xzf "tempo_${TEMPO_VERSION}_linux_amd64.tar.gz"
cp tempo /usr/local/bin/tempo
mkdir -p /var/lib/tempo/wal /var/lib/tempo/blocks /etc/tempo
rm -f "tempo_${TEMPO_VERSION}_linux_amd64.tar.gz" tempo

echo "==> Creating application user: ${APP_USER}"
if ! id "${APP_USER}" &>/dev/null; then
  useradd --system --shell /bin/false --home-dir "${APP_DIR}" --create-home "${APP_USER}"
fi

echo "==> Creating directory structure"
mkdir -p "${APP_DIR}" "${LOG_DIR}" "${DATA_DIR}"
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}" "${LOG_DIR}" "${DATA_DIR}"
chown -R prometheus:prometheus /var/lib/prometheus 2>/dev/null || true
chown -R "${APP_USER}:${APP_USER}" /var/lib/tempo /etc/tempo

echo "==> Configuring Redis"
cp /opt/order-cache-worker/conf/redis.conf /etc/redis/redis.conf
systemctl enable redis-server
systemctl restart redis-server

echo "==> Configuring firewall (ufw)"
ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow ssh
ufw allow 8000/tcp   # metrics
ufw allow 8001/tcp   # health
ufw allow 3000/tcp   # grafana
ufw allow 9090/tcp   # prometheus
ufw --force enable

echo "==> Enabling Grafana"
mkdir -p /etc/grafana/provisioning/datasources
cp "${APP_DIR}/conf/grafana-datasources.yml" /etc/grafana/provisioning/datasources/
systemctl enable grafana-server
systemctl start grafana-server

echo "==> Server installation complete."
echo "    Next step: run scripts/install_app.sh"
