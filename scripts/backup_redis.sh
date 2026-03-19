#!/bin/bash
# backup_redis.sh — Trigger a background Redis RDB save and archive the dump
set -euo pipefail

REDIS_CLI="${REDIS_CLI:-redis-cli}"
DUMP_SRC="${REDIS_DUMP_DIR:-/var/lib/redis}/dump.rdb"
BACKUP_DIR="${REDIS_BACKUP_DIR:-/var/backups/redis}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
DEST="${BACKUP_DIR}/dump_${TIMESTAMP}.rdb"

mkdir -p "${BACKUP_DIR}"

echo "==> Triggering BGSAVE..."
${REDIS_CLI} BGSAVE

echo "==> Waiting for BGSAVE to complete..."
while true; do
  STATUS=$(${REDIS_CLI} LASTSAVE)
  CURRENT=$(date +%s)
  # Poll until LASTSAVE timestamp is recent (within last 30 s)
  if (( CURRENT - STATUS < 30 )); then
    break
  fi
  sleep 1
done

echo "==> Copying ${DUMP_SRC} → ${DEST}"
cp "${DUMP_SRC}" "${DEST}"

# Keep only last 7 backups
ls -t "${BACKUP_DIR}"/dump_*.rdb | tail -n +8 | xargs -r rm --

echo "==> Backup complete: ${DEST}"
