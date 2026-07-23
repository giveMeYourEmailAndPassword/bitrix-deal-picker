#!/bin/sh
# entrypoint.sh — подготовка APP_DATA_DIR перед запуском приложения.
#
# Railway Volume монтируется в /data (APP_DATA_DIR=/data).
# При первом запуске Volume пуст — сеем managers.json из образа.
# Runtime-файлы состояния (access_rules, claim_log, reject_log,
# greeting_log) НЕ перезаписываем: их переносят с VibeCode вручную
# (см. README, раздел «Перенос данных»).

set -e

DATA_DIR="${APP_DATA_DIR:-/data}"
mkdir -p "$DATA_DIR"

# Сеем только managers.json, если его ещё нет в Volume.
if [ ! -f "$DATA_DIR/managers.json" ] && [ -f /app/seed-managers.json ]; then
  cp /app/seed-managers.json "$DATA_DIR/managers.json"
  echo "[entrypoint] Seeded managers.json into $DATA_DIR"
fi

# Предупреждение, если runtime-файлы состояния отсутствуют —
# данные с VibeCode ещё не перенесены.
for f in access_rules.json claim_log.json reject_log.json greeting_log.json; do
  if [ ! -f "$DATA_DIR/$f" ]; then
    echo "[entrypoint] WARNING: $DATA_DIR/$f missing — migrate it from VibeCode (see README)."
  fi
done

exec "$@"
