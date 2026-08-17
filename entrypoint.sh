#!/bin/sh
# Prepare the Railway Volume, seed a safe managers placeholder once, then run
# the application as UID/GID 10001. SQLite migration is handled by
# state_store.py so it can be transactional and reported through readiness.

set -eu
umask 027

DATA_DIR="${APP_DATA_DIR:-/data}"
DB_FILENAME="${STATE_DB_FILENAME:-state.sqlite3}"

case "$DATA_DIR" in
  ""|/|/app|/bin|/etc|/usr|/var)
    echo "[entrypoint] Refusing unsafe APP_DATA_DIR: $DATA_DIR" >&2
    exit 1
    ;;
esac

case "$DB_FILENAME" in
  ""|.|..|*/*)
    echo "[entrypoint] STATE_DB_FILENAME must be a filename, not a path" >&2
    exit 1
    ;;
  managers.json|access_rules.json|claim_log.json|reject_log.json|greeting_log.json)
    echo "[entrypoint] STATE_DB_FILENAME collides with another state file" >&2
    exit 1
    ;;
  *[!A-Za-z0-9._-]*)
    echo "[entrypoint] STATE_DB_FILENAME contains unsupported characters" >&2
    exit 1
    ;;
esac

export APP_DATA_DIR="$DATA_DIR"
export STATE_DB_FILENAME="$DB_FILENAME"

validate_data_paths() {
  if [ -L "$DATA_DIR" ]; then
    echo "[entrypoint] Refusing symlink APP_DATA_DIR: $DATA_DIR" >&2
    exit 1
  fi
  for file in \
    managers.json \
    "$DB_FILENAME" "$DB_FILENAME-journal" "$DB_FILENAME-wal" "$DB_FILENAME-shm" \
    access_rules.json claim_log.json reject_log.json greeting_log.json
  do
    path="$DATA_DIR/$file"
    if [ -L "$path" ]; then
      echo "[entrypoint] Refusing symlink in APP_DATA_DIR: $file" >&2
      exit 1
    fi
    if [ -e "$path" ] && [ ! -f "$path" ]; then
      echo "[entrypoint] Refusing non-file in APP_DATA_DIR: $file" >&2
      exit 1
    fi
  done
}

seed_managers() {
  if [ ! -e "$DATA_DIR/managers.json" ] && [ -f /app/seed-managers.json ]; then
    cp /app/seed-managers.json "$DATA_DIR/managers.json"
    chmod 0640 "$DATA_DIR/managers.json"
    echo "[entrypoint] Seeded disabled managers.json placeholder into $DATA_DIR"
  fi
}

if [ "$(id -u)" = "0" ]; then
  case "$DATA_DIR" in
    /data) ;;
    *)
      echo "[entrypoint] Root bootstrap requires APP_DATA_DIR=/data exactly" >&2
      exit 1
      ;;
  esac
  validate_data_paths
  mkdir -p "$DATA_DIR"
  validate_data_paths
  chown bitrix:bitrix "$DATA_DIR"
  chmod 0750 "$DATA_DIR"

  # Files copied into the Volume through an administrative shell can be
  # root-owned. Limit ownership repair to the exact files the app may read or
  # write; never recurse through an operator's backup directory.
  for file in \
    managers.json \
    "$DB_FILENAME" "$DB_FILENAME-journal" "$DB_FILENAME-wal" "$DB_FILENAME-shm" \
    access_rules.json claim_log.json reject_log.json greeting_log.json
  do
    if [ -e "$DATA_DIR/$file" ]; then
      chown bitrix:bitrix "$DATA_DIR/$file"
      chmod 0640 "$DATA_DIR/$file"
    fi
  done

  seed_managers
  if [ -e "$DATA_DIR/managers.json" ]; then
    chown bitrix:bitrix "$DATA_DIR/managers.json"
  fi
  echo "[entrypoint] Starting application as uid=10001 gid=10001"
  exec gosu bitrix:bitrix "$@"
fi

mkdir -p "$DATA_DIR"
validate_data_paths
if [ ! -w "$DATA_DIR" ]; then
  echo "[entrypoint] APP_DATA_DIR is not writable by uid $(id -u): $DATA_DIR" >&2
  exit 1
fi
seed_managers
exec "$@"
