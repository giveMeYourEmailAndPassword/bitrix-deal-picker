# Keep the human-readable tag and immutable multi-platform digest together.
# Updating the base image is an explicit reviewed change exercised by CI.
FROM python:3.12.13-slim-bookworm@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b

# gosu lets the entrypoint prepare a root-owned Railway Volume and then run
# the application as an unprivileged user. passwd supplies useradd/groupadd.
RUN apt-get update \
    && apt-get install --no-install-recommends -y gosu passwd \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 bitrix \
    && useradd --system --uid 10001 --gid bitrix --home-dir /app --shell /usr/sbin/nologin bitrix

ENV HOST=0.0.0.0 \
    APP_DATA_DIR=/data \
    STATE_DB_FILENAME=state.sqlite3 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Runtime code only. Tests, secrets and local state are excluded by
# .dockerignore; state.sqlite3 and legacy JSON belong on the Volume.
COPY --chown=root:root app.py state_store.py baza_bridge.py ./
COPY --chown=root:root managers.json ./seed-managers.json
COPY --chown=root:root entrypoint.sh /usr/local/bin/bitrix-entrypoint

RUN chmod 0444 /app/app.py /app/state_store.py /app/baza_bridge.py /app/seed-managers.json \
    && chmod 0555 /usr/local/bin/bitrix-entrypoint \
    && install -d -o bitrix -g bitrix -m 0750 /data

EXPOSE 3000

# Non-Railway containers run unprivileged by default. Railway mounts Volumes as
# root, so production sets RAILWAY_RUN_UID=0: the entrypoint fixes only the
# known paths and immediately drops back to this user before Python starts.
USER bitrix:bitrix

ENTRYPOINT ["/usr/local/bin/bitrix-entrypoint"]
CMD ["python", "app.py"]
