#!/bin/sh
set -e

APP_USER="node"
APP_GROUP="node"

create_group() {
  if command -v groupadd >/dev/null 2>&1; then
    groupadd -g "$1" appgroup
  else
    addgroup -g "$1" appgroup
  fi
}

create_user() {
  if command -v useradd >/dev/null 2>&1; then
    useradd -M -u "$1" -g "$2" -s /usr/sbin/nologin appuser
  else
    adduser -D -H -u "$1" -G "$2" appuser
  fi
}

if [ -n "${PUID:-}" ] && [ -n "${PGID:-}" ]; then
  if ! getent group "${PGID}" >/dev/null 2>&1; then
    create_group "${PGID}"
    APP_GROUP="appgroup"
  else
    APP_GROUP="$(getent group "${PGID}" | cut -d: -f1)"
  fi

  if ! getent passwd "${PUID}" >/dev/null 2>&1; then
    create_user "${PUID}" "${APP_GROUP}"
    APP_USER="appuser"
  else
    APP_USER="$(getent passwd "${PUID}" | cut -d: -f1)"
  fi

  find /app/data /app/config /app/public/icons/custom ! -user "${PUID}" -exec chown "${PUID}:${PGID}" {} + 2>/dev/null || true
else
  find /app/data /app/config /app/public/icons/custom ! -user node -exec chown node:node {} + 2>/dev/null || true
fi

chown "${APP_USER}:${APP_GROUP}" /app/config/config.json 2>/dev/null || true
chmod 644 /app/config/config.json 2>/dev/null || true

if command -v gosu >/dev/null 2>&1; then
  exec gosu "${APP_USER}:${APP_GROUP}" "$@"
fi

exec su-exec "${APP_USER}:${APP_GROUP}" "$@"
