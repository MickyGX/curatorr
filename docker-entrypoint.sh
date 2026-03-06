#!/bin/sh
set -e

APP_USER="node"
APP_GROUP="node"

if [ -n "${PUID:-}" ] && [ -n "${PGID:-}" ]; then
  if ! getent group "${PGID}" >/dev/null 2>&1; then
    addgroup -g "${PGID}" appgroup
    APP_GROUP="appgroup"
  else
    APP_GROUP="$(getent group "${PGID}" | cut -d: -f1)"
  fi

  if ! getent passwd "${PUID}" >/dev/null 2>&1; then
    adduser -D -H -u "${PUID}" -G "${APP_GROUP}" appuser
    APP_USER="appuser"
  else
    APP_USER="$(getent passwd "${PUID}" | cut -d: -f1)"
  fi

  chown -R "${PUID}:${PGID}" /app/data /app/config /app/public/icons/custom 2>/dev/null || true
else
  chown -R node:node /app/data /app/config /app/public/icons/custom 2>/dev/null || true
fi

if [ -f /app/default-apps.json ] && [ ! -f /app/config/default-apps.json ]; then
  cp /app/default-apps.json /app/config/default-apps.json 2>/dev/null || true
fi
if [ -f /app/default-categories.json ] && [ ! -f /app/config/default-categories.json ]; then
  cp /app/default-categories.json /app/config/default-categories.json 2>/dev/null || true
fi
if [ -f /app/config.example.json ] && [ ! -f /app/config/config.json ]; then
  cp /app/config.example.json /app/config/config.json 2>/dev/null || true
fi

chown "${APP_USER}:${APP_GROUP}" /app/default-apps.json /app/default-categories.json 2>/dev/null || true
chown "${APP_USER}:${APP_GROUP}" /app/config/default-apps.json /app/config/default-categories.json 2>/dev/null || true
chown "${APP_USER}:${APP_GROUP}" /app/config/config.json 2>/dev/null || true

chmod 644 /app/default-apps.json /app/default-categories.json 2>/dev/null || true
chmod 644 /app/config/default-apps.json /app/config/default-categories.json 2>/dev/null || true
chmod 644 /app/config/config.json 2>/dev/null || true

exec su-exec "${APP_USER}:${APP_GROUP}" "$@"
