(() => {
  'use strict';

  const config = window.GUACAMOLE_OVERVIEW_CONFIG || {};
  const appId = String(config.appId || 'guacamole').trim() || 'guacamole';
  const appName = String(config.appName || 'Guacamole').trim() || 'Guacamole';

  const activeBody = document.getElementById(appId + 'ActiveSessionsBody');
  const connectionsBody = document.getElementById(appId + 'ConnectionsBody');

  const hasActive = Boolean(activeBody);
  const hasConnections = Boolean(connectionsBody);

  if (!hasActive && !hasConnections) return;

  const CACHE_TTL_MS = 30 * 1000;
  let cachedActiveSessions = null;
  let cachedConnections = null;
  let cacheTs = 0;

  function escapeHtml(value) {
    return String(value || '').replace(/[&<>\"']/g, (char) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[char]
    ));
  }

  function formatDate(value) {
    if (!value) return '\u2014';
    const ts = typeof value === 'number' ? value : new Date(value).getTime();
    if (!Number.isFinite(ts)) return '\u2014';
    const d = new Date(ts);
    return (d.getMonth() + 1) + '/' + d.getDate() + '/' + d.getFullYear() +
      ' ' + String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0');
  }

  function renderActiveSessionsTable(sessions) {
    if (!activeBody) return;
    if (!sessions.length) {
      activeBody.innerHTML = '<div class="queue-empty">No active sessions.</div>';
      return;
    }

    activeBody.innerHTML = sessions.map((s) => {
      const name = escapeHtml(s.connectionName || s.identifier || '\u2014');
      const protocol = escapeHtml(s.protocol ? s.protocol.toUpperCase() : '\u2014');
      const user = escapeHtml(s.username || '\u2014');
      const started = escapeHtml(formatDate(s.startDate));

      return '<div class="queue-row">' +
        '<div class="queue-col-title">' + name + '</div>' +
        '<div class="queue-col-detail">' + user + '</div>' +
        '<div class="queue-col-protocol">' + protocol + '</div>' +
        '<div class="queue-col-time">' + started + '</div>' +
        '</div>';
    }).join('');
  }

  function renderConnectionsTable(connections) {
    if (!connectionsBody) return;
    if (!connections.length) {
      connectionsBody.innerHTML = '<div class="queue-empty">No connections configured.</div>';
      return;
    }

    connectionsBody.innerHTML = connections.map((c) => {
      const name = escapeHtml(c.name || c.identifier || '\u2014');
      const protocol = escapeHtml(c.protocol ? c.protocol.toUpperCase() : '\u2014');
      const active = Number(c.activeConnections ?? 0);
      const activeClass = active > 0 ? 'queue-status--ok' : '';

      return '<div class="queue-row">' +
        '<div class="queue-col-title">' + name + '</div>' +
        '<div class="queue-col-detail">' + protocol + '</div>' +
        '<div class="queue-col-size"><span class="' + activeClass + '">' + active + '</span></div>' +
        '</div>';
    }).join('');
  }

  async function loadOverview() {
    const now = Date.now();
    if (cachedActiveSessions !== null && cachedConnections !== null && (now - cacheTs) < CACHE_TTL_MS) {
      renderActiveSessionsTable(cachedActiveSessions);
      renderConnectionsTable(cachedConnections);
      return;
    }

    if (activeBody) activeBody.innerHTML = '<div class="queue-empty">Loading\u2026</div>';
    if (connectionsBody) connectionsBody.innerHTML = '<div class="queue-empty">Loading\u2026</div>';

    try {
      const response = await fetch('/api/guacamole/overview', {
        headers: { Accept: 'application/json' },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        const errMsg = 'Unable to load ' + escapeHtml(appName) + ' data.';
        if (activeBody) activeBody.innerHTML = '<div class="queue-empty">' + errMsg + '</div>';
        if (connectionsBody) connectionsBody.innerHTML = '<div class="queue-empty">' + errMsg + '</div>';
        return;
      }
      cachedActiveSessions = Array.isArray(payload?.activeSessions) ? payload.activeSessions : [];
      cachedConnections = Array.isArray(payload?.connections) ? payload.connections : [];
      cacheTs = Date.now();
      renderActiveSessionsTable(cachedActiveSessions);
      renderConnectionsTable(cachedConnections);
    } catch (_err) {
      const errMsg = 'Unable to load ' + escapeHtml(appName) + ' data.';
      if (activeBody) activeBody.innerHTML = '<div class="queue-empty">' + errMsg + '</div>';
      if (connectionsBody) connectionsBody.innerHTML = '<div class="queue-empty">' + errMsg + '</div>';
    }
  }

  loadOverview();

  // Auto-refresh every 30s to catch new sessions
  setInterval(() => {
    cachedActiveSessions = null;
    cachedConnections = null;
    loadOverview();
  }, 30000);
})();
