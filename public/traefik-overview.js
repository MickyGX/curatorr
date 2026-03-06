(() => {
  'use strict';

  const config = window.TRAEFIK_OVERVIEW_CONFIG || {};
  const appId = String(config.appId || 'traefik').trim() || 'traefik';
  const appName = String(config.appName || 'Traefik').trim() || 'Traefik';

  const routersBody = document.getElementById(appId + 'RoutersBody');
  const servicesBody = document.getElementById(appId + 'ServicesBody');

  const hasRouters = Boolean(routersBody);
  const hasServices = Boolean(servicesBody);

  if (!hasRouters && !hasServices) return;

  const CACHE_TTL_MS = 30 * 1000;
  let cachedRouters = null;
  let cachedServices = null;
  let cacheTs = 0;

  function escapeHtml(value) {
    return String(value || '').replace(/[&<>"']/g, (char) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[char]
    ));
  }

  function renderRoutersTable(routers) {
    if (!routersBody) return;
    if (!routers.length) {
      routersBody.innerHTML = '<div class="queue-empty">No routers configured.</div>';
      return;
    }

    routersBody.innerHTML = routers.map((r) => {
      const name = escapeHtml(r.name || '\u2014');
      const status = escapeHtml(r.status || 'unknown');
      const entryPoints = escapeHtml(r.entryPoints || '\u2014');
      const service = escapeHtml(r.service || '\u2014');
      const statusClass = r.status === 'enabled' ? 'queue-status--ok' : r.status === 'disabled' ? 'queue-status--warn' : '';

      return '<div class="queue-row">' +
        '<div class="queue-col-title">' + name + '</div>' +
        '<div class="queue-col-detail"><span class="' + statusClass + '">' + status + '</span></div>' +
        '<div class="queue-col-protocol">' + entryPoints + '</div>' +
        '<div class="queue-col-time">' + service + '</div>' +
        '</div>';
    }).join('');
  }

  function renderServicesTable(services) {
    if (!servicesBody) return;
    if (!services.length) {
      servicesBody.innerHTML = '<div class="queue-empty">No services configured.</div>';
      return;
    }

    servicesBody.innerHTML = services.map((s) => {
      const name = escapeHtml(s.name || '\u2014');
      const status = escapeHtml(s.status || 'unknown');
      const type = escapeHtml(s.type || '\u2014');
      const servers = Number(s.serverCount ?? 0);
      const statusClass = s.status === 'enabled' ? 'queue-status--ok' : s.status === 'disabled' ? 'queue-status--warn' : '';

      return '<div class="queue-row">' +
        '<div class="queue-col-title">' + name + '</div>' +
        '<div class="queue-col-detail"><span class="' + statusClass + '">' + status + '</span></div>' +
        '<div class="queue-col-protocol">' + type + '</div>' +
        '<div class="queue-col-size">' + servers + '</div>' +
        '</div>';
    }).join('');
  }

  async function loadOverview() {
    const now = Date.now();
    if (cachedRouters !== null && cachedServices !== null && (now - cacheTs) < CACHE_TTL_MS) {
      renderRoutersTable(cachedRouters);
      renderServicesTable(cachedServices);
      return;
    }

    if (routersBody) routersBody.innerHTML = '<div class="queue-empty">Loading\u2026</div>';
    if (servicesBody) servicesBody.innerHTML = '<div class="queue-empty">Loading\u2026</div>';

    try {
      const response = await fetch('/api/traefik/overview', {
        headers: { Accept: 'application/json' },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        const errMsg = 'Unable to load ' + escapeHtml(appName) + ' data.';
        if (routersBody) routersBody.innerHTML = '<div class="queue-empty">' + errMsg + '</div>';
        if (servicesBody) servicesBody.innerHTML = '<div class="queue-empty">' + errMsg + '</div>';
        return;
      }
      cachedRouters = Array.isArray(payload?.routers) ? payload.routers : [];
      cachedServices = Array.isArray(payload?.services) ? payload.services : [];
      cacheTs = Date.now();
      renderRoutersTable(cachedRouters);
      renderServicesTable(cachedServices);
    } catch (_err) {
      const errMsg = 'Unable to load ' + escapeHtml(appName) + ' data.';
      if (routersBody) routersBody.innerHTML = '<div class="queue-empty">' + errMsg + '</div>';
      if (servicesBody) servicesBody.innerHTML = '<div class="queue-empty">' + errMsg + '</div>';
    }
  }

  loadOverview();

  setInterval(() => {
    cachedRouters = null;
    cachedServices = null;
    loadOverview();
  }, 30000);
})();
