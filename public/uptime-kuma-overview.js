(() => {
  'use strict';

  const config = window.UPTIME_KUMA_OVERVIEW_CONFIG || {};
  const appName = String(config.appName || 'Uptime Kuma').trim() || 'Uptime Kuma';
  const queueVisibleRows = Number(config.queueVisibleRows) > 0 ? Number(config.queueVisibleRows) : 10;
  const showDetail = config.queueShowDetail !== false;
  const showTimeLeft = config.queueShowTimeLeft !== false;
  const showProgress = config.queueShowProgress !== false;

  const container = document.getElementById('uptimeKumaStatusContainer');
  if (!container) return;

  const CACHE_TTL_MS = 60 * 1000;
  const MAX_BARS = 50;

  let cacheTs = 0;
  let cachedGroups = null;

  function esc(v) {
    return String(v || '').replace(/[&<>"']/g, (c) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[c]
    ));
  }

  function statusLabel(s) {
    return s === 1 ? 'Up' : s === 0 ? 'Down' : s === 3 ? 'Maintenance' : 'Pending';
  }

  function badgeClass(s) {
    return s === 1 ? 'uk-badge--up' : s === 0 ? 'uk-badge--down' : s === 3 ? 'uk-badge--maintenance' : 'uk-badge--pending';
  }

  function segColor(s) {
    if (s === 1) return '#2bd56f';
    if (s === 0) return '#ff6b6b';
    if (s === 3) return '#4fb4ff';
    if (s === 2) return '#ffb446';
    return 'rgba(255,255,255,.08)';
  }

  function buildBarChart(statuses) {
    const seg = Array.isArray(statuses) && statuses.length
      ? (statuses.length > MAX_BARS ? statuses.slice(-MAX_BARS) : statuses)
      : [];
    if (!seg.length) {
      return '<div class="uk-bar-chart uk-bar-chart--empty"></div>';
    }
    const bars = seg.map((s) =>
      '<span class="uk-bar-seg" style="background:' + segColor(s) + '" title="' + statusLabel(s) + '"></span>'
    ).join('');
    return '<div class="uk-bar-chart">' + bars + '</div>';
  }

  function buildGroupHtml(group, showLabel) {
    const rows = group.monitors.map((mon) => {
      const uptime = typeof mon.uptime === 'number' && mon.uptime >= 0
        ? '<span class="uk-uptime">' + mon.uptime.toFixed(mon.uptime === 100 ? 0 : 2) + '%</span>'
        : '';
      return '<div class="queue-row">' +
        '<div class="queue-col-title">' + esc(mon.name) + '</div>' +
        '<div class="queue-col-detail"><span class="uk-badge ' + badgeClass(mon.status) + '">' + esc(statusLabel(mon.status)) + '</span></div>' +
        '<div class="queue-col-time">' + uptime + '</div>' +
        '<div class="queue-col-progress">' + buildBarChart(mon.statuses) + '</div>' +
        '</div>';
    }).join('');

    const tableCls = ['queue-table', 'queue-table--manual', 'queue-hide-subdetail', 'queue-hide-size', 'queue-hide-protocol',
      showDetail ? '' : 'queue-hide-detail',
      showTimeLeft ? '' : 'queue-hide-timeleft',
      showProgress ? '' : 'queue-hide-progress',
    ].filter(Boolean).join(' ');

    const tplParts = ['minmax(180px,1fr)'];
    if (showDetail) tplParts.push('140px');
    if (showTimeLeft) tplParts.push('110px');
    if (showProgress) tplParts.push('170px');
    const gridTpl = tplParts.join(' ');

    return '<div class="uk-queue-group">' +
      (showLabel ? '<div class="uk-queue-group-label">' + esc(group.name) + '</div>' : '') +
      '<div class="' + tableCls + '" style="--queue-visible-rows:' + queueVisibleRows + ';--queue-grid-template:' + gridTpl + '">' +
        '<div class="queue-row header">' +
          '<div class="queue-col-title">Monitor</div>' +
          '<div class="queue-col-detail">Status</div>' +
          '<div class="queue-col-time">Uptime</div>' +
          '<div class="queue-col-progress">History</div>' +
        '</div>' +
        '<div class="queue-body">' + (rows || '<div class="queue-empty">No monitors in this group.</div>') + '</div>' +
      '</div>' +
      '</div>';
  }

  function renderGroups(groups) {
    const nonEmpty = groups.filter((g) => g.monitors.length > 0);
    if (!nonEmpty.length) {
      container.innerHTML = '<div class="plex-empty">No monitors found.</div>';
      return;
    }
    const showLabel = nonEmpty.length > 1 || (nonEmpty.length === 1 && nonEmpty[0].name !== 'Monitors');
    container.innerHTML = nonEmpty.map((g) => buildGroupHtml(g, showLabel)).join('');
  }

  async function loadStatus() {
    const now = Date.now();
    if (cachedGroups && (now - cacheTs) < CACHE_TTL_MS) {
      renderGroups(cachedGroups);
      return;
    }

    container.innerHTML = '<div class="plex-empty">Loading\u2026</div>';

    try {
      const response = await fetch('/api/uptime-kuma/status', { headers: { Accept: 'application/json' } });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        container.innerHTML = '<div class="plex-empty">Unable to load ' + esc(appName) + ' data.</div>';
        return;
      }
      cachedGroups = Array.isArray(payload?.groups) ? payload.groups : [];
      cacheTs = Date.now();
      renderGroups(cachedGroups);
    } catch (_err) {
      container.innerHTML = '<div class="plex-empty">Unable to load ' + esc(appName) + ' data.</div>';
    }
  }

  loadStatus();
  setInterval(loadStatus, 60 * 1000);
})();
