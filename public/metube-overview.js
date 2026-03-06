(() => {
  'use strict';

  const config = window.METUBE_OVERVIEW_CONFIG || {};
  const appId = String(config.appId || 'metube').trim() || 'metube';
  const appName = String(config.appName || 'MeTube').trim() || 'MeTube';

  const queueBody = document.getElementById(appId + 'QueueBody');
  const statusFilter = document.getElementById(appId + 'QueueStatusFilter');
  const limitFilter = document.getElementById(appId + 'QueueLimitFilter');

  if (!queueBody) return;

  const CACHE_TTL_MS = 15 * 1000;
  let cachedQueue = null;
  let cachedDone = null;
  let cacheTs = 0;

  function escapeHtml(value) {
    return String(value || '').replace(/[&<>"']/g, (char) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[char]
    ));
  }

  function formatEta(seconds) {
    const s = Number(seconds);
    if (!s || !Number.isFinite(s) || s <= 0) return '\u2014';
    if (s < 60) return s + 's';
    if (s < 3600) return Math.floor(s / 60) + 'm ' + (s % 60) + 's';
    return Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm';
  }

  function statusLabel(status) {
    if (status === 'downloading') return 'Downloading';
    if (status === 'done') return 'Done';
    if (status === 'error') return 'Error';
    return 'Queued';
  }

  function statusClass(status) {
    if (status === 'downloading') return 'queue-status--ok';
    if (status === 'done') return 'queue-status--ok';
    if (status === 'error') return 'queue-status--bad';
    return '';
  }

  function renderProgress(item) {
    if (item.status === 'done') {
      return '<div class="queue-progress-wrap"><div class="queue-progress-bar" style="width:100%"></div><span class="queue-progress-label">100%</span></div>';
    }
    if (item.status !== 'downloading') {
      return '<div class="queue-progress-wrap"><div class="queue-progress-bar" style="width:0%"></div><span class="queue-progress-label">\u2014</span></div>';
    }
    const pct = Math.min(100, Math.max(0, Math.round(Number(item.percent) || 0)));
    return '<div class="queue-progress-wrap"><div class="queue-progress-bar" style="width:' + pct + '%"></div><span class="queue-progress-label">' + pct + '%</span></div>';
  }

  function applyFilters() {
    const allItems = [...(cachedQueue || []), ...(cachedDone || [])];
    if (!allItems.length) {
      queueBody.innerHTML = '<div class="queue-empty">Queue is empty.</div>';
      return;
    }

    const statusValue = String(statusFilter?.value || 'all').toLowerCase();
    const limit = Math.max(1, Number(limitFilter?.value) || 10);

    let filtered = allItems;
    if (statusValue !== 'all') {
      filtered = filtered.filter((item) => item.status === statusValue);
    }
    const page = filtered.slice(0, limit);

    if (!page.length) {
      queueBody.innerHTML = '<div class="queue-empty">No items match the current filter.</div>';
      return;
    }

    queueBody.innerHTML = page.map((item) => {
      const title = escapeHtml(item.title || item.url || 'Unknown');
      const label = escapeHtml(statusLabel(item.status));
      const sClass = statusClass(item.status);
      const eta = escapeHtml(formatEta(item.eta));

      return '<div class="queue-row">' +
        '<div class="queue-col-title">' + title + '</div>' +
        '<div class="queue-col-detail"><span class="' + sClass + '">' + label + '</span></div>' +
        '<div class="queue-col-time">' + eta + '</div>' +
        '<div class="queue-col-progress">' + renderProgress(item) + '</div>' +
        '</div>';
    }).join('');
  }

  async function loadQueue() {
    const now = Date.now();
    if (cachedQueue !== null && (now - cacheTs) < CACHE_TTL_MS) {
      applyFilters();
      return;
    }

    queueBody.innerHTML = '<div class="queue-empty">Loading\u2026</div>';

    try {
      const response = await fetch('/api/metube/queue', {
        headers: { Accept: 'application/json' },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        queueBody.innerHTML = '<div class="queue-empty">Unable to load ' + escapeHtml(appName) + ' data.</div>';
        return;
      }
      cachedQueue = Array.isArray(payload?.queue) ? payload.queue : [];
      cachedDone = Array.isArray(payload?.done) ? payload.done : [];
      cacheTs = Date.now();
      applyFilters();
    } catch (_err) {
      queueBody.innerHTML = '<div class="queue-empty">Unable to load ' + escapeHtml(appName) + ' data.</div>';
    }
  }

  statusFilter?.addEventListener('change', applyFilters);
  limitFilter?.addEventListener('change', applyFilters);

  loadQueue();

  // Auto-refresh while downloading
  setInterval(() => {
    if (cachedQueue && cachedQueue.some((item) => item.status === 'downloading')) {
      cachedQueue = null;
      loadQueue();
    }
  }, 10000);
})();
