(() => {
  'use strict';

  const config = window.TDARR_OVERVIEW_CONFIG || {};
  const appName = String(config.appName || 'Tdarr').trim() || 'Tdarr';

  const container = document.getElementById('tdarrStatsContainer');
  if (!container) return;

  const CACHE_TTL_MS = 30 * 1000;
  let cacheTs = 0;

  function escapeHtml(value) {
    return String(value || '').replace(/[&<>"']/g, (char) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[char]
    ));
  }

  function renderStats(data) {
    const stats = [
      { label: 'Queue',       value: String(data.queue     ?? '\u2014'), accent: false },
      { label: 'Processed',   value: String(data.processed ?? '\u2014'), accent: false },
      { label: 'Errored',     value: String(data.errored   ?? '\u2014'), accent: data.errored > 0 },
      { label: 'Space Saved', value: (data.savedGb ?? '0.00') + ' GB', accent: false },
    ];

    container.innerHTML = stats.map((stat) =>
      '<div class="dashboard-stat-card">' +
        '<div class="dashboard-stat-label">' + escapeHtml(stat.label) + '</div>' +
        '<div class="dashboard-stat-value' + (stat.accent ? ' dashboard-stat-value--warn' : '') + '">' + escapeHtml(stat.value) + '</div>' +
      '</div>'
    ).join('');
  }

  async function loadStats() {
    const now = Date.now();
    if (cacheTs && (now - cacheTs) < CACHE_TTL_MS) return;

    container.innerHTML = '<div class="plex-empty">Loading\u2026</div>';

    try {
      const response = await fetch('/api/tdarr/stats', {
        headers: { Accept: 'application/json' },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        container.innerHTML = '<div class="plex-empty">Unable to load ' + escapeHtml(appName) + ' data.</div>';
        return;
      }
      cacheTs = Date.now();
      renderStats(payload);
    } catch (_err) {
      container.innerHTML = '<div class="plex-empty">Unable to load ' + escapeHtml(appName) + ' data.</div>';
    }
  }

  loadStats();

  // Refresh stats periodically
  setInterval(loadStats, 60 * 1000);
})();
