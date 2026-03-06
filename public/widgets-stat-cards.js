(function () {
  'use strict';

  const STATUS_CLASS = { up: 'is-up', down: 'is-down', unknown: 'is-unknown' };
  const STATUS_LABEL = { up: 'Online', down: 'Offline', unknown: 'Unknown' };
  const safeStorage = {
    get(key) {
      try { return localStorage.getItem(key); } catch (_err) { return null; }
    },
    set(key, value) {
      try { localStorage.setItem(key, value); } catch (_err) { /* ignore */ }
    },
  };
  const collapseKey = (barId) => 'widgetBarCollapsed:' + String(barId || '').trim().toLowerCase();
  function normalizeMetricKey(value) {
    return String(value || '').trim().toLowerCase();
  }
  function getSelectedMetricKeySet(cardEl) {
    if (!cardEl || !cardEl.hasAttribute('data-has-selected-metric-keys')) return null;
    var raw = String(cardEl.getAttribute('data-selected-metric-keys') || '');
    if (!raw.trim()) return new Set();
    return new Set(raw.split(',').map(normalizeMetricKey).filter(Boolean));
  }

  function getSelectedLibraryKeys(cardEl) {
    var raw = cardEl && cardEl.getAttribute && cardEl.getAttribute('data-selected-library-keys');
    if (!raw) return null;
    try { var p = JSON.parse(raw); return (p && typeof p === 'object' && !Array.isArray(p)) ? p : null; } catch (_e) { return null; }
  }

  function applyLibraryFilter(metrics, libraryInfo, selectedLibraryKeys) {
    if (!Array.isArray(libraryInfo) || !libraryInfo.length || !selectedLibraryKeys) return metrics;
    return metrics.map(function (m) {
      var metricKey = normalizeMetricKey(m && m.key);
      var libFilter = selectedLibraryKeys[metricKey];
      if (!Array.isArray(libFilter) || !libFilter.length) return m;
      var libFilterSet = new Set(libFilter.map(function (k) { return String(k || '').trim(); }));
      var filteredTotal = libraryInfo
        .filter(function (lib) { return normalizeMetricKey(lib && lib.metricKey) === metricKey && libFilterSet.has(String(lib.key || '').trim()); })
        .reduce(function (sum, lib) { return sum + (Number(lib.count) || 0); }, 0);
      return Object.assign({}, m, { value: filteredTotal });
    });
  }

  function applyBarCollapsedState(barEl, collapsed) {
    if (!barEl) return;
    const nextCollapsed = Boolean(collapsed);
    barEl.classList.toggle('is-collapsed', nextCollapsed);
    var btn = barEl.querySelector('[data-widget-bar-collapse-toggle]');
    if (btn) {
      btn.setAttribute('title', nextCollapsed ? 'Expand' : 'Collapse');
      btn.setAttribute('aria-label', nextCollapsed ? 'Expand' : 'Collapse');
      btn.setAttribute('aria-expanded', nextCollapsed ? 'false' : 'true');
    }
  }

  function bindBarControls(barEl) {
    var barId = String(barEl && barEl.dataset && barEl.dataset.widgetBarId || '').trim();
    if (!barId) return;
    applyBarCollapsedState(barEl, safeStorage.get(collapseKey(barId)) === '1');
    var scrollRows = Array.from(barEl.querySelectorAll('.dashboard-widget-bar-row.row--scroll'));
    var prevBtn = barEl.querySelector('[data-widget-bar-scroll-prev]');
    var nextBtn = barEl.querySelector('[data-widget-bar-scroll-next]');
    if (prevBtn) prevBtn.disabled = scrollRows.length === 0;
    if (nextBtn) nextBtn.disabled = scrollRows.length === 0;

    function openWidgetCard(cardEl) {
      if (!cardEl) return false;
      var appId = String(cardEl.dataset && cardEl.dataset.appId || '').trim();
      if (!appId) return false;
      var launchMode = String(cardEl.dataset && cardEl.dataset.launchMode || '').trim().toLowerCase();
      if (launchMode === 'disabled') return false;
      var href = String(cardEl.dataset && cardEl.dataset.launchHref || '').trim();
      if (!href) href = '/apps/' + encodeURIComponent(appId) + '/launch';
      if (!href) return false;
      if (launchMode === 'new-tab') {
        window.open(href, '_blank', 'noopener,noreferrer');
      } else {
        window.location.href = href;
      }
      return true;
    }

    barEl.addEventListener('click', function (e) {
      var prev = e.target.closest('[data-widget-bar-scroll-prev]');
      if (prev && barEl.contains(prev)) {
        e.preventDefault();
        if (prev.disabled) return;
        scrollRows.forEach(function (row) {
          var amount = Math.max(160, Math.round(row.clientWidth * 0.85));
          row.scrollBy({ left: -amount, behavior: 'smooth' });
        });
        return;
      }

      var next = e.target.closest('[data-widget-bar-scroll-next]');
      if (next && barEl.contains(next)) {
        e.preventDefault();
        if (next.disabled) return;
        scrollRows.forEach(function (row) {
          var amount = Math.max(160, Math.round(row.clientWidth * 0.85));
          row.scrollBy({ left: amount, behavior: 'smooth' });
        });
        return;
      }

      var collapseBtn = e.target.closest('[data-widget-bar-collapse-toggle]');
      if (collapseBtn && barEl.contains(collapseBtn)) {
        e.preventDefault();
        var collapsed = !barEl.classList.contains('is-collapsed');
        applyBarCollapsedState(barEl, collapsed);
        safeStorage.set(collapseKey(barId), collapsed ? '1' : '0');
        return;
      }
      var card = e.target.closest('.dashboard-stat-card[data-app-id]');
      if (card && barEl.contains(card)) {
        if (e.target.closest('a,button,input,select,textarea,label')) return;
        if (openWidgetCard(card)) {
          e.preventDefault();
          return;
        }
      }
    });

    barEl.addEventListener('keydown', function (e) {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      var card = e.target.closest('.dashboard-stat-card[data-app-id][tabindex]');
      if (!card || !barEl.contains(card)) return;
      if (!openWidgetCard(card)) return;
      e.preventDefault();
    });

    // Bind search forms within this bar (including dynamically built ones)
    function bindSearchForms() {
      barEl.querySelectorAll('.dashboard-system-search').forEach(function (form) {
        if (form._searchBound) return;
        form._searchBound = true;
        form.addEventListener('submit', function (e) {
          e.preventDefault();
          handleSearchSubmit(form);
        });
      });
    }
    bindSearchForms();
    barEl._bindSearchForms = bindSearchForms;
  }

  // ─── Search widget ────────────────────────────────────────────────────────

  var SEARCH_PROVIDER_URLS = {
    duckduckgo: 'https://duckduckgo.com/?q={query}',
    google:     'https://www.google.com/search?q={query}',
    bing:       'https://www.bing.com/search?q={query}',
    brave:      'https://search.brave.com/search?q={query}',
  };

  function handleSearchSubmit(form) {
    var input = form.querySelector('.system-search-input');
    if (!input) return;
    var query = String(input.value || '').trim();
    if (!query) return;
    var provider = String(form.dataset.searchProvider || 'duckduckgo').trim().toLowerCase();
    var target = form.dataset.searchTarget === '_self' ? '_self' : '_blank';
    var url;
    if (provider === 'searxng') {
      var baseUrl = String(form.dataset.searchBaseUrl || '').trim().replace(/\/$/, '');
      if (!baseUrl) return;
      url = baseUrl + '/search?q=' + encodeURIComponent(query);
    } else {
      var template = SEARCH_PROVIDER_URLS[provider] || SEARCH_PROVIDER_URLS.duckduckgo;
      url = template.replace('{query}', encodeURIComponent(query));
    }
    if (target === '_self') {
      window.location.href = url;
    } else {
      window.open(url, '_blank', 'noopener,noreferrer');
    }
    input.value = '';
  }

  // ─── App stat cards ───────────────────────────────────────────────────────

  async function fetchStats(appId) {
    const resp = await fetch('/api/widget-stats/' + encodeURIComponent(appId), {
      credentials: 'same-origin',
    });
    if (!resp.ok) {
      let errMsg = 'Request failed';
      try { const d = await resp.json(); errMsg = d.error || errMsg; } catch (_e) { /* ignore */ }
      throw new Error(errMsg);
    }
    return resp.json();
  }

  function renderCard(cardEl, data) {
    const statusEl = cardEl.querySelector('.stat-card-status');
    const metricsEl = cardEl.querySelector('.stat-card-metrics');
    if (!statusEl || !metricsEl) return;

    // Status badge
    const statusKey = (data && (data.status === 'up' || data.status === 'down')) ? data.status : 'unknown';
    Object.values(STATUS_CLASS).forEach((cls) => statusEl.classList.remove(cls));
    statusEl.classList.add(STATUS_CLASS[statusKey]);
    statusEl.textContent = STATUS_LABEL[statusKey] || 'Unknown';

    // Metrics
    var selectedMetricKeySet = getSelectedMetricKeySet(cardEl);
    var selectedLibraryKeys = getSelectedLibraryKeys(cardEl);
    var libraryInfo = Array.isArray(data && data.libraryInfo) ? data.libraryInfo : null;
    var allMetrics = (Array.isArray(data && data.metrics) ? data.metrics : []);
    if (libraryInfo && selectedLibraryKeys) allMetrics = applyLibraryFilter(allMetrics, libraryInfo, selectedLibraryKeys);
    const metrics = allMetrics
      .filter(function (m) { return !selectedMetricKeySet || selectedMetricKeySet.has(normalizeMetricKey(m && m.key)); });
    if (!cardEl.hasAttribute('data-widget-metric-cols')) {
      var autoMetricCols = Math.max(1, Math.min(4, Math.round(Number((metrics && metrics.length) || (allMetrics && allMetrics.length) || 2))));
      cardEl.setAttribute('data-widget-metric-cols', String(autoMetricCols));
    }
    if (metrics.length) {
      metricsEl.innerHTML = metrics.map(function (m) {
        const val = m.value !== undefined && m.value !== null ? String(m.value) : '—';
        return (
          '<span class="stat-card-metric">' +
          '<span class="stat-card-metric-value">' + escapeHtml(val) + '</span>' +
          '<span class="stat-card-metric-label">' + escapeHtml(String(m.label || m.key || '')) + '</span>' +
          '</span>'
        );
      }).join('');
    } else {
      metricsEl.innerHTML = '';
    }

    cardEl.classList.remove('is-loading');
  }

  function renderCardError(cardEl) {
    const statusEl = cardEl.querySelector('.stat-card-status');
    const metricsEl = cardEl.querySelector('.stat-card-metrics');
    if (statusEl) {
      Object.values(STATUS_CLASS).forEach((cls) => statusEl.classList.remove(cls));
      statusEl.classList.add(STATUS_CLASS.down);
      statusEl.textContent = STATUS_LABEL.down;
    }
    if (metricsEl) metricsEl.innerHTML = '';
    cardEl.classList.remove('is-loading');
  }

  // ─── System Info widget (sys-resources) ───────────────────────────────────

  var WEATHER_ICONS = {
    'clear':         '/icons/weather-clear.svg',
    'mainly-clear':  '/icons/weather-mainly-clear.svg',
    'partly-cloudy': '/icons/weather-partly-cloudy.svg',
    'overcast':      '/icons/weather-overcast.svg',
    'foggy':         '/icons/weather-foggy.svg',
    'drizzle':       '/icons/weather-drizzle.svg',
    'rainy':         '/icons/weather-rainy.svg',
    'snowy':         '/icons/weather-snowy.svg',
    'showers':       '/icons/weather-showers.svg',
    'stormy':        '/icons/weather-stormy.svg',
  };
  var WEATHER_ICON_FALLBACK = '/icons/weather.svg';
  var SEARCH_SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>';

  function formatBytes(bytes) {
    if (!Number.isFinite(bytes) || bytes < 0) return '—';
    var units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    var i = 0;
    var val = bytes;
    while (val >= 1024 && i < units.length - 1) { val /= 1024; i++; }
    return (i === 0 ? val.toFixed(0) : val.toFixed(2).replace(/\.?0+$/, '')) + ' ' + units[i];
  }
  function normalizeSystemPath(value) {
    var pathValue = String(value || '').trim() || '/';
    if (!pathValue.startsWith('/')) pathValue = '/' + pathValue;
    pathValue = pathValue.replace(/\/{2,}/g, '/');
    if (pathValue.length > 1) pathValue = pathValue.replace(/\/+$/, '');
    return pathValue || '/';
  }
  function decodeHtmlEntities(value) {
    var source = String(value || '');
    if (!source || source.indexOf('&') === -1 || typeof document === 'undefined') return source;
    var textarea = document.createElement('textarea');
    textarea.innerHTML = source;
    return textarea.value;
  }
  function parseSystemConfigAttr(value) {
    var source = String(value || '').trim() || '{}';
    try { return JSON.parse(source); } catch (_e) { /* fall through */ }
    var decoded = decodeHtmlEntities(source);
    if (decoded !== source) {
      try { return JSON.parse(decoded); } catch (_e2) { /* fall through */ }
    }
    return {};
  }
  function normalizeLinksUrl(value) {
    var input = String(value || '').trim();
    if (!input) return '';
    var candidate = input;
    if (!/^[a-z][a-z0-9+.-]*:/i.test(candidate)) candidate = 'https://' + candidate;
    try {
      var parsed = new URL(candidate);
      if (!parsed.hostname) return '';
      if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return '';
      return parsed.toString();
    } catch (_e) {
      return '';
    }
  }
  function linkInitials(name, fallback) {
    var source = String(name || '').trim() || String(fallback || '').trim();
    if (!source) return '??';
    var tokens = source
      .replace(/[_\-.]+/g, ' ')
      .split(/\s+/)
      .map(function (token) { return token.trim(); })
      .filter(Boolean);
    if (tokens.length >= 2) return ((tokens[0][0] || '') + (tokens[1][0] || '')).toUpperCase();
    var clean = source.replace(/[^a-z0-9]/gi, '');
    return (clean.slice(0, 2) || source.slice(0, 2)).toUpperCase();
  }
  function bindLinksWidgetFaviconFallback(rootEl) {
    if (!rootEl) return;
    rootEl.querySelectorAll('img[data-link-favicon]').forEach(function (img) {
      if (img._linksFaviconBound) return;
      img._linksFaviconBound = true;
      img.addEventListener('error', function () {
        var fallbackSrc = String(img.dataset.fallbackSrc || '').trim();
        var triedFallback = img.dataset.fallbackTried === '1';
        if (fallbackSrc && !triedFallback && img.src !== fallbackSrc) {
          img.dataset.fallbackTried = '1';
          img.src = fallbackSrc;
          return;
        }
        img.style.display = 'none';
        var initialsEl = img.closest('.dashboard-links-avatar') && img.closest('.dashboard-links-avatar').querySelector('.dashboard-links-initials');
        if (initialsEl) initialsEl.hidden = false;
      });
    });
  }
  function buildLinksWidget(containerEl) {
    var raw = containerEl.getAttribute('data-system-config') || '{}';
    var cfg = parseSystemConfigAttr(raw);
    var title = String(cfg.title || cfg.name || 'Links').trim() || 'Links';
    var showUrl = cfg.showUrl !== false;
    var rows = (Array.isArray(cfg.links) ? cfg.links : [])
      .map(function (row) {
        var rowObj = (row && typeof row === 'object') ? row : {};
        var url = normalizeLinksUrl(rowObj.url || rowObj.href || '');
        if (!url) return null;
        var hostname = '';
        var origin = '';
        try {
          var parsed = new URL(url);
          hostname = String(parsed.hostname || '').trim();
          origin = String(parsed.origin || '').trim();
        } catch (_e) { /* ignore */ }
        var name = String(rowObj.name || rowObj.title || '').trim() || hostname || url;
        var favicon = origin ? origin.replace(/\/$/, '') + '/favicon.ico' : '';
        var fallback = hostname ? 'https://www.google.com/s2/favicons?domain=' + encodeURIComponent(hostname) + '&sz=64' : '';
        return { name: name, url: url, hostname: hostname, favicon: favicon, fallback: fallback };
      })
      .filter(Boolean)
      .slice(0, 250);
    var rowsHtml = rows.map(function (row) {
      var initials = linkInitials(row.name, row.hostname);
      var urlHtml = showUrl ? '<span class="dashboard-links-host">' + escapeHtml(row.hostname || row.url) + '</span>' : '';
      return '<a class="dashboard-links-row" href="' + escapeHtml(row.url) + '" target="_blank" rel="noopener noreferrer">' +
        '<span class="dashboard-links-avatar">' +
          (row.favicon ? '<img data-link-favicon src="' + escapeHtml(row.favicon) + '" data-fallback-src="' + escapeHtml(row.fallback) + '" alt="" />' : '') +
          '<span class="dashboard-links-initials"' + (row.favicon ? ' hidden' : '') + '>' + escapeHtml(initials) + '</span>' +
        '</span>' +
        '<span class="dashboard-links-name">' + escapeHtml(row.name) + '</span>' +
        urlHtml +
      '</a>';
    }).join('');
    containerEl.innerHTML = '<div class="dashboard-links-title">' + escapeHtml(title) + '</div>' +
      '<div class="dashboard-links-list">' + (rowsHtml || '<div class="dashboard-links-empty">No links configured yet</div>') + '</div>';
    bindLinksWidgetFaviconFallback(containerEl);
  }

  // Build the child elements of a sys-resources container from its config
  function buildSystemBar(containerEl) {
    var raw = containerEl.getAttribute('data-system-config') || '{}';
    var cfg = parseSystemConfigAttr(raw);
    var showTotalSpace = cfg.showTotalSpace === true;
    containerEl.setAttribute('data-system-show-total-space', showTotalSpace ? '1' : '0');

    var html = '';

    if (cfg.cpu !== false) {
      html += '<div class="dashboard-system-chip is-loading" data-chip-type="cpu">' +
        '<div class="system-chip-row"><img class="system-chip-icon" src="/icons/cpu.svg" alt="" onerror="this.src=\'/icons/app.svg\'" />' +
        '<div class="system-chip-body"><span class="system-chip-value">–</span><span class="system-chip-label">CPU</span></div></div>' +
        '<div class="system-chip-bar"><div class="system-chip-bar-fill"></div></div></div>';
    }

    if (cfg.memory !== false) {
      html += '<div class="dashboard-system-chip is-loading" data-chip-type="memory">' +
        '<div class="system-chip-row"><img class="system-chip-icon" src="/icons/memory.svg" alt="" onerror="this.src=\'/icons/app.svg\'" />' +
        '<div class="system-chip-body"><span class="system-chip-value">–</span><span class="system-chip-label">' + (showTotalSpace ? 'Total' : 'Free') + '</span></div></div>' +
        '<div class="system-chip-bar"><div class="system-chip-bar-fill"></div></div></div>';
    }

    var disks = Array.isArray(cfg.disks) ? cfg.disks : [];
    disks.forEach(function (d) {
      var path = normalizeSystemPath((d && d.path) || '/');
      var label = String((d && d.label) || '').trim() || path;
      html += '<div class="dashboard-system-chip is-loading" data-chip-type="disk" data-chip-path="' + escapeHtml(path) + '" title="' + escapeHtml(label + ' (' + path + ')') + '">' +
        '<div class="system-chip-row"><img class="system-chip-icon" src="/icons/disk.svg" alt="" onerror="this.src=\'/icons/app.svg\'" />' +
        '<div class="system-chip-body"><span class="system-chip-value">–</span><span class="system-chip-label">' + escapeHtml(label) + '</span></div></div>' +
        '<div class="system-chip-bar"><div class="system-chip-bar-fill"></div></div></div>';
    });

    var search = cfg.search || {};
    if (search.enabled) {
      var baseUrl = String(search.baseUrl || '').trim().replace(/\/$/, '');
      html += '<form class="dashboard-system-search" data-search-provider="' + escapeHtml(search.provider || 'duckduckgo') + '"' +
        ' data-search-target="' + (search.target === '_self' ? '_self' : '_blank') + '"' +
        (search.provider === 'searxng' && baseUrl ? ' data-search-base-url="' + escapeHtml(baseUrl) + '"' : '') +
        ' action="" method="get" autocomplete="off" novalidate>' +
        '<input class="system-search-input" type="search" name="q" placeholder="Search…" aria-label="Search" />' +
        '<button class="system-search-btn" type="submit" aria-label="Search">' + SEARCH_SVG + '</button></form>';
    }

    var weather = cfg.weather || {};
    if (weather.enabled) {
      var wLabel = String(weather.label || '').trim();
      html += '<div class="dashboard-system-chip dashboard-system-weather is-loading" data-chip-type="weather"' +
        ' data-weather-label="' + escapeHtml(wLabel) + '"' +
        ' data-weather-lat="' + Number(weather.latitude || 0) + '"' +
        ' data-weather-lon="' + Number(weather.longitude || 0) + '"' +
        ' data-weather-timezone="' + escapeHtml(String(weather.timezone || 'UTC')) + '"' +
        ' data-weather-units="' + (weather.units === 'imperial' ? 'imperial' : 'metric') + '"' +
        ' data-weather-cache="' + Math.max(1, Math.min(60, Number(weather.cache || 5))) + '">' +
        '<div class="system-chip-row"><img class="system-chip-icon system-weather-icon" src="' + WEATHER_ICON_FALLBACK + '" alt="" />' +
        '<div class="system-chip-body"><span class="system-chip-value system-weather-value">–</span>' +
        '<span class="system-chip-label system-weather-condition">Loading…</span></div></div></div>';
    }

    containerEl.innerHTML = html;
  }

  function updateChipBar(barFill, pct) {
    if (!barFill) return;
    barFill.style.width = pct + '%';
    barFill.classList.toggle('bar-warning', pct >= 70 && pct < 90);
    barFill.classList.toggle('bar-danger', pct >= 90);
  }

  function refreshSystemBarWithData(containerEl, sysData, wxData) {
    var showTotalSpace = containerEl.dataset.systemShowTotalSpace === '1';

    // CPU
    var cpuChip = containerEl.querySelector('[data-chip-type="cpu"]');
    if (cpuChip && sysData) {
      var pct = Math.max(0, Math.min(100, Number(sysData.cpu && sysData.cpu.percent) || 0));
      var v = cpuChip.querySelector('.system-chip-value');
      if (v) v.textContent = pct.toFixed(0) + '%';
      updateChipBar(cpuChip.querySelector('.system-chip-bar-fill'), pct);
      cpuChip.classList.remove('is-loading');
    }

    // Memory
    var memChip = containerEl.querySelector('[data-chip-type="memory"]');
    if (memChip && sysData) {
      var memFree = Number(sysData.memory && sysData.memory.free) || 0;
      var memTotal = Number(sysData.memory && sysData.memory.total) || 0;
      var memPct = memTotal > 0 ? Math.round(100 * (memTotal - memFree) / memTotal) : 0;
      var mv = memChip.querySelector('.system-chip-value');
      if (mv) mv.textContent = formatBytes(showTotalSpace ? memTotal : memFree);
      updateChipBar(memChip.querySelector('.system-chip-bar-fill'), memPct);
      memChip.classList.remove('is-loading');
    }

    // Disks
    containerEl.querySelectorAll('[data-chip-type="disk"]').forEach(function (diskChip) {
      if (!sysData) return;
      var diskPath = normalizeSystemPath(diskChip.dataset.chipPath || '/');
      var entry = Array.isArray(sysData.disks)
        ? (sysData.disks.find(function (d) { return normalizeSystemPath(d.path) === diskPath; })
          || sysData.disks.find(function (d) { return normalizeSystemPath(d.resolvedPath) === diskPath; }))
        : null;
      var dv = diskChip.querySelector('.system-chip-value');
      if (entry && entry.ok) {
        var free = Number(entry.free) || 0;
        var total = Number(entry.total) || 0;
        var dpct = total > 0 ? Math.round(100 * (total - free) / total) : 0;
        if (dv) dv.textContent = formatBytes(showTotalSpace ? total : free);
        updateChipBar(diskChip.querySelector('.system-chip-bar-fill'), dpct);
      } else {
        if (dv) dv.textContent = '—';
      }
      diskChip.classList.remove('is-loading');
    });

    // Weather
    var wxChip = containerEl.querySelector('[data-chip-type="weather"]');
    if (wxChip) {
      var wLabel = String(wxChip.dataset.weatherLabel || '').trim();
      var wUnits = wxChip.dataset.weatherUnits === 'imperial' ? '°F' : '°C';
      var wv = wxChip.querySelector('.system-weather-value');
      var wc = wxChip.querySelector('.system-weather-condition');
      var wi = wxChip.querySelector('.system-weather-icon');
      if (wxData && wxData.ok) {
        var temp = Number(wxData.temperature).toFixed(1);
        if (wv) wv.textContent = (wLabel ? wLabel + ', ' : '') + temp + wUnits;
        if (wc) wc.textContent = wxData.condition || '';
        if (wi) { wi.src = WEATHER_ICONS[wxData.icon] || WEATHER_ICON_FALLBACK; wi.onerror = function () { this.src = WEATHER_ICON_FALLBACK; }; }
      } else {
        if (wv) wv.textContent = wLabel || '—';
        if (wc) wc.textContent = 'Unavailable';
      }
      wxChip.classList.remove('is-loading');
    }
  }

  function buildDeploymentSummaryBar(containerEl) {
    var cfg = parseSystemConfigAttr(containerEl.getAttribute('data-system-config') || '{}');
    var rawStats = (cfg.stats && typeof cfg.stats === 'object') ? cfg.stats : {};
    var onlineVisible = rawStats.online !== false;
    var offlineVisible = rawStats.offline !== false;
    var totalVisible = rawStats.total !== false;
    if (!onlineVisible && !offlineVisible && !totalVisible) onlineVisible = true;
    var visibleStats = [];
    if (onlineVisible) visibleStats.push({ key: 'online', label: 'Online' });
    if (offlineVisible) visibleStats.push({ key: 'offline', label: 'Offline' });
    if (totalVisible) visibleStats.push({ key: 'total', label: 'Total' });
    var configuredCols = Number(cfg.columns);
    var columnCount = Number.isFinite(configuredCols) ? Math.max(1, Math.min(4, Math.round(configuredCols))) : 3;
    columnCount = Math.min(columnCount, Math.max(1, visibleStats.length));
    var metricsHtml = visibleStats.map(function (stat) {
      return '<span class="stat-card-metric" data-deployment-stat="' + stat.key + '">' +
        '<span class="stat-card-metric-value">0</span>' +
        '<span class="stat-card-metric-label">' + escapeHtml(stat.label) + '</span>' +
      '</span>';
    }).join('');
    containerEl.classList.remove('dashboard-system-bar');
    containerEl.classList.add('dashboard-stat-card', 'deployment-summary-stat-card');
    containerEl.classList.add('is-loading');
    containerEl.setAttribute('data-widget-metric-cols', String(columnCount));
    containerEl.innerHTML =
      '<div class="stat-card-header">' +
        '<img class="stat-card-icon" src="/icons/launcharr-icon.png" alt="" onerror="this.src=\'/icons/app.svg\'" />' +
        '<span class="stat-card-name">Launcharr</span>' +
        '<span class="stat-card-status is-down">0.0%</span>' +
      '</div>' +
      '<div class="stat-card-metrics">' + metricsHtml + '</div>';
  }

  function updateDeploymentChip(containerEl, chipType, value) {
    var metricKey = String(chipType || '').replace(/^deployment-/, '');
    var chip = containerEl.querySelector('[data-deployment-stat="' + metricKey + '"]');
    if (!chip) return;
    var valueEl = chip.querySelector('.stat-card-metric-value');
    if (valueEl) valueEl.textContent = String(Math.max(0, Math.round(Number(value) || 0)));
  }

  function refreshDeploymentSummaryWithData(containerEl, data) {
    var onlineRaw = (data && data.onlineWidgets != null) ? data.onlineWidgets : (data && data.online);
    var offlineRaw = (data && data.offlineWidgets != null) ? data.offlineWidgets : (data && data.offline);
    var unknownRaw = (data && data.unknownWidgets != null) ? data.unknownWidgets : (data && data.unknown);
    var totalRaw = (data && data.totalWidgets != null) ? data.totalWidgets : (data && data.total);
    var online = Number(onlineRaw) || 0;
    var offline = Number(offlineRaw) || 0;
    var unknown = Number(unknownRaw) || 0;
    var total = Number(totalRaw) || 0;
    var offlineDisplay = offline + unknown;
    var totalDisplay = Math.max(total, online + offlineDisplay);
    updateDeploymentChip(containerEl, 'deployment-online', online);
    updateDeploymentChip(containerEl, 'deployment-offline', offlineDisplay);
    updateDeploymentChip(containerEl, 'deployment-total', totalDisplay);
    var statusEl = containerEl.querySelector('.stat-card-status');
    if (statusEl) {
      statusEl.classList.remove('is-up', 'is-warn', 'is-down', 'is-unknown');
      var pct = totalDisplay > 0 ? (online / totalDisplay) * 100 : 0;
      if (!Number.isFinite(pct)) pct = 0;
      pct = Math.max(0, Math.min(100, pct));
      var statusClass = pct >= 90 ? 'is-up' : (pct >= 50 ? 'is-warn' : 'is-down');
      statusEl.classList.add(statusClass);
      statusEl.textContent = pct.toFixed(1) + '%';
    }
    containerEl.classList.remove('is-loading');
  }

  async function fetchSystemInfo(diskPaths) {
    var pathsParam = (diskPaths && diskPaths.length) ? diskPaths.join(',') : '/';
    var resp = await fetch('/api/system-info?paths=' + encodeURIComponent(pathsParam), { credentials: 'same-origin' });
    if (!resp.ok) throw new Error('System info request failed');
    return resp.json();
  }

  async function fetchWeather(wxChip) {
    var lat = Number(wxChip.dataset.weatherLat || 0);
    var lon = Number(wxChip.dataset.weatherLon || 0);
    var timezone = encodeURIComponent(String(wxChip.dataset.weatherTimezone || 'UTC'));
    var units = wxChip.dataset.weatherUnits === 'imperial' ? 'imperial' : 'metric';
    var cache = Math.max(1, Math.min(60, Number(wxChip.dataset.weatherCache || 5)));
    var url = '/api/openmeteo?lat=' + lat + '&lon=' + lon + '&timezone=' + timezone + '&units=' + units + '&cache=' + cache;
    var resp = await fetch(url, { credentials: 'same-origin' });
    if (!resp.ok) throw new Error('Weather request failed');
    return resp.json();
  }

  async function fetchDeploymentSummary() {
    var resp = await fetch('/api/widget-deployment-summary', { credentials: 'same-origin' });
    if (!resp.ok) throw new Error('Deployment summary request failed');
    return resp.json();
  }

  // ─── Bar loader ────────────────────────────────────────────────────────────

  function loadBar(barEl) {
    var refreshSeconds = parseInt(barEl.dataset.refreshSeconds, 10);
    if (!Number.isFinite(refreshSeconds)) refreshSeconds = 60;
    if (refreshSeconds < 0) refreshSeconds = 60;
    if (refreshSeconds > 0 && refreshSeconds < 15) refreshSeconds = 60;

    var cards = Array.from(barEl.querySelectorAll('.dashboard-stat-card[data-app-id]'));
    var sysResourceContainers = Array.from(barEl.querySelectorAll('.dashboard-system-bar[data-system-type="sys-resources"]'));
    var linksContainers = Array.from(barEl.querySelectorAll('.dashboard-links-widget[data-system-type="links"]'));
    var deploymentContainers = Array.from(barEl.querySelectorAll('[data-system-type="deployment-summary"]'));

    if (!cards.length && !sysResourceContainers.length && !linksContainers.length && !deploymentContainers.length) return;

    // Build sys-resources child DOM from config
    linksContainers.forEach(function (c) { buildLinksWidget(c); });
    sysResourceContainers.forEach(function (c) { buildSystemBar(c); });
    deploymentContainers.forEach(function (c) { buildDeploymentSummaryBar(c); });
    if (barEl._bindSearchForms) barEl._bindSearchForms();

    function refreshCard(card) {
      var appId = String(card.dataset.appId || '').trim();
      if (!appId) return;
      if (card.dataset.statsLoading === '1') return;
      card.dataset.statsLoading = '1';
      fetchStats(appId)
        .then(function (data) { renderCard(card, data); })
        .catch(function () { renderCardError(card); })
        .finally(function () { card.dataset.statsLoading = '0'; });
    }

    function refreshSysContainers() {
      sysResourceContainers.forEach(function (containerEl) {
        if (containerEl.dataset.systemLoading === '1') return;
        containerEl.dataset.systemLoading = '1';
        // Collect disk paths needed for this container
        var diskPaths = Array.from(containerEl.querySelectorAll('[data-chip-type="disk"][data-chip-path]'))
          .map(function (el) { return el.dataset.chipPath; })
          .filter(Boolean);
        var needsResourceInfo = Boolean(
          containerEl.querySelector('[data-chip-type="cpu"]') ||
          containerEl.querySelector('[data-chip-type="memory"]') ||
          diskPaths.length
        );
        var wxChip = containerEl.querySelector('[data-chip-type="weather"]');

        var resourcePromise = needsResourceInfo ? fetchSystemInfo(diskPaths) : Promise.resolve(null);
        var weatherPromise = wxChip ? fetchWeather(wxChip) : Promise.resolve(null);

        Promise.all([resourcePromise, weatherPromise])
          .then(function (results) {
            refreshSystemBarWithData(containerEl, results[0], results[1]);
          })
          .catch(function () {
            refreshSystemBarWithData(containerEl, null, null);
          })
          .finally(function () {
            containerEl.dataset.systemLoading = '0';
          });
      });
    }

    function refreshDeploymentContainers() {
      deploymentContainers.forEach(function (containerEl) {
        if (containerEl.dataset.systemLoading === '1') return;
        containerEl.dataset.systemLoading = '1';
        fetchDeploymentSummary()
          .then(function (data) {
            refreshDeploymentSummaryWithData(containerEl, data || {});
          })
          .catch(function () {
            refreshDeploymentSummaryWithData(containerEl, {});
          })
          .finally(function () {
            containerEl.dataset.systemLoading = '0';
          });
      });
    }

    function refreshAll() {
      if (!document.body.contains(barEl)) {
        if (timer) clearInterval(timer);
        return;
      }
      if (barEl.classList.contains('is-collapsed')) return;
      cards.forEach(refreshCard);
      refreshSysContainers();
      refreshDeploymentContainers();
    }

    // Initial load
    refreshAll();
    // Periodic refresh
    var timer = null;
    if (refreshSeconds > 0) {
      timer = setInterval(refreshAll, refreshSeconds * 1000);
    }
  }

  function escapeHtml(str) {
    return String(str || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function init() {
    var bars = Array.from(document.querySelectorAll('.dashboard-widget-bar[data-widget-bar-id]'));
    bars.forEach(function (barEl) {
      bindBarControls(barEl);
      loadBar(barEl);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
