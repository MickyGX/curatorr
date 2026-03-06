(() => {
  const configs = resolveConfigs();
  if (!configs.length) return;

  configs.forEach((config) => {
    try {
      initIndexerSearch(config);
    } catch (err) {
      console.error('Indexer search init failed for', config?.prefix || config?.appId || 'unknown', err);
    }
  });

  function resolveConfigs() {
    const list = [];
    if (Array.isArray(window.INDEXER_SEARCH_CONFIGS)) {
      window.INDEXER_SEARCH_CONFIGS.forEach((entry) => {
        if (entry && (entry.prefix || entry.appId)) list.push(entry);
      });
    }
    if (window.INDEXER_SEARCH_CONFIG) {
      list.push(window.INDEXER_SEARCH_CONFIG);
    }
    if (window.PROWLARR_OVERVIEW_CONFIG && !list.some((entry) => String(entry?.prefix || '').trim() === 'prowlarr')) {
      list.push({
        appId: 'prowlarr',
        appName: window.PROWLARR_OVERVIEW_CONFIG?.appName || 'Prowlarr',
        prefix: 'prowlarr',
        endpoint: '/api/prowlarr/search',
        canDownload: true,
        downloadEndpoint: '/api/prowlarr/download',
      });
    }
    const dedupe = new Map();
    list.forEach((entry) => {
      const prefix = String(entry?.prefix || entry?.appId || '').trim().toLowerCase();
      if (!prefix) return;
      dedupe.set(prefix, {
        appId: String(entry?.appId || prefix).trim().toLowerCase(),
        appName: String(entry?.appName || prefix).trim() || prefix,
        prefix,
        endpoint: String(entry?.endpoint || '/api/prowlarr/search').trim(),
        filtersEndpoint: String(
          entry?.filtersEndpoint
          || (prefix === 'jackett' ? '/api/jackett/search/filters' : '/api/prowlarr/search/filters')
        ).trim(),
        canDownload: Boolean(entry?.canDownload),
        downloadEndpoint: String(entry?.downloadEndpoint || '/api/prowlarr/download').trim(),
      });
    });
    return Array.from(dedupe.values());
  }

  function initIndexerSearch(config) {
    const prefix = String(config.prefix || config.appId || '').trim();
    if (!prefix) return;
    const isJackett = prefix.toLowerCase() === 'jackett';
    const input = document.getElementById(`${prefix}SearchInput`);
    const button = document.getElementById(`${prefix}SearchButton`);
    const limitSelect = document.getElementById(`${prefix}SearchLimitSelect`);
    const typeFilter = document.getElementById(`${prefix}SearchTypeFilter`);
    const indexerFilter = document.getElementById(`${prefix}SearchIndexerFilter`);
    const categoryFilter = document.getElementById(`${prefix}SearchCategoryFilter`);
    const resultsEl = document.getElementById(`${prefix}SearchResults`);
    const prevBtn = document.getElementById(`${prefix}SearchPrevBtn`);
    const nextBtn = document.getElementById(`${prefix}SearchNextBtn`);
    const pageLabel = document.getElementById(`${prefix}SearchPageLabel`);
    if (!input || !button || !resultsEl) return;

    const state = {
      requestId: 0,
      page: 0,
      pageSize: Number(limitSelect?.value || 25),
      total: 0,
      itemMap: new Map(),
      indexers: [],
      categories: [],
    };

    const escapeHtml = (value) => String(value || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');

    const formatBytes = (value) => {
      const size = Number(value);
      if (!Number.isFinite(size) || size <= 0) return '';
      const units = ['B', 'KB', 'MB', 'GB', 'TB'];
      let amount = size;
      let index = 0;
      while (amount >= 1024 && index < units.length - 1) {
        amount /= 1024;
        index += 1;
      }
      return `${amount.toFixed(amount >= 10 || index === 0 ? 0 : 1)} ${units[index]}`;
    };

    const formatNumber = (value) => {
      const amount = Number(value);
      if (!Number.isFinite(amount)) return '';
      return new Intl.NumberFormat().format(amount);
    };

    const formatAge = (value) => {
      const parsed = value ? new Date(value) : null;
      if (!parsed || Number.isNaN(parsed.getTime())) return '';
      const diffMs = Date.now() - parsed.getTime();
      if (!Number.isFinite(diffMs) || diffMs <= 0) return 'Just now';
      const minutes = Math.floor(diffMs / 60000);
      if (minutes < 60) return `${minutes}m`;
      const hours = Math.floor(minutes / 60);
      if (hours < 24) return `${hours}h`;
      return `${Math.floor(hours / 24)}d`;
    };

    const getItemKey = (item) => {
      const guid = String(item?.guid || '').trim();
      if (guid) return guid;
      const id = String(item?.id || '').trim();
      if (id) return id;
      return String(item?.infoUrl || item?.downloadUrl || item?.title || Math.random());
    };

    const normalizeProtocol = (value) => {
      const raw = String(value || '').trim().toLowerCase();
      if (!raw) return '';
      if (raw.includes('usenet') || raw.includes('newznab') || raw === '1') return 'usenet';
      if (raw.includes('torrent') || raw.includes('torznab') || raw === '2') return 'torrent';
      return '';
    };

    const currentTypeValue = () => {
      const value = normalizeProtocol(typeFilter?.value || '');
      return value || 'all';
    };

    const dispatchOptionsUpdated = (select) => {
      if (!select) return;
      select.dispatchEvent(new Event('arr:options-updated', { bubbles: true }));
    };

    const setSelectOptions = (select, options, preferredValue = 'all') => {
      if (!select) return;
      const current = String(select.value || '').trim();
      const nextOptions = Array.isArray(options) ? options : [];
      select.innerHTML = nextOptions.map((entry) => {
        const value = String(entry?.value || '').trim();
        const label = String(entry?.label || value || '').trim();
        const icon = String(entry?.icon || '').trim();
        const name = String(entry?.name || '').trim();
        const protocol = String(entry?.protocol || '').trim();
        const attrs = [
          icon ? ` data-icon="${escapeHtml(icon)}"` : '',
          name ? ` data-name="${escapeHtml(name)}"` : '',
          protocol ? ` data-protocol="${escapeHtml(protocol)}"` : '',
        ].join('');
        return `<option value="${escapeHtml(value)}"${attrs}>${escapeHtml(label)}</option>`;
      }).join('');
      const hasCurrent = nextOptions.some((entry) => String(entry?.value || '').trim() === current);
      const hasPreferred = nextOptions.some((entry) => String(entry?.value || '').trim() === preferredValue);
      const fallback = hasPreferred
        ? preferredValue
        : String(nextOptions[0]?.value || '').trim();
      select.value = hasCurrent ? current : fallback;
      dispatchOptionsUpdated(select);
    };

    const buildCategoryOptions = () => {
      const typeValue = currentTypeValue();
      const selectedIndexerId = String(indexerFilter?.value || 'all').trim();
      const selectedIndexer = selectedIndexerId === 'all'
        ? null
        : state.indexers.find((entry) => entry.id === selectedIndexerId);
      const effectiveProtocol = selectedIndexer?.protocol || (typeValue === 'all' ? '' : typeValue);
      const filtered = state.categories.filter((entry) => {
        if (!effectiveProtocol) return true;
        if (!Array.isArray(entry.protocols) || !entry.protocols.length) return true;
        return entry.protocols.includes(effectiveProtocol);
      });
      return [
        { value: 'all', label: 'All top level categories', icon: '/icons/all-type.svg' },
        ...filtered.map((entry) => ({
          value: entry.id,
          label: entry.name,
          icon: '/icons/all-type.svg',
        })),
      ];
    };

    const syncDependentFilters = ({ preserveIndexer = true } = {}) => {
      const typeValue = currentTypeValue();
      if (indexerFilter) {
        const filteredIndexers = state.indexers.filter((entry) => typeValue === 'all' || entry.protocol === typeValue);
        const previousIndexer = preserveIndexer ? String(indexerFilter.value || '').trim() : '';
        setSelectOptions(indexerFilter, [
          { value: 'all', label: 'All available indexers', icon: '/icons/all-type.svg' },
          ...filteredIndexers.map((entry) => ({
            value: entry.id,
            label: entry.name,
            name: entry.name,
            protocol: entry.protocol,
            icon: '/icons/all-type.svg',
          })),
        ]);
        if (!preserveIndexer && previousIndexer !== indexerFilter.value) {
          indexerFilter.value = 'all';
          dispatchOptionsUpdated(indexerFilter);
        }
      }
      if (categoryFilter) {
        setSelectOptions(categoryFilter, buildCategoryOptions());
      }
    };

    const normalizeIndexers = (payload) => {
      const list = Array.isArray(payload?.indexers) ? payload.indexers : [];
      const dedupe = new Map();
      list.forEach((entry) => {
        const id = String(entry?.id || entry?.value || entry?.name || '').trim();
        const name = String(entry?.name || entry?.label || id || '').trim();
        if (!id || !name) return;
        const protocol = normalizeProtocol(entry?.protocol || entry?.type || entry?.kind || '');
        if (!protocol) return;
        if (!dedupe.has(id)) {
          dedupe.set(id, { id, name, protocol });
        }
      });
      return Array.from(dedupe.values()).sort((a, b) => a.name.localeCompare(b.name));
    };

    const normalizeCategories = (payload) => {
      const list = Array.isArray(payload?.categories) ? payload.categories : [];
      const dedupe = new Map();
      list.forEach((entry) => {
        const id = String(entry?.id || entry?.value || '').trim();
        const name = String(entry?.name || entry?.label || '').trim();
        if (!id || !name) return;
        const protocolValues = Array.isArray(entry?.protocols)
          ? entry.protocols
          : (entry?.protocol ? [entry.protocol] : []);
        const protocols = protocolValues
          .map((value) => normalizeProtocol(value))
          .filter(Boolean);
        const existing = dedupe.get(id);
        if (!existing) {
          dedupe.set(id, { id, name, protocols: Array.from(new Set(protocols)) });
          return;
        }
        const mergedProtocols = new Set([...(existing.protocols || []), ...protocols]);
        existing.name = existing.name || name;
        existing.protocols = Array.from(mergedProtocols);
      });
      return Array.from(dedupe.values()).sort((a, b) => Number(a.id) - Number(b.id));
    };

    const loadFilterMetadata = async () => {
      if (!config.filtersEndpoint) {
        syncDependentFilters();
        return;
      }
      try {
        const response = await fetch(config.filtersEndpoint, { headers: { Accept: 'application/json' } });
        if (!response.ok) throw new Error(`Filter metadata request failed (${response.status}).`);
        const payload = await response.json();
        state.indexers = normalizeIndexers(payload);
        state.categories = normalizeCategories(payload);
      } catch (err) {
        state.indexers = [];
        state.categories = [];
      }
      syncDependentFilters();
    };

    const renderMessage = (message) => {
      resultsEl.innerHTML = `<div class="queue-row indexer-row indexer-row--empty"><div class="queue-col-title">${escapeHtml(message)}</div></div>`;
    };

    const renderResults = (items) => {
      if (!items.length) {
        renderMessage('No results found.');
        return;
      }
      resultsEl.innerHTML = items.map((item) => {
        const itemKey = getItemKey(item);
        const title = item?.title || item?.name || 'Untitled';
        const indexer = item?.indexer || item?.indexerName || '';
        const peers = (Number.isFinite(Number(item?.seeders)) || Number.isFinite(Number(item?.leechers)))
          ? `${formatNumber(item?.seeders) || '0'} / ${formatNumber(item?.leechers) || '0'}`
          : '';
        const protocol = String(item?.protocol || '').trim().toUpperCase();
        const protocolClass = protocol.toLowerCase() === 'usenet' ? ' usenet' : '';
        const size = formatBytes(item?.size);
        const age = formatAge(item?.publishDate || item?.publishedDate || item?.published);
        const infoUrl = String(item?.infoUrl || '').trim();
        const downloadUrl = String(item?.downloadUrl || '').trim();
        const actionHtml = config.canDownload
          ? `<button class="prowlarr-download-btn" type="button" data-key="${escapeHtml(itemKey)}">Send</button>`
          : (downloadUrl
            ? `<a class="indexer-result-link" href="${escapeHtml(downloadUrl)}" target="_blank" rel="noreferrer">Open</a>`
            : (infoUrl ? `<a class="indexer-result-link" href="${escapeHtml(infoUrl)}" target="_blank" rel="noreferrer">Info</a>` : '-'));
        return `
          <div class="queue-row indexer-row" data-prowlarr-key="${escapeHtml(itemKey)}">
            <div class="queue-col-title">
              <div class="indexer-result-title">${escapeHtml(title)}</div>
              <div class="indexer-result-meta">
                ${indexer ? `Indexer: ${escapeHtml(indexer)}` : ''}
                ${age ? `${indexer ? ' · ' : ''}Age: ${escapeHtml(age)}` : ''}
              </div>
              ${infoUrl ? `<a class="indexer-result-link" href="${escapeHtml(infoUrl)}" target="_blank" rel="noreferrer">Info</a>` : ''}
            </div>
            <div class="queue-col-detail">${escapeHtml(peers || '-')}</div>
            <div class="queue-col-subdetail"><span class="queue-quality">${escapeHtml(size || '-')}</span></div>
            <div class="queue-col-size"><span class="queue-protocol${protocolClass}">${escapeHtml(protocol || '-')}</span></div>
            <div class="queue-col-protocol indexer-action-cell">${actionHtml}</div>
          </div>
        `;
      }).join('');
      if (config.canDownload) bindDownloadButtons();
    };

    const withParams = (base, params) => {
      const url = new URL(base, window.location.origin);
      Object.entries(params || {}).forEach(([key, value]) => {
        if (value === undefined || value === null || value === '') return;
        url.searchParams.set(key, String(value));
      });
      return `${url.pathname}${url.search}`;
    };

    const buildFilterParams = () => {
      const params = {};
      const typeValue = currentTypeValue();
      const indexerValue = String(indexerFilter?.value || 'all').trim();
      const categoryValue = String(categoryFilter?.value || 'all').trim();
      if (isJackett) {
        if (typeValue !== 'all') params.protocol = typeValue;
        if (indexerValue && indexerValue !== 'all') {
          const selected = indexerFilter?.selectedOptions?.[0] || null;
          const selectedName = String(selected?.dataset?.name || selected?.textContent || '').trim();
          params.indexer = indexerValue;
          if (selectedName) params.indexerName = selectedName;
        }
        if (categoryValue && categoryValue !== 'all') params.category = categoryValue;
        return params;
      }

      if (indexerValue && indexerValue !== 'all') {
        params.indexerIds = indexerValue;
      } else if (typeValue !== 'all') {
        const ids = state.indexers
          .filter((entry) => entry.protocol === typeValue)
          .map((entry) => entry.id)
          .filter(Boolean);
        if (ids.length) {
          params.indexerIds = ids.join(',');
        } else {
          params.type = typeValue;
        }
      }
      if (categoryValue && categoryValue !== 'all') params.categories = categoryValue;
      return params;
    };

    const extractItems = (payload) => {
      if (Array.isArray(payload)) return { list: payload, total: payload.length, hasExplicitTotal: false };
      if (Array.isArray(payload?.records)) {
        const hasTotalRecords = payload?.totalRecords !== undefined && payload?.totalRecords !== null && payload?.totalRecords !== '';
        const hasTotal = payload?.total !== undefined && payload?.total !== null && payload?.total !== '';
        const totalRaw = hasTotalRecords ? payload.totalRecords : (hasTotal ? payload.total : payload.records.length);
        const total = Number(totalRaw);
        return {
          list: payload.records,
          total: Number.isFinite(total) ? total : payload.records.length,
          hasExplicitTotal: hasTotalRecords || hasTotal,
        };
      }
      if (Array.isArray(payload?.results)) {
        const hasTotal = payload?.total !== undefined && payload?.total !== null && payload?.total !== '';
        const total = Number(hasTotal ? payload.total : payload.results.length);
        return {
          list: payload.results,
          total: Number.isFinite(total) ? total : payload.results.length,
          hasExplicitTotal: hasTotal,
        };
      }
      return { list: [], total: 0, hasExplicitTotal: false };
    };

    const updatePagination = () => {
      const totalPages = Math.max(1, Math.ceil((state.total || 0) / Math.max(1, state.pageSize)));
      if (state.page > totalPages - 1) state.page = Math.max(0, totalPages - 1);
      if (pageLabel) pageLabel.textContent = `Page ${state.page + 1}${state.total ? ` of ${totalPages}` : ''}`;
      if (prevBtn) prevBtn.disabled = state.page <= 0;
      if (nextBtn) nextBtn.disabled = state.page >= totalPages - 1;
    };

    const runSearch = async ({ resetPage = false } = {}) => {
      const query = String(input.value || '').trim();
      if (!query) {
        state.total = 0;
        updatePagination();
        renderMessage('Enter a search term to get results.');
        return;
      }
      if (resetPage) state.page = 0;
      const pageSize = Number(limitSelect?.value || 25);
      state.pageSize = Number.isFinite(pageSize) && pageSize > 0 ? pageSize : 25;
      const offset = state.pageSize * state.page;
      const requestId = ++state.requestId;
      renderMessage('Searching…');
      try {
        const url = withParams(config.endpoint, { query, limit: state.pageSize, offset, ...buildFilterParams() });
        const response = await fetch(url);
        if (!response.ok) {
          const errorText = await response.text();
          throw new Error(errorText || `Search failed (${response.status}).`);
        }
        const payload = await response.json();
        if (requestId !== state.requestId) return;
        const extracted = extractItems(payload);
        const minimumTotal = offset + extracted.list.length;
        if (extracted.hasExplicitTotal) {
          state.total = extracted.total;
        } else if (resetPage || state.page === 0 || !Number.isFinite(state.total) || state.total <= 0) {
          state.total = extracted.total || extracted.list.length;
        } else {
          const maybeMore = extracted.list.length >= state.pageSize ? 1 : 0;
          state.total = Math.max(state.total, minimumTotal + maybeMore);
        }
        state.itemMap = new Map(extracted.list.map((item) => [getItemKey(item), item]));
        renderResults(extracted.list);
        updatePagination();
      } catch (err) {
        renderMessage(err?.message || 'Search failed.');
        updatePagination();
      }
    };

    const triggerSearchFromFilters = () => {
      const query = String(input.value || '').trim();
      if (!query) {
        renderMessage('Enter a search term to get results.');
        return;
      }
      runSearch({ resetPage: true });
    };

    function bindDownloadButtons() {
      const buttons = resultsEl.querySelectorAll('.prowlarr-download-btn[data-key]');
      buttons.forEach((btn) => {
        btn.addEventListener('click', async () => {
          const key = String(btn.getAttribute('data-key') || '').trim();
          const release = key ? state.itemMap.get(key) : null;
          if (!release) return;
          const original = btn.textContent;
          btn.disabled = true;
          btn.textContent = 'Sending…';
          try {
            const response = await fetch(config.downloadEndpoint, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ release }),
            });
            if (!response.ok) throw new Error('Send failed');
            btn.textContent = 'Sent';
          } catch (err) {
            btn.textContent = 'Failed';
          }
          setTimeout(() => {
            btn.textContent = original;
            btn.disabled = false;
          }, 1200);
        });
      });
    }

    button.addEventListener('click', () => runSearch({ resetPage: true }));
    input.addEventListener('keydown', (event) => {
      if (event.key !== 'Enter') return;
      event.preventDefault();
      runSearch({ resetPage: true });
    });
    typeFilter?.addEventListener('change', () => {
      syncDependentFilters({ preserveIndexer: false });
      triggerSearchFromFilters();
    });
    indexerFilter?.addEventListener('change', () => {
      if (categoryFilter) setSelectOptions(categoryFilter, buildCategoryOptions());
      triggerSearchFromFilters();
    });
    categoryFilter?.addEventListener('change', triggerSearchFromFilters);
    limitSelect?.addEventListener('change', () => runSearch({ resetPage: true }));
    prevBtn?.addEventListener('click', () => {
      if (state.page <= 0) return;
      state.page -= 1;
      runSearch();
    });
    nextBtn?.addEventListener('click', () => {
      const totalPages = Math.max(1, Math.ceil((state.total || 0) / Math.max(1, state.pageSize)));
      if (state.page >= totalPages - 1) return;
      state.page += 1;
      runSearch();
    });

    syncDependentFilters();
    loadFilterMetadata();
    updatePagination();
  }
})();
