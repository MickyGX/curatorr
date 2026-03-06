(() => {
  try {
    const input = document.getElementById('prowlarrSearchInput');
    const button = document.getElementById('prowlarrSearchButton');
    const limitSelect = document.getElementById('prowlarrSearchLimitSelect');
    const resultsEl = document.getElementById('prowlarrSearchResults');
    const prevBtn = document.getElementById('prowlarrSearchPrevBtn');
    const nextBtn = document.getElementById('prowlarrSearchNextBtn');
    const pageLabel = document.getElementById('prowlarrSearchPageLabel');

    if (!input || !button || !resultsEl) return;

    const state = {
      requestId: 0,
      page: 0,
      pageSize: Number(limitSelect?.value || 25),
      total: 0,
      items: [],
      pagedItems: [],
      serverPaged: false,
      itemMap: new Map(),
      allResults: null,
      forceClientPaging: false,
    };

    const escapeHtml = (value) =>
      String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');

    const formatBytes = (value) => {
      const size = Number(value);
      if (!Number.isFinite(size) || size <= 0) return '';
      const units = ['B', 'KB', 'MB', 'GB', 'TB'];
      let index = 0;
      let amount = size;
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
      const days = Math.floor(hours / 24);
      return `${days}d`;
    };

    const logApi = (level, message, meta) => {
      const logger = console[level] || console.log;
      if (meta && typeof meta === 'object') {
        logger(`[Launcharr] ${message}`, meta);
      } else {
        logger(`[Launcharr] ${message}`);
      }
    };

    const renderMessage = (message) => {
      resultsEl.innerHTML = `<div class="prowlarr-row prowlarr-row--empty">${escapeHtml(message)}</div>`;
    };

    const getItemKey = (item) => {
      const guid = String(item?.guid || '').trim();
      if (guid) return guid;
      const id = String(item?.id || '').trim();
      const indexer = String(item?.indexerId || '').trim();
      if (id || indexer) return `${indexer}:${id}`;
      const infoUrl = String(item?.infoUrl || item?.downloadUrl || '').trim();
      if (infoUrl) return infoUrl;
      return String(Math.random());
    };

    const renderResults = (items) => {
      if (!items || !items.length) {
        renderMessage('No results found.');
        return;
      }
      const html = items
        .map((item) => {
          const itemKey = getItemKey(item);
          const title = item?.title || item?.name || 'Untitled';
          const indexer = item?.indexer || item?.indexerName || item?.indexerId || '';
          const protocol = item?.protocol ? String(item.protocol).toUpperCase() : '';
          const size = formatBytes(item?.size);
          const seeders = formatNumber(item?.seeders);
          const leechers = formatNumber(item?.leechers);
          const age = formatAge(item?.publishDate || item?.publishedDate || item?.published);
          const infoUrl = item?.infoUrl || item?.details || '';
          const downloadUrl = item?.downloadUrl || item?.link || '';
          const peers = seeders || leechers ? `${seeders || '0'} / ${leechers || '0'}` : '';
          const rowId = String(item?.id || item?.guid || '');
          return `
            <div class="prowlarr-row" data-prowlarr-key="${escapeHtml(itemKey)}" data-prowlarr-id="${escapeHtml(rowId)}">
              <div>
                <div class="prowlarr-result-title">${escapeHtml(title)}</div>
                <div class="prowlarr-result-meta">
                  ${indexer ? `Indexer: ${escapeHtml(indexer)}` : ''}
                  ${age ? `${indexer ? ' · ' : ''}Age: ${escapeHtml(age)}` : ''}
                </div>
                ${infoUrl ? `<a class="prowlarr-result-link" href="${escapeHtml(infoUrl)}" target="_blank" rel="noreferrer">Info</a>` : ''}
              </div>
              <div>${escapeHtml(peers)}</div>
              <div>${escapeHtml(size)}</div>
              <div>${escapeHtml(protocol || '')}</div>
              <div>
                <button class="prowlarr-download-btn" type="button" data-key="${escapeHtml(itemKey)}" data-id="${escapeHtml(String(item?.id || ''))}" data-guid="${escapeHtml(String(item?.guid || ''))}" data-indexer="${escapeHtml(String(item?.indexerId || ''))}" data-client="${escapeHtml(String(item?.downloadClientId || item?.downloadClient || ''))}">Send</button>
              </div>
            </div>
          `;
        })
        .join('');

      resultsEl.innerHTML = html;
    };

    const withParams = (base, params) => {
      const url = new URL(base, window.location.origin);
      Object.entries(params || {}).forEach(([key, value]) => {
        if (value === undefined || value === null || value === '') return;
        url.searchParams.set(key, String(value));
      });
      return url.pathname + url.search;
    };

    const fetchResults = async (query, limit, offset) => {
      const params = { query };
      if (limit) params.limit = limit;
      if (offset) params.offset = offset;
      const url = withParams('/api/prowlarr/search', params);
      logApi('info', 'Prowlarr search request', { url, limit, offset });
      const response = await fetch(url);
      logApi('info', 'Prowlarr search response', { status: response.status, ok: response.ok });
      if (!response.ok) {
        const errorText = await response.text();
        logApi('error', 'Prowlarr search failed', { status: response.status, error: errorText });
        throw new Error(errorText || 'Search failed.');
      }
      return response.json();
    };

    const fetchAllResults = async (query) => {
      const params = { query, limit: 1000, offset: 0 };
      const url = withParams('/api/prowlarr/search', params);
      logApi('info', 'Prowlarr search (all) request', { url });
      const response = await fetch(url);
      logApi('info', 'Prowlarr search (all) response', { status: response.status, ok: response.ok });
      if (!response.ok) {
        const errorText = await response.text();
        logApi('error', 'Prowlarr search (all) failed', { status: response.status, error: errorText });
        throw new Error(errorText || 'Search failed.');
      }
      return response.json();
    };

    const updatePagination = () => {
      const page = state.page + 1;
      const totalPages = state.pageSize ? Math.max(1, Math.ceil(state.total / state.pageSize)) : 1;
      if (pageLabel) pageLabel.textContent = `Page ${page}${state.total ? ` of ${totalPages}` : ''}`;
      if (prevBtn) prevBtn.disabled = state.page <= 0;
      if (nextBtn) nextBtn.disabled = state.page >= totalPages - 1;
    };

    const extractItems = (payload) => {
      if (Array.isArray(payload)) return { list: payload, total: payload.length, serverPaged: false };
      if (Array.isArray(payload?.records)) return { list: payload.records, total: Number(payload.totalRecords || payload.total || payload.records.length), serverPaged: true };
      if (Array.isArray(payload?.results)) return { list: payload.results, total: Number(payload.total || payload.results.length), serverPaged: true };
      return { list: [], total: 0, serverPaged: false };
    };

    const bindDownloadButtons = () => {
      resultsEl.querySelectorAll('.prowlarr-download-btn').forEach((btn) => {
        btn.addEventListener('click', async () => {
          if (btn.disabled) return;
          const id = btn.getAttribute('data-id') || '';
          const guid = btn.getAttribute('data-guid') || '';
          const downloadClientId = btn.getAttribute('data-client') || '';
          const indexerId = btn.getAttribute('data-indexer') || '';
          const key = btn.getAttribute('data-key') || '';
          const release = (key && state.itemMap?.get(key)) ? { ...state.itemMap.get(key) } : null;
          btn.disabled = true;
          const original = btn.textContent;
          btn.textContent = 'Sending…';
          try {
            logApi('info', 'Prowlarr download request', {
              id,
              guid,
              indexerId,
              downloadClientId,
            });
            const response = await fetch('/api/prowlarr/download', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ id, guid, downloadClientId, indexerId, release }),
            });
            logApi('info', 'Prowlarr download response', { status: response.status, ok: response.ok });
            if (!response.ok) throw new Error('Download failed');
            btn.textContent = 'Sent';
            setTimeout(() => {
              btn.textContent = original;
              btn.disabled = false;
            }, 1200);
          } catch (err) {
            logApi('error', 'Prowlarr download failed', { error: err?.message || String(err) });
            btn.textContent = 'Failed';
            setTimeout(() => {
              btn.textContent = original;
              btn.disabled = false;
            }, 1500);
          }
        });
      });
    };

    const runSearch = async (options = {}) => {
      const query = String(input.value || '').trim();
      if (!query) {
        renderMessage('Enter a search term to get results.');
        return;
      }
      if (options.resetPage) state.page = 0;
      const limit = Number(limitSelect?.value || 25);
      state.pageSize = Number.isFinite(limit) && limit > 0 ? limit : 25;
      const offset = state.pageSize * state.page;
      const currentRequest = ++state.requestId;
      renderMessage('Searching…');
      try {
        const payload = await fetchResults(query, state.pageSize, offset);
        if (currentRequest !== state.requestId) return;
        // If the API ignores offset and returns an array for page>0, force client paging.
        if (Array.isArray(payload) && offset > 0) {
          try {
            const allPayload = await fetchAllResults(query);
            const allExtracted = extractItems(allPayload);
            const allList = allExtracted.list || [];
            if (allList.length) {
              state.forceClientPaging = true;
              state.allResults = allList;
            }
          } catch (err) {
            // fall through to standard handling
          }
        }

        const extracted = extractItems(payload);
        let list = extracted.list || [];
        let total = extracted.total || 0;
        let serverPaged = extracted.serverPaged;

        if (Array.isArray(payload) && offset > 0 && list.length > state.pageSize) {
          serverPaged = false;
        }

        if (!serverPaged || state.forceClientPaging) {
          total = list.length;
          if (state.page === 0 && list.length === state.pageSize) {
            try {
              const allPayload = await fetchAllResults(query);
              const allExtracted = extractItems(allPayload);
              const allList = allExtracted.list || [];
              if (allList.length > list.length) {
                list = allList;
                total = allList.length;
                state.allResults = allList;
              }
            } catch (err) {
              // ignore; keep current list
            }
          }
          if (state.allResults) {
            list = state.allResults;
            total = state.allResults.length;
          }
          list = list.slice(offset, offset + state.pageSize);
        } else {
          if (!Number.isFinite(total) || total <= 0) {
            total = state.total || (offset + list.length);
          }
          if (state.page > 0 && list.length === 0) {
            try {
              const allPayload = await fetchAllResults(query);
              const allExtracted = extractItems(allPayload);
              const allList = allExtracted.list || [];
              if (allList.length) {
                state.forceClientPaging = true;
                state.allResults = allList;
                total = allList.length;
                list = allList.slice(offset, offset + state.pageSize);
              }
            } catch (err) {
              // keep serverPaged fallback
            }
          }
        }

        state.items = extracted.list || [];
        state.pagedItems = list;
        state.total = total;
        state.serverPaged = serverPaged;
        state.itemMap = new Map(list.map((item) => [getItemKey(item), item]));
        renderResults(list);
        bindDownloadButtons();
        updatePagination();
      } catch (err) {
        if (currentRequest !== state.requestId) return;
        renderMessage('Unable to load Prowlarr results.');
      }
    };

    button.addEventListener('click', () => runSearch({ resetPage: true }));
    input.addEventListener('keydown', (event) => {
      if (event.key !== 'Enter') return;
      event.preventDefault();
      runSearch({ resetPage: true });
    });
    limitSelect?.addEventListener('change', () => {
      if (String(input.value || '').trim()) runSearch({ resetPage: true });
    });
    prevBtn?.addEventListener('click', () => {
      if (state.page <= 0) return;
      state.page -= 1;
      runSearch();
    });
    nextBtn?.addEventListener('click', () => {
      const totalPages = state.pageSize ? Math.max(1, Math.ceil(state.total / state.pageSize)) : 1;
      if (state.page >= totalPages - 1) return;
      state.page += 1;
      runSearch();
    });
    updatePagination();

    document.querySelectorAll('.plex-collapse-btn[data-target^="prowlarr-"]').forEach((buttonEl) => {
      buttonEl.addEventListener('click', () => {
        const targetId = buttonEl.getAttribute('data-target');
        const section = targetId ? document.getElementById(targetId) : null;
        if (!section) return;
        section.classList.toggle('plex-collapsed');
      });
    });
  } catch (err) {
    console.error('Prowlarr overview failed', err);
  }
})();
