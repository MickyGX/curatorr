(() => {
  const TABLE_SELECTOR = '.queue-table:not(.queue-table--manual)';
  const STATE_BY_TABLE = new WeakMap();
  let refreshQueued = false;

  function parseVisibleRows(table) {
    if (!table || typeof window.getComputedStyle !== 'function') return 10;
    const raw = window.getComputedStyle(table).getPropertyValue('--queue-visible-rows');
    const parsed = Number.parseInt(String(raw || '').trim(), 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 10;
  }

  function parseLinkedPageSize(table, fallback) {
    if (!table) return fallback;
    const linkedId = String(table.getAttribute('data-page-size-select-id') || '').trim();
    if (!linkedId) return fallback;
    const selectEl = document.getElementById(linkedId);
    const parsed = Number.parseInt(String(selectEl?.value || '').trim(), 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  }

  function queueVisibility(table) {
    return {
      detail: !table.classList.contains('queue-hide-detail'),
      subdetail: !table.classList.contains('queue-hide-subdetail'),
      size: !table.classList.contains('queue-hide-size'),
      protocol: !table.classList.contains('queue-hide-protocol'),
      timeleft: !table.classList.contains('queue-hide-timeleft'),
      progress: !table.classList.contains('queue-hide-progress'),
    };
  }

  function buildTemplate(visibility) {
    const columns = ['minmax(220px, 1fr)'];
    if (visibility.detail) columns.push('140px');
    if (visibility.subdetail) columns.push('160px');
    if (visibility.size) columns.push('130px');
    if (visibility.protocol) columns.push('116px');
    if (visibility.timeleft) columns.push('110px');
    if (visibility.progress) columns.push('170px');
    return columns.join(' ');
  }

  function toggleColumn(table, selector, show) {
    table.querySelectorAll(selector).forEach((cell) => {
      cell.style.display = show ? '' : 'none';
    });
  }

  function enforceQueueLayout(table) {
    if (!table) return;
    if (table.classList.contains('queue-mode-releases')) {
      table.style.setProperty('--queue-grid-template', '100px minmax(620px, 2.2fr) 120px 120px minmax(200px, 280px)');
      toggleColumn(table, '.queue-col-detail', true);
      toggleColumn(table, '.queue-col-title', true);
      toggleColumn(table, '.queue-col-links', true);
      toggleColumn(table, '.queue-col-actions', true);
      toggleColumn(table, '.queue-col-size', true);
      toggleColumn(table, '.queue-col-subdetail', false);
      toggleColumn(table, '.queue-col-protocol', false);
      toggleColumn(table, '.queue-col-time', false);
      toggleColumn(table, '.queue-col-progress', false);
      return;
    }
    const visibility = queueVisibility(table);
    table.style.setProperty('--queue-grid-template', buildTemplate(visibility));
    toggleColumn(table, '.queue-col-detail', visibility.detail);
    toggleColumn(table, '.queue-col-subdetail', visibility.subdetail);
    toggleColumn(table, '.queue-col-size', visibility.size);
    toggleColumn(table, '.queue-col-protocol', visibility.protocol);
    toggleColumn(table, '.queue-col-time', visibility.timeleft);
    toggleColumn(table, '.queue-col-progress', visibility.progress);
  }

  function ensureControls(table, state) {
    const panel = table.closest('.queue-panel');
    if (!panel) return null;
    let root = panel.querySelector('.queue-pagination');
    if (!root) {
      root = document.createElement('div');
      root.className = 'queue-pagination';
      root.innerHTML =
        '<button class="plex-iconbtn queue-page-prev" type="button" aria-label="Previous page"><div class="plex-chev plex-left"></div></button>' +
        '<span class="queue-page-label">Page 1</span>' +
        '<button class="plex-iconbtn queue-page-next" type="button" aria-label="Next page"><div class="plex-chev plex-right"></div></button>';
      panel.appendChild(root);
    }
    const controls = {
      root,
      prevBtn: root.querySelector('.queue-page-prev'),
      nextBtn: root.querySelector('.queue-page-next'),
      label: root.querySelector('.queue-page-label'),
    };
    if (!root.dataset.paginationBound) {
      controls.prevBtn?.addEventListener('click', () => {
        if (state.page <= 0) return;
        state.page -= 1;
        applyPagination(table, state);
      });
      controls.nextBtn?.addEventListener('click', () => {
        const totalPages = Math.max(1, Math.ceil((state.totalRows || 0) / Math.max(1, state.pageSize)));
        if (state.page >= totalPages - 1) return;
        state.page += 1;
        applyPagination(table, state);
      });
      root.dataset.paginationBound = '1';
    }
    return controls;
  }

  function applyPagination(table, state) {
    const body = table.querySelector('.queue-body');
    if (!body) return;

    enforceQueueLayout(table);

    const rows = Array.from(body.querySelectorAll('.queue-row'));
    const visibleRows = parseVisibleRows(table);
    state.pageSize = parseLinkedPageSize(table, visibleRows);
    state.totalRows = rows.length;
    const totalPages = rows.length ? Math.max(1, Math.ceil(rows.length / Math.max(1, state.pageSize))) : 1;
    if (state.page > totalPages - 1) state.page = totalPages - 1;
    if (state.page < 0) state.page = 0;
    const start = state.page * state.pageSize;
    const end = start + state.pageSize;

    rows.forEach((row, index) => {
      row.style.display = index >= start && index < end ? '' : 'none';
    });

    const controls = state.controls;
    if (controls?.label) {
      const current = rows.length ? state.page + 1 : 1;
      controls.label.textContent = `Page ${current}${rows.length ? ` of ${totalPages}` : ''}`;
    }
    if (controls?.prevBtn) controls.prevBtn.disabled = rows.length === 0 || state.page <= 0;
    if (controls?.nextBtn) controls.nextBtn.disabled = rows.length === 0 || state.page >= totalPages - 1;
  }

  function initTable(table) {
    if (!table) return;
    let state = STATE_BY_TABLE.get(table);
    if (!state) {
      state = {
        page: 0,
        pageSize: parseVisibleRows(table),
        totalRows: 0,
        observer: null,
        controls: null,
      };
      state.controls = ensureControls(table, state);
      const linkedId = String(table.getAttribute('data-page-size-select-id') || '').trim();
      if (linkedId) {
        const linkedSelect = document.getElementById(linkedId);
        if (linkedSelect && linkedSelect.dataset.queuePageSizeBound !== '1') {
          linkedSelect.addEventListener('change', () => {
            state.page = 0;
            applyPagination(table, state);
          });
          linkedSelect.dataset.queuePageSizeBound = '1';
        }
      }
      const body = table.querySelector('.queue-body');
      if (body) {
        const observer = new MutationObserver(() => {
          state.page = 0;
          applyPagination(table, state);
        });
        observer.observe(body, { childList: true });
        state.observer = observer;
      }
      STATE_BY_TABLE.set(table, state);
    }
    applyPagination(table, state);
  }

  function refreshAll() {
    document.querySelectorAll(TABLE_SELECTOR).forEach(initTable);
  }

  function queueRefresh() {
    if (refreshQueued) return;
    refreshQueued = true;
    requestAnimationFrame(() => {
      refreshQueued = false;
      refreshAll();
    });
  }

  const rootObserver = new MutationObserver(queueRefresh);
  const start = () => {
    refreshAll();
    rootObserver.observe(document.body, { childList: true, subtree: true });
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start, { once: true });
  } else {
    start();
  }
})();
