(() => {
  const configs = resolveConfigs();
  if (!configs.length) return;

  configs.forEach((config) => {
    try {
      initQueueCard(config);
    } catch (err) {
      console.error('Queue card init failed for', config?.moduleId || config?.endpoint || 'unknown', err);
    }
  });

  function resolveConfigs() {
    const list = [];
    if (Array.isArray(window.QUEUE_CARD_CONFIGS)) {
      window.QUEUE_CARD_CONFIGS.forEach((entry) => {
        if (entry && entry.moduleId && entry.endpoint) list.push(entry);
      });
    }
    if (window.QUEUE_CARD_CONFIG && window.QUEUE_CARD_CONFIG.moduleId && window.QUEUE_CARD_CONFIG.endpoint) {
      list.push(window.QUEUE_CARD_CONFIG);
    }
    const dedupe = new Map();
    list.forEach((entry) => {
      const key = String(entry?.moduleId || '').trim();
      if (!key) return;
      dedupe.set(key, {
        moduleId: key,
        bodyId: String(entry?.bodyId || '').trim(),
        typeFilterId: String(entry?.typeFilterId || '').trim(),
        statusFilterId: String(entry?.statusFilterId || '').trim(),
        endpoint: String(entry?.endpoint || '').trim(),
        appName: String(entry?.appName || 'App').trim() || 'App',
        mode: String(entry?.mode || '').trim(),
      });
    });
    return Array.from(dedupe.values());
  }

  function initQueueCard(config) {
    if (!config.endpoint) return;
    const module = document.getElementById(config.moduleId);
    if (!module) return;
    const table = module.querySelector('.queue-table');
    const body = document.getElementById(config.bodyId);
    if (!table || !body) return;
    const typeFilter = config.typeFilterId ? document.getElementById(config.typeFilterId) : null;
    const statusFilter = config.statusFilterId ? document.getElementById(config.statusFilterId) : null;
    const sortHeaders = Array.from(module.querySelectorAll('.queue-row.header > div'));
    const isReleasesMode = config.mode === 'releases';
    const state = {
      items: [],
      renderedItems: [],
      sortIndex: isReleasesMode ? 0 : 0,
      sortDir: isReleasesMode ? 'desc' : 'asc',
    };
    let actionPopover = null;
    let actionPopoverTrigger = null;

    table.classList.toggle('queue-mode-releases', isReleasesMode);
    syncQueueTableLayout(table);

    sortHeaders.forEach((header, index) => {
      header.classList.add('queue-sortable');
      header.addEventListener('click', () => {
        if (state.sortIndex === index) {
          state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
        } else {
          state.sortIndex = index;
          state.sortDir = 'asc';
        }
        applyFilters();
      });
    });

    typeFilter?.addEventListener('change', applyFilters);
    statusFilter?.addEventListener('change', applyFilters);
    if (isReleasesMode) {
      body.addEventListener('click', onReleaseActionClick);
      table.addEventListener('scroll', closeActionPopover);
      window.addEventListener('resize', closeActionPopover);
      document.addEventListener('pointerdown', onDocumentPointerDown, true);
    }

    loadItems();

    async function loadItems() {
      body.innerHTML = '<div class="queue-empty">Loading...</div>';
      try {
        const response = await fetch(config.endpoint, { headers: { Accept: 'application/json' } });
        if (!response.ok) throw new Error(`Request failed (${response.status})`);
        const payload = await response.json();
        const list = Array.isArray(payload?.items) ? payload.items : [];
        state.items = list.map(normalizeItem).filter(Boolean);
        applyFilters();
      } catch (err) {
        body.innerHTML = `<div class="queue-empty">Unable to load ${escapeHtml(config.appName)} data.</div>`;
      }
    }

    function applyFilters() {
      const typeValue = String(typeFilter?.value || 'all').toLowerCase();
      const statusValue = String(statusFilter?.value || 'all').toLowerCase();
      const filtered = state.items.filter((item) => {
        const typeMatch = typeValue === 'all' || String(item.kind || '').toLowerCase() === typeValue;
        const statusMatch = statusValue === 'all' || (Array.isArray(item.statusKeys)
          ? item.statusKeys.map((entry) => String(entry || '').toLowerCase()).includes(statusValue)
          : String(item.statusKey || '').toLowerCase() === statusValue);
        return typeMatch && statusMatch;
      });
      filtered.sort((a, b) => {
        const left = queueSortValue(a, state.sortIndex, isReleasesMode);
        const right = queueSortValue(b, state.sortIndex, isReleasesMode);
        if (left < right) return state.sortDir === 'asc' ? -1 : 1;
        if (left > right) return state.sortDir === 'asc' ? 1 : -1;
        return 0;
      });
      renderRows(filtered);
    }

    function renderRows(items) {
      state.renderedItems = Array.isArray(items) ? items.slice() : [];
      closeActionPopover();
      if (!items.length) {
        body.innerHTML = '<div class="queue-empty">No items available.</div>';
        syncQueueTableLayout(table);
        return;
      }
      if (isReleasesMode) {
        body.innerHTML = items.map((item, index) => `
          <div class="queue-row" data-index="${index}">
            <div class="queue-col-detail rls-age">${escapeHtml(item.age || '-')}</div>
            <div class="queue-col-title"><div class="rls-name">${escapeHtml(item.title || 'Unknown')}</div><div class="rls-sub">${escapeHtml(item.subDetailLine || '')}</div></div>
            <div class="queue-col-links">${renderReleaseLinks(item.links)}</div>
            <div class="queue-col-actions">${renderReleaseActions(item.actions, index)}</div>
            <div class="queue-col-size rls-indexer">${escapeHtml(item.indexer || '-')}</div>
          </div>
        `).join('');
      } else {
        body.innerHTML = items.map((item, index) => {
          const progress = Math.max(0, Math.min(100, Number(item.progress || 0)));
          const protocol = escapeHtml(item.protocol || '-');
          const protocolClass = String(item.protocol || '').toLowerCase() === 'usenet' ? ' usenet' : '';
          return `
            <div class="queue-row" data-index="${index}">
              <div class="queue-col-title">${escapeHtml(item.title || 'Unknown')}</div>
              <div class="queue-col-detail queue-episode">${escapeHtml(item.episode || '-')}</div>
              <div class="queue-col-subdetail queue-ep-title">${escapeHtml(item.episodeTitle || '-')}</div>
              <div class="queue-col-size"><span class="queue-quality">${escapeHtml(item.quality || '-')}</span></div>
              <div class="queue-col-protocol queue-protocol${protocolClass}">${protocol}</div>
              <div class="queue-col-time queue-time">${escapeHtml(item.timeLeft || '-')}</div>
              <div class="queue-col-progress queue-progress"><span style="width:${progress}%"></span></div>
            </div>
          `;
        }).join('');
      }
      syncQueueTableLayout(table);
    }

    function syncQueueTableLayout(table) {
      if (!table) return;
      if (isReleasesMode) {
        table.style.setProperty('--queue-grid-template', '100px minmax(620px, 2.2fr) 120px 120px minmax(200px, 280px)');
        setQueueColumnDisplay(table, '.queue-col-detail', true);
        setQueueColumnDisplay(table, '.queue-col-title', true);
        setQueueColumnDisplay(table, '.queue-col-links', true);
        setQueueColumnDisplay(table, '.queue-col-actions', true);
        setQueueColumnDisplay(table, '.queue-col-size', true);
        setQueueColumnDisplay(table, '.queue-col-subdetail', false);
        setQueueColumnDisplay(table, '.queue-col-protocol', false);
        setQueueColumnDisplay(table, '.queue-col-time', false);
        setQueueColumnDisplay(table, '.queue-col-progress', false);
        return;
      }
      const visibility = queueColumnVisibility(table);
      table.style.setProperty('--queue-grid-template', buildQueueGridTemplate(visibility));
      setQueueColumnDisplay(table, '.queue-col-detail', visibility.detail);
      setQueueColumnDisplay(table, '.queue-col-subdetail', visibility.subdetail);
      setQueueColumnDisplay(table, '.queue-col-size', visibility.size);
      setQueueColumnDisplay(table, '.queue-col-protocol', visibility.protocol);
      setQueueColumnDisplay(table, '.queue-col-time', visibility.timeleft);
      setQueueColumnDisplay(table, '.queue-col-progress', visibility.progress);
    }

    function onReleaseActionClick(event) {
      const trigger = event.target.closest('[data-release-action-trigger]');
      if (!trigger || !body.contains(trigger)) return;
      event.preventDefault();
      event.stopPropagation();
      if (trigger.disabled) return;
      const rowIndex = Number(trigger.getAttribute('data-row-index'));
      const actionIndex = Number(trigger.getAttribute('data-action-index'));
      if (!Number.isInteger(rowIndex) || rowIndex < 0) return;
      if (actionPopover && actionPopoverTrigger === trigger) {
        closeActionPopover();
        return;
      }
      const item = state.renderedItems[rowIndex];
      if (!item) return;
      openActionPopover(trigger, item, actionIndex);
    }

    function onDocumentPointerDown(event) {
      if (!actionPopover) return;
      const target = event.target;
      if (actionPopover.contains(target)) return;
      if (actionPopoverTrigger && actionPopoverTrigger.contains(target)) return;
      closeActionPopover();
    }

    function openActionPopover(trigger, item, actionIndex) {
      closeActionPopover();
      const actions = Array.isArray(item.actions) ? item.actions : [];
      const action = actions[actionIndex] || actions[0] || null;
      const meta = item?.actionMeta && typeof item.actionMeta === 'object' ? item.actionMeta : {};
      actionPopover = document.createElement('div');
      actionPopover.className = 'queue-action-popover';
      actionPopover.innerHTML = renderReleaseActionPopover(item, action, meta);
      document.body.appendChild(actionPopover);
      actionPopoverTrigger = trigger;
      trigger.setAttribute('aria-expanded', 'true');
      positionActionPopover(trigger, actionPopover);
      const retryBtn = actionPopover.querySelector('[data-action-popover-close]');
      retryBtn?.addEventListener('click', () => closeActionPopover());
    }

    function positionActionPopover(trigger, popover) {
      if (!trigger || !popover) return;
      const margin = 10;
      const rect = trigger.getBoundingClientRect();
      const width = popover.offsetWidth || 300;
      const height = popover.offsetHeight || 180;
      let left = rect.right + 10;
      if ((left + width) > (window.innerWidth - margin)) {
        left = rect.left - width - 10;
      }
      if (left < margin) left = margin;
      let top = rect.top - 12;
      if ((top + height) > (window.innerHeight - margin)) {
        top = window.innerHeight - height - margin;
      }
      if (top < margin) top = margin;
      popover.style.left = `${Math.round(left)}px`;
      popover.style.top = `${Math.round(top)}px`;
    }

    function closeActionPopover() {
      if (actionPopoverTrigger) actionPopoverTrigger.setAttribute('aria-expanded', 'false');
      actionPopoverTrigger = null;
      if (actionPopover && actionPopover.parentNode) actionPopover.parentNode.removeChild(actionPopover);
      actionPopover = null;
    }
  }

  function normalizeItem(item) {
    if (!item || typeof item !== 'object') return null;
    const statusKey = String(item.statusKey || '').trim().toLowerCase() || 'queued';
    const statusKeys = Array.isArray(item.statusKeys) && item.statusKeys.length
      ? item.statusKeys.map((entry) => String(entry || '').trim().toLowerCase()).filter(Boolean)
      : [statusKey];
    const timestamp = String(item.timestamp || '').trim();
    const actionMeta = item?.actionMeta && typeof item.actionMeta === 'object'
      ? {
          status: String(item.actionMeta.status || '').trim(),
          app: String(item.actionMeta.app || '').trim(),
          type: String(item.actionMeta.type || '').trim(),
          filter: String(item.actionMeta.filter || '').trim(),
          time: String(item.actionMeta.time || '').trim(),
          reason: String(item.actionMeta.reason || '').trim(),
        }
      : null;
    return {
      kind: String(item.kind || 'other').trim().toLowerCase() || 'other',
      title: String(item.title || '').trim() || 'Unknown',
      episode: String(item.episode || '').trim() || '-',
      episodeTitle: String(item.episodeTitle || '').trim() || '-',
      quality: String(item.quality || '').trim() || '-',
      protocol: String(item.protocol || '').trim() || '-',
      timeLeft: String(item.timeLeft || '').trim() || '-',
      progress: Number.isFinite(Number(item.progress)) ? Number(item.progress) : 0,
      statusKey,
      statusKeys,
      timestamp,
      age: timestamp ? formatRelativeAge(timestamp) : '-',
      subDetailLine: String(item.subDetailLine || '').trim(),
      indexer: String(item.indexer || item.episode || '').trim() || '-',
      actionMeta,
      links: Array.isArray(item.links) ? item.links.map((entry) => ({
        kind: String(entry?.kind || '').trim().toLowerCase() || 'link',
        label: String(entry?.label || entry?.kind || 'Link').trim() || 'Link',
        href: String(entry?.href || '').trim(),
      })).filter((entry) => /^https?:\/\//i.test(entry.href)) : [],
      actions: Array.isArray(item.actions) ? item.actions.map((entry) => ({
        kind: String(entry?.kind || '').trim().toLowerCase() || 'status',
        label: String(entry?.label || entry?.kind || 'Action').trim() || 'Action',
        disabled: entry?.disabled !== false,
      })) : [],
    };
  }

  function formatRelativeAge(ts) {
    if (!ts) return '-';
    const ms = Date.now() - new Date(ts).getTime();
    if (!Number.isFinite(ms) || ms < 0) return '-';
    const minutes = Math.floor(ms / 60000);
    if (minutes < 60) return minutes + 'm';
    const hours = Math.floor(ms / 3600000);
    if (hours < 24) return hours + 'h';
    const days = Math.floor(ms / 86400000);
    return days + 'd';
  }

  function queueSortValue(item, index, isReleasesMode) {
    if (isReleasesMode) {
      // columns: 0=age(timestamp), 1=release(title), 2=links(count), 3=actions(label), 4=indexer
      switch (index) {
        case 0: return String(item.timestamp || '').toLowerCase();
        case 1: return String(item.title || '').toLowerCase();
        case 2: return Number(Array.isArray(item.links) ? item.links.length : 0);
        case 3: return String((Array.isArray(item.actions) && item.actions[0]?.kind) || '').toLowerCase();
        case 4: return String(item.indexer || '').toLowerCase();
        default: return String(item.timestamp || '').toLowerCase();
      }
    }
    switch (index) {
      case 0:
        return String(item.title || '').toLowerCase();
      case 1:
        return String(item.episode || '').toLowerCase();
      case 2:
        return String(item.episodeTitle || '').toLowerCase();
      case 3:
        return String(item.quality || '').toLowerCase();
      case 4:
        return String(item.protocol || '').toLowerCase();
      case 5:
        return String(item.timeLeft || '').toLowerCase();
      case 6:
        return Number(item.progress || 0);
      default:
        return String(item.title || '').toLowerCase();
    }
  }

  function queueColumnVisibility(table) {
    return {
      detail: !table.classList.contains('queue-hide-detail'),
      subdetail: !table.classList.contains('queue-hide-subdetail'),
      size: !table.classList.contains('queue-hide-size'),
      protocol: !table.classList.contains('queue-hide-protocol'),
      timeleft: !table.classList.contains('queue-hide-timeleft'),
      progress: !table.classList.contains('queue-hide-progress'),
    };
  }

  function buildQueueGridTemplate(visibility) {
    const columns = ['minmax(220px, 1fr)'];
    if (visibility.detail) columns.push('140px');
    if (visibility.subdetail) columns.push('160px');
    if (visibility.size) columns.push('130px');
    if (visibility.protocol) columns.push('116px');
    if (visibility.timeleft) columns.push('110px');
    if (visibility.progress) columns.push('170px');
    return columns.join(' ');
  }

  function setQueueColumnDisplay(table, selector, show) {
    table.querySelectorAll(selector).forEach((cell) => {
      cell.style.display = show ? '' : 'none';
    });
  }

  function renderReleaseLinks(links) {
    const items = Array.isArray(links) ? links : [];
    if (!items.length) return '<span class="queue-icon-muted">-</span>';
    return items.slice(0, 4).map((entry) => {
      const kind = String(entry?.kind || '').trim().toLowerCase();
      const label = escapeHtml(String(entry?.label || 'Link'));
      const href = escapeHtml(String(entry?.href || ''));
      const glyph = kind === 'download' ? '↓' : (kind === 'open' ? '↗' : '⧉');
      return `<a class="queue-icon-link queue-icon-link--${escapeHtml(kind || 'link')}" href="${href}" target="_blank" rel="noreferrer" title="${label}" aria-label="${label}">${glyph}</a>`;
    }).join('');
  }

  function renderReleaseActions(actions, rowIndex) {
    const items = Array.isArray(actions) ? actions : [];
    if (!items.length) return '<span class="queue-icon-muted">-</span>';
    return items.slice(0, 3).map((entry, actionIndex) => {
      const kind = String(entry?.kind || 'status').trim().toLowerCase();
      const label = escapeHtml(String(entry?.label || 'Action'));
      const glyph = kind === 'block' ? '⊘' : '◌';
      const disabled = entry?.disabled !== false;
      if (disabled) {
        return `<button type="button" class="queue-icon-link queue-icon-action queue-icon-action--${escapeHtml(kind)}${disabled ? ' is-disabled' : ''}" data-release-action-trigger data-row-index="${Number.isInteger(rowIndex) ? rowIndex : 0}" data-action-index="${actionIndex}" title="${label}" aria-label="${label}" aria-expanded="false">${glyph}</button>`;
      }
      return `<button type="button" class="queue-icon-link queue-icon-action queue-icon-action--${escapeHtml(kind)}" data-release-action-trigger data-row-index="${Number.isInteger(rowIndex) ? rowIndex : 0}" data-action-index="${actionIndex}" title="${label}" aria-label="${label}" aria-expanded="false">${glyph}</button>`;
    }).join('');
  }

  function renderReleaseActionPopover(item, action, meta) {
    const statusText = String(meta?.status || action?.label || action?.kind || 'unknown').trim();
    const appName = String(meta?.app || item?.kind || '').trim();
    const typeText = String(meta?.type || '').trim();
    const filterText = String(meta?.filter || '').trim();
    const timeText = String(meta?.time || item?.timestamp || '').trim();
    const reasonText = String(meta?.reason || '').trim();
    const statusLabel = /^rejected?$/i.test(statusText) ? 'Rejected' : 'Status';
    const rows = [
      ['Type', typeText],
      ['Filter', filterText],
      ['Time', timeText ? formatPopupDateTime(timeText) : ''],
      [statusLabel, reasonText || statusText],
    ].filter((entry) => String(entry[1] || '').trim());
    const titleParts = [];
    if (statusText) titleParts.push(`Action ${statusText.toLowerCase()}`);
    else titleParts.push('Action');
    if (appName) titleParts.push(`: ${appName}`);
    return `
      <div class="queue-action-popover-header">
        <div class="queue-action-popover-title">${escapeHtml(titleParts.join(''))}</div>
        <button type="button" class="queue-action-popover-close" data-action-popover-close aria-label="Close action details">✕</button>
      </div>
      <div class="queue-action-popover-body">
        ${rows.map(([label, value]) => `
          <div class="queue-action-popover-row">
            <span class="queue-action-popover-key">${escapeHtml(label)}:</span>
            <span class="queue-action-popover-value">${escapeHtml(value)}</span>
          </div>
        `).join('') || '<div class="queue-action-popover-row"><span class="queue-action-popover-value">No action details available.</span></div>'}
      </div>
    `;
  }

  function formatPopupDateTime(value) {
    const raw = String(value || '').trim();
    if (!raw) return '';
    const parsed = new Date(raw);
    if (Number.isNaN(parsed.getTime())) return raw;
    const yyyy = parsed.getFullYear();
    const mm = String(parsed.getMonth() + 1).padStart(2, '0');
    const dd = String(parsed.getDate()).padStart(2, '0');
    const hh = String(parsed.getHours()).padStart(2, '0');
    const mi = String(parsed.getMinutes()).padStart(2, '0');
    const ss = String(parsed.getSeconds()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
  }

  function escapeHtml(value) {
    return String(value || '').replace(/[&<>"']/g, (char) => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      '\'': '&#039;',
    }[char]));
  }
})();
