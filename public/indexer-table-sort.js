(() => {
  const SORTABLE_COLUMNS = new Set([0, 1, 2, 3]);

  function parseNumber(value) {
    const normalized = String(value || '').replace(/,/g, '').trim();
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : NaN;
  }

  function parsePeers(value) {
    const match = String(value || '').match(/([0-9,]+)\s*\/\s*([0-9,]+)/);
    if (!match) return NaN;
    return parseNumber(match[1]);
  }

  function parseSize(value) {
    const match = String(value || '').trim().match(/([0-9]+(?:\.[0-9]+)?)\s*([KMGTP]?B)\b/i);
    if (!match) return NaN;
    const amount = Number(match[1]);
    if (!Number.isFinite(amount)) return NaN;
    const unit = String(match[2] || 'B').toUpperCase();
    const factors = {
      B: 1,
      KB: 1024,
      MB: 1024 ** 2,
      GB: 1024 ** 3,
      TB: 1024 ** 4,
      PB: 1024 ** 5,
    };
    return amount * (factors[unit] || 1);
  }

  function sortValue(row, index) {
    const cell = row?.children?.[index];
    const text = String(cell?.textContent || '').trim();
    if (index === 1) return parsePeers(text);
    if (index === 2) return parseSize(text);
    return text.toLowerCase();
  }

  function compareValues(left, right, direction) {
    const leftNum = typeof left === 'number' ? left : NaN;
    const rightNum = typeof right === 'number' ? right : NaN;
    const numeric = Number.isFinite(leftNum) && Number.isFinite(rightNum);
    let result = 0;
    if (numeric) {
      if (leftNum < rightNum) result = -1;
      else if (leftNum > rightNum) result = 1;
    } else {
      result = String(left || '').localeCompare(String(right || ''), undefined, { sensitivity: 'base' });
    }
    return direction === 'desc' ? -result : result;
  }

  function initTableSort(table) {
    if (!table || table.dataset.indexerSortBound === '1') return;
    const headerRow = table.querySelector('.indexer-head') || table.querySelector('.prowlarr-row--head');
    const results = table.querySelector('.indexer-results') || table.querySelector('.prowlarr-results');
    if (!headerRow || !results) return;

    const headerCells = Array.from(headerRow.children || []);
    if (!headerCells.length) return;

    const state = {
      index: -1,
      direction: 'asc',
      applying: false,
    };

    const updateArrows = () => {
      headerCells.forEach((cell, index) => {
        if (!SORTABLE_COLUMNS.has(index)) return;
        const arrow = cell.querySelector('.indexer-sort-arrow');
        if (!arrow) return;
        const active = state.index === index;
        cell.classList.toggle('is-sorted', active);
        cell.dataset.sortDir = active ? state.direction : 'none';
        arrow.textContent = active ? (state.direction === 'asc' ? '↑' : '↓') : '↕';
      });
    };

    const applySort = () => {
      if (state.index < 0) return;
      const rows = Array.from(results.querySelectorAll('.indexer-row:not(.indexer-row--empty), .prowlarr-row:not(.prowlarr-row--empty)'));
      if (rows.length < 2) return;
      rows.sort((a, b) => compareValues(sortValue(a, state.index), sortValue(b, state.index), state.direction));
      state.applying = true;
      rows.forEach((row) => results.appendChild(row));
      state.applying = false;
    };

    headerCells.forEach((cell, index) => {
      if (!SORTABLE_COLUMNS.has(index)) return;
      cell.classList.add('indexer-sortable');
      cell.setAttribute('role', 'button');
      cell.setAttribute('tabindex', '0');
      if (!cell.querySelector('.indexer-sort-arrow')) {
        const arrow = document.createElement('span');
        arrow.className = 'indexer-sort-arrow';
        arrow.textContent = '↕';
        arrow.setAttribute('aria-hidden', 'true');
        cell.appendChild(arrow);
      }
      const activate = () => {
        if (state.index === index) {
          state.direction = state.direction === 'asc' ? 'desc' : 'asc';
        } else {
          state.index = index;
          state.direction = 'asc';
        }
        updateArrows();
        applySort();
      };
      cell.addEventListener('click', activate);
      cell.addEventListener('keydown', (event) => {
        if (event.key !== 'Enter' && event.key !== ' ') return;
        event.preventDefault();
        activate();
      });
    });

    const observer = new MutationObserver(() => {
      if (state.applying) return;
      if (state.index >= 0) applySort();
      updateArrows();
    });
    observer.observe(results, { childList: true });

    table.dataset.indexerSortBound = '1';
    updateArrows();
  }

  function init() {
    document.querySelectorAll('.indexer-table, .prowlarr-table').forEach(initTableSort);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();
