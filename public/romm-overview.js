(function () {
  'use strict';

  var carouselFreeScroll = (function () {
    try {
      return localStorage.getItem('launcharr-carousel-free-scroll') === '1';
    } catch (_err) {
      return false;
    }
  })();

  var configs = [];
  if (Array.isArray(window.ROMM_OVERVIEW_CONFIGS)) configs = window.ROMM_OVERVIEW_CONFIGS.slice();
  if (window.ROMM_OVERVIEW_CONFIG && typeof window.ROMM_OVERVIEW_CONFIG === 'object') {
    configs.push(window.ROMM_OVERVIEW_CONFIG);
  }
  if (!configs.length) return;

  function escapeHtml(value) {
    return String(value || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function sectionDisplaySettings(raw) {
    var settings = raw && typeof raw === 'object' ? raw : {};
    return {
      showSubtitle: settings.showSubtitle !== false,
      showMeta: settings.showMeta !== false,
      showPill: settings.showPill !== false,
      showTypeIcon: settings.showTypeIcon !== false,
      showViewIcon: settings.showViewIcon !== false,
      showUsername: settings.showUsername !== false,
    };
  }

  function parseTs(value) {
    var numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric;
    var parsed = Date.parse(String(value || '').trim());
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function parseCount(value) {
    if (value === null || value === undefined || value === '') return 0;
    var numeric = Number(value);
    if (Number.isFinite(numeric)) return Math.max(0, Math.round(numeric));
    var text = String(value || '').trim();
    if (!text) return 0;
    var compact = text.replace(/[\s,]+/g, '');
    if (/^\d+(\.\d+)?$/.test(compact)) {
      var compactNumeric = Number(compact);
      return Number.isFinite(compactNumeric) ? Math.max(0, Math.round(compactNumeric)) : 0;
    }
    var match = text.match(/(\d[\d,]*)/);
    if (!match || !match[1]) return 0;
    var parsed = Number(match[1].replace(/,/g, ''));
    return Number.isFinite(parsed) ? Math.max(0, Math.round(parsed)) : 0;
  }

  function normalizeStats(rawStats) {
    var source = rawStats;
    var output = [];
    var push = function (label, value) {
      var normalizedLabel = String(label || '').trim();
      var normalizedValue = String(value || '').trim();
      if (!normalizedLabel || !normalizedValue) return;
      if (output.some(function (item) { return item.label === normalizedLabel; })) return;
      output.push({ label: normalizedLabel, value: normalizedValue });
    };
    if (Array.isArray(source)) {
      source.forEach(function (entry) {
        if (!entry || typeof entry !== 'object') return;
        push(entry.label || entry.key || entry.name || '', entry.value || entry.count || entry.total || '');
      });
      return output;
    }
    if (source && typeof source === 'object') {
      Object.entries(source).forEach(function (entryPair) {
        var key = String(entryPair[0] || '').trim();
        var value = entryPair[1];
        if (!key) return;
        if (value && typeof value === 'object' && !Array.isArray(value)) {
          var nested = String(value.value || value.count || value.total || '').trim();
          push(key, nested);
          return;
        }
        push(key, value);
      });
      return output;
    }
    return output;
  }

  function relativePill(value) {
    var ts = parseTs(value);
    if (!ts) return '';
    var diffMs = Date.now() - ts;
    var mins = Math.max(1, Math.round(diffMs / 60000));
    if (mins < 60) return mins + 'm ago';
    var hours = Math.round(mins / 60);
    if (hours < 48) return hours + 'h ago';
    var days = Math.round(hours / 24);
    return days + 'd ago';
  }

  function normalizeKind(value, fallback) {
    var text = String(value || '').trim().toLowerCase();
    if (!text) return fallback;
    if (text.indexOf('handheld') >= 0 || text.indexOf('portable') >= 0) return 'handheld';
    if (text.indexOf('console') >= 0 || text.indexOf('system') >= 0 || text.indexOf('platform') >= 0) return 'console';
    if (text.indexOf('bios') >= 0 || text.indexOf('firmware') >= 0) return 'bios';
    if (text.indexOf('homebrew') >= 0) return 'homebrew';
    if (text.indexOf('game') >= 0 || text.indexOf('rom') >= 0) return 'game';
    return fallback;
  }

  function gameIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M6 12h2m2 0h0"></path><path d="M8 10v4"></path><path d="M16 11h.01"></path><path d="M18 13h.01"></path>' +
      '<path d="M6.2 7h11.6a3.2 3.2 0 0 1 3.1 3.8l-1 5a3 3 0 0 1-4.9 1.7l-2.2-1.9a1.2 1.2 0 0 0-1.6 0L9 17.5a3 3 0 0 1-4.9-1.7l-1-5A3.2 3.2 0 0 1 6.2 7z"></path>' +
      '</svg>';
  }

  function chipIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<rect x="7" y="7" width="10" height="10" rx="2"></rect><path d="M9 1v3M15 1v3M9 20v3M15 20v3M20 9h3M20 15h3M1 9h3M1 15h3"></path>' +
      '</svg>';
  }

  function consoleIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<rect x="3" y="7" width="18" height="11" rx="2"></rect><path d="M9 21h6"></path>' +
      '</svg>';
  }

  function handheldIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<rect x="4" y="3" width="16" height="18" rx="2"></rect><path d="M8 8h8"></path><path d="M9 15h.01M15 15h.01"></path>' +
      '</svg>';
  }

  function eyeSvg() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#e8eef7" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12z"></path>' +
      '<circle cx="12" cy="12" r="3"></circle></svg>';
  }

  function rommFallbackArtworkHtml() {
    return '<div class="plex-placeholder plex-placeholder-romm">' +
      '<img class="plex-placeholder-romm-icon" src="/icons/romm.svg" alt="Romm" loading="lazy" onerror="this.onerror=null;this.src=\'/icons/games.svg\'" />' +
      '<div class="plex-placeholder-small">No artwork</div>' +
      '</div>';
  }

  function typeIcon(kind) {
    var value = String(kind || '').toLowerCase();
    if (value === 'bios') return chipIcon();
    if (value === 'console') return consoleIcon();
    if (value === 'handheld') return handheldIcon();
    return gameIcon();
  }

  function kindLabel(kind) {
    var value = String(kind || '').toLowerCase();
    if (value === 'bios') return 'BIOS';
    if (value === 'homebrew') return 'Homebrew';
    if (value === 'console') return 'Console';
    if (value === 'handheld') return 'Handheld';
    return 'Game';
  }

  function matchesSelector(node, selector) {
    if (!node || node.nodeType !== 1) return false;
    var fn = node.matches || node.msMatchesSelector || node.webkitMatchesSelector;
    if (!fn) return false;
    return fn.call(node, selector);
  }

  function closestNode(node, selector, boundary) {
    var cursor = node;
    while (cursor && cursor !== boundary) {
      if (matchesSelector(cursor, selector)) return cursor;
      cursor = cursor.parentNode;
    }
    if (cursor === boundary && matchesSelector(cursor, selector)) return cursor;
    return null;
  }

  var modalRefs = {
    backdrop: null,
    close: null,
    typeIcon: null,
    title: null,
    subtitle: null,
    body: null,
    bound: false,
  };

  function ensureModalRefs() {
    var backdrop = document.getElementById('rommModalBackdrop');
    if (!backdrop) {
      var host = document.createElement('div');
      host.innerHTML =
        '<div id="rommModalBackdrop" class="plex-modal-backdrop plex-hidden">' +
          '<div class="plex-modal">' +
            '<button id="rommModalClose" class="plex-modal-close" aria-label="Close">✕</button>' +
            '<div class="plex-modal-header">' +
              '<div class="plex-modal-title">' +
                '<span id="rommModalTypeIcon" class="plex-mini-icon"></span>' +
                '<span id="rommModalTitle">Loading…</span>' +
              '</div>' +
              '<div id="rommModalSubtitle" class="plex-modal-subtitle"></div>' +
            '</div>' +
            '<div id="rommModalBody" class="plex-modal-body"></div>' +
          '</div>' +
        '</div>';
      if (host.firstElementChild) document.body.appendChild(host.firstElementChild);
      backdrop = document.getElementById('rommModalBackdrop');
    }
    modalRefs.backdrop = backdrop;
    modalRefs.close = document.getElementById('rommModalClose');
    modalRefs.typeIcon = document.getElementById('rommModalTypeIcon');
    modalRefs.title = document.getElementById('rommModalTitle');
    modalRefs.subtitle = document.getElementById('rommModalSubtitle');
    modalRefs.body = document.getElementById('rommModalBody');
    if (!modalRefs.bound && modalRefs.backdrop) {
      if (modalRefs.close) {
        modalRefs.close.addEventListener('click', function (event) {
          event.preventDefault();
          closeModal();
        });
      }
      modalRefs.backdrop.addEventListener('click', function (event) {
        if (event.target === modalRefs.backdrop) closeModal();
      });
      document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape' && modalRefs.backdrop && !modalRefs.backdrop.classList.contains('plex-hidden')) {
          closeModal();
        }
      });
      modalRefs.bound = true;
    }
    return modalRefs;
  }

  function closeModal() {
    var refs = ensureModalRefs();
    if (!refs.backdrop) return;
    refs.backdrop.classList.add('plex-hidden');
  }

  function openModal(item, config) {
    var refs = ensureModalRefs();
    if (!refs.backdrop || !refs.body || !item) return;
    var title = String(item.title || '').trim() || 'Untitled';
    var subtitleBits = [];
    if (item.subtitle) subtitleBits.push(String(item.subtitle));
    if (item.meta) subtitleBits.push(String(item.meta));
    var subtitle = subtitleBits.join(' • ');
    var launchUrl = String(item.launchUrl || buildLaunchUrl(config, item)).trim();
    var stats = normalizeStats(item.stats);
    if (!stats.length) {
      var isConsoleKind = String(item.kind || '').toLowerCase() === 'console' || String(item.kind || '').toLowerCase() === 'handheld';
      var romCount = parseCount(item.romCount || (isConsoleKind ? item.sortTs : 0));
      if (romCount > 0) stats.push({ label: 'ROMs', value: String(romCount) });
    }
    var overview = String(item.overview || '').trim();
    if (!overview && stats.length) {
      overview = 'Library summary: ' + stats.map(function (entry) {
        return String(entry.label || '').trim() + ': ' + String(entry.value || '').trim();
      }).filter(Boolean).join(' • ');
    }
    if (!overview) overview = 'No description available for this title.';
    var poster = item.thumb
      ? '<img src="' + escapeHtml(item.thumb) + '" alt="' + escapeHtml(title) + '" referrerpolicy="no-referrer" onerror="this.onerror=null;this.src=\'/icons/romm.svg\';this.classList.add(\'plex-fallback-art\')" />'
      : rommFallbackArtworkHtml();
    var pills = [
      '<span class="plex-pill2">' + escapeHtml(kindLabel(item.kind)) + '</span>',
      subtitleBits.length ? '<span class="plex-pill2">' + escapeHtml(subtitleBits[0]) + '</span>' : '',
      item.pill ? '<span class="plex-pill2">' + escapeHtml(item.pill) + '</span>' : '',
    ].filter(Boolean).join('');
    var statsSection = stats.length
      ? '<div class="plex-section">' +
          '<h4>Stats</h4>' +
          '<div class="plex-overview-text">' +
            stats.map(function (entry) {
              return escapeHtml(String(entry.label || '').trim() + ': ' + String(entry.value || '').trim());
            }).join('<br>') +
          '</div>' +
        '</div>'
      : '';

    if (refs.typeIcon) refs.typeIcon.innerHTML = typeIcon(item.kind);
    if (refs.title) refs.title.textContent = title;
    if (refs.subtitle) refs.subtitle.textContent = subtitle;
    refs.body.innerHTML =
      '<div class="plex-modal-scroll">' +
        '<div class="plex-modal-hero">' +
          (item.art ? '<img class="plex-modal-bg" src="' + escapeHtml(item.art) + '" alt="" referrerpolicy="no-referrer" />' : '') +
          '<div class="plex-modal-content">' +
            '<div class="plex-modal-poster">' +
              poster +
            '</div>' +
            '<div class="plex-modal-meta">' +
              '<div class="plex-pills">' + pills + '</div>' +
              '<div class="plex-section">' +
                '<h4>Overview</h4>' +
                '<div class="plex-overview-text">' + escapeHtml(overview) + '</div>' +
              '</div>' +
              statsSection +
            '</div>' +
          '</div>' +
        '</div>' +
      '</div>' +
      '<div class="plex-modal-footer">' +
        (launchUrl ? '<a class="plex-modal-link" href="' + escapeHtml(launchUrl) + '" target="_blank" rel="noreferrer">Open in Romm</a>' : '') +
      '</div>';
    refs.backdrop.classList.remove('plex-hidden');
  }

  function fetchJson(url) {
    return fetch(url, {
      credentials: 'same-origin',
      headers: { Accept: 'application/json' },
    })
      .then(function (res) {
        return res.text().then(function (text) {
          var payload = {};
          try {
            payload = text ? JSON.parse(text) : {};
          } catch (_err) {
            payload = {};
          }
          if (!res.ok) {
            var message = payload && payload.error ? payload.error : ('Status ' + res.status);
            throw new Error(message);
          }
          return payload;
        });
      });
  }

  function bindCarousel(viewport, prevBtn, nextBtn) {
    if (!viewport) return;
    var freeScrollMode = carouselFreeScroll;
    var step = function () {
      var card = viewport.querySelector('.plex-card');
      var cardWidth = card ? card.getBoundingClientRect().width : 220;
      return Math.max(220, Math.round(cardWidth + 20));
    };
    var slidePrev = function () {
      viewport.scrollBy({ left: -step(), behavior: 'smooth' });
    };
    var slideNext = function () {
      viewport.scrollBy({ left: step(), behavior: 'smooth' });
    };
    if (freeScrollMode) {
      viewport.style.overflowX = 'auto';
      viewport.style.overflowY = 'hidden';
      viewport.style.scrollBehavior = 'smooth';
      viewport.style.webkitOverflowScrolling = 'touch';
    } else {
      viewport.style.overflowX = '';
      viewport.style.overflowY = '';
      viewport.style.scrollBehavior = '';
      viewport.style.webkitOverflowScrolling = '';
    }
    if (prevBtn) {
      prevBtn.addEventListener('click', function () {
        slidePrev();
      });
    }
    if (nextBtn) {
      nextBtn.addEventListener('click', function () {
        slideNext();
      });
    }

    if (viewport.__rommSwipeBound) return;
    viewport.__rommSwipeBound = true;
    if (freeScrollMode) {
      viewport.style.touchAction = 'pan-x pan-y';
      return;
    }
    viewport.style.touchAction = 'pan-y';
    var startX = 0;
    var startY = 0;
    var movedX = 0;
    var movedY = 0;
    var tracking = false;
    var threshold = 40;

    var isInteractive = function (target) {
      return Boolean(
        target && target.closest
        && target.closest('[data-action="view"], button, a, input, select, textarea, .plex-modal, .plex-modal-backdrop')
      );
    };

    var onStart = function (x, y, target) {
      if (isInteractive(target)) {
        tracking = false;
        return;
      }
      tracking = true;
      startX = x;
      startY = y;
      movedX = 0;
      movedY = 0;
    };

    var onMove = function (x, y) {
      if (!tracking) return;
      movedX = x - startX;
      movedY = y - startY;
    };

    var onEnd = function () {
      if (!tracking) return;
      tracking = false;
      if (Math.abs(movedX) > threshold && Math.abs(movedX) > Math.abs(movedY) * 1.2) {
        if (movedX > 0) slidePrev();
        else slideNext();
      }
    };

    viewport.addEventListener('pointerdown', function (event) {
      onStart(event.clientX, event.clientY, event.target);
      if (tracking && event.pointerType === 'mouse' && viewport.setPointerCapture) viewport.setPointerCapture(event.pointerId);
    });
    viewport.addEventListener('pointermove', function (event) {
      onMove(event.clientX, event.clientY);
    });
    viewport.addEventListener('pointerup', onEnd);
    viewport.addEventListener('pointercancel', function () { tracking = false; });

    viewport.addEventListener('touchstart', function (event) {
      if (!event.touches || !event.touches.length) return;
      var touch = event.touches[0];
      onStart(touch.clientX, touch.clientY, event.target);
    }, { passive: true });
    viewport.addEventListener('touchmove', function (event) {
      if (!event.touches || !event.touches.length) return;
      var touch = event.touches[0];
      onMove(touch.clientX, touch.clientY);
    }, { passive: true });
    viewport.addEventListener('touchend', onEnd);
    viewport.addEventListener('touchcancel', function () { tracking = false; });
  }

  function buildLaunchUrl(config, item) {
    var explicit = String(item.launchUrl || '').trim();
    if (explicit) return explicit;
    var appId = encodeURIComponent(String(config.appId || 'romm').trim() || 'romm');
    var title = encodeURIComponent(String(item.title || '').trim());
    if (!title) return '/apps/' + appId + '/launch';
    return '/apps/' + appId + '/launch?q=' + title;
  }

  function normalizeItem(item, config, fallbackIndex) {
    var raw = item && typeof item === 'object' ? item : {};
    var sectionId = String(config.sectionId || '').trim().toLowerCase();
    var defaultKind = sectionId === 'consoles' ? 'console' : 'game';
    var title = String(raw.title || raw.name || 'Untitled').trim() || 'Untitled';
    var subtitle = String(raw.subtitle || raw.episode || '').trim();
    var meta = String(raw.meta || raw.episodeTitle || raw.quality || '').trim();
    var kind = normalizeKind(raw.kind || raw.type, defaultKind);
    var sortTs = parseTs(raw.sortTs || raw.sortValue || 0);
    var parsedSourceIndex = Number(raw.sourceIndex);
    var sourceIndex = Number.isFinite(parsedSourceIndex) && parsedSourceIndex >= 0
      ? Math.round(parsedSourceIndex)
      : (Number.isFinite(Number(fallbackIndex)) && Number(fallbackIndex) >= 0 ? Math.round(Number(fallbackIndex)) : 0);
    var romCount = parseCount(raw.romCount || raw.romsCount || raw.rom_count || raw.roms_count || raw.totalRoms || raw.total_roms || (defaultKind === 'console' ? raw.sortTs : 0));
    var stats = normalizeStats(raw.stats);
    if (defaultKind === 'console' && !stats.length && romCount > 0) {
      stats.push({ label: 'ROMs', value: String(romCount) });
    }
    var thumb = '';
    if (sectionId === 'recently-added') {
      thumb = String(raw.thumb || raw.poster || raw.cover || '').trim();
    } else {
      thumb = String(raw.thumb || raw.poster || raw.image || raw.art || raw.backdrop || '').trim();
    }
    return {
      id: String(raw.id || title).trim() || title,
      title: title,
      subtitle: subtitle,
      meta: meta,
      pill: String(raw.pill || relativePill(sortTs)).trim(),
      kind: kind,
      user: String(raw.user || '').trim(),
      thumb: thumb,
      art: String(raw.art || raw.backdrop || raw.background || raw.thumb || raw.poster || '').trim(),
      overview: String(raw.overview || raw.description || '').trim(),
      romCount: romCount,
      stats: stats,
      sortTs: sortTs,
      sourceIndex: sourceIndex,
      launchUrl: buildLaunchUrl(config, { title: title, launchUrl: raw.launchUrl }),
    };
  }

  function renderTrack(track, items, settings, emptyMessage) {
    if (!track) return;
    var rows = Array.isArray(items) ? items : [];
    if (!rows.length) {
      track.innerHTML = '<div class="plex-empty">' + escapeHtml(emptyMessage || 'No items found.') + '</div>';
      return;
    }
    track.innerHTML = rows.map(function (item, idx) {
      var subtitle = settings.showSubtitle ? String(item.subtitle || '').trim() : '';
      var metaParts = [];
      if (settings.showMeta && item.meta) metaParts.push(String(item.meta).trim());
      if (settings.showUsername && item.user) metaParts.push('@' + String(item.user).trim());
      var metaLine = metaParts.filter(Boolean).join(' • ');
      var pill = settings.showPill ? String(item.pill || '').trim() : '';
      var icon = settings.showTypeIcon
        ? '<div class="plex-type-icon" title="' + escapeHtml(kindLabel(item.kind)) + '">' + typeIcon(item.kind) + '</div>'
        : '';
      var viewIcon = settings.showViewIcon
        ? '<div class="plex-eye-icon" title="Open Romm" data-action="view">' + eyeSvg() + '</div>'
        : '';
      var poster = item.thumb
        ? '<img src="' + escapeHtml(item.thumb) + '" alt="' + escapeHtml(item.title || 'Poster') + '" loading="lazy" referrerpolicy="no-referrer" onerror="this.onerror=null;this.src=\'/icons/romm.svg\';this.classList.add(\'plex-fallback-art\')" />'
        : rommFallbackArtworkHtml();
      return '' +
        '<div class="plex-card" data-index="' + String(idx) + '">' +
          '<div class="plex-poster-wrap">' +
            '<div class="plex-poster-well">' +
              poster +
              (pill ? '<div class="plex-pill">' + escapeHtml(pill) + '</div>' : '') +
              icon +
              viewIcon +
            '</div>' +
          '</div>' +
          '<div class="plex-footer">' +
            '<div class="plex-name">' + escapeHtml(item.title || 'Untitled') + '</div>' +
            (subtitle ? '<div class="plex-meta">' + escapeHtml(subtitle) + '</div>' : '') +
            (metaLine ? '<div class="plex-meta">' + escapeHtml(metaLine) + '</div>' : '') +
          '</div>' +
        '</div>';
    }).join('');
  }

  function bindOpenAction(track, config) {
    if (!track || track.__rommViewBound) return;
    track.addEventListener('click', function (event) {
      var viewBtn = closestNode(event.target, '[data-action="view"]', track);
      if (!viewBtn) return;
      event.preventDefault();
      event.stopPropagation();
      var card = closestNode(viewBtn, '.plex-card', track);
      if (!card) return;
      var idx = Number(card.getAttribute('data-index'));
      var items = Array.isArray(track.__rommItems) ? track.__rommItems : [];
      if (!Number.isFinite(idx) || !items[idx]) return;
      openModal(items[idx], config);
    });
    track.__rommViewBound = true;
  }

  function matchesType(item, selectedType) {
    if (selectedType === 'all') return true;
    return String(item.kind || '').toLowerCase() === selectedType;
  }

  configs.forEach(function (entry) {
    var config = entry && typeof entry === 'object' ? entry : {};
    var controlPrefix = String(config.controlPrefix || '').trim();
    if (!controlPrefix) return;
    var sectionId = String(config.sectionId || '').trim().toLowerCase();
    if (sectionId !== 'recently-added' && sectionId !== 'consoles') return;
    var endpoint = String(config.endpoint || '').trim();
    if (!endpoint) return;

    var viewport = document.getElementById(controlPrefix + 'Viewport');
    var track = document.getElementById(controlPrefix + 'Track');
    if (!viewport || !track) return;

    var typeFilter = document.getElementById(controlPrefix + 'TypeFilter');
    var limitFilter = document.getElementById(controlPrefix + 'LimitSelect');
    var sortFilter = document.getElementById(controlPrefix + 'SortFilter');
    var settings = sectionDisplaySettings(config.displaySettings);

    track.__rommItems = [];
    bindOpenAction(track, config);
    bindCarousel(
      viewport,
      document.getElementById(controlPrefix + 'PrevBtn'),
      document.getElementById(controlPrefix + 'NextBtn')
    );

    function applyFilters() {
      var sourceItems = Array.isArray(track.__rommItems) ? track.__rommItems : [];
      var selectedType = String(typeFilter && typeFilter.value ? typeFilter.value : 'all').toLowerCase();
      var selectedSort = String(
        sortFilter && sortFilter.value
          ? sortFilter.value
          : (sectionId === 'consoles' ? 'roms-desc' : 'newest')
      ).toLowerCase();
      var limitValue = String(limitFilter && limitFilter.value ? limitFilter.value : '20').trim().toLowerCase();
      var showAll = limitValue === 'all';
      var limit = Number(limitValue);
      if (!showAll && (!Number.isFinite(limit) || limit < 1)) limit = 20;
      var filtered = sourceItems
        .filter(function (item) { return matchesType(item, selectedType); });
      if (sectionId === 'consoles') {
        filtered = filtered.sort(function (left, right) {
          var leftTitle = String(left && left.title || '');
          var rightTitle = String(right && right.title || '');
          var leftCount = parseCount(left && (left.romCount || left.sortTs || 0));
          var rightCount = parseCount(right && (right.romCount || right.sortTs || 0));
          if (selectedSort === 'a-z') return leftTitle.localeCompare(rightTitle);
          if (selectedSort === 'z-a') return rightTitle.localeCompare(leftTitle);
          if (selectedSort === 'roms-asc') {
            if (leftCount !== rightCount) return leftCount - rightCount;
            return leftTitle.localeCompare(rightTitle);
          }
          if (rightCount !== leftCount) return rightCount - leftCount;
          return leftTitle.localeCompare(rightTitle);
        });
      } else {
        filtered = filtered.sort(function (left, right) {
          var leftTitle = String(left && left.title || '');
          var rightTitle = String(right && right.title || '');
          var leftSort = parseTs(left && left.sortTs || 0);
          var rightSort = parseTs(right && right.sortTs || 0);
          var leftSourceIndex = Number(left && left.sourceIndex);
          var rightSourceIndex = Number(right && right.sourceIndex);
          if (!Number.isFinite(leftSourceIndex)) leftSourceIndex = 0;
          if (!Number.isFinite(rightSourceIndex)) rightSourceIndex = 0;
          if (selectedSort === 'oldest') {
            if (leftSort !== rightSort) return leftSort - rightSort;
            if (leftSourceIndex !== rightSourceIndex) return rightSourceIndex - leftSourceIndex;
            return leftTitle.localeCompare(rightTitle);
          }
          if (selectedSort === 'a-z') return leftTitle.localeCompare(rightTitle);
          if (selectedSort === 'z-a') return rightTitle.localeCompare(leftTitle);
          if (rightSort !== leftSort) return rightSort - leftSort;
          if (leftSourceIndex !== rightSourceIndex) return leftSourceIndex - rightSourceIndex;
          return leftTitle.localeCompare(rightTitle);
        });
      }
      if (!showAll) filtered = filtered.slice(0, limit);
      renderTrack(
        track,
        filtered,
        settings,
        sectionId === 'consoles' ? 'No consoles found.' : 'No recently added titles.'
      );
    }

    typeFilter && typeFilter.addEventListener('change', applyFilters);
    limitFilter && limitFilter.addEventListener('change', applyFilters);
    sortFilter && sortFilter.addEventListener('change', applyFilters);

    track.innerHTML = '<div class="plex-empty">Loading…</div>';
    fetchJson(endpoint)
      .then(function (payload) {
        var rows = Array.isArray(payload && payload.items) ? payload.items : [];
        track.__rommItems = rows.map(function (item, index) { return normalizeItem(item, config, index); });
        applyFilters();
      })
      .catch(function (err) {
        track.__rommItems = [];
        track.innerHTML = '<div class="plex-empty">' + escapeHtml(err && err.message ? err.message : 'Failed to load Romm data.') + '</div>';
      });
  });
})();
