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
  if (Array.isArray(window.MAINTAINERR_OVERVIEW_CONFIGS)) configs = window.MAINTAINERR_OVERVIEW_CONFIGS.slice();
  if (window.MAINTAINERR_OVERVIEW_CONFIG && typeof window.MAINTAINERR_OVERVIEW_CONFIG === 'object') {
    configs.push(window.MAINTAINERR_OVERVIEW_CONFIG);
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

  function parseTs(value) {
    var numeric = Number(value);
    if (Number.isFinite(numeric) && numeric > 0) {
      if (numeric > 1e11) return Math.round(numeric);
      return Math.round(numeric * 1000);
    }
    var parsed = Date.parse(String(value || '').trim());
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function normalizeLetter(value) {
    var text = String(value || '').trim();
    if (!text) return '#';
    var first = text.charAt(0).toUpperCase();
    return /[A-Z]/.test(first) ? first : '#';
  }

  function normalizeKind(value, fallback) {
    var text = String(value || '').trim().toLowerCase();
    if (!text) return fallback;
    if (text === 'movie' || text === 'movies' || text === '1') return 'movie';
    if (text === 'show' || text === 'shows' || text === 'tv' || text === 'series' || text === '2') return 'show';
    if (text === 'season' || text === 'episode') return 'show';
    return fallback;
  }

  function kindLabel(kind) {
    return String(kind || '').toLowerCase() === 'show' ? 'TV' : 'Movie';
  }

  function movieIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M5 9h14a1 1 0 0 1 1 1v9a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2v-9a1 1 0 0 1 1-1z"></path>' +
      '<path d="M5 13h15"></path><path d="M4.5 9.2 3.8 7a1.7 1.7 0 0 1 1.1-2.1L16.8 1a1.7 1.7 0 0 1 2.1 1.1l.7 2.2"></path>' +
      '<path d="m6.2 9.1 2.5-2.6"></path><path d="m9.8 9.1 2.5-2.6"></path><path d="m13.4 9.1 2.5-2.6"></path><path d="m17 9.1 2.2-2.3"></path>' +
      '<path d="m7.4 4 2.7 2.2"></path><path d="m11 2.8 2.7 2.2"></path><path d="m14.6 1.6 2.7 2.2"></path>' +
      '</svg>';
  }

  function tvIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<rect x="2" y="6" width="20" height="13" rx="2"></rect><path d="M8 3l4 3 4-3"></path><path d="M7 21h10"></path>' +
      '</svg>';
  }

  function eyeSvg() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#e8eef7" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12z"></path>' +
      '<circle cx="12" cy="12" r="3"></circle></svg>';
  }

  function playSvg() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#e8eef7" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<polygon points="8,6 19,12 8,18"></polygon></svg>';
  }

  function typeIcon(kind) {
    return String(kind || '').toLowerCase() === 'show' ? tvIcon() : movieIcon();
  }

  function formatDurationLabel(secondsValue) {
    var seconds = Number(secondsValue);
    if (!Number.isFinite(seconds) || seconds <= 0) return '';
    var total = Math.max(1, Math.round(seconds));
    var hours = Math.floor(total / 3600);
    var minutes = Math.floor((total % 3600) / 60);
    var secondsRemainder = total % 60;
    if (hours > 0) return String(hours) + 'h ' + String(minutes) + 'm';
    if (minutes > 0) return String(minutes) + 'm ' + String(secondsRemainder) + 's';
    return String(secondsRemainder) + 's';
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

  function maintainerrFallbackArtworkHtml(variant) {
    var mode = String(variant || 'card').trim().toLowerCase() === 'modal' ? 'modal' : 'card';
    return '<div class="plex-placeholder plex-placeholder-romm maintainerr-fallback maintainerr-fallback--' + mode + '">' +
      '<img class="plex-placeholder-romm-icon maintainerr-fallback-icon maintainerr-fallback-icon--' + mode + '" src="/icons/maintainerr.svg" alt="Maintainerr" loading="lazy" onerror="this.onerror=null;this.src=\'/icons/app.svg\'" />' +
      '</div>';
  }

  function isFallbackIconUrl(url) {
    var text = String(url || '').trim().toLowerCase();
    if (!text) return false;
    return /\/icons\/(maintainerr|app)\.svg(?:\?|$)/.test(text);
  }

  function isLibraryAssetThumb(url) {
    var text = String(url || '').trim();
    if (!text) return false;
    try {
      var parsed = new URL(text, window.location.origin);
      return /\/library\/metadata\/\d+\/(?:thumb|art|clearlogo)(?:\/\d+)?$/i.test(parsed.pathname);
    } catch (_err) {
      return /\/library\/metadata\/\d+\/(?:thumb|art|clearlogo)(?:\/\d+)?$/i.test(text);
    }
  }

  function fetchJson(url, options) {
    return fetch(url, Object.assign({
      credentials: 'same-origin',
      headers: { Accept: 'application/json' },
    }, options || {}))
      .then(function (res) {
        return res.text().then(function (text) {
          var payload = {};
          try {
            payload = text ? JSON.parse(text) : {};
          } catch (_err) {
            payload = {};
          }
          if (!res.ok) {
            var message = payload && (payload.error || payload.message) ? (payload.error || payload.message) : ('Status ' + res.status);
            throw new Error(message);
          }
          return payload;
        });
      });
  }

  function appendQuery(url, params) {
    var parsed = new URL(url, window.location.origin);
    Object.entries(params || {}).forEach(function (entry) {
      if (entry[1] === undefined || entry[1] === null || entry[1] === '') return;
      parsed.searchParams.set(entry[0], String(entry[1]));
    });
    return parsed.pathname + parsed.search;
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
    var backdrop = document.getElementById('maintainerrModalBackdrop');
    if (!backdrop) {
      var host = document.createElement('div');
      host.innerHTML =
        '<div id="maintainerrModalBackdrop" class="plex-modal-backdrop plex-hidden">' +
          '<div class="plex-modal">' +
            '<button id="maintainerrModalClose" class="plex-modal-close" aria-label="Close">✕</button>' +
            '<div class="plex-modal-header">' +
              '<div class="plex-modal-title">' +
                '<span id="maintainerrModalTypeIcon" class="plex-mini-icon"></span>' +
                '<span id="maintainerrModalTitle">Loading…</span>' +
              '</div>' +
              '<div id="maintainerrModalSubtitle" class="plex-modal-subtitle"></div>' +
            '</div>' +
            '<div id="maintainerrModalBody" class="plex-modal-body"></div>' +
          '</div>' +
        '</div>';
      if (host.firstElementChild) document.body.appendChild(host.firstElementChild);
      backdrop = document.getElementById('maintainerrModalBackdrop');
    }

    modalRefs.backdrop = backdrop;
    modalRefs.close = document.getElementById('maintainerrModalClose');
    modalRefs.typeIcon = document.getElementById('maintainerrModalTypeIcon');
    modalRefs.title = document.getElementById('maintainerrModalTitle');
    modalRefs.subtitle = document.getElementById('maintainerrModalSubtitle');
    modalRefs.body = document.getElementById('maintainerrModalBody');

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

  function buildLaunchUrl(config, item) {
    var explicit = String(item && item.launchUrl || '').trim();
    if (explicit) return explicit;
    var appId = encodeURIComponent(String(config && config.appId || 'maintainerr').trim() || 'maintainerr');
    var title = encodeURIComponent(String(item && (item.title || item.name) || '').trim());
    return '/apps/' + appId + '/launch' + (title ? ('?q=' + title) : '');
  }

  function openModal(item, config) {
    var refs = ensureModalRefs();
    if (!refs.backdrop || !refs.body || !item) return;

    var title = String(item.title || item.name || '').trim() || 'Untitled';
    var subtitleBits = [];
    if (item.subtitle) subtitleBits.push(String(item.subtitle));
    if (item.meta) subtitleBits.push(String(item.meta));
    var subtitle = subtitleBits.join(' • ');
    var launchUrl = String(buildLaunchUrl(config, item)).trim();

    var status = String(item.status || '').trim();
    var pills = [
      '<span class="plex-pill2">' + escapeHtml(kindLabel(item.kind)) + '</span>',
      status ? '<span class="plex-pill2">' + escapeHtml(status) + '</span>' : '',
      item.pill ? '<span class="plex-pill2">' + escapeHtml(String(item.pill)) + '</span>' : '',
      item.collectionTitle ? '<span class="plex-pill2">' + escapeHtml(String(item.collectionTitle)) + '</span>' : '',
    ].filter(Boolean).join('');

    var overview = String(item.overview || item.description || '').trim();
    if (!overview) overview = 'No overview available.';

    var extraRows = [];
    if (item.libraryLabel) extraRows.push('Library: ' + String(item.libraryLabel));
    if (Number.isFinite(Number(item.rulesCount)) && Number(item.rulesCount) > 0) {
      extraRows.push('Conditions: ' + String(Math.max(0, Math.round(Number(item.rulesCount)))));
    }
    var durationLabel = formatDurationLabel(item.lastDurationSeconds);
    if (durationLabel) extraRows.push('Last run: ' + durationLabel);
    if (Number.isFinite(Number(item.handledMediaAmount)) && Number(item.handledMediaAmount) > 0) {
      extraRows.push('Handled media: ' + String(Math.max(0, Math.round(Number(item.handledMediaAmount)))));
    }
    if (item.cronSchedule) {
      extraRows.push('Schedule: ' + String(item.cronSchedule));
    } else if (item.rulesCount) {
      extraRows.push('Schedule: On demand');
    }
    if (Number.isFinite(Number(item.deleteAfterDays)) && Number(item.deleteAfterDays) > 0) {
      extraRows.push('Delete after: ' + String(Math.max(0, Math.round(Number(item.deleteAfterDays)))) + ' days');
    }
    if (Number.isFinite(Number(item.keepLogsForMonths)) && Number(item.keepLogsForMonths) > 0) {
      extraRows.push('Keep logs: ' + String(Math.max(0, Math.round(Number(item.keepLogsForMonths)))) + ' months');
    }
    if (item.arrAction) extraRows.push('Arr action: ' + String(item.arrAction));
    var conditionLines = Array.isArray(item.ruleConditions)
      ? item.ruleConditions.map(function (entry) { return String(entry || '').trim(); }).filter(Boolean)
      : [];

    var modalThumb = String(item.thumb || '').trim();
    var modalThumbFallback = isFallbackIconUrl(modalThumb);
    var modalInvalidThumb = isLibraryAssetThumb(modalThumb);
    var poster = (modalThumb && !modalInvalidThumb)
      ? '<img class="maintainerr-card-art' + (modalThumbFallback ? ' plex-fallback-art maintainerr-fallback-art maintainerr-fallback-art--modal' : '') + '" src="' + escapeHtml(modalThumb) + '" alt="' + escapeHtml(title) + '" referrerpolicy="no-referrer" onerror="this.onerror=null;this.src=\'/icons/maintainerr.svg\';this.classList.add(\'plex-fallback-art\',\'maintainerr-fallback-art\',\'maintainerr-fallback-art--modal\')" />'
      : maintainerrFallbackArtworkHtml('modal');

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
              (extraRows.length
                ? '<div class="plex-section"><h4>Details</h4><div class="plex-overview-text">' + escapeHtml(extraRows.join(' • ')) + '</div></div>'
                : '') +
              (conditionLines.length
                ? '<div class="plex-section"><h4>Rule Conditions</h4><ul class="maintainerr-modal-rule-list">' +
                    conditionLines.map(function (line) { return '<li>' + escapeHtml(line) + '</li>'; }).join('') +
                  '</ul></div>'
                : '') +
            '</div>' +
          '</div>' +
        '</div>' +
      '</div>' +
      '<div class="plex-modal-footer">' +
        (launchUrl ? '<a class="plex-modal-link" href="' + escapeHtml(launchUrl) + '" target="_blank" rel="noreferrer">Open in Maintainerr</a>' : '') +
      '</div>';

    refs.backdrop.classList.remove('plex-hidden');
  }

  function bindOpenAction(track, config) {
    if (!track || track.__maintainerrViewBound) return;
    track.addEventListener('click', function (event) {
      var viewBtn = closestNode(event.target, '[data-action="view"]', track);
      if (!viewBtn) return;
      event.preventDefault();
      event.stopPropagation();
      var card = closestNode(viewBtn, '.plex-card', track);
      if (!card) return;
      var idx = Number(card.getAttribute('data-index'));
      var items = Array.isArray(track.__maintainerrItems) ? track.__maintainerrItems : [];
      if (!Number.isFinite(idx) || !items[idx]) return;
      openModal(items[idx], config);
    });
    track.__maintainerrViewBound = true;
  }

  function bindCarousel(viewport, prevBtn, nextBtn) {
    if (!viewport) return;
    var freeScrollMode = carouselFreeScroll;
    var isTouchLike = window.matchMedia && window.matchMedia('(hover: none), (pointer: coarse)').matches;

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

    if (freeScrollMode || isTouchLike) {
      viewport.style.overflowX = 'auto';
      viewport.style.overflowY = 'hidden';
      viewport.style.scrollBehavior = 'smooth';
      viewport.style.webkitOverflowScrolling = 'touch';
      viewport.style.touchAction = 'pan-x pan-y';
    } else {
      viewport.style.overflowX = 'hidden';
      viewport.style.overflowY = 'hidden';
      viewport.style.scrollBehavior = '';
      viewport.style.webkitOverflowScrolling = '';
      viewport.style.touchAction = 'pan-y';
    }

    if (prevBtn) prevBtn.addEventListener('click', slidePrev);
    if (nextBtn) nextBtn.addEventListener('click', slideNext);

    if (viewport.__maintainerrSwipeBound || freeScrollMode || isTouchLike) return;
    viewport.__maintainerrSwipeBound = true;

    var startX = 0;
    var startY = 0;
    var movedX = 0;
    var movedY = 0;
    var tracking = false;
    var threshold = 40;

    var isInteractive = function (target) {
      return Boolean(
        target && target.closest
        && target.closest('[data-action="view"], [data-action="execute"], button, a, input, select, textarea, .plex-modal, .plex-modal-backdrop')
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

  function normalizeMediaItem(item, sectionId) {
    var source = item && typeof item === 'object' ? item : {};
    var defaultKind = sectionId === 'library-media' || sectionId === 'collections-media' ? 'movie' : 'movie';
    var kind = normalizeKind(source.kind || source.type, defaultKind);
    var title = String(source.title || source.name || 'Untitled').trim() || 'Untitled';
    var subtitle = String(source.subtitle || source.libraryTitle || source.collectionTitle || '').trim();
    var meta = String(source.meta || source.addedLabel || '').trim();
    var sortTs = parseTs(source.sortTs || source.addDate || source.addedAt || 0);
    var letter = normalizeLetter(source.letter || title);
    return {
      id: String(source.id || title).trim() || title,
      title: title,
      name: title,
      kind: kind,
      subtitle: subtitle,
      meta: meta,
      pill: String(source.pill || '').trim(),
      sortTs: sortTs,
      letter: letter,
      thumb: String(source.thumb || source.poster || '').trim(),
      art: String(source.art || source.background || '').trim(),
      overview: String(source.overview || source.description || '').trim(),
      description: String(source.description || source.overview || '').trim(),
      collectionId: String(source.collectionId || '').trim(),
      collectionTitle: String(source.collectionTitle || '').trim(),
      status: String(source.status || '').trim(),
      rulesCount: Number(source.rulesCount || 0),
      ruleConditions: Array.isArray(source.ruleConditions)
        ? source.ruleConditions.map(function (entry) { return String(entry || '').trim(); }).filter(Boolean)
        : [],
      libraryLabel: String(source.libraryLabel || '').trim(),
      cronSchedule: String(source.cronSchedule || '').trim(),
      arrAction: String(source.arrAction || '').trim(),
      lastDurationSeconds: Number(source.lastDurationSeconds || 0),
      handledMediaAmount: Number(source.handledMediaAmount || 0),
      deleteAfterDays: Number(source.deleteAfterDays || 0),
      keepLogsForMonths: Number(source.keepLogsForMonths || 0),
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
      var metaLine = settings.showMeta ? String(item.meta || '').trim() : '';
      var pill = settings.showPill ? String(item.pill || '').trim() : '';
      var icon = settings.showTypeIcon
        ? '<div class="plex-type-icon" title="' + escapeHtml(kindLabel(item.kind)) + '">' + typeIcon(item.kind) + '</div>'
        : '';
      var viewIcon = settings.showViewIcon
        ? '<div class="plex-eye-icon" title="View" data-action="view">' + eyeSvg() + '</div>'
        : '';
      var cardThumb = String(item.thumb || '').trim();
      var cardThumbFallback = isFallbackIconUrl(cardThumb);
      var cardInvalidThumb = isLibraryAssetThumb(cardThumb);
      var poster = (cardThumb && !cardInvalidThumb)
        ? '<img class="maintainerr-card-art' + (cardThumbFallback ? ' plex-fallback-art maintainerr-fallback-art maintainerr-fallback-art--card' : '') + '" src="' + escapeHtml(cardThumb) + '" alt="' + escapeHtml(item.title || 'Poster') + '" loading="lazy" referrerpolicy="no-referrer" onerror="this.onerror=null;this.src=\'/icons/maintainerr.svg\';this.classList.add(\'plex-fallback-art\',\'maintainerr-fallback-art\',\'maintainerr-fallback-art--card\')" />'
        : maintainerrFallbackArtworkHtml('card');

      return '' +
        '<div class="plex-card maintainerr-media-card" data-index="' + String(idx) + '">' +
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

  function renderRulesTrack(track, items, settings, emptyMessage) {
    if (!track) return;
    var rows = Array.isArray(items) ? items : [];
    if (!rows.length) {
      track.innerHTML = '<div class="plex-empty">' + escapeHtml(emptyMessage || 'No rules found.') + '</div>';
      return;
    }

    track.innerHTML = rows.map(function (item, idx) {
      var status = String(item.status || '').trim().toLowerCase() === 'paused' ? 'paused' : 'active';
      var statusLabel = status === 'paused' ? 'Paused' : 'Active';
      var icon = settings.showTypeIcon
        ? '<div class="plex-type-icon" title="' + escapeHtml(kindLabel(item.kind)) + '">' + typeIcon(item.kind) + '</div>'
        : '';
      var lastRunLabel = formatDurationLabel(item.lastDurationSeconds) || 'N/A';

      return '' +
        '<div class="plex-card maintainerr-rule-plex-card" data-index="' + String(idx) + '">' +
          '<div class="plex-poster-wrap">' +
            '<div class="plex-poster-well maintainerr-rule-poster">' +
              maintainerrFallbackArtworkHtml('card') +
              '<div class="plex-pill maintainerr-rule-pill maintainerr-rule-pill--' + status + '">' + statusLabel + '</div>' +
              icon +
              '<button class="maintainerr-rule-run maintainerr-rule-run--overlay" type="button" title="Execute" aria-label="Execute" data-action="execute" data-rule-id="' + escapeHtml(String(item.id || '')) + '">' + playSvg() + '</button>' +
            '</div>' +
          '</div>' +
          '<div class="plex-footer maintainerr-rule-footer">' +
            '<div class="plex-name">' + escapeHtml(String(item.name || 'Rule')) + '</div>' +
            '<div class="plex-meta">Last run: ' + escapeHtml(lastRunLabel) + '</div>' +
          '</div>' +
        '</div>';
    }).join('');
  }

  function mountLibrarySection(config) {
    var controlPrefix = String(config.controlPrefix || '').trim();
    if (!controlPrefix) return;

    var viewport = document.getElementById(controlPrefix + 'Viewport');
    var track = document.getElementById(controlPrefix + 'Track');
    if (!viewport || !track) return;

    var typeFilter = document.getElementById(controlPrefix + 'TypeFilter');
    var letterFilter = document.getElementById(controlPrefix + 'LetterFilter');
    var limitFilter = document.getElementById(controlPrefix + 'LimitSelect');
    var settings = sectionDisplaySettings(config.displaySettings);
    var sourceItems = [];

    bindOpenAction(track, config);
    bindCarousel(
      viewport,
      document.getElementById(controlPrefix + 'PrevBtn'),
      document.getElementById(controlPrefix + 'NextBtn')
    );

    function applyFilters() {
      var selectedType = String(typeFilter && typeFilter.value ? typeFilter.value : 'all').toLowerCase();
      var selectedLetter = String(letterFilter && letterFilter.value ? letterFilter.value : 'all').toUpperCase();
      var limitValue = String(limitFilter && limitFilter.value ? limitFilter.value : '50').trim().toLowerCase();
      var showAll = limitValue === 'all';
      var limit = Number(limitValue);
      if (!showAll && (!Number.isFinite(limit) || limit < 1)) limit = 50;

      var filtered = sourceItems
        .filter(function (item) {
          if (selectedType !== 'all' && String(item.kind || '').toLowerCase() !== selectedType) return false;
          if (selectedLetter !== 'ALL' && String(item.letter || '#').toUpperCase() !== selectedLetter) return false;
          return true;
        })
        .sort(function (left, right) {
          return String(left && left.title || '').localeCompare(String(right && right.title || ''));
        });

      if (!showAll) filtered = filtered.slice(0, limit);
      track.__maintainerrItems = filtered;
      renderTrack(track, filtered, settings, 'No library media found.');
    }

    typeFilter && typeFilter.addEventListener('change', applyFilters);
    letterFilter && letterFilter.addEventListener('change', applyFilters);
    limitFilter && limitFilter.addEventListener('change', applyFilters);

    track.innerHTML = '<div class="plex-empty">Loading…</div>';
    fetchJson(config.endpoint)
      .then(function (payload) {
        var list = Array.isArray(payload && payload.items) ? payload.items : [];
        sourceItems = list.map(function (entry) { return normalizeMediaItem(entry, 'library-media'); });
        applyFilters();
      })
      .catch(function (err) {
        sourceItems = [];
        track.innerHTML = '<div class="plex-empty">' + escapeHtml(err && err.message ? err.message : 'Failed to load Maintainerr library.') + '</div>';
      });
  }

  function mountRulesSection(config) {
    var controlPrefix = String(config.controlPrefix || '').trim();
    if (!controlPrefix) return;

    var viewport = document.getElementById(controlPrefix + 'Viewport');
    var track = document.getElementById(controlPrefix + 'Track');
    if (!viewport || !track) return;

    var typeFilter = document.getElementById(controlPrefix + 'TypeFilter');
    var statusFilter = document.getElementById(controlPrefix + 'StatusFilter');
    var settings = sectionDisplaySettings(config.displaySettings);
    var sourceItems = [];

    bindOpenAction(track, config);
    bindCarousel(
      viewport,
      document.getElementById(controlPrefix + 'PrevBtn'),
      document.getElementById(controlPrefix + 'NextBtn')
    );

    function applyFilters() {
      var selectedType = String(typeFilter && typeFilter.value ? typeFilter.value : 'all').toLowerCase();
      var selectedStatus = String(statusFilter && statusFilter.value ? statusFilter.value : 'all').toLowerCase();
      var filtered = sourceItems
        .filter(function (item) {
          if (selectedType !== 'all' && String(item.kind || '').toLowerCase() !== selectedType) return false;
          if (selectedStatus !== 'all' && String(item.status || '').toLowerCase() !== selectedStatus) return false;
          return true;
        })
        .sort(function (left, right) {
          return String(left && left.name || '').localeCompare(String(right && right.name || ''));
        });

      track.__maintainerrItems = filtered;
      renderRulesTrack(track, filtered, settings, 'No rules found.');
    }

    function executeRule(ruleId, button) {
      if (!ruleId || !button) return;
      var original = button.textContent;
      button.disabled = true;
      button.textContent = 'Queueing...';
      fetchJson('/api/maintainerr/rules/' + encodeURIComponent(String(ruleId)) + '/execute', { method: 'POST' })
        .then(function () {
          button.textContent = 'Queued';
          setTimeout(function () {
            button.textContent = 'Execute';
            button.disabled = false;
          }, 1200);
        })
        .catch(function (err) {
          button.textContent = 'Retry';
          button.disabled = false;
          button.title = err && err.message ? String(err.message) : 'Failed to execute rule.';
          setTimeout(function () {
            button.textContent = original || 'Execute';
          }, 2000);
        });
    }

    track.addEventListener('click', function (event) {
      var target = closestNode(event.target, '[data-action="execute"]', track);
      if (!target) return;
      event.preventDefault();
      event.stopPropagation();
      executeRule(target.getAttribute('data-rule-id') || '', target);
    });

    typeFilter && typeFilter.addEventListener('change', applyFilters);
    statusFilter && statusFilter.addEventListener('change', applyFilters);

    track.innerHTML = '<div class="plex-empty">Loading…</div>';
    fetchJson(config.endpoint)
      .then(function (payload) {
        var list = Array.isArray(payload && payload.items) ? payload.items : [];
        sourceItems = list.map(function (entry) {
          var normalized = normalizeMediaItem(entry, 'rules');
          normalized.id = entry && entry.id;
          normalized.name = String(entry && entry.name || 'Rule').trim();
          normalized.title = normalized.name;
          normalized.description = String(entry && entry.description || '').trim();
          normalized.overview = normalized.description;
          normalized.status = String(entry && entry.status || (entry && entry.isActive === false ? 'paused' : 'active')).trim().toLowerCase();
          normalized.kind = normalizeKind(entry && entry.kind, 'movie');
          normalized.libraryLabel = String(entry && entry.libraryLabel || '').trim();
          normalized.subtitle = normalized.libraryLabel;
          normalized.meta = 'Conditions: ' + String(Number(entry && entry.rulesCount || 0));
          normalized.rulesCount = Number(entry && entry.rulesCount || 0);
          normalized.pill = normalized.status === 'paused' ? 'Paused' : 'Active';
          normalized.thumb = '';
          normalized.art = '';
          return normalized;
        }).filter(function (entry) { return Boolean(entry && entry.id); });
        applyFilters();
      })
      .catch(function (err) {
        sourceItems = [];
        track.innerHTML = '<div class="plex-empty">' + escapeHtml(err && err.message ? err.message : 'Failed to load Maintainerr rules.') + '</div>';
      });
  }

  function mountCollectionsSection(config) {
    var controlPrefix = String(config.controlPrefix || '').trim();
    if (!controlPrefix) return;

    var viewport = document.getElementById(controlPrefix + 'Viewport');
    var track = document.getElementById(controlPrefix + 'Track');
    if (!viewport || !track) return;

    var collectionFilter = document.getElementById(controlPrefix + 'CollectionFilter');
    var typeFilter = document.getElementById(controlPrefix + 'TypeFilter');
    var limitFilter = document.getElementById(controlPrefix + 'LimitSelect');
    var settings = sectionDisplaySettings(config.displaySettings);
    var sourceItems = [];
    var collections = [];

    bindOpenAction(track, config);
    bindCarousel(
      viewport,
      document.getElementById(controlPrefix + 'PrevBtn'),
      document.getElementById(controlPrefix + 'NextBtn')
    );

    function syncCollectionOptions() {
      if (!collectionFilter) return;
      var selected = String(collectionFilter.value || 'all');
      collectionFilter.innerHTML = '<option value="all">All collections</option>' + collections.map(function (item) {
        return '<option value="' + escapeHtml(String(item.id)) + '">' + escapeHtml(String(item.title || ('Collection ' + item.id))) + '</option>';
      }).join('');
      var hasSelected = selected === 'all' || collections.some(function (item) { return String(item.id) === selected; });
      collectionFilter.value = hasSelected ? selected : 'all';
    }

    function applyFilters() {
      var selectedType = String(typeFilter && typeFilter.value ? typeFilter.value : 'all').toLowerCase();
      var limitValue = String(limitFilter && limitFilter.value ? limitFilter.value : '50').trim().toLowerCase();
      var showAll = limitValue === 'all';
      var limit = Number(limitValue);
      if (!showAll && (!Number.isFinite(limit) || limit < 1)) limit = 50;

      var filtered = sourceItems
        .filter(function (item) {
          if (selectedType !== 'all' && String(item.kind || '').toLowerCase() !== selectedType) return false;
          return true;
        })
        .sort(function (left, right) {
          var leftTs = parseTs(left && left.sortTs || 0);
          var rightTs = parseTs(right && right.sortTs || 0);
          if (rightTs !== leftTs) return rightTs - leftTs;
          return String(left && left.title || '').localeCompare(String(right && right.title || ''));
        });

      if (!showAll) filtered = filtered.slice(0, limit);
      track.__maintainerrItems = filtered;
      renderTrack(track, filtered, settings, 'No collection media found.');
    }

    function loadCollectionsItems() {
      var selectedCollection = String(collectionFilter && collectionFilter.value ? collectionFilter.value : 'all').trim() || 'all';
      var requestUrl = appendQuery(config.endpoint, { collectionId: selectedCollection, limit: '400' });
      track.innerHTML = '<div class="plex-empty">Loading…</div>';
      fetchJson(requestUrl)
        .then(function (payload) {
          collections = Array.isArray(payload && payload.collections) ? payload.collections : [];
          syncCollectionOptions();
          sourceItems = (Array.isArray(payload && payload.items) ? payload.items : [])
            .map(function (entry) { return normalizeMediaItem(entry, 'collections-media'); });
          applyFilters();
        })
        .catch(function (err) {
          sourceItems = [];
          track.innerHTML = '<div class="plex-empty">' + escapeHtml(err && err.message ? err.message : 'Failed to load Maintainerr collections.') + '</div>';
        });
    }

    collectionFilter && collectionFilter.addEventListener('change', loadCollectionsItems);
    typeFilter && typeFilter.addEventListener('change', applyFilters);
    limitFilter && limitFilter.addEventListener('change', applyFilters);

    loadCollectionsItems();
  }

  configs.forEach(function (entry) {
    var config = entry && typeof entry === 'object' ? entry : {};
    var sectionId = String(config.sectionId || '').trim().toLowerCase();
    if (!sectionId) return;
    if (sectionId === 'library-media') {
      mountLibrarySection(config);
      return;
    }
    if (sectionId === 'rules') {
      mountRulesSection(config);
      return;
    }
    if (sectionId === 'collections-media') {
      mountCollectionsSection(config);
    }
  });
})();
