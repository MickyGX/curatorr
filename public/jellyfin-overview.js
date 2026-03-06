(function () {
  'use strict';

  var config = window.JELLYFIN_OVERVIEW_CONFIG || {};
  var displaySettings = config.displaySettings || {};

  function settingFor(id) {
    var raw = displaySettings && displaySettings[id] ? displaySettings[id] : {};
    return {
      showSubtitle: raw.showSubtitle !== false,
      showMeta: raw.showMeta !== false,
      showPill: raw.showPill !== false,
      showTypeIcon: raw.showTypeIcon !== false,
      showViewIcon: raw.showViewIcon !== false,
      showUsername: raw.showUsername !== false,
    };
  }

  function escapeHtml(value) {
    return String(value || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function movieIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M5 9h14a1 1 0 0 1 1 1v9a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2v-9a1 1 0 0 1 1-1z"></path>' +
      '<path d="M5 13h15"></path><path d="M4.5 9.2 3.8 7a1.7 1.7 0 0 1 1.1-2.1L16.8 1a1.7 1.7 0 0 1 2.1 1.1l.7 2.2"></path>' +
      '<path d="m6.2 9.1 2.5-2.6"></path><path d="m9.8 9.1 2.5-2.6"></path><path d="m13.4 9.1 2.5-2.6"></path><path d="m17 9.1 2.2-2.3"></path>' +
      '<path d="m7.4 4 2.7 2.2"></path><path d="m11 2.8 2.7 2.2"></path><path d="m14.6 1.6 2.7 2.2"></path></svg>';
  }

  function tvIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M9 7 6 4"></path><path d="M15 7l3-3"></path><rect x="3" y="7" width="18" height="11" rx="2"></rect><path d="M9 21h6"></path></svg>';
  }

  function typeSvg(kind) {
    return String(kind || '').toLowerCase() === 'movie' ? movieIcon() : tvIcon();
  }

  function eyeSvg() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#e8eef7" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12z"></path>' +
      '<circle cx="12" cy="12" r="3"></circle></svg>';
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
    var backdrop = document.getElementById('jellyfinModalBackdrop');
    if (!backdrop) {
      var host = document.createElement('div');
      host.innerHTML =
        '<div id="jellyfinModalBackdrop" class="plex-modal-backdrop plex-hidden">' +
          '<div class="plex-modal">' +
            '<button id="jellyfinModalClose" class="plex-modal-close" aria-label="Close">✕</button>' +
            '<div class="plex-modal-header">' +
              '<div class="plex-modal-title">' +
                '<span id="jellyfinModalTypeIcon" class="plex-mini-icon"></span>' +
                '<span id="jellyfinModalTitle">Loading…</span>' +
              '</div>' +
              '<div id="jellyfinModalSubtitle" class="plex-modal-subtitle"></div>' +
            '</div>' +
            '<div id="jellyfinModalBody" class="plex-modal-body"></div>' +
          '</div>' +
        '</div>';
      if (host.firstElementChild) document.body.appendChild(host.firstElementChild);
      backdrop = document.getElementById('jellyfinModalBackdrop');
    }
    modalRefs.backdrop = backdrop;
    modalRefs.close = document.getElementById('jellyfinModalClose');
    modalRefs.typeIcon = document.getElementById('jellyfinModalTypeIcon');
    modalRefs.title = document.getElementById('jellyfinModalTitle');
    modalRefs.subtitle = document.getElementById('jellyfinModalSubtitle');
    modalRefs.body = document.getElementById('jellyfinModalBody');
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

  function openModal(item) {
    var refs = ensureModalRefs();
    if (!refs.backdrop || !refs.body || !item) return;
    var mediaType = String(item.kind || '').toLowerCase() === 'tv' ? 'tv' : 'movie';
    var title = String(item.title || '').trim() || 'Untitled';
    var subtitleBits = [];
    if (item.subtitle) subtitleBits.push(String(item.subtitle));
    if (item.meta) subtitleBits.push(String(item.meta));
    if (item.user) subtitleBits.push('@' + String(item.user));
    var query = encodeURIComponent(title);
    var imdbUrl = item.imdbId
      ? ('https://www.imdb.com/title/' + encodeURIComponent(String(item.imdbId)) + '/')
      : ('https://www.imdb.com/find/?q=' + query);
    var tmdbUrl = item.tmdbId
      ? ('https://www.themoviedb.org/' + (mediaType === 'tv' ? 'tv/' : 'movie/') + encodeURIComponent(String(item.tmdbId)))
      : ('https://www.themoviedb.org/search?query=' + query);
    var jellyfinLaunch = '/apps/jellyfin/launch?q=' + query + '&type=' + encodeURIComponent(mediaType);
    var overview = String(item.overview || '').trim() || 'No overview available for this title.';

    if (refs.typeIcon) refs.typeIcon.innerHTML = mediaType === 'tv' ? tvIcon() : movieIcon();
    if (refs.title) refs.title.textContent = title;
    if (refs.subtitle) refs.subtitle.textContent = subtitleBits.join(' • ');
    refs.body.innerHTML =
      '<div class="plex-modal-scroll">' +
        '<div class="plex-modal-hero">' +
          (item.art ? '<img class="plex-modal-bg" src="' + escapeHtml(item.art) + '" alt="" referrerpolicy="no-referrer" />' : '') +
          '<div class="plex-modal-content">' +
            '<div class="plex-modal-poster">' +
              (item.thumb
                ? '<img src="' + escapeHtml(item.thumb) + '" alt="' + escapeHtml(title) + '" referrerpolicy="no-referrer" />'
                : '<div class="plex-placeholder" style="height:340px"><div class="plex-placeholder-big">' + escapeHtml(title) + '</div><div class="plex-placeholder-small">No poster</div></div>'
              ) +
            '</div>' +
            '<div class="plex-modal-meta">' +
              '<div class="plex-pills">' +
                '<span class="plex-pill2">' + (mediaType === 'tv' ? 'TV Show' : 'Movie') + '</span>' +
                '<span class="plex-pill2">Jellyfin</span>' +
                (item.pill ? '<span class="plex-pill2">' + escapeHtml(String(item.pill)) + '</span>' : '') +
              '</div>' +
              '<div class="plex-section">' +
                '<h4>Overview</h4>' +
                '<div class="plex-overview-text">' + escapeHtml(overview) + '</div>' +
              '</div>' +
            '</div>' +
          '</div>' +
        '</div>' +
      '</div>' +
      '<div class="plex-modal-footer">' +
        '<a class="plex-modal-link" href="' + imdbUrl + '" target="_blank" rel="noreferrer">IMDb</a>' +
        '<a class="plex-modal-link" href="' + tmdbUrl + '" target="_blank" rel="noreferrer">TMDb</a>' +
        '<a class="plex-modal-link" href="' + jellyfinLaunch + '" target="_blank" rel="noreferrer">Jellyfin</a>' +
      '</div>';
    refs.backdrop.classList.remove('plex-hidden');
  }

  function closeModal() {
    var refs = ensureModalRefs();
    if (!refs.backdrop) return;
    refs.backdrop.classList.add('plex-hidden');
  }

  function bindViewModal(track) {
    if (!track || track.__jellyfinViewBound) return;
    track.addEventListener('click', function (event) {
      var viewBtn = closestNode(event.target, '[data-action="view"]', track);
      if (!viewBtn) return;
      event.preventDefault();
      event.stopPropagation();
      var card = closestNode(viewBtn, '.plex-card', track);
      if (!card) return;
      var idx = Number(card.getAttribute('data-index'));
      var items = Array.isArray(track.__jellyfinItems) ? track.__jellyfinItems : [];
      if (!Number.isFinite(idx) || !items[idx]) return;
      openModal(items[idx]);
    });
    track.__jellyfinViewBound = true;
  }

  function fetchJson(url, onDone, onFail) {
    fetch(url, {
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
            var errorMessage = payload && payload.error ? payload.error : ('Status ' + res.status);
            throw new Error(errorMessage);
          }
          return payload;
        });
      })
      .then(onDone)
      .catch(function (err) {
        if (typeof onFail === 'function') onFail(err);
      });
  }

  function bindCarousel(viewport, prevBtn, nextBtn) {
    if (!viewport) return;
    var step = function () {
      var card = viewport.querySelector('.plex-card');
      var cardWidth = card ? card.getBoundingClientRect().width : 220;
      return Math.max(220, Math.round(cardWidth + 20));
    };
    if (prevBtn) {
      prevBtn.addEventListener('click', function () {
        viewport.scrollBy({ left: -step(), behavior: 'smooth' });
      });
    }
    if (nextBtn) {
      nextBtn.addEventListener('click', function () {
        viewport.scrollBy({ left: step(), behavior: 'smooth' });
      });
    }
  }

  function renderTrack(track, items, settings, options) {
    if (!track) return;
    var rows = Array.isArray(items) ? items : [];
    if (!rows.length) {
      var emptyMessage = options && options.emptyMessage ? options.emptyMessage : 'No items found.';
      var emptyCard = options && options.emptyCard;
      if (emptyCard) {
        var emptyTitle = options && options.emptyTitle ? String(options.emptyTitle) : 'No items found';
        var emptySubtitle = options && options.emptySubtitle ? String(options.emptySubtitle) : 'Check back soon';
        track.innerHTML =
          '<div class="plex-card">' +
            '<div class="plex-poster-wrap">' +
              '<div class="plex-poster-well" style="height:calc(var(--plex-posterH) * 1px);display:flex;align-items:center;justify-content:center">' +
                '<div class="plex-placeholder" style="height:100%;width:100%;justify-content:center">' +
                  '<div class="plex-placeholder-big">' + escapeHtml(emptyTitle) + '</div>' +
                  '<div class="plex-placeholder-small">' + escapeHtml(emptySubtitle) + '</div>' +
                '</div>' +
              '</div>' +
            '</div>' +
            '<div class="plex-footer">' +
              '<div class="plex-name">All quiet</div>' +
              '<div class="plex-meta">Check back soon</div>' +
            '</div>' +
          '</div>';
        return;
      }
      track.innerHTML = '<div class="plex-empty">' + escapeHtml(emptyMessage) + '</div>';
      return;
    }
    track.innerHTML = rows.map(function (item, idx) {
      var subtitle = settings.showSubtitle ? String(item.subtitle || '').trim() : '';
      var metaParts = [];
      if (settings.showMeta && item.meta) metaParts.push(String(item.meta).trim());
      if (settings.showUsername && item.user) metaParts.push('@' + String(item.user).trim());
      var metaLine = metaParts.filter(Boolean).join(' • ');
      var pill = settings.showPill ? String(item.pill || '').trim() : '';
      var typeIcon = settings.showTypeIcon ? '<div class="plex-type-icon" title="' + (item.kind === 'movie' ? 'Movie' : 'TV') + '">' + typeSvg(item.kind) + '</div>' : '';
      var viewIcon = settings.showViewIcon ? '<div class="plex-eye-icon" title="View" data-action="view">' + eyeSvg() + '</div>' : '';
      var poster = item.thumb
        ? '<img src="' + escapeHtml(item.thumb) + '" alt="' + escapeHtml(item.title || 'Poster') + '" loading="lazy" referrerpolicy="no-referrer" />'
        : '<div class="plex-placeholder"><div class="plex-placeholder-big">' + escapeHtml(item.title || 'Untitled') + '</div><div class="plex-placeholder-small">No poster</div></div>';
      return '' +
        '<div class="plex-card" data-index="' + String(idx) + '">' +
          '<div class="plex-poster-wrap">' +
            '<div class="plex-poster-well">' +
              poster +
              (pill ? '<div class="plex-pill">' + escapeHtml(pill) + '</div>' : '') +
              typeIcon +
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

  function initActive() {
    var viewport = document.getElementById('jellyfinActiveViewport');
    var track = document.getElementById('jellyfinActiveTrack');
    if (!viewport || !track) return;
    track.__jellyfinItems = [];
    bindViewModal(track);
    bindCarousel(
      viewport,
      document.getElementById('jellyfinActivePrevBtn'),
      document.getElementById('jellyfinActiveNextBtn')
    );
    var settings = settingFor('active');
    track.innerHTML = '<div class="plex-empty">Loading…</div>';
    fetchJson('/api/jellyfin/active', function (payload) {
      var rows = Array.isArray(payload && payload.items) ? payload.items : [];
      track.__jellyfinItems = rows;
      renderTrack(track, rows, settings, {
        emptyMessage: 'No active streams.',
        emptyCard: true,
        emptyTitle: 'No active streams',
        emptySubtitle: 'Nothing is playing right now',
      });
    }, function (err) {
      track.__jellyfinItems = [];
      track.innerHTML = '<div class="plex-empty">' + escapeHtml(err && err.message ? err.message : 'Failed to load active streams.') + '</div>';
    });
  }

  function initRecent() {
    var viewport = document.getElementById('jellyfinRecentViewport');
    var track = document.getElementById('jellyfinRecentTrack');
    var typeSelect = document.getElementById('jellyfinRecentTypeFilter');
    var limitSelect = document.getElementById('jellyfinRecentLimitSelect');
    if (!viewport || !track || !typeSelect || !limitSelect) return;
    track.__jellyfinItems = [];
    bindViewModal(track);
    bindCarousel(
      viewport,
      document.getElementById('jellyfinRecentPrevBtn'),
      document.getElementById('jellyfinRecentNextBtn')
    );
    var settings = settingFor('recent');
    var reload = function () {
      var type = String(typeSelect.value || 'movie').toLowerCase();
      var limit = String(limitSelect.value || '20');
      track.innerHTML = '<div class="plex-empty">Loading…</div>';
      var query = new URLSearchParams();
      query.set('type', type);
      query.set('limit', limit);
      fetchJson('/api/jellyfin/recent?' + query.toString(), function (payload) {
        var rows = Array.isArray(payload && payload.items) ? payload.items : [];
        track.__jellyfinItems = rows;
        renderTrack(track, rows, settings, { emptyMessage: 'No recently added items.' });
      }, function (err) {
        track.__jellyfinItems = [];
        track.innerHTML = '<div class="plex-empty">' + escapeHtml(err && err.message ? err.message : 'Failed to load recently added items.') + '</div>';
      });
    };
    typeSelect.addEventListener('change', reload);
    limitSelect.addEventListener('change', reload);
    reload();
  }

  initActive();
  initRecent();
})();
