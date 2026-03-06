(function () {
  'use strict';

  var carouselFreeScroll = (function () {
    try {
      return localStorage.getItem('launcharr-carousel-free-scroll') === '1';
    } catch (_err) {
      return false;
    }
  })();

  var configs = Array.isArray(window.MEDIA_OVERVIEW_COMBINED_CONFIGS)
    ? window.MEDIA_OVERVIEW_COMBINED_CONFIGS
    : [];
  if (!configs.length) return;

  function escapeHtml(value) {
    return String(value || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function normalizeBaseUrl(value) {
    var raw = String(value || '').trim();
    if (!raw) return '';
    if (!/^https?:\/\//i.test(raw)) raw = 'http://' + raw;
    try {
      return new URL(raw).origin;
    } catch (_err) {
      return raw.replace(/\/+$/, '');
    }
  }

  function capWord(value) {
    var text = String(value || '').trim().toLowerCase();
    if (!text) return '';
    return text.charAt(0).toUpperCase() + text.slice(1);
  }

  function pad2(value) {
    return String(Math.max(0, Number(value) || 0)).padStart(2, '0');
  }

  function buildPlexUrl(baseUrl, path, token) {
    if (!baseUrl || !path) return '';
    try {
      var url = new URL(path, baseUrl);
      if (token) url.searchParams.set('X-Plex-Token', token);
      return url.toString();
    } catch (_err) {
      return '';
    }
  }

  function fetchJson(url) {
    return fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' } })
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

  function fetchText(url) {
    return fetch(url, { credentials: 'omit', headers: { Accept: 'application/xml,text/xml,*/*' } })
      .then(function (res) {
        return res.text().then(function (text) {
          if (!res.ok) throw new Error('Status ' + res.status);
          return text;
        });
      });
  }

  function kindIcon(kind) {
    return String(kind || '').toLowerCase() === 'movie' ? movieIcon() : tvIcon();
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

  function eyeIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#e8eef7" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12z"></path>' +
      '<circle cx="12" cy="12" r="3"></circle></svg>';
  }

  function sourceIconPath(sourceId) {
    var key = String(sourceId || '').trim().toLowerCase();
    if (key === 'jellyfin') return '/icons/jellyfin.png';
    if (key === 'emby') return '/icons/emby.png';
    if (key === 'plex') return '/icons/plex.png';
    return '/icons/media-play.svg';
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
    var backdrop = document.getElementById('mediaCombinedModalBackdrop');
    if (!backdrop) {
      var host = document.createElement('div');
      host.innerHTML =
        '<div id="mediaCombinedModalBackdrop" class="plex-modal-backdrop plex-hidden">' +
          '<div class="plex-modal">' +
            '<button id="mediaCombinedModalClose" class="plex-modal-close" aria-label="Close">✕</button>' +
            '<div class="plex-modal-header">' +
              '<div class="plex-modal-title">' +
                '<span id="mediaCombinedModalTypeIcon" class="plex-mini-icon"></span>' +
                '<span id="mediaCombinedModalTitle">Loading…</span>' +
              '</div>' +
              '<div id="mediaCombinedModalSubtitle" class="plex-modal-subtitle"></div>' +
            '</div>' +
            '<div id="mediaCombinedModalBody" class="plex-modal-body"></div>' +
          '</div>' +
        '</div>';
      if (host.firstElementChild) document.body.appendChild(host.firstElementChild);
      backdrop = document.getElementById('mediaCombinedModalBackdrop');
    }
    modalRefs.backdrop = backdrop;
    modalRefs.close = document.getElementById('mediaCombinedModalClose');
    modalRefs.typeIcon = document.getElementById('mediaCombinedModalTypeIcon');
    modalRefs.title = document.getElementById('mediaCombinedModalTitle');
    modalRefs.subtitle = document.getElementById('mediaCombinedModalSubtitle');
    modalRefs.body = document.getElementById('mediaCombinedModalBody');
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

  function openModal(item) {
    var refs = ensureModalRefs();
    if (!refs.backdrop || !refs.body || !item) return;
    var mediaType = String(item.kind || '').toLowerCase() === 'tv' ? 'tv' : 'movie';
    var title = String(item.title || '').trim() || 'Untitled';
    var query = encodeURIComponent(title);
    var imdbUrl = item.imdbId
      ? ('https://www.imdb.com/title/' + encodeURIComponent(String(item.imdbId)) + '/')
      : ('https://www.imdb.com/find/?q=' + query);
    var tmdbUrl = item.tmdbId
      ? ('https://www.themoviedb.org/' + (mediaType === 'tv' ? 'tv/' : 'movie/') + encodeURIComponent(String(item.tmdbId)))
      : ('https://www.themoviedb.org/search?query=' + query);
    var sourceLaunchUrl = '/apps/' + encodeURIComponent(String(item.sourceId || '').trim()) + '/launch?q=' + query + '&type=' + encodeURIComponent(mediaType);
    var overview = String(item.overview || '').trim() || 'No overview available for this title.';
    var subtitleBits = [];
    if (item.sourceName) subtitleBits.push(String(item.sourceName));
    if (item.subtitle) subtitleBits.push(String(item.subtitle));
    if (item.meta) subtitleBits.push(String(item.meta));

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
                (item.sourceName ? '<span class="plex-pill2">' + escapeHtml(String(item.sourceName)) + '</span>' : '') +
                (item.pill ? '<span class="plex-pill2">' + escapeHtml(String(item.pill)) + '</span>' : '') +
                (item.meta ? '<span class="plex-pill2">' + escapeHtml(String(item.meta)) + '</span>' : '') +
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
        (item.sourceId ? '<a class="plex-modal-link" href="' + sourceLaunchUrl + '" target="_blank" rel="noreferrer">' + escapeHtml(String(item.sourceName || item.sourceId)) + '</a>' : '') +
      '</div>';
    refs.backdrop.classList.remove('plex-hidden');
  }

  function closeModal() {
    var refs = ensureModalRefs();
    if (!refs.backdrop) return;
    refs.backdrop.classList.add('plex-hidden');
  }

  function toRelativePill(dateValue) {
    var parsed = Date.parse(String(dateValue || ''));
    if (!Number.isFinite(parsed)) return '';
    var diffMs = Date.now() - parsed;
    var mins = Math.max(1, Math.round(diffMs / 60000));
    if (mins < 60) return mins + 'm ago';
    var hours = Math.round(mins / 60);
    if (hours < 48) return hours + 'h ago';
    var days = Math.round(hours / 24);
    return days + 'd ago';
  }

  function parsePlexSessions(xmlText, source) {
    var parser = new DOMParser();
    var xml = parser.parseFromString(String(xmlText || ''), 'application/xml');
    var videos = Array.from(xml.getElementsByTagName('Video'));
    var baseUrl = normalizeBaseUrl(source && source.baseUrl);
    var token = String(source && source.token || '').trim();

    return videos.map(function (video) {
      var plexType = String(video.getAttribute('type') || '').toLowerCase();
      var isTv = plexType === 'episode' || plexType === 'season' || plexType === 'show';
      var season = pad2(video.getAttribute('parentIndex') || 0);
      var episode = pad2(video.getAttribute('index') || 0);
      var code = isTv && season !== '00' && episode !== '00' ? ('S' + season + 'E' + episode) : '';
      var episodeTitle = String(video.getAttribute('title') || '').trim();
      var subtitle = isTv ? [code, episodeTitle].filter(Boolean).join(' • ') : '';
      var playerNode = video.getElementsByTagName('Player')[0] || null;
      var userNode = video.getElementsByTagName('User')[0] || null;
      var state = capWord(playerNode ? playerNode.getAttribute('state') : 'playing') || 'Playing';
      var device = String(playerNode ? (playerNode.getAttribute('product') || playerNode.getAttribute('title') || '') : '').trim();
      var title = isTv
        ? String(video.getAttribute('grandparentTitle') || video.getAttribute('parentTitle') || episodeTitle || 'Unknown').trim()
        : String(video.getAttribute('title') || 'Unknown').trim();
      var user = String(userNode ? (userNode.getAttribute('title') || userNode.getAttribute('username') || '') : '').trim();
      var thumbPath = isTv
        ? (video.getAttribute('grandparentThumb') || video.getAttribute('parentThumb') || video.getAttribute('thumb') || '')
        : (video.getAttribute('thumb') || '');
      return {
        id: String(video.getAttribute('ratingKey') || '').trim(),
        sourceId: source.appId,
        sourceName: source.appName,
        title: title || 'Now Playing',
        subtitle: subtitle,
        meta: device,
        pill: state,
        kind: isTv ? 'tv' : 'movie',
        user: user,
        overview: String(video.getAttribute('summary') || '').trim(),
        thumb: buildPlexUrl(baseUrl, thumbPath, token),
        art: buildPlexUrl(baseUrl, video.getAttribute('art') || '', token),
      };
    });
  }

  function parsePlexRecent(xmlText, source, typeFilter, limit) {
    var parser = new DOMParser();
    var xml = parser.parseFromString(String(xmlText || ''), 'application/xml');
    var videos = Array.from(xml.getElementsByTagName('Video'));
    var baseUrl = normalizeBaseUrl(source && source.baseUrl);
    var token = String(source && source.token || '').trim();
    var filtered = videos
      .map(function (video) {
        var plexType = String(video.getAttribute('type') || '').toLowerCase();
        var isTv = plexType === 'episode' || plexType === 'season' || plexType === 'show';
        var kind = isTv ? 'tv' : 'movie';
        var title = isTv
          ? String(video.getAttribute('grandparentTitle') || video.getAttribute('parentTitle') || video.getAttribute('title') || 'Untitled').trim()
          : String(video.getAttribute('title') || 'Untitled').trim();
        var season = pad2(video.getAttribute('parentIndex') || 0);
        var episode = pad2(video.getAttribute('index') || 0);
        var code = isTv && season !== '00' && episode !== '00' ? ('S' + season + 'E' + episode) : '';
        var year = String(video.getAttribute('year') || '').trim();
        var meta = year;
        var thumbPath = isTv
          ? (video.getAttribute('grandparentThumb') || video.getAttribute('parentThumb') || video.getAttribute('thumb') || '')
          : (video.getAttribute('thumb') || '');
        var addedRaw = Number(video.getAttribute('addedAt'));
        return {
          id: String(video.getAttribute('ratingKey') || '').trim(),
          sourceId: source.appId,
          sourceName: source.appName,
          title: title || 'Untitled',
          subtitle: isTv ? code : '',
          meta: meta,
          pill: addedRaw ? toRelativePill(new Date(addedRaw * 1000).toISOString()) : '',
          kind: kind,
          user: '',
          overview: String(video.getAttribute('summary') || '').trim(),
          thumb: buildPlexUrl(baseUrl, thumbPath, token),
          art: buildPlexUrl(baseUrl, video.getAttribute('art') || '', token),
          sortTs: Number.isFinite(addedRaw) ? addedRaw : 0,
        };
      })
      .filter(function (item) {
        if (typeFilter === 'all') return true;
        if (typeFilter === 'show') return item.kind === 'tv';
        return item.kind === 'movie';
      })
      .sort(function (a, b) {
        return (b.sortTs || 0) - (a.sortTs || 0);
      });
    return filtered.slice(0, Math.max(1, Number(limit) || 20));
  }

  function normalizeJellyfinItem(item, source) {
    var row = item && typeof item === 'object' ? item : {};
    return {
      id: String(row.id || '').trim(),
      sourceId: source.appId,
      sourceName: source.appName,
      title: String(row.title || '').trim() || 'Untitled',
      subtitle: String(row.subtitle || '').trim(),
      meta: String(row.meta || '').trim(),
      pill: String(row.pill || '').trim(),
      kind: String(row.kind || 'movie').toLowerCase() === 'movie' ? 'movie' : 'tv',
      user: String(row.user || '').trim(),
      overview: String(row.overview || '').trim(),
      thumb: String(row.thumb || '').trim(),
      art: String(row.art || '').trim(),
      sortTs: Number(row.sortTs || 0),
    };
  }

  function mergeItemsByTitle(a, b) {
    return String(a && a.title || '').localeCompare(String(b && b.title || ''));
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

    if (viewport.__mediaSwipeBound) return;
    viewport.__mediaSwipeBound = true;
    if (freeScrollMode) {
      viewport.style.touchAction = 'pan-x pan-y';
      var dragStartX = 0;
      var dragStartScroll = 0;
      var dragging = false;
      var isInteractive = function (target) {
        return Boolean(
          target && target.closest
          && target.closest('[data-action="view"], button, a, input, select, textarea')
        );
      };
      var onStart = function (x, target) {
        if (isInteractive(target)) return;
        dragging = true;
        dragStartX = x;
        dragStartScroll = viewport.scrollLeft;
      };
      var onMove = function (x) {
        if (!dragging) return;
        viewport.scrollLeft = dragStartScroll - (x - dragStartX);
      };
      var onEnd = function () {
        dragging = false;
      };
      viewport.addEventListener('pointerdown', function (event) {
        if (event.pointerType === 'touch') return;
        onStart(event.clientX, event.target);
        if (dragging && event.pointerType === 'mouse' && viewport.setPointerCapture) viewport.setPointerCapture(event.pointerId);
      });
      viewport.addEventListener('pointermove', function (event) {
        if (event.pointerType === 'touch') return;
        onMove(event.clientX);
      });
      viewport.addEventListener('pointerup', onEnd);
      viewport.addEventListener('pointercancel', onEnd);
      return;
    }
    viewport.style.touchAction = 'pan-y';
    var startX = 0;
    var deltaX = 0;
    var tracking = false;
    var threshold = 42;
    var onSwipeStart = function (x, target) {
      if (target && target.closest && target.closest('button, input, select, textarea, a, [data-action="view"]')) {
        tracking = false;
        return;
      }
      tracking = true;
      startX = x;
      deltaX = 0;
    };
    var onSwipeMove = function (x) {
      if (!tracking) return;
      deltaX = x - startX;
    };
    var onSwipeEnd = function () {
      if (!tracking) return;
      if (Math.abs(deltaX) > threshold) {
        if (deltaX > 0) slidePrev();
        else slideNext();
      }
      tracking = false;
    };
    viewport.addEventListener('pointerdown', function (event) {
      onSwipeStart(event.clientX, event.target);
      if (tracking && event.pointerType === 'mouse' && viewport.setPointerCapture) viewport.setPointerCapture(event.pointerId);
    });
    viewport.addEventListener('pointermove', function (event) {
      onSwipeMove(event.clientX);
    });
    viewport.addEventListener('pointerup', onSwipeEnd);
    viewport.addEventListener('pointercancel', onSwipeEnd);
    viewport.addEventListener('touchstart', function (event) {
      if (!event.touches || !event.touches.length) return;
      onSwipeStart(event.touches[0].clientX, event.target);
    }, { passive: true });
    viewport.addEventListener('touchmove', function (event) {
      if (!event.touches || !event.touches.length) return;
      onSwipeMove(event.touches[0].clientX);
    }, { passive: true });
    viewport.addEventListener('touchend', onSwipeEnd);
    viewport.addEventListener('touchcancel', onSwipeEnd);
  }

  function renderTrack(track, items, displaySettings, options) {
    if (!track) return;
    var opts = options || {};
    var settings = displaySettings || {};
    var showSubtitle = settings.showSubtitle !== false;
    var showMeta = settings.showMeta !== false;
    var showPill = settings.showPill !== false;
    var showTypeIcon = settings.showTypeIcon !== false;
    var showViewIcon = settings.showViewIcon !== false;
    var showUsername = settings.showUsername !== false;
    var rows = Array.isArray(items) ? items : [];
    if (!rows.length) {
      if (opts.emptyCard) {
        var emptyTitle = opts.emptyTitle ? String(opts.emptyTitle) : 'No items found';
        var emptySubtitle = opts.emptySubtitle ? String(opts.emptySubtitle) : 'Check back soon';
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
      track.innerHTML = '<div class="plex-empty">' + escapeHtml(opts.emptyMessage || 'No items found.') + '</div>';
      return;
    }
    track.innerHTML = rows.map(function (item, idx) {
      var subtitle = showSubtitle ? String(item.subtitle || '').trim() : '';
      var metaParts = [];
      if (item.sourceName) metaParts.push(String(item.sourceName).trim());
      if (showMeta && item.meta) metaParts.push(String(item.meta).trim());
      if (showUsername && item.user) metaParts.push('@' + String(item.user).trim());
      var metaLine = metaParts.filter(Boolean).join(' • ');
      var poster = item.thumb
        ? '<img src="' + escapeHtml(item.thumb) + '" alt="' + escapeHtml(item.title || 'Poster') + '" loading="lazy" referrerpolicy="no-referrer" />'
        : '<div class="plex-placeholder"><div class="plex-placeholder-big">' + escapeHtml(item.title || 'Untitled') + '</div><div class="plex-placeholder-small">No poster</div></div>';
      var sourceBadge = opts.showSourceBadge && item.sourceId
        ? '<div class="plex-source-icon" title="' + escapeHtml(item.sourceName || item.sourceId) + '"><img src="' + escapeHtml(sourceIconPath(item.sourceId)) + '" alt="' + escapeHtml(item.sourceName || item.sourceId) + '" loading="lazy" /></div>'
        : '';
      return '' +
        '<div class="plex-card" data-index="' + String(idx) + '">' +
          '<div class="plex-poster-wrap">' +
            '<div class="plex-poster-well">' +
              poster +
              (showPill && item.pill ? '<div class="plex-pill">' + escapeHtml(item.pill) + '</div>' : '') +
              (showTypeIcon ? '<div class="plex-type-icon" title="' + (item.kind === 'movie' ? 'Movie' : 'TV') + '">' + kindIcon(item.kind) + '</div>' : '') +
              sourceBadge +
              (showViewIcon ? '<div class="plex-eye-icon" title="View" data-action="view">' + eyeIcon() + '</div>' : '') +
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

  function loadSourceActive(source) {
    var sourceType = String(source && source.type || '').toLowerCase();
    if (sourceType === 'jellyfin') {
      return fetchJson('/api/jellyfin/active')
        .then(function (payload) {
          return (Array.isArray(payload && payload.items) ? payload.items : []).map(function (item) {
            return normalizeJellyfinItem(item, source);
          });
        });
    }
    if (sourceType === 'emby') {
      return fetchJson('/api/emby/active')
        .then(function (payload) {
          return (Array.isArray(payload && payload.items) ? payload.items : []).map(function (item) {
            return normalizeJellyfinItem(item, source);
          });
        });
    }
    if (sourceType === 'plex') {
      var baseUrl = normalizeBaseUrl(source && source.baseUrl);
      var token = String(source && source.token || '').trim();
      if (!baseUrl || !token) return Promise.resolve([]);
      var url = new URL('/status/sessions', baseUrl);
      url.searchParams.set('X-Plex-Token', token);
      return fetchText(url.toString())
        .then(function (xmlText) {
          return parsePlexSessions(xmlText, source);
        });
    }
    return Promise.resolve([]);
  }

  function loadSourceRecent(source, typeFilter, limit) {
    var sourceType = String(source && source.type || '').toLowerCase();
    if (sourceType === 'jellyfin') {
      var query = new URLSearchParams();
      query.set('type', typeFilter || 'movie');
      query.set('limit', String(limit || 20));
      return fetchJson('/api/jellyfin/recent?' + query.toString())
        .then(function (payload) {
          return (Array.isArray(payload && payload.items) ? payload.items : []).map(function (item) {
            return normalizeJellyfinItem(item, source);
          });
        });
    }
    if (sourceType === 'emby') {
      var embyQuery = new URLSearchParams();
      embyQuery.set('type', typeFilter || 'movie');
      embyQuery.set('limit', String(limit || 20));
      return fetchJson('/api/emby/recent?' + embyQuery.toString())
        .then(function (payload) {
          return (Array.isArray(payload && payload.items) ? payload.items : []).map(function (item) {
            return normalizeJellyfinItem(item, source);
          });
        });
    }
    if (sourceType === 'plex') {
      var baseUrl = normalizeBaseUrl(source && source.baseUrl);
      var token = String(source && source.token || '').trim();
      if (!baseUrl || !token) return Promise.resolve([]);
      var url = new URL('/library/recentlyAdded', baseUrl);
      url.searchParams.set('X-Plex-Token', token);
      return fetchText(url.toString())
        .then(function (xmlText) {
          return parsePlexRecent(xmlText, source, typeFilter, limit);
        });
    }
    return Promise.resolve([]);
  }

  function buildLoader(config, track) {
    return function loadAll(sourceFilter, typeFilter, limitValue) {
      var selectedSource = String(sourceFilter || 'all').toLowerCase();
      var selectedSources = (Array.isArray(config.sources) ? config.sources : []).filter(function (source) {
        return selectedSource === 'all' || source.appId === selectedSource;
      });
      if (!selectedSources.length) {
        track.__mediaItems = [];
        renderTrack(track, [], config.displaySettings, { emptyMessage: 'No sources selected.' });
        return Promise.resolve();
      }
      var requestList = selectedSources.map(function (source) {
        return config.section === 'active'
          ? loadSourceActive(source)
          : loadSourceRecent(source, typeFilter, limitValue);
      });
      return Promise.allSettled(requestList).then(function (results) {
        var merged = [];
        var errors = [];
        results.forEach(function (result) {
          if (result && result.status === 'fulfilled') {
            merged = merged.concat(Array.isArray(result.value) ? result.value : []);
          } else if (result && result.reason) {
            errors.push(String(result.reason.message || result.reason));
          }
        });
        if (!merged.length && errors.length) {
          track.__mediaItems = [];
          track.innerHTML = '<div class="plex-empty">' + escapeHtml(errors[0]) + '</div>';
          return;
        }
        if (config.section === 'recent') {
          merged.sort(function (a, b) {
            var bTs = Number(b && b.sortTs || 0);
            var aTs = Number(a && a.sortTs || 0);
            if (bTs !== aTs) return bTs - aTs;
            return mergeItemsByTitle(a, b);
          });
        } else {
          merged.sort(mergeItemsByTitle);
        }
        track.__mediaItems = merged;
        renderTrack(track, merged, config.displaySettings, {
          emptyMessage: config.section === 'active' ? 'No active streams.' : 'No recently added items.',
          emptyCard: config.section === 'active',
          emptyTitle: config.section === 'active' ? 'No active streams' : 'No items found',
          emptySubtitle: config.section === 'active' ? 'Nothing is playing right now' : 'Check back soon',
          showSourceBadge: config.section === 'active',
        });
      });
    };
  }

  configs.forEach(function (config) {
    var prefix = String(config && config.controlPrefix || '').trim();
    if (!prefix) return;
    var viewport = document.getElementById(prefix + 'Viewport');
    var track = document.getElementById(prefix + 'Track');
    var logo = document.getElementById(prefix + 'Logo');
    if (!viewport || !track) return;
    track.__mediaItems = [];
    bindCarousel(
      viewport,
      document.getElementById(prefix + 'PrevBtn'),
      document.getElementById(prefix + 'NextBtn')
    );

    var sourceSelect = document.getElementById(prefix + 'SourceFilter');
    var typeSelect = document.getElementById(prefix + 'TypeFilter');
    var limitSelect = document.getElementById(prefix + 'LimitSelect');
    var loadAll = buildLoader(config, track);
    var syncLogo = function () {
      if (!logo) return;
      var defaultIcon = String(logo.getAttribute('data-default-icon') || logo.getAttribute('src') || '').trim();
      if (!sourceSelect) {
        if (defaultIcon) logo.setAttribute('src', defaultIcon);
        return;
      }
      var selectedOption = sourceSelect.options[sourceSelect.selectedIndex];
      var selectedValue = String(sourceSelect.value || 'all').toLowerCase();
      var selectedIcon = selectedOption ? String(selectedOption.getAttribute('data-icon') || '').trim() : '';
      if (selectedValue !== 'all' && selectedIcon) {
        logo.setAttribute('src', selectedIcon);
      } else if (defaultIcon) {
        logo.setAttribute('src', defaultIcon);
      }
    };
    var reload = function () {
      var sourceValue = sourceSelect ? sourceSelect.value : 'all';
      var typeValue = typeSelect ? typeSelect.value : 'movie';
      var limitValue = limitSelect ? Number(limitSelect.value || 20) : 20;
      syncLogo();
      track.innerHTML = '<div class="plex-empty">Loading…</div>';
      loadAll(sourceValue, typeValue, limitValue);
    };

    if (!track.__mediaViewBound) {
      track.addEventListener('click', function (event) {
        var viewBtn = closestNode(event.target, '[data-action="view"]', track);
        if (!viewBtn) return;
        event.preventDefault();
        event.stopPropagation();
        var cardEl = closestNode(viewBtn, '.plex-card', track);
        if (!cardEl) return;
        var idx = Number(cardEl.getAttribute('data-index'));
        var items = Array.isArray(track.__mediaItems) ? track.__mediaItems : [];
        if (!Number.isFinite(idx) || !items[idx]) return;
        openModal(items[idx]);
      });
      track.__mediaViewBound = true;
    }

    if (sourceSelect) sourceSelect.addEventListener('change', reload);
    if (typeSelect) typeSelect.addEventListener('change', reload);
    if (limitSelect) limitSelect.addEventListener('change', reload);
    reload();
  });
})();
