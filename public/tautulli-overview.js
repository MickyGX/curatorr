(() => {
  const carouselFreeScroll = (() => {
    try {
      return localStorage.getItem('launcharr-carousel-free-scroll') === '1';
    } catch (err) {
      return false;
    }
  })();

  const config = window.TAUTULLI_OVERVIEW_CONFIG || {};
  const apiKey = String(config.apiKey || '').trim();
  const rawBaseUrl = String(config.baseUrl || '').trim();
  const baseUrl = normalizeBaseUrl(rawBaseUrl);
  const plexBaseUrl = normalizeBaseUrl(String(config.plexBaseUrl || '').trim());
  const preferredStatsCards = Array.isArray(config.statsCards)
    ? config.statsCards.map((item) => String(item || '').trim()).filter(Boolean)
    : [];
  let plexMachineId = '';
  const musicStatIds = new Set(['top_music', 'popular_music']);
  const root = document.querySelector('#tautulliStatsViewport') || document.querySelector('.plex-overview') || document.documentElement;

  const viewport = document.getElementById('tautulliStatsViewport');
  const track = document.getElementById('tautulliStatsTrack');
  const prevBtn = document.getElementById('tautulliStatsPrevBtn');
  const nextBtn = document.getElementById('tautulliStatsNextBtn');
  const wheelViewport = document.getElementById('tautulliWheelViewport');
  const wheelTrack = document.getElementById('tautulliWheelTrack');
  const wheelPrevBtn = document.getElementById('tautulliWheelPrevBtn');
  const wheelNextBtn = document.getElementById('tautulliWheelNextBtn');

  if (!viewport || !track) {
    if (!wheelViewport || !wheelTrack) return;
  }

  if (!baseUrl || !apiKey) {
    if (track) track.innerHTML = '<div class="plex-empty">Add a Tautulli API key in settings.</div>';
    if (wheelTrack) wheelTrack.innerHTML = '<div class="plex-empty">Add a Tautulli API key in settings.</div>';
    return;
  }

  function normalizeBaseUrl(value) {
    let url = String(value || '').trim();
    if (!url) return '';
    if (!/^https?:\/\//i.test(url)) url = 'http://' + url;
    try {
      const parsed = new URL(url);
      return parsed.origin;
    } catch (err) {
      return url.replace(/\/+$/, '');
    }
  }

  function escapeHtml(s) {
    return String(s || '').replace(/[&<>"']/g, function (m) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[m];
    });
  }

  function cssNum(name, fallback) {
    const v = getComputedStyle(root).getPropertyValue(name).trim();
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  }

  function formatHM(seconds) {
    const total = Math.max(0, Number(seconds) || 0);
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    return String(h).padStart(1, '0') + ':' + String(m).padStart(2, '0');
  }

  function apiUrl(cmd, params) {
    const query = new URLSearchParams({ apikey: apiKey, cmd });
    Object.keys(params || {}).forEach((key) => {
      if (params[key] !== undefined && params[key] !== null && params[key] !== '') {
        query.set(key, params[key]);
      }
    });
    return baseUrl + '/api/v2?' + query.toString();
  }

  function logApi(level, message, meta) {
    const logger = console[level] || console.log;
    if (meta && typeof meta === 'object') {
      logger(`[Launcharr] ${message}`, meta);
    } else {
      logger(`[Launcharr] ${message}`);
    }
  }

  function sanitizeUrl(value) {
    try {
      const url = new URL(value, window.location.origin);
      if (url.searchParams.has('apikey')) url.searchParams.set('apikey', 'redacted');
      return url.toString();
    } catch (err) {
      return value;
    }
  }

  function postActivityLog(appId, level, action, message, meta) {
    try {
      fetch('/api/logs/client', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          app: appId,
          level,
          action,
          message,
          meta,
        }),
      }).catch(() => {});
    } catch (err) {
      // best-effort only
    }
  }

  function actionSlug(label, suffix) {
    const base = String(label || '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '.')
      .replace(/^\.+|\.+$/g, '');
    return suffix ? `${base}.${suffix}` : base || 'event';
  }

  function fetchWithLog(url, options, label) {
    const safeUrl = sanitizeUrl(url);
    logApi('info', `${label} request`, { url: safeUrl });
    postActivityLog('tautulli', 'info', actionSlug(label, 'request'), 'Request started.', { url: safeUrl });
    return fetch(url, options)
      .then((res) => {
        logApi('info', `${label} response`, { url: safeUrl, status: res.status, ok: res.ok });
        postActivityLog('tautulli', 'info', actionSlug(label, 'response'), 'Response received.', {
          url: safeUrl,
          status: res.status,
          ok: res.ok,
        });
        return res;
      })
      .catch((err) => {
        logApi('error', `${label} failed`, { url: safeUrl, error: err?.message || String(err) });
        postActivityLog('tautulli', 'error', actionSlug(label, 'error'), 'Request failed.', {
          url: safeUrl,
          error: err?.message || String(err),
        });
        throw err;
      });
  }

  function infoLink(row) {
    if (!row) return '#';
    const params = new URLSearchParams();
    if (row.live && row.guid) params.set('guid', row.guid);
    else if (row.rating_key) params.set('rating_key', row.rating_key);
    if (row.guid) params.set('guid', row.guid);
    if (!params.toString()) return '#';
    params.set('source', 'history');
    return baseUrl + '/info?' + params.toString();
  }

  function libraryLink(row) {
    if (!row || !row.section_id) return '#';
    return baseUrl + '/library?section_id=' + encodeURIComponent(row.section_id);
  }

  function userLink(row) {
    if (!row || !row.user_id) return '#';
    return baseUrl + '/user?user_id=' + encodeURIComponent(row.user_id);
  }

  function imageProxy(img, ratingKey, width, height, fallback) {
    if (!img) return '';
    const params = new URLSearchParams({ img });
    if (ratingKey) params.set('rating_key', ratingKey);
    if (width) params.set('width', width);
    if (height) params.set('height', height);
    if (fallback) params.set('fallback', fallback);
    return baseUrl + '/pms_image_proxy?' + params.toString();
  }

  function statUnits(stat) {
    if (!stat) return '';
    if (stat.stat_id && stat.stat_id.startsWith('top') && stat.stat_type === 'total_plays') return 'plays';
    if (stat.stat_id && stat.stat_id.startsWith('top') && stat.stat_type === 'total_duration') return 'hh:mm';
    if (stat.stat_id && stat.stat_id.startsWith('popular')) return 'users';
    if (stat.stat_id === 'last_watched') {
      const row0 = stat.rows && stat.rows[0];
      return row0 ? row0.friendly_name || row0.user || '' : '';
    }
    if (stat.stat_id === 'most_concurrent') return 'streams';
    return '';
  }

  function rowTitle(statId, row) {
    if (!row) return '';
    if (statId === 'top_libraries') return row.section_name || row.title || 'Library';
    if (statId === 'top_users') return row.friendly_name || row.user || 'User';
    if (statId === 'top_platforms') return row.platform || row.platform_name || 'Platform';
    if (statId === 'most_concurrent') return row.title || 'Concurrent';
    return row.title || row.grandparent_title || row.full_title || 'Unknown';
  }

  function rowLink(statId, row) {
    if (statId === 'top_libraries') return libraryLink(row);
    if (statId === 'top_users') return userLink(row);
    if (statId === 'most_concurrent') return baseUrl + '/graphs#concurrent-graph';
    if (statId === 'top_platforms') return '';
    return infoLink(row);
  }

  function searchLink(query) {
    if (!query) return baseUrl + '/search';
    return baseUrl + '/search?query=' + encodeURIComponent(query);
  }

  function plexLink(row) {
    const ratingKey = row && (row.grandparent_rating_key || row.rating_key);
    if (!ratingKey) return '';
    const key = encodeURIComponent('/library/metadata/' + ratingKey);

    // Use Plex hosted web app when possible to avoid reverse-proxy path mismatches.
    if (plexMachineId) {
      return 'https://app.plex.tv/desktop#!/server/' + encodeURIComponent(plexMachineId) + '/details?key=' + key;
    }

    if (!plexBaseUrl) return '';
    const base = /\/web\/?$/i.test(plexBaseUrl) ? plexBaseUrl : (plexBaseUrl + '/web');
    if (/\/index\.html$/i.test(base)) {
      return base + '#!/details?key=' + key;
    }
    return base + '/index.html#!/details?key=' + key;
  }

  function eyeSvg() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#e8eef7" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12z"></path>' +
      '<circle cx="12" cy="12" r="3"></circle></svg>';
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

  function musicIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M9 18V5l11-2v13"></path><circle cx="6" cy="18" r="3"></circle><circle cx="18" cy="16" r="3"></circle></svg>';
  }

  function bookIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"></path><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"></path></svg>';
  }

  function mediaTypeIcon(rowWrap) {
    const rawType = String((rowWrap && rowWrap.raw && rowWrap.raw.media_type) || '').toLowerCase();
    const statId = String((rowWrap && rowWrap.statId) || '').toLowerCase();
    if (statId.includes('music') || rawType.includes('artist') || rawType.includes('album') || rawType.includes('track')) return { title: 'Music', svg: musicIcon() };
    if (statId.includes('book') || rawType.includes('book') || rawType.includes('audiobook')) return { title: 'Book', svg: bookIcon() };
    if (rawType === 'show' || rawType === 'episode' || rawType === 'season') return { title: 'TV', svg: tvIcon() };
    if (rawType === 'movie') return { title: 'Movie', svg: movieIcon() };
    return { title: 'Media', svg: movieIcon() };
  }

  function mediaTypeForRow(row, stat) {
    const rawType = String((row && row.media_type) || '').toLowerCase();
    const statId = String((stat && stat.stat_id) || '').toLowerCase();
    if (rawType === 'show' || rawType === 'episode' || rawType === 'season') return 'tv';
    if (rawType === 'movie') return 'movie';
    if (statId.includes('tv') || statId.includes('show')) return 'tv';
    return 'movie';
  }

  function extractExternalIdsFromValues(values) {
    const out = { imdbId: '', tmdbId: '' };
    const candidates = Array.isArray(values) ? values.filter(Boolean) : [];

    candidates.forEach((raw) => {
      const value = String(raw || '').trim();
      const lower = value.toLowerCase();
      if (!out.imdbId) {
        if (lower.startsWith('imdb://')) {
          const imdb = value.slice('imdb://'.length).split('?')[0].trim();
          if (/^tt\d+$/i.test(imdb)) out.imdbId = imdb;
        } else {
          const imdbMatch = value.match(/(?:^|[/:?&=])(tt\d{5,})(?:[/?&#]|$)/i);
          if (imdbMatch) out.imdbId = imdbMatch[1];
        }
      }
      if (!out.tmdbId) {
        if (lower.startsWith('tmdb://')) {
          const tmdb = value.slice('tmdb://'.length).split('?')[0].trim();
          if (/^\d+$/.test(tmdb)) out.tmdbId = tmdb;
        } else {
          const tmdbMatch = value.match(/(?:tmdb[:/]|themoviedb\.org\/(?:movie|tv)\/)(\d+)/i);
          if (tmdbMatch) out.tmdbId = tmdbMatch[1];
        }
      }
    });
    return out;
  }

  function extractExternalIdsFromRow(row) {
    return extractExternalIdsFromValues(
      []
        .concat(String(row && row.guid || '').trim())
        .concat(Array.isArray(row && row.guids) ? row.guids : [])
        .concat(String(row && row.parent_guid || '').trim())
        .concat(String(row && row.grandparent_guid || '').trim())
    );
  }

  function extractExternalIdsFromMetadata(data) {
    if (!data || typeof data !== 'object') return { imdbId: '', tmdbId: '' };
    const guids = Array.isArray(data.guids)
      ? data.guids.map((item) => {
          if (typeof item === 'string') return item;
          if (item && typeof item === 'object') return item.id || item.guid || '';
          return '';
        })
      : [];
    return extractExternalIdsFromValues(
      []
        .concat(String(data.guid || '').trim())
        .concat(String(data.parent_guid || '').trim())
        .concat(String(data.grandparent_guid || '').trim())
        .concat(guids)
    );
  }

  function rowValue(stat, row) {
    if (!stat || !row) return '';
    if (stat.stat_id && stat.stat_id.startsWith('top') && stat.stat_type === 'total_plays') return row.total_plays;
    if (stat.stat_id && stat.stat_id.startsWith('top') && stat.stat_type === 'total_duration') return formatHM(row.total_duration);
    if (stat.stat_id && stat.stat_id.startsWith('popular')) return row.users_watched;
    if (stat.stat_id === 'most_concurrent') return row.count;
    return '';
  }

  function resolveArtistRow(rowWrap) {
    if (!rowWrap || rowWrap.resolved || !musicStatIds.has(rowWrap.statId)) {
      return Promise.resolve(rowWrap);
    }
    const query = rowWrap.title || rowTitle(rowWrap.statId, rowWrap.raw);
    if (!query) return Promise.resolve(rowWrap);
    return fetchWithLog(apiUrl('search', { query, limit: 20 }), null, 'Tautulli search')
      .then((res) => res.json())
      .then((payload) => {
        const data = payload && payload.response && payload.response.data ? payload.response.data : {};
        const artists = data && data.results_list && Array.isArray(data.results_list.artist)
          ? data.results_list.artist
          : [];
        if (!artists.length) {
          rowWrap.resolved = true;
          return rowWrap;
        }
        const lower = String(query).toLowerCase();
        const match = artists.find((item) => String(item.title || '').toLowerCase() === lower) || artists[0];
        if (match && match.rating_key) {
          const ratingKey = String(match.rating_key);
          rowWrap.resolvedArtistKey = ratingKey;
          rowWrap.href = baseUrl + '/info?rating_key=' + encodeURIComponent(ratingKey);
          if (match.thumb) {
            rowWrap.image = imageProxy(match.thumb, ratingKey, 240, 360, 'cover');
          }
        }
        rowWrap.resolved = true;
        return rowWrap;
      })
      .catch(() => {
        rowWrap.resolved = true;
        return rowWrap;
      });
  }

  function getModalRow(rowWrap) {
    if (!rowWrap || !rowWrap.raw) return null;
    const next = { ...rowWrap.raw };
    if (rowWrap.resolvedArtistKey) {
      next.rating_key = rowWrap.resolvedArtistKey;
      next.grandparent_rating_key = rowWrap.resolvedArtistKey;
    }
    if (!next.thumb && rowWrap.image) next.poster_url = rowWrap.image;
    return next;
  }

  const modalBackdrop = document.getElementById('tautulliModalBackdrop');
  const modalClose = document.getElementById('tautulliModalClose');
  const modalTitle = document.getElementById('tautulliModalTitle');
  const modalSubtitle = document.getElementById('tautulliModalSubtitle');
  const modalBody = document.getElementById('tautulliModalBody');
  const modalTypeIcon = document.getElementById('tautulliModalTypeIcon');

  function openModal() {
    if (!modalBackdrop) return;
    modalBackdrop.classList.remove('plex-hidden');
    document.body.style.overflow = 'hidden';
  }

  function closeModal() {
    if (!modalBackdrop) return;
    modalBackdrop.classList.add('plex-hidden');
    document.body.style.overflow = '';
  }

  if (modalClose) modalClose.addEventListener('click', closeModal);
  if (modalBackdrop) {
    modalBackdrop.addEventListener('click', (e) => {
      if (e.target === modalBackdrop) closeModal();
    });
  }
  window.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeModal();
  });

  function createCarousel() {
    let cards = [];
    let slideIndex = 0;
    let visibleCount = 1;
    let cardWidth = 240;
    let gap = 18;
    const freeScrollMode = carouselFreeScroll;

    function applyFreeScrollViewportStyle() {
      if (!freeScrollMode) {
        viewport.style.overflowX = '';
        viewport.style.overflowY = '';
        viewport.style.scrollBehavior = '';
        viewport.style.webkitOverflowScrolling = '';
        return;
      }
      viewport.style.overflowX = 'auto';
      viewport.style.overflowY = 'hidden';
      viewport.style.scrollBehavior = 'smooth';
      viewport.style.webkitOverflowScrolling = 'touch';
    }

    function computeLayout() {
      const viewportWidth = viewport.clientWidth;
      const defaultWidth = cssNum('--plex-cardW', 203);
      const defaultGap = cssNum('--plex-gap', 24);
      const firstCard = track.querySelector('.plex-card') || viewport.querySelector('.plex-card');
      const measuredWidth = firstCard ? Math.round(firstCard.getBoundingClientRect().width) : 0;
      cardWidth = measuredWidth > 0 ? measuredWidth : defaultWidth;
      gap = defaultGap;
      if (viewportWidth <= 0) return;
      visibleCount = Math.max(1, Math.floor((viewportWidth + gap) / (cardWidth + gap)));
    }

    function clampIndex() {
      const maxLeft = Math.max(0, cards.length - visibleCount);
      slideIndex = Math.min(Math.max(0, slideIndex), maxLeft);
    }

    function applyTransform(animated) {
      if (freeScrollMode) {
        track.style.transition = 'none';
        track.style.transform = 'none';
        return;
      }
      track.style.transition = animated ? 'transform .25s ease' : 'none';
      const offset = slideIndex * (cardWidth + gap);
      track.style.transform = 'translateX(' + (-offset) + 'px)';
    }

    function updateButtons() {
      if (!prevBtn || !nextBtn) return;
      if (freeScrollMode) {
        const maxScroll = Math.max(0, viewport.scrollWidth - viewport.clientWidth);
        prevBtn.disabled = viewport.scrollLeft <= 2;
        nextBtn.disabled = viewport.scrollLeft >= maxScroll - 2;
        return;
      }
      const maxLeft = Math.max(0, cards.length - visibleCount);
      prevBtn.disabled = slideIndex <= 0;
      nextBtn.disabled = slideIndex >= maxLeft;
    }

    function render() {
      track.innerHTML = '';
      applyFreeScrollViewportStyle();
      if (!cards.length) {
        track.innerHTML =
          '<div class="plex-card">' +
            '<div class="plex-poster-wrap">' +
              '<div class="plex-poster-well" style="height:266px;display:flex;align-items:center;justify-content:center">' +
                '<div class="plex-placeholder" style="height:100%;width:100%;justify-content:center">' +
                  '<div class="plex-placeholder-big">No stats</div>' +
                  '<div class="plex-placeholder-small">Check back soon</div>' +
                '</div>' +
              '</div>' +
            '</div>' +
            '<div class="plex-footer">' +
              '<div class="plex-name">All quiet</div>' +
              '<div class="plex-meta">Nothing to show</div>' +
            '</div>' +
          '</div>';
        if (freeScrollMode) viewport.scrollLeft = 0;
        updateButtons();
        return;
      }

      cards.forEach((card, idx) => {
        const el = document.createElement('div');
        el.className = 'plex-card tautulli-card';
        el.dataset.index = String(idx);
        const listHtml = card.rows
          .map((row, index) => {
            const link = row.href;
            const title = escapeHtml(row.title);
            const linkHtml = link
              ? '<a href="' + link + '" data-action="modal" target="_blank" rel="noreferrer">' + title + '</a>'
              : '<span>' + title + '</span>';
            return (
              '<li class="tautulli-item" data-image="' + escapeHtml(row.image || '') + '" data-title="' + title + '" data-index="' + index + '">' +
                '<span class="tautulli-rank">' + (index + 1) + '</span>' +
                '<span class="tautulli-name">' + linkHtml + '</span>' +
              '</li>'
            );
          })
          .join('');
        const firstRow = card.rows[0] || null;
        const initialPoster = (firstRow && firstRow.image) || card.image || '';
        const initialPosterTitle = (firstRow && firstRow.title) || card.title || 'Unknown';
        const initialType = mediaTypeIcon(firstRow || { statId: card.id, raw: null });
        const posterHtml = initialPoster
          ? '<img src="' + escapeHtml(initialPoster) + '" alt="' + escapeHtml(initialPosterTitle) + '" loading="lazy" referrerpolicy="no-referrer" />'
          : '<div class="plex-placeholder"><div class="plex-placeholder-big">' + escapeHtml(initialPosterTitle) + '</div><div class="plex-placeholder-small">No poster</div></div>';

        el.innerHTML =
          '<div class="tautulli-card-title">' +
            '<div class="tautulli-card-label">' + escapeHtml(card.title) + '</div>' +
          '</div>' +
          '<div class="tautulli-card-content">' +
            '<div class="tautulli-card-poster-wrap">' +
              '<div class="plex-poster-wrap tautulli-card-poster-shell">' +
                '<div class="plex-poster-well tautulli-card-poster">' + posterHtml + '</div>' +
                '<div class="plex-type-icon tautulli-card-type" title="' + escapeHtml(initialType.title) + '">' + initialType.svg + '</div>' +
                '<div class="plex-pill tautulli-card-rank">#1</div>' +
                '<div class="tautulli-card-actions">' +
                  '<button class="tautulli-card-action tautulli-action tautulli-action--view" type="button" title="View details">' +
                    eyeSvg() +
                  '</button>' +
                '</div>' +
              '</div>' +
              '<div class="tautulli-card-selected-title">' + escapeHtml(initialPosterTitle) + '</div>' +
            '</div>' +
            '<div class="tautulli-card-body">' +
              '<ul class="tautulli-list">' + listHtml + '</ul>' +
            '</div>' +
          '</div>';

        track.appendChild(el);
      });

      computeLayout();
      clampIndex();
      applyTransform(false);
      if (freeScrollMode) viewport.scrollLeft = 0;
      updateButtons();
    }

    function slidePrev() {
      if (freeScrollMode) {
        computeLayout();
        const amount = Math.max(cardWidth + gap, Math.floor(viewport.clientWidth * 0.85));
        viewport.scrollBy({ left: -amount, behavior: 'smooth' });
        return;
      }
      computeLayout();
      slideIndex = Math.max(0, slideIndex - visibleCount);
      applyTransform(true);
      updateButtons();
    }

    function slideNext() {
      if (freeScrollMode) {
        computeLayout();
        const amount = Math.max(cardWidth + gap, Math.floor(viewport.clientWidth * 0.85));
        viewport.scrollBy({ left: amount, behavior: 'smooth' });
        return;
      }
      computeLayout();
      const maxLeft = Math.max(0, cards.length - visibleCount);
      slideIndex = Math.min(maxLeft, slideIndex + visibleCount);
      applyTransform(true);
      updateButtons();
    }

    function addSwipe() {
      if (!viewport) return;
      if (freeScrollMode) {
        viewport.style.touchAction = 'pan-x pan-y';
        return;
      }
      viewport.style.touchAction = 'pan-y';
      let startX = 0;
      let deltaX = 0;
      let tracking = false;
      let ignore = false;
      const threshold = 42;

      const isInteractive = (target) => !!(
        target &&
        target.closest &&
        target.closest('button, input, textarea, select, a')
      );

      const onStart = (x, target) => {
        ignore = isInteractive(target);
        if (ignore) return;
        tracking = true;
        startX = x;
        deltaX = 0;
      };

      const onMove = (x) => {
        if (!tracking || ignore) return;
        deltaX = x - startX;
      };

      const onEnd = () => {
        if (!tracking || ignore) {
          tracking = false;
          ignore = false;
          return;
        }
        if (Math.abs(deltaX) > threshold) {
          if (deltaX > 0) slidePrev();
          else slideNext();
        }
        tracking = false;
        ignore = false;
      };

      viewport.addEventListener('pointerdown', (event) => {
        onStart(event.clientX, event.target);
        if (tracking && event.pointerType === 'mouse' && viewport.setPointerCapture) viewport.setPointerCapture(event.pointerId);
      });
      viewport.addEventListener('pointermove', (event) => onMove(event.clientX));
      viewport.addEventListener('pointerup', onEnd);
      viewport.addEventListener('pointercancel', onEnd);

      viewport.addEventListener('touchstart', (event) => {
        if (!event.touches || !event.touches.length) return;
        onStart(event.touches[0].clientX, event.target);
      }, { passive: true });
      viewport.addEventListener('touchmove', (event) => {
        if (!event.touches || !event.touches.length) return;
        onMove(event.touches[0].clientX);
      }, { passive: true });
      viewport.addEventListener('touchend', onEnd);
      viewport.addEventListener('touchcancel', onEnd);
    }

    if (prevBtn) prevBtn.addEventListener('click', slidePrev);
    if (nextBtn) nextBtn.addEventListener('click', slideNext);
    addSwipe();
    if (freeScrollMode) {
      viewport.addEventListener('scroll', updateButtons, { passive: true });
    }

    window.addEventListener('resize', () => {
      computeLayout();
      clampIndex();
      applyFreeScrollViewportStyle();
      applyTransform(false);
      updateButtons();
    });

    return {
      setCards(nextCards) {
        cards = nextCards;
        render();
      }
    };
  }

  const carousel = (viewport && track) ? createCarousel() : null;

  function createWheelCarousel() {
    if (!wheelViewport || !wheelTrack) return null;
    let cards = [];
    let statMap = new Map();
    let slideIndex = 0;
    let visibleCount = 1;
    let cardWidth = 203;
    let gap = 24;
    const freeScrollMode = carouselFreeScroll;

    function applyFreeScrollViewportStyle() {
      if (!freeScrollMode) {
        wheelViewport.style.overflowX = '';
        wheelViewport.style.overflowY = '';
        wheelViewport.style.scrollBehavior = '';
        wheelViewport.style.webkitOverflowScrolling = '';
        return;
      }
      wheelViewport.style.overflowX = 'auto';
      wheelViewport.style.overflowY = 'hidden';
      wheelViewport.style.scrollBehavior = 'smooth';
      wheelViewport.style.webkitOverflowScrolling = 'touch';
    }

    function computeLayout() {
      const viewportWidth = wheelViewport.clientWidth;
      const firstCard = wheelTrack.querySelector('.plex-card');
      cardWidth = firstCard ? firstCard.getBoundingClientRect().width : cssNum('--plex-cardW', 203);
      gap = cssNum('--plex-gap', 24);
      if (viewportWidth <= 0) return;
      visibleCount = Math.max(1, Math.floor((viewportWidth + gap) / (cardWidth + gap)));
    }

    function clampIndex() {
      const maxLeft = Math.max(0, cards.length - visibleCount);
      slideIndex = Math.min(Math.max(0, slideIndex), maxLeft);
    }

    function applyTransform(animated) {
      if (freeScrollMode) {
        wheelTrack.style.transition = 'none';
        wheelTrack.style.transform = 'none';
        return;
      }
      wheelTrack.style.transition = animated ? 'transform .25s ease' : 'none';
      const offset = slideIndex * (cardWidth + gap);
      wheelTrack.style.transform = 'translateX(' + (-offset) + 'px)';
    }

    function updateButtons() {
      if (!wheelPrevBtn || !wheelNextBtn) return;
      if (freeScrollMode) {
        const maxScroll = Math.max(0, wheelViewport.scrollWidth - wheelViewport.clientWidth);
        wheelPrevBtn.disabled = wheelViewport.scrollLeft <= 2;
        wheelNextBtn.disabled = wheelViewport.scrollLeft >= maxScroll - 2;
        return;
      }
      const maxLeft = Math.max(0, cards.length - visibleCount);
      wheelPrevBtn.disabled = slideIndex <= 0;
      wheelNextBtn.disabled = slideIndex >= maxLeft;
    }

    function slidePrev() {
      if (freeScrollMode) {
        computeLayout();
        const amount = Math.max(cardWidth + gap, Math.floor(wheelViewport.clientWidth * 0.85));
        wheelViewport.scrollBy({ left: -amount, behavior: 'smooth' });
        return;
      }
      computeLayout();
      slideIndex = Math.max(0, slideIndex - visibleCount);
      applyTransform(true);
      updateButtons();
    }

    function slideNext() {
      if (freeScrollMode) {
        computeLayout();
        const amount = Math.max(cardWidth + gap, Math.floor(wheelViewport.clientWidth * 0.85));
        wheelViewport.scrollBy({ left: amount, behavior: 'smooth' });
        return;
      }
      computeLayout();
      const maxLeft = Math.max(0, cards.length - visibleCount);
      slideIndex = Math.min(maxLeft, slideIndex + visibleCount);
      applyTransform(true);
      updateButtons();
    }

    function addSwipe() {
      if (!wheelViewport) return;
      if (freeScrollMode) {
        wheelViewport.style.touchAction = 'pan-x pan-y';
        return;
      }
      wheelViewport.style.touchAction = 'pan-y';
      let startX = 0;
      let deltaX = 0;
      let tracking = false;
      let ignore = false;
      const threshold = 42;

      const isInteractive = (target) => !!(
        target &&
        target.closest &&
        target.closest('button, input, textarea, select, .tautulli-thumbwheel')
      );

      const onStart = (x, target) => {
        ignore = isInteractive(target);
        if (ignore) return;
        tracking = true;
        startX = x;
        deltaX = 0;
      };

      const onMove = (x) => {
        if (!tracking || ignore) return;
        deltaX = x - startX;
      };

      const onEnd = () => {
        if (!tracking || ignore) {
          tracking = false;
          ignore = false;
          return;
        }
        if (Math.abs(deltaX) > threshold) {
          if (deltaX > 0) slidePrev();
          else slideNext();
        }
        tracking = false;
        ignore = false;
      };

      wheelViewport.addEventListener('pointerdown', (event) => {
        onStart(event.clientX, event.target);
        if (tracking && event.pointerType === 'mouse' && wheelViewport.setPointerCapture) wheelViewport.setPointerCapture(event.pointerId);
      });
      wheelViewport.addEventListener('pointermove', (event) => onMove(event.clientX));
      wheelViewport.addEventListener('pointerup', onEnd);
      wheelViewport.addEventListener('pointercancel', onEnd);

      wheelViewport.addEventListener('touchstart', (event) => {
        if (!event.touches || !event.touches.length) return;
        onStart(event.touches[0].clientX, event.target);
      }, { passive: true });
      wheelViewport.addEventListener('touchmove', (event) => {
        if (!event.touches || !event.touches.length) return;
        onMove(event.touches[0].clientX);
      }, { passive: true });
      wheelViewport.addEventListener('touchend', onEnd);
      wheelViewport.addEventListener('touchcancel', onEnd);
    }

    function updateCardSelection(cardEl, card, rowIndex) {
      const rowWrap = card.rows[rowIndex];
      if (!rowWrap) return;
      cardEl.dataset.activeIndex = String(rowIndex);
      const posterWell = cardEl.querySelector('.plex-poster-well');
      const nameEl = cardEl.querySelector('.plex-name');
      const metaEl = cardEl.querySelector('.plex-meta');
      const rankEl = cardEl.querySelector('.tautulli-wheel-rank');
      const eyeBtn = cardEl.querySelector('.tautulli-wheel-eye');
      const typeEl = cardEl.querySelector('.tautulli-wheel-type');
      const itemTitleEl = cardEl.querySelector('.tautulli-wheel-item-title');
      if (posterWell) {
        posterWell.innerHTML = rowWrap.image
          ? '<img src="' + escapeHtml(rowWrap.image) + '" alt="' + escapeHtml(rowWrap.title) + '" loading="lazy" referrerpolicy="no-referrer" />'
          : '<div class="plex-placeholder"><div class="plex-placeholder-big">' + escapeHtml(rowWrap.title) + '</div><div class="plex-placeholder-small">No poster</div></div>';
      }
      if (nameEl) nameEl.textContent = rowWrap.title || card.title || 'Unknown';
      if (itemTitleEl) itemTitleEl.textContent = rowWrap.title || 'Unknown';
      if (metaEl) {
        const bits = [];
        if (rowWrap.value !== undefined && rowWrap.value !== null && rowWrap.value !== '') bits.push(String(rowWrap.value));
        if (card.unit) bits.push(card.unit);
        metaEl.textContent = bits.join(' ') || 'No extra details';
      }
      if (rankEl) rankEl.textContent = '#' + String(rowIndex + 1);
      if (eyeBtn) eyeBtn.disabled = !rowWrap.raw;
      if (typeEl) {
        const typeIcon = mediaTypeIcon(rowWrap);
        typeEl.innerHTML = typeIcon.svg;
        typeEl.title = typeIcon.title;
      }
      resolveArtistRow(rowWrap).then((resolved) => {
        if (!resolved) return;
        if (cardEl.dataset.activeIndex !== String(rowIndex)) return;
        if (posterWell) {
          posterWell.innerHTML = resolved.image
            ? '<img src="' + escapeHtml(resolved.image) + '" alt="' + escapeHtml(resolved.title || rowWrap.title || '') + '" loading="lazy" referrerpolicy="no-referrer" />'
            : '<div class="plex-placeholder"><div class="plex-placeholder-big">' + escapeHtml(resolved.title || rowWrap.title || '') + '</div><div class="plex-placeholder-small">No poster</div></div>';
        }
      });
    }

    function render() {
      wheelTrack.innerHTML = '';
      applyFreeScrollViewportStyle();
      if (!cards.length) {
        wheelTrack.innerHTML = '<div class="plex-empty">No stats available.</div>';
        if (freeScrollMode) wheelViewport.scrollLeft = 0;
        updateButtons();
        return;
      }

      cards.forEach((card, cardIndex) => {
        const el = document.createElement('div');
        el.className = 'plex-card tautulli-wheel-card';
        el.dataset.index = String(cardIndex);
        const rowCount = Math.max(1, Math.min(10, card.rows.length));
        const defaultIndex = 0;

        el.innerHTML =
          '<div class="plex-poster-wrap">' +
            '<div class="plex-poster-well"></div>' +
            '<div class="plex-type-icon tautulli-wheel-type" title="Media"></div>' +
            '<div class="plex-pill tautulli-wheel-rank">#1</div>' +
            '<div class="tautulli-wheel-item-title"></div>' +
            '<button class="plex-eye-icon tautulli-wheel-eye" type="button" title="View"><svg viewBox="0 0 24 24" fill="none" stroke="#e8eef7" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12z"></path><circle cx="12" cy="12" r="3"></circle></svg></button>' +
          '</div>' +
          '<div class="tautulli-thumbwheel-wrap">' +
            '<input class="tautulli-thumbwheel" type="range" min="1" max="' + String(rowCount) + '" step="1" value="' + String(defaultIndex + 1) + '" />' +
          '</div>' +
          '<div class="tautulli-wheel-card-title">' + escapeHtml(card.title || 'Top 10') + '</div>';

        wheelTrack.appendChild(el);
        updateCardSelection(el, card, defaultIndex);

        const wheelInput = el.querySelector('.tautulli-thumbwheel');
        const eyeBtn = el.querySelector('.tautulli-wheel-eye');
        if (wheelInput) {
          wheelInput.addEventListener('input', () => {
            const nextIdx = Math.max(0, Math.min(card.rows.length - 1, Number(wheelInput.value) - 1));
            updateCardSelection(el, card, nextIdx);
          });
        }

        if (eyeBtn) {
          eyeBtn.addEventListener('click', () => {
            const idx = Math.max(0, Number(el.dataset.activeIndex || 0));
            const rowWrap = card.rows[idx];
            if (!rowWrap || !rowWrap.raw) return;
            resolveArtistRow(rowWrap).then((resolved) => {
              const stat = statMap.get(card.id) || card;
              const modalRow = getModalRow(resolved || rowWrap) || rowWrap.raw;
              buildModalContent(modalRow, stat, { posterUrl: rowWrap.image || '' });
              openModal();
              loadModalOverview(modalRow);
            });
          });
        }
      });

      computeLayout();
      clampIndex();
      applyTransform(false);
      if (freeScrollMode) wheelViewport.scrollLeft = 0;
      updateButtons();
    }

    if (wheelPrevBtn) wheelPrevBtn.addEventListener('click', slidePrev);
    if (wheelNextBtn) wheelNextBtn.addEventListener('click', slideNext);
    addSwipe();
    if (freeScrollMode) {
      wheelViewport.addEventListener('scroll', updateButtons, { passive: true });
    }

    window.addEventListener('resize', () => {
      computeLayout();
      clampIndex();
      applyFreeScrollViewportStyle();
      applyTransform(false);
      updateButtons();
    });

    return {
      setCards(nextCards, nextStats) {
        cards = Array.isArray(nextCards) ? nextCards : [];
        statMap = nextStats instanceof Map ? nextStats : new Map();
        slideIndex = 0;
        render();
      }
    };
  }

  const wheelCarousel = createWheelCarousel();

  function buildCard(stat) {
    const rows = Array.isArray(stat.rows) ? stat.rows : [];
    const row0 = rows[0] || {};
    const thumb =
      row0.thumb ||
      row0.grandparent_thumb ||
      row0.library_thumb ||
      row0.user_thumb ||
      '';
    const ratingKey = row0.grandparent_rating_key || row0.rating_key || '';
    const isMusicCard = stat.stat_id === 'top_music' || stat.stat_id === 'popular_music';
    const imageFallback = isMusicCard ? 'cover' : 'poster';
    const image = thumb ? imageProxy(thumb, ratingKey, 240, 360, imageFallback) : '';
    const unit = statUnits(stat);

    const mappedRows = rows.map((row) => {
      const rowThumb =
        row.thumb ||
        row.grandparent_thumb ||
        row.library_thumb ||
        row.user_thumb ||
        '';
      const rowKey = row.grandparent_rating_key || row.rating_key || '';
      const rowImage = rowThumb ? imageProxy(rowThumb, rowKey, 240, 360, imageFallback) : '';
      return {
        title: rowTitle(stat.stat_id, row),
        href: musicStatIds.has(stat.stat_id) ? searchLink(rowTitle(stat.stat_id, row)) : rowLink(stat.stat_id, row),
        value: rowValue(stat, row),
        image: rowImage,
        raw: row,
        statId: stat.stat_id,
        resolved: false,
      };
    });

    return {
      id: stat.stat_id,
      title: 'Top 10 ' + (stat.stat_title || stat.stat_id),
      unit,
      image,
      rows: mappedRows,
    };
  }

  function buildModalContent(row, stat, options) {
    const opts = options && typeof options === 'object' ? options : {};
    if (!modalTitle || !modalBody) return;
    const title = rowTitle(stat.stat_id, row) || 'Details';
    const subtitleBits = [];
    if (row.friendly_name || row.user) subtitleBits.push(row.friendly_name || row.user);
    if (row.last_play) subtitleBits.push('Last played');
    modalTitle.textContent = title;
    if (modalSubtitle) modalSubtitle.textContent = subtitleBits.join(' • ');
    if (modalTypeIcon) {
      const typeIcon = mediaTypeIcon({ raw: row, statId: stat.stat_id });
      modalTypeIcon.innerHTML = typeIcon.svg;
    }

    const selectedPosterUrl = String(opts.posterUrl || '').trim();
    const directPosterUrl = String(row.poster_url || '').trim();
    const poster =
      row.thumb ||
      row.grandparent_thumb ||
      row.library_thumb ||
      row.user_thumb ||
      '';
    const posterKey = row.grandparent_rating_key || row.rating_key || '';
    const posterUrl = selectedPosterUrl || directPosterUrl || (poster ? imageProxy(poster, posterKey, 300, 450, 'poster') : '');
    const art = row.art ? imageProxy(row.art, row.rating_key || '', 800, 450, 'art') : '';
    const link = rowLink(stat.stat_id, row);
    const plexHref = plexLink(row);
    const ids = extractExternalIdsFromRow(row);
    const mediaType = mediaTypeForRow(row, stat);
    const encodedQuery = encodeURIComponent(String(title || '').trim());
    const imdbUrl = ids.imdbId
      ? ('https://www.imdb.com/title/' + encodeURIComponent(String(ids.imdbId)) + '/')
      : ('https://www.imdb.com/find/?q=' + encodedQuery);
    const tmdbUrl = ids.tmdbId
      ? ('https://www.themoviedb.org/' + (mediaType === 'tv' ? 'tv/' : 'movie/') + encodeURIComponent(String(ids.tmdbId)))
      : ('https://www.themoviedb.org/search?query=' + encodedQuery);
    const unit = statUnits(stat);
    const value = rowValue(stat, row);

    const pills = [];
    if (unit && value !== undefined && value !== null && value !== '') {
      pills.push('<div class="plex-pill2"><span class="plex-dot" style="background:var(--plex-pill)"></span>' + escapeHtml(String(value)) + ' ' + escapeHtml(unit) + '</div>');
    }
    if (row.media_type) {
      pills.push('<div class="plex-pill2"><span class="plex-dot" style="background:#f6d365"></span>' + escapeHtml(String(row.media_type)) + '</div>');
    }
    if (row.year) {
      pills.push('<div class="plex-pill2"><span class="plex-dot" style="background:#9aa3b5"></span>' + escapeHtml(String(row.year)) + '</div>');
    }

    modalBody.innerHTML =
      '<div class="plex-modal-scroll tautulli-modal-scroll">' +
        '<div class="plex-modal-hero">' +
          (art ? '<img id="tautulliModalBgImg" class="plex-modal-bg" src="' + art + '" alt="" referrerpolicy="no-referrer" />' : '') +
          '<div class="plex-modal-content">' +
            '<div class="plex-modal-poster tautulli-modal-poster">' +
              (posterUrl
                ? '<img id="tautulliModalPosterImg" src="' + posterUrl + '" alt="" referrerpolicy="no-referrer" onerror="this.style.display=\'none\';var fb=document.getElementById(\'tautulliModalPosterFallback\');if(fb)fb.style.display=\'flex\';" />' +
                  '<div id="tautulliModalPosterFallback" class="plex-placeholder" style="height:340px;display:none"><div class="plex-placeholder-big">' + escapeHtml(title) + '</div><div class="plex-placeholder-small">No poster</div></div>'
                : '<div id="tautulliModalPosterFallback" class="plex-placeholder" style="height:340px"><div class="plex-placeholder-big">' + escapeHtml(title) + '</div><div class="plex-placeholder-small">No poster</div></div>'
              ) +
            '</div>' +
            '<div class="plex-modal-meta">' +
              '<div class="plex-pills">' + pills.join('') + '</div>' +
              '<div class="plex-section">' +
                '<h4>Overview</h4>' +
                '<div class="plex-overview-text" id="tautulliModalOverview">Loading overview...</div>' +
              '</div>' +
            '</div>' +
          '</div>' +
        '</div>' +
      '</div>' +
      '<div class="plex-modal-footer tautulli-modal-footer">' +
        '<a id="tautulliModalImdbLink" class="plex-modal-link" href="' + imdbUrl + '" target="_blank" rel="noreferrer">IMDb</a>' +
        '<a id="tautulliModalTmdbLink" class="plex-modal-link" href="' + tmdbUrl + '" target="_blank" rel="noreferrer">TMDb</a>' +
        (link && link !== '#'
          ? '<a class="plex-modal-link" href="' + link + '" target="_blank" rel="noreferrer">Tautulli</a>'
          : ''
        ) +
        (plexHref && plexHref !== '#'
          ? '<a class="plex-modal-link" href="' + plexHref + '" target="_blank" rel="noreferrer">Plex</a>'
          : ''
        ) +
      '</div>';
  }

  function fallbackOverview(row) {
    const bits = [];
    if (row.year) bits.push(String(row.year));
    if (row.content_rating) bits.push(String(row.content_rating));
    if (row.media_type) bits.push(String(row.media_type));
    if (bits.length) return 'Details: ' + bits.join(' • ');
    return 'No overview available for this item.';
  }

  function loadModalOverview(row) {
    const target = document.getElementById('tautulliModalOverview');
    const imdbLinkEl = document.getElementById('tautulliModalImdbLink');
    const tmdbLinkEl = document.getElementById('tautulliModalTmdbLink');
    const bgImgEl = document.getElementById('tautulliModalBgImg');
    if (!target) return;
    const ratingKey = row && (row.rating_key || row.grandparent_rating_key);
    if (!ratingKey) {
      target.textContent = fallbackOverview(row || {});
      return;
    }
    fetchWithLog(apiUrl('get_metadata', { rating_key: ratingKey }), null, 'Tautulli metadata')
      .then((res) => res.json())
      .then((payload) => {
        const data = payload && payload.response && payload.response.data ? payload.response.data : null;
        const summary = data && typeof data.summary === 'string' ? data.summary.trim() : '';
        target.textContent = summary || fallbackOverview(row || {});
        if (bgImgEl && data) {
          const art = data.art || data.grandparent_art || '';
          const artKey = data.grandparent_rating_key || data.rating_key || ratingKey;
          const bgThumb = data.thumb || data.grandparent_thumb || data.parent_thumb || '';
          const bgSource = art
            ? imageProxy(art, artKey, 800, 450, 'art')
            : (bgThumb ? imageProxy(bgThumb, artKey, 800, 450, 'cover') : '');
          if (bgSource) bgImgEl.src = bgSource;
        }
        const ids = extractExternalIdsFromMetadata(data);
        const mediaType = mediaTypeForRow(data || row || {}, null);
        if (imdbLinkEl && /^tt\d+$/i.test(String(ids.imdbId || ''))) {
          imdbLinkEl.href = 'https://www.imdb.com/title/' + encodeURIComponent(String(ids.imdbId)) + '/';
        }
        if (tmdbLinkEl && /^\d+$/.test(String(ids.tmdbId || ''))) {
          tmdbLinkEl.href = 'https://www.themoviedb.org/' + (mediaType === 'tv' ? 'tv/' : 'movie/') + encodeURIComponent(String(ids.tmdbId));
        }
      })
      .catch(() => {
        target.textContent = fallbackOverview(row || {});
      });

    // Try to upgrade IMDb/TMDb links to exact item URLs using Plex metadata IDs.
    fetchWithLog(
      '/api/plex/discovery/details?ratingKey=' + encodeURIComponent(String(ratingKey)),
      null,
      'Plex discovery details'
    )
      .then((res) => res.json())
      .then((payload) => {
        const imdbId = String(payload && payload.imdbId || '').trim();
        const tmdbId = String(payload && payload.tmdbId || '').trim();
        const mediaType = mediaTypeForRow(row, null);
        if (imdbLinkEl && /^tt\d+$/i.test(imdbId)) {
          imdbLinkEl.href = 'https://www.imdb.com/title/' + encodeURIComponent(imdbId) + '/';
        }
        if (tmdbLinkEl && /^\d+$/.test(tmdbId)) {
          tmdbLinkEl.href = 'https://www.themoviedb.org/' + (mediaType === 'tv' ? 'tv/' : 'movie/') + encodeURIComponent(tmdbId);
        }
      })
      .catch(() => {});
  }

  function fetchHomeStats() {
    const url = apiUrl('get_home_stats', {
      stats_count: 10,
      stats_type: 'plays',
      time_range: 30,
    });
    fetchWithLog(url, null, 'Tautulli home stats')
      .then((res) => res.json())
      .then((payload) => {
        const stats = payload && payload.response && payload.response.data ? payload.response.data : [];
        const list = Array.isArray(stats) ? stats : [];
        const sortedList = (() => {
          if (!preferredStatsCards.length) return list;
          const rank = new Map(preferredStatsCards.map((id, index) => [id, index]));
          return list
            .filter((stat) => rank.has(stat.stat_id))
            .sort((a, b) => (rank.get(a.stat_id) || 0) - (rank.get(b.stat_id) || 0));
        })();
        const cards = sortedList.filter((stat) => Array.isArray(stat.rows) && stat.rows.length).map(buildCard);
        const statsMap = new Map(sortedList.map((stat) => [stat.stat_id, stat]));
        if (carousel) carousel.setCards(cards);
        if (wheelCarousel) wheelCarousel.setCards(cards, statsMap);

        if (track) {
          track.querySelectorAll('.tautulli-card').forEach((cardEl, cardIndex) => {
            const poster = cardEl.querySelector('.tautulli-card-poster');
            const selectedTitleEl = cardEl.querySelector('.tautulli-card-selected-title');
            const typeEl = cardEl.querySelector('.tautulli-card-type');
            const rankEl = cardEl.querySelector('.tautulli-card-rank');
            const viewButton = cardEl.querySelector('.tautulli-action--view');
            const card = cards[cardIndex];
            if (!card || !poster) return;
            const setPoster = (image, title) => {
              const nextTitle = title || card.title || 'Unknown';
              poster.innerHTML = image
                ? '<img src="' + escapeHtml(image) + '" alt="' + escapeHtml(nextTitle) + '" loading="lazy" referrerpolicy="no-referrer" />'
                : '<div class="plex-placeholder"><div class="plex-placeholder-big">' + escapeHtml(nextTitle) + '</div><div class="plex-placeholder-small">No poster</div></div>';
            };

            let activeRowWrap = null;
            const setActiveRow = (rowWrap, itemEl) => {
              if (!rowWrap || !rowWrap.raw) return;
              activeRowWrap = rowWrap;
              setPoster(rowWrap.image || card.image || '', rowWrap.title || card.title);
              if (selectedTitleEl) selectedTitleEl.textContent = rowWrap.title || card.title || 'Unknown';
              if (rankEl && itemEl) rankEl.textContent = '#' + String(Number(itemEl.dataset.index || 0) + 1);
              if (typeEl) {
                const typeIcon = mediaTypeIcon(rowWrap);
                typeEl.innerHTML = typeIcon.svg;
                typeEl.title = typeIcon.title;
              }
              cardEl.querySelectorAll('.tautulli-item').forEach((el) => el.classList.remove('is-active'));
              if (itemEl) itemEl.classList.add('is-active');

              resolveArtistRow(rowWrap).then((resolved) => {
                if (activeRowWrap !== rowWrap) return;
                if (resolved) {
                  setPoster(resolved.image || rowWrap.image || card.image || '', resolved.title || rowWrap.title || card.title);
                  if (selectedTitleEl) selectedTitleEl.textContent = resolved.title || rowWrap.title || card.title || 'Unknown';
                }
              });
            };

            const firstRow = card.rows[0];
            const firstRowEl = cardEl.querySelector('.tautulli-item[data-index="0"]');
            if (firstRow) setActiveRow(firstRow, firstRowEl);

            if (viewButton) {
              viewButton.addEventListener('click', () => {
                if (!activeRowWrap || !activeRowWrap.raw) return;
                const modalRow = getModalRow(activeRowWrap) || activeRowWrap.raw;
                buildModalContent(modalRow, sortedList.find((stat) => stat.stat_id === card.id) || card, { posterUrl: activeRowWrap.image || '' });
                openModal();
                loadModalOverview(modalRow);
              });
            }

            cardEl.querySelectorAll('.tautulli-item').forEach((itemEl) => {
              itemEl.addEventListener('mouseenter', () => {
                const img = itemEl.dataset.image || '';
                if (img) setPoster(img, itemEl.dataset.title || card.title);
              });
              itemEl.addEventListener('mouseleave', () => {
                if (activeRowWrap && activeRowWrap.image) {
                  setPoster(activeRowWrap.image, activeRowWrap.title || card.title);
                } else if (card.image) {
                  setPoster(card.image, card.title);
                } else if (activeRowWrap) {
                  setPoster('', activeRowWrap.title || card.title);
                } else {
                  setPoster('', card.title);
                }
              });
              itemEl.addEventListener('click', (event) => {
                event.preventDefault();
                event.stopPropagation();
                const idx = Number(itemEl.dataset.index);
                const rowWrap = card.rows[idx];
                if (!rowWrap) return;
                setActiveRow(rowWrap, itemEl);
              });
            });
          });
        }
      })
      .catch(() => {
        if (track) track.innerHTML = '<div class="plex-empty">Unable to load Tautulli watch statistics.</div>';
        if (wheelTrack) wheelTrack.innerHTML = '<div class="plex-empty">Unable to load Tautulli watch statistics.</div>';
      });
  }

  function fetchServerIdentifier() {
    const url = apiUrl('get_server_info');
    return fetchWithLog(url, null, 'Tautulli server info')
      .then((res) => res.json())
      .then((payload) => {
        const data = payload && payload.response && payload.response.data ? payload.response.data : {};
        plexMachineId = String(
          data.pms_identifier ||
          data.pms_machine_identifier ||
          data.machine_identifier ||
          ''
        ).trim();
      })
      .catch(() => {
        plexMachineId = '';
      });
  }

  fetchServerIdentifier().finally(fetchHomeStats);
})();
