(() => {
  const carouselFreeScroll = (() => {
    try {
      return localStorage.getItem('launcharr-carousel-free-scroll') === '1';
    } catch (err) {
      return false;
    }
  })();

  const config = window.PULSARR_OVERVIEW_CONFIG || {};
  const appId = String(config.appId || 'pulsarr').trim().toLowerCase() || 'pulsarr';
  const domPrefix = String(config.domPrefix || appId).trim() || appId;
  const appName = String(config.appName || 'Pulsarr').trim() || 'Pulsarr';
  const root = document.querySelector('.plex-overview') || document.documentElement;
  const defaultDisplaySettings = {
    showSubtitle: true,
    showMeta: true,
    showPill: true,
    showTypeIcon: true,
    showViewIcon: true,
    showUsername: true,
  };
  const displaySettingsMap = config.displaySettings && typeof config.displaySettings === 'object'
    ? config.displaySettings
    : {};

  function sectionDisplaySettings(sectionId) {
    const raw = sectionId && typeof displaySettingsMap[sectionId] === 'object'
      ? displaySettingsMap[sectionId]
      : {};
    return {
      showSubtitle: raw.showSubtitle !== false,
      showMeta: raw.showMeta !== false,
      showPill: raw.showPill !== false,
      showTypeIcon: raw.showTypeIcon !== false,
      showViewIcon: raw.showViewIcon !== false,
      showUsername: raw.showUsername !== false,
    };
  }

  const requestsDisplaySettings = sectionDisplaySettings('recent-requests');
  const watchlistedDisplaySettings = sectionDisplaySettings('most-watchlisted');

  function prefixedId(suffix) {
    return domPrefix + suffix;
  }

  const modules = {
    requests: {
      viewport: document.getElementById(prefixedId('RequestsViewport')),
      track: document.getElementById(prefixedId('RequestsTrack')),
      prevBtn: document.getElementById(prefixedId('RequestsPrevBtn')),
      nextBtn: document.getElementById(prefixedId('RequestsNextBtn')),
      statusFilter: document.getElementById(prefixedId('RequestsStatusFilter')),
      limitFilter: document.getElementById(prefixedId('RequestsLimitSelect')),
      items: [],
      carousel: null,
    },
    watchlisted: {
      viewport: document.getElementById(prefixedId('WatchlistedViewport')),
      track: document.getElementById(prefixedId('WatchlistedTrack')),
      prevBtn: document.getElementById(prefixedId('WatchlistedPrevBtn')),
      nextBtn: document.getElementById(prefixedId('WatchlistedNextBtn')),
      typeFilter: document.getElementById(prefixedId('WatchlistedTypeFilter')),
      limitFilter: document.getElementById(prefixedId('WatchlistedLimitSelect')),
      items: [],
      carousel: null,
    },
  };

  const hasRequests = Boolean(modules.requests.viewport && modules.requests.track);
  const hasWatchlisted = Boolean(modules.watchlisted.viewport && modules.watchlisted.track);
  if (!hasRequests && !hasWatchlisted) return;
  const CACHE_TTL_MS = 5 * 60 * 1000;

  const modal = {
    backdrop: document.getElementById(prefixedId('ModalBackdrop')),
    close: document.getElementById(prefixedId('ModalClose')),
    title: document.getElementById(prefixedId('ModalTitle')),
    subtitle: document.getElementById(prefixedId('ModalSubtitle')),
    body: document.getElementById(prefixedId('ModalBody')),
    typeIcon: document.getElementById(prefixedId('ModalTypeIcon')),
  };

  if (hasRequests) {
    modules.requests.carousel = createCarousel({
      viewport: modules.requests.viewport,
      track: modules.requests.track,
      prevBtn: modules.requests.prevBtn,
      nextBtn: modules.requests.nextBtn,
      onView: openItemModal,
      renderCard: (item) => renderCard(item, requestsDisplaySettings),
    });
    modules.requests.statusFilter?.addEventListener('change', loadRecentRequests);
    modules.requests.limitFilter?.addEventListener('change', applyRequestFilters);
    loadRecentRequests();
  }

  if (hasWatchlisted) {
    modules.watchlisted.carousel = createCarousel({
      viewport: modules.watchlisted.viewport,
      track: modules.watchlisted.track,
      prevBtn: modules.watchlisted.prevBtn,
      nextBtn: modules.watchlisted.nextBtn,
      onView: openItemModal,
      renderCard: (item) => renderCard(item, watchlistedDisplaySettings),
    });
    modules.watchlisted.typeFilter?.addEventListener('change', applyWatchlistedFilters);
    modules.watchlisted.limitFilter?.addEventListener('change', applyWatchlistedFilters);
    loadMostWatchlisted();
  }

  wireModal();
  bindCollapseButtons();

  window.addEventListener('resize', () => {
    if (modules.requests.carousel) modules.requests.carousel.updateLayout();
    if (modules.watchlisted.carousel) modules.watchlisted.carousel.updateLayout();
  });

  function escapeHtml(value) {
    return String(value || '').replace(/[&<>"']/g, (char) => (
      {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;',
      }[char]
    ));
  }

  function cssNum(name, fallback) {
    const raw = getComputedStyle(root).getPropertyValue(name).trim();
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function pad2(value) {
    return String(value).padStart(2, '0');
  }

  function formatDateLabel(value) {
    if (!value) return '';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    return pad2(date.getDate()) + '/' + pad2(date.getMonth() + 1) + '/' + date.getFullYear();
  }

  function timeAgo(value) {
    const timestamp = new Date(value).getTime();
    if (!Number.isFinite(timestamp)) return '';
    const seconds = Math.max(0, (Date.now() - timestamp) / 1000);
    if (seconds < 60) return 'just now';
    if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
    if (seconds < 86400) return Math.floor(seconds / 3600) + 'h ago';
    return Math.floor(seconds / 86400) + 'd ago';
  }

  function toArray(value) {
    return Array.isArray(value) ? value : [];
  }

  function safeJsonParse(value) {
    try {
      return JSON.parse(value);
    } catch (_err) {
      return null;
    }
  }

  function cacheRead(key) {
    try {
      const raw = window.localStorage.getItem(key);
      if (!raw) return null;
      const parsed = safeJsonParse(raw);
      if (!parsed || typeof parsed !== 'object') return null;
      if (!Array.isArray(parsed.items) || typeof parsed.ts !== 'number') return null;
      if ((Date.now() - parsed.ts) > CACHE_TTL_MS) return null;
      return parsed.items;
    } catch (_err) {
      return null;
    }
  }

  function cacheWrite(key, items) {
    try {
      window.localStorage.setItem(key, JSON.stringify({ ts: Date.now(), items }));
    } catch (_err) {
      // ignore localStorage issues
    }
  }

  function requestsCacheKey(limit, status) {
    return 'launcharr:' + appId + ':recent-requests:v1:limit:' + String(limit) + ':status:' + String(status || '');
  }

  function watchlistedCacheKey(limit) {
    return 'launcharr:' + appId + ':most-watchlisted:v1:limit:' + String(limit);
  }

  function findTmdbIdFromGuids(guids) {
    if (!Array.isArray(guids)) return '';
    const hit = guids.find((item) => String(item || '').toLowerCase().startsWith('tmdb:'));
    if (!hit) return '';
    const parts = String(hit).split(':');
    return parts[1] ? String(parts[1]).trim() : '';
  }

  function findImdbIdFromGuids(guids) {
    if (!Array.isArray(guids)) return '';
    const hit = guids.find((item) => String(item || '').toLowerCase().startsWith('imdb:'));
    if (!hit) return '';
    const parts = String(hit).split(':');
    const id = parts[1] ? String(parts[1]).trim() : '';
    return /^tt\d+$/i.test(id) ? id : '';
  }

  function unwrapTmdbDetails(payload) {
    if (!payload || typeof payload !== 'object') return {};
    if (payload.metadata && payload.metadata.details && typeof payload.metadata.details === 'object') {
      return payload.metadata.details;
    }
    if (payload.details && typeof payload.details === 'object') return payload.details;
    if (payload.data && typeof payload.data === 'object') return payload.data;
    if (payload.result && typeof payload.result === 'object') return payload.result;
    return payload;
  }

  function withParams(path, params) {
    const url = new URL(path, window.location.origin);
    Object.entries(params || {}).forEach(([key, value]) => {
      if (value === undefined || value === null || value === '') return;
      url.searchParams.set(key, String(value));
    });
    return url.toString();
  }

  function logApi(level, message, meta) {
    const logger = console[level] || console.log;
    if (meta && typeof meta === 'object') {
      logger(`[Launcharr] ${message}`, meta);
    } else {
      logger(`[Launcharr] ${message}`);
    }
  }

  async function fetchJson(url) {
    logApi('info', appName + ' request', { url });
    const response = await fetch(url, { headers: { Accept: 'application/json' } });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      logApi('error', appName + ' response failed', { url, status: response.status, error: payload?.error || '' });
      throw new Error(payload?.error || ('Request failed with status ' + response.status));
    }
    logApi('info', appName + ' response ok', { url, status: response.status });
    return payload;
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

  function eyeSvg() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#e8eef7" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12z"></path>' +
      '<circle cx="12" cy="12" r="3"></circle></svg>';
  }

  function statusColor(status) {
    const value = String(status || '').toLowerCase();
    if (value === 'available') return '#2bd56f';
    if (value === 'requested') return '#ffd36c';
    return '#9aa3b5';
  }

  function posterUrl(item) {
    const explicit = item?.poster || item?.posterUrl || item?.image || item?.thumb || item?.poster_path || item?.posterPath;
    if (!explicit) return '';
    const value = String(explicit).trim();
    if (!value) return '';
    if (/^https?:\/\//i.test(value)) return value;
    if (value.startsWith('/')) return 'https://image.tmdb.org/t/p/w500' + value;
    return '';
  }

  function normalizeRequestStatus(value) {
    const text = String(value || '').toLowerCase();
    if (text.includes('avail')) return 'available';
    if (text.includes('request')) return 'requested';
    return 'requested';
  }

  function statusFilterValue(value) {
    const text = String(value || '').toLowerCase();
    if (text === 'pending') return 'requested';
    if (text === 'approved') return 'available';
    if (text === 'declined') return 'declined';
    if (text === 'available' || text === 'requested') return text;
    return 'all';
  }

  function statusParamValue(value) {
    const normalized = statusFilterValue(value);
    if (normalized === 'requested' || normalized === 'available') return normalized;
    return '';
  }

  function normalizeRequest(item) {
    const typeValue = String(item?.contentType || item?.content_type || item?.type || '').toLowerCase();
    const mediaType = (typeValue.includes('show') || typeValue.includes('tv') || typeValue.includes('series')) ? 'show' : 'movie';
    const createdAtRaw = item?.createdAt || item?.created_at || item?.time || item?.ts || '';
    const status = normalizeRequestStatus(item?.status);
    return {
      title: String(item?.title || item?.name || 'Unknown Request'),
      subtitle: '@' + String(item?.userName || item?.username || item?.user || 'unknown'),
      meta: timeAgo(createdAtRaw) || formatDateLabel(createdAtRaw),
      thumb: posterUrl(item),
      pill: status.charAt(0).toUpperCase() + status.slice(1),
      pillColor: statusColor(status),
      status,
      mediaType,
      createdAt: new Date(createdAtRaw).getTime(),
      overview: String(item?.overview || item?.description || ''),
      tmdbId: findTmdbIdFromGuids(item?.guids),
      imdbId: findImdbIdFromGuids(item?.guids),
      raw: item,
    };
  }

  function normalizeWatchlistedEntry(entry, mediaType) {
    const typeValue = String(entry?.content_type || entry?.contentType || entry?.type || mediaType || '').toLowerCase();
    const resolvedType = typeValue.includes('show') || typeValue.includes('tv') ? 'show' : 'movie';
    const count = Number(entry?.count ?? entry?.watched ?? entry?.plays ?? 0) || 0;
    const users = Array.isArray(entry?.users) ? entry.users.length : 0;
    const createdAtRaw = entry?.createdAt
      || entry?.created_at
      || entry?.requestDate
      || entry?.request_date
      || entry?.addedAt
      || entry?.added_at
      || entry?.firstRequested
      || entry?.first_requested
      || entry?.date
      || '';
    return {
      title: String(entry?.title || entry?.name || 'Unknown Title'),
      subtitle: resolvedType === 'show' ? 'TV Show' : 'Movie',
      meta: users ? String(users) + ' users' : '',
      thumb: posterUrl(entry),
      pill: count > 0 ? String(count) + ' watched' : 'Top',
      score: count,
      mediaType: resolvedType,
      createdAt: new Date(createdAtRaw).getTime(),
      overview: String(entry?.overview || entry?.description || ''),
      tmdbId: findTmdbIdFromGuids(entry?.guids),
      imdbId: findImdbIdFromGuids(entry?.guids),
      raw: entry,
    };
  }

  function createCarousel(options) {
    const viewport = options.viewport;
    const track = options.track;
    const prevBtn = options.prevBtn;
    const nextBtn = options.nextBtn;
    const onView = typeof options.onView === 'function' ? options.onView : null;
    let cards = [];
    let slideIndex = 0;
    let visibleCount = 1;
    let cardWidth = 203;
    let gap = 24;
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
      const firstCard = track.querySelector('.plex-card') || viewport.querySelector('.plex-card');
      const measuredWidth = firstCard ? Math.round(firstCard.getBoundingClientRect().width) : 0;
      cardWidth = measuredWidth > 0 ? measuredWidth : cssNum('--plex-cardW', 203);
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
        track.innerHTML = '<div class="plex-empty">No results found.</div>';
        if (freeScrollMode) viewport.scrollLeft = 0;
        updateButtons();
        return;
      }

      cards.forEach((item, index) => {
        const cardRenderer = typeof options.renderCard === 'function'
          ? options.renderCard
          : (cardItem) => renderCard(cardItem, defaultDisplaySettings);
        const card = cardRenderer(item);
        card.dataset.index = String(index);
        const viewBtn = card.querySelector('[data-action="view"]');
        if (viewBtn && onView) {
          viewBtn.addEventListener('click', (event) => {
            event.preventDefault();
            event.stopPropagation();
            onView(item);
          });
        }
        track.appendChild(card);
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
      if (freeScrollMode) {
        viewport.style.touchAction = 'pan-x pan-y';
        return;
      }
      viewport.style.touchAction = 'pan-y';
      let startX = 0;
      let deltaX = 0;
      let tracking = false;
      const threshold = 42;

      const isInteractive = (target) => !!(
        target && target.closest && target.closest('button, input, select, textarea, a, [data-action="view"]')
      );

      const onStart = (x, target) => {
        if (isInteractive(target)) {
          tracking = false;
          return;
        }
        tracking = true;
        startX = x;
        deltaX = 0;
      };

      const onMove = (x) => {
        if (!tracking) return;
        deltaX = x - startX;
      };

      const onEnd = () => {
        if (!tracking) return;
        if (Math.abs(deltaX) > threshold) {
          if (deltaX > 0) slidePrev();
          else slideNext();
        }
        tracking = false;
      };

      viewport.addEventListener('pointerdown', (event) => {
        onStart(event.clientX, event.target);
        if (tracking && event.pointerType === 'mouse' && viewport.setPointerCapture) viewport.setPointerCapture(event.pointerId);
      });
      viewport.addEventListener('pointermove', (event) => onMove(event.clientX));
      viewport.addEventListener('pointerup', onEnd);
      viewport.addEventListener('pointercancel', onEnd);

      viewport.addEventListener('touchstart', (event) => {
        if (!event.touches?.length) return;
        onStart(event.touches[0].clientX, event.target);
      }, { passive: true });
      viewport.addEventListener('touchmove', (event) => {
        if (!event.touches?.length) return;
        onMove(event.touches[0].clientX);
      }, { passive: true });
      viewport.addEventListener('touchend', onEnd);
      viewport.addEventListener('touchcancel', onEnd);
    }

    prevBtn?.addEventListener('click', slidePrev);
    nextBtn?.addEventListener('click', slideNext);
    addSwipe();
    if (freeScrollMode) {
      viewport.addEventListener('scroll', updateButtons, { passive: true });
    }

    return {
      setItems(nextItems) {
        cards = Array.isArray(nextItems) ? nextItems : [];
        slideIndex = 0;
        render();
      },
      updateLayout() {
        computeLayout();
        clampIndex();
        applyFreeScrollViewportStyle();
        applyTransform(false);
        updateButtons();
      },
    };
  }

  function renderCard(item, displaySettings) {
    const card = document.createElement('div');
    card.className = 'plex-card';
    const title = escapeHtml(item.title || 'Unknown');
    const rawSubtitle = !displaySettings.showUsername && String(item.subtitle || '').trim().startsWith('@')
      ? ''
      : String(item.subtitle || '');
    const subtitle = displaySettings.showSubtitle ? escapeHtml(rawSubtitle) : '';
    const meta = displaySettings.showMeta ? escapeHtml(item.meta || '') : '';
    const pill = escapeHtml(item.pill || '');
    const typeSvg = item.mediaType === 'show' ? tvIcon() : movieIcon();
    const metaLine = [subtitle, meta].filter(Boolean).join(' | ');

    card.innerHTML =
      '<div class="plex-poster-wrap">' +
        '<div class="plex-poster-well">' +
          (item.thumb
            ? '<img src="' + item.thumb + '" alt="' + title + '" loading="lazy" referrerpolicy="no-referrer" />'
            : '<div class="plex-placeholder"><div class="plex-placeholder-big">' + title + '</div><div class="plex-placeholder-small">No poster</div></div>') +
          (displaySettings.showTypeIcon ? '<div class="plex-type-icon" title="' + (item.mediaType === 'show' ? 'TV' : 'Movie') + '">' + typeSvg + '</div>' : '') +
          (displaySettings.showViewIcon ? '<div class="plex-eye-icon" title="View" data-action="view">' + eyeSvg() + '</div>' : '') +
          (displaySettings.showPill && pill ? '<div class="plex-pill"' + (item.pillColor ? ' style="background:' + item.pillColor + '"' : '') + '>' + pill + '</div>' : '') +
        '</div>' +
      '</div>' +
      '<div class="plex-footer">' +
        '<div class="plex-name">' + title + '</div>' +
        (metaLine ? '<div class="plex-meta">' + metaLine + '</div>' : '') +
      '</div>';

    return card;
  }

  function openItemModal(item) {
    if (!modal.backdrop || !modal.body || !modal.title) return;
    const sectionDisplay = item?.sectionId === 'recent-requests'
      ? requestsDisplaySettings
      : watchlistedDisplaySettings;
    const typeSvg = item.mediaType === 'show' ? tvIcon() : movieIcon();
    if (modal.typeIcon) modal.typeIcon.innerHTML = typeSvg;
    modal.title.textContent = item.title || 'Details';
    if (modal.subtitle) {
      const subtitleText = !sectionDisplay.showUsername && String(item.subtitle || '').trim().startsWith('@')
        ? ''
        : String(item.subtitle || '');
      const subtitle = [
        sectionDisplay.showSubtitle ? subtitleText : '',
        sectionDisplay.showMeta ? (item.meta || '') : '',
      ].filter(Boolean).join(' • ');
      modal.subtitle.textContent = subtitle;
    }

    const poster = item.thumb || '';
    const overview = escapeHtml(item.overview || 'Loading overview...');
    const query = encodeURIComponent(String(item.title || '').trim());
    const mediaType = item.mediaType === 'show' ? 'tv' : 'movie';
    const tmdbPart = item.tmdbId ? ('&tmdb=' + encodeURIComponent(String(item.tmdbId))) : '';
    const imdbUrl = item.imdbId
      ? ('https://www.imdb.com/title/' + encodeURIComponent(String(item.imdbId)) + '/')
      : ('https://www.imdb.com/find/?q=' + query);
    const tmdbUrl = item.tmdbId
      ? ('https://www.themoviedb.org/' + (mediaType === 'tv' ? 'tv/' : 'movie/') + encodeURIComponent(String(item.tmdbId)))
      : ('https://www.themoviedb.org/search?query=' + query);
    const pills = [
      '<div class="plex-pill2"><span class="plex-dot" style="background:#f6d365"></span>' + escapeHtml(item.mediaType === 'show' ? 'TV Show' : 'Movie') + '</div>',
      sectionDisplay.showPill && item.pill ? '<div class="plex-pill2"><span class="plex-dot" style="background:var(--plex-pill)"></span>' + escapeHtml(item.pill) + '</div>' : '',
      sectionDisplay.showMeta && item.meta ? '<div class="plex-pill2"><span class="plex-dot" style="background:#9aa3b5"></span>' + escapeHtml(item.meta) + '</div>' : '',
    ].filter(Boolean).join('');

    modal.body.innerHTML =
      '<div class="plex-modal-scroll">' +
        '<div class="plex-modal-hero">' +
          (poster ? '<img class="plex-modal-bg" src="' + poster + '" alt="" referrerpolicy="no-referrer" />' : '') +
          '<div class="plex-modal-content">' +
            '<div class="plex-modal-poster">' +
              (poster ? '<img src="' + poster + '" alt="" referrerpolicy="no-referrer" />' : '') +
            '</div>' +
            '<div class="plex-modal-meta">' +
              '<div class="plex-pills">' + pills + '</div>' +
              '<div class="plex-section"><h4>Overview</h4><div class="plex-overview-text">' + overview + '</div></div>' +
            '</div>' +
          '</div>' +
        '</div>' +
      '</div>' +
      '<div class="plex-modal-footer">' +
        '<a id="' + prefixedId('ModalImdbLink') + '" class="plex-modal-link" href="' + imdbUrl + '" target="_blank" rel="noreferrer">IMDb</a>' +
        '<a id="' + prefixedId('ModalTmdbLink') + '" class="plex-modal-link" href="' + tmdbUrl + '" target="_blank" rel="noreferrer">TMDb</a>' +
        '<a class="plex-modal-link" href="/apps/tautulli/launch?q=' + query + tmdbPart + '&type=' + encodeURIComponent(mediaType) + '" target="_blank" rel="noreferrer">Tautulli</a>' +
        '<a class="plex-modal-link" href="/apps/plex/launch?q=' + query + tmdbPart + '&type=' + encodeURIComponent(mediaType) + '" target="_blank" rel="noreferrer">Plex</a>' +
      '</div>';

    modal.backdrop.classList.remove('plex-hidden');
    document.body.style.overflow = 'hidden';
    loadModalOverview(item);
  }

  async function loadModalOverview(item) {
    if (!modal.body) return;
    const overviewNode = modal.body.querySelector('.plex-overview-text');
    const imdbLinkEl = modal.body.querySelector('#' + prefixedId('ModalImdbLink'));
    const tmdbLinkEl = modal.body.querySelector('#' + prefixedId('ModalTmdbLink'));
    if (!overviewNode) return;
    if (item.overview) {
      overviewNode.textContent = item.overview;
      return;
    }
    if (!item.tmdbId) {
      overviewNode.textContent = 'No overview available for this title.';
      return;
    }
    try {
      const detailsPayload = await fetchJson(withParams('/api/' + encodeURIComponent(appId) + '/tmdb/' + (item.mediaType === 'show' ? 'tv' : 'movie') + '/' + encodeURIComponent(item.tmdbId), {}));
      const details = unwrapTmdbDetails(detailsPayload);
      const summary = String(details?.overview || '').trim();
      overviewNode.textContent = summary || 'No overview available for this title.';
      const imdbId = String(details?.imdb_id || '').trim();
      if (imdbLinkEl && /^tt\d+$/i.test(imdbId)) {
        imdbLinkEl.href = 'https://www.imdb.com/title/' + encodeURIComponent(imdbId) + '/';
      }
      if (tmdbLinkEl && item.tmdbId) {
        const mediaType = item.mediaType === 'show' ? 'tv' : 'movie';
        tmdbLinkEl.href = 'https://www.themoviedb.org/' + mediaType + '/' + encodeURIComponent(String(item.tmdbId));
      }
    } catch (err) {
      overviewNode.textContent = 'No overview available for this title.';
    }
  }

  function closeModal() {
    if (!modal.backdrop) return;
    modal.backdrop.classList.add('plex-hidden');
    document.body.style.overflow = '';
  }

  function wireModal() {
    modal.close?.addEventListener('click', closeModal);
    modal.backdrop?.addEventListener('click', (event) => {
      if (event.target === modal.backdrop) closeModal();
    });
    window.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') closeModal();
    });
  }

  function applyRequestFilters() {
    if (!modules.requests.carousel) return;
    const statusValue = statusFilterValue(modules.requests.statusFilter?.value || 'all');
    const limitValue = Number(modules.requests.limitFilter?.value || 20);
    const limit = Number.isFinite(limitValue) && limitValue > 0 ? limitValue : 20;
    const filtered = modules.requests.items
      .filter((item) => {
        if (statusValue === 'all') return true;
        if (statusValue === 'declined') return item.status !== 'requested' && item.status !== 'available';
        return item.status === statusValue;
      })
      .slice(0, limit);
    modules.requests.carousel.setItems(filtered);
  }

  function applyWatchlistedFilters() {
    if (!modules.watchlisted.carousel) return;
    const typeValue = String(modules.watchlisted.typeFilter?.value || 'all');
    const limitValue = Number(modules.watchlisted.limitFilter?.value || 20);
    const limit = Number.isFinite(limitValue) && limitValue > 0 ? limitValue : 20;
    const filtered = modules.watchlisted.items
      .filter((item) => typeValue === 'all' || item.mediaType === typeValue)
      .slice(0, limit);
    modules.watchlisted.carousel.setItems(filtered);
  }

  async function loadRecentRequests() {
    if (!hasRequests) return;

    try {
      const limitValue = Number(modules.requests.limitFilter?.value || 20);
      const limit = Number.isFinite(limitValue) && limitValue > 0 ? limitValue : 20;
      const status = statusParamValue(modules.requests.statusFilter?.value || '');
      const cacheKey = requestsCacheKey(limit, status);
      const cachedRecords = cacheRead(cacheKey);

      if (cachedRecords) {
        modules.requests.items = cachedRecords
          .map(normalizeRequest)
          .map((item) => ({ ...item, sectionId: 'recent-requests' }))
          .sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
        applyRequestFilters();
      } else {
        modules.requests.track.innerHTML = '<div class="plex-empty">Loading...</div>';
      }

      const payload = await fetchJson(withParams('/api/' + encodeURIComponent(appId) + '/stats/recent-requests', { limit, status }));
      const records = toArray(payload?.results).length
        ? toArray(payload.results)
        : (toArray(payload?.items).length
          ? toArray(payload.items)
          : (toArray(payload?.data).length ? toArray(payload.data) : toArray(payload)));
      cacheWrite(cacheKey, records);

      modules.requests.items = records
        .map(normalizeRequest)
        .map((item) => ({ ...item, sectionId: 'recent-requests' }))
        .sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
      applyRequestFilters();
    } catch (err) {
      modules.requests.track.innerHTML = '<div class="plex-empty">Unable to load ' + escapeHtml(appName) + ' requests: ' + escapeHtml(err.message || '') + '</div>';
    }
  }

  async function loadMostWatchlisted() {
    if (!hasWatchlisted) return;

    try {
      const limitValue = Number(modules.watchlisted.limitFilter?.value || 20);
      const limit = Number.isFinite(limitValue) && limitValue > 0 ? limitValue : 20;
      const cacheKey = watchlistedCacheKey(limit);
      const cachedRecords = cacheRead(cacheKey);

      const extractRecords = (payload) => (
        toArray(payload?.results).length
          ? toArray(payload.results)
          : (toArray(payload?.items).length
            ? toArray(payload.items)
            : (toArray(payload?.data).length ? toArray(payload.data) : toArray(payload)))
      );

      if (cachedRecords) {
        const moviesCached = cachedRecords
          .filter((item) => item && item.__mediaType === 'movie')
          .map((item) => normalizeWatchlistedEntry(item, 'movie'));
        const showsCached = cachedRecords
          .filter((item) => item && item.__mediaType === 'show')
          .map((item) => normalizeWatchlistedEntry(item, 'show'));
        modules.watchlisted.items = moviesCached
          .concat(showsCached)
          .map((item) => ({ ...item, sectionId: 'most-watchlisted' }))
          .sort((a, b) => (b.score || 0) - (a.score || 0));
        applyWatchlistedFilters();
      } else {
        modules.watchlisted.track.innerHTML = '<div class="plex-empty">Loading...</div>';
      }

      let movieItems = [];
      let showItems = [];
      let successCount = 0;

      const applyMergedItems = () => {
        modules.watchlisted.items = movieItems
          .concat(showItems)
          .map((item) => ({ ...item, sectionId: 'most-watchlisted' }))
          .sort((a, b) => (b.score || 0) - (a.score || 0));
        applyWatchlistedFilters();
      };

      const moviePromise = fetchJson(withParams('/api/' + encodeURIComponent(appId) + '/stats/movies', { limit, offset: 0, days: 30 }))
        .then((moviePayload) => {
          const movieRecords = extractRecords(moviePayload);
          movieItems = movieRecords.map((item) => normalizeWatchlistedEntry(item, 'movie'));
          successCount += 1;
          applyMergedItems();
        });

      const showPromise = fetchJson(withParams('/api/' + encodeURIComponent(appId) + '/stats/shows', { limit, offset: 0, days: 30 }))
        .then((showPayload) => {
          const showRecords = extractRecords(showPayload);
          showItems = showRecords.map((item) => normalizeWatchlistedEntry(item, 'show'));
          successCount += 1;
          applyMergedItems();
        });

      await Promise.allSettled([moviePromise, showPromise]);
      if (!successCount) {
        throw new Error('Failed to load watchlisted items');
      }
      const cachePayload = movieItems.map((item) => ({ ...(item.raw || {}), __mediaType: 'movie' }))
        .concat(showItems.map((item) => ({ ...(item.raw || {}), __mediaType: 'show' })));
      cacheWrite(cacheKey, cachePayload);
    } catch (err) {
      modules.watchlisted.track.innerHTML = '<div class="plex-empty">Unable to load ' + escapeHtml(appName) + ' most watchlisted: ' + escapeHtml(err.message || '') + '</div>';
    }
  }

  function bindCollapseButtons() {
    document.querySelectorAll('.plex-collapse-btn[data-target^="' + domPrefix + '-"]').forEach((button) => {
      button.addEventListener('click', () => {
        const targetId = button.getAttribute('data-target');
        const section = targetId ? document.getElementById(targetId) : null;
        if (!section) return;
        section.classList.toggle('plex-collapsed');
      });
    });
  }
})();
