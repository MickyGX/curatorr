(function () {
  const carouselFreeScroll = (() => {
    try {
      return localStorage.getItem('launcharr-carousel-free-scroll') === '1';
    } catch (err) {
      return false;
    }
  })();

  const config = window.PLEX_OVERVIEW_CONFIG || {};
  const token = String(config.token || '').trim();
  const rawBaseUrl = String(config.baseUrl || config.host || '').trim();
  const baseUrl = normalizeBaseUrl(rawBaseUrl);
  const hasPlexConnection = Boolean(baseUrl && token);
  const root = document.querySelector('.plex-overview') || document.documentElement;
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

  if (!hasPlexConnection) {
    const activeTrack = document.getElementById('plexActiveTrack');
    const recentTrack = document.getElementById('plexRecentTrack');
    const message = '<div class="plex-empty">Configure Plex URL and token in Plex settings.</div>';
    if (activeTrack) activeTrack.innerHTML = message;
    if (recentTrack) recentTrack.innerHTML = message;
  }

  function normalizeBaseUrl(value) {
    let url = String(value || '').trim();
    if (!url) return '';
    if (!/^https?:\/\//i.test(url)) url = 'https://' + url;
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

  function pad2(n) {
    return String(n).padStart(2, '0');
  }

  function fmtTime(sec) {
    sec = Math.max(0, Math.floor(Number(sec) || 0));
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    if (h > 0) return h + ':' + pad2(m) + ':' + pad2(s);
    return m + ':' + pad2(s);
  }

  function episodeCode(season, episode) {
    const s = parseInt(season, 10);
    const e = parseInt(episode, 10);
    if (Number.isFinite(s) && Number.isFinite(e)) return 'S' + pad2(s) + 'E' + pad2(e);
    return '';
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

  function plexUrl(path) {
    if (!path) return '';
    return baseUrl + path + '?X-Plex-Token=' + encodeURIComponent(token);
  }

  function fetchXml(url, callback, errorCB) {
    const xhr = new XMLHttpRequest();
    xhr.open('GET', url, true);
    xhr.onreadystatechange = function () {
      if (xhr.readyState === 4) {
        if (xhr.status >= 200 && xhr.status < 400) callback(xhr.responseText);
        else if (errorCB) errorCB(xhr.status);
      }
    };
    xhr.send();
  }

  function fetchJson(url, callback, errorCB) {
    const xhr = new XMLHttpRequest();
    xhr.open('GET', url, true);
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) return;
      if (xhr.status >= 200 && xhr.status < 400) {
        try {
          const payload = JSON.parse(xhr.responseText || '{}');
          callback(payload);
        } catch (err) {
          if (errorCB) errorCB(0);
        }
      } else if (errorCB) {
        errorCB(xhr.status);
      }
    };
    xhr.send();
  }

  function pillBgForState(state) {
    state = String(state || '').toLowerCase();
    if (state === 'paused') return '#ffd36c';
    if (state === 'buffering') return '#ffb86b';
    if (state === 'stopped') return '#9aa3b5';
    return 'var(--plex-pill)';
  }

  function getSummary(v) {
    const s = v.getAttribute('summary') || '';
    if (s && String(s).trim().length) return String(s).trim();
    return '';
  }

  function sessionsUrl() {
    return baseUrl + '/status/sessions?X-Plex-Token=' + encodeURIComponent(token);
  }

  function parseSessions(xmlText) {
    const parser = new DOMParser();
    const xml = parser.parseFromString(xmlText, 'application/xml');
    const videos = xml.getElementsByTagName('Video');
    const out = [];

    for (let i = 0; i < videos.length; i += 1) {
      const v = videos[i];
      const plexType = String(v.getAttribute('type') || '').toLowerCase();
      const isTv = plexType === 'episode' || plexType === 'season' || plexType === 'show';

      const userNode = v.getElementsByTagName('User')[0] || null;
      const playerNode = v.getElementsByTagName('Player')[0] || null;
      const transcodeNode = v.getElementsByTagName('TranscodeSession')[0] || null;

      const user = userNode ? (userNode.getAttribute('title') || userNode.getAttribute('username') || '') : '';
      const state = playerNode ? (playerNode.getAttribute('state') || '') : '';
      const device = playerNode ? (playerNode.getAttribute('product') || playerNode.getAttribute('title') || '') : '';

      const showTitle = v.getAttribute('grandparentTitle') || '';
      const epTitle = v.getAttribute('title') || '';
      const season = v.getAttribute('parentIndex') || '';
      const episode = v.getAttribute('index') || '';
      const code = episodeCode(season, episode);

      const displayTitle = isTv
        ? (showTitle || v.getAttribute('parentTitle') || epTitle || 'Unknown')
        : (v.getAttribute('title') || 'Unknown');

      const posterPath = isTv
        ? (v.getAttribute('grandparentThumb') || v.getAttribute('parentThumb') || v.getAttribute('thumb') || '')
        : (v.getAttribute('thumb') || '');
      const artPath = v.getAttribute('art') || '';

      const viewOffsetMs = Number(v.getAttribute('viewOffset') || 0);
      const durationMs = Number(v.getAttribute('duration') || 0);
      const pos = fmtTime(viewOffsetMs / 1000);
      const dur = fmtTime(durationMs / 1000);
      const progress = durationMs > 0 ? pos + ' / ' + dur : '';

      let decision = '';
      if (transcodeNode) decision = transcodeNode.getAttribute('decision') || '';
      decision = decision ? decision.charAt(0).toUpperCase() + decision.slice(1) : '';

      const media = v.getElementsByTagName('Media')[0] || null;
      const w = media ? (media.getAttribute('width') || '') : '';
      const h = media ? (media.getAttribute('height') || '') : '';
      const res = w && h ? w + 'x' + h : '';

      out.push({
        kind: isTv ? 'tv' : 'movie',
        title: displayTitle,
        episodeTitle: isTv ? epTitle : '',
        code: isTv ? code : '',
        user: user || 'unknown',
        state: state || 'playing',
        device: device || '',
        decision: decision || '',
        progress: progress || '',
        resolution: res || '',
        thumb: plexUrl(posterPath),
        art: plexUrl(artPath),
        overview: getSummary(v)
      });
    }

    return out;
  }

  function formatDateFromEpochSeconds(sec) {
    if (!sec) return '';
    const d = new Date(Number(sec) * 1000);
    if (Number.isNaN(d.getTime())) return '';
    return pad2(d.getDate()) + '/' + pad2(d.getMonth() + 1) + '/' + d.getFullYear();
  }

  function formatRecentAddedPill(sec) {
    if (!sec) return '';
    const d = new Date(Number(sec) * 1000);
    if (Number.isNaN(d.getTime())) return '';
    const now = new Date();
    const isToday = (
      d.getFullYear() === now.getFullYear() &&
      d.getMonth() === now.getMonth() &&
      d.getDate() === now.getDate()
    );
    if (isToday) return 'Today';
    return pad2(d.getDate()) + '/' + pad2(d.getMonth() + 1) + '/' + String(d.getFullYear()).slice(-2);
  }

  function yearFromDateString(s) {
    if (!s) return '';
    const m = String(s).match(/^(\d{4})/);
    return m ? m[1] : '';
  }

  function extractExternalIdsFromNode(node) {
    const out = { imdbId: '', tmdbId: '' };
    if (!node || typeof node.getElementsByTagName !== 'function') return out;

    const rawGuidValues = [
      node.getAttribute('guid'),
      node.getAttribute('parentGuid'),
      node.getAttribute('grandparentGuid'),
    ];
    for (let i = 0; i < rawGuidValues.length; i += 1) {
      const raw = String(rawGuidValues[i] || '').trim();
      if (!raw) continue;
      if (!out.imdbId) {
        const imdbMatch = raw.match(/imdb:\/\/(tt\d+)/i);
        if (imdbMatch) out.imdbId = imdbMatch[1];
      }
      if (!out.tmdbId) {
        const tmdbMatch = raw.match(/tmdb:\/\/(\d+)/i);
        if (tmdbMatch) out.tmdbId = tmdbMatch[1];
      }
      if (out.imdbId && out.tmdbId) break;
    }

    const guidNodes = node.getElementsByTagName('Guid');
    for (let i = 0; i < guidNodes.length; i += 1) {
      const idValue = String(guidNodes[i].getAttribute('id') || '').trim();
      if (!idValue) continue;
      const lower = idValue.toLowerCase();
      if (!out.imdbId && lower.startsWith('imdb://')) {
        const imdbId = idValue.slice('imdb://'.length).split('?')[0].trim();
        if (/^tt\d+$/i.test(imdbId)) out.imdbId = imdbId;
      }
      if (!out.tmdbId && lower.startsWith('tmdb://')) {
        const tmdbId = idValue.slice('tmdb://'.length).split('?')[0].trim();
        if (/^\d+$/.test(tmdbId)) out.tmdbId = tmdbId;
      }
      if (out.imdbId && out.tmdbId) break;
    }

    return out;
  }

  function parseRecentlyAdded(xmlText) {
    const parser = new DOMParser();
    const xml = parser.parseFromString(xmlText, 'application/xml');
    const nodes = xml.getElementsByTagName('Video');
    const out = [];

    for (let i = 0; i < nodes.length; i += 1) {
      const v = nodes[i];
      const plexType = (v.getAttribute('type') || '').toLowerCase();
      const isTv = plexType === 'episode' || plexType === 'season' || plexType === 'show';

      const title = isTv
        ? (v.getAttribute('grandparentTitle') || v.getAttribute('parentTitle') || v.getAttribute('title') || 'Unknown')
        : (v.getAttribute('title') || 'Unknown');

      const seasonNum = v.getAttribute('parentIndex') || v.getAttribute('seasonIndex') || '';
      const episodeNum = v.getAttribute('index') || '';

      const yr =
        v.getAttribute('year') ||
        yearFromDateString(v.getAttribute('originallyAvailableAt')) ||
        v.getAttribute('grandparentYear') ||
        '';

      const posterPath = isTv
        ? (v.getAttribute('grandparentThumb') || v.getAttribute('parentThumb') || v.getAttribute('thumb') || '')
        : (v.getAttribute('thumb') || '');

      const artPath = v.getAttribute('art') || '';
      const externalIds = extractExternalIdsFromNode(v);

      out.push({
        kind: isTv ? 'tv' : 'movie',
        title: title,
        year: yr,
        season: seasonNum,
        episode: episodeNum,
        summary: v.getAttribute('summary') || '',
        studio: v.getAttribute('studio') || '',
        contentRating: v.getAttribute('contentRating') || '',
        addedAt: v.getAttribute('addedAt') || '',
        thumb: plexUrl(posterPath),
        art: plexUrl(artPath),
        imdbId: externalIds.imdbId,
        tmdbId: externalIds.tmdbId
      });
    }

    return out;
  }

  function createCarousel(options) {
    const {
      viewport,
      track,
      prevBtn,
      nextBtn,
      modalBackdrop,
      modalClose,
      modalTitle,
      modalSubtitle,
      modalBody,
      modalTypeIcon,
      emptyTitle,
      emptySubtitle
    } = options;

    let items = [];
    let slideIndex = 0;
    let visibleCount = 4;
    let stepPx = 314;
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

    function computeCarouselLayout() {
      const firstCard = track.querySelector('.plex-card') || viewport.querySelector('.plex-card');
      const measuredWidth = firstCard ? Math.round(firstCard.getBoundingClientRect().width) : 0;
      const cardW = measuredWidth > 0 ? measuredWidth : cssNum('--plex-cardW', 203);
      const gap = cssNum('--plex-gap', 24);
      stepPx = cardW + gap;

      const vw = viewport.clientWidth;
      visibleCount = Math.max(1, Math.floor((vw + gap) / (cardW + gap)));
    }

    function clampSlideIndex() {
      const maxLeft = Math.max(0, items.length - visibleCount);
      slideIndex = Math.max(0, Math.min(slideIndex, maxLeft));
    }

    function updateNavButtons() {
      if (freeScrollMode) {
        const maxScroll = Math.max(0, viewport.scrollWidth - viewport.clientWidth);
        if (prevBtn) prevBtn.disabled = viewport.scrollLeft <= 2;
        if (nextBtn) nextBtn.disabled = viewport.scrollLeft >= maxScroll - 2;
        return;
      }
      const maxLeft = Math.max(0, items.length - visibleCount);
      if (prevBtn) prevBtn.disabled = slideIndex <= 0;
      if (nextBtn) nextBtn.disabled = slideIndex >= maxLeft;
    }

    function applyTransform(animate) {
      if (freeScrollMode) {
        track.style.transition = 'none';
        track.style.transform = 'none';
        return;
      }
      if (animate === false) {
        track.style.transition = 'none';
        track.style.transform = 'translate3d(' + (-slideIndex * stepPx) + 'px,0,0)';
        void track.offsetHeight;
        track.style.transition = 'transform 420ms cubic-bezier(.22,.9,.24,1)';
        return;
      }
      track.style.transform = 'translate3d(' + (-slideIndex * stepPx) + 'px,0,0)';
    }

    function renderTrack(renderCard) {
      track.innerHTML = '';
      applyFreeScrollViewportStyle();

      if (!items.length) {
        slideIndex = 0;
        applyTransform(false);
        if (freeScrollMode) viewport.scrollLeft = 0;
        updateNavButtons();
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

      items.forEach((it, idx) => {
        const card = renderCard(it, idx);
        track.appendChild(card);
      });

      computeCarouselLayout();
      clampSlideIndex();
      applyTransform(false);
      if (freeScrollMode) viewport.scrollLeft = 0;
      updateNavButtons();
    }

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

    function slidePrev() {
      if (freeScrollMode) {
        computeCarouselLayout();
        const amount = Math.max(stepPx, Math.floor(viewport.clientWidth * 0.85));
        viewport.scrollBy({ left: -amount, behavior: 'smooth' });
        return;
      }
      computeCarouselLayout();
      slideIndex = Math.max(0, slideIndex - visibleCount);
      applyTransform(true);
      updateNavButtons();
    }

    function slideNext() {
      if (freeScrollMode) {
        computeCarouselLayout();
        const amount = Math.max(stepPx, Math.floor(viewport.clientWidth * 0.85));
        viewport.scrollBy({ left: amount, behavior: 'smooth' });
        return;
      }
      computeCarouselLayout();
      const maxLeft = Math.max(0, items.length - visibleCount);
      slideIndex = Math.min(maxLeft, slideIndex + visibleCount);
      applyTransform(true);
      updateNavButtons();
    }

    if (prevBtn) prevBtn.addEventListener('click', slidePrev);
    if (nextBtn) nextBtn.addEventListener('click', slideNext);

    function addSwipe() {
      if (freeScrollMode) {
        viewport.style.touchAction = 'pan-x pan-y';
        return;
      }
      let startX = 0;
      let startY = 0;
      let movedX = 0;
      let movedY = 0;
      let down = false;

      const isInteractive = (target) => !!(target.closest && target.closest('[data-action="view"], button, a, input, textarea, .plex-modal, .plex-modal-backdrop'));

      const onDown = (x, y) => {
        down = true;
        startX = x;
        startY = y;
        movedX = 0;
        movedY = 0;
      };

      const onMove = (x, y) => {
        if (!down) return;
        movedX = x - startX;
        movedY = y - startY;
      };

      const onUp = () => {
        if (!down) return;
        down = false;
        if (Math.abs(movedX) > 40 && Math.abs(movedX) > Math.abs(movedY) * 1.2) {
          if (movedX > 0) slidePrev();
          else slideNext();
        }
      };

      viewport.addEventListener('pointerdown', (e) => {
        if (isInteractive(e.target)) return;
        onDown(e.clientX, e.clientY);
        if (e.pointerType === 'mouse' && viewport.setPointerCapture) viewport.setPointerCapture(e.pointerId);
      });

      viewport.addEventListener('pointermove', (e) => {
        onMove(e.clientX, e.clientY);
      });

      viewport.addEventListener('pointerup', onUp);
      viewport.addEventListener('pointercancel', () => { down = false; });

      viewport.addEventListener('touchstart', (e) => {
        if (isInteractive(e.target)) return;
        const touch = e.touches[0];
        if (!touch) return;
        onDown(touch.clientX, touch.clientY);
      }, { passive: true });

      viewport.addEventListener('touchmove', (e) => {
        const touch = e.touches[0];
        if (!touch) return;
        onMove(touch.clientX, touch.clientY);
      }, { passive: true });

      viewport.addEventListener('touchend', onUp);
      viewport.addEventListener('touchcancel', () => { down = false; });
    }

    addSwipe();
    if (freeScrollMode) {
      viewport.addEventListener('scroll', updateNavButtons, { passive: true });
    }

    let hasDetailHandler = false;

    return {
      setItems(nextItems, renderCard, detailHandler) {
        items = nextItems;
        renderTrack(renderCard);
        if (detailHandler && !hasDetailHandler) {
          track.addEventListener('click', (ev) => {
            const viewBtn = ev.target.closest && ev.target.closest('[data-action="view"]');
            if (!viewBtn) return;
            ev.preventDefault();
            ev.stopPropagation();
            const cardEl = viewBtn.closest('.plex-card');
            if (!cardEl) return;
            const idx = Number(cardEl.dataset.index);
            if (Number.isFinite(idx) && items[idx]) detailHandler(items[idx]);
          });
          hasDetailHandler = true;
        }
      },
      updateLayout() {
        computeCarouselLayout();
        clampSlideIndex();
        applyFreeScrollViewportStyle();
        applyTransform(false);
        updateNavButtons();
      },
      openModal,
      closeModal
    };
  }

  // Plex Active Streams
  (function () {
    const elPrev = document.getElementById('plexActivePrevBtn');
    const elNext = document.getElementById('plexActiveNextBtn');
    const elViewport = document.getElementById('plexActiveViewport');
    const elTrack = document.getElementById('plexActiveTrack');

    const modalBackdrop = document.getElementById('plexActiveModalBackdrop');
    const modalClose = document.getElementById('plexActiveModalClose');
    const modalTitle = document.getElementById('plexActiveModalTitle');
    const modalSubtitle = document.getElementById('plexActiveModalSubtitle');
    const modalBody = document.getElementById('plexActiveModalBody');
    const modalTypeIcon = document.getElementById('plexActiveModalTypeIcon');
    const activeDisplay = sectionDisplaySettings('active');

    if (!elViewport || !elTrack || !hasPlexConnection) return;

    const carousel = createCarousel({
      viewport: elViewport,
      track: elTrack,
      prevBtn: elPrev,
      nextBtn: elNext,
      modalBackdrop,
      modalClose,
      modalTitle,
      modalSubtitle,
      modalBody,
      modalTypeIcon,
      emptyTitle: 'No active streams',
      emptySubtitle: 'Nothing is playing right now'
    });

    function showDetailsModal(it) {
      if (!modalBackdrop) return;
      modalTypeIcon.innerHTML = it.kind === 'tv' ? tvIcon() : movieIcon();
      modalTitle.textContent = it.title;

      const subBits = [];
      if (activeDisplay.showSubtitle && activeDisplay.showUsername) subBits.push('@' + it.user);
      if (activeDisplay.showPill && it.state) subBits.push(it.state);
      if (activeDisplay.showMeta && it.progress) subBits.push(it.progress);
      modalSubtitle.textContent = subBits.join(' • ');

      carousel.openModal();

      const pillStateBg = pillBgForState(it.state);

      let overviewText = '';
      if (it.kind === 'tv' && it.episodeTitle) {
        overviewText = it.episodeTitle;
        if (it.overview) overviewText += '\n\n' + it.overview;
      } else {
        overviewText = it.overview || '';
      }
      if (!overviewText) overviewText = 'No overview available for this title.';
      const query = encodeURIComponent(String(it.title || '').trim());
      const mediaType = it.kind === 'tv' ? 'tv' : 'movie';
      const imdbUrl = it.imdbId
        ? ('https://www.imdb.com/title/' + encodeURIComponent(String(it.imdbId)) + '/')
        : ('https://www.imdb.com/find/?q=' + query);
      const tmdbUrl = it.tmdbId
        ? ('https://www.themoviedb.org/' + (mediaType === 'tv' ? 'tv/' : 'movie/') + encodeURIComponent(String(it.tmdbId)))
        : ('https://www.themoviedb.org/search?query=' + query);

      modalBody.innerHTML =
        '<div class="plex-modal-scroll">' +
          '<div class="plex-modal-hero">' +
            (it.art ? '<img class="plex-modal-bg" src="' + it.art + '" alt="" referrerpolicy="no-referrer" />' : '') +
            '<div class="plex-modal-content">' +
              '<div class="plex-modal-poster">' +
                (it.thumb
                  ? '<img src="' + it.thumb + '" alt="' + escapeHtml(it.title) + '" referrerpolicy="no-referrer" />'
                  : '<div class="plex-placeholder" style="height:340px"><div class="plex-placeholder-big">' + escapeHtml(it.title) + '</div><div class="plex-placeholder-small">No poster</div></div>'
                ) +
              '</div>' +
              '<div class="plex-modal-meta">' +
                '<div class="plex-pills">' +
                  '<span class="plex-pill2">' + (it.kind === 'tv' ? 'TV Show' : 'Movie') + '</span>' +
                  (it.kind === 'tv' && it.code ? '<span class="plex-pill2">' + escapeHtml(it.code) + '</span>' : '') +
                  (activeDisplay.showUsername ? '<span class="plex-pill2">User @' + escapeHtml(it.user) + '</span>' : '') +
                  (activeDisplay.showPill && it.state ? '<span class="plex-pill2"><span class="plex-dot" style="background:' + pillStateBg + '"></span>' + escapeHtml(it.state) + '</span>' : '') +
                  (activeDisplay.showMeta && it.progress ? '<span class="plex-pill2">Progress ' + escapeHtml(it.progress) + '</span>' : '') +
                  (it.device ? '<span class="plex-pill2">Device ' + escapeHtml(it.device) + '</span>' : '') +
                  (it.decision ? '<span class="plex-pill2">Decision ' + escapeHtml(it.decision) + '</span>' : '') +
                  (it.resolution ? '<span class="plex-pill2">Resolution ' + escapeHtml(it.resolution) + '</span>' : '') +
                '</div>' +
                '<div class="plex-section">' +
                  '<h4>Overview</h4>' +
                  '<div class="plex-overview-text">' + escapeHtml(overviewText) + '</div>' +
                '</div>' +
              '</div>' +
            '</div>' +
          '</div>' +
        '</div>' +
        '<div class="plex-modal-footer">' +
          '<a class="plex-modal-link" href="' + imdbUrl + '" target="_blank" rel="noreferrer">IMDb</a>' +
          '<a class="plex-modal-link" href="' + tmdbUrl + '" target="_blank" rel="noreferrer">TMDb</a>' +
          '<a class="plex-modal-link" href="/apps/tautulli/launch?q=' + query + '&type=' + encodeURIComponent(mediaType) + '" target="_blank" rel="noreferrer">Tautulli</a>' +
          '<a class="plex-modal-link" href="/apps/plex/launch?q=' + query + '&type=' + encodeURIComponent(mediaType) + '" target="_blank" rel="noreferrer">Plex</a>' +
        '</div>';
    }

    function renderCard(it, idx) {
      const typeSvg = it.kind === 'tv' ? tvIcon() : movieIcon();
      const pillText = it.state || 'playing';
      const pillBg = pillBgForState(it.state);

      const metaBits = [];
      if (activeDisplay.showSubtitle && it.kind === 'tv' && it.code) metaBits.push(it.code);
      if (activeDisplay.showSubtitle && activeDisplay.showUsername) metaBits.push('@' + it.user);
      if (activeDisplay.showMeta && it.progress) metaBits.push(it.progress);
      const metaLine = metaBits.join(' • ');

      const card = document.createElement('div');
      card.className = 'plex-card';
      card.dataset.index = String(idx);

      card.innerHTML =
        '<div class="plex-poster-wrap">' +
          '<div class="plex-poster-well">' +
            (it.thumb
              ? '<img src="' + it.thumb + '" alt="' + escapeHtml(it.title) + '" loading="lazy" referrerpolicy="no-referrer" />'
              : '<div class="plex-placeholder"><div class="plex-placeholder-big">' + escapeHtml(it.title) + '</div><div class="plex-placeholder-small">No poster provided</div></div>'
            ) +
            (activeDisplay.showPill ? '<div class="plex-pill" style="background:' + pillBg + '">' + escapeHtml(pillText) + '</div>' : '') +
            (activeDisplay.showTypeIcon ? '<div class="plex-type-icon" title="' + (it.kind === 'tv' ? 'TV' : 'Movie') + '">' + typeSvg + '</div>' : '') +
            (activeDisplay.showViewIcon ? '<div class="plex-eye-icon" title="View" data-action="view">' + eyeSvg() + '</div>' : '') +
          '</div>' +
        '</div>' +
        '<div class="plex-footer">' +
          '<div class="plex-name">' + escapeHtml(it.title) + '</div>' +
          (metaLine ? '<div class="plex-meta">' + escapeHtml(metaLine) + '</div>' : '') +
        '</div>';

      return card;
    }

    function loadSessions() {
      elTrack.innerHTML = '<div class="plex-empty">Loading…</div>';

      fetchXml(sessionsUrl(), function (xmlText) {
        const items = parseSessions(xmlText);
        carousel.setItems(items, renderCard, showDetailsModal);
      }, function (status) {
        elTrack.innerHTML = '<div class="plex-empty">Failed to load (Status ' + status + ')</div>';
      });
    }

    window.addEventListener('resize', () => {
      carousel.updateLayout();
    });

    setInterval(loadSessions, 15000);
    loadSessions();
  })();

  // Plex Recently Added
  (function () {
    const SECTION_IDS_FALLBACK = { movie: '1', show: '2' };
    let sectionIds = { ...SECTION_IDS_FALLBACK };
    let sectionsResolved = false;
    let sectionsResolving = false;
    let sectionResolveQueue = [];

    const elType = document.getElementById('plexRecentTypeFilter');
    const elWindow = document.getElementById('plexRecentWindowFilter');
    const elPrev = document.getElementById('plexRecentPrevBtn');
    const elNext = document.getElementById('plexRecentNextBtn');
    const elViewport = document.getElementById('plexRecentViewport');
    const elTrack = document.getElementById('plexRecentTrack');

    const modalBackdrop = document.getElementById('plexRecentModalBackdrop');
    const modalClose = document.getElementById('plexRecentModalClose');
    const modalTitle = document.getElementById('plexRecentModalTitle');
    const modalSubtitle = document.getElementById('plexRecentModalSubtitle');
    const modalBody = document.getElementById('plexRecentModalBody');
    const modalTypeIcon = document.getElementById('plexRecentModalTypeIcon');
    const recentDisplay = sectionDisplaySettings('recent');

    if (!elViewport || !elTrack || !hasPlexConnection) return;

    const carousel = createCarousel({
      viewport: elViewport,
      track: elTrack,
      prevBtn: elPrev,
      nextBtn: elNext,
      modalBackdrop,
      modalClose,
      modalTitle,
      modalSubtitle,
      modalBody,
      modalTypeIcon,
      emptyTitle: 'Nothing added yet',
      emptySubtitle: 'Check back soon'
    });

    function scoreLibrarySection(section, mediaType) {
      if (!section) return -9999;
      const title = String(section.title || '').trim().toLowerCase();
      let score = section.hidden ? 0 : 100;
      if (mediaType === 'movie') {
        if (title === 'movies' || title === 'movie') score += 60;
        else if (title.includes('movie')) score += 30;
      } else {
        if (title === 'tv shows' || title === 'shows' || title === 'series') score += 60;
        else if (title.includes('tv') || title.includes('show') || title.includes('series')) score += 30;
        if (title.includes('kids') || title.includes('recorded')) score -= 10;
      }
      return score;
    }

    function chooseLibrarySectionKey(list, mediaType) {
      if (!Array.isArray(list) || !list.length) return '';
      let best = list[0];
      let bestScore = scoreLibrarySection(best, mediaType);
      for (let i = 1; i < list.length; i += 1) {
        const current = list[i];
        const score = scoreLibrarySection(current, mediaType);
        if (score > bestScore) {
          best = current;
          bestScore = score;
        }
      }
      return String(best?.key || '').trim();
    }

    function resolveLibrarySectionIds(done) {
      const onDone = typeof done === 'function' ? done : function () {};
      if (sectionsResolved) {
        onDone();
        return;
      }
      sectionResolveQueue.push(onDone);
      if (sectionsResolving) return;
      sectionsResolving = true;

      const complete = () => {
        sectionsResolved = true;
        sectionsResolving = false;
        const queue = sectionResolveQueue.slice();
        sectionResolveQueue = [];
        queue.forEach((fn) => {
          try { fn(); } catch (err) {}
        });
      };

      const url = baseUrl + '/library/sections?X-Plex-Token=' + encodeURIComponent(token);
      fetchXml(url, function (xmlText) {
        try {
          const parser = new DOMParser();
          const xml = parser.parseFromString(xmlText, 'application/xml');
          const directories = Array.from(xml.getElementsByTagName('Directory'));
          const movies = [];
          const shows = [];

          directories.forEach((node) => {
            const key = String(node.getAttribute('key') || '').trim();
            const type = String(node.getAttribute('type') || '').trim().toLowerCase();
            if (!key) return;
            const entry = {
              key,
              title: String(node.getAttribute('title') || '').trim(),
              hidden: String(node.getAttribute('hidden') || '0') === '1',
            };
            if (type === 'movie') movies.push(entry);
            if (type === 'show') shows.push(entry);
          });

          const movieKey = chooseLibrarySectionKey(movies, 'movie');
          const showKey = chooseLibrarySectionKey(shows, 'show');
          sectionIds.movie = movieKey || sectionIds.movie || SECTION_IDS_FALLBACK.movie;
          sectionIds.show = showKey || sectionIds.show || SECTION_IDS_FALLBACK.show;
        } catch (err) {
          // Keep fallbacks if parsing fails.
        }
        complete();
      }, function () {
        complete();
      });
    }

    function showDetailsModal(it) {
      if (!modalBackdrop) return;
      modalTypeIcon.innerHTML = it.kind === 'tv' ? tvIcon() : movieIcon();
      modalTitle.textContent = it.title;

      const subBits = [];
      if (recentDisplay.showMeta && it.year) subBits.push(it.year);
      if (recentDisplay.showSubtitle && it.addedAt) subBits.push('Added ' + formatDateFromEpochSeconds(it.addedAt));
      modalSubtitle.textContent = subBits.join(' • ');

      carousel.openModal();

      const overviewText = it.summary || 'No summary available for this title.';
      const query = encodeURIComponent(String(it.title || '').trim());
      const mediaType = it.kind === 'tv' ? 'tv' : 'movie';
      const imdbUrl = it.imdbId
        ? ('https://www.imdb.com/title/' + encodeURIComponent(String(it.imdbId)) + '/')
        : ('https://www.imdb.com/find/?q=' + query);
      const tmdbUrl = it.tmdbId
        ? ('https://www.themoviedb.org/' + (mediaType === 'tv' ? 'tv/' : 'movie/') + encodeURIComponent(String(it.tmdbId)))
        : ('https://www.themoviedb.org/search?query=' + query);

      modalBody.innerHTML =
        '<div class="plex-modal-scroll">' +
          '<div class="plex-modal-hero">' +
            (it.art ? '<img class="plex-modal-bg" src="' + it.art + '" alt="" referrerpolicy="no-referrer" />' : '') +
            '<div class="plex-modal-content">' +
              '<div class="plex-modal-poster">' +
                (it.thumb
                  ? '<img src="' + it.thumb + '" alt="' + escapeHtml(it.title) + '" referrerpolicy="no-referrer" />'
                  : '<div class="plex-placeholder" style="height:340px"><div class="plex-placeholder-big">' + escapeHtml(it.title) + '</div><div class="plex-placeholder-small">No poster</div></div>'
                ) +
              '</div>' +
              '<div class="plex-modal-meta">' +
                '<div class="plex-pills">' +
                  '<span class="plex-pill2">' + (it.kind === 'tv' ? 'TV Show' : 'Movie') + '</span>' +
                  (it.season ? '<span class="plex-pill2">S' + escapeHtml(it.season) + 'E' + escapeHtml(it.episode || '') + '</span>' : '') +
                  (it.contentRating ? '<span class="plex-pill2">' + escapeHtml(it.contentRating) + '</span>' : '') +
                  (it.studio ? '<span class="plex-pill2">' + escapeHtml(it.studio) + '</span>' : '') +
                '</div>' +
                '<div class="plex-section">' +
                  '<h4>Overview</h4>' +
                  '<div class="plex-overview-text">' + escapeHtml(overviewText) + '</div>' +
                '</div>' +
              '</div>' +
            '</div>' +
          '</div>' +
        '</div>' +
        '<div class="plex-modal-footer">' +
          '<a class="plex-modal-link" href="' + imdbUrl + '" target="_blank" rel="noreferrer">IMDb</a>' +
          '<a class="plex-modal-link" href="' + tmdbUrl + '" target="_blank" rel="noreferrer">TMDb</a>' +
          '<a class="plex-modal-link" href="/apps/tautulli/launch?q=' + query + '&type=' + encodeURIComponent(mediaType) + '" target="_blank" rel="noreferrer">Tautulli</a>' +
          '<a class="plex-modal-link" href="/apps/plex/launch?q=' + query + '&type=' + encodeURIComponent(mediaType) + '" target="_blank" rel="noreferrer">Plex</a>' +
        '</div>';
    }

    function renderCard(it, idx) {
      const typeSvg = it.kind === 'tv' ? tvIcon() : movieIcon();
      const addedPill = formatRecentAddedPill(it.addedAt);

      const metaBits = [];
      if (recentDisplay.showMeta && it.year) metaBits.push(it.year);
      if (recentDisplay.showSubtitle && it.season) metaBits.push('S' + it.season + 'E' + (it.episode || ''));
      const metaLine = metaBits.join(' • ');

      const card = document.createElement('div');
      card.className = 'plex-card';
      card.dataset.index = String(idx);

      card.innerHTML =
        '<div class="plex-poster-wrap">' +
          '<div class="plex-poster-well">' +
            (it.thumb
              ? '<img src="' + it.thumb + '" alt="' + escapeHtml(it.title) + '" loading="lazy" referrerpolicy="no-referrer" />'
              : '<div class="plex-placeholder"><div class="plex-placeholder-big">' + escapeHtml(it.title) + '</div><div class="plex-placeholder-small">No poster provided</div></div>'
            ) +
            (recentDisplay.showPill && addedPill ? '<div class="plex-pill">' + escapeHtml(addedPill) + '</div>' : '') +
            (recentDisplay.showTypeIcon ? '<div class="plex-type-icon" title="' + (it.kind === 'tv' ? 'TV' : 'Movie') + '">' + typeSvg + '</div>' : '') +
            (recentDisplay.showViewIcon ? '<div class="plex-eye-icon" title="View" data-action="view">' + eyeSvg() + '</div>' : '') +
          '</div>' +
        '</div>' +
        '<div class="plex-footer">' +
          '<div class="plex-name">' + escapeHtml(it.title) + '</div>' +
          (metaLine ? '<div class="plex-meta">' + escapeHtml(metaLine) + '</div>' : '') +
        '</div>';

      return card;
    }

    function sectionUrl(type) {
      const key = type === 'show' ? 'show' : 'movie';
      const sectionId = sectionIds[key] || sectionIds.movie || SECTION_IDS_FALLBACK[key] || SECTION_IDS_FALLBACK.movie;
      const size = 100;
      return baseUrl + '/library/sections/' + sectionId + '/recentlyAdded?X-Plex-Token=' + encodeURIComponent(token) + '&X-Plex-Container-Size=' + encodeURIComponent(String(size)) + '&includeGuids=1';
    }

    function withinWindowEpochSeconds(value, windowValue) {
      const seconds = Number(value);
      if (!Number.isFinite(seconds) || seconds <= 0) return true;
      const ageMs = Date.now() - (seconds * 1000);
      if (ageMs < 0) return true;
      if (windowValue === 'today') return ageMs <= 86400000;
      if (windowValue === 'week') return ageMs <= (7 * 86400000);
      if (windowValue === 'month') return ageMs <= (31 * 86400000);
      return true;
    }

    function loadRecent() {
      elTrack.innerHTML = '<div class="plex-empty">Loading…</div>';
      const type = elType ? String(elType.value || 'movie') : 'movie';
      const windowValue = elWindow ? String(elWindow.value || 'month') : 'month';
      resolveLibrarySectionIds(() => {
        fetchXml(sectionUrl(type), function (xmlText) {
          const items = parseRecentlyAdded(xmlText)
            .filter((it) => withinWindowEpochSeconds(it.addedAt, windowValue));
          carousel.setItems(items, renderCard, showDetailsModal);
        }, function (status) {
          elTrack.innerHTML = '<div class="plex-empty">Failed to load (Status ' + status + ')</div>';
        });
      });
    }

    if (elType) elType.addEventListener('change', loadRecent);
    if (elWindow) elWindow.addEventListener('change', loadRecent);

    window.addEventListener('resize', () => {
      carousel.updateLayout();
    });

    loadRecent();
  })();

  // Plex Discover: Most Watchlisted This Week
  (function () {
    const elType = document.getElementById('plexWatchlistedTypeFilter');
    const elPrev = document.getElementById('plexWatchlistedPrevBtn');
    const elNext = document.getElementById('plexWatchlistedNextBtn');
    const elViewport = document.getElementById('plexWatchlistedViewport');
    const elTrack = document.getElementById('plexWatchlistedTrack');

    const modalBackdrop = document.getElementById('plexWatchlistedModalBackdrop');
    const modalClose = document.getElementById('plexWatchlistedModalClose');
    const modalTitle = document.getElementById('plexWatchlistedModalTitle');
    const modalSubtitle = document.getElementById('plexWatchlistedModalSubtitle');
    const modalBody = document.getElementById('plexWatchlistedModalBody');
    const modalTypeIcon = document.getElementById('plexWatchlistedModalTypeIcon');
    const watchlistedDisplay = sectionDisplaySettings('watchlisted');

    if (!elViewport || !elTrack) return;

    const carousel = createCarousel({
      viewport: elViewport,
      track: elTrack,
      prevBtn: elPrev,
      nextBtn: elNext,
      modalBackdrop,
      modalClose,
      modalTitle,
      modalSubtitle,
      modalBody,
      modalTypeIcon,
      emptyTitle: 'No watchlisted titles',
      emptySubtitle: 'Discovery did not return any items'
    });

    let sourceItems = [];
    let activeItem = null;
    let activeDetails = null;
    let activeMessage = '';
    let watchlistBusy = false;
    let detailsRequestId = 0;
    let cardRequestId = 0;
    const watchlistCache = new Map();

    function postJson(url, body, callback, errorCB) {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', url, true);
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.onreadystatechange = function () {
        if (xhr.readyState !== 4) return;
        if (xhr.status >= 200 && xhr.status < 400) {
          try {
            callback(JSON.parse(xhr.responseText || '{}'));
          } catch (err) {
            if (errorCB) errorCB(0);
          }
        } else if (errorCB) {
          errorCB(xhr.status, xhr.responseText || '');
        }
      };
      xhr.send(JSON.stringify(body || {}));
    }

    function renderWatchlistedModal(it) {
      if (!modalBody) return;
      const details = activeDetails || {};
      const overviewText = details.summary || it.subtitle || 'No overview available for this title.';
      const watchLink = '';
      const watchlist = details.watchlist || null;
      const query = encodeURIComponent(String(it.title || '').trim());
      const mediaType = it.kind === 'tv' ? 'tv' : 'movie';
      const imdbUrl = it.imdbId
        ? ('https://www.imdb.com/title/' + encodeURIComponent(String(it.imdbId)) + '/')
        : ('https://www.imdb.com/find/?q=' + query);
      const tmdbUrl = it.tmdbId
        ? ('https://www.themoviedb.org/' + (mediaType === 'tv' ? 'tv/' : 'movie/') + encodeURIComponent(String(it.tmdbId)))
        : ('https://www.themoviedb.org/search?query=' + query);

      const watchlistState = watchlist || {};
      const canWatchlist = Boolean(watchlistState.allowed);
      const nextAction = escapeHtml(watchlistState.nextAction || 'add');
      const isWatchlisted = Boolean(watchlistState.isWatchlisted);
      const watchlistDisabled = watchlistBusy || !canWatchlist;
      const watchlistTitle = watchlistState.label || (isWatchlisted ? 'Remove from Watchlist' : 'Add to Watchlist');

      modalBody.innerHTML =
        '<div class="plex-modal-scroll">' +
          '<div class="plex-modal-hero">' +
            (it.thumb ? '<img class="plex-modal-bg" src="' + it.thumb + '" alt="" referrerpolicy="no-referrer" />' : '') +
            '<div class="plex-modal-content">' +
              '<div class="plex-modal-poster">' +
                (it.thumb
                  ? '<img src="' + it.thumb + '" alt="' + escapeHtml(it.title) + '" referrerpolicy="no-referrer" />'
                  : '<div class="plex-placeholder" style="height:340px"><div class="plex-placeholder-big">' + escapeHtml(it.title) + '</div><div class="plex-placeholder-small">No poster</div></div>'
                ) +
                '<button class="plex-watchlist-btn plex-watchlist-btn--modal" type="button" data-action="watchlist-toggle" data-next-action="' + nextAction + '"' + (watchlistDisabled ? ' disabled' : '') + ' title="' + escapeHtml(watchlistTitle) + '">' +
                  watchlistIcon(isWatchlisted) +
                '</button>' +
              '</div>' +
              '<div class="plex-modal-meta">' +
                '<div class="plex-pills">' +
                  '<span class="plex-pill2">' + (it.kind === 'tv' ? 'TV Show' : 'Movie') + '</span>' +
                  (it.year ? '<span class="plex-pill2">' + escapeHtml(it.year) + '</span>' : '') +
                  (details.studio ? '<span class="plex-pill2">' + escapeHtml(details.studio) + '</span>' : '') +
                  (details.contentRating ? '<span class="plex-pill2">' + escapeHtml(details.contentRating) + '</span>' : '') +
                  watchLink +
                '</div>' +
                (activeMessage ? '<div class="plex-overview-text" style="margin-bottom:10px">' + escapeHtml(activeMessage) + '</div>' : '') +
                '<div class="plex-section">' +
                  '<h4>Overview</h4>' +
                  '<div class="plex-overview-text">' + escapeHtml(overviewText) + '</div>' +
                '</div>' +
              '</div>' +
            '</div>' +
          '</div>' +
        '</div>' +
        '<div class="plex-modal-footer">' +
          '<a class="plex-modal-link" href="' + imdbUrl + '" target="_blank" rel="noreferrer">IMDb</a>' +
          '<a class="plex-modal-link" href="' + tmdbUrl + '" target="_blank" rel="noreferrer">TMDb</a>' +
          '<a class="plex-modal-link" href="/apps/tautulli/launch?q=' + query + '&type=' + encodeURIComponent(mediaType) + '" target="_blank" rel="noreferrer">Tautulli</a>' +
          '<a class="plex-modal-link" href="/apps/plex/launch?q=' + query + '&type=' + encodeURIComponent(mediaType) + '" target="_blank" rel="noreferrer">Plex</a>' +
        '</div>';
    }

    function loadDetails(it) {
      if (!it || !it.slug) return;
      const currentRequestId = ++detailsRequestId;
      const query = new URLSearchParams();
      query.set('kind', it.kind === 'tv' ? 'tv' : 'movie');
      query.set('slug', String(it.slug || ''));
      if (it.ratingKey) query.set('ratingKey', String(it.ratingKey));

      fetchJson('/api/plex/discovery/details?' + query.toString(), function (payload) {
        if (currentRequestId !== detailsRequestId) return;
        if (!activeItem || activeItem.link !== it.link) return;
        activeDetails = payload || {};
        if (it.slug) {
          watchlistCache.set(String(it.kind) + ':' + String(it.slug), {
            watchlist: activeDetails.watchlist || null,
          });
        }
        if (payload && payload.ratingKey && !activeItem.ratingKey) activeItem.ratingKey = payload.ratingKey;
        if (payload && payload.imdbId && !activeItem.imdbId) activeItem.imdbId = String(payload.imdbId);
        if (payload && payload.tmdbId && !activeItem.tmdbId) activeItem.tmdbId = String(payload.tmdbId);
        watchlistBusy = false;
        activeMessage = '';
        renderWatchlistedModal(it);
      }, function () {
        if (currentRequestId !== detailsRequestId) return;
        if (!activeItem || activeItem.link !== it.link) return;
        activeMessage = 'Could not load full overview details.';
        renderWatchlistedModal(it);
      });
    }

    function watchlistIcon(isOn) {
      return isOn
        ? '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M6 3h12a1 1 0 0 1 1 1v17.5a1 1 0 0 1-1.5.86L12 19.5l-5.5 2.86A1 1 0 0 1 5 21.5V4a1 1 0 0 1 1-1z"/></svg>'
        : '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M7 3h10a2 2 0 0 1 2 2v16.4a1 1 0 0 1-1.46.88L12 19.3l-5.54 2.98A1 1 0 0 1 5 21.4V5a2 2 0 0 1 2-2zm0 2v14.8l4.54-2.45a1 1 0 0 1 .92 0L17 19.8V5H7z"/></svg>';
    }

    function updateWatchlistButton(button, watchlist) {
      if (!button) return;
      const state = watchlist || {};
      const allowed = Boolean(state.allowed);
      const isOn = Boolean(state.isWatchlisted);
      button.innerHTML = watchlistIcon(isOn);
      button.setAttribute('data-next-action', String(state.nextAction || (isOn ? 'remove' : 'add')));
      button.setAttribute('title', state.label || (isOn ? 'Remove from Watchlist' : 'Add to Watchlist'));
      button.disabled = watchlistBusy || !allowed;
    }

    function hydrateWatchlistState(items) {
      const current = ++cardRequestId;
      items.forEach((it, idx) => {
        if (!it || !it.slug || (it.kind !== 'tv' && it.kind !== 'movie')) return;
        const cacheKey = String(it.kind) + ':' + String(it.slug);
        const cached = watchlistCache.get(cacheKey);
        const card = elTrack.querySelector('.plex-card[data-index="' + String(idx) + '"]');
        const watchBtn = card ? card.querySelector('.plex-watchlist-btn') : null;
        if (cached) {
          if (watchBtn) updateWatchlistButton(watchBtn, cached.watchlist);
          return;
        }
        const query = new URLSearchParams();
        query.set('kind', it.kind === 'tv' ? 'tv' : 'movie');
        query.set('slug', String(it.slug || ''));
        if (it.ratingKey) query.set('ratingKey', String(it.ratingKey));
        fetchJson('/api/plex/discovery/details?' + query.toString(), function (payload) {
          if (current !== cardRequestId) return;
          const next = payload || {};
          watchlistCache.set(cacheKey, {
            watchlist: next.watchlist || null,
          });
          const targetCard = elTrack.querySelector('.plex-card[data-index="' + String(idx) + '"]');
          if (!targetCard) return;
          const btn = targetCard.querySelector('.plex-watchlist-btn');
          if (btn) updateWatchlistButton(btn, next.watchlist);
        });
      });
    }

    function showDetailsModal(it) {
      if (!modalBackdrop) return;
      activeItem = it;
      activeDetails = null;
      activeMessage = '';
      watchlistBusy = false;
      modalTypeIcon.innerHTML = it.kind === 'tv' ? tvIcon() : movieIcon();
      modalTitle.textContent = it.title;
      const subtitleBits = [];
      if (watchlistedDisplay.showMeta && it.year) subtitleBits.push(it.year);
      if (watchlistedDisplay.showSubtitle) subtitleBits.push('Most Watchlisted This Week');
      modalSubtitle.textContent = subtitleBits.join(' • ');
      carousel.openModal();
      renderWatchlistedModal(it);
      loadDetails(it);
    }

    if (modalBody) {
      modalBody.addEventListener('click', function (ev) {
        const btn = ev.target && ev.target.closest ? ev.target.closest('[data-action="watchlist-toggle"]') : null;
        if (!btn || !activeItem || watchlistBusy) return;
        const nextAction = String(btn.getAttribute('data-next-action') || 'add').toLowerCase();
        if (nextAction !== 'add' && nextAction !== 'remove') return;
        if (!activeItem.slug) return;

        watchlistBusy = true;
        activeMessage = '';
        renderWatchlistedModal(activeItem);

        postJson('/api/plex/discovery/watchlist', {
          kind: activeItem.kind === 'tv' ? 'tv' : 'movie',
          slug: activeItem.slug,
          action: nextAction
        }, function (payload) {
          watchlistBusy = false;
          if (payload && payload.watchlist) {
            activeDetails = { ...(activeDetails || {}), watchlist: payload.watchlist };
            activeMessage = 'Watchlist updated.';
            const cacheKey = String(activeItem.kind) + ':' + String(activeItem.slug);
            const cached = watchlistCache.get(cacheKey) || {};
            watchlistCache.set(cacheKey, { ...cached, watchlist: payload.watchlist });
          } else {
            activeMessage = 'Watchlist updated.';
          }
          renderWatchlistedModal(activeItem);
        }, function (_status, responseText) {
          watchlistBusy = false;
          let msg = 'Could not update watchlist.';
          try {
            const parsed = JSON.parse(responseText || '{}');
            if (parsed && parsed.error) msg = parsed.error;
          } catch (_err) {}
          activeMessage = msg;
          renderWatchlistedModal(activeItem);
        });
      });
    }

    function resolveWatchlistedPill(item) {
      const direct = String(item?.watchlistedCountLabel || '').trim();
      if (direct) return direct;
      const subtitle = String(item?.subtitle || '').trim();
      if (!subtitle) return '';
      const patterns = [
        /(\d[\d.,]*\s*[kmb]?)\s+(?:people\s+)?watchlist(?:ed|s)?/i,
        /watchlist(?:ed|s)?\s+by\s+(\d[\d.,]*\s*[kmb]?)/i,
      ];
      for (let index = 0; index < patterns.length; index += 1) {
        const match = subtitle.match(patterns[index]);
        if (!match) continue;
        const count = String(match[1] || '').replace(/\s+/g, '').trim();
        if (count) return count + ' watchlists';
      }
      return '';
    }

    function renderCard(it, idx) {
      const typeSvg = it.kind === 'tv' ? tvIcon() : movieIcon();
      const rankPill = '#' + String(Number(idx) + 1);
      const metaBits = [];
      if (watchlistedDisplay.showMeta && it.year) metaBits.push(it.year);
      if (watchlistedDisplay.showSubtitle && it.subtitle) metaBits.push(it.subtitle);
      const metaLine = metaBits.join(' • ');

      const card = document.createElement('div');
      card.className = 'plex-card plex-card--watchlisted';
      card.dataset.index = String(idx);

      card.innerHTML =
        '<div class="plex-poster-wrap">' +
          '<div class="plex-poster-well">' +
            (it.thumb
              ? '<img src="' + it.thumb + '" alt="' + escapeHtml(it.title) + '" loading="lazy" referrerpolicy="no-referrer" />'
              : '<div class="plex-placeholder"><div class="plex-placeholder-big">' + escapeHtml(it.title) + '</div><div class="plex-placeholder-small">No poster provided</div></div>'
            ) +
            '<div class="plex-pill">' + escapeHtml(rankPill) + '</div>' +
            (watchlistedDisplay.showTypeIcon ? '<div class="plex-type-icon" title="' + (it.kind === 'tv' ? 'TV' : 'Movie') + '">' + typeSvg + '</div>' : '') +
            (watchlistedDisplay.showViewIcon ? '<div class="plex-eye-icon" title="View" data-action="view">' + eyeSvg() + '</div>' : '') +
          '</div>' +
        '</div>' +
        '<div class="plex-footer">' +
          '<div class="plex-name">' + escapeHtml(it.title) + '</div>' +
          (metaLine ? '<div class="plex-meta">' + escapeHtml(metaLine) + '</div>' : '') +
        '</div>';

      return card;
    }

    function filteredItems() {
      const type = elType ? elType.value : 'all';
      return sourceItems
        .filter((it) => {
          if (type === 'movie') return it.kind === 'movie';
          if (type === 'show') return it.kind === 'tv';
          return true;
        });
    }

    function applyFilters() {
      const items = filteredItems();
      carousel.setItems(items, renderCard, showDetailsModal);
      hydrateWatchlistState(items);
    }

    function loadWatchlisted() {
      elTrack.innerHTML = '<div class="plex-empty">Loading…</div>';
      fetchJson('/api/plex/discovery/watchlisted', function (payload) {
        sourceItems = Array.isArray(payload.items) ? payload.items : [];
        applyFilters();
      }, function (status) {
        elTrack.innerHTML = '<div class="plex-empty">Failed to load (Status ' + status + ')</div>';
      });
    }

    if (elType) elType.addEventListener('change', applyFilters);
    // watchlist toggle handled inside the modal only
    window.addEventListener('resize', () => {
      carousel.updateLayout();
    });

    loadWatchlisted();
  })();

  document.querySelectorAll('.plex-collapse-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      const targetId = btn.getAttribute('data-target');
      const section = targetId ? document.getElementById(targetId) : null;
      if (!section) return;
      section.classList.toggle('plex-collapsed');
    });
  });
})();
