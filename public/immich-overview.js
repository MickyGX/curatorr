(() => {
  'use strict';

  const carouselFreeScroll = (() => {
    try {
      return localStorage.getItem('launcharr-carousel-free-scroll') === '1';
    } catch (_err) {
      return false;
    }
  })();

  const config = window.IMMICH_OVERVIEW_CONFIG || {};
  const appId = String(config.appId || 'immich').trim() || 'immich';
  const appName = String(config.appName || 'Immich').trim() || 'Immich';

  const viewport = document.getElementById('immichRecentViewport');
  const track = document.getElementById('immichRecentTrack');
  const prevBtn = document.getElementById('immichRecentPrevBtn');
  const nextBtn = document.getElementById('immichRecentNextBtn');
  const typeFilter = document.getElementById('immichRecentTypeFilter');
  const limitSelect = document.getElementById('immichRecentLimitSelect');

  if (!viewport || !track) return;

  const CACHE_TTL_MS = 3 * 60 * 1000;
  let allItems = [];
  let carousel = null;

  function escapeHtml(value) {
    return String(value || '').replace(/[&<>"']/g, (char) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[char]
    ));
  }

  function timeAgo(value) {
    if (!value) return '';
    const ts = new Date(value).getTime();
    if (!Number.isFinite(ts)) return '';
    const seconds = Math.max(0, (Date.now() - ts) / 1000);
    if (seconds < 60) return 'just now';
    if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
    if (seconds < 86400) return Math.floor(seconds / 3600) + 'h ago';
    if (seconds < 86400 * 30) return Math.floor(seconds / 86400) + 'd ago';
    const d = new Date(ts);
    return (d.getMonth() + 1) + '/' + d.getDate() + '/' + d.getFullYear();
  }

  function cacheKey(size) {
    return `launcharr:${appId}:immich-recent:v1:size:${size}`;
  }

  function cacheRead(key) {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed || !Array.isArray(parsed.items) || typeof parsed.ts !== 'number') return null;
      if ((Date.now() - parsed.ts) > CACHE_TTL_MS) return null;
      return parsed.items;
    } catch (_err) {
      return null;
    }
  }

  function cacheWrite(key, items) {
    try {
      localStorage.setItem(key, JSON.stringify({ ts: Date.now(), items }));
    } catch (_err) {
      // ignore
    }
  }

  function cssNum(name, fallback) {
    const raw = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function cameraIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"></path>' +
      '<circle cx="12" cy="13" r="3"></circle></svg>';
  }

  function videoIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<polygon points="23 7 16 12 23 17 23 7"></polygon>' +
      '<rect x="1" y="5" width="15" height="14" rx="2" ry="2"></rect></svg>';
  }

  function renderCard(item) {
    const card = document.createElement('div');
    card.className = 'plex-card';
    const title = escapeHtml(item.title || 'Untitled');
    const meta = escapeHtml(timeAgo(item.date));
    const isVideo = item.type === 'video';
    const typeIcon = isVideo ? videoIcon() : cameraIcon();
    const typeLabel = isVideo ? 'Video' : 'Photo';

    card.innerHTML =
      '<div class="plex-poster-wrap">' +
        '<div class="plex-poster-well">' +
          (item.thumbUrl
            ? '<img src="' + escapeHtml(item.thumbUrl) + '" alt="' + title + '" loading="lazy" />'
            : '<div class="plex-placeholder"><div class="plex-placeholder-big">' + title + '</div></div>') +
          '<div class="plex-type-icon" title="' + typeLabel + '">' + typeIcon + '</div>' +
        '</div>' +
      '</div>' +
      '<div class="plex-footer">' +
        '<div class="plex-name">' + title + '</div>' +
        (meta ? '<div class="plex-meta">' + meta + '</div>' : '') +
      '</div>';

    return card;
  }

  function createCarousel() {
    let cards = [];
    let slideIndex = 0;
    let visibleCount = 1;
    let cardWidth = 203;
    let gap = 24;

    function applyFreeScrollViewportStyle() {
      if (!carouselFreeScroll) {
        viewport.style.overflowX = '';
        viewport.style.overflowY = '';
        viewport.style.scrollBehavior = '';
        viewport.style.webkitOverflowScrolling = '';
        viewport.style.touchAction = 'pan-y';
        return;
      }
      viewport.style.overflowX = 'auto';
      viewport.style.overflowY = 'hidden';
      viewport.style.scrollBehavior = 'smooth';
      viewport.style.webkitOverflowScrolling = 'touch';
      viewport.style.touchAction = 'pan-x pan-y';
    }

    function computeLayout() {
      const viewportWidth = viewport.clientWidth;
      const firstCard = track.querySelector('.plex-card');
      const measured = firstCard ? Math.round(firstCard.getBoundingClientRect().width) : 0;
      cardWidth = measured > 0 ? measured : cssNum('--plex-cardW', 203);
      gap = cssNum('--plex-gap', 24);
      if (viewportWidth <= 0) return;
      visibleCount = Math.max(1, Math.floor((viewportWidth + gap) / (cardWidth + gap)));
    }

    function clampIndex() {
      const maxLeft = Math.max(0, cards.length - visibleCount);
      slideIndex = Math.min(Math.max(0, slideIndex), maxLeft);
    }

    function applyTransform(animated) {
      if (carouselFreeScroll) {
        track.style.transition = 'none';
        track.style.transform = 'none';
        return;
      }
      track.style.transition = animated ? 'transform .25s ease' : 'none';
      track.style.transform = 'translateX(' + (-slideIndex * (cardWidth + gap)) + 'px)';
    }

    function updateButtons() {
      if (!prevBtn || !nextBtn) return;
      if (carouselFreeScroll) {
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
        if (carouselFreeScroll) viewport.scrollLeft = 0;
        updateButtons();
        return;
      }
      cards.forEach((item, index) => {
        const card = renderCard(item);
        card.dataset.index = String(index);
        track.appendChild(card);
      });
      computeLayout();
      clampIndex();
      applyTransform(false);
      if (carouselFreeScroll) viewport.scrollLeft = 0;
      updateButtons();
    }

    function slidePrev() {
      if (carouselFreeScroll) {
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
      if (carouselFreeScroll) {
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
      if (carouselFreeScroll) return;
      let startX = 0;
      let startY = 0;
      let movedX = 0;
      let movedY = 0;
      let tracking = false;
      const swipeThreshold = 42;
      const isInteractive = (target) => !!(target && target.closest && target.closest('button, input, select, textarea, a, [data-action="view"]'));

      const onStart = (x, y, target) => {
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

      const onMove = (x, y) => {
        if (!tracking) return;
        movedX = x - startX;
        movedY = y - startY;
      };

      const onEnd = () => {
        if (!tracking) return;
        if (Math.abs(movedX) > swipeThreshold && Math.abs(movedX) > Math.abs(movedY) * 1.2) {
          if (movedX > 0) slidePrev();
          else slideNext();
        }
        tracking = false;
      };

      viewport.addEventListener('pointerdown', (event) => {
        onStart(event.clientX, event.clientY, event.target);
        if (tracking && event.pointerType === 'mouse' && viewport.setPointerCapture) viewport.setPointerCapture(event.pointerId);
      });
      viewport.addEventListener('pointermove', (event) => {
        onMove(event.clientX, event.clientY);
      });
      viewport.addEventListener('pointerup', onEnd);
      viewport.addEventListener('pointercancel', () => { tracking = false; });

      viewport.addEventListener('touchstart', (event) => {
        const touch = event.touches && event.touches[0];
        if (!touch) return;
        onStart(touch.clientX, touch.clientY, event.target);
      }, { passive: true });
      viewport.addEventListener('touchmove', (event) => {
        const touch = event.touches && event.touches[0];
        if (!touch) return;
        onMove(touch.clientX, touch.clientY);
      }, { passive: true });
      viewport.addEventListener('touchend', onEnd);
      viewport.addEventListener('touchcancel', () => { tracking = false; });
    }

    prevBtn?.addEventListener('click', slidePrev);
    nextBtn?.addEventListener('click', slideNext);
    addSwipe();
    if (carouselFreeScroll) {
      viewport.addEventListener('scroll', updateButtons, { passive: true });
    }

    return {
      setItems(nextCards) {
        cards = Array.isArray(nextCards) ? nextCards : [];
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

  carousel = createCarousel();

  function applyFilters() {
    const typeValue = String(typeFilter?.value || 'all').toLowerCase();
    const limit = Math.max(1, Number(limitSelect?.value) || 20);
    let filtered = allItems;
    if (typeValue !== 'all') {
      filtered = filtered.filter((item) => item.type === typeValue);
    }
    carousel.setItems(filtered.slice(0, limit));
  }

  async function loadRecent() {
    const limit = Math.max(1, Number(limitSelect?.value) || 20);
    const fetchSize = Math.min(100, Math.max(limit, 50));
    const key = cacheKey(fetchSize);
    const cached = cacheRead(key);
    if (cached) {
      allItems = cached;
      applyFilters();
      return;
    }

    track.innerHTML = '<div class="plex-empty">Loading\u2026</div>';

    try {
      const response = await fetch(`/api/immich/recent?size=${fetchSize}`, {
        headers: { Accept: 'application/json' },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        track.innerHTML = '<div class="plex-empty">Unable to load ' + escapeHtml(appName) + ' data.</div>';
        return;
      }
      allItems = Array.isArray(payload?.items) ? payload.items : [];
      cacheWrite(key, allItems);
      applyFilters();
    } catch (_err) {
      track.innerHTML = '<div class="plex-empty">Unable to load ' + escapeHtml(appName) + ' data.</div>';
    }
  }

  typeFilter?.addEventListener('change', applyFilters);
  limitSelect?.addEventListener('change', () => { allItems = []; loadRecent(); });

  window.addEventListener('resize', () => { carousel.updateLayout(); });

  function bindCollapseButtons() {
    document.querySelectorAll('.plex-collapse-btn[data-collapse-global="true"]').forEach((btn) => {
      btn.addEventListener('click', () => {
        setTimeout(() => carousel.updateLayout(), 310);
      });
    });
  }
  bindCollapseButtons();

  loadRecent();
})();
