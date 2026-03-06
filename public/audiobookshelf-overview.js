(() => {
  'use strict';

  const carouselFreeScroll = (() => {
    try {
      return localStorage.getItem('launcharr-carousel-free-scroll') === '1';
    } catch (_err) {
      return false;
    }
  })();

  const config = window.ABS_OVERVIEW_CONFIG || {};
  const appId = String(config.appId || 'audiobookshelf').trim() || 'audiobookshelf';
  const appName = String(config.appName || 'Audiobookshelf').trim() || 'Audiobookshelf';

  const viewport = document.getElementById('absRecentViewport');
  const track = document.getElementById('absRecentTrack');
  const prevBtn = document.getElementById('absRecentPrevBtn');
  const nextBtn = document.getElementById('absRecentNextBtn');
  const typeFilter = document.getElementById('absRecentTypeFilter');
  const limitSelect = document.getElementById('absRecentLimitSelect');

  if (!viewport || !track) return;

  const CACHE_TTL_MS = 5 * 60 * 1000;
  let allItems = [];
  let cacheTs = 0;
  let carousel = null;

  function escapeHtml(value) {
    return String(value || '').replace(/[&<>"']/g, (char) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[char]
    ));
  }

  function timeAgo(value) {
    if (!value) return '';
    const ts = Number(value);
    if (!Number.isFinite(ts) || ts <= 0) return '';
    const seconds = Math.max(0, (Date.now() - ts) / 1000);
    if (seconds < 60) return 'just now';
    if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
    if (seconds < 86400) return Math.floor(seconds / 3600) + 'h ago';
    if (seconds < 86400 * 30) return Math.floor(seconds / 86400) + 'd ago';
    const d = new Date(ts);
    return (d.getMonth() + 1) + '/' + d.getDate() + '/' + d.getFullYear();
  }

  function cssNum(name, fallback) {
    const raw = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function bookIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"></path>' +
      '<path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"></path></svg>';
  }

  function podcastIcon() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="#ffffff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<circle cx="12" cy="11" r="1"></circle>' +
      '<path d="M11 17a1 1 0 0 1 2 0c0 .5-.34 3-.5 4.5a.5.5 0 0 1-1 0C11.34 20 11 17.5 11 17z"></path>' +
      '<path d="M6.7 15.3a6 6 0 1 1 10.6 0"></path>' +
      '<path d="M9.5 12.4a3 3 0 1 1 5 0"></path></svg>';
  }

  function renderCard(item) {
    const card = document.createElement('div');
    card.className = 'plex-card';
    const title = escapeHtml(item.title || 'Unknown');
    const author = escapeHtml(item.author || '');
    const meta = escapeHtml(timeAgo(item.addedAt));
    const isPodcast = item.mediaType === 'podcast';
    const typeIcon = isPodcast ? podcastIcon() : bookIcon();
    const typeLabel = isPodcast ? 'Podcast' : 'Book';
    const metaLine = [author, meta].filter(Boolean).join(' | ');

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
        (metaLine ? '<div class="plex-meta">' + metaLine + '</div>' : '') +
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
        return;
      }
      viewport.style.overflowX = 'auto';
      viewport.style.overflowY = 'hidden';
      viewport.style.scrollBehavior = 'smooth';
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
      cards.forEach((item) => {
        track.appendChild(renderCard(item));
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

    let startX = 0, deltaX = 0, tracking = false;
    const swipeThreshold = 42;
    const isInteractive = (target) => !!(target && target.closest && target.closest('button, input, select, a'));

    viewport.addEventListener('pointerdown', (event) => {
      if (carouselFreeScroll || isInteractive(event.target)) { tracking = false; return; }
      tracking = true;
      startX = event.clientX;
      deltaX = 0;
      if (event.pointerType === 'mouse' && viewport.setPointerCapture) viewport.setPointerCapture(event.pointerId);
    });
    viewport.addEventListener('pointermove', (event) => {
      if (!tracking) return;
      deltaX = event.clientX - startX;
    });
    const onPointerEnd = () => {
      if (!tracking) return;
      if (Math.abs(deltaX) > swipeThreshold) {
        if (deltaX > 0) slidePrev();
        else slideNext();
      }
      tracking = false;
    };
    viewport.addEventListener('pointerup', onPointerEnd);
    viewport.addEventListener('pointercancel', onPointerEnd);

    prevBtn?.addEventListener('click', slidePrev);
    nextBtn?.addEventListener('click', slideNext);
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
      filtered = filtered.filter((item) => item.mediaType === typeValue);
    }
    carousel.setItems(filtered.slice(0, limit));
  }

  async function loadRecent() {
    const now = Date.now();
    if (allItems.length && (now - cacheTs) < CACHE_TTL_MS) {
      applyFilters();
      return;
    }

    const limit = Math.max(1, Number(limitSelect?.value) || 20);
    const fetchLimit = Math.min(100, Math.max(limit, 50));
    track.innerHTML = '<div class="plex-empty">Loading\u2026</div>';

    try {
      const response = await fetch(`/api/audiobookshelf/recent?limit=${fetchLimit}`, {
        headers: { Accept: 'application/json' },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        track.innerHTML = '<div class="plex-empty">Unable to load ' + escapeHtml(appName) + ' data.</div>';
        return;
      }
      allItems = Array.isArray(payload?.items) ? payload.items : [];
      cacheTs = Date.now();
      applyFilters();
    } catch (_err) {
      track.innerHTML = '<div class="plex-empty">Unable to load ' + escapeHtml(appName) + ' data.</div>';
    }
  }

  typeFilter?.addEventListener('change', applyFilters);
  limitSelect?.addEventListener('change', () => { allItems = []; cacheTs = 0; loadRecent(); });

  window.addEventListener('resize', () => { carousel.updateLayout(); });

  document.querySelectorAll('.plex-collapse-btn[data-collapse-global="true"]').forEach((btn) => {
    btn.addEventListener('click', () => {
      setTimeout(() => carousel.updateLayout(), 310);
    });
  });

  loadRecent();
})();
