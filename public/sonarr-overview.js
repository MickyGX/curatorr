(() => {
  const config = window.SONARR_OVERVIEW_CONFIG || {};
  const apiKey = String(config.apiKey || '').trim();
  const baseUrl = normalizeBaseUrl(String(config.baseUrl || '').trim());
  const root = document.querySelector('.plex-overview') || document.documentElement;

  const modules = {
    soon: {
      viewport: document.getElementById('sonarrSoonViewport'),
      track: document.getElementById('sonarrSoonTrack'),
      prevBtn: document.getElementById('sonarrSoonPrevBtn'),
      nextBtn: document.getElementById('sonarrSoonNextBtn'),
      windowFilter: document.getElementById('sonarrSoonWindowFilter'),
      limitFilter: document.getElementById('sonarrSoonLimitSelect'),
      items: [],
      carousel: null,
    },
    recent: {
      viewport: document.getElementById('sonarrRecentViewport'),
      track: document.getElementById('sonarrRecentTrack'),
      prevBtn: document.getElementById('sonarrRecentPrevBtn'),
      nextBtn: document.getElementById('sonarrRecentNextBtn'),
      typeFilter: document.getElementById('sonarrRecentTypeFilter'),
      limitFilter: document.getElementById('sonarrRecentLimitSelect'),
      items: [],
      carousel: null,
    },
  };

  const hasSoon = Boolean(modules.soon.viewport && modules.soon.track);
  const hasRecent = Boolean(modules.recent.viewport && modules.recent.track);
  if (!hasSoon && !hasRecent) return;

  if (!baseUrl || !apiKey) {
    const message = '<div class="plex-empty">Add Sonarr URL and API key in settings.</div>';
    if (hasSoon) modules.soon.track.innerHTML = message;
    if (hasRecent) modules.recent.track.innerHTML = message;
    bindCollapseButtons();
    return;
  }

  if (hasSoon) {
    modules.soon.carousel = createCarousel({
      viewport: modules.soon.viewport,
      track: modules.soon.track,
      prevBtn: modules.soon.prevBtn,
      nextBtn: modules.soon.nextBtn,
    });
    modules.soon.windowFilter?.addEventListener('change', applySoonFilters);
    modules.soon.limitFilter?.addEventListener('change', applySoonFilters);
    loadSoon();
  }

  if (hasRecent) {
    modules.recent.carousel = createCarousel({
      viewport: modules.recent.viewport,
      track: modules.recent.track,
      prevBtn: modules.recent.prevBtn,
      nextBtn: modules.recent.nextBtn,
    });
    modules.recent.typeFilter?.addEventListener('change', applyRecentFilters);
    modules.recent.limitFilter?.addEventListener('change', applyRecentFilters);
    loadRecent();
  }

  window.addEventListener('resize', () => {
    if (modules.soon.carousel) modules.soon.carousel.updateLayout();
    if (modules.recent.carousel) modules.recent.carousel.updateLayout();
  });

  bindCollapseButtons();

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

  function episodeCode(season, episode) {
    const seasonNumber = Number(season);
    const episodeNumber = Number(episode);
    if (!Number.isFinite(seasonNumber) || !Number.isFinite(episodeNumber)) return '';
    return 'S' + pad2(seasonNumber) + 'E' + pad2(episodeNumber);
  }

  function logApi(level, message, meta) {
    const logger = console[level] || console.log;
    if (meta && typeof meta === 'object') {
      logger(`[Launcharr] ${message}`, meta);
    } else {
      logger(`[Launcharr] ${message}`);
    }
  }

  function buildPosterUrl(series) {
    const images = Array.isArray(series?.images) ? series.images : [];
    const preferred = images.find((image) => String(image?.coverType || '').toLowerCase() === 'poster') || images[0];
    if (!preferred) return '';
    if (preferred.remoteUrl) return String(preferred.remoteUrl);
    const relative = String(preferred.url || '').trim();
    if (!relative) return '';
    if (/^https?:\/\//i.test(relative)) return relative;

    try {
      const resolved = new URL(relative, baseUrl);
      resolved.searchParams.set('apikey', apiKey);
      return resolved.toString();
    } catch (err) {
      return '';
    }
  }

  async function fetchSonarr(path, params) {
    const url = new URL(path, baseUrl);
    Object.entries(params || {}).forEach(([key, value]) => {
      if (value === undefined || value === null || value === '') return;
      url.searchParams.set(key, String(value));
    });

    logApi('info', 'Sonarr request', { url: url.toString() });
    const response = await fetch(url.toString(), {
      headers: {
        Accept: 'application/json',
        'X-Api-Key': apiKey,
      },
    });

    if (!response.ok) {
      logApi('error', 'Sonarr response failed', { status: response.status });
      throw new Error('Sonarr request failed with status ' + response.status);
    }

    logApi('info', 'Sonarr response ok', { status: response.status });
    return response.json();
  }

  function createCarousel(options) {
    const viewport = options.viewport;
    const track = options.track;
    const prevBtn = options.prevBtn;
    const nextBtn = options.nextBtn;
    let cards = [];
    let slideIndex = 0;
    let visibleCount = 1;
    let cardWidth = 203;
    let gap = 24;

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
      track.style.transition = animated ? 'transform .25s ease' : 'none';
      const offset = slideIndex * (cardWidth + gap);
      track.style.transform = 'translateX(' + (-offset) + 'px)';
    }

    function updateButtons() {
      if (!prevBtn || !nextBtn) return;
      const maxLeft = Math.max(0, cards.length - visibleCount);
      prevBtn.disabled = slideIndex <= 0;
      nextBtn.disabled = slideIndex >= maxLeft;
    }

    function render() {
      track.innerHTML = '';
      if (!cards.length) {
        track.innerHTML = '<div class="plex-empty">No results found.</div>';
        updateButtons();
        return;
      }

      cards.forEach((item) => {
        track.appendChild(renderCard(item));
      });

      computeLayout();
      clampIndex();
      applyTransform(false);
      updateButtons();
    }

    function slidePrev() {
      computeLayout();
      slideIndex = Math.max(0, slideIndex - visibleCount);
      applyTransform(true);
      updateButtons();
    }

    function slideNext() {
      computeLayout();
      const maxLeft = Math.max(0, cards.length - visibleCount);
      slideIndex = Math.min(maxLeft, slideIndex + visibleCount);
      applyTransform(true);
      updateButtons();
    }

    function addSwipe() {
      viewport.style.touchAction = 'pan-y';
      let startX = 0;
      let deltaX = 0;
      let tracking = false;
      const threshold = 42;

      const onStart = (x, target) => {
        if (target && target.closest && target.closest('button, input, select, textarea, a')) {
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

    return {
      setItems(nextItems) {
        cards = Array.isArray(nextItems) ? nextItems : [];
        slideIndex = 0;
        render();
      },
      updateLayout() {
        computeLayout();
        clampIndex();
        applyTransform(false);
        updateButtons();
      },
    };
  }

  function renderCard(item) {
    const card = document.createElement('div');
    card.className = 'plex-card';
    const title = escapeHtml(item.title || 'Unknown');
    const subtitle = escapeHtml(item.subtitle || '');
    const meta = escapeHtml(item.meta || '');
    const pill = escapeHtml(item.pill || '');
    const metaLine = [subtitle, meta].filter(Boolean).join(' | ');

    card.innerHTML =
      '<div class="plex-poster-wrap">' +
        '<div class="plex-poster-well">' +
          (item.thumb
            ? '<img src="' + item.thumb + '" alt="' + title + '" loading="lazy" referrerpolicy="no-referrer" />'
            : '<div class="plex-placeholder"><div class="plex-placeholder-big">' + title + '</div><div class="plex-placeholder-small">No poster</div></div>') +
          (pill ? '<div class="plex-pill">' + pill + '</div>' : '') +
        '</div>' +
      '</div>' +
      '<div class="plex-footer">' +
        '<div class="plex-name">' + title + '</div>' +
        '<div class="plex-meta">' + (metaLine || 'No extra details') + '</div>' +
      '</div>';

    return card;
  }

  function normalizeSoonWindow(item) {
    const now = new Date();
    now.setHours(0, 0, 0, 0);
    const msPerDay = 24 * 60 * 60 * 1000;
    const deltaMs = item.airTimestamp - now.getTime();
    const dayDiff = Math.floor(deltaMs / msPerDay);
    if (!Number.isFinite(dayDiff)) return 'all';
    if (dayDiff <= 0) return 'today';
    if (dayDiff <= 7) return 'week';
    if (dayDiff <= 14) return 'fortnight';
    return 'all';
  }

  function mapSoonItem(entry) {
    const series = entry?.series || {};
    const airDate = entry?.airDateUtc || entry?.airDate || '';
    const airTimestamp = new Date(airDate).getTime();
    const code = episodeCode(entry?.seasonNumber, entry?.episodeNumber);
    const subtitleParts = [];
    if (code) subtitleParts.push(code);
    if (entry?.title) subtitleParts.push(String(entry.title));

    return {
      id: 'soon-' + String(entry?.id || Math.random()),
      title: String(series?.title || entry?.seriesTitle || 'Unknown Series'),
      subtitle: subtitleParts.join(' - '),
      meta: formatDateLabel(airDate),
      thumb: buildPosterUrl(series),
      pill: 'Soon',
      airTimestamp: Number.isFinite(airTimestamp) ? airTimestamp : Number.MAX_SAFE_INTEGER,
      window: 'all',
    };
  }

  function eventType(record) {
    const value = String(record?.eventType || '').toLowerCase();
    if (value.includes('import')) return 'imported';
    if (value.includes('grab')) return 'grabbed';
    if (value.includes('fail')) return 'failed';
    return 'other';
  }

  function mapRecentItem(record) {
    const series = record?.series || {};
    const episode = record?.episode || {};
    const type = eventType(record);
    const code = episodeCode(episode?.seasonNumber, episode?.episodeNumber);
    const subtitleParts = [];
    if (code) subtitleParts.push(code);
    if (episode?.title) subtitleParts.push(String(episode.title));

    const date = record?.date || '';
    const timestamp = new Date(date).getTime();
    const pill = type === 'failed'
      ? 'Failed'
      : (type === 'grabbed' ? 'Grabbed' : 'Imported');

    return {
      id: 'recent-' + String(record?.id || Math.random()),
      title: String(series?.title || record?.sourceTitle || 'Unknown Series'),
      subtitle: subtitleParts.join(' - '),
      meta: formatDateLabel(date),
      thumb: buildPosterUrl(series),
      pill,
      eventType: type,
      timestamp: Number.isFinite(timestamp) ? timestamp : 0,
    };
  }

  function applySoonFilters() {
    if (!modules.soon.carousel) return;
    const windowValue = String(modules.soon.windowFilter?.value || 'all');
    const limit = Number(modules.soon.limitFilter?.value || 20) || 20;
    const filtered = modules.soon.items
      .filter((item) => {
        if (windowValue === 'today') return item.window === 'today';
        if (windowValue === 'week') return item.window === 'today' || item.window === 'week';
        if (windowValue === 'fortnight') return item.window !== 'all';
        return true;
      })
      .slice(0, limit);
    modules.soon.carousel.setItems(filtered);
  }

  function applyRecentFilters() {
    if (!modules.recent.carousel) return;
    const typeValue = String(modules.recent.typeFilter?.value || 'imported');
    const limit = Number(modules.recent.limitFilter?.value || 20) || 20;
    const filtered = modules.recent.items
      .filter((item) => typeValue === 'all' || item.eventType === typeValue)
      .slice(0, limit);
    modules.recent.carousel.setItems(filtered);
  }

  async function loadSoon() {
    if (!hasSoon) return;
    modules.soon.track.innerHTML = '<div class="plex-empty">Loading...</div>';

    const startDate = new Date();
    startDate.setHours(0, 0, 0, 0);
    const endDate = new Date(startDate.getTime());
    endDate.setDate(endDate.getDate() + 14);

    try {
      const payload = await fetchSonarr('/api/v3/calendar', {
        includeSeries: true,
        includeEpisodeFile: false,
        start: startDate.toISOString().slice(0, 10),
        end: endDate.toISOString().slice(0, 10),
      });
      const list = Array.isArray(payload) ? payload : [];
      modules.soon.items = list
        .filter((entry) => !entry?.hasFile)
        .filter((entry) => entry?.series?.monitored !== false)
        .map(mapSoonItem)
        .map((item) => ({ ...item, window: normalizeSoonWindow(item) }))
        .sort((a, b) => a.airTimestamp - b.airTimestamp);
      applySoonFilters();
    } catch (err) {
      modules.soon.track.innerHTML = '<div class="plex-empty">Unable to load Sonarr calendar.</div>';
    }
  }

  async function loadRecent() {
    if (!hasRecent) return;
    modules.recent.track.innerHTML = '<div class="plex-empty">Loading...</div>';

    try {
      const payload = await fetchSonarr('/api/v3/history', {
        page: 1,
        pageSize: 100,
        sortKey: 'date',
        sortDirection: 'descending',
        includeSeries: true,
        includeEpisode: true,
      });
      const records = Array.isArray(payload?.records) ? payload.records : [];
      modules.recent.items = records
        .map(mapRecentItem)
        .filter((item) => item.eventType !== 'other')
        .sort((a, b) => b.timestamp - a.timestamp);
      applyRecentFilters();
    } catch (err) {
      modules.recent.track.innerHTML = '<div class="plex-empty">Unable to load Sonarr history.</div>';
    }
  }

  function bindCollapseButtons() {
    document.querySelectorAll('.plex-collapse-btn[data-target^="sonarr-"]').forEach((button) => {
      button.addEventListener('click', () => {
        const targetId = button.getAttribute('data-target');
        const section = targetId ? document.getElementById(targetId) : null;
        if (!section) return;
        section.classList.toggle('plex-collapsed');
      });
    });
  }
})();
