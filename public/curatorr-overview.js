(function() {
  var modalState = {
    backdrop: null,
    lastTrigger: null,
  };

  function escHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function buildUrl(trigger) {
    var kind = trigger.dataset.curOverviewKind;
    var user = trigger.dataset.curOverviewUser || '';
    if (kind === 'artist') {
      return '/api/music/overview/artist/' + encodeURIComponent(trigger.dataset.curOverviewName || '') + (user ? ('?user=' + encodeURIComponent(user)) : '');
    }
    if (kind === 'album') {
      var artist = trigger.dataset.curOverviewArtist || '';
      var album = trigger.dataset.curOverviewAlbum || '';
      return '/api/music/overview/album?artist=' + encodeURIComponent(artist) + '&album=' + encodeURIComponent(album) + (user ? ('&user=' + encodeURIComponent(user)) : '');
    }
    if (kind === 'track') {
      return '/api/music/overview/track/' + encodeURIComponent(trigger.dataset.curOverviewKey || '') + (user ? ('?user=' + encodeURIComponent(user)) : '');
    }
    return '';
  }

  function ensureModal() {
    if (modalState.backdrop) return modalState.backdrop;
    var backdrop = document.createElement('div');
    backdrop.className = 'plex-modal-backdrop plex-hidden';
    backdrop.innerHTML =
      '<div class="plex-modal" role="dialog" aria-modal="true" aria-labelledby="curOverviewTitle">' +
        '<button type="button" class="plex-modal-close" aria-label="Close overview">×</button>' +
        '<div class="plex-modal-header">' +
          '<h2 class="plex-modal-title" id="curOverviewTitle">Loading…</h2>' +
          '<div class="plex-modal-subtitle"></div>' +
        '</div>' +
        '<div class="plex-modal-body">' +
          '<div class="plex-modal-hero">' +
            '<div class="plex-modal-bg"></div>' +
            '<div class="plex-modal-content">' +
              '<div class="plex-modal-poster"></div>' +
              '<div class="plex-modal-meta">' +
                '<div class="plex-pills"></div>' +
                '<div class="plex-section">' +
                  '<h4>Overview</h4>' +
                  '<p class="plex-overview-text"></p>' +
                '</div>' +
              '</div>' +
            '</div>' +
          '</div>' +
        '</div>' +
        '<div class="plex-modal-footer">' +
          '<div class="plex-pills plex-pills--stats"></div>' +
        '</div>' +
      '</div>';
    document.body.appendChild(backdrop);
    backdrop.addEventListener('click', function(event) {
      if (event.target === backdrop) closeModal();
    });
    backdrop.querySelector('.plex-modal-close').addEventListener('click', closeModal);
    modalState.backdrop = backdrop;
    return backdrop;
  }

  function closeModal() {
    if (!modalState.backdrop) return;
    modalState.backdrop.classList.add('plex-hidden');
    document.body.classList.remove('cur-modal-open');
    if (modalState.lastTrigger) {
      modalState.lastTrigger.focus();
      modalState.lastTrigger = null;
    }
  }

  function renderPoster(container, item) {
    var title = String(item.title || item.subtitle || item.kind || '?').trim();
    if (item.thumb) {
      container.innerHTML = '<img src="' + escHtml(item.thumb) + '" alt="' + escHtml(title) + '" loading="lazy" />';
      return;
    }
    container.innerHTML = '<div class="plex-placeholder-big">' + escHtml(title.slice(0, 1).toUpperCase() || '?') + '</div>';
  }

  function renderPills(container, pills) {
    var items = Array.isArray(pills) ? pills.filter(Boolean) : [];
    if (!items.length) {
      container.innerHTML = '';
      return;
    }
    container.innerHTML = items.map(function(pill) {
      return '<span class="plex-pill2">' + escHtml(pill) + '</span>';
    }).join('');
  }

  function renderStats(container, stats) {
    var items = Array.isArray(stats) ? stats.filter(function(stat) {
      return stat && stat.label && typeof stat.value !== 'undefined' && stat.value !== null && stat.value !== '';
    }) : [];
    if (!items.length) {
      container.innerHTML = '';
      return;
    }
    container.innerHTML = items.map(function(stat) {
      return '<span class="plex-pill2 plex-pill2--stat">' +
        '<span class="plex-pill2-stat-label">' + escHtml(stat.label) + '</span>' +
        '<strong class="plex-pill2-stat-value">' + escHtml(stat.value) + '</strong>' +
      '</span>';
    }).join('');
  }

  function renderItem(item) {
    var backdrop = ensureModal();
    var modal = backdrop.querySelector('.plex-modal');
    var bg = modal.querySelector('.plex-modal-bg');
    var poster = modal.querySelector('.plex-modal-poster');
    var title = modal.querySelector('.plex-modal-title');
    var subtitle = modal.querySelector('.plex-modal-subtitle');
    var pills = modal.querySelector('.plex-pills');
    var statsPills = modal.querySelector('.plex-pills--stats');
    var overview = modal.querySelector('.plex-overview-text');
    var kindPills = pills;

    bg.style.backgroundImage = item.art ? 'url("' + String(item.art).replace(/"/g, '&quot;') + '")' : '';
    renderPoster(poster, item);
    title.textContent = item.title || 'Untitled';
    subtitle.textContent = item.subtitle || '';
    renderPills(kindPills, item.pills);
    overview.textContent = item.overview || 'No overview available for this item yet.';
    renderStats(statsPills, item.stats);

    backdrop.classList.remove('plex-hidden');
    document.body.classList.add('cur-modal-open');
    modal.querySelector('.plex-modal-close').focus();
  }

  function loadOverview(trigger) {
    var url = buildUrl(trigger);
    if (!url) return;
    modalState.lastTrigger = trigger;
    renderItem({
      title: 'Loading…',
      subtitle: '',
      overview: 'Fetching item details…',
      pills: [],
      stats: [],
    });
    fetch(url)
      .then(function(response) {
        return response.json().then(function(data) {
          return { ok: response.ok, data: data };
        });
      })
      .then(function(result) {
        if (!result.ok || !result.data || !result.data.item) {
          throw new Error((result.data && result.data.error) || 'Failed to load item overview.');
        }
        renderItem(result.data.item);
      })
      .catch(function(error) {
        renderItem({
          title: 'Overview unavailable',
          subtitle: '',
          overview: error.message || 'Failed to load item overview.',
          pills: [],
          stats: [],
        });
      });
  }

  document.addEventListener('click', function(event) {
    var trigger = event.target.closest('[data-cur-overview-kind]');
    if (!trigger) return;
    event.preventDefault();
    loadOverview(trigger);
  });

  document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape' && modalState.backdrop && !modalState.backdrop.classList.contains('plex-hidden')) {
      closeModal();
      return;
    }
    if (event.key !== 'Enter' && event.key !== ' ') return;
    var trigger = event.target.closest('[data-cur-overview-kind]');
    if (!trigger) return;
    event.preventDefault();
    loadOverview(trigger);
  });
})();
