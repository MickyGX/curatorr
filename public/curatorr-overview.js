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
    if (kind === 'manual-album') {
      var params = new URLSearchParams();
      params.set('artist', trigger.dataset.curOverviewArtist || '');
      params.set('album', trigger.dataset.curOverviewAlbum || '');
      if (trigger.dataset.curOverviewAlbumId) params.set('albumId', trigger.dataset.curOverviewAlbumId);
      if (trigger.dataset.curOverviewForeignAlbumId) params.set('foreignAlbumId', trigger.dataset.curOverviewForeignAlbumId);
      if (trigger.dataset.curOverviewSource) params.set('source', trigger.dataset.curOverviewSource);
      if (trigger.dataset.curOverviewAlbumType) params.set('albumType', trigger.dataset.curOverviewAlbumType);
      if (trigger.dataset.curOverviewReleaseDate) params.set('releaseDate', trigger.dataset.curOverviewReleaseDate);
      return '/api/music/lidarr/manual/album-overview?' + params.toString();
    }
    if (kind === 'manual-curatorr-pick') {
      var pickParams = new URLSearchParams();
      pickParams.set('artist', trigger.dataset.curOverviewArtist || '');
      if (trigger.dataset.curOverviewForeignArtistId) pickParams.set('foreignArtistId', trigger.dataset.curOverviewForeignArtistId);
      return '/api/music/lidarr/manual/curatorr-pick-overview?' + pickParams.toString();
    }
    return '';
  }

  function getCsrfToken() {
    var field = document.querySelector('[name="_csrf"]');
    return field ? String(field.value || '').trim() : '';
  }

  function handleBuiltInAction(kind, payload, button) {
    if (kind !== 'track-pin-toggle') return false;
    if (!payload || !payload.ratingKey || (button && button.disabled)) return true;
    var csrfToken = getCsrfToken();
    if (!csrfToken) return true;
    if (button) button.disabled = true;
    fetch('/api/music/tracks/' + encodeURIComponent(String(payload.ratingKey || '')) + '/include', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRF-Token': csrfToken,
      },
      body: JSON.stringify({ included: payload.included !== false }),
    }).then(function(response) {
      if (!response.ok) throw new Error('Track pin update failed.');
      window.location.reload();
    }).catch(function() {
      if (button) button.disabled = false;
    });
    return true;
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
          '<div class="plex-modal-scroll">' +
            '<div class="plex-modal-hero">' +
              '<div class="plex-modal-bg"></div>' +
              '<div class="plex-modal-content">' +
                '<div class="plex-modal-poster"></div>' +
                '<div class="plex-modal-meta">' +
                  '<div class="plex-pills"></div>' +
                  '<div class="plex-modal-meta-scroll">' +
                    '<div class="cur-overview-details"></div>' +
                    '<div class="plex-section plex-section--tracks plex-hidden">' +
                      '<h4>Tracks</h4>' +
                      '<div class="cur-overview-track-list"></div>' +
                    '</div>' +
                  '</div>' +
                '</div>' +
              '</div>' +
            '</div>' +
          '</div>' +
        '</div>' +
        '<div class="plex-modal-footer">' +
          '<div class="plex-pills plex-pills--stats"></div>' +
          '<div class="plex-modal-actions"></div>' +
        '</div>' +
      '</div>';
    document.body.appendChild(backdrop);
    backdrop.addEventListener('click', function(event) {
      if (event.target === backdrop) closeModal();
    });
    backdrop.querySelector('.plex-modal-close').addEventListener('click', closeModal);
    backdrop.addEventListener('click', function(event) {
      var actionBtn = event.target.closest('[data-cur-overview-action-kind]');
      if (!actionBtn || actionBtn.disabled) return;
      var payload = {};
      try {
        payload = JSON.parse(actionBtn.dataset.curOverviewActionPayload || '{}');
      } catch (_err) {
        payload = {};
      }
      if (handleBuiltInAction(actionBtn.dataset.curOverviewActionKind || '', payload, actionBtn)) return;
      var handler = window.curatorrOverviewHandleAction;
      if (typeof handler !== 'function') return;
      handler(actionBtn.dataset.curOverviewActionKind || '', payload, actionBtn, modalState.lastTrigger);
    });
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

  function renderDetailSections(container, sections) {
    var items = Array.isArray(sections) ? sections.filter(Boolean) : [];
    if (!items.length) {
      container.innerHTML = '';
      return;
    }
    container.innerHTML = items.map(function(section) {
      var rows = Array.isArray(section.rows) ? section.rows.filter(function(row) {
        return row && row.label && typeof row.value !== 'undefined' && row.value !== null && row.value !== '';
      }) : [];
      var text = String(section.text || '').trim();
      return '<div class="plex-section">' +
        (section.title ? '<h4>' + escHtml(section.title) + '</h4>' : '') +
        (text ? '<p class="cur-overview-detail-text">' + escHtml(text) + '</p>' : '') +
        (rows.length
          ? '<div class="cur-overview-detail-grid">' + rows.map(function(row) {
            return '<div class="cur-overview-detail-row">' +
              '<span class="cur-overview-detail-label">' + escHtml(row.label) + '</span>' +
              '<span class="cur-overview-detail-value">' + escHtml(row.value) + '</span>' +
            '</div>';
          }).join('') + '</div>'
          : '') +
      '</div>';
    }).join('');
  }

  function renderTrackList(container, tracks) {
    var items = Array.isArray(tracks) ? tracks.filter(function(track) {
      return track && track.title;
    }) : [];
    var section = container.closest('.plex-section--tracks');
    if (!items.length) {
      container.innerHTML = '';
      if (section) section.classList.add('plex-hidden');
      return;
    }
    var mediaNumbers = items
      .map(function(track) { return Number(track.mediumNumber || 0) || 0; })
      .filter(function(value) { return value > 0; });
    var mediumDisplayMap = new Map();
    Array.from(new Set(mediaNumbers))
      .sort(function(left, right) { return left - right; })
      .forEach(function(value, index) {
        mediumDisplayMap.set(value, index + 1);
      });
    var hasDiscHeaders = (new Set(mediaNumbers)).size > 1;
    var currentMedium = null;
    container.innerHTML = items.map(function(track) {
      var mediumNumber = Number(track.mediumNumber || 0) || 0;
      var html = '';
      if (hasDiscHeaders && mediumNumber > 0 && mediumNumber !== currentMedium) {
        currentMedium = mediumNumber;
        html += '<div class="cur-overview-track-group">CD ' + escHtml(mediumDisplayMap.get(mediumNumber) || mediumNumber) + '</div>';
      }
      html += '<div class="cur-overview-track-row' + (track.thumb ? ' has-thumb' : '') + '">' +
        '<span class="cur-overview-track-index">' + escHtml(track.index || '') + '</span>' +
        (track.thumb
          ? '<span class="cur-overview-track-thumb"><img src="' + escHtml(track.thumb) + '" alt="" loading="lazy" /></span>'
          : '') +
        '<span class="cur-overview-track-body">' +
          '<span class="cur-overview-track-title">' + escHtml(track.title || '') + '</span>' +
          (track.meta ? '<span class="cur-overview-track-meta">' + escHtml(track.meta || '') + '</span>' : '') +
        '</span>' +
      '</div>';
      return html;
    }).join('');
    if (section) section.classList.remove('plex-hidden');
  }

  function renderActions(container, actions) {
    var items = Array.isArray(actions) ? actions.filter(Boolean) : [];
    if (!items.length) {
      container.innerHTML = '';
      return;
    }
    container.innerHTML = items.map(function(action) {
      var payload = '{}';
      try {
        payload = JSON.stringify(action.payload || {});
      } catch (_err) {
        payload = '{}';
      }
      return '<button type="button" class="plex-modal-link plex-modal-link--action' + (action.disabled ? ' is-disabled' : '') + '"' +
        ' data-cur-overview-action-kind="' + escHtml(action.kind || '') + '"' +
        ' data-cur-overview-action-payload="' + escHtml(payload) + '"' +
        (action.disabled ? ' disabled' : '') + '>' + escHtml(action.label || 'Action') + '</button>';
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
    var actions = modal.querySelector('.plex-modal-actions');
    var detailSections = modal.querySelector('.cur-overview-details');
    var scroll = modal.querySelector('.plex-modal-meta-scroll');
    var trackList = modal.querySelector('.cur-overview-track-list');
    var trackHeading = modal.querySelector('.plex-section--tracks h4');
    var kindPills = pills;

    bg.style.backgroundImage = item.art ? 'url("' + String(item.art).replace(/"/g, '&quot;') + '")' : '';
    poster.classList.toggle('is-square', item.posterRatio === 'square');
    poster.classList.toggle('is-contain', item.posterFit === 'contain');
    renderPoster(poster, item);
    title.textContent = item.title || 'Untitled';
    subtitle.textContent = item.subtitle || '';
    renderPills(kindPills, item.pills);
    renderDetailSections(detailSections, item.detailSections);
    renderStats(statsPills, item.stats);
    if (trackHeading) trackHeading.textContent = item.trackSectionTitle || 'Tracks';
    renderTrackList(trackList, item.trackList);
    renderActions(actions, item.actions);
    if (scroll) scroll.scrollTop = 0;

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
