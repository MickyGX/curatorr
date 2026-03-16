(function () {
  var panels = Array.prototype.slice.call(document.querySelectorAll('.cur-panel'));
  if (!panels.length) return;

  function slugify(value, fallback) {
    return String(value || fallback || 'panel')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '') || (fallback || 'panel');
  }

  function getStorageKey(panel, index) {
    var explicit = panel.getAttribute('data-panel-key');
    if (explicit) return 'curatorr-panel:' + explicit;
    var title = panel.querySelector('.cur-panel-title, .cur-panel-subtitle');
    return 'curatorr-panel:' + window.location.pathname + ':' + slugify(title && title.textContent, 'panel-' + index);
  }

  panels.forEach(function (panel, index) {
    if (panel.getAttribute('data-panel-collapsible') === 'false') return;
    var header = panel.querySelector(':scope > .cur-panel-header');
    if (!header) return;

    var actions = header.querySelector('.cur-panel-actions');
    if (!actions) {
      actions = document.createElement('div');
      actions.className = 'cur-panel-actions';
      header.appendChild(actions);
    }

    Array.prototype.slice.call(header.children).forEach(function (child) {
      if (child === actions) return;
      if (child.classList && (child.classList.contains('cur-panel-link') || child.classList.contains('cur-panel-meta'))) {
        actions.appendChild(child);
      }
    });

    var toggle = document.createElement('button');
    toggle.type = 'button';
    toggle.className = 'plex-iconbtn cur-panel-toggle';
    toggle.setAttribute('aria-expanded', 'true');
    toggle.setAttribute('title', 'Collapse section');

    var storageKey = getStorageKey(panel, index);

    function render(collapsed) {
      panel.classList.toggle('is-collapsed', collapsed);
      toggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
      toggle.setAttribute('title', collapsed ? 'Expand section' : 'Collapse section');
      toggle.innerHTML = '<div class="plex-doublechev" aria-hidden="true"><div class="plex-chev plex-up"></div><div class="plex-chev plex-down"></div></div>';
    }

    try {
      render(localStorage.getItem(storageKey) === 'collapsed');
    } catch (_storageReadErr) {
      render(false);
    }

    toggle.addEventListener('click', function () {
      var collapsed = !panel.classList.contains('is-collapsed');
      render(collapsed);
      try {
        localStorage.setItem(storageKey, collapsed ? 'collapsed' : 'expanded');
      } catch (_storageWriteErr) {}
    });

    actions.appendChild(toggle);
  });
})();
