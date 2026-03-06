(() => {
  const status = document.querySelector('.main-window-status');
  if (!status) return;
  const root = document.documentElement;
  const releaseBase = 'https://github.com/MickyGX/launcharr/releases/tag/';
  const supportUrl = 'https://buymeacoffee.com/mickygx';
  const dot = status.querySelector('.status-dot');
  const themeStorageKey = 'launcharr-theme';
  const getThemeMode = () => root.dataset.theme === 'day' ? 'day' : 'night';
  const applyThemeMode = (value) => {
    const next = value === 'day' ? 'day' : 'night';
    root.dataset.theme = next;
    if (document.body) document.body.dataset.theme = next;
    try {
      localStorage.setItem(themeStorageKey, next);
    } catch (err) {
      // Ignore storage failures.
    }
  };
  const themeToggle = document.createElement('button');
  themeToggle.type = 'button';
  themeToggle.className = 'theme-toggle';
  const themeIcon = document.createElement('img');
  themeIcon.className = 'theme-toggle-icon';
  themeIcon.alt = '';
  themeIcon.setAttribute('aria-hidden', 'true');
  themeToggle.appendChild(themeIcon);
  const syncThemeToggle = () => {
    const mode = getThemeMode();
    const targetLabel = mode === 'day' ? 'Switch to Dark Mode' : 'Switch to Light Mode';
    themeIcon.src = mode === 'day' ? '/icons/sun.svg' : '/icons/moon.svg';
    themeToggle.setAttribute('aria-label', targetLabel);
    themeToggle.title = targetLabel;
  };
  themeToggle.addEventListener('click', (event) => {
    event.preventDefault();
    applyThemeMode(getThemeMode() === 'day' ? 'night' : 'day');
    syncThemeToggle();
  });
  syncThemeToggle();

  if (typeof MutationObserver === 'function') {
    const observer = new MutationObserver(syncThemeToggle);
    observer.observe(root, { attributes: true, attributeFilter: ['data-theme'] });
  }

  if (dot) {
    status.insertBefore(themeToggle, dot);
  } else {
    status.appendChild(themeToggle);
  }

  const activeRoleViewOption = document.querySelector('.user-menu-role-option.is-active[data-role-value]');
  const displayedRole = String(document.querySelector('.user-meta .role')?.textContent || '').trim().toLowerCase();
  const inAdminView = activeRoleViewOption
    ? String(activeRoleViewOption.getAttribute('data-role-value') || '').trim().toLowerCase() === 'admin'
    : displayedRole === 'admin';
  const normalizeVersionTag = (value) => {
    const raw = String(value || '').trim();
    if (!raw) return '';
    return raw.startsWith('v') ? raw : `v${raw}`;
  };
  const escapeHtml = (value) => String(value || '').replace(/[&<>"']/g, (char) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#039;',
  }[char]));
  const buildReleaseUrl = (versionTag) => releaseBase + encodeURIComponent(String(versionTag || '').trim());

  const maybeShowReleaseWelcome = ({ current, highlights, releaseNotesUrl }) => {
    const versionTag = normalizeVersionTag(current);
    if (!versionTag) return;
    if (document.getElementById('releaseWelcomeBackdrop')) return;
    const storageKey = 'launcharr-release-welcome-seen';
    let seenVersion = '';
    try {
      seenVersion = String(localStorage.getItem(storageKey) || '').trim();
    } catch (err) {
      seenVersion = '';
    }
    if (seenVersion === versionTag) return;
    const normalizedHighlights = (Array.isArray(highlights) ? highlights : [])
      .map((item) => String(item || '').trim())
      .filter(Boolean)
      .slice(0, 6);
    const displayHighlights = normalizedHighlights.length
      ? normalizedHighlights
      : ['Release notes are available for this version.'];
    const parsedHighlights = displayHighlights.map((item) => {
      const match = String(item || '').match(/^([A-Za-z][A-Za-z\s]+):\s*(.+)$/);
      const section = match ? String(match[1] || '').trim() : 'Update';
      const detail = match ? String(match[2] || '').trim() : String(item || '').trim();
      const sectionKey = section.toLowerCase().replace(/[^a-z]+/g, '');
      const sectionClass = ['added', 'changed', 'fixed'].includes(sectionKey)
        ? ` release-welcome-tag--${sectionKey}`
        : '';
      return { section, detail, sectionClass };
    });
    const effectiveReleaseNotesUrl = String(releaseNotesUrl || '').trim() || buildReleaseUrl(versionTag);

    const backdrop = document.createElement('div');
    backdrop.id = 'releaseWelcomeBackdrop';
    backdrop.className = 'plex-modal-backdrop release-welcome-backdrop';
    backdrop.innerHTML =
      '<div class="plex-modal release-welcome-modal" role="dialog" aria-modal="true" aria-labelledby="releaseWelcomeTitle">' +
        '<button class="plex-modal-close" type="button" aria-label="Close">✕</button>' +
        '<div class="plex-modal-header">' +
          '<div class="plex-modal-title" id="releaseWelcomeTitle">' + escapeHtml(`Welcome to ${versionTag}`) + '</div>' +
          '<div class="plex-modal-subtitle">' + escapeHtml(`Main changes in ${versionTag}`) + '</div>' +
        '</div>' +
        '<div class="plex-modal-body release-welcome-body">' +
          '<div class="plex-modal-scroll release-welcome-scroll">' +
            '<div class="plex-modal-hero release-welcome-hero">' +
              '<div class="release-welcome-content">' +
                '<div class="release-welcome-brand">' +
                  '<img class="release-welcome-icon" src="/icons/launcharr-icon.png" alt="Launcharr" loading="eager" />' +
                  '<div class="plex-pills release-welcome-pills">' +
                    `<span class="plex-pill2 release-welcome-pill">${escapeHtml(versionTag)}</span>` +
                  '</div>' +
                '</div>' +
                '<div class="release-welcome-summary">Highlights from this release:</div>' +
                '<ul class="release-welcome-list">' +
                  parsedHighlights
                    .map((item) => (
                      '<li class="release-welcome-item">' +
                        `<span class="release-welcome-tag${item.sectionClass}">${escapeHtml(item.section)}</span>` +
                        `<span class="release-welcome-text">${escapeHtml(item.detail)}</span>` +
                      '</li>'
                    ))
                    .join('') +
                '</ul>' +
              '</div>' +
            '</div>' +
          '</div>' +
        '</div>' +
        '<div class="plex-modal-footer release-welcome-footer">' +
          `<a class="plex-modal-link release-welcome-nav" href="${escapeHtml(effectiveReleaseNotesUrl)}" target="_blank" rel="noreferrer noopener">Release Notes</a>` +
          `<a class="plex-modal-link release-welcome-support" href="${escapeHtml(supportUrl)}" target="_blank" rel="noreferrer noopener">Buy Me a Coffee</a>` +
          '<a class="plex-modal-link release-welcome-settings" href="/settings?tab=custom&settingsCustomTab=dashboard">Dashboard Settings</a>' +
          '<button class="plex-modal-link release-welcome-close" type="button">Continue</button>' +
        '</div>' +
      '</div>';

    const closeButton = backdrop.querySelector('.release-welcome-close');
    const iconCloseButton = backdrop.querySelector('.plex-modal-close');
    const modal = backdrop.querySelector('.release-welcome-modal');
    const markSeen = () => {
      try {
        localStorage.setItem(storageKey, versionTag);
      } catch (err) {
        // ignore storage errors
      }
    };
    const close = () => {
      markSeen();
      backdrop.remove();
      window.removeEventListener('keydown', onKeyDown);
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') close();
    };

    backdrop.addEventListener('click', (event) => {
      if (event.target === backdrop) close();
    });
    backdrop.querySelectorAll('a').forEach((linkEl) => {
      linkEl.addEventListener('click', markSeen);
    });
    if (closeButton) closeButton.addEventListener('click', close);
    if (iconCloseButton) iconCloseButton.addEventListener('click', close);
    window.addEventListener('keydown', onKeyDown);
    document.body.appendChild(backdrop);
    if (modal && typeof modal.focus === 'function') {
      modal.setAttribute('tabindex', '-1');
      modal.focus();
    }
  };

  const dismissQuickStartGuide = () => fetch('/api/onboarding/quick-start/dismiss', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: '{}',
  }).catch(() => {});

  const maybeShowQuickStartGuide = () => fetch('/api/onboarding/quick-start')
    .then((res) => {
      if (!res.ok) throw new Error('quick-start-unavailable');
      return res.json();
    })
    .then((data) => {
      if (!data || !data.show) return false;
      if (document.getElementById('quickStartBackdrop')) return true;

      const steps = (Array.isArray(data.steps) ? data.steps : [])
        .map((item) => String(item || '').trim())
        .filter(Boolean)
        .slice(0, 6);
      const displaySteps = steps.length
        ? steps
        : [
          'Open Settings and add your first app.',
          'Open Dashboard settings and add your first card.',
        ];
      const actions = data && typeof data.actions === 'object' ? data.actions : {};
      const sidebarHref = String(actions.sidebar || '/settings?tab=custom&settingsCustomTab=sidebar').trim() || '/settings?tab=custom&settingsCustomTab=sidebar';

      const backdrop = document.createElement('div');
      backdrop.id = 'quickStartBackdrop';
      backdrop.className = 'plex-modal-backdrop quick-start-backdrop';
      backdrop.innerHTML =
        '<div class="plex-modal quick-start-modal" role="dialog" aria-modal="true" aria-labelledby="quickStartTitle">' +
          '<button class="plex-modal-close" type="button" aria-label="Close">✕</button>' +
          '<div class="plex-modal-header">' +
            '<div class="plex-modal-title" id="quickStartTitle">Quick Start Guide</div>' +
            '<div class="plex-modal-subtitle">Set up your first app and dashboard cards.</div>' +
          '</div>' +
          '<div class="plex-modal-body quick-start-body">' +
            '<div class="plex-modal-scroll quick-start-scroll">' +
              '<div class="plex-modal-hero quick-start-hero">' +
                '<div class="quick-start-content">' +
                  '<div class="quick-start-brand">' +
                    '<img class="quick-start-icon" src="/icons/launcharr-icon.png" alt="Launcharr" loading="eager" />' +
                    '<div class="quick-start-summary">Recommended first steps:</div>' +
                  '</div>' +
                  '<ol class="quick-start-list">' +
                    displaySteps
                      .map((step, index) => (
                        '<li class="quick-start-item">' +
                          `<span class="quick-start-step">${index + 1}</span>` +
                          `<span class="quick-start-text">${escapeHtml(step)}</span>` +
                        '</li>'
                      ))
                      .join('') +
                  '</ol>' +
                '</div>' +
              '</div>' +
            '</div>' +
          '</div>' +
          '<div class="plex-modal-footer quick-start-footer">' +
            `<a class="plex-modal-link quick-start-link" href="${escapeHtml(sidebarHref)}">Open Sidebar Settings</a>` +
            '<button class="plex-modal-link quick-start-close" type="button">Got it</button>' +
          '</div>' +
        '</div>';

      const closeButton = backdrop.querySelector('.quick-start-close');
      const iconCloseButton = backdrop.querySelector('.plex-modal-close');
      const modal = backdrop.querySelector('.quick-start-modal');
      const close = () => {
        dismissQuickStartGuide();
        backdrop.remove();
        window.removeEventListener('keydown', onKeyDown);
      };
      const onKeyDown = (event) => {
        if (event.key === 'Escape') close();
      };

      backdrop.addEventListener('click', (event) => {
        if (event.target === backdrop) close();
      });
      backdrop.querySelectorAll('a').forEach((linkEl) => {
        linkEl.addEventListener('click', (event) => {
          event.preventDefault();
          const href = linkEl.getAttribute('href') || '/settings';
          const target = String(linkEl.getAttribute('target') || '').trim();
          dismissQuickStartGuide().finally(() => {
            if (target === '_blank') {
              window.open(href, '_blank', 'noopener,noreferrer');
              return;
            }
            window.location.href = href;
          });
        });
      });
      if (closeButton) closeButton.addEventListener('click', close);
      if (iconCloseButton) iconCloseButton.addEventListener('click', close);
      window.addEventListener('keydown', onKeyDown);
      document.body.appendChild(backdrop);
      if (modal && typeof modal.focus === 'function') {
        modal.setAttribute('tabindex', '-1');
        modal.focus();
      }
      return true;
    })
    .catch(() => false);

  if (!inAdminView) return;

  let quickStartShown = false;
  maybeShowQuickStartGuide()
    .then((shown) => {
      quickStartShown = Boolean(shown);
      return fetch('/api/version');
    })
    .then((res) => res.json())
    .then((data) => {
      const current = String(data?.current || '').trim();
      const latest = String(data?.latest || '').trim();
      if (!current) return;
      const compactLabel = Boolean(window.matchMedia && window.matchMedia('(max-width: 980px)').matches);
      const buildVersionPill = ({ text, className, versionTag, ariaPrefix }) => {
        const link = document.createElement('a');
        link.className = className;
        link.href = buildReleaseUrl(versionTag);
        link.target = '_blank';
        link.rel = 'noreferrer noopener';
        link.textContent = text;
        link.setAttribute('aria-label', `${ariaPrefix} ${versionTag} release notes`);
        return link;
      };

      const wrap = document.createElement('div');
      wrap.className = 'version-badge';

      wrap.appendChild(buildVersionPill({
        text: compactLabel ? current : `Current ${current}`,
        className: 'version-pill',
        versionTag: current,
        ariaPrefix: 'Current version',
      }));

      if (latest && latest !== current) {
        wrap.appendChild(buildVersionPill({
          text: compactLabel ? latest : `Latest ${latest}`,
          className: 'version-pill version-pill--latest',
          versionTag: latest,
          ariaPrefix: 'Latest version',
        }));
      }

      status.prepend(wrap);
      if (!quickStartShown) {
        maybeShowReleaseWelcome({
          current,
          highlights: data?.highlights,
          releaseNotesUrl: data?.releaseNotesUrl,
        });
      }
    })
    .catch(() => {});
})();
