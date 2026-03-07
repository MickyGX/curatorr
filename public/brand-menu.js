(function () {
  const brand = document.querySelector('.dash-brand');
  const root = document.documentElement;
  if (!brand || !root) return;

  const safeStorage = {
    get(key) {
      try {
        return localStorage.getItem(key);
      } catch (_err) {
        return null;
      }
    },
    set(key, value) {
      try {
        localStorage.setItem(key, value);
      } catch (_err) {
        return;
      }
    },
  };

  const storageKeys = {
    theme: 'curatorr-theme',
    bgMotion: 'curatorr-bg-motion',
    maximized: 'curatorr-maximized-window',
  };
  const mobileMotionQuery = window.matchMedia
    ? window.matchMedia('(max-width: 980px)')
    : null;

  const wrapper = document.createElement('div');
  const maximizeIconSvg = ''
    + '<svg class="dash-brand-menu-item-icon" viewBox="0 0 24 24" aria-hidden="true" focusable="false">'
    + '  <rect x="5" y="5" width="14" height="14" rx="2" ry="2" fill="none" stroke="currentColor" stroke-width="1.8" />'
    + '</svg>';
  const minimizeIconSvg = ''
    + '<svg class="dash-brand-menu-item-icon" viewBox="0 0 24 24" aria-hidden="true" focusable="false">'
    + '  <rect x="5.5" y="8" width="11" height="10.5" rx="1.6" ry="1.6" fill="none" stroke="currentColor" stroke-width="1.8" />'
    + '  <path fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" d="M9 5.5h9.5V15" />'
    + '</svg>';
  wrapper.className = 'dash-brand-menu';
  wrapper.innerHTML = ''
    + '<button class="dash-brand-menu-toggle" type="button" aria-label="Open Launcharr menu" aria-expanded="false">'
    + '  <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">'
    + '    <circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="1.9" />'
    + '    <line x1="8" y1="9" x2="16" y2="9" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" />'
    + '    <line x1="8" y1="12" x2="16" y2="12" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" />'
    + '    <line x1="8" y1="15" x2="16" y2="15" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" />'
    + '  </svg>'
    + '</button>'
    + '<div class="dash-brand-menu-popover" hidden>'
    + '  <div class="dash-brand-menu-heading">Settings</div>'
    + '  <button class="dash-brand-menu-item" type="button" data-action="theme">'
    + '    <img class="dash-brand-menu-item-icon" src="/icons/moon.svg" alt="" data-icon="theme" />'
    + '    <span class="dash-brand-menu-item-label" data-label="theme">Switch to Light Mode</span>'
    + '  </button>'
    + '  <button class="dash-brand-menu-item" type="button" data-action="bg-motion">'
    + '    <svg class="dash-brand-menu-item-icon" viewBox="0 0 24 24" aria-hidden="true" focusable="false">'
    + '      <path fill="currentColor" d="M12 4.2l1.15 2.73 2.73 1.15-2.73 1.15L12 12l-1.15-2.77-2.73-1.15 2.73-1.15L12 4.2z" />'
    + '      <path fill="currentColor" d="M18.5 3.6l.62 1.48 1.48.62-1.48.62-.62 1.48-.62-1.48-1.48-.62 1.48-.62.62-1.48zM5.2 14.5l.7 1.66 1.66.7-1.66.7-.7 1.66-.7-1.66-1.66-.7 1.66-.7.7-1.66zM16.2 14.8l.48 1.12 1.12.48-1.12.48-.48 1.12-.48-1.12-1.12-.48 1.12-.48.48-1.12z" />'
    + '    </svg>'
    + '    <span class="dash-brand-menu-item-label" data-label="bg-motion">Disable Starfield</span>'
    + '  </button>'
    + '  <button class="dash-brand-menu-item" type="button" data-action="maximize">'
    + '    <span data-icon="maximize"></span>'
    + '    <span class="dash-brand-menu-item-label" data-label="maximize">Maximize window</span>'
    + '  </button>'
    + '</div>';
  brand.appendChild(wrapper);

  const toggle = wrapper.querySelector('.dash-brand-menu-toggle');
  const menu = wrapper.querySelector('.dash-brand-menu-popover');
  const themeBtn = wrapper.querySelector('[data-action="theme"]');
  const motionBtn = wrapper.querySelector('[data-action="bg-motion"]');
  const maximizeBtn = wrapper.querySelector('[data-action="maximize"]');
  const themeLabel = wrapper.querySelector('[data-label="theme"]');
  const motionLabel = wrapper.querySelector('[data-label="bg-motion"]');
  const maximizeLabel = wrapper.querySelector('[data-label="maximize"]');
  const themeIcon = wrapper.querySelector('[data-icon="theme"]');
  const maximizeIcon = wrapper.querySelector('[data-icon="maximize"]');
  if (!toggle || !menu || !themeBtn || !motionBtn || !maximizeBtn || !themeLabel || !motionLabel || !maximizeLabel || !themeIcon || !maximizeIcon) return;

  const getThemeMode = () => root.dataset.theme === 'day' ? 'day' : 'night';
  const applyTheme = (theme) => {
    const mode = theme === 'day' ? 'day' : 'night';
    root.dataset.theme = mode;
    if (document.body) {
      document.body.dataset.theme = mode;
    }
    safeStorage.set(storageKeys.theme, mode);
  };
  const isMotionPreferredEnabled = () => root.dataset.bgMotion !== '0';
  const isMaximized = () => root.dataset.maximized === '1';
  const isMobileView = () => Boolean(mobileMotionQuery && mobileMotionQuery.matches);
  const isMotionLocked = () => isMaximized() || isMobileView();
  const applyMotion = (enabled) => {
    root.dataset.bgMotion = enabled ? '1' : '0';
    safeStorage.set(storageKeys.bgMotion, enabled ? '1' : '0');
  };
  const applyMaximized = (enabled) => {
    root.dataset.maximized = enabled ? '1' : '0';
    safeStorage.set(storageKeys.maximized, enabled ? '1' : '0');
  };

  const updateLabels = () => {
    const themeMode = getThemeMode();
    themeLabel.textContent = themeMode === 'day' ? 'Switch to Dark Mode' : 'Switch to Light Mode';
    themeIcon.src = themeMode === 'day' ? '/icons/sun.svg' : '/icons/moon.svg';
    const motionPreferredEnabled = isMotionPreferredEnabled();
    const motionLocked = isMotionLocked();
    motionLabel.textContent = motionPreferredEnabled ? 'Disable Starfield' : 'Enable Starfield';
    motionBtn.disabled = motionLocked;
    motionBtn.setAttribute('aria-disabled', motionLocked ? 'true' : 'false');
    motionBtn.title = motionLocked
      ? 'Starfield is disabled in mobile and maximized views.'
      : '';
    maximizeLabel.textContent = isMaximized() ? 'Minimise window' : 'Maximize window';
    maximizeIcon.innerHTML = isMaximized() ? minimizeIconSvg : maximizeIconSvg;
  };

  const openMenu = () => {
    toggle.setAttribute('aria-expanded', 'true');
    menu.hidden = false;
  };
  const closeMenu = () => {
    toggle.setAttribute('aria-expanded', 'false');
    menu.hidden = true;
  };

  const storedMaximized = safeStorage.get(storageKeys.maximized) === '1';
  if (storedMaximized) applyMaximized(true);
  updateLabels();

  toggle.addEventListener('click', (event) => {
    event.preventDefault();
    event.stopPropagation();
    if (menu.hidden) openMenu();
    else closeMenu();
  });

  themeBtn.addEventListener('click', (event) => {
    event.preventDefault();
    applyTheme(getThemeMode() === 'day' ? 'night' : 'day');
    updateLabels();
    closeMenu();
  });

  motionBtn.addEventListener('click', (event) => {
    event.preventDefault();
    if (isMotionLocked()) return;
    applyMotion(!isMotionPreferredEnabled());
    updateLabels();
    closeMenu();
  });

  maximizeBtn.addEventListener('click', (event) => {
    event.preventDefault();
    const next = !isMaximized();
    applyMaximized(next);
    updateLabels();
    closeMenu();
  });

  wrapper.addEventListener('click', (event) => {
    event.stopPropagation();
  });

  document.addEventListener('click', () => {
    closeMenu();
  });

  document.addEventListener('keydown', (event) => {
    if (event.key !== 'Escape') return;
    closeMenu();
  });

  if (mobileMotionQuery) {
    if (typeof mobileMotionQuery.addEventListener === 'function') {
      mobileMotionQuery.addEventListener('change', updateLabels);
    } else if (typeof mobileMotionQuery.addListener === 'function') {
      mobileMotionQuery.addListener(updateLabels);
    }
  }
  window.addEventListener('resize', updateLabels, { passive: true });
})();
