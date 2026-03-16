(function () {
  var menuStates = Array.prototype.slice.call(document.querySelectorAll('[data-cur-filter-menu]'))
    .map(function (menu) {
      return {
        menu: menu,
        toggle: menu.querySelector('[data-cur-filter-toggle]'),
        popover: menu.querySelector('[data-cur-filter-popover]'),
      };
    })
    .filter(function (state) {
      return state.toggle && state.popover;
    });
  if (!menuStates.length) return;

  function closeMenu(state) {
    if (!state || !state.toggle || !state.popover) return;
    state.popover.hidden = true;
    state.toggle.setAttribute('aria-expanded', 'false');
  }

  function openMenu(state) {
    if (!state || !state.toggle || !state.popover) return;
    menuStates.forEach(function (entry) {
      if (entry !== state) closeMenu(entry);
    });

    state.popover.hidden = false;
    state.toggle.setAttribute('aria-expanded', 'true');

    var autoFocusTarget = state.popover.querySelector('[data-cur-filter-autofocus]');
    if (autoFocusTarget && typeof autoFocusTarget.focus === 'function') {
      autoFocusTarget.focus({ preventScroll: true });
    }
  }

  menuStates.forEach(function (state) {
    state.toggle.addEventListener('click', function (event) {
      event.preventDefault();
      event.stopPropagation();
      if (state.popover.hidden) openMenu(state);
      else closeMenu(state);
    });

    state.popover.addEventListener('click', function (event) {
      event.stopPropagation();
    });

    Array.prototype.slice.call(state.popover.querySelectorAll('[data-cur-filter-close]')).forEach(function (button) {
      button.addEventListener('click', function () {
        closeMenu(state);
      });
    });
  });

  document.addEventListener('click', function () {
    menuStates.forEach(closeMenu);
  });

  document.addEventListener('keydown', function (event) {
    if (event.key !== 'Escape') return;
    menuStates.forEach(closeMenu);
  });
})();
