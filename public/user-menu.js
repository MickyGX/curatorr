(function () {
  const userMenus = document.querySelectorAll('[data-user-menu]');
  if (!userMenus.length) return;

  function closeAll() {
    userMenus.forEach(function (pill) {
      pill.setAttribute('aria-expanded', 'false');
      pill.classList.remove('user-pill--open');
    });
  }

  userMenus.forEach(function (pill) {
    pill.addEventListener('click', function (e) {
      e.stopPropagation();
      const isOpen = pill.classList.contains('user-pill--open');
      closeAll();
      if (!isOpen) {
        pill.setAttribute('aria-expanded', 'true');
        pill.classList.add('user-pill--open');
      }
    });

    pill.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        pill.click();
      }
      if (e.key === 'Escape') {
        closeAll();
      }
    });
  });

  document.addEventListener('click', function (e) {
    if (!e.target.closest('[data-user-menu]')) closeAll();
  });

  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') closeAll();
  });
})();
