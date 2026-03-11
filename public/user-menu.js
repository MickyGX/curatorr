(function () {
  const userMenus = document.querySelectorAll('[data-user-menu]');
  if (!userMenus.length) return;

  function closeAll(except) {
    userMenus.forEach(function (pill) {
      if (pill === except) return;
      pill.querySelectorAll('[data-user-role-picker][open]').forEach(function (picker) {
        picker.open = false;
      });
      pill.setAttribute('aria-expanded', 'false');
      pill.classList.remove('user-pill--open');
    });
  }

  userMenus.forEach(function (pill) {
    pill.addEventListener('click', function (e) {
      if (e.target.closest('.user-menu')) return;
      e.stopPropagation();
      e.preventDefault();
      const isOpen = pill.classList.contains('user-pill--open');
      closeAll(pill);
      if (!isOpen) {
        pill.setAttribute('aria-expanded', 'true');
        pill.classList.add('user-pill--open');
      } else {
        pill.querySelectorAll('[data-user-role-picker][open]').forEach(function (picker) {
          picker.open = false;
        });
        pill.setAttribute('aria-expanded', 'false');
        pill.classList.remove('user-pill--open');
      }
    });

    pill.addEventListener('keydown', function (e) {
      if (e.target.closest('.user-menu')) return;
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
