(function () {
  const dashFrame = document.querySelector('.dash-frame');
  const toggle = document.querySelector('.sidebar-toggle');
  const mobileQuery = window.matchMedia('(max-width: 980px)');
  const isMobile = () => mobileQuery.matches;
  const safeGet = (k) => { try { return localStorage.getItem(k); } catch (_) { return null; } };
  const safeSet = (k, v) => { try { localStorage.setItem(k, v); } catch (_) {} };

  function applySidebarState() {
    if (!dashFrame) return;
    if (isMobile()) {
      dashFrame.classList.remove('sidebar-collapsed', 'mobile-nav-open');
      document.body.classList.remove('mobile-nav-open');
    } else {
      const collapsed = safeGet('curatorr-sidebar-collapsed') === 'true';
      dashFrame.classList.toggle('sidebar-collapsed', collapsed);
      dashFrame.classList.remove('mobile-nav-open');
      document.body.classList.remove('mobile-nav-open');
    }
  }

  applySidebarState();
  mobileQuery.addEventListener('change', applySidebarState);

  if (toggle) {
    toggle.addEventListener('click', function (e) {
      e.preventDefault();
      e.stopPropagation();
      if (isMobile()) {
        const open = dashFrame.classList.toggle('mobile-nav-open');
        document.body.classList.toggle('mobile-nav-open', open);
      } else {
        const collapsed = dashFrame.classList.toggle('sidebar-collapsed');
        safeSet('curatorr-sidebar-collapsed', collapsed ? 'true' : 'false');
      }
    });
  }

  document.addEventListener('click', function (e) {
    if (!isMobile()) return;
    if (!dashFrame || !dashFrame.classList.contains('mobile-nav-open')) return;
    const sidebar = document.querySelector('.dash-sidebar');
    if (sidebar && sidebar.contains(e.target)) return;
    if (toggle && toggle.contains(e.target)) return;
    dashFrame.classList.remove('mobile-nav-open');
    document.body.classList.remove('mobile-nav-open');
  });

  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Escape') return;
    if (isMobile()) {
      dashFrame && dashFrame.classList.remove('mobile-nav-open');
      document.body.classList.remove('mobile-nav-open');
    }
  });
})();
