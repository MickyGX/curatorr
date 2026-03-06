(() => {
  'use strict';

  const config = window.WIZARR_OVERVIEW_CONFIG || {};
  const appId = String(config.appId || 'wizarr').trim() || 'wizarr';
  const appName = String(config.appName || 'Wizarr').trim() || 'Wizarr';

  const usersBody = document.getElementById(appId + 'UsersBody');
  const usersStatusFilter = document.getElementById(appId + 'UsersStatusFilter');
  const usersLimitFilter = document.getElementById(appId + 'UsersLimitFilter');

  const invBody = document.getElementById(appId + 'InvitationsBody');
  const invStatusFilter = document.getElementById(appId + 'InvitationsStatusFilter');
  const invLimitFilter = document.getElementById(appId + 'InvitationsLimitFilter');

  const hasUsers = Boolean(usersBody);
  const hasInvitations = Boolean(invBody);

  if (!hasUsers && !hasInvitations) return;

  const CACHE_TTL_MS = 2 * 60 * 1000;
  let cachedUsers = null;
  let cachedInvitations = null;
  let cacheTs = 0;

  function escapeHtml(value) {
    return String(value || '').replace(/[&<>"']/g, (char) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[char]
    ));
  }

  function formatDate(value) {
    if (!value) return '\u2014';
    const ts = new Date(value).getTime();
    if (!Number.isFinite(ts)) return '\u2014';
    const d = new Date(ts);
    return (d.getMonth() + 1) + '/' + d.getDate() + '/' + d.getFullYear();
  }

  function isPast(value) {
    if (!value) return false;
    const ts = new Date(value).getTime();
    return Number.isFinite(ts) && ts < Date.now();
  }

  function normalizeUserStatus(user) {
    if (user.disabled === true) return 'disabled';
    const expires = user.expires || user.expiry || '';
    if (expires && isPast(expires)) return 'expired';
    return 'active';
  }

  function normalizeInvitationStatus(inv) {
    const expires = inv.expires || inv.expiry || '';
    if (expires && isPast(expires)) return 'expired';
    const unlimited = Boolean(inv.unlimited);
    if (!unlimited) {
      const used = Number(inv.times_used ?? inv.used_count ?? inv.used ?? 0);
      const max = Number(inv.duration_ms ? null : (inv.sessions ?? inv.max_uses ?? null)) ?? null;
      if (max !== null && used >= max) return 'used';
    }
    return 'active';
  }

  function renderUsersTable(users) {
    if (!usersBody) return;
    const statusValue = String(usersStatusFilter?.value || 'all').toLowerCase();
    const limit = Math.max(1, Number(usersLimitFilter?.value) || 10);

    let filtered = users;
    if (statusValue !== 'all') {
      filtered = filtered.filter((user) => normalizeUserStatus(user) === statusValue);
    }
    const page = filtered.slice(0, limit);

    if (!page.length) {
      usersBody.innerHTML = '<div class="queue-empty">No users found.</div>';
      return;
    }

    usersBody.innerHTML = page.map((user) => {
      const email = escapeHtml(user.email || user.username || user.name || '\u2014');
      const role = user.admin || user.is_admin || user.role === 'admin' ? 'Admin' : 'Member';
      const created = formatDate(user.created || user.date_created || user.createdAt || user.created_at || '');
      const expiresRaw = user.expires || user.expiry || '';
      const expires = expiresRaw ? formatDate(expiresRaw) : 'Never';
      const status = normalizeUserStatus(user);
      const statusClass = status === 'active' ? 'queue-status--ok' : 'queue-status--bad';

      return '<div class="queue-row">' +
        '<div class="queue-col-title">' + email + '</div>' +
        '<div class="queue-col-detail">' + escapeHtml(role) + '</div>' +
        '<div class="queue-col-size">' + escapeHtml(created) + '</div>' +
        '<div class="queue-col-time"><span class="' + statusClass + '">' + escapeHtml(expires) + '</span></div>' +
        '</div>';
    }).join('');
  }

  function renderInvitationsTable(invitations) {
    if (!invBody) return;
    const statusValue = String(invStatusFilter?.value || 'all').toLowerCase();
    const limit = Math.max(1, Number(invLimitFilter?.value) || 10);

    let filtered = invitations;
    if (statusValue !== 'all') {
      filtered = filtered.filter((inv) => normalizeInvitationStatus(inv) === statusValue);
    }
    const page = filtered.slice(0, limit);

    if (!page.length) {
      invBody.innerHTML = '<div class="queue-empty">No invitations found.</div>';
      return;
    }

    invBody.innerHTML = page.map((inv) => {
      const code = escapeHtml(inv.code || inv.token || inv.id || '\u2014');
      const unlimited = Boolean(inv.unlimited);
      const usedCount = Number(inv.times_used ?? inv.used_count ?? inv.used ?? 0);
      const usesDisplay = unlimited ? 'Unlimited' : String(usedCount);
      const expiresRaw = inv.expires || inv.expiry || '';
      const expires = expiresRaw ? formatDate(expiresRaw) : 'Never';
      const created = formatDate(inv.created || inv.date_created || inv.createdAt || inv.created_at || '');
      const status = normalizeInvitationStatus(inv);
      const statusClass = status === 'active' ? 'queue-status--ok' : 'queue-status--bad';

      return '<div class="queue-row">' +
        '<div class="queue-col-title">' + code + '</div>' +
        '<div class="queue-col-detail">' + escapeHtml(usesDisplay) + '</div>' +
        '<div class="queue-col-protocol"><span class="' + statusClass + '">' + escapeHtml(expires) + '</span></div>' +
        '<div class="queue-col-time">' + escapeHtml(created) + '</div>' +
        '</div>';
    }).join('');
  }

  function applyUsersFilters() {
    if (cachedUsers) renderUsersTable(cachedUsers);
  }

  function applyInvitationsFilters() {
    if (cachedInvitations) renderInvitationsTable(cachedInvitations);
  }

  async function loadOverview() {
    const now = Date.now();
    if (cachedUsers !== null && cachedInvitations !== null && (now - cacheTs) < CACHE_TTL_MS) {
      applyUsersFilters();
      applyInvitationsFilters();
      return;
    }

    if (usersBody) usersBody.innerHTML = '<div class="queue-empty">Loading\u2026</div>';
    if (invBody) invBody.innerHTML = '<div class="queue-empty">Loading\u2026</div>';

    try {
      const response = await fetch('/api/wizarr/overview', {
        headers: { Accept: 'application/json' },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        const errMsg = 'Unable to load ' + escapeHtml(appName) + ' data.';
        if (usersBody) usersBody.innerHTML = '<div class="queue-empty">' + errMsg + '</div>';
        if (invBody) invBody.innerHTML = '<div class="queue-empty">' + errMsg + '</div>';
        return;
      }
      cachedUsers = Array.isArray(payload?.users) ? payload.users : [];
      cachedInvitations = Array.isArray(payload?.invitations) ? payload.invitations : [];
      cacheTs = Date.now();
      applyUsersFilters();
      applyInvitationsFilters();
    } catch (_err) {
      const errMsg = 'Unable to load ' + escapeHtml(appName) + ' data.';
      if (usersBody) usersBody.innerHTML = '<div class="queue-empty">' + errMsg + '</div>';
      if (invBody) invBody.innerHTML = '<div class="queue-empty">' + errMsg + '</div>';
    }
  }

  usersStatusFilter?.addEventListener('change', applyUsersFilters);
  usersLimitFilter?.addEventListener('change', applyUsersFilters);
  invStatusFilter?.addEventListener('change', applyInvitationsFilters);
  invLimitFilter?.addEventListener('change', applyInvitationsFilters);

  loadOverview();
})();
