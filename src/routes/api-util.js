export function registerApiUtil(app, ctx) {
  const {
    requireUser,
    loadConfig,
    pushLog,
    LOG_BUFFER,
    normalizeVersionTag,
    APP_VERSION,
    VERSION_CACHE_TTL_MS,
    buildReleaseNotesUrl,
    loadReleaseHighlights,
    fetchLatestDockerTag,
    resolveRoleSwitchRedirectPath,
    getActualRole,
    getEffectiveRole,
    isLocalAdminDataViewer,
    setPreviewUserId,
    db,
    normalizeThemeSettings,
    serializeUserThemePreferences,
    saveConfig,
  } = ctx;

  app.get('/healthz', (_req, res) => {
    res.json({ status: 'ok', app: 'curatorr', version: APP_VERSION });
  });

  app.get('/api/logs', requireUser, (req, res) => {
    const appId = String(req.query?.appId || '').trim().toLowerCase();
    const level = String(req.query?.level || '').trim().toLowerCase();
    const limitValue = Number(req.query?.limit || 120);
    const limit = Number.isFinite(limitValue) ? Math.max(1, Math.min(250, limitValue)) : 120;
    const list = LOG_BUFFER
      .filter((entry) => !appId || String(entry.app || '').toLowerCase() === appId)
      .filter((entry) => !level || entry.level === level)
      .slice(-limit);
    res.json({ items: list });
  });

  app.post('/api/logs/client', requireUser, (req, res) => {
    const level = String(req.body?.level || 'info').trim().toLowerCase() || 'info';
    const action = String(req.body?.action || '').trim() || 'event';
    const message = String(req.body?.message || '').trim();
    if (!message) return res.status(400).json({ error: 'Missing message.' });
    pushLog({ level, app: 'client', action, message, meta: req.body?.meta || null });
    return res.json({ ok: true });
  });

  app.post('/switch-view', requireUser, (req, res) => {
    const actualRole = getActualRole(req);
    if (actualRole !== 'admin') return res.status(403).send('Admin access required.');
    const desired = String(req.body?.role ?? req.query?.role ?? '').trim().toLowerCase();
    const allowedViewRoles = new Set(['guest', 'user', 'power-user', 'co-admin', 'admin']);
    req.session.viewRole = allowedViewRoles.has(desired) && desired !== 'admin' ? desired : null;
    const targetPath = resolveRoleSwitchRedirectPath(req, getEffectiveRole(req), {});
    return res.redirect(targetPath);
  });

  app.get('/switch-view', requireUser, (_req, res) => res.status(405).send('Method Not Allowed. Use POST.'));

  app.post('/admin/preview-user', requireUser, (req, res) => {
    if (!isLocalAdminDataViewer(req)) return res.status(403).send('Admin preview access required.');
    const requestedUserId = String(req.body?.previewUserId || '').trim();
    if (!requestedUserId) {
      setPreviewUserId(req, '');
    } else {
      const exists = db.prepare(`
        SELECT 1
        FROM play_events
        WHERE TRIM(COALESCE(user_plex_id, '')) = ?
        LIMIT 1
      `).get(requestedUserId);
      // For Jellyfin/Emby, users may not have any play history yet — accept them anyway.
      const msType = String(loadConfig()?.mediaServer?.type || 'plex').trim().toLowerCase();
      const isNonPlex = msType === 'jellyfin' || msType === 'emby';
      setPreviewUserId(req, (exists || isNonPlex) ? requestedUserId : '');
      // Resolve canonical ID from the map cached during page load (avoids a Plex API round-trip).
      const idMap = req.session?.previewUserMap || {};
      req.session.previewCanonicalId = idMap[requestedUserId] || requestedUserId || null;
    }
    const nextPath = String(req.body?.next || req.headers?.referer || '/dashboard').trim();
    return res.redirect(nextPath.startsWith('/') ? nextPath : '/dashboard');
  });

  app.get('/api/version', requireUser, async (_req, res) => {
    const current = normalizeVersionTag(APP_VERSION || '');
    const releaseNotesUrl = buildReleaseNotesUrl(current);
    const highlights = loadReleaseHighlights(current);
    const now = Date.now();
    if (ctx.versionCache.payload && (now - ctx.versionCache.fetchedAt) < VERSION_CACHE_TTL_MS) {
      return res.json({ ...ctx.versionCache.payload, current, releaseNotesUrl, highlights });
    }
    try {
      const latest = await fetchLatestDockerTag();
      const payload = { current, latest, upToDate: Boolean(current && latest && current === latest), releaseNotesUrl, highlights };
      ctx.versionCache = { fetchedAt: now, payload };
      return res.json(payload);
    } catch (err) {
      const payload = { current, latest: '', upToDate: true, releaseNotesUrl, highlights };
      ctx.versionCache = { fetchedAt: now, payload };
      return res.json(payload);
    }
  });

  // Per-user theme preference
  app.post('/api/theme', requireUser, (req, res) => {
    const config = loadConfig();
    const settings = normalizeThemeSettings(req.body || {});
    const updated = serializeUserThemePreferences(config, req.session.user, settings);
    saveConfig(updated);
    return res.json({ ok: true });
  });
}
