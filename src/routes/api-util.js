export function registerApiUtil(app, ctx) {
  const {
    requireUser,
    requireSettingsAdmin,
    loadConfig,
    saveConfig,
    canAccessDashboardApp,
    getEffectiveRole,
    getActualRole,
    pushLog,
    LOG_BUFFER,
    normalizeVersionTag,
    APP_VERSION,
    VERSION_CACHE_TTL_MS,
    buildReleaseNotesUrl,
    loadReleaseHighlights,
    fetchLatestDockerTag,
    resolveRoleSwitchRedirectPath,
    resolveOnboardingSettings,
    hasActiveOnboardingApps,
    shouldShowQuickStartOnboarding,
  } = ctx;

  app.get('/healthz', (req, res) => {
    res.json({ status: 'ok' });
  });

  app.get('/api/logs', requireUser, (req, res) => {
    const appId = String(req.query?.appId || '').trim().toLowerCase();
    const level = String(req.query?.level || '').trim().toLowerCase();
    const limitValue = Number(req.query?.limit || 120);
    const limit = Number.isFinite(limitValue) ? Math.max(1, Math.min(250, limitValue)) : 120;
    const list = LOG_BUFFER
      .filter((entry) => !appId || entry.app === appId)
      .filter((entry) => !level || entry.level === level)
      .slice(-limit);
    res.json({ items: list });
  });

  app.post('/api/logs/client', requireUser, (req, res) => {
    const appId = String(req.body?.app || '').trim().toLowerCase();
    const level = String(req.body?.level || '').trim().toLowerCase() || 'info';
    const action = String(req.body?.action || '').trim() || 'event';
    const message = String(req.body?.message || '').trim();
    const meta = req.body?.meta || null;
    if (!appId) return res.status(400).json({ error: 'Missing app id.' });

    const config = loadConfig();
    const apps = config.apps || [];
    const appItem = apps.find((item) => String(item.id || '').toLowerCase() === appId);
    if (!appItem) return res.status(404).json({ error: 'Unknown app.' });
    if (!canAccessDashboardApp(config, appItem, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Access denied.' });
    }

    pushLog({
      level,
      app: appId,
      action,
      message,
      meta,
    });

    res.json({ ok: true });
  });

  const switchViewHandler = (req, res) => {
    const actualRole = getActualRole(req);
    if (actualRole !== 'admin') {
      return res.status(403).send('Admin access required.');
    }
    const desired = String(req.body?.role ?? req.query?.role ?? '').trim().toLowerCase();
    const allowedViewRoles = new Set(['guest', 'user', 'co-admin', 'admin']);
    req.session.viewRole = allowedViewRoles.has(desired) && desired !== 'admin'
      ? desired
      : null;
    const targetRole = getEffectiveRole(req);
    const config = loadConfig();
    const targetPath = resolveRoleSwitchRedirectPath(req, targetRole, { config });
    res.redirect(targetPath);
  };

  app.post('/switch-view', requireUser, switchViewHandler);
  app.get('/switch-view', requireUser, (_req, res) => {
    return res.status(405).send('Method Not Allowed. Use POST /switch-view.');
  });

  app.get('/api/onboarding/quick-start', requireSettingsAdmin, (req, res) => {
    const config = loadConfig();
    const onboarding = resolveOnboardingSettings(config);
    const hasActiveApps = hasActiveOnboardingApps(config);
    if (onboarding.quickStartPending && hasActiveApps) {
      return res.json({ show: false, steps: [], actions: {} });
    }
    const show = shouldShowQuickStartOnboarding(config);
    return res.json({
      show,
      steps: [
        'Go to Settings -> Custom -> Sidebar and restore the default apps you want to use.',
        'Go to Settings -> Apps, configure each app URL/API details, and save.',
        'Go to Settings -> Custom -> Dashboard, add the cards you want for Dashboard and Overview, set role visibility, and adjust order as needed.',
        'Open Dashboard or Overview to review the layout and fine-tune anything you want to change.',
      ],
      actions: {
        sidebar: '/settings?tab=custom&settingsCustomTab=sidebar',
      },
    });
  });

  app.post('/api/onboarding/quick-start/dismiss', requireSettingsAdmin, (req, res) => {
    const config = loadConfig();
    const onboarding = resolveOnboardingSettings(config);
    if (!onboarding.quickStartPending) {
      return res.json({ ok: true, show: false });
    }
    const source = (config && typeof config.onboarding === 'object') ? config.onboarding : {};
    saveConfig({
      ...config,
      onboarding: {
        ...source,
        quickStartPending: false,
      },
    });
    return res.json({ ok: true, show: false });
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
      const payload = {
        current,
        latest,
        upToDate: Boolean(current && latest && current === latest),
        releaseNotesUrl,
        highlights,
      };
      ctx.versionCache = { fetchedAt: now, payload };
      return res.json(payload);
    } catch (err) {
      const payload = {
        current,
        latest: '',
        upToDate: true,
        releaseNotesUrl,
        highlights,
      };
      ctx.versionCache = { fetchedAt: now, payload };
      return res.json(payload);
    }
  });
}
