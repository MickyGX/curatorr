export function registerApiPlex(app, ctx) {
  const {
    requireUser,
    requireAdmin,
    loadConfig,
    canAccessDashboardApp,
    getEffectiveRole,
    pushLog,
    safeMessage,
    fetchPlexResources,
    resolvePlexServerToken,
    resolvePlexMachineIdentifier,
    normalizeBaseUrl,
    resolveLaunchUrl,
    uniqueList,
    getPlexDiscoveryWatchlisted,
    resolvePlexDiscoverRatingKey,
    fetchPlexDiscoveryMetadata,
    fetchPlexDiscoveryActions,
    buildWatchlistStateFromActions,
    updatePlexWatchlist,
    fetchPlexWatchlistState,
  } = ctx;

  app.get('/api/plex/token', requireAdmin, (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const plexApp = apps.find((appItem) => appItem.id === 'plex');
    const sessionToken = String(req.session?.authToken || '').trim();
    const sessionServerToken = String(req.session?.plexServerToken || '').trim();
    const fallbackToken = String(plexApp?.plexToken || '').trim();
    const token = sessionServerToken || fallbackToken || sessionToken;
    if (!token) return res.status(400).json({ error: 'Missing Plex token.' });

    (async () => {
      if (sessionServerToken) return { token: sessionServerToken };
      if (!sessionToken) return { token };
      try {
        const resources = await fetchPlexResources(sessionToken);
        const serverToken = resolvePlexServerToken(resources, {
          machineId: String(plexApp?.plexMachine || '').trim(),
          localUrl: plexApp?.localUrl,
          remoteUrl: plexApp?.remoteUrl,
          plexHost: plexApp?.plexHost,
        });
        if (serverToken) return { token: serverToken };
        pushLog({
          level: 'error',
          app: 'plex',
          action: 'token.resolve',
          message: 'Plex server token could not be resolved.',
          meta: {
            machineId: String(plexApp?.plexMachine || '').trim(),
            localUrl: plexApp?.localUrl || '',
            remoteUrl: plexApp?.remoteUrl || '',
          },
        });
        return { error: 'Unable to resolve Plex server token. Set Plex Machine/URL and try again.' };
      } catch (err) {
        pushLog({
          level: 'error',
          app: 'plex',
          action: 'token.resolve',
          message: safeMessage(err) || 'Plex server token lookup failed.',
        });
        return { error: 'Plex server token lookup failed.' };
      }
    })()
      .then((payload) => res.json(payload))
      .catch(() => res.json({ error: 'Plex server token lookup failed.' }));
  });

  app.get('/api/plex/machine', requireAdmin, async (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const plexApp = apps.find((appItem) => appItem.id === 'plex');
    if (!plexApp) return res.status(404).json({ error: 'Plex app is not configured.' });
    const token = String(req.session?.authToken || plexApp.plexToken || '').trim();
    if (!token) return res.status(400).json({ error: 'Missing Plex token.' });

    const candidates = uniqueList([
      normalizeBaseUrl(plexApp.remoteUrl || '', { stripWeb: true }),
      normalizeBaseUrl(resolveLaunchUrl(plexApp, req), { stripWeb: true }),
      normalizeBaseUrl(plexApp.localUrl || '', { stripWeb: true }),
      normalizeBaseUrl(plexApp.url || '', { stripWeb: true }),
    ]);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Plex URL.' });

    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      try {
        const machineId = await resolvePlexMachineIdentifier({ baseUrl, token });
        if (machineId) return res.json({ machineId });
        lastError = 'Unable to resolve Plex machine identifier.';
      } catch (err) {
        lastError = safeMessage(err) || 'Failed to reach Plex.';
      }
    }

    return res.status(502).json({ error: lastError || 'Failed to reach Plex.' });
  });

  app.get('/api/plex/discovery/watchlisted', requireUser, async (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const plexApp = apps.find((appItem) => appItem.id === 'plex');
    if (!plexApp) return res.status(404).json({ error: 'Plex app is not configured.' });
    if (!canAccessDashboardApp(config, plexApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Plex dashboard access denied.' });
    }

    try {
      const payload = await getPlexDiscoveryWatchlisted();
      pushLog({
        level: 'info',
        app: 'plex',
        action: 'discovery.watchlisted',
        message: 'Plex discovery watchlisted fetched.',
        meta: { cached: Boolean(payload?.cached), count: Array.isArray(payload?.items) ? payload.items.length : 0 },
      });
      return res.json(payload);
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'plex',
        action: 'discovery.watchlisted',
        message: safeMessage(err) || 'Failed to fetch Plex discovery items.',
      });
      return res.status(502).json({ error: safeMessage(err) || 'Failed to fetch Plex discovery items.' });
    }
  });

  app.get('/api/plex/discovery/details', requireUser, async (req, res) => {
    const ratingKey = String(req.query.ratingKey || '').trim();
    const kind = String(req.query.kind || '').trim().toLowerCase();
    const slug = String(req.query.slug || '').trim();
    if (!ratingKey && (!slug || (kind !== 'movie' && kind !== 'tv'))) {
      return res.status(400).json({ error: 'Missing metadata identifier.' });
    }

    const config = loadConfig();
    const apps = config.apps || [];
    const plexApp = apps.find((appItem) => appItem.id === 'plex');
    if (!plexApp) return res.status(404).json({ error: 'Plex app is not configured.' });
    if (!canAccessDashboardApp(config, plexApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Plex dashboard access denied.' });
    }

    const token = String(req.session?.authToken || plexApp.plexToken || '').trim();
    if (!token) return res.status(400).json({ error: 'Missing Plex token.' });

    try {
      const resolvedRatingKey = ratingKey || await resolvePlexDiscoverRatingKey({ kind, slug, token });
      const metadata = resolvedRatingKey
        ? await fetchPlexDiscoveryMetadata(resolvedRatingKey, token)
        : { summary: '', studio: '', contentRating: '', tagline: '', year: '' };
      let watchlist = { allowed: false };
      if (slug && (kind === 'movie' || kind === 'tv')) {
        const actions = await fetchPlexDiscoveryActions({ kind, slug, token });
        watchlist = buildWatchlistStateFromActions(actions);
      }
      pushLog({
        level: 'info',
        app: 'plex',
        action: 'discovery.details',
        message: 'Plex discovery details fetched.',
        meta: { ratingKey: resolvedRatingKey || '', kind: kind || '', hasSlug: Boolean(slug) },
      });
      return res.json({ ...metadata, ratingKey: resolvedRatingKey || '', watchlist });
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'plex',
        action: 'discovery.details',
        message: safeMessage(err) || 'Failed to fetch details.',
      });
      return res.status(502).json({ error: safeMessage(err) || 'Failed to fetch details.' });
    }
  });

  app.post('/api/plex/discovery/watchlist', requireUser, async (req, res) => {
    const kind = String(req.body?.kind || '').trim().toLowerCase();
    const slug = String(req.body?.slug || '').trim();
    const action = String(req.body?.action || '').trim().toLowerCase();
    if (!slug || (kind !== 'movie' && kind !== 'tv')) {
      return res.status(400).json({ error: 'Missing item identifier.' });
    }
    if (action !== 'add' && action !== 'remove') {
      return res.status(400).json({ error: 'Invalid watchlist action.' });
    }

    const token = String(req.session?.authToken || '').trim();
    if (!token) return res.status(401).json({ error: 'You must sign in with Plex to update watchlist.' });

    try {
      await updatePlexWatchlist({ kind, slug, action, token });
      const watchlist = await fetchPlexWatchlistState({ kind, slug, token });
      pushLog({
        level: 'info',
        app: 'plex',
        action: 'discovery.watchlist',
        message: `Plex watchlist ${action}.`,
        meta: { kind, slug },
      });
      return res.json({ ok: true, watchlist });
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'plex',
        action: 'discovery.watchlist',
        message: safeMessage(err) || 'Failed to update watchlist.',
      });
      return res.status(502).json({ error: safeMessage(err) || 'Failed to update watchlist.' });
    }
  });
}
