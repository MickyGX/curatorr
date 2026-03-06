export function registerApiMedia(app, ctx) {
  const {
    requireUser,
    loadConfig,
    canAccessDashboardApp,
    getEffectiveRole,
    pushLog,
    safeMessage,
    buildAppApiUrl,
    resolveJellyfinCandidates,
    resolveEmbyCandidates,
    buildJellyfinImageUrl,
    buildEmbyImageUrl,
    formatDurationFromTicks,
    formatRelativeTime,
    toPaddedEpisode,
    mapJellyfinKind,
    fetchJellyfinJson,
    fetchJellyfinRecentItems,
    fetchEmbyJson,
    fetchEmbyRecentItems,
    mapSeerrRequestStatus,
    mapSeerrFilter,
    fetchSeerrJson,
    resolveRequestApiCandidates,
  } = ctx;

  app.get('/api/jellyfin/active', requireUser, async (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const jellyfinApp = apps.find((appItem) => appItem.id === 'jellyfin');
    if (!jellyfinApp) return res.status(404).json({ error: 'Jellyfin app is not configured.' });
    if (!canAccessDashboardApp(config, jellyfinApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Jellyfin dashboard access denied.' });
    }

    const apiKey = String(jellyfinApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Jellyfin API key.' });

    const candidates = resolveJellyfinCandidates(jellyfinApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Jellyfin URL.' });

    try {
      const sessionResponse = await fetchJellyfinJson({
        candidates,
        apiKey,
        path: '/Sessions',
        query: { ActiveWithinSeconds: 21600 },
      });
      const sessions = Array.isArray(sessionResponse.payload) ? sessionResponse.payload : [];
      const items = sessions
        .filter((session) => session && session.NowPlayingItem && session.NowPlayingItem.Id)
        .map((session) => {
          const media = session.NowPlayingItem || {};
          const kind = mapJellyfinKind(media.Type);
          const seriesName = String(media.SeriesName || '').trim();
          const season = toPaddedEpisode(media.ParentIndexNumber);
          const episode = toPaddedEpisode(media.IndexNumber);
          const episodeCode = season && episode ? `S${season}E${episode}` : '';
          const subtitle = kind === 'tv'
            ? [seriesName || String(media.Name || '').trim(), episodeCode].filter(Boolean).join(' ')
            : '';
          const runtime = formatDurationFromTicks(media.RunTimeTicks);
          const user = String(session.UserName || '').trim();
          const device = String(session.Client || session.DeviceName || '').trim();
          const playState = session.PlayState || {};
          const progress = Number(media.RunTimeTicks) > 0
            ? Math.max(0, Math.min(100, Math.round((Number(playState.PositionTicks || 0) / Number(media.RunTimeTicks)) * 100)))
            : 0;
          const stateLabel = playState.IsPaused ? 'Paused' : 'Playing';
          const meta = [runtime, device].filter(Boolean).join(' • ');
          const pill = progress > 0 ? `${stateLabel} ${progress}%` : stateLabel;
          const primaryTag = String(media.PrimaryImageTag || '').trim();
          const backdropTag = Array.isArray(media.BackdropImageTags) && media.BackdropImageTags.length
            ? String(media.BackdropImageTags[0] || '').trim()
            : '';
          return {
            id: String(media.Id || ''),
            title: String(media.Name || '').trim() || 'Now Playing',
            subtitle,
            meta,
            pill,
            kind,
            user,
            overview: String(media.Overview || '').trim(),
            thumb: buildJellyfinImageUrl({
              baseUrl: sessionResponse.baseUrl,
              itemId: media.Id,
              type: 'Primary',
              apiKey,
              tag: primaryTag,
            }),
            art: backdropTag
              ? buildJellyfinImageUrl({
                baseUrl: sessionResponse.baseUrl,
                itemId: media.Id,
                type: 'Backdrop',
                index: '0',
                apiKey,
                tag: backdropTag,
              })
              : '',
          };
        });
      pushLog({
        level: 'info',
        app: 'jellyfin',
        action: 'overview.active',
        message: 'Jellyfin active sessions fetched.',
        meta: { count: items.length },
      });
      return res.json({ items });
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'jellyfin',
        action: 'overview.active',
        message: safeMessage(err) || 'Failed to fetch Jellyfin active sessions.',
      });
      return res.status(502).json({ error: safeMessage(err) || 'Failed to fetch Jellyfin active sessions.' });
    }
  });

  app.get('/api/jellyfin/recent', requireUser, async (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const jellyfinApp = apps.find((appItem) => appItem.id === 'jellyfin');
    if (!jellyfinApp) return res.status(404).json({ error: 'Jellyfin app is not configured.' });
    if (!canAccessDashboardApp(config, jellyfinApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Jellyfin dashboard access denied.' });
    }

    const apiKey = String(jellyfinApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Jellyfin API key.' });

    const candidates = resolveJellyfinCandidates(jellyfinApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Jellyfin URL.' });

    const rawLimit = Number(req.query?.limit);
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(50, rawLimit)) : 20;
    const requestedType = String(req.query?.type || 'movie').trim().toLowerCase();
    const mediaType = requestedType === 'show' || requestedType === 'all' ? requestedType : 'movie';

    try {
      const recentResponse = await fetchJellyfinRecentItems({
        candidates,
        apiKey,
        limit,
        mediaType,
      });
      const items = recentResponse.items.map((media) => {
        const kind = mapJellyfinKind(media.Type);
        const seriesName = String(media.SeriesName || '').trim();
        const season = toPaddedEpisode(media.ParentIndexNumber);
        const episode = toPaddedEpisode(media.IndexNumber);
        const episodeCode = season && episode ? `S${season}E${episode}` : '';
        const subtitle = kind === 'tv'
          ? [seriesName || String(media.Name || '').trim(), episodeCode].filter(Boolean).join(' ')
          : '';
        const runtime = formatDurationFromTicks(media.RunTimeTicks);
        const year = Number(media.ProductionYear);
        const yearText = Number.isFinite(year) && year > 0 ? String(year) : '';
        const meta = [yearText, runtime].filter(Boolean).join(' • ');
        const pill = formatRelativeTime(media.DateCreated) || 'Recently added';
        const primaryTag = String(media.PrimaryImageTag || '').trim();
        const backdropTag = Array.isArray(media.BackdropImageTags) && media.BackdropImageTags.length
          ? String(media.BackdropImageTags[0] || '').trim()
          : '';
        const providerIds = media.ProviderIds && typeof media.ProviderIds === 'object' ? media.ProviderIds : {};
        return {
          id: String(media.Id || ''),
          title: String(media.Name || '').trim() || 'Untitled',
          subtitle,
          meta,
          pill,
          kind,
          overview: String(media.Overview || '').trim(),
          imdbId: String(providerIds.Imdb || providerIds.IMDB || '').trim(),
          tmdbId: String(providerIds.Tmdb || providerIds.TMDB || '').trim(),
          thumb: buildJellyfinImageUrl({
            baseUrl: recentResponse.baseUrl,
            itemId: media.Id,
            type: 'Primary',
            apiKey,
            tag: primaryTag,
          }),
          art: backdropTag
            ? buildJellyfinImageUrl({
              baseUrl: recentResponse.baseUrl,
              itemId: media.Id,
              type: 'Backdrop',
              index: '0',
              apiKey,
              tag: backdropTag,
            })
            : '',
        };
      });
      pushLog({
        level: 'info',
        app: 'jellyfin',
        action: 'overview.recent',
        message: 'Jellyfin recent items fetched.',
        meta: { count: items.length, type: mediaType },
      });
      return res.json({ items });
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'jellyfin',
        action: 'overview.recent',
        message: safeMessage(err) || 'Failed to fetch Jellyfin recent items.',
      });
      return res.status(502).json({ error: safeMessage(err) || 'Failed to fetch Jellyfin recent items.' });
    }
  });

  app.get('/api/emby/active', requireUser, async (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const embyApp = apps.find((appItem) => appItem.id === 'emby');
    if (!embyApp) return res.status(404).json({ error: 'Emby app is not configured.' });
    if (!canAccessDashboardApp(config, embyApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Emby dashboard access denied.' });
    }

    const apiKey = String(embyApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Emby API key.' });

    const candidates = resolveEmbyCandidates(embyApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Emby URL.' });

    try {
      const sessionResponse = await fetchEmbyJson({
        candidates,
        apiKey,
        path: '/Sessions',
        query: { ActiveWithinSeconds: 21600 },
      });
      const sessions = Array.isArray(sessionResponse.payload) ? sessionResponse.payload : [];
      const items = sessions
        .filter((session) => session && session.NowPlayingItem && session.NowPlayingItem.Id)
        .map((session) => {
          const media = session.NowPlayingItem || {};
          const kind = mapJellyfinKind(media.Type);
          const seriesName = String(media.SeriesName || '').trim();
          const season = toPaddedEpisode(media.ParentIndexNumber);
          const episode = toPaddedEpisode(media.IndexNumber);
          const episodeCode = season && episode ? `S${season}E${episode}` : '';
          const subtitle = kind === 'tv'
            ? [seriesName || String(media.Name || '').trim(), episodeCode].filter(Boolean).join(' ')
            : '';
          const runtime = formatDurationFromTicks(media.RunTimeTicks);
          const user = String(session.UserName || '').trim();
          const device = String(session.Client || session.DeviceName || '').trim();
          const playState = session.PlayState || {};
          const progress = Number(media.RunTimeTicks) > 0
            ? Math.max(0, Math.min(100, Math.round((Number(playState.PositionTicks || 0) / Number(media.RunTimeTicks)) * 100)))
            : 0;
          const stateLabel = playState.IsPaused ? 'Paused' : 'Playing';
          const meta = [runtime, device].filter(Boolean).join(' • ');
          const pill = progress > 0 ? `${stateLabel} ${progress}%` : stateLabel;
          const primaryTag = String(media.PrimaryImageTag || '').trim();
          const backdropTag = Array.isArray(media.BackdropImageTags) && media.BackdropImageTags.length
            ? String(media.BackdropImageTags[0] || '').trim()
            : '';
          return {
            id: String(media.Id || ''),
            title: String(media.Name || '').trim() || 'Now Playing',
            subtitle,
            meta,
            pill,
            kind,
            user,
            overview: String(media.Overview || '').trim(),
            thumb: buildEmbyImageUrl({
              baseUrl: sessionResponse.baseUrl,
              itemId: media.Id,
              type: 'Primary',
              apiKey,
              tag: primaryTag,
            }),
            art: backdropTag
              ? buildEmbyImageUrl({
                baseUrl: sessionResponse.baseUrl,
                itemId: media.Id,
                type: 'Backdrop',
                index: '0',
                apiKey,
                tag: backdropTag,
              })
              : '',
          };
        });
      pushLog({
        level: 'info',
        app: 'emby',
        action: 'overview.active',
        message: 'Emby active sessions fetched.',
        meta: { count: items.length },
      });
      return res.json({ items });
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'emby',
        action: 'overview.active',
        message: safeMessage(err) || 'Failed to fetch Emby active sessions.',
      });
      return res.status(502).json({ error: safeMessage(err) || 'Failed to fetch Emby active sessions.' });
    }
  });

  app.get('/api/emby/recent', requireUser, async (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const embyApp = apps.find((appItem) => appItem.id === 'emby');
    if (!embyApp) return res.status(404).json({ error: 'Emby app is not configured.' });
    if (!canAccessDashboardApp(config, embyApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Emby dashboard access denied.' });
    }

    const apiKey = String(embyApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Emby API key.' });

    const candidates = resolveEmbyCandidates(embyApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Emby URL.' });

    const rawLimit = Number(req.query?.limit);
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(50, rawLimit)) : 20;
    const requestedType = String(req.query?.type || 'movie').trim().toLowerCase();
    const mediaType = requestedType === 'show' || requestedType === 'all' ? requestedType : 'movie';

    try {
      const recentResponse = await fetchEmbyRecentItems({
        candidates,
        apiKey,
        limit,
        mediaType,
      });
      const items = recentResponse.items.map((media) => {
        const kind = mapJellyfinKind(media.Type);
        const seriesName = String(media.SeriesName || '').trim();
        const season = toPaddedEpisode(media.ParentIndexNumber);
        const episode = toPaddedEpisode(media.IndexNumber);
        const episodeCode = season && episode ? `S${season}E${episode}` : '';
        const subtitle = kind === 'tv'
          ? [seriesName || String(media.Name || '').trim(), episodeCode].filter(Boolean).join(' ')
          : '';
        const runtime = formatDurationFromTicks(media.RunTimeTicks);
        const year = Number(media.ProductionYear);
        const yearText = Number.isFinite(year) && year > 0 ? String(year) : '';
        const meta = [yearText, runtime].filter(Boolean).join(' • ');
        const pill = formatRelativeTime(media.DateCreated) || 'Recently added';
        const primaryTag = String(media.PrimaryImageTag || '').trim();
        const backdropTag = Array.isArray(media.BackdropImageTags) && media.BackdropImageTags.length
          ? String(media.BackdropImageTags[0] || '').trim()
          : '';
        const providerIds = media.ProviderIds && typeof media.ProviderIds === 'object' ? media.ProviderIds : {};
        return {
          id: String(media.Id || ''),
          title: String(media.Name || '').trim() || 'Untitled',
          subtitle,
          meta,
          pill,
          kind,
          overview: String(media.Overview || '').trim(),
          imdbId: String(providerIds.Imdb || providerIds.IMDB || '').trim(),
          tmdbId: String(providerIds.Tmdb || providerIds.TMDB || '').trim(),
          thumb: buildEmbyImageUrl({
            baseUrl: recentResponse.baseUrl,
            itemId: media.Id,
            type: 'Primary',
            apiKey,
            tag: primaryTag,
          }),
          art: backdropTag
            ? buildEmbyImageUrl({
              baseUrl: recentResponse.baseUrl,
              itemId: media.Id,
              type: 'Backdrop',
              index: '0',
              apiKey,
              tag: backdropTag,
            })
            : '',
        };
      });
      pushLog({
        level: 'info',
        app: 'emby',
        action: 'overview.recent',
        message: 'Emby recent items fetched.',
        meta: { count: items.length, type: mediaType },
      });
      return res.json({ items });
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'emby',
        action: 'overview.recent',
        message: safeMessage(err) || 'Failed to fetch Emby recent items.',
      });
      return res.status(502).json({ error: safeMessage(err) || 'Failed to fetch Emby recent items.' });
    }
  });

  app.get('/api/pulsarr/stats/:kind', requireUser, async (req, res) => {
    const kind = String(req.params.kind || '').trim().toLowerCase();
    const endpointByKind = {
      'recent-requests': '/v1/stats/recent-requests',
      movies: '/v1/stats/movies',
      shows: '/v1/stats/shows',
    };
    const endpointPath = endpointByKind[kind];
    if (!endpointPath) return res.status(400).json({ error: 'Unsupported Pulsarr stats endpoint.' });

    const config = loadConfig();
    const apps = config.apps || [];
    const pulsarrApp = apps.find((appItem) => appItem.id === 'pulsarr');
    if (!pulsarrApp) return res.status(404).json({ error: 'Pulsarr app is not configured.' });
    if (!canAccessDashboardApp(config, pulsarrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Pulsarr dashboard access denied.' });
    }

    const apiKey = String(pulsarrApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Pulsarr API key.' });

    const candidates = resolveRequestApiCandidates(pulsarrApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Pulsarr URL.' });

    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      const upstreamUrl = buildAppApiUrl(baseUrl, endpointPath);
      Object.entries(req.query || {}).forEach(([key, value]) => {
        if (value === undefined || value === null || value === '') return;
        upstreamUrl.searchParams.set(key, String(value));
      });

      try {
        const upstreamRes = await fetch(upstreamUrl.toString(), {
          headers: {
            Accept: 'application/json',
            'X-API-Key': apiKey,
          },
        });
        const text = await upstreamRes.text();
        if (!upstreamRes.ok) {
          const bodyMessage = String(text || '').trim();
          lastError = `Pulsarr request failed (${upstreamRes.status}) via ${baseUrl}${bodyMessage ? `: ${bodyMessage.slice(0, 220)}` : ''}`;
          continue;
        }
        try {
          const parsed = JSON.parse(text || '{}');
          pushLog({
            level: 'info',
            app: 'pulsarr',
            action: `stats.${kind}`,
            message: 'Pulsarr stats response received.',
          });
          return res.json(parsed);
        } catch (err) {
          lastError = `Invalid JSON response from Pulsarr via ${baseUrl}.`;
        }
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Pulsarr via ${baseUrl}.`;
      }
    }

    pushLog({
      level: 'error',
      app: 'pulsarr',
      action: `stats.${kind}`,
      message: lastError || 'Failed to reach Pulsarr on configured URLs.',
    });
    return res.status(502).json({ error: lastError || 'Failed to reach Pulsarr on configured URLs.' });
  });

  app.get('/api/seerr/stats/:kind', requireUser, async (req, res) => {
    const kind = String(req.params.kind || '').trim().toLowerCase();
    const config = loadConfig();
    const apps = config.apps || [];
    const seerrApp = apps.find((appItem) => appItem.id === 'seerr');
    if (!seerrApp) return res.status(404).json({ error: 'Seerr app is not configured.' });
    if (!canAccessDashboardApp(config, seerrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Seerr dashboard access denied.' });
    }

    const apiKey = String(seerrApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Seerr API key.' });

    const candidates = resolveRequestApiCandidates(seerrApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Seerr URL.' });

    try {
      if (kind === 'recent-requests') {
        const rawLimit = Number(req.query?.limit);
        const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(50, rawLimit)) : 20;
        const filter = mapSeerrFilter(req.query?.status);
        const requestPayload = await fetchSeerrJson({
          candidates,
          apiKey,
          path: '/api/v1/request',
          query: { take: limit, skip: 0, sort: 'added', filter },
        });
        const results = Array.isArray(requestPayload?.results) ? requestPayload.results : [];
        const detailCache = new Map();

        const fetchDetail = async (mediaType, tmdbId) => {
          const detailKey = `${mediaType}:${tmdbId}`;
          if (detailCache.has(detailKey)) return detailCache.get(detailKey);
          const detailPath = mediaType === 'show'
            ? `/api/v1/tv/${encodeURIComponent(tmdbId)}`
            : `/api/v1/movie/${encodeURIComponent(tmdbId)}`;
          try {
            const detail = await fetchSeerrJson({
              candidates,
              apiKey,
              path: detailPath,
              query: {},
            });
            detailCache.set(detailKey, detail);
            return detail;
          } catch (_err) {
            detailCache.set(detailKey, null);
            return null;
          }
        };

        const selected = results.slice(0, limit).map((entry) => {
          const rawType = String(entry?.type || entry?.media?.mediaType || '').toLowerCase();
          const mediaType = rawType === 'tv' || rawType === 'show' ? 'show' : 'movie';
          const tmdbId = Number(entry?.media?.tmdbId || entry?.tmdbId || 0) || 0;
          return { entry, mediaType, tmdbId };
        });
        await Promise.all(selected.map(({ mediaType, tmdbId }) => (
          tmdbId ? fetchDetail(mediaType, tmdbId) : Promise.resolve(null)
        )));
        const normalized = selected.map(({ entry, mediaType, tmdbId }) => {
          const detail = tmdbId ? detailCache.get(`${mediaType}:${tmdbId}`) : null;
          const imdbId = String(detail?.imdbId || detail?.imdb_id || entry?.media?.imdbId || '').trim();
          return {
            title: String(
              detail?.title
              || detail?.name
              || entry?.subject
              || entry?.media?.title
              || entry?.media?.name
              || ''
            ).trim(),
            contentType: mediaType,
            createdAt: entry?.createdAt || entry?.updatedAt || '',
            status: mapSeerrRequestStatus(entry?.status),
            userName: String(entry?.requestedBy?.displayName || entry?.requestedBy?.username || '').trim(),
            guids: [
              tmdbId ? `tmdb:${tmdbId}` : '',
              imdbId ? `imdb:${imdbId}` : '',
            ].filter(Boolean),
            posterPath: detail?.posterPath || detail?.poster_path || '',
            overview: String(detail?.overview || '').trim(),
          };
        });

        pushLog({
          level: 'info',
          app: 'seerr',
          action: `stats.${kind}`,
          message: 'Seerr stats response received.',
          meta: { count: normalized.length },
        });
        return res.json({ results: normalized });
      }

      if (kind === 'movies' || kind === 'shows') {
        const rawLimit = Number(req.query?.limit);
        const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(50, rawLimit)) : 20;
        const discoverPath = kind === 'movies' ? '/api/v1/discover/movies' : '/api/v1/discover/tv';
        const discoverPayload = await fetchSeerrJson({
          candidates,
          apiKey,
          path: discoverPath,
          query: { page: 1 },
        });
        const records = Array.isArray(discoverPayload?.results) ? discoverPayload.results : [];
        const normalized = records.slice(0, limit).map((entry) => {
          const tmdbId = Number(entry?.id || entry?.tmdbId || 0) || 0;
          const mediaType = kind === 'shows' ? 'show' : 'movie';
          return {
            title: String(entry?.title || entry?.name || '').trim(),
            content_type: mediaType,
            count: Number(entry?.voteCount ?? entry?.popularity ?? 0) || 0,
            posterPath: entry?.posterPath || entry?.poster_path || '',
            overview: String(entry?.overview || '').trim(),
            guids: tmdbId ? [`tmdb:${tmdbId}`] : [],
          };
        });

        pushLog({
          level: 'info',
          app: 'seerr',
          action: `stats.${kind}`,
          message: 'Seerr stats response received.',
          meta: { count: normalized.length },
        });
        return res.json({ results: normalized });
      }

      return res.status(400).json({ error: 'Unsupported Seerr stats endpoint.' });
    } catch (err) {
      const lastError = safeMessage(err) || 'Failed to reach Seerr on configured URLs.';
      pushLog({
        level: 'error',
        app: 'seerr',
        action: `stats.${kind}`,
        message: lastError,
      });
      return res.status(502).json({ error: lastError });
    }
  });

  app.get('/api/pulsarr/tmdb/:kind/:id', requireUser, async (req, res) => {
    const kindRaw = String(req.params.kind || '').trim().toLowerCase();
    const kind = kindRaw === 'show' ? 'tv' : kindRaw;
    const tmdbId = String(req.params.id || '').trim();
    if (!tmdbId || (kind !== 'movie' && kind !== 'tv')) {
      return res.status(400).json({ error: 'Invalid TMDB request.' });
    }

    const config = loadConfig();
    const apps = config.apps || [];
    const pulsarrApp = apps.find((appItem) => appItem.id === 'pulsarr');
    if (!pulsarrApp) return res.status(404).json({ error: 'Pulsarr app is not configured.' });
    if (!canAccessDashboardApp(config, pulsarrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Pulsarr dashboard access denied.' });
    }

    const apiKey = String(pulsarrApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Pulsarr API key.' });

    const candidates = resolveRequestApiCandidates(pulsarrApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Pulsarr URL.' });

    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      const upstreamUrl = buildAppApiUrl(baseUrl, `v1/tmdb/${kind}/${encodeURIComponent(tmdbId)}`);
      try {
        const upstreamRes = await fetch(upstreamUrl.toString(), {
          headers: {
            Accept: 'application/json',
            'X-API-Key': apiKey,
          },
        });
        const text = await upstreamRes.text();
        if (!upstreamRes.ok) {
          const bodyMessage = String(text || '').trim();
          lastError = `Pulsarr TMDB request failed (${upstreamRes.status}) via ${baseUrl}${bodyMessage ? `: ${bodyMessage.slice(0, 220)}` : ''}`;
          continue;
        }
        try {
          const parsed = JSON.parse(text || '{}');
          pushLog({
            level: 'info',
            app: 'pulsarr',
            action: 'tmdb',
            message: 'Pulsarr TMDB response received.',
            meta: { kind, tmdbId },
          });
          return res.json(parsed);
        } catch (err) {
          lastError = `Invalid JSON response from Pulsarr via ${baseUrl}.`;
        }
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Pulsarr via ${baseUrl}.`;
      }
    }

    pushLog({
      level: 'error',
      app: 'pulsarr',
      action: 'tmdb',
      message: lastError || 'Failed to fetch Pulsarr TMDB details.',
      meta: { kind, tmdbId },
    });
    return res.status(502).json({ error: lastError || 'Failed to fetch Pulsarr TMDB details.' });
  });

  app.get('/api/seerr/tmdb/:kind/:id', requireUser, async (req, res) => {
    const kindRaw = String(req.params.kind || '').trim().toLowerCase();
    const kind = kindRaw === 'show' ? 'tv' : kindRaw;
    const tmdbId = String(req.params.id || '').trim();
    if (!tmdbId || (kind !== 'movie' && kind !== 'tv')) {
      return res.status(400).json({ error: 'Invalid TMDB request.' });
    }

    const config = loadConfig();
    const apps = config.apps || [];
    const seerrApp = apps.find((appItem) => appItem.id === 'seerr');
    if (!seerrApp) return res.status(404).json({ error: 'Seerr app is not configured.' });
    if (!canAccessDashboardApp(config, seerrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Seerr dashboard access denied.' });
    }

    const apiKey = String(seerrApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Seerr API key.' });

    const candidates = resolveRequestApiCandidates(seerrApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Seerr URL.' });

    try {
      const parsed = await fetchSeerrJson({
        candidates,
        apiKey,
        path: `/api/v1/${kind === 'tv' ? 'tv' : 'movie'}/${encodeURIComponent(tmdbId)}`,
        query: {},
      });
      const payload = { ...parsed, imdb_id: parsed?.imdb_id || parsed?.imdbId || '' };
      pushLog({
        level: 'info',
        app: 'seerr',
        action: 'tmdb',
        message: 'Seerr TMDB response received.',
        meta: { kind, tmdbId },
      });
      return res.json(payload);
    } catch (err) {
      const lastError = safeMessage(err) || 'Failed to fetch Seerr TMDB details.';
      pushLog({
        level: 'error',
        app: 'seerr',
        action: 'tmdb',
        message: lastError,
        meta: { kind, tmdbId },
      });
      return res.status(502).json({ error: lastError });
    }
  });
}
