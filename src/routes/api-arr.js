export function registerApiArr(app, ctx) {
  const {
    requireUser,
    loadConfig,
    getEffectiveRole,
    canAccessDashboardApp,
    pushLog,
    safeMessage,
    normalizeAppId,
    resolveAppApiCandidates,
    buildAppApiUrl,
    uniqueList,
    normalizeBaseUrl,
    resolveLaunchUrl,
    // prowlarr/jackett indexer helpers
    normalizeIndexerProtocol,
    extractTopLevelCategoryIds,
    toTopLevelCategoryOptions,
    toTopLevelCategoryId,
    parseJackettJsonItems,
    parseJackettTorznabItems,
    // bazarr
    mapBazarrQueueItem,
    // autobrr
    mapAutobrrQueueItem,
    // maintainerr
    normalizeMaintainerrTmdbKind,
    buildBasicAuthHeader,
    buildMaintainerrTmdbImageUrl,
    mapMaintainerrRuleItem,
    normalizeMaintainerrMediaKind,
    mapMaintainerrLibraryItem,
    pickFirstNonEmpty,
    mapMaintainerrCollectionMediaItem,
    parseFiniteNumber,
    // cleanuparr
    extractCleanuparrList,
    mapCleanuparrStrikeItem,
    mapCleanuparrEventItem,
    // downloaders
    getAppBaseId,
    fetchTransmissionQueue,
    fetchNzbgetQueue,
    fetchQbittorrentQueue,
    fetchSabnzbdQueue,
    // arr proxy
    isAppInSet,
    // constants
    DOWNLOADER_APP_IDS,
    ARR_APP_IDS,
  } = ctx;

  const maintainerrTmdbAssetCache = new Map();

  app.get('/api/prowlarr/search/filters', requireUser, async (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const prowlarrApp = apps.find((appItem) => appItem.id === 'prowlarr');
    if (!prowlarrApp) return res.status(404).json({ error: 'Prowlarr app is not configured.' });
    if (!canAccessDashboardApp(config, prowlarrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Prowlarr dashboard access denied.' });
    }
  
    const apiKey = String(prowlarrApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Prowlarr API key.' });
  
    const candidates = resolveAppApiCandidates(prowlarrApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Prowlarr URL.' });
  
    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);
        let indexerPayload = [];
        let categoriesPayload = null;
        try {
          const indexerUrl = buildAppApiUrl(baseUrl, 'api/v1/indexer');
          const indexerRes = await fetch(indexerUrl.toString(), {
            headers: {
              Accept: 'application/json',
              'X-Api-Key': apiKey,
            },
            signal: controller.signal,
          });
          const indexerText = await indexerRes.text();
          if (!indexerRes.ok) {
            lastError = `Prowlarr indexer metadata failed (${indexerRes.status}) via ${baseUrl}.`;
            continue;
          }
          indexerPayload = indexerText ? JSON.parse(indexerText) : [];
  
          const categoriesUrl = buildAppApiUrl(baseUrl, 'api/v1/indexercategory');
          const categoriesRes = await fetch(categoriesUrl.toString(), {
            headers: {
              Accept: 'application/json',
              'X-Api-Key': apiKey,
            },
            signal: controller.signal,
          });
          if (categoriesRes.ok) {
            const categoriesText = await categoriesRes.text();
            categoriesPayload = categoriesText ? JSON.parse(categoriesText) : [];
          }
        } finally {
          clearTimeout(timeout);
        }
  
        const rawIndexers = Array.isArray(indexerPayload) ? indexerPayload : [];
        const categoryProtocols = new Map();
        const indexers = rawIndexers
          .map((entry) => {
            const enabled = entry?.enable !== false && entry?.enabled !== false;
            if (!enabled) return null;
            const id = String(entry?.id || entry?.indexerId || '').trim();
            const name = String(entry?.name || entry?.title || '').trim();
            if (!id || !name) return null;
            const protocol = normalizeIndexerProtocol(entry?.protocol || entry?.implementation || entry?.implementationName) || 'torrent';
            const categoryIds = extractTopLevelCategoryIds(entry?.capabilities?.categories || entry?.categories || entry?.caps?.categories);
            categoryIds.forEach((categoryId) => {
              if (!categoryProtocols.has(categoryId)) categoryProtocols.set(categoryId, new Set());
              categoryProtocols.get(categoryId).add(protocol);
            });
            return { id, name, protocol };
          })
          .filter(Boolean)
          .sort((a, b) => a.name.localeCompare(b.name));
  
        const categorySource = categoriesPayload !== null
          ? categoriesPayload
          : rawIndexers.map((entry) => entry?.capabilities?.categories || entry?.categories || entry?.caps?.categories);
        const categories = toTopLevelCategoryOptions(categorySource).map((entry) => {
          const numericId = Number(entry.id);
          const protocols = categoryProtocols.has(numericId)
            ? Array.from(categoryProtocols.get(numericId))
            : [];
          return {
            id: entry.id,
            name: entry.name,
            protocols,
          };
        });
  
        return res.json({ indexers, categories });
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Prowlarr via ${baseUrl}.`;
      }
    }
  
    return res.status(502).json({ error: lastError || 'Failed to fetch Prowlarr search filters.' });
  });

  app.get('/api/prowlarr/search', requireUser, async (req, res) => {
    const query = String(req.query?.query || req.query?.q || '').trim();
    if (!query) return res.status(400).json({ error: 'Missing search query.' });
  
    const config = loadConfig();
    const apps = config.apps || [];
    const prowlarrApp = apps.find((appItem) => appItem.id === 'prowlarr');
    if (!prowlarrApp) return res.status(404).json({ error: 'Prowlarr app is not configured.' });
    if (!canAccessDashboardApp(config, prowlarrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Prowlarr dashboard access denied.' });
    }
  
    const apiKey = String(prowlarrApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Prowlarr API key.' });
  
    const candidates = uniqueList([
      normalizeBaseUrl(prowlarrApp.remoteUrl || ''),
      normalizeBaseUrl(resolveLaunchUrl(prowlarrApp, req)),
      normalizeBaseUrl(prowlarrApp.localUrl || ''),
      normalizeBaseUrl(prowlarrApp.url || ''),
    ]);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Prowlarr URL.' });
  
    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      const queryParams = {};
      Object.entries(req.query || {}).forEach(([key, value]) => {
        if (value === undefined || value === null || value === '') return;
        if (key === 'q' || key === 'query') return;
        if (Array.isArray(value)) {
          const entries = value.map((entry) => String(entry || '').trim()).filter(Boolean);
          if (entries.length) queryParams[key] = entries;
          return;
        }
        if (['indexerids', 'categories'].includes(String(key || '').trim().toLowerCase())) {
          const entries = String(value)
            .split(',')
            .map((entry) => entry.trim())
            .filter(Boolean);
          if (entries.length > 1) {
            queryParams[key] = entries;
            return;
          }
        }
        queryParams[key] = String(value);
      });
  
      const tryRequest = async (method) => {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 6000);
        try {
          if (method === 'GET') {
            const upstreamUrl = buildAppApiUrl(baseUrl, 'api/v1/search');
            upstreamUrl.searchParams.set('query', query);
            Object.entries(queryParams).forEach(([key, value]) => {
              if (Array.isArray(value)) {
                value.forEach((entry) => upstreamUrl.searchParams.append(key, String(entry)));
                return;
              }
              upstreamUrl.searchParams.set(key, String(value));
            });
            return fetch(upstreamUrl.toString(), {
              headers: {
                Accept: 'application/json',
                'X-Api-Key': apiKey,
              },
              signal: controller.signal,
            });
          }
          const upstreamUrl = buildAppApiUrl(baseUrl, 'api/v1/search');
          return fetch(upstreamUrl.toString(), {
            method: 'POST',
            headers: {
              Accept: 'application/json',
              'Content-Type': 'application/json',
              'X-Api-Key': apiKey,
            },
            body: JSON.stringify({ query, ...queryParams }),
            signal: controller.signal,
          });
        } finally {
          clearTimeout(timeout);
        }
      };
  
      try {
        let upstreamRes = await tryRequest('GET');
        let text = await upstreamRes.text();
        if (!upstreamRes.ok) {
          upstreamRes = await tryRequest('POST');
          text = await upstreamRes.text();
        }
        if (!upstreamRes.ok) {
          lastError = `Prowlarr request failed (${upstreamRes.status}) via ${baseUrl}.`;
          pushLog({
            level: 'error',
            app: 'prowlarr',
            action: 'search',
            message: lastError,
            meta: { status: upstreamRes.status },
          });
          continue;
        }
        try {
          const parsed = JSON.parse(text || '[]');
          const list = Array.isArray(parsed)
            ? parsed
            : (Array.isArray(parsed?.records) ? parsed.records : (Array.isArray(parsed?.results) ? parsed.results : []));
          const total = Array.isArray(parsed)
            ? parsed.length
            : Number(parsed?.totalRecords || parsed?.total || list.length || 0);
          pushLog({
            level: 'info',
            app: 'prowlarr',
            action: 'search',
            message: 'Search response received.',
            meta: {
              count: list.length,
              total,
              keys: parsed && !Array.isArray(parsed) ? Object.keys(parsed).slice(0, 8) : ['array'],
            },
          });
          return res.json(parsed);
        } catch (err) {
          lastError = `Invalid JSON response from Prowlarr via ${baseUrl}.`;
          pushLog({
            level: 'error',
            app: 'prowlarr',
            action: 'search',
            message: lastError,
          });
        }
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Prowlarr via ${baseUrl}.`;
        pushLog({
          level: 'error',
          app: 'prowlarr',
          action: 'search',
          message: lastError,
        });
      }
    }
  
    return res.status(502).json({ error: lastError || 'Failed to reach Prowlarr.' });
  });

  app.post('/api/prowlarr/download', requireUser, async (req, res) => {
    const searchId = String(req.body?.id || '').trim();
    const guid = String(req.body?.guid || '').trim();
    const indexerId = String(req.body?.indexerId || '').trim();
    const downloadClientId = String(req.body?.downloadClientId || '').trim();
    const release = req.body?.release || null;
    if (!release && !searchId && !guid) return res.status(400).json({ error: 'Missing search result details.' });
  
    const config = loadConfig();
    const apps = config.apps || [];
    const prowlarrApp = apps.find((appItem) => appItem.id === 'prowlarr');
    if (!prowlarrApp) return res.status(404).json({ error: 'Prowlarr app is not configured.' });
    if (!canAccessDashboardApp(config, prowlarrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Prowlarr dashboard access denied.' });
    }
  
    const apiKey = String(prowlarrApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Prowlarr API key.' });
  
    const candidates = uniqueList([
      normalizeBaseUrl(prowlarrApp.remoteUrl || ''),
      normalizeBaseUrl(resolveLaunchUrl(prowlarrApp, req)),
      normalizeBaseUrl(prowlarrApp.localUrl || ''),
      normalizeBaseUrl(prowlarrApp.url || ''),
    ]);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Prowlarr URL.' });
  
    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      const idValue = searchId || guid;
      const searchDownloadUrl = buildAppApiUrl(baseUrl, `api/v1/search/${encodeURIComponent(idValue)}/download`);
      if (downloadClientId) searchDownloadUrl.searchParams.set('downloadClientId', downloadClientId);
      const releaseDownloadUrl = buildAppApiUrl(baseUrl, 'api/v1/release/download');
      const searchGrabUrl = buildAppApiUrl(baseUrl, 'api/v1/search');
      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 6000);
        let upstreamRes;
        try {
          if (release) {
            const grabBody = { ...release };
            if (downloadClientId) grabBody.downloadClientId = Number(downloadClientId);
            upstreamRes = await fetch(searchGrabUrl.toString(), {
              method: 'POST',
              headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
                'X-Api-Key': apiKey,
              },
              body: JSON.stringify(grabBody),
              signal: controller.signal,
            });
            if (upstreamRes.ok) {
              // handled below
            }
          }
          const releaseBody = {
            guid: guid || undefined,
            indexerId: indexerId ? Number(indexerId) : undefined,
            downloadClientId: downloadClientId ? Number(downloadClientId) : undefined,
          };
          if (!upstreamRes || !upstreamRes.ok) {
            upstreamRes = await fetch(releaseDownloadUrl.toString(), {
              method: 'POST',
              headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
                'X-Api-Key': apiKey,
              },
              body: JSON.stringify(releaseBody),
              signal: controller.signal,
            });
          }
          if (!upstreamRes.ok) {
            upstreamRes = await fetch(searchDownloadUrl.toString(), {
              method: 'POST',
              headers: {
                Accept: 'application/json',
                'X-Api-Key': apiKey,
              },
              signal: controller.signal,
            });
          }
          if (!upstreamRes.ok) {
            upstreamRes = await fetch(searchDownloadUrl.toString(), {
              method: 'GET',
              headers: {
                Accept: 'application/json',
                'X-Api-Key': apiKey,
              },
              signal: controller.signal,
            });
          }
        } finally {
          clearTimeout(timeout);
        }
        const text = await upstreamRes.text();
        if (!upstreamRes.ok) {
          lastError = `Prowlarr download failed (${upstreamRes.status}) via ${baseUrl}.`;
          pushLog({
            level: 'error',
            app: 'prowlarr',
            action: 'download',
            message: lastError,
            meta: { status: upstreamRes.status, body: text.slice(0, 500) },
          });
          continue;
        }
        try {
          pushLog({
            level: 'info',
            app: 'prowlarr',
            action: 'download',
            message: 'Sent to download client.',
          });
          return res.json(text ? JSON.parse(text) : { ok: true });
        } catch (err) {
          pushLog({
            level: 'info',
            app: 'prowlarr',
            action: 'download',
            message: 'Sent to download client.',
          });
          return res.json({ ok: true });
        }
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Prowlarr via ${baseUrl}.`;
        pushLog({
          level: 'error',
          app: 'prowlarr',
          action: 'download',
          message: lastError,
        });
      }
    }
  
    return res.status(502).json({ error: lastError || 'Failed to send to download client.' });
  });

  app.get('/api/jackett/search/filters', requireUser, async (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const jackettApp = apps.find((appItem) => normalizeAppId(appItem?.id) === 'jackett');
    if (!jackettApp) return res.status(404).json({ error: 'Jackett app is not configured.' });
    if (!canAccessDashboardApp(config, jackettApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Jackett dashboard access denied.' });
    }
  
    const apiKey = String(jackettApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Jackett API key.' });
  
    const candidates = resolveAppApiCandidates(jackettApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Jackett URL.' });
  
    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);
        let payload = {};
        try {
          const upstreamUrl = buildAppApiUrl(baseUrl, 'api/v2.0/indexers');
          upstreamUrl.searchParams.set('apikey', apiKey);
          const upstreamRes = await fetch(upstreamUrl.toString(), {
            headers: { Accept: 'application/json' },
            signal: controller.signal,
          });
          const text = await upstreamRes.text();
          if (!upstreamRes.ok) {
            lastError = `Jackett indexer metadata failed (${upstreamRes.status}) via ${baseUrl}.`;
            continue;
          }
          payload = text ? JSON.parse(text) : {};
        } finally {
          clearTimeout(timeout);
        }
  
        const rawIndexers = Array.isArray(payload?.Indexers)
          ? payload.Indexers
          : (Array.isArray(payload?.indexers)
            ? payload.indexers
            : (Array.isArray(payload) ? payload : []));
        const categoryProtocols = new Map();
        const indexers = rawIndexers
          .map((entry) => {
            const enabled = entry?.enabled !== false && entry?.Enabled !== false;
            if (!enabled) return null;
            const id = String(entry?.id || entry?.Id || entry?.ID || entry?.name || entry?.Name || '').trim();
            const name = String(entry?.name || entry?.Name || id || '').trim();
            if (!id || !name) return null;
            const protocol = normalizeIndexerProtocol(
              entry?.type
              || entry?.Type
              || entry?.protocol
              || entry?.Protocol
              || entry?.searchType
              || entry?.SearchType
              || entry?.caps?.type
            ) || 'torrent';
            const categoryIds = extractTopLevelCategoryIds(
              entry?.categories
              || entry?.Categories
              || entry?.caps?.categories
              || entry?.caps?.Categories
            );
            categoryIds.forEach((categoryId) => {
              if (!categoryProtocols.has(categoryId)) categoryProtocols.set(categoryId, new Set());
              categoryProtocols.get(categoryId).add(protocol);
            });
            return { id, name, protocol };
          })
          .filter(Boolean)
          .sort((a, b) => a.name.localeCompare(b.name));
  
        const categories = toTopLevelCategoryOptions(
          payload?.Categories
          || payload?.categories
          || rawIndexers.map((entry) => entry?.categories || entry?.Categories || entry?.caps?.categories || entry?.caps?.Categories)
        ).map((entry) => {
          const numericId = Number(entry.id);
          const protocols = categoryProtocols.has(numericId)
            ? Array.from(categoryProtocols.get(numericId))
            : [];
          return {
            id: entry.id,
            name: entry.name,
            protocols,
          };
        });
  
        return res.json({ indexers, categories });
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Jackett via ${baseUrl}.`;
      }
    }
  
    return res.status(502).json({ error: lastError || 'Failed to fetch Jackett search filters.' });
  });

  app.get('/api/jackett/search', requireUser, async (req, res) => {
    const query = String(req.query?.query || req.query?.q || '').trim();
    if (!query) return res.status(400).json({ error: 'Missing search query.' });
  
    const config = loadConfig();
    const apps = config.apps || [];
    const jackettApp = apps.find((appItem) => normalizeAppId(appItem?.id) === 'jackett');
    if (!jackettApp) return res.status(404).json({ error: 'Jackett app is not configured.' });
    if (!canAccessDashboardApp(config, jackettApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Jackett dashboard access denied.' });
    }
  
    const apiKey = String(jackettApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Jackett API key.' });
  
    const limit = Math.max(1, Math.min(250, parseFiniteNumber(req.query?.limit || 25, 25)));
    const offset = Math.max(0, parseFiniteNumber(req.query?.offset || 0, 0));
    const protocolFilter = normalizeIndexerProtocol(req.query?.protocol || req.query?.type || '');
    const indexerFilter = String(req.query?.indexer || req.query?.indexerId || '').trim().toLowerCase();
    const indexerNameFilter = String(req.query?.indexerName || '').trim().toLowerCase();
    const categoryFilter = toTopLevelCategoryId(req.query?.category || req.query?.categories || '');
    const candidates = resolveAppApiCandidates(jackettApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Jackett URL.' });
  
    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      try {
        const jsonUrl = buildAppApiUrl(baseUrl, 'api/v2.0/indexers/all/results');
        jsonUrl.searchParams.set('apikey', apiKey);
        jsonUrl.searchParams.set('Query', query);
        jsonUrl.searchParams.set('limit', String(Math.max(limit + offset + 100, 250)));
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);
        let response;
        try {
          response = await fetch(jsonUrl.toString(), {
            headers: { Accept: 'application/json' },
            signal: controller.signal,
          });
        } finally {
          clearTimeout(timeout);
        }
        const bodyText = await response.text();
        if (!response.ok) {
          lastError = `Jackett request failed (${response.status}) via ${baseUrl}.`;
          continue;
        }
        let items = [];
        try {
          const parsed = bodyText ? JSON.parse(bodyText) : {};
          items = parseJackettJsonItems(parsed);
        } catch (err) {
          items = parseJackettTorznabItems(bodyText);
        }
        if (protocolFilter) {
          items = items.filter((item) => normalizeIndexerProtocol(item?.protocol) === protocolFilter);
        }
        if (indexerFilter || indexerNameFilter) {
          items = items.filter((item) => {
            const candidateValues = [
              item?.indexerId,
              item?.indexer,
            ]
              .map((value) => String(value || '').trim().toLowerCase())
              .filter(Boolean);
            if (indexerFilter && candidateValues.includes(indexerFilter)) return true;
            if (indexerNameFilter && candidateValues.includes(indexerNameFilter)) return true;
            return false;
          });
        }
        if (categoryFilter) {
          items = items.filter((item) => Array.isArray(item?.categoryIds) && item.categoryIds.includes(categoryFilter));
        }
        const total = items.length;
        const pageItems = items.slice(offset, offset + limit);
        return res.json({
          records: pageItems,
          totalRecords: total,
          offset,
          limit,
        });
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Jackett via ${baseUrl}.`;
      }
    }
  
    return res.status(502).json({ error: lastError || 'Failed to reach Jackett.' });
  });

  app.get('/api/bazarr/subtitle-queue', requireUser, async (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const bazarrApp = apps.find((appItem) => normalizeAppId(appItem?.id) === 'bazarr');
    if (!bazarrApp) return res.status(404).json({ error: 'Bazarr app is not configured.' });
    if (!canAccessDashboardApp(config, bazarrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Bazarr dashboard access denied.' });
    }
  
    const apiKey = String(bazarrApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Bazarr API key.' });
  
    const candidates = resolveAppApiCandidates(bazarrApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Bazarr URL.' });
  
    const fetchWanted = async (baseUrl, suffix) => {
      const url = buildAppApiUrl(baseUrl, suffix);
      url.searchParams.set('start', '0');
      url.searchParams.set('length', '200');
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);
      try {
        const response = await fetch(url.toString(), {
          headers: {
            Accept: 'application/json',
            'X-API-KEY': apiKey,
          },
          signal: controller.signal,
        });
        const text = await response.text();
        if (!response.ok) {
          return { ok: false, error: `Bazarr request failed (${response.status}).` };
        }
        const parsed = text ? JSON.parse(text) : {};
        const list = Array.isArray(parsed?.data)
          ? parsed.data
          : (Array.isArray(parsed?.records)
            ? parsed.records
            : (Array.isArray(parsed) ? parsed : []));
        return { ok: true, items: list };
      } catch (err) {
        return { ok: false, error: safeMessage(err) || 'Failed to reach Bazarr.' };
      } finally {
        clearTimeout(timeout);
      }
    };
  
    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      try {
        const episodeResult = await fetchWanted(baseUrl, 'api/episodes/wanted');
        const movieResult = await fetchWanted(baseUrl, 'api/movies/wanted');
        if (!episodeResult.ok && !movieResult.ok) {
          lastError = episodeResult.error || movieResult.error || `Failed to reach Bazarr via ${baseUrl}.`;
          continue;
        }
        const episodeItems = Array.isArray(episodeResult.items) ? episodeResult.items : [];
        const movieItems = Array.isArray(movieResult.items) ? movieResult.items : [];
        const mapped = [...episodeItems, ...movieItems]
          .map(mapBazarrQueueItem)
          .filter((item) => Boolean(item?.title));
        return res.json({ items: mapped });
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Bazarr via ${baseUrl}.`;
      }
    }
  
    return res.status(502).json({ error: lastError || 'Failed to fetch Bazarr subtitle queue.' });
  });

  app.get('/api/autobrr/:kind', requireUser, async (req, res) => {
    const kind = String(req.params.kind || '').trim().toLowerCase();
    if (!['recent-matches', 'delivery-queue'].includes(kind)) {
      return res.status(400).json({ error: 'Unsupported Autobrr endpoint.' });
    }
  
    const config = loadConfig();
    const apps = config.apps || [];
    const autobrrApp = apps.find((appItem) => normalizeAppId(appItem?.id) === 'autobrr');
    if (!autobrrApp) return res.status(404).json({ error: 'Autobrr app is not configured.' });
    if (!canAccessDashboardApp(config, autobrrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Autobrr dashboard access denied.' });
    }
  
    const apiKey = String(autobrrApp.apiKey || '').trim();
    if (!apiKey) return res.status(400).json({ error: 'Missing Autobrr API key.' });
  
    const candidates = resolveAppApiCandidates(autobrrApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Autobrr URL.' });
  
    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      try {
        const url = buildAppApiUrl(baseUrl, 'api/release');
        url.searchParams.set('limit', '200');
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);
        let response;
        try {
          response = await fetch(url.toString(), {
            headers: {
              Accept: 'application/json',
              'X-API-Token': apiKey,
            },
            signal: controller.signal,
          });
        } finally {
          clearTimeout(timeout);
        }
        const text = await response.text();
        if (!response.ok) {
          lastError = `Autobrr request failed (${response.status}) via ${baseUrl}.`;
          continue;
        }
        const parsed = text ? JSON.parse(text) : {};
        const list = Array.isArray(parsed?.data)
          ? parsed.data
          : (Array.isArray(parsed?.items)
            ? parsed.items
            : (Array.isArray(parsed?.releases)
              ? parsed.releases
              : (Array.isArray(parsed) ? parsed : [])));
        const mapped = list.map((entry) => mapAutobrrQueueItem(entry, kind)).filter((item) => Boolean(item?.title));
        const filtered = kind === 'delivery-queue'
          ? mapped.filter((item) => ['queued', 'active', 'paused', 'completed', 'error'].includes(item.statusKey))
          : mapped;
        const items = filtered.length ? filtered : mapped;
        return res.json({ items: items.slice(0, 200) });
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Autobrr via ${baseUrl}.`;
      }
    }
  
    return res.status(502).json({ error: lastError || 'Failed to fetch Autobrr data.' });
  });

  app.get('/api/maintainerr-poster/:kind/:id', requireUser, async (req, res) => {
    const rawKind = String(req.params.kind || '').trim().toLowerCase();
    const kind = normalizeMaintainerrTmdbKind(rawKind, '');
    const tmdbId = parseFiniteNumber(req.params.id, 0);
    if (!['movie', 'tv'].includes(kind) || !tmdbId) {
      return res.status(400).json({ error: 'Invalid poster request.' });
    }
  
    const config = loadConfig();
    const apps = config.apps || [];
    const maintainerrApp = apps.find((appItem) => normalizeAppId(appItem?.id) === 'maintainerr');
    if (!maintainerrApp) return res.status(404).json({ error: 'Maintainerr app is not configured.' });
    if (!canAccessDashboardApp(config, maintainerrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Maintainerr dashboard access denied.' });
    }
  
    const cacheKey = `poster:${kind}:${tmdbId}`;
    const cached = maintainerrTmdbAssetCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now() && cached.url) {
      return res.redirect(302, cached.url);
    }
  
    const candidates = resolveAppApiCandidates(maintainerrApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Maintainerr URL.' });
  
    const apiKey = String(maintainerrApp.apiKey || '').trim();
    const authHeader = buildBasicAuthHeader(maintainerrApp.username || '', maintainerrApp.password || '');
    const headers = { Accept: 'text/plain,application/json' };
    if (apiKey) {
      headers['X-Api-Key'] = apiKey;
      headers['X-API-KEY'] = apiKey;
      if (!authHeader) headers.Authorization = /^bearer\s+/i.test(apiKey) ? apiKey : `Bearer ${apiKey}`;
    }
    if (authHeader) headers.Authorization = authHeader;
  
    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      try {
        const url = buildAppApiUrl(baseUrl, `api/moviedb/image/${kind}/${tmdbId}`);
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 15000);
        let response;
        try {
          response = await fetch(url.toString(), {
            headers,
            signal: controller.signal,
          });
        } finally {
          clearTimeout(timeout);
        }
        const text = (await response.text()).trim();
        if (!response.ok) {
          lastError = `Poster lookup failed (${response.status}) via ${baseUrl}.`;
          continue;
        }
        const resolved = buildMaintainerrTmdbImageUrl(text, 'w500');
        if (!resolved) {
          lastError = `Poster lookup returned empty path via ${baseUrl}.`;
          continue;
        }
        maintainerrTmdbAssetCache.set(cacheKey, {
          url: resolved,
          expiresAt: Date.now() + (6 * 60 * 60 * 1000),
        });
        return res.redirect(302, resolved);
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Maintainerr via ${baseUrl}.`;
      }
    }
  
    return res.status(404).json({ error: lastError || 'Poster not found.' });
  });

  app.get('/api/maintainerr-backdrop/:kind/:id', requireUser, async (req, res) => {
    const rawKind = String(req.params.kind || '').trim().toLowerCase();
    const kind = normalizeMaintainerrTmdbKind(rawKind, '');
    const tmdbId = parseFiniteNumber(req.params.id, 0);
    if (!['movie', 'tv'].includes(kind) || !tmdbId) {
      return res.status(400).json({ error: 'Invalid backdrop request.' });
    }
  
    const config = loadConfig();
    const apps = config.apps || [];
    const maintainerrApp = apps.find((appItem) => normalizeAppId(appItem?.id) === 'maintainerr');
    if (!maintainerrApp) return res.status(404).json({ error: 'Maintainerr app is not configured.' });
    if (!canAccessDashboardApp(config, maintainerrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Maintainerr dashboard access denied.' });
    }
  
    const cacheKey = `backdrop:${kind}:${tmdbId}`;
    const cached = maintainerrTmdbAssetCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now() && cached.url) {
      return res.redirect(302, cached.url);
    }
  
    const candidates = resolveAppApiCandidates(maintainerrApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Maintainerr URL.' });
  
    const apiKey = String(maintainerrApp.apiKey || '').trim();
    const authHeader = buildBasicAuthHeader(maintainerrApp.username || '', maintainerrApp.password || '');
    const headers = { Accept: 'text/plain,application/json' };
    if (apiKey) {
      headers['X-Api-Key'] = apiKey;
      headers['X-API-KEY'] = apiKey;
      if (!authHeader) headers.Authorization = /^bearer\s+/i.test(apiKey) ? apiKey : `Bearer ${apiKey}`;
    }
    if (authHeader) headers.Authorization = authHeader;
  
    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      try {
        const url = buildAppApiUrl(baseUrl, `api/moviedb/backdrop/${kind}/${tmdbId}`);
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 15000);
        let response;
        try {
          response = await fetch(url.toString(), {
            headers,
            signal: controller.signal,
          });
        } finally {
          clearTimeout(timeout);
        }
        const text = (await response.text()).trim();
        if (!response.ok) {
          lastError = `Backdrop lookup failed (${response.status}) via ${baseUrl}.`;
          continue;
        }
        const resolved = buildMaintainerrTmdbImageUrl(text, 'w1280');
        if (!resolved) {
          lastError = `Backdrop lookup returned empty path via ${baseUrl}.`;
          continue;
        }
        maintainerrTmdbAssetCache.set(cacheKey, {
          url: resolved,
          expiresAt: Date.now() + (6 * 60 * 60 * 1000),
        });
        return res.redirect(302, resolved);
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Maintainerr via ${baseUrl}.`;
      }
    }
  
    return res.status(404).json({ error: lastError || 'Backdrop not found.' });
  });

  app.get('/api/maintainerr/:kind', requireUser, async (req, res) => {
    const kind = String(req.params.kind || '').trim().toLowerCase();
    if (!['library-media', 'rules', 'collections-media'].includes(kind)) {
      return res.status(400).json({ error: 'Unsupported Maintainerr endpoint.' });
    }
  
    const config = loadConfig();
    const apps = config.apps || [];
    const maintainerrApp = apps.find((appItem) => normalizeAppId(appItem?.id) === 'maintainerr');
    if (!maintainerrApp) return res.status(404).json({ error: 'Maintainerr app is not configured.' });
    if (!canAccessDashboardApp(config, maintainerrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Maintainerr dashboard access denied.' });
    }
  
    const candidates = resolveAppApiCandidates(maintainerrApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Maintainerr URL.' });
  
    const apiKey = String(maintainerrApp.apiKey || '').trim();
    const authHeader = buildBasicAuthHeader(maintainerrApp.username || '', maintainerrApp.password || '');
    const headers = {
      Accept: 'application/json',
    };
    if (apiKey) {
      headers['X-Api-Key'] = apiKey;
      headers['X-API-KEY'] = apiKey;
      if (!authHeader) headers.Authorization = /^bearer\s+/i.test(apiKey) ? apiKey : `Bearer ${apiKey}`;
    }
    if (authHeader) headers.Authorization = authHeader;
  
    const fetchMaintainerrJson = async (baseUrl, path, query = {}) => {
      const url = buildAppApiUrl(baseUrl, path);
      Object.entries(query || {}).forEach(([key, value]) => {
        if (value !== undefined && value !== null && value !== '') {
          url.searchParams.set(key, String(value));
        }
      });
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 15000);
      try {
        const response = await fetch(url.toString(), {
          headers,
          signal: controller.signal,
        });
        const text = await response.text();
        let payload = {};
        try {
          payload = text ? JSON.parse(text) : {};
        } catch (err) {
          payload = {};
        }
        if (!response.ok) {
          const message = String(payload?.message || payload?.error || '').trim();
          throw new Error(message || `Maintainerr request failed (${response.status}).`);
        }
        return payload;
      } finally {
        clearTimeout(timeout);
      }
    };
  
    const mediaFilterRaw = String(req.query?.media || 'all').trim().toLowerCase();
    const mediaFilter = mediaFilterRaw === 'movie' || mediaFilterRaw === 'show' ? mediaFilterRaw : 'all';
    const requestedLimitRaw = String(req.query?.limit || '').trim().toLowerCase();
    const maxCap = kind === 'library-media' ? 8000 : 1200;
    let itemLimit = kind === 'library-media' ? 2000 : 200;
    if (requestedLimitRaw === 'all') {
      itemLimit = maxCap;
    } else if (requestedLimitRaw) {
      const parsed = Number(requestedLimitRaw);
      if (Number.isFinite(parsed) && parsed > 0) {
        itemLimit = Math.min(maxCap, Math.max(1, Math.round(parsed)));
      }
    }
  
    let lastError = '';
    for (let candidateIndex = 0; candidateIndex < candidates.length; candidateIndex += 1) {
      const baseUrl = candidates[candidateIndex];
      if (!baseUrl) continue;
      try {
        if (kind === 'rules') {
          const payload = await fetchMaintainerrJson(baseUrl, 'api/rules');
          const list = Array.isArray(payload)
            ? payload
            : (Array.isArray(payload?.items) ? payload.items : []);
          let items = list.map((entry) => mapMaintainerrRuleItem(entry)).filter((entry) => Boolean(entry?.id));
          if (mediaFilter !== 'all') {
            items = items.filter((entry) => String(entry?.kind || '').toLowerCase() === mediaFilter);
          }
          items = items.sort((left, right) => String(left?.name || '').localeCompare(String(right?.name || '')));
          return res.json({ items: items.slice(0, itemLimit) });
        }
  
        if (kind === 'library-media') {
          const librariesPayload = await fetchMaintainerrJson(baseUrl, 'api/plex/libraries');
          const librariesList = Array.isArray(librariesPayload)
            ? librariesPayload
            : (Array.isArray(librariesPayload?.items) ? librariesPayload.items : []);
          const libraries = librariesList
            .map((library) => {
              const libraryId = parseFiniteNumber(library?.id || library?.key || library?.librarySectionID, 0);
              const mediaKind = normalizeMaintainerrMediaKind(library?.type, '');
              if (!libraryId || !['movie', 'show'].includes(mediaKind)) return null;
              return {
                id: libraryId,
                title: pickFirstNonEmpty([library?.title, `Library ${libraryId}`]),
                kind: mediaKind,
              };
            })
            .filter(Boolean)
            .filter((library) => mediaFilter === 'all' || library.kind === mediaFilter);
  
          const perPage = Math.max(50, Math.min(500, parseFiniteNumber(req.query?.pageSize || 200, 200)));
          const maxPages = Math.max(1, Math.min(100, parseFiniteNumber(req.query?.maxPages || 50, 50)));
          const items = [];
  
          for (let libraryIndex = 0; libraryIndex < libraries.length; libraryIndex += 1) {
            const library = libraries[libraryIndex];
            let page = 1;
            let loaded = 0;
            let expectedTotal = Number.POSITIVE_INFINITY;
            while (
              items.length < itemLimit
              && page <= maxPages
              && loaded < expectedTotal
            ) {
              const pagePayload = await fetchMaintainerrJson(baseUrl, `api/plex/library/${library.id}/content/${page}`, {
                amount: perPage,
              });
              const pageItems = Array.isArray(pagePayload?.items)
                ? pagePayload.items
                : (Array.isArray(pagePayload) ? pagePayload : []);
              if (!pageItems.length) break;
              expectedTotal = Math.max(loaded + pageItems.length, parseFiniteNumber(pagePayload?.totalSize, loaded + pageItems.length));
              pageItems.forEach((entry) => {
                const mapped = mapMaintainerrLibraryItem(entry, {
                  baseUrl,
                  libraryId: library.id,
                  libraryTitle: library.title,
                  libraryType: library.kind,
                });
                if (mediaFilter !== 'all' && mapped.kind !== mediaFilter) return;
                items.push(mapped);
              });
              loaded += pageItems.length;
              page += 1;
              if (items.length >= itemLimit) break;
            }
            if (items.length >= itemLimit) break;
          }
  
          const ordered = items
            .sort((left, right) => String(left?.title || '').localeCompare(String(right?.title || '')));
          return res.json({
            libraries,
            items: ordered.slice(0, itemLimit),
          });
        }
  
        const collectionsPayload = await fetchMaintainerrJson(baseUrl, 'api/collections');
        const collectionList = Array.isArray(collectionsPayload)
          ? collectionsPayload
          : (Array.isArray(collectionsPayload?.items) ? collectionsPayload.items : []);
        const collections = collectionList
          .map((entry) => {
            const collectionId = parseFiniteNumber(entry?.id, 0);
            const kind = normalizeMaintainerrMediaKind(entry?.type, '');
            if (!collectionId || !['movie', 'show'].includes(kind)) return null;
            return {
              id: collectionId,
              title: pickFirstNonEmpty([entry?.title, `Collection ${collectionId}`]),
              kind,
              isActive: entry?.isActive !== false,
              mediaCount: Array.isArray(entry?.media) ? entry.media.length : 0,
            };
          })
          .filter(Boolean)
          .filter((entry) => mediaFilter === 'all' || entry.kind === mediaFilter);
  
        const selectedRaw = String(req.query?.collectionIds || req.query?.collectionId || 'all').trim().toLowerCase();
        let selectedCollectionIds = [];
        if (!selectedRaw || selectedRaw === 'all') {
          selectedCollectionIds = collections.map((entry) => entry.id);
        } else {
          selectedCollectionIds = selectedRaw
            .split(',')
            .map((value) => parseFiniteNumber(value, 0))
            .filter((value) => value > 0)
            .filter((value, index, array) => array.indexOf(value) === index);
        }
        if (!selectedCollectionIds.length) selectedCollectionIds = collections.map((entry) => entry.id);
        selectedCollectionIds = selectedCollectionIds.filter((value) => collections.some((entry) => entry.id === value));
  
        const perCollectionSize = Math.max(
          20,
          Math.min(
            250,
            parseFiniteNumber(
              req.query?.collectionSize,
              Math.ceil(itemLimit / Math.max(1, selectedCollectionIds.length)) + 25
            )
          )
        );
        const combined = [];
        for (let index = 0; index < selectedCollectionIds.length; index += 1) {
          const collectionId = selectedCollectionIds[index];
          const collection = collections.find((entry) => entry.id === collectionId);
          if (!collection) continue;
          const mediaPayload = await fetchMaintainerrJson(baseUrl, `api/collections/media/${collectionId}/content/1`, {
            size: perCollectionSize,
          });
          const list = Array.isArray(mediaPayload?.items)
            ? mediaPayload.items
            : (Array.isArray(mediaPayload) ? mediaPayload : []);
          list.forEach((entry) => {
            const mapped = mapMaintainerrCollectionMediaItem(entry, {
              baseUrl,
              collectionId,
              collectionTitle: collection.title,
              type: collection.kind,
            });
            if (mediaFilter !== 'all' && mapped.kind !== mediaFilter) return;
            combined.push(mapped);
          });
        }
  
        const ordered = combined
          .sort((left, right) => parseFiniteNumber(right?.sortTs, 0) - parseFiniteNumber(left?.sortTs, 0));
        return res.json({
          collections,
          selectedCollectionIds,
          items: ordered.slice(0, itemLimit),
        });
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Maintainerr via ${baseUrl}.`;
      }
    }
  
    return res.status(502).json({ error: lastError || 'Failed to fetch Maintainerr data.' });
  });

  app.post('/api/maintainerr/rules/:id/execute', requireUser, async (req, res) => {
    const ruleId = parseFiniteNumber(req.params.id, 0);
    if (!ruleId) return res.status(400).json({ error: 'Invalid rule id.' });
  
    const config = loadConfig();
    const apps = config.apps || [];
    const maintainerrApp = apps.find((appItem) => normalizeAppId(appItem?.id) === 'maintainerr');
    if (!maintainerrApp) return res.status(404).json({ error: 'Maintainerr app is not configured.' });
    if (!canAccessDashboardApp(config, maintainerrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Maintainerr dashboard access denied.' });
    }
  
    const candidates = resolveAppApiCandidates(maintainerrApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Maintainerr URL.' });
  
    const apiKey = String(maintainerrApp.apiKey || '').trim();
    const authHeader = buildBasicAuthHeader(maintainerrApp.username || '', maintainerrApp.password || '');
    const headers = {
      Accept: 'application/json',
    };
    if (apiKey) {
      headers['X-Api-Key'] = apiKey;
      headers['X-API-KEY'] = apiKey;
      if (!authHeader) headers.Authorization = /^bearer\s+/i.test(apiKey) ? apiKey : `Bearer ${apiKey}`;
    }
    if (authHeader) headers.Authorization = authHeader;
  
    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      try {
        const url = buildAppApiUrl(baseUrl, `api/rules/${ruleId}/execute`);
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 15000);
        let response;
        try {
          response = await fetch(url.toString(), {
            method: 'POST',
            headers,
            signal: controller.signal,
          });
        } finally {
          clearTimeout(timeout);
        }
        const text = await response.text();
        let payload = {};
        try {
          payload = text ? JSON.parse(text) : {};
        } catch (err) {
          payload = {};
        }
        if (!response.ok) {
          const message = String(payload?.message || payload?.error || '').trim();
          if ([400, 404, 409].includes(response.status)) {
            return res.status(response.status).json({ error: message || `Rule execute failed (${response.status}).` });
          }
          lastError = message || `Rule execute failed (${response.status}) via ${baseUrl}.`;
          continue;
        }
        return res.json({ ok: true, id: ruleId, status: response.status });
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach Maintainerr via ${baseUrl}.`;
      }
    }
  
    return res.status(502).json({ error: lastError || 'Failed to execute Maintainerr rule.' });
  });

  app.get('/api/cleanuparr/:kind', requireUser, async (req, res) => {
    const kind = String(req.params.kind || '').trim().toLowerCase();
    if (!['recent-strikes', 'events', 'stats'].includes(kind)) {
      return res.status(400).json({ error: 'Unsupported Cleanuparr endpoint.' });
    }
  
    const config = loadConfig();
    const apps = config.apps || [];
    const cleanuparrApp = apps.find((appItem) => normalizeAppId(appItem?.id) === 'cleanuparr');
    if (!cleanuparrApp) return res.status(404).json({ error: 'Cleanuparr app is not configured.' });
    if (!canAccessDashboardApp(config, cleanuparrApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Cleanuparr dashboard access denied.' });
    }
  
    const candidates = resolveAppApiCandidates(cleanuparrApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Cleanuparr URL.' });
  
    const apiKey = String(cleanuparrApp.apiKey || '').trim();
    const authHeader = buildBasicAuthHeader(cleanuparrApp.username || '', cleanuparrApp.password || '');
    const headers = {
      Accept: 'application/json',
    };
    if (apiKey) {
      headers['X-Api-Key'] = apiKey;
      headers['X-API-KEY'] = apiKey;
      if (!authHeader) headers.Authorization = /^bearer\s+/i.test(apiKey) ? apiKey : `Bearer ${apiKey}`;
    }
    if (authHeader) headers.Authorization = authHeader;
  
    const endpointPlans = kind === 'recent-strikes'
      ? [
        { path: 'api/strikes/recent' },
        { path: 'api/v1/strikes/recent' },
        { path: 'api/strikes', query: { recent: 'true' } },
        { path: 'api/v1/strikes', query: { recent: 'true' } },
        { path: 'api/strikes' },
        { path: 'api/v1/strikes' },
      ]
      : (kind === 'events'
        ? [
          { path: 'api/events' },
          { path: 'api/v1/events' },
          { path: 'api/events/recent' },
          { path: 'api/v1/events/recent' },
        ]
        : [
          { path: 'api/stats' },
          { path: 'api/v1/stats' },
        ]);
  
    const requestedLimitRaw = String(req.query?.limit || '').trim().toLowerCase();
    const defaultLimit = kind === 'stats' ? 50 : 200;
    const maxCap = kind === 'stats' ? 200 : 2000;
    let itemLimit = defaultLimit;
    if (requestedLimitRaw === 'all') {
      itemLimit = maxCap;
    } else if (requestedLimitRaw) {
      const parsedLimit = Number(requestedLimitRaw);
      if (Number.isFinite(parsedLimit) && parsedLimit > 0) {
        itemLimit = Math.min(maxCap, Math.max(1, Math.round(parsedLimit)));
      }
    }
  
    let lastError = '';
    for (let candidateIndex = 0; candidateIndex < candidates.length; candidateIndex += 1) {
      const baseUrl = candidates[candidateIndex];
      if (!baseUrl) continue;
      for (let planIndex = 0; planIndex < endpointPlans.length; planIndex += 1) {
        const endpoint = endpointPlans[planIndex];
        try {
          const url = buildAppApiUrl(baseUrl, endpoint.path);
          Object.entries(endpoint.query || {}).forEach(([key, value]) => {
            if (value !== undefined && value !== null && value !== '') {
              url.searchParams.set(key, String(value));
            }
          });
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), 12000);
          let response;
          try {
            response = await fetch(url.toString(), {
              headers,
              signal: controller.signal,
            });
          } finally {
            clearTimeout(timeout);
          }
          const text = await response.text();
          if (!response.ok) {
            lastError = `Cleanuparr request failed (${response.status}) via ${baseUrl}.`;
            continue;
          }
          let payload = {};
          try {
            payload = text ? JSON.parse(text) : {};
          } catch (_err) {
            payload = {};
          }
          if (kind === 'stats') {
            const statsSource = payload && typeof payload === 'object' ? payload : {};
            const statsItems = Object.entries(statsSource)
              .filter(([key]) => Boolean(String(key || '').trim()))
              .map(([key, value]) => ({
                id: String(key || '').trim(),
                title: String(key || '').trim(),
                subtitle: '',
                meta: '',
                pill: '',
                sortTs: 0,
                status: '',
                statusKey: '',
                kind: 'movie',
                overview: String(value ?? '').trim(),
                thumb: '',
                art: '',
                value: value ?? '',
              }))
              .slice(0, itemLimit);
            return res.json({ items: statsItems });
          }
  
          const list = extractCleanuparrList(payload, kind);
          if (!Array.isArray(list)) {
            lastError = `Unexpected Cleanuparr response format via ${baseUrl}.`;
            continue;
          }
          const mapper = kind === 'recent-strikes' ? mapCleanuparrStrikeItem : mapCleanuparrEventItem;
          const items = list
            .map((entry) => mapper(entry, baseUrl))
            .filter((entry) => Boolean(entry?.title))
            .sort((left, right) => {
              const sortDelta = parseFiniteNumber(right?.sortTs, 0) - parseFiniteNumber(left?.sortTs, 0);
              if (sortDelta !== 0) return sortDelta;
              return String(left?.title || '').localeCompare(String(right?.title || ''));
            })
            .slice(0, itemLimit);
          return res.json({ items });
        } catch (err) {
          lastError = safeMessage(err) || `Failed to reach Cleanuparr via ${baseUrl}.`;
        }
      }
    }
  
    return res.status(502).json({ error: lastError || 'Failed to fetch Cleanuparr data.' });
  });

  app.get('/api/downloaders/:appId/queue', requireUser, async (req, res) => {
    const requestedAppId = normalizeAppId(req.params.appId || '');
    const baseId = getAppBaseId(requestedAppId);
    if (!requestedAppId || !DOWNLOADER_APP_IDS.includes(baseId)) {
      return res.status(400).json({ error: 'Unsupported downloader app.' });
    }
  
    const config = loadConfig();
    const apps = config.apps || [];
    const appItem = apps.find((item) => normalizeAppId(item?.id) === requestedAppId);
    if (!appItem) return res.status(404).json({ error: `${requestedAppId} is not configured.` });
    if (!canAccessDashboardApp(config, appItem, getEffectiveRole(req))) {
      return res.status(403).json({ error: `${appItem.name || requestedAppId} dashboard access denied.` });
    }
  
    const candidates = uniqueList([
      normalizeBaseUrl(appItem.remoteUrl || ''),
      normalizeBaseUrl(resolveLaunchUrl(appItem, req)),
      normalizeBaseUrl(appItem.localUrl || ''),
      normalizeBaseUrl(appItem.url || ''),
    ]);
    if (!candidates.length) return res.status(400).json({ error: `Missing ${appItem.name || requestedAppId} URL.` });
  
    const authHeader = buildBasicAuthHeader(appItem.username || '', appItem.password || '');
    const apiKey = String(appItem.apiKey || '').trim();
    let lastError = '';
  
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      try {
        let result;
        if (baseId === 'transmission') {
          result = await fetchTransmissionQueue(baseUrl, authHeader);
        } else if (baseId === 'nzbget') {
          result = await fetchNzbgetQueue(baseUrl, authHeader);
        } else if (baseId === 'qbittorrent') {
          result = await fetchQbittorrentQueue(baseUrl, appItem.username || '', appItem.password || '');
        } else {
          result = await fetchSabnzbdQueue(baseUrl, apiKey, authHeader);
        }
        if (result.items) {
          pushLog({
            level: 'info',
            app: requestedAppId,
            action: 'downloader.queue',
            message: `${appItem.name || requestedAppId} queue response received.`,
          });
          return res.json({ items: result.items });
        }
        lastError = result.error || `Failed to reach ${appItem.name || requestedAppId}.`;
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach ${appItem.name || requestedAppId}.`;
      }
    }
  
    pushLog({
      level: 'error',
      app: requestedAppId,
      action: 'downloader.queue',
      message: lastError || `Failed to reach ${appItem.name || requestedAppId}.`,
    });
    return res.status(502).json({ error: lastError || `Failed to reach ${appItem.name || requestedAppId}.` });
  });

  app.get('/api/arr/:appId/:version/*', requireUser, async (req, res) => {
    const appId = String(req.params.appId || '').trim().toLowerCase();
    const version = String(req.params.version || '').trim().toLowerCase();
    const pathSuffix = String(req.params[0] || '').trim().replace(/^\/+/, '');
    const reject = (status, message, meta = null) => {
      pushLog({
        level: status >= 500 ? 'error' : 'warn',
        app: appId || 'arr',
        action: 'arr.proxy.reject',
        message,
        meta: meta || null,
      });
      return res.status(status).json({ error: message });
    };
    if (!isAppInSet(appId, ARR_APP_IDS)) {
      return reject(400, 'Unsupported ARR app.', { appId, version, path: pathSuffix });
    }
    if (version !== 'v1' && version !== 'v3') {
      return reject(400, 'Unsupported ARR API version.', { appId, version, path: pathSuffix });
    }
    if (!pathSuffix) {
      return reject(400, 'Missing ARR endpoint path.', { appId, version });
    }
  
    const config = loadConfig();
    const apps = config.apps || [];
    const arrApp = apps.find((appItem) => appItem.id === appId);
    if (!arrApp) {
      return reject(404, `${appId} is not configured.`, { appId, version, path: pathSuffix });
    }
    if (!canAccessDashboardApp(config, arrApp, getEffectiveRole(req))) {
      return reject(403, `${arrApp.name || appId} dashboard access denied.`, {
        appId,
        version,
        path: pathSuffix,
      });
    }
  
    const apiKey = String(arrApp.apiKey || '').trim();
    if (!apiKey) {
      return reject(400, `Missing ${arrApp.name || appId} API key.`, { appId, version, path: pathSuffix });
    }
  
    const candidates = uniqueList([
      normalizeBaseUrl(arrApp.remoteUrl || ''),
      normalizeBaseUrl(resolveLaunchUrl(arrApp, req)),
      normalizeBaseUrl(arrApp.localUrl || ''),
      normalizeBaseUrl(arrApp.url || ''),
    ]);
    if (!candidates.length) {
      return reject(400, `Missing ${arrApp.name || appId} URL.`, { appId, version, path: pathSuffix });
    }
  
    let lastError = '';
    for (let index = 0; index < candidates.length; index += 1) {
      const baseUrl = candidates[index];
      if (!baseUrl) continue;
      const upstreamUrl = buildAppApiUrl(baseUrl, `api/${version}/${pathSuffix}`);
      Object.entries(req.query || {}).forEach(([key, value]) => {
        if (value === undefined || value === null || value === '') return;
        upstreamUrl.searchParams.set(key, String(value));
      });
      upstreamUrl.searchParams.set('apikey', apiKey);
  
      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 6000);
        let upstreamRes;
        try {
          upstreamRes = await fetch(upstreamUrl.toString(), {
            headers: {
              Accept: 'application/json',
              'X-Api-Key': apiKey,
            },
            signal: controller.signal,
          });
        } finally {
          clearTimeout(timeout);
        }
        const text = await upstreamRes.text();
        if (!upstreamRes.ok) {
          lastError = `${arrApp.name || appId} request failed (${upstreamRes.status}) via ${baseUrl}.`;
          continue;
        }
        try {
          const parsed = JSON.parse(text || '{}');
          pushLog({
            level: 'info',
            app: appId,
            action: 'arr.proxy',
            message: 'ARR response received.',
            meta: { version, path: pathSuffix },
          });
          return res.json(parsed);
        } catch (err) {
          lastError = `Invalid JSON response from ${arrApp.name || appId} via ${baseUrl}.`;
        }
      } catch (err) {
        lastError = safeMessage(err) || `Failed to reach ${arrApp.name || appId} via ${baseUrl}.`;
      }
    }
  
    pushLog({
      level: 'error',
      app: appId,
      action: 'arr.proxy',
      message: lastError || `Failed to reach ${arrApp.name || appId}.`,
      meta: { version, path: pathSuffix },
    });
    return res.status(502).json({ error: lastError || `Failed to reach ${arrApp.name || appId}.` });
  });

  app.all('/api/arr/*', requireUser, (req, res) => {
    const path = String(req.path || '').trim();
    pushLog({
      level: 'warn',
      app: 'arr',
      action: 'arr.proxy.miss',
      message: 'ARR proxy route did not match request path.',
      meta: {
        method: req.method,
        path,
        query: req.query || {},
      },
    });
    return res.status(404).json({ error: 'Unknown ARR proxy route.' });
  });
}
