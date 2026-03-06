export function registerApiSpecialty(app, ctx) {
  const {
    requireUser,
    requireAdmin,
    requireSettingsAdmin,
    loadConfig,
    saveConfig,
    getEffectiveRole,
    canAccessDashboardApp,
    normalizeAppId,
    safeMessage,
    pushLog,
    resolveNotificationSettings,
    sendAppriseNotification,
    widgetStatsInternalToken,
    // URL helpers
    getAppBaseId,
    resolveLaunchUrl,
    injectBasicAuthIntoUrl,
    stripUrlEmbeddedCredentials,
    resolveAppApiCandidates,
    resolveRequestApiCandidates,
    normalizeBaseUrl,
    buildAppApiUrl,
    // launch helpers
    resolveRoleAwareLaunchUrl,
    hasEmbeddedUrlCredentials,
    // Romm session/priming
    bootstrapRommIframeSession,
    buildCookieHeaderFromSetCookies,
    getRommCsrfTokenFromSetCookies,
    buildRommCookiePrimingPlan,
    evaluateRommCookiePrimingCompatibility,
    prepareRommPrimedSetCookies,
    buildBasicAuthHeader,
    // Romm data
    extractRommList,
    mapRommConsoleItem,
    mapRommRecentlyAddedItem,
    parseFiniteNumber,
    // widget bars
    resolveWidgetBars,
    serializeWidgetBars,
    normalizeWidgetBar,
    normalizeWidgetBarId,
    normalizeWidgetInBar,
    normalizeWidgetId,
    buildWidgetBarId,
    buildWidgetRowId,
    normalizeWidgetRow,
    normalizeWidgetRowSettings,
    resolveNextWidgetBarOrder,
    resolveNextWidgetRowOrder,
    resolveWidgetBarTypes,
    getWidgetStatType,
    // system widgets
    SYSTEM_WIDGET_TYPES,
    SYSTEM_WIDGET_TYPE_BY_ID,
    SYSTEM_WIDGET_SEARCH_PROVIDERS,
    SYSTEM_WIDGET_TIMEZONES,
    normalizeSystemWidget,
  } = ctx;
  const WIDGET_STATUS_MONITOR_POLL_SECONDS_DEFAULT = 45;
  const WIDGET_STATUS_MONITOR_POLL_SECONDS_MIN = 15;
  const WIDGET_STATUS_MONITOR_POLL_SECONDS_MAX = 600;
  const WIDGET_STATUS_MONITOR_REQUEST_TIMEOUT_MS_DEFAULT = 4000;
  const WIDGET_STATUS_MONITOR_REQUEST_TIMEOUT_MS_MIN = 1000;
  const WIDGET_STATUS_MONITOR_REQUEST_TIMEOUT_MS_MAX = 20000;
  const WIDGET_STATUS_MONITOR_MAX_CONCURRENCY_DEFAULT = 4;
  const WIDGET_STATUS_MONITOR_MAX_CONCURRENCY_MIN = 1;
  const WIDGET_STATUS_MONITOR_MAX_CONCURRENCY_MAX = 10;
  const WIDGET_STATUS_MONITOR_INTERNAL_HEADER = 'x-launcharr-internal-token';
  const widgetStatusInternalToken = String(widgetStatsInternalToken || '').trim();
  const widgetStatusMonitorBaseUrl = `http://127.0.0.1:${Number(process.env.PORT) || 3333}`;
  const widgetStatusMonitorState = new Map();
  let widgetStatusMonitorTickRunning = false;

  function resolveWidgetMonitorRuntimeSettings(notificationSettings = {}) {
    const rawPollSeconds = Number(notificationSettings?.widgetStatusPollSeconds);
    const rawRequestTimeoutMs = Number(notificationSettings?.widgetStatusRequestTimeoutMs);
    const rawMaxConcurrency = Number(notificationSettings?.widgetStatusMaxConcurrency);
    const pollSeconds = Number.isFinite(rawPollSeconds)
      ? Math.max(WIDGET_STATUS_MONITOR_POLL_SECONDS_MIN, Math.min(WIDGET_STATUS_MONITOR_POLL_SECONDS_MAX, Math.round(rawPollSeconds)))
      : WIDGET_STATUS_MONITOR_POLL_SECONDS_DEFAULT;
    const requestTimeoutMs = Number.isFinite(rawRequestTimeoutMs)
      ? Math.max(WIDGET_STATUS_MONITOR_REQUEST_TIMEOUT_MS_MIN, Math.min(WIDGET_STATUS_MONITOR_REQUEST_TIMEOUT_MS_MAX, Math.round(rawRequestTimeoutMs)))
      : WIDGET_STATUS_MONITOR_REQUEST_TIMEOUT_MS_DEFAULT;
    const maxConcurrency = Number.isFinite(rawMaxConcurrency)
      ? Math.max(WIDGET_STATUS_MONITOR_MAX_CONCURRENCY_MIN, Math.min(WIDGET_STATUS_MONITOR_MAX_CONCURRENCY_MAX, Math.round(rawMaxConcurrency)))
      : WIDGET_STATUS_MONITOR_MAX_CONCURRENCY_DEFAULT;
    return {
      pollSeconds,
      pollMs: pollSeconds * 1000,
      requestTimeoutMs,
      maxConcurrency,
    };
  }

  function normalizeWidgetMonitorState(value) {
    const raw = String(value || '').trim().toLowerCase();
    if (raw === 'up' || raw === 'online') return 'online';
    if (raw === 'down' || raw === 'offline') return 'offline';
    return 'unknown';
  }

  function isInternalWidgetStatsRequest(req) {
    if (!widgetStatusInternalToken) return false;
    const provided = String(req.get(WIDGET_STATUS_MONITOR_INTERNAL_HEADER) || '').trim();
    return Boolean(provided) && provided === widgetStatusInternalToken;
  }

  function requireWidgetStatsAccess(req, res, next) {
    if (isInternalWidgetStatsRequest(req)) {
      req.__launcharrInternalWidgetStats = true;
      return next();
    }
    return requireUser(req, res, next);
  }

  function buildWidgetStatusMonitorSnapshot() {
    const config = loadConfig();
    const notificationSettings = resolveNotificationSettings(config);
    const runtimeSettings = resolveWidgetMonitorRuntimeSettings(notificationSettings);
    const delaySecondsRaw = Number(notificationSettings?.widgetStatusDelaySeconds);
    const delaySeconds = Number.isFinite(delaySecondsRaw)
      ? Math.max(5, Math.min(3600, Math.round(delaySecondsRaw)))
      : 60;
    const enabled = Boolean(notificationSettings?.appriseEnabled && notificationSettings?.widgetStatusEnabled);
    const now = Date.now();
    const items = Array.from(widgetStatusMonitorState.entries())
      .map(([appId, state]) => {
        const appName = String(state?.appName || appId).trim() || appId;
        const currentState = normalizeWidgetMonitorState(state?.currentState);
        const pendingStateRaw = normalizeWidgetMonitorState(state?.pendingState);
        const pendingSinceMs = Number(state?.pendingSince);
        const hasPending = (pendingStateRaw === 'online' || pendingStateRaw === 'offline')
          && Number.isFinite(pendingSinceMs)
          && pendingSinceMs > 0;
        const pendingElapsedSeconds = hasPending
          ? Math.max(0, Math.floor((now - pendingSinceMs) / 1000))
          : 0;
        const pendingRemainingSeconds = hasPending
          ? Math.max(0, delaySeconds - pendingElapsedSeconds)
          : 0;
        return {
          appId,
          appName,
          currentState,
          pendingState: hasPending ? pendingStateRaw : '',
          pendingSince: hasPending ? new Date(pendingSinceMs).toISOString() : '',
          pendingElapsedSeconds,
          pendingRemainingSeconds,
        };
      })
      .sort((a, b) => String(a.appName || a.appId || '').localeCompare(String(b.appName || b.appId || '')));
    return {
      ok: true,
      enabled,
      appriseEnabled: Boolean(notificationSettings?.appriseEnabled),
      widgetStatusEnabled: Boolean(notificationSettings?.widgetStatusEnabled),
      delaySeconds,
      pollSeconds: runtimeSettings.pollSeconds,
      requestTimeoutMs: runtimeSettings.requestTimeoutMs,
      maxConcurrency: runtimeSettings.maxConcurrency,
      items,
      generatedAt: new Date().toISOString(),
    };
  }

  function resolveWidgetMonitorTargets(config) {
    const apps = Array.isArray(config?.apps) ? config.apps : [];
    const appById = new Map();
    apps.forEach((appItem) => {
      const appId = normalizeAppId(appItem?.id);
      if (!appId) return;
      appById.set(appId, appItem);
    });

    const bars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
    const appIds = new Set();
    bars.forEach((bar) => {
      const rows = Array.isArray(bar?.rows) ? bar.rows : [];
      rows.forEach((row) => {
        const widgets = Array.isArray(row?.widgets) ? row.widgets : [];
        widgets.forEach((widget) => {
          if (!widget || widget.systemType || widget.available === false) return;
          const appId = normalizeAppId(widget.appId);
          if (!appId) return;
          const baseId = getAppBaseId(appId);
          const statType = getWidgetStatType(baseId);
          if (!statType) return;
          if (!appById.has(appId)) return;
          appIds.add(appId);
        });
      });
    });

    return Array.from(appIds).map((appId) => ({
      appId,
      appName: String(appById.get(appId)?.name || appId).trim() || appId,
    }));
  }

  async function fetchWidgetMonitorState(appId, requestTimeoutMs) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
    try {
      const headers = { Accept: 'application/json' };
      if (widgetStatusInternalToken) {
        headers[WIDGET_STATUS_MONITOR_INTERNAL_HEADER] = widgetStatusInternalToken;
      }
      const response = await fetch(`${widgetStatusMonitorBaseUrl}/api/widget-stats/${encodeURIComponent(appId)}`, {
        headers,
        signal: controller.signal,
      });
      if (!response.ok) {
        return { state: 'unknown', error: `HTTP ${response.status}` };
      }
      const payload = await response.json().catch(() => ({}));
      return { state: normalizeWidgetMonitorState(payload?.status) };
    } catch (err) {
      return { state: 'unknown', error: safeMessage(err) || 'Failed to fetch widget state.' };
    } finally {
      clearTimeout(timeout);
    }
  }

  async function mapWithConcurrency(items, maxConcurrency, mapper) {
    const list = Array.isArray(items) ? items : [];
    if (!list.length) return [];
    const workerCount = Math.max(1, Math.min(Number(maxConcurrency) || 1, list.length));
    const results = new Array(list.length);
    let cursor = 0;

    const workers = Array.from({ length: workerCount }, async () => {
      while (cursor < list.length) {
        const index = cursor;
        cursor += 1;
        results[index] = await mapper(list[index], index);
      }
    });

    await Promise.all(workers);
    return results;
  }

  async function runWidgetStatusMonitorTick() {
    if (widgetStatusMonitorTickRunning) return;
    widgetStatusMonitorTickRunning = true;
    try {
      const config = loadConfig();
      const notificationSettings = resolveNotificationSettings(config);
      const monitorEnabled = Boolean(notificationSettings?.appriseEnabled && notificationSettings?.widgetStatusEnabled);
      if (!monitorEnabled) {
        if (widgetStatusMonitorState.size) widgetStatusMonitorState.clear();
        return;
      }
      const delaySeconds = Number(notificationSettings?.widgetStatusDelaySeconds);
      const effectiveDelaySeconds = Number.isFinite(delaySeconds)
        ? Math.max(5, Math.min(3600, Math.round(delaySeconds)))
        : 60;
      const delayMs = effectiveDelaySeconds * 1000;
      const runtimeSettings = resolveWidgetMonitorRuntimeSettings(notificationSettings);
      const targets = resolveWidgetMonitorTargets(config);
      const targetIdSet = new Set(targets.map((target) => target.appId));

      Array.from(widgetStatusMonitorState.keys()).forEach((appId) => {
        if (!targetIdSet.has(appId)) widgetStatusMonitorState.delete(appId);
      });

      if (!targets.length) return;
      const now = Date.now();
      const statusResults = await mapWithConcurrency(
        targets,
        runtimeSettings.maxConcurrency,
        async (target) => ({
          ...target,
          ...(await fetchWidgetMonitorState(target.appId, runtimeSettings.requestTimeoutMs)),
        })
      );
      const notifications = [];

      statusResults.forEach((result) => {
        const nextState = normalizeWidgetMonitorState(result.state);
        const existing = widgetStatusMonitorState.get(result.appId);
        if (!existing) {
          widgetStatusMonitorState.set(result.appId, {
            appName: result.appName,
            currentState: nextState,
            pendingState: '',
            pendingSince: 0,
          });
          return;
        }
        existing.appName = String(result.appName || existing.appName || result.appId).trim() || result.appId;

        if (!monitorEnabled) {
          existing.currentState = nextState;
          existing.pendingState = '';
          existing.pendingSince = 0;
          return;
        }

        if (nextState === existing.currentState) {
          existing.pendingState = '';
          existing.pendingSince = 0;
          return;
        }

        if (nextState === 'unknown') {
          // Keep last known state so transient probe failures do not suppress real transitions.
          existing.pendingState = '';
          existing.pendingSince = 0;
          return;
        }

        if (existing.currentState === 'unknown') {
          existing.currentState = nextState;
          existing.pendingState = '';
          existing.pendingSince = 0;
          return;
        }

        if (existing.pendingState !== nextState) {
          existing.pendingState = nextState;
          existing.pendingSince = now;
          return;
        }

        if ((now - existing.pendingSince) < delayMs) return;
        const fromState = existing.currentState;
        existing.currentState = nextState;
        existing.pendingState = '';
        existing.pendingSince = 0;
        notifications.push({
          appId: result.appId,
          appName: result.appName,
          fromState,
          toState: nextState,
          delaySeconds: effectiveDelaySeconds,
        });
      });

      for (const notification of notifications) {
        const timestamp = new Date().toISOString().replace('T', ' ').slice(0, 19);
        const toOnline = notification.toState === 'online';
        const title = toOnline
          ? `Launcharr widget online: ${notification.appName}`
          : `Launcharr widget offline: ${notification.appName}`;
        const body = toOnline
          ? `${notification.appName} came online at ${timestamp} after ${notification.delaySeconds}s in the new state.`
          : `${notification.appName} went offline at ${timestamp} after ${notification.delaySeconds}s in the new state.`;
        try {
          await sendAppriseNotification(notificationSettings, {
            title,
            body,
            tag: notificationSettings.appriseTag,
          });
          pushLog({
            level: 'info',
            app: notification.appId,
            action: 'notifications.widget-status',
            message: `${notification.appName} changed from ${notification.fromState} to ${notification.toState}.`,
            meta: { delaySeconds: notification.delaySeconds },
          });
        } catch (err) {
          pushLog({
            level: 'error',
            app: notification.appId,
            action: 'notifications.widget-status',
            message: `Failed to send widget status notification for ${notification.appName}.`,
            meta: { error: safeMessage(err) || 'Unknown notification error.' },
          });
        }
      }
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'widgets',
        action: 'notifications.widget-status.monitor',
        message: 'Widget status monitor tick failed.',
        meta: { error: safeMessage(err) || 'Unknown monitor error.' },
      });
    } finally {
      widgetStatusMonitorTickRunning = false;
    }
  }

  app.post('/api/romm/viewer-session-test', requireAdmin, async (req, res) => {
    const config = loadConfig();
    const apps = Array.isArray(config?.apps) ? config.apps : [];
    const requestedAppId = String(req.body?.appId || '').trim();
    const rommApp = apps.find((appItem) => {
      if (requestedAppId && String(appItem?.id || '') === requestedAppId) return true;
      return getAppBaseId(appItem?.id) === 'romm';
    });
    if (!rommApp) {
      return res.json({ ok: false, message: 'Romm app is not configured.' });
    }
  
    const localUrl = String(req.body?.localUrl !== undefined ? req.body.localUrl : (rommApp.localUrl || '')).trim();
    const remoteUrl = String(req.body?.remoteUrl !== undefined ? req.body.remoteUrl : (rommApp.remoteUrl || '')).trim();
    const fallbackUrl = String(req.body?.url !== undefined ? req.body.url : (rommApp.url || '')).trim();
    const appUsername = String(req.body?.username !== undefined ? req.body.username : (rommApp.username || '')).trim();
    const appPassword = String(req.body?.password !== undefined ? req.body.password : (rommApp.password || ''));
    const viewerUsername = String(req.body?.viewerUsername !== undefined ? req.body.viewerUsername : (rommApp.viewerUsername || '')).trim();
    const viewerPassword = String(req.body?.viewerPassword !== undefined ? req.body.viewerPassword : (rommApp.viewerPassword || ''));
    const credentialModeRaw = String(req.body?.credentialMode || 'viewer').trim().toLowerCase();
    const credentialMode = credentialModeRaw === 'admin' ? 'admin' : 'viewer';
    const usingAdminCredentials = credentialMode === 'admin';
    const sessionUsername = usingAdminCredentials ? appUsername : viewerUsername;
    const sessionPassword = usingAdminCredentials ? appPassword : viewerPassword;
    const sessionLabel = usingAdminCredentials ? 'admin' : 'viewer';
    const primeBrowserRaw = String(req.body?.primeBrowser ?? 'true').trim().toLowerCase();
    const primeBrowser = ['1', 'true', 'yes', 'on'].includes(primeBrowserRaw);
  
    if (!sessionUsername || !sessionPassword) {
      return res.json({
        ok: false,
        message: usingAdminCredentials
          ? 'Romm admin username and password are required (uses the main Username/Password fields).'
          : 'Viewer username and password are required.',
      });
    }
  
    const effectiveApp = {
      ...rommApp,
      localUrl,
      remoteUrl,
      url: fallbackUrl || rommApp.url || '',
    };
    const baseLaunchUrl = String(resolveLaunchUrl(effectiveApp, req) || '').trim();
    if (!baseLaunchUrl) {
      return res.json({ ok: false, message: 'Missing Romm launch URL (local or remote URL).' });
    }
  
    const credentialedLaunchUrl = injectBasicAuthIntoUrl(baseLaunchUrl, sessionUsername, sessionPassword);
    const cleanLaunchUrl = stripUrlEmbeddedCredentials(credentialedLaunchUrl);
    const primingPlan = buildRommCookiePrimingPlan({
      config,
      req,
      browserUrl: cleanLaunchUrl,
    });
  
    const bootstrap = await bootstrapRommIframeSession({
      req,
      launchUrl: credentialedLaunchUrl,
      authBaseCandidates: resolveAppApiCandidates(effectiveApp, req),
    });
  
    let probe = { ok: false };
    if (bootstrap?.ok) {
      try {
        const probeBase = normalizeBaseUrl(bootstrap.authBaseUrl || cleanLaunchUrl);
        const meUrl = buildAppApiUrl(probeBase, 'api/users/me').toString();
        const cookieHeader = buildCookieHeaderFromSetCookies(bootstrap.setCookies || []);
        const csrfToken = getRommCsrfTokenFromSetCookies(bootstrap.setCookies || []);
        const headers = { Accept: 'application/json' };
        if (cookieHeader) headers.Cookie = cookieHeader;
        if (csrfToken) headers['x-csrftoken'] = csrfToken;
        const response = await fetch(meUrl, { headers });
        const text = await response.text().catch(() => '');
        let payload = {};
        try { payload = text ? JSON.parse(text) : {}; } catch (_err) { payload = {}; }
        probe = {
          ok: response.ok,
          status: response.status,
          user: response.ok ? {
            username: String(payload?.username || '').trim(),
            role: String(payload?.role || '').trim(),
            id: payload?.id,
          } : null,
          error: response.ok ? '' : (String(payload?.detail || payload?.error || text || '').trim() || `Status ${response.status}`),
        };
      } catch (err) {
        probe = { ok: false, error: safeMessage(err) || 'Failed to verify /api/users/me.' };
      }
    }
  
    const primingCompatibility = bootstrap?.ok
      ? evaluateRommCookiePrimingCompatibility(bootstrap.setCookies, primingPlan)
      : { ok: false, blocking: false, reason: '' };
    const primedSetCookies = (bootstrap?.ok && primeBrowser && primingCompatibility.ok)
      ? prepareRommPrimedSetCookies(bootstrap.setCookies, primingPlan)
      : [];
    if (primedSetCookies.length) {
      res.append('Set-Cookie', primedSetCookies);
    }
  
    const cookieNames = Array.isArray(bootstrap?.setCookies)
      ? Array.from(new Set(bootstrap.setCookies.map((cookie) => String(cookie || '').split('=')[0].trim()).filter(Boolean)))
      : [];
    const primedBrowser = Boolean(primedSetCookies.length);
    const message = (() => {
      if (!bootstrap?.ok) return bootstrap?.error || `Romm ${sessionLabel} session bootstrap failed.`;
      if (!primingCompatibility.ok) {
        return `Romm login succeeded server-side, but browser cookie priming is blocked. ${primingCompatibility.reason || primingPlan.reason || 'No compatible shared cookie domain found.'} Launcharr host: ${primingPlan.configuredLauncharrHost || primingPlan.requestHost || 'unknown'}; Romm host: ${primingPlan.targetHost || 'unknown'}.`;
      }
      if (!probe.ok) {
        return `Romm login cookies were obtained${primedBrowser ? ' and primed in this browser' : ''}, but /api/users/me verification failed${probe.status ? ` (${probe.status})` : ''}${probe.error ? `: ${probe.error}` : '.'}`;
      }
      if (primedBrowser && primingPlan.mode === 'shared-domain' && primingPlan.cookieDomain) {
        return `Romm ${sessionLabel} session OK and browser cookies primed for ${primingPlan.cookieDomain}. ${probe.user?.username ? `Logged in as ${probe.user.username}` : `${usingAdminCredentials ? 'Admin' : 'Viewer'} user verified`}.`;
      }
      return `Romm ${sessionLabel} session OK${primedBrowser ? ' and browser cookies primed' : ''}. ${probe.user?.username ? `Logged in as ${probe.user.username}` : `${usingAdminCredentials ? 'Admin' : 'Viewer'} user verified`}.`;
    })();
  
    return res.json({
      ok: Boolean(bootstrap?.ok && probe.ok),
      message,
      diagnostics: {
        credentialMode,
        requestHost: primingPlan.requestHost,
        targetHost: primingPlan.targetHost,
        configuredLauncharrHost: primingPlan.configuredLauncharrHost,
        canPrimeBrowserCookies: primingPlan.canPrime,
        cookiePrimingCompatible: primingCompatibility.ok,
        cookiePrimingCompatibilityReason: primingCompatibility.reason,
        cookiePrimingMode: primingPlan.mode,
        cookieDomain: primingPlan.cookieDomain,
        baseLaunchUrl,
        cleanLaunchUrl,
        cookieNames,
        primedBrowser,
        authBaseUrl: String(bootstrap?.authBaseUrl || '').trim(),
        attemptedAuthBases: Array.isArray(bootstrap?.attemptedBases) ? bootstrap.attemptedBases : [],
        probe,
      },
    });
  });

  app.get('/api/romm/:kind', requireUser, async (req, res) => {
    const kind = String(req.params.kind || '').trim().toLowerCase();
    if (!['recently-added', 'consoles'].includes(kind)) {
      return res.status(400).json({ error: 'Unsupported Romm endpoint.' });
    }
  
    const config = loadConfig();
    const apps = config.apps || [];
    const rommApp = apps.find((appItem) => normalizeAppId(appItem?.id) === 'romm');
    if (!rommApp) return res.status(404).json({ error: 'Romm app is not configured.' });
    if (!canAccessDashboardApp(config, rommApp, getEffectiveRole(req))) {
      return res.status(403).json({ error: 'Romm dashboard access denied.' });
    }
  
    const candidates = resolveAppApiCandidates(rommApp, req);
    if (!candidates.length) return res.status(400).json({ error: 'Missing Romm URL.' });
  
    const apiKey = String(rommApp.apiKey || '').trim();
    const authHeader = buildBasicAuthHeader(rommApp.username || '', rommApp.password || '');
    const headers = {
      Accept: 'application/json',
    };
    if (apiKey) {
      headers['X-API-Key'] = apiKey;
      headers['X-API-KEY'] = apiKey;
      if (!authHeader) headers.Authorization = /^bearer\s+/i.test(apiKey) ? apiKey : `Bearer ${apiKey}`;
    }
    if (authHeader) headers.Authorization = authHeader;
  
    const effectiveRole = getEffectiveRole(req);
    let rommSessionHeaders = null;
    let rommSessionBootstrapAttempted = false;
    async function getRommSessionHeaders() {
      if (rommSessionBootstrapAttempted) return rommSessionHeaders;
      rommSessionBootstrapAttempted = true;
  
      const baseLaunchUrl = String(resolveLaunchUrl(rommApp, req) || '').trim();
      if (!baseLaunchUrl) return null;
  
      let credentialedLaunchUrl = resolveRoleAwareLaunchUrl(rommApp, req, baseLaunchUrl, effectiveRole) || baseLaunchUrl;
      if (!hasEmbeddedUrlCredentials(credentialedLaunchUrl)) {
        const roleText = String(effectiveRole || '').trim().toLowerCase();
        const prefersViewer = roleText === 'user' || roleText === 'co-admin';
        const loginUsername = String(
          (prefersViewer ? (rommApp.viewerUsername || rommApp.username) : rommApp.username) || '',
        ).trim();
        const loginPassword = String(
          (prefersViewer ? (rommApp.viewerPassword || rommApp.password) : rommApp.password) || '',
        );
        credentialedLaunchUrl = injectBasicAuthIntoUrl(baseLaunchUrl, loginUsername, loginPassword);
      }
      if (!hasEmbeddedUrlCredentials(credentialedLaunchUrl)) return null;
  
      const bootstrap = await bootstrapRommIframeSession({
        req,
        launchUrl: credentialedLaunchUrl,
        authBaseCandidates: candidates,
      });
      if (!bootstrap?.ok) return null;
  
      const cookieHeader = buildCookieHeaderFromSetCookies(bootstrap.setCookies || []);
      if (!cookieHeader) return null;
      rommSessionHeaders = { Accept: 'application/json', Cookie: cookieHeader };
      const csrfToken = getRommCsrfTokenFromSetCookies(bootstrap.setCookies || []);
      if (csrfToken) rommSessionHeaders['x-csrftoken'] = csrfToken;
      return rommSessionHeaders;
    }
  
    async function fetchRommApi(urlString, requestHeaders) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);
      try {
        return await fetch(urlString, {
          headers: requestHeaders,
          signal: controller.signal,
        });
      } finally {
        clearTimeout(timeout);
      }
    }
  
    const endpointPlans = kind === 'consoles'
      ? [
        { path: 'api/platforms' },
        { path: 'api/v1/platforms' },
        { path: 'api/consoles' },
        { path: 'api/v1/consoles' },
        { path: 'api/systems' },
        { path: 'api/v1/systems' },
      ]
      : (() => {
        const configuredProbeLimit = Number(rommApp?.rommRecentProbeLimit);
        const probeLimit = Number.isFinite(configuredProbeLimit) && configuredProbeLimit > 0
          ? Math.min(200, Math.max(50, Math.round(configuredProbeLimit)))
          : 50;
        const probeLimitText = String(probeLimit);
        return [
          { path: 'api/games/recent', query: { limit: probeLimitText } },
          { path: 'api/v1/games/recent', query: { limit: probeLimitText } },
          { path: 'api/games/recently-added', query: { limit: probeLimitText } },
          { path: 'api/v1/games/recently-added', query: { limit: probeLimitText } },
          { path: 'api/roms/recent', query: { limit: probeLimitText } },
          { path: 'api/roms/recently-added', query: { limit: probeLimitText } },
          { path: 'api/roms', query: { order_by: 'id', order_dir: 'desc', with_char_index: 'false', with_filter_values: 'false', limit: probeLimitText } },
          { path: 'api/roms', query: { order_by: 'updated_at', order_dir: 'desc', with_char_index: 'false', with_filter_values: 'false', limit: probeLimitText } },
          { path: 'api/roms', query: { order_by: 'created_at', order_dir: 'desc', with_char_index: 'false', with_filter_values: 'false', limit: probeLimitText } },
          { path: 'api/roms', query: { sort: 'created_at', order: 'desc', limit: probeLimitText } },
          { path: 'api/v1/roms', query: { sort: 'created_at', order: 'desc', limit: probeLimitText } },
        ];
      })();
  
    let lastError = '';
    for (let candidateIndex = 0; candidateIndex < candidates.length; candidateIndex += 1) {
      const baseUrl = candidates[candidateIndex];
      if (!baseUrl) continue;
      for (let planIndex = 0; planIndex < endpointPlans.length; planIndex += 1) {
        const endpoint = endpointPlans[planIndex];
        try {
          const url = buildAppApiUrl(baseUrl, endpoint.path);
          Object.entries(endpoint.query || {}).forEach(([key, value]) => {
            if (value !== undefined && value !== null) {
              url.searchParams.set(key, String(value));
            }
          });
          let response;
          response = await fetchRommApi(url.toString(), headers);
          if (!response.ok && (response.status === 401 || response.status === 403)) {
            const sessionHeaders = await getRommSessionHeaders();
            if (sessionHeaders) {
              response = await fetchRommApi(url.toString(), sessionHeaders);
            }
          }
          const text = await response.text();
          if (!response.ok) {
            lastError = `Romm request failed (${response.status}) via ${baseUrl}.`;
            continue;
          }
          const payload = text ? JSON.parse(text) : {};
          const list = extractRommList(payload, kind);
          if (!Array.isArray(list)) {
            lastError = `Unexpected Romm response format via ${baseUrl}.`;
            continue;
          }
          const mapper = kind === 'consoles' ? mapRommConsoleItem : mapRommRecentlyAddedItem;
          const mapped = list
            .map((entry, sourceIndex) => ({ item: mapper(entry, baseUrl), sourceIndex }))
            .filter(({ item }) => Boolean(item?.title));
          let ordered = mapped.map(({ item }) => item);
          if (kind === 'consoles') {
            ordered = mapped.slice().sort((leftEntry, rightEntry) => {
              const leftSort = parseFiniteNumber(leftEntry?.item?.sortTs, 0);
              const rightSort = parseFiniteNumber(rightEntry?.item?.sortTs, 0);
              if (rightSort !== leftSort) return rightSort - leftSort;
              return String(leftEntry?.item?.title || '').localeCompare(String(rightEntry?.item?.title || ''));
            }).map(({ item }) => item);
          } else {
            ordered = mapped
              .slice()
              .sort((leftEntry, rightEntry) => {
                const leftSort = parseFiniteNumber(leftEntry?.item?.sortTs, 0);
                const rightSort = parseFiniteNumber(rightEntry?.item?.sortTs, 0);
                if (rightSort !== leftSort) return rightSort - leftSort;
                const leftSourceIndex = Number(leftEntry?.sourceIndex || 0);
                const rightSourceIndex = Number(rightEntry?.sourceIndex || 0);
                if (leftSourceIndex !== rightSourceIndex) return leftSourceIndex - rightSourceIndex;
                return String(leftEntry?.item?.title || '').localeCompare(String(rightEntry?.item?.title || ''));
              })
              .map(({ item }) => item);
          }
          const requestedLimitRaw = String(req.query?.limit || '').trim().toLowerCase();
          const defaultLimit = kind === 'consoles' ? 500 : 200;
          const maxCap = kind === 'consoles' ? 5000 : 1000;
          let itemLimit = defaultLimit;
          if (requestedLimitRaw === 'all') {
            itemLimit = maxCap;
          } else if (requestedLimitRaw) {
            const parsedLimit = Number(requestedLimitRaw);
            if (Number.isFinite(parsedLimit) && parsedLimit > 0) {
              itemLimit = Math.min(maxCap, Math.max(1, Math.round(parsedLimit)));
            }
          }
          const items = ordered.slice(0, itemLimit);
          return res.json({ items });
        } catch (err) {
          lastError = safeMessage(err) || `Failed to reach Romm via ${baseUrl}.`;
        }
      }
    }
  
    return res.status(502).json({ error: lastError || 'Failed to fetch Romm data.' });
  });

  // ─── MeTube overview ──────────────────────────────────────────────────────

  app.get('/api/metube/queue', requireUser, async (req, res) => {
    try {
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const metubeApp = apps.find((appItem) => getAppBaseId(normalizeAppId(appItem?.id)) === 'metube');
      if (!metubeApp) return res.status(404).json({ error: 'MeTube app is not configured.' });
      if (!canAccessDashboardApp(config, metubeApp, getEffectiveRole(req))) {
        return res.status(403).json({ error: 'MeTube dashboard access denied.' });
      }

      const candidates = resolveAppApiCandidates(metubeApp, req);
      if (!candidates.length) return res.status(400).json({ error: 'Missing MeTube URL.' });

      let lastError = '';

      for (const baseUrl of candidates) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 10000);
          let response;
          try {
            response = await fetch(
              buildAppApiUrl(baseUrl, 'history').toString(),
              { headers: { Accept: 'application/json' }, signal: controller.signal },
            );
          } finally {
            clearTimeout(timeoutId);
          }
          if (!response.ok) {
            lastError = `MeTube responded with status ${response.status}`;
            continue;
          }
          const json = await response.json().catch(() => ({}));
          // /history returns arrays: { queue: [...], pending: [...], done: [...] }
          const queueArr   = Array.isArray(json.queue)   ? json.queue   : [];
          const pendingArr = Array.isArray(json.pending) ? json.pending : [];
          const doneArr    = Array.isArray(json.done)    ? json.done    : [];

          const mapEntry = (item) => {
            const rawStatus = String(item?.status ?? item?.state ?? '').toLowerCase();
            let status = 'queued';
            if (rawStatus === 'downloading' || rawStatus === 'started') status = 'downloading';
            else if (rawStatus === 'error' || rawStatus === 'failed') status = 'error';
            else if (rawStatus === 'finished' || rawStatus === 'done' || rawStatus === 'complete') status = 'done';
            return {
              id: String(item?.id || ''),
              title: String(item?.title || item?.url || item?.id || 'Unknown').trim(),
              url: String(item?.url || '').trim(),
              status,
              percent: Number(item?.percent ?? item?.progress ?? 0) || 0,
              eta: Number(item?.eta ?? 0) || 0,
              speed: String(item?.speed || item?.download_speed || '').trim(),
            };
          };

          const queue = [...queueArr, ...pendingArr].map(mapEntry);
          const done  = doneArr.map(mapEntry);

          return res.json({ queue, done });
        } catch (err) {
          lastError = safeMessage(err) || `Failed to reach MeTube via ${baseUrl}.`;
        }
      }

      return res.status(502).json({ error: lastError || 'Failed to fetch MeTube data.' });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to fetch MeTube queue.' });
    }
  });

  // ─── Audiobookshelf overview ───────────────────────────────────────────────

  app.get('/api/audiobookshelf/recent', requireUser, async (req, res) => {
    try {
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const absApp = apps.find((appItem) => getAppBaseId(normalizeAppId(appItem?.id)) === 'audiobookshelf');
      if (!absApp) return res.status(404).json({ error: 'Audiobookshelf app is not configured.' });
      if (!canAccessDashboardApp(config, absApp, getEffectiveRole(req))) {
        return res.status(403).json({ error: 'Audiobookshelf dashboard access denied.' });
      }

      const candidates = resolveAppApiCandidates(absApp, req);
      if (!candidates.length) return res.status(400).json({ error: 'Missing Audiobookshelf URL.' });

      const apiKey = String(absApp.apiKey || '').trim();
      if (!apiKey) return res.status(400).json({ error: 'Audiobookshelf API key is not configured.' });

      const rawLimit = Number(req.query?.limit);
      const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(100, Math.max(1, Math.round(rawLimit))) : 20;

      const headers = { Accept: 'application/json', Authorization: `Bearer ${apiKey}` };
      let lastError = '';

      for (const baseUrl of candidates) {
        try {
          // Fetch all libraries first
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 10000);
          let libsResponse;
          try {
            libsResponse = await fetch(
              buildAppApiUrl(baseUrl, 'api/libraries').toString(),
              { headers, signal: controller.signal },
            );
          } finally {
            clearTimeout(timeoutId);
          }
          if (!libsResponse.ok) {
            lastError = `Audiobookshelf responded with status ${libsResponse.status}`;
            continue;
          }
          const libsJson = await libsResponse.json().catch(() => ({}));
          const libraries = Array.isArray(libsJson?.libraries) ? libsJson.libraries : [];
          if (!libraries.length) return res.json({ items: [] });

          // Fetch recently added from each library in parallel
          const perLib = Math.max(limit, 10);
          const libResults = await Promise.all(
            libraries.map(async (lib) => {
              try {
                const ctrl2 = new AbortController();
                const tid2 = setTimeout(() => ctrl2.abort(), 10000);
                let r;
                try {
                  r = await fetch(
                    buildAppApiUrl(baseUrl, `api/libraries/${encodeURIComponent(lib.id)}/items`).toString() +
                      `?sort=addedAt&desc=1&limit=${perLib}`,
                    { headers, signal: ctrl2.signal },
                  );
                } finally {
                  clearTimeout(tid2);
                }
                if (!r.ok) return [];
                const data = await r.json().catch(() => ({}));
                const results = Array.isArray(data?.results) ? data.results : (Array.isArray(data?.items) ? data.items : []);
                return results.map((item) => ({
                  ...item,
                  _libraryId: lib.id,
                  _mediaType: String(lib.mediaType || '').toLowerCase(),
                }));
              } catch (_err) {
                return [];
              }
            }),
          );

          // Merge and sort by addedAt descending
          const allItems = libResults.flat();
          allItems.sort((a, b) => {
            const aTs = Number(a.addedAt ?? 0);
            const bTs = Number(b.addedAt ?? 0);
            return bTs - aTs;
          });

          const items = allItems.slice(0, limit).map((item) => {
            const id = String(item?.id || '').trim();
            const mediaType = String(item?._mediaType || item?.mediaType || '').toLowerCase();
            const isPodcast = mediaType === 'podcast';
            const meta = isPodcast ? item?.media?.metadata : item?.media?.metadata;
            const title = String(meta?.title || item?.media?.metadata?.title || id || 'Unknown').trim();
            const author = String(meta?.authorName || meta?.author || '').trim();
            const addedAt = Number(item?.addedAt ?? 0);
            return {
              id,
              title,
              author,
              mediaType: isPodcast ? 'podcast' : 'book',
              addedAt: addedAt ? addedAt * 1000 : 0,
              thumbUrl: id ? `/api/audiobookshelf/cover/${encodeURIComponent(id)}` : '',
            };
          }).filter((item) => Boolean(item.id));

          return res.json({ items });
        } catch (err) {
          lastError = safeMessage(err) || `Failed to reach Audiobookshelf via ${baseUrl}.`;
        }
      }

      return res.status(502).json({ error: lastError || 'Failed to fetch Audiobookshelf data.' });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to fetch Audiobookshelf recent items.' });
    }
  });

  app.get('/api/audiobookshelf/cover/:itemId', requireUser, async (req, res) => {
    try {
      const itemId = String(req.params.itemId || '').trim();
      if (!itemId || !/^[\w-]+$/.test(itemId)) return res.status(400).send('');

      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const absApp = apps.find((appItem) => getAppBaseId(normalizeAppId(appItem?.id)) === 'audiobookshelf');
      if (!absApp) return res.status(404).send('');
      if (!canAccessDashboardApp(config, absApp, getEffectiveRole(req))) return res.status(403).send('');

      const candidates = resolveAppApiCandidates(absApp, req);
      if (!candidates.length) return res.status(400).send('');

      const apiKey = String(absApp.apiKey || '').trim();
      if (!apiKey) return res.status(400).send('');

      const fetchHeaders = { Authorization: `Bearer ${apiKey}` };

      for (const baseUrl of candidates) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 15000);
          let response;
          try {
            response = await fetch(
              buildAppApiUrl(baseUrl, `api/items/${encodeURIComponent(itemId)}/cover`).toString(),
              { headers: fetchHeaders, signal: controller.signal },
            );
          } finally {
            clearTimeout(timeoutId);
          }
          if (!response.ok) continue;
          const contentType = response.headers.get('content-type') || 'image/jpeg';
          res.set('Content-Type', contentType);
          res.set('Cache-Control', 'public, max-age=86400');
          const buffer = await response.arrayBuffer();
          return res.send(Buffer.from(buffer));
        } catch (_err) {
          // try next candidate
        }
      }

      return res.status(502).send('');
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to proxy Audiobookshelf cover.' });
    }
  });

  // ─── Tdarr overview ───────────────────────────────────────────────────────

  app.get('/api/tdarr/stats', requireUser, async (req, res) => {
    try {
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const tdarrApp = apps.find((appItem) => getAppBaseId(normalizeAppId(appItem?.id)) === 'tdarr');
      if (!tdarrApp) return res.status(404).json({ error: 'Tdarr app is not configured.' });
      if (!canAccessDashboardApp(config, tdarrApp, getEffectiveRole(req))) {
        return res.status(403).json({ error: 'Tdarr dashboard access denied.' });
      }

      const candidates = resolveAppApiCandidates(tdarrApp, req);
      if (!candidates.length) return res.status(400).json({ error: 'Missing Tdarr URL.' });

      const apiKey = String(tdarrApp.apiKey || '').trim();
      const fetchHeaders = { 'Content-Type': 'application/json', Accept: 'application/json' };
      if (apiKey) fetchHeaders['x-api-key'] = apiKey;
      const body = JSON.stringify({ data: { collection: 'StatisticsJSONDB', mode: 'getById', docID: 'statistics' } });
      let lastError = '';

      for (const baseUrl of candidates) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 10000);
          let response;
          try {
            response = await fetch(
              buildAppApiUrl(baseUrl, 'api/v2/cruddb').toString(),
              { method: 'POST', headers: fetchHeaders, body, signal: controller.signal },
            );
          } finally {
            clearTimeout(timeoutId);
          }
          if (!response.ok) {
            lastError = `Tdarr responded with status ${response.status}`;
            continue;
          }
          const j = await response.json().catch(() => ({}));
          const v = (a, b) => Number(j[a] ?? j[b] ?? 0);
          const queue     = v('table1ViewableCount', 'table1Count') + v('table4ViewableCount', 'table4Count');
          const processed = v('table2ViewableCount', 'table2Count') + v('table5ViewableCount', 'table5Count');
          const errored   = v('table3ViewableCount', 'table3Count') + v('table6ViewableCount', 'table6Count');
          const savedGb   = Math.abs(Number(j.sizeDiff ?? 0)).toFixed(2);
          return res.json({ queue, processed, errored, savedGb });
        } catch (err) {
          lastError = safeMessage(err) || `Failed to reach Tdarr via ${baseUrl}.`;
        }
      }

      return res.status(502).json({ error: lastError || 'Failed to fetch Tdarr stats.' });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to fetch Tdarr stats.' });
    }
  });

  // ─── Immich overview ──────────────────────────────────────────────────────

  app.get('/api/immich/recent', requireUser, async (req, res) => {
    try {
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const immichApp = apps.find((appItem) => getAppBaseId(normalizeAppId(appItem?.id)) === 'immich');
      if (!immichApp) return res.status(404).json({ error: 'Immich app is not configured.' });
      if (!canAccessDashboardApp(config, immichApp, getEffectiveRole(req))) {
        return res.status(403).json({ error: 'Immich dashboard access denied.' });
      }

      const candidates = resolveAppApiCandidates(immichApp, req);
      if (!candidates.length) return res.status(400).json({ error: 'Missing Immich URL.' });

      const apiKey = String(immichApp.apiKey || '').trim();
      if (!apiKey) return res.status(400).json({ error: 'Immich API key is not configured.' });

      const rawSize = Number(req.query?.size);
      const size = Number.isFinite(rawSize) && rawSize > 0 ? Math.min(100, Math.max(1, Math.round(rawSize))) : 20;

      const headers = { Accept: 'application/json', 'x-api-key': apiKey };
      let lastError = '';

      function extractImmichAssets(payload) {
        if (Array.isArray(payload)) return payload;
        if (Array.isArray(payload?.assets)) return payload.assets;
        if (Array.isArray(payload?.assets?.items)) return payload.assets.items;
        if (Array.isArray(payload?.items)) return payload.items;
        if (Array.isArray(payload?.results)) return payload.results;
        if (Array.isArray(payload?.data)) return payload.data;
        if (Array.isArray(payload?.data?.items)) return payload.data.items;
        return [];
      }

      function normalizeImmichItems(payload) {
        const rawItems = extractImmichAssets(payload);
        return rawItems
          .map((asset) => {
            const id = String(asset?.id || asset?.assetId || '').trim();
            if (!id) return null;
            const title = String(asset?.originalFileName || asset?.fileName || asset?.originalPath || id || 'Untitled').trim();
            const typeRaw = String(asset?.type || asset?.assetType || '').trim().toUpperCase();
            const mimeTypeRaw = String(asset?.originalMimeType || asset?.mimeType || '').trim().toLowerCase();
            const isVideo = typeRaw === 'VIDEO' || mimeTypeRaw.startsWith('video/');
            const date = asset?.fileCreatedAt || asset?.localDateTime || asset?.createdAt || asset?.updatedAt || '';
            return {
              id,
              title,
              type: isVideo ? 'video' : 'photo',
              date,
              thumbUrl: `/api/immich/thumbnail/${encodeURIComponent(id)}`,
            };
          })
          .filter(Boolean);
      }

      for (const baseUrl of candidates) {
        try {
          const requestPlans = [
            {
              method: 'GET',
              url: buildAppApiUrl(baseUrl, 'api/assets').toString() + `?order=desc&size=${size}&withExif=false`,
              headers,
            },
            {
              method: 'GET',
              url: buildAppApiUrl(baseUrl, 'api/assets').toString() + `?size=${size}&page=1&withExif=false`,
              headers,
            },
            {
              method: 'GET',
              url: buildAppApiUrl(baseUrl, 'api/assets').toString() + `?take=${size}&skip=0&withExif=false`,
              headers,
            },
            {
              method: 'POST',
              url: buildAppApiUrl(baseUrl, 'api/search/metadata').toString(),
              headers: { ...headers, 'Content-Type': 'application/json' },
              body: JSON.stringify({
                page: 1,
                size,
                withExif: false,
                order: 'desc',
              }),
            },
          ];
          let hasOkEmptyResponse = false;
          for (const plan of requestPlans) {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 10000);
            let response;
            try {
              response = await fetch(plan.url, {
                method: plan.method,
                headers: plan.headers,
                body: plan.body,
                signal: controller.signal,
              });
            } finally {
              clearTimeout(timeoutId);
            }
            if (!response.ok) {
              lastError = `Immich responded with status ${response.status}`;
              continue;
            }
            const json = await response.json().catch(() => ({}));
            const items = normalizeImmichItems(json);
            if (items.length) {
              return res.json({ items });
            }
            hasOkEmptyResponse = true;
          }
          if (hasOkEmptyResponse) {
            return res.json({ items: [] });
          }
        } catch (err) {
          lastError = safeMessage(err) || `Failed to reach Immich via ${baseUrl}.`;
        }
      }

      return res.status(502).json({ error: lastError || 'Failed to fetch Immich data.' });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to fetch Immich recent assets.' });
    }
  });

  app.get('/api/immich/thumbnail/:assetId', requireUser, async (req, res) => {
    try {
      const assetId = String(req.params.assetId || '').trim();
      if (!assetId || !/^[\w-]+$/.test(assetId)) return res.status(400).send('');

      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const immichApp = apps.find((appItem) => getAppBaseId(normalizeAppId(appItem?.id)) === 'immich');
      if (!immichApp) return res.status(404).send('');
      if (!canAccessDashboardApp(config, immichApp, getEffectiveRole(req))) {
        return res.status(403).send('');
      }

      const candidates = resolveAppApiCandidates(immichApp, req);
      if (!candidates.length) return res.status(400).send('');

      const apiKey = String(immichApp.apiKey || '').trim();
      if (!apiKey) return res.status(400).send('');

      const thumbSize = String(req.query?.size || 'preview').trim().toLowerCase() === 'thumbnail' ? 'thumbnail' : 'preview';
      const fetchHeaders = { 'x-api-key': apiKey };

      for (const baseUrl of candidates) {
        try {
          const format = thumbSize === 'thumbnail' ? 'JPEG' : 'WEBP';
          const thumbUrls = [
            buildAppApiUrl(baseUrl, `api/assets/${encodeURIComponent(assetId)}/thumbnail`).toString() + `?size=${thumbSize}`,
            buildAppApiUrl(baseUrl, `api/assets/${encodeURIComponent(assetId)}/thumbnail`).toString() + `?format=${format}`,
            buildAppApiUrl(baseUrl, `api/assets/${encodeURIComponent(assetId)}/thumbnail`).toString(),
          ];
          for (const thumbUrl of thumbUrls) {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 15000);
            let response;
            try {
              response = await fetch(thumbUrl, { headers: fetchHeaders, signal: controller.signal });
            } finally {
              clearTimeout(timeoutId);
            }
            if (!response.ok) continue;
            const contentType = response.headers.get('content-type') || 'image/jpeg';
            res.set('Content-Type', contentType);
            res.set('Cache-Control', 'public, max-age=86400');
            const buffer = await response.arrayBuffer();
            return res.send(Buffer.from(buffer));
          }
        } catch (_err) {
          // try next candidate
        }
      }

      return res.status(502).send('');
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to proxy Immich thumbnail.' });
    }
  });

  // ─── Wizarr overview ──────────────────────────────────────────────────────

  app.get('/api/wizarr/overview', requireUser, async (req, res) => {
    try {
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const wizarrApp = apps.find((appItem) => getAppBaseId(normalizeAppId(appItem?.id)) === 'wizarr');
      if (!wizarrApp) return res.status(404).json({ error: 'Wizarr app is not configured.' });
      if (!canAccessDashboardApp(config, wizarrApp, getEffectiveRole(req))) {
        return res.status(403).json({ error: 'Wizarr dashboard access denied.' });
      }

      const candidates = resolveAppApiCandidates(wizarrApp, req);
      if (!candidates.length) return res.status(400).json({ error: 'Missing Wizarr URL.' });

      const apiKey = String(wizarrApp.apiKey || '').trim();
      const headers = { Accept: 'application/json' };
      if (apiKey) headers['X-API-Key'] = apiKey;

      let lastError = '';

      for (const baseUrl of candidates) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 10000);
          let usersRes, invitationsRes;
          try {
            [usersRes, invitationsRes] = await Promise.all([
              fetch(buildAppApiUrl(baseUrl, 'api/users').toString(), { headers, signal: controller.signal }),
              fetch(buildAppApiUrl(baseUrl, 'api/invitations').toString(), { headers, signal: controller.signal }),
            ]);
          } finally {
            clearTimeout(timeoutId);
          }

          if (!usersRes.ok && !invitationsRes.ok) {
            lastError = `Wizarr responded with status ${usersRes.status}`;
            continue;
          }

          const usersJson = usersRes.ok ? await usersRes.json().catch(() => []) : [];
          const invJson = invitationsRes.ok ? await invitationsRes.json().catch(() => []) : [];
          const users = Array.isArray(usersJson) ? usersJson : (Array.isArray(usersJson?.data) ? usersJson.data : []);
          const invitations = Array.isArray(invJson) ? invJson : (Array.isArray(invJson?.data) ? invJson.data : []);

          return res.json({ users, invitations });
        } catch (err) {
          lastError = safeMessage(err) || `Failed to reach Wizarr via ${baseUrl}.`;
        }
      }

      return res.status(502).json({ error: lastError || 'Failed to fetch Wizarr data.' });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to fetch Wizarr overview.' });
    }
  });

  // ─── Uptime Kuma overview ─────────────────────────────────────────────────

  app.get('/api/uptime-kuma/status', requireUser, async (req, res) => {
    try {
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const ukApp = apps.find((appItem) => getAppBaseId(normalizeAppId(appItem?.id)) === 'uptime-kuma');
      if (!ukApp) return res.status(404).json({ error: 'Uptime Kuma app is not configured.' });
      if (!canAccessDashboardApp(config, ukApp, getEffectiveRole(req))) {
        return res.status(403).json({ error: 'Uptime Kuma dashboard access denied.' });
      }

      const slug = String(ukApp.uptimeKumaSlug || 'default').trim();
      const candidates = resolveAppApiCandidates(ukApp, req);
      if (!candidates.length) return res.status(400).json({ error: 'Missing Uptime Kuma URL.' });

      let lastError = null;
      for (const baseUrl of candidates) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 12000);
        try {
          const encodedSlug = encodeURIComponent(slug);
          const pageUrl = buildAppApiUrl(baseUrl, `api/status-page/${encodedSlug}`).toString();
          const hbUrl = buildAppApiUrl(baseUrl, `api/status-page/heartbeat/${encodedSlug}`).toString();
          const headers = { Accept: 'application/json' };

          // Fetch status page (names + groups) and heartbeats in parallel
          const [pageRes, hbRes] = await Promise.all([
            fetch(pageUrl, { headers, signal: controller.signal }),
            fetch(hbUrl, { headers, signal: controller.signal }),
          ]);
          clearTimeout(timeout);

          if (!pageRes.ok && !hbRes.ok) {
            lastError = `HTTP ${pageRes.status}/${hbRes.status} from ${baseUrl}`;
            continue;
          }

          const pagePayload = pageRes.ok ? await pageRes.json().catch(() => ({})) : {};
          const hbPayload = hbRes.ok ? await hbRes.json().catch(() => ({})) : {};

          const heartbeatList = hbPayload?.heartbeatList ?? {};

          // Build a monitorId→{name} map from the public group list
          const nameById = {};
          const publicGroupList = Array.isArray(pagePayload?.publicGroupList) ? pagePayload.publicGroupList : [];
          for (const group of publicGroupList) {
            for (const mon of (Array.isArray(group.monitorList) ? group.monitorList : [])) {
              const id = String(mon.id ?? '');
              if (id) nameById[id] = String(mon.name || id).trim();
            }
          }

          // Helper: extract status array + compute uptime% from a heartbeat array
          function summariseBeats(rawBeats) {
            const arr = Array.isArray(rawBeats) ? rawBeats : [];
            const statuses = arr.map((b) => Number(b?.status ?? -1));
            const up = statuses.filter((s) => s === 1).length;
            const total = statuses.length;
            const uptime = total > 0 ? Math.round((up / total) * 1000) / 10 : -1;
            return { statuses, uptime };
          }

          // Build groups preserving status-page order; fall back to flat list if no groups
          let groups;
          if (publicGroupList.length > 0) {
            groups = publicGroupList.map((group) => {
              const monitors = (Array.isArray(group.monitorList) ? group.monitorList : []).map((mon) => {
                const id = String(mon.id ?? '');
                const rawBeats = heartbeatList[id];
                const { statuses, uptime } = summariseBeats(rawBeats);
                const latest = Array.isArray(rawBeats) ? rawBeats[rawBeats.length - 1] : (rawBeats || {});
                return {
                  id,
                  name: String(mon.name || id).trim(),
                  status: Number(latest?.status ?? -1),
                  msg: String(latest?.msg || '').trim(),
                  statuses,
                  uptime,
                };
              });
              return { name: String(group.name || 'Monitors').trim(), monitors };
            });
          } else {
            // Heartbeat-only fallback — no names available
            const monitors = Object.entries(heartbeatList).map(([id, rawBeats]) => {
              const { statuses, uptime } = summariseBeats(rawBeats);
              const latest = Array.isArray(rawBeats) ? rawBeats[rawBeats.length - 1] : (rawBeats || {});
              return {
                id,
                name: nameById[id] || id,
                status: Number(latest?.status ?? -1),
                msg: String(latest?.msg || '').trim(),
                statuses,
                uptime,
              };
            });
            monitors.sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }));
            groups = [{ name: 'Monitors', monitors }];
          }

          return res.json({ slug, groups });
        } catch (err) {
          clearTimeout(timeout);
          lastError = safeMessage(err) || `Failed to reach Uptime Kuma via ${baseUrl}.`;
        }
      }

      return res.status(502).json({ error: lastError || 'Failed to fetch Uptime Kuma data.' });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to fetch Uptime Kuma status.' });
    }
  });

  // ─── Guacamole overview ───────────────────────────────────────────────────

  app.get('/api/guacamole/overview', requireUser, async (req, res) => {
    try {
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const guacApp = apps.find((appItem) => getAppBaseId(normalizeAppId(appItem?.id)) === 'guacamole');
      if (!guacApp) return res.status(404).json({ error: 'Guacamole app is not configured.' });
      if (!canAccessDashboardApp(config, guacApp, getEffectiveRole(req))) {
        return res.status(403).json({ error: 'Guacamole dashboard access denied.' });
      }

      const candidates = resolveAppApiCandidates(guacApp, req);
      if (!candidates.length) return res.status(400).json({ error: 'Missing Guacamole URL.' });

      const guacUsername = String(guacApp.username || '').trim();
      const guacPassword = String(guacApp.password || '').trim();
      let lastError = '';

      for (const baseUrl of candidates) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 12000);
        try {
          // Step 1: authenticate and get token
          const tokenBody = new URLSearchParams({ username: guacUsername, password: guacPassword }).toString();
          const tokenRes = await fetch(buildAppApiUrl(baseUrl, 'api/tokens').toString(), {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded', Accept: 'application/json' },
            body: tokenBody,
            signal: controller.signal,
          });
          if (!tokenRes.ok) {
            lastError = `Guacamole auth failed with status ${tokenRes.status}`;
            clearTimeout(timeout);
            continue;
          }
          const tokenPayload = await tokenRes.json().catch(() => ({}));
          const authToken = String(tokenPayload?.authToken || '').trim();
          const dataSource = String(tokenPayload?.dataSource || 'mysql').trim();
          if (!authToken) {
            lastError = 'Guacamole did not return an auth token.';
            clearTimeout(timeout);
            continue;
          }

          // Step 2: fetch active connections, connections list, and users in parallel
          const tokenParam = `token=${encodeURIComponent(authToken)}`;
          const [activeRes, connRes, usersRes] = await Promise.all([
            fetch(`${buildAppApiUrl(baseUrl, `api/session/data/${dataSource}/activeConnections`)}?${tokenParam}`, { headers: { Accept: 'application/json' }, signal: controller.signal }),
            fetch(`${buildAppApiUrl(baseUrl, `api/session/data/${dataSource}/connections`)}?${tokenParam}`, { headers: { Accept: 'application/json' }, signal: controller.signal }),
            fetch(`${buildAppApiUrl(baseUrl, `api/session/data/${dataSource}/users`)}?${tokenParam}`, { headers: { Accept: 'application/json' }, signal: controller.signal }),
          ]);
          clearTimeout(timeout);

          const activeJson = activeRes.ok ? await activeRes.json().catch(() => ({})) : {};
          const connJson = connRes.ok ? await connRes.json().catch(() => ({})) : {};
          const usersJson = usersRes.ok ? await usersRes.json().catch(() => ({})) : {};

          // Shape active sessions — each value has connection info nested
          const activeSessions = Object.values(activeJson || {}).map((s) => ({
            identifier: String(s?.identifier || ''),
            connectionName: String(s?.connection?.name || s?.connectionIdentifier || ''),
            protocol: String(s?.connection?.protocol || ''),
            username: String(s?.username || ''),
            startDate: s?.startDate || null,
            remoteHost: String(s?.remoteHost || ''),
          }));

          // Shape connections list
          const connectionsList = Object.values(connJson || {}).map((c) => ({
            identifier: String(c?.identifier || ''),
            name: String(c?.name || ''),
            protocol: String(c?.protocol || ''),
            parentIdentifier: String(c?.parentIdentifier || ''),
            activeConnections: Number(c?.activeConnections ?? 0),
          })).sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }));

          return res.json({ activeSessions, connections: connectionsList });
        } catch (err) {
          clearTimeout(timeout);
          lastError = safeMessage(err) || `Failed to reach Guacamole via ${baseUrl}.`;
        }
      }

      return res.status(502).json({ error: lastError || 'Failed to fetch Guacamole data.' });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to fetch Guacamole overview.' });
    }
  });

  // ─── Traefik overview ─────────────────────────────────────────────────────

  app.get('/api/traefik/overview', requireUser, async (req, res) => {
    try {
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const traefikApp = apps.find((appItem) => getAppBaseId(normalizeAppId(appItem?.id)) === 'traefik');
      if (!traefikApp) return res.status(404).json({ error: 'Traefik app is not configured.' });
      if (!canAccessDashboardApp(config, traefikApp, getEffectiveRole(req))) {
        return res.status(403).json({ error: 'Traefik dashboard access denied.' });
      }

      const candidates = resolveAppApiCandidates(traefikApp, req);
      if (!candidates.length) return res.status(400).json({ error: 'Missing Traefik URL.' });

      const apiKey = String(traefikApp.apiKey || '').trim();
      let lastError = '';

      for (const baseUrl of candidates) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 12000);
        try {
          const headers = { Accept: 'application/json' };
          if (apiKey) {
            headers['Authorization'] = `Bearer ${apiKey}`;
          } else if (traefikApp.username && traefikApp.password) {
            headers['Authorization'] = 'Basic ' + Buffer.from(`${traefikApp.username}:${traefikApp.password}`).toString('base64');
          }

          const [routersRes, servicesRes] = await Promise.all([
            fetch(buildAppApiUrl(baseUrl, 'api/http/routers').toString(), { headers, signal: controller.signal }),
            fetch(buildAppApiUrl(baseUrl, 'api/http/services').toString(), { headers, signal: controller.signal }),
          ]);
          clearTimeout(timeout);

          // If both fail, record the error and try the next candidate rather than silently returning empty data
          if (!routersRes.ok && !servicesRes.ok) {
            lastError = `Traefik API returned ${routersRes.status} for routers and ${servicesRes.status} for services. Ensure the Traefik API is enabled (--api or --api.insecure=true).`;
            continue;
          }

          const routersJson = routersRes.ok ? await routersRes.json().catch(() => []) : [];
          const servicesJson = servicesRes.ok ? await servicesRes.json().catch(() => []) : [];

          const routers = (Array.isArray(routersJson) ? routersJson : Object.values(routersJson || {}))
            .map((r) => ({
              name: String(r?.name || r?.routerName || ''),
              status: String(r?.status || 'unknown'),
              entryPoints: Array.isArray(r?.entryPoints) ? r.entryPoints.join(', ') : String(r?.entryPoints || ''),
              service: String(r?.service || ''),
              rule: String(r?.rule || ''),
            }))
            .sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }));

          const services = (Array.isArray(servicesJson) ? servicesJson : Object.values(servicesJson || {}))
            .map((s) => ({
              name: String(s?.name || s?.serviceName || ''),
              status: String(s?.status || 'unknown'),
              type: String(s?.type || ''),
              serverCount: Number(s?.loadBalancer?.servers?.length ?? 0),
            }))
            .sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }));

          return res.json({ routers, services });
        } catch (err) {
          clearTimeout(timeout);
          lastError = safeMessage(err) || `Failed to reach Traefik via ${baseUrl}.`;
        }
      }

      return res.status(502).json({ error: lastError || 'Failed to fetch Traefik data.' });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to fetch Traefik overview.' });
    }
  });

  // ─── Widget Bar CRUD ──────────────────────────────────────────────────────

  app.get('/api/widget-bars', requireSettingsAdmin, (req, res) => {
    try {
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const bars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
      return res.json({ ok: true, items: bars });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to list widget bars.' });
    }
  });

  app.post('/api/widget-bars', requireSettingsAdmin, (req, res) => {
    try {
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const body = req.body || {};
      const nextOrder = resolveNextWidgetBarOrder(config);
      const normalized = normalizeWidgetBar({
        id: normalizeWidgetBarId(body.id || '') || normalizeWidgetBarId(buildWidgetBarId()),
        name: String(body.name || '').trim() || 'Widget Bar',
        icon: body.icon,
        visibilityRoles: body.visibilityRoles,
        visibilityRole: body.visibilityRole,
        refreshSeconds: body.refreshSeconds,
        order: nextOrder,
        widgets: [],
      });
      if (!normalized) return res.status(400).json({ error: 'Invalid widget bar payload.' });
      // Ensure unique ID
      const existing = Array.isArray(config?.widgetBars) ? config.widgetBars : [];
      const existingIds = new Set(existing.map((b) => normalizeWidgetBarId(b?.id || '')).filter(Boolean));
      let barId = normalized.id;
      if (existingIds.has(barId)) {
        let suffix = 2;
        while (existingIds.has(`${barId}-${suffix}`)) suffix += 1;
        barId = `${barId}-${suffix}`;
      }
      const newBars = serializeWidgetBars([...resolveWidgetBars(config, apps, 'admin', { includeHidden: true }), { ...normalized, id: barId }]);
      // New bars start hidden from all dashboards — user must explicitly add via dashboard manager
      const elementKey = `widget-bar:${barId}`;
      const nextDashboardRemovedElements = {
        ...((config?.dashboardRemovedElements && typeof config.dashboardRemovedElements === 'object') ? config.dashboardRemovedElements : {}),
        [elementKey]: true,
      };
      const rawDashboards = Array.isArray(config?.dashboards) ? config.dashboards : null;
      const nextDashboards = rawDashboards
        ? rawDashboards.map((dash) => {
            if (!dash?.state?.dashboardRemovedElements || typeof dash.state.dashboardRemovedElements !== 'object') return dash;
            return { ...dash, state: { ...dash.state, dashboardRemovedElements: { ...dash.state.dashboardRemovedElements, [elementKey]: true } } };
          })
        : null;
      saveConfig({
        ...config,
        widgetBars: newBars,
        dashboardRemovedElements: nextDashboardRemovedElements,
        ...(nextDashboards ? { dashboards: nextDashboards } : {}),
      });
      const savedConfig = loadConfig();
      const savedBars = resolveWidgetBars(savedConfig, Array.isArray(savedConfig?.apps) ? savedConfig.apps : apps, 'admin', { includeHidden: true });
      const savedBar = savedBars.find((b) => b.id === barId) || null;
      return res.json({ ok: true, item: savedBar, items: savedBars });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to create widget bar.' });
    }
  });

  app.put('/api/widget-bars/:id', requireSettingsAdmin, (req, res) => {
    try {
      const barId = normalizeWidgetBarId(req.params.id || '');
      if (!barId) return res.status(400).json({ error: 'Invalid widget bar id.' });
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const bars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
      const index = bars.findIndex((b) => b.id === barId);
      if (index === -1) return res.status(404).json({ error: 'Widget bar not found.' });
      const existing = bars[index];
      const body = req.body || {};
      const updated = normalizeWidgetBar({
        ...existing,
        name: body.name !== undefined ? String(body.name || '').trim() || existing.name : existing.name,
        icon: body.icon !== undefined ? body.icon : existing.icon,
        visibilityRoles: body.visibilityRoles !== undefined ? body.visibilityRoles : existing.visibilityRoles,
        visibilityRole: body.visibilityRole !== undefined ? body.visibilityRole : existing.visibilityRole,
        refreshSeconds: body.refreshSeconds !== undefined ? body.refreshSeconds : existing.refreshSeconds,
        order: body.order !== undefined ? Number(body.order) : existing.order,
        widgets: existing.widgets,
      });
      if (!updated) return res.status(400).json({ error: 'Invalid widget bar payload.' });
      const nextBars = bars.map((b, i) => (i === index ? updated : b));
      saveConfig({ ...config, widgetBars: serializeWidgetBars(nextBars) });
      const savedConfig = loadConfig();
      const savedBars = resolveWidgetBars(savedConfig, Array.isArray(savedConfig?.apps) ? savedConfig.apps : apps, 'admin', { includeHidden: true });
      const savedBar = savedBars.find((b) => b.id === barId) || null;
      return res.json({ ok: true, item: savedBar, items: savedBars });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to update widget bar.' });
    }
  });

  app.delete('/api/widget-bars/:id', requireSettingsAdmin, (req, res) => {
    try {
      const barId = normalizeWidgetBarId(req.params.id || '');
      if (!barId) return res.status(400).json({ error: 'Invalid widget bar id.' });
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const bars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
      if (!bars.find((b) => b.id === barId)) return res.status(404).json({ error: 'Widget bar not found.' });
      const nextBars = bars.filter((b) => b.id !== barId);
      // Also remove from dashboardRemovedElements if present
      const dashboardRemovedElements = (config?.dashboardRemovedElements && typeof config.dashboardRemovedElements === 'object')
        ? { ...config.dashboardRemovedElements }
        : {};
      delete dashboardRemovedElements[`widget-bar:${barId}`];
      saveConfig({ ...config, widgetBars: serializeWidgetBars(nextBars), dashboardRemovedElements });
      const savedConfig = loadConfig();
      const savedBars = resolveWidgetBars(savedConfig, Array.isArray(savedConfig?.apps) ? savedConfig.apps : apps, 'admin', { includeHidden: true });
      return res.json({ ok: true, items: savedBars });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to delete widget bar.' });
    }
  });

  // Backward-compat: add widget to first row of bar
  app.post('/api/widget-bars/:id/widgets', requireSettingsAdmin, (req, res) => {
    try {
      const barId = normalizeWidgetBarId(req.params.id || '');
      if (!barId) return res.status(400).json({ error: 'Invalid widget bar id.' });
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const bars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
      const barIndex = bars.findIndex((b) => b.id === barId);
      if (barIndex === -1) return res.status(404).json({ error: 'Widget bar not found.' });
      const bar = bars[barIndex];
      const body = req.body || {};
      const appId = String(body.appId || '').trim();
      if (!appId) return res.status(400).json({ error: 'appId is required.' });
      // Collect all widget IDs across all rows for uniqueness
      const existingWidgetIds = new Set(bar.rows.flatMap((r) => r.widgets.map((w) => normalizeWidgetId(w.id || ''))).filter(Boolean));
      const baseWidgetId = normalizeWidgetId(`wg-${appId}`);
      let widgetId = baseWidgetId;
      let suffix = 2;
      while (existingWidgetIds.has(widgetId)) { widgetId = `${baseWidgetId}-${suffix}`; suffix += 1; }
      const newWidget = normalizeWidgetInBar({ id: widgetId, appId });
      if (!newWidget) return res.status(400).json({ error: 'Invalid widget entry.' });
      // Add to first row
      const firstRow = bar.rows[0];
      const updatedRows = bar.rows.map((r, i) => i === 0 ? { ...firstRow, widgets: [...firstRow.widgets, newWidget] } : r);
      const updatedBar = normalizeWidgetBar({ ...bar, rows: updatedRows });
      if (!updatedBar) return res.status(400).json({ error: 'Failed to update widget bar.' });
      const nextBars = bars.map((b, i) => (i === barIndex ? updatedBar : b));
      saveConfig({ ...config, widgetBars: serializeWidgetBars(nextBars) });
      const savedConfig = loadConfig();
      const savedBars = resolveWidgetBars(savedConfig, Array.isArray(savedConfig?.apps) ? savedConfig.apps : apps, 'admin', { includeHidden: true });
      const savedBar = savedBars.find((b) => b.id === barId) || null;
      return res.json({ ok: true, item: savedBar, items: savedBars });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to add widget to bar.' });
    }
  });

  // Backward-compat: remove widget from whichever row contains it
  app.delete('/api/widget-bars/:id/widgets/:widgetId', requireSettingsAdmin, (req, res) => {
    try {
      const barId = normalizeWidgetBarId(req.params.id || '');
      const widgetId = normalizeWidgetId(req.params.widgetId || '');
      if (!barId || !widgetId) return res.status(400).json({ error: 'Invalid bar or widget id.' });
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const bars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
      const barIndex = bars.findIndex((b) => b.id === barId);
      if (barIndex === -1) return res.status(404).json({ error: 'Widget bar not found.' });
      const bar = bars[barIndex];
      const totalBefore = bar.rows.reduce((s, r) => s + r.widgets.length, 0);
      const updatedRows = bar.rows.map((r) => ({ ...r, widgets: r.widgets.filter((w) => normalizeWidgetId(w.id || '') !== widgetId) }));
      const totalAfter = updatedRows.reduce((s, r) => s + r.widgets.length, 0);
      if (totalAfter === totalBefore) return res.status(404).json({ error: 'Widget not found in bar.' });
      const updatedBar = normalizeWidgetBar({ ...bar, rows: updatedRows });
      const nextBars = bars.map((b, i) => (i === barIndex ? updatedBar : b));
      saveConfig({ ...config, widgetBars: serializeWidgetBars(nextBars) });
      const savedConfig = loadConfig();
      const savedBars = resolveWidgetBars(savedConfig, Array.isArray(savedConfig?.apps) ? savedConfig.apps : apps, 'admin', { includeHidden: true });
      const savedBar = savedBars.find((b) => b.id === barId) || null;
      return res.json({ ok: true, item: savedBar, items: savedBars });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to remove widget from bar.' });
    }
  });

  // ─── Widget Bar Row CRUD ───────────────────────────────────────────────────

  app.post('/api/widget-bars/:id/rows', requireSettingsAdmin, (req, res) => {
    try {
      const barId = normalizeWidgetBarId(req.params.id || '');
      if (!barId) return res.status(400).json({ error: 'Invalid widget bar id.' });
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const bars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
      const barIndex = bars.findIndex((b) => b.id === barId);
      if (barIndex === -1) return res.status(404).json({ error: 'Widget bar not found.' });
      const bar = bars[barIndex];
      const body = req.body || {};
      const beforeRowId = normalizeWidgetBarId(body.beforeRowId || '');
      const newRow = { id: buildWidgetRowId(), order: resolveNextWidgetRowOrder(bar), settings: normalizeWidgetRowSettings({}), widgets: [] };
      const nextRows = Array.isArray(bar.rows) ? [...bar.rows] : [];
      if (beforeRowId) {
        const beforeIndex = nextRows.findIndex((row) => normalizeWidgetBarId(row.id || '') === beforeRowId);
        if (beforeIndex === -1) return res.status(404).json({ error: 'Target row not found.' });
        nextRows.splice(beforeIndex, 0, newRow);
      } else {
        nextRows.push(newRow);
      }
      const orderedRows = nextRows.map((row, index) => ({ ...row, order: (index + 1) * 10 }));
      const updatedBar = normalizeWidgetBar({ ...bar, rows: orderedRows });
      if (!updatedBar) return res.status(400).json({ error: 'Failed to add row.' });
      const nextBars = bars.map((b, i) => (i === barIndex ? updatedBar : b));
      saveConfig({ ...config, widgetBars: serializeWidgetBars(nextBars) });
      const savedConfig = loadConfig();
      const savedBars = resolveWidgetBars(savedConfig, Array.isArray(savedConfig?.apps) ? savedConfig.apps : apps, 'admin', { includeHidden: true });
      const savedBar = savedBars.find((b) => b.id === barId) || null;
      return res.json({ ok: true, item: savedBar, items: savedBars });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to add row.' });
    }
  });

  app.put('/api/widget-bars/:id/rows/:rowId', requireSettingsAdmin, (req, res) => {
    try {
      const barId = normalizeWidgetBarId(req.params.id || '');
      const rowId = normalizeWidgetBarId(req.params.rowId || '');
      if (!barId || !rowId) return res.status(400).json({ error: 'Invalid bar or row id.' });
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const bars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
      const barIndex = bars.findIndex((b) => b.id === barId);
      if (barIndex === -1) return res.status(404).json({ error: 'Widget bar not found.' });
      const bar = bars[barIndex];
      const rowIndex = bar.rows.findIndex((r) => r.id === rowId);
      if (rowIndex === -1) return res.status(404).json({ error: 'Row not found.' });
      const existingRow = bar.rows[rowIndex];
      const body = req.body || {};
      const newSettings = normalizeWidgetRowSettings({
        maxCols: body.maxCols !== undefined ? body.maxCols : existingRow.settings.maxCols,
        fixedWidth: body.fixedWidth !== undefined ? body.fixedWidth : existingRow.settings.fixedWidth,
        scroll: body.scroll !== undefined ? body.scroll : existingRow.settings.scroll,
        fill: body.fill !== undefined ? body.fill : existingRow.settings.fill,
      });
      let nextWidgets = Array.isArray(existingRow.widgets) ? [...existingRow.widgets] : [];
      if (Array.isArray(body.widgetOrder)) {
        const widgetById = new Map(
          nextWidgets
            .map((widget) => [normalizeWidgetId(widget?.id || ''), widget])
            .filter(([id]) => Boolean(id))
        );
        const usedIds = new Set();
        const orderedWidgets = [];
        for (const rawId of body.widgetOrder) {
          const id = normalizeWidgetId(rawId || '');
          if (!id || usedIds.has(id)) continue;
          const widget = widgetById.get(id);
          if (!widget) continue;
          orderedWidgets.push(widget);
          usedIds.add(id);
        }
        for (const widget of nextWidgets) {
          const id = normalizeWidgetId(widget?.id || '');
          if (!id || usedIds.has(id)) continue;
          orderedWidgets.push(widget);
          usedIds.add(id);
        }
        nextWidgets = orderedWidgets;
      }
      const updatedRow = { ...existingRow, settings: newSettings, widgets: nextWidgets };
      const updatedRows = bar.rows.map((r, i) => (i === rowIndex ? updatedRow : r));
      const updatedBar = normalizeWidgetBar({ ...bar, rows: updatedRows });
      if (!updatedBar) return res.status(400).json({ error: 'Failed to update row settings.' });
      const nextBars = bars.map((b, i) => (i === barIndex ? updatedBar : b));
      saveConfig({ ...config, widgetBars: serializeWidgetBars(nextBars) });
      const savedConfig = loadConfig();
      const savedBars = resolveWidgetBars(savedConfig, Array.isArray(savedConfig?.apps) ? savedConfig.apps : apps, 'admin', { includeHidden: true });
      const savedBar = savedBars.find((b) => b.id === barId) || null;
      return res.json({ ok: true, item: savedBar, items: savedBars });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to update row settings.' });
    }
  });

  app.delete('/api/widget-bars/:id/rows/:rowId', requireSettingsAdmin, (req, res) => {
    try {
      const barId = normalizeWidgetBarId(req.params.id || '');
      const rowId = normalizeWidgetBarId(req.params.rowId || '');
      if (!barId || !rowId) return res.status(400).json({ error: 'Invalid bar or row id.' });
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const bars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
      const barIndex = bars.findIndex((b) => b.id === barId);
      if (barIndex === -1) return res.status(404).json({ error: 'Widget bar not found.' });
      const bar = bars[barIndex];
      if (bar.rows.length <= 1) return res.status(400).json({ error: 'Cannot delete the last row.' });
      if (!bar.rows.find((r) => r.id === rowId)) return res.status(404).json({ error: 'Row not found.' });
      const updatedRows = bar.rows.filter((r) => r.id !== rowId);
      const updatedBar = normalizeWidgetBar({ ...bar, rows: updatedRows });
      const nextBars = bars.map((b, i) => (i === barIndex ? updatedBar : b));
      saveConfig({ ...config, widgetBars: serializeWidgetBars(nextBars) });
      const savedConfig = loadConfig();
      const savedBars = resolveWidgetBars(savedConfig, Array.isArray(savedConfig?.apps) ? savedConfig.apps : apps, 'admin', { includeHidden: true });
      const savedBar = savedBars.find((b) => b.id === barId) || null;
      return res.json({ ok: true, item: savedBar, items: savedBars });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to delete row.' });
    }
  });

  app.post('/api/widget-bars/:id/rows/:rowId/widgets', requireSettingsAdmin, (req, res) => {
    try {
      const barId = normalizeWidgetBarId(req.params.id || '');
      const rowId = normalizeWidgetBarId(req.params.rowId || '');
      if (!barId || !rowId) return res.status(400).json({ error: 'Invalid bar or row id.' });
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const bars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
      const barIndex = bars.findIndex((b) => b.id === barId);
      if (barIndex === -1) return res.status(404).json({ error: 'Widget bar not found.' });
      const bar = bars[barIndex];
      const rowIndex = bar.rows.findIndex((r) => r.id === rowId);
      if (rowIndex === -1) return res.status(404).json({ error: 'Row not found.' });
      const body = req.body || {};
      const systemType = String(body.systemType || '').trim().toLowerCase();
      const appId = String(body.appId || '').trim();
      if (!systemType && !appId) return res.status(400).json({ error: 'appId or systemType is required.' });
      if (systemType && appId) return res.status(400).json({ error: 'Provide either appId or systemType, not both.' });
      if (systemType && !SYSTEM_WIDGET_TYPE_BY_ID.has(systemType)) {
        return res.status(400).json({ error: `Unsupported system widget type: ${systemType}` });
      }
      // Widget IDs must be unique across all widget bars to avoid cross-bar collisions in DOM/data attributes.
      const existingWidgetIds = new Set(
        bars
          .flatMap((entryBar) => (Array.isArray(entryBar?.rows) ? entryBar.rows : []))
          .flatMap((rowEntry) => (Array.isArray(rowEntry?.widgets) ? rowEntry.widgets : []))
          .map((widgetEntry) => normalizeWidgetId(widgetEntry?.id || ''))
          .filter(Boolean)
      );
      const baseWidgetId = normalizeWidgetId(`wg-${systemType || appId}`);
      let widgetId = baseWidgetId;
      let suffix = 2;
      while (existingWidgetIds.has(widgetId)) { widgetId = `${baseWidgetId}-${suffix}`; suffix += 1; }
      const newWidgetEntry = systemType
        ? { id: widgetId, systemType, systemConfig: body.systemConfig || {} }
        : { id: widgetId, appId };
      const newWidget = normalizeWidgetInBar(newWidgetEntry);
      if (!newWidget) return res.status(400).json({ error: 'Invalid widget entry.' });
      const targetRow = bar.rows[rowIndex];
      const updatedRows = bar.rows.map((r, i) => i === rowIndex ? { ...targetRow, widgets: [...targetRow.widgets, newWidget] } : r);
      const updatedBar = normalizeWidgetBar({ ...bar, rows: updatedRows });
      if (!updatedBar) return res.status(400).json({ error: 'Failed to update widget bar.' });
      const nextBars = bars.map((b, i) => (i === barIndex ? updatedBar : b));
      saveConfig({ ...config, widgetBars: serializeWidgetBars(nextBars) });
      const savedConfig = loadConfig();
      const savedBars = resolveWidgetBars(savedConfig, Array.isArray(savedConfig?.apps) ? savedConfig.apps : apps, 'admin', { includeHidden: true });
      const savedBar = savedBars.find((b) => b.id === barId) || null;
      return res.json({ ok: true, item: savedBar, items: savedBars });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to add widget to row.' });
    }
  });

  app.put('/api/widget-bars/:id/rows/:rowId/widgets/:widgetId', requireSettingsAdmin, (req, res) => {
    try {
      const barId = normalizeWidgetBarId(req.params.id || '');
      const rowId = normalizeWidgetBarId(req.params.rowId || '');
      const widgetId = normalizeWidgetId(req.params.widgetId || '');
      if (!barId || !rowId || !widgetId) return res.status(400).json({ error: 'Invalid bar, row, or widget id.' });
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const bars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
      const barIndex = bars.findIndex((b) => b.id === barId);
      if (barIndex === -1) return res.status(404).json({ error: 'Widget bar not found.' });
      const bar = bars[barIndex];
      const rowIndex = bar.rows.findIndex((r) => r.id === rowId);
      if (rowIndex === -1) return res.status(404).json({ error: 'Row not found.' });
      const row = bar.rows[rowIndex];
      const widgetIndex = row.widgets.findIndex((w) => normalizeWidgetId(w.id || '') === widgetId);
      if (widgetIndex === -1) return res.status(404).json({ error: 'Widget not found in row.' });

      const body = req.body || {};
      const existingWidget = row.widgets[widgetIndex];
      const updatedWidget = normalizeWidgetInBar(
        existingWidget.systemType
          ? { ...existingWidget, systemConfig: body.systemConfig || existingWidget.systemConfig || {} }
          : {
              ...existingWidget,
              selectedMetricKeys: body.selectedMetricKeys,
              metricColumns: body.metricColumns,
              selectedLibraryKeys: body.selectedLibraryKeys,
            }
      );
      if (!updatedWidget) return res.status(400).json({ error: 'Invalid widget update.' });

      const nextWidgets = row.widgets.map((w, i) => (i === widgetIndex ? updatedWidget : w));
      const updatedRows = bar.rows.map((r, i) => (i === rowIndex ? { ...row, widgets: nextWidgets } : r));
      const updatedBar = normalizeWidgetBar({ ...bar, rows: updatedRows });
      if (!updatedBar) return res.status(400).json({ error: 'Failed to update widget.' });
      const nextBars = bars.map((b, i) => (i === barIndex ? updatedBar : b));
      saveConfig({ ...config, widgetBars: serializeWidgetBars(nextBars) });
      const savedConfig = loadConfig();
      const savedBars = resolveWidgetBars(savedConfig, Array.isArray(savedConfig?.apps) ? savedConfig.apps : apps, 'admin', { includeHidden: true });
      const savedBar = savedBars.find((b) => b.id === barId) || null;
      return res.json({ ok: true, item: savedBar, items: savedBars });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to update widget.' });
    }
  });

  app.delete('/api/widget-bars/:id/rows/:rowId/widgets/:widgetId', requireSettingsAdmin, (req, res) => {
    try {
      const barId = normalizeWidgetBarId(req.params.id || '');
      const rowId = normalizeWidgetBarId(req.params.rowId || '');
      const widgetId = normalizeWidgetId(req.params.widgetId || '');
      if (!barId || !rowId || !widgetId) return res.status(400).json({ error: 'Invalid bar, row, or widget id.' });
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const bars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
      const barIndex = bars.findIndex((b) => b.id === barId);
      if (barIndex === -1) return res.status(404).json({ error: 'Widget bar not found.' });
      const bar = bars[barIndex];
      const rowIndex = bar.rows.findIndex((r) => r.id === rowId);
      if (rowIndex === -1) return res.status(404).json({ error: 'Row not found.' });
      const row = bar.rows[rowIndex];
      const nextWidgets = row.widgets.filter((w) => normalizeWidgetId(w.id || '') !== widgetId);
      if (nextWidgets.length === row.widgets.length) return res.status(404).json({ error: 'Widget not found in row.' });
      const updatedRows = bar.rows.map((r, i) => i === rowIndex ? { ...row, widgets: nextWidgets } : r);
      const updatedBar = normalizeWidgetBar({ ...bar, rows: updatedRows });
      const nextBars = bars.map((b, i) => (i === barIndex ? updatedBar : b));
      saveConfig({ ...config, widgetBars: serializeWidgetBars(nextBars) });
      const savedConfig = loadConfig();
      const savedBars = resolveWidgetBars(savedConfig, Array.isArray(savedConfig?.apps) ? savedConfig.apps : apps, 'admin', { includeHidden: true });
      const savedBar = savedBars.find((b) => b.id === barId) || null;
      return res.json({ ok: true, item: savedBar, items: savedBars });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to remove widget from row.' });
    }
  });

  // ─── System Widget APIs ────────────────────────────────────────────────────

  app.get('/api/widget-deployment-summary', requireWidgetStatsAccess, async (req, res) => {
    try {
      const config = loadConfig();
      const targets = resolveWidgetMonitorTargets(config);
      const totalWidgets = targets.length;

      if (!totalWidgets) {
        return res.json({
          ok: true,
          online: 0,
          offline: 0,
          unknown: 0,
          total: 0,
          onlineWidgets: 0,
          offlineWidgets: 0,
          unknownWidgets: 0,
          totalWidgets: 0,
          appUpPercent: 0,
        });
      }

      const notificationSettings = resolveNotificationSettings(config);
      const runtimeSettings = resolveWidgetMonitorRuntimeSettings(notificationSettings);
      const states = await mapWithConcurrency(targets, runtimeSettings.maxConcurrency, async (target) => {
        const result = await fetchWidgetMonitorState(target.appId, runtimeSettings.requestTimeoutMs);
        return normalizeWidgetMonitorState(result?.state);
      });

      let onlineWidgets = 0;
      let offlineWidgets = 0;
      let unknownWidgets = 0;
      states.forEach((state) => {
        if (state === 'online') onlineWidgets += 1;
        else if (state === 'offline') offlineWidgets += 1;
        else unknownWidgets += 1;
      });

      const appUpPercent = totalWidgets > 0
        ? Math.round((onlineWidgets / totalWidgets) * 1000) / 10
        : 0;

      return res.json({
        ok: true,
        online: onlineWidgets,
        offline: offlineWidgets,
        unknown: unknownWidgets,
        total: totalWidgets,
        onlineWidgets,
        offlineWidgets,
        unknownWidgets,
        totalWidgets,
        appUpPercent,
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to fetch deployment summary.' });
    }
  });

  // CPU, RAM, Disk stats for system widgets
  app.get('/api/system-info', requireUser, async (req, res) => {
    try {
      const os = (await import('os')).default;
      const { execFile } = await import('child_process');
      const { promisify } = await import('util');
      const fsPromises = (await import('fs/promises')).default;
      const path = (await import('path')).default;
      const execFileAsync = promisify(execFile);

      function parseDfBytesOutput(stdout) {
        const lines = String(stdout || '').trim().split('\n').map((line) => line.trim()).filter(Boolean);
        if (lines.length < 2) return null;
        const parts = lines[lines.length - 1].split(/\s+/);
        const total = Number(parts[0]);
        const free = Number(parts[1]);
        if (!Number.isFinite(total) || !Number.isFinite(free)) return null;
        return { total, free };
      }

      function parseDfKbOutput(stdout) {
        const lines = String(stdout || '').trim().split('\n').map((line) => line.trim()).filter(Boolean);
        if (lines.length < 2) return null;
        const parts = lines[lines.length - 1].split(/\s+/);
        const totalKb = Number(parts[1]);
        const freeKb = Number(parts[3]);
        if (!Number.isFinite(totalKb) || !Number.isFinite(freeKb)) return null;
        return { total: totalKb * 1024, free: freeKb * 1024 };
      }

      async function resolveDiskPath(rawPath) {
        let inputPath = String(rawPath || '/').trim() || '/';
        if (!inputPath.startsWith('/')) inputPath = `/${inputPath}`;
        try {
          await fsPromises.access(inputPath);
          return inputPath;
        } catch (_err) { /* continue */ }

        // Fast common-case for /Media vs /media style paths.
        const lowerFirst = inputPath.replace(/^\/([A-Z])/, (_m, c) => `/${String(c).toLowerCase()}`);
        if (lowerFirst !== inputPath) {
          try {
            await fsPromises.access(lowerFirst);
            return lowerFirst;
          } catch (_err) { /* continue */ }
        }

        // Walk filesystem case-insensitively segment by segment.
        const segments = inputPath.split('/').filter(Boolean);
        let current = '/';
        for (const segmentRaw of segments) {
          const segment = String(segmentRaw || '').trim();
          if (!segment) continue;
          const direct = path.join(current, segment);
          try {
            await fsPromises.access(direct);
            current = direct;
            continue;
          } catch (_err) { /* continue */ }
          try {
            const entries = await fsPromises.readdir(current, { withFileTypes: true });
            const found = entries.find((entry) => String(entry.name || '').toLowerCase() === segment.toLowerCase());
            if (!found) return inputPath;
            current = path.join(current, found.name);
          } catch (_err) {
            return inputPath;
          }
        }
        return current;
      }

      async function readDiskUsage(diskPath) {
        const resolvedPath = await resolveDiskPath(diskPath);
        const attempts = [
          { args: ['-B1', '--output=size,avail', resolvedPath], parser: parseDfBytesOutput },
          { args: ['-Pk', resolvedPath], parser: parseDfKbOutput },
          { args: ['-k', resolvedPath], parser: parseDfKbOutput },
        ];
        for (const attempt of attempts) {
          try {
            const { stdout } = await execFileAsync('df', attempt.args, { timeout: 5000 });
            const parsed = attempt.parser(stdout);
            if (parsed && Number.isFinite(parsed.total) && Number.isFinite(parsed.free)) {
              return { ...parsed, ok: true, resolvedPath };
            }
          } catch (_err) { /* try next parser */ }
        }
        return { total: 0, free: 0, ok: false, resolvedPath };
      }

      // CPU: sample twice 100ms apart
      const cpuSample = () => os.cpus().map((c) => ({ idle: c.times.idle, total: Object.values(c.times).reduce((a, b) => a + b, 0) }));
      const s1 = cpuSample();
      await new Promise((r) => setTimeout(r, 100));
      const s2 = cpuSample();
      let idleSum = 0; let totalSum = 0;
      for (let i = 0; i < s1.length; i++) {
        idleSum += s2[i].idle - s1[i].idle;
        totalSum += s2[i].total - s1[i].total;
      }
      const cpuPercent = totalSum > 0 ? Math.round(100 * (1 - idleSum / totalSum)) : 0;

      // RAM
      const totalMem = os.totalmem();
      const freeMem = os.freemem();

      // Disk paths from query
      const rawPaths = String(req.query.paths || '/').trim();
      const diskPaths = [...new Set(rawPaths.split(',').map((p) => p.trim()).filter(Boolean))];
      const diskResults = await Promise.all(diskPaths.map(async (path) => {
        const usage = await readDiskUsage(path);
        return { path, total: usage.total, free: usage.free, ok: usage.ok, resolvedPath: usage.resolvedPath };
      }));

      return res.json({
        ok: true,
        cpu: { percent: cpuPercent },
        memory: { total: totalMem, free: freeMem, used: totalMem - freeMem },
        disks: diskResults,
      });
    } catch (err) {
      return res.status(500).json({ ok: false, error: safeMessage(err) || 'Failed to fetch system info.' });
    }
  });

  // OpenMeteo weather proxy with server-side cache
  const _openmeteoCache = new Map();
  app.get('/api/openmeteo', requireUser, async (req, res) => {
    try {
      const WMO_CONDITIONS = {
        0: { label: 'Clear sky', icon: 'clear' },
        1: { label: 'Mainly clear', icon: 'mainly-clear' },
        2: { label: 'Partly cloudy', icon: 'partly-cloudy' },
        3: { label: 'Overcast', icon: 'overcast' },
        45: { label: 'Foggy', icon: 'foggy' }, 48: { label: 'Icy fog', icon: 'foggy' },
        51: { label: 'Light drizzle', icon: 'drizzle' }, 53: { label: 'Drizzle', icon: 'drizzle' }, 55: { label: 'Heavy drizzle', icon: 'drizzle' },
        56: { label: 'Freezing drizzle', icon: 'drizzle' }, 57: { label: 'Heavy freezing drizzle', icon: 'drizzle' },
        61: { label: 'Light rain', icon: 'rainy' }, 63: { label: 'Rain', icon: 'rainy' }, 65: { label: 'Heavy rain', icon: 'rainy' },
        66: { label: 'Freezing rain', icon: 'rainy' }, 67: { label: 'Heavy freezing rain', icon: 'rainy' },
        71: { label: 'Light snow', icon: 'snowy' }, 73: { label: 'Snow', icon: 'snowy' }, 75: { label: 'Heavy snow', icon: 'snowy' },
        77: { label: 'Snow grains', icon: 'snowy' },
        80: { label: 'Light showers', icon: 'showers' }, 81: { label: 'Showers', icon: 'showers' }, 82: { label: 'Heavy showers', icon: 'showers' },
        85: { label: 'Snow showers', icon: 'snowy' }, 86: { label: 'Heavy snow showers', icon: 'snowy' },
        95: { label: 'Thunderstorm', icon: 'stormy' },
        96: { label: 'Thunderstorm w/ hail', icon: 'stormy' }, 99: { label: 'Thunderstorm w/ hail', icon: 'stormy' },
      };
      const lat = Number(req.query.lat);
      const lon = Number(req.query.lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        return res.status(400).json({ ok: false, error: 'lat and lon are required.' });
      }
      const timezone = String(req.query.timezone || 'UTC').trim() || 'UTC';
      const units = req.query.units === 'imperial' ? 'imperial' : 'metric';
      const cacheMinutes = Math.max(1, Math.min(60, Number(req.query.cache) || 5));
      const cacheKey = `${lat},${lon},${timezone},${units}`;
      const cached = _openmeteoCache.get(cacheKey);
      if (cached && Date.now() < cached.expiresAt) {
        return res.json({ ok: true, ...cached.data });
      }
      const tempUnit = units === 'imperial' ? 'fahrenheit' : 'celsius';
      const windUnit = units === 'imperial' ? 'mph' : 'kmh';
      const apiUrl = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&current_weather=true&timezone=${encodeURIComponent(timezone)}&temperature_unit=${tempUnit}&wind_speed_unit=${windUnit}`;
      const https = await import('https');
      const wxData = await new Promise((resolve, reject) => {
        const req2 = https.get(apiUrl, { timeout: 8000 }, (response) => {
          let body = '';
          response.on('data', (chunk) => { body += chunk; });
          response.on('end', () => {
            try { resolve(JSON.parse(body)); } catch { reject(new Error('Invalid JSON from OpenMeteo')); }
          });
        });
        req2.on('error', reject);
        req2.on('timeout', () => { req2.destroy(); reject(new Error('OpenMeteo request timed out')); });
      });
      if (!wxData?.current_weather) {
        return res.status(502).json({ ok: false, error: 'Invalid response from OpenMeteo.' });
      }
      const cw = wxData.current_weather;
      const wmoCode = Number(cw.weathercode);
      const condition = WMO_CONDITIONS[wmoCode] || { label: 'Unknown', icon: 'partly-cloudy' };
      const result = {
        temperature: cw.temperature,
        windspeed: cw.windspeed,
        weathercode: wmoCode,
        condition: condition.label,
        icon: condition.icon,
        units,
        tempUnit: units === 'imperial' ? '°F' : '°C',
      };
      _openmeteoCache.set(cacheKey, { data: result, expiresAt: Date.now() + cacheMinutes * 60 * 1000 });
      return res.json({ ok: true, ...result });
    } catch (err) {
      return res.status(502).json({ ok: false, error: safeMessage(err) || 'Failed to fetch weather data.' });
    }
  });

  // ─── Widget Stats ─────────────────────────────────────────────────────────

  app.get('/api/widget-status-monitor', requireSettingsAdmin, (_req, res) => {
    try {
      return res.json(buildWidgetStatusMonitorSnapshot());
    } catch (err) {
      return res.status(500).json({ ok: false, error: safeMessage(err) || 'Failed to load widget monitor state.' });
    }
  });

  app.get('/api/widget-stats/:appId', requireWidgetStatsAccess, async (req, res) => {
    try {
      const config = loadConfig();
      const apps = Array.isArray(config?.apps) ? config.apps : [];
      const rawAppId = String(req.params.appId || '').trim();
      const appItem = apps.find((a) => normalizeAppId(String(a?.id || '')) === normalizeAppId(rawAppId));
      if (!appItem) return res.status(404).json({ error: 'App not configured.' });

      const isInternalWidgetRequest = req.__launcharrInternalWidgetStats === true;
      const effectiveRole = getEffectiveRole(req);
      if (!isInternalWidgetRequest && !canAccessDashboardApp(config, appItem, effectiveRole)) {
        // Also allow access if the app is in a widget bar visible to this role.
        // canAccessDashboardApp only covers arr/downloader/media/overview apps,
        // so apps like QNAP that live only in widget bars would otherwise always get 403.
        const normalizedId = normalizeAppId(appItem.id);
        const bars = resolveWidgetBars(config, apps, effectiveRole, { includeHidden: false });
        const inVisibleWidgetBar = bars.some((bar) =>
          (bar.rows || []).some((row) =>
            (row.widgets || []).some((w) => w.appId && normalizeAppId(w.appId) === normalizedId)
          )
        );
        if (!inVisibleWidgetBar) {
          return res.status(403).json({ error: 'Access denied.' });
        }
      }

      const typeId = getAppBaseId(appItem.id);

      const candidates = resolveAppApiCandidates(appItem, req);
      if (!candidates.length) {
        return res.json({ ok: true, appId: rawAppId, typeId, status: 'unknown', metrics: [] });
      }

      const apiKey = String(appItem.apiKey || '').trim();

      async function doFetch(urlString, fetchHeaders, opts) {
        const method = String(opts?.method || 'GET').toUpperCase();
        const body = opts?.body;
        const timeoutMsRaw = Number(opts?.timeoutMs);
        const timeoutMs = Number.isFinite(timeoutMsRaw)
          ? Math.max(1000, Math.min(15000, Math.round(timeoutMsRaw)))
          : 8000;
        const startedAt = Date.now();
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);
        try {
          const response = await fetch(urlString, {
            method,
            headers: fetchHeaders,
            ...(body !== undefined ? { body } : {}),
            signal: controller.signal,
          });
          const text = await response.text().catch(() => '');
          let json = null;
          try { json = text ? JSON.parse(text) : null; } catch (_e) { json = null; }
          return {
            ok: response.ok,
            status: response.status,
            headers: response.headers,
            json,
            text,
            durationMs: Math.max(0, Date.now() - startedAt),
          };
        } finally {
          clearTimeout(timeout);
        }
      }

      // Try each candidate base URL in order; return on first success
      async function tryAllCandidates(buildRequest) {
        let lastResult = null;
        for (const baseUrl of candidates) {
          try {
            const result = await buildRequest(baseUrl);
            if (result && result.ok) return result;
            lastResult = result;
          } catch (_e) {
            lastResult = null;
          }
        }
        return lastResult;
      }

      async function tryCandidatePaths(paths, fetchHeaders, opts) {
        const requestOpts = (opts && typeof opts === 'object') ? { ...opts } : {};
        const acceptedStatuses = new Set(
          (Array.isArray(requestOpts.acceptStatuses) ? requestOpts.acceptStatuses : [])
            .map((value) => Number(value))
            .filter((value) => Number.isFinite(value))
        );
        delete requestOpts.acceptStatuses;
        let lastResult = null;
        for (const path of (Array.isArray(paths) ? paths : [])) {
          for (const baseUrl of candidates) {
            try {
              const result = await doFetch(buildAppApiUrl(baseUrl, path).toString(), fetchHeaders, requestOpts);
              if (result?.ok || acceptedStatuses.has(Number(result?.status))) return result;
              if (result) lastResult = result;
            } catch (_e) {
              // Try the next candidate URL/path pair.
            }
          }
        }
        return lastResult;
      }

      function pickFiniteFromObject(source, keys) {
        const obj = (source && typeof source === 'object') ? source : null;
        if (!obj) return null;
        for (const key of keys) {
          if (!(key in obj)) continue;
          const parsed = parseMetricNumber(obj[key]);
          if (Number.isFinite(parsed)) return parsed;
        }
        return null;
      }

      function pickFiniteFromPath(source, path) {
        const rawPath = String(path || '').trim();
        if (!rawPath) return null;
        const parts = rawPath.split('.').map((part) => part.trim()).filter(Boolean);
        if (!parts.length) return null;
        let cur = source;
        for (const part of parts) {
          if (!cur || typeof cur !== 'object') return null;
          if (!(part in cur)) return null;
          cur = cur[part];
        }
        const parsed = parseMetricNumber(cur);
        return Number.isFinite(parsed) ? parsed : null;
      }

      function pickFiniteFromPaths(source, paths) {
        for (const path of (Array.isArray(paths) ? paths : [])) {
          const parsed = pickFiniteFromPath(source, path);
          if (parsed !== null) return parsed;
        }
        return null;
      }

      function pickValueFromPath(source, path) {
        const rawPath = String(path || '').trim();
        if (!rawPath) return null;
        const parts = rawPath.split('.').map((part) => part.trim()).filter(Boolean);
        if (!parts.length) return null;
        let cur = source;
        for (const part of parts) {
          if (!cur || typeof cur !== 'object') return null;
          if (!(part in cur)) return null;
          cur = cur[part];
        }
        return cur;
      }

      function pickTextFromPaths(source, paths) {
        for (const path of (Array.isArray(paths) ? paths : [])) {
          const value = pickValueFromPath(source, path);
          if (value === null || value === undefined) continue;
          if (typeof value === 'string') {
            const trimmed = value.trim();
            if (trimmed) return trimmed;
            continue;
          }
          if (typeof value === 'number' && Number.isFinite(value)) return String(value);
          if (typeof value === 'boolean') return value ? 'true' : 'false';
        }
        return '';
      }

      function parseMetricNumber(value) {
        const direct = parseFiniteNumber(value);
        if (Number.isFinite(direct)) return direct;
        const raw = String(value || '').trim().toLowerCase();
        if (!raw) return null;
        const normalized = raw
          .replace(/,/g, '')
          .replace(/\s+/g, '')
          .replace(/_/g, '');
        const numeric = Number(normalized);
        if (Number.isFinite(numeric)) return numeric;
        const match = normalized.match(/^(-?\d+(?:\.\d+)?)\s*([kmb])$/i);
        if (!match) return null;
        const base = Number(match[1]);
        if (!Number.isFinite(base)) return null;
        const suffix = String(match[2] || '').toLowerCase();
        const mult = suffix === 'k' ? 1e3 : (suffix === 'm' ? 1e6 : (suffix === 'b' ? 1e9 : 1));
        return base * mult;
      }

      function toArrayPayload(payload) {
        if (Array.isArray(payload)) return payload;
        if (!payload || typeof payload !== 'object') return [];
        const list = payload.records || payload.items || payload.results || payload.data;
        return Array.isArray(list) ? list : [];
      }

      function resolveArrayPayload(payload, preferredKeys = []) {
        if (Array.isArray(payload)) return { list: payload, found: true };
        if (!payload || typeof payload !== 'object') return { list: [], found: false };
        for (const key of (Array.isArray(preferredKeys) ? preferredKeys : [])) {
          if (Array.isArray(payload?.[key])) return { list: payload[key], found: true };
        }
        const fallbackKeys = ['records', 'items', 'results', 'data'];
        for (const key of fallbackKeys) {
          if (Array.isArray(payload?.[key])) return { list: payload[key], found: true };
        }
        return { list: [], found: false };
      }

      function extractCountFromPayload(payload, opts = {}) {
        const options = (opts && typeof opts === 'object') ? opts : {};
        const extraKeys = Array.isArray(options.numericKeys) ? options.numericKeys : [];
        const extraPaths = Array.isArray(options.numericPaths) ? options.numericPaths : [];
        const listKeys = Array.isArray(options.arrayKeys) ? options.arrayKeys : [];
        const count = pickFiniteFromObject(payload, [
          'count',
          'total',
          'totalCount',
          'totalItems',
          'itemCount',
          'resultsCount',
          ...extraKeys,
        ]);
        if (count !== null) return Math.max(0, Math.round(count));
        const countFromPath = pickFiniteFromPaths(payload, [
          'count',
          'total',
          'stats.count',
          'stats.total',
          ...extraPaths,
        ]);
        if (countFromPath !== null) return Math.max(0, Math.round(countFromPath));
        const { list, found } = resolveArrayPayload(payload, listKeys);
        if (found) return list.length;
        return null;
      }

      function countEnabledEntries(list) {
        const items = Array.isArray(list) ? list : [];
        return items.filter((entry) => entry?.enable !== false && entry?.enabled !== false).length;
      }

      function summarizeProwlarrStatList(list) {
        let queries = 0;
        let grabs = 0;
        let hasQueries = false;
        let hasGrabs = false;
        for (const entry of (Array.isArray(list) ? list : [])) {
          const q = pickFiniteFromObject(entry, [
            'totalQueries',
            'queries',
            'queryCount',
            'numberOfQueries',
            'queryTotal',
          ]);
          const g = pickFiniteFromObject(entry, [
            'totalGrabs',
            'grabs',
            'grabCount',
            'numberOfGrabs',
            'grabTotal',
          ]);
          if (q !== null) { queries += q; hasQueries = true; }
          if (g !== null) { grabs += g; hasGrabs = true; }
        }
        return { queries, grabs, hasQueries, hasGrabs };
      }

      function extractProwlarrTotals(payload) {
        const directQueries = pickFiniteFromObject(payload, [
          'totalQueries',
          'queries',
          'queryCount',
          'numberOfQueries',
          'queryTotal',
        ]);
        const directGrabs = pickFiniteFromObject(payload, [
          'totalGrabs',
          'grabs',
          'grabCount',
          'numberOfGrabs',
          'grabTotal',
        ]);
        const nestedQueries = pickFiniteFromPaths(payload, [
          'queries.total',
          'query.total',
          'searches.total',
          'stats.queries.total',
          'stats.searches.total',
          'indexers.totalQueries',
        ]);
        const nestedGrabs = pickFiniteFromPaths(payload, [
          'grabs.total',
          'grab.total',
          'stats.grabs.total',
          'indexers.totalGrabs',
        ]);
        if (directQueries !== null || directGrabs !== null) {
          return {
            queries: Math.max(0, directQueries !== null ? directQueries : (nestedQueries !== null ? nestedQueries : 0)),
            grabs: Math.max(0, directGrabs !== null ? directGrabs : (nestedGrabs !== null ? nestedGrabs : 0)),
            hasQueries: directQueries !== null,
            hasGrabs: directGrabs !== null,
          };
        }
        if (nestedQueries !== null || nestedGrabs !== null) {
          return {
            queries: Math.max(0, nestedQueries !== null ? nestedQueries : 0),
            grabs: Math.max(0, nestedGrabs !== null ? nestedGrabs : 0),
            hasQueries: nestedQueries !== null,
            hasGrabs: nestedGrabs !== null,
          };
        }

        if (Array.isArray(payload)) return summarizeProwlarrStatList(payload);

        const listCandidate = payload?.records || payload?.items || payload?.results || payload?.data || payload?.indexers;
        if (Array.isArray(listCandidate)) return summarizeProwlarrStatList(listCandidate);

        return { queries: 0, grabs: 0, hasQueries: false, hasGrabs: false };
      }

      function formatCompactCount(value) {
        const n = Math.max(0, Math.round(Number(value) || 0));
        if (n >= 1000000) {
          const scaled = n / 1000000;
          const text = Number.isInteger(scaled)
            ? String(scaled)
            : (scaled >= 100 ? String(Math.round(scaled)) : scaled.toFixed(1).replace(/\.0$/, ''));
          return `${text}M`;
        }
        if (n >= 1000) {
          const scaled = n / 1000;
          const text = Number.isInteger(scaled)
            ? String(scaled)
            : (scaled >= 100 ? String(Math.round(scaled)) : scaled.toFixed(1).replace(/\.0$/, ''));
          return `${text}K`;
        }
        return String(n);
      }

      function formatDurationFromSeconds(value) {
        const parsed = parseMetricNumber(value);
        if (!Number.isFinite(parsed)) return '';
        let totalSeconds = Math.max(0, Math.round(parsed));
        const days = Math.floor(totalSeconds / 86400);
        totalSeconds -= days * 86400;
        const hours = Math.floor(totalSeconds / 3600);
        totalSeconds -= hours * 3600;
        const minutes = Math.floor(totalSeconds / 60);
        const seconds = totalSeconds - (minutes * 60);
        if (days > 0) return `${days}d ${hours}h`;
        if (hours > 0) return `${hours}h ${minutes}m`;
        if (minutes > 0) return `${minutes}m ${seconds}s`;
        return `${seconds}s`;
      }

      function buildGenericApiHeaders() {
        const headers = { Accept: 'application/json' };
        if (apiKey) {
          // Send only one variant — HTTP headers are case-insensitive at the protocol
          // level, but Node.js fetch (undici) appends duplicate JS property names as a
          // comma-joined multi-value which breaks strict auth middleware (e.g. Profilarr).
          if (/^bearer\s+/i.test(apiKey)) {
            headers.Authorization = apiKey;
          } else {
            headers['X-Api-Key'] = apiKey;
          }
        }
        const authHeader = buildBasicAuthHeader(appItem.username || '', appItem.password || '');
        if (authHeader && !headers.Authorization) headers.Authorization = authHeader;
        return headers;
      }

      function hasReachableResult(results) {
        return (Array.isArray(results) ? results : []).some((result) => {
          const code = Number(result?.status);
          return Number.isFinite(code) && code >= 100 && code < 500;
        });
      }

      function hasAuthRequiredResult(results) {
        return (Array.isArray(results) ? results : []).some((result) => {
          const code = Number(result?.status);
          return code === 401 || code === 403;
        });
      }

      let status = 'unknown';
      let metrics = [];
      let libraryInfo = null;

      // ── plex ─────────────────────────────────────────────────────────────
      if (typeId === 'plex') {
        const plexToken = String(appItem.plexToken || appItem.apiKey || '').trim();

        // Find the first working base URL and get the sections list
        let workingBase = null;
        let sectionsResult = null;
        for (const baseUrl of candidates) {
          try {
            const url = buildAppApiUrl(baseUrl, 'library/sections');
            if (plexToken) url.searchParams.set('X-Plex-Token', plexToken);
            const r = await doFetch(url.toString(), { Accept: 'application/json' });
            if (r?.ok && r.json?.MediaContainer) {
              workingBase = baseUrl;
              sectionsResult = r;
              break;
            }
          } catch (_e) { /* try next */ }
        }

        if (sectionsResult) {
          status = 'up';
          const dirs = Array.isArray(sectionsResult.json.MediaContainer.Directory)
            ? sectionsResult.json.MediaContainer.Directory : [];

          // Fetch accurate item counts per section using lightweight container-size=0 requests
          // (library/sections does not reliably return per-section counts)
          // For artist sections, also fetch album count (Plex type=9) in parallel
          const sectionCounts = await Promise.all(
            dirs.map(async (d) => {
              try {
                const makeUrl = (extra) => {
                  const url = buildAppApiUrl(workingBase, `library/sections/${d.key}/all`);
                  url.searchParams.set('X-Plex-Container-Start', '0');
                  url.searchParams.set('X-Plex-Container-Size', '0');
                  if (plexToken) url.searchParams.set('X-Plex-Token', plexToken);
                  if (extra) for (const [k, v] of Object.entries(extra)) url.searchParams.set(k, v);
                  return url.toString();
                };
                const [r, albumR] = await Promise.all([
                  doFetch(makeUrl(), { Accept: 'application/json' }),
                  d.type === 'artist' ? doFetch(makeUrl({ type: '9' }), { Accept: 'application/json' }) : Promise.resolve(null),
                ]);
                return {
                  key: String(d.key || ''), title: String(d.title || d.key || '').trim(), type: d.type,
                  count: Number(r?.json?.MediaContainer?.totalSize) || 0,
                  albumCount: d.type === 'artist' ? (Number(albumR?.json?.MediaContainer?.totalSize) || 0) : 0,
                };
              } catch (_e) {
                return { key: String(d.key || ''), title: String(d.title || d.key || '').trim(), type: d.type, count: 0, albumCount: 0 };
              }
            })
          );

          const movies = sectionCounts.filter((s) => s.type === 'movie').reduce((sum, s) => sum + s.count, 0);
          const shows = sectionCounts.filter((s) => s.type === 'show').reduce((sum, s) => sum + s.count, 0);
          const artists = sectionCounts.filter((s) => s.type === 'artist').reduce((sum, s) => sum + s.count, 0);
          const albums = sectionCounts.filter((s) => s.type === 'artist').reduce((sum, s) => sum + s.albumCount, 0);
          metrics = [
            { key: 'movies',  label: 'Movies',   value: movies },
            { key: 'shows',   label: 'TV Shows',  value: shows },
            { key: 'artists', label: 'Artists',   value: artists },
            { key: 'albums',  label: 'Albums',    value: albums },
          ];
          const plexTypeToMetric = { movie: 'movies', show: 'shows', artist: 'artists' };
          libraryInfo = [
            ...sectionCounts
              .filter((s) => plexTypeToMetric[s.type])
              .map((s) => ({ key: s.key, title: s.title, metricKey: plexTypeToMetric[s.type], count: s.count })),
            ...sectionCounts
              .filter((s) => s.type === 'artist')
              .map((s) => ({ key: s.key, title: s.title, metricKey: 'albums', count: s.albumCount })),
          ];
        } else {
          status = 'down';
        }

      // ── tautulli ─────────────────────────────────────────────────────────
      } else if (typeId === 'tautulli') {
        const result = await tryAllCandidates(async (baseUrl) => {
          const url = buildAppApiUrl(baseUrl, 'api/v2');
          if (apiKey) url.searchParams.set('apikey', apiKey);
          url.searchParams.set('cmd', 'get_activity');
          url.searchParams.set('output', 'json');
          return doFetch(url.toString(), { Accept: 'application/json' });
        });
        if (result?.ok && result.json?.response?.result === 'success') {
          status = 'up';
          const data = result.json.response.data || {};
          metrics = [
            { key: 'streams', label: 'Active Streams', value: Number(data.stream_count) || 0 },
            { key: 'transcodes', label: 'Transcoding', value: Number(data.stream_count_transcode) || 0 },
            { key: 'direct', label: 'Direct Play', value: Number(data.stream_count_direct_play) || 0 },
          ];
        } else {
          status = 'down';
        }

      // ── jellyfin / emby ──────────────────────────────────────────────────
      } else if (typeId === 'jellyfin' || typeId === 'emby') {
        const token = apiKey;
        const result = await tryAllCandidates(async (baseUrl) => {
          const url = buildAppApiUrl(baseUrl, 'Items/Counts');
          const headers = { Accept: 'application/json' };
          if (token) headers['X-MediaBrowser-Token'] = token;
          return doFetch(url.toString(), headers);
        });
        if (result?.ok && result.json) {
          status = 'up';
          const j = result.json;
          metrics = [
            { key: 'movies', label: 'Movies', value: Number(j.MovieCount) || 0 },
            { key: 'series', label: 'TV Shows', value: Number(j.SeriesCount) || 0 },
            { key: 'episodes', label: 'Episodes', value: Number(j.EpisodeCount) || 0 },
            { key: 'songs', label: 'Songs', value: Number(j.SongCount) || 0 },
          ];
        } else {
          status = 'down';
        }

      // ── radarr ───────────────────────────────────────────────────────────
      } else if (typeId === 'radarr') {
        const headers = { Accept: 'application/json' };
        if (apiKey) headers['X-Api-Key'] = apiKey;
        const result = await tryAllCandidates(async (baseUrl) => doFetch(buildAppApiUrl(baseUrl, 'api/v3/movie').toString(), headers));
        if (result?.ok && Array.isArray(result.json)) {
          status = 'up';
          const total = result.json.length;
          const movieFiles = result.json.filter((m) => m.hasFile).length;
          const monitored = result.json.filter((m) => m.monitored).length;
          const unmonitored = result.json.filter((m) => !m.monitored).length;
          metrics = [
            { key: 'movies',      label: 'Movies',      value: total },
            { key: 'movie_files', label: 'Movie Files', value: movieFiles },
            { key: 'monitored',   label: 'Monitored',   value: monitored },
            { key: 'unmonitored', label: 'Unmonitored', value: unmonitored },
          ];
        } else {
          status = 'down';
        }

      // ── sonarr ───────────────────────────────────────────────────────────
      } else if (typeId === 'sonarr') {
        const headers = { Accept: 'application/json' };
        if (apiKey) headers['X-Api-Key'] = apiKey;
        const result = await tryAllCandidates(async (baseUrl) => doFetch(buildAppApiUrl(baseUrl, 'api/v3/series').toString(), headers));
        if (result?.ok && Array.isArray(result.json)) {
          status = 'up';
          const total = result.json.length;
          const ended = result.json.filter((s) => String(s.status || '').toLowerCase() === 'ended').length;
          const continuing = total - ended;
          const monitored = result.json.filter((s) => s.monitored).length;
          const unmonitored = result.json.filter((s) => !s.monitored).length;
          const episodes = result.json.reduce((sum, s) => sum + (Number(s.statistics?.episodeCount) || 0), 0);
          metrics = [
            { key: 'series',      label: 'Series',      value: total },
            { key: 'ended',       label: 'Ended',       value: ended },
            { key: 'continuing',  label: 'Continuing',  value: continuing },
            { key: 'monitored',   label: 'Monitored',   value: monitored },
            { key: 'unmonitored', label: 'Unmonitored', value: unmonitored },
            { key: 'episodes',    label: 'Episodes',    value: episodes },
          ];
        } else {
          status = 'down';
        }

      // ── lidarr ───────────────────────────────────────────────────────────
      } else if (typeId === 'lidarr') {
        const headers = { Accept: 'application/json' };
        if (apiKey) headers['X-Api-Key'] = apiKey;
        const result = await tryAllCandidates(async (baseUrl) => doFetch(buildAppApiUrl(baseUrl, 'api/v1/artist').toString(), headers));
        if (result?.ok && Array.isArray(result.json)) {
          status = 'up';
          const albumCount = result.json.reduce((sum, a) => sum + (Number(a.albumCount) || 0), 0);
          metrics = [
            { key: 'artists', label: 'Artists', value: result.json.length },
            { key: 'albums', label: 'Albums', value: albumCount },
          ];
        } else {
          status = 'down';
        }

      // ── readarr ──────────────────────────────────────────────────────────
      } else if (typeId === 'readarr') {
        const headers = { Accept: 'application/json' };
        if (apiKey) headers['X-Api-Key'] = apiKey;
        const result = await tryAllCandidates(async (baseUrl) => doFetch(buildAppApiUrl(baseUrl, 'api/v1/book').toString(), headers));
        if (result?.ok && Array.isArray(result.json)) {
          status = 'up';
          const authorIds = new Set(result.json.map((b) => b.authorId).filter(Boolean));
          metrics = [
            { key: 'books', label: 'Books', value: result.json.length },
            { key: 'authors', label: 'Authors', value: authorIds.size },
          ];
        } else {
          status = 'down';
        }

      // ── prowlarr ─────────────────────────────────────────────────────────
      } else if (typeId === 'prowlarr') {
        const headers = { Accept: 'application/json' };
        if (apiKey) headers['X-Api-Key'] = apiKey;
        const [indexerResult, statsResult, overviewStatsResult, appsResult, fallbackClientResult] = await Promise.all([
          tryAllCandidates(async (baseUrl) => doFetch(buildAppApiUrl(baseUrl, 'api/v1/indexer').toString(), headers)),
          tryCandidatePaths(['api/v1/indexerstats', 'api/v1/indexer/stats'], headers),
          tryCandidatePaths(['api/v1/stats', 'api/stats'], headers),
          tryCandidatePaths(['api/v1/applications', 'api/v1/application'], headers),
          tryCandidatePaths(['api/v1/downloadclient', 'api/v1/downloadclients'], headers),
        ]);

        if (indexerResult?.ok || statsResult?.ok || overviewStatsResult?.ok || appsResult?.ok || fallbackClientResult?.ok) {
          status = 'up';

          const indexers = toArrayPayload(indexerResult?.json);
          const activeIndexersFromList = countEnabledEntries(indexers);
          const activeIndexersFromStatsRaw = [
            pickFiniteFromObject(statsResult?.json, ['activeIndexers', 'activeIndexerCount', 'enabledIndexers', 'enabledIndexerCount']),
            pickFiniteFromObject(overviewStatsResult?.json, ['activeIndexers', 'activeIndexerCount', 'enabledIndexers', 'enabledIndexerCount']),
            pickFiniteFromPaths(statsResult?.json, ['indexers.active', 'stats.indexers.active']),
            pickFiniteFromPaths(overviewStatsResult?.json, ['indexers.active', 'stats.indexers.active']),
          ].filter((value) => value !== null);
          const activeIndexersFromStats = activeIndexersFromStatsRaw.length
            ? Math.max(...activeIndexersFromStatsRaw.map((value) => Math.max(0, Math.round(value))))
            : null;
          const activeIndexers = Math.max(
            Math.max(0, activeIndexersFromList),
            activeIndexersFromStats !== null ? Math.max(0, Math.round(activeIndexersFromStats)) : 0
          );

          const statTotalsA = extractProwlarrTotals(statsResult?.json);
          const statTotalsB = extractProwlarrTotals(overviewStatsResult?.json);
          const statTotalsC = extractProwlarrTotals(indexers);
          const totalQueries = Math.max(
            0,
            Math.round(statTotalsA.queries || 0),
            Math.round(statTotalsB.queries || 0),
            Math.round(statTotalsC.queries || 0)
          );
          const totalGrabs = Math.max(
            0,
            Math.round(statTotalsA.grabs || 0),
            Math.round(statTotalsB.grabs || 0),
            Math.round(statTotalsC.grabs || 0)
          );

          const appLinks = toArrayPayload(appsResult?.json);
          const downloadClients = toArrayPayload(fallbackClientResult?.json);
          const activeAppsFromLists = countEnabledEntries(appLinks) + countEnabledEntries(downloadClients);
          const activeAppsFromStatsRaw = [
            pickFiniteFromObject(statsResult?.json, ['activeApps', 'activeAppCount', 'activeApplications', 'activeApplicationCount']),
            pickFiniteFromObject(overviewStatsResult?.json, ['activeApps', 'activeAppCount', 'activeApplications', 'activeApplicationCount']),
            pickFiniteFromPaths(statsResult?.json, ['apps.active', 'applications.active', 'stats.apps.active', 'stats.applications.active']),
            pickFiniteFromPaths(overviewStatsResult?.json, ['apps.active', 'applications.active', 'stats.apps.active', 'stats.applications.active']),
          ].filter((value) => value !== null);
          const activeAppsFromStats = activeAppsFromStatsRaw.length
            ? Math.max(...activeAppsFromStatsRaw.map((value) => Math.max(0, Math.round(value))))
            : null;
          const activeApps = Math.max(
            Math.max(0, activeAppsFromLists),
            activeAppsFromStats !== null ? Math.max(0, Math.round(activeAppsFromStats)) : 0
          );

          metrics = [
            { key: 'active_indexers', label: 'Active Indexers', value: activeIndexers },
            { key: 'total_queries', label: 'Total Queries', value: formatCompactCount(totalQueries) },
            { key: 'total_grabs', label: 'Total Grabs', value: formatCompactCount(totalGrabs) },
            { key: 'active_apps', label: 'Active Apps', value: activeApps },
          ];
        } else {
          status = 'down';
        }

      // ── jackett ──────────────────────────────────────────────────────────
      } else if (typeId === 'jackett') {
        const result = await tryAllCandidates(async (baseUrl) => {
          const url = buildAppApiUrl(baseUrl, 'api/v2.0/indexers');
          url.searchParams.set('configured', 'true');
          if (apiKey) url.searchParams.set('apikey', apiKey);
          return doFetch(url.toString(), { Accept: 'application/json' });
        });
        if (result?.ok && Array.isArray(result.json)) {
          status = 'up';
          metrics = [{ key: 'configured', label: 'Configured', value: result.json.length }];
        } else {
          status = result?.status != null ? 'down' : 'unknown';
        }

      // ── bazarr ───────────────────────────────────────────────────────────
      } else if (typeId === 'bazarr') {
        const headers = { Accept: 'application/json' };
        if (apiKey) headers['X-API-KEY'] = apiKey;
        const [epResult, mvResult] = await Promise.all([
          tryAllCandidates(async (baseUrl) => doFetch(buildAppApiUrl(baseUrl, 'api/episodes/wanted').toString(), headers)),
          tryAllCandidates(async (baseUrl) => doFetch(buildAppApiUrl(baseUrl, 'api/movies/wanted').toString(), headers)),
        ]);
        if (epResult?.ok || mvResult?.ok) {
          status = 'up';
          const epTotal = Number(epResult?.json?.total) || (Array.isArray(epResult?.json?.data) ? epResult.json.data.length : 0);
          const mvTotal = Number(mvResult?.json?.total) || (Array.isArray(mvResult?.json?.data) ? mvResult.json.data.length : 0);
          metrics = [
            { key: 'episodes', label: 'Episodes Wanted', value: epTotal },
            { key: 'movies', label: 'Movies Wanted', value: mvTotal },
          ];
        } else {
          status = 'down';
        }

      // ── autobrr ──────────────────────────────────────────────────────────
      } else if (typeId === 'autobrr') {
        const autobrrHeaders = { Accept: 'application/json' };
        if (apiKey) autobrrHeaders['X-API-Token'] = apiKey;
        const result = await tryAllCandidates(async (baseUrl) =>
          doFetch(buildAppApiUrl(baseUrl, 'api/release/stats').toString(), autobrrHeaders));
        if (result?.ok && result.json && typeof result.json === 'object') {
          status = 'up';
          const j = result.json;
          metrics = [
            { key: 'filtered',        label: 'Filtered Releases', value: Number(j.filtered_count)      || 0 },
            { key: 'push_approved',   label: 'Approved Pushes',   value: Number(j.push_approved_count) || 0 },
            { key: 'push_rejected',   label: 'Rejected Pushes',   value: Number(j.push_rejected_count) || 0 },
            { key: 'push_error',      label: 'Errored Pushes',    value: Number(j.push_error_count)    || 0 },
          ];
        } else {
          status = 'down';
        }

      // ── qbittorrent ──────────────────────────────────────────────────────
      } else if (typeId === 'qbittorrent') {
        const qbUsername = String(appItem.username || '').trim();
        const qbPassword = String(appItem.password || '').trim();
        const allowAuth = Boolean(qbUsername || qbPassword);
        let found = false;

        for (const baseUrl of candidates) {
          let cookieHeader = '';
          let transferResult = await doFetch(
            buildAppApiUrl(baseUrl, 'api/v2/transfer/info').toString(),
            { Accept: 'application/json' },
          ).catch(() => null);

          // If direct access failed and credentials exist, authenticate to qBittorrent WebUI.
          if ((!transferResult || !transferResult.ok) && allowAuth) {
            const loginPayload = new URLSearchParams({
              username: qbUsername,
              password: qbPassword,
            }).toString();
            const loginHeaders = {
              Accept: 'text/plain',
              'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
              Origin: String(baseUrl || '').trim(),
              Referer: String(baseUrl || '').trim().replace(/\/+$/, '/'),
            };
            const loginResult = await doFetch(
              buildAppApiUrl(baseUrl, 'api/v2/auth/login').toString(),
              loginHeaders,
              { method: 'POST', body: loginPayload },
            ).catch(() => null);
            const loginText = String(loginResult?.text || '').trim();
            if (loginResult?.ok && /^ok\.?$/i.test(loginText)) {
              const setCookie = String(loginResult?.headers?.get('set-cookie') || '').trim();
              const firstCookie = setCookie.split(';')[0].trim();
              if (firstCookie) {
                cookieHeader = firstCookie;
                transferResult = await doFetch(
                  buildAppApiUrl(baseUrl, 'api/v2/transfer/info').toString(),
                  { Accept: 'application/json', Cookie: cookieHeader },
                ).catch(() => null);
              }
            }
          }

          if (!(transferResult?.ok && transferResult.json)) continue;

          found = true;
          status = 'up';
          const info = transferResult.json;
          const dlSpeed = Number(info.dl_info_speed) || 0;
          const upSpeed = Number(info.up_info_speed) || 0;
          const torrentHeaders = { Accept: 'application/json' };
          if (cookieHeader) torrentHeaders.Cookie = cookieHeader;
          const torrentsResult = await doFetch(
            buildAppApiUrl(baseUrl, 'api/v2/torrents/info').toString(),
            torrentHeaders,
          ).catch(() => null);
          const torrents = Array.isArray(torrentsResult?.json) ? torrentsResult.json : [];
          const downloading = torrents.filter((t) => ['downloading', 'stalledDL', 'forcedDL'].includes(t.state)).length;
          const seeding = torrents.filter((t) => ['uploading', 'stalledUP', 'forcedUP'].includes(t.state)).length;
          metrics = [
            { key: 'downloading', label: 'Downloading', value: downloading },
            { key: 'seeding', label: 'Seeding', value: seeding },
            { key: 'dlspeed', label: 'DL Speed', value: dlSpeed >= 1048576 ? `${(dlSpeed / 1048576).toFixed(1)} MB/s` : `${Math.round(dlSpeed / 1024)} KB/s` },
            { key: 'upspeed', label: 'UL Speed', value: upSpeed >= 1048576 ? `${(upSpeed / 1048576).toFixed(1)} MB/s` : `${Math.round(upSpeed / 1024)} KB/s` },
          ];
          break;
        }

        if (!found) status = 'down';

      // ── sabnzbd ──────────────────────────────────────────────────────────
      } else if (typeId === 'sabnzbd') {
        const result = await tryAllCandidates(async (baseUrl) => {
          const url = buildAppApiUrl(baseUrl, 'api');
          url.searchParams.set('mode', 'queue');
          url.searchParams.set('output', 'json');
          if (apiKey) url.searchParams.set('apikey', apiKey);
          return doFetch(url.toString(), { Accept: 'application/json' });
        });
        if (result?.ok && result.json?.queue) {
          status = 'up';
          const q = result.json.queue;
          metrics = [
            { key: 'queued', label: 'Queued', value: Number(q.noofslots) || 0 },
            { key: 'speed', label: 'Speed', value: String(q.speed || '0 B/s') },
            { key: 'sizeleft', label: 'Remaining', value: String(q.sizeleft || '0') + ' ' + String(q.sizeleftunits || 'MB') },
          ];
        } else {
          status = 'down';
        }

      // ── nzbget ───────────────────────────────────────────────────────────
      } else if (typeId === 'nzbget') {
        const username = String(appItem.username || 'nzbget').trim();
        const password = String(appItem.password || '').trim();
        const authHeader = buildBasicAuthHeader(username, password);
        const rpcHeaders = { Accept: 'application/json', 'Content-Type': 'application/json' };
        if (authHeader) rpcHeaders.Authorization = authHeader;

        async function nzbgetRpc(method, params) {
          return tryAllCandidates(async (baseUrl) => {
            const url = buildAppApiUrl(baseUrl, 'jsonrpc');
            return doFetch(url.toString(), rpcHeaders, {
              method: 'POST',
              body: JSON.stringify({ method, params: Array.isArray(params) ? params : [], id: 1 }),
            });
          });
        }

        const [statusResult, groupsResult] = await Promise.all([
          nzbgetRpc('status', []),
          nzbgetRpc('listgroups', [0]),
        ]);

        if ((statusResult?.ok && statusResult.json?.result) || (groupsResult?.ok && Array.isArray(groupsResult?.json?.result))) {
          status = 'up';
          const s = statusResult?.json?.result || {};
          const speed = Number(s.DownloadRate) || 0;
          const remaining = Number(s.RemainingSizeMB) || 0;
          const paused = Boolean(s.DownloadPaused);
          const groups = Array.isArray(groupsResult?.json?.result) ? groupsResult.json.result : [];
          const downloading = groups.filter((entry) => {
            const statusText = String(entry?.Status || entry?.status || '').toUpperCase();
            return statusText.includes('DOWNLOADING') || statusText.includes('FETCHING');
          }).length;
          let queued = groups.filter((entry) => {
            const statusText = String(entry?.Status || entry?.status || '').toUpperCase();
            return statusText.includes('QUEUED') || statusText.includes('PAUSED');
          }).length;
          if (groups.length && queued === 0) queued = Math.max(0, groups.length - downloading);
          metrics = [
            { key: 'downloading', label: 'Downloading', value: downloading },
            { key: 'queue', label: 'Queued', value: queued },
            { key: 'speed', label: 'Speed', value: paused ? 'Paused' : (speed >= 1048576 ? `${(speed / 1048576).toFixed(1)} MB/s` : `${Math.round(speed / 1024)} KB/s`) },
            { key: 'remaining', label: 'Remaining', value: `${remaining} MB` },
          ];
        } else {
          status = 'down';
        }

      // ── transmission ─────────────────────────────────────────────────────
      } else if (typeId === 'transmission') {
        const username = String(appItem.username || '').trim();
        const password = String(appItem.password || '').trim();
        const authHeader = buildBasicAuthHeader(username, password);
        const baseUrl = candidates[0];
        const rpcUrl = buildAppApiUrl(baseUrl, 'transmission/rpc').toString();
        const rpcHeaders = { 'Content-Type': 'application/json', Accept: 'application/json' };
        if (authHeader) rpcHeaders.Authorization = authHeader;
        rpcHeaders['X-Transmission-Session-Id'] = '0';

        async function transmissionRpc(method, args) {
          const body = JSON.stringify({ method, arguments: args || {}, tag: 1 });
          const first = await doFetch(rpcUrl, { ...rpcHeaders }, { method: 'POST', body }).catch(() => null);
          if (first?.status === 409) {
            const sessionId = String(first.headers?.get?.('X-Transmission-Session-Id') || '').trim();
            if (sessionId) {
              rpcHeaders['X-Transmission-Session-Id'] = sessionId;
              return doFetch(rpcUrl, { ...rpcHeaders }, { method: 'POST', body }).catch(() => null);
            }
          }
          return first;
        }

        function formatRate(bytesPerSecond) {
          const value = Number(bytesPerSecond) || 0;
          if (value >= 1048576) return `${(value / 1048576).toFixed(1)} MB/s`;
          return `${Math.round(value / 1024)} KB/s`;
        }

        const [sessionStatsResult, torrentsResult] = await Promise.all([
          transmissionRpc('session-stats', {}),
          transmissionRpc('torrent-get', { fields: ['id', 'status', 'rateDownload', 'rateUpload'] }),
        ]);

        if (sessionStatsResult?.ok && sessionStatsResult.json?.result === 'success') {
          status = 'up';
          const args = sessionStatsResult.json.arguments || {};
          const active = Number(args.activeTorrentCount) || 0;
          const paused = Number(args.pausedTorrentCount) || 0;
          const total = Number(args.torrentCount) || 0;
          const torrentItems = Array.isArray(torrentsResult?.json?.arguments?.torrents)
            ? torrentsResult.json.arguments.torrents
            : [];
          const downloading = torrentItems.filter((t) => Number(t?.status) === 3 || Number(t?.status) === 4).length;
          const seeding = torrentItems.filter((t) => Number(t?.status) === 5 || Number(t?.status) === 6).length;
          const dlSpeed = Number(args.downloadSpeed) || torrentItems.reduce((sum, t) => sum + (Number(t?.rateDownload) || 0), 0);
          const upSpeed = Number(args.uploadSpeed) || torrentItems.reduce((sum, t) => sum + (Number(t?.rateUpload) || 0), 0);
          metrics = [
            { key: 'active', label: 'Active', value: active },
            { key: 'paused', label: 'Paused', value: paused },
            { key: 'total', label: 'Total', value: total },
            { key: 'downloading', label: 'Downloading', value: downloading },
            { key: 'seeding', label: 'Seeding', value: seeding },
            { key: 'dlspeed', label: 'Download Speed', value: formatRate(dlSpeed) },
            { key: 'upspeed', label: 'Upload Speed', value: formatRate(upSpeed) },
          ];
        } else {
          status = 'down';
        }

      // ── maintainerr ──────────────────────────────────────────────────────
      } else if (typeId === 'maintainerr') {
        const headers = { Accept: 'application/json' };
        if (apiKey) headers['X-Api-Key'] = apiKey;
        const result = await tryAllCandidates(async (baseUrl) => doFetch(buildAppApiUrl(baseUrl, 'api/rules').toString(), headers));
        if (result?.ok && Array.isArray(result.json)) {
          status = 'up';
          const active = result.json.filter((r) => r.isActive !== false).length;
          metrics = [
            { key: 'rules', label: 'Total Rules', value: result.json.length },
            { key: 'active', label: 'Active', value: active },
          ];
        } else {
          status = 'down';
        }

      // ── cleanuparr ───────────────────────────────────────────────────────
      } else if (typeId === 'cleanuparr') {
        const headers = { Accept: 'application/json' };
        if (apiKey) headers['X-Api-Key'] = apiKey;
        // /api/statistics is served by the SPA (returns HTML) — use /api/strikes instead
        const [healthResult, strikesResult] = await Promise.all([
          tryAllCandidates(async (baseUrl) => doFetch(buildAppApiUrl(baseUrl, 'api/health').toString(), headers)),
          tryAllCandidates(async (baseUrl) => doFetch(buildAppApiUrl(baseUrl, 'api/strikes').toString(), headers)),
        ]);
        const isUp = healthResult?.ok && healthResult.json && typeof healthResult.json === 'object' && !Array.isArray(healthResult.json);
        if (isUp || (strikesResult?.ok && strikesResult.json?.totalCount !== undefined)) {
          status = 'up';
          const j = strikesResult?.json || {};
          const items = Array.isArray(j.items) ? j.items : [];
          const removed = items.filter((i) => i.isRemoved).length;
          metrics = [
            { key: 'tracked', label: 'Tracked',  value: Number(j.totalCount) || 0 },
            { key: 'removed', label: 'Removed',  value: removed },
          ];
        } else {
          status = 'down';
        }

      // ── romm ─────────────────────────────────────────────────────────────
      } else if (typeId === 'romm') {
        const headers = { Accept: 'application/json' };
        if (apiKey) {
          headers['X-Api-Key'] = apiKey;
          headers.Authorization = /^bearer\s+/i.test(apiKey) ? apiKey : `Bearer ${apiKey}`;
        }
        const authHdr = buildBasicAuthHeader(appItem.username || '', appItem.password || '');
        if (authHdr) headers.Authorization = authHdr;

        function asArray(payload, kind = 'recently-added') {
          if (Array.isArray(payload)) return payload;
          if (!payload || typeof payload !== 'object') return [];
          if (kind === 'collections') {
            const direct = payload.collections || payload.items || payload.results || payload.records || payload.data;
            if (Array.isArray(direct)) return direct;
          }
          const extracted = extractRommList(payload, kind === 'consoles' ? 'consoles' : 'recently-added');
          if (Array.isArray(extracted)) return extracted;
          const fallback = payload.items || payload.results || payload.data || payload.records || payload.consoles || payload.collections;
          return Array.isArray(fallback) ? fallback : [];
        }

        function parseCountish(value, seen = new Set()) {
          if (value === null || value === undefined || value === '') return 0;
          const direct = Number(value);
          if (Number.isFinite(direct)) return Math.max(0, Math.round(direct));
          if (typeof value === 'string') {
            const text = value.trim();
            if (!text) return 0;
            const compact = text.replace(/[\s,]+/g, '');
            const compactNum = Number(compact);
            if (Number.isFinite(compactNum)) return Math.max(0, Math.round(compactNum));
            const embedded = text.match(/(\d[\d,]*)/);
            if (embedded && embedded[1]) {
              const embeddedNum = Number(String(embedded[1]).replace(/,/g, ''));
              if (Number.isFinite(embeddedNum)) return Math.max(0, Math.round(embeddedNum));
            }
            return 0;
          }
          if (Array.isArray(value)) return value.length;
          if (typeof value !== 'object') return 0;
          if (seen.has(value)) return 0;
          seen.add(value);

          const preferred = [
            value.count,
            value.total,
            value.totalCount,
            value.totalItems,
            value.itemsCount,
            value.romCount,
            value.romsCount,
            value.rom_count,
            value.roms_count,
            value.gameCount,
            value.gamesCount,
            value.game_count,
            value.games_count,
            value.consoleCount,
            value.consolesCount,
            value.console_count,
            value.consoles_count,
            value.collectionCount,
            value.collectionsCount,
            value.collection_count,
            value.collections_count,
            value.biosCount,
            value.bios_count,
            value.saveCount,
            value.savesCount,
            value.save_count,
            value.saves_count,
            value.stats,
            value.totals,
            value.summary,
            value.meta,
            value.metadata,
            value.pagination,
            value.page,
            value.pageInfo,
          ];
          for (const candidate of preferred) {
            const parsed = parseCountish(candidate, seen);
            if (parsed > 0) return parsed;
          }

          let best = 0;
          Object.entries(value).forEach(([rawKey, rawValue]) => {
            const key = String(rawKey || '').toLowerCase();
            if (!key) return;
            const maybeCount = key.includes('count')
              || key.includes('total')
              || key.includes('rom')
              || key.includes('game')
              || key.includes('console')
              || key.includes('collection')
              || key.includes('bios')
              || key.includes('save')
              || key.includes('item')
              || key.includes('entry');
            if (!maybeCount) return;
            const parsed = parseCountish(rawValue, seen);
            if (parsed > best) best = parsed;
          });
          return best;
        }

        function pickCount(payload, keys = []) {
          const obj = (payload && typeof payload === 'object') ? payload : null;
          if (!obj) return null;
          for (const key of keys) {
            const parts = String(key || '').split('.').filter(Boolean);
            if (!parts.length) continue;
            let cur = obj;
            let ok = true;
            for (const part of parts) {
              if (!cur || typeof cur !== 'object' || !(part in cur)) { ok = false; break; }
              cur = cur[part];
            }
            if (!ok) continue;
            const parsed = parseCountish(cur);
            if (parsed > 0) return parsed;
          }
          const fallback = parseCountish(payload);
          return Number.isFinite(fallback) ? fallback : null;
        }


        const [romsResult, consolesResult, collectionsResult, virtualCollResult, smartCollResult] = await Promise.all([
          tryAllCandidates(async (baseUrl) => {
            const url = buildAppApiUrl(baseUrl, 'api/roms');
            url.searchParams.set('limit', '1');
            url.searchParams.set('with_char_index', 'false');
            url.searchParams.set('with_filter_values', 'false');
            return doFetch(url.toString(), headers);
          }),
          tryCandidatePaths(['api/platforms', 'api/v1/platforms', 'api/consoles', 'api/v1/consoles', 'api/systems', 'api/v1/systems'], headers),
          tryCandidatePaths(['api/collections', 'api/v1/collections', 'api/collection'], headers),
          // type=collection = IGDB series/collection groupings (the user-visible "auto-collections")
          tryAllCandidates(async (baseUrl) => {
            const url = buildAppApiUrl(baseUrl, 'api/collections/virtual');
            url.searchParams.set('type', 'collection');
            return doFetch(url.toString(), headers);
          }),
          tryCandidatePaths(['api/collections/smart'], headers),
        ]);

        if ((romsResult?.ok && romsResult.json) || (consolesResult?.ok && consolesResult.json) || (collectionsResult?.ok && collectionsResult.json) || (virtualCollResult?.ok && virtualCollResult.json) || (smartCollResult?.ok && smartCollResult.json)) {
          status = 'up';
          const romsPayload = romsResult?.json;
          const consolesPayload = consolesResult?.json;
          const collectionsPayload = collectionsResult?.json;
          const virtualCollPayload = virtualCollResult?.json;
          const smartCollPayload = smartCollResult?.json;

          const romItems = asArray(romsPayload, 'recently-added');
          const consoleItems = asArray(consolesPayload, 'consoles');
          const mappedConsoleItems = consoleItems.map((entry) => mapRommConsoleItem(entry, ''));
          const consoleRomSum = mappedConsoleItems.reduce((sum, item) => sum + (Number(item?.romCount) || 0), 0);

          const games = Math.max(
            pickCount(romsPayload, ['total', 'count', 'totalItems', 'totals.roms', 'stats.roms', 'stats.games']) || 0,
            romItems.length,
            consoleRomSum
          );
          const consoles = Math.max(
            pickCount(consolesPayload, ['total', 'count', 'totalItems', 'totals.consoles', 'stats.consoles']) || 0,
            consoleItems.length
          );
          // Count by array length only — never use pickCount on these payloads since
          // each item carries rom_count which would be picked up as an aggregate count.
          const collectionItems = asArray(collectionsPayload, 'collections');
          const collections = collectionItems.length;
          const virtualCount = asArray(virtualCollPayload, 'collections').length;
          const smartCount = asArray(smartCollPayload, 'collections').length;
          const biosFromConsoleStats = mappedConsoleItems.reduce((sum, item) => {
            const biosEntry = Array.isArray(item?.stats) ? item.stats.find((stat) => String(stat?.label || '').toLowerCase() === 'bios') : null;
            return sum + parseCountish(biosEntry?.value);
          }, 0);
          const savesFromConsoleStats = mappedConsoleItems.reduce((sum, item) => {
            const savesEntry = Array.isArray(item?.stats) ? item.stats.find((stat) => String(stat?.label || '').toLowerCase() === 'saves') : null;
            return sum + parseCountish(savesEntry?.value);
          }, 0);
          // Use only explicitly-labeled stats from mapped items; sumCount's parseCountish
          // fallback would pick up rom_count when bios/save fields are absent.
          const bios = biosFromConsoleStats;
          const saves = savesFromConsoleStats;

          metrics = [
            { key: 'games', label: 'Games', value: games },
            { key: 'consoles', label: 'Consoles', value: consoles },
            { key: 'collections', label: 'Collections', value: collections },
            { key: 'virtual_collections', label: 'Virtual Collections', value: virtualCount },
            { key: 'smart_collections', label: 'Smart Collections', value: smartCount },
            { key: 'bios', label: 'BIOS', value: bios },
            { key: 'saves', label: 'Saves', value: saves },
          ];
        } else {
          status = 'down';
        }

      // ── seerr (overseerr / jellyseerr) ───────────────────────────────────
      } else if (typeId === 'seerr') {
        const seerrHeaders = { Accept: 'application/json' };
        if (apiKey) seerrHeaders['X-Api-Key'] = apiKey;
        const seerrCandidates = resolveRequestApiCandidates(appItem, req);
        // Try /request/count (requires manage-requests permission), then paginated list,
        // then public /api/v1/status as final fallback to at least show ONLINE
        let seerrResult = null;
        let seerrLastResponse = null;
        let seerrMode = 'count';
        for (const baseUrl of seerrCandidates) {
          try {
            const r = await doFetch(buildAppApiUrl(baseUrl, 'api/v1/request/count').toString(), seerrHeaders);
            seerrLastResponse = r;
            if (r?.ok) { seerrResult = r; break; }
          } catch (_e) { /* try next */ }
        }
        if (!seerrResult) {
          seerrMode = 'list';
          for (const baseUrl of seerrCandidates) {
            try {
              const url = buildAppApiUrl(baseUrl, 'api/v1/request');
              url.searchParams.set('take', '1');
              url.searchParams.set('skip', '0');
              url.searchParams.set('filter', 'all');
              const r = await doFetch(url.toString(), seerrHeaders);
              seerrLastResponse = r;
              if (r?.ok) { seerrResult = r; break; }
            } catch (_e) { /* try next */ }
          }
        }
        if (!seerrResult) {
          // Final fallback: public status endpoint (no auth needed) — at least show ONLINE
          seerrMode = 'status';
          for (const baseUrl of seerrCandidates) {
            try {
              const r = await doFetch(buildAppApiUrl(baseUrl, 'api/v1/status').toString(), { Accept: 'application/json' });
              seerrLastResponse = r;
              if (r?.ok) { seerrResult = r; break; }
            } catch (_e) { /* try next */ }
          }
        }
        if (seerrResult?.ok) {
          status = 'up';
          if (seerrMode === 'count') {
            const j = seerrResult.json || {};
            metrics = [
              { key: 'pending', label: 'Pending', value: Number(j.pending) || 0 },
              { key: 'approved', label: 'Approved', value: Number(j.approved) || 0 },
              { key: 'processing', label: 'Processing', value: Number(j.processing) || 0 },
              { key: 'available', label: 'Available', value: Number(j.available) || 0 },
            ];
            if ((Number(j.available) || 0) === 0 && (Number(j.total) || 0) > 0) {
              pushLog({ level: 'warn', app: 'seerr', action: 'widget.stats',
                message: `available=0 but total=${j.total} — raw count: ${JSON.stringify(j).slice(0, 400)}` });
            }
          } else if (seerrMode === 'list') {
            const total = Number(seerrResult.json?.pageInfo?.results
              || seerrResult.json?.pageInfo?.total
              || seerrResult.json?.total) || 0;
            metrics = [{ key: 'total', label: 'Total Requests', value: total }];
          }
          // seerrMode==='status': server is up, no request metrics (key not configured)
        } else {
          pushLog({ level: 'error', app: 'seerr', action: 'widget.stats',
            message: `All seerr endpoints failed. last HTTP=${seerrLastResponse?.status ?? 'null'} body=${String(seerrLastResponse?.text || '').slice(0, 200)} candidates=${seerrCandidates.join(', ')}` });
          status = 'down';
        }

      // ── pulsarr ──────────────────────────────────────────────────────────
      } else if (typeId === 'pulsarr') {
        const pulsarrHeaders = { Accept: 'application/json', 'X-API-Key': apiKey };
        const pulsarrCandidates = resolveRequestApiCandidates(appItem, req);
        // Fetch approval stats (totals) and all auto-approved requests (for per-content-type counts)
        const [approvalResult, autoApprovedResult] = await Promise.all([
          (async () => {
            for (const baseUrl of pulsarrCandidates) {
              try {
                const r = await doFetch(buildAppApiUrl(baseUrl, 'v1/approval/stats').toString(), pulsarrHeaders);
                if (r?.ok) return r;
              } catch (_e) { /* try next */ }
            }
            return null;
          })(),
          (async () => {
            for (const baseUrl of pulsarrCandidates) {
              try {
                const url = buildAppApiUrl(baseUrl, 'v1/approval/requests');
                url.searchParams.set('status', 'auto_approved');
                url.searchParams.set('limit', '500');
                const r = await doFetch(url.toString(), pulsarrHeaders);
                if (r?.ok) return r;
              } catch (_e) { /* try next */ }
            }
            return null;
          })(),
        ]);
        if (approvalResult?.ok || autoApprovedResult?.ok) {
          status = 'up';
          const approvalStats = approvalResult?.json?.stats || {};
          const autoApprovedReqs = Array.isArray(autoApprovedResult?.json?.approvalRequests) ? autoApprovedResult.json.approvalRequests : [];
          const movieCount = autoApprovedReqs.filter((i) => String(i?.contentType || '').toLowerCase() === 'movie').length;
          const showCount = autoApprovedReqs.filter((i) => String(i?.contentType || '').toLowerCase() === 'show').length;
          metrics = [
            { key: 'auto_approved', label: 'Auto Approved', value: Number(approvalStats.auto_approved) || 0 },
            { key: 'approved',      label: 'Approved',      value: Number(approvalStats.approved) || 0 },
            { key: 'movies',        label: 'Movies',        value: movieCount },
            { key: 'shows',         label: 'TV Shows',      value: showCount },
          ];
        } else {
          status = 'down';
        }

      // ── immich ───────────────────────────────────────────────────────────
      } else if (typeId === 'immich') {
        const headers = { Accept: 'application/json' };
        if (apiKey) headers['x-api-key'] = apiKey;
        const result = await tryCandidatePaths(['api/server/statistics', 'api/server-info/stats'], headers);
        if (result?.ok && result.json) {
          status = 'up';
          const j = result.json;
          const photos = Number(j.photos ?? j.photoCount ?? j.total_photos) || 0;
          const videos = Number(j.videos ?? j.videoCount ?? j.total_videos) || 0;
          const users = Number(j.usage?.length ?? j.usageByUser?.length ?? j.users) || 0;
          const usageBytes = j.usage
            ? (Array.isArray(j.usage) ? j.usage.reduce((s, u) => s + (Number(u.usageRaw ?? u.usage) || 0), 0) : Number(j.usage))
            : Number(j.diskSizeRaw ?? j.diskSize ?? 0);
          const storageTB = usageBytes >= 1e12 ? `${(usageBytes / 1e12).toFixed(2)} TB`
            : usageBytes >= 1e9 ? `${(usageBytes / 1e9).toFixed(1)} GB`
            : usageBytes >= 1e6 ? `${(usageBytes / 1e6).toFixed(0)} MB` : `${usageBytes} B`;
          metrics = [
            { key: 'photos',  label: 'Photos',  value: photos },
            { key: 'videos',  label: 'Videos',  value: videos },
            { key: 'users',   label: 'Users',   value: users },
            { key: 'storage', label: 'Storage', value: storageTB },
          ];
        } else {
          status = 'down';
        }

      // ── portainer ────────────────────────────────────────────────────────
      } else if (typeId === 'portainer') {
        const portainerHeaders = { Accept: 'application/json' };
        if (apiKey) portainerHeaders['X-API-Key'] = apiKey;
        // Fetch endpoint list; pick first local endpoint (type=1) or just endpoint id 1
        const endpointsResult = await tryAllCandidates(async (baseUrl) =>
          doFetch(buildAppApiUrl(baseUrl, 'api/endpoints').toString(), portainerHeaders)
        );
        if (endpointsResult?.ok && Array.isArray(endpointsResult.json)) {
          status = 'up';
          const endpoints = endpointsResult.json;
          const endpoint = endpoints.find((e) => e.Type === 1) || endpoints[0];
          let running = 0, stopped = 0, total = 0;
          if (endpoint) {
            const envId = endpoint.Id;
            const containersResult = await tryAllCandidates(async (baseUrl) =>
              doFetch(buildAppApiUrl(baseUrl, `api/endpoints/${envId}/docker/containers/json`).toString() + '?all=1', portainerHeaders)
            );
            if (containersResult?.ok && Array.isArray(containersResult.json)) {
              const containers = containersResult.json;
              total = containers.length;
              running = containers.filter((c) => String(c.State || '').toLowerCase() === 'running').length;
              stopped = total - running;
            } else {
              // Fall back to snapshot stats
              const snap = endpoint.Snapshots?.[0] || endpoint.Snapshot || {};
              running = Number(snap.RunningContainerCount ?? snap.running) || 0;
              stopped = Number(snap.StoppedContainerCount ?? snap.stopped) || 0;
              total = running + stopped;
            }
          }
          metrics = [
            { key: 'running', label: 'Running', value: running },
            { key: 'stopped', label: 'Stopped', value: stopped },
            { key: 'total',   label: 'Total',   value: total },
          ];
        } else {
          status = 'down';
        }

      // ── glances ──────────────────────────────────────────────────────────
      } else if (typeId === 'glances') {
        const glancesAuth = {};
        const username = String(appItem.username || '').trim();
        const password = String(appItem.password || '').trim();
        if (username && password) glancesAuth.Authorization = buildBasicAuthHeader(username, password);
        const glancesHeaders = { Accept: 'application/json', ...glancesAuth };
        // Try v3 API then v4 naming
        const [cpuResult, memResult, loadResult] = await Promise.all([
          tryCandidatePaths(['api/3/cpu', 'api/4/cpu', 'api/cpu'], glancesHeaders),
          tryCandidatePaths(['api/3/mem', 'api/4/mem', 'api/mem'], glancesHeaders),
          tryCandidatePaths(['api/3/load', 'api/4/load', 'api/load'], glancesHeaders),
        ]);
        if (cpuResult?.ok || memResult?.ok) {
          status = 'up';
          const cpu = cpuResult?.json;
          const mem = memResult?.json;
          const load = loadResult?.json;
          const cpuPct = Number(cpu?.total ?? cpu?.user ?? 0);
          const memPct = Number(mem?.percent ?? 0);
          const loadVal = Number(load?.min5 ?? load?.['1min'] ?? load?.avg1 ?? 0);
          metrics = [
            { key: 'cpu',    label: 'CPU',      value: `${cpuPct.toFixed(1)}%` },
            { key: 'memory', label: 'Memory',   value: `${memPct.toFixed(1)}%` },
            { key: 'load',   label: 'Load Avg', value: loadVal.toFixed(2) },
          ];
        } else {
          status = 'down';
        }

      // ── uptime-kuma ──────────────────────────────────────────────────────
      } else if (typeId === 'uptime-kuma') {
        // Uptime Kuma exposes public status page heartbeat JSON; slug configured via uptimeKumaSlug field
        const slug = String(appItem.uptimeKumaSlug || 'default').trim();
        const result = await tryAllCandidates(async (baseUrl) =>
          doFetch(buildAppApiUrl(baseUrl, `api/status-page/heartbeat/${slug}`).toString(), { Accept: 'application/json' })
        );
        if (result?.ok && result.json) {
          status = 'up';
          const heartbeatList = result.json?.heartbeatList ?? result.json?.data ?? {};
          const monitorList = result.json?.monitorList ?? {};
          const monitors = Object.values(heartbeatList).map((beats) => {
            const latest = Array.isArray(beats) ? beats[beats.length - 1] : beats;
            return Number(latest?.status ?? -1);
          });
          const up = monitors.filter((s) => s === 1).length;
          const down = monitors.filter((s) => s === 0).length;
          const pending = monitors.filter((s) => s === 2).length;
          const maintenance = monitors.filter((s) => s === 3).length;
          metrics = [
            { key: 'up',          label: 'Up',          value: up },
            { key: 'down',        label: 'Down',        value: down },
            { key: 'pending',     label: 'Pending',     value: pending },
            { key: 'maintenance', label: 'Maintenance', value: maintenance },
          ];
        } else {
          status = 'down';
        }

      // ── speedtest-tracker ────────────────────────────────────────────────
      } else if (typeId === 'speedtest-tracker') {
        const stHeaders = { Accept: 'application/json' };
        if (apiKey) stHeaders['Authorization'] = `Bearer ${apiKey}`;
        const username = String(appItem.username || '').trim();
        const password = String(appItem.password || '').trim();
        if (!apiKey && username && password) stHeaders.Authorization = buildBasicAuthHeader(username, password);
        // Primary: /api/speedtest/latest (alexjustesen/speedtest-tracker v1.x backwards-compat route)
        // Fallback: /api/v1/results/latest for older forks; /api/healthcheck as last-resort ping
        const result = await tryCandidatePaths(
          ['api/speedtest/latest', 'api/v1/results/latest', 'api/healthcheck'],
          stHeaders
        );
        if (result?.ok && result.json) {
          status = 'up';
          const j = result.json?.data ?? result.json;
          // download/upload returned in Mbps by the backwards-compat route
          const rawDown = Number(j.download ?? j.download_bits ?? j.download_mbps ?? 0);
          const rawUp = Number(j.upload ?? j.upload_bits ?? j.upload_mbps ?? 0);
          // Values > 10000 are raw bits/s; smaller values are already Mbps
          const toMbps = (v) => v > 10000 ? `${(v / 1e6).toFixed(1)} Mbps` : `${Number(v).toFixed(1)} Mbps`;
          const ping = Number(j.ping ?? j.latency ?? 0);
          if (rawDown === 0 && rawUp === 0) {
            metrics = []; // healthcheck fallback — up but no test data yet
          } else {
            metrics = [
              { key: 'download', label: 'Download', value: toMbps(rawDown) },
              { key: 'upload',   label: 'Upload',   value: toMbps(rawUp) },
              { key: 'ping',     label: 'Ping',     value: `${ping.toFixed(0)} ms` },
            ];
          }
        } else {
          status = 'down';
        }

      // ── gluetun ──────────────────────────────────────────────────────────
      } else if (typeId === 'gluetun') {
        const [vpnResult, ipResult] = await Promise.all([
          tryAllCandidates(async (baseUrl) =>
            doFetch(buildAppApiUrl(baseUrl, 'v1/openvpn/status').toString(), { Accept: 'application/json' })
          ),
          tryAllCandidates(async (baseUrl) =>
            doFetch(buildAppApiUrl(baseUrl, 'v1/publicip/ip').toString(), { Accept: 'application/json' })
          ),
        ]);
        if (vpnResult?.ok || ipResult?.ok) {
          status = 'up';
          const vpnStatus = String(vpnResult?.json?.status ?? vpnResult?.json?.state ?? '').toLowerCase();
          const connected = vpnStatus === 'running' || vpnStatus === 'connected';
          const publicIp = String(ipResult?.json?.public_ip ?? ipResult?.json?.ip ?? '—');
          const country = String(ipResult?.json?.country ?? ipResult?.json?.location ?? '—');
          metrics = [
            { key: 'status',  label: 'VPN Status', value: connected ? 'Connected' : (vpnStatus || 'Unknown') },
            { key: 'ip',      label: 'Public IP',  value: publicIp },
            { key: 'country', label: 'Country',    value: country },
          ];
        } else {
          status = 'down';
        }

      // ── paperless-ngx ────────────────────────────────────────────────────
      } else if (typeId === 'paperless-ngx') {
        const plxHeaders = { Accept: 'application/json' };
        if (apiKey) plxHeaders.Authorization = `Token ${apiKey}`;
        const username = String(appItem.username || '').trim();
        const password = String(appItem.password || '').trim();
        if (!apiKey && username && password) plxHeaders.Authorization = buildBasicAuthHeader(username, password);
        const result = await tryCandidatePaths(
          ['api/statistics/', 'api/statistics', 'api/v1/statistics/'],
          plxHeaders
        );
        if (result?.ok && result.json) {
          status = 'up';
          const j = result.json;
          const documents = Number(j.documents_total ?? j.total ?? j.count) || 0;
          const inbox = Number(j.documents_inbox ?? j.inbox_count ?? j.inbox) || 0;
          metrics = [
            { key: 'documents', label: 'Documents', value: documents },
            { key: 'inbox',     label: 'Inbox',     value: inbox },
          ];
        } else {
          status = 'down';
        }

      // ── metube ───────────────────────────────────────────────────────────
      } else if (typeId === 'metube') {
        // MeTube exposes /history which returns { queue: [...], pending: [...], done: [...] }
        const result = await tryAllCandidates(async (baseUrl) =>
          doFetch(buildAppApiUrl(baseUrl, 'history').toString(), { Accept: 'application/json' })
        );
        if (result?.ok && result.json) {
          status = 'up';
          const j = result.json;
          const queueArr   = Array.isArray(j.queue)   ? j.queue   : [];
          const pendingArr = Array.isArray(j.pending) ? j.pending : [];
          const doneArr    = Array.isArray(j.done)    ? j.done    : [];
          const allActive  = [...queueArr, ...pendingArr];
          const downloading = allActive.filter((e) => {
            const s = String(e?.status ?? e?.state ?? '').toLowerCase();
            return s === 'downloading' || s === 'started';
          }).length;
          const queued = allActive.filter((e) => {
            const s = String(e?.status ?? e?.state ?? '').toLowerCase();
            return !s || s === 'pending' || s === 'queued';
          }).length;
          metrics = [
            { key: 'downloading', label: 'Downloading', value: downloading },
            { key: 'queued',      label: 'Queued',      value: queued },
            { key: 'done',        label: 'Done',        value: doneArr.length },
          ];
        } else {
          status = 'down';
        }

      // ── audiobookshelf ───────────────────────────────────────────────────
      } else if (typeId === 'audiobookshelf') {
        const absHeaders = { Accept: 'application/json' };
        if (apiKey) absHeaders.Authorization = `Bearer ${apiKey}`;
        const libsResult = await tryAllCandidates(async (baseUrl) =>
          doFetch(buildAppApiUrl(baseUrl, 'api/libraries').toString(), absHeaders)
        );
        if (libsResult?.ok && libsResult.json) {
          const libraries = Array.isArray(libsResult.json.libraries) ? libsResult.json.libraries : [];
          const statsResults = await Promise.all(
            libraries.map((lib) =>
              tryAllCandidates(async (baseUrl) =>
                doFetch(buildAppApiUrl(baseUrl, `api/libraries/${lib.id}/stats`).toString(), absHeaders)
              )
            )
          );
          let books = 0, podcasts = 0;
          libraries.forEach((lib, i) => {
            const s = statsResults[i]?.json ?? {};
            if (lib.mediaType === 'book') books += Number(s.totalItems ?? 0);
            else if (lib.mediaType === 'podcast') podcasts += Number(s.totalItems ?? 0);
          });
          status = 'up';
          metrics = [
            { key: 'books',    label: 'Books',    value: books },
            { key: 'podcasts', label: 'Podcasts', value: podcasts },
          ];
        } else {
          status = 'down';
        }

      // ── tdarr ────────────────────────────────────────────────────────────
      } else if (typeId === 'tdarr') {
        const tdarrBody = JSON.stringify({ data: { collection: 'StatisticsJSONDB', mode: 'getById', docID: 'statistics' } });
        const tdarrHeaders = { 'Content-Type': 'application/json', Accept: 'application/json' };
        if (apiKey) tdarrHeaders['x-api-key'] = apiKey;
        const result = await tryAllCandidates(async (baseUrl) =>
          doFetch(buildAppApiUrl(baseUrl, 'api/v2/cruddb').toString(), tdarrHeaders, { method: 'POST', body: tdarrBody })
        );
        if (result?.ok && result.json) {
          status = 'up';
          const j = result.json;
          const v = (a, b) => Number(j[a] ?? j[b] ?? 0);
          const queue     = v('table1ViewableCount', 'table1Count') + v('table4ViewableCount', 'table4Count');
          const processed = v('table2ViewableCount', 'table2Count') + v('table5ViewableCount', 'table5Count');
          const errored   = v('table3ViewableCount', 'table3Count') + v('table6ViewableCount', 'table6Count');
          const savedGb   = Math.abs(Number(j.sizeDiff ?? 0)).toFixed(2);
          metrics = [
            { key: 'queue',     label: 'Queue',       value: queue },
            { key: 'processed', label: 'Processed',   value: processed },
            { key: 'errored',   label: 'Errored',     value: errored },
            { key: 'saved',     label: 'Space Saved', value: `${savedGb} GB` },
          ];
        } else {
          status = 'down';
        }

      // ── guacamole ────────────────────────────────────────────────────────
      } else if (typeId === 'guacamole') {
        const guacUsername = String(appItem.username || '').trim();
        const guacPassword = String(appItem.password || '').trim();
        // Must use the same baseUrl for all requests after token auth
        let guacDone = false;
        for (const candidateUrl of candidates) {
          const tokenBody = new URLSearchParams({ username: guacUsername, password: guacPassword }).toString();
          const tokenResult = await doFetch(
            buildAppApiUrl(candidateUrl, 'api/tokens').toString(),
            { 'Content-Type': 'application/x-www-form-urlencoded', Accept: 'application/json' },
            { method: 'POST', body: tokenBody }
          );
          if (!tokenResult?.ok || !tokenResult.json?.authToken) continue;
          const { authToken, dataSource } = tokenResult.json;
          const ds = String(dataSource || 'mysql').trim();
          const tokenParam = `token=${encodeURIComponent(authToken)}`;
          const [activeResult, connResult, usersResult] = await Promise.all([
            doFetch(`${buildAppApiUrl(candidateUrl, `api/session/data/${ds}/activeConnections`)}?${tokenParam}`, { Accept: 'application/json' }),
            doFetch(`${buildAppApiUrl(candidateUrl, `api/session/data/${ds}/connections`)}?${tokenParam}`, { Accept: 'application/json' }),
            doFetch(`${buildAppApiUrl(candidateUrl, `api/session/data/${ds}/users`)}?${tokenParam}`, { Accept: 'application/json' }),
          ]);
          status = 'up';
          const activeSessions = activeResult?.ok && activeResult.json && typeof activeResult.json === 'object' ? Object.keys(activeResult.json).length : 0;
          const totalConnections = connResult?.ok && connResult.json && typeof connResult.json === 'object' ? Object.keys(connResult.json).length : 0;
          const totalUsers = usersResult?.ok && usersResult.json && typeof usersResult.json === 'object' ? Object.keys(usersResult.json).length : 0;
          metrics = [
            { key: 'active',      label: 'Active',      value: activeSessions },
            { key: 'connections', label: 'Connections', value: totalConnections },
            { key: 'users',       label: 'Users',       value: totalUsers },
          ];
          guacDone = true;
          break;
        }
        if (!guacDone) status = 'down';

      // ── traefik ──────────────────────────────────────────────────────────
      } else if (typeId === 'traefik') {
        const traefikHeaders = { Accept: 'application/json' };
        if (apiKey) traefikHeaders['Authorization'] = `Bearer ${apiKey}`;
        if (appItem.username && appItem.password) {
          traefikHeaders['Authorization'] = 'Basic ' + Buffer.from(`${appItem.username}:${appItem.password}`).toString('base64');
        }
        const result = await tryCandidatePaths(['api/overview'], traefikHeaders);
        if (result?.ok && result.json) {
          status = 'up';
          const j = result.json;
          metrics = [
            { key: 'routers',     label: 'Routers',     value: Number(j?.http?.routers?.total     ?? 0) },
            { key: 'services',    label: 'Services',    value: Number(j?.http?.services?.total    ?? 0) },
            { key: 'middlewares', label: 'Middlewares', value: Number(j?.http?.middlewares?.total ?? 0) },
          ];
        } else {
          status = 'down';
        }

      // ── dozzle ───────────────────────────────────────────────────────────
      } else if (typeId === 'dozzle') {
        // Helper: read the initial containers-changed SSE event from a connected response.
        // controller is aborted by the caller when done; timeoutMs guards against slow streams.
        const readDozzleContainersChanged = (sseRes, controller, timeoutMs = 3000) => {
          const body = sseRes?.body;
          if (!body || typeof body.getReader !== 'function') return Promise.resolve(null);
          const reader = body.getReader();
          const decoder = new TextDecoder();
          let buffer = '';

          return new Promise((resolve) => {
            let settled = false;
            let currentEvent = '';
            let currentDataLines = [];
            let timer = null;

            const cleanup = (result = null) => {
              if (settled) return;
              settled = true;
              if (timer) clearTimeout(timer);
              controller.signal.removeEventListener('abort', onAbort);
              reader.cancel().catch(() => {});
              resolve(result);
            };
            const onAbort = () => cleanup(null);
            const dispatchEvent = () => {
              if (currentEvent === 'containers-changed' && currentDataLines.length) {
                try {
                  const parsed = JSON.parse(currentDataLines.join('\n').trim());
                  if (Array.isArray(parsed)) { cleanup(parsed); return; }
                } catch {}
              }
              currentEvent = '';
              currentDataLines = [];
            };
            const parseLines = () => {
              const lines = buffer.split('\n');
              buffer = lines.pop() || '';
              for (const rawLine of lines) {
                if (settled) return;
                const line = rawLine.endsWith('\r') ? rawLine.slice(0, -1) : rawLine;
                if (!line) { dispatchEvent(); continue; }
                if (line.startsWith(':')) continue;
                if (line.startsWith('event:')) { currentEvent = line.slice(6).trim(); continue; }
                if (line.startsWith('data:')) currentDataLines.push(line.slice(5).trimStart());
              }
            };
            const pump = () => {
              if (settled) return;
              reader.read()
                .then(({ done, value }) => {
                  if (settled) return;
                  if (done) { buffer += '\n'; parseLines(); cleanup(null); return; }
                  if (value) { buffer += decoder.decode(value, { stream: true }); parseLines(); }
                  if (!settled) pump();
                })
                .catch(() => cleanup(null));
            };

            controller.signal.addEventListener('abort', onAbort, { once: true });
            timer = setTimeout(() => cleanup(null), Math.max(500, timeoutMs));
            pump();
          });
        };

        // Single 4-second budget covers ALL operations (auth + SSE across all candidates).
        // Per-step signals are composed with AbortSignal.any() so the tighter limit always wins.
        const budgetCtrl = new AbortController();
        const budgetTimer = setTimeout(() => budgetCtrl.abort(), 4000);
        let dozzleDone = false;

        try {
          const dozzleHeaders = { Accept: 'text/event-stream', 'Cache-Control': 'no-cache' };

          // Step 1: resolve credentials.
          // Dozzle supports two auth modes:
          //   a) Dozzle simple auth (users.yml) — POST /api/token → session cookie
          //   b) Reverse-proxy basic auth (e.g. Traefik) — Authorization: Basic header
          // Try (a) first; if /api/token succeeds use the cookie; otherwise fall back to (b).
          if (appItem.username && appItem.password) {
            const basicAuth = 'Basic ' + Buffer.from(`${appItem.username}:${appItem.password}`).toString('base64');
            let gotSessionCookie = false;
            for (const baseUrl of candidates) {
              if (budgetCtrl.signal.aborted) break;
              try {
                const tokenRes = await fetch(buildAppApiUrl(baseUrl, 'api/token').toString(), {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                  body: new URLSearchParams({ username: appItem.username, password: appItem.password }).toString(),
                  signal: AbortSignal.any([budgetCtrl.signal, AbortSignal.timeout(1500)]),
                });
                if (tokenRes.ok) {
                  const cookie = (tokenRes.headers.get('set-cookie') || '').split(';')[0].trim();
                  if (cookie) { dozzleHeaders['Cookie'] = cookie; gotSessionCookie = true; }
                }
                // Any HTTP response (200, 401, 404, etc.) means we've learned enough:
                // either we got a cookie (Dozzle simple-auth) or this instance has no token auth.
                // No need to try the remaining candidates — they're the same Dozzle instance.
                break;
              } catch {}
              // catch = timeout/network error: try next candidate
            }
            // Fall back to Basic auth (covers Traefik/proxy-level auth on the remote URL)
            if (!gotSessionCookie) dozzleHeaders['Authorization'] = basicAuth;
          }

          // Step 2: connect to SSE stream and read the initial containers-changed event
          for (const baseUrl of candidates) {
            if (budgetCtrl.signal.aborted) break;
            try {
              const sseRes = await fetch(buildAppApiUrl(baseUrl, 'api/events/stream').toString(), {
                headers: dozzleHeaders,
                signal: AbortSignal.any([budgetCtrl.signal, AbortSignal.timeout(2000)]),
              });
              if (!sseRes.ok) continue;

              // Wire an inner controller to the budget so cleanup always fires
              const innerCtrl = new AbortController();
              const propagate = () => innerCtrl.abort();
              budgetCtrl.signal.addEventListener('abort', propagate, { once: true });
              const containers = await readDozzleContainersChanged(sseRes, innerCtrl, 2500);
              budgetCtrl.signal.removeEventListener('abort', propagate);
              innerCtrl.abort(); // release any remaining reader

              if (containers !== null) {
                status = 'up';
                // Dozzle v10's SSE containers-changed initial event only includes running
                // containers — stopped containers are not in the snapshot. Report the count
                // as "Running" only; there is no reliable way to count stopped containers
                // from a single SSE connection.
                const running = containers.length;
                metrics = [
                  { key: 'running', label: 'Running', value: running },
                ];
                dozzleDone = true;
                break;
              }
            } catch {}
          }
        } finally {
          clearTimeout(budgetTimer);
          budgetCtrl.abort();
        }

        if (!dozzleDone) status = 'down';

      // ── qnap ─────────────────────────────────────────────────────────────
      } else if (typeId === 'qnap') {
        // QNAP uses a CGI XML API with session-based auth.
        // Step 1: POST /cgi-bin/authLogin.cgi with base64-encoded password to get a session ID.
        // Step 2: Use the session ID to fetch sysinfo (CPU/memory) and volume stats in parallel.
        const extractXmlCdata = (xml, tag) => {
          const m = xml.match(new RegExp(`<${tag}><!\\[CDATA\\[([^\\]]*?)\\]\\]><\\/${tag}>`))
            || xml.match(new RegExp(`<${tag}>([^<]*)<\\/${tag}>`));
          return m?.[1]?.trim() ?? null;
        };
        const qnapUser = String(appItem.username || '').trim();
        const b64pw = Buffer.from(String(appItem.password || '')).toString('base64');
        let qnapDone = false;

        for (const baseUrl of candidates) {
          if (qnapDone) break;
          try {
            // Authenticate
            const authRes = await fetch(buildAppApiUrl(baseUrl, 'cgi-bin/authLogin.cgi').toString(), {
              method: 'POST',
              headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
              body: `user=${encodeURIComponent(qnapUser)}&pwd=${encodeURIComponent(b64pw)}`,
              signal: AbortSignal.timeout(4000),
            });
            if (!authRes.ok) continue;
            const authXml = await authRes.text();
            const authSid = extractXmlCdata(authXml, 'authSid');
            const authPassed = extractXmlCdata(authXml, 'authPassed');
            if (!authSid || authPassed !== '1') continue;

            // Fetch sysinfo and volume stats in parallel
            const sysinfoUrl = buildAppApiUrl(baseUrl, 'cgi-bin/management/manaRequest.cgi');
            sysinfoUrl.searchParams.set('subfunc', 'sysinfo');
            sysinfoUrl.searchParams.set('hd', 'no');
            sysinfoUrl.searchParams.set('multicpu', '1');
            sysinfoUrl.searchParams.set('sid', authSid);

            const volumeUrl = buildAppApiUrl(baseUrl, 'cgi-bin/management/chartReq.cgi');
            volumeUrl.searchParams.set('chart_func', 'disk_usage');
            volumeUrl.searchParams.set('disk_select', 'all');
            volumeUrl.searchParams.set('include', 'all');
            volumeUrl.searchParams.set('sid', authSid);

            const [sysinfoRes, volumeRes] = await Promise.all([
              fetch(sysinfoUrl.toString(), { signal: AbortSignal.timeout(4000) }),
              fetch(volumeUrl.toString(), { signal: AbortSignal.timeout(4000) }),
            ]);
            if (!sysinfoRes.ok) continue;
            const sysinfoXml = await sysinfoRes.text();
            const volumeXml = volumeRes.ok ? await volumeRes.text() : '';

            // Parse CPU (returned as "85.8 %")
            const cpuRaw = extractXmlCdata(sysinfoXml, 'cpu_usage') || '';
            const cpuPct = parseFloat(cpuRaw.replace('%', '').trim());

            // Parse memory — QNAP returns MB for total_memory / free_memory
            const totalMemMb = parseFloat(extractXmlCdata(sysinfoXml, 'total_memory') || '0');
            const freeMemMb = parseFloat(extractXmlCdata(sysinfoXml, 'free_memory') || '0');
            const memPct = totalMemMb > 0 ? (totalMemMb - freeMemMb) / totalMemMb * 100 : 0;

            // Parse volume usage (bytes) — aggregate across all volumeUse blocks
            let volTotalBytes = 0;
            let volFreeBytes = 0;
            for (const m of volumeXml.matchAll(/<volumeUse>([\s\S]*?)<\/volumeUse>/g)) {
              const total = Number(extractXmlCdata(m[1], 'total_size'));
              const free = Number(extractXmlCdata(m[1], 'free_size'));
              if (Number.isFinite(total) && total > 0) volTotalBytes += total;
              if (Number.isFinite(free) && free >= 0) volFreeBytes += free;
            }
            const volPct = volTotalBytes > 0 ? (volTotalBytes - volFreeBytes) / volTotalBytes * 100 : 0;

            status = 'up';
            metrics = [
              { key: 'cpu',    label: 'CPU',    value: Number.isFinite(cpuPct)  ? `${cpuPct.toFixed(1)}%`  : '—' },
              { key: 'memory', label: 'Memory', value: totalMemMb > 0           ? `${memPct.toFixed(1)}%`  : '—' },
              { key: 'volume', label: 'Volume', value: volTotalBytes > 0        ? `${volPct.toFixed(1)}%`  : '—' },
            ];
            qnapDone = true;
          } catch {}
        }

        if (!qnapDone) status = 'down';

      // ── wizarr ───────────────────────────────────────────────────────────
      } else if (typeId === 'wizarr') {
        const wzHeaders = { Accept: 'application/json' };
        if (apiKey) wzHeaders['X-API-Key'] = apiKey;
        const result = await tryCandidatePaths(['api/status', 'api/v1/status'], wzHeaders);
        if (result?.ok && result.json) {
          status = 'up';
          const j = result.json;
          metrics = [
            { key: 'users',   label: 'Users',   value: Number(j.users   ?? 0) },
            { key: 'pending', label: 'Pending', value: Number(j.pending ?? 0) },
            { key: 'expired', label: 'Expired', value: Number(j.expired ?? 0) },
          ];
        } else {
          status = 'down';
        }

      // ── termix ───────────────────────────────────────────────────────────
      } else if (typeId === 'termix') {
        const headers = buildGenericApiHeaders();
        const lightProbeOpts = { timeoutMs: 2500, acceptStatuses: [401, 403] };
        const [statusResult, statsResult, sessionsResult, versionResult] = await Promise.all([
          tryCandidatePaths(['api/status', 'api/v1/status', 'status'], headers, lightProbeOpts),
          tryCandidatePaths(['api/stats', 'api/v1/stats', 'api/server/stats', 'api/system/stats'], headers, lightProbeOpts),
          tryCandidatePaths(['api/sessions', 'api/v1/sessions', 'api/session', 'api/v1/session'], headers, lightProbeOpts),
          tryCandidatePaths(['api/version', 'api/v1/version', 'version'], headers, lightProbeOpts),
        ]);
        const results = [statusResult, statsResult, sessionsResult, versionResult].filter(Boolean);
        if (hasReachableResult(results)) {
          status = 'up';
          const authRequired = hasAuthRequiredResult(results);
          const sessionsCount = extractCountFromPayload(sessionsResult?.json, {
            arrayKeys: ['sessions', 'connections'],
            numericKeys: ['activeSessions', 'sessionCount'],
            numericPaths: ['sessions.active', 'stats.sessions.active', 'stats.sessions.total'],
          });
          const clientsCount = extractCountFromPayload(statsResult?.json, {
            arrayKeys: ['clients', 'connections'],
            numericKeys: ['activeConnections', 'clientCount', 'connectionCount'],
            numericPaths: ['clients.active', 'connections.active', 'stats.clients.active'],
          });
          const uptimeText = formatDurationFromSeconds(
            pickFiniteFromPaths(statsResult?.json, ['uptimeSeconds', 'uptime', 'stats.uptimeSeconds', 'stats.uptime', 'server.uptime'])
              ?? pickFiniteFromPaths(statusResult?.json, ['uptimeSeconds', 'uptime', 'stats.uptimeSeconds', 'stats.uptime', 'server.uptime'])
          );
          metrics = [
            { key: 'sessions', label: 'Sessions', value: sessionsCount !== null ? sessionsCount : (authRequired ? 'Auth required' : '—') },
            { key: 'clients',  label: 'Clients',  value: clientsCount !== null ? clientsCount : (authRequired ? 'Auth required' : '—') },
            { key: 'uptime',   label: 'Uptime',   value: uptimeText || (authRequired ? 'Auth required' : '—') },
          ];
        } else {
          status = 'down';
        }

      // ── apprise ──────────────────────────────────────────────────────────
      } else if (typeId === 'apprise') {
        const headers = buildGenericApiHeaders();
        const result = await tryCandidatePaths(
          ['status', 'api/status', 'api/v1/status'],
          headers,
          { timeoutMs: 2500, acceptStatuses: [401, 403] }
        );
        const results = [result].filter(Boolean);
        if (hasReachableResult(results)) {
          status = 'up';
          const authRequired = hasAuthRequiredResult(results);
          const payload = (result?.json && typeof result.json === 'object') ? result.json : {};
          const stateText = pickTextFromPaths(payload, ['status', 'result', 'state', 'details.status', 'details.state'])
            || (authRequired ? 'Auth required' : 'Online');
          const queued = extractCountFromPayload(payload, {
            numericKeys: ['queued', 'queue', 'pending'],
            numericPaths: ['details.queued', 'details.queue', 'details.queues.pending', 'queues.pending', 'queue.pending'],
          });
          const versionText = pickTextFromPaths(payload, ['version', 'appVersion', 'app_version', 'details.version']);
          metrics = [
            { key: 'state',   label: 'State',   value: stateText },
            { key: 'queued',  label: 'Queued',  value: queued !== null ? queued : (authRequired ? 'Auth required' : '—') },
            { key: 'version', label: 'Version', value: versionText || '—' },
          ];
        } else {
          status = 'down';
        }

      // ── ersatztv ─────────────────────────────────────────────────────────
      } else if (typeId === 'ersatztv') {
        const headers = buildGenericApiHeaders();
        const lightProbeOpts = { timeoutMs: 2500, acceptStatuses: [401, 403] };
        const [channelsResult, sessionsResult] = await Promise.all([
          tryCandidatePaths(['api/channels', 'api/v1/channels'], headers, lightProbeOpts),
          tryCandidatePaths(['api/sessions'], headers, lightProbeOpts),
        ]);
        const results = [channelsResult, sessionsResult].filter(Boolean);
        if (hasReachableResult(results)) {
          status = 'up';
          const authRequired = hasAuthRequiredResult(results);
          const channels = extractCountFromPayload(channelsResult?.json, {
            arrayKeys: ['channels'],
            numericKeys: ['channelCount'],
            numericPaths: ['stats.channels', 'counts.channels'],
          });
          const streams = Array.isArray(sessionsResult?.json) ? sessionsResult.json.length : null;
          metrics = [
            { key: 'channels', label: 'Channels', value: channels !== null ? channels : (authRequired ? 'Auth required' : '—') },
            { key: 'streams',  label: 'Streams',  value: streams !== null ? streams : (authRequired ? 'Auth required' : '—') },
          ];
        } else {
          status = 'down';
        }

      // ── sortarr ──────────────────────────────────────────────────────────
      } else if (typeId === 'sortarr') {
        const headers = buildGenericApiHeaders();
        const lightProbeOpts = { timeoutMs: 2500, acceptStatuses: [401, 403] };
        const [healthResult, versionResult, profilesResult, rulesResult] = await Promise.all([
          tryCandidatePaths(['api/health', 'api/v1/health', 'api/status', 'api/v1/status', 'health'], headers, lightProbeOpts),
          tryCandidatePaths(['api/version', 'api/v1/version'], headers, lightProbeOpts),
          tryCandidatePaths(['api/profiles', 'api/v1/profiles', 'api/profile', 'api/v1/profile'], headers, lightProbeOpts),
          tryCandidatePaths(['api/rules', 'api/v1/rules', 'api/sorts', 'api/v1/sorts'], headers, lightProbeOpts),
        ]);
        const results = [healthResult, versionResult, profilesResult, rulesResult].filter(Boolean);
        if (hasReachableResult(results)) {
          status = 'up';
          const authRequired = hasAuthRequiredResult(results);
          const profiles = extractCountFromPayload(profilesResult?.json, {
            arrayKeys: ['profiles'],
            numericKeys: ['profileCount'],
            numericPaths: ['stats.profiles', 'counts.profiles'],
          });
          const rules = extractCountFromPayload(rulesResult?.json, {
            arrayKeys: ['rules', 'sorts'],
            numericKeys: ['ruleCount', 'sortCount'],
            numericPaths: ['stats.rules', 'counts.rules'],
          });
          const versionText = pickTextFromPaths(versionResult?.json, ['version', 'appVersion', 'apiVersion', 'build.version']);
          metrics = [
            { key: 'profiles', label: 'Profiles', value: profiles !== null ? profiles : (authRequired ? 'Auth required' : '—') },
            { key: 'rules',    label: 'Rules',    value: rules !== null ? rules : (authRequired ? 'Auth required' : '—') },
            { key: 'version',  label: 'Version',  value: versionText || '—' },
          ];
        } else {
          status = 'down';
        }

      // ── agregarr ─────────────────────────────────────────────────────────
      } else if (typeId === 'agregarr') {
        const headers = buildGenericApiHeaders();
        const lightProbeOpts = { timeoutMs: 2500, acceptStatuses: [401, 403] };
        const [healthResult, versionResult, appsResult, rulesResult] = await Promise.all([
          tryCandidatePaths(['api/health', 'api/v1/health', 'api/status', 'api/v1/status', 'health'], headers, lightProbeOpts),
          tryCandidatePaths(['api/version', 'api/v1/version'], headers, lightProbeOpts),
          tryCandidatePaths(['api/apps', 'api/v1/apps', 'api/arrs', 'api/v1/arrs'], headers, lightProbeOpts),
          tryCandidatePaths(['api/rules', 'api/v1/rules', 'api/filters', 'api/v1/filters'], headers, lightProbeOpts),
        ]);
        const results = [healthResult, versionResult, appsResult, rulesResult].filter(Boolean);
        if (hasReachableResult(results)) {
          status = 'up';
          const authRequired = hasAuthRequiredResult(results);
          const appsCount = extractCountFromPayload(appsResult?.json, {
            arrayKeys: ['apps', 'arrs'],
            numericKeys: ['appCount'],
            numericPaths: ['stats.apps', 'counts.apps'],
          });
          const rulesCount = extractCountFromPayload(rulesResult?.json, {
            arrayKeys: ['rules', 'filters'],
            numericKeys: ['ruleCount', 'filterCount'],
            numericPaths: ['stats.rules', 'counts.rules'],
          });
          const versionText = pickTextFromPaths(versionResult?.json, ['version', 'appVersion', 'apiVersion', 'build.version']);
          metrics = [
            { key: 'apps',    label: 'Apps',    value: appsCount !== null ? appsCount : (authRequired ? 'Auth required' : '—') },
            { key: 'rules',   label: 'Rules',   value: rulesCount !== null ? rulesCount : (authRequired ? 'Auth required' : '—') },
            { key: 'version', label: 'Version', value: versionText || '—' },
          ];
        } else {
          status = 'down';
        }

      // ── profilarr ────────────────────────────────────────────────────────
      } else if (typeId === 'profilarr') {
        const headers = buildGenericApiHeaders();
        const [settingsResult, tasksResult] = await Promise.all([
          tryCandidatePaths(['api/settings'], headers, { timeoutMs: 2500, acceptStatuses: [401, 403] }),
          tryCandidatePaths(['api/tasks'], headers, { timeoutMs: 2500, acceptStatuses: [401, 403] }),
        ]);
        const results = [settingsResult, tasksResult].filter(Boolean);
        if (hasReachableResult(results)) {
          status = 'up';
          const authRequired = hasAuthRequiredResult(results);
          const tasks = Array.isArray(tasksResult?.json) ? tasksResult.json : [];
          const taskStatus = (name) => {
            const task = tasks.find((t) => String(t?.name || '').toLowerCase().includes(name));
            if (!task) return null;
            const s = String(task.status || '').toLowerCase();
            if (s === 'success') return 'OK';
            if (s === 'running' || s === 'in_progress') return 'Syncing';
            if (s === 'failed' || s === 'error') return 'Failed';
            return s || '—';
          };
          const syncStatus  = taskStatus('sync');
          const backupStatus = taskStatus('backup');
          metrics = [
            { key: 'sync',   label: 'Sync',   value: syncStatus   !== null ? syncStatus   : (authRequired ? 'Auth required' : '—') },
            { key: 'backup', label: 'Backup', value: backupStatus !== null ? backupStatus : (authRequired ? 'Auth required' : '—') },
          ];
        } else {
          status = 'down';
        }

      // ── guardian ─────────────────────────────────────────────────────────
      } else if (typeId === 'guardian') {
        const headers = buildGenericApiHeaders();
        const lightProbeOpts = { timeoutMs: 2500, acceptStatuses: [401, 403] };
        const [healthResult, versionResult, devicesResult, blockedResult] = await Promise.all([
          tryCandidatePaths(['api/health', 'api/v1/health', 'api/status', 'api/v1/status', 'health'], headers, lightProbeOpts),
          tryCandidatePaths(['api/version', 'api/v1/version'], headers, lightProbeOpts),
          tryCandidatePaths(['api/devices', 'api/v1/devices', 'api/clients', 'api/v1/clients'], headers, lightProbeOpts),
          tryCandidatePaths(['api/blocked', 'api/v1/blocked', 'api/strikes', 'api/v1/strikes'], headers, lightProbeOpts),
        ]);
        const results = [healthResult, versionResult, devicesResult, blockedResult].filter(Boolean);
        if (hasReachableResult(results)) {
          status = 'up';
          const authRequired = hasAuthRequiredResult(results);
          const devices = extractCountFromPayload(devicesResult?.json, {
            arrayKeys: ['devices', 'clients'],
            numericKeys: ['deviceCount', 'clientCount'],
            numericPaths: ['stats.devices', 'counts.devices'],
          });
          const blocked = extractCountFromPayload(blockedResult?.json, {
            arrayKeys: ['blocked', 'strikes'],
            numericKeys: ['blockedCount', 'strikeCount'],
            numericPaths: ['stats.blocked', 'counts.blocked'],
          });
          const versionText = pickTextFromPaths(versionResult?.json, ['version', 'appVersion', 'apiVersion', 'build.version']);
          metrics = [
            { key: 'devices', label: 'Devices', value: devices !== null ? devices : (authRequired ? 'Auth required' : '—') },
            { key: 'blocked', label: 'Blocked', value: blocked !== null ? blocked : (authRequired ? 'Auth required' : '—') },
            { key: 'version', label: 'Version', value: versionText || '—' },
          ];
        } else {
          status = 'down';
        }

      // ── fallback: generic reachability check for unsupported app types ──
      } else {
        const headers = { Accept: 'application/json' };
        if (apiKey) {
          headers['X-Api-Key'] = apiKey;
          if (!headers.Authorization) {
            headers.Authorization = /^bearer\s+/i.test(apiKey) ? apiKey : `Bearer ${apiKey}`;
          }
        }
        const genericAuth = buildBasicAuthHeader(appItem.username || '', appItem.password || '');
        if (genericAuth && !headers.Authorization) headers.Authorization = genericAuth;

        const genericProbePaths = [''];
        const genericResult = await tryCandidatePaths(genericProbePaths, headers, { timeoutMs: 1500 });
        const statusCode = Number(genericResult?.status);
        const latencyMs = Number(genericResult?.durationMs);
        metrics = [
          { key: 'http_status', label: 'HTTP', value: Number.isFinite(statusCode) && statusCode > 0 ? statusCode : '—' },
          { key: 'latency_ms', label: 'Latency', value: Number.isFinite(latencyMs) ? `${Math.max(0, Math.round(latencyMs))} ms` : '—' },
        ];
        status = genericResult && (genericResult.ok || (Number(genericResult.status) >= 200 && Number(genericResult.status) < 500))
          ? 'up'
          : 'down';
      }

      if (status === 'down') {
        pushLog({ level: 'error', app: typeId, action: 'widget.stats.down',
          message: `Widget stats returned down for ${typeId} (${rawAppId}). candidates: ${candidates.join(', ')}` });
      }
      return res.json({ ok: true, appId: rawAppId, typeId, status, metrics, ...(libraryInfo ? { libraryInfo } : {}) });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to fetch widget stats.' });
    }
  });

  if (!app.locals.__launcharrWidgetStatusMonitorStarted) {
    app.locals.__launcharrWidgetStatusMonitorStarted = true;
    if (!widgetStatusInternalToken) {
      pushLog({
        level: 'warn',
        app: 'widgets',
        action: 'notifications.widget-status.monitor',
        message: 'Widget status monitor disabled: missing internal token.',
      });
    } else {
      const scheduleNextMonitorTick = () => {
        const config = loadConfig();
        const notificationSettings = resolveNotificationSettings(config);
        const runtimeSettings = resolveWidgetMonitorRuntimeSettings(notificationSettings);
        const monitorTimer = setTimeout(async () => {
          try {
            await runWidgetStatusMonitorTick();
          } catch {}
          scheduleNextMonitorTick();
        }, runtimeSettings.pollMs);
        if (typeof monitorTimer.unref === 'function') monitorTimer.unref();
        app.locals.__launcharrWidgetStatusMonitorTimer = monitorTimer;
      };
      runWidgetStatusMonitorTick()
        .catch(() => {})
        .finally(() => {
          scheduleNextMonitorTick();
        });
    }
  }

}
