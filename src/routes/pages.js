export function registerPages(app, ctx) {
  const {
    requireUser,
    requireAdmin,
    loadConfig,
    getEffectiveRole,
    getActualRole,
    canAccessDashboardApp,
    canAccess,
    canAccessOverviewElement,
    // dashboard helpers
    resolveDashboardSelection,
    resolveRequestedDashboardId,
    resolveBestRoleLandingPath,
    applyDashboardStateSnapshot,
    resolveCategoryEntries,
    buildAppBaseUrls,
    resolveArrDashboardCombineSettings,
    resolveMediaDashboardCombineSettings,
    resolveCombinedQueueDisplaySettings,
    resolveDownloaderDashboardCombineSettings,
    getNavApps,
    buildNavCategories,
    buildCategoryRank,
    canAccessDashboardElement,
    mergeOverviewElementSettings,
    resolveArrDashboardCombinedCards,
    resolveDownloaderDashboardCards,
    resolveMediaDashboardCards,
    isAppInSet,
    normalizeAppId,
    canAccessCombinedDashboardVisibility,
    getArrCombineSection,
    getDownloaderCombineSection,
    getMediaCombineSection,
    normalizeCombinedCardToken,
    buildArrCombinedDisplayMeta,
    buildDownloaderCombinedDisplayMeta,
    buildMediaCombinedDisplayMeta,
    getMultiInstanceBaseIds,
    getMultiInstancePlaceholderMap,
    mergeTautulliCardSettings,
    resolveDashboardDefinitions,
    resolvePersistedAppIconPath,
    // launch helpers
    resolveDeepLaunchUrl,
    resolveRoleAwareLaunchUrl,
    resolveEffectiveLaunchMode,
    resolveIframeLaunchUrl,
    hasEmbeddedUrlCredentials,
    stripUrlEmbeddedCredentials,
    buildRommCookiePrimingPlan,
    bootstrapRommIframeSession,
    resolveAppApiCandidates,
    evaluateRommCookiePrimingCompatibility,
    prepareRommPrimedSetCookies,
    logRommLaunchServerDiagnostic,
    sendClientLaunchRedirectPage,
    normalizeMenu,
    resolveLaunchUrl,
    // app settings helpers
    loadAdmins,
    // user-settings helpers
    resolveGeneralSettings,
    resolveLocalUsers,
    findLocalUserIndex,
    // constants
    DASHBOARD_MAIN_ID,
    DEFAULT_DASHBOARD_ICON,
    ARR_APP_IDS,
    DOWNLOADER_APP_IDS,
    MEDIA_APP_IDS,
    ARR_COMBINE_SECTIONS,
    DOWNLOADER_COMBINE_SECTIONS,
    MEDIA_COMBINE_SECTIONS,
    ARR_COMBINED_SECTION_PREFIX,
    DOWNLOADER_COMBINED_SECTION_PREFIX,
    MEDIA_COMBINED_SECTION_PREFIX,
    resolveWidgetBars,
  } = ctx;

  app.get('/dashboard', requireUser, (req, res) => {
    const baseConfig = loadConfig();
    const role = getEffectiveRole(req);
    const actualRole = getActualRole(req);
    const dashboardSelection = resolveDashboardSelection(baseConfig, resolveRequestedDashboardId(req), role);
    if (!dashboardSelection.visibleDashboards.length || !dashboardSelection.activeDashboard) {
      return res.redirect(resolveBestRoleLandingPath(req, role, { config: baseConfig, excludeDashboard: true }));
    }
    const activeDashboard = dashboardSelection.activeDashboard;
    const config = applyDashboardStateSnapshot(baseConfig, activeDashboard);
    const apps = config.apps || [];
    const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
      ? config.dashboardRemovedElements
      : {};
    const categoryEntries = resolveCategoryEntries(config, apps);
    const categoryOrder = categoryEntries.map((entry) => entry.name);
    const appBaseUrls = buildAppBaseUrls(apps, req);
    const arrDashboardCombine = resolveArrDashboardCombineSettings(config, apps);
    const mediaDashboardCombine = resolveMediaDashboardCombineSettings(config, apps);
    const arrCombinedQueueDisplay = resolveCombinedQueueDisplaySettings(config, 'arrCombinedQueueDisplay');
    const downloaderCombinedQueueDisplay = resolveCombinedQueueDisplaySettings(config, 'downloaderCombinedQueueDisplay');
    const downloaderDashboardCombine = resolveDownloaderDashboardCombineSettings(config, apps);
    const dashboardCombinedSettings = (config && typeof config.dashboardCombinedSettings === 'object' && config.dashboardCombinedSettings)
      ? config.dashboardCombinedSettings
      : {};
    const dashboardCombinedOrder = (config && typeof config.dashboardCombinedOrder === 'object' && config.dashboardCombinedOrder)
      ? config.dashboardCombinedOrder
      : {};
    const navApps = getNavApps(apps, role, req, categoryOrder);
    const navCategories = buildNavCategories(navApps, categoryEntries, role);
    const rankCategory = buildCategoryRank(categoryOrder);
  
    const dashboardAccessibleApps = apps.filter((appItem) => !appItem?.removed && canAccessDashboardApp(config, appItem, role));
    const arrDashboardCombinedCards = resolveArrDashboardCombinedCards(config, dashboardAccessibleApps);
    const downloaderDashboardCards = resolveDownloaderDashboardCards(config, dashboardAccessibleApps);
    const mediaDashboardCards = resolveMediaDashboardCards(config, dashboardAccessibleApps);
    const arrDashboardAppIds = dashboardAccessibleApps
      .filter((appItem) => isAppInSet(appItem.id, ARR_APP_IDS))
      .map((appItem) => appItem.id);
    const arrDashboardAppLookup = new Map(
      dashboardAccessibleApps
        .filter((appItem) => isAppInSet(appItem.id, ARR_APP_IDS))
        .map((appItem) => [normalizeAppId(appItem.id), appItem])
    );
    const downloaderDashboardAppIds = dashboardAccessibleApps
      .filter((appItem) => isAppInSet(appItem.id, DOWNLOADER_APP_IDS))
      .map((appItem) => appItem.id);
    const downloaderDashboardAppLookup = new Map(
      dashboardAccessibleApps
        .filter((appItem) => isAppInSet(appItem.id, DOWNLOADER_APP_IDS))
        .map((appItem) => [normalizeAppId(appItem.id), appItem])
    );
    const mediaDashboardAppIds = dashboardAccessibleApps
      .filter((appItem) => isAppInSet(appItem.id, MEDIA_APP_IDS))
      .map((appItem) => appItem.id);
    const mediaDashboardAppLookup = new Map(
      dashboardAccessibleApps
        .filter((appItem) => isAppInSet(appItem.id, MEDIA_APP_IDS))
        .map((appItem) => [normalizeAppId(appItem.id), appItem])
    );
    const appById = new Map(dashboardAccessibleApps.map((appItem) => [appItem.id, appItem]));
    const elementsByAppId = new Map(
      dashboardAccessibleApps.map((appItem) => [appItem.id, mergeOverviewElementSettings(appItem)])
    );
    const dashboardModules = dashboardAccessibleApps
      .map((appItem) => ({
        app: appItem,
        elements: (elementsByAppId.get(appItem.id) || []).filter((item) => (
          canAccessDashboardElement(appItem, item, role)
          && !dashboardRemovedElements[`app:${appItem.id}:${item.id}`]
        )),
      }))
      .filter((entry) => entry.elements.length)
      .flatMap((entry) =>
        entry.elements.map((element) => ({
          app: entry.app,
          element,
          category: entry.app.category || 'Tools',
        }))
      )
      .sort((a, b) => {
        const orderDelta = (a.element.order || 0) - (b.element.order || 0);
        if (orderDelta !== 0) return orderDelta;
        return String(a.element.name || '').localeCompare(String(b.element.name || ''));
      })
      .map((item) => ({
        ...item,
        arrCombined: null,
        downloaderCombined: null,
        mediaCombined: null,
      }));
  
    const buildSectionModules = (appIds, elementId) => appIds
      .map((appId) => {
        const app = appById.get(appId);
        if (!app) return null;
        const elements = elementsByAppId.get(appId) || [];
        const element = elements.find((item) => item.id === elementId);
        if (!element) return null;
        return {
          app,
          element,
          category: app.category || 'Tools',
        };
      })
      .filter(Boolean);
  
    ARR_COMBINE_SECTIONS.forEach((section) => {
      const combinedKey = `combined:arr:${section.key}`;
      if (dashboardRemovedElements[combinedKey]) return;
      const combinedSettings = dashboardCombinedSettings[combinedKey];
      if (!canAccessCombinedDashboardVisibility(combinedSettings, role, 'user')) return;
      const sectionModules = buildSectionModules(arrDashboardAppIds, section.elementId);
      const combinedModules = sectionModules.filter((item) =>
        Boolean(arrDashboardCombine?.[section.key]?.[item.app.id])
      );
      const combinedApps = combinedModules.length ? combinedModules : sectionModules;
      if (!combinedApps.length) return;
  
      const leader = combinedApps[0];
      const combinedAppIds = combinedApps.map((item) => normalizeAppId(item.app.id)).filter(Boolean);
      const meta = buildArrCombinedDisplayMeta(arrDashboardAppLookup, section.key, combinedAppIds);
      const combinedEntry = {
        ...leader,
        arrCombined: {
          sectionKey: section.key,
          elementId: section.elementId,
          appIds: meta.appIds,
          appNames: meta.appNames,
          displayName: meta.displayName,
          iconPath: meta.iconPath,
          custom: false,
          cardId: '',
          moduleKey: ARR_COMBINED_SECTION_PREFIX[section.key] || (`arrcombined-${section.key}`),
        },
        downloaderCombined: null,
        mediaCombined: null,
      };
      const insertIndex = dashboardModules.findIndex((item) =>
        arrDashboardAppIds.includes(item.app.id) && item.element.id === section.elementId
      );
      dashboardModules.splice(insertIndex === -1 ? dashboardModules.length : insertIndex, 0, combinedEntry);
    });
    arrDashboardCombinedCards.forEach((card, index) => {
      const section = getArrCombineSection(card.sectionKey);
      if (!section) return;
      const customToken = normalizeCombinedCardToken(card.id) || `card-${index + 1}`;
      const combinedKey = `combined:arrcustom:${customToken}`;
      if (dashboardRemovedElements[combinedKey]) return;
      const combinedSettings = dashboardCombinedSettings[combinedKey];
      if (!canAccessCombinedDashboardVisibility(combinedSettings, role, 'user')) return;
      const sectionModules = buildSectionModules(card.appIds, section.elementId);
      if (!sectionModules.length) return;
      const leader = sectionModules[0];
      const orderedAppIds = [...new Set(sectionModules.map((item) => normalizeAppId(item.app.id)).filter(Boolean))];
      const meta = buildArrCombinedDisplayMeta(arrDashboardAppLookup, card.sectionKey, orderedAppIds);
      if (!meta.appIds.length) return;
      const modulePrefixBase = ARR_COMBINED_SECTION_PREFIX[card.sectionKey] || `arrcombined-${card.sectionKey}`;
      dashboardModules.push({
        ...leader,
        arrCombined: {
          sectionKey: card.sectionKey,
          elementId: section.elementId,
          appIds: meta.appIds,
          appNames: meta.appNames,
          displayName: meta.displayName,
          iconPath: meta.iconPath,
          custom: true,
          cardId: customToken,
          moduleKey: `${modulePrefixBase}-${customToken}`,
        },
        downloaderCombined: null,
        mediaCombined: null,
      });
    });
  
    DOWNLOADER_COMBINE_SECTIONS.forEach((section) => {
      const combinedKey = `combined:downloader:${section.key}`;
      if (dashboardRemovedElements[combinedKey]) return;
      const combinedSettings = dashboardCombinedSettings[combinedKey];
      if (!canAccessCombinedDashboardVisibility(combinedSettings, role, 'user')) return;
      const sectionModules = buildSectionModules(downloaderDashboardAppIds, section.elementId);
      const combinedModules = sectionModules.filter((item) =>
        Boolean(downloaderDashboardCombine?.[section.key]?.[item.app.id])
      );
      const combinedApps = combinedModules.length ? combinedModules : sectionModules;
      if (!combinedApps.length) return;
  
      const leader = combinedApps[0];
      const combinedAppIds = combinedApps.map((item) => normalizeAppId(item.app.id)).filter(Boolean);
      const meta = buildDownloaderCombinedDisplayMeta(downloaderDashboardAppLookup, section.key, combinedAppIds);
      const combinedEntry = {
        ...leader,
        downloaderCombined: {
          sectionKey: section.key,
          elementId: section.elementId,
          appIds: meta.appIds,
          appNames: meta.appNames,
          displayName: meta.displayName,
          iconPath: meta.iconPath,
          custom: false,
          cardId: '',
          moduleKey: DOWNLOADER_COMBINED_SECTION_PREFIX[section.key] || `downloaderscombined-${section.key}`,
        },
        arrCombined: null,
        mediaCombined: null,
      };
      const insertIndex = dashboardModules.findIndex((item) =>
        downloaderDashboardAppIds.includes(item.app.id) && item.element.id === section.elementId
      );
      dashboardModules.splice(insertIndex === -1 ? dashboardModules.length : insertIndex, 0, combinedEntry);
    });
    downloaderDashboardCards.forEach((card, index) => {
      const section = getDownloaderCombineSection(card.sectionKey);
      if (!section) return;
      const customToken = normalizeCombinedCardToken(card.id) || `card-${index + 1}`;
      const combinedKey = `combined:downloadercustom:${customToken}`;
      if (dashboardRemovedElements[combinedKey]) return;
      const combinedSettings = dashboardCombinedSettings[combinedKey];
      if (!canAccessCombinedDashboardVisibility(combinedSettings, role, 'user')) return;
      const sectionModules = buildSectionModules(card.appIds, section.elementId);
      if (!sectionModules.length) return;
      const leader = sectionModules[0];
      const orderedAppIds = [...new Set(sectionModules.map((item) => normalizeAppId(item.app.id)).filter(Boolean))];
      const meta = buildDownloaderCombinedDisplayMeta(downloaderDashboardAppLookup, card.sectionKey, orderedAppIds);
      if (!meta.appIds.length) return;
      const modulePrefixBase = DOWNLOADER_COMBINED_SECTION_PREFIX[card.sectionKey] || `downloaderscombined-${card.sectionKey}`;
      dashboardModules.push({
        ...leader,
        downloaderCombined: {
          sectionKey: card.sectionKey,
          elementId: section.elementId,
          appIds: meta.appIds,
          appNames: meta.appNames,
          displayName: meta.displayName,
          iconPath: meta.iconPath,
          custom: true,
          cardId: customToken,
          moduleKey: `${modulePrefixBase}-${customToken}`,
        },
        arrCombined: null,
        mediaCombined: null,
      });
    });
  
    MEDIA_COMBINE_SECTIONS.forEach((section) => {
      const combinedKey = `combined:media:${section.key}`;
      if (dashboardRemovedElements[combinedKey]) return;
      const combinedSettings = dashboardCombinedSettings[combinedKey];
      if (!canAccessCombinedDashboardVisibility(combinedSettings, role, 'user')) return;
      const sectionModules = buildSectionModules(mediaDashboardAppIds, section.elementId);
      const combinedModules = sectionModules.filter((item) =>
        Boolean(mediaDashboardCombine?.[section.key]?.[item.app.id])
      );
      const combinedApps = combinedModules.length ? combinedModules : sectionModules;
      if (!combinedApps.length) return;
  
      const leader = combinedApps[0];
      const combinedAppIds = combinedApps.map((item) => normalizeAppId(item.app.id)).filter(Boolean);
      const meta = buildMediaCombinedDisplayMeta(mediaDashboardAppLookup, section.key, combinedAppIds);
      const combinedEntry = {
        ...leader,
        mediaCombined: {
          sectionKey: section.key,
          elementId: section.elementId,
          appIds: meta.appIds,
          appNames: meta.appNames,
          displayName: meta.displayName,
          iconPath: meta.iconPath,
          custom: false,
          cardId: '',
          moduleKey: MEDIA_COMBINED_SECTION_PREFIX[section.key] || `mediacombined-${section.key}`,
        },
        arrCombined: null,
        downloaderCombined: null,
      };
      const insertIndex = dashboardModules.findIndex((item) =>
        mediaDashboardAppIds.includes(item.app.id) && item.element.id === section.elementId
      );
      dashboardModules.splice(insertIndex === -1 ? dashboardModules.length : insertIndex, 0, combinedEntry);
    });
    mediaDashboardCards.forEach((card, index) => {
      const section = getMediaCombineSection(card.sectionKey);
      if (!section) return;
      const customToken = normalizeCombinedCardToken(card.id) || `card-${index + 1}`;
      const combinedKey = `combined:mediacustom:${customToken}`;
      if (dashboardRemovedElements[combinedKey]) return;
      const combinedSettings = dashboardCombinedSettings[combinedKey];
      if (!canAccessCombinedDashboardVisibility(combinedSettings, role, 'user')) return;
      const sectionModules = buildSectionModules(card.appIds, section.elementId);
      if (!sectionModules.length) return;
      const leader = sectionModules[0];
      const orderedAppIds = [...new Set(sectionModules.map((item) => normalizeAppId(item.app.id)).filter(Boolean))];
      const meta = buildMediaCombinedDisplayMeta(mediaDashboardAppLookup, card.sectionKey, orderedAppIds);
      if (!meta.appIds.length) return;
      const modulePrefixBase = MEDIA_COMBINED_SECTION_PREFIX[card.sectionKey] || `mediacombined-${card.sectionKey}`;
      dashboardModules.push({
        ...leader,
        mediaCombined: {
          sectionKey: card.sectionKey,
          elementId: section.elementId,
          appIds: meta.appIds,
          appNames: meta.appNames,
          displayName: meta.displayName,
          iconPath: meta.iconPath,
          custom: true,
          cardId: customToken,
          moduleKey: `${modulePrefixBase}-${customToken}`,
        },
        arrCombined: null,
        downloaderCombined: null,
      });
    });
  
    // ── Widget bars ──────────────────────────────────────────────────────────
    const getWidgetBarDashboardOrder = (bar) => {
      const barId = String(bar?.id || '').trim();
      const combinedOrderKey = `combined:widgetbar:${barId}`;
      const storedOrder = Number(dashboardCombinedOrder?.[combinedOrderKey]);
      if (Number.isFinite(storedOrder)) return storedOrder;
      const barOrder = Number(bar?.order);
      return Number.isFinite(barOrder) ? barOrder : 999;
    };
    const visibleWidgetBars = resolveWidgetBars(config, apps, role)
      .filter((bar) => !dashboardRemovedElements[`widget-bar:${bar.id}`])
      .sort((a, b) => {
        const orderDelta = getWidgetBarDashboardOrder(a) - getWidgetBarDashboardOrder(b);
        if (orderDelta !== 0) return orderDelta;
        return String(a?.name || '').localeCompare(String(b?.name || ''));
      });
    visibleWidgetBars.forEach((bar) => {
      const effectiveOrder = getWidgetBarDashboardOrder(bar);
      dashboardModules.push({
        app: { id: `widget-bar-${bar.id}`, name: bar.name || 'Widget Bar', order: effectiveOrder },
        element: { id: 'widget-bar', name: bar.name || 'Widget Bar', order: effectiveOrder },
        category: 'Widgets',
        arrCombined: null,
        downloaderCombined: null,
        mediaCombined: null,
        widgetBar: bar,
      });
    });

    const getCombinedOrderKey = (item) => {
      if (item?.widgetBar) return `combined:widgetbar:${item.widgetBar.id}`;
      if (item?.arrCombined) {
        if (item.arrCombined.custom) return `combined:arrcustom:${item.arrCombined.cardId}`;
        return `combined:arr:${item.arrCombined.sectionKey}`;
      }
      if (item?.downloaderCombined) {
        if (item.downloaderCombined.custom) return `combined:downloadercustom:${item.downloaderCombined.cardId}`;
        return `combined:downloader:${item.downloaderCombined.sectionKey}`;
      }
      if (item?.mediaCombined) {
        if (item.mediaCombined.custom) return `combined:mediacustom:${item.mediaCombined.cardId}`;
        return `combined:media:${item.mediaCombined.sectionKey}`;
      }
      return '';
    };
    const getDashboardOrder = (item) => {
      const combinedKey = getCombinedOrderKey(item);
      if (combinedKey) {
        const combinedValue = Number(dashboardCombinedOrder?.[combinedKey]);
        if (Number.isFinite(combinedValue)) return combinedValue;
      }
      const orderValue = Number(item?.element?.order);
      return Number.isFinite(orderValue) ? orderValue : 0;
    };
    dashboardModules.sort((a, b) => {
      const orderDelta = getDashboardOrder(a) - getDashboardOrder(b);
      if (orderDelta !== 0) return orderDelta;
      const appOrderDelta = (Number(a.app?.order) || 0) - (Number(b.app?.order) || 0);
      if (appOrderDelta !== 0) return appOrderDelta;
      const appNameDelta = String(a.app?.name || '').localeCompare(String(b.app?.name || ''));
      if (appNameDelta !== 0) return appNameDelta;
      return String(a.element?.name || '').localeCompare(String(b.element?.name || ''));
    });
  
    res.render('dashboard', {
      user: req.session.user,
      apps: navApps,
      navCategories,
      multiInstanceBaseIds: getMultiInstanceBaseIds(),
      appBaseUrls,
      dashboardDefinitions: dashboardSelection.visibleDashboards,
      activeDashboard,
      dashboardModules,
      arrDashboardCombine,
      mediaDashboardCombine,
      arrCombinedQueueDisplay,
      downloaderCombinedQueueDisplay,
      downloaderDashboardCombine,
      tautulliCards: mergeTautulliCardSettings(apps.find((appItem) => appItem.id === 'tautulli')),
      role,
      actualRole,
      visibleWidgetBars,
    });
  });
  
  app.get('/apps/:id', requireUser, (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const categoryEntries = resolveCategoryEntries(config, apps);
    const categoryOrder = categoryEntries.map((entry) => entry.name);
    const appBaseUrls = buildAppBaseUrls(apps, req);
    const role = getEffectiveRole(req);
    const actualRole = getActualRole(req);
    const dashboardSelection = resolveDashboardSelection(config, resolveRequestedDashboardId(req), role);
    const activeDashboard = dashboardSelection.activeDashboard || resolveDashboardDefinitions(config)[0] || {
      id: DASHBOARD_MAIN_ID,
      name: 'Dashboard',
      icon: DEFAULT_DASHBOARD_ICON,
      visibilityRole: 'user',
    };
    const navApps = getNavApps(apps, role, req, categoryOrder);
    const navCategories = buildNavCategories(navApps, categoryEntries, role);
    const appItem = apps.find((item) => item.id === req.params.id);
  
    if (!appItem) return res.status(404).send('App not found.');
    if (!canAccess(appItem, role, 'overview')) {
      return res.status(403).send('Overview access denied.');
    }
    const appWithIcon = { ...appItem, icon: resolvePersistedAppIconPath(appItem) };
    const overviewElements = mergeOverviewElementSettings(appItem).map((element) => ({
      ...element,
      enable: element.enable !== false && canAccessOverviewElement(appItem, element, role),
    }));
  
    res.render('app-overview', {
      user: req.session.user,
      role,
      actualRole,
      page: 'overview',
      apps: navApps,
      navCategories,
      multiInstanceBaseIds: getMultiInstanceBaseIds(),
      appBaseUrls,
      dashboardDefinitions: dashboardSelection.visibleDashboards,
      activeDashboard,
      app: appWithIcon,
      overviewElements,
      tautulliCards: mergeTautulliCardSettings(appItem),
    });
  });
  
  app.get('/apps/:id/activity', requireAdmin, (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const categoryEntries = resolveCategoryEntries(config, apps);
    const categoryOrder = categoryEntries.map((entry) => entry.name);
    const role = getEffectiveRole(req);
    const actualRole = getActualRole(req);
    const dashboardSelection = resolveDashboardSelection(config, resolveRequestedDashboardId(req), role);
    const activeDashboard = dashboardSelection.activeDashboard || resolveDashboardDefinitions(config)[0] || {
      id: DASHBOARD_MAIN_ID,
      name: 'Dashboard',
      icon: DEFAULT_DASHBOARD_ICON,
      visibilityRole: 'user',
    };
    const navApps = getNavApps(apps, role, req, categoryOrder);
    const navCategories = buildNavCategories(navApps, categoryEntries, role);
    const appItem = apps.find((item) => item.id === req.params.id);
  
    if (!appItem) return res.status(404).send('App not found.');
    if (!canAccess(appItem, role, 'overview')) {
      return res.status(403).send('Activity access denied.');
    }
    const appWithIcon = { ...appItem, icon: resolvePersistedAppIconPath(appItem) };
  
    res.render('app-activity', {
      user: req.session.user,
      role,
      actualRole,
      page: 'activity',
      navCategories,
      dashboardDefinitions: dashboardSelection.visibleDashboards,
      activeDashboard,
      app: appWithIcon,
    });
  });
  
  app.get('/apps/:id/launch', requireUser, async (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const categoryEntries = resolveCategoryEntries(config, apps);
    const categoryOrder = categoryEntries.map((entry) => entry.name);
    const appItem = apps.find((item) => item.id === req.params.id);
    const role = getEffectiveRole(req);
    const actualRole = getActualRole(req);
  
    if (!appItem) return res.status(404).send('App not found.');
    if (!canAccess(appItem, role, 'launch')) {
      return res.status(403).send('Launch access denied.');
    }
    const appWithIcon = { ...appItem, icon: resolvePersistedAppIconPath(appItem) };
  
    const deepQuery = String(req.query?.q || req.query?.query || '').trim();
    if (deepQuery) {
      const deepUrl = await resolveDeepLaunchUrl(appWithIcon, req, {
        query: deepQuery,
        imdbId: String(req.query?.imdb || '').trim(),
        tmdbId: String(req.query?.tmdb || '').trim(),
        mediaType: String(req.query?.type || '').trim().toLowerCase(),
        plexToken: String(req.session?.authToken || appWithIcon.plexToken || '').trim(),
      });
      if (deepUrl) {
        const roleAwareDeepUrl = resolveRoleAwareLaunchUrl(appWithIcon, req, deepUrl, role);
        return res.redirect(roleAwareDeepUrl || deepUrl);
      }
    }
  
    const launchUrl = resolveRoleAwareLaunchUrl(appWithIcon, req, resolveLaunchUrl(appWithIcon, req), role);
    if (!launchUrl) return res.status(400).send('Launch URL not configured.');
  
    const launchMode = resolveEffectiveLaunchMode(appWithIcon, req, normalizeMenu(appWithIcon));
    if (launchMode === 'iframe') {
      let iframeLaunchTarget = launchUrl;
      // When we prime Romm cookies on this same response, delay iframe navigation
      // so the browser applies Set-Cookie before the iframe's first request.
      let deferIframeNavigation = false;
      if (hasEmbeddedUrlCredentials(launchUrl)) {
        const browserLaunchTarget = stripUrlEmbeddedCredentials(launchUrl) || launchUrl;
        const primingPlan = buildRommCookiePrimingPlan({
          config,
          req,
          browserUrl: browserLaunchTarget,
        });
        const rommBootstrap = await bootstrapRommIframeSession({
          req,
          launchUrl,
          authBaseCandidates: resolveAppApiCandidates(appWithIcon, req),
        });
        if (rommBootstrap?.ok) {
          iframeLaunchTarget = rommBootstrap.launchUrl || browserLaunchTarget;
          const primingCompatibility = evaluateRommCookiePrimingCompatibility(rommBootstrap.setCookies, primingPlan);
          const primedCookies = prepareRommPrimedSetCookies(rommBootstrap.setCookies, primingPlan);
          logRommLaunchServerDiagnostic(req, {
            route: 'launch',
            launchMode: 'iframe',
            stage: 'bootstrap-ok',
            role,
            browserLaunchTarget,
            primingPlan,
            rommBootstrap,
            primedCookies,
          });
          if (primedCookies.length) {
            res.append('Set-Cookie', primedCookies);
            deferIframeNavigation = true;
          } else if (!primingCompatibility.ok) {
            logRommLaunchServerDiagnostic(req, {
              route: 'launch',
              launchMode: 'iframe',
              stage: 'fallback-top-level-cookie-priming-incompatible',
              role,
              browserLaunchTarget,
              primingPlan,
              rommBootstrap,
              primedCookies,
            });
            return res.redirect(launchUrl);
          }
        } else {
          logRommLaunchServerDiagnostic(req, {
            route: 'launch',
            launchMode: 'iframe',
            stage: 'bootstrap-failed-fallback-top-level',
            role,
            browserLaunchTarget,
            primingPlan,
            rommBootstrap,
            primedCookies: [],
          });
          // Chromium blocks iframe URLs with embedded credentials (user:pass@host),
          // so the fallback is a top-level navigation when bootstrap cannot complete.
          return res.redirect(launchUrl);
        }
      }
      const navApps = getNavApps(apps, role, req, categoryOrder);
      const navCategories = buildNavCategories(navApps, categoryEntries, role);
      const dashboardSelection = resolveDashboardSelection(config, resolveRequestedDashboardId(req), role);
      const activeDashboard = dashboardSelection.activeDashboard || resolveDashboardDefinitions(config)[0] || {
        id: DASHBOARD_MAIN_ID,
        name: 'Dashboard',
        icon: DEFAULT_DASHBOARD_ICON,
        visibilityRole: 'user',
      };
      const iframeLaunchUrl = resolveIframeLaunchUrl(req, iframeLaunchTarget);
      return res.render('app-launch', {
        user: req.session.user,
        role,
        actualRole,
        page: 'launch',
        navCategories,
        dashboardDefinitions: dashboardSelection.visibleDashboards,
        activeDashboard,
        app: appWithIcon,
        launchUrl: iframeLaunchUrl || iframeLaunchTarget,
        deferIframeNavigation,
      });
    }
  
    if (hasEmbeddedUrlCredentials(launchUrl)) {
      const browserLaunchTarget = stripUrlEmbeddedCredentials(launchUrl) || launchUrl;
      const primingPlan = buildRommCookiePrimingPlan({
        config,
        req,
        browserUrl: browserLaunchTarget,
      });
      const rommBootstrap = await bootstrapRommIframeSession({
        req,
        launchUrl,
        authBaseCandidates: resolveAppApiCandidates(appWithIcon, req),
      });
      if (rommBootstrap?.ok) {
        const primingCompatibility = evaluateRommCookiePrimingCompatibility(rommBootstrap.setCookies, primingPlan);
        const primedCookies = prepareRommPrimedSetCookies(rommBootstrap.setCookies, primingPlan);
        logRommLaunchServerDiagnostic(req, {
          route: 'launch',
          launchMode: 'new-tab',
          stage: 'bootstrap-ok',
          role,
          browserLaunchTarget,
          primingPlan,
          rommBootstrap,
          primedCookies,
        });
        if (primedCookies.length) {
          res.append('Set-Cookie', primedCookies);
          return sendClientLaunchRedirectPage(res, browserLaunchTarget, {
            title: `${String(appWithIcon?.name || 'App').trim() || 'App'} Launch`,
            message: 'Starting app...',
          });
        }
        if (!primingCompatibility.ok) {
          logRommLaunchServerDiagnostic(req, {
            route: 'launch',
            launchMode: 'new-tab',
            stage: 'fallback-direct-redirect-cookie-priming-incompatible',
            role,
            browserLaunchTarget,
            primingPlan,
            rommBootstrap,
            primedCookies,
          });
        }
      }
  
      logRommLaunchServerDiagnostic(req, {
        route: 'launch',
        launchMode: 'new-tab',
        stage: 'fallback-direct-redirect',
        role,
        browserLaunchTarget,
        primingPlan,
        rommBootstrap,
        primedCookies: [],
      });
    }
  
    return res.redirect(launchUrl);
  });
  
  app.get('/apps/:id/settings', requireAdmin, (req, res) => {
    const config = loadConfig();
    const admins = loadAdmins();
    const apps = config.apps || [];
    const categoryEntries = resolveCategoryEntries(config, apps);
    const categoryOrder = categoryEntries.map((entry) => entry.name);
    const role = getEffectiveRole(req);
    const actualRole = getActualRole(req);
    const dashboardSelection = resolveDashboardSelection(config, resolveRequestedDashboardId(req), role);
    const activeDashboard = dashboardSelection.activeDashboard || resolveDashboardDefinitions(config)[0] || {
      id: DASHBOARD_MAIN_ID,
      name: 'Dashboard',
      icon: DEFAULT_DASHBOARD_ICON,
      visibilityRole: 'user',
    };
    const navApps = getNavApps(apps, role, req, categoryOrder);
    const navCategories = buildNavCategories(navApps, categoryEntries, role);
    const appItem = apps.find((item) => item.id === req.params.id);
  
    if (!appItem) return res.status(404).send('App not found.');
    if (!canAccess(appItem, role, 'settings')) {
      return res.status(403).send('App settings access denied.');
    }
    const appWithIcon = { ...appItem, icon: resolvePersistedAppIconPath(appItem) };
  
    res.render('app-settings', {
      user: req.session.user,
      admins,
      role,
      actualRole,
      page: 'settings',
      navCategories,
      dashboardDefinitions: dashboardSelection.visibleDashboards,
      activeDashboard,
      multiInstanceBaseIds: getMultiInstanceBaseIds(),
      multiInstancePlaceholderMap: getMultiInstancePlaceholderMap(config.apps || []),
      app: appWithIcon,
      overviewElements: mergeOverviewElementSettings(appWithIcon),
      tautulliCards: mergeTautulliCardSettings(appWithIcon),
    });
  });
  
  app.get('/user-settings', requireUser, (req, res) => {
    const config = loadConfig();
    const apps = config.apps || [];
    const categoryEntries = resolveCategoryEntries(config, apps);
    const categoryOrder = categoryEntries.map((entry) => entry.name);
    const role = getEffectiveRole(req);
    const actualRole = getActualRole(req);
    const dashboardSelection = resolveDashboardSelection(config, resolveRequestedDashboardId(req), role);
    const activeDashboard = dashboardSelection.activeDashboard || resolveDashboardDefinitions(config)[0] || {
      id: DASHBOARD_MAIN_ID,
      name: 'Dashboard',
      icon: DEFAULT_DASHBOARD_ICON,
      visibilityRole: 'user',
    };
    const navApps = getNavApps(apps, role, req, categoryOrder);
    const navCategories = buildNavCategories(navApps, categoryEntries, role);
    const generalSettings = resolveGeneralSettings(config);
    const profileResult = String(req.query?.profileResult || '').trim();
    const profileError = String(req.query?.profileError || '').trim();
    const themeResult = String(req.query?.themeResult || '').trim();
    const themeError = String(req.query?.themeError || '').trim();
    const localUsers = resolveLocalUsers(config);
    const localUserIndex = findLocalUserIndex(localUsers, {
      username: req.session?.user?.username,
      email: req.session?.user?.email,
    });
    const localProfile = localUserIndex >= 0 ? localUsers[localUserIndex] : null;
    const isLocalUser = String(req.session?.user?.source || '').trim().toLowerCase() === 'local' && Boolean(localProfile);
  
    res.render('user-settings', {
      user: req.session.user,
      role,
      actualRole,
      navCategories,
      dashboardDefinitions: dashboardSelection.visibleDashboards,
      activeDashboard,
      generalSettings,
      isLocalUser,
      localProfile,
      profileResult,
      profileError,
      themeResult,
      themeError,
    });
  });
  
  
}
