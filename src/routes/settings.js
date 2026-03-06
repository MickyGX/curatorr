import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.join(__dirname, '..', '..');
const PUBLIC_DIR = path.join(PROJECT_ROOT, 'public');

export function registerSettings(app, ctx) {
  const {
    requireUser,
    requireAdmin,
    requireSettingsAdmin,
    requireActualAdmin,
    loadConfig,
    saveConfig,
    getActualRole,
    getEffectiveRole,
    pushLog,
    normalizeAppId,
    normalizeVersionTag,
    APP_VERSION,
    resolvePersistedAppIconPath,
    getAppBaseId,
    loadAdmins,
    resolveGeneralSettings,
    resolveLocalUsers,
    findLocalUserIndex,
    resolveDashboardSelection,
    resolveRequestedDashboardId,
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
    canAccess,
    canAccessOverviewElement,
    canAccessCombinedDashboardVisibility,
    canAccessDashboardApp,
    mergeOverviewElementSettings,
    resolveArrDashboardCombinedCards,
    resolveDownloaderDashboardCards,
    resolveMediaDashboardCards,
    isAppInSet,
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
    resolveDeepLaunchUrl,
    resolveRoleAwareLaunchUrl,
    resolveEffectiveLaunchMode,
    normalizeMenu,
    resolveLaunchUrl,
    resolveAppApiCandidates,
    injectBasicAuthIntoUrl,
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
    buildReleaseNotesUrl,
    loadReleaseHighlights,
    DASHBOARD_MAX_COUNT,
    DATA_DIR,
    DEFAULT_GENERAL_SETTINGS,
    DEFAULT_LOG_SETTINGS,
    ENABLE_ARR_UNIFIED_CARDS,
    ENABLE_DOWNLOADER_UNIFIED_CARDS,
    USER_AVATAR_BASE,
    LOCAL_AUTH_MIN_PASSWORD,
    validateLocalPasswordStrength,
    applyLogRetention,
    buildCombinedCardId,
    buildDashboardElementsFromRequest,
    buildDashboardInstanceId,
    buildDashboardSettingsRedirect,
    buildDisabledMenuAccess,
    buildDisabledOverviewElements,
    buildEmptyDashboardStateSnapshot,
    buildMenuAccessConfig,
    buildNextInstanceId,
    buildOverviewElementsFromRequest,
    buildTautulliCardsFromDashboardRequest,
    buildTautulliCardsFromRequest,
    canManageWithDefaultAppManager,
    deepEqual,
    deleteCustomIcon,
    deriveDashboardLegacyVisibilityRole,
    extractDashboardStateSnapshot,
    fetchPlexHistoryLastSeenMap,
    getAppIconOptions,
    getArrCombineSectionIconPath,
    getArrCombineSectionLabel,
    getBaseAppTitle,
    getCategoryIconOptions,
    getCustomAppIconOptions,
    getCustomSystemIconOptions,
    getDefaultAppIconOptions,
    getDefaultInstanceName,
    getDefaultSystemIconOptions,
    getDownloaderCombineSectionIconPath,
    getDownloaderCombineSectionLabel,
    getInstanceSuffix,
    getMaxMultiInstancesForBase,
    getMediaCombineSectionIconPath,
    getMediaCombineSectionLabel,
    getMultiInstanceMaxMap,
    getMultiInstanceTitleMap,
    isValidEmail,
    loadCoAdmins,
    loadDefaultApps,
    loadDefaultCategories,
    loadSettingsReleases,
    migrateDeprecatedDashboardCards,
    normalizeBasePath,
    normalizeCategoryEntries,
    normalizeCategoryName,
    normalizeDashboardDisplayName,
    normalizeDashboardIcon,
    normalizeDashboardInstanceId,
    normalizeLaunchMode,
    normalizeLocalRole,
    normalizePlexLastSeen,
    normalizeSidebarAppButtonAction,
    normalizeSidebarButtonPressActions,
    normalizeStoredAvatarPath,
    normalizeThemeSettings,
    normalizeUserKey,
    normalizeVisibilityRole,
    parseCsv,
    parsePlexUsers,
    parseUserAvatarDataUrl,
    persistLogsToDisk,
    plexHeaders,
    resolveAppLaunchMode,
    resolveCategoryOrder,
    resolveCombinedDashboardVisibilityRole,
    resolveDashboardDefinitionVisibilityRoles,
    resolveDashboardVisibilityRolesFromRequest,
    resolveDefaultCategoryIcon,
    resolveDeprecatedDashboardCardDescriptor,
    resolveDeprecatedDashboardElementIdsForApp,
    resolveLogSettings,
    resolveNotificationSettings,
    resolveThemeDefaults,
    resolveThemePreferenceKey,
    resolveUserLogins,
    resolveUserThemePreferences,
    saveAdmins,
    saveCoAdmins,
    saveCustomAppIcon,
    saveCustomIcon,
    saveCustomUserAvatar,
    saveDashboardScopedConfig,
    sendAppriseNotification,
    serializeUserThemePreferences,
    slugifyId,
    supportsAppInstances,
    uniqueList,
    normalizeBaseUrl,
    safeMessage,
    // widget bars
    resolveWidgetBars,
    resolveWidgetBarTypes,
    normalizeWidgetBarId,
    serializeWidgetBars,
    // system widgets
    SYSTEM_WIDGET_TYPES,
    SYSTEM_WIDGET_SEARCH_PROVIDERS,
    SYSTEM_WIDGET_TIMEZONES,
  } = ctx;

  const buildDefaultAppIdSet = () => new Set(
    (Array.isArray(loadDefaultApps()) ? loadDefaultApps() : [])
      .map((appItem) => normalizeAppId(appItem?.id))
      .filter(Boolean)
  );

  const isCustomAppRecord = (appItem, defaultAppIdSet = null) => {
    const appId = normalizeAppId(appItem?.id);
    if (!appId) return false;
    if (Boolean(appItem?.custom) || appId.startsWith('custom-')) return true;
    if (!(defaultAppIdSet instanceof Set) || !defaultAppIdSet.size) return false;
    if (defaultAppIdSet.has(appId)) return false;
    const baseId = normalizeAppId(getAppBaseId(appId));
    if (baseId && defaultAppIdSet.has(baseId)) return false;
    return true;
  };

  const APP_VISIBILITY_ROLE_ORDER = ['guest', 'user', 'co-admin', 'admin'];
  const APP_SETTINGS_AND_ACTIVITY_ALLOWED_ROLES = new Set(['co-admin', 'admin']);

  const parseAppVisibilityRole = (value) => {
    const raw = String(value || '').trim().toLowerCase();
    if (!raw) return '';
    if (raw === 'coadmin' || raw === 'co_admin') return 'co-admin';
    return APP_VISIBILITY_ROLE_ORDER.includes(raw) ? raw : '';
  };

  const appVisibilityRolesFromLegacyMinRole = (minRole = 'disabled') => {
    const normalized = normalizeVisibilityRole(minRole, 'disabled');
    if (normalized === 'disabled') return [];
    const minRank = APP_VISIBILITY_ROLE_ORDER.indexOf(normalized);
    if (minRank === -1) return [];
    return APP_VISIBILITY_ROLE_ORDER.filter((_role, index) => index >= minRank);
  };

  const normalizeAppVisibilityRoles = (value, fallback = undefined) => {
    const hasExplicitArray = Array.isArray(value);
    const inputList = hasExplicitArray
      ? value
      : (typeof value === 'string' && value.includes(','))
        ? value.split(',')
        : (value === undefined ? [] : [value]);
    const parsed = uniqueList(
      inputList
        .map((entry) => parseAppVisibilityRole(entry))
        .filter((role) => role && APP_VISIBILITY_ROLE_ORDER.includes(role))
    );
    if (parsed.length) return APP_VISIBILITY_ROLE_ORDER.filter((role) => parsed.includes(role));
    if (hasExplicitArray) return [];
    if (fallback === undefined) return [];
    if (Array.isArray(fallback)) return normalizeAppVisibilityRoles(fallback);
    return appVisibilityRolesFromLegacyMinRole(fallback);
  };

  const buildMenuRoleSection = (currentSection, visibilityRoles, { fallbackMinRole = 'disabled', includeLegacyFlags = false } = {}) => {
    const source = currentSection && typeof currentSection === 'object' ? currentSection : {};
    const normalizedRoles = normalizeAppVisibilityRoles(visibilityRoles);
    const minRole = normalizedRoles.length
      ? normalizedRoles[0]
      : normalizeVisibilityRole('disabled', fallbackMinRole);
    const next = {
      ...source,
      minRole,
      visibilityRoles: normalizedRoles,
    };
    if (includeLegacyFlags) {
      next.user = normalizedRoles.includes('user');
      next.admin = normalizedRoles.includes('admin');
    }
    return next;
  };

  const ensureAdminMenuSection = (currentSection, { includeLegacyFlags = false } = {}) => {
    const source = currentSection && typeof currentSection === 'object' ? currentSection : {};
    const normalizedRoles = normalizeAppVisibilityRoles(
      source.visibilityRoles,
      normalizeVisibilityRole(source.minRole, 'admin')
    );
    const nextRoles = normalizedRoles.length ? normalizedRoles : ['admin'];
    return buildMenuRoleSection(source, nextRoles, { fallbackMinRole: 'admin', includeLegacyFlags });
  };

  const enforceSidebarAdminDefaults = (menuValue) => {
    const menu = menuValue && typeof menuValue === 'object' ? menuValue : {};
    return {
      ...menu,
      sidebar: ensureAdminMenuSection(menu.sidebar),
      sidebarOverview: ensureAdminMenuSection(menu.sidebarOverview),
      sidebarSettings: ensureAdminMenuSection(menu.sidebarSettings),
      sidebarActivity: ensureAdminMenuSection(menu.sidebarActivity),
    };
  };

  app.get('/settings', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const dashboardSettingsSelection = resolveDashboardSelection(config, resolveRequestedDashboardId(req), 'admin', { includeHidden: true });
  const dashboardSidebarSelection = resolveDashboardSelection(config, resolveRequestedDashboardId(req), getEffectiveRole(req));
  const activeSettingsDashboard = dashboardSettingsSelection.activeDashboard || resolveDashboardDefinitions(config)[0] || {
    id: DASHBOARD_MAIN_ID,
    name: 'Dashboard',
    icon: DEFAULT_DASHBOARD_ICON,
    visibilityRole: 'user',
  };
  const dashboardConfig = applyDashboardStateSnapshot(config, activeSettingsDashboard);
  const admins = loadAdmins();
  const apps = config.apps || [];
  const categoryEntries = resolveCategoryEntries(config, apps);
  const categoryOrder = categoryEntries.map((entry) => entry.name);
  const categoryIcons = getCategoryIconOptions(apps);
  const appIcons = getAppIconOptions(apps);
  const dashboardApps = dashboardConfig.apps || [];
  const arrDashboardCombine = resolveArrDashboardCombineSettings(dashboardConfig, dashboardApps);
  const mediaDashboardCombine = resolveMediaDashboardCombineSettings(dashboardConfig, dashboardApps);
  const arrCombinedQueueDisplay = resolveCombinedQueueDisplaySettings(dashboardConfig, 'arrCombinedQueueDisplay');
  const downloaderCombinedQueueDisplay = resolveCombinedQueueDisplaySettings(dashboardConfig, 'downloaderCombinedQueueDisplay');
  const downloaderDashboardCombine = resolveDownloaderDashboardCombineSettings(dashboardConfig, dashboardApps);
  const dashboardCombinedOrder = (dashboardConfig && typeof dashboardConfig.dashboardCombinedOrder === 'object' && dashboardConfig.dashboardCombinedOrder)
    ? dashboardConfig.dashboardCombinedOrder
    : {};
  const dashboardCombinedSettings = (dashboardConfig && typeof dashboardConfig.dashboardCombinedSettings === 'object' && dashboardConfig.dashboardCombinedSettings)
    ? dashboardConfig.dashboardCombinedSettings
    : {};
  const logSettings = resolveLogSettings(config);
  const generalSettings = resolveGeneralSettings(config);
  const notificationSettings = resolveNotificationSettings(config);
  const notificationResult = String(req.query?.notificationResult || '').trim();
  const notificationError = String(req.query?.notificationError || '').trim();
  const themeDefaultsResult = String(req.query?.themeDefaultsResult || '').trim();
  const themeDefaultsError = String(req.query?.themeDefaultsError || '').trim();
  const appGeneralResult = String(req.query?.appGeneralResult || '').trim();
  const appGeneralError = String(req.query?.appGeneralError || '').trim();
  const appInstanceResult = String(req.query?.appInstanceResult || '').trim();
  const appInstanceError = String(req.query?.appInstanceError || '').trim();
  const defaultAppResult = String(req.query?.defaultAppResult || '').trim();
  const defaultAppError = String(req.query?.defaultAppError || '').trim();
  const selectedSettingsAppId = normalizeAppId(req.query?.instance || req.query?.app || '');
  const aboutCurrentVersion = normalizeVersionTag(APP_VERSION || '');
  const aboutReleases = loadSettingsReleases({ limit: 12, currentVersion: aboutCurrentVersion });
  const aboutLatestVersion = String(aboutReleases[0]?.tag || '').trim();
  const localUsersResult = String(req.query?.localUsersResult || '').trim();
  const localUsersError = String(req.query?.localUsersError || '').trim();
  const localLoginStore = resolveUserLogins(config).launcharr || {};
  const sessionUser = req.session?.user || {};
  const sessionUsernameKey = normalizeUserKey(sessionUser.username || '');
  const sessionEmailKey = normalizeUserKey(sessionUser.email || '');
  const localUsers = resolveLocalUsers(config).map((entry) => {
    const usernameKey = normalizeUserKey(entry.username || '');
    const emailKey = normalizeUserKey(entry.email || '');
    const loginKey = emailKey || usernameKey;
    const isCurrentSessionUser = Boolean(
      sessionUsernameKey && sessionUsernameKey === usernameKey
      || (sessionEmailKey && emailKey && sessionEmailKey === emailKey)
    );
    const isOwnerAccount = Boolean(
      entry?.isSetupAdmin
      || entry?.setupAccount === true
      || String(entry?.createdBy || '').trim().toLowerCase() === 'setup'
    );
    return {
      ...entry,
      isCurrentSessionUser,
      isOwnerAccount,
      canDelete: !isOwnerAccount && !isCurrentSessionUser && entry?.systemCreated !== false,
      lastLauncharrLogin: loginKey ? String(localLoginStore[loginKey] || '') : '',
    };
  });
  const rankCategory = buildCategoryRank(categoryOrder);
  const role = getEffectiveRole(req);
  const actualRole = getActualRole(req);
  const settingsApps = [...apps].sort((a, b) => {
    const favouriteDelta = (b.favourite ? 1 : 0) - (a.favourite ? 1 : 0);
    if (favouriteDelta !== 0) return favouriteDelta;
    const categoryDelta = rankCategory(a.category) - rankCategory(b.category);
    if (categoryDelta !== 0) return categoryDelta;
    const orderDelta = (a.order || 0) - (b.order || 0);
    if (orderDelta !== 0) return orderDelta;
    return String(a.name || '').localeCompare(String(b.name || ''));
  });
  const defaultAppIdSet = buildDefaultAppIdSet();
  const settingsAppsWithIcons = settingsApps.map((appItem) => ({
    ...appItem,
    menu: enforceSidebarAdminDefaults(normalizeMenu(appItem)),
    icon: resolvePersistedAppIconPath(appItem),
    canRemoveDefaultApp: canManageWithDefaultAppManager(appItem),
    isCustomApp: isCustomAppRecord(appItem, defaultAppIdSet),
  }));
  const dashboardSettingsApps = dashboardApps
    .slice()
    .sort((a, b) => {
      const favouriteDelta = (b.favourite ? 1 : 0) - (a.favourite ? 1 : 0);
      if (favouriteDelta !== 0) return favouriteDelta;
      const categoryDelta = rankCategory(a.category) - rankCategory(b.category);
      if (categoryDelta !== 0) return categoryDelta;
      const orderDelta = (a.order || 0) - (b.order || 0);
      if (orderDelta !== 0) return orderDelta;
      return String(a.name || '').localeCompare(String(b.name || ''));
    })
    .map((appItem) => ({
      ...appItem,
      menu: enforceSidebarAdminDefaults(normalizeMenu(appItem)),
      icon: resolvePersistedAppIconPath(appItem),
      canRemoveDefaultApp: canManageWithDefaultAppManager(appItem),
    }))
    .filter((appItem) => !appItem?.removed);
  const multiInstanceBaseIds = getMultiInstanceBaseIds();
  const multiInstanceTitleMap = getMultiInstanceTitleMap();
  const multiInstanceMaxMap = getMultiInstanceMaxMap(settingsAppsWithIcons);
  const multiInstancePlaceholderMap = getMultiInstancePlaceholderMap(settingsAppsWithIcons);
  const arrDashboardCombinedCards = resolveArrDashboardCombinedCards(dashboardConfig, dashboardSettingsApps);
  const downloaderDashboardCards = resolveDownloaderDashboardCards(dashboardConfig, dashboardSettingsApps);
  const mediaDashboardCards = resolveMediaDashboardCards(dashboardConfig, dashboardSettingsApps);
  const arrSettingsAppIds = dashboardSettingsApps
    .filter((appItem) => isAppInSet(appItem.id, ARR_APP_IDS))
    .map((appItem) => appItem.id);
  const arrAppLookup = new Map(
    dashboardSettingsApps
      .filter((appItem) => isAppInSet(appItem.id, ARR_APP_IDS))
      .map((appItem) => [normalizeAppId(appItem.id), appItem])
  );
  const downloaderSettingsAppIds = dashboardSettingsApps
    .filter((appItem) => isAppInSet(appItem.id, DOWNLOADER_APP_IDS))
    .map((appItem) => appItem.id);
  const downloaderAppLookup = new Map(
    dashboardSettingsApps
      .filter((appItem) => isAppInSet(appItem.id, DOWNLOADER_APP_IDS))
      .map((appItem) => [normalizeAppId(appItem.id), appItem])
  );
  const mediaSettingsAppIds = dashboardSettingsApps
    .filter((appItem) => isAppInSet(appItem.id, MEDIA_APP_IDS))
    .map((appItem) => appItem.id);
  const mediaAppLookup = new Map(
    dashboardSettingsApps
      .filter((appItem) => isAppInSet(appItem.id, MEDIA_APP_IDS))
      .map((appItem) => [normalizeAppId(appItem.id), appItem])
  );
  const multiInstanceCountsByBase = dashboardSettingsApps.reduce((acc, appItem) => {
    const baseId = getAppBaseId(appItem?.id);
    if (!supportsAppInstances(baseId)) return acc;
    acc[baseId] = (acc[baseId] || 0) + 1;
    return acc;
  }, {});
  const baseDashboardElements = dashboardSettingsApps.flatMap((appItem) => {
    const elements = mergeOverviewElementSettings(appItem);
    const baseId = getAppBaseId(appItem?.id);
    const isMultiInstanceGroup = supportsAppInstances(baseId) && Number(multiInstanceCountsByBase[baseId] || 0) > 1;
    const appTitle = String(appItem?.name || '').trim() || getDefaultInstanceName(baseId, appItem?.id);
    const itemIconPath = resolvePersistedAppIconPath(appItem);
    return elements.map((element) => ({
      appId: appItem.id,
      appName: appItem.name,
      appOrder: appItem.order || 0,
      category: appItem.category || 'Tools',
      element,
      displayName: isMultiInstanceGroup ? `${appTitle} ${element.name || ''}`.trim() : (element.name || ''),
      iconPath: itemIconPath,
      appAccess: true,
    }));
  });

  const applyCombinedDashboardElements = (items, options) => {
    const {
      appIds = [],
      sections = [],
      combineMap = {},
      labelPrefix,
      iconPath,
      combinedType,
    } = options;
    const resolveCombinedIconPath = (section) => {
      if (typeof iconPath === 'function') return iconPath(section);
      if (iconPath && typeof iconPath === 'object') {
        const bySection = String(section?.key || '').trim();
        if (bySection && String(iconPath[bySection] || '').trim()) {
          return String(iconPath[bySection]).trim();
        }
        return String(iconPath.default || '').trim();
      }
      return String(iconPath || '').trim();
    };
    let updated = items.map((item) => ({ ...item }));
    sections.forEach((section) => {
      const availableAppIds = new Set(
        updated
          .filter((item) => item.element?.id === section.elementId)
          .map((item) => item.appId)
      );
      let combinedAppIds = appIds.filter(
        (appId) => availableAppIds.has(appId) && Boolean(combineMap?.[section.key]?.[appId])
      );
      if (!combinedAppIds.length) {
        combinedAppIds = appIds.filter((appId) => availableAppIds.has(appId));
      }
      const leaderId = combinedAppIds[0] || appIds.find((appId) => availableAppIds.has(appId));
      const leaderItem = updated.find((item) =>
        (leaderId ? item.appId === leaderId : appIds.includes(item.appId))
        && item.element?.id === section.elementId
      );
      if (!leaderItem) return;
      if (combinedType === 'arr') {
        const meta = buildArrCombinedDisplayMeta(arrAppLookup, section.key, combinedAppIds);
        updated.push({
          ...leaderItem,
          displayName: meta.displayName,
          iconPath: meta.iconPath,
          combined: true,
          combinedType,
          combinedSection: section.key,
          combinedApps: meta.appIds,
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
        });
        return;
      }
      if (combinedType === 'downloader') {
        const meta = buildDownloaderCombinedDisplayMeta(downloaderAppLookup, section.key, combinedAppIds);
        updated.push({
          ...leaderItem,
          displayName: meta.displayName,
          iconPath: meta.iconPath,
          combined: true,
          combinedType,
          combinedSection: section.key,
          combinedApps: meta.appIds,
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
        });
        return;
      }
      const combinedName = `${labelPrefix} ${leaderItem.element?.name || section.elementId}`;
      updated.push({
        ...leaderItem,
        displayName: combinedName,
        iconPath: resolveCombinedIconPath(section),
        combined: true,
        combinedType,
        combinedSection: section.key,
        combinedApps: combinedAppIds,
      });
    });
    return updated;
  };

  const arrCombinedElements = applyCombinedDashboardElements(baseDashboardElements, {
    appIds: arrSettingsAppIds,
    sections: ARR_COMBINE_SECTIONS,
    combineMap: arrDashboardCombine,
    labelPrefix: 'Combined',
    iconPath: '/icons/arr-suite.svg',
    combinedType: 'arr',
  });
  const arrCombinedElementsWithCustom = arrCombinedElements.map((item) => ({ ...item }));
  arrDashboardCombinedCards.forEach((card, cardIndex) => {
    const section = getArrCombineSection(card.sectionKey);
    if (!section) return;
    const sourceItems = baseDashboardElements.filter((item) =>
      item.element?.id === section.elementId
      && Array.isArray(card.appIds)
      && card.appIds.includes(item.appId)
    );
    if (!sourceItems.length) return;
    const orderedAppIds = [...new Set(sourceItems.map((item) => normalizeAppId(item.appId)).filter(Boolean))];
    const meta = buildArrCombinedDisplayMeta(arrAppLookup, card.sectionKey, orderedAppIds);
    if (!meta.appIds.length) return;
    const leaderItem = sourceItems[0];
    const customToken = normalizeCombinedCardToken(card.id) || `card-${cardIndex + 1}`;
    const modulePrefixBase = ARR_COMBINED_SECTION_PREFIX[card.sectionKey] || `arrcombined-${card.sectionKey}`;
    arrCombinedElementsWithCustom.push({
      ...leaderItem,
      displayName: meta.displayName,
      iconPath: meta.iconPath,
      combined: true,
      combinedType: 'arrcustom',
      combinedSection: customToken,
      combinedApps: meta.appIds,
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
    });
  });
  const downloaderCombinedElements = applyCombinedDashboardElements(arrCombinedElementsWithCustom, {
    appIds: downloaderSettingsAppIds,
    sections: DOWNLOADER_COMBINE_SECTIONS,
    combineMap: downloaderDashboardCombine,
    labelPrefix: 'Combined',
    iconPath: '/icons/download.svg',
    combinedType: 'downloader',
  });
  const downloaderCombinedElementsWithCustom = downloaderCombinedElements.map((item) => ({ ...item }));
  downloaderDashboardCards.forEach((card, cardIndex) => {
    const section = getDownloaderCombineSection(card.sectionKey);
    if (!section) return;
    const sectionItems = baseDashboardElements.filter((item) =>
      item.element?.id === section.elementId
      && isAppInSet(item.appId, DOWNLOADER_APP_IDS)
    );
    if (!sectionItems.length) return;
    const sourceItems = sectionItems.filter((item) =>
      item.element?.id === section.elementId
      && Array.isArray(card.appIds)
      && card.appIds.includes(normalizeAppId(item.appId))
    );
    const orderedAppIds = [...new Set(sourceItems.map((item) => normalizeAppId(item.appId)).filter(Boolean))];
    const meta = buildDownloaderCombinedDisplayMeta(downloaderAppLookup, card.sectionKey, orderedAppIds);
    const leaderItem = sourceItems[0] || sectionItems[0];
    const customToken = normalizeCombinedCardToken(card.id) || `card-${cardIndex + 1}`;
    const modulePrefixBase = DOWNLOADER_COMBINED_SECTION_PREFIX[card.sectionKey] || `downloaderscombined-${card.sectionKey}`;
    downloaderCombinedElementsWithCustom.push({
      ...leaderItem,
      displayName: meta.displayName,
      iconPath: meta.iconPath,
      combined: true,
      combinedType: 'downloadercustom',
      combinedSection: customToken,
      combinedApps: meta.appIds,
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
    });
  });
  const combinedDashboardElements = applyCombinedDashboardElements(downloaderCombinedElementsWithCustom, {
    appIds: mediaSettingsAppIds,
    sections: MEDIA_COMBINE_SECTIONS,
    combineMap: mediaDashboardCombine,
    labelPrefix: 'Combined',
    iconPath: {
      active: '/icons/media-play.svg',
      recent: '/icons/recently-added.svg',
      default: '/icons/media-play.svg',
    },
    combinedType: 'media',
  });
  const combinedDashboardElementsWithMediaCustom = combinedDashboardElements.map((item) => ({ ...item }));
  mediaDashboardCards.forEach((card, cardIndex) => {
    const section = getMediaCombineSection(card.sectionKey);
    if (!section) return;
    const sectionItems = baseDashboardElements.filter((item) =>
      item.element?.id === section.elementId
      && isAppInSet(item.appId, MEDIA_APP_IDS)
    );
    if (!sectionItems.length) return;
    const sourceItems = sectionItems.filter((item) =>
      item.element?.id === section.elementId
      && Array.isArray(card.appIds)
      && card.appIds.includes(normalizeAppId(item.appId))
    );
    const orderedAppIds = [...new Set(sourceItems.map((item) => normalizeAppId(item.appId)).filter(Boolean))];
    const meta = buildMediaCombinedDisplayMeta(mediaAppLookup, card.sectionKey, orderedAppIds);
    const leaderItem = sourceItems[0] || sectionItems[0];
    const customToken = normalizeCombinedCardToken(card.id) || `card-${cardIndex + 1}`;
    const modulePrefixBase = MEDIA_COMBINED_SECTION_PREFIX[card.sectionKey] || `mediacombined-${card.sectionKey}`;
    combinedDashboardElementsWithMediaCustom.push({
      ...leaderItem,
      displayName: meta.displayName,
      iconPath: meta.iconPath,
      combined: true,
      combinedType: 'mediacustom',
      combinedSection: customToken,
      combinedApps: meta.appIds,
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
    });
  });

  const getCombinedOrderKey = (item) => {
    if (!item || !item.combined) return '';
    if (item.combinedType === 'arrcustom') {
      const customId = String(item.combinedSection || item.arrCombined?.cardId || '').trim();
      return customId ? `combined:arrcustom:${customId}` : '';
    }
    if (item.combinedType === 'downloadercustom') {
      const customId = String(item.combinedSection || item.downloaderCombined?.cardId || '').trim();
      return customId ? `combined:downloadercustom:${customId}` : '';
    }
    const section = item.combinedSection || item.element?.id || 'unknown';
    return `combined:${item.combinedType || 'mixed'}:${section}`;
  };
  const getDashboardOrder = (item) => {
    if (item?.combined) {
      const combinedKey = getCombinedOrderKey(item);
      const combinedValue = Number(dashboardCombinedOrder?.[combinedKey]);
      if (Number.isFinite(combinedValue)) return combinedValue;
    }
    const orderValue = Number(item?.element?.order);
    return Number.isFinite(orderValue) ? orderValue : 0;
  };
  const dashboardRemovedElements = (dashboardConfig && typeof dashboardConfig.dashboardRemovedElements === 'object' && dashboardConfig.dashboardRemovedElements)
    ? dashboardConfig.dashboardRemovedElements
    : {};
  const getDashboardElementKey = (item) => {
    if (!item) return '';
    if (item.combined) {
      if (item.combinedType === 'arrcustom') {
        const customId = String(item.arrCombined?.cardId || item.combinedSection || '').trim();
        return customId ? `combined:arrcustom:${customId}` : '';
      }
      if (item.combinedType === 'downloadercustom') {
        const customId = String(item.downloaderCombined?.cardId || item.combinedSection || '').trim();
        return customId ? `combined:downloadercustom:${customId}` : '';
      }
      const section = String(item.combinedSection || item.element?.id || '').trim();
      return section ? `combined:${item.combinedType || 'mixed'}:${section}` : '';
    }
    const appId = String(item.appId || '').trim();
    const elementId = String(item.element?.id || '').trim();
    if (!appId || !elementId) return '';
    return `app:${appId}:${elementId}`;
  };
  const dashboardElementsWithKeys = combinedDashboardElementsWithMediaCustom.map((item) => ({
    ...item,
    dashboardElementKey: getDashboardElementKey(item),
  }));
  const resolveDashboardAddCategory = (value, fallback = 'Tools') => {
    const normalized = normalizeCategoryName(value || fallback);
    return normalized || normalizeCategoryName(fallback) || 'Tools';
  };
  const resolveSetCategory = (baseIds, fallback) => {
    const match = settingsAppsWithIcons.find((appItem) => (
      !appItem?.removed
      && isAppInSet(appItem?.id, baseIds)
      && String(appItem?.category || '').trim()
    ));
    return resolveDashboardAddCategory(match?.category, fallback);
  };
  const arrCategoryGroup = resolveSetCategory(ARR_APP_IDS, 'Arr Suite');
  const mediaCategoryGroup = resolveSetCategory(MEDIA_APP_IDS, 'Media');
  const downloaderCategoryGroup = resolveSetCategory(DOWNLOADER_APP_IDS, 'Downloaders');
  const appNameById = new Map(
    settingsAppsWithIcons
      .map((appItem) => {
        const id = normalizeAppId(appItem?.id);
        if (!id) return null;
        const fallbackName = getBaseAppTitle(getAppBaseId(id));
        const name = String(appItem?.name || '').trim() || fallbackName;
        return [id, name];
      })
      .filter(Boolean)
  );
  const hasAppBasePrefix = (appId, baseIds) => {
    const normalizedAppId = normalizeAppId(appId);
    if (!normalizedAppId || !Array.isArray(baseIds) || !baseIds.length) return false;
    return baseIds.some((baseIdRaw) => {
      const baseId = normalizeAppId(baseIdRaw);
      if (!baseId) return false;
      return normalizedAppId === baseId || normalizedAppId.startsWith(`${baseId}-`);
    });
  };
  const allArrAddSourceIds = [...new Set(
    settingsAppsWithIcons
      .filter((appItem) => !appItem?.removed && isAppInSet(appItem?.id, ARR_APP_IDS))
      .map((appItem) => normalizeAppId(appItem?.id))
      .filter(Boolean)
  )];
  const allMediaAddSourceIds = [...new Set(
    settingsAppsWithIcons
      .filter((appItem) => !appItem?.removed && isAppInSet(appItem?.id, MEDIA_APP_IDS))
      .map((appItem) => normalizeAppId(appItem?.id))
      .filter(Boolean)
  )];
  const allDownloaderAddSourceIds = [...new Set(
    settingsAppsWithIcons
      .filter((appItem) => !appItem?.removed && isAppInSet(appItem?.id, DOWNLOADER_APP_IDS))
      .map((appItem) => normalizeAppId(appItem?.id))
      .filter(Boolean)
  )];
  const sameIdSet = (left, right) => {
    const leftSet = [...new Set((Array.isArray(left) ? left : []).map((entry) => normalizeAppId(entry)).filter(Boolean))].sort();
    const rightSet = [...new Set((Array.isArray(right) ? right : []).map((entry) => normalizeAppId(entry)).filter(Boolean))].sort();
    if (leftSet.length !== rightSet.length) return false;
    return leftSet.every((value, index) => value === rightSet[index]);
  };
  const dashboardBaseAddOptions = dashboardElementsWithKeys
    .filter((item) => item?.dashboardElementKey)
    .map((item) => {
      const optionKey = String(item?.dashboardElementKey || '').trim();
      const appKeyMatch = optionKey.match(/^app:([^:]+):(.+)$/);
      const keyAppId = appKeyMatch ? normalizeAppId(appKeyMatch[1]) : '';
      const keyElementId = appKeyMatch ? String(appKeyMatch[2] || '').trim() : '';
      const resolvedAppId = normalizeAppId(item?.appId || keyAppId);
      const resolvedElementId = String(item?.element?.id || keyElementId).trim().replace(/,+$/, '');
      const appLabel = String(item.appName || '').trim();
      const baseName = String(item.displayName || item.element?.name || 'Dashboard card').trim() || 'Dashboard card';
      const combinedType = String(item?.combinedType || '').trim();
      const isUnifiedCombinedCard = Boolean(item?.combined);
      const isUnifiedCustomCard = Boolean(
        isUnifiedCombinedCard
        && ['arrcustom', 'downloadercustom', 'mediacustom'].includes(combinedType)
      );
      const combinedSourceNames = isUnifiedCustomCard
        ? [...new Set(
          (Array.isArray(item?.combinedApps) ? item.combinedApps : [])
            .map((appId) => normalizeAppId(appId))
            .filter(Boolean)
            .map((appId) => appNameById.get(appId) || getBaseAppTitle(getAppBaseId(appId)))
            .map((name) => String(name || '').trim())
            .filter(Boolean)
        )]
        : [];
      const unifiedBaseName = isUnifiedCustomCard && !/^new\s+/i.test(baseName)
        ? `New ${baseName}`
        : baseName;
      const optionBaseName = (isUnifiedCustomCard && combinedSourceNames.length)
        ? `${unifiedBaseName} (${combinedSourceNames.join(' + ')})`
        : unifiedBaseName;
      const prefixedName = (
        !isUnifiedCombinedCard
        && appLabel
        && !optionBaseName.toLowerCase().startsWith(appLabel.toLowerCase())
      )
        ? `${appLabel} ${optionBaseName}`.trim()
        : optionBaseName;
      const isOnDashboard = !dashboardRemovedElements[item.dashboardElementKey];
      const sourceSectionKey = String(
        item?.arrCombined?.sectionKey
        || item?.downloaderCombined?.sectionKey
        || item?.mediaCombined?.sectionKey
        || ''
      ).trim();
      const sourceAppIds = [...new Set(
        (Array.isArray(item?.combinedApps) ? item.combinedApps : [])
          .map((appId) => normalizeAppId(appId))
          .filter(Boolean)
      )];
      const isRemovedCustomCard = Boolean(
        isUnifiedCustomCard
        && dashboardRemovedElements[item.dashboardElementKey]
      );
      const hideInAddPicker = Boolean(
        isRemovedCustomCard
        || (
          !item?.combined
          && resolvedAppId
          && hasAppBasePrefix(resolvedAppId, ['autobrr'])
          && resolvedElementId === 'delivery-queue'
        )
        || (
          isUnifiedCustomCard
          && (
          (combinedType === 'arrcustom' && sourceSectionKey && sameIdSet(sourceAppIds, allArrAddSourceIds))
          || (combinedType === 'mediacustom' && sourceSectionKey && sameIdSet(sourceAppIds, allMediaAddSourceIds))
          || (combinedType === 'downloadercustom' && sourceSectionKey && sameIdSet(sourceAppIds, allDownloaderAddSourceIds))
          )
        )
      );
      const isDeprecatedLegacyCombined = Boolean(
        item?.combined
        && ['arr', 'downloader', 'media'].includes(String(item?.combinedType || '').trim())
      );
      const isDeprecatedAppCard = Boolean(
        !item?.combined
        && optionKey.startsWith('app:')
        && resolvedAppId
        && resolvedElementId
        && (
          (
            (isAppInSet(resolvedAppId, ARR_APP_IDS) || hasAppBasePrefix(resolvedAppId, ARR_APP_IDS))
            && ARR_COMBINE_SECTIONS.some((section) => section.elementId === resolvedElementId)
          )
          || (
            (isAppInSet(resolvedAppId, DOWNLOADER_APP_IDS) || hasAppBasePrefix(resolvedAppId, DOWNLOADER_APP_IDS))
            && DOWNLOADER_COMBINE_SECTIONS.some((section) => section.elementId === resolvedElementId)
          )
          || (
            (isAppInSet(resolvedAppId, MEDIA_APP_IDS) || hasAppBasePrefix(resolvedAppId, MEDIA_APP_IDS))
            && MEDIA_COMBINE_SECTIONS.some((section) => section.elementId === resolvedElementId)
          )
        )
      );
      const deprecated = isDeprecatedLegacyCombined || isDeprecatedAppCard;
      return {
        key: optionKey,
        group: resolveDashboardAddCategory(item.category, 'Tools'),
        name: prefixedName,
        icon: String(item.iconPath || resolvePersistedAppIconPath({ id: resolvedAppId || item.appId }) || '/icons/app.svg').trim() || '/icons/app.svg',
        disabled: isOnDashboard,
        deprecated,
        hideInAddPicker,
      };
    });
  const dashboardCardStateByKey = new Map(
    dashboardBaseAddOptions
      .map((entry) => {
        const key = String(entry?.key || '').trim();
        if (!key) return null;
        const deprecated = Boolean(entry?.deprecated);
        return [key, { deprecated, current: !deprecated }];
      })
      .filter(Boolean)
  );
  const hasMediaSources = settingsAppsWithIcons.some((appItem) => !appItem?.removed && isAppInSet(appItem.id, MEDIA_APP_IDS));
  const dashboardAvailableBaseAddOptions = dashboardBaseAddOptions.filter((entry) => !entry?.disabled && !entry?.deprecated && !entry?.hideInAddPicker);
  const dashboardDeprecatedAddOptions = [];
  const dashboardUnifiedAddOptions = [...dashboardAvailableBaseAddOptions];
  const dashboardCombinedAddOptions = [...dashboardUnifiedAddOptions];
  const pushDashboardCombinedAddOption = (entry) => {
    dashboardUnifiedAddOptions.push(entry);
    dashboardCombinedAddOptions.push(entry);
  };
  if (ENABLE_ARR_UNIFIED_CARDS && settingsAppsWithIcons.some((appItem) => !appItem?.removed && isAppInSet(appItem.id, ARR_APP_IDS))) {
    ARR_COMBINE_SECTIONS.forEach((section) => {
      pushDashboardCombinedAddOption({
        key: `new:arr:${section.key}`,
        group: arrCategoryGroup,
        name: `New ${getArrCombineSectionLabel(section.key)}`,
        icon: getArrCombineSectionIconPath(section.key),
        disabled: false,
        deprecated: false,
      });
    });
  }
  if (hasMediaSources) {
    MEDIA_COMBINE_SECTIONS.forEach((section) => {
      pushDashboardCombinedAddOption({
        key: `new:media:${section.key}`,
        group: mediaCategoryGroup,
        name: `New ${getMediaCombineSectionLabel(section.key)}`,
        icon: getMediaCombineSectionIconPath(section.key),
        disabled: false,
        deprecated: false,
      });
    });
  }
  if (ENABLE_DOWNLOADER_UNIFIED_CARDS && settingsAppsWithIcons.some((appItem) => !appItem?.removed && isAppInSet(appItem.id, DOWNLOADER_APP_IDS))) {
    DOWNLOADER_COMBINE_SECTIONS.forEach((section) => {
      pushDashboardCombinedAddOption({
        key: `new:downloader:${section.key}`,
        group: downloaderCategoryGroup,
        name: `New ${getDownloaderCombineSectionLabel(section.key)}`,
        icon: getDownloaderCombineSectionIconPath(section.key),
        disabled: false,
        deprecated: false,
      });
    });
  }
  const allWidgetBars = resolveWidgetBars(config, apps, 'admin', { includeHidden: true });
  const getWidgetBarDashboardOrder = (bar) => {
    const barId = String(bar?.id || '').trim();
    const combinedOrderKey = `combined:widgetbar:${barId}`;
    const storedOrder = Number(dashboardCombinedOrder?.[combinedOrderKey]);
    if (Number.isFinite(storedOrder)) return storedOrder;
    const barOrder = Number(bar?.order);
    return Number.isFinite(barOrder) ? barOrder : 9999;
  };
  allWidgetBars.forEach((bar) => {
    const elementKey = `widget-bar:${bar.id}`;
    const isOnDashboard = !dashboardRemovedElements[elementKey];
    pushDashboardCombinedAddOption({
      key: `new:widget-bar:${bar.id}`,
      group: 'Widget Bars',
      name: String(bar.name || 'Widget Bar').trim(),
      icon: '/icons/dashboard.svg',
      disabled: isOnDashboard,
      deprecated: false,
    });
  });
  const widgetBars = [...allWidgetBars].sort((a, b) => {
    const orderDelta = getWidgetBarDashboardOrder(a) - getWidgetBarDashboardOrder(b);
    if (orderDelta !== 0) return orderDelta;
    return String(a?.name || '').localeCompare(String(b?.name || ''));
  });
  const widgetBarTypes = resolveWidgetBarTypes(apps);
  const widgetBarTypeIds = new Set(widgetBarTypes.map((t) => t.typeId));
  const widgetBarApps = settingsAppsWithIcons
    .filter((appItem) => !appItem?.removed && widgetBarTypeIds.has(getAppBaseId(appItem?.id)))
    .map((appItem) => ({
      id: String(appItem?.id || '').trim(),
      name: String(appItem?.name || '').trim() || String(appItem?.id || '').trim(),
      icon: String(appItem?.icon || '/icons/app.svg').trim(),
      typeId: getAppBaseId(appItem?.id),
      metricFields: Array.isArray(widgetBarTypes.find((t) => t.typeId === getAppBaseId(appItem?.id))?.metricFields)
        ? widgetBarTypes.find((t) => t.typeId === getAppBaseId(appItem?.id)).metricFields.map((f) => ({
            key: String(f?.key || '').trim(),
            label: String(f?.label || f?.key || '').trim(),
          })).filter((f) => f.key)
        : [],
    }));
  const sortDashboardAddOptions = (a, b) => {
    const categoryDelta = rankCategory(a.group) - rankCategory(b.group);
    if (categoryDelta !== 0) return categoryDelta;
    const groupDelta = String(a.group || '').localeCompare(String(b.group || ''));
    if (groupDelta !== 0) return groupDelta;
    return String(a.name || '').localeCompare(String(b.name || ''));
  };
  dashboardDeprecatedAddOptions.sort(sortDashboardAddOptions);
  dashboardCombinedAddOptions.sort(sortDashboardAddOptions);
  dashboardUnifiedAddOptions.sort(sortDashboardAddOptions);
  const dashboardElements = dashboardElementsWithKeys
    .map((item) => {
      const key = String(item?.dashboardElementKey || '').trim();
      const state = key ? dashboardCardStateByKey.get(key) : null;
      return {
        ...item,
        dashboardIsDeprecated: Boolean(state?.deprecated),
        dashboardIsCurrent: Boolean(state?.current),
      };
    })
    .filter((item) => !item.dashboardElementKey || !dashboardRemovedElements[item.dashboardElementKey])
    .sort((a, b) => {
    const orderDelta = getDashboardOrder(a) - getDashboardOrder(b);
    if (orderDelta !== 0) return orderDelta;
    const appOrderDelta = (Number(a.appOrder) || 0) - (Number(b.appOrder) || 0);
    if (appOrderDelta !== 0) return appOrderDelta;
    const appNameDelta = String(a.appName || '').localeCompare(String(b.appName || ''));
    if (appNameDelta !== 0) return appNameDelta;
    return String(a.element.name || '').localeCompare(String(b.element.name || ''));
  });
  // Append visible widget bars to the main dashboard elements table
  allWidgetBars
    .filter((bar) => !dashboardRemovedElements[`widget-bar:${bar.id}`])
    .forEach((bar) => {
      const barId = String(bar.id || '').trim();
      const combinedOrderKey = `combined:widgetbar:${barId}`;
      const storedOrder = Number(dashboardCombinedOrder?.[combinedOrderKey]);
      const elementOrder = Number.isFinite(storedOrder) ? storedOrder : 9999;
      dashboardElements.push({
        combined: true,
        combinedType: 'widgetbar',
        combinedSection: barId,
        isWidgetBar: true,
        widgetBar: bar,
        appId: barId,
        appName: String(bar.name || 'Widget Bar').trim(),
        appOrder: 9999,
        category: 'Widget Bars',
        iconPath: '/icons/dashboard.svg',
        element: { id: 'widget-bar', name: String(bar.name || 'Widget Bar').trim(), order: elementOrder },
        dashboardElementKey: `widget-bar:${barId}`,
        dashboardIsCurrent: true,
        dashboardIsDeprecated: false,
      });
    });
  dashboardElements.sort((a, b) => {
    const orderDelta = getDashboardOrder(a) - getDashboardOrder(b);
    if (orderDelta !== 0) return orderDelta;
    const appOrderDelta = (Number(a.appOrder) || 0) - (Number(b.appOrder) || 0);
    if (appOrderDelta !== 0) return appOrderDelta;
    const appNameDelta = String(a.appName || '').localeCompare(String(b.appName || ''));
    if (appNameDelta !== 0) return appNameDelta;
    return String(a.element?.name || '').localeCompare(String(b.element?.name || ''));
  });
  const navApps = getNavApps(apps, role, req, categoryOrder);
  const navCategories = buildNavCategories(navApps, categoryEntries, role);
  const defaultAppCatalog = loadDefaultApps()
    .map((appItem) => {
      const id = normalizeAppId(appItem?.id);
      if (!id) return null;
      const parsedOrder = Number(appItem?.order);
      return {
        id,
        name: String(appItem?.name || '').trim() || getBaseAppTitle(getAppBaseId(id)),
        icon: resolvePersistedAppIconPath({ ...appItem, id }),
        category: String(appItem?.category || '').trim() || 'Tools',
        order: Number.isFinite(parsedOrder) ? parsedOrder : Number.MAX_SAFE_INTEGER,
      };
    })
    .filter(Boolean)
    .sort((a, b) => {
      const categoryDelta = rankCategory(a.category) - rankCategory(b.category);
      if (categoryDelta !== 0) return categoryDelta;
      const groupDelta = String(a.category || '').localeCompare(String(b.category || ''));
      if (groupDelta !== 0) return groupDelta;
      const leftOrder = Number(a?.order);
      const rightOrder = Number(b?.order);
      const orderDelta = (Number.isFinite(leftOrder) ? leftOrder : Number.MAX_SAFE_INTEGER)
        - (Number.isFinite(rightOrder) ? rightOrder : Number.MAX_SAFE_INTEGER);
      if (orderDelta !== 0) return orderDelta;
      return String(a.name || '').localeCompare(String(b.name || ''));
    });
  const defaultCategoryCatalog = loadDefaultCategories()
    .map((entry) => {
      const name = normalizeCategoryName(entry?.name);
      if (!name) return null;
      const iconValue = String(entry?.icon || '').trim();
      const sidebarMinRole = normalizeVisibilityRole(
        entry?.sidebarMinRole,
        entry?.sidebarMenu ? 'user' : 'disabled'
      );
      return {
        name,
        sidebarMenu: sidebarMinRole !== 'disabled',
        sidebarMinRole,
        icon: iconValue || resolveDefaultCategoryIcon(name),
      };
    })
    .filter(Boolean)
    .filter((entry, index, list) => (
      list.findIndex((candidate) => String(candidate?.name || '').toLowerCase() === String(entry?.name || '').toLowerCase()) === index
    ));
  const systemIconDefaults = getDefaultSystemIconOptions(apps);
  const systemIconCustom = getCustomSystemIconOptions();
  const appIconDefaults = getDefaultAppIconOptions(apps);
  const appIconCustom = getCustomAppIconOptions();

  res.render('settings', {
    user: req.session.user,
    admins,
    apps: settingsAppsWithIcons,
    dashboardDefinitions: dashboardSettingsSelection.dashboards,
    activeDashboard: activeSettingsDashboard,
    sidebarDashboardDefinitions: dashboardSidebarSelection.visibleDashboards,
    sidebarActiveDashboard: dashboardSidebarSelection.activeDashboard,
    categories: categoryOrder,
    categoryEntries,
    categoryIcons,
    appIcons,
    tautulliCards: mergeTautulliCardSettings(dashboardApps.find((appItem) => appItem.id === 'tautulli')),
    dashboardElements,
    dashboardCombinedOrder,
    dashboardCombinedSettings,
    systemIconDefaults,
    systemIconCustom,
    appIconDefaults,
    appIconCustom,
    arrApps: settingsAppsWithIcons.filter((appItem) => !appItem?.removed && isAppInSet(appItem.id, ARR_APP_IDS)),
    arrDashboardCombine,
    arrCombinedQueueDisplay,
    mediaApps: settingsAppsWithIcons.filter((appItem) => !appItem?.removed && isAppInSet(appItem.id, MEDIA_APP_IDS)),
    mediaDashboardCombine,
    downloaderApps: settingsAppsWithIcons.filter((appItem) => !appItem?.removed && isAppInSet(appItem.id, DOWNLOADER_APP_IDS)),
    downloaderDashboardCombine,
    downloaderCombinedQueueDisplay,
    logSettings,
    generalSettings,
    notificationSettings,
    notificationResult,
    notificationError,
    themeDefaultsResult,
    themeDefaultsError,
    appGeneralResult,
    appGeneralError,
    appInstanceResult,
    appInstanceError,
    arrCombinedCardResult: String(req.query?.arrCombinedCardResult || '').trim(),
    arrCombinedCardError: String(req.query?.arrCombinedCardError || '').trim(),
    arrDashboardCombinedCards,
    downloaderDashboardCards,
    mediaDashboardCards,
    dashboardAddOptions: dashboardUnifiedAddOptions,
    dashboardDeprecatedAddOptions,
    dashboardCombinedAddOptions,
    dashboardElementResult: String(req.query?.dashboardElementResult || '').trim(),
    dashboardElementError: String(req.query?.dashboardElementError || '').trim(),
    defaultAppCatalog,
    defaultCategoryCatalog,
    defaultAppResult,
    defaultAppError,
    aboutCurrentVersion,
    aboutLatestVersion,
    aboutReleases,
    aboutDataDirectory: DATA_DIR,
    selectedSettingsAppId,
    localUsers,
    localUsersResult,
    localUsersError,
    navCategories,
    coAdmins: loadCoAdmins(),
    multiInstanceBaseIds,
    multiInstanceTitleMap,
    multiInstanceMaxMap,
    multiInstancePlaceholderMap,
    role,
    actualRole,
    widgetBars,
    widgetBarTypes,
    widgetBarApps,
    systemWidgetTypes: SYSTEM_WIDGET_TYPES,
    systemWidgetSearchProviders: SYSTEM_WIDGET_SEARCH_PROVIDERS,
    systemWidgetTimezones: SYSTEM_WIDGET_TIMEZONES,
  });
  });

app.post('/settings/theme-defaults', requireSettingsAdmin, (req, res) => {
  try {
    const config = loadConfig();
    const currentDefaults = resolveThemeDefaults(config);
    const nextDefaults = normalizeThemeSettings({
      mode: req.body?.theme_mode,
      brandTheme: req.body?.theme_brand_theme,
      customColor: req.body?.theme_custom_color,
      sidebarInvert: req.body?.theme_sidebar_invert,
      squareCorners: req.body?.theme_square_corners,
      bgMotion: req.body?.theme_bg_motion,
      carouselFreeScroll: req.body?.theme_carousel_free_scroll,
      hideScrollbars: req.body?.theme_hide_scrollbars,
    }, currentDefaults);
    saveConfig({
      ...config,
      themeDefaults: nextDefaults,
    });
    return res.redirect('/settings?tab=custom&settingsCustomTab=themes&themeDefaultsResult=saved');
  } catch (err) {
    const encoded = encodeURIComponent('Failed to save default theme.');
    return res.redirect(`/settings?tab=custom&settingsCustomTab=themes&themeDefaultsError=${encoded}`);
  }
});

app.post('/settings/dashboard-elements', requireSettingsAdmin, (req, res) => {
  const baseConfig = loadConfig();
  const dashboardSelection = resolveDashboardSelection(baseConfig, resolveRequestedDashboardId(req), 'admin', { includeHidden: true });
  const activeDashboard = dashboardSelection.activeDashboard || resolveDashboardDefinitions(baseConfig)[0] || {
    id: DASHBOARD_MAIN_ID,
    name: 'Dashboard',
    icon: DEFAULT_DASHBOARD_ICON,
    visibilityRole: 'user',
  };
  const selectedDashboardId = String(activeDashboard.id || DASHBOARD_MAIN_ID).trim() || DASHBOARD_MAIN_ID;
  const config = applyDashboardStateSnapshot(baseConfig, activeDashboard);
  const nextDashboardDefinitions = dashboardSelection.dashboards.map((entry) => (
    entry.id === selectedDashboardId
      ? (() => {
        const visibilityRoles = resolveDashboardVisibilityRolesFromRequest(req.body, entry);
        return {
          ...entry,
          name: normalizeDashboardDisplayName(req.body?.dashboard_name, entry.name || 'Dashboard'),
          icon: normalizeDashboardIcon(req.body?.dashboard_icon, entry.icon || DEFAULT_DASHBOARD_ICON),
          visibilityRoles,
          visibilityRole: deriveDashboardLegacyVisibilityRole(
            visibilityRoles,
            visibilityRoles.length ? (entry.visibilityRole || 'user') : 'disabled',
          ),
        };
      })()
      : entry
  ));
  const shouldUpdateTautulliCards = Boolean(req.body.tautulliCardsForm);
  const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
    ? config.dashboardRemovedElements
    : {};
  const existingDashboardCombinedOrder = (config && typeof config.dashboardCombinedOrder === 'object' && config.dashboardCombinedOrder)
    ? config.dashboardCombinedOrder
    : {};
  const dashboardCombinedOrder = { ...existingDashboardCombinedOrder };
  Object.entries(req.body || {}).forEach(([key, value]) => {
    if (!key.startsWith('dashboard_combined_') || !key.endsWith('_order')) return;
    const raw = Number(value);
    if (!Number.isFinite(raw)) return;
    const bodyKey = key.slice('dashboard_combined_'.length, -'_order'.length);
    const parts = bodyKey.split('_');
    if (parts.length < 2) return;
    const combinedType = parts.shift();
    const combinedSection = parts.join('_');
    const mapKey = `combined:${combinedType}:${combinedSection}`;
    dashboardCombinedOrder[mapKey] = raw;
  });
  const existingDashboardCombinedSettings = (config && typeof config.dashboardCombinedSettings === 'object' && config.dashboardCombinedSettings)
    ? config.dashboardCombinedSettings
    : {};
  const dashboardCombinedSettings = {};
  ARR_COMBINE_SECTIONS.forEach((section) => {
    const mapKey = `combined:arr:${section.key}`;
    const existing = existingDashboardCombinedSettings[mapKey] || {};
    if (dashboardRemovedElements[mapKey] && existingDashboardCombinedSettings[mapKey]) {
      dashboardCombinedSettings[mapKey] = existingDashboardCombinedSettings[mapKey];
      return;
    }
    const visibilityRole = normalizeVisibilityRole(
      req.body[`dashboard_combined_arr_${section.key}_visibility_role`],
      resolveCombinedDashboardVisibilityRole(existing, 'user')
    );
    dashboardCombinedSettings[mapKey] = {
      ...existing,
      visibilityRole,
      enable: visibilityRole !== 'disabled',
      dashboard: visibilityRole !== 'disabled',
    };
  });
  DOWNLOADER_COMBINE_SECTIONS.forEach((section) => {
    const mapKey = `combined:downloader:${section.key}`;
    const existing = existingDashboardCombinedSettings[mapKey] || {};
    if (dashboardRemovedElements[mapKey] && existingDashboardCombinedSettings[mapKey]) {
      dashboardCombinedSettings[mapKey] = existingDashboardCombinedSettings[mapKey];
      return;
    }
    const visibilityRole = normalizeVisibilityRole(
      req.body[`dashboard_combined_downloader_${section.key}_visibility_role`],
      resolveCombinedDashboardVisibilityRole(existing, 'user')
    );
    dashboardCombinedSettings[mapKey] = {
      ...existing,
      visibilityRole,
      enable: visibilityRole !== 'disabled',
      dashboard: visibilityRole !== 'disabled',
    };
  });
  MEDIA_COMBINE_SECTIONS.forEach((section) => {
    const mapKey = `combined:media:${section.key}`;
    const existing = existingDashboardCombinedSettings[mapKey] || {};
    if (dashboardRemovedElements[mapKey] && existingDashboardCombinedSettings[mapKey]) {
      dashboardCombinedSettings[mapKey] = existingDashboardCombinedSettings[mapKey];
      return;
    }
    const visibilityRole = normalizeVisibilityRole(
      req.body[`dashboard_combined_media_${section.key}_visibility_role`],
      resolveCombinedDashboardVisibilityRole(existing, 'user')
    );
    dashboardCombinedSettings[mapKey] = {
      ...existing,
      visibilityRole,
      enable: visibilityRole !== 'disabled',
      dashboard: visibilityRole !== 'disabled',
    };
  });
  Object.keys(req.body || {}).forEach((key) => {
    const match = key.match(/^dashboard_combined_arrcustom_(.+)_present$/);
    if (!match) return;
    const customToken = normalizeCombinedCardToken(match[1] || '');
    if (!customToken) return;
    const mapKey = `combined:arrcustom:${customToken}`;
    const existing = existingDashboardCombinedSettings[mapKey] || {};
    const visibilityRole = normalizeVisibilityRole(
      req.body[`dashboard_combined_arrcustom_${customToken}_visibility_role`],
      resolveCombinedDashboardVisibilityRole(existing, 'user')
    );
    dashboardCombinedSettings[mapKey] = {
      ...existing,
      visibilityRole,
      enable: visibilityRole !== 'disabled',
      dashboard: visibilityRole !== 'disabled',
    };
  });
  Object.keys(req.body || {}).forEach((key) => {
    const match = key.match(/^dashboard_combined_downloadercustom_(.+)_present$/);
    if (!match) return;
    const customToken = normalizeCombinedCardToken(match[1] || '');
    if (!customToken) return;
    const mapKey = `combined:downloadercustom:${customToken}`;
    const existing = existingDashboardCombinedSettings[mapKey] || {};
    const visibilityRole = normalizeVisibilityRole(
      req.body[`dashboard_combined_downloadercustom_${customToken}_visibility_role`],
      resolveCombinedDashboardVisibilityRole(existing, 'user')
    );
    dashboardCombinedSettings[mapKey] = {
      ...existing,
      visibilityRole,
      enable: visibilityRole !== 'disabled',
      dashboard: visibilityRole !== 'disabled',
    };
  });
  Object.keys(req.body || {}).forEach((key) => {
    const match = key.match(/^dashboard_combined_mediacustom_(.+)_present$/);
    if (!match) return;
    const customToken = normalizeCombinedCardToken(match[1] || '');
    if (!customToken) return;
    const mapKey = `combined:mediacustom:${customToken}`;
    const existing = existingDashboardCombinedSettings[mapKey] || {};
    const visibilityRole = normalizeVisibilityRole(
      req.body[`dashboard_combined_mediacustom_${customToken}_visibility_role`],
      resolveCombinedDashboardVisibilityRole(existing, 'user')
    );
    dashboardCombinedSettings[mapKey] = {
      ...existing,
      visibilityRole,
      enable: visibilityRole !== 'disabled',
      dashboard: visibilityRole !== 'disabled',
    };
  });
  Object.entries(existingDashboardCombinedSettings).forEach(([mapKey, value]) => {
    if (
      !String(mapKey || '').startsWith('combined:arrcustom:')
      && !String(mapKey || '').startsWith('combined:downloadercustom:')
      && !String(mapKey || '').startsWith('combined:mediacustom:')
    ) return;
    if (!dashboardRemovedElements[mapKey]) return;
    if (dashboardCombinedSettings[mapKey]) return;
    dashboardCombinedSettings[mapKey] = value;
  });
  const apps = (config.apps || []).map((appItem) => ({
    ...appItem,
    overviewElements: buildDashboardElementsFromRequest(appItem, req.body),
    tautulliCards: shouldUpdateTautulliCards
      ? buildTautulliCardsFromDashboardRequest(appItem, req.body)
      : appItem.tautulliCards,
  }));
  const combinedOverviewScopes = [
    { prefix: 'arr', appIds: ARR_APP_IDS, sections: ARR_COMBINE_SECTIONS, getSection: getArrCombineSection },
    { prefix: 'downloader', appIds: DOWNLOADER_APP_IDS, sections: DOWNLOADER_COMBINE_SECTIONS, getSection: getDownloaderCombineSection },
    { prefix: 'media', appIds: MEDIA_APP_IDS, sections: MEDIA_COMBINE_SECTIONS, getSection: getMediaCombineSection },
  ];
  const currentCombinedOverviewRoleByKey = new Map();
  apps.forEach((appItem) => {
    const normalizedAppId = normalizeAppId(appItem?.id);
    if (!normalizedAppId) return;
    const overviewElements = mergeOverviewElementSettings(appItem);
    combinedOverviewScopes.forEach((scope) => {
      if (!isAppInSet(normalizedAppId, scope.appIds)) return;
      scope.sections.forEach((section) => {
        const sectionElement = overviewElements.find((element) => element.id === section.elementId);
        const overviewVisibilityRole = normalizeVisibilityRole(
          sectionElement?.overviewVisibilityRole,
          (sectionElement && sectionElement.enable === false) ? 'disabled' : 'user'
        );
        currentCombinedOverviewRoleByKey.set(`${scope.prefix}:${section.key}:${normalizedAppId}`, overviewVisibilityRole);
      });
    });
  });
  const combinedOverviewRoleUpdates = new Map();
  Object.keys(req.body || {}).forEach((key) => {
    combinedOverviewScopes.forEach((scope) => {
      const match = key.match(new RegExp(`^${scope.prefix}_app_state_present__(.+?)__(.+?)__(.+)$`));
      if (!match) return;
      const token = String(match[1] || '').trim();
      const sectionKey = String(match[2] || '').trim();
      const rawAppId = String(match[3] || '').trim();
      const normalizedAppId = normalizeAppId(rawAppId);
      if (!token || !sectionKey || !normalizedAppId) return;
      if (!isAppInSet(normalizedAppId, scope.appIds)) return;
      const section = scope.getSection(sectionKey);
      if (!section) return;
      const overviewRoleField = `${scope.prefix}_app_overview_role__${token}__${sectionKey}__${rawAppId}`;
      const updateKey = `${scope.prefix}:${section.key}:${normalizedAppId}`;
      const currentOverviewRole = currentCombinedOverviewRoleByKey.get(updateKey) || 'user';
      const nextUpdate = {
        appId: normalizedAppId,
        elementId: section.elementId,
        overviewVisibilityRole: normalizeVisibilityRole(req.body[overviewRoleField], currentOverviewRole),
      };
      const existingUpdate = combinedOverviewRoleUpdates.get(updateKey);
      if (!existingUpdate) {
        combinedOverviewRoleUpdates.set(updateKey, nextUpdate);
        return;
      }
      const existingDiffers = existingUpdate.overviewVisibilityRole !== currentOverviewRole;
      const nextDiffers = nextUpdate.overviewVisibilityRole !== currentOverviewRole;
      if (!existingDiffers && nextDiffers) {
        combinedOverviewRoleUpdates.set(updateKey, nextUpdate);
        return;
      }
      if (existingDiffers === nextDiffers) {
        combinedOverviewRoleUpdates.set(updateKey, nextUpdate);
      }
    });
  });
  if (combinedOverviewRoleUpdates.size) {
    const combinedUpdatesByAppId = new Map();
    combinedOverviewRoleUpdates.forEach((update) => {
      const appUpdates = combinedUpdatesByAppId.get(update.appId) || [];
      appUpdates.push(update);
      combinedUpdatesByAppId.set(update.appId, appUpdates);
    });
    for (let appIndex = 0; appIndex < apps.length; appIndex += 1) {
      const appItem = apps[appIndex];
      const normalizedAppId = normalizeAppId(appItem?.id);
      if (!normalizedAppId) continue;
      const appUpdates = combinedUpdatesByAppId.get(normalizedAppId);
      if (!appUpdates || !appUpdates.length) continue;
      const nextOverviewElements = mergeOverviewElementSettings(appItem).map((element) => {
        const elementUpdate = appUpdates.find((update) => update.elementId === element.id);
        if (!elementUpdate) return element;
        const overviewVisibilityRole = normalizeVisibilityRole(
          elementUpdate.overviewVisibilityRole,
          element.enable === false ? 'disabled' : 'user'
        );
        return {
          ...element,
          enable: overviewVisibilityRole !== 'disabled',
          overviewVisibilityRole,
        };
      });
      apps[appIndex] = {
        ...appItem,
        overviewElements: nextOverviewElements,
      };
    }
  }
  const arrDashboardCombine = resolveArrDashboardCombineSettings(config, apps);
  ARR_COMBINE_SECTIONS.forEach((section) => {
    const mapKey = `combined:arr:${section.key}`;
    if (dashboardRemovedElements[mapKey]) return;
    arrDashboardCombine[section.key] = arrDashboardCombine[section.key] || {};
    apps
      .filter((appItem) => isAppInSet(appItem.id, ARR_APP_IDS))
      .forEach((appItem) => {
        const field = `arr_combine_${section.key}_${appItem.id}`;
        arrDashboardCombine[section.key][appItem.id] = Boolean(req.body[field]);
      });
  });
  const arrSelectableAppIds = apps
    .filter((appItem) => !appItem?.removed && isAppInSet(appItem.id, ARR_APP_IDS))
    .map((appItem) => normalizeAppId(appItem.id))
    .filter(Boolean);
  const arrDashboardCombinedCards = resolveArrDashboardCombinedCards(config, apps).map((card) => {
    const cardKey = `combined:arrcustom:${card.id}`;
    if (dashboardRemovedElements[cardKey]) return card;
    const selected = arrSelectableAppIds.filter((appId) => Boolean(req.body[`arrcustom_combine_${card.id}_${appId}`]));
    return {
      ...card,
      appIds: selected.length ? selected : [...arrSelectableAppIds],
    };
  });
  const downloaderDashboardCombine = resolveDownloaderDashboardCombineSettings(config, apps);
  DOWNLOADER_COMBINE_SECTIONS.forEach((section) => {
    const mapKey = `combined:downloader:${section.key}`;
    if (dashboardRemovedElements[mapKey]) return;
    downloaderDashboardCombine[section.key] = downloaderDashboardCombine[section.key] || {};
    apps
      .filter((appItem) => isAppInSet(appItem.id, DOWNLOADER_APP_IDS))
      .forEach((appItem) => {
        const field = `downloader_combine_${section.key}_${appItem.id}`;
        downloaderDashboardCombine[section.key][appItem.id] = Boolean(req.body[field]);
      });
  });
  const downloaderSelectableAppIds = apps
    .filter((appItem) => !appItem?.removed && isAppInSet(appItem.id, DOWNLOADER_APP_IDS))
    .map((appItem) => normalizeAppId(appItem.id))
    .filter(Boolean);
  const downloaderDashboardCards = resolveDownloaderDashboardCards(config, apps).map((card) => {
    const cardKey = `combined:downloadercustom:${card.id}`;
    if (dashboardRemovedElements[cardKey]) return card;
    const selected = downloaderSelectableAppIds.filter((appId) => Boolean(req.body[`downloadercustom_combine_${card.id}_${appId}`]));
    return {
      ...card,
      appIds: selected,
    };
  });
  const mediaDashboardCombine = resolveMediaDashboardCombineSettings(config, apps);
  MEDIA_COMBINE_SECTIONS.forEach((section) => {
    const mapKey = `combined:media:${section.key}`;
    if (dashboardRemovedElements[mapKey]) return;
    mediaDashboardCombine[section.key] = mediaDashboardCombine[section.key] || {};
    apps
      .filter((appItem) => isAppInSet(appItem.id, MEDIA_APP_IDS))
      .forEach((appItem) => {
        const field = `media_combine_${section.key}_${appItem.id}`;
        mediaDashboardCombine[section.key][appItem.id] = Boolean(req.body[field]);
      });
  });
  const mediaSelectableAppIds = apps
    .filter((appItem) => !appItem?.removed && isAppInSet(appItem.id, MEDIA_APP_IDS))
    .map((appItem) => normalizeAppId(appItem.id))
    .filter(Boolean);
  const mediaDashboardCards = resolveMediaDashboardCards(config, apps).map((card) => {
    const cardKey = `combined:mediacustom:${card.id}`;
    if (dashboardRemovedElements[cardKey]) return card;
    const selected = mediaSelectableAppIds.filter((appId) => Boolean(req.body[`mediacustom_combine_${card.id}_${appId}`]));
    return {
      ...card,
      appIds: selected,
    };
  });
  const arrCombinedQueueDisplay = resolveCombinedQueueDisplaySettings(config, 'arrCombinedQueueDisplay');
  arrCombinedQueueDisplay.queueShowDetail = Boolean(req.body.arr_combined_queue_col_detail);
  arrCombinedQueueDisplay.queueShowSubDetail = Boolean(req.body.arr_combined_queue_col_subdetail);
  arrCombinedQueueDisplay.queueShowSize = Boolean(req.body.arr_combined_queue_col_size);
  arrCombinedQueueDisplay.queueShowProtocol = Boolean(req.body.arr_combined_queue_col_protocol);
  arrCombinedQueueDisplay.queueShowTimeLeft = Boolean(req.body.arr_combined_queue_col_timeleft);
  arrCombinedQueueDisplay.queueShowProgress = Boolean(req.body.arr_combined_queue_col_progress);
  const arrQueueRows = Number(req.body.arr_combined_queue_visible_rows);
  if (Number.isFinite(arrQueueRows)) {
    arrCombinedQueueDisplay.queueVisibleRows = Math.max(5, Math.min(50, arrQueueRows));
  }

  const downloaderCombinedQueueDisplay = resolveCombinedQueueDisplaySettings(config, 'downloaderCombinedQueueDisplay');
  downloaderCombinedQueueDisplay.queueShowDetail = Boolean(req.body.downloader_combined_queue_col_detail);
  downloaderCombinedQueueDisplay.queueShowSubDetail = Boolean(req.body.downloader_combined_queue_col_subdetail);
  downloaderCombinedQueueDisplay.queueShowSize = Boolean(req.body.downloader_combined_queue_col_size);
  downloaderCombinedQueueDisplay.queueShowProtocol = Boolean(req.body.downloader_combined_queue_col_protocol);
  downloaderCombinedQueueDisplay.queueShowTimeLeft = Boolean(req.body.downloader_combined_queue_col_timeleft);
  downloaderCombinedQueueDisplay.queueShowProgress = Boolean(req.body.downloader_combined_queue_col_progress);
  const downloaderQueueRows = Number(req.body.downloader_combined_queue_visible_rows);
  if (Number.isFinite(downloaderQueueRows)) {
    downloaderCombinedQueueDisplay.queueVisibleRows = Math.max(5, Math.min(50, downloaderQueueRows));
  }
  saveDashboardScopedConfig(baseConfig, {
    ...config,
    apps,
    arrDashboardCombine,
    mediaDashboardCombine,
    downloaderDashboardCombine,
    arrCombinedQueueDisplay,
    downloaderCombinedQueueDisplay,
    arrDashboardCombinedCards,
    downloaderDashboardCards,
    mediaDashboardCards,
    dashboardCombinedOrder,
    dashboardCombinedSettings,
  }, selectedDashboardId, nextDashboardDefinitions);
  res.redirect(buildDashboardSettingsRedirect(req));
});

app.post('/settings/dashboard-elements/remove', requireSettingsAdmin, (req, res) => {
  const baseConfig = loadConfig();
  const dashboardSelection = resolveDashboardSelection(baseConfig, resolveRequestedDashboardId(req), 'admin', { includeHidden: true });
  const activeDashboard = dashboardSelection.activeDashboard || resolveDashboardDefinitions(baseConfig)[0] || { id: DASHBOARD_MAIN_ID };
  const selectedDashboardId = String(activeDashboard.id || DASHBOARD_MAIN_ID).trim() || DASHBOARD_MAIN_ID;
  const config = applyDashboardStateSnapshot(baseConfig, activeDashboard);
  const key = String(req.body?.dashboard_element_key || req.body?.key || '').trim();
  if (!key) {
    return res.redirect(buildDashboardSettingsRedirect(req, { dashboardElementError: 'Missing dashboard item key.' }));
  }

  const customCombinedMatch = key.match(/^combined:(arrcustom|downloadercustom|mediacustom):(.+)$/);
  if (customCombinedMatch) {
    const customType = String(customCombinedMatch[1] || '').trim();
    const customToken = normalizeCombinedCardToken(customCombinedMatch[2] || '');
    if (!customToken) {
      return res.redirect(buildDashboardSettingsRedirect(req, { dashboardElementError: 'Invalid custom dashboard card id.' }));
    }
    const listField = customType === 'arrcustom'
      ? 'arrDashboardCombinedCards'
      : (customType === 'downloadercustom' ? 'downloaderDashboardCards' : 'mediaDashboardCards');
    const customKey = `combined:${customType}:${customToken}`;
    const existingCards = Array.isArray(config?.[listField]) ? config[listField] : [];
    const nextCards = existingCards.filter((card) => normalizeCombinedCardToken(card?.id || '') !== customToken);
    const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
      ? { ...config.dashboardRemovedElements }
      : {};
    const dashboardCombinedSettings = (config && typeof config.dashboardCombinedSettings === 'object' && config.dashboardCombinedSettings)
      ? { ...config.dashboardCombinedSettings }
      : {};
    const dashboardCombinedOrder = (config && typeof config.dashboardCombinedOrder === 'object' && config.dashboardCombinedOrder)
      ? { ...config.dashboardCombinedOrder }
      : {};
    delete dashboardRemovedElements[key];
    delete dashboardRemovedElements[customKey];
    delete dashboardCombinedSettings[key];
    delete dashboardCombinedSettings[customKey];
    delete dashboardCombinedOrder[key];
    delete dashboardCombinedOrder[customKey];
    saveDashboardScopedConfig(baseConfig, {
      ...config,
      [listField]: nextCards,
      dashboardRemovedElements,
      dashboardCombinedSettings,
      dashboardCombinedOrder,
    }, selectedDashboardId);
    return res.redirect(buildDashboardSettingsRedirect(req, { dashboardElementResult: 'removed' }));
  }

  const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
    ? { ...config.dashboardRemovedElements }
    : {};
  dashboardRemovedElements[key] = true;
  saveDashboardScopedConfig(baseConfig, {
    ...config,
    dashboardRemovedElements,
  }, selectedDashboardId);
  return res.redirect(buildDashboardSettingsRedirect(req, { dashboardElementResult: 'removed' }));
});

function redirectDashboardAddError(req, res, message) {
  return res.redirect(buildDashboardSettingsRedirect(req, { dashboardElementError: message }));
}

function redirectDashboardAddResult(req, res, result = 'added') {
  return res.redirect(buildDashboardSettingsRedirect(req, { dashboardElementResult: result }));
}

function resolveNextDashboardOrder(config) {
  const apps = Array.isArray(config?.apps) ? config.apps : [];
  const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
    ? config.dashboardRemovedElements
    : {};
  const dashboardCombinedOrder = (config && typeof config.dashboardCombinedOrder === 'object' && config.dashboardCombinedOrder)
    ? config.dashboardCombinedOrder
    : {};
  let maxOrder = 0;

  const updateMaxOrder = (value) => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return;
    if (parsed > maxOrder) maxOrder = parsed;
  };

  apps
    .filter((appItem) => !appItem?.removed)
    .forEach((appItem) => {
      const appId = String(appItem?.id || '').trim();
      if (!appId) return;
      const overviewElements = mergeOverviewElementSettings(appItem);
      overviewElements.forEach((element) => {
        const elementId = String(element?.id || '').trim();
        if (!elementId) return;
        if (dashboardRemovedElements[`app:${appId}:${elementId}`]) return;
        updateMaxOrder(element?.order);
      });
    });

  Object.entries(dashboardCombinedOrder).forEach(([combinedKey, orderValue]) => {
    if (dashboardRemovedElements[combinedKey]) return;
    updateMaxOrder(orderValue);
  });

  return maxOrder + 1;
}

function handleDashboardAddRequest(req, res) {
  const baseConfig = loadConfig();
  const dashboardSelection = resolveDashboardSelection(baseConfig, resolveRequestedDashboardId(req), 'admin', { includeHidden: true });
  const activeDashboard = dashboardSelection.activeDashboard || resolveDashboardDefinitions(baseConfig)[0] || { id: DASHBOARD_MAIN_ID };
  const selectedDashboardId = String(activeDashboard.id || DASHBOARD_MAIN_ID).trim() || DASHBOARD_MAIN_ID;
  const config = applyDashboardStateSnapshot(baseConfig, activeDashboard);
  const apps = Array.isArray(config.apps) ? config.apps : [];
  const persistConfig = (nextConfig) => saveDashboardScopedConfig(baseConfig, nextConfig, selectedDashboardId);
  const normalizeSubmittedValues = (value) => {
    if (Array.isArray(value)) {
      return value.flatMap((entry) => normalizeSubmittedValues(entry));
    }
    const text = String(value || '').trim();
    if (!text) return [];
    return text
      .split(',')
      .map((entry) => String(entry || '').trim())
      .filter(Boolean);
  };
  const submittedKeys = [
    ...normalizeSubmittedValues(req.body?.dashboard_add_key_current),
    ...normalizeSubmittedValues(req.body?.dashboard_add_key_deprecated),
    ...normalizeSubmittedValues(req.body?.dashboard_add_key),
    ...normalizeSubmittedValues(req.body?.dashboard_element_key),
    ...normalizeSubmittedValues(req.body?.dashboard_combined_key),
    ...normalizeSubmittedValues(req.body?.key),
  ];
  const key = submittedKeys.find((entry) => /^(app:|new:|combined:)/.test(entry)) || submittedKeys[0] || '';
  if (!key) {
    return redirectDashboardAddError(req, res, 'Select a dashboard card to add.');
  }

  const deprecatedDescriptor = resolveDeprecatedDashboardCardDescriptor(key);
  if (deprecatedDescriptor) {
    const draftDashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
      ? { ...config.dashboardRemovedElements }
      : {};
    delete draftDashboardRemovedElements[key];
    const draftConfig = {
      ...config,
      dashboardRemovedElements: draftDashboardRemovedElements,
    };
    const nextOrder = resolveNextDashboardOrder(config);

    if (deprecatedDescriptor.kind === 'app') {
      draftConfig.apps = apps.map((appItem) => {
        if (normalizeAppId(appItem?.id) !== deprecatedDescriptor.appId) return appItem;
        const existingOverview = Array.isArray(appItem?.overviewElements) ? appItem.overviewElements : [];
        const nextOverview = existingOverview.map((entry) => ({ ...(entry || {}) }));
        const existingIndex = nextOverview.findIndex((entry) => String(entry?.id || '').trim() === deprecatedDescriptor.elementId);
        if (existingIndex >= 0) {
          nextOverview[existingIndex] = {
            ...nextOverview[existingIndex],
            id: deprecatedDescriptor.elementId,
            dashboard: true,
            dashboardVisibilityRole: normalizeVisibilityRole(nextOverview[existingIndex]?.dashboardVisibilityRole, 'user') === 'disabled'
              ? 'user'
              : normalizeVisibilityRole(nextOverview[existingIndex]?.dashboardVisibilityRole, 'user'),
            order: nextOrder,
          };
        } else {
          nextOverview.push({
            id: deprecatedDescriptor.elementId,
            dashboard: true,
            dashboardVisibilityRole: 'user',
            order: nextOrder,
          });
        }
        return {
          ...appItem,
          overviewElements: nextOverview,
        };
      });
    } else if (deprecatedDescriptor.kind === 'combined') {
      const existingCombinedSettings = (config && typeof config.dashboardCombinedSettings === 'object' && config.dashboardCombinedSettings)
        ? { ...config.dashboardCombinedSettings }
        : {};
      const existingSetting = existingCombinedSettings[key] && typeof existingCombinedSettings[key] === 'object'
        ? existingCombinedSettings[key]
        : {};
      const visibilityRole = resolveCombinedDashboardVisibilityRole(existingSetting, 'user') === 'disabled'
        ? 'user'
        : resolveCombinedDashboardVisibilityRole(existingSetting, 'user');
      existingCombinedSettings[key] = {
        ...existingSetting,
        visibilityRole,
        enable: visibilityRole !== 'disabled',
        dashboard: visibilityRole !== 'disabled',
      };
      draftConfig.dashboardCombinedSettings = existingCombinedSettings;
      const existingCombinedOrder = (config && typeof config.dashboardCombinedOrder === 'object' && config.dashboardCombinedOrder)
        ? { ...config.dashboardCombinedOrder }
        : {};
      if (!Number.isFinite(Number(existingCombinedOrder[key]))) {
        existingCombinedOrder[key] = nextOrder;
      }
      draftConfig.dashboardCombinedOrder = existingCombinedOrder;
    }

    const migration = migrateDeprecatedDashboardCards(draftConfig);
    if (migration.changed) {
      persistConfig(migration.config);
      return redirectDashboardAddResult(req, res, 'added');
    }
  }

  if (key.startsWith('app:')) {
    const appMatch = key.match(/^app:([^:]+):(.+)$/);
    if (!appMatch) {
      return redirectDashboardAddError(req, res, 'Invalid app card selection.');
    }
    const selectedAppId = String(appMatch[1] || '').trim();
    const selectedElementId = String(appMatch[2] || '').trim();
    if (!selectedAppId || !selectedElementId) {
      return redirectDashboardAddError(req, res, 'Invalid app card selection.');
    }
    const nextDashboardOrder = resolveNextDashboardOrder(config);
    const nextApps = apps.map((appItem) => {
      if (String(appItem?.id || '').trim() !== selectedAppId) return appItem;
      const existingOverview = Array.isArray(appItem?.overviewElements) ? appItem.overviewElements : [];
      const nextOverview = existingOverview.map((entry) => ({ ...(entry || {}) }));
      const existingIndex = nextOverview.findIndex((entry) => String(entry?.id || '').trim() === selectedElementId);
      if (existingIndex >= 0) {
        nextOverview[existingIndex] = {
          ...nextOverview[existingIndex],
          id: selectedElementId,
          dashboard: true,
          order: nextDashboardOrder,
        };
      } else {
        nextOverview.push({
          id: selectedElementId,
          dashboard: true,
          order: nextDashboardOrder,
        });
      }
      return {
        ...appItem,
        overviewElements: nextOverview,
      };
    });
    const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
      ? { ...config.dashboardRemovedElements }
      : {};
    delete dashboardRemovedElements[key];
    persistConfig({
      ...config,
      apps: nextApps,
      dashboardRemovedElements,
    });
    return redirectDashboardAddResult(req, res, 'added');
  }

  const newArrMatch = key.match(/^new:arr:(.+)$/);
  if (newArrMatch) {
    if (!ENABLE_ARR_UNIFIED_CARDS) {
      return redirectDashboardAddError(req, res, 'ARR unified cards are currently disabled.');
    }
    const sectionKey = String(newArrMatch[1] || '').trim();
    if (!getArrCombineSection(sectionKey)) {
      return redirectDashboardAddError(req, res, 'Invalid combined section selected.');
    }
    const allowedAppIds = [
      ...new Set(
        apps
          .filter((appItem) => !appItem?.removed && isAppInSet(appItem?.id, ARR_APP_IDS))
          .map((appItem) => normalizeAppId(appItem?.id))
          .filter(Boolean)
      ),
    ];
    if (!allowedAppIds.length) {
      return redirectDashboardAddError(req, res, 'No ARR sources available to build a dashboard card.');
    }
    const existingCards = resolveArrDashboardCombinedCards(config, apps);
    const cardId = normalizeCombinedCardToken(buildCombinedCardId());
    const nextCards = [...existingCards, {
      id: cardId,
      sectionKey,
      appIds: allowedAppIds,
    }];

    const combinedKey = `combined:arrcustom:${cardId}`;
    const existingCombinedSettings = (config && typeof config.dashboardCombinedSettings === 'object' && config.dashboardCombinedSettings)
      ? { ...config.dashboardCombinedSettings }
      : {};
    existingCombinedSettings[combinedKey] = { enable: true, dashboard: true, visibilityRole: 'user' };
    const existingCombinedOrder = (config && typeof config.dashboardCombinedOrder === 'object' && config.dashboardCombinedOrder)
      ? { ...config.dashboardCombinedOrder }
      : {};
    const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
      ? { ...config.dashboardRemovedElements }
      : {};
    existingCombinedOrder[combinedKey] = resolveNextDashboardOrder(config);
    delete dashboardRemovedElements[combinedKey];

    persistConfig({
      ...config,
      arrDashboardCombinedCards: nextCards,
      dashboardCombinedSettings: existingCombinedSettings,
      dashboardCombinedOrder: existingCombinedOrder,
      dashboardRemovedElements,
    });
    return redirectDashboardAddResult(req, res, 'added');
  }

  const newMediaMatch = key.match(/^new:media:(.+)$/);
  if (newMediaMatch) {
    const sectionKey = String(newMediaMatch[1] || '').trim();
    if (!getMediaCombineSection(sectionKey)) {
      return redirectDashboardAddError(req, res, 'Invalid combined section selected.');
    }
    const allowedAppIds = [
      ...new Set(
        apps
          .filter((appItem) => !appItem?.removed && isAppInSet(appItem?.id, MEDIA_APP_IDS))
          .map((appItem) => normalizeAppId(appItem?.id))
          .filter(Boolean)
      ),
    ];
    if (!allowedAppIds.length) {
      return redirectDashboardAddError(req, res, 'No media sources available to build a dashboard card.');
    }
    const existingCards = resolveMediaDashboardCards(config, apps);
    const cardId = normalizeCombinedCardToken(buildCombinedCardId());
    const nextCards = [...existingCards, {
      id: cardId,
      sectionKey,
      appIds: allowedAppIds,
    }];
    const combinedKey = `combined:mediacustom:${cardId}`;
    const existingCombinedSettings = (config && typeof config.dashboardCombinedSettings === 'object' && config.dashboardCombinedSettings)
      ? { ...config.dashboardCombinedSettings }
      : {};
    existingCombinedSettings[combinedKey] = { enable: true, dashboard: true, visibilityRole: 'user' };
    const existingCombinedOrder = (config && typeof config.dashboardCombinedOrder === 'object' && config.dashboardCombinedOrder)
      ? { ...config.dashboardCombinedOrder }
      : {};
    const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
      ? { ...config.dashboardRemovedElements }
      : {};
    existingCombinedOrder[combinedKey] = resolveNextDashboardOrder(config);
    delete dashboardRemovedElements[combinedKey];

    persistConfig({
      ...config,
      mediaDashboardCards: nextCards,
      dashboardCombinedSettings: existingCombinedSettings,
      dashboardCombinedOrder: existingCombinedOrder,
      dashboardRemovedElements,
    });
    return redirectDashboardAddResult(req, res, 'added');
  }

  const newDownloaderMatch = key.match(/^new:downloader:(.+)$/);
  if (newDownloaderMatch) {
    if (!ENABLE_DOWNLOADER_UNIFIED_CARDS) {
      return redirectDashboardAddError(req, res, 'Downloader unified cards are currently disabled.');
    }
    const sectionKey = String(newDownloaderMatch[1] || '').trim();
    if (!getDownloaderCombineSection(sectionKey)) {
      return redirectDashboardAddError(req, res, 'Invalid combined section selected.');
    }
    const allowedAppIds = [
      ...new Set(
        apps
          .filter((appItem) => !appItem?.removed && isAppInSet(appItem?.id, DOWNLOADER_APP_IDS))
          .map((appItem) => normalizeAppId(appItem?.id))
          .filter(Boolean)
      ),
    ];
    if (!allowedAppIds.length) {
      return redirectDashboardAddError(req, res, 'No downloader sources available to build a dashboard card.');
    }
    const existingCards = resolveDownloaderDashboardCards(config, apps);
    const cardId = normalizeCombinedCardToken(buildCombinedCardId());
    const nextCards = [...existingCards, {
      id: cardId,
      sectionKey,
      appIds: allowedAppIds,
    }];
    const combinedKey = `combined:downloadercustom:${cardId}`;
    const existingCombinedSettings = (config && typeof config.dashboardCombinedSettings === 'object' && config.dashboardCombinedSettings)
      ? { ...config.dashboardCombinedSettings }
      : {};
    existingCombinedSettings[combinedKey] = { enable: true, dashboard: true, visibilityRole: 'user' };
    const existingCombinedOrder = (config && typeof config.dashboardCombinedOrder === 'object' && config.dashboardCombinedOrder)
      ? { ...config.dashboardCombinedOrder }
      : {};
    const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
      ? { ...config.dashboardRemovedElements }
      : {};
    existingCombinedOrder[combinedKey] = resolveNextDashboardOrder(config);
    delete dashboardRemovedElements[combinedKey];

    persistConfig({
      ...config,
      downloaderDashboardCards: nextCards,
      dashboardCombinedSettings: existingCombinedSettings,
      dashboardCombinedOrder: existingCombinedOrder,
      dashboardRemovedElements,
    });
    return redirectDashboardAddResult(req, res, 'added');
  }

  const newWidgetBarMatch = key.match(/^new:widget-bar:(.+)$/);
  if (newWidgetBarMatch) {
    const barId = normalizeWidgetBarId(String(newWidgetBarMatch[1] || '').trim());
    if (!barId) {
      return redirectDashboardAddError(req, res, 'Invalid widget bar id.');
    }
    const allBars = Array.isArray(config?.widgetBars) ? config.widgetBars : [];
    const barExists = allBars.some((b) => normalizeWidgetBarId(String(b?.id || '')) === barId);
    if (!barExists) {
      return redirectDashboardAddError(req, res, 'Widget bar not found.');
    }
    const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
      ? { ...config.dashboardRemovedElements }
      : {};
    delete dashboardRemovedElements[`widget-bar:${barId}`];
    persistConfig({ ...config, dashboardRemovedElements });
    return redirectDashboardAddResult(req, res, 'added');
  }

  const arrCustomAddKeys = resolveArrDashboardCombinedCards(config, apps).map((card, index) => {
    const customToken = normalizeCombinedCardToken(card?.id || '') || `card-${index + 1}`;
    return `combined:arrcustom:${customToken}`;
  });
  const downloaderCustomAddKeys = resolveDownloaderDashboardCards(config, apps).map((card, index) => {
    const customToken = normalizeCombinedCardToken(card?.id || '') || `card-${index + 1}`;
    return `combined:downloadercustom:${customToken}`;
  });
  const mediaCustomAddKeys = resolveMediaDashboardCards(config, apps).map((card, index) => {
    const customToken = normalizeCombinedCardToken(card?.id || '') || `card-${index + 1}`;
    return `combined:mediacustom:${customToken}`;
  });
  const allowedKeys = new Set([
    ...ARR_COMBINE_SECTIONS.map((section) => `combined:arr:${section.key}`),
    ...DOWNLOADER_COMBINE_SECTIONS.map((section) => `combined:downloader:${section.key}`),
    ...MEDIA_COMBINE_SECTIONS.map((section) => `combined:media:${section.key}`),
    ...arrCustomAddKeys,
    ...downloaderCustomAddKeys,
    ...mediaCustomAddKeys,
  ]);
  if (!allowedKeys.has(key)) {
    return redirectDashboardAddError(req, res, 'Invalid combined card selection.');
  }

  const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
    ? { ...config.dashboardRemovedElements }
    : {};
  const existingCombinedOrder = (config && typeof config.dashboardCombinedOrder === 'object' && config.dashboardCombinedOrder)
    ? { ...config.dashboardCombinedOrder }
    : {};
  existingCombinedOrder[key] = resolveNextDashboardOrder(config);
  delete dashboardRemovedElements[key];
  persistConfig({
    ...config,
    dashboardCombinedOrder: existingCombinedOrder,
    dashboardRemovedElements,
  });
  return redirectDashboardAddResult(req, res, 'added');
}

app.post('/settings/dashboards/add', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const dashboards = resolveDashboardDefinitions(config);
  if (dashboards.length >= DASHBOARD_MAX_COUNT) {
    return res.redirect(buildDashboardSettingsRedirect(req, { dashboardElementError: `Maximum dashboards reached (${DASHBOARD_MAX_COUNT}).` }));
  }
  const selection = resolveDashboardSelection(config, resolveRequestedDashboardId(req), 'admin', { includeHidden: true });
  const activeDashboard = selection.activeDashboard || dashboards[0] || {
    id: DASHBOARD_MAIN_ID,
    name: 'Dashboard',
    icon: DEFAULT_DASHBOARD_ICON,
    visibilityRole: 'user',
  };
  const duplicateSourceRaw = String(req.body?.dashboard_duplicate_source || '').trim();
  const duplicateSelectionPresent = Object.prototype.hasOwnProperty.call(req.body || {}, 'dashboard_duplicate_source');
  const useBlankDashboard = duplicateSelectionPresent && duplicateSourceRaw === 'blank';
  const duplicateSourceId = useBlankDashboard ? '' : normalizeDashboardInstanceId(duplicateSourceRaw);
  const duplicateSourceDashboard = duplicateSourceId
    ? (dashboards.find((entry) => entry.id === duplicateSourceId) || null)
    : null;
  const sourceDashboard = duplicateSourceDashboard || activeDashboard;
  const sourceConfig = (!useBlankDashboard && sourceDashboard)
    ? applyDashboardStateSnapshot(config, sourceDashboard)
    : config;
  const nextId = buildDashboardInstanceId(dashboards);
  const nextDashboards = [
    ...dashboards,
    (() => {
      const visibilityRoles = duplicateSourceDashboard
        ? resolveDashboardDefinitionVisibilityRoles(duplicateSourceDashboard, 'user')
        : resolveDashboardVisibilityRolesFromRequest(req.body, sourceDashboard);
      return {
        id: nextId,
        name: duplicateSourceDashboard
          ? normalizeDashboardDisplayName(duplicateSourceDashboard.name, `Dashboard ${dashboards.length + 1}`)
          : normalizeDashboardDisplayName(req.body?.dashboard_name, `Dashboard ${dashboards.length + 1}`),
        icon: duplicateSourceDashboard
          ? normalizeDashboardIcon(duplicateSourceDashboard.icon, DEFAULT_DASHBOARD_ICON)
          : normalizeDashboardIcon(req.body?.dashboard_icon, sourceDashboard.icon || DEFAULT_DASHBOARD_ICON),
        visibilityRoles,
        visibilityRole: deriveDashboardLegacyVisibilityRole(
          visibilityRoles,
          visibilityRoles.length ? (sourceDashboard.visibilityRole || 'user') : 'disabled',
        ),
        state: useBlankDashboard
          ? buildEmptyDashboardStateSnapshot(config)
          : extractDashboardStateSnapshot(sourceConfig),
      };
    })(),
  ];
  saveConfig({
    ...config,
    dashboards: nextDashboards,
  });
  return res.redirect(buildDashboardSettingsRedirect(req, { dashboardId: nextId }));
});

app.post('/settings/dashboards/delete', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const dashboards = resolveDashboardDefinitions(config);
  const requestedDeleteId = normalizeDashboardInstanceId(req.body?.deleteDashboardId || req.body?.dashboardId || req.body?.id);
  if (!requestedDeleteId || requestedDeleteId === DASHBOARD_MAIN_ID) {
    return res.redirect(buildDashboardSettingsRedirect(req, { dashboardElementError: 'The main dashboard cannot be deleted.' }));
  }
  const nextDashboards = dashboards.filter((entry) => entry.id !== requestedDeleteId);
  if (nextDashboards.length === dashboards.length) {
    return res.redirect(buildDashboardSettingsRedirect(req, { dashboardElementError: 'Dashboard not found.' }));
  }
  const currentDashboardId = resolveRequestedDashboardId(req);
  const redirectDashboardId = currentDashboardId && currentDashboardId !== requestedDeleteId
    ? currentDashboardId
    : DASHBOARD_MAIN_ID;
  saveConfig({
    ...config,
    dashboards: nextDashboards,
  });
  return res.redirect(buildDashboardSettingsRedirect(req, { dashboardId: redirectDashboardId }));
});

app.post('/settings/dashboard/add', requireSettingsAdmin, handleDashboardAddRequest);

app.post('/settings/dashboard-elements/add', requireSettingsAdmin, handleDashboardAddRequest);

app.post('/settings/dashboard-combined/add', requireSettingsAdmin, handleDashboardAddRequest);

app.post('/settings/dashboard-combined/arr/add', requireSettingsAdmin, (req, res) => {
  if (!ENABLE_ARR_UNIFIED_CARDS) {
    return res.redirect('/settings?tab=custom&settingsCustomTab=dashboard&arrCombinedCardError=ARR+unified+cards+are+currently+disabled.');
  }
  const config = loadConfig();
  const apps = Array.isArray(config.apps) ? config.apps : [];
  const sectionKey = String(req.body?.arr_combined_section || req.body?.section || '').trim();
  if (!getArrCombineSection(sectionKey)) {
    return res.redirect('/settings?tab=custom&settingsCustomTab=dashboard&arrCombinedCardError=Invalid+combined+section+selected.');
  }

  const allowedAppIds = [
    ...new Set(
      apps
        .filter((appItem) => !appItem?.removed && isAppInSet(appItem?.id, ARR_APP_IDS))
        .map((appItem) => normalizeAppId(appItem?.id))
        .filter(Boolean)
    ),
  ];
  if (!allowedAppIds.length) {
    return res.redirect('/settings?tab=custom&settingsCustomTab=dashboard&arrCombinedCardError=No+ARR+sources+available+to+build+a+dashboard+card.');
  }

  const existingCards = resolveArrDashboardCombinedCards(config, apps);
  const cardId = normalizeCombinedCardToken(buildCombinedCardId());
  const nextCards = [...existingCards, {
    id: cardId,
    sectionKey,
    appIds: allowedAppIds,
  }];

  const combinedKey = `combined:arrcustom:${cardId}`;
  const existingCombinedSettings = (config && typeof config.dashboardCombinedSettings === 'object' && config.dashboardCombinedSettings)
    ? { ...config.dashboardCombinedSettings }
    : {};
  existingCombinedSettings[combinedKey] = { enable: true, dashboard: true };
  const existingCombinedOrder = (config && typeof config.dashboardCombinedOrder === 'object' && config.dashboardCombinedOrder)
    ? { ...config.dashboardCombinedOrder }
    : {};
  const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
    ? { ...config.dashboardRemovedElements }
    : {};
  existingCombinedOrder[combinedKey] = resolveNextDashboardOrder(config);
  delete dashboardRemovedElements[combinedKey];

  saveConfig({
    ...config,
    arrDashboardCombinedCards: nextCards,
    dashboardCombinedSettings: existingCombinedSettings,
    dashboardCombinedOrder: existingCombinedOrder,
    dashboardRemovedElements,
  });
  return res.redirect('/settings?tab=custom&settingsCustomTab=dashboard&arrCombinedCardResult=added');
});

app.post('/settings/dashboard-combined/arr/delete', requireSettingsAdmin, (req, res) => {
  if (!ENABLE_ARR_UNIFIED_CARDS) {
    return res.redirect('/settings?tab=custom&settingsCustomTab=dashboard&arrCombinedCardError=ARR+unified+cards+are+currently+disabled.');
  }
  const config = loadConfig();
  const apps = Array.isArray(config.apps) ? config.apps : [];
  const cardId = normalizeCombinedCardToken(req.body?.id || '');
  if (!cardId) {
    return res.redirect('/settings?tab=custom&settingsCustomTab=dashboard&arrCombinedCardError=Missing+combined+card+id.');
  }
  const existingCards = resolveArrDashboardCombinedCards(config, apps);
  const nextCards = existingCards.filter((card) => card.id !== cardId);
  if (nextCards.length === existingCards.length) {
    return res.redirect('/settings?tab=custom&settingsCustomTab=dashboard&arrCombinedCardError=Combined+card+not+found.');
  }

  const combinedKey = `combined:arrcustom:${cardId}`;
  const existingCombinedSettings = (config && typeof config.dashboardCombinedSettings === 'object' && config.dashboardCombinedSettings)
    ? { ...config.dashboardCombinedSettings }
    : {};
  const existingCombinedOrder = (config && typeof config.dashboardCombinedOrder === 'object' && config.dashboardCombinedOrder)
    ? { ...config.dashboardCombinedOrder }
    : {};
  const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
    ? { ...config.dashboardRemovedElements }
    : {};
  delete existingCombinedSettings[combinedKey];
  delete existingCombinedOrder[combinedKey];
  delete dashboardRemovedElements[combinedKey];

  saveConfig({
    ...config,
    arrDashboardCombinedCards: nextCards,
    dashboardCombinedSettings: existingCombinedSettings,
    dashboardCombinedOrder: existingCombinedOrder,
    dashboardRemovedElements,
  });
  return res.redirect('/settings?tab=custom&settingsCustomTab=dashboard&arrCombinedCardResult=removed');
});

// GET /user-settings — moved to src/routes/pages.js

app.post('/user-settings/profile', requireUser, (req, res) => {
  const source = String(req.session?.user?.source || '').trim().toLowerCase();
  if (source !== 'local') {
    return res.redirect('/user-settings?profileError=Only+local+Launcharr+accounts+can+edit+profile+details.');
  }

  const config = loadConfig();
  const users = resolveLocalUsers(config);
  const index = findLocalUserIndex(users, {
    username: req.session?.user?.username,
    email: req.session?.user?.email,
  });
  if (index < 0) {
    return res.redirect('/user-settings?profileError=Local+account+record+was+not+found.');
  }

  const currentUser = users[index];
  const username = String(req.body?.username || '').trim();
  const email = String(req.body?.email || '').trim();
  const newPassword = String(req.body?.newPassword || '');
  const confirmPassword = String(req.body?.confirmPassword || '');
  const avatarDataUrl = String(req.body?.avatarDataUrl || '').trim();

  if (!username) {
    return res.redirect('/user-settings?profileError=Username+is+required.');
  }
  if (email && !isValidEmail(email)) {
    return res.redirect('/user-settings?profileError=A+valid+email+is+required.');
  }
  const profilePasswordError = newPassword
    ? validateLocalPasswordStrength(newPassword)
    : '';
  if (profilePasswordError) {
    return res.redirect(`/user-settings?profileError=${encodeURIComponent(profilePasswordError)}`);
  }
  if (newPassword && newPassword !== confirmPassword) {
    return res.redirect('/user-settings?profileError=Passwords+do+not+match.');
  }

  const nextUsernameKey = normalizeUserKey(username);
  const nextEmailKey = normalizeUserKey(email);
  const duplicate = users.find((entry, entryIndex) => {
    if (entryIndex === index) return false;
    const entryUsername = normalizeUserKey(entry?.username || '');
    const entryEmail = normalizeUserKey(entry?.email || '');
    if (entryUsername && nextUsernameKey && entryUsername === nextUsernameKey) return true;
    if (entryEmail && nextEmailKey && entryEmail === nextEmailKey) return true;
    return false;
  });
  if (duplicate) {
    return res.redirect('/user-settings?profileError=Username+or+email+is+already+in+use.');
  }

  const nextUser = {
    ...currentUser,
    username,
    email,
  };
  if (newPassword) {
    const salt = crypto.randomBytes(16).toString('hex');
    nextUser.salt = salt;
    nextUser.passwordHash = hashPassword(newPassword, salt);
  }
  if (avatarDataUrl) {
    const parsedAvatar = parseUserAvatarDataUrl(avatarDataUrl);
    if (!parsedAvatar.ok) {
      const encodedError = encodeURIComponent(parsedAvatar.error || 'Invalid avatar image.');
      return res.redirect(`/user-settings?profileError=${encodedError}`);
    }
    const savedAvatar = saveCustomUserAvatar(parsedAvatar.buffer, parsedAvatar.ext, `${username}-avatar`);
    if (!savedAvatar) {
      return res.redirect('/user-settings?profileError=Failed+to+save+avatar+image.');
    }
    const previousAvatar = normalizeStoredAvatarPath(currentUser.avatar || '');
    nextUser.avatar = savedAvatar;
    if (previousAvatar && previousAvatar !== savedAvatar) {
      deleteCustomIcon(previousAvatar, [USER_AVATAR_BASE]);
    }
  }

  const nextUsers = [...users];
  nextUsers[index] = nextUser;
  const userThemePreferences = resolveUserThemePreferences(config, resolveThemeDefaults(config));
  const previousThemeKey = resolveThemePreferenceKey({
    source: 'local',
    username: currentUser.username,
    email: currentUser.email,
  });
  const nextThemeKey = resolveThemePreferenceKey({
    source: 'local',
    username: nextUser.username,
    email: nextUser.email,
  });
  if (previousThemeKey && nextThemeKey && previousThemeKey !== nextThemeKey && userThemePreferences[previousThemeKey]) {
    if (!userThemePreferences[nextThemeKey]) {
      userThemePreferences[nextThemeKey] = userThemePreferences[previousThemeKey];
    }
    delete userThemePreferences[previousThemeKey];
  }
  saveConfig({
    ...config,
    users: serializeLocalUsers(nextUsers),
    userThemePreferences: serializeUserThemePreferences(userThemePreferences, resolveThemeDefaults(config)),
  });
  setSessionUser(req, nextUser, 'local');

  return res.redirect('/user-settings?profileResult=saved');
});

app.post('/user-settings/theme', requireUser, (req, res) => {
  try {
    const config = loadConfig();
    const userKey = resolveThemePreferenceKey(req.session?.user);
    if (!userKey) {
      return res.redirect('/user-settings?themeError=Unable+to+resolve+your+theme+profile.');
    }
    const defaults = resolveThemeDefaults(config);
    const userThemePreferences = resolveUserThemePreferences(config, defaults);
    userThemePreferences[userKey] = normalizeThemeSettings({
      mode: req.body?.theme_mode,
      brandTheme: req.body?.theme_brand_theme,
      customColor: req.body?.theme_custom_color,
      sidebarInvert: req.body?.theme_sidebar_invert,
      squareCorners: req.body?.theme_square_corners,
      bgMotion: req.body?.theme_bg_motion,
      carouselFreeScroll: req.body?.theme_carousel_free_scroll,
      hideScrollbars: req.body?.theme_hide_scrollbars,
    }, defaults);
    saveConfig({
      ...config,
      userThemePreferences: serializeUserThemePreferences(userThemePreferences, defaults),
    });
    return res.redirect('/user-settings?themeResult=saved');
  } catch (err) {
    const encoded = encodeURIComponent('Failed to save your theme preference.');
    return res.redirect(`/user-settings?themeError=${encoded}`);
  }
});

app.post('/settings/local-users', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const users = resolveLocalUsers(config);
  const username = String(req.body?.username || '').trim();
  const email = String(req.body?.email || '').trim();
  const password = String(req.body?.password || '');
  const role = normalizeLocalRole(req.body?.role, 'user');

  if (!username) return res.redirect('/settings?tab=user&localUsersError=Username+is+required.');
  if (email && !isValidEmail(email)) return res.redirect('/settings?tab=user&localUsersError=A+valid+email+is+required.');
  const localUserPasswordError = validateLocalPasswordStrength(password);
  if (localUserPasswordError) {
    return res.redirect(`/settings?tab=user&localUsersError=${encodeURIComponent(localUserPasswordError)}`);
  }

  const usernameKey = normalizeUserKey(username);
  const emailKey = normalizeUserKey(email);
  const duplicate = users.find((entry) => {
    const entryUsername = normalizeUserKey(entry?.username || '');
    const entryEmail = normalizeUserKey(entry?.email || '');
    if (entryUsername && usernameKey && entryUsername === usernameKey) return true;
    if (entryEmail && emailKey && entryEmail === emailKey) return true;
    return false;
  });
  if (duplicate) return res.redirect('/settings?tab=user&localUsersError=Username+or+email+already+exists.');

  const salt = crypto.randomBytes(16).toString('hex');
  const newUser = {
    username,
    email,
    role,
    salt,
    passwordHash: hashPassword(password, salt),
    avatar: '',
    createdBy: 'system',
    setupAccount: false,
    systemCreated: true,
    createdAt: new Date().toISOString(),
  };
  saveConfig({ ...config, users: serializeLocalUsers([...users, newUser]) });
  return res.redirect('/settings?tab=user&localUsersResult=added');
});

app.post('/settings/local-users/role', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const users = resolveLocalUsers(config);
  const username = String(req.body?.username || '').trim();
  const role = normalizeLocalRole(req.body?.role, 'user');
  if (!username) return res.redirect('/settings?tab=user&localUsersError=Missing+username.');
  const index = users.findIndex((entry) => normalizeUserKey(entry?.username || '') === normalizeUserKey(username));
  if (index < 0) return res.redirect('/settings?tab=user&localUsersError=Launcharr+user+not+found.');

  const currentSessionSource = String(req.session?.user?.source || '').trim().toLowerCase();
  const isCurrentSessionUser = currentSessionSource === 'local'
    && normalizeUserKey(req.session?.user?.username || '') === normalizeUserKey(users[index].username || '');
  if (isCurrentSessionUser && role !== 'admin') {
    return res.redirect('/settings?tab=user&localUsersError=You+cannot+change+your+current+session+role+away+from+admin.');
  }
  if (users[index].role === 'admin' && role !== 'admin') {
    const otherAdminExists = users.some((entry, entryIndex) => entryIndex !== index && entry.role === 'admin');
    if (!otherAdminExists) {
      return res.redirect('/settings?tab=user&localUsersError=At+least+one+local+admin+is+required.');
    }
  }

  const nextUsers = [...users];
  nextUsers[index] = { ...nextUsers[index], role };
  saveConfig({ ...config, users: serializeLocalUsers(nextUsers) });
  return res.redirect('/settings?tab=user&localUsersResult=role-saved');
});

app.post('/settings/local-users/delete', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const users = resolveLocalUsers(config);
  const username = String(req.body?.username || '').trim();
  if (!username) return res.redirect('/settings?tab=user&localUsersError=Missing+username.');

  const index = users.findIndex((entry) => normalizeUserKey(entry?.username || '') === normalizeUserKey(username));
  if (index < 0) return res.redirect('/settings?tab=user&localUsersError=Launcharr+user+not+found.');

  const targetUser = users[index];
  const isOwnerAccount = Boolean(
    targetUser?.isSetupAdmin
    || targetUser?.setupAccount === true
    || String(targetUser?.createdBy || '').trim().toLowerCase() === 'setup'
  );
  if (isOwnerAccount) {
    return res.redirect('/settings?tab=user&localUsersError=Owner+admin+account+cannot+be+deleted.');
  }
  if (targetUser?.systemCreated === false) {
    return res.redirect('/settings?tab=user&localUsersError=Only+system-generated+local+users+can+be+deleted.');
  }

  const currentSessionSource = String(req.session?.user?.source || '').trim().toLowerCase();
  const isCurrentSessionUser = currentSessionSource === 'local'
    && normalizeUserKey(req.session?.user?.username || '') === normalizeUserKey(targetUser.username || '');
  if (isCurrentSessionUser) {
    return res.redirect('/settings?tab=user&localUsersError=You+cannot+delete+your+current+session+account.');
  }

  const avatarPath = normalizeStoredAvatarPath(targetUser.avatar || '');
  if (avatarPath) {
    deleteCustomIcon(avatarPath, [USER_AVATAR_BASE]);
  }

  const nextUsers = users.filter((_entry, entryIndex) => entryIndex !== index);
  saveConfig({ ...config, users: serializeLocalUsers(nextUsers) });
  return res.redirect('/settings?tab=user&localUsersResult=removed');
});

// DEPRECATED: Legacy endpoint kept for compatibility with older clients.
// Remove in v0.3.0 after confirming no callers remain.
app.post('/user-settings/access', requireActualAdmin, (req, res) => {
  pushLog({
    level: 'warning',
    app: 'settings',
    action: 'user-settings.access.deprecated',
    message: 'Deprecated endpoint /user-settings/access was used. Remove in v0.3.0.',
  });
  const config = loadConfig();
  const generalSettings = resolveGeneralSettings(config);
  const restrictGuests = Boolean(req.body?.restrictGuests);
  const nextGeneral = {
    ...generalSettings,
    restrictGuests,
  };
  saveConfig({ ...config, general: nextGeneral });
  res.redirect('/settings?tab=user');
});

app.post('/settings/categories', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const parsedBody = String(req.body?.categories_json || '').trim();
  let requestedCategories = [];

  if (parsedBody) {
    try {
      const parsed = JSON.parse(parsedBody);
      if (Array.isArray(parsed)) requestedCategories = parsed;
    } catch (err) {
      requestedCategories = [];
    }
  }

  const categoryEntries = normalizeCategoryEntries(requestedCategories);
  const nextEntries = categoryEntries.length
    ? categoryEntries
    : loadDefaultCategories();
  const nextCategories = nextEntries.map((entry) => entry.name);
  const fallbackCategory = nextCategories.find((item) => item.toLowerCase() === 'tools')
    || nextCategories[0]
    || 'Tools';
  const categoryKeys = new Set(nextCategories.map((item) => item.toLowerCase()));
  const apps = (config.apps || []).map((appItem) => {
    const currentCategory = String(appItem?.category || '').trim().toLowerCase();
    if (currentCategory && categoryKeys.has(currentCategory)) return appItem;
    return {
      ...appItem,
      category: fallbackCategory,
    };
  });

  saveConfig({ ...config, categories: nextEntries, apps });
  res.redirect('/settings');
});

app.post('/settings/admins', requireSettingsAdmin, (req, res) => {
  const admins = parseCsv(req.body.admins || '');
  saveAdmins(admins);
  res.redirect('/settings');
});

app.post('/settings/apps', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const defaultAppIdSet = buildDefaultAppIdSet();
  const categoryOrder = resolveCategoryOrder(config, config.apps || [], { includeAppCategories: false });
  const categoryKeys = new Set(categoryOrder.map((item) => item.toLowerCase()));
  const fallbackCategory = categoryOrder.find((item) => item.toLowerCase() === 'utilities')
    || categoryOrder[0]
    || 'Tools';
  const apps = (config.apps || []).map((appItem) => {
    const id = appItem.id;
    const launchModeInput = req.body[`display_launch_mode_${id}`];
    const favouriteValue = Boolean(req.body[`display_favourite_${id}`]);
    const categoryValue = req.body[`display_category_${id}`];
    const orderValue = req.body[`display_order_${id}`];
    const parsedOrder = Number(orderValue);
    const currentMenu = normalizeMenu(appItem);
    const currentLaunchMode = resolveAppLaunchMode(appItem, currentMenu);
    const launchMode = normalizeLaunchMode(
      launchModeInput,
      currentLaunchMode === 'disabled' ? 'new-tab' : currentLaunchMode
    );
    const sidebarVisibilityRolesField = `display_sidebar_visibility_roles_${id}`;
    const sidebarVisibilityRolesPresentField = `display_sidebar_visibility_roles_present_${id}`;
    const hasSidebarVisibilityRolesField = Object.prototype.hasOwnProperty.call(req.body || {}, sidebarVisibilityRolesPresentField);
    const isCustom = isCustomAppRecord(appItem, defaultAppIdSet);
    const sidebarVisibilityRoles = hasSidebarVisibilityRolesField
      ? normalizeAppVisibilityRoles(req.body?.[sidebarVisibilityRolesField])
      : normalizeAppVisibilityRoles(
        currentMenu.sidebar?.visibilityRoles,
        normalizeVisibilityRole(
          req.body[`display_sidebar_min_role_${id}`],
          currentMenu.sidebar?.minRole || 'disabled'
        )
      );
    const overviewSidebarMinRole = isCustom
      ? 'disabled'
      : normalizeVisibilityRole(
        req.body[`display_overview_min_role_${id}`] || req.body[`display_dashboard_min_role_${id}`],
        currentMenu.sidebarOverview?.minRole || currentMenu.overview?.minRole || 'disabled'
      );
    const appSettingsSidebarMinRole = normalizeVisibilityRole(
      req.body[`display_app_settings_min_role_${id}`],
      currentMenu.sidebarSettings?.minRole || currentMenu.settings?.minRole || 'admin'
    );
    const activitySidebarMinRole = normalizeVisibilityRole(
      req.body[`display_activity_min_role_${id}`],
      currentMenu.sidebarActivity?.minRole || 'admin'
    );
    const launchMinRoleInput = normalizeVisibilityRole(
      req.body[`display_launch_min_role_${id}`],
      currentMenu.launch?.minRole || 'disabled'
    );
    const launchMinRole = launchMode === 'disabled' ? 'disabled' : launchMinRoleInput;
    const settingsMinRole = normalizeVisibilityRole(currentMenu.settings?.minRole || 'admin', 'admin');
    const menu = buildMenuAccessConfig({
      sidebar: sidebarVisibilityRoles,
      sidebarOverview: overviewSidebarMinRole,
      sidebarSettings: appSettingsSidebarMinRole,
      sidebarActivity: activitySidebarMinRole,
      overview: isCustom ? 'disabled' : overviewSidebarMinRole,
      launch: launchMinRole,
      settings: settingsMinRole,
    });
    const normalizedMenu = enforceSidebarAdminDefaults(menu);

    return {
      ...appItem,
      favourite: favouriteValue,
      category: categoryKeys.has(String(categoryValue || '').trim().toLowerCase())
        ? categoryValue
        : (categoryKeys.has(String(appItem.category || '').trim().toLowerCase()) ? appItem.category : fallbackCategory),
      order: Number.isFinite(parsedOrder) ? parsedOrder : appItem.order,
      launchMode,
      menu: normalizedMenu,
    };
  });
  const arrDashboardCombine = resolveArrDashboardCombineSettings(config, apps);
  ARR_COMBINE_SECTIONS.forEach((section) => {
    arrDashboardCombine[section.key] = arrDashboardCombine[section.key] || {};
    apps
      .filter((appItem) => isAppInSet(appItem.id, ARR_APP_IDS))
      .forEach((appItem) => {
        const field = `arr_combine_${section.key}_${appItem.id}`;
        arrDashboardCombine[section.key][appItem.id] = Boolean(req.body[field]);
      });
  });
  const downloaderDashboardCombine = resolveDownloaderDashboardCombineSettings(config, apps);
  DOWNLOADER_COMBINE_SECTIONS.forEach((section) => {
    downloaderDashboardCombine[section.key] = downloaderDashboardCombine[section.key] || {};
    apps
      .filter((appItem) => isAppInSet(appItem.id, DOWNLOADER_APP_IDS))
      .forEach((appItem) => {
        const field = `downloader_combine_${section.key}_${appItem.id}`;
        downloaderDashboardCombine[section.key][appItem.id] = Boolean(req.body[field]);
      });
  });
  const mediaDashboardCombine = resolveMediaDashboardCombineSettings(config, apps);
  MEDIA_COMBINE_SECTIONS.forEach((section) => {
    mediaDashboardCombine[section.key] = mediaDashboardCombine[section.key] || {};
    apps
      .filter((appItem) => isAppInSet(appItem.id, MEDIA_APP_IDS))
      .forEach((appItem) => {
        const field = `media_combine_${section.key}_${appItem.id}`;
        mediaDashboardCombine[section.key][appItem.id] = Boolean(req.body[field]);
      });
  });
  const arrCombinedQueueDisplay = resolveCombinedQueueDisplaySettings(config, 'arrCombinedQueueDisplay');
  arrCombinedQueueDisplay.queueShowDetail = Boolean(req.body.arr_combined_queue_col_detail);
  arrCombinedQueueDisplay.queueShowSubDetail = Boolean(req.body.arr_combined_queue_col_subdetail);
  arrCombinedQueueDisplay.queueShowSize = Boolean(req.body.arr_combined_queue_col_size);
  arrCombinedQueueDisplay.queueShowProtocol = Boolean(req.body.arr_combined_queue_col_protocol);
  arrCombinedQueueDisplay.queueShowTimeLeft = Boolean(req.body.arr_combined_queue_col_timeleft);
  arrCombinedQueueDisplay.queueShowProgress = Boolean(req.body.arr_combined_queue_col_progress);
  const arrQueueRows = Number(req.body.arr_combined_queue_visible_rows);
  if (Number.isFinite(arrQueueRows)) {
    arrCombinedQueueDisplay.queueVisibleRows = Math.max(5, Math.min(50, arrQueueRows));
  }

  const downloaderCombinedQueueDisplay = resolveCombinedQueueDisplaySettings(config, 'downloaderCombinedQueueDisplay');
  downloaderCombinedQueueDisplay.queueShowDetail = Boolean(req.body.downloader_combined_queue_col_detail);
  downloaderCombinedQueueDisplay.queueShowSubDetail = Boolean(req.body.downloader_combined_queue_col_subdetail);
  downloaderCombinedQueueDisplay.queueShowSize = Boolean(req.body.downloader_combined_queue_col_size);
  downloaderCombinedQueueDisplay.queueShowProtocol = Boolean(req.body.downloader_combined_queue_col_protocol);
  downloaderCombinedQueueDisplay.queueShowTimeLeft = Boolean(req.body.downloader_combined_queue_col_timeleft);
  downloaderCombinedQueueDisplay.queueShowProgress = Boolean(req.body.downloader_combined_queue_col_progress);
  const downloaderQueueRows = Number(req.body.downloader_combined_queue_visible_rows);
  if (Number.isFinite(downloaderQueueRows)) {
    downloaderCombinedQueueDisplay.queueVisibleRows = Math.max(5, Math.min(50, downloaderQueueRows));
  }

  saveConfig({
    ...config,
    apps,
    arrDashboardCombine,
    mediaDashboardCombine,
    downloaderDashboardCombine,
    arrCombinedQueueDisplay,
    downloaderCombinedQueueDisplay,
  });
  res.redirect('/settings');
});

app.post('/settings/apps/instances/add', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const apps = Array.isArray(config.apps) ? config.apps : [];
  const sourceId = normalizeAppId(req.body?.sourceId || '');
  const baseId = getAppBaseId(req.body?.baseId || sourceId);
  const maxInstances = getMaxMultiInstancesForBase(baseId, apps);
  const focusApp = sourceId || baseId;
  const redirectWithError = (message) => {
    const appParam = encodeURIComponent(String(focusApp || baseId || '').trim());
    const encodedMessage = encodeURIComponent(String(message || 'Unable to add instance.').trim());
    return res.redirect(`/settings?tab=app&app=${appParam}&appInstanceError=${encodedMessage}`);
  };

  if (!supportsAppInstances(baseId)) {
    return redirectWithError('Unsupported app for instances.');
  }

  const sameBaseApps = apps.filter((appItem) => getAppBaseId(appItem?.id) === baseId);
  if (sameBaseApps.length >= maxInstances) {
    return redirectWithError(`Maximum ${maxInstances} instances reached for ${getBaseAppTitle(baseId)}.`);
  }

  const nextId = buildNextInstanceId(baseId, apps);
  if (!nextId) {
    return redirectWithError(`Unable to allocate a new ${getBaseAppTitle(baseId)} instance id.`);
  }

  const sourceApp = sameBaseApps.find((appItem) => normalizeAppId(appItem?.id) === sourceId)
    || sameBaseApps[0]
    || loadDefaultApps().find((appItem) => normalizeAppId(appItem?.id) === baseId);
  if (!sourceApp) {
    return redirectWithError(`Missing ${getBaseAppTitle(baseId)} template app.`);
  }

  const category = String(sourceApp.category || 'Arr Suite').trim() || 'Arr Suite';
  const maxOrder = Math.max(
    0,
    ...apps
      .filter((appItem) => String(appItem?.category || '').trim().toLowerCase() === category.toLowerCase())
      .map((appItem) => Number(appItem?.order) || 0)
  );

  const newApp = {
    ...sourceApp,
    id: nextId,
    name: getDefaultInstanceName(baseId, nextId),
    instanceName: '',
    icon: resolvePersistedAppIconPath({ ...sourceApp, id: nextId }),
    localUrl: '',
    remoteUrl: '',
    url: '',
    apiKey: '',
    username: '',
    password: '',
    plexToken: '',
    plexMachine: '',
    order: maxOrder + 1,
    favourite: false,
  };
  if (Array.isArray(newApp.overviewElements) && newApp.overviewElements.length) {
    const activeAppsMaxOverviewOrder = Math.max(
      0,
      ...apps
        .filter((appItem) => !appItem?.removed)
        .flatMap((appItem) =>
          mergeOverviewElementSettings(appItem).map((entry) => Number(entry?.order))
        )
        .filter((value) => Number.isFinite(value))
    );
    const orderedOverviewElements = [...newApp.overviewElements]
      .map((entry) => ({ ...(entry || {}) }))
      .sort((left, right) => {
        const leftOrder = Number(left?.order);
        const rightOrder = Number(right?.order);
        if (Number.isFinite(leftOrder) && Number.isFinite(rightOrder)) return leftOrder - rightOrder;
        if (Number.isFinite(leftOrder)) return -1;
        if (Number.isFinite(rightOrder)) return 1;
        return String(left?.id || '').localeCompare(String(right?.id || ''));
      })
      .map((entry, index) => ({
        ...entry,
        order: activeAppsMaxOverviewOrder + index + 1,
      }));
    newApp.overviewElements = orderedOverviewElements;
  }

  const deprecatedElementIdSet = new Set(resolveDeprecatedDashboardElementIdsForApp(nextId));
  if (deprecatedElementIdSet.size && Array.isArray(newApp.overviewElements)) {
    newApp.overviewElements = newApp.overviewElements.map((entry) => {
      const elementId = String(entry?.id || '').trim();
      if (!deprecatedElementIdSet.has(elementId)) return entry;
      return {
        ...(entry || {}),
        id: elementId,
        dashboard: false,
        dashboardVisibilityRole: 'disabled',
      };
    });
  }
  const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
    ? { ...config.dashboardRemovedElements }
    : {};
  deprecatedElementIdSet.forEach((elementId) => {
    dashboardRemovedElements[`app:${nextId}:${elementId}`] = true;
  });

  saveConfig({ ...config, apps: [...apps, newApp], dashboardRemovedElements });
  const appParam = encodeURIComponent(nextId);
  return res.redirect(`/settings?tab=app&app=${appParam}&appInstanceResult=added`);
});

app.post('/settings/apps/instances/delete', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const apps = Array.isArray(config.apps) ? config.apps : [];
  const appId = normalizeAppId(req.body?.appId || '');
  const baseId = getAppBaseId(appId);
  const appParam = encodeURIComponent(baseId || appId || '');
  const redirectWithError = (message) => {
    const encodedMessage = encodeURIComponent(String(message || 'Unable to delete instance.').trim());
    return res.redirect(`/settings?tab=app&app=${appParam}&appInstanceError=${encodedMessage}`);
  };

  if (!appId || !supportsAppInstances(baseId)) {
    return redirectWithError('Unsupported app for instance deletion.');
  }

  const sameBaseApps = apps.filter((appItem) => getAppBaseId(appItem?.id) === baseId);
  if (!sameBaseApps.length) {
    return redirectWithError('Instance not found.');
  }

  if (getInstanceSuffix(appId, baseId) === 1) {
    return redirectWithError('The first instance cannot be deleted.');
  }

  if (!sameBaseApps.some((appItem) => normalizeAppId(appItem?.id) === appId)) {
    return redirectWithError('Instance not found.');
  }

  const nextApps = apps.filter((appItem) => normalizeAppId(appItem?.id) !== appId);
  saveConfig({ ...config, apps: nextApps });
  return res.redirect(`/settings?tab=app&app=${appParam}&appInstanceResult=deleted`);
});

app.post('/settings/default-apps/add', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const apps = Array.isArray(config.apps) ? config.apps : [];
  const defaultAppId = normalizeAppId(req.body?.default_app_id || req.body?.id || '');
  const requestedCategory = String(req.body?.default_app_category || req.body?.category || '').trim();
  const requestedSettingsTab = String(req.body?.settings_tab || '').trim().toLowerCase();
  const redirectBase = requestedSettingsTab === 'app'
    ? '/settings?tab=app&appCategory=general'
    : '/settings?tab=custom';
  const redirectWithError = (message) => {
    const encodedMessage = encodeURIComponent(String(message || 'Unable to add default app.').trim());
    return res.redirect(`${redirectBase}&defaultAppError=${encodedMessage}`);
  };

  if (!defaultAppId) {
    return redirectWithError('Select a default app to add.');
  }

  const existingApp = apps.find((appItem) => normalizeAppId(appItem?.id) === defaultAppId);
  const catalogTemplate = loadDefaultApps().find((appItem) => normalizeAppId(appItem?.id) === defaultAppId);
  const defaultTemplate = catalogTemplate
    || (canManageWithDefaultAppManager(existingApp) ? existingApp : null);
  if (!defaultTemplate) {
    return redirectWithError('Default app not found.');
  }

  const categoryOrder = resolveCategoryOrder(config, apps, { includeAppCategories: false });
  const categoryKeys = new Set(categoryOrder.map((item) => String(item || '').trim().toLowerCase()).filter(Boolean));
  const fallbackCategory = categoryOrder.find((item) => String(item || '').trim().toLowerCase() === 'tools')
    || categoryOrder[0]
    || 'Tools';
  const templateCategory = String(defaultTemplate?.category || '').trim();
  const resolvedCategory = categoryKeys.has(requestedCategory.toLowerCase())
    ? requestedCategory
    : (categoryKeys.has(templateCategory.toLowerCase()) ? templateCategory : fallbackCategory);

  const existingIndex = apps.findIndex((appItem) => normalizeAppId(appItem?.id) === defaultAppId);
  const baseTemplate = {
    ...defaultTemplate,
    id: defaultAppId,
    name: String(defaultTemplate?.name || '').trim() || getBaseAppTitle(getAppBaseId(defaultAppId)),
    icon: resolvePersistedAppIconPath({ ...defaultTemplate, id: defaultAppId }),
    category: resolvedCategory,
  };
  const current = existingIndex >= 0 ? apps[existingIndex] : null;
  const nextAppSeed = current ? { ...baseTemplate, ...current, id: defaultAppId } : baseTemplate;
  const removedStateBackup = (current?.removed && current.removedStateBackup && typeof current.removedStateBackup === 'object')
    ? current.removedStateBackup
    : null;
  const normalizedSeedMenu = normalizeMenu(nextAppSeed);
  const shouldRecoverLegacyRemovedMenu = Boolean(
    current?.removed
    && !removedStateBackup
    && normalizedSeedMenu.sidebar?.minRole === 'disabled'
    && normalizedSeedMenu.sidebarOverview?.minRole === 'disabled'
    && normalizedSeedMenu.sidebarSettings?.minRole === 'disabled'
    && normalizedSeedMenu.overview?.minRole === 'disabled'
    && normalizedSeedMenu.launch?.minRole === 'disabled'
    && normalizedSeedMenu.settings?.minRole === 'disabled'
  );
  const shouldRecoverLegacyRemovedOverview = Boolean(
    current?.removed
    && !removedStateBackup
    && deepEqual(nextAppSeed?.overviewElements, buildDisabledOverviewElements(nextAppSeed))
  );
  const recoveryTemplate = catalogTemplate || defaultTemplate || {};
  const legacyRecoveredMenu = shouldRecoverLegacyRemovedMenu
    ? normalizeMenu({
      ...nextAppSeed,
      custom: false,
      menu: recoveryTemplate?.menu,
    })
    : null;
  const legacyRecoveredOverviewElements = shouldRecoverLegacyRemovedOverview
    ? recoveryTemplate?.overviewElements
    : null;
  const legacyRecoveredLaunchMode = shouldRecoverLegacyRemovedMenu
    ? String(recoveryTemplate?.launchMode || '').trim()
    : '';
  const legacyRecoveredFavourite = shouldRecoverLegacyRemovedMenu
    ? Boolean(recoveryTemplate?.favourite || recoveryTemplate?.favorite)
    : null;
  const recoveredMenu = removedStateBackup?.menu
    || legacyRecoveredMenu
    || nextAppSeed.menu
    || buildDisabledMenuAccess();
  const recoveredOverviewElements = removedStateBackup?.overviewElements
    || legacyRecoveredOverviewElements
    || nextAppSeed.overviewElements;
  const recoveredLaunchMode = String(
    removedStateBackup?.launchMode
    || legacyRecoveredLaunchMode
    || nextAppSeed.launchMode
    || 'new-tab'
  ).trim() || 'new-tab';
  const recoveredFavourite = removedStateBackup
    ? Boolean(removedStateBackup.favourite)
    : Boolean(
      legacyRecoveredFavourite
      || nextAppSeed.favourite
      || nextAppSeed.favorite
    );
  const nextApp = {
    ...nextAppSeed,
    custom: false,
    removed: false,
    favourite: recoveredFavourite,
    launchMode: recoveredLaunchMode,
    menu: enforceSidebarAdminDefaults(recoveredMenu),
    overviewElements: Array.isArray(recoveredOverviewElements) && recoveredOverviewElements.length
      ? recoveredOverviewElements
      : buildDisabledOverviewElements(nextAppSeed),
  };
  const shouldAppendOverviewOrders = Boolean((existingIndex === -1 || current?.removed) && !removedStateBackup);
  if (shouldAppendOverviewOrders && Array.isArray(nextApp.overviewElements) && nextApp.overviewElements.length) {
    const activeAppsMaxOverviewOrder = Math.max(
      0,
      ...apps
        .filter((appItem) => !appItem?.removed && normalizeAppId(appItem?.id) !== defaultAppId)
        .flatMap((appItem) =>
          mergeOverviewElementSettings(appItem).map((entry) => Number(entry?.order))
        )
        .filter((value) => Number.isFinite(value))
    );
    const orderedOverviewElements = [...nextApp.overviewElements]
      .map((entry) => ({ ...(entry || {}) }))
      .sort((left, right) => {
        const leftOrder = Number(left?.order);
        const rightOrder = Number(right?.order);
        if (Number.isFinite(leftOrder) && Number.isFinite(rightOrder)) return leftOrder - rightOrder;
        if (Number.isFinite(leftOrder)) return -1;
        if (Number.isFinite(rightOrder)) return 1;
        return String(left?.id || '').localeCompare(String(right?.id || ''));
      })
      .map((entry, index) => ({
        ...entry,
        order: activeAppsMaxOverviewOrder + index + 1,
      }));
    nextApp.overviewElements = orderedOverviewElements;
  }
  delete nextApp.favorite;
  delete nextApp.removedStateBackup;
  const deprecatedElementIdSet = new Set(resolveDeprecatedDashboardElementIdsForApp(defaultAppId));
  if (deprecatedElementIdSet.size && Array.isArray(nextApp.overviewElements)) {
    nextApp.overviewElements = nextApp.overviewElements.map((entry) => {
      const elementId = String(entry?.id || '').trim();
      if (!deprecatedElementIdSet.has(elementId)) return entry;
      return {
        ...(entry || {}),
        id: elementId,
        dashboard: false,
        dashboardVisibilityRole: 'disabled',
      };
    });
  }
  const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
    ? { ...config.dashboardRemovedElements }
    : {};
  deprecatedElementIdSet.forEach((elementId) => {
    dashboardRemovedElements[`app:${defaultAppId}:${elementId}`] = true;
  });

  if (existingIndex >= 0) {
    const nextApps = [...apps];
    nextApps[existingIndex] = nextApp;
    saveConfig({ ...config, apps: nextApps, dashboardRemovedElements });
  } else {
    const category = String(nextApp.category || 'Tools').trim() || 'Tools';
    const maxOrder = Math.max(
      0,
      ...apps
        .filter((appItem) => String(appItem?.category || '').trim().toLowerCase() === category.toLowerCase())
        .map((appItem) => Number(appItem?.order) || 0)
    );
    saveConfig({ ...config, apps: [...apps, { ...nextApp, order: maxOrder + 1 }], dashboardRemovedElements });
  }

  return res.redirect(`${redirectBase}&defaultAppResult=added`);
});

app.post('/settings/default-apps/remove', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const apps = Array.isArray(config.apps) ? config.apps : [];
  const defaultCatalogIdSet = buildDefaultAppIdSet();
  const defaultAppId = normalizeAppId(req.body?.id || req.body?.default_app_id || '');
  const wantsJson = String(req.get('content-type') || '').toLowerCase().includes('application/json')
    || String(req.get('accept') || '').toLowerCase().includes('application/json');
  const replyError = (message, status = 400) => {
    if (wantsJson) return res.status(status).json({ error: String(message || 'Unable to remove default app.') });
    const encodedMessage = encodeURIComponent(String(message || 'Unable to remove default app.').trim());
    return res.redirect(`/settings?tab=custom&defaultAppError=${encodedMessage}`);
  };

  if (!defaultAppId) {
    return replyError('Missing default app id.');
  }

  const appIndex = apps.findIndex((appItem) => normalizeAppId(appItem?.id) === defaultAppId);
  if (appIndex === -1) {
    return replyError('Default app not found.', 404);
  }

  const current = apps[appIndex];
  const isDefaultCatalogApp = defaultCatalogIdSet.has(normalizeAppId(current?.id));
  if (isCustomAppRecord(current, defaultCatalogIdSet)) {
    return replyError('Custom apps must be removed with the custom app delete action.');
  }
  if (!canManageWithDefaultAppManager(current) && !isDefaultCatalogApp) {
    return replyError('Only built-in primary apps can be removed here.');
  }

  const removalBackup = {
    menu: current?.menu,
    overviewElements: current?.overviewElements,
    launchMode: String(current?.launchMode || '').trim() || 'new-tab',
    favourite: Boolean(current?.favourite || current?.favorite),
  };
  const nextApps = [...apps];
  nextApps[appIndex] = {
    ...current,
    removed: true,
    favourite: false,
    removedStateBackup: removalBackup,
  };
  saveConfig({ ...config, apps: nextApps });
  if (wantsJson) return res.json({ ok: true });
  return res.redirect('/settings?tab=custom&defaultAppResult=removed');
});

app.post('/settings/icons/upload', requireSettingsAdmin, (req, res) => {
  const iconType = String(req.body?.icon_type || '').trim().toLowerCase();
  const iconData = String(req.body?.icon_data || '').trim();
  const iconName = String(req.body?.icon_name || '').trim();
  const iconBase = iconName.replace(/\.[^/.]+$/, '').trim();
  if (!iconBase) {
    res.redirect('/settings?tab=custom&subtab=images&iconError=name-required');
    return;
  }
  if (!iconData) {
    res.redirect('/settings?tab=custom&subtab=images&iconError=image-required');
    return;
  }
  const targetDir = iconType === 'app'
    ? path.join(PUBLIC_DIR, 'icons', 'custom', 'apps')
    : path.join(PUBLIC_DIR, 'icons', 'custom', 'system');
  const result = saveCustomIcon(iconData, targetDir, iconBase);
  if (!result?.ok || !result?.iconPath) {
    const errorCodeMap = {
      'missing-icon-data': 'image-required',
      'invalid-data-url': 'image-invalid',
      'unsupported-mime': 'image-type',
      'invalid-icon-name': 'name-required',
      'decode-failed': 'image-invalid',
      'empty-image-data': 'image-invalid',
      'write-failed': 'write-failed',
    };
    const iconError = errorCodeMap[String(result?.error || '').trim()] || 'upload-failed';
    pushLog({
      level: 'error',
      app: 'settings',
      action: 'icons.upload',
      message: 'Failed to upload custom icon.',
      meta: {
        iconType,
        iconName: iconBase,
        error: String(result?.error || 'unknown').trim() || 'unknown',
        detail: String(result?.detail || '').trim(),
      },
    });
    res.redirect(`/settings?tab=custom&subtab=images&iconError=${encodeURIComponent(iconError)}`);
    return;
  }
  res.redirect('/settings?tab=custom&subtab=images&iconResult=uploaded');
});

app.post('/settings/icons/delete', requireSettingsAdmin, (req, res) => {
  const iconType = String(req.body?.icon_type || '').trim().toLowerCase();
  const iconPath = String(req.body?.icon_path || '').trim();
  const allowedBases = iconType === 'app'
    ? ['/icons/custom/apps', '/icons/custom']
    : ['/icons/custom/system'];
  deleteCustomIcon(iconPath, allowedBases);
  res.redirect('/settings?tab=custom&subtab=images');
});

app.post('/settings/general', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const currentGeneral = resolveGeneralSettings(config);
  const serverName = String(req.body?.server_name || '').trim();
  const remoteUrl = String(req.body?.remote_url || '').trim();
  const localUrl = String(req.body?.local_url || '').trim();
  const basePath = normalizeBasePath(req.body?.base_path || '');
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const hasField = (fieldName) => Object.prototype.hasOwnProperty.call(body, fieldName);
  const restrictGuests = hasField('restrictGuests')
    ? Boolean(body.restrictGuests)
    : Boolean(currentGeneral.restrictGuests);
  const autoOpenSingleAppMenuItem = hasField('autoOpenSingleAppMenuItem')
    ? Boolean(body.autoOpenSingleAppMenuItem)
    : Boolean(currentGeneral.autoOpenSingleAppMenuItem);
  const currentPressActions = normalizeSidebarButtonPressActions(currentGeneral.sidebarButtonPressActions);
  const parseRolePressActions = (roleKey) => ({
    short: normalizeSidebarAppButtonAction(
      hasField(`sidebar_button_short_press_action_${roleKey}`)
        ? body[`sidebar_button_short_press_action_${roleKey}`]
        : currentPressActions?.[roleKey]?.short,
      currentPressActions?.[roleKey]?.short || 'default'
    ),
    long: normalizeSidebarAppButtonAction(
      hasField(`sidebar_button_long_press_action_${roleKey}`)
        ? body[`sidebar_button_long_press_action_${roleKey}`]
        : currentPressActions?.[roleKey]?.long,
      currentPressActions?.[roleKey]?.long || 'default'
    ),
  });
  const sidebarButtonPressActions = normalizeSidebarButtonPressActions({
    guest: parseRolePressActions('guest'),
    user: parseRolePressActions('user'),
    'co-admin': parseRolePressActions('co-admin'),
    admin: parseRolePressActions('admin'),
  });
  const hideSidebarAppSettingsLink = hasField('hideSidebarAppSettingsLink')
    ? Boolean(body.hideSidebarAppSettingsLink)
    : Boolean(currentGeneral.hideSidebarAppSettingsLink);
  const hideSidebarActivityLink = hasField('hideSidebarActivityLink')
    ? Boolean(body.hideSidebarActivityLink)
    : Boolean(currentGeneral.hideSidebarActivityLink);
  const nextGeneral = {
    serverName: serverName || DEFAULT_GENERAL_SETTINGS.serverName,
    remoteUrl,
    localUrl,
    basePath,
    restrictGuests,
    autoOpenSingleAppMenuItem,
    sidebarButtonShortPressAction: sidebarButtonPressActions.user.short,
    sidebarButtonLongPressAction: sidebarButtonPressActions.user.long,
    sidebarButtonPressActions,
    hideSidebarAppSettingsLink,
    hideSidebarActivityLink,
  };
  saveConfig({ ...config, general: nextGeneral });
  res.redirect('/settings');
});

app.post('/settings/apps/general', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const currentGeneral = resolveGeneralSettings(config);
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const hasField = (fieldName) => Object.prototype.hasOwnProperty.call(body, fieldName);
  const currentPressActions = normalizeSidebarButtonPressActions(currentGeneral.sidebarButtonPressActions);
  const parseRolePressActions = (roleKey) => ({
    short: normalizeSidebarAppButtonAction(
      hasField(`sidebar_button_short_press_action_${roleKey}`)
        ? body[`sidebar_button_short_press_action_${roleKey}`]
        : currentPressActions?.[roleKey]?.short,
      currentPressActions?.[roleKey]?.short || 'default'
    ),
    long: normalizeSidebarAppButtonAction(
      hasField(`sidebar_button_long_press_action_${roleKey}`)
        ? body[`sidebar_button_long_press_action_${roleKey}`]
        : currentPressActions?.[roleKey]?.long,
      currentPressActions?.[roleKey]?.long || 'default'
    ),
  });
  const sidebarButtonPressActions = normalizeSidebarButtonPressActions({
    guest: parseRolePressActions('guest'),
    user: parseRolePressActions('user'),
    'co-admin': parseRolePressActions('co-admin'),
    admin: parseRolePressActions('admin'),
  });
  const autoOpenSingleAppMenuItem = hasField('autoOpenSingleAppMenuItem')
    ? Boolean(body.autoOpenSingleAppMenuItem)
    : Boolean(currentGeneral.autoOpenSingleAppMenuItem);
  const hideSidebarAppSettingsLink = hasField('hideSidebarAppSettingsLink')
    ? Boolean(body.hideSidebarAppSettingsLink)
    : Boolean(currentGeneral.hideSidebarAppSettingsLink);
  const hideSidebarActivityLink = hasField('hideSidebarActivityLink')
    ? Boolean(body.hideSidebarActivityLink)
    : Boolean(currentGeneral.hideSidebarActivityLink);

  const nextGeneral = {
    ...currentGeneral,
    autoOpenSingleAppMenuItem,
    sidebarButtonShortPressAction: sidebarButtonPressActions.user.short,
    sidebarButtonLongPressAction: sidebarButtonPressActions.user.long,
    sidebarButtonPressActions,
    hideSidebarAppSettingsLink,
    hideSidebarActivityLink,
  };
  saveConfig({ ...config, general: nextGeneral });
  res.redirect('/settings?tab=app&appCategory=general&appGeneralResult=saved');
});

function buildNotificationSettingsFromBody(body, currentSettings = DEFAULT_NOTIFICATION_SETTINGS) {
  const fallback = currentSettings || DEFAULT_NOTIFICATION_SETTINGS;
  const rawMode = String(body?.apprise_mode || fallback.appriseMode || '').trim().toLowerCase();
  const rawDelaySeconds = Number(body?.widget_status_delay_seconds ?? fallback.widgetStatusDelaySeconds);
  const rawPollSeconds = Number(body?.widget_status_poll_seconds ?? fallback.widgetStatusPollSeconds);
  const rawRequestTimeoutMs = Number(body?.widget_status_request_timeout_ms ?? fallback.widgetStatusRequestTimeoutMs);
  const rawMaxConcurrency = Number(body?.widget_status_max_concurrency ?? fallback.widgetStatusMaxConcurrency);
  const widgetStatusDelaySeconds = Number.isFinite(rawDelaySeconds)
    ? Math.max(5, Math.min(3600, Math.round(rawDelaySeconds)))
    : (Number(fallback.widgetStatusDelaySeconds) || DEFAULT_NOTIFICATION_SETTINGS.widgetStatusDelaySeconds);
  const widgetStatusPollSeconds = Number.isFinite(rawPollSeconds)
    ? Math.max(15, Math.min(600, Math.round(rawPollSeconds)))
    : (Number(fallback.widgetStatusPollSeconds) || DEFAULT_NOTIFICATION_SETTINGS.widgetStatusPollSeconds);
  const widgetStatusRequestTimeoutMs = Number.isFinite(rawRequestTimeoutMs)
    ? Math.max(1000, Math.min(20000, Math.round(rawRequestTimeoutMs)))
    : (Number(fallback.widgetStatusRequestTimeoutMs) || DEFAULT_NOTIFICATION_SETTINGS.widgetStatusRequestTimeoutMs);
  const widgetStatusMaxConcurrency = Number.isFinite(rawMaxConcurrency)
    ? Math.max(1, Math.min(10, Math.round(rawMaxConcurrency)))
    : (Number(fallback.widgetStatusMaxConcurrency) || DEFAULT_NOTIFICATION_SETTINGS.widgetStatusMaxConcurrency);
  return {
    appriseEnabled: Boolean(body?.apprise_enabled),
    appriseApiUrl: String(body?.apprise_api_url || fallback.appriseApiUrl || '').trim(),
    appriseMode: rawMode === 'config-key' ? 'config-key' : 'targets',
    appriseConfigKey: String(body?.apprise_config_key || fallback.appriseConfigKey || '').trim(),
    appriseTargets: String(body?.apprise_targets || fallback.appriseTargets || '').trim(),
    appriseTag: String(body?.apprise_tag || fallback.appriseTag || '').trim(),
    widgetStatusEnabled: Boolean(body?.widget_status_enabled),
    widgetStatusDelaySeconds,
    widgetStatusPollSeconds,
    widgetStatusRequestTimeoutMs,
    widgetStatusMaxConcurrency,
  };
}

app.post('/settings/notifications', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const currentSettings = resolveNotificationSettings(config);
  const nextSettings = buildNotificationSettingsFromBody(req.body, currentSettings);
  saveConfig({ ...config, notifications: nextSettings });
  res.redirect('/settings?tab=notifications&notificationResult=saved');
});

app.post('/settings/notifications/test', requireSettingsAdmin, async (req, res) => {
  const config = loadConfig();
  const currentSettings = resolveNotificationSettings(config);
  const nextSettings = buildNotificationSettingsFromBody(req.body, currentSettings);
  saveConfig({ ...config, notifications: nextSettings });

  try {
    const timestamp = new Date().toISOString().replace('T', ' ').slice(0, 19);
    await sendAppriseNotification(nextSettings, {
      title: 'Launcharr test notification',
      body: `Launcharr sent this Apprise test notification at ${timestamp}.`,
      tag: nextSettings.appriseTag,
    });
    pushLog({
      level: 'info',
      app: 'settings',
      action: 'notifications.apprise.test',
      message: 'Apprise test notification sent successfully.',
    });
    res.redirect('/settings?tab=notifications&notificationResult=test-ok');
  } catch (err) {
    const errorMessage = String(err?.message || 'Unknown Apprise error.').trim();
    pushLog({
      level: 'error',
      app: 'settings',
      action: 'notifications.apprise.test',
      message: 'Apprise test notification failed.',
      meta: { error: errorMessage },
    });
    const encoded = encodeURIComponent(errorMessage.slice(0, 240));
    res.redirect(`/settings?tab=notifications&notificationResult=test-error&notificationError=${encoded}`);
  }
});

app.post('/settings/custom-apps', requireSettingsAdmin, (req, res) => {
  try {
    const config = loadConfig();
    const name = String(req.body?.name || '').trim();
    const category = String(req.body?.category || '').trim();
    const iconData = String(req.body?.iconData || '').trim();
    const iconPath = String(req.body?.iconPath || '').trim();
    if (!name) return res.status(400).json({ error: 'Missing app name.' });

    const categoryOrder = resolveCategoryOrder(config, config.apps || [], { includeAppCategories: false });
    const categoryKeys = new Set(categoryOrder.map((item) => String(item || '').toLowerCase()).filter(Boolean));
    const fallbackCategory = categoryOrder.find((item) => String(item || '').toLowerCase() === 'utilities')
      || categoryOrder[0]
      || 'Tools';
    const normalizedCategory = category.toLowerCase();
    const resolvedCategory = categoryKeys.has(normalizedCategory) ? category : fallbackCategory;

    const slug = slugifyId(name) || 'custom-app';
    const id = `custom-${slug}-${crypto.randomBytes(3).toString('hex')}`;
    let iconValue = '';
    if (iconPath) {
      iconValue = iconPath;
    } else if (iconData) {
      const iconResult = saveCustomAppIcon(iconData, id, name);
      iconValue = iconResult.iconPath || iconResult.iconData || '';
    }

    const apps = Array.isArray(config.apps) ? config.apps : [];
    const maxOrder = Math.max(0, ...apps.filter((app) => app.category === resolvedCategory).map((app) => Number(app.order) || 0));
    const appItem = {
      id,
      name,
      category: resolvedCategory,
      order: maxOrder + 1,
      favourite: false,
      custom: true,
      icon: iconValue,
      url: '',
      localUrl: '',
      remoteUrl: '',
      apiKey: '',
      menu: buildMenuAccessConfig({
        sidebar: 'user',
        overview: 'disabled',
        launch: 'user',
        settings: 'admin',
      }),
      launchMode: 'new-tab',
    };

    saveConfig({ ...config, apps: [...apps, appItem] });
    res.json({ ok: true, app: appItem });
  } catch (err) {
    const errorMessage = safeMessage(err, 'Failed to add custom app.');
    pushLog({
      level: 'error',
      app: 'settings',
      action: 'custom-apps.add',
      message: 'Failed to add custom app.',
      meta: { error: errorMessage },
    });
    res.status(500).json({ error: errorMessage });
  }
});

app.post('/settings/custom-apps/delete', requireSettingsAdmin, (req, res) => {
  try {
    const config = loadConfig();
    const defaultAppIdSet = buildDefaultAppIdSet();
    const id = String(req.body?.id || '').trim();
    if (!id) return res.status(400).json({ error: 'Missing app id.' });

    const apps = Array.isArray(config.apps) ? config.apps : [];
    const appItem = apps.find((app) => app.id === id);
    if (!appItem || !isCustomAppRecord(appItem, defaultAppIdSet)) return res.status(404).json({ error: 'Custom app not found.' });

    if (appItem.icon && appItem.icon.startsWith('/icons/custom/')) {
      const iconPath = path.join(PUBLIC_DIR, appItem.icon.replace(/^\/+/, ''));
      if (fs.existsSync(iconPath)) {
        try {
          fs.unlinkSync(iconPath);
        } catch (err) {
          // ignore delete errors
        }
      }
    }

    saveConfig({ ...config, apps: apps.filter((app) => app.id !== id) });
    res.json({ ok: true });
  } catch (err) {
    const errorMessage = safeMessage(err, 'Failed to delete custom app.');
    pushLog({
      level: 'error',
      app: 'settings',
      action: 'custom-apps.delete',
      message: 'Failed to delete custom app.',
      meta: { error: errorMessage },
    });
    res.status(500).json({ error: errorMessage });
  }
});

app.post('/settings/custom-apps/update', requireSettingsAdmin, (req, res) => {
  try {
    const config = loadConfig();
    const defaultAppIdSet = buildDefaultAppIdSet();
    const id = String(req.body?.id || '').trim();
    const name = String(req.body?.name || '').trim();
    const category = String(req.body?.category || '').trim();
    const iconData = String(req.body?.iconData || '').trim();
    const iconPath = String(req.body?.iconPath || '').trim();
    if (!id) return res.status(400).json({ error: 'Missing app id.' });
    if (!name) return res.status(400).json({ error: 'Missing app name.' });

    const apps = Array.isArray(config.apps) ? config.apps : [];
    const appIndex = apps.findIndex((app) => app.id === id && isCustomAppRecord(app, defaultAppIdSet));
    if (appIndex === -1) return res.status(404).json({ error: 'Custom app not found.' });

    const categoryOrder = resolveCategoryOrder(config, apps, { includeAppCategories: false });
    const categoryKeys = new Set(categoryOrder.map((item) => String(item || '').toLowerCase()).filter(Boolean));
    const fallbackCategory = categoryOrder.find((item) => String(item || '').toLowerCase() === 'utilities')
      || categoryOrder[0]
      || 'Tools';
    const normalizedCategory = category.toLowerCase();
    const resolvedCategory = categoryKeys.has(normalizedCategory) ? category : fallbackCategory;

    const current = apps[appIndex];
    let iconValue = current.icon || '';
    if (iconPath) {
      if (iconValue.startsWith('/icons/custom/')) {
        const iconFile = path.join(PUBLIC_DIR, iconValue.replace(/^\/+/, ''));
        if (fs.existsSync(iconFile)) {
          try {
            fs.unlinkSync(iconFile);
          } catch (err) {
            // ignore delete errors
          }
        }
      }
      iconValue = iconPath;
    } else if (iconData) {
      if (iconValue.startsWith('/icons/custom/')) {
        const previousIconPath = path.join(PUBLIC_DIR, iconValue.replace(/^\/+/, ''));
        if (fs.existsSync(previousIconPath)) {
          try {
            fs.unlinkSync(previousIconPath);
          } catch (err) {
            // ignore delete errors
          }
        }
      }
      const iconResult = saveCustomAppIcon(iconData, id, name);
      iconValue = iconResult.iconPath || iconResult.iconData || '';
    }

    const nextApp = {
      ...current,
      name,
      category: resolvedCategory,
      icon: iconValue,
    };
    const nextApps = [...apps];
    nextApps[appIndex] = nextApp;
    saveConfig({ ...config, apps: nextApps });
    res.json({ ok: true, app: nextApp });
  } catch (err) {
    const errorMessage = safeMessage(err, 'Failed to update custom app.');
    pushLog({
      level: 'error',
      app: 'settings',
      action: 'custom-apps.update',
      message: 'Failed to update custom app.',
      meta: { error: errorMessage },
    });
    res.status(500).json({ error: errorMessage });
  }
});

app.post('/settings/logs', requireSettingsAdmin, (req, res) => {
  const config = loadConfig();
  const maxEntries = Number(req.body?.log_max_entries);
  const maxDays = Number(req.body?.log_max_days);
  const visibleRows = Number(req.body?.log_visible_rows);
  const nextSettings = {
    maxEntries: Number.isFinite(maxEntries) && maxEntries > 0 ? Math.floor(maxEntries) : DEFAULT_LOG_SETTINGS.maxEntries,
    maxDays: Number.isFinite(maxDays) && maxDays > 0 ? Math.floor(maxDays) : DEFAULT_LOG_SETTINGS.maxDays,
    visibleRows: Number.isFinite(visibleRows) && visibleRows > 0 ? Math.floor(visibleRows) : DEFAULT_LOG_SETTINGS.visibleRows,
  };
  const nextConfig = { ...config, logs: nextSettings };
  saveConfig(nextConfig);
  const pruned = applyLogRetention(LOG_BUFFER, nextSettings);
  LOG_BUFFER.splice(0, LOG_BUFFER.length, ...pruned);
  persistLogsToDisk(nextSettings);
  res.redirect('/settings');
});

app.get('/settings/plex-users', requireSettingsAdmin, async (req, res) => {
  const config = loadConfig();
  const plexApp = Array.isArray(config.apps)
    ? config.apps.find((appItem) => normalizeAppId(appItem?.id) === 'plex')
    : null;
  const admins = loadAdmins();
  const ownerKey = admins[0] ? String(admins[0]).toLowerCase() : '';
  const identifiers = [
    req.session?.user?.username,
    req.session?.user?.email,
  ].filter(Boolean).map((value) => String(value).toLowerCase());
  const isOwner = ownerKey ? identifiers.includes(ownerKey) : true;
  const token = String((isOwner ? req.session?.authToken : '') || plexApp?.plexToken || '').trim();
  if (!token) return res.status(401).json({ error: 'Missing Plex token.' });

  try {
    pushLog({
      level: 'info',
      app: 'plex',
      action: 'users',
      message: 'Fetching Plex users.',
    });
    const url = `https://plex.tv/api/users?X-Plex-Token=${encodeURIComponent(token)}`;
    const plexRes = await fetch(url, {
      headers: {
        ...plexHeaders(),
        Accept: 'application/xml',
      },
    });
    const xmlText = await plexRes.text();
    if (!plexRes.ok) {
      return res.status(plexRes.status).json({ error: 'Failed to fetch Plex users.' });
    }

    const machineId = String(plexApp?.plexMachine || '').trim();
    const users = parsePlexUsers(xmlText, { machineId });
    const coAdmins = loadCoAdmins();
    const loginStore = resolveUserLogins(config);
    let plexHistory = {};
    if (plexApp) {
      const sessionServerToken = String(req.session?.plexServerToken || '').trim();
      let serverToken = sessionServerToken || String(plexApp.plexToken || '').trim();
      if (!serverToken) {
        const sessionToken = String(req.session?.authToken || '').trim();
        if (sessionToken) {
          try {
            const resources = await fetchPlexResources(sessionToken);
            serverToken = resolvePlexServerToken(resources, {
              machineId,
              localUrl: plexApp?.localUrl,
              remoteUrl: plexApp?.remoteUrl,
              plexHost: plexApp?.plexHost,
            }) || '';
          } catch (err) {
            serverToken = '';
          }
        }
      }
      if (serverToken) {
        const candidates = uniqueList([
          normalizeBaseUrl(resolveLaunchUrl(plexApp, req), { stripWeb: true }),
          normalizeBaseUrl(plexApp.localUrl || '', { stripWeb: true }),
          normalizeBaseUrl(plexApp.remoteUrl || '', { stripWeb: true }),
          normalizeBaseUrl(plexApp.url || '', { stripWeb: true }),
        ]).filter(Boolean);
        for (let index = 0; index < candidates.length; index += 1) {
          const baseUrl = candidates[index];
          plexHistory = await fetchPlexHistoryLastSeenMap(baseUrl, serverToken);
          if (Object.keys(plexHistory).length) break;
        }
      }
    }
    const hasPlexHistory = Object.keys(plexHistory).length > 0;

    const payload = users.map((user) => {
      const name = user.title || user.username || user.email || 'Plex User';
      const identifier = user.email || user.username || user.title || user.id || name;
      const identLower = normalizeUserKey(identifier);
      const historySeen = plexHistory[String(user.id || '').trim()]
        || plexHistory[String(user.uuid || '').trim()]
        || plexHistory[identLower]
        || '';
      const lastPlexSeen = hasPlexHistory
        ? historySeen
        : (normalizePlexLastSeen(user.lastSeenAt) || loginStore.plex?.[identLower] || '');
      const lastLauncharrLogin = loginStore.launcharr?.[identLower] || '';
      let role = 'user';
      let locked = false;

      if (ownerKey && identLower === ownerKey) {
        role = 'admin';
        locked = true;
      } else if (admins.some((admin) => String(admin).toLowerCase() === identLower)) {
        role = 'admin';
      } else if (coAdmins.some((coAdmin) => String(coAdmin).toLowerCase() === identLower)) {
        role = 'co-admin';
      }

      return {
        id: user.id || user.uuid || identifier,
        name,
        username: user.username || '',
        email: user.email || '',
        identifier,
        lastPlexSeen,
        lastLauncharrLogin,
        role,
        locked,
      };
    });

    pushLog({
      level: 'info',
      app: 'plex',
      action: 'users',
      message: 'Plex users loaded.',
      meta: { count: payload.length },
    });
    return res.json({ users: payload });
  } catch (err) {
    pushLog({
      level: 'error',
      app: 'plex',
      action: 'users',
      message: safeMessage(err) || 'Failed to fetch Plex users.',
    });
    return res.status(500).json({ error: safeMessage(err) });
  }
});

app.post('/settings/roles', requireSettingsAdmin, (req, res) => {
  const roles = Array.isArray(req.body?.roles) ? req.body.roles : [];
  const admins = loadAdmins();
  const owner = admins[0] ? String(admins[0]) : '';
  const ownerKey = owner.toLowerCase();
  const nextAdmins = owner ? [owner] : [];
  const nextCoAdmins = [];

  roles.forEach((entry) => {
    const identifier = String(entry?.identifier || '').trim();
    if (!identifier) return;
    const role = String(entry?.role || 'user').toLowerCase();
    if (ownerKey && identifier.toLowerCase() === ownerKey) return;
    if (role === 'admin') nextAdmins.push(identifier);
    if (role === 'co-admin') nextCoAdmins.push(identifier);
  });

  saveAdmins(uniqueList(nextAdmins));
  saveCoAdmins(uniqueList(nextCoAdmins));

  res.json({ ok: true });
});


app.post('/apps/:id/settings', requireAdmin, (req, res) => {
  const config = loadConfig();
  const defaultAppIdSet = buildDefaultAppIdSet();
  const shouldUpdateOverviewElements = Boolean(req.body.overviewElementsForm);
  const shouldUpdateTautulliCards = Boolean(req.body.tautulliCardsForm);
  const isDisplayOnlyUpdate = shouldUpdateOverviewElements || shouldUpdateTautulliCards;
  const hasOwnBodyField = (field) => Object.prototype.hasOwnProperty.call(req.body || {}, field);
  const hasVisibilityUpdate = (
    hasOwnBodyField('app_visibility_roles_launch_present')
    || hasOwnBodyField('app_visibility_roles_overview_present')
    || hasOwnBodyField('app_visibility_roles_settings_present')
    || hasOwnBodyField('app_visibility_roles_activity_present')
  );
  const resolveVisibilityRolesFromBody = (field, currentRoles) => {
    if (!hasOwnBodyField(`${field}_present`)) return normalizeAppVisibilityRoles(currentRoles);
    return normalizeAppVisibilityRoles(req.body?.[field]);
  };
  const plexAdminUser = String(req.body?.plexAdminUser || '').trim();
  const shouldIgnoreJwtToken = (value) => {
    const raw = String(value || '').trim();
    return raw && raw.split('.').length >= 3;
  };
  const apps = (config.apps || []).map((appItem) => {
    if (appItem.id !== req.params.id) return appItem;
    const baseId = getAppBaseId(appItem.id);
    const supportsInstances = supportsAppInstances(baseId);
    const nextInstanceName = supportsInstances && !isDisplayOnlyUpdate
      ? String(req.body?.instanceName || '').trim()
      : String(appItem.instanceName || '').trim();
    const nextName = supportsInstances
      ? (nextInstanceName || getDefaultInstanceName(baseId, appItem.id))
      : String(appItem.name || '').trim();
    const overviewElements = shouldUpdateOverviewElements
      ? buildOverviewElementsFromRequest(appItem, req.body)
      : appItem.overviewElements;
    const tautulliCards = shouldUpdateTautulliCards
      ? buildTautulliCardsFromRequest(appItem, req.body)
      : appItem.tautulliCards;
    const currentMenu = normalizeMenu(appItem);
    const isCustomApp = isCustomAppRecord(appItem, defaultAppIdSet);
    const currentLaunchRoles = normalizeAppVisibilityRoles(currentMenu?.launch?.visibilityRoles, currentMenu?.launch?.minRole);
    const currentOverviewRoles = normalizeAppVisibilityRoles(currentMenu?.overview?.visibilityRoles, currentMenu?.overview?.minRole);
    const currentSidebarSettingsRoles = normalizeAppVisibilityRoles(
      currentMenu?.sidebarSettings?.visibilityRoles,
      currentMenu?.sidebarSettings?.minRole || currentMenu?.settings?.minRole || 'admin'
    );
    const currentActivityRoles = normalizeAppVisibilityRoles(currentMenu?.sidebarActivity?.visibilityRoles, currentMenu?.sidebarActivity?.minRole);
    const nextLaunchRoles = resolveVisibilityRolesFromBody('app_visibility_roles_launch', currentLaunchRoles);
    const nextOverviewRoles = isCustomApp
      ? []
      : resolveVisibilityRolesFromBody('app_visibility_roles_overview', currentOverviewRoles);
    const nextSidebarSettingsRoles = resolveVisibilityRolesFromBody('app_visibility_roles_settings', currentSidebarSettingsRoles)
      .filter((role) => APP_SETTINGS_AND_ACTIVITY_ALLOWED_ROLES.has(role));
    const nextActivityRoles = resolveVisibilityRolesFromBody('app_visibility_roles_activity', currentActivityRoles)
      .filter((role) => APP_SETTINGS_AND_ACTIVITY_ALLOWED_ROLES.has(role));
    const nextMenu = hasVisibilityUpdate
      ? {
        ...currentMenu,
        launch: buildMenuRoleSection(currentMenu?.launch, nextLaunchRoles, { fallbackMinRole: 'disabled', includeLegacyFlags: true }),
        overview: buildMenuRoleSection(currentMenu?.overview, nextOverviewRoles, { fallbackMinRole: 'disabled', includeLegacyFlags: true }),
        sidebarOverview: buildMenuRoleSection(currentMenu?.sidebarOverview, nextOverviewRoles, { fallbackMinRole: 'disabled' }),
        sidebarSettings: buildMenuRoleSection(currentMenu?.sidebarSettings, nextSidebarSettingsRoles, { fallbackMinRole: 'admin' }),
        sidebarActivity: buildMenuRoleSection(currentMenu?.sidebarActivity, nextActivityRoles, { fallbackMinRole: 'admin' }),
      }
      : appItem.menu;
    return {
      ...appItem,
      name: nextName || appItem.name || getBaseAppTitle(baseId),
      instanceName: supportsInstances ? nextInstanceName : '',
      icon: resolvePersistedAppIconPath(appItem),
      localUrl: isDisplayOnlyUpdate
        ? appItem.localUrl || ''
        : (req.body.localUrl !== undefined ? req.body.localUrl : (appItem.localUrl || '')),
      remoteUrl: isDisplayOnlyUpdate
        ? appItem.remoteUrl || ''
        : (req.body.remoteUrl !== undefined ? req.body.remoteUrl : (appItem.remoteUrl || '')),
      apiKey: isDisplayOnlyUpdate
        ? appItem.apiKey || ''
        : (req.body.apiKey !== undefined ? req.body.apiKey : (appItem.apiKey || '')),
      username: isDisplayOnlyUpdate
        ? appItem.username || ''
        : (req.body.username !== undefined ? req.body.username : (appItem.username || '')),
      password: isDisplayOnlyUpdate
        ? appItem.password || ''
        : (req.body.password !== undefined ? req.body.password : (appItem.password || '')),
      viewerUsername: isDisplayOnlyUpdate
        ? appItem.viewerUsername || ''
        : (req.body.viewerUsername !== undefined ? req.body.viewerUsername : (appItem.viewerUsername || '')),
      viewerPassword: isDisplayOnlyUpdate
        ? appItem.viewerPassword || ''
        : (req.body.viewerPassword !== undefined ? req.body.viewerPassword : (appItem.viewerPassword || '')),
      rommRecentProbeLimit: isDisplayOnlyUpdate
        ? (appItem.rommRecentProbeLimit ?? '')
        : (req.body.rommRecentProbeLimit !== undefined ? req.body.rommRecentProbeLimit : (appItem.rommRecentProbeLimit ?? '')),
      uptimeKumaSlug: isDisplayOnlyUpdate
        ? (appItem.uptimeKumaSlug ?? '')
        : (req.body.uptimeKumaSlug !== undefined ? req.body.uptimeKumaSlug : (appItem.uptimeKumaSlug ?? '')),
      plexToken: (() => {
        if (isDisplayOnlyUpdate) return appItem.plexToken || '';
        const nextToken = req.body.plexToken !== undefined ? req.body.plexToken : (appItem.plexToken || '');
        if (appItem.id === 'plex' && shouldIgnoreJwtToken(nextToken)) {
          pushLog({
            level: 'error',
            app: 'plex',
            action: 'token.save',
            message: 'Rejected Plex auth JWT. Server token required.',
          });
          return appItem.plexToken || '';
        }
        return nextToken;
      })(),
      plexMachine: isDisplayOnlyUpdate
        ? appItem.plexMachine || ''
        : (req.body.plexMachine !== undefined ? req.body.plexMachine : (appItem.plexMachine || '')),
      menu: nextMenu,
      overviewElements,
      tautulliCards,
    };
  });
  saveConfig({ ...config, apps });
  if (!isDisplayOnlyUpdate && req.params.id === 'plex' && plexAdminUser) {
    saveAdmins([plexAdminUser]);
  }
  const fromSettings = String(req.query?.from || req.body?.from || '').trim().toLowerCase();
  if (fromSettings === 'settings' || fromSettings === '1' || fromSettings === 'true') {
    const appId = encodeURIComponent(String(req.params.id || '').trim());
    return res.redirect(`/settings?tab=app&app=${appId}`);
  }
  res.redirect(`/apps/${req.params.id}/settings`);
});
}
