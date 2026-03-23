import crypto from 'crypto';
import { dedupeMasterArtistNames, getUserPreferences, saveUserPreferences, updateLastfmBackfillCursor, PRESET_VALUES, previewGlobalPlaylist, getAllUserIds, getGenresFromMaster, getMoodsFromMaster, getAllLastfmTags } from '../db.js';
import { JOB_DEFS } from '../services/jobs.js';
import { runLastfmHistoryBackfillForUser } from '../services/lastfm-backfill.js';
import * as jellyfinAdapter from '../services/media-servers/jellyfin.js';
import * as embyAdapter from '../services/media-servers/emby.js';

// Settings routes — GET /settings and all POST /settings/*

const DEFAULT_LASTFM_REGION = 'united kingdom';
const LASTFM_REGION_NAMES = [
  'afghanistan',
  'albania',
  'algeria',
  'andorra',
  'angola',
  'antigua and barbuda',
  'argentina',
  'armenia',
  'australia',
  'austria',
  'azerbaijan',
  'bahamas',
  'bahrain',
  'bangladesh',
  'barbados',
  'belarus',
  'belgium',
  'belize',
  'benin',
  'bhutan',
  'bolivia',
  'bosnia and herzegovina',
  'botswana',
  'brazil',
  'brunei',
  'bulgaria',
  'burkina faso',
  'burundi',
  'cabo verde',
  'cambodia',
  'cameroon',
  'canada',
  'central african republic',
  'chad',
  'chile',
  'china',
  'colombia',
  'comoros',
  'congo',
  'costa rica',
  'croatia',
  'cuba',
  'cyprus',
  'czechia',
  'democratic republic of the congo',
  'denmark',
  'djibouti',
  'dominica',
  'dominican republic',
  'ecuador',
  'egypt',
  'el salvador',
  'equatorial guinea',
  'eritrea',
  'estonia',
  'eswatini',
  'ethiopia',
  'fiji',
  'finland',
  'france',
  'gabon',
  'gambia',
  'georgia',
  'germany',
  'ghana',
  'greece',
  'grenada',
  'guatemala',
  'guinea',
  'guinea-bissau',
  'guyana',
  'haiti',
  'honduras',
  'hong kong',
  'hungary',
  'iceland',
  'india',
  'indonesia',
  'iran',
  'iraq',
  'ireland',
  'israel',
  'italy',
  'jamaica',
  'japan',
  'jordan',
  'kazakhstan',
  'kenya',
  'kiribati',
  'kuwait',
  'kyrgyzstan',
  'laos',
  'latvia',
  'lebanon',
  'lesotho',
  'liberia',
  'libya',
  'liechtenstein',
  'lithuania',
  'luxembourg',
  'madagascar',
  'malawi',
  'malaysia',
  'maldives',
  'mali',
  'malta',
  'marshall islands',
  'mauritania',
  'mauritius',
  'mexico',
  'micronesia',
  'moldova',
  'monaco',
  'mongolia',
  'montenegro',
  'morocco',
  'mozambique',
  'myanmar',
  'namibia',
  'nauru',
  'nepal',
  'netherlands',
  'new zealand',
  'nicaragua',
  'niger',
  'nigeria',
  'north korea',
  'north macedonia',
  'norway',
  'oman',
  'pakistan',
  'palau',
  'palestine',
  'panama',
  'papua new guinea',
  'paraguay',
  'peru',
  'philippines',
  'poland',
  'portugal',
  'qatar',
  'romania',
  'russia',
  'rwanda',
  'saint kitts and nevis',
  'saint lucia',
  'saint vincent and the grenadines',
  'samoa',
  'san marino',
  'sao tome and principe',
  'saudi arabia',
  'senegal',
  'serbia',
  'seychelles',
  'sierra leone',
  'singapore',
  'slovakia',
  'slovenia',
  'solomon islands',
  'somalia',
  'south africa',
  'south korea',
  'south sudan',
  'spain',
  'sri lanka',
  'sudan',
  'suriname',
  'sweden',
  'switzerland',
  'syria',
  'taiwan',
  'tajikistan',
  'tanzania',
  'thailand',
  'timor-leste',
  'togo',
  'tonga',
  'trinidad and tobago',
  'tunisia',
  'turkiye',
  'turkmenistan',
  'tuvalu',
  'uganda',
  'ukraine',
  'united arab emirates',
  'united kingdom',
  'united states',
  'uruguay',
  'uzbekistan',
  'vanuatu',
  'vatican city',
  'venezuela',
  'vietnam',
  'yemen',
  'zambia',
  'zimbabwe',
];

function titleCaseWords(value) {
  return String(value || '')
    .split(/\s+/)
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

function buildLastfmRegionOptions(selectedValue = DEFAULT_LASTFM_REGION) {
  const selected = String(selectedValue || DEFAULT_LASTFM_REGION).trim().toLowerCase() || DEFAULT_LASTFM_REGION;
  const options = new Map(
    LASTFM_REGION_NAMES.map((value) => [value, titleCaseWords(value)])
  );
  if (selected && !options.has(selected)) options.set(selected, titleCaseWords(selected));
  return [...options.entries()]
    .map(([value, label]) => ({ value, label }))
    .sort((a, b) => a.label.localeCompare(b.label));
}

export function registerSettings(app, ctx) {
  const {
    requireUser,
    requireAdmin,
    requireSettingsAdmin,
    loadConfig,
    saveConfig,
    LOG_BUFFER,
    applyLogRetention,
    persistLogsToDisk,
    resolveLogSettings,
    resolveLocalUsers,
    serializeLocalUsers,
    findLocalUserIndex,
    hashPassword,
    validateLocalPasswordStrength,
    normalizeBaseUrl,
    normalizeIdentityList,
    normalizeLidarrAutomationScope,
    resolveLidarrAutomationSettings,
    isValidEmail,
    getEffectiveRole,
    getActualRole,
    pushLog,
    safeMessage,
    parseUserAvatarDataUrl,
    saveCustomUserAvatar,
    normalizeStoredAvatarPath,
    USER_AVATAR_BASE,
    normalizeVersionTag,
    APP_VERSION,
    loadSettingsReleases,
    DATA_DIR,
    loadAdmins,
    saveAdmins,
    loadCoAdmins,
    saveCoAdmins,
    loadPowerUsers,
    savePowerUsers,
    loadGuestUsers,
    saveGuestUsers,
    loadDisabledUsers,
    saveDisabledUsers,
    parsePlexUsers,
    fetchPlexUser,
    buildAppApiUrl,
    buildConfiguredWebhookUrl,
    getWebhookSharedSecret,
    LOCAL_AUTH_MIN_PASSWORD,
    db,
    jobService,
    makeGlobalPlaylistId,
    DEFAULT_SMART_PLAYLIST_SETTINGS,
    playlistService,
    resolvePublicBaseUrl,
  } = ctx;

  function buildReachableWebhookUrl(config, req, webhookPath) {
    const configuredBase = normalizeBaseUrl(
      String(
        config?.tautulli?.curatorrUrl
        || config?.general?.remoteUrl
        || config?.general?.localUrl
        || ''
      ).trim()
    );
    if (configuredBase) return buildConfiguredWebhookUrl(config, webhookPath);
    const requestBase = normalizeBaseUrl(resolvePublicBaseUrl(req));
    if (!requestBase) return buildConfiguredWebhookUrl(config, webhookPath);
    const url = buildAppApiUrl(requestBase, webhookPath);
    const secret = String(getWebhookSharedSecret(config) || '').trim();
    if (secret) url.searchParams.set('key', secret);
    return url.toString();
  }

  // ── GET /settings ─────────────────────────────────────────────────────────

  app.get('/settings', requireSettingsAdmin, (req, res) => {
    const config = loadConfig();
    const themeDefaultsResult = String(req.query?.themeDefaultsResult || '').trim();
    const themeDefaultsError = String(req.query?.themeDefaultsError || '').trim();
    const actualRole = getActualRole(req);
    const canViewServiceSecrets = actualRole === 'admin';
    const currentUserId = String(req.session?.user?.username || '').trim();
    const currentUserKey = String(req.session?.user?.username || '').trim().toLowerCase();
    const userLogins = config?.userLogins?.curatorr && typeof config.userLogins.curatorr === 'object'
      ? config.userLogins.curatorr
      : {};
    const users = resolveLocalUsers(config).map((u) => {
      const loginKey = String(u.email || u.username || '').trim().toLowerCase();
      return {
        ...u,
        lastCuratorrLogin: userLogins[loginKey] || '',
        isCurrentSessionUser: currentUserKey && currentUserKey === String(u.username || '').trim().toLowerCase(),
        isOwnerAccount: Boolean(u.isSetupAdmin || u.setupAccount),
        canDelete: !(u.isSetupAdmin || u.setupAccount) && !(currentUserKey && currentUserKey === String(u.username || '').trim().toLowerCase()),
      };
    });
    const plexAdmins = loadAdmins();
    const plexCoAdmins = loadCoAdmins();
    const plexPowerUsers = loadPowerUsers();
    const plexGuestUsers = loadGuestUsers();
    const plexDisabledUsers = loadDisabledUsers();
    const lidarrAutomation = resolveLidarrAutomationSettings(config);
    const logSettings = resolveLogSettings(config);
    const renderedConfig = {
      ...config,
      plex: {
        ...config.plex,
        token: canViewServiceSecrets ? String(config.plex?.token || '') : '',
        tokenSet: Boolean(String(config.plex?.token || '').trim()),
      },
      tautulli: {
        ...config.tautulli,
        apiKey: canViewServiceSecrets ? String(config.tautulli?.apiKey || '') : '',
        apiKeySet: Boolean(String(config.tautulli?.apiKey || '').trim()),
      },
      lidarr: {
        ...config.lidarr,
        apiKey: canViewServiceSecrets ? String(config.lidarr?.apiKey || '') : '',
        apiKeySet: Boolean(String(config.lidarr?.apiKey || '').trim()),
      },
    };
    const aboutCurrentVersion = normalizeVersionTag(APP_VERSION || '') || 'Unknown';
    const aboutReleases = loadSettingsReleases({ limit: 12, currentVersion: aboutCurrentVersion });

    res.render('settings', {
      title: 'Settings — Curatorr',
      user: req.session.user,
      role: getEffectiveRole(req),
      actualRole,
      canViewServiceSecrets,
      mediaServerType: String(config?.mediaServer?.type || 'plex'),
      webhookUrls: canViewServiceSecrets ? {
        plex: buildReachableWebhookUrl(config, req, 'webhook/plex'),
        tautulli: buildReachableWebhookUrl(config, req, 'webhook/tautulli'),
        jellyfin: buildReachableWebhookUrl(config, req, 'webhook/jellyfin'),
        emby: buildReachableWebhookUrl(config, req, 'webhook/emby'),
      } : null,
      config: renderedConfig,
      users,
      plexAdmins,
      plexCoAdmins,
      plexPowerUsers,
      plexGuestUsers,
      plexDisabledUsers,
      lidarrAutomation,
      logSettings,
      jobDefs: JOB_DEFS,
      jobStatus: jobService?.getStatus() || {},
      aboutCurrentVersion,
      aboutLatestVersion: String(aboutReleases[0]?.tag || '').trim() || aboutCurrentVersion,
      aboutDataDirectory: DATA_DIR || '/data',
      aboutReleases,
      themeDefaultsResult,
      themeDefaultsError,
      lastfmRegionOptions: buildLastfmRegionOptions(renderedConfig.discovery?.region || DEFAULT_LASTFM_REGION),
      globalPlaylists: config.globalPlaylists || [],
      allUserIds: (() => { try { return db.prepare('SELECT DISTINCT user_plex_id FROM artist_stats').all().map((r) => r.user_plex_id); } catch { return []; } })(),
      blendableUsers: (() => {
        try {
          const cfg = loadConfig();
          const laMap = new Map(
            resolveLocalUsers(cfg)
              .filter((u) => u.username && u.avatar)
              .map((u) => [u.username.toLowerCase(), normalizeStoredAvatarPath(u.avatar)]),
          );
          return db.prepare(
            'SELECT DISTINCT user_plex_id FROM artist_stats WHERE TRIM(COALESCE(user_plex_id, \'\')) != \'\' ORDER BY user_plex_id COLLATE NOCASE',
          ).all()
            .map((r) => String(r.user_plex_id || '').trim()).filter(Boolean)
            .map((id) => ({ id, avatar: laMap.get(id.toLowerCase()) || '' }));
        } catch { return []; }
      })(),
      allGenres:     (() => { try { return getGenresFromMaster(db); } catch { return []; } })(),
      allMoods:      (() => { try { return getMoodsFromMaster(db);  } catch { return []; } })(),
      allLastfmTags: (() => { try { return getAllLastfmTags(db);    } catch { return []; } })(),
      localAuthMinPassword: LOCAL_AUTH_MIN_PASSWORD,
      error: String(req.query?.error || '').trim() || null,
      success: String(req.query?.success || '').trim() || null,
      tab: req.query?.tab || 'general',
      extraCss: ['/styles-layout.css', '/styles-settings.css'],
    });
  });

  app.post('/settings/theme-defaults', requireSettingsAdmin, (req, res) => {
    try {
      const config = loadConfig();
      const { normalizeThemeSettings, resolveThemeDefaults } = ctx;
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
        theme: nextDefaults,
      });
      return res.redirect('/settings?tab=appearance&themeDefaultsResult=saved');
    } catch (_err) {
      const encoded = encodeURIComponent('Failed to save default theme.');
      return res.redirect(`/settings?tab=appearance&themeDefaultsError=${encoded}`);
    }
  });

  // ── Log settings ─────────────────────────────────────────────────────────

  app.post('/settings/logs', requireSettingsAdmin, (req, res) => {
    const config = loadConfig();
    const maxEntries = Number(req.body?.log_max_entries);
    const maxDays = Number(req.body?.log_max_days);
    const visibleRows = Number(req.body?.log_visible_rows);
    const current = resolveLogSettings(config);
    const nextSettings = {
      maxEntries: Number.isFinite(maxEntries) && maxEntries > 0 ? Math.floor(maxEntries) : current.maxEntries,
      maxDays: Number.isFinite(maxDays) && maxDays > 0 ? Math.floor(maxDays) : current.maxDays,
      visibleRows: Number.isFinite(visibleRows) && visibleRows > 0 ? Math.floor(visibleRows) : current.visibleRows,
    };
    saveConfig({ ...config, logs: nextSettings });
    const pruned = applyLogRetention(LOG_BUFFER, nextSettings);
    LOG_BUFFER.splice(0, LOG_BUFFER.length, ...pruned);
    persistLogsToDisk(nextSettings);
    pushLog({
      level: 'info',
      app: 'settings',
      action: 'logs.save',
      message: 'Updated Curatorr log retention settings.',
      meta: nextSettings,
    });
    return res.redirect('/settings?tab=logs&success=1');
  });

  // ── General settings ──────────────────────────────────────────────────────

  app.post('/settings/general', requireSettingsAdmin, (req, res) => {
    const config = loadConfig();
    const serverName = String(req.body?.serverName || 'Curatorr').trim() || 'Curatorr';
    const remoteUrl = normalizeBaseUrl(String(req.body?.remoteUrl || '').trim());
    const localUrl = normalizeBaseUrl(String(req.body?.localUrl || '').trim());
    const playbackSource = String(req.body?.playbackSource || config.general?.playbackSource || 'plex').trim().toLowerCase() === 'tautulli'
      ? 'tautulli'
      : 'plex';
    // normalizeBasePath is on ctx only for settings; inline it here
    const rawPath = String(req.body?.basePath || '').trim();
    const basePath = rawPath ? (rawPath.startsWith('/') ? rawPath.replace(/\/+$/, '') : `/${rawPath}`.replace(/\/+$/, '')) : '';
    // Checkbox: present = checked, absent = unchecked (form always submits this field via hidden sentinel)
    const restrictGuests = Boolean(req.body?.restrictGuests);
    const updated = { ...config, general: { ...config.general, serverName, remoteUrl, localUrl, basePath, playbackSource, restrictGuests } };
    saveConfig(updated);
    return res.redirect('/settings?tab=general&success=1');
  });

  // ── Plex settings ─────────────────────────────────────────────────────────

  app.post('/settings/plex', requireAdmin, async (req, res) => {
    const config = loadConfig();
    const localUrl = normalizeBaseUrl(String(req.body?.plexLocalUrl || '').trim());
    const remoteUrl = normalizeBaseUrl(String(req.body?.plexRemoteUrl || '').trim());
    const token = String(req.body?.plexToken || '').trim();
    const machineId = String(req.body?.machineId || '').trim();
    const adminUser = String(req.body?.plexAdminUser || '').trim();

    const librariesRaw = req.body?.libraries;
    const libraries = Array.isArray(librariesRaw) ? librariesRaw : (librariesRaw ? [librariesRaw] : config.plex?.libraries || []);

    // Keep url in sync with localUrl so existing code that reads config.plex.url keeps working
    const updated = { ...config, plex: { ...config.plex, url: localUrl, localUrl, remoteUrl, machineId, adminUser, libraries, ...(token ? { token } : {}) } };
    saveConfig(updated);
    return res.redirect('/settings?tab=plex&success=1');
  });

  // ── Tautulli settings ─────────────────────────────────────────────────────

  app.post('/settings/tautulli', requireAdmin, (req, res) => {
    const config = loadConfig();
    const localUrl = normalizeBaseUrl(String(req.body?.tautulliLocalUrl || '').trim());
    const remoteUrl = normalizeBaseUrl(String(req.body?.tautulliRemoteUrl || '').trim());
    const apiKey = String(req.body?.apiKey || '').trim();
    const updated = { ...config, tautulli: { url: localUrl, localUrl, remoteUrl, apiKey } };
    saveConfig(updated);
    return res.redirect('/settings?tab=tautulli&success=1');
  });

  // ── Jellyfin settings ─────────────────────────────────────────────────────

  app.post('/settings/jellyfin', requireAdmin, (req, res) => {
    const config = loadConfig();
    const url = normalizeBaseUrl(String(req.body?.jellyfinUrl || '').trim());
    const apiKey = String(req.body?.apiKey || '').trim();
    const librariesRaw = req.body?.libraries;
    const libraries = Array.isArray(librariesRaw) ? librariesRaw : (librariesRaw ? [librariesRaw] : config.jellyfin?.libraries || []);
    const updated = { ...config, jellyfin: { ...config.jellyfin, ...(url ? { url } : {}), ...(apiKey ? { apiKey, apiKeySet: true } : {}), libraries } };
    saveConfig(updated);
    return res.redirect('/settings?tab=jellyfin&success=1');
  });

  // ── Emby settings ─────────────────────────────────────────────────────────

  app.post('/settings/emby', requireAdmin, (req, res) => {
    const config = loadConfig();
    const url = normalizeBaseUrl(String(req.body?.embyUrl || '').trim());
    const apiKey = String(req.body?.apiKey || '').trim();
    const librariesRaw = req.body?.libraries;
    const libraries = Array.isArray(librariesRaw) ? librariesRaw : (librariesRaw ? [librariesRaw] : config.emby?.libraries || []);
    const updated = { ...config, emby: { ...config.emby, ...(url ? { url } : {}), ...(apiKey ? { apiKey, apiKeySet: true } : {}), libraries } };
    saveConfig(updated);
    return res.redirect('/settings?tab=emby&success=1');
  });

  // ── Lidarr settings ───────────────────────────────────────────────────────

  app.post('/settings/lidarr', requireAdmin, (req, res) => {
    const config = loadConfig();
    const localUrl = normalizeBaseUrl(String(req.body?.lidarrLocalUrl || '').trim());
    const remoteUrl = normalizeBaseUrl(String(req.body?.lidarrRemoteUrl || '').trim());
    const apiKey = String(req.body?.apiKey || '').trim();
    const automationEnabled = Boolean(req.body?.automationEnabled);
    const autoTriggerManualSearch = Boolean(req.body?.autoTriggerManualSearch);
    const autoAddArtists = Boolean(req.body?.autoAddArtists);
    const manualSearchFallbackAttempts = Math.max(1, Math.min(10, Number(req.body?.manualSearchFallbackAttempts) || 2));
    const manualSearchFallbackHours = Math.max(1, Math.min(168, Number(req.body?.manualSearchFallbackHours) || 24));
    const minimumReleasePeers = Math.max(0, Math.min(999, Number(req.body?.minimumReleasePeers) || 2));
    const preferApprovedReleases = Boolean(req.body?.preferApprovedReleases);
    const automationScope = automationEnabled
      ? normalizeLidarrAutomationScope(req.body?.automationScope)
      : 'off';
    const normalizeQuota = (value, fallback) => {
      const parsed = Number(value);
      if (!Number.isFinite(parsed)) return fallback;
      return Math.max(-1, Math.min(999, Math.round(parsed)));
    };
    const roleQuotas = {
      admin: {
        weeklyArtists: -1,
        weeklyAlbums: -1,
      },
      'co-admin': {
        weeklyArtists: normalizeQuota(req.body?.coAdminWeeklyArtists, 3),
        weeklyAlbums: normalizeQuota(req.body?.coAdminWeeklyAlbums, 6),
      },
      'power-user': {
        weeklyArtists: normalizeQuota(req.body?.powerUserWeeklyArtists, 1),
        weeklyAlbums: normalizeQuota(req.body?.powerUserWeeklyAlbums, 2),
      },
      user: {
        weeklyArtists: normalizeQuota(req.body?.userWeeklyArtists, 0),
        weeklyAlbums: normalizeQuota(req.body?.userWeeklyAlbums, 0),
      },
    };
    const autoAddQuotas = {
      weeklyArtists: normalizeQuota(req.body?.autoAddWeeklyArtists, 1),
      weeklyAlbums: normalizeQuota(req.body?.autoAddWeeklyAlbums, 1),
    };
    const updated = {
      ...config,
      lidarr: {
        ...config.lidarr,
        url: localUrl,
        localUrl,
        remoteUrl,
        apiKey,
        automationEnabled,
        autoTriggerManualSearch,
        autoAddArtists,
        autoAddQuotas,
        manualSearchFallbackAttempts,
        manualSearchFallbackHours,
        minimumReleasePeers,
        preferApprovedReleases,
        automationScope: automationEnabled && automationScope !== 'off' ? 'global' : 'off',
        enabledUsers: [],
        roleQuotas,
      },
    };
    saveConfig(updated);
    return res.redirect('/settings?tab=lidarr&success=1');
  });

  // ── Smart playlist settings ───────────────────────────────────────────────

  app.post('/settings/smart-playlist', requireSettingsAdmin, async (req, res) => {
    const config = loadConfig();
    const VALID_PRESETS = ['cautious', 'measured', 'aggressive'];
    const defaultPreset = VALID_PRESETS.includes(req.body?.defaultPreset) ? req.body.defaultPreset : (config.smartPlaylist?.defaultPreset || 'measured');
    const skipThresholdSeconds = Math.max(15, Math.min(45, Number(req.body?.skipThresholdSeconds) || 30));
    const completionThresholdSeconds = Math.max(15, Math.min(45, Number(req.body?.completionThresholdSeconds) || 30));
    const songSkipLimit = Math.max(1, Math.min(3, Number(req.body?.songSkipLimit) || 2));
    const syncIntervalMinutes = Math.max(5, Math.min(1440, Number(req.body?.syncIntervalMinutes) || 30));
    const skipWeight = Math.max(-1.5, Math.min(-0.5, Number(req.body?.skipWeight) || -1));
    const belterWeight = Math.max(0.5, Math.min(1.5, Number(req.body?.belterWeight) || 1));
    // halfDecentWeight and decentWeight are always derived (skipWeight/2, belterWeight/2) — not stored
    const artistSkipRank = Math.max(0, Math.min(5, Number(req.body?.artistSkipRank) || 2));
    const artistBelterRank = Math.max(5, Math.min(10, Number(req.body?.artistBelterRank) || 8));
    const playlistId = String(req.body?.playlistId || config.smartPlaylist?.playlistId || '').trim();
    const playlistTitle = String(req.body?.playlistTitle || 'Curatorr Smart Playlist').trim();
    const appendUsernameToPlaylistTitles = Boolean(req.body?.appendUsernameToPlaylistTitles);
    const previousAppendUsernameToPlaylistTitles = config.smartPlaylist?.appendUsernameToPlaylistTitles !== false;

    const updated = {
      ...config,
      smartPlaylist: {
        ...config.smartPlaylist,
        defaultPreset,
        skipThresholdSeconds, completionThresholdSeconds, songSkipLimit,
        syncIntervalMinutes, skipWeight, belterWeight,
        artistSkipRank, artistBelterRank,
        appendUsernameToPlaylistTitles,
        playlistId, playlistTitle,
      },
    };
    saveConfig(updated);
    if (previousAppendUsernameToPlaylistTitles !== appendUsernameToPlaylistTitles) {
      const renameResult = await playlistService?.renameAllGeneratedPlaylistTitles(updated).catch((err) => {
        pushLog({
          level: 'warn',
          app: 'settings',
          action: 'playlist.rename',
          message: `Generated playlist rename pass failed after title toggle change: ${safeMessage(err)}`,
        });
        return null;
      });
      if (renameResult) {
        pushLog({
          level: 'info',
          app: 'settings',
          action: 'playlist.rename',
          message: `Generated playlist titles updated after username suffix toggle change: ${renameResult.renamed}/${renameResult.processed} renamed.`,
        });
      }
    }
    return res.redirect('/settings?tab=smart-playlist&success=1');
  });

  // ── Crescive / Curative / rule settings ──────────────────────────────────

  app.post('/settings/smart-playlist-types', requireAdmin, (req, res) => {
    const config = loadConfig();
    const pct = (name, def) => Math.max(0, Math.min(100, Number(req.body?.[name]) || def)) / 100;
    const int = (name, def) => Math.max(1, Math.min(50, Math.floor(Number(req.body?.[name]) || def)));

    const crescive = {
      favouriteArtistTrackPct: pct('cr_favArtistTrackPct', 80),
      favouriteGenreArtistPct: pct('cr_favGenreArtistPct', 80),
      favouriteGenreTrackPct:  pct('cr_favGenreTrackPct',  20),
      otherGenreArtistPct:     pct('cr_otherArtistPct',    20),
      otherGenreTrackPct:      pct('cr_otherTrackPct',     20),
    };
    const curative = {
      favouriteArtistTrackPct: pct('cu_favArtistTrackPct', 100),
      favouriteGenreArtistPct: pct('cu_favGenreArtistPct', 100),
      favouriteGenreTrackPct:  pct('cu_favGenreTrackPct',   80),
      otherGenreArtistPct:     pct('cu_otherArtistPct',     50),
      otherGenreTrackPct:      pct('cu_otherTrackPct',      50),
    };
    const additionRules = {
      belter:     { playedPct: pct('ar_belter_pct',     50), addCount: int('ar_belter_count',     15) },
      decent:     { playedPct: pct('ar_decent_pct',     80), addCount: int('ar_decent_count',     10) },
      halfDecent: { playedPct: pct('ar_halfDecent_pct', 100), addCount: int('ar_halfDecent_count',  5) },
    };
    const subtractionRules = {
      skip: [0, 1, 2].map((i) => ({
        playedPct:   pct(`sr_skip_${i}_pct`,   [20, 50, 80][i]),
        removeCount: int(`sr_skip_${i}_count`, [15, 10,  5][i]),
      })),
    };
    saveConfig({ ...config, smartPlaylist: { ...config.smartPlaylist, crescive, curative, additionRules, subtractionRules } });
    return res.redirect('/settings?tab=smart-playlist&success=1');
  });

  // ── Discovery settings ────────────────────────────────────────────────────

  app.post('/settings/discovery', requireSettingsAdmin, (req, res) => {
    const config = loadConfig();
    const lastfmApiKey = String(req.body?.lastfmApiKey || '').trim();
    const region = String(req.body?.region || DEFAULT_LASTFM_REGION).trim().toLowerCase() || DEFAULT_LASTFM_REGION;
    const showTrendingArtists = Boolean(req.body?.showTrendingArtists);
    const showTrendingTracks  = Boolean(req.body?.showTrendingTracks);
    const showSimilarArtists  = Boolean(req.body?.showSimilarArtists);
    saveConfig({ ...config, discovery: { lastfmApiKey, region, showTrendingArtists, showTrendingTracks, showSimilarArtists } });
    return res.redirect('/settings?tab=discovery&success=1');
  });

  // ── Artist filters ────────────────────────────────────────────────────────

  app.post('/settings/filters', requireSettingsAdmin, (req, res) => {
    const userPlexId = String(req.session?.user?.username || '').trim();
    const existingPrefs = userPlexId ? getUserPreferences(db, userPlexId) : { likedGenres: [], ignoredGenres: [], likedArtists: [], ignoredArtists: [], userWizardCompleted: false };
    const parseCsv = (value) => {
      const values = Array.isArray(value) ? value : [value];
      return dedupeMasterArtistNames(values
        .flatMap((entry) => String(entry || '').split(/[\n,]/))
        .map((entry) => entry.trim())
        .filter(Boolean));
    };
    const mustInclude = parseCsv(req.body?.mustIncludeArtists);
    const neverInclude = parseCsv(req.body?.neverIncludeArtists);
    saveUserPreferences(db, userPlexId, {
      likedGenres: existingPrefs.likedGenres || [],
      ignoredGenres: existingPrefs.ignoredGenres || [],
      likedArtists: mustInclude,
      ignoredArtists: neverInclude,
      userWizardCompleted: Boolean(existingPrefs.userWizardCompleted),
    });
    return res.redirect('/settings?tab=filters&success=1');
  });

  app.post('/user-settings/filters', requireUser, (req, res) => {
    const userPlexId = String(req.session?.user?.username || '').trim();
    const existingPrefs = userPlexId ? getUserPreferences(db, userPlexId) : { likedGenres: [], ignoredGenres: [], likedArtists: [], ignoredArtists: [], userWizardCompleted: false };
    const parseCsv = (value) => {
      const values = Array.isArray(value) ? value : [value];
      return dedupeMasterArtistNames(values
        .flatMap((entry) => String(entry || '').split(/[\n,]/))
        .map((entry) => entry.trim())
        .filter(Boolean));
    };
    const mustInclude = parseCsv(req.body?.mustIncludeArtists);
    const neverInclude = parseCsv(req.body?.neverIncludeArtists);
    saveUserPreferences(db, userPlexId, {
      likedGenres: existingPrefs.likedGenres || [],
      ignoredGenres: existingPrefs.ignoredGenres || [],
      likedArtists: mustInclude,
      ignoredArtists: neverInclude,
      userWizardCompleted: Boolean(existingPrefs.userWizardCompleted),
    });
    return res.redirect('/user-settings?success=filters-updated');
  });

  // ── Local users ───────────────────────────────────────────────────────────

  app.post('/settings/local-users/add', requireAdmin, (req, res) => {
    const config = loadConfig();
    const username = String(req.body?.username || '').trim();
    const email = String(req.body?.email || '').trim();
    const password = String(req.body?.password || '');
    const role = ['admin', 'co-admin', 'power-user', 'user', 'guest', 'disabled'].includes(req.body?.role)
      ? req.body.role : 'user';

    if (!username) return res.redirect('/settings?tab=users&error=username-required');
    if (email && !isValidEmail(email)) return res.redirect('/settings?tab=users&error=email-invalid');
    const pwErr = validateLocalPasswordStrength(password);
    if (pwErr) return res.redirect(`/settings?tab=users&error=${encodeURIComponent(pwErr)}`);

    const users = resolveLocalUsers(config);
    if (users.find((u) => u.username.toLowerCase() === username.toLowerCase())) {
      return res.redirect('/settings?tab=users&error=username-taken');
    }

    // crypto imported at top of file
    const salt = crypto.randomBytes(16).toString('hex');
    const newUser = {
      username, email, role,
      passwordHash: hashPassword(password, salt),
      salt, avatar: '',
      createdBy: 'system', setupAccount: false, systemCreated: true,
      createdAt: new Date().toISOString(),
    };

    saveConfig({ ...config, users: serializeLocalUsers([...users, newUser]) });
    pushLog({ level: 'info', app: 'settings', action: 'user.add', message: `Local user added: ${username}` });
    return res.redirect('/settings?tab=users&success=1');
  });

  app.post('/settings/local-users/role', requireAdmin, (req, res) => {
    const config = loadConfig();
    const username = String(req.body?.username || '').trim();
    const role = ['admin', 'co-admin', 'power-user', 'user', 'guest', 'disabled'].includes(req.body?.role)
      ? req.body.role
      : 'user';
    if (!username) return res.redirect('/settings?tab=users&error=username-required');

    const users = resolveLocalUsers(config);
    const idx = findLocalUserIndex(users, { username });
    if (idx < 0) return res.redirect('/settings?tab=users&error=not-found');

    const target = users[idx];
    if (target.isSetupAdmin || target.setupAccount) {
      return res.redirect('/settings?tab=users&error=cannot-change-setup-admin');
    }

    const currentSessionUser = String(req.session?.user?.username || '').trim().toLowerCase();
    if (currentSessionUser && currentSessionUser === String(target.username || '').trim().toLowerCase() && role !== 'admin') {
      return res.redirect('/settings?tab=users&error=cannot-demote-current-session');
    }

    if (target.role === 'admin' && role !== 'admin') {
      const otherAdminExists = users.some((entry, index) => index !== idx && entry.role === 'admin');
      if (!otherAdminExists) return res.redirect('/settings?tab=users&error=last-admin');
    }

    const updatedUsers = users.map((entry, index) => (index === idx ? { ...entry, role } : entry));
    saveConfig({ ...config, users: serializeLocalUsers(updatedUsers) });
    pushLog({ level: 'info', app: 'settings', action: 'user.role', message: `Updated role for ${username} to ${role}` });
    return res.redirect('/settings?tab=users&success=1');
  });

  app.post('/settings/local-users/remove', requireAdmin, (req, res) => {
    const config = loadConfig();
    const username = String(req.body?.username || '').trim();
    if (!username) return res.redirect('/settings?tab=users&error=username-required');

    const users = resolveLocalUsers(config);
    const idx = findLocalUserIndex(users, { username });
    if (idx < 0) return res.redirect('/settings?tab=users&error=not-found');

    const target = users[idx];
    if (target.isSetupAdmin || target.setupAccount) {
      return res.redirect('/settings?tab=users&error=cannot-remove-setup-admin');
    }

    const updated = users.filter((_, i) => i !== idx);
    saveConfig({ ...config, users: serializeLocalUsers(updated) });
    pushLog({ level: 'info', app: 'settings', action: 'user.remove', message: `Local user removed: ${username}` });
    return res.redirect('/settings?tab=users&success=1');
  });

  // ── Plex admins/co-admins ─────────────────────────────────────────────────

  app.post('/settings/plex-admins', requireAdmin, (req, res) => {
    const admins = String(req.body?.admins || '').split(/[\n,]/).map((s) => s.trim()).filter(Boolean);
    const coAdmins = String(req.body?.coAdmins || '').split(/[\n,]/).map((s) => s.trim()).filter(Boolean);
    const powerUsers = String(req.body?.powerUsers || '').split(/[\n,]/).map((s) => s.trim()).filter(Boolean);
    const guests = String(req.body?.guests || '').split(/[\n,]/).map((s) => s.trim()).filter(Boolean);
    const disabledUsers = String(req.body?.disabledUsers || '').split(/[\n,]/).map((s) => s.trim()).filter(Boolean);
    saveAdmins(admins);
    saveCoAdmins(coAdmins);
    savePowerUsers(powerUsers);
    saveGuestUsers(guests);
    saveDisabledUsers(disabledUsers);
    return res.redirect('/settings?tab=users&success=1');
  });

  app.get('/settings/plex-users', requireAdmin, async (_req, res) => {
    const config = loadConfig();
    const { token, machineId } = config.plex || {};
    if (!token) return res.status(400).json({ error: 'Plex not configured.' });

    const curatorrLoginStore = config?.userLogins?.curatorr && typeof config.userLogins.curatorr === 'object'
      ? config.userLogins.curatorr
      : {};
    const plexLoginStore = config?.userLogins?.plex && typeof config.userLogins.plex === 'object'
      ? config.userLogins.plex
      : {};
    const admins = loadAdmins();
    const coAdmins = loadCoAdmins();
    const powerUsers = loadPowerUsers();
    const guests = loadGuestUsers();
    const disabledUsers = loadDisabledUsers();
    const ownerKey = admins[0] ? String(admins[0]).trim().toLowerCase() : '';
    const hasMatch = (list, ids) => {
      const set = new Set((Array.isArray(list) ? list : []).map((entry) => String(entry || '').trim().toLowerCase()).filter(Boolean));
      return ids.some((id) => set.has(id));
    };
    const resolveLogin = (ids) => {
      for (const id of ids) {
        if (curatorrLoginStore[id]) return curatorrLoginStore[id];
      }
      for (const id of ids) {
        if (plexLoginStore[id]) return plexLoginStore[id];
      }
      return '';
    };
    const normalizePlexLastSeen = (value) => {
      const raw = String(value || '').trim();
      if (!raw) return '';
      const numeric = Number(raw);
      if (Number.isFinite(numeric) && numeric > 0) {
        const stamp = numeric > 1e12 ? numeric : numeric * 1000;
        const date = new Date(stamp);
        return Number.isNaN(date.getTime()) ? '' : date.toISOString();
      }
      const parsed = Date.parse(raw);
      return Number.isNaN(parsed) ? '' : new Date(parsed).toISOString();
    };

    try {
      pushLog({ level: 'info', app: 'plex', action: 'users', message: 'Fetching Plex users for settings.' });

      // Fetch managed/home/shared users and the token owner in parallel.
      // plex.tv/api/users intentionally omits the server owner, so we fetch
      // them separately via /api/v2/user and inject them at the top of the list.
      const [usersRes, ownerUser] = await Promise.all([
        fetch('https://plex.tv/api/users', { headers: { Accept: 'application/xml', 'X-Plex-Token': token } }),
        fetchPlexUser(token).catch(() => null),
      ]);
      if (!usersRes.ok) throw new Error(`Failed to fetch Plex users (${usersRes.status})`);
      const xmlText = await usersRes.text();
      const users = parsePlexUsers(xmlText, { machineId: machineId || '' });

      const mapUser = (user, forceOwner = false) => {
        const ids = normalizeIdentityList([
          user.email,
          user.username,
          user.title,
          user.id,
          user.uuid,
        ]).map((entry) => entry.toLowerCase());
        const identifier = user.email || user.username || user.title || user.id || user.uuid || 'plex-user';
        const locked = forceOwner || (ownerKey ? ids.includes(ownerKey) : false);
        let role = 'user';
        if (locked) role = 'admin';
        else if (hasMatch(disabledUsers, ids)) role = 'disabled';
        else if (hasMatch(admins, ids)) role = 'admin';
        else if (hasMatch(coAdmins, ids)) role = 'co-admin';
        else if (hasMatch(powerUsers, ids)) role = 'power-user';
        else if (hasMatch(guests, ids)) role = 'guest';

        return {
          id: user.id || user.uuid || identifier,
          name: user.title || user.username || user.email || 'Plex User',
          username: user.username || '',
          email: user.email || '',
          identifier,
          lastPlexSeen: normalizePlexLastSeen(user.lastSeenAt || ''),
          lastCuratorrLogin: resolveLogin(ids),
          role,
          locked,
        };
      };

      const payload = users.map((u) => mapUser(u));

      // Inject the server owner at the top if they aren't already in the list.
      if (ownerUser) {
        const ownerIds = new Set(
          normalizeIdentityList([ownerUser.email, ownerUser.username, ownerUser.title, String(ownerUser.id || ''), String(ownerUser.uuid || '')])
            .map((v) => v.toLowerCase()),
        );
        const alreadyPresent = payload.some((u) => [u.email, u.username, u.identifier, u.id]
          .some((v) => v && ownerIds.has(String(v).toLowerCase())));
        if (!alreadyPresent) {
          payload.unshift(mapUser({
            id: String(ownerUser.id || ''),
            uuid: String(ownerUser.uuid || ''),
            username: ownerUser.username || '',
            email: ownerUser.email || '',
            title: ownerUser.title || ownerUser.username || ownerUser.email || '',
            lastSeenAt: '',
          }, true));
        }
      }

      return res.json({ ok: true, users: payload });
    } catch (err) {
      pushLog({ level: 'error', app: 'plex', action: 'users', message: safeMessage(err) });
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Jellyfin / Emby user list for role assignment ─────────────────────────
  app.get('/settings/ms-users', requireAdmin, async (_req, res) => {
    const config = loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').trim().toLowerCase();
    if (msType !== 'jellyfin' && msType !== 'emby') {
      return res.status(400).json({ error: 'This endpoint is only available for Jellyfin/Emby.' });
    }
    const msCfg = config?.[msType] || {};
    const msUrl  = String(msCfg?.url    || '').trim();
    const msKey  = String(msCfg?.apiKey || '').trim();
    if (!msUrl || !msKey) return res.status(400).json({ error: `${msType} not configured.` });

    const curatorrLoginStore = config?.userLogins?.curatorr && typeof config.userLogins.curatorr === 'object'
      ? config.userLogins.curatorr : {};
    const admins         = loadAdmins();
    const coAdmins       = loadCoAdmins();
    const powerUsers     = loadPowerUsers();
    const guests         = loadGuestUsers();
    const disabledUsers  = loadDisabledUsers();
    const ownerKey       = admins[0] ? String(admins[0]).trim().toLowerCase() : '';
    const hasMatch = (list, ids) => {
      const set = new Set((Array.isArray(list) ? list : []).map((e) => String(e || '').trim().toLowerCase()).filter(Boolean));
      return ids.some((id) => set.has(id));
    };
    const resolveLogin = (ids) => {
      for (const id of ids) if (curatorrLoginStore[id]) return curatorrLoginStore[id];
      return '';
    };
    const lastPlayStmt = db.prepare('SELECT MAX(started_at) AS last_play_at FROM play_events WHERE user_plex_id = ?');

    try {
      const adapter = msType === 'emby' ? embyAdapter : jellyfinAdapter;
      const users   = await adapter.getUsers(msUrl, msKey);
      const payload = users.map((u) => {
        const identifier = u.name;
        const ids = [u.name.toLowerCase()];
        const locked = ownerKey ? ids.includes(ownerKey) : false;
        let role = 'user';
        if (locked) role = 'admin';
        else if (hasMatch(disabledUsers, ids)) role = 'disabled';
        else if (hasMatch(admins, ids))        role = 'admin';
        else if (hasMatch(coAdmins, ids))      role = 'co-admin';
        else if (hasMatch(powerUsers, ids))    role = 'power-user';
        else if (hasMatch(guests, ids))        role = 'guest';
        const lastPlayAt = Number(lastPlayStmt.get(identifier)?.last_play_at || 0);
        const lastPlayIso = lastPlayAt ? new Date(lastPlayAt).toISOString() : '';
        return {
          id: u.id,
          name: u.name,
          username: u.name,
          email: '',
          identifier,
          lastPlexSeen: lastPlayIso,
          lastCuratorrLogin: resolveLogin(ids),
          role,
          locked,
        };
      });
      return res.json({ ok: true, users: payload });
    } catch (err) {
      pushLog({ level: 'error', app: msType, action: 'users', message: safeMessage(err) });
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/settings/roles', requireAdmin, (req, res) => {
    const roles = Array.isArray(req.body?.roles) ? req.body.roles : [];
    const admins = loadAdmins();
    const owner = admins[0] ? String(admins[0]).trim() : '';
    const ownerKey = owner.toLowerCase();
    const nextAdmins = owner ? [owner] : [];
    const nextCoAdmins = [];
    const nextPowerUsers = [];
    const nextGuests = [];
    const nextDisabledUsers = [];
    const seen = new Set();
    const pushUnique = (bucket, target, value) => {
      const raw = String(value || '').trim();
      if (!raw) return;
      const key = raw.toLowerCase();
      if (seen.has(`${bucket}:${key}`)) return;
      seen.add(`${bucket}:${key}`);
      target.push(raw);
    };

    roles.forEach((entry) => {
      const identifier = String(entry?.identifier || '').trim();
      const role = String(entry?.role || 'user').trim().toLowerCase();
      if (!identifier) return;
      if (ownerKey && identifier.toLowerCase() === ownerKey) return;
      if (role === 'admin') pushUnique('admin', nextAdmins, identifier);
      else if (role === 'co-admin') pushUnique('co-admin', nextCoAdmins, identifier);
      else if (role === 'power-user') pushUnique('power-user', nextPowerUsers, identifier);
      else if (role === 'guest') pushUnique('guest', nextGuests, identifier);
      else if (role === 'disabled') pushUnique('disabled', nextDisabledUsers, identifier);
    });

    saveAdmins(nextAdmins);
    saveCoAdmins(nextCoAdmins);
    savePowerUsers(nextPowerUsers);
    saveGuestUsers(nextGuests);
    saveDisabledUsers(nextDisabledUsers);
    return res.json({ ok: true });
  });

  // ── User settings (self) ──────────────────────────────────────────────────

  app.post('/user-settings/password', requireUser, (req, res) => {
    const config = loadConfig();
    const sessionUser = req.session.user;
    if (String(sessionUser.source || '').toLowerCase() !== 'local') {
      return res.redirect('/user-settings?error=not-local');
    }

    const current = String(req.body?.currentPassword || '');
    const newPw = String(req.body?.newPassword || '');
    const confirm = String(req.body?.confirmPassword || '');

    const users = resolveLocalUsers(config);
    const idx = findLocalUserIndex(users, { username: sessionUser.username, email: sessionUser.email });
    if (idx < 0) return res.redirect('/user-settings?error=not-found');

    const { verifyPassword } = ctx;
    if (!verifyPassword(current, users[idx])) {
      return res.redirect('/user-settings?error=wrong-password');
    }

    const pwErr = validateLocalPasswordStrength(newPw);
    if (pwErr) return res.redirect(`/user-settings?error=${encodeURIComponent(pwErr)}`);
    if (newPw !== confirm) return res.redirect('/user-settings?error=passwords-mismatch');

    // crypto imported at top of file
    const salt = crypto.randomBytes(16).toString('hex');
    const updatedUsers = users.map((u, i) => i === idx ? { ...u, passwordHash: hashPassword(newPw, salt), salt } : u);
    saveConfig({ ...config, users: serializeLocalUsers(updatedUsers) });
    return res.redirect('/user-settings?success=password-changed');
  });

  app.post('/user-settings/avatar', requireUser, (req, res) => {
    const config = loadConfig();
    const sessionUser = req.session.user;

    const result = parseUserAvatarDataUrl(req.body?.avatarData);
    if (!result.ok) return res.redirect(`/user-settings?error=${encodeURIComponent(result.error)}`);

    const avatarPath = saveCustomUserAvatar(result.buffer, result.ext, sessionUser.username);
    if (!avatarPath) return res.redirect('/user-settings?error=avatar-save-failed');

    // Update local user record if local auth
    if (String(sessionUser.source || '').toLowerCase() === 'local') {
      const users = resolveLocalUsers(config);
      const idx = findLocalUserIndex(users, { username: sessionUser.username });
      if (idx >= 0) {
        const updatedUsers = users.map((u, i) => i === idx ? { ...u, avatar: avatarPath } : u);
        saveConfig({ ...config, users: serializeLocalUsers(updatedUsers) });
      }
    }

    // Update session
    req.session.user = { ...req.session.user, avatar: normalizeStoredAvatarPath(avatarPath) };
    return res.redirect('/user-settings?success=avatar-updated');
  });

  // ── Theme ─────────────────────────────────────────────────────────────────

  app.post('/user-settings/theme', requireUser, (req, res) => {
    const config = loadConfig();
    const { normalizeThemeSettings, serializeUserThemePreferences } = ctx;
    const settings = normalizeThemeSettings(req.body || {});
    const updated = serializeUserThemePreferences(config, req.session.user, settings);
    saveConfig(updated);
    return res.redirect('/user-settings?success=theme-updated');
  });

  // ── Curation preset ───────────────────────────────────────────────────────

  app.post('/user-settings/preset', requireUser, (req, res) => {
    const userPlexId = String(req.session?.user?.username || '').trim();
    const preset = String(req.body?.preset || '').trim();
    if (!preset || !PRESET_VALUES[preset]) {
      return res.redirect('/user-settings?error=invalid-preset');
    }
    const prefs = getUserPreferences(db, userPlexId);
    saveUserPreferences(db, userPlexId, { ...prefs, smartConfig: { preset } });
    return res.redirect('/user-settings?success=preset-updated');
  });

  // ── Last.fm username ──────────────────────────────────────────────────────

  app.post('/user-settings/lastfm', requireUser, (req, res) => {
    const userPlexId = String(req.session?.user?.username || '').trim();
    if (!userPlexId) return res.redirect('/user-settings?error=not-found');
    const lastfmUsername = String(req.body?.lastfmUsername || '').trim();
    const rawStations = req.body?.lastfmStations;
    const VALID_UNDOCUMENTED = ['recommended', 'mix', 'library', 'neighbours', 'loved'];
    const VALID_PERIODS = ['overall', '7day', '1month', '3month', '6month', '12month'];
    const lastfmEnabledStations = (Array.isArray(rawStations) ? rawStations : rawStations ? [rawStations] : [])
      .filter((s) => VALID_UNDOCUMENTED.includes(s) || (s.startsWith('topTracks:') && VALID_PERIODS.includes(s.slice('topTracks:'.length))));
    const topTracksPeriod = String(req.body?.lastfmTopTracks || '').trim();
    if (topTracksPeriod && VALID_PERIODS.includes(topTracksPeriod)) {
      lastfmEnabledStations.push(`topTracks:${topTracksPeriod}`);
    }
    const prefs = getUserPreferences(db, userPlexId);
    saveUserPreferences(db, userPlexId, { ...prefs, lastfmUsername, lastfmEnabledStations });
    return res.redirect('/user-settings?success=lastfm-updated');
  });

  app.post('/user-settings/lastfm/run-backfill', requireUser, (req, res) => {
    const userPlexId = String(req.session?.user?.username || '').trim();
    if (!userPlexId) return res.status(403).json({ error: 'Not authenticated' });
    runLastfmHistoryBackfillForUser(ctx, userPlexId).catch(() => {});
    return res.json({ ok: true });
  });

  app.post('/user-settings/lastfm/reset-backfill', requireUser, (req, res) => {
    const userPlexId = String(req.session?.user?.username || '').trim();
    if (!userPlexId) return res.redirect('/user-settings?error=not-found');
    updateLastfmBackfillCursor(db, userPlexId, 0);
    return res.redirect('/user-settings?success=backfill-reset');
  });

  // ── Jobs settings ─────────────────────────────────────────────────────────

  app.post('/settings/jobs', requireAdmin, (req, res) => {
    const config = loadConfig();
    const current = config.jobs || {};
    const updated = {};
    for (const jobId of Object.keys(JOB_DEFS)) {
      if (JOB_DEFS[jobId].manualOnly) continue;
      const intervalMinutes = Math.max(1, Math.min(1440, Number(req.body?.[`${jobId}_interval`]) || JOB_DEFS[jobId].defaultIntervalMinutes));
      const enabled = Boolean(req.body?.[`${jobId}_enabled`]);
      updated[jobId] = { ...current[jobId], intervalMinutes, enabled };
      jobService?.reschedule(jobId, intervalMinutes, enabled);
    }
    // keep smartPlaylist.syncIntervalMinutes in sync for backwards compatibility
    const syncInterval = updated.smartPlaylistSync?.intervalMinutes;
    const nextSmartPlaylist = syncInterval
      ? { ...config.smartPlaylist, syncIntervalMinutes: syncInterval }
      : config.smartPlaylist;
    saveConfig({ ...config, jobs: updated, smartPlaylist: nextSmartPlaylist });
    return res.redirect('/settings?tab=jobs&success=1');
  });

  // ── Jobs API ──────────────────────────────────────────────────────────────

  app.get('/api/jobs/status', requireAdmin, (req, res) => {
    res.json(jobService?.getStatus() || {});
  });

  app.post('/api/jobs/:jobId/run', requireAdmin, (req, res) => {
    const jobId = String(req.params.jobId || '');
    if (!JOB_DEFS[jobId]) return res.status(404).json({ error: 'Unknown job' });
    if (!jobService) return res.status(503).json({ error: 'Job service unavailable' });
    jobService.runJob(jobId).catch(() => {});
    res.json({ ok: true, jobId, startedAt: Date.now() });
  });

  // ── Global Playlist CRUD ──────────────────────────────────────────────────

  // GET /api/playlists/global — list
  app.get('/api/playlists/global', requireAdmin, (req, res) => {
    const config = loadConfig();
    res.json({ ok: true, playlists: config.globalPlaylists || [] });
  });

  // POST /api/playlists/global — create
  app.post('/api/playlists/global', requireAdmin, (req, res) => {
    const name = String(req.body?.name || '').trim();
    if (!name) return res.status(400).json({ error: 'Name is required' });
    const BLEND_MODES = ['average', 'intersection', 'union', 'veto'];
    const blendUsers = Array.isArray(req.body?.blendUsers) ? req.body.blendUsers.filter(Boolean) : [];
    const rules = {
      artistTiers: Array.isArray(req.body?.artistTiers) ? req.body.artistTiers.filter(Boolean) : [],
      trackTiers:  Array.isArray(req.body?.trackTiers)  ? req.body.trackTiers.filter(Boolean)  : [],
      genres:      Array.isArray(req.body?.genres)      ? req.body.genres.filter(Boolean)      : [],
      moods:       Array.isArray(req.body?.moods)       ? req.body.moods.filter(Boolean)       : [],
      tags:        Array.isArray(req.body?.tags)        ? req.body.tags.filter(Boolean)        : [],
      topNPerArtist: req.body?.topNPerArtist ? Number(req.body.topNPerArtist) : null,
      maxTracks:     req.body?.maxTracks     ? Number(req.body.maxTracks)     : null,
      sortBy: String(req.body?.sortBy || 'ratingCount'),
      blendUsers,
      blendMode: blendUsers.length && BLEND_MODES.includes(req.body?.blendMode) ? req.body.blendMode : 'average',
    };
    const entry = { id: makeGlobalPlaylistId(), name, rules, enabled: true, createdAt: Date.now() };
    const config = loadConfig();
    const playlists = [...(config.globalPlaylists || []), entry];
    saveConfig({ ...config, globalPlaylists: playlists });
    pushLog({ level: 'info', app: 'playlist', action: 'global.create', message: `Global playlist created: ${name}` });
    res.json({ ok: true, playlist: entry });
    // Sync immediately in background. Blend playlists only go to blend users; regular to all.
    setImmediate(async () => {
      const syncIds = blendUsers.length
        ? blendUsers
        : getAllUserIds(db).filter((uid) => getUserPreferences(db, uid).userWizardCompleted);
      for (const userId of syncIds) {
        await playlistService?.syncGlobalPlaylist(userId, entry).catch(() => {});
      }
    });
  });

  // PUT /api/playlists/global/:id — update
  app.put('/api/playlists/global/:id', requireAdmin, (req, res) => {
    const id = String(req.params.id || '');
    const config = loadConfig();
    const playlists = config.globalPlaylists || [];
    const idx = playlists.findIndex((p) => p.id === id);
    if (idx === -1) return res.status(404).json({ error: 'Not found' });
    const existing = playlists[idx];
    const BLEND_MODES = ['average', 'intersection', 'union', 'veto'];
    const updBlendUsers = req.body?.blendUsers !== undefined
      ? (Array.isArray(req.body.blendUsers) ? req.body.blendUsers.filter(Boolean) : [])
      : (existing.rules?.blendUsers || []);
    const updated = {
      ...existing,
      name: req.body?.name !== undefined ? String(req.body.name).trim() || existing.name : existing.name,
      enabled: req.body?.enabled !== undefined ? Boolean(req.body.enabled) : existing.enabled,
      rules: {
        artistTiers: Array.isArray(req.body?.artistTiers) ? req.body.artistTiers.filter(Boolean) : existing.rules?.artistTiers || [],
        trackTiers:  Array.isArray(req.body?.trackTiers)  ? req.body.trackTiers.filter(Boolean)  : existing.rules?.trackTiers  || [],
        genres:      Array.isArray(req.body?.genres)      ? req.body.genres.filter(Boolean)      : existing.rules?.genres      || [],
        moods:       Array.isArray(req.body?.moods)       ? req.body.moods.filter(Boolean)       : existing.rules?.moods       || [],
        tags:        Array.isArray(req.body?.tags)        ? req.body.tags.filter(Boolean)        : existing.rules?.tags        || [],
        topNPerArtist: req.body?.topNPerArtist !== undefined ? (req.body.topNPerArtist ? Number(req.body.topNPerArtist) : null) : existing.rules?.topNPerArtist,
        maxTracks:     req.body?.maxTracks     !== undefined ? (req.body.maxTracks     ? Number(req.body.maxTracks)     : null) : existing.rules?.maxTracks,
        sortBy: req.body?.sortBy !== undefined ? String(req.body.sortBy) : existing.rules?.sortBy || 'ratingCount',
        blendUsers: updBlendUsers,
        blendMode: req.body?.blendMode !== undefined
          ? (BLEND_MODES.includes(req.body.blendMode) ? req.body.blendMode : 'average')
          : (existing.rules?.blendMode || 'average'),
      },
      updatedAt: Date.now(),
    };
    const newList = [...playlists];
    newList[idx] = updated;
    saveConfig({ ...config, globalPlaylists: newList });
    res.json({ ok: true, playlist: updated });
  });

  // DELETE /api/playlists/global/:id
  app.delete('/api/playlists/global/:id', requireAdmin, (req, res) => {
    const id = String(req.params.id || '');
    const config = loadConfig();
    const playlists = (config.globalPlaylists || []).filter((p) => p.id !== id);
    saveConfig({ ...config, globalPlaylists: playlists });
    pushLog({ level: 'info', app: 'playlist', action: 'global.delete', message: `Global playlist deleted: ${id}` });
    res.json({ ok: true });
  });

  // GET /api/playlists/global/preview — live track/artist count estimate
  app.get('/api/playlists/global/preview', requireAdmin, (req, res) => {
    let rules;
    try { rules = JSON.parse(String(req.query?.rules || '{}')); } catch { return res.status(400).json({ error: 'Invalid rules JSON' }); }
    const userId = String(req.query?.userId || '').trim() || null;
    const config = loadConfig();
    const smartSettings = config.smartPlaylist || DEFAULT_SMART_PLAYLIST_SETTINGS;
    const result = previewGlobalPlaylist(db, rules, userId, smartSettings);
    res.json({ ok: true, ...result });
  });
}
