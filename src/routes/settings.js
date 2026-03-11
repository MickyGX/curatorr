import crypto from 'crypto';
import { dedupeMasterArtistNames, getUserPreferences, saveUserPreferences, PRESET_VALUES, previewGlobalPlaylist, getAllUserIds } from '../db.js';
import { JOB_DEFS } from '../services/jobs.js';

// Settings routes — GET /settings and all POST /settings/*

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
    LOCAL_AUTH_MIN_PASSWORD,
    db,
    jobService,
    makeGlobalPlaylistId,
    DEFAULT_SMART_PLAYLIST_SETTINGS,
    playlistService,
  } = ctx;

  // ── GET /settings ─────────────────────────────────────────────────────────

  app.get('/settings', requireSettingsAdmin, (req, res) => {
    const config = loadConfig();
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
    const renderedConfig = { ...config };
    const aboutCurrentVersion = normalizeVersionTag(APP_VERSION || '') || 'Unknown';
    const aboutReleases = [];

    res.render('settings', {
      title: 'Settings — Curatorr',
      user: req.session.user,
      role: getEffectiveRole(req),
      actualRole: getActualRole(req),
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
      aboutLatestVersion: aboutCurrentVersion,
      aboutDataDirectory: DATA_DIR || '/data',
      aboutReleases,
      globalPlaylists: config.globalPlaylists || [],
      allUserIds: (() => { try { return db.prepare('SELECT DISTINCT user_plex_id FROM artist_stats').all().map((r) => r.user_plex_id); } catch { return []; } })(),
      error: String(req.query?.error || '').trim() || null,
      success: String(req.query?.success || '').trim() || null,
      tab: req.query?.tab || 'general',
      extraCss: ['/styles-layout.css', '/styles-settings.css'],
    });
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
    // normalizeBasePath is on ctx only for settings; inline it here
    const rawPath = String(req.body?.basePath || '').trim();
    const basePath = rawPath ? (rawPath.startsWith('/') ? rawPath.replace(/\/+$/, '') : `/${rawPath}`.replace(/\/+$/, '')) : '';
    // Checkbox: present = checked, absent = unchecked (form always submits this field via hidden sentinel)
    const restrictGuests = Boolean(req.body?.restrictGuests);
    const updated = { ...config, general: { ...config.general, serverName, remoteUrl, localUrl, basePath, restrictGuests } };
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

  app.post('/settings/smart-playlist', requireSettingsAdmin, (req, res) => {
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

    const updated = {
      ...config,
      smartPlaylist: {
        ...config.smartPlaylist,
        defaultPreset,
        skipThresholdSeconds, completionThresholdSeconds, songSkipLimit,
        syncIntervalMinutes, skipWeight, belterWeight,
        artistSkipRank, artistBelterRank,
        playlistId, playlistTitle,
      },
    };
    saveConfig(updated);
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
    const region = String(req.body?.region || 'united states').trim().toLowerCase() || 'united states';
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

    const loginStore = config?.userLogins?.curatorr && typeof config.userLogins.curatorr === 'object'
      ? config.userLogins.curatorr
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
        if (loginStore[id]) return loginStore[id];
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
      const usersRes = await fetch('https://plex.tv/api/users', {
        headers: { Accept: 'application/xml', 'X-Plex-Token': token },
      });
      if (!usersRes.ok) throw new Error(`Failed to fetch Plex users (${usersRes.status})`);
      const xmlText = await usersRes.text();
      const users = parsePlexUsers(xmlText, { machineId: machineId || '' });
      const payload = users.map((user) => {
        const ids = normalizeIdentityList([
          user.email,
          user.username,
          user.title,
          user.id,
          user.uuid,
        ]).map((entry) => entry.toLowerCase());
        const identifier = user.email || user.username || user.title || user.id || user.uuid || 'plex-user';
        const locked = ownerKey ? ids.includes(ownerKey) : false;
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
          lastPlexSeen: normalizePlexLastSeen(user.lastSeenAt),
          lastCuratorrLogin: resolveLogin(ids),
          role,
          locked,
        };
      });
      return res.json({ ok: true, users: payload });
    } catch (err) {
      pushLog({ level: 'error', app: 'plex', action: 'users', message: safeMessage(err) });
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

  // ── Jobs settings ─────────────────────────────────────────────────────────

  app.post('/settings/jobs', requireAdmin, (req, res) => {
    const config = loadConfig();
    const current = config.jobs || {};
    const updated = {};
    for (const jobId of Object.keys(JOB_DEFS)) {
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
    const rules = {
      artistTiers: Array.isArray(req.body?.artistTiers) ? req.body.artistTiers.filter(Boolean) : [],
      trackTiers:  Array.isArray(req.body?.trackTiers)  ? req.body.trackTiers.filter(Boolean)  : [],
      topNPerArtist: req.body?.topNPerArtist ? Number(req.body.topNPerArtist) : null,
      maxTracks:     req.body?.maxTracks     ? Number(req.body.maxTracks)     : null,
      sortBy: String(req.body?.sortBy || 'ratingCount'),
    };
    const entry = { id: makeGlobalPlaylistId(), name, rules, enabled: true, createdAt: Date.now() };
    const config = loadConfig();
    const playlists = [...(config.globalPlaylists || []), entry];
    saveConfig({ ...config, globalPlaylists: playlists });
    pushLog({ level: 'info', app: 'playlist', action: 'global.create', message: `Global playlist created: ${name}` });
    res.json({ ok: true, playlist: entry });
    // Sync immediately in background for all users so new playlist appears in Plex without waiting for the next job run
    setImmediate(async () => {
      const userIds = getAllUserIds(db);
      for (const userId of userIds) {
        const prefs = getUserPreferences(db, userId);
        if (!prefs.userWizardCompleted) continue;
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
    const updated = {
      ...existing,
      name: req.body?.name !== undefined ? String(req.body.name).trim() || existing.name : existing.name,
      enabled: req.body?.enabled !== undefined ? Boolean(req.body.enabled) : existing.enabled,
      rules: {
        artistTiers: Array.isArray(req.body?.artistTiers) ? req.body.artistTiers.filter(Boolean) : existing.rules?.artistTiers || [],
        trackTiers:  Array.isArray(req.body?.trackTiers)  ? req.body.trackTiers.filter(Boolean)  : existing.rules?.trackTiers  || [],
        topNPerArtist: req.body?.topNPerArtist !== undefined ? (req.body.topNPerArtist ? Number(req.body.topNPerArtist) : null) : existing.rules?.topNPerArtist,
        maxTracks:     req.body?.maxTracks     !== undefined ? (req.body.maxTracks     ? Number(req.body.maxTracks)     : null) : existing.rules?.maxTracks,
        sortBy: req.body?.sortBy !== undefined ? String(req.body.sortBy) : existing.rules?.sortBy || 'ratingCount',
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
