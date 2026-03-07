// Settings routes — GET /settings and all POST /settings/*

import crypto from 'crypto';

export function registerSettings(app, ctx) {
  const {
    requireUser,
    requireAdmin,
    requireSettingsAdmin,
    loadConfig,
    saveConfig,
    resolveLocalUsers,
    serializeLocalUsers,
    findLocalUserIndex,
    hashPassword,
    validateLocalPasswordStrength,
    normalizeBaseUrl,
    isValidEmail,
    getEffectiveRole,
    pushLog,
    safeMessage,
    parseUserAvatarDataUrl,
    saveCustomUserAvatar,
    normalizeStoredAvatarPath,
    USER_AVATAR_BASE,
    loadAdmins,
    saveAdmins,
    loadCoAdmins,
    saveCoAdmins,
    parsePlexUsers,
    LOCAL_AUTH_MIN_PASSWORD,
  } = ctx;

  // ── GET /settings ─────────────────────────────────────────────────────────

  app.get('/settings', requireSettingsAdmin, (req, res) => {
    const config = loadConfig();
    const users = resolveLocalUsers(config);
    const plexAdmins = loadAdmins();
    const plexCoAdmins = loadCoAdmins();

    res.render('settings', {
      title: 'Settings — Curatorr',
      user: req.session.user,
      role: getEffectiveRole(req),
      config: config,
      users,
      plexAdmins,
      plexCoAdmins,
      error: null,
      success: null,
      tab: req.query?.tab || 'general',
      extraCss: ['/styles-layout.css', '/styles-settings.css'],
    });
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
    const updated = { ...config, lidarr: { url: localUrl, localUrl, remoteUrl, apiKey } };
    saveConfig(updated);
    return res.redirect('/settings?tab=lidarr&success=1');
  });

  // ── Smart playlist settings ───────────────────────────────────────────────

  app.post('/settings/smart-playlist', requireSettingsAdmin, (req, res) => {
    const config = loadConfig();
    const skipThresholdSeconds = Math.max(15, Math.min(45, Number(req.body?.skipThresholdSeconds) || 30));
    const completionThresholdSeconds = Math.max(15, Math.min(45, Number(req.body?.completionThresholdSeconds) || 30));
    const songSkipLimit = Math.max(1, Math.min(3, Number(req.body?.songSkipLimit) || 2));
    const syncIntervalMinutes = Math.max(5, Math.min(1440, Number(req.body?.syncIntervalMinutes) || 30));
    const skipWeight = Math.max(-1.5, Math.min(-0.5, Number(req.body?.skipWeight) || -1));
    const belterWeight = Math.max(0.5, Math.min(1.5, Number(req.body?.belterWeight) || 1));
    // halfDecentWeight and decentWeight are always derived (skipWeight/2, belterWeight/2) — not stored
    const artistSkipRank = Math.max(0, Math.min(5, Number(req.body?.artistSkipRank) || 2));
    const artistBelterRank = Math.max(5, Math.min(10, Number(req.body?.artistBelterRank) || 8));
    const excludeUnplayedForSkipArtists = Boolean(req.body?.excludeUnplayedForSkipArtists);
    const playlistId = String(req.body?.playlistId || config.smartPlaylist?.playlistId || '').trim();
    const playlistTitle = String(req.body?.playlistTitle || 'Curatorr Smart Playlist').trim();

    const updated = {
      ...config,
      smartPlaylist: {
        ...config.smartPlaylist,
        skipThresholdSeconds, completionThresholdSeconds, songSkipLimit,
        syncIntervalMinutes, skipWeight, belterWeight,
        artistSkipRank, artistBelterRank, excludeUnplayedForSkipArtists,
        playlistId, playlistTitle,
      },
    };
    saveConfig(updated);
    return res.redirect('/settings?tab=smart-playlist&success=1');
  });

  // ── Artist filters ────────────────────────────────────────────────────────

  app.post('/settings/filters', requireSettingsAdmin, (req, res) => {
    const config = loadConfig();
    const parseCsv = (v) => String(v || '').split(/[\n,]/).map((s) => s.trim()).filter(Boolean);
    const mustInclude = parseCsv(req.body?.mustIncludeArtists);
    const neverInclude = parseCsv(req.body?.neverIncludeArtists);
    const updated = { ...config, filters: { mustIncludeArtists: mustInclude, neverIncludeArtists: neverInclude } };
    saveConfig(updated);
    return res.redirect('/settings?tab=filters&success=1');
  });

  // ── Local users ───────────────────────────────────────────────────────────

  app.post('/settings/local-users/add', requireAdmin, (req, res) => {
    const config = loadConfig();
    const username = String(req.body?.username || '').trim();
    const email = String(req.body?.email || '').trim();
    const password = String(req.body?.password || '');
    const role = ['admin', 'co-admin', 'user', 'guest', 'disabled'].includes(req.body?.role)
      ? req.body.role : 'user';

    if (!username) return res.redirect('/settings?tab=users&error=username-required');
    if (!email || !isValidEmail(email)) return res.redirect('/settings?tab=users&error=email-invalid');
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
    saveAdmins(admins);
    saveCoAdmins(coAdmins);
    return res.redirect('/settings?tab=users&success=1');
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
}
