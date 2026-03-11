// Setup wizards.
// Server wizard (admin, once): account → plex → libraries → tautulli → lidarr → done
// User wizard (every user on first login): genres-like → artists-like → genres-ignore → artists-ignore → create playlist

import {
  getUserPreferences, saveUserPreferences,
  getUserPlaylist, saveUserPlaylist,
  getPlaylistJob, savePlaylistJob, recordPlaylistSync,
  refreshMasterTracks, getMasterTracks, getMasterTrackCount,
  getGenresFromMaster, getArtistsFromMaster, dedupeMasterArtistNames, getResolvedUserArtistFilters,
  listUserGeneratedPlaylists, saveUserGeneratedPlaylist,
  clearPlaylistState,
  PRESET_VALUES,
} from '../db.js';

const SERVER_STEPS = 5;
const USER_STEPS = 6;
const activePlaylistJobs = new Set();

function buildWizardArtistOptions(artists = []) {
  return (Array.isArray(artists) ? artists : []).map((artistName) => ({
    name: artistName,
    thumb: `/api/music/thumb/artist/${encodeURIComponent(artistName)}?v=wizard-artist-thumb-1`,
  }));
}

function resolveSavedArtistPrefs(req, db, loadConfig) {
  const userId = String(req.session?.user?.username || '').trim();
  const saved = userId ? getUserPreferences(db, userId) : {};
  const config = typeof loadConfig === 'function' ? loadConfig() : {};
  const { mustIncludeArtists: likedArtists, neverIncludeArtists: ignoredArtists } = getResolvedUserArtistFilters(db, config, userId);
  return {
    likedArtists,
    ignoredArtists,
    saved,
  };
}

function resolveWizardState(req, db, loadConfig) {
  const sessionState = req.session?.userWizard && typeof req.session.userWizard === 'object'
    ? req.session.userWizard
    : {};
  const { likedArtists, ignoredArtists, saved } = resolveSavedArtistPrefs(req, db, loadConfig);
  const mergeArtists = (sessionArtists, savedArtists) => {
    const merged = [
      ...(Array.isArray(savedArtists) ? savedArtists : []),
      ...(Array.isArray(sessionArtists) ? sessionArtists : []),
    ];
    return dedupeMasterArtistNames(merged);
  };
  return {
    likedGenres: Array.isArray(sessionState.likedGenres) ? sessionState.likedGenres : (saved.likedGenres || []),
    likedArtists: mergeArtists(sessionState.likedArtists, likedArtists),
    ignoredGenres: Array.isArray(sessionState.ignoredGenres) ? sessionState.ignoredGenres : (saved.ignoredGenres || []),
    ignoredArtists: mergeArtists(sessionState.ignoredArtists, ignoredArtists),
  };
}

function storeWizardState(req, db, loadConfig, patch = {}) {
  const nextState = { ...resolveWizardState(req, db, loadConfig), ...patch };
  req.session.userWizard = nextState;
  return nextState;
}

function getWizardFilterArtistSet(req, db, loadConfig) {
  const prefs = resolveSavedArtistPrefs(req, db, loadConfig);
  const mustInclude = Array.isArray(prefs?.likedArtists) ? prefs.likedArtists : [];
  const neverInclude = Array.isArray(prefs?.ignoredArtists) ? prefs.ignoredArtists : [];
  return new Set(
    dedupeMasterArtistNames([...mustInclude, ...neverInclude]).map((artist) => String(artist || '').trim().toLowerCase())
  );
}

function filterWizardArtists(req, db, loadConfig, artists = [], wizardState = {}) {
  const blocked = getWizardFilterArtistSet(req, db, loadConfig);
  (wizardState.likedArtists || []).forEach((artist) => blocked.add(String(artist || '').trim().toLowerCase()));
  (wizardState.ignoredArtists || []).forEach((artist) => blocked.add(String(artist || '').trim().toLowerCase()));
  return dedupeMasterArtistNames(artists).filter((artist) => !blocked.has(String(artist || '').trim().toLowerCase()));
}

async function resolveWizardMachineId(ctx, url, token, machineId = '') {
  const { buildAppApiUrl, saveConfig, loadConfig } = ctx;
  let resolvedMachineId = String(machineId || '').trim();
  if (resolvedMachineId) return resolvedMachineId;
  const idUrl = buildAppApiUrl(url, '');
  idUrl.searchParams.set('X-Plex-Token', token);
  const response = await fetch(idUrl.toString(), { headers: { Accept: 'application/json' } });
  if (!response.ok) return '';
  const json = await response.json();
  resolvedMachineId = json?.MediaContainer?.machineIdentifier || '';
  if (resolvedMachineId) {
    const latest = loadConfig();
    saveConfig({ ...latest, plex: { ...latest.plex, machineId: resolvedMachineId } });
  }
  return resolvedMachineId;
}

async function buildWizardPlaylistPayload(ctx, userId, ignoredArtists = [], likedArtists = []) {
  const { db, pushLog } = ctx;
  let masterTracks = getMasterTracks(db);
  if (!masterTracks.length) {
    pushLog({ level: 'info', app: 'wizard', action: 'master.cache.wait', message: 'Master cache empty — building now before creating playlist' });
    await refreshMasterTrackCache(ctx);
    masterTracks = getMasterTracks(db);
  }

  pushLog({ level: 'info', app: 'wizard', action: 'playlist.build', message: `Building playlist from ${masterTracks.length} cached tracks` });

  const ignoredArtistSet = new Set((ignoredArtists || []).map((a) => String(a).toLowerCase()));
  const likedArtistSet = new Set((likedArtists || []).map((a) => String(a).toLowerCase()));
  const excludedArtistNames = new Set();

  const included = masterTracks.filter((track) => {
    const artist = String(track.artistName || '').toLowerCase();
    if (likedArtistSet.has(artist)) return true;
    if (ignoredArtistSet.has(artist)) {
      excludedArtistNames.add(String(track.artistName || ''));
      return false;
    }
    return true;
  });

  const ratingKeys = included.map((track) => track.ratingKey);
  pushLog({ level: 'info', app: 'wizard', action: 'playlist.tracks', message: `${ratingKeys.length} tracks included after exclusions` });

  return {
    ratingKeys,
    trackCount: ratingKeys.length,
    excludedArtists: [...excludedArtistNames].filter(Boolean).length,
    excludedTracks: Math.max(0, masterTracks.length - ratingKeys.length),
    playlistTitle: `${userId}'s Curatorred Playlist`,
  };
}

async function runWizardPlaylistJob(ctx, { user, sessionPlexToken = '', trigger = 'wizard' }) {
  const { db, loadConfig, pushLog, safeMessage, playlistService } = ctx;
  const userId = String(user?.username || '').trim();
  if (!userId || activePlaylistJobs.has(userId)) return;

  activePlaylistJobs.add(userId);
  try {
    const config = loadConfig();
    const { url, libraries: selectedKeys = [] } = config.plex || {};
    if (!url || !selectedKeys.length) {
      savePlaylistJob(db, userId, { status: 'failed', trigger, message: 'Plex is not configured for playlist creation.', errorMessage: 'Plex is not configured.', completedAt: Date.now() });
      return;
    }

    // ── Step 1: Crescive (smaller — build first) ───────────────────────────
    savePlaylistJob(db, userId, { status: 'building', trigger, message: 'Creating Crescive playlist…', startedAt: Date.now(), completedAt: null, errorMessage: '' });
    clearPlaylistState(db, userId, 'crescive');
    await playlistService.syncCrescive(userId);

    // ── Step 2: Curative (larger — replaces old curatorred playlist) ───────
    savePlaylistJob(db, userId, { status: 'building', trigger, message: 'Creating Curative playlist…', completedAt: null, errorMessage: '' });

    // If an existing curatorred playlist is in user_generated_playlists, reuse its Plex ID
    const existingCuratorred = listUserGeneratedPlaylists(db, userId, { activeOnly: false })
      .find((e) => e.playlistType === 'curatorred' || e.playlistKey === 'curatorred');
    if (existingCuratorred?.plexPlaylistId) {
      // Migrate: rename in Plex and convert record to curative
      try {
        const { url: plexUrl, token } = config.plex || {};
        const renameUrl = new URL(`${plexUrl.replace(/\/$/, '')}/playlists/${existingCuratorred.plexPlaylistId}`);
        renameUrl.searchParams.set('title', `${userId}'s Curative Playlist`);
        renameUrl.searchParams.set('X-Plex-Token', token);
        await fetch(renameUrl.toString(), { method: 'PUT', headers: { Accept: 'application/json' } });
      } catch { /* non-fatal — rename is cosmetic */ }
      saveUserGeneratedPlaylist(db, userId, {
        ...existingCuratorred,
        playlistType: 'curative',
        playlistKey: 'curative',
        playlistTitle: `${userId}'s Curative Playlist`,
        active: true,
        updatedAt: Date.now(),
      });
    }

    clearPlaylistState(db, userId, 'curative');
    await playlistService.syncCurative(userId);

    const curativeRow = listUserGeneratedPlaylists(db, userId).find((e) => e.playlistKey === 'curative');
    savePlaylistJob(db, userId, {
      status: 'completed', trigger,
      message: 'Crescive and Curative playlists created.',
      playlistId: curativeRow?.plexPlaylistId || '',
      playlistTitle: curativeRow?.playlistTitle || `${userId}'s Curative Playlist`,
      trackCount: curativeRow?.trackCount || 0,
      startedAt: getPlaylistJob(db, userId)?.started_at || Date.now(),
      completedAt: Date.now(),
      errorMessage: '',
    });
    pushLog({ level: 'info', app: 'wizard', action: 'user.playlist.synced', message: `Crescive + Curative playlists created for ${userId}` });
  } catch (err) {
    savePlaylistJob(db, userId, { status: 'failed', trigger, message: 'Playlist creation failed.', errorMessage: safeMessage(err), completedAt: Date.now() });
    pushLog({ level: 'error', app: 'wizard', action: 'user.playlist.error', message: safeMessage(err) });
  } finally {
    activePlaylistJobs.delete(userId);
  }
}

function queueWizardPlaylistJob(ctx, options) {
  setTimeout(() => {
    runWizardPlaylistJob(ctx, options).catch(() => {});
  }, 10);
}

// ── Master track cache refresh ────────────────────────────────────────────────
// Fetches all tracks from selected Plex libraries and stores in master_tracks table.
// Returns count of tracks cached.

export async function refreshMasterTrackCache(ctx) {
  const { db, loadConfig, buildAppApiUrl, pushLog, safeMessage } = ctx;
  const config = loadConfig();
  const { url, token, libraries: selectedKeys = [] } = config.plex || {};
  if (!url || !token || !selectedKeys.length) return 0;

  try {
    const tracks = [];
    for (const key of selectedKeys) {
      const u = buildAppApiUrl(url, `library/sections/${key}/all`);
      u.searchParams.set('type', '10'); // tracks
      u.searchParams.set('X-Plex-Token', token);
      const r = await fetch(u.toString(), { headers: { Accept: 'application/json' } });
      if (!r.ok) continue;
      const json = await r.json();
      for (const t of json?.MediaContainer?.Metadata || []) {
        tracks.push({
          ratingKey: String(t.ratingKey || ''),
          artistName: String(t.originalTitle || t.grandparentTitle || ''),
          trackTitle: String(t.title || ''),
          albumName: String(t.parentTitle || ''),
          genres: (t.Genre || []).map((g) => g.tag),
          libraryKey: String(key),
          ratingCount: Number(t.ratingCount || 0),
          viewCount: Number(t.viewCount || 0),
        });
      }
    }
    refreshMasterTracks(db, tracks);
    pushLog({ level: 'info', app: 'wizard', action: 'master.refresh', message: `Master track cache refreshed: ${tracks.length} tracks` });
    return tracks.length;
  } catch (err) {
    pushLog({ level: 'error', app: 'wizard', action: 'master.refresh.error', message: safeMessage(err) });
    return 0;
  }
}

// ── Tautulli webhook auto-configurator ────────────────────────────────────────
// Creates (or detects existing) Curatorr webhook notifier in Tautulli.
// Idempotent — will not create a duplicate if our webhook URL already exists.

const TAUTULLI_WEBHOOK_BODY = JSON.stringify({
  action: '{action}',
  session_key: '{session_key}',
  user: '{username}',
  user_id: '{user_id}',
  media_type: '{media_type}',
  rating_key: '{rating_key}',
  title: '{title}',
  parent_title: '{parent_title}',
  grandparent_title: '{grandparent_title}',
  original_title: '{original_title}',
  library_name: '{library_name}',
  section_id: '{section_id}',
  duration: '{duration}',
  progress_percent: '{progress_percent}',
  view_offset: '{view_offset}',
}, null, 2);

async function configureTautulliWebhook(tautulliUrl, apiKey, ctx) {
  const { loadConfig, pushLog } = ctx;
  const config = loadConfig();
  const baseUrl = (config.tautulli?.curatorrUrl || config.general?.localUrl || config.general?.remoteUrl || '').replace(/\/$/, '');
  if (!baseUrl || !tautulliUrl || !apiKey) return { ok: false, reason: 'missing config' };

  const webhookUrl = `${baseUrl}/webhook/tautulli`;
  const api = `${tautulliUrl.replace(/\/$/, '')}/api/v2`;

  // Check for existing notifier pointing to our URL
  const listRes = await fetch(`${api}?apikey=${encodeURIComponent(apiKey)}&cmd=get_notifiers`);
  const listJson = await listRes.json();
  const notifiers = listJson?.response?.data || [];

  for (const n of notifiers) {
    if (n.agent_name !== 'webhook') continue;
    // Fetch full config to check the URL
    const cfgRes = await fetch(`${api}?apikey=${encodeURIComponent(apiKey)}&cmd=get_notifier_config&notifier_id=${n.id}`);
    const cfgJson = await cfgRes.json();
    const hook = cfgJson?.response?.data?.config?.hook || '';
    if (hook === webhookUrl) {
      pushLog({ level: 'info', app: 'wizard', action: 'tautulli.webhook.exists', message: `Tautulli webhook already configured (notifier ${n.id})` });
      return { ok: true, notifierId: n.id, created: false };
    }
  }

  // Build add_notifier_config params
  const triggers = ['on_play', 'on_stop', 'on_pause', 'on_resume', 'on_watched'];
  const params = new URLSearchParams({
    apikey: apiKey,
    cmd: 'add_notifier_config',
    agent_id: '25',
    friendly_name: 'Curatorr',
    webhook_hook: webhookUrl,
    webhook_method: 'POST',
  });
  for (const t of triggers) params.set(t, '1');
  // Set JSON body for each active trigger
  for (const t of triggers) {
    params.set(`${t}_subject`, '');
    params.set(`${t}_body`, TAUTULLI_WEBHOOK_BODY);
  }

  const addRes = await fetch(`${api}`, { method: 'POST', body: params });
  const addJson = await addRes.json();
  if (addJson?.response?.result !== 'success') {
    throw new Error(addJson?.response?.message || 'Failed to add notifier');
  }

  const notifierId = addJson?.response?.data?.notifier_id;
  pushLog({ level: 'info', app: 'wizard', action: 'tautulli.webhook.created', message: `Tautulli webhook notifier created (id=${notifierId}) → ${webhookUrl}` });
  return { ok: true, notifierId, created: true };
}

export function registerWizard(app, ctx) {
  const {
    loadConfig, saveConfig,
    resolveLocalUsers, serializeLocalUsers,
    hashPassword, validateLocalPasswordStrength, setSessionUser,
    resolveUserPlexServerToken,
    fetchPlexMusicLibraries,
    buildAppApiUrl,
    normalizeBaseUrl, safeMessage, pushLog,
    DEFAULT_SMART_PLAYLIST_SETTINGS,
    db,
  } = ctx;

  // ── Server wizard entry ───────────────────────────────────────────────────

  app.get('/wizard', (req, res) => {
    const config = loadConfig();
    if (config.wizard?.completed) return res.redirect('/dashboard');
    const hasAdminAccount = resolveLocalUsers(config).length > 0;
    return renderServerWizard(res, config, hasAdminAccount ? 2 : 1, null);
  });

  // ── Server Step 1: Create admin account ───────────────────────────────────

  app.post('/wizard/account', async (req, res) => {
    const config = loadConfig();
    const username = String(req.body?.username || '').trim();
    const email = String(req.body?.email || '').trim();
    const password = String(req.body?.password || '');
    const confirm = String(req.body?.confirmPassword || '');

    if (!username) return renderServerWizard(res, config, 1, 'Username is required.');
    if (!email || !email.includes('@')) return renderServerWizard(res, config, 1, 'A valid email is required.');
    const pwError = validateLocalPasswordStrength(password);
    if (pwError) return renderServerWizard(res, config, 1, pwError);
    if (password !== confirm) return renderServerWizard(res, config, 1, 'Passwords do not match.');

    const users = resolveLocalUsers(config);
    if (users.find((u) => u.username.toLowerCase() === username.toLowerCase())) {
      return renderServerWizard(res, config, 1, 'Username already taken.');
    }

    const crypto = await import('crypto');
    const salt = crypto.randomBytes(16).toString('hex');
    const passwordHash = hashPassword(password, salt);
    const newUser = {
      username, email, role: 'admin', passwordHash, salt,
      avatar: '', createdBy: 'setup', setupAccount: true,
      systemCreated: true, createdAt: new Date().toISOString(),
    };

    const updated = { ...config, users: serializeLocalUsers([...users, newUser]) };
    saveConfig(updated);
    setSessionUser(req, newUser, 'local');
    pushLog({ level: 'info', app: 'wizard', action: 'account.created', message: `Admin account created: ${username}` });
    return renderServerWizard(res, updated, 2, null);
  });

  // ── Server Step 2: Plex connection ────────────────────────────────────────

  app.post('/wizard/plex', async (req, res) => {
    const config = loadConfig();
    const url = normalizeBaseUrl(String(req.body?.plexUrl || '').trim());
    const token = String(req.body?.plexToken || '').trim();

    if (!url) return renderServerWizard(res, config, 2, 'Plex server URL is required.');
    if (!token) return renderServerWizard(res, config, 2, 'Plex token is required.');

    let libraries = [];
    try {
      libraries = await fetchPlexMusicLibraries(url, token);
    } catch (err) {
      return renderServerWizard(res, config, 2, `Could not connect to Plex: ${safeMessage(err)}`);
    }

    if (!libraries.length) {
      return renderServerWizard(res, config, 2, 'No music libraries found. Add a music library in Plex first.');
    }

    // Also fetch machine identifier for playlist URI building
    let machineId = '';
    try {
      const idUrl = buildAppApiUrl(url, '');
      idUrl.searchParams.set('X-Plex-Token', token);
      const r = await fetch(idUrl.toString(), { headers: { Accept: 'application/json' } });
      if (r.ok) {
        const json = await r.json();
        machineId = json?.MediaContainer?.machineIdentifier || '';
      }
    } catch (_) { /* non-fatal */ }

    const updated = { ...config, plex: { ...config.plex, url, token, machineId, availableLibraries: libraries } };
    saveConfig(updated);
    pushLog({ level: 'info', app: 'wizard', action: 'plex.connected', message: `Plex connected: ${url}` });
    return renderServerWizard(res, updated, 3, null);
  });

  // ── Server Step 3: Select libraries ───────────────────────────────────────

  app.post('/wizard/libraries', (req, res) => {
    const config = loadConfig();
    const selected = parseCheckboxArray(req.body?.libraries);
    if (!selected.length) return renderServerWizard(res, config, 3, 'Select at least one music library.');

    const validKeys = new Set((config.plex?.availableLibraries || []).map((l) => l.key));
    const valid = selected.filter((k) => validKeys.has(String(k)));
    if (!valid.length) return renderServerWizard(res, config, 3, 'Invalid library selection.');

    const updated = { ...config, plex: { ...config.plex, libraries: valid } };
    saveConfig(updated);
    return renderServerWizard(res, updated, 4, null);
  });

  // ── Server Step 4: Tautulli (optional) ───────────────────────────────────

  app.post('/wizard/tautulli', async (req, res) => {
    const config = loadConfig();
    if (req.body?.skip === '1') {
      saveConfig({ ...config, tautulli: { url: '', apiKey: '' } });
      return renderServerWizard(loadConfig(), loadConfig(), 5, null);
    }

    const url = normalizeBaseUrl(String(req.body?.tautulliUrl || '').trim());
    const apiKey = String(req.body?.tautulliApiKey || '').trim();
    const curatorrUrl = normalizeBaseUrl(String(req.body?.curatorrUrl || '').trim());
    if (!url || !apiKey) return renderServerWizard(res, config, 4, 'URL and API key required (or click Skip).');

    try {
      const u = new URL(`${url}/api/v2`);
      u.searchParams.set('apikey', apiKey);
      u.searchParams.set('cmd', 'get_server_info');
      const r = await fetch(u.toString(), { headers: { Accept: 'application/json' } });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const json = await r.json();
      if (json?.response?.result !== 'success') throw new Error(json?.response?.message || 'Bad response');
    } catch (err) {
      return renderServerWizard(res, config, 4, `Could not connect to Tautulli: ${safeMessage(err)}`);
    }

    const tautulliCfg = { url, apiKey };
    if (curatorrUrl) tautulliCfg.curatorrUrl = curatorrUrl;
    const updated = { ...config, tautulli: tautulliCfg };
    saveConfig(updated);
    pushLog({ level: 'info', app: 'wizard', action: 'tautulli.connected', message: `Tautulli connected: ${url}` });

    // Auto-configure Tautulli webhook if curatorrUrl provided (non-blocking — failure is soft)
    if (curatorrUrl) configureTautulliWebhook(url, apiKey, ctx).catch(() => {});

    return renderServerWizard(res, updated, 5, null);
  });

  // ── Server Step 5: Lidarr → build master cache → complete ────────────────

  app.post('/wizard/lidarr', async (req, res) => {
    const config = loadConfig();

    if (req.body?.skip !== '1') {
      const url = normalizeBaseUrl(String(req.body?.lidarrUrl || '').trim());
      const apiKey = String(req.body?.lidarrApiKey || '').trim();
      if (!url || !apiKey) return renderServerWizard(res, config, 5, 'URL and API key required (or click Skip).');

      try {
        const r = await fetch(new URL(`${url}/api/v1/system/status`).toString(), {
          headers: { Accept: 'application/json', 'X-Api-Key': apiKey },
        });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
      } catch (err) {
        return renderServerWizard(res, config, 5, `Could not connect to Lidarr: ${safeMessage(err)}`);
      }

      saveConfig({ ...loadConfig(), lidarr: { url, apiKey } });
      pushLog({ level: 'info', app: 'wizard', action: 'lidarr.connected', message: `Lidarr connected: ${url}` });
    } else {
      saveConfig({ ...config, lidarr: { url: '', apiKey: '' } });
    }

    // Mark server wizard complete
    saveConfig({ ...loadConfig(), wizard: { completed: true, completedAt: new Date().toISOString() } });

    // Build master track cache (background — non-blocking response)
    refreshMasterTrackCache(ctx).catch(() => {});

    // Schedule periodic refresh every 6 hours
    setInterval(() => refreshMasterTrackCache(ctx).catch(() => {}), 6 * 60 * 60 * 1000).unref();

    pushLog({ level: 'info', app: 'wizard', action: 'server.complete', message: 'Server wizard completed — building master track cache' });
    return res.redirect('/wizard/user');
  });

  // ── Back navigation (server wizard) ──────────────────────────────────────

  app.post('/wizard/back', (req, res) => {
    const config = loadConfig();
    const minimumStep = resolveLocalUsers(config).length > 0 ? 2 : 1;
    const step = Math.max(minimumStep, Number(req.body?.step || minimumStep) - 1);
    return renderServerWizard(res, config, step, null);
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // USER WIZARD
  // ═══════════════════════════════════════════════════════════════════════════

  app.get('/wizard/user', async (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const config = loadConfig();
    if (!config.wizard?.completed) return res.redirect('/wizard');
    req.session.userWizard = resolveWizardState(req, db, loadConfig);
    let genres = getGenresFromMaster(db);
    if (!genres.length && getMasterTrackCount(db) === 0) {
      await refreshMasterTrackCache(ctx).catch(() => {});
      genres = getGenresFromMaster(db);
    }
    return renderUserWizard(res, req, 1, null, { genres });
  });

  // Step 1: Genres you like
  app.post('/wizard/user/genres-like', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const selected = parseCheckboxArray(req.body?.genres);
    const wizardState = storeWizardState(req, db, loadConfig, { likedGenres: selected });
    const artists = filterWizardArtists(req, db, loadConfig, getArtistsFromMaster(db, selected), wizardState);
    return renderUserWizard(res, req, 2, null, { artistOptions: buildWizardArtistOptions(artists), likedGenres: selected });
  });

  // Step 2: Artists you like (from liked genres)
  app.post('/wizard/user/artists-like', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const selected = parseCheckboxArray(req.body?.artists);
    const wizardState = storeWizardState(req, db, loadConfig, { likedArtists: selected });
    const likedGenres = wizardState.likedGenres || [];
    // All genres for ignore step (excluding already-liked)
    const allGenres = getGenresFromMaster(db).filter((g) => !likedGenres.includes(g));
    return renderUserWizard(res, req, 3, null, { genres: allGenres });
  });

  // Step 3: Genres to ignore
  app.post('/wizard/user/genres-ignore', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const selected = parseCheckboxArray(req.body?.genres);
    const wizardState = storeWizardState(req, db, loadConfig, { ignoredGenres: selected });
    const artists = filterWizardArtists(req, db, loadConfig, getArtistsFromMaster(db, selected), wizardState);
    return renderUserWizard(res, req, 4, null, { artistOptions: buildWizardArtistOptions(artists) });
  });

  // Step 4: Artists to ignore (from ignored genres, not already liked)
  app.post('/wizard/user/artists-ignore', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const selected = parseCheckboxArray(req.body?.artists);
    storeWizardState(req, db, loadConfig, { ignoredArtists: selected });
    return renderUserWizard(res, req, 5, null, {});
  });

  // Step 5: Curation preset
  app.post('/wizard/user/preset', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const userId = req.session.user.username;
    const preset = String(req.body?.preset || '').trim();
    if (PRESET_VALUES[preset]) {
      const prefs = getUserPreferences(db, userId);
      saveUserPreferences(db, userId, { ...prefs, smartConfig: { preset } });
      pushLog({ level: 'info', app: 'wizard', action: 'preset.selected', message: `Smart playlist preset "${preset}" set for ${userId}` });
    }
    const masterCount = getMasterTrackCount(db);
    return renderUserWizard(res, req, 6, null, { masterCount });
  });

  // Step 6: Create playlist
  app.post('/wizard/user/create-playlist', async (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const user = req.session.user;
    const userId = user.username;
    const w = req.session.userWizard || {};

    const likedGenres = w.likedGenres || [];
    const likedArtists = w.likedArtists || [];
    const ignoredGenres = w.ignoredGenres || [];
    const ignoredArtists = w.ignoredArtists || [];

    // ignoredGenres is only used to build the artist exclusion list — not stored as a filter
    saveUserPreferences(db, userId, { likedGenres, ignoredGenres: [], likedArtists, ignoredArtists, userWizardCompleted: true });

    const existingPlaylist = getUserPlaylist(db, userId);
    savePlaylistJob(db, userId, {
      status: 'queued',
      trigger: 'wizard',
      message: existingPlaylist?.playlist_id ? 'Wizard preferences saved. Playlist rebuild queued.' : 'Wizard preferences saved. Playlist creation queued.',
      playlistId: existingPlaylist?.playlist_id || '',
      playlistTitle: existingPlaylist?.playlist_title || `${user.username}'s Curatorred Playlist`,
      trackCount: 0,
      errorMessage: '',
      startedAt: Date.now(),
      completedAt: null,
    });
    queueWizardPlaylistJob(ctx, {
      user,
      sessionPlexToken: req.session?.plexServerToken || '',
      trigger: 'wizard',
    });

    delete req.session.userWizard;
    pushLog({ level: 'info', app: 'wizard', action: 'user.complete', message: `User wizard completed by ${userId}` });
    return res.redirect('/playlists?wizardJob=queued');
  });

  // User wizard back
  app.post('/wizard/user/back', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const step = Math.max(1, Number(req.body?.step || 1) - 1);
    const w = storeWizardState(req, db, loadConfig);
    const extra = {};
    if (step === 1) extra.genres = getGenresFromMaster(db);
    if (step === 2) extra.artistOptions = buildWizardArtistOptions(filterWizardArtists(req, db, loadConfig, getArtistsFromMaster(db, w.likedGenres || []), w));
    if (step === 3) extra.genres = getGenresFromMaster(db).filter((g) => !(w.likedGenres || []).includes(g));
    if (step === 4) extra.artistOptions = buildWizardArtistOptions(filterWizardArtists(req, db, loadConfig, getArtistsFromMaster(db, w.ignoredGenres || []), w));
    return renderUserWizard(res, req, step, null, extra);
  });

  // ── Misc wizard APIs ──────────────────────────────────────────────────────

  app.post('/api/wizard/test-plex', async (req, res) => {
    const url = normalizeBaseUrl(String(req.body?.url || '').trim());
    const token = String(req.body?.token || '').trim();
    if (!url || !token) return res.status(400).json({ error: 'URL and token required.' });
    try {
      const libs = await fetchPlexMusicLibraries(url, token);
      return res.json({ ok: true, musicLibraries: libs });
    } catch (err) {
      return res.status(400).json({ error: safeMessage(err) });
    }
  });

  // Auto-configure Tautulli webhook (admin, callable from settings)
  app.post('/api/wizard/configure-tautulli-webhook', async (req, res) => {
    if (!req.session?.user) return res.status(401).json({ error: 'Auth required.' });
    const config = loadConfig();
    const { url, apiKey } = config.tautulli || {};
    if (!url || !apiKey) return res.status(400).json({ error: 'Tautulli not configured.' });
    try {
      const result = await configureTautulliWebhook(url, apiKey, ctx);
      return res.json(result);
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // Manual master cache refresh (admin)
  app.post('/api/wizard/refresh-master', async (req, res) => {
    if (!req.session?.user) return res.status(401).json({ error: 'Auth required.' });
    try {
      const count = await refreshMasterTrackCache(ctx);
      return res.json({ ok: true, count });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // Rebuild user playlist from master cache (re-adds tracks using correct URI format)
  app.post('/api/wizard/rebuild-playlist', async (req, res) => {
    if (!req.session?.user) return res.status(401).json({ error: 'Auth required.' });
    const userId = req.session.user.username;
    const playlistRow = getUserPlaylist(db, userId);
    if (!playlistRow?.playlist_id) return res.status(400).json({ error: 'No playlist found — complete the user wizard first.' });

    savePlaylistJob(db, userId, {
      status: 'queued',
      trigger: 'manual',
      message: 'Playlist rebuild queued.',
      playlistId: playlistRow.playlist_id,
      playlistTitle: playlistRow.playlist_title,
      trackCount: 0,
      errorMessage: '',
      startedAt: Date.now(),
      completedAt: null,
    });
    queueWizardPlaylistJob(ctx, {
      user: req.session.user,
      sessionPlexToken: req.session?.plexServerToken || '',
      trigger: 'manual',
    });
    return res.json({ ok: true, queued: true });
  });
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function parseCheckboxArray(val) {
  if (!val) return [];
  return (Array.isArray(val) ? val : [val]).map((v) => String(v).trim()).filter(Boolean);
}

function sanitizeWizardConfig(config) {
  return {
    plex: {
      url: config.plex?.url || '',
      tokenSet: Boolean(config.plex?.token),
      libraries: config.plex?.libraries || [],
      availableLibraries: config.plex?.availableLibraries || [],
    },
    tautulli: { url: config.tautulli?.url || '', apiKeySet: Boolean(config.tautulli?.apiKey) },
    lidarr: { url: config.lidarr?.url || '', apiKeySet: Boolean(config.lidarr?.apiKey) },
  };
}

function renderServerWizard(res, config, step, error) {
  return res.render('wizard', {
    title: 'Curatorr Setup',
    wizardType: 'server',
    step,
    totalSteps: SERVER_STEPS,
    config: sanitizeWizardConfig(config),
    error,
    extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
  });
}

function renderUserWizard(res, req, step, error, extra) {
  return res.render('wizard-user', {
    title: 'Personalise Curatorr',
    wizardType: 'user',
    step,
    totalSteps: USER_STEPS,
    user: req.session?.user || {},
    wizardState: req.session?.userWizard || {},
    error,
    extra: extra || {},
    extraCss: ['/styles-layout.css', '/styles-curatorr.css', '/styles-settings.css'],
  });
}
