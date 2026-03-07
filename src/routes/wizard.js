// Setup wizards.
// Server wizard (admin, once): account → plex → libraries → tautulli → lidarr → done
// User wizard (every user on first login): genres-like → artists-like → genres-ignore → artists-ignore → create playlist

import {
  getUserPreferences, saveUserPreferences,
  getUserPlaylist, saveUserPlaylist,
  refreshMasterTracks, getMasterTracks, getMasterTrackCount,
  getGenresFromMaster, getArtistsFromMaster,
} from '../db.js';

const SERVER_STEPS = 5;
const USER_STEPS = 6;

const PRESET_VALUES = {
  cautious:   { skipThresholdSeconds: 20, completionThresholdSeconds: 20, skipWeight: -0.5, belterWeight: 0.5,  artistSkipRank: 1, artistBelterRank: 9, songSkipLimit: 3 },
  measured:   { skipThresholdSeconds: 30, completionThresholdSeconds: 30, skipWeight: -1,   belterWeight: 1,    artistSkipRank: 2, artistBelterRank: 8, songSkipLimit: 2 },
  aggressive: { skipThresholdSeconds: 40, completionThresholdSeconds: 40, skipWeight: -1.5, belterWeight: 1.5,  artistSkipRank: 3, artistBelterRank: 7, songSkipLimit: 1 },
};

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
  const baseUrl = (config.tautulli?.curatorrUrl || '').replace(/\/$/, '');
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
    return renderServerWizard(res, config, 1, null);
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
    const step = Math.max(1, Number(req.body?.step || 1) - 1);
    return renderServerWizard(res, config, step, null);
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // USER WIZARD
  // ═══════════════════════════════════════════════════════════════════════════

  app.get('/wizard/user', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const config = loadConfig();
    if (!config.wizard?.completed) return res.redirect('/wizard');
    const genres = getGenresFromMaster(db);
    return renderUserWizard(res, req, 1, null, { genres });
  });

  // Step 1: Genres you like
  app.post('/wizard/user/genres-like', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const selected = parseCheckboxArray(req.body?.genres);
    req.session.userWizard = { ...req.session.userWizard, likedGenres: selected };
    // Get artists from liked genres
    const artists = getArtistsFromMaster(db, selected);
    return renderUserWizard(res, req, 2, null, { artists, likedGenres: selected });
  });

  // Step 2: Artists you like (from liked genres)
  app.post('/wizard/user/artists-like', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const selected = parseCheckboxArray(req.body?.artists);
    req.session.userWizard = { ...req.session.userWizard, likedArtists: selected };
    const likedGenres = req.session.userWizard?.likedGenres || [];
    // All genres for ignore step (excluding already-liked)
    const allGenres = getGenresFromMaster(db).filter((g) => !likedGenres.includes(g));
    return renderUserWizard(res, req, 3, null, { genres: allGenres });
  });

  // Step 3: Genres to ignore
  app.post('/wizard/user/genres-ignore', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const selected = parseCheckboxArray(req.body?.genres);
    req.session.userWizard = { ...req.session.userWizard, ignoredGenres: selected };
    const w = req.session.userWizard || {};
    // Artists in ignored genres, excluding already-liked artists
    const likedArtistSet = new Set(w.likedArtists || []);
    const artists = getArtistsFromMaster(db, selected).filter((a) => !likedArtistSet.has(a));
    return renderUserWizard(res, req, 4, null, { artists });
  });

  // Step 4: Artists to ignore (from ignored genres, not already liked)
  app.post('/wizard/user/artists-ignore', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const selected = parseCheckboxArray(req.body?.artists);
    req.session.userWizard = { ...req.session.userWizard, ignoredArtists: selected };
    const masterCount = getMasterTrackCount(db);
    return renderUserWizard(res, req, 5, null, { masterCount });
  });

  // Step 5: Create playlist
  app.post('/wizard/user/create-playlist', async (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const config = loadConfig();
    const user = req.session.user;
    const userId = user.username;
    const w = req.session.userWizard || {};

    const likedGenres = w.likedGenres || [];
    const likedArtists = w.likedArtists || [];
    const ignoredGenres = w.ignoredGenres || [];
    const ignoredArtists = w.ignoredArtists || [];

    // ignoredGenres is only used to build the artist exclusion list — not stored as a filter
    saveUserPreferences(db, userId, { likedGenres, ignoredGenres: [], likedArtists, ignoredArtists, userWizardCompleted: true });

    const { url, token, machineId = '', libraries: selectedKeys = [] } = config.plex || {};
    const playlistTitle = `${user.username}'s Curatorred Playlist`;

    if (url && token && selectedKeys.length) {
      try {
        // Resolve machineId — if missing from config, fetch it now
        let resolvedMachineId = machineId;
        if (!resolvedMachineId) {
          try {
            const idUrl = buildAppApiUrl(url, '');
            idUrl.searchParams.set('X-Plex-Token', token);
            const r = await fetch(idUrl.toString(), { headers: { Accept: 'application/json' } });
            if (r.ok) {
              const json = await r.json();
              resolvedMachineId = json?.MediaContainer?.machineIdentifier || '';
              if (resolvedMachineId) {
                saveConfig({ ...loadConfig(), plex: { ...loadConfig().plex, machineId: resolvedMachineId } });
              }
            }
          } catch (_) { /* non-fatal */ }
        }

        if (!resolvedMachineId) {
          throw new Error('Could not determine Plex machine ID — check your Plex server connection.');
        }

        // Ensure master track cache is populated (may not be ready if wizard was just completed)
        let masterTracks = getMasterTracks(db);
        if (!masterTracks.length) {
          pushLog({ level: 'info', app: 'wizard', action: 'master.cache.wait', message: 'Master cache empty — building now before creating playlist' });
          await refreshMasterTrackCache(ctx);
          masterTracks = getMasterTracks(db);
        }

        pushLog({ level: 'info', app: 'wizard', action: 'playlist.build', message: `Building playlist from ${masterTracks.length} cached tracks` });

        const ignoredArtistSet = new Set(ignoredArtists.map((a) => a.toLowerCase()));
        const likedArtistSet = new Set(likedArtists.map((a) => a.toLowerCase()));

        const included = masterTracks.filter((t) => {
          const artist = t.artistName.toLowerCase();
          if (likedArtistSet.has(artist)) return true;
          if (ignoredArtistSet.has(artist)) return false;
          return true;
        });
        const ratingKeys = included.map((t) => t.ratingKey);

        pushLog({ level: 'info', app: 'wizard', action: 'playlist.tracks', message: `${ratingKeys.length} tracks included after exclusions` });

        // Create empty playlist
        const createUrl = buildAppApiUrl(url, 'playlists');
        createUrl.searchParams.set('type', 'audio');
        createUrl.searchParams.set('title', playlistTitle);
        createUrl.searchParams.set('smart', '0');
        createUrl.searchParams.set('uri', `server://${resolvedMachineId}/com.plexapp.plugins.library`);
        createUrl.searchParams.set('X-Plex-Token', token);
        const createRes = await fetch(createUrl.toString(), { method: 'POST', headers: { Accept: 'application/json' } });
        if (!createRes.ok) throw new Error(`Create playlist failed: HTTP ${createRes.status}`);
        const createJson = await createRes.json();
        const playlistId = createJson?.MediaContainer?.Metadata?.[0]?.ratingKey;

        if (!playlistId) throw new Error('Plex did not return a playlist ID after creation');

        if (ratingKeys.length) {
          const base = url.replace(/\/$/, '');
          // Add tracks in batches of 100 (conservative to avoid URI length limits)
          for (let i = 0; i < ratingKeys.length; i += 100) {
            const batch = ratingKeys.slice(i, i + 100);
            const uri = `server://${resolvedMachineId}/com.plexapp.plugins.library/library/metadata/${batch.join(',')}`;
            const addUrl = new URL(`${base}/playlists/${playlistId}/items`);
            addUrl.searchParams.set('uri', uri);
            addUrl.searchParams.set('X-Plex-Token', token);
            const addRes = await fetch(addUrl.toString(), { method: 'PUT', headers: { Accept: 'application/json' } });
            if (!addRes.ok) {
              pushLog({ level: 'warn', app: 'wizard', action: 'playlist.add.warn', message: `Batch add returned HTTP ${addRes.status} at offset ${i}` });
            }
          }
        }

        saveUserPlaylist(db, userId, String(playlistId), playlistTitle);
        pushLog({ level: 'info', app: 'wizard', action: 'user.playlist.created', message: `Created "${playlistTitle}" with ${ratingKeys.length} tracks for ${userId}` });
      } catch (err) {
        pushLog({ level: 'error', app: 'wizard', action: 'user.playlist.error', message: safeMessage(err) });
      }
    }

    delete req.session.userWizard;
    pushLog({ level: 'info', app: 'wizard', action: 'user.complete', message: `User wizard completed by ${userId}` });
    return res.redirect('/dashboard');
  });

  // User wizard back
  app.post('/wizard/user/back', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    const step = Math.max(1, Number(req.body?.step || 1) - 1);
    const w = req.session.userWizard || {};
    const extra = {};
    if (step === 1) extra.genres = getGenresFromMaster(db);
    if (step === 2) extra.artists = getArtistsFromMaster(db, w.likedGenres || []);
    if (step === 3) extra.genres = getGenresFromMaster(db).filter((g) => !(w.likedGenres || []).includes(g));
    if (step === 4) extra.artists = getArtistsFromMaster(db, w.ignoredGenres || []).filter((a) => !(w.likedArtists || []).includes(a));
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
    const config = loadConfig();
    const userId = req.session.user.username;
    const { url, token, machineId = '' } = config.plex || {};

    if (!url || !token) return res.status(400).json({ error: 'Plex not configured.' });

    try {
      const playlistRow = getUserPlaylist(db, userId);
      if (!playlistRow?.playlist_id) return res.status(400).json({ error: 'No playlist found — complete the user wizard first.' });
      const playlistId = playlistRow.playlist_id;

      const prefs = getUserPreferences(db, userId);
      const ignoredArtistSet = new Set((prefs.ignoredArtists || []).map((a) => a.toLowerCase()));
      const likedArtistSet = new Set((prefs.likedArtists || []).map((a) => a.toLowerCase()));

      // Ensure master cache is populated
      let masterTracks = getMasterTracks(db);
      if (!masterTracks.length) {
        await refreshMasterTrackCache(ctx);
        masterTracks = getMasterTracks(db);
      }

      const included = masterTracks.filter((t) => {
        const artist = t.artistName.toLowerCase();
        if (likedArtistSet.has(artist)) return true;
        if (ignoredArtistSet.has(artist)) return false;
        return true;
      });
      const ratingKeys = included.map((t) => t.ratingKey);

      // Resolve machineId
      let mid = machineId;
      if (!mid) {
        const idUrl = buildAppApiUrl(url, '');
        idUrl.searchParams.set('X-Plex-Token', token);
        const r = await fetch(idUrl.toString(), { headers: { Accept: 'application/json' } });
        if (r.ok) mid = (await r.json())?.MediaContainer?.machineIdentifier || '';
      }
      if (!mid) return res.status(500).json({ error: 'Could not determine Plex machine ID.' });

      const base = url.replace(/\/$/, '');

      // Clear existing playlist items
      const clearUrl = new URL(`${base}/playlists/${playlistId}/items`);
      clearUrl.searchParams.set('X-Plex-Token', token);
      await fetch(clearUrl.toString(), { method: 'DELETE', headers: { Accept: 'application/json' } });

      // Add tracks in batches of 100
      for (let i = 0; i < ratingKeys.length; i += 100) {
        const batch = ratingKeys.slice(i, i + 100);
        const uri = `server://${mid}/com.plexapp.plugins.library/library/metadata/${batch.join(',')}`;
        const addUrl = new URL(`${base}/playlists/${playlistId}/items`);
        addUrl.searchParams.set('uri', uri);
        addUrl.searchParams.set('X-Plex-Token', token);
        await fetch(addUrl.toString(), { method: 'PUT', headers: { Accept: 'application/json' } });
      }

      pushLog({ level: 'info', app: 'wizard', action: 'playlist.rebuilt', message: `Rebuilt playlist for ${userId} with ${ratingKeys.length} tracks` });
      return res.json({ ok: true, trackCount: ratingKeys.length });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
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
    extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
  });
}
