// Plex API proxy routes — library browsing, playlist management, user list

export function registerApiPlex(app, ctx) {
  const {
    requireUser,
    requireAdmin,
    loadConfig,
    saveConfig,
    buildAppApiUrl,
    buildPlexAuthHeaders,
    safeMessage,
    pushLog,
    fetchPlexLibraries,
    fetchPlexMusicLibraries,
    fetchPlexPlaylistsForToken,
    parsePlexUsers,
  } = ctx;

  // ── Library listing ───────────────────────────────────────────────────────

  app.get('/api/plex/libraries', requireUser, async (_req, res) => {
    const config = loadConfig();
    const { url, token } = config.plex || {};
    if (!url || !token) return res.status(400).json({ error: 'Plex not configured.' });
    try {
      const libs = await fetchPlexMusicLibraries(url, token);
      return res.json({ ok: true, libraries: libs });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── All tracks in selected libraries ─────────────────────────────────────

  app.get('/api/plex/tracks', requireUser, async (req, res) => {
    const config = loadConfig();
    const { url, token, libraries: selectedKeys = [] } = config.plex || {};
    if (!url || !token) return res.status(400).json({ error: 'Plex not configured.' });

    const libraryKey = String(req.query?.libraryKey || '').trim();
    const keysToFetch = libraryKey ? [libraryKey] : selectedKeys;

    if (!keysToFetch.length) return res.json({ ok: true, tracks: [] });

    try {
      const allTracks = [];
      for (const key of keysToFetch) {
        const tracksUrl = buildAppApiUrl(url, `library/sections/${key}/all`);
        tracksUrl.searchParams.set('type', '10'); // type 10 = track
        const fetchRes = await fetch(tracksUrl.toString(), {
          headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
        });
        if (!fetchRes.ok) continue;
        const json = await fetchRes.json();
        const items = json?.MediaContainer?.Metadata || [];
        items.forEach((t) => allTracks.push({
          ratingKey: String(t.ratingKey || ''),
          title: String(t.title || ''),
          artistName: String(t.grandparentTitle || ''),
          albumName: String(t.parentTitle || ''),
          duration: Number(t.duration || 0),
          thumb: t.thumb || '',
        }));
      }
      return res.json({ ok: true, tracks: allTracks });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Active sessions (music only, selected libraries) ─────────────────────

  app.get('/api/plex/sessions', requireUser, async (_req, res) => {
    const config = loadConfig();
    const { url, token, libraries: selectedKeys = [] } = config.plex || {};
    if (!url || !token) return res.status(400).json({ error: 'Plex not configured.' });
    try {
      const sessionsUrl = buildAppApiUrl(url, 'status/sessions');
      const fetchRes = await fetch(sessionsUrl.toString(), {
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
      if (!fetchRes.ok) return res.status(502).json({ error: 'Plex returned ' + fetchRes.status });
      const json = await fetchRes.json();
      const all = json?.MediaContainer?.Metadata || [];
      // Filter: music tracks (type=track) in selected libraries (if any selected)
      const sessions = all
        .filter((s) => s.type === 'track')
        .filter((s) => !selectedKeys.length || selectedKeys.includes(String(s.librarySectionID || '')))
        .map((s) => ({
          key: String(s.ratingKey || ''),
          title: String(s.title || ''),
          artist: String(s.grandparentTitle || ''),
          album: String(s.parentTitle || ''),
          albumThumb: s.parentThumb || s.thumb || '',
          artistThumb: s.grandparentThumb || '',
          duration: Number(s.duration || 0),
          viewOffset: Number(s.viewOffset || 0),
          state: String(s.Player?.state || 'playing'),
          playerTitle: String(s.Player?.title || ''),
          userName: String(s.User?.title || ''),
          userThumb: String(s.User?.thumb || ''),
        }));
      return res.json({ ok: true, sessions });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Art proxy (forwards Plex image with token, avoids exposing token to browser) ──

  app.get('/api/plex/art', requireUser, async (req, res) => {
    const config = loadConfig();
    const { url, token } = config.plex || {};
    if (!url || !token) return res.status(400).end();
    const path = String(req.query?.path || '').trim();
    if (!path || !path.startsWith('/')) return res.status(400).end();
    try {
      const artUrl = buildAppApiUrl(url, path.replace(/^\//, ''));
      const upstream = await fetch(artUrl.toString(), {
        headers: buildPlexAuthHeaders(token, { Accept: 'image/*,*/*' }),
      });
      if (!upstream.ok) return res.status(upstream.status).end();
      const ct = upstream.headers.get('content-type') || 'image/jpeg';
      const buf = Buffer.from(await upstream.arrayBuffer());
      res.set('Content-Type', ct);
      res.set('Cache-Control', 'public, max-age=3600');
      res.end(buf);
    } catch (err) {
      return res.status(500).end();
    }
  });

  // ── Playlists ─────────────────────────────────────────────────────────────

  app.get('/api/plex/playlists', requireUser, async (_req, res) => {
    const config = loadConfig();
    const { url, token } = config.plex || {};
    if (!url || !token) return res.status(400).json({ error: 'Plex not configured.' });
    try {
      const playlists = await fetchPlexPlaylistsForToken(url, token);
      return res.json({ ok: true, playlists });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // Register an existing playlist into managedPlaylists (used to migrate legacy entries)
  app.post('/api/plex/managed-playlists', requireAdmin, (req, res) => {
    const ratingKey = String(req.body?.ratingKey || '').trim();
    const title = String(req.body?.title || '').trim();
    if (!ratingKey || !title) return res.status(400).json({ error: 'ratingKey and title required.' });
    const cfg = loadConfig();
    const managed = Array.isArray(cfg.smartPlaylist?.managedPlaylists) ? cfg.smartPlaylist.managedPlaylists : [];
    if (!managed.find((p) => String(p.ratingKey) === ratingKey)) {
      saveConfig({ ...cfg, smartPlaylist: { ...cfg.smartPlaylist, managedPlaylists: [...managed, { ratingKey, title }] } });
    }
    return res.json({ ok: true });
  });

  // Create a new Plex playlist
  app.post('/api/plex/playlists', requireAdmin, async (req, res) => {
    const config = loadConfig();
    const { url, token, machineId } = config.plex || {};
    if (!url || !token) return res.status(400).json({ error: 'Plex not configured.' });
    const title = String(req.body?.title || '').trim();
    if (!title) return res.status(400).json({ error: 'Playlist title is required.' });
    try {
      const createUrl = buildAppApiUrl(url, 'playlists');
      createUrl.searchParams.set('type', 'audio');
      createUrl.searchParams.set('title', title);
      createUrl.searchParams.set('smart', '0');
      createUrl.searchParams.set('uri', `server://${machineId || ''}/com.plexapp.plugins.library`);
      const createRes = await fetch(createUrl.toString(), {
        method: 'POST',
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
      if (!createRes.ok) throw new Error(`HTTP ${createRes.status}`);
      const json = await createRes.json();
      const playlist = json?.MediaContainer?.Metadata?.[0];
      const newEntry = { ratingKey: String(playlist?.ratingKey || ''), title: String(playlist?.title || '') };
      // Track Curatorr-created playlists in config
      const cfg2 = loadConfig();
      const managed = Array.isArray(cfg2.smartPlaylist?.managedPlaylists) ? cfg2.smartPlaylist.managedPlaylists : [];
      if (newEntry.ratingKey && !managed.find((p) => String(p.ratingKey) === newEntry.ratingKey)) {
        saveConfig({ ...cfg2, smartPlaylist: { ...cfg2.smartPlaylist, managedPlaylists: [...managed, newEntry] } });
      }
      return res.json({ ok: true, playlist: newEntry });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Users ─────────────────────────────────────────────────────────────────

  app.get('/api/plex/users', requireAdmin, async (_req, res) => {
    const config = loadConfig();
    const { token, machineId } = config.plex || {};
    if (!token) return res.status(400).json({ error: 'Plex not configured.' });
    try {
      const usersRes = await fetch('https://plex.tv/api/users', {
        headers: { Accept: 'application/xml', 'X-Plex-Token': token },
      });
      if (!usersRes.ok) throw new Error(`HTTP ${usersRes.status}`);
      const xmlText = await usersRes.text();
      const users = parsePlexUsers(xmlText, { machineId: machineId || '' });
      return res.json({ ok: true, users });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Genres from selected music libraries ─────────────────────────────────

  app.get('/api/plex/genres', requireUser, async (_req, res) => {
    const config = loadConfig();
    const { url, token, libraries: selectedKeys = [] } = config.plex || {};
    if (!url || !token) return res.status(400).json({ error: 'Plex not configured.' });
    try {
      const genreMap = new Map();
      for (const key of selectedKeys) {
        const u = buildAppApiUrl(url, `library/sections/${key}/genre`);
        const r = await fetch(u.toString(), {
          headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
        });
        if (!r.ok) continue;
        const json = await r.json();
        for (const g of json?.MediaContainer?.Directory || []) {
          genreMap.set(g.title, { id: g.key, title: g.title });
        }
      }
      return res.json({ ok: true, genres: [...genreMap.values()].sort((a, b) => a.title.localeCompare(b.title)) });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Artists filtered by genre(s) ──────────────────────────────────────────

  app.get('/api/plex/artists-by-genre', requireUser, async (req, res) => {
    const config = loadConfig();
    const { url, token, libraries: selectedKeys = [] } = config.plex || {};
    if (!url || !token) return res.status(400).json({ error: 'Plex not configured.' });
    const genres = req.query.genre ? (Array.isArray(req.query.genre) ? req.query.genre : [req.query.genre]) : [];
    try {
      const artistMap = new Map();
      for (const key of selectedKeys) {
        const u = buildAppApiUrl(url, `library/sections/${key}/all`);
        u.searchParams.set('type', '8'); // 8 = artist
        if (genres.length) {
          for (const g of genres) u.append('genre', g);
        }
        const r = await fetch(u.toString(), {
          headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
        });
        if (!r.ok) continue;
        const json = await r.json();
        for (const a of json?.MediaContainer?.Metadata || []) {
          artistMap.set(a.title, { title: a.title, thumb: a.thumb || null });
        }
      }
      return res.json({ ok: true, artists: [...artistMap.values()].sort((a, b) => a.title.localeCompare(b.title)) });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Token / Machine fetch (for settings UI) ───────────────────────────────

  app.get('/api/plex/token', requireAdmin, (req, res) => {
    const config = loadConfig();
    const token = String(config.plex?.token || '').trim();
    if (!token) return res.status(400).json({ error: 'No Plex token saved yet.' });
    return res.json({ token });
  });

  app.get('/api/plex/machine', requireAdmin, async (_req, res) => {
    const config = loadConfig();
    const { url, localUrl, token } = config.plex || {};
    const base = (localUrl || url || '').replace(/\/$/, '');
    if (!base || !token) return res.status(400).json({ error: 'Plex URL and token required.' });
    try {
      const r = await fetch(base, {
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
      if (!r.ok) throw new Error(`Plex returned ${r.status}`);
      const machineId = (await r.json())?.MediaContainer?.machineIdentifier || '';
      if (!machineId) return res.status(502).json({ error: 'Could not read machine identifier from Plex.' });
      return res.json({ machineId });
    } catch (err) {
      return res.status(502).json({ error: safeMessage(err) });
    }
  });

  // ── Connection test ───────────────────────────────────────────────────────

  app.post('/api/plex/test', requireAdmin, async (req, res) => {
    const { normalizeBaseUrl } = ctx;
    const url = normalizeBaseUrl(String(req.body?.url || '').trim());
    const token = String(req.body?.token || '').trim();
    if (!url || !token) return res.status(400).json({ error: 'URL and token required.' });
    try {
      const libs = await fetchPlexMusicLibraries(url, token);
      return res.json({ ok: true, musicLibraryCount: libs.length, libraries: libs });
    } catch (err) {
      return res.status(400).json({ error: safeMessage(err) });
    }
  });
}
