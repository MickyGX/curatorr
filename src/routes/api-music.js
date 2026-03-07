// Music stats, smart playlist management, and Lidarr integration

import {
  getTopArtists,
  getTopTracks,
  getRecentHistory,
  getPlayStats,
  getPlayStatsByDay,
  getExcludedTrackKeys,
  getExcludedArtists,
  getSkipTierArtists,
  getPlayedTrackKeys,
  getAllUserIds,
  recordPlaylistSync,
  getLastPlaylistSync,
  setTrackExclusion,
  setTrackInclusion,
  setArtistExclusion,
  resetTrackSkipStreak,
  resetArtistSkipStreak,
  getUserPlaylist,
  getUserPreferences,
  getMasterTracks,
} from '../db.js';

// ── Smart playlist rebuild ────────────────────────────────────────────────────
// Called after skip events (debounced) and on demand.

export async function rebuildSmartPlaylist(ctx, userPlexId) {
  const { db, loadConfig, pushLog, safeMessage } = ctx;
  const config = loadConfig();
  const { url, token, machineId = '' } = config.plex || {};

  if (!url || !token) return;

  // Look up this user's personal playlist
  const playlistRow = getUserPlaylist(db, userPlexId);
  if (!playlistRow?.playlist_id) return; // user hasn't completed wizard yet

  const playlistId = playlistRow.playlist_id;
  const prefs = getUserPreferences(db, userPlexId);
  const ignoredArtistSet = new Set((prefs.ignoredArtists || []).map((a) => a.toLowerCase()));
  const likedArtistSet = new Set((prefs.likedArtists || []).map((a) => a.toLowerCase()));

  try {
    // Use master cache — no Plex API call needed
    const masterTracks = getMasterTracks(db);
    if (!masterTracks.length) return; // cache not ready

    const smartSettings = config.smartPlaylist || {};
    const artistSkipRankThreshold = smartSettings.artistSkipRank ?? 2;
    const excludeUnplayedForSkipArtists = Boolean(smartSettings.excludeUnplayedForSkipArtists);

    // Get skip-based exclusions from play stats
    const excludedKeys = new Set(getExcludedTrackKeys(db, userPlexId));
    const excludedArtists = new Set(getExcludedArtists(db, userPlexId).map((a) => a.toLowerCase()));
    // Artists whose ranking_score has fallen to or below the skip threshold
    const skipTierArtists = new Set(getSkipTierArtists(db, userPlexId, artistSkipRankThreshold).map((a) => a.toLowerCase()));
    // Tracks that have been heard at least once (needed for the unplayed-exclusion toggle)
    const playedKeys = (skipTierArtists.size > 0) ? getPlayedTrackKeys(db, userPlexId) : new Set();

    let excludedTrackCount = 0;
    let excludedArtistCount = 0;
    const included = masterTracks.filter((t) => {
      const artist = t.artistName.toLowerCase();
      // Liked artists always kept, regardless of ranking
      if (likedArtistSet.has(artist)) return true;
      // Excluded by manual preference or legacy skip rules
      if (excludedArtists.has(artist) || ignoredArtistSet.has(artist)) { excludedArtistCount++; return false; }
      // Skip-tier artist: exclude their played tracks; toggle controls unplayed tracks
      if (skipTierArtists.has(artist)) {
        if (excludeUnplayedForSkipArtists) {
          // Exclude ALL tracks (played + unplayed) — artist fully removed
          excludedArtistCount++; return false;
        }
        // Exclude only tracks that have been played (allow unplayed to remain for future scoring)
        if (playedKeys.has(t.ratingKey)) { excludedTrackCount++; return false; }
      }
      // Track excluded by consecutive skips
      if (excludedKeys.has(t.ratingKey)) { excludedTrackCount++; return false; }
      return true;
    });

    const ratingKeys = included.map((t) => t.ratingKey);

    // Resolve machineId
    let mid = machineId;
    if (!mid) {
      try {
        const r = await fetch(`${url.replace(/\/$/, '')}?X-Plex-Token=${token}`, { headers: { Accept: 'application/json' } });
        if (r.ok) mid = (await r.json())?.MediaContainer?.machineIdentifier || '';
      } catch (_) { /* non-fatal */ }
    }
    if (!mid) throw new Error('Could not determine Plex machine ID');

    const base = url.replace(/\/$/, '');

    // Clear all existing items with a single DELETE
    await fetch(`${base}/playlists/${playlistId}/items?X-Plex-Token=${token}`, { method: 'DELETE' });

    // Add in batches of 100
    for (let i = 0; i < ratingKeys.length; i += 100) {
      const batch = ratingKeys.slice(i, i + 100);
      const uri = `server://${mid}/com.plexapp.plugins.library/library/metadata/${batch.join(',')}`;
      const addUrl = new URL(`${base}/playlists/${playlistId}/items`);
      addUrl.searchParams.set('uri', uri);
      addUrl.searchParams.set('X-Plex-Token', token);
      await fetch(addUrl.toString(), { method: 'PUT', headers: { Accept: 'application/json' } });
    }

    recordPlaylistSync(db, {
      userPlexId,
      plexPlaylistId: playlistId,
      playlistTitle: playlistRow.playlist_title,
      trackCount: ratingKeys.length,
      excludedTracks: excludedTrackCount,
      excludedArtists: excludedArtistCount,
      trigger: 'auto',
    });

    pushLog({
      level: 'info', app: 'playlist', action: 'sync',
      message: `Playlist synced: ${ratingKeys.length} tracks (${excludedTrackCount} tracks + ${excludedArtistCount} artists excluded)`,
    });
  } catch (err) {
    pushLog({ level: 'error', app: 'playlist', action: 'sync.error', message: safeMessage(err) });
  }
}

// ─── Route registration ───────────────────────────────────────────────────────

export function registerApiMusic(app, ctx) {
  const { db, requireUser, requireAdmin, loadConfig, pushLog, safeMessage } = ctx;

  // ── Dashboard stats ───────────────────────────────────────────────────────

  app.get('/api/music/stats', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const days = Number(req.query?.days || 30);
    const since = Date.now() - days * 24 * 60 * 60 * 1000;
    const stats = getPlayStats(db, userPlexId, since);
    const byDay = getPlayStatsByDay(db, userPlexId, days);
    const lastSync = getLastPlaylistSync(db, userPlexId);
    return res.json({ ok: true, stats, byDay, lastSync });
  });

  // ── Play history ──────────────────────────────────────────────────────────

  app.get('/api/music/history', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const limit = Math.min(200, Number(req.query?.limit || 50));
    const offset = Number(req.query?.offset || 0);
    const history = getRecentHistory(db, userPlexId, limit, offset);
    return res.json({ ok: true, history });
  });

  // ── Artists ───────────────────────────────────────────────────────────────

  app.get('/api/music/artists', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const limit = Math.min(500, Number(req.query?.limit || 100));
    const artists = getTopArtists(db, userPlexId, limit);
    const excludedArtists = new Set(getExcludedArtists(db, userPlexId));
    return res.json({ ok: true, artists, excludedArtists: [...excludedArtists] });
  });

  app.post('/api/music/artists/:name/exclude', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const artistName = decodeURIComponent(req.params.name);
    const excluded = req.body?.excluded !== false;
    setArtistExclusion(db, userPlexId, artistName, excluded);
    pushLog({ level: 'info', app: 'music', action: excluded ? 'artist.exclude' : 'artist.include', message: `${artistName} ${excluded ? 'excluded from' : 'included in'} smart playlist` });
    return res.json({ ok: true });
  });

  app.post('/api/music/artists/:name/reset-skips', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const artistName = decodeURIComponent(req.params.name);
    resetArtistSkipStreak(db, userPlexId, artistName);
    return res.json({ ok: true });
  });

  // ── Tracks ────────────────────────────────────────────────────────────────

  app.get('/api/music/tracks', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const limit = Math.min(500, Number(req.query?.limit || 100));
    const tracks = getTopTracks(db, userPlexId, limit);
    return res.json({ ok: true, tracks });
  });

  app.post('/api/music/tracks/:key/exclude', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const ratingKey = decodeURIComponent(req.params.key);
    const excluded = req.body?.excluded !== false;
    setTrackExclusion(db, userPlexId, ratingKey, excluded);
    return res.json({ ok: true });
  });

  app.post('/api/music/tracks/:key/include', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const ratingKey = decodeURIComponent(req.params.key);
    const included = req.body?.included !== false;
    setTrackInclusion(db, userPlexId, ratingKey, included);
    return res.json({ ok: true });
  });

  app.post('/api/music/tracks/:key/reset-skips', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const ratingKey = decodeURIComponent(req.params.key);
    resetTrackSkipStreak(db, userPlexId, ratingKey);
    return res.json({ ok: true });
  });

  // ── Playlist track listing (paginated, fetched from Plex) ────────────────

  app.get('/api/music/playlist/tracks', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const config = loadConfig();
    const { url, token } = config.plex || {};
    if (!url || !token) return res.json({ tracks: [], total: 0, playlistTitle: null });

    const playlistRow = getUserPlaylist(db, userPlexId);
    if (!playlistRow?.playlist_id) return res.json({ tracks: [], total: 0, playlistTitle: null });

    const offset = Math.max(0, Number(req.query.offset || 0));
    const limit = Math.min(200, Math.max(1, Number(req.query.limit || 100)));

    try {
      const plexUrl = new URL(`${url.replace(/\/$/, '')}/playlists/${playlistRow.playlist_id}/items`);
      plexUrl.searchParams.set('X-Plex-Token', token);
      plexUrl.searchParams.set('X-Plex-Container-Start', String(offset));
      plexUrl.searchParams.set('X-Plex-Container-Size', String(limit));
      const r = await fetch(plexUrl.toString(), { headers: { Accept: 'application/json' } });
      if (!r.ok) return res.status(502).json({ error: `Plex returned ${r.status}` });
      const json = await r.json();
      const tracks = (json?.MediaContainer?.Metadata || []).map((t) => ({
        ratingKey: t.ratingKey,
        title: t.title || '',
        artistName: t.originalTitle || t.grandparentTitle || '',
        albumName: t.parentTitle || '',
        duration: t.duration || 0,
        thumb: t.thumb || t.parentThumb || '',
        playlistItemID: t.playlistItemID,
      }));
      return res.json({
        ok: true,
        tracks,
        total: json?.MediaContainer?.totalSize || 0,
        playlistTitle: playlistRow.playlist_title,
        playlistId: playlistRow.playlist_id,
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Smart playlist ────────────────────────────────────────────────────────

  app.post('/api/music/playlist/sync', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    try {
      await rebuildSmartPlaylist(ctx, userPlexId);
      const lastSync = getLastPlaylistSync(db, userPlexId);
      return res.json({ ok: true, lastSync });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/playlist/excluded', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const excludedKeys = getExcludedTrackKeys(db, userPlexId);
    const excludedArtistNames = getExcludedArtists(db, userPlexId);
    return res.json({ ok: true, excludedTracks: excludedKeys, excludedArtists: excludedArtistNames });
  });

  // ── Admin: all-users aggregate ────────────────────────────────────────────

  app.get('/api/music/admin/users', requireAdmin, (req, res) => {
    const users = getAllUserIds(db);
    return res.json({ ok: true, users });
  });

  // ── Image proxies (avoid exposing Plex token to browser) ─────────────────

  // Album art for a track by its Plex rating key (uses parentThumb from metadata)
  app.get('/api/music/thumb/track/:key', requireUser, async (req, res) => {
    const config = loadConfig();
    const { url, token } = config.plex || {};
    if (!url || !token) return res.status(404).end();
    const key = req.params.key;
    const base = url.replace(/\/$/, '');
    try {
      const mr = await fetch(`${base}/library/metadata/${encodeURIComponent(key)}?X-Plex-Token=${token}`, { headers: { Accept: 'application/json' } });
      if (!mr.ok) return res.status(404).end();
      const meta = await mr.json();
      const trackMeta = (meta?.MediaContainer?.Metadata || [])[0];
      const thumb = trackMeta?.parentThumb || trackMeta?.thumb || trackMeta?.grandparentThumb;
      if (!thumb) return res.status(404).end();

      const ir = await fetch(`${base}${thumb}?X-Plex-Token=${token}`);
      if (!ir.ok) return res.status(404).end();
      const buf = await ir.arrayBuffer();
      res.set('Content-Type', ir.headers.get('Content-Type') || 'image/jpeg');
      res.set('Cache-Control', 'public, max-age=86400');
      return res.send(Buffer.from(buf));
    } catch (_) {
      return res.status(404).end();
    }
  });

  // Artist art — find any track by the artist in master_tracks, fetch its grandparentThumb
  app.get('/api/music/thumb/artist/:name', requireUser, async (req, res) => {
    const config = loadConfig();
    const { url, token } = config.plex || {};
    if (!url || !token) return res.status(404).end();
    const artistName = decodeURIComponent(req.params.name);
    const base = url.replace(/\/$/, '');

    // Find any track rating key for this artist from the master cache
    const trackRow = db.prepare(
      'SELECT rating_key FROM master_tracks WHERE artist_name = ? LIMIT 1',
    ).get(artistName);
    if (!trackRow?.rating_key) return res.status(404).end();

    try {
      // Fetch track metadata — grandparentThumb is the artist photo
      const metaUrl = `${base}/library/metadata/${encodeURIComponent(trackRow.rating_key)}?X-Plex-Token=${token}`;
      const mr = await fetch(metaUrl, { headers: { Accept: 'application/json' } });
      if (!mr.ok) return res.status(404).end();
      const meta = await mr.json();
      const thumb = (meta?.MediaContainer?.Metadata || [])[0]?.grandparentThumb;
      if (!thumb) return res.status(404).end();

      const ir = await fetch(`${base}${thumb}?X-Plex-Token=${token}`);
      if (!ir.ok) return res.status(404).end();
      const buf = await ir.arrayBuffer();
      res.set('Content-Type', ir.headers.get('Content-Type') || 'image/jpeg');
      res.set('Cache-Control', 'public, max-age=86400');
      return res.send(Buffer.from(buf));
    } catch (_) {
      return res.status(404).end();
    }
  });
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function resolveQueryUserId(req) {
  // Always use username — this matches how the user wizard stores playlist/prefs rows
  const user = req.session?.user || {};
  return String(user.username || '').trim();
}
