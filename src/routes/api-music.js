// Music stats, smart playlist management, and Lidarr integration

import {
  getTopArtists,
  getTopTracks,
  getRecentHistory,
  getPlayStats,
  getPlayStatsByDay,
  getExcludedTrackKeys,
  getSkipTierArtists,
  getPlayedTrackKeys,
  getAllUserIds,
  recordPlaylistSync,
  getLastPlaylistSync,
  setTrackExclusion,
  setTrackInclusion,
  resetTrackSkipStreak,
  resetArtistSkipStreak,
  getSuggestedArtist,
  setSuggestedArtistStatus,
  getUserPlaylist,
  listUserGeneratedPlaylists,
  saveUserGeneratedPlaylist,
  getResolvedUserArtistFilters,
  cleanMasterArtistName,
  getMasterTracks,
  clearPlaylistJob,
  getCurrentLidarrUsage,
  getArtistRankSnapshot,
  enqueueLidarrRequest,
  getLidarrRequest,
  listLidarrRequests,
  recordLidarrUsage,
  removeQueuedLidarrRequest,
  reorderQueuedLidarrRequests,
  upsertSuggestedAlbum,
  getLidarrArtistProgress,
  saveLidarrArtistProgress,
  updateLidarrRequest,
  removePlaylistTracks,
  addPlaylistTracks,
  listAllGeneratedPlaylists,
  clearGeneratedPlaylistPlexId,
} from '../db.js';

const DAY_MS = 24 * 60 * 60 * 1000;

// ── Smart playlist rebuild ────────────────────────────────────────────────────
// Called after skip events (debounced) and on demand.

// Returns true if any credited artist in `fullName` (e.g. "A & B", "A, B") is in `nameSet`.
// Handles feat./featuring already stripped by cleanMasterArtistName. Splits on & , / and.
function artistInSet(fullName, nameSet) {
  if (nameSet.has(fullName)) return true;
  // Split co-credits separated by &, / or " and "
  const parts = fullName.split(/\s*[&\/,]\s*|\s+and\s+/).map((s) => s.trim()).filter(Boolean);
  return parts.length > 1 && parts.some((p) => nameSet.has(p));
}

function isAllowedLidarrImagePath(value) {
  const raw = String(value || '').trim();
  return /^\/(?:api\/v\d+\/)?MediaCover\//.test(raw);
}

export async function rebuildSmartPlaylist(ctx, userPlexId) {
  const {
    db,
    loadConfig,
    pushLog,
    safeMessage,
    resolveUserPlexServerToken,
    userHasOwnPlexToken,
    buildPlexAuthHeaders,
  } = ctx;
  const config = loadConfig();
  // Skip local-only users — they have no personal Plex token
  if (!userHasOwnPlexToken(config, userPlexId)) return;
  const { url, machineId = '' } = config.plex || {};
  const token = resolveUserPlexServerToken(config, userPlexId);

  if (!url || !token) return;

  // Look up this user's personal playlist
  const playlistRow = getUserPlaylist(db, userPlexId);
  if (!playlistRow?.playlist_id) return; // user hasn't completed wizard yet

  const playlistId = playlistRow.playlist_id;
  const { mustIncludeArtists, neverIncludeArtists } = getResolvedUserArtistFilters(db, config, userPlexId);
  const ignoredArtistSet = new Set(neverIncludeArtists.map((a) => cleanMasterArtistName(a).toLowerCase()));
  const likedArtistSet = new Set(mustIncludeArtists.map((a) => cleanMasterArtistName(a).toLowerCase()));

  try {
    // Use master cache — no Plex API call needed
    const masterTracks = getMasterTracks(db);
    if (!masterTracks.length) return; // cache not ready

    const smartSettings = config.smartPlaylist || {};
    const artistSkipRankThreshold = smartSettings.artistSkipRank ?? 2;
    // Get skip-based exclusions from play stats
    const excludedKeys = new Set(getExcludedTrackKeys(db, userPlexId));
    // Artists whose ranking_score has fallen to or below the skip threshold
    const skipTierArtists = new Set(getSkipTierArtists(db, userPlexId, artistSkipRankThreshold).map((a) => cleanMasterArtistName(a).toLowerCase()));
    // Tracks that have been heard at least once
    const playedKeys = (skipTierArtists.size > 0) ? getPlayedTrackKeys(db, userPlexId) : new Set();

    let excludedTrackCount = 0;
    const excludedArtistNames = new Set();
    const included = masterTracks.filter((t) => {
      const artist = cleanMasterArtistName(t.artistName).toLowerCase();
      // Liked artists always kept, regardless of ranking
      if (artistInSet(artist, likedArtistSet)) return true;
      // Excluded by ignored artist preference
      if (artistInSet(artist, ignoredArtistSet)) {
        excludedArtistNames.add(artist);
        return false;
      }
      // Skip-tier artist: exclude their played tracks
      if (artistInSet(artist, skipTierArtists)) {
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
        const r = await fetch(url.replace(/\/$/, ''), {
          headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
        });
        if (r.ok) mid = (await r.json())?.MediaContainer?.machineIdentifier || '';
      } catch (_) { /* non-fatal */ }
    }
    if (!mid) throw new Error('Could not determine Plex machine ID');

    const base = url.replace(/\/$/, '');

    // Clear all existing items with a single DELETE
    await fetch(`${base}/playlists/${playlistId}/items`, {
      method: 'DELETE',
      headers: buildPlexAuthHeaders(token),
    });

    // Add in batches of 100
    for (let i = 0; i < ratingKeys.length; i += 100) {
      const batch = ratingKeys.slice(i, i + 100);
      const uri = `server://${mid}/com.plexapp.plugins.library/library/metadata/${batch.join(',')}`;
      const addUrl = new URL(`${base}/playlists/${playlistId}/items`);
      addUrl.searchParams.set('uri', uri);
      await fetch(addUrl.toString(), {
        method: 'PUT',
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
    }

    recordPlaylistSync(db, {
      userPlexId,
      plexPlaylistId: playlistId,
      playlistTitle: playlistRow.playlist_title,
      trackCount: ratingKeys.length,
      excludedTracks: excludedTrackCount,
      excludedArtists: excludedArtistNames.size,
      trigger: 'auto',
    });

    pushLog({
      level: 'info', app: 'playlist', action: 'sync',
      message: `Playlist synced: ${ratingKeys.length} tracks (${excludedTrackCount} tracks + ${excludedArtistNames.size} artists excluded)`,
    });
  } catch (err) {
    pushLog({ level: 'error', app: 'playlist', action: 'sync.error', message: safeMessage(err) });
  }
}

// ─── Route registration ───────────────────────────────────────────────────────

export function registerApiMusic(app, ctx) {
  const {
    db,
    requireUser,
    requireAdmin,
    loadConfig,
    pushLog,
    safeMessage,
    recommendationService,
    playlistService,
    lidarrService,
    canUserAccessLidarrAutomation,
    resolveUserPlexServerToken,
    buildAppApiUrl,
    buildPlexAuthHeaders,
  } = ctx;

  function resolveOverviewUserId(req) {
    const user = req.session?.user || {};
    const role = String(user.role || '').trim().toLowerCase();
    if (role === 'admin') return String(req.query?.user || '').trim();
    return String(user.username || '').trim();
  }

  function isLookupArtistAlreadyAdded(item) {
    const addedValue = String(item?.added || '').trim();
    return Number(item?.id || 0) > 0
      && Boolean(item?.path)
      && addedValue
      && addedValue !== '0001-01-01T00:01:00Z';
  }

  function normalizeLookupText(value) {
    return String(value || '').trim().toLowerCase().replace(/\s+/g, ' ');
  }

  function normalizeArtistMatchText(value) {
    return String(value || '')
      .trim()
      .toLowerCase()
      .replace(/&/g, ' and ')
      .replace(/[^a-z0-9]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function scoreLookupArtistResult(item, term) {
    const query = normalizeLookupText(term);
    const artistName = normalizeLookupText(item?.artistName);
    const sortName = normalizeLookupText(item?.sortName);
    const disambiguation = normalizeLookupText(item?.disambiguation);
    let score = 0;
    if (!query) return score;
    if (artistName === query) score += 1000;
    else if (sortName === query) score += 920;
    else if (artistName.startsWith(query)) score += 780;
    else if (sortName.startsWith(query)) score += 720;
    else if (artistName.includes(query)) score += 560;
    else if (sortName.includes(query)) score += 520;
    if (disambiguation.includes(query)) score += 120;
    if (Array.isArray(item?.genres) && item.genres.length) score += 15;
    if (Array.isArray(item?.images) && item.images.length) score += 10;
    return score;
  }

  async function getLidarrArtistImageUrl(name) {
    if (!lidarrService?.isConfigured() || !name) return null;
    try {
      const results = await lidarrService.lookupArtist(name);
      const items = Array.isArray(results) ? results : [];
      const best = items
        .map((item) => ({ item, score: scoreLookupArtistResult(item, name) }))
        .sort((a, b) => b.score - a.score)[0]?.item;
      const imagePath = Array.isArray(best?.images)
        ? (best.images.find((img) => /poster|fanart/i.test(String(img?.coverType || '')))?.url || '')
        : '';
      return imagePath ? `/api/music/lidarr/image?path=${encodeURIComponent(imagePath)}` : null;
    } catch {
      return null;
    }
  }

  function normalizeTierKey(value) {
    const key = String(value || '').trim().toLowerCase();
    if (key === 'half decent') return 'half-decent';
    return key;
  }

  function formatTierLabel(value) {
    const key = normalizeTierKey(value);
    if (key === 'half-decent') return 'Half Decent';
    if (key === 'belter') return 'Belter';
    if (key === 'decent') return 'Decent';
    if (key === 'skip') return 'Skip';
    return 'Curatorr';
  }

  function deriveArtistTier(stats, config) {
    if (!stats) return 'curatorr';
    if (Number(stats.excluded || 0) === 1) return 'skip';
    const smartSettings = config?.smartPlaylist || {};
    const skipThreshold = Number(smartSettings.artistSkipRank ?? 2);
    const belterThreshold = Number(smartSettings.artistBelterRank ?? 8);
    const score = Number(stats.rankingScore);
    if (Number.isFinite(score)) {
      if (score <= skipThreshold) return 'skip';
      if (score < 5) return 'half-decent';
      if (score >= belterThreshold) return 'belter';
      return 'decent';
    }
    return 'curatorr';
  }

  async function fetchPlexMetadata(base, token, ratingKey) {
    if (!base || !token || !ratingKey) return null;
    const response = await fetch(`${base}/library/metadata/${encodeURIComponent(ratingKey)}`, {
      headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
    });
    if (!response.ok) return null;
    const payload = await response.json();
    return (payload?.MediaContainer?.Metadata || [])[0] || null;
  }

  function buildOverviewText(summary, fallback) {
    const text = String(summary || '').trim();
    return text || String(fallback || '').trim() || 'No overview available for this item yet.';
  }

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
    return res.json({ ok: true, artists });
  });

  app.post('/api/music/artists/:name/exclude', requireUser, (req, res) => {
    // Kept as a no-op stub so any existing client calls don't 404
    return res.json({ ok: true });
    return res.json({ ok: true });
  });

  app.post('/api/music/artists/:name/reset-skips', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const artistName = decodeURIComponent(req.params.name);
    resetArtistSkipStreak(db, userPlexId, artistName);
    return res.json({ ok: true });
  });

  // ── Manual discovery / queue ─────────────────────────────────────────────

  app.get('/api/music/lidarr/manual/search', requireUser, async (req, res) => {
    const term = String(req.query?.term || '').trim();
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    if (!lidarrService?.isConfigured()) {
      return res.status(400).json({ error: 'Lidarr is not configured.' });
    }
    if (term.length < 2) return res.json({ ok: true, results: [] });
    try {
      const results = await lidarrService.lookupArtist(term);
      const normalizedResults = (Array.isArray(results) ? results : [])
        .map((item) => ({
          item,
          alreadyAdded: isLookupArtistAlreadyAdded(item),
          relevance: scoreLookupArtistResult(item, term),
        }))
        .sort((a, b) => {
          if (a.relevance !== b.relevance) return b.relevance - a.relevance;
          if (a.alreadyAdded !== b.alreadyAdded) return a.alreadyAdded ? 1 : -1;
          return String(a.item?.artistName || '').localeCompare(String(b.item?.artistName || ''));
        })
        .map((entry) => entry.item);
      return res.json({
        ok: true,
        results: normalizedResults.slice(0, 20).map((item) => ({
          artistName: String(item?.artistName || ''),
          foreignArtistId: String(item?.foreignArtistId || ''),
          disambiguation: String(item?.disambiguation || ''),
          genres: Array.isArray(item?.genres) ? item.genres.filter(Boolean).slice(0, 4) : [],
          artistType: String(item?.artistType || ''),
          added: isLookupArtistAlreadyAdded(item),
          lidarrArtistId: Number(item?.id || 0) || null,
          image: (() => {
            const imagePath = Array.isArray(item?.images)
              ? (item.images.find((img) => /poster|fanart/i.test(String(img?.coverType || '')))?.url || '')
              : '';
            return imagePath
              ? `/api/music/lidarr/image?path=${encodeURIComponent(imagePath)}`
              : '';
          })(),
        })),
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/lidarr/artist-thumb', requireUser, async (req, res) => {
    const name = String(req.query?.name || '').trim();
    if (!name || !lidarrService?.isConfigured()) return res.status(404).end();
    try {
      const results = await lidarrService.lookupArtist(name);
      const match = (Array.isArray(results) ? results : []).find((item) => {
        const n = String(item?.artistName || '').toLowerCase();
        return n === name.toLowerCase() || n.startsWith(name.toLowerCase());
      }) || (Array.isArray(results) ? results[0] : null);
      const imagePath = Array.isArray(match?.images)
        ? (match.images.find((img) => /poster|fanart/i.test(String(img?.coverType || '')))?.url || '')
        : '';
      if (!imagePath) return res.status(404).end();
      return res.redirect(`/api/music/lidarr/image?path=${encodeURIComponent(imagePath)}`);
    } catch (_err) {
      return res.status(404).end();
    }
  });

  app.post('/api/music/lidarr/manual/albums', requireUser, async (req, res) => {
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    if (!lidarrService?.isConfigured()) {
      return res.status(400).json({ error: 'Lidarr is not configured.' });
    }
    try {
      const artistName = String(req.body?.artistName || '').trim();
      const foreignArtistId = String(req.body?.foreignArtistId || '').trim();
      const preview = await lidarrService.previewManualArtistAlbums({ artistName, foreignArtistId });
      return res.json({
        ok: true,
        artist: {
          artistName: String(preview?.artist?.artistName || artistName || ''),
          foreignArtistId: String(preview?.artist?.foreignArtistId || foreignArtistId || ''),
          lidarrArtistId: Number(preview?.artist?.id || 0) || null,
          added: Boolean(preview?.artist?.added),
        },
        source: String(preview?.source || ''),
        albums: (Array.isArray(preview?.albums) ? preview.albums : []).map((album) => ({
          ...album,
          image: album?.imageUrl
            ? String(album.imageUrl)
            : (album?.imagePath ? `/api/music/lidarr/image?path=${encodeURIComponent(String(album.imagePath))}` : ''),
        })),
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/lidarr/requests', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    return res.json({
      ok: true,
      queued: listLidarrRequests(db, userPlexId, { statuses: ['queued', 'processing'], limit: 200 }),
      history: listLidarrRequests(db, userPlexId, { statuses: ['completed', 'failed'], limit: 50 }),
    });
  });

  app.post('/api/music/lidarr/manual/request', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const role = String(req.session?.user?.role || 'user').trim().toLowerCase();
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    if (!lidarrService?.isConfigured()) {
      return res.status(400).json({ error: 'Lidarr is not configured.' });
    }
    const artistName = String(req.body?.artistName || '').trim();
    const foreignArtistId = String(req.body?.foreignArtistId || '').trim();
    const preferredAlbumTitle = String(req.body?.preferredAlbumTitle || '').trim();
    const useCuratorrPick = req.body?.useCuratorrPick === true || req.body?.useCuratorrPick === 'true' || !preferredAlbumTitle;
    if (!artistName) return res.status(400).json({ error: 'artistName is required.' });

    const usage = getCurrentLidarrUsage(db, userPlexId).usage || {};
    const lookupResults = await lidarrService.lookupArtist(artistName);
    const lookupMatch = lidarrService.pickLookupArtist(lookupResults, artistName, { foreignArtistId });
    if (!lookupMatch) return res.status(404).json({ error: 'Artist not found in Lidarr lookup.' });
    const needsArtistQuota = !(lookupMatch?.added && Number(lookupMatch?.id || 0) > 0);
    try {
      lidarrService.assertQuotaAvailable(role, usage, {
        artists: needsArtistQuota ? 1 : 0,
        albums: 1,
      });
      const request = enqueueLidarrRequest(db, userPlexId, {
        sourceKind: 'manual',
        requestKind: 'artist_album',
        artistName,
        albumTitle: useCuratorrPick ? '' : preferredAlbumTitle,
        foreignArtistId,
        status: 'processing',
        detail: {
          preferredAlbumTitle: useCuratorrPick ? '' : preferredAlbumTitle,
          useCuratorrPick,
          note: 'Manual request created from Discover page.',
        },
      });
      const result = await lidarrService.executeArtistAlbumRequest({
        userPlexId,
        role,
        artistName,
        foreignArtistId,
        preferredAlbumTitle: useCuratorrPick ? '' : preferredAlbumTitle,
        sourceKind: 'manual',
        requestId: request.id,
        lookupArtistResult: lookupMatch,
      });
      return res.json({ ok: true, queued: false, request: getLidarrRequest(db, request.id, userPlexId), result });
    } catch (err) {
      if (err?.code === 'ARTIST_QUOTA_REACHED' || err?.code === 'ALBUM_QUOTA_REACHED') {
        const request = await lidarrService.queueArtistAlbumRequest({
          userPlexId,
          artistName,
          foreignArtistId,
          preferredAlbumTitle: useCuratorrPick ? '' : preferredAlbumTitle,
          sourceKind: 'manual',
          allowCuratorrFallback: true,
          note: 'Queued because weekly quota was reached.',
        });
        return res.status(202).json({
          ok: true,
          queued: true,
          request,
          quota: err.quota || lidarrService.getRoleQuota(role, usage),
          message: 'Quota reached. Added to your queue instead.',
        });
      }
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/lidarr/requests/:id/remove', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const removed = removeQueuedLidarrRequest(db, req.params.id, userPlexId);
    if (!removed) return res.status(404).json({ error: 'Queue item not found.' });
    return res.json({ ok: true, request: removed });
  });

  app.post('/api/music/lidarr/requests/reorder', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const ids = Array.isArray(req.body?.requestIds) ? req.body.requestIds : [];
    const queued = reorderQueuedLidarrRequests(db, userPlexId, ids);
    return res.json({ ok: true, queued });
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

  // ── Music overview popup data ────────────────────────────────────────────

  app.get('/api/music/overview/artist/:name', requireUser, async (req, res) => {
    const config = loadConfig();
    const userPlexId = resolveOverviewUserId(req);
    const artistName = decodeURIComponent(req.params.name);
    const base = String(config?.plex?.url || '').replace(/\/$/, '');
    const token = String(resolveUserPlexServerToken(config, userPlexId) || config?.plex?.token || '').trim();
    const sampleTrack = db.prepare(`
      SELECT rating_key, track_title, album_name
      FROM master_tracks
      WHERE artist_name = ?
      ORDER BY album_name ASC, track_title ASC
      LIMIT 1
    `).get(artistName);
    if (!sampleTrack?.rating_key) return res.status(404).json({ error: 'Artist not found.' });
    try {
      const sampleMeta = await fetchPlexMetadata(base, token, sampleTrack.rating_key);
      const artistMeta = sampleMeta?.grandparentRatingKey
        ? await fetchPlexMetadata(base, token, sampleMeta.grandparentRatingKey)
        : null;
      const stats = userPlexId
        ? db.prepare(`
          SELECT play_count, skip_count, consecutive_skips, ranking_score, excluded_from_smart
          FROM artist_stats
          WHERE user_plex_id = ? AND artist_name = ?
          LIMIT 1
        `).get(userPlexId, artistName) || {}
        : db.prepare(`
          SELECT COALESCE(SUM(play_count), 0) AS play_count,
                 COALESCE(SUM(skip_count), 0) AS skip_count,
                 COALESCE(MAX(consecutive_skips), 0) AS consecutive_skips,
                 COALESCE(AVG(ranking_score), 5) AS ranking_score,
                 COALESCE(MAX(excluded_from_smart), 0) AS excluded_from_smart
          FROM artist_stats
          WHERE artist_name = ?
        `).get(artistName) || {};
      const libraryStats = db.prepare(`
        SELECT COUNT(DISTINCT album_name) AS album_count, COUNT(*) AS track_count
        FROM master_tracks
        WHERE artist_name = ?
      `).get(artistName) || {};
      const tier = deriveArtistTier({
        excluded: stats.excluded_from_smart,
        rankingScore: stats.ranking_score,
      }, config);
      return res.json({
        ok: true,
        item: {
          kind: 'artist',
          title: artistName,
          subtitle: `${Number(libraryStats.album_count || 0)} album${Number(libraryStats.album_count || 0) === 1 ? '' : 's'} · ${Number(libraryStats.track_count || 0)} track${Number(libraryStats.track_count || 0) === 1 ? '' : 's'}`,
          overview: buildOverviewText(artistMeta?.summary, `${artistName} currently has ${Number(stats.play_count || 0)} plays and ${Number(stats.skip_count || 0)} skips in Curatorr.`),
          thumb: `/api/music/thumb/artist/${encodeURIComponent(artistName)}`,
          art: `/api/music/thumb/artist/${encodeURIComponent(artistName)}`,
          pills: ['Artist', formatTierLabel(tier)],
          stats: [
            { label: 'Plays', value: Number(stats.play_count || 0) },
            { label: 'Skips', value: Number(stats.skip_count || 0) },
            { label: 'Ranking', value: `${Number(Number(stats.ranking_score || 5).toFixed(1))}/10` },
            { label: 'Albums', value: Number(libraryStats.album_count || 0) },
          ],
        },
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/overview/album', requireUser, async (req, res) => {
    const config = loadConfig();
    const userPlexId = resolveOverviewUserId(req);
    const artistName = String(req.query?.artist || '').trim();
    const albumName = String(req.query?.album || '').trim();
    if (!artistName || !albumName) return res.status(400).json({ error: 'artist and album are required.' });
    const base = String(config?.plex?.url || '').replace(/\/$/, '');
    const token = String(resolveUserPlexServerToken(config, userPlexId) || config?.plex?.token || '').trim();
    const sampleTrack = db.prepare(`
      SELECT rating_key, track_title
      FROM master_tracks
      WHERE artist_name = ? AND album_name = ?
      ORDER BY track_title ASC
      LIMIT 1
    `).get(artistName, albumName);
    if (!sampleTrack?.rating_key) return res.status(404).json({ error: 'Album not found.' });
    try {
      const sampleMeta = await fetchPlexMetadata(base, token, sampleTrack.rating_key);
      const albumMeta = sampleMeta?.parentRatingKey
        ? await fetchPlexMetadata(base, token, sampleMeta.parentRatingKey)
        : null;
      const stats = userPlexId
        ? db.prepare(`
          SELECT COUNT(*) AS track_count,
                 COALESCE(SUM(play_count), 0) AS total_plays,
                 COALESCE(SUM(skip_count), 0) AS total_skips,
                 MAX(consecutive_skips) AS max_skip_streak
          FROM track_stats
          WHERE user_plex_id = ? AND artist_name = ? AND album_name = ?
        `).get(userPlexId, artistName, albumName) || {}
        : db.prepare(`
          SELECT COUNT(*) AS track_count,
                 COALESCE(SUM(play_count), 0) AS total_plays,
                 COALESCE(SUM(skip_count), 0) AS total_skips,
                 COALESCE(MAX(consecutive_skips), 0) AS max_skip_streak
          FROM track_stats
          WHERE artist_name = ? AND album_name = ?
        `).get(artistName, albumName) || {};
      return res.json({
        ok: true,
        item: {
          kind: 'album',
          title: albumName,
          subtitle: artistName,
          overview: buildOverviewText(albumMeta?.summary, `${albumName} by ${artistName} currently has ${Number(stats.total_plays || 0)} plays and ${Number(stats.total_skips || 0)} skips across ${Number(stats.track_count || 0)} tracked songs.`),
          thumb: `/api/music/thumb/album?artist=${encodeURIComponent(artistName)}&album=${encodeURIComponent(albumName)}`,
          art: `/api/music/thumb/album?artist=${encodeURIComponent(artistName)}&album=${encodeURIComponent(albumName)}`,
          pills: ['Album'],
          stats: [
            { label: 'Tracks', value: Number(stats.track_count || 0) },
            { label: 'Plays', value: Number(stats.total_plays || 0) },
            { label: 'Skips', value: Number(stats.total_skips || 0) },
            { label: 'Max streak', value: Number(stats.max_skip_streak || 0) },
          ],
        },
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/overview/track/:key', requireUser, async (req, res) => {
    const config = loadConfig();
    const userPlexId = resolveOverviewUserId(req);
    const ratingKey = decodeURIComponent(req.params.key);
    const base = String(config?.plex?.url || '').replace(/\/$/, '');
    const token = String(resolveUserPlexServerToken(config, userPlexId) || config?.plex?.token || '').trim();
    try {
      const trackMeta = await fetchPlexMetadata(base, token, ratingKey);
      if (!trackMeta) return res.status(404).json({ error: 'Track not found.' });
      const stats = userPlexId
        ? db.prepare(`
          SELECT track_title, artist_name, album_name, play_count, skip_count, consecutive_skips, excluded_from_smart, manually_included, tier
          FROM track_stats
          WHERE user_plex_id = ? AND plex_rating_key = ?
          LIMIT 1
        `).get(userPlexId, ratingKey) || {}
        : db.prepare(`
          SELECT track_title, artist_name, album_name,
                 COALESCE(SUM(play_count), 0) AS play_count,
                 COALESCE(SUM(skip_count), 0) AS skip_count,
                 COALESCE(MAX(consecutive_skips), 0) AS consecutive_skips,
                 COALESCE(MAX(excluded_from_smart), 0) AS excluded_from_smart,
                 COALESCE(MAX(manually_included), 0) AS manually_included,
                 COALESCE(MAX(tier), 'curatorr') AS tier
          FROM track_stats
          WHERE plex_rating_key = ?
          GROUP BY track_title, artist_name, album_name
          ORDER BY play_count DESC
          LIMIT 1
        `).get(ratingKey) || {};
      const historyStats = userPlexId
        ? db.prepare(`
          SELECT track_title, artist_name, album_name,
                 COUNT(*) AS play_count,
                 COALESCE(SUM(is_skip), 0) AS skip_count
          FROM play_events
          WHERE user_plex_id = ? AND plex_rating_key = ?
          GROUP BY track_title, artist_name, album_name
          ORDER BY MAX(started_at) DESC
          LIMIT 1
        `).get(userPlexId, ratingKey) || {}
        : db.prepare(`
          SELECT track_title, artist_name, album_name,
                 COUNT(*) AS play_count,
                 COALESCE(SUM(is_skip), 0) AS skip_count
          FROM play_events
          WHERE plex_rating_key = ?
          GROUP BY track_title, artist_name, album_name
          ORDER BY MAX(started_at) DESC
          LIMIT 1
        `).get(ratingKey) || {};
      const trackTitle = String(stats.track_title || historyStats.track_title || trackMeta.title || '').trim();
      const artistName = String(stats.artist_name || historyStats.artist_name || trackMeta.originalTitle || trackMeta.grandparentTitle || '').trim();
      const albumName = String(stats.album_name || historyStats.album_name || trackMeta.parentTitle || '').trim();
      const fallbackStats = (trackTitle && artistName)
        ? (userPlexId
          ? db.prepare(`
            SELECT track_title, artist_name, album_name,
                   play_count, skip_count, consecutive_skips, excluded_from_smart, manually_included, tier
            FROM track_stats
            WHERE user_plex_id = ?
              AND LOWER(track_title) = LOWER(?)
              AND LOWER(artist_name) = LOWER(?)
              AND (? = '' OR LOWER(album_name) = LOWER(?))
            ORDER BY play_count DESC, updated_at DESC
            LIMIT 1
          `).get(userPlexId, trackTitle, artistName, albumName, albumName) || {}
          : db.prepare(`
            SELECT track_title, artist_name, album_name,
                   COALESCE(SUM(play_count), 0) AS play_count,
                   COALESCE(SUM(skip_count), 0) AS skip_count,
                   COALESCE(MAX(consecutive_skips), 0) AS consecutive_skips,
                   COALESCE(MAX(excluded_from_smart), 0) AS excluded_from_smart,
                   COALESCE(MAX(manually_included), 0) AS manually_included,
                   COALESCE(MAX(tier), 'curatorr') AS tier
            FROM track_stats
            WHERE LOWER(track_title) = LOWER(?)
              AND LOWER(artist_name) = LOWER(?)
              AND (? = '' OR LOWER(album_name) = LOWER(?))
            GROUP BY track_title, artist_name, album_name
            ORDER BY play_count DESC
            LIMIT 1
          `).get(trackTitle, artistName, albumName, albumName) || {})
        : {};
      const fallbackHistoryStats = (trackTitle && artistName)
        ? (userPlexId
          ? db.prepare(`
            SELECT track_title, artist_name, album_name,
                   COUNT(*) AS play_count,
                   COALESCE(SUM(is_skip), 0) AS skip_count
            FROM play_events
            WHERE user_plex_id = ?
              AND LOWER(track_title) = LOWER(?)
              AND LOWER(artist_name) = LOWER(?)
              AND (? = '' OR LOWER(album_name) = LOWER(?))
            GROUP BY track_title, artist_name, album_name
            ORDER BY MAX(started_at) DESC
            LIMIT 1
          `).get(userPlexId, trackTitle, artistName, albumName, albumName) || {}
          : db.prepare(`
            SELECT track_title, artist_name, album_name,
                   COUNT(*) AS play_count,
                   COALESCE(SUM(is_skip), 0) AS skip_count
            FROM play_events
            WHERE LOWER(track_title) = LOWER(?)
              AND LOWER(artist_name) = LOWER(?)
              AND (? = '' OR LOWER(album_name) = LOWER(?))
            GROUP BY track_title, artist_name, album_name
            ORDER BY MAX(started_at) DESC
            LIMIT 1
          `).get(trackTitle, artistName, albumName, albumName) || {})
        : {};
      const effectiveStats = Object.keys(stats).length ? stats : fallbackStats;
      const playCount = Math.max(
        Number(stats.play_count || 0),
        Number(historyStats.play_count || 0),
        Number(fallbackStats.play_count || 0),
        Number(fallbackHistoryStats.play_count || 0),
      );
      const skipCount = Math.max(
        Number(stats.skip_count || 0),
        Number(historyStats.skip_count || 0),
        Number(fallbackStats.skip_count || 0),
        Number(fallbackHistoryStats.skip_count || 0),
      );
      const tier = normalizeTierKey(effectiveStats.excluded_from_smart ? 'skip' : effectiveStats.tier || 'curatorr');
      return res.json({
        ok: true,
        item: {
          kind: 'track',
          title: trackTitle || 'Unknown track',
          subtitle: [artistName, albumName].filter(Boolean).join(' · '),
          overview: buildOverviewText(trackMeta?.summary, `${trackTitle || 'This track'} currently has ${playCount} plays and ${skipCount} skips in Curatorr.`),
          thumb: `/api/music/thumb/track/${encodeURIComponent(ratingKey)}`,
          art: `/api/music/thumb/track/${encodeURIComponent(ratingKey)}`,
          pills: ['Track', formatTierLabel(tier)],
          stats: [
            { label: 'Plays', value: playCount },
            { label: 'Skips', value: skipCount },
            { label: 'Streak', value: Number(effectiveStats.consecutive_skips || 0) },
            { label: 'Pinned', value: Number(effectiveStats.manually_included || 0) ? 'Yes' : 'No' },
          ],
        },
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Suggestions ───────────────────────────────────────────────────────────

  app.get('/api/music/suggestions/artists', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const limit = Math.min(100, Math.max(1, Number(req.query?.limit || 25)));
    const artists = recommendationService.listCachedSuggestions(userPlexId, { artistLimit: limit }).artists;
    return res.json({ ok: true, artists });
  });

  app.get('/api/music/suggestions/albums', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const limit = Math.min(100, Math.max(1, Number(req.query?.limit || 25)));
    const albums = recommendationService.listCachedSuggestions(userPlexId, { albumLimit: limit }).albums;
    return res.json({ ok: true, albums });
  });

  app.get('/api/music/suggestions/tracks', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const limit = Math.min(200, Math.max(1, Number(req.query?.limit || 50)));
    const tracks = recommendationService.listCachedSuggestions(userPlexId, { trackLimit: limit }).tracks;
    return res.json({ ok: true, tracks });
  });

  app.post('/api/music/suggestions/rebuild', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    try {
      const rebuilt = recommendationService.rebuildSuggestionsForUser(userPlexId, {
        artistLimit: Math.min(100, Math.max(1, Number(req.body?.artistLimit || 12))),
        albumLimit: Math.min(100, Math.max(1, Number(req.body?.albumLimit || 12))),
        trackLimit: Math.min(200, Math.max(1, Number(req.body?.trackLimit || 24))),
      });
      pushLog({
        level: 'info',
        app: 'recommendations',
        action: 'rebuild',
        message: `Suggestions rebuilt for ${userPlexId}`,
        meta: rebuilt.counts,
      });
      return res.json({ ok: true, ...rebuilt });
    } catch (err) {
      pushLog({ level: 'error', app: 'recommendations', action: 'rebuild.error', message: safeMessage(err) });
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/suggestions/artists/:name/queue', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const artistName = decodeURIComponent(req.params.name);
    try {
      const config = loadConfig();
      if (!canUserAccessLidarrAutomation(config, req.session?.user)) {
        pushLog({
          level: 'warn',
          app: 'lidarr',
          action: 'artist.add.denied',
          message: `Blocked Lidarr add for ${userPlexId}: ${artistName}`,
          meta: { reason: 'automation_disabled_for_user', role: req.session?.user?.role || 'guest' },
        });
        return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
      }
      if (!lidarrService?.isConfigured()) {
        pushLog({
          level: 'warn',
          app: 'lidarr',
          action: 'artist.add.denied',
          message: `Blocked Lidarr add for ${userPlexId}: ${artistName}`,
          meta: { reason: 'lidarr_not_configured' },
        });
        return res.status(400).json({ error: 'Lidarr is not configured.' });
      }
      const existing = getSuggestedArtist(db, userPlexId, artistName);
      if (!existing) return res.status(404).json({ error: 'Suggestion not found.' });
      const role = String(req.session?.user?.role || 'user').trim().toLowerCase();
      const usageSnapshot = getCurrentLidarrUsage(db, userPlexId);
      const usage = usageSnapshot.usage || {};
      let quota = lidarrService.getRoleQuota(role, usage);
      let lidarrResult;
      let starterAlbum = null;
      let albumWarning = null;

      pushLog({
        level: 'info',
        app: 'lidarr',
        action: 'artist.add.request',
        message: `Received Lidarr add request for ${userPlexId}: ${artistName}`,
        meta: {
          role,
          usage,
          quota,
        },
      });

      try {
        const lookupResults = await lidarrService.lookupArtist(artistName);
        const lookupMatch = lidarrService.pickLookupArtist(lookupResults, artistName);
        const alreadyExists = Boolean(lookupMatch?.added && Number(lookupMatch?.id || 0) > 0);
        pushLog({
          level: 'info',
          app: 'lidarr',
          action: 'artist.add.lookup',
          message: `Resolved Lidarr lookup for ${artistName}`,
          meta: {
            lookupResults: Array.isArray(lookupResults) ? lookupResults.length : 0,
            matchedArtist: lookupMatch?.artistName || '',
            alreadyExists,
            lidarrArtistId: Number(lookupMatch?.id || 0) || null,
          },
        });
        if (!alreadyExists) {
          quota = lidarrService.assertQuotaAvailable(role, usage, { artists: 1 });
          pushLog({
            level: 'info',
            app: 'lidarr',
            action: 'artist.add.quota',
            message: `Quota check passed for ${userPlexId}: ${artistName}`,
            meta: { quota },
          });
        }
        lidarrResult = await lidarrService.addArtistFromSuggestion(artistName, {
          searchForMissingAlbums: false,
        });
        if (!lidarrResult.existing) {
          recordLidarrUsage(db, userPlexId, { roleName: role, usageKey: 'artists', amount: 1 });
          quota = lidarrService.getRoleQuota(role, getCurrentLidarrUsage(db, userPlexId).usage || {});
        }
      } catch (err) {
        if (err?.code === 'ARTIST_QUOTA_REACHED' || err?.code === 'ALBUM_QUOTA_REACHED') {
          const queuedRequest = await lidarrService.queueArtistAlbumRequest({
            userPlexId,
            artistName,
            sourceKind: 'automatic',
            allowCuratorrFallback: true,
            note: 'Queued from suggested artists because weekly quota was reached.',
          });
          pushLog({
            level: 'warn',
            app: 'lidarr',
            action: 'artist.add.queued',
            message: `Queued Lidarr add for ${userPlexId}: ${artistName} after quota limit`,
            meta: { quota: err.quota || quota, code: err.code, requestId: queuedRequest.id },
          });
          return res.status(202).json({
            ok: true,
            queued: true,
            request: queuedRequest,
            quota: err.quota || quota,
            message: 'Quota reached. Added to your queue instead.',
          });
        }
        throw err;
      }

      const existingProgress = getLidarrArtistProgress(db, userPlexId, artistName);
      const currentAlbumsAdded = Number(existingProgress?.albumsAddedCount || 0);
      const now = Date.now();
      const autoTriggerManualSearch = Boolean(lidarrService.getSettings().autoTriggerManualSearch);
      const fallbackDelayMs = Math.max(1, Number(lidarrService.getSettings().manualSearchFallbackHours || 24)) * 60 * 60 * 1000;
      const rankSnapshot = getArtistRankSnapshot(db, userPlexId, artistName);
      const normalizedObservedRank = (() => {
        const current = Number(rankSnapshot?.rankingScore);
        if (Number.isFinite(current) && current >= 0 && current <= 10) return current;
        const previous = Number(existingProgress?.highestObservedRank);
        return Number.isFinite(previous) && previous >= 0 && previous <= 10 ? previous : 0;
      })();
      let nextProgress = {
        artistName,
        lidarrArtistId: lidarrResult.artistId || existingProgress?.lidarrArtistId || existing.lidarrArtistId || null,
        currentStage: lidarrResult.existing ? 'added' : 'queued',
        albumsAddedCount: currentAlbumsAdded,
        highestObservedRank: normalizedObservedRank,
        lastAlbumAddedAt: existingProgress?.lastAlbumAddedAt ?? null,
        nextReviewAt: existingProgress?.nextReviewAt ?? (now + DAY_MS),
        lastManualSearchAt: existingProgress?.lastManualSearchAt ?? null,
        lastManualSearchStatus: existingProgress?.lastManualSearchStatus || '',
        updatedAt: now,
      };

      if (Number(lidarrResult.artistId || 0) > 0) {
        try {
          const albumList = await lidarrService.listArtistAlbums(lidarrResult.artistId, { timeoutMs: 15000 });
          const pickedAlbum = lidarrService.pickStarterAlbum(albumList);
          if (pickedAlbum?.album) {
            const album = pickedAlbum.album;
            const albumId = Number(album.id || 0);
            const albumTitle = String(album.title || '').trim();
            const alreadyMonitored = Boolean(album.monitored);
            let searchCommand = null;
            let latestUsage = getCurrentLidarrUsage(db, userPlexId).usage || {};
            let albumQuota = lidarrService.getRoleQuota(role, latestUsage);

            if (!alreadyMonitored) {
              albumQuota = lidarrService.assertQuotaAvailable(role, latestUsage, { albums: 1 });
              await lidarrService.setAlbumMonitored(albumId, true);
              recordLidarrUsage(db, userPlexId, { roleName: role, usageKey: 'albums', amount: 1 });
              latestUsage = getCurrentLidarrUsage(db, userPlexId).usage || {};
              albumQuota = lidarrService.getRoleQuota(role, latestUsage);
              if (autoTriggerManualSearch) {
                searchCommand = await lidarrService.triggerAlbumSearch([albumId]);
              }
            }

            upsertSuggestedAlbum(db, userPlexId, {
              artistName,
              albumTitle,
              albumType: String(album.albumType || ''),
              releaseDate: String(album.releaseDate || ''),
              selectionReason: pickedAlbum.selectionReason,
              rankScore: Number(album?.ratings?.value || 0),
              status: alreadyMonitored ? 'already_monitored' : 'added_to_lidarr',
              lidarrAlbumId: albumId || null,
              updatedAt: Date.now(),
            });

            starterAlbum = {
              albumId,
              albumTitle,
              albumType: String(album.albumType || ''),
              releaseDate: String(album.releaseDate || ''),
              selectionReason: pickedAlbum.selectionReason,
              alreadyMonitored,
              commandId: Number(searchCommand?.id || 0) || null,
            };
            nextProgress = {
              ...nextProgress,
              currentStage: alreadyMonitored ? 'starter_album_linked' : 'starter_album_added',
              albumsAddedCount: alreadyMonitored ? Math.max(currentAlbumsAdded, 1) : currentAlbumsAdded + 1,
              lastAlbumAddedAt: alreadyMonitored ? (existingProgress?.lastAlbumAddedAt ?? null) : Date.now(),
              nextReviewAt: Date.now() + fallbackDelayMs,
              lastManualSearchAt: searchCommand ? Date.now() : (existingProgress?.lastManualSearchAt ?? null),
              lastManualSearchStatus: searchCommand ? 'queued' : (existingProgress?.lastManualSearchStatus || ''),
              updatedAt: Date.now(),
            };
            quota = albumQuota;
            pushLog({
              level: 'info',
              app: 'lidarr',
              action: alreadyMonitored ? 'album.exists' : 'album.add',
              message: `${alreadyMonitored ? 'Linked monitored' : 'Seeded starter'} album for ${userPlexId}: ${artistName} — ${albumTitle}`,
              meta: {
                lidarrArtistId: lidarrResult.artistId || null,
                lidarrAlbumId: albumId || null,
                selectionReason: pickedAlbum.selectionReason,
                searchCommandId: Number(searchCommand?.id || 0) || null,
                quota,
              },
            });
          } else {
            albumWarning = { type: 'no_album_match', message: 'No starter album could be selected.' };
            nextProgress = {
              ...nextProgress,
              currentStage: 'added',
              nextReviewAt: Date.now() + DAY_MS,
              updatedAt: Date.now(),
            };
            pushLog({
              level: 'warn',
              app: 'lidarr',
              action: 'album.pick.none',
              message: `No starter album could be selected for ${userPlexId}: ${artistName}`,
              meta: { lidarrArtistId: lidarrResult.artistId || null },
            });
          }
        } catch (err) {
          albumWarning = { type: err?.code === 'ALBUM_QUOTA_REACHED' ? 'album_quota' : 'album_seed_error', message: safeMessage(err) };
          nextProgress = {
            ...nextProgress,
            currentStage: err?.code === 'ALBUM_QUOTA_REACHED' ? 'quota_blocked' : nextProgress.currentStage,
            nextReviewAt: Date.now() + DAY_MS,
            updatedAt: Date.now(),
          };
          pushLog({
            level: err?.code === 'ALBUM_QUOTA_REACHED' ? 'warn' : 'error',
            app: 'lidarr',
            action: err?.code === 'ALBUM_QUOTA_REACHED' ? 'album.quota_rejected' : 'album.seed.error',
            message: `Starter album step failed for ${userPlexId}: ${artistName}`,
            meta: {
              error: safeMessage(err),
              code: err?.code || '',
              lidarrArtistId: lidarrResult.artistId || null,
            },
          });
        }
      }

      saveLidarrArtistProgress(db, userPlexId, nextProgress);
      const nextReason = {
        ...(existing.reason || {}),
        manualAction: lidarrResult.existing ? 'already_in_lidarr' : 'added_to_lidarr',
        manualActionAt: Date.now(),
        lidarrExisting: Boolean(lidarrResult.existing),
        starterAlbum,
        albumWarning,
      };
      const updated = setSuggestedArtistStatus(
        db,
        userPlexId,
        artistName,
        'added_to_lidarr',
        {
          reason: nextReason,
          lidarrArtistId: lidarrResult.artistId || null,
        },
      );
      pushLog({
        level: 'info',
        app: 'lidarr',
        action: lidarrResult.existing ? 'artist.exists' : 'artist.add',
        message: `${lidarrResult.existing ? 'Linked existing' : 'Added'} Lidarr artist for ${userPlexId}: ${artistName}`,
        meta: { lidarrArtistId: lidarrResult.artistId || null, quota, starterAlbum, albumWarning },
      });
      enqueueLidarrRequest(db, userPlexId, {
        sourceKind: 'automatic',
        requestKind: 'artist_album',
        artistName,
        albumTitle: String(starterAlbum?.albumTitle || ''),
        status: 'completed',
        foreignArtistId: '',
        lidarrArtistId: lidarrResult.artistId || null,
        lidarrAlbumId: starterAlbum?.albumId || null,
        processedAt: Date.now(),
        detail: {
          selectionReason: String(starterAlbum?.selectionReason || ''),
          commandId: Number(starterAlbum?.commandId || 0) || null,
          note: 'Completed from suggested artist automation.',
        },
      });
      return res.json({ ok: true, artist: updated, lidarr: lidarrResult, starterAlbum, albumWarning, quota });
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'lidarr',
        action: 'artist.add.error',
        message: `Lidarr add request failed for ${userPlexId}: ${artistName}`,
        meta: { error: safeMessage(err) },
      });
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/lidarr/review', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const role = String(req.session?.user?.role || 'user').trim().toLowerCase();
    const artistName = String(req.body?.artistName || '').trim();
    const force = req.body?.force !== false;
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    if (!lidarrService?.isConfigured()) {
      return res.status(400).json({ error: 'Lidarr is not configured.' });
    }
    try {
      if (artistName) {
        const result = await lidarrService.reviewArtistProgression({
          userPlexId,
          artistName,
          role,
          force,
        });
        if (result?.status === 'quota_blocked') {
          return res.status(409).json({ ok: false, error: 'Weekly album quota reached.', result });
        }
        return res.json({ ok: true, result });
      }
      const results = await lidarrService.reviewDueArtists({
        userPlexId,
        limit: Math.max(1, Math.min(25, Number(req.body?.limit || 10))),
      });
      return res.json({ ok: true, results });
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'lidarr',
        action: 'review.error',
        message: `Manual Lidarr review failed for ${userPlexId}${artistName ? `: ${artistName}` : ''}`,
        meta: { error: safeMessage(err) },
      });
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/suggestions/artists/:name/dismiss', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const artistName = decodeURIComponent(req.params.name);
    try {
      const existing = getSuggestedArtist(db, userPlexId, artistName);
      if (!existing) return res.status(404).json({ error: 'Suggestion not found.' });
      const nextReason = { ...(existing.reason || {}), manualAction: 'dismissed', manualActionAt: Date.now() };
      const updated = setSuggestedArtistStatus(db, userPlexId, artistName, 'dismissed', { reason: nextReason });
      pushLog({
        level: 'info',
        app: 'recommendations',
        action: 'artist.dismiss',
        message: `Dismissed suggested artist: ${artistName}`,
      });
      return res.json({ ok: true, artist: updated });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Playlist track listing (paginated, fetched from Plex) ────────────────

  app.get('/api/music/playlist/tracks', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const config = loadConfig();
    const { url } = config.plex || {};
    const token = resolveUserPlexServerToken(config, req.session?.user || userPlexId, req.session?.plexServerToken || '');
    if (!url || !token) return res.json({ tracks: [], total: 0, playlistTitle: null });

    const offset = Math.max(0, Number(req.query.offset || 0));
    const limit = Math.min(200, Math.max(1, Number(req.query.limit || 100)));

    // Optional: caller may specify a specific Plex playlist ID to view
    let playlistId, playlistTitle;
    const requestedPlexId = String(req.query.plexPlaylistId || '').trim();
    if (requestedPlexId) {
      // Validate ownership — must be the user's legacy or one of their generated playlists
      const legacyRow = getUserPlaylist(db, userPlexId);
      const generated = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
        .find((p) => p.plexPlaylistId === requestedPlexId);
      if (generated) {
        playlistId = generated.plexPlaylistId;
        playlistTitle = generated.playlistTitle;
      } else if (legacyRow?.playlist_id === requestedPlexId) {
        playlistId = legacyRow.playlist_id;
        playlistTitle = legacyRow.playlist_title;
      } else {
        return res.status(403).json({ error: 'Not authorized to view this playlist.' });
      }
    } else {
      const playlistRow = getUserPlaylist(db, userPlexId);
      if (!playlistRow?.playlist_id) return res.json({ tracks: [], total: 0, playlistTitle: null });
      playlistId = playlistRow.playlist_id;
      playlistTitle = playlistRow.playlist_title;
    }

    try {
      const plexUrl = new URL(`${url.replace(/\/$/, '')}/playlists/${playlistId}/items`);
      plexUrl.searchParams.set('X-Plex-Container-Start', String(offset));
      plexUrl.searchParams.set('X-Plex-Container-Size', String(limit));
      const r = await fetch(plexUrl.toString(), {
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
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
        playlistTitle,
        playlistId,
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

  app.post('/api/music/playlist/job/dismiss', requireUser, (req, res) => {
    const userPlexId = String(req.session?.user?.username || '').trim();
    if (!userPlexId) return res.status(401).json({ error: 'Auth required.' });
    clearPlaylistJob(db, userPlexId);
    return res.json({ ok: true });
  });

  app.post('/api/music/playlists/daily-mix/sync', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    try {
      const result = await playlistService.syncDailyMix(userPlexId, {
        favoriteLimit: Number(req.body?.favoriteLimit || 12),
        suggestedLimit: Number(req.body?.suggestedLimit || 8),
        freshLimit: Number(req.body?.freshLimit || 10),
        maxTracks: Number(req.body?.maxTracks || 24),
      });
      pushLog({
        level: 'info',
        app: 'playlist',
        action: 'daily-mix.sync',
        message: `Daily Mix synced for ${userPlexId}`,
        meta: {
          trackCount: result.trackCount,
          plexPlaylistId: result.plexPlaylistId,
          sourceBreakdown: result.sourceBreakdown,
        },
      });
      return res.json({ ok: true, dailyMix: result });
    } catch (err) {
      pushLog({ level: 'error', app: 'playlist', action: 'daily-mix.error', message: safeMessage(err) });
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/playlist/excluded', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const excludedKeys = getExcludedTrackKeys(db, userPlexId);
    return res.json({ ok: true, excludedTracks: excludedKeys });
  });

  // ── Playlist track removal ────────────────────────────────────────────────

  app.delete('/api/music/playlist/tracks', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const config = loadConfig();
    const { url } = config.plex || {};
    const token = resolveUserPlexServerToken(config, req.session?.user || userPlexId, req.session?.plexServerToken || '');
    if (!url || !token) return res.status(400).json({ error: 'Plex not configured.' });

    const { plexPlaylistId, playlistItemID, ratingKey } = req.body || {};
    if (!plexPlaylistId || !playlistItemID) return res.status(400).json({ error: 'Missing plexPlaylistId or playlistItemID.' });

    // Validate ownership
    const legacyRow = getUserPlaylist(db, userPlexId);
    const generated = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((p) => String(p.plexPlaylistId) === String(plexPlaylistId));

    let playlistKey = null;
    if (generated) {
      playlistKey = generated.playlistKey;
    } else if (String(legacyRow?.playlist_id) !== String(plexPlaylistId)) {
      return res.status(403).json({ error: 'Not authorized to edit this playlist.' });
    }

    try {
      const base = url.replace(/\/$/, '');
      const delUrl = new URL(`${base}/playlists/${plexPlaylistId}/items`);
      delUrl.searchParams.set('playlistItemID', String(playlistItemID));
      const r = await fetch(delUrl.toString(), {
        method: 'DELETE',
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
      if (!r.ok) return res.status(502).json({ error: `Plex returned ${r.status}` });

      // Remove from playlist_tracks so sync doesn't re-add it
      if (ratingKey && playlistKey && (playlistKey === 'crescive' || playlistKey === 'curative')) {
        removePlaylistTracks(db, userPlexId, playlistKey, [String(ratingKey)]);
      }
      return res.json({ ok: true });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Add all artist tracks to a playlist ──────────────────────────────────

  app.post('/api/music/playlist/tracks/add-artist', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const config = loadConfig();
    const { url, machineId = '' } = config.plex || {};
    const token = resolveUserPlexServerToken(config, req.session?.user || userPlexId, req.session?.plexServerToken || '');
    if (!url || !token) return res.status(400).json({ error: 'Plex not configured.' });

    const { plexPlaylistId, artistName } = req.body || {};
    if (!plexPlaylistId || !artistName) return res.status(400).json({ error: 'Missing plexPlaylistId or artistName.' });

    // Validate ownership
    const legacyRow = getUserPlaylist(db, userPlexId);
    const generated = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((p) => String(p.plexPlaylistId) === String(plexPlaylistId));

    let playlistKey = null;
    if (generated) {
      playlistKey = generated.playlistKey;
    } else if (String(legacyRow?.playlist_id) !== String(plexPlaylistId)) {
      return res.status(403).json({ error: 'Not authorized to edit this playlist.' });
    }

    try {
      const normalised = cleanMasterArtistName(artistName);
      const artistTracks = getMasterTracks(db).filter(
        (t) => cleanMasterArtistName(t.artistName) === normalised,
      );
      if (!artistTracks.length) return res.json({ ok: true, added: 0 });

      let mid = machineId;
      if (!mid) {
        try {
          const r = await fetch(url.replace(/\/$/, ''), {
            headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
          });
          if (r.ok) mid = (await r.json())?.MediaContainer?.machineIdentifier || '';
        } catch (_) { /* non-fatal */ }
      }
      if (!mid) return res.status(500).json({ error: 'Could not determine Plex machine ID.' });

      const base = url.replace(/\/$/, '');
      const ratingKeys = artistTracks.map((t) => t.ratingKey);
      for (let i = 0; i < ratingKeys.length; i += 100) {
        const batch = ratingKeys.slice(i, i + 100);
        const uri = `server://${mid}/com.plexapp.plugins.library/library/metadata/${batch.join(',')}`;
        const addUrl = new URL(`${base}/playlists/${plexPlaylistId}/items`);
        addUrl.searchParams.set('uri', uri);
        await fetch(addUrl.toString(), {
          method: 'PUT',
          headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
        });
      }

      if (playlistKey && (playlistKey === 'crescive' || playlistKey === 'curative')) {
        addPlaylistTracks(db, userPlexId, playlistKey, artistTracks.map((t) => ({ ratingKey: t.ratingKey, artistName: t.artistName })));
      }
      return res.json({ ok: true, added: ratingKeys.length });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Create a custom playlist ──────────────────────────────────────────────

  app.post('/api/music/playlists/custom', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const config = loadConfig();
    const { url, machineId = '' } = config.plex || {};
    const token = resolveUserPlexServerToken(config, req.session?.user || userPlexId, req.session?.plexServerToken || '');
    if (!url || !token) return res.status(400).json({ error: 'Plex not configured.' });

    const { title } = req.body || {};
    if (!String(title || '').trim()) return res.status(400).json({ error: 'Playlist title is required.' });
    const playlistTitle = String(title).trim();

    try {
      let mid = machineId;
      if (!mid) {
        try {
          const r = await fetch(url.replace(/\/$/, ''), {
            headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
          });
          if (r.ok) mid = (await r.json())?.MediaContainer?.machineIdentifier || '';
        } catch (_) { /* non-fatal */ }
      }
      if (!mid) return res.status(500).json({ error: 'Could not determine Plex machine ID.' });

      const base = url.replace(/\/$/, '');
      // Create empty playlist with a dummy item first (Plex requires at least one item to create a playlist)
      // We'll use a placeholder approach: create then clear
      const createUrl = new URL(`${base}/playlists`);
      createUrl.searchParams.set('type', 'audio');
      createUrl.searchParams.set('title', playlistTitle);
      createUrl.searchParams.set('smart', '0');
      createUrl.searchParams.set('uri', `server://${mid}/com.plexapp.plugins.library`);
      const createRes = await fetch(createUrl.toString(), {
        method: 'POST',
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
      if (!createRes.ok) return res.status(502).json({ error: `Plex returned ${createRes.status}` });
      const createJson = await createRes.json();
      const newPlexId = String(createJson?.MediaContainer?.Metadata?.[0]?.ratingKey || '');
      if (!newPlexId) return res.status(502).json({ error: 'Plex did not return a playlist ID.' });

      // Record in generated playlists
      const playlistKey = `custom-${newPlexId}`;
      const now = Date.now();
      saveUserGeneratedPlaylist(db, userPlexId, {
        playlistKey,
        playlistTitle,
        plexPlaylistId: newPlexId,
        playlistType: 'custom',
        active: true,
        trackCount: 0,
        lastBuiltAt: now,
        lastSyncedAt: now,
      });

      return res.json({ ok: true, plexPlaylistId: newPlexId, playlistKey, playlistTitle });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Admin: all-users aggregate ────────────────────────────────────────────

  app.get('/api/music/admin/users', requireAdmin, (req, res) => {
    const users = getAllUserIds(db);
    return res.json({ ok: true, users });
  });

  // ── Admin: reassign all generated playlists to their correct user accounts ─
  // Deletes each playlist from Plex using the admin token (which is what created them),
  // then clears the stored plexPlaylistId so the next sync recreates them under each
  // user's own Plex token.

  app.post('/api/admin/playlists/reassign', requireAdmin, async (req, res) => {
    const config = loadConfig();
    const { url, token: adminToken } = config.plex || {};
    if (!url || !adminToken) return res.status(400).json({ error: 'Plex not configured.' });

    const all = listAllGeneratedPlaylists(db);
    if (!all.length) return res.json({ ok: true, reassigned: 0, skipped: 0 });

    const base = url.replace(/\/$/, '');
    let reassigned = 0;
    let skipped = 0;
    const errors = [];

    for (const entry of all) {
      try {
        const delUrl = `${base}/playlists/${entry.plexPlaylistId}`;
        const r = await fetch(delUrl, {
          method: 'DELETE',
          headers: buildPlexAuthHeaders(adminToken),
        });
        if (!r.ok && r.status !== 404) {
          errors.push(`${entry.playlistTitle} (${entry.plexPlaylistId}): HTTP ${r.status}`);
          skipped++;
          continue;
        }
        clearGeneratedPlaylistPlexId(db, entry.userPlexId, entry.playlistKey);
        reassigned++;
        pushLog({ level: 'info', app: 'playlist', action: 'playlist.reassign', message: `Deleted ${entry.playlistTitle} for ${entry.userPlexId} — will recreate under user token on next sync` });
      } catch (err) {
        errors.push(`${entry.playlistTitle}: ${safeMessage(err)}`);
        skipped++;
      }
    }

    return res.json({ ok: true, reassigned, skipped, errors });
  });

  // ── Image proxies (avoid exposing Plex token to browser) ─────────────────

  // Album art for a track by its Plex rating key (uses parentThumb from metadata)
  app.get('/api/music/thumb/track/:key', async (req, res) => {
    const config = loadConfig();
    const { url, token } = config.plex || {};
    if (!url || !token) return res.status(404).end();
    const key = req.params.key;
    const base = url.replace(/\/$/, '');
    try {
      const mr = await fetch(`${base}/library/metadata/${encodeURIComponent(key)}`, {
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
      if (!mr.ok) return res.status(404).end();
      const meta = await mr.json();
      const trackMeta = (meta?.MediaContainer?.Metadata || [])[0];
      const thumb = trackMeta?.parentThumb || trackMeta?.thumb || trackMeta?.grandparentThumb;
      if (!thumb) return res.status(404).end();

      const ir = await fetch(`${base}${thumb}`, {
        headers: buildPlexAuthHeaders(token, { Accept: 'image/*,*/*' }),
      });
      if (!ir.ok) return res.status(404).end();
      const buf = await ir.arrayBuffer();
      res.set('Content-Type', ir.headers.get('Content-Type') || 'image/jpeg');
      res.set('Cache-Control', 'public, max-age=86400');
      return res.send(Buffer.from(buf));
    } catch (_) {
      return res.status(404).end();
    }
  });

  // Artist art — prefer real Plex artist metadata, fall back to a named artist lookup
  app.get('/api/music/thumb/artist/:name', async (req, res) => {
    const config = loadConfig();
    const { url, token, libraries: selectedKeys = [] } = config.plex || {};
    if (!url || !token) return res.status(404).end();
    const artistName = decodeURIComponent(req.params.name);
    const base = url.replace(/\/$/, '');
    const trackRows = db.prepare(`
      SELECT rating_key, album_name
      FROM master_tracks
      WHERE artist_name = ?
      ORDER BY
        CASE
          WHEN lower(trim(album_name)) IN ('various artists', 'va', 'v/a', '[unknown]', 'unknown') THEN 1
          ELSE 0
        END,
        album_name,
        rating_key
      LIMIT 12
    `).all(artistName);
    if (!trackRows.length) return res.status(404).end();

    try {
      const requestedArtist = normalizeArtistMatchText(artistName);
      for (const key of selectedKeys) {
        const searchUrl = buildAppApiUrl(url, `library/sections/${key}/all`);
        searchUrl.searchParams.set('type', '8');
        searchUrl.searchParams.set('title', artistName);
        const searchRes = await fetch(searchUrl.toString(), {
          headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
        });
        if (!searchRes.ok) continue;
        const searchJson = await searchRes.json();
        const artistMeta = (searchJson?.MediaContainer?.Metadata || []).find((item) => {
          return normalizeArtistMatchText(item?.title) === requestedArtist;
        });
        const artistThumb = artistMeta?.thumb || artistMeta?.art;
        if (!artistThumb) continue;
        const ir = await fetch(`${base}${artistThumb}`, {
          headers: buildPlexAuthHeaders(token, { Accept: 'image/*,*/*' }),
        });
        if (!ir.ok) continue;
        const buf = await ir.arrayBuffer();
        res.set('Content-Type', ir.headers.get('Content-Type') || 'image/jpeg');
        res.set('Cache-Control', 'public, max-age=86400');
        return res.send(Buffer.from(buf));
      }

      for (const trackRow of trackRows) {
        const metaUrl = `${base}/library/metadata/${encodeURIComponent(trackRow.rating_key)}`;
        const mr = await fetch(metaUrl, {
          headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
        });
        if (!mr.ok) continue;
        const meta = await mr.json();
        const trackMeta = (meta?.MediaContainer?.Metadata || [])[0];
        if (!trackMeta) continue;

        const artistKey = String(trackMeta.grandparentRatingKey || trackMeta.grandparentKey || '')
          .match(/\/library\/metadata\/([^/?]+)/)?.[1] || String(trackMeta.grandparentRatingKey || '').trim();

        if (artistKey) {
          const artistMetaRes = await fetch(`${base}/library/metadata/${encodeURIComponent(artistKey)}`, {
            headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
          });
          if (artistMetaRes.ok) {
            const artistMetaJson = await artistMetaRes.json();
            const artistMeta = (artistMetaJson?.MediaContainer?.Metadata || [])[0];
            const artistTitle = normalizeArtistMatchText(artistMeta?.title);
            const requestedTitle = requestedArtist;
            const artistThumb = artistMeta?.thumb || artistMeta?.art;
            if (artistThumb && (!artistTitle || artistTitle === requestedTitle)) {
              const ir = await fetch(`${base}${artistThumb}`, {
                headers: buildPlexAuthHeaders(token, { Accept: 'image/*,*/*' }),
              });
              if (!ir.ok) continue;
              const buf = await ir.arrayBuffer();
              res.set('Content-Type', ir.headers.get('Content-Type') || 'image/jpeg');
              res.set('Cache-Control', 'public, max-age=86400');
              return res.send(Buffer.from(buf));
            }
          }
        }

        const fallbackArtistTitle = normalizeArtistMatchText(trackMeta?.grandparentTitle);
        const fallbackThumb = trackMeta?.grandparentThumb;
        if (fallbackThumb && fallbackArtistTitle && fallbackArtistTitle === requestedArtist) {
          const ir = await fetch(`${base}${fallbackThumb}`, {
            headers: buildPlexAuthHeaders(token, { Accept: 'image/*,*/*' }),
          });
          if (!ir.ok) continue;
          const buf = await ir.arrayBuffer();
          res.set('Content-Type', ir.headers.get('Content-Type') || 'image/jpeg');
          res.set('Cache-Control', 'public, max-age=86400');
          return res.send(Buffer.from(buf));
        }
      }

      // Plex does not create standalone artist metadata for some compilation-only tracks.
      // Use a lightweight artist search fallback so those entries do not collapse to blanks.
      try {
        const deezerUrl = new URL('https://api.deezer.com/search/artist');
        deezerUrl.searchParams.set('q', artistName);
        const deezerRes = await fetch(deezerUrl.toString(), {
          headers: {
            Accept: 'application/json',
            'User-Agent': 'Curatorr/1.0',
          },
        });
        if (deezerRes.ok) {
          const deezerJson = await deezerRes.json();
          const candidates = Array.isArray(deezerJson?.data) ? deezerJson.data : [];
          const picked = candidates.find((item) => normalizeArtistMatchText(item?.name) === requestedArtist)
            || candidates.find((item) => normalizeArtistMatchText(item?.name).startsWith(requestedArtist))
            || candidates[0];
          const remoteThumb = picked?.picture_big || picked?.picture_medium || picked?.picture;
          if (remoteThumb) {
            const ir = await fetch(remoteThumb, {
              headers: {
                Accept: 'image/*',
                'User-Agent': 'Curatorr/1.0',
              },
            });
            if (ir.ok) {
              const buf = await ir.arrayBuffer();
              res.set('Content-Type', ir.headers.get('Content-Type') || 'image/jpeg');
              res.set('Cache-Control', 'public, max-age=86400');
              return res.send(Buffer.from(buf));
            }
          }
        }
      } catch (_) {
        // Ignore external fallback failures and keep the endpoint safe.
      }

      res.set('Cache-Control', 'no-store');
      return res.status(404).end();
    } catch (_) {
      res.set('Cache-Control', 'no-store');
      return res.status(404).end();
    }
  });

  app.get('/api/music/thumb/album', async (req, res) => {
    const config = loadConfig();
    const { url, token } = config.plex || {};
    if (!url || !token) return res.status(404).end();
    const artistName = String(req.query?.artist || '').trim();
    const albumName = String(req.query?.album || '').trim();
    if (!artistName || !albumName) return res.status(404).end();
    const base = url.replace(/\/$/, '');
    const trackRow = db.prepare(
      'SELECT rating_key FROM master_tracks WHERE artist_name = ? AND album_name = ? LIMIT 1',
    ).get(artistName, albumName);
    if (!trackRow?.rating_key) return res.status(404).end();

    try {
      const mr = await fetch(`${base}/library/metadata/${encodeURIComponent(trackRow.rating_key)}`, {
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
      if (!mr.ok) return res.status(404).end();
      const meta = await mr.json();
      const trackMeta = (meta?.MediaContainer?.Metadata || [])[0];
      const thumb = trackMeta?.parentThumb || trackMeta?.thumb;
      if (!thumb) return res.status(404).end();

      const ir = await fetch(`${base}${thumb}`, {
        headers: buildPlexAuthHeaders(token, { Accept: 'image/*,*/*' }),
      });
      if (!ir.ok) return res.status(404).end();
      const buf = await ir.arrayBuffer();
      res.set('Content-Type', ir.headers.get('Content-Type') || 'image/jpeg');
      res.set('Cache-Control', 'public, max-age=86400');
      return res.send(Buffer.from(buf));
    } catch (_) {
      return res.status(404).end();
    }
  });

  app.get('/api/music/lidarr/image', requireUser, async (req, res) => {
    const config = loadConfig();
    const baseUrl = String(config?.lidarr?.url || '').replace(/\/$/, '');
    const path = String(req.query?.path || '').trim();
    if (!baseUrl || !isAllowedLidarrImagePath(path)) return res.status(404).end();
    try {
      const upstream = await fetch(`${baseUrl}${path}`, {
        headers: {
          'X-Api-Key': String(config?.lidarr?.apiKey || '').trim(),
          Accept: 'image/*,*/*',
        },
      });
      if (!upstream.ok) return res.status(upstream.status).end();
      const buf = await upstream.arrayBuffer();
      res.set('Content-Type', upstream.headers.get('Content-Type') || 'image/jpeg');
      res.set('Cache-Control', 'public, max-age=3600');
      return res.send(Buffer.from(buf));
    } catch (_err) {
      return res.status(404).end();
    }
  });

  app.get('/api/music/cover/release-group/:id', async (req, res) => {
    const id = String(req.params.id || '').trim();
    if (!id) return res.status(404).end();
    try {
      const upstream = await fetch(`https://coverartarchive.org/release-group/${encodeURIComponent(id)}/front-250`, {
        headers: {
          Accept: 'image/*,*/*',
          'User-Agent': 'Curatorr/phase2 (+https://github.com/MickyGX/curatorr)',
        },
      });
      if (!upstream.ok) return res.status(upstream.status).end();
      const buf = await upstream.arrayBuffer();
      res.set('Content-Type', upstream.headers.get('Content-Type') || 'image/jpeg');
      res.set('Cache-Control', 'public, max-age=21600');
      return res.send(Buffer.from(buf));
    } catch (_err) {
      return res.status(404).end();
    }
  });

  // ── Discovery carousels (Last.fm proxy) ───────────────────────────────────

  app.get('/api/discovery/artist-art/:name', async (req, res) => {
    const config = loadConfig();
    const disc = config.discovery || {};
    const artistName = decodeURIComponent(req.params.name || '').trim();
    if (!artistName) return res.status(404).end();

    try {
      if (disc.lastfmApiKey) {
        const infoUrl = new URL('https://ws.audioscrobbler.com/2.0/');
        infoUrl.searchParams.set('method', 'artist.getinfo');
        infoUrl.searchParams.set('artist', artistName);
        infoUrl.searchParams.set('api_key', disc.lastfmApiKey);
        infoUrl.searchParams.set('format', 'json');
        const infoRes = await fetch(infoUrl.toString(), {
          headers: {
            Accept: 'application/json',
            'User-Agent': 'Curatorr/1.0',
          },
          signal: AbortSignal.timeout(8000),
        });
        if (infoRes.ok) {
          const infoJson = await infoRes.json();
          const images = Array.isArray(infoJson?.artist?.image) ? infoJson.artist.image : [];
          const remoteThumb =
            images.find((img) => String(img?.size || '').toLowerCase() === 'extralarge')?.['#text']
            || images.find((img) => String(img?.size || '').toLowerCase() === 'large')?.['#text']
            || images.find((img) => String(img?.size || '').toLowerCase() === 'medium')?.['#text']
            || images.find((img) => String(img?.size || '').toLowerCase() === 'small')?.['#text']
            || '';
          if (remoteThumb) {
            const ir = await fetch(remoteThumb, {
              headers: {
                Accept: 'image/*',
                'User-Agent': 'Curatorr/1.0',
              },
              signal: AbortSignal.timeout(8000),
            });
            if (ir.ok) {
              const buf = await ir.arrayBuffer();
              res.set('Content-Type', ir.headers.get('Content-Type') || 'image/jpeg');
              res.set('Cache-Control', 'public, max-age=21600');
              return res.send(Buffer.from(buf));
            }
          }
        }
      }
    } catch (_) {
      // Ignore and fall through.
    }

    try {
      const deezerUrl = new URL('https://api.deezer.com/search/artist');
      deezerUrl.searchParams.set('q', artistName);
      const deezerRes = await fetch(deezerUrl.toString(), {
        headers: {
          Accept: 'application/json',
          'User-Agent': 'Curatorr/1.0',
        },
        signal: AbortSignal.timeout(8000),
      });
      if (deezerRes.ok) {
        const deezerJson = await deezerRes.json();
        const requestedArtist = normalizeArtistMatchText(artistName);
        const candidates = Array.isArray(deezerJson?.data) ? deezerJson.data : [];
        const picked = candidates.find((item) => normalizeArtistMatchText(item?.name) === requestedArtist)
          || candidates.find((item) => normalizeArtistMatchText(item?.name).startsWith(requestedArtist))
          || candidates[0];
        const remoteThumb = picked?.picture_big || picked?.picture_medium || picked?.picture;
        if (remoteThumb) {
          const ir = await fetch(remoteThumb, {
            headers: {
              Accept: 'image/*',
              'User-Agent': 'Curatorr/1.0',
            },
            signal: AbortSignal.timeout(8000),
          });
          if (ir.ok) {
            const buf = await ir.arrayBuffer();
            res.set('Content-Type', ir.headers.get('Content-Type') || 'image/jpeg');
            res.set('Cache-Control', 'public, max-age=21600');
            return res.send(Buffer.from(buf));
          }
        }
      }
    } catch (_) {
      // Ignore and fall through.
    }

    return res.redirect(302, `/api/music/thumb/artist/${encodeURIComponent(artistName)}?v=discover-artist-fallback-1`);
  });

  app.get('/api/discovery/trending', requireUser, async (req, res) => {
    const config = loadConfig();
    const disc = config.discovery || {};
    if (!disc.lastfmApiKey) return res.status(403).json({ ok: false, error: 'Discovery not configured.' });
    const type = req.query.type === 'tracks' ? 'tracks' : 'artists';
    if (type === 'artists' && !disc.showTrendingArtists) return res.json({ ok: true, items: [] });
    if (type === 'tracks'  && !disc.showTrendingTracks)  return res.json({ ok: true, items: [] });
    const method = type === 'artists' ? 'geo.getTopArtists' : 'geo.getTopTracks';
    const url = new URL('https://ws.audioscrobbler.com/2.0/');
    url.searchParams.set('method', method);
    url.searchParams.set('country', disc.region || 'united states');
    url.searchParams.set('limit', '20');
    url.searchParams.set('api_key', disc.lastfmApiKey);
    url.searchParams.set('format', 'json');
    try {
      const r = await fetch(url.toString(), { headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000) });
      if (!r.ok) return res.status(502).json({ ok: false, error: 'Last.fm upstream error.' });
      const data = await r.json();
      let rawItems;
      if (type === 'artists') {
        rawItems = (data?.topartists?.artist || []).map((a) => ({ name: a.name, listeners: Number(a.listeners || 0) }));
      } else {
        rawItems = (data?.tracks?.track || []).map((t) => ({ name: t.name, artistName: t.artist?.name || '', listeners: Number(t.listeners || 0) }));
      }
      const thumbNames = rawItems.map((item) => item.artistName || item.name);
      const thumbUrls = await Promise.all(thumbNames.map((n) => getLidarrArtistImageUrl(n)));
      const items = rawItems.map((item, i) => ({
        ...item,
        image: thumbUrls[i] || `/api/discovery/artist-art/${encodeURIComponent(item.artistName || item.name)}`,
      }));
      return res.json({ ok: true, items });
    } catch (err) {
      return res.status(502).json({ ok: false, error: 'Failed to fetch from Last.fm.' });
    }
  });

  app.get('/api/discovery/similar', requireUser, async (req, res) => {
    const config = loadConfig();
    const disc = config.discovery || {};
    if (!disc.lastfmApiKey) return res.status(403).json({ ok: false, error: 'Discovery not configured.' });
    if (!disc.showSimilarArtists) return res.json({ ok: true, items: [], basedOn: [] });
    const userPlexId = String(req.session?.user?.username || '').trim();
    const seedArtists = getTopArtists(db, userPlexId, 3).map((r) => r.artist_name).filter(Boolean);
    if (!seedArtists.length) return res.json({ ok: true, items: [], basedOn: [] });
    try {
      const calls = seedArtists.map((artist) => {
        const u = new URL('https://ws.audioscrobbler.com/2.0/');
        u.searchParams.set('method', 'artist.getSimilar');
        u.searchParams.set('artist', artist);
        u.searchParams.set('limit', '10');
        u.searchParams.set('api_key', disc.lastfmApiKey);
        u.searchParams.set('format', 'json');
        return fetch(u.toString(), { headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000) })
          .then((r) => r.ok ? r.json() : null).catch(() => null);
      });
      const results = await Promise.all(calls);
      const seedSet = new Set(seedArtists.map((a) => a.toLowerCase()));
      const seen = new Set();
      const items = [];
      for (const result of results) {
        for (const a of (result?.similarartists?.artist || [])) {
          const key = String(a.name || '').toLowerCase();
          if (!key || seedSet.has(key) || seen.has(key)) continue;
          seen.add(key);
          items.push({
            name: a.name,
            match: Number(a.match || 0),
            image: `/api/discovery/artist-art/${encodeURIComponent(a.name)}`,
          });
        }
      }
      items.sort((a, b) => b.match - a.match);
      const top20 = items.slice(0, 20);
      const thumbUrls = await Promise.all(top20.map((item) => getLidarrArtistImageUrl(item.name)));
      const enriched = top20.map((item, i) => ({
        ...item,
        image: thumbUrls[i] || `/api/discovery/artist-art/${encodeURIComponent(item.name)}`,
      }));
      return res.json({ ok: true, items: enriched, basedOn: seedArtists });
    } catch (err) {
      return res.status(502).json({ ok: false, error: 'Failed to fetch similar artists.' });
    }
  });

}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function resolveQueryUserId(req) {
  // Always use username — this matches how the user wizard stores playlist/prefs rows
  const user = req.session?.user || {};
  return String(user.username || '').trim();
}
