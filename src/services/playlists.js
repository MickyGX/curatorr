import {
  getUserPlaylist,
  listSuggestedTracks,
  listUserGeneratedPlaylists,
  recordPlaylistSync,
  saveUserGeneratedPlaylist,
  getUserPreferences,
  getMasterTracks,
  getPlaylistTracks,
  setPlaylistTracks,
  addPlaylistTracks,
  removePlaylistTracks,
  getPlaylistArtistState,
  setPlaylistArtistState,
  clearPlaylistState,
} from '../db.js';

const DAILY_MIX_PLAYLIST_KEY = 'daily-mix';
const DAILY_MIX_PLAYLIST_TYPE = 'daily-mix';
const CRESCIVE_PLAYLIST_KEY  = 'crescive';
const CRESCIVE_PLAYLIST_TYPE = 'crescive';
const CURATIVE_PLAYLIST_KEY  = 'curative';
const CURATIVE_PLAYLIST_TYPE = 'curative';
const ALGORITHM_VERSION = 'crescive-curative-v1';

function dedupeByRatingKey(items) {
  const seen = new Set();
  return items.filter((item) => {
    const key = String(item?.ratingKey || '').trim();
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function pickFavoriteTracks(db, userPlexId, limit = 12) {
  return db.prepare(`
    SELECT plex_rating_key, track_title, artist_name, album_name, play_count, tier, tier_weight, last_played_at
    FROM track_stats
    WHERE user_plex_id = ?
      AND manually_excluded = 0
      AND excluded_from_smart = 0
      AND play_count > 0
      AND (tier IS NULL OR tier != 'skip')
    ORDER BY tier_weight DESC, play_count DESC, last_played_at DESC
    LIMIT ?
  `).all(userPlexId, limit).map((row) => ({
    ratingKey: row.plex_rating_key,
    trackTitle: row.track_title,
    artistName: row.artist_name,
    albumName: row.album_name,
    source: 'favorite',
  }));
}

function pickSuggestedTracks(db, userPlexId, limit = 8) {
  return listSuggestedTracks(db, userPlexId, { limit }).map((row) => ({
    ratingKey: row.ratingKey,
    trackTitle: row.trackTitle,
    artistName: row.artistName,
    albumName: row.albumName,
    source: row.source || 'suggested',
  }));
}

function pickFreshLibraryTracks(db, userPlexId, limit = 10) {
  return db.prepare(`
    SELECT m.rating_key, m.track_title, m.artist_name, m.album_name
    FROM master_tracks m
    LEFT JOIN track_stats t
      ON t.plex_rating_key = m.rating_key AND t.user_plex_id = ?
    LEFT JOIN artist_stats a
      ON a.artist_name = m.artist_name AND a.user_plex_id = ?
    WHERE COALESCE(t.manually_excluded, 0) = 0
      AND COALESCE(t.excluded_from_smart, 0) = 0
      AND COALESCE(a.manually_excluded, 0) = 0
      AND COALESCE(a.excluded_from_smart, 0) = 0
      AND (t.plex_rating_key IS NULL OR COALESCE(t.play_count, 0) = 0)
    ORDER BY COALESCE(a.ranking_score, 0) DESC, m.artist_name ASC, m.album_name ASC, m.track_title ASC
    LIMIT ?
  `).all(userPlexId, userPlexId, limit).map((row) => ({
    ratingKey: row.rating_key,
    trackTitle: row.track_title,
    artistName: row.artist_name,
    albumName: row.album_name,
    source: 'fresh-library',
  }));
}

async function resolveMachineId(ctx, config) {
  const { buildAppApiUrl, saveConfig, loadConfig } = ctx;
  let machineId = String(config?.plex?.machineId || '').trim();
  const url = String(config?.plex?.url || '').trim();
  const token = String(config?.plex?.token || '').trim();
  if (machineId || !url || !token) return machineId;

  const idUrl = buildAppApiUrl(url, '');
  const response = await fetch(idUrl.toString(), {
    headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
  });
  if (!response.ok) return '';
  const json = await response.json();
  machineId = json?.MediaContainer?.machineIdentifier || '';
  if (machineId) {
    const latest = loadConfig();
    saveConfig({ ...latest, plex: { ...latest.plex, machineId } });
  }
  return machineId;
}

async function ensurePlexPlaylist(ctx, userPlexId, playlistKey, playlistTitle, machineId) {
  const { loadConfig } = ctx;
  const config = loadConfig();
  const { url } = config.plex || {};
  const token = ctx.resolveUserPlexServerToken(config, userPlexId);
  if (!url || !token || !machineId) throw new Error('Plex is not configured for playlist sync');

  const existing = listUserGeneratedPlaylists(ctx.db, userPlexId, { activeOnly: false })
    .find((entry) => entry.playlistKey === playlistKey);
  if (existing?.plexPlaylistId) return existing;

  // DB record exists but has no Plex ID — search Plex by title to avoid creating a duplicate
  const base = url.replace(/\/$/, '');
  if (existing && !existing.plexPlaylistId) {
    try {
      const searchRes = await fetch(`${base}/playlists?playlistType=audio`, {
        headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
      if (searchRes.ok) {
        const searchJson = await searchRes.json();
        const match = (searchJson?.MediaContainer?.Metadata || []).find((p) => p.title === playlistTitle);
        if (match?.ratingKey) {
          saveUserGeneratedPlaylist(ctx.db, userPlexId, {
            playlistType: DAILY_MIX_PLAYLIST_TYPE, playlistKey,
            plexPlaylistId: String(match.ratingKey),
            playlistTitle, algorithmVersion: 'phase2c-daily-mix',
            active: true, updatedAt: Date.now(),
          });
          return listUserGeneratedPlaylists(ctx.db, userPlexId, { activeOnly: false })
            .find((entry) => entry.playlistKey === playlistKey) || null;
        }
      }
    } catch { /* fall through to create */ }
  }

  const createUrl = new URL(`${base}/playlists`);
  createUrl.searchParams.set('type', 'audio');
  createUrl.searchParams.set('title', playlistTitle);
  createUrl.searchParams.set('smart', '0');
  createUrl.searchParams.set('uri', `server://${machineId}/com.plexapp.plugins.library`);
  const response = await fetch(createUrl.toString(), {
    method: 'POST',
    headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
  });
  if (!response.ok) throw new Error(`Create playlist failed: HTTP ${response.status}`);
  const json = await response.json();
  const plexPlaylistId = String(json?.MediaContainer?.Metadata?.[0]?.ratingKey || '').trim();
  if (!plexPlaylistId) throw new Error('Plex did not return a playlist ID after creation');

  saveUserGeneratedPlaylist(ctx.db, userPlexId, {
    playlistType: DAILY_MIX_PLAYLIST_TYPE,
    playlistKey,
    plexPlaylistId,
    playlistTitle,
    algorithmVersion: 'phase2c-daily-mix',
    active: true,
    updatedAt: Date.now(),
  });

  return listUserGeneratedPlaylists(ctx.db, userPlexId, { activeOnly: false })
    .find((entry) => entry.playlistKey === playlistKey) || null;
}

async function replacePlexPlaylistItems(ctx, userPlexId, plexPlaylistId, machineId, ratingKeys) {
  const config = ctx.loadConfig();
  const { url } = config.plex || {};
  const token = ctx.resolveUserPlexServerToken(config, userPlexId);
  const base = String(url || '').replace(/\/$/, '');
  if (!base || !token) throw new Error('Plex is not configured for playlist sync');

  const clearUrl = new URL(`${base}/playlists/${plexPlaylistId}/items`);
  await fetch(clearUrl.toString(), {
    method: 'DELETE',
    headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
  });

  for (let i = 0; i < ratingKeys.length; i += 100) {
    const batch = ratingKeys.slice(i, i + 100);
    const uri = `server://${machineId}/com.plexapp.plugins.library/library/metadata/${batch.join(',')}`;
    const addUrl = new URL(`${base}/playlists/${plexPlaylistId}/items`);
    addUrl.searchParams.set('uri', uri);
    const response = await fetch(addUrl.toString(), {
      method: 'PUT',
      headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
    });
    if (!response.ok) throw new Error(`Add playlist items failed: HTTP ${response.status}`);
  }
}

// ─── Smart playlist build pipeline ───────────────────────────────────────────

function buildSmartPlaylistPayload(db, userId, playlistCfg, masterTracks) {
  const prefs = getUserPreferences(db, userId);
  const likedArtistSet   = new Set((prefs.likedArtists   || []).map((a) => a.toLowerCase()));
  const ignoredArtistSet = new Set((prefs.ignoredArtists || []).map((a) => a.toLowerCase()));
  const likedGenreSet    = new Set(prefs.likedGenres   || []);
  const ignoredGenreSet  = new Set(prefs.ignoredGenres || []);

  // Artist similarity scores (best-effort — missing artists get 0)
  const scoreRows = db.prepare('SELECT artist_name, total_score FROM suggested_artists WHERE user_plex_id = ?').all(userId);
  const scoreMap  = new Map(scoreRows.map((r) => [r.artist_name.toLowerCase(), r.total_score]));

  // Per-user track play stats for ordering
  const statRows = db.prepare('SELECT plex_rating_key, tier_weight, play_count FROM track_stats WHERE user_plex_id = ?').all(userId);
  const statMap  = new Map(statRows.map((r) => [r.plex_rating_key, { tierWeight: r.tier_weight || 0, playCount: r.play_count || 0 }]));

  // Group tracks by artist
  const artistMap = new Map(); // lowerName → { name, genres: Set, tracks[] }
  for (const t of masterTracks) {
    const lower = t.artistName.toLowerCase();
    if (!artistMap.has(lower)) artistMap.set(lower, { name: t.artistName, genres: new Set(), tracks: [] });
    const entry = artistMap.get(lower);
    for (const g of (t.genres || [])) entry.genres.add(g);
    entry.tracks.push(t);
  }

  // Categorise artists
  const favourite = [], favouriteGenre = [], other = [];
  for (const [lower, { name, genres, tracks }] of artistMap) {
    if (ignoredArtistSet.has(lower)) continue;
    if (genres.size > 0 && [...genres].every((g) => ignoredGenreSet.has(g))) continue;
    const score = scoreMap.get(lower) || 0;
    if (likedArtistSet.has(lower)) {
      favourite.push({ name, lower, tracks, score, trackPct: playlistCfg.favouriteArtistTrackPct });
    } else if (likedGenreSet.size > 0 && [...genres].some((g) => likedGenreSet.has(g))) {
      favouriteGenre.push({ name, lower, tracks, score, trackPct: playlistCfg.favouriteGenreTrackPct });
    } else {
      other.push({ name, lower, tracks, score, trackPct: playlistCfg.otherGenreTrackPct });
    }
  }

  // Sort non-favourite buckets by similarity score DESC (closest match to user's taste first)
  favouriteGenre.sort((a, b) => b.score - a.score);
  other.sort((a, b) => b.score - a.score);

  // Apply artist count limits
  const fgLimit    = Math.ceil(favouriteGenre.length * (playlistCfg.favouriteGenreArtistPct ?? 1));
  const otherLimit = Math.ceil(other.length         * (playlistCfg.otherGenreArtistPct    ?? 0));
  const selected   = [...favourite, ...favouriteGenre.slice(0, fgLimit), ...other.slice(0, otherLimit)];

  // Per-artist: select top N% of tracks ordered by highest-rated
  const seen = new Set();
  const ratingKeys = [], trackList = [];
  for (const artist of selected) {
    const sorted = artist.tracks.slice().sort((a, b) => {
      const sa = statMap.get(a.ratingKey) || { tierWeight: 0, playCount: 0 };
      const sb = statMap.get(b.ratingKey) || { tierWeight: 0, playCount: 0 };
      if (sb.tierWeight !== sa.tierWeight) return sb.tierWeight - sa.tierWeight;
      if ((b.ratingCount || 0) !== (a.ratingCount || 0)) return (b.ratingCount || 0) - (a.ratingCount || 0);
      return sb.playCount - sa.playCount;
    });
    const count = Math.max(1, Math.ceil(sorted.length * (artist.trackPct ?? 1)));
    for (const t of sorted.slice(0, count)) {
      if (seen.has(t.ratingKey)) continue;
      seen.add(t.ratingKey);
      ratingKeys.push(t.ratingKey);
      trackList.push({ ratingKey: t.ratingKey, artistName: artist.name });
    }
  }

  return { ratingKeys, trackList };
}

// ─── Playlist evolution (additions + subtractions during sync) ────────────────

function applyPlaylistEvolution(db, userId, playlistKey, smartConfig, masterTracks) {
  const addRules       = smartConfig.additionRules  || {};
  const subRules       = (smartConfig.subtractionRules?.skip) || [];
  const skipRankThresh = smartConfig.artistSkipRank ?? 2;

  const currentTracks = getPlaylistTracks(db, userId, playlistKey);
  if (!currentTracks.length) return { toAdd: [], toRemove: [] };

  const masterMap = new Map(masterTracks.map((t) => [t.ratingKey, t]));

  // Track play stats
  const statRows = db.prepare(`
    SELECT plex_rating_key, tier, tier_weight, play_count, excluded_from_smart, manually_included
    FROM track_stats WHERE user_plex_id = ?
  `).all(userId);
  const statMap = new Map(statRows.map((r) => [r.plex_rating_key, r]));

  // Artist ranking scores — used to determine skip status
  const artistStatRows = db.prepare('SELECT artist_name, ranking_score FROM artist_stats WHERE user_plex_id = ?').all(userId);
  const artistScoreMap = new Map(artistStatRows.map((r) => [r.artist_name.toLowerCase(), r.ranking_score]));

  // Group current playlist tracks by artist
  const artistGroups = new Map(); // lower → { name, ratingKeys[] }
  for (const { ratingKey, artistName } of currentTracks) {
    const lower = artistName.toLowerCase();
    if (!artistGroups.has(lower)) artistGroups.set(lower, { name: artistName, ratingKeys: [] });
    artistGroups.get(lower).ratingKeys.push(ratingKey);
  }

  const toAdd    = []; // { ratingKey, artistName }
  const toRemove = new Set();

  // 1. Individual track consecutive-skip removal (applies to both playlist types)
  for (const { ratingKey } of currentTracks) {
    const stat = statMap.get(ratingKey);
    if (stat && stat.excluded_from_smart === 1 && stat.manually_included !== 1) toRemove.add(ratingKey);
  }

  // 2. Per-artist evolution rules
  for (const [lower, { name: artistName, ratingKeys }] of artistGroups) {
    const active = ratingKeys.filter((k) => !toRemove.has(k));
    if (!active.length) continue;

    // Artist skip status is determined by ranking_score (same threshold as smart playlist)
    const rankingScore  = artistScoreMap.get(lower) ?? 5;
    const isSkipArtist  = rankingScore <= skipRankThresh;

    const played    = active.filter((k) => { const s = statMap.get(k); return s && s.tier && s.tier !== 'curatorr'; });
    const playedPct = played.length / active.length;

    // Dominant tier of played tracks (used for addition rules only)
    const tierCounts = { belter: 0, decent: 0, 'half-decent': 0, skip: 0 };
    for (const k of played) { const t = statMap.get(k)?.tier; if (t && tierCounts[t] !== undefined) tierCounts[t]++; }
    const dominantTier = played.length === 0 ? 'curatorr' : Object.entries(tierCounts).sort((a, b) => b[1] - a[1])[0][0];

    const firedList = getPlaylistArtistState(db, userId, playlistKey, artistName);
    const fired     = new Set(firedList);
    let changed     = false;

    // Addition rules — only for non-skip-status artists
    if (!isSkipArtist && dominantTier !== 'curatorr') {
      const ruleKey  = dominantTier === 'half-decent' ? 'halfDecent' : dominantTier === 'skip' ? null : dominantTier;
      const rule     = ruleKey ? addRules[ruleKey] : null;
      if (rule) {
        const tKey = `${ruleKey}_${Math.round(rule.playedPct * 100)}`;
        if (!fired.has(tKey) && playedPct >= rule.playedPct) {
          const inSet = new Set(ratingKeys);
          const candidates = masterTracks
            .filter((t) => t.artistName.toLowerCase() === lower && !inSet.has(t.ratingKey))
            .sort((a, b) => {
              const sa = statMap.get(a.ratingKey) || { tier_weight: 0, play_count: 0 };
              const sb = statMap.get(b.ratingKey) || { tier_weight: 0, play_count: 0 };
              if ((sb.tier_weight || 0) !== (sa.tier_weight || 0)) return (sb.tier_weight || 0) - (sa.tier_weight || 0);
              if ((b.ratingCount || 0) !== (a.ratingCount || 0)) return (b.ratingCount || 0) - (a.ratingCount || 0);
              return (sb.play_count || 0) - (sa.play_count || 0);
            });
          for (const t of candidates.slice(0, rule.addCount)) toAdd.push({ ratingKey: t.ratingKey, artistName: t.artistName });
          fired.add(tKey);
          changed = true;
        }
      }
    }

    // Subtraction rules — only for artists that have reached skip STATUS (ranking_score <= threshold)
    if (isSkipArtist) {
      for (const rule of subRules) {
        const tKey = `skip_${Math.round(rule.playedPct * 100)}`;
        if (!fired.has(tKey) && playedPct >= rule.playedPct) {
          const candidates = active
            .filter((k) => { const s = statMap.get(k); return !s || s.tier === 'curatorr' || s.tier === 'skip'; })
            .sort((a, b) => {
              const sa = statMap.get(a) || { tier_weight: 0 };
              const sb = statMap.get(b) || { tier_weight: 0 };
              const ma = masterMap.get(a) || { ratingCount: 0 };
              const mb = masterMap.get(b) || { ratingCount: 0 };
              if ((sa.tier_weight || 0) !== (sb.tier_weight || 0)) return (sa.tier_weight || 0) - (sb.tier_weight || 0);
              return (ma.ratingCount || 0) - (mb.ratingCount || 0);
            });
          for (const k of candidates.slice(0, rule.removeCount)) toRemove.add(k);
          fired.add(tKey);
          changed = true;
        }
      }
    }

    if (changed) setPlaylistArtistState(db, userId, playlistKey, artistName, [...fired]);
  }

  return { toAdd, toRemove: [...toRemove] };
}

// ─── Plex playlist helper (generic, reusable) ─────────────────────────────────

async function ensureGeneratedPlaylist(ctx, userId, playlistType, playlistKey, playlistTitle, machineId) {
  const existing = listUserGeneratedPlaylists(ctx.db, userId, { activeOnly: false })
    .find((e) => e.playlistKey === playlistKey);
  if (existing?.plexPlaylistId) return existing;

  const config = ctx.loadConfig();
  const { url } = config.plex || {};
  const token = ctx.resolveUserPlexServerToken(config, userId);
  if (!url || !token || !machineId) throw new Error('Plex is not configured for playlist creation');

  const createUrl = new URL(`${url.replace(/\/$/, '')}/playlists`);
  createUrl.searchParams.set('type', 'audio');
  createUrl.searchParams.set('title', playlistTitle);
  createUrl.searchParams.set('smart', '0');
  createUrl.searchParams.set('uri', `server://${machineId}/com.plexapp.plugins.library`);
  const res = await fetch(createUrl.toString(), {
    method: 'POST',
    headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
  });
  if (!res.ok) throw new Error(`Create playlist failed: HTTP ${res.status}`);
  const json = await res.json();
  const plexPlaylistId = String(json?.MediaContainer?.Metadata?.[0]?.ratingKey || '').trim();
  if (!plexPlaylistId) throw new Error('Plex did not return a playlist ID after creation');

  saveUserGeneratedPlaylist(ctx.db, userId, {
    playlistType, playlistKey, plexPlaylistId, playlistTitle,
    algorithmVersion: ALGORITHM_VERSION, active: true, updatedAt: Date.now(),
  });
  return listUserGeneratedPlaylists(ctx.db, userId, { activeOnly: false }).find((e) => e.playlistKey === playlistKey);
}

// ─── Full crescive / curative sync ───────────────────────────────────────────

async function syncSmartPlaylistForUser(ctx, userId, playlistType, playlistKey, playlistCfg, smartConfig) {
  const { db, loadConfig, pushLog } = ctx;
  const config = loadConfig();
  // Skip local-only users — they have no personal Plex token so any playlist
  // operation would fall back to the admin token and appear on the wrong account.
  if (!ctx.userHasOwnPlexToken(config, userId)) return;
  const { url, machineId: storedMid = '' } = config.plex || {};
  const token = ctx.resolveUserPlexServerToken(config, userId);
  if (!url || !token) return;

  const masterTracks = getMasterTracks(db);
  if (!masterTracks.length) return;

  // Resolve machine ID (use admin token since this is a server property)
  const adminToken = String(config.plex?.token || '').trim() || token;
  let machineId = storedMid;
  if (!machineId) {
    try {
      const r = await fetch(url.replace(/\/$/, ''), {
        headers: ctx.buildPlexAuthHeaders(adminToken, { Accept: 'application/json' }),
      });
      if (r.ok) machineId = (await r.json())?.MediaContainer?.machineIdentifier || '';
    } catch { /* non-fatal */ }
  }
  if (!machineId) throw new Error('Could not determine Plex machine ID');

  const title = `${userId}'s ${playlistType === CRESCIVE_PLAYLIST_TYPE ? 'Crescive' : 'Curative'} Playlist`;
  const playlistRow = await ensureGeneratedPlaylist(ctx, userId, playlistType, playlistKey, title, machineId);

  const existing = getPlaylistTracks(db, userId, playlistKey);
  let finalTrackList;

  if (!existing.length) {
    // ── Initial build ──────────────────────────────────────────────────────
    const { ratingKeys, trackList } = buildSmartPlaylistPayload(db, userId, playlistCfg, masterTracks);
    setPlaylistTracks(db, userId, playlistKey, trackList);
    finalTrackList = trackList;
    pushLog({ level: 'info', app: 'playlist', action: `${playlistKey}.build`, message: `Initial ${playlistType} build: ${ratingKeys.length} tracks for ${userId}` });
  } else {
    // ── Evolution pass ─────────────────────────────────────────────────────
    const { toAdd, toRemove } = applyPlaylistEvolution(db, userId, playlistKey, smartConfig, masterTracks);
    if (toAdd.length) addPlaylistTracks(db, userId, playlistKey, toAdd);
    if (toRemove.length) removePlaylistTracks(db, userId, playlistKey, toRemove);
    finalTrackList = getPlaylistTracks(db, userId, playlistKey);
    if (toAdd.length || toRemove.length) {
      pushLog({ level: 'info', app: 'playlist', action: `${playlistKey}.evolve`, message: `${playlistType} evolved: +${toAdd.length} / -${toRemove.length} for ${userId}` });
    }
  }

  const ratingKeys = finalTrackList.map((t) => t.ratingKey);
  await replacePlexPlaylistItems(ctx, userId, playlistRow.plexPlaylistId, machineId, ratingKeys);

  const now = Date.now();
  saveUserGeneratedPlaylist(db, userId, {
    playlistType, playlistKey,
    plexPlaylistId: playlistRow.plexPlaylistId,
    playlistTitle: title,
    algorithmVersion: ALGORITHM_VERSION,
    lastBuiltAt: now, lastSyncedAt: now,
    trackCount: ratingKeys.length,
    active: true, updatedAt: now,
  });
  recordPlaylistSync(db, {
    userPlexId: userId, plexPlaylistId: playlistRow.plexPlaylistId,
    playlistTitle: title, trackCount: ratingKeys.length,
    excludedTracks: 0, excludedArtists: 0, trigger: 'auto',
  });
}

export function createPlaylistService(ctx) {
  const { db } = ctx;

  function listGenerated(userPlexId, options = {}) {
    return listUserGeneratedPlaylists(db, userPlexId, options);
  }

  function getGeneratedByKey(userPlexId, playlistKey) {
    return listGenerated(userPlexId, { activeOnly: false }).find((entry) => entry.playlistKey === playlistKey) || null;
  }

  function upsertGenerated(userPlexId, playlist) {
    saveUserGeneratedPlaylist(db, userPlexId, playlist);
    return getGeneratedByKey(userPlexId, playlist.playlistKey);
  }

  function getCanonicalPlaylist(userPlexId) {
    const legacy = getUserPlaylist(db, userPlexId);
    const generated = listGenerated(userPlexId, { activeOnly: false });
    const hasCrescive = generated.some((e) => e.playlistKey === CRESCIVE_PLAYLIST_KEY && e.active);
    const hasCurative = generated.some((e) => e.playlistKey === CURATIVE_PLAYLIST_KEY && e.active);
    return {
      legacy: (hasCrescive && hasCurative) ? null : legacy,
      generated,
      curatorred: generated.find((entry) => entry.playlistType === 'curatorred') || null,
    };
  }

  function buildDailyMix(userPlexId, options = {}) {
    const favoriteLimit = Math.max(1, Number(options.favoriteLimit || 12));
    const suggestedLimit = Math.max(1, Number(options.suggestedLimit || 8));
    const freshLimit = Math.max(1, Number(options.freshLimit || 10));
    const maxTracks = Math.max(10, Number(options.maxTracks || 24));

    const favorites = pickFavoriteTracks(db, userPlexId, favoriteLimit);
    const suggestions = pickSuggestedTracks(db, userPlexId, suggestedLimit);
    const fresh = pickFreshLibraryTracks(db, userPlexId, freshLimit);
    const combined = dedupeByRatingKey([...favorites, ...suggestions, ...fresh]).slice(0, maxTracks);

    return {
      playlistKey: DAILY_MIX_PLAYLIST_KEY,
      playlistType: DAILY_MIX_PLAYLIST_TYPE,
      playlistTitle: `${userPlexId}'s Daily Mix`,
      algorithmVersion: 'phase2c-daily-mix',
      trackCount: combined.length,
      trackKeys: combined.map((track) => track.ratingKey),
      tracks: combined,
      sourceBreakdown: {
        favorites: favorites.length,
        suggestions: suggestions.length,
        fresh: fresh.length,
      },
      builtAt: Date.now(),
    };
  }

  async function syncDailyMix(userPlexId, options = {}) {
    const config = ctx.loadConfig();
    if (!ctx.userHasOwnPlexToken(config, userPlexId)) throw new Error('User has no Plex account — playlist sync is not available for local-only users.');
    const { url } = config.plex || {};
    const token = ctx.resolveUserPlexServerToken(config, userPlexId);
    if (!url || !token) throw new Error('Plex is not configured');

    const mix = buildDailyMix(userPlexId, options);
    if (!mix.trackKeys.length) throw new Error('No Daily Mix tracks are available yet');

    const machineId = await resolveMachineId(ctx, config);
    if (!machineId) throw new Error('Could not determine Plex machine ID');

    const playlistRow = await ensurePlexPlaylist(ctx, userPlexId, mix.playlistKey, mix.playlistTitle, machineId);
    await replacePlexPlaylistItems(ctx, userPlexId, playlistRow.plexPlaylistId, machineId, mix.trackKeys);

    const syncedAt = Date.now();
    saveUserGeneratedPlaylist(db, userPlexId, {
      playlistType: mix.playlistType,
      playlistKey: mix.playlistKey,
      plexPlaylistId: playlistRow.plexPlaylistId,
      playlistTitle: mix.playlistTitle,
      algorithmVersion: mix.algorithmVersion,
      lastBuiltAt: mix.builtAt,
      lastSyncedAt: syncedAt,
      trackCount: mix.trackCount,
      active: true,
      updatedAt: syncedAt,
    });

    recordPlaylistSync(db, {
      userPlexId,
      plexPlaylistId: playlistRow.plexPlaylistId,
      playlistTitle: mix.playlistTitle,
      trackCount: mix.trackCount,
      excludedTracks: 0,
      excludedArtists: 0,
      trigger: 'manual',
    });

    return {
      ...mix,
      syncedAt,
      plexPlaylistId: playlistRow.plexPlaylistId,
    };
  }

  async function syncGlobalPlaylist(userId, playlistDef) {
    const config = ctx.loadConfig();
    if (!ctx.userHasOwnPlexToken(config, userId)) return;
    const { url, machineId: storedMid = '' } = config.plex || {};
    const token = ctx.resolveUserPlexServerToken(config, userId);
    if (!url || !token) return;

    const masterTracks = getMasterTracks(db);
    if (!masterTracks.length) return;

    const smartSettings = config.smartPlaylist || {};
    const skipRank   = Number(smartSettings.artistSkipRank   ?? 2);
    const belterRank = Number(smartSettings.artistBelterRank ?? 8);
    const rules = playlistDef.rules || {};

    function classifyArtist(score) {
      if (score === null || score === undefined) return 'unranked';
      if (score >= belterRank) return 'belter';
      if (score >= 5) return 'decent';
      if (score > skipRank) return 'halfDecent';
      return 'skip';
    }

    const artistMap = new Map(
      db.prepare('SELECT artist_name, ranking_score FROM artist_stats WHERE user_plex_id = ?').all(userId)
        .map((r) => [r.artist_name.toLowerCase(), r.ranking_score]),
    );
    const trackMap = new Map(
      db.prepare('SELECT plex_rating_key, tier, tier_weight FROM track_stats WHERE user_plex_id = ?').all(userId)
        .map((r) => [r.plex_rating_key, r]),
    );

    const artistTierFilter = Array.isArray(rules.artistTiers) && rules.artistTiers.length ? new Set(rules.artistTiers) : null;
    const trackTierFilter  = Array.isArray(rules.trackTiers)  && rules.trackTiers.length  ? new Set(rules.trackTiers)  : null;
    const topN = rules.topNPerArtist ? Math.max(1, Number(rules.topNPerArtist)) : null;
    const maxT = rules.maxTracks     ? Math.max(1, Number(rules.maxTracks))     : null;
    const sortBy = rules.sortBy || 'ratingCount';

    const byArtist = new Map();
    for (const t of masterTracks) {
      const score = artistMap.get((t.artistName || '').toLowerCase()) ?? null;
      const artistTier = classifyArtist(score);
      if (artistTierFilter && !artistTierFilter.has(artistTier)) continue;

      const stat = trackMap.get(t.ratingKey);
      const rawTier = stat?.tier || 'curatorr';
      const normTier = rawTier === 'half-decent' ? 'halfDecent' : rawTier === 'curatorr' ? 'unplayed' : rawTier;
      if (trackTierFilter && !trackTierFilter.has(normTier)) continue;

      if (!byArtist.has(t.artistName)) byArtist.set(t.artistName, []);
      byArtist.get(t.artistName).push({ ratingKey: t.ratingKey, rc: t.ratingCount || 0, tw: stat?.tier_weight || 0, pc: stat?.play_count || 0 });
    }

    let ratingKeys = [];
    for (const [, tracks] of byArtist) {
      const sorted = [...tracks].sort((a, b) => {
        if (sortBy === 'tierWeight') return (b.tw - a.tw) || (b.rc - a.rc);
        if (sortBy === 'playCount')  return (b.pc - a.pc) || (b.rc - a.rc);
        return (b.rc - a.rc) || (b.tw - a.tw); // default: ratingCount
      });
      const selected = topN ? sorted.slice(0, topN) : sorted;
      ratingKeys.push(...selected.map((t) => t.ratingKey));
    }
    if (maxT) ratingKeys = ratingKeys.slice(0, maxT);
    if (!ratingKeys.length) return;

    const adminToken = String(config.plex?.token || '').trim() || token;
    let machineId = storedMid;
    if (!machineId) {
      try {
        const r = await fetch(url.replace(/\/$/, ''), {
          headers: ctx.buildPlexAuthHeaders(adminToken, { Accept: 'application/json' }),
        });
        if (r.ok) machineId = (await r.json())?.MediaContainer?.machineIdentifier || '';
      } catch { /* non-fatal */ }
    }
    if (!machineId) throw new Error('Could not determine Plex machine ID');

    const playlistKey   = `global:${playlistDef.id}`;
    const playlistTitle = `${userId} - ${playlistDef.name}`;
    const playlistRow   = await ensureGeneratedPlaylist(ctx, userId, 'global', playlistKey, playlistTitle, machineId);
    await replacePlexPlaylistItems(ctx, userId, playlistRow.plexPlaylistId, machineId, ratingKeys);

    const now = Date.now();
    saveUserGeneratedPlaylist(db, userId, {
      playlistType: 'global', playlistKey,
      plexPlaylistId: playlistRow.plexPlaylistId,
      playlistTitle,
      algorithmVersion: 'global-playlist-v1',
      lastBuiltAt: now, lastSyncedAt: now,
      trackCount: ratingKeys.length,
      active: true, updatedAt: now,
    });
    ctx.pushLog({ level: 'info', app: 'playlist', action: 'global.sync', message: `Global playlist "${playlistDef.name}" synced: ${ratingKeys.length} tracks for ${userId}` });
  }

  async function syncCrescive(userId) {
    const smartConfig = ctx.loadConfig().smartPlaylist || {};
    const playlistCfg = { ...{ favouriteArtistTrackPct: 0.80, favouriteGenreArtistPct: 0.80, favouriteGenreTrackPct: 0.20, otherGenreArtistPct: 0.20, otherGenreTrackPct: 0.20 }, ...(smartConfig.crescive || {}) };
    return syncSmartPlaylistForUser(ctx, userId, CRESCIVE_PLAYLIST_TYPE, CRESCIVE_PLAYLIST_KEY, playlistCfg, smartConfig);
  }

  async function syncCurative(userId) {
    const smartConfig = ctx.loadConfig().smartPlaylist || {};
    const playlistCfg = { ...{ favouriteArtistTrackPct: 1.00, favouriteGenreArtistPct: 1.00, favouriteGenreTrackPct: 0.80, otherGenreArtistPct: 0.50, otherGenreTrackPct: 0.50 }, ...(smartConfig.curative || {}) };
    return syncSmartPlaylistForUser(ctx, userId, CURATIVE_PLAYLIST_TYPE, CURATIVE_PLAYLIST_KEY, playlistCfg, smartConfig);
  }

  async function syncBothForUser(userId) {
    await syncCrescive(userId);
    await syncCurative(userId);
  }

  return {
    listGenerated,
    getGeneratedByKey,
    upsertGenerated,
    getCanonicalPlaylist,
    buildDailyMix,
    syncDailyMix,
    syncGlobalPlaylist,
    syncCrescive,
    syncCurative,
    syncBothForUser,
    CRESCIVE_PLAYLIST_KEY,
    CURATIVE_PLAYLIST_KEY,
  };
}
