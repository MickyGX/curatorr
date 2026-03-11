import {
  getUserPreferences,
  listSuggestedAlbums,
  listSuggestedArtists,
  listSuggestedTracks,
  upsertSuggestedAlbum,
  upsertSuggestedArtist,
  upsertSuggestedTrack,
  cleanMasterArtistName,
} from '../db.js';

const DEFAULT_LIMITS = {
  artists: 12,
  albums: 12,
  tracks: 24,
};

const ARTIST_SUGGESTION_RETENTION_DAYS = 14;

function normalizeLimit(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

function normalizeScore(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function normalizeText(value) {
  return String(value || '').trim();
}

function keyify(value) {
  return normalizeText(value).toLowerCase();
}

function parseJson(value, fallback) {
  try { return JSON.parse(value); } catch (err) { return fallback; }
}

function daysSince(timestamp) {
  const n = Number(timestamp);
  if (!Number.isFinite(n) || n <= 0) return Number.POSITIVE_INFINITY;
  return (Date.now() - n) / (24 * 60 * 60 * 1000);
}

function scoreGenreSet(genres, affinity) {
  const weights = (Array.isArray(genres) ? genres : [])
    .map((genre) => Number(affinity.get(keyify(genre)) || 0))
    .filter((value) => value > 0)
    .sort((a, b) => b - a);
  return weights.slice(0, 3).reduce((sum, value) => sum + value, 0);
}

function topGenresFor(genres, affinity, limit = 3) {
  return [...new Set((Array.isArray(genres) ? genres : []).filter(Boolean))]
    .map((genre) => ({ genre, score: Number(affinity.get(keyify(genre)) || 0) }))
    .filter((entry) => entry.score > 0)
    .sort((a, b) => b.score - a.score || a.genre.localeCompare(b.genre))
    .slice(0, limit)
    .map((entry) => entry.genre);
}

function buildCatalog(db) {
  const rows = db.prepare(`
    SELECT rating_key, artist_name, track_title, album_name, genres
    FROM master_tracks
    ORDER BY artist_name ASC, album_name ASC, track_title ASC
  `).all();

  const tracks = [];
  const trackByRatingKey = new Map();
  const artists = new Map();
  const albums = new Map();

  for (const row of rows) {
    const track = {
      ratingKey: row.rating_key,
      artistName: cleanMasterArtistName(normalizeText(row.artist_name)),
      trackTitle: normalizeText(row.track_title),
      albumName: normalizeText(row.album_name),
      genres: parseJson(row.genres || '[]', []),
    };
    tracks.push(track);
    if (track.ratingKey) trackByRatingKey.set(track.ratingKey, track);

    const artistKey = keyify(track.artistName);
    if (!artists.has(artistKey)) {
      artists.set(artistKey, {
        artistName: track.artistName,
        genres: new Set(),
        albumTitles: new Set(),
        tracks: [],
      });
    }
    const artist = artists.get(artistKey);
    for (const genre of track.genres) if (genre) artist.genres.add(genre);
    if (track.albumName) artist.albumTitles.add(track.albumName);
    artist.tracks.push(track);

    const albumKey = `${artistKey}::${keyify(track.albumName)}`;
    if (!albums.has(albumKey)) {
      albums.set(albumKey, {
        albumKey,
        artistName: track.artistName,
        albumName: track.albumName,
        genres: new Set(),
        tracks: [],
      });
    }
    const album = albums.get(albumKey);
    for (const genre of track.genres) if (genre) album.genres.add(genre);
    album.tracks.push(track);
  }

  return {
    tracks,
    trackByRatingKey,
    artists,
    albums,
  };
}

function loadUserState(db, userPlexId) {
  const artistRows = db.prepare(`
    SELECT artist_name, play_count, skip_count, consecutive_skips,
           excluded_from_smart, manually_excluded, manually_included,
           ranking_score, last_played_at, last_skipped_at
    FROM artist_stats
    WHERE user_plex_id = ?
  `).all(userPlexId);

  const trackRows = db.prepare(`
    SELECT plex_rating_key, artist_name, track_title, album_name,
           play_count, skip_count, consecutive_skips,
           excluded_from_smart, manually_excluded, manually_included,
           tier, tier_weight, last_played_at, last_skipped_at
    FROM track_stats
    WHERE user_plex_id = ?
  `).all(userPlexId);

  return {
    artistStats: new Map(artistRows.map((row) => [keyify(cleanMasterArtistName(row.artist_name)), row])),
    trackStats: new Map(trackRows.map((row) => [row.plex_rating_key, row])),
  };
}

function getUserTasteProfile(db, userPlexId, options = {}) {
  const artistLimit = normalizeLimit(options.artistLimit, 10);
  const trackLimit = normalizeLimit(options.trackLimit, 25);
  const prefs = getUserPreferences(db, userPlexId);

  const topArtists = db.prepare(`
    SELECT artist_name, ranking_score, play_count, skip_count, last_played_at
    FROM artist_stats
    WHERE user_plex_id = ?
    ORDER BY ranking_score DESC, play_count DESC, last_played_at DESC
    LIMIT ?
  `).all(userPlexId, artistLimit).map((row) => ({
    artistName: row.artist_name,
    rankingScore: normalizeScore(row.ranking_score),
    playCount: Number(row.play_count || 0),
    skipCount: Number(row.skip_count || 0),
    lastPlayedAt: row.last_played_at,
  }));

  const recentTracks = db.prepare(`
    SELECT plex_rating_key, artist_name, track_title, album_name, tier, tier_weight, last_played_at
    FROM track_stats
    WHERE user_plex_id = ? AND last_played_at IS NOT NULL
    ORDER BY last_played_at DESC, updated_at DESC
    LIMIT ?
  `).all(userPlexId, trackLimit).map((row) => ({
    ratingKey: row.plex_rating_key,
    artistName: row.artist_name,
    trackTitle: row.track_title,
    albumName: row.album_name,
    tier: row.tier,
    tierWeight: normalizeScore(row.tier_weight),
    lastPlayedAt: row.last_played_at,
  }));

  return {
    userPlexId,
    likedGenres: prefs.likedGenres,
    ignoredGenres: prefs.ignoredGenres,
    likedArtists: prefs.likedArtists,
    ignoredArtists: prefs.ignoredArtists,
    topArtists,
    recentTracks,
  };
}

function buildGenreAffinity(profile, catalog) {
  const affinity = new Map();
  const addGenre = (genre, weight) => {
    const key = keyify(genre);
    if (!key) return;
    affinity.set(key, Number(affinity.get(key) || 0) + weight);
  };

  for (const genre of profile.likedGenres || []) addGenre(genre, 4);
  for (const genre of profile.ignoredGenres || []) addGenre(genre, -5);

  for (const artist of profile.topArtists || []) {
    const catalogArtist = catalog.artists.get(keyify(artist.artistName));
    if (!catalogArtist) continue;
    const weight = 1 + Math.min(4, artist.rankingScore / 2) + Math.min(3, artist.playCount / 5);
    for (const genre of catalogArtist.genres) addGenre(genre, weight);
  }

  for (const track of profile.recentTracks || []) {
    const catalogTrack = catalog.trackByRatingKey.get(track.ratingKey);
    if (!catalogTrack) continue;
    let weight = 0;
    if (track.tier === 'belter') weight = 3.5;
    else if (track.tier === 'half-decent') weight = 2.25;
    else if (track.tier === 'decent') weight = 1.25;
    else if (track.tier === 'skip') weight = -2.5;
    if (!weight) continue;
    for (const genre of catalogTrack.genres) addGenre(genre, weight);
  }

  return affinity;
}

function buildArtistSuggestions(profile, catalog, userState, limits) {
  const likedArtistKeys = new Set((profile.likedArtists || []).map(keyify));
  const ignoredArtistKeys = new Set((profile.ignoredArtists || []).map(keyify));
  const topArtistKeys = new Set((profile.topArtists || []).map((artist) => keyify(artist.artistName)));
  const suggestions = [];

  for (const artist of catalog.artists.values()) {
    const artistKey = keyify(artist.artistName);
    const stats = userState.artistStats.get(artistKey);
    if (!artist.artistName || artistKeyInSet(artistKey, ignoredArtistKeys)) continue;
    if (stats?.manually_excluded || stats?.excluded_from_smart) continue;
    if (likedArtistKeys.has(artistKey)) continue;
    if (stats && Number(stats.play_count || 0) >= 12 && Number(stats.ranking_score || 0) >= 7) continue;

    const genreScore = scoreGenreSet([...artist.genres], limits.genreAffinity);
    const playCount = Number(stats?.play_count || 0);
    const rankingScore = normalizeScore(stats?.ranking_score || 0);
    const skipCount = Number(stats?.skip_count || 0);
    const recencyDays = daysSince(stats?.last_played_at);

    let behaviorScore = 0;
    if (!stats || playCount === 0) behaviorScore += 4;
    else if (playCount <= 2) behaviorScore += 2.75;
    else if (playCount <= 5) behaviorScore += 1.5;
    else behaviorScore -= Math.min(2, playCount * 0.15);

    if (recencyDays >= 30 && Number.isFinite(recencyDays)) behaviorScore += 1.5;
    if (recencyDays >= 90 && Number.isFinite(recencyDays)) behaviorScore += 1;
    behaviorScore += Math.max(0, rankingScore - 3) * 0.35;
    behaviorScore -= skipCount * 0.5;

    let editorialScore = 0;
    if (topArtistKeys.has(artistKey)) editorialScore -= 3;
    if (artist.albumTitles.size >= 2) editorialScore += 0.75;
    if (artist.tracks.length >= 8) editorialScore += 0.5;
    if ((profile.likedGenres || []).some((genre) => artist.genres.has(genre))) editorialScore += 1;

    const totalScore = genreScore + behaviorScore + editorialScore;
    if (totalScore <= 0.5) continue;

    suggestions.push({
      artistName: artist.artistName,
      source: 'library-affinity',
      similarityScore: Number(genreScore.toFixed(3)),
      behaviorScore: Number(behaviorScore.toFixed(3)),
      editorialScore: Number(editorialScore.toFixed(3)),
      totalScore: Number(totalScore.toFixed(3)),
      status: 'suggested',
      reason: {
        topGenres: topGenresFor([...artist.genres], limits.genreAffinity),
        playCount,
        rankingScore,
        recencyDays: Number.isFinite(recencyDays) ? Math.round(recencyDays) : null,
        albumCount: artist.albumTitles.size,
        trackCount: artist.tracks.length,
      },
    });
  }

  return suggestions
    .sort((a, b) => b.totalScore - a.totalScore || a.artistName.localeCompare(b.artistName))
    .slice(0, limits.artistLimit);
}

function artistKeyInSet(artistKey, keySet) {
  if (keySet.has(artistKey)) return true;
  const parts = artistKey.split(/\s*[&\/,]\s*|\s+and\s+/).map((s) => s.trim()).filter(Boolean);
  return parts.length > 1 && parts.some((p) => keySet.has(p));
}

function buildTrackSuggestions(profile, catalog, userState, artistSuggestions, limits) {
  const ignoredArtistKeys = new Set((profile.ignoredArtists || []).map(keyify));
  const suggestedArtistKeys = new Set(artistSuggestions.map((artist) => keyify(artist.artistName)));
  const tracks = [];

  for (const track of catalog.tracks) {
    const artistKey = keyify(track.artistName);
    const stats = userState.trackStats.get(track.ratingKey);
    const artistStats = userState.artistStats.get(artistKey);
    if (!track.trackTitle || !track.artistName || artistKeyInSet(artistKey, ignoredArtistKeys)) continue;
    if (stats?.manually_excluded || stats?.excluded_from_smart) continue;
    if (artistStats?.manually_excluded || artistStats?.excluded_from_smart) continue;

    const playCount = Number(stats?.play_count || 0);
    const skipCount = Number(stats?.skip_count || 0);
    const recencyDays = daysSince(stats?.last_played_at);
    const genreScore = scoreGenreSet(track.genres, limits.genreAffinity);
    const artistRank = normalizeScore(artistStats?.ranking_score || 0);

    let behaviorScore = 0;
    if (!stats || playCount === 0) behaviorScore += 3.25;
    else if (recencyDays >= 21) behaviorScore += 2.25;
    else if (recencyDays >= 10) behaviorScore += 0.75;
    else behaviorScore -= 4;

    if (stats?.tier === 'belter') behaviorScore += 1.5;
    if (stats?.tier === 'skip') behaviorScore -= 3;
    behaviorScore += Math.max(0, artistRank - 4) * 0.3;
    behaviorScore -= skipCount * 0.6;

    let editorialScore = 0;
    if (suggestedArtistKeys.has(artistKey)) editorialScore += 1.25;
    if (playCount === 0) editorialScore += 0.75;
    if (track.albumName) editorialScore += 0.15;

    const totalScore = genreScore + behaviorScore + editorialScore;
    if (totalScore <= 0.5) continue;

    tracks.push({
      suggestionKey: track.ratingKey || `${track.artistName}::${track.trackTitle}`,
      ratingKey: track.ratingKey,
      artistName: track.artistName,
      trackTitle: track.trackTitle,
      albumName: track.albumName,
      source: playCount === 0 ? 'unplayed-library-fit' : 'rediscovery-fit',
      totalScore: Number(totalScore.toFixed(3)),
      reason: {
        topGenres: topGenresFor(track.genres, limits.genreAffinity),
        playCount,
        skipCount,
        artistRank,
        recencyDays: Number.isFinite(recencyDays) ? Math.round(recencyDays) : null,
        tier: stats?.tier || 'curatorr',
      },
    });
  }

  return tracks
    .sort((a, b) => b.totalScore - a.totalScore || a.artistName.localeCompare(b.artistName) || a.trackTitle.localeCompare(b.trackTitle))
    .slice(0, limits.trackLimit);
}

function buildAlbumSuggestions(catalog, userState, artistSuggestions, trackSuggestions, limits) {
  const suggestedArtistMap = new Map(artistSuggestions.map((artist) => [keyify(artist.artistName), artist]));
  const trackSuggestionMap = new Map(trackSuggestions.map((track) => [track.ratingKey, track]));
  const albums = [];

  for (const album of catalog.albums.values()) {
    if (!album.albumName || !album.artistName) continue;
    const artistKey = keyify(album.artistName);
    const artistStats = userState.artistStats.get(artistKey);
    if (artistStats?.manually_excluded || artistStats?.excluded_from_smart) continue;

    const trackMatches = album.tracks
      .map((track) => trackSuggestionMap.get(track.ratingKey))
      .filter(Boolean)
      .sort((a, b) => b.totalScore - a.totalScore);

    const unplayedCount = album.tracks.filter((track) => {
      const stats = userState.trackStats.get(track.ratingKey);
      return !stats || Number(stats.play_count || 0) === 0;
    }).length;

    const artistSuggestion = suggestedArtistMap.get(artistKey);
    const trackSignal = trackMatches.slice(0, 3).reduce((sum, track) => sum + track.totalScore, 0) / Math.max(1, Math.min(3, trackMatches.length));
    const discoveryScore = (unplayedCount / Math.max(1, album.tracks.length)) * 4;
    const artistScore = Number(artistSuggestion?.totalScore || 0) * 0.45;
    const totalScore = trackSignal + discoveryScore + artistScore;
    if (totalScore <= 0.75) continue;

    albums.push({
      artistName: album.artistName,
      albumTitle: album.albumName,
      albumType: '',
      selectionReason: `Strong fit with ${unplayedCount}/${album.tracks.length} unplayed tracks`,
      rankScore: Number(totalScore.toFixed(3)),
      status: 'candidate',
      reason: {
        topGenres: topGenresFor([...album.genres], limits.genreAffinity),
        unplayedCount,
        trackCount: album.tracks.length,
        sourceTracks: trackMatches.slice(0, 3).map((track) => ({
          ratingKey: track.ratingKey,
          trackTitle: track.trackTitle,
          totalScore: track.totalScore,
        })),
      },
    });
  }

  return albums
    .sort((a, b) => b.rankScore - a.rankScore || a.artistName.localeCompare(b.artistName) || a.albumTitle.localeCompare(b.albumTitle))
    .slice(0, limits.albumLimit);
}

function loadExistingArtistSuggestionState(db, userPlexId) {
  return new Map(db.prepare(`
    SELECT artist_name, status, reason_json, accepted_at, dismissed_at, lidarr_artist_id
    FROM suggested_artists
    WHERE user_plex_id = ?
  `).all(userPlexId).map((row) => [keyify(row.artist_name), row]));
}

export function createRecommendationService(ctx) {
  const { db } = ctx;

  function listCachedSuggestions(userPlexId, options = {}) {
    return {
      artists: listSuggestedArtists(db, userPlexId, { limit: normalizeLimit(options.artistLimit, DEFAULT_LIMITS.artists) }),
      albums: listSuggestedAlbums(db, userPlexId, { limit: normalizeLimit(options.albumLimit, DEFAULT_LIMITS.albums) }),
      tracks: listSuggestedTracks(db, userPlexId, { limit: normalizeLimit(options.trackLimit, DEFAULT_LIMITS.tracks) }),
    };
  }

  function replaceSuggestions(userPlexId, payload = {}) {
    const existingArtistState = loadExistingArtistSuggestionState(db, userPlexId);
    const retentionCutoff = Date.now() - (ARTIST_SUGGESTION_RETENTION_DAYS * 24 * 60 * 60 * 1000);
    const tx = db.transaction((data) => {
      db.prepare('DELETE FROM suggested_albums WHERE user_plex_id = ?').run(userPlexId);
      db.prepare('DELETE FROM suggested_tracks WHERE user_plex_id = ?').run(userPlexId);
      const keepArtistNames = new Set((data.artists || []).map((artist) => keyify(artist.artistName)));
      for (const artist of data.artists || []) {
        const prior = existingArtistState.get(keyify(artist.artistName));
        const preserved = prior && prior.status && prior.status !== 'suggested'
          ? {
              ...artist,
              status: prior.status,
              reason: {
                ...(artist.reason || {}),
                ...parseJson(prior.reason_json || '{}', {}),
              },
              acceptedAt: prior.accepted_at,
              dismissedAt: prior.dismissed_at,
              lidarrArtistId: prior.lidarr_artist_id,
            }
          : artist;
        upsertSuggestedArtist(db, userPlexId, preserved);
      }
      db.prepare(`
        DELETE FROM suggested_artists
        WHERE user_plex_id = ?
          AND status = 'suggested'
          AND last_evaluated_at < ?
      `).run(userPlexId, retentionCutoff);
      const preservedRows = db.prepare(`
        SELECT artist_name, status, last_evaluated_at
        FROM suggested_artists
        WHERE user_plex_id = ?
          AND status = 'suggested'
          AND last_evaluated_at >= ?
      `).all(userPlexId, retentionCutoff);
      for (const row of preservedRows) {
        if (keepArtistNames.has(keyify(row.artist_name))) continue;
        db.prepare(`
          UPDATE suggested_artists
          SET last_evaluated_at = ?
          WHERE user_plex_id = ? AND artist_name = ?
        `).run(Number(row.last_evaluated_at || Date.now()), userPlexId, row.artist_name);
      }
      for (const album of data.albums || []) upsertSuggestedAlbum(db, userPlexId, album);
      for (const track of data.tracks || []) upsertSuggestedTrack(db, userPlexId, track);
    });
    tx(payload);
    return listCachedSuggestions(userPlexId, payload.limits || {});
  }

  function rebuildSuggestionsForUser(userPlexId, options = {}) {
    const profile = getUserTasteProfile(db, userPlexId, options);
    const catalog = buildCatalog(db);
    const userState = loadUserState(db, userPlexId);
    const limits = {
      artistLimit: normalizeLimit(options.artistLimit, DEFAULT_LIMITS.artists),
      albumLimit: normalizeLimit(options.albumLimit, DEFAULT_LIMITS.albums),
      trackLimit: normalizeLimit(options.trackLimit, DEFAULT_LIMITS.tracks),
      genreAffinity: buildGenreAffinity(profile, catalog),
    };

    const artists = buildArtistSuggestions(profile, catalog, userState, limits);
    const tracks = buildTrackSuggestions(profile, catalog, userState, artists, limits);
    const albums = buildAlbumSuggestions(catalog, userState, artists, tracks, limits);
    const cached = replaceSuggestions(userPlexId, { artists, albums, tracks, limits });

    return {
      generatedAt: Date.now(),
      mode: 'phase2b-internal-library-affinity',
      profile,
      counts: {
        artists: cached.artists.length,
        albums: cached.albums.length,
        tracks: cached.tracks.length,
      },
      cached,
    };
  }

  return {
    getUserTasteProfile: (userPlexId, options = {}) => getUserTasteProfile(db, userPlexId, options),
    listCachedSuggestions,
    rebuildSuggestionsForUser,
  };
}
