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
const LASTFM_SIMILARITY_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const LASTFM_TAG_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const LASTFM_SIMILAR_SEED_LIMIT = 5;
const LASTFM_SIMILAR_PER_SEED_LIMIT = 20;
const LASTFM_TAG_ENRICH_LIMIT = 30;
export const ARTIST_RECOMMENDATION_MODEL_VERSION = 'phase2h-lastfm-tokenized-tags';
const THRESHOLD_TO_MIN_SCORE = { 1: 0.1, 2: 0.3, 3: 0.6, 4: 1.0, 5: 1.5 };

const lastfmSimilarityCache = new Map();
const lastfmTagCache = new Map();

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

function expandGenreSignals(input) {
  const values = Array.isArray(input) ? input : [input];
  const expanded = new Set();
  const push = (value) => {
    const normalized = normalizeText(value);
    if (!normalized || /^\d+$/.test(normalized)) return;
    expanded.add(normalized);
    if (/^r&b$/i.test(normalized)) expanded.add('rnb');
  };

  for (const value of values) {
    const normalized = normalizeText(value);
    if (!normalized) continue;
    for (const semicolonPart of normalized.split(';').map((s) => s.trim()).filter(Boolean)) {
      push(semicolonPart);
      for (const punctPart of semicolonPart.split(/[\/,|·]+/).map((s) => s.trim()).filter(Boolean)) {
        push(punctPart);
        if (/\s&\s/.test(punctPart)) {
          for (const ampPart of punctPart.split(/\s*&\s*/).map((s) => s.trim()).filter(Boolean)) push(ampPart);
        }
      }
    }
  }

  return [...expanded];
}

function parseJson(value, fallback) {
  try { return JSON.parse(value); } catch (err) { return fallback; }
}

function daysSince(timestamp) {
  const n = Number(timestamp);
  if (!Number.isFinite(n) || n <= 0) return Number.POSITIVE_INFINITY;
  return (Date.now() - n) / (24 * 60 * 60 * 1000);
}

function getTimedCache(cache, key) {
  const entry = cache.get(key);
  if (!entry) return null;
  if (Number(entry.expiresAt || 0) <= Date.now()) {
    cache.delete(key);
    return null;
  }
  return entry.value;
}

function setTimedCache(cache, key, value, ttlMs) {
  cache.set(key, {
    value,
    expiresAt: Date.now() + ttlMs,
  });
}

function scoreGenreSet(genres, affinity) {
  const weights = expandGenreSignals(genres)
    .map((genre) => Number(affinity.get(keyify(genre)) || 0))
    .filter((value) => value > 0)
    .sort((a, b) => b - a);
  return weights.slice(0, 3).reduce((sum, value) => sum + value, 0);
}

function compressArtistGenreScore(rawScore) {
  const score = Math.max(0, Number(rawScore) || 0);
  if (score <= 0) return 0;
  // Genre affinity accumulates across many signals and can get very large for broad
  // genres like Pop/Rock. Compress it for artist suggestions so library-affinity
  // scores stay on a comparable scale with the Last.fm similarity pool.
  return Math.sqrt(score);
}

function deriveLastfmCandidateGenres(lastfmSimilarity, catalog) {
  const derived = [];
  for (const seedArtistName of Array.isArray(lastfmSimilarity?.basedOn) ? lastfmSimilarity.basedOn : []) {
    const seedArtist = catalog.artists.get(keyify(seedArtistName));
    if (!seedArtist) continue;
    derived.push(...seedArtist.genres);
  }
  for (const tag of Array.isArray(lastfmSimilarity?.topTags) ? lastfmSimilarity.topTags : []) {
    derived.push(tag);
  }
  return derived;
}

async function fetchLastfmArtistTopTags(apiKey, artistName) {
  const normalizedArtistName = normalizeText(artistName);
  if (!normalizedArtistName) return [];
  const cacheKey = keyify(normalizedArtistName);
  const cached = getTimedCache(lastfmTagCache, cacheKey);
  if (cached) return cached;

  try {
    const url = new URL('https://ws.audioscrobbler.com/2.0/');
    url.searchParams.set('method', 'artist.getTopTags');
    url.searchParams.set('artist', normalizedArtistName);
    url.searchParams.set('api_key', apiKey);
    url.searchParams.set('format', 'json');
    const response = await fetch(url.toString(), {
      headers: { Accept: 'application/json' },
      signal: AbortSignal.timeout(15000),
    });
    if (!response.ok) return [];
    const data = await response.json();
    const tags = (Array.isArray(data?.toptags?.tag) ? data.toptags.tag : [])
      .map((tag) => ({
        name: normalizeText(tag?.name),
        count: Number(tag?.count || 0),
      }))
      .filter((tag) => tag.name && tag.count > 0)
      .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name))
      .slice(0, 8)
      .map((tag) => tag.name);
    setTimedCache(lastfmTagCache, cacheKey, tags, LASTFM_TAG_CACHE_TTL_MS);
    return tags;
  } catch (_err) {
    return [];
  }
}

function computeArtistBehaviorScore(stats) {
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

  return {
    playCount,
    rankingScore,
    skipCount,
    recencyDays,
    behaviorScore,
  };
}

function computeArtistEditorialScore({
  artistKey,
  topArtistKeys,
  albumCount,
  trackCount,
  candidateGenres,
  likedGenres,
  lastfmSimilarityScore,
  lastfmSeedCount,
  inCatalog,
}) {
  let editorialScore = 0;
  if (topArtistKeys.has(artistKey)) editorialScore -= 3;
  if (albumCount >= 2) editorialScore += 0.75;
  if (trackCount >= 8) editorialScore += 0.5;
  if ((likedGenres || []).some((genre) => candidateGenres.includes(genre))) editorialScore += 1;
  if (lastfmSimilarityScore > 0) editorialScore += Math.min(6, lastfmSimilarityScore * 6);
  if (lastfmSeedCount > 0) editorialScore += Math.min(1.5, lastfmSeedCount * 0.5);
  if (!inCatalog) editorialScore += 0.75;
  return editorialScore;
}

function topGenresFor(genres, affinity, limit = 3) {
  return [...new Set((Array.isArray(genres) ? genres : []).map((genre) => normalizeText(genre)).filter(Boolean))]
    .map((genre) => ({
      genre,
      score: expandGenreSignals(genre)
        .map((signal) => Number(affinity.get(keyify(signal)) || 0))
        .reduce((max, value) => Math.max(max, value), 0),
    }))
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
    for (const genre of track.genres) {
      for (const g of String(genre).split(';').map((s) => s.trim()).filter((s) => s && !/^\d+$/.test(s))) artist.genres.add(g);
    }
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
    for (const genre of track.genres) {
      for (const g of String(genre).split(';').map((s) => s.trim()).filter(Boolean)) album.genres.add(g);
    }
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
    // Split semicolon-joined genres (Jellyfin format) into individual genres
    for (const g of String(genre).split(';').map((s) => s.trim()).filter((s) => s && !/^\d+$/.test(s))) {
      const key = keyify(g);
      if (!key) continue;
      affinity.set(key, Number(affinity.get(key) || 0) + weight);
      // Also add individual slash/comma-delimited tokens (e.g. 'Pop/Rock' → 'pop', 'rock')
      // so Last.fm tags like 'rock' or 'pop' can match compound catalog genres.
      for (const sub of g.split(/[\/,|·]+/).map((s) => s.trim()).filter((s) => s && !/^\d+$/.test(s))) {
        const subKey = keyify(sub);
        if (!subKey || subKey === key) continue;
        affinity.set(subKey, Number(affinity.get(subKey) || 0) + weight * 0.75);
      }
    }
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
  const lastfmSimilar = limits.lastfmSimilarArtists || new Map();
  const librarySuggestions = [];
  const lastfmSuggestions = [];

  const catalogArtistKeys = new Set(catalog.artists.keys());
  for (const artist of catalog.artists.values()) {
    const artistKey = keyify(artist.artistName);
    const stats = userState.artistStats.get(artistKey);
    if (!artist.artistName || artistKeyInSet(artistKey, ignoredArtistKeys)) continue;
    if (stats?.manually_excluded || stats?.excluded_from_smart) continue;
    if (likedArtistKeys.has(artistKey)) continue;
    if (stats && Number(stats.play_count || 0) >= 12 && Number(stats.ranking_score || 0) >= 7) continue;
    if (isCollaborationCredit(artist.artistName, catalogArtistKeys)) continue;

    const candidateGenres = [...artist.genres];
    const genreScore = compressArtistGenreScore(scoreGenreSet(candidateGenres, limits.genreAffinity));
    const {
      playCount,
      rankingScore,
      recencyDays,
      behaviorScore,
    } = computeArtistBehaviorScore(stats);
    const lastfmSimilarity = lastfmSimilar.get(artistKey) || null;
    const lastfmSimilarityScore = Number(lastfmSimilarity?.score || 0);
    const editorialScore = computeArtistEditorialScore({
      artistKey,
      topArtistKeys,
      albumCount: artist.albumTitles.size,
      trackCount: artist.tracks.length,
      candidateGenres,
      likedGenres: profile.likedGenres,
      lastfmSimilarityScore,
      lastfmSeedCount: Array.isArray(lastfmSimilarity?.basedOn) ? lastfmSimilarity.basedOn.length : 0,
      inCatalog: true,
    });

    const totalScore = genreScore + behaviorScore + editorialScore;
    if (totalScore <= 0.5) continue;

    librarySuggestions.push({
      artistName: artist.artistName,
      source: 'library-affinity',
      similarityScore: Number(genreScore.toFixed(3)),
      behaviorScore: Number(behaviorScore.toFixed(3)),
      editorialScore: Number(editorialScore.toFixed(3)),
      totalScore: Number(totalScore.toFixed(3)),
      status: 'suggested',
      reason: {
        modelVersion: ARTIST_RECOMMENDATION_MODEL_VERSION,
        topGenres: topGenresFor(candidateGenres, limits.genreAffinity),
        lastfmTags: Array.isArray(lastfmSimilarity?.topTags) ? lastfmSimilarity.topTags.slice(0, 5) : [],
        playCount,
        rankingScore,
        lastfmSimilarityScore: Number(lastfmSimilarityScore.toFixed(3)),
        similarTo: Array.isArray(lastfmSimilarity?.basedOn) ? lastfmSimilarity.basedOn.slice(0, 3) : [],
        recencyDays: Number.isFinite(recencyDays) ? Math.round(recencyDays) : null,
        albumCount: artist.albumTitles.size,
        trackCount: artist.tracks.length,
      },
    });
  }

  const minSimilarityScore = Number(limits.minSimilarityScore ?? 0);
  for (const [artistKey, lastfmSimilarity] of lastfmSimilar.entries()) {
    if (!lastfmSimilarity?.artistName || catalog.artists.has(artistKey)) continue;
    if (artistKeyInSet(artistKey, ignoredArtistKeys)) continue;
    if (likedArtistKeys.has(artistKey)) continue;
    if (topArtistKeys.has(artistKey)) continue;
    if (isCollaborationCredit(lastfmSimilarity.artistName, catalogArtistKeys)) continue;

    const stats = userState.artistStats.get(artistKey);
    if (stats?.manually_excluded || stats?.excluded_from_smart) continue;

    const lastfmSimilarityScore = Number(lastfmSimilarity.score || 0);
    if (minSimilarityScore > 0 && lastfmSimilarityScore < minSimilarityScore) continue;
    const candidateGenres = deriveLastfmCandidateGenres(lastfmSimilarity, catalog);
    const genreScore = compressArtistGenreScore(scoreGenreSet(candidateGenres, limits.genreAffinity));
    const {
      playCount,
      rankingScore,
      recencyDays,
      behaviorScore,
    } = computeArtistBehaviorScore(stats);
    const editorialScore = computeArtistEditorialScore({
      artistKey,
      topArtistKeys,
      albumCount: 0,
      trackCount: 0,
      candidateGenres,
      likedGenres: profile.likedGenres,
      lastfmSimilarityScore,
      lastfmSeedCount: Array.isArray(lastfmSimilarity.basedOn) ? lastfmSimilarity.basedOn.length : 0,
      inCatalog: false,
    });
    const totalScore = genreScore + behaviorScore + editorialScore;
    if (totalScore <= 0.5) continue;

    lastfmSuggestions.push({
      artistName: lastfmSimilarity.artistName,
      source: 'lastfm-similar',
      similarityScore: Number(genreScore.toFixed(3)),
      behaviorScore: Number(behaviorScore.toFixed(3)),
      editorialScore: Number(editorialScore.toFixed(3)),
      totalScore: Number(totalScore.toFixed(3)),
      status: 'suggested',
      reason: {
        modelVersion: ARTIST_RECOMMENDATION_MODEL_VERSION,
        topGenres: topGenresFor(candidateGenres, limits.genreAffinity),
        playCount,
        rankingScore,
        lastfmSimilarityScore: Number(lastfmSimilarityScore.toFixed(3)),
        similarTo: Array.isArray(lastfmSimilarity.basedOn) ? lastfmSimilarity.basedOn.slice(0, 3) : [],
        recencyDays: Number.isFinite(recencyDays) ? Math.round(recencyDays) : null,
        albumCount: 0,
        trackCount: 0,
      },
    });
  }

  librarySuggestions.sort((a, b) => b.totalScore - a.totalScore || a.artistName.localeCompare(b.artistName));
  lastfmSuggestions.sort((a, b) => b.totalScore - a.totalScore || a.artistName.localeCompare(b.artistName));

  const artistLimit = Math.max(1, Number(limits.artistLimit || 0) || DEFAULT_LIMITS.artists);
  const reservedLastfmSlots = lastfmSuggestions.length
    ? Math.min(lastfmSuggestions.length, Math.max(2, Math.ceil(artistLimit * 0.4)))
    : 0;
  const primaryLibrarySlots = Math.max(0, artistLimit - reservedLastfmSlots);

  const selected = [
    ...librarySuggestions.slice(0, primaryLibrarySlots),
    ...lastfmSuggestions.slice(0, reservedLastfmSlots),
  ];

  if (selected.length < artistLimit) {
    const selectedKeys = new Set(selected.map((artist) => keyify(artist.artistName)));
    const fillPool = [...librarySuggestions.slice(primaryLibrarySlots), ...lastfmSuggestions.slice(reservedLastfmSlots)]
      .filter((artist) => !selectedKeys.has(keyify(artist.artistName)))
      .sort((a, b) => b.totalScore - a.totalScore || a.artistName.localeCompare(b.artistName));
    for (const artist of fillPool) {
      if (selected.length >= artistLimit) break;
      selected.push(artist);
    }
  }

  return {
    selected: selected
      .sort((a, b) => b.totalScore - a.totalScore || a.artistName.localeCompare(b.artistName))
      .slice(0, artistLimit),
    allScored: [...librarySuggestions, ...lastfmSuggestions],
  };
}

function artistKeyInSet(artistKey, keySet) {
  if (keySet.has(artistKey)) return true;
  const parts = artistKey.split(/\s*[&\/,]\s*|\s+and\s+/).map((s) => s.trim()).filter(Boolean);
  return parts.length > 1 && parts.some((p) => keySet.has(p));
}

// Returns true for MusicBrainz-style collaboration credits that should not be
// treated as standalone discovery candidates.
//
// Strategy: split the name on commas and ampersands to get the credited
// individuals, then apply two tests:
//
//   A. ANY component part is a key in catalogArtistKeys — the user already has
//      that artist in their library, so the joint credit is a collaboration of
//      a known artist, not a standalone act to discover. This catches Last.fm
//      suggestions like "Doja Cat & The Weeknd" when the user has either artist,
//      "Labrinth & Zendaya", "Doechii & SZA", "Coldplay & Rihanna", etc.
//      Single-word band-name components ("Earth", "Wind", "Fire") won't appear
//      as standalone catalog keys, so real named acts are left alone.
//
//   B. The joint name itself is in the library (library-affinity path where only
//      the collaboration tracks are present, e.g. "Leon Thomas & Benny the
//      Butcher") AND at least one part is multi-word (person-name indicator) AND
//      no part opens with a definite/indefinite article — which would signal a
//      named band component like "The Asbury Jukes".
function isCollaborationCredit(artistName, catalogArtistKeys) {
  const name = String(artistName || '').trim();
  // feat./featuring/ft. anywhere → always a joint credit, never a standalone act
  if (/\b(feat|featuring|ft)\.?\s/i.test(name)) return true;
  if (!catalogArtistKeys) return false;
  const parts = name.split(/\s*[,&]\s*/).map((s) => s.trim()).filter(Boolean);
  if (parts.length < 2) return false;
  const partKeys = parts.map((p) => p.toLowerCase());
  // Test A — any component is a known library artist
  if (partKeys.some((p) => catalogArtistKeys.has(p))) return true;
  // Test B — only the joint name is in the library (no individual solo entries)
  if (catalogArtistKeys.has(name.toLowerCase())) {
    const noArticleStart = partKeys.every((p) => !/^(the|a|an)\s/i.test(p));
    if (noArticleStart && partKeys.some((p) => p.split(/\s+/).length >= 2)) return true;
  }
  return false;
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

async function fetchLastfmSimilarArtists(ctx, profile) {
  const loadConfig = typeof ctx?.loadConfig === 'function' ? ctx.loadConfig : null;
  const config = loadConfig ? (loadConfig() || {}) : {};
  const apiKey = String(config?.discovery?.lastfmApiKey || '').trim();
  if (!apiKey) return new Map();

  const seeds = (Array.isArray(profile?.topArtists) ? profile.topArtists : [])
    .map((artist) => ({
      artistName: normalizeText(artist?.artistName),
      rankingScore: normalizeScore(artist?.rankingScore || 0),
      playCount: Number(artist?.playCount || 0),
    }))
    .filter((artist) => artist.artistName)
    .slice(0, LASTFM_SIMILAR_SEED_LIMIT);
  if (!seeds.length) return new Map();

  const cacheKey = JSON.stringify(seeds.map((artist) => [artist.artistName, artist.rankingScore, artist.playCount]));
  const cached = getTimedCache(lastfmSimilarityCache, cacheKey);
  if (cached) return new Map(cached);

  try {
    const results = await Promise.all(seeds.map(async (seed) => {
      const url = new URL('https://ws.audioscrobbler.com/2.0/');
      url.searchParams.set('method', 'artist.getSimilar');
      url.searchParams.set('artist', seed.artistName);
      url.searchParams.set('limit', String(LASTFM_SIMILAR_PER_SEED_LIMIT));
      url.searchParams.set('api_key', apiKey);
      url.searchParams.set('format', 'json');
      const response = await fetch(url.toString(), {
        headers: { Accept: 'application/json' },
        signal: AbortSignal.timeout(15000),
      });
      if (!response.ok) return null;
      const data = await response.json();
      return { seed, items: data?.similarartists?.artist || [] };
    }));

    const affinity = new Map();
    for (const result of results) {
      if (!result) continue;
      const seedWeight = Math.max(1, Math.min(4, 1 + (result.seed.rankingScore / 8) + (result.seed.playCount / 20)));
      for (const item of result.items) {
        const artistKey = keyify(item?.name);
        if (!artistKey || artistKey === keyify(result.seed.artistName)) continue;
        const match = normalizeScore(item?.match || 0);
        if (match <= 0) continue;
        const existing = affinity.get(artistKey) || {
          artistName: normalizeText(item?.name),
          score: 0,
          basedOn: [],
        };
        existing.score += (match * seedWeight);
        if (!existing.basedOn.includes(result.seed.artistName)) existing.basedOn.push(result.seed.artistName);
        affinity.set(artistKey, existing);
      }
    }

    const enrichable = [...affinity.entries()]
      .sort((a, b) => Number(b[1]?.score || 0) - Number(a[1]?.score || 0))
      .slice(0, LASTFM_TAG_ENRICH_LIMIT);
    const tagLists = await Promise.all(enrichable.map(async ([artistKey, entry]) => ({
      artistKey,
      topTags: await fetchLastfmArtistTopTags(apiKey, entry.artistName),
    })));
    for (const tagEntry of tagLists) {
      const existing = affinity.get(tagEntry.artistKey);
      if (!existing) continue;
      existing.topTags = tagEntry.topTags;
      affinity.set(tagEntry.artistKey, existing);
    }

    setTimedCache(lastfmSimilarityCache, cacheKey, [...affinity.entries()], LASTFM_SIMILARITY_CACHE_TTL_MS);
    return affinity;
  } catch (err) {
    ctx?.pushLog?.({
      level: 'warn',
      app: 'recommendations',
      action: 'lastfm.similar',
      message: `Last.fm similar artist boost skipped: ${err?.message || 'request failed'}`,
    });
    return new Map();
  }
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
      const selectedArtists = Array.isArray(data.artists) ? data.artists : [];
      const allScoredArtists = Array.isArray(data.allScoredArtists) ? data.allScoredArtists : selectedArtists;
      db.prepare('DELETE FROM suggested_albums WHERE user_plex_id = ?').run(userPlexId);
      db.prepare('DELETE FROM suggested_tracks WHERE user_plex_id = ?').run(userPlexId);
      const keepArtistNames = new Set(selectedArtists.map((artist) => keyify(artist.artistName)));
      const selectedArtistKeys = new Set(keepArtistNames);
      for (const artist of selectedArtists) {
        const prior = existingArtistState.get(keyify(artist.artistName));
        const preserved = prior && prior.status && prior.status !== 'suggested'
          ? {
              ...artist,
              status: prior.status,
              reason: {
                ...parseJson(prior.reason_json || '{}', {}),
                ...(artist.reason || {}),
              },
              acceptedAt: prior.accepted_at,
              dismissedAt: prior.dismissed_at,
              lidarrArtistId: prior.lidarr_artist_id,
            }
          : artist;
        upsertSuggestedArtist(db, userPlexId, preserved);
      }
      for (const artist of allScoredArtists) {
        const artistKey = keyify(artist.artistName);
        if (!artistKey || selectedArtistKeys.has(artistKey)) continue;
        const prior = existingArtistState.get(artistKey);
        if (!prior || !prior.status || prior.status === 'dismissed') continue;
        upsertSuggestedArtist(db, userPlexId, {
          ...artist,
          status: prior.status,
          reason: {
            ...parseJson(prior.reason_json || '{}', {}),
            ...(artist.reason || {}),
          },
          acceptedAt: prior.accepted_at,
          dismissedAt: prior.dismissed_at,
          lidarrArtistId: prior.lidarr_artist_id,
        });
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

  async function rebuildSuggestionsForUser(userPlexId, options = {}) {
    const profile = getUserTasteProfile(db, userPlexId, options);
    const catalog = buildCatalog(db);
    const userState = loadUserState(db, userPlexId);
    const lastfmSimilarArtists = await fetchLastfmSimilarArtists(ctx, profile);
    const loadConfig = typeof ctx?.loadConfig === 'function' ? ctx.loadConfig : null;
    const config = loadConfig ? (loadConfig() || {}) : {};
    const thresholdRaw = Math.max(1, Math.min(5, Number(config?.discovery?.similarArtistThreshold) || 3));
    const minSimilarityScore = THRESHOLD_TO_MIN_SCORE[thresholdRaw] ?? 0.6;
    const limits = {
      artistLimit: normalizeLimit(options.artistLimit, DEFAULT_LIMITS.artists),
      albumLimit: normalizeLimit(options.albumLimit, DEFAULT_LIMITS.albums),
      trackLimit: normalizeLimit(options.trackLimit, DEFAULT_LIMITS.tracks),
      genreAffinity: buildGenreAffinity(profile, catalog),
      lastfmSimilarArtists,
      minSimilarityScore,
    };

    const artistPool = buildArtistSuggestions(profile, catalog, userState, limits);
    const tracks = buildTrackSuggestions(profile, catalog, userState, artistPool.selected, limits);
    const albums = buildAlbumSuggestions(catalog, userState, artistPool.selected, tracks, limits);
    const cached = replaceSuggestions(userPlexId, {
      artists: artistPool.selected,
      allScoredArtists: artistPool.allScored,
      albums,
      tracks,
      limits,
    });

    // Purge any collaboration credits that survived from previous rebuilds.
    // replaceSuggestions retains existing 'suggested' rows within the retention
    // window, so entries written before the filter existed must be cleaned up
    // explicitly here.
    const catalogArtistKeys = new Set(catalog.artists.keys());
    const staleSuggested = db.prepare(`
      SELECT artist_name FROM suggested_artists
      WHERE user_plex_id = ? AND status = 'suggested'
    `).all(userPlexId);
    const collaborationNames = staleSuggested
      .map((r) => r.artist_name)
      .filter((name) => isCollaborationCredit(name, catalogArtistKeys));
    if (collaborationNames.length > 0) {
      const del = db.prepare(`
        DELETE FROM suggested_artists
        WHERE user_plex_id = ? AND artist_name = ? AND status = 'suggested'
      `);
      for (const name of collaborationNames) del.run(userPlexId, name);
    }

    return {
      generatedAt: Date.now(),
      mode: ARTIST_RECOMMENDATION_MODEL_VERSION,
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
