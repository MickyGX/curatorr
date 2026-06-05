// Music stats, smart playlist management, and Lidarr integration

import path from 'path';
import Database from 'better-sqlite3';
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
  upsertSuggestedArtist,
  setSuggestedArtistStatus,
  getUserPlaylist,
  getUserPreferences,
  listUserGeneratedPlaylists,
  saveUserGeneratedPlaylist,
  saveUserPreferences,
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
  setPlaylistTracks,
  getPlaylistTracks,
  listImportedPlaylistUnmatched,
  setImportedPlaylistUnmatched,
  setImportedPlaylistUnmatchedSelection,
  listAllGeneratedPlaylists,
  clearGeneratedPlaylistPlexId,
  listSuggestedAlbums,
  listUserPersonalPlaylists,
  getUserPersonalPlaylist,
  findUserPersonalPlaylistByName,
  createUserPersonalPlaylist,
  updateUserPersonalPlaylist,
  deleteUserPersonalPlaylist,
  deleteUserGeneratedPlaylist,
  setCustomPlaylistAudience,
  setAllCopiesPlaylistAudience,
  previewGlobalPlaylist,
  getAlbumPopularTrackRanks,
  getArtistTagMap,
  getEffectiveTrackTags,
  getTrackDecadeTag,
  listRuleTemplates,
  saveRuleTemplate,
  updateRuleTemplate,
  deleteRuleTemplate,
  classifyTier,
} from '../db.js';
import { paginateRolledHistory } from '../history-rollup.js';
import { promoteCompletedRequestsFromLidarr, resolveLibraryAlbumMatch } from '../services/album-reconciliation.js';
import { applyFeaturePresetFilters, applyTrackFiltersWithReport } from '../services/playlists.js';
import {
  getStoredPlaylistArtworkInfo,
  parsePlaylistArtworkDataUrl,
  savePlaylistArtworkBuffer,
} from '../services/playlist-artwork.js';

const DAY_MS = 24 * 60 * 60 * 1000;
const DISCOVERY_ART_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const DISCOVERY_ART_CACHE_MAX = 300;
const DISCOVERY_ART_URL_VERSION = 'discover-art-v6';
const discoveryArtCache = new Map();
const THUMB_CACHE_TTL_MS = 30 * 60 * 1000;
const THUMB_NEGATIVE_CACHE_TTL_MS = 5 * 60 * 1000;
const THUMB_CACHE_MAX = 600;
const thumbCache = new Map();
const PLAYLIST_FEATURE_PRESETS = ['none', 'club', 'driving', 'workout', 'chill', 'harmonic', 'wakeup', 'downtempo'];
const CAMELOT_MODES = ['exact', 'adjacent', 'relative', 'harmonic'];
const PLAYLIST_SORT_VALUES = ['default', 'source', 'ratingCount', 'tierWeight', 'playCount', 'random', 'libraryAddedDesc', 'releaseDateDesc', 'bpmAsc', 'bpmDesc', 'energyAsc', 'energyDesc', 'danceabilityDesc', 'camelot', 'djFlow'];
const PLAYLIST_FINAL_ORDERING_VALUES = ['none', 'plexSonic', 'loudness', 'plexSonicLoudness'];
const PLAYLIST_ALBUM_POPULARITY_VALUES = ['all', 'top3Only', 'excludeTop3'];
const PLAYLIST_POPULARITY_VALUES = ['all', 'top50', 'top25', 'top10', 'top5', 'custom'];
const PLAYLIST_LAST_PLAYED_MODES = ['any', 'within', 'notWithin', 'never'];
const LASTFM_STATION_LABELS = {
  recommended: 'Recommended',
  mix: 'Mix',
  library: 'Library',
  neighbours: 'Neighbours',
  loved: 'Loved',
};
const TOP_TRACKS_PERIOD_LABELS = {
  overall: 'All Time',
  '7day': 'Last 7 Days',
  '1month': 'Last Month',
  '3month': 'Last 3 Months',
  '6month': 'Last 6 Months',
  '12month': 'Last Year',
};
const LISTENBRAINZ_PLAYLIST_LABELS = {
  'daily-jams': 'Daily Jams',
  'weekly-jams': 'Weekly Jams',
  'weekly-exploration': 'Weekly Exploration',
};
const IMPORTED_SYNC_PERIOD_LABELS = {
  disabled: 'Disabled',
  daily: 'Daily',
  weekly: 'Weekly',
  monthly: 'Monthly',
};
const IMPORTED_SYNC_PERIOD_MS = {
  disabled: 0,
  daily: DAY_MS,
  weekly: 7 * DAY_MS,
  monthly: 30 * DAY_MS,
};
const LASTFM_IMPORT_SOURCE_KEYS = ['recommended', 'mix', 'library', 'neighbours', 'loved'];
const LASTFM_TOP_TRACKS_PERIODS = ['overall', '7day', '1month', '3month', '6month', '12month'];
const LISTENBRAINZ_IMPORT_SOURCE_KEYS = ['daily-jams', 'weekly-jams', 'weekly-exploration'];
// Cache Jellyfin/Emby userId lookups — keyed by "username@serverUrl", TTL 1 hour
const msUserIdCache = new Map();
const MS_USERID_CACHE_TTL_MS = 60 * 60 * 1000;

function parseNullablePlaylistNumber(value) {
  if (value === '' || value == null) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function parseNullablePlaylistDayCount(value) {
  const num = parseNullablePlaylistNumber(value);
  if (!Number.isFinite(num)) return null;
  return Math.max(1, Math.round(num));
}

function formatOverviewReleaseDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const isoMatch = raw.match(/^(\d{4}-\d{2}-\d{2})T/);
  if (isoMatch) return isoMatch[1];
  return raw;
}

function formatOverviewDuration(ms) {
  const totalMs = Number(ms || 0);
  if (!Number.isFinite(totalMs) || totalMs <= 0) return '';
  const totalSeconds = Math.floor(totalMs / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) return `${hours}h ${String(minutes).padStart(2, '0')}m`;
  return `${minutes}m ${String(seconds).padStart(2, '0')}s`;
}

function formatOverviewNumber(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return '';
  return num.toLocaleString();
}

function formatOverviewMetric(value, digits = 2) {
  const num = Number(value);
  if (!Number.isFinite(num)) return '';
  return String(Number(num.toFixed(digits)));
}

function attachAlbumPopularity(items = [], popularityByKey = new Map(), keyField = 'ratingKey') {
  return (Array.isArray(items) ? items : []).map((item) => {
    const popularity = popularityByKey.get(String(item?.[keyField] || '')) || null;
    return popularity
      ? { ...item, popularRank: popularity.rank, ratingCount: popularity.ratingCount }
      : { ...item, popularRank: null, ratingCount: Number(item?.ratingCount || item?.rating_count || 0) };
  });
}

// Accepts a flat string[] (legacy) or { include: string[], exclude: string[], includeMode?: 'any'|'all' } from the tri-state UI.
function normaliseTriStateInput(value) {
  if (!value) return { include: [], exclude: [], includeMode: 'any' };
  if (Array.isArray(value)) return { include: value.filter(Boolean), exclude: [], includeMode: 'any' };
  return {
    include: Array.isArray(value.include) ? value.include.filter(Boolean) : [],
    exclude: Array.isArray(value.exclude) ? value.exclude.filter(Boolean) : [],
    includeMode: value.includeMode === 'all' ? 'all' : 'any',
  };
}

function normaliseTrackFiltersInput(value) {
  if (!value || typeof value !== 'object') return null;
  const includeFolders = Array.isArray(value.includeFolders) ? value.includeFolders.map(String).filter(Boolean) : [];
  const excludeFolders = Array.isArray(value.excludeFolders) ? value.excludeFolders.map(String).filter(Boolean) : [];
  const deduplicateByMbid = Boolean(value.deduplicateByMbid);
  const deduplicateByArtistTitle = Boolean(value.deduplicateByArtistTitle);
  const deduplicateByDuration = Boolean(value.deduplicateByDuration);
  const deduplicateIgnoreLikelyVariants = Boolean(value.deduplicateIgnoreLikelyVariants);
  const deduplicateIgnoreLiveAlbums = Boolean(value.deduplicateIgnoreLiveAlbums);
  const rules = (Array.isArray(value.rules) ? value.rules : [])
    .filter((r) => r && r.field && r.operator && r.value != null && r.value !== '')
    .map((r) => ({ field: String(r.field), operator: String(r.operator), value: String(r.value), caseSensitive: Boolean(r.caseSensitive) }));
  if (!includeFolders.length && !excludeFolders.length && !deduplicateByMbid && !deduplicateByArtistTitle && !deduplicateByDuration && !deduplicateIgnoreLikelyVariants && !deduplicateIgnoreLiveAlbums && !rules.length) return null;
  return {
    includeFolders,
    excludeFolders,
    deduplicateByMbid,
    deduplicateByArtistTitle,
    deduplicateByDuration,
    deduplicateIgnoreLikelyVariants,
    deduplicateIgnoreLiveAlbums,
    rules,
  };
}

function buildPlaylistFeatureRules(payload = {}) {
  const rawPreset = String(payload.featurePreset || '').trim().toLowerCase();
  const libraryAddedMode = ['any', 'within', 'notWithin'].includes(String(payload.libraryAddedMode || '').trim())
    ? String(payload.libraryAddedMode).trim()
    : 'any';
  const releaseDateMode = ['any', 'within', 'notWithin'].includes(String(payload.releaseDateMode || '').trim())
    ? String(payload.releaseDateMode).trim()
    : 'any';
  return {
    albumPopularityMode: PLAYLIST_ALBUM_POPULARITY_VALUES.includes(String(payload.albumPopularityMode || '').trim())
      ? String(payload.albumPopularityMode).trim()
      : 'all',
    popularityMode: PLAYLIST_POPULARITY_VALUES.includes(String(payload.popularityMode || '').trim())
      ? String(payload.popularityMode).trim()
      : 'all',
    popularityPercent: parseNullablePlaylistNumber(payload.popularityPercent),
    featurePreset: PLAYLIST_FEATURE_PRESETS.includes(rawPreset) ? rawPreset : 'none',
    bpmMin: parseNullablePlaylistNumber(payload.bpmMin),
    bpmMax: parseNullablePlaylistNumber(payload.bpmMax),
    energyMin: parseNullablePlaylistNumber(payload.energyMin),
    energyMax: parseNullablePlaylistNumber(payload.energyMax),
    danceabilityMin: parseNullablePlaylistNumber(payload.danceabilityMin),
    danceabilityMax: parseNullablePlaylistNumber(payload.danceabilityMax),
    camelotFocus: String(payload.camelotFocus || '').trim().toUpperCase(),
    camelotMode: CAMELOT_MODES.includes(String(payload.camelotMode || '').trim()) ? String(payload.camelotMode).trim() : 'exact',
    seasonalGenres: Array.isArray(payload.seasonalGenres) ? payload.seasonalGenres.map(String).filter(Boolean) : [],
    seasonalKeywords: Array.isArray(payload.seasonalKeywords) ? payload.seasonalKeywords.map(String).filter(Boolean) : [],
    seasonalExcludeGenres: Array.isArray(payload.seasonalExcludeGenres) ? payload.seasonalExcludeGenres.map(String).filter(Boolean) : [],
    seasonalExcludeKeywords: Array.isArray(payload.seasonalExcludeKeywords) ? payload.seasonalExcludeKeywords.map(String).filter(Boolean) : [],
    seasonalGenresMode: String(payload.seasonalGenresMode || '').trim() === 'all' ? 'all' : 'any',
    seasonalKeywordsMode: String(payload.seasonalKeywordsMode || '').trim() === 'all' ? 'all' : 'any',
    lastPlayedMode: PLAYLIST_LAST_PLAYED_MODES.includes(String(payload.lastPlayedMode || '').trim())
      ? String(payload.lastPlayedMode).trim()
      : 'any',
    lastPlayedDays: parseNullablePlaylistDayCount(payload.lastPlayedDays),
    libraryAddedMode,
    libraryAddedDays: parseNullablePlaylistDayCount(payload.libraryAddedDays),
    releaseYearMin: parseNullablePlaylistNumber(payload.releaseYearMin),
    releaseYearMax: parseNullablePlaylistNumber(payload.releaseYearMax),
    releaseDateAfter: String(payload.releaseDateAfter || '').trim(),
    releaseDateBefore: String(payload.releaseDateBefore || '').trim(),
    releaseDateMode,
    releaseDateDays: parseNullablePlaylistDayCount(payload.releaseDateDays),
  };
}

function buildPlaylistPreviewSnapshot(db, userPlexId, rules, trackFilters, smartSettings, options = {}) {
  const duplicateLimit = Math.max(0, Math.min(10000, Number(options.dedupeReportLimit || 0) || 0));
  const filterResult = trackFilters
    ? applyTrackFiltersWithReport(getMasterTracks(db), trackFilters, { duplicateLimit })
    : { tracks: getMasterTracks(db), duplicateCount: 0, duplicateMatches: [] };
  const baseTracks = filterResult.tracks;
  const filteredTracks = applyFeaturePresetFilters(baseTracks, rules || {});
  const result = previewGlobalPlaylist(db, rules || {}, userPlexId, smartSettings || {}, filteredTracks);
  const counts = result.forUser || result.average || {
    artistCount: 0,
    trackCount: 0,
    eligibleArtistCount: 0,
    eligibleTrackCount: 0,
  };
  return {
    counts,
    featureTrackCount: filteredTracks.length,
    dedupeDuplicateCount: filterResult.duplicateCount || 0,
    dedupeDuplicateMatches: filterResult.duplicateMatches || [],
  };
}

function sanitizeImportedSourceInput(value) {
  if (!value || typeof value !== 'object') return null;
  const sourceType = String(value.sourceType || '').trim().toLowerCase();
  const playlistKey = String(value.playlistKey || '').trim();
  if (!playlistKey || !['spotify-playlist', 'youtube-playlist', 'plex-playlist', 'plex-collection', 'lastfm-station', 'listenbrainz-playlist'].includes(sourceType)) return null;
  return {
    playlistKey,
    sourceType,
    sourceRef: String(value.sourceRef || '').trim(),
    sourceTitle: String(value.sourceTitle || '').trim().slice(0, 200),
    sourceOwner: String(value.sourceOwner || '').trim().slice(0, 200),
    playlistTitle: String(value.playlistTitle || '').trim().slice(0, 200),
  };
}

function sanitizeImportedContentSetInput(value) {
  if (!value || typeof value !== 'object') return null;
  const kinds = ['genres', 'moods', 'albumGenres', 'albumStyles', 'albumMoods', 'tags', 'decades'];
  const next = {};
  let hasAny = false;
  for (const kind of kinds) {
    const triState = normaliseTriStateInput(value[kind]);
    next[kind] = {
      include: triState.include,
      exclude: [],
      includeMode: 'any',
    };
    if (triState.include.length) hasAny = true;
  }
  return hasAny ? next : null;
}

function percentile(values, ratio) {
  const sorted = (Array.isArray(values) ? values : [])
    .map(Number)
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);
  if (!sorted.length) return null;
  const index = Math.max(0, Math.min(sorted.length - 1, Math.round((sorted.length - 1) * Math.max(0, Math.min(1, Number(ratio) || 0)))));
  return sorted[index];
}

function inferImportedFeaturePreset(tracks = []) {
  const total = Array.isArray(tracks) ? tracks.length : 0;
  if (!total) return 'none';
  let best = { preset: 'none', count: 0, ratio: 0 };
  for (const preset of PLAYLIST_FEATURE_PRESETS) {
    if (preset === 'none') continue;
    const count = applyFeaturePresetFilters(tracks, { featurePreset: preset }).length;
    const ratio = total > 0 ? (count / total) : 0;
    if (ratio > best.ratio || (ratio === best.ratio && count > best.count)) {
      best = { preset, count, ratio };
    }
  }
  if (best.count < Math.max(3, Math.ceil(total * 0.25))) return 'none';
  return best.ratio >= 0.45 ? best.preset : 'none';
}

function inferImportedValueRange(values, options = {}) {
  const low = percentile(values, options.lowRatio == null ? 0.15 : options.lowRatio);
  const high = percentile(values, options.highRatio == null ? 0.85 : options.highRatio);
  if (low == null || high == null) return { min: null, max: null };
  const round = typeof options.round === 'function' ? options.round : ((value) => value);
  const minValue = round(Math.min(low, high));
  const maxValue = round(Math.max(low, high));
  return {
    min: Number.isFinite(minValue) ? minValue : null,
    max: Number.isFinite(maxValue) ? maxValue : null,
  };
}

function inferImportedTopValues(values = [], options = {}) {
  const maxValues = Math.max(1, Number(options.maxValues || 3));
  const minShare = Math.max(0, Math.min(1, Number(options.minShare || 0.3)));
  const counts = new Map();
  (Array.isArray(values) ? values : []).forEach((value) => {
    const normalized = String(value || '').trim();
    if (!normalized) return;
    counts.set(normalized, (counts.get(normalized) || 0) + 1);
  });
  const total = Array.from(counts.values()).reduce((sum, value) => sum + value, 0);
  if (!total) return [];
  return Array.from(counts.entries())
    .sort((a, b) => (b[1] - a[1]) || a[0].localeCompare(b[0]))
    .filter((entry) => (entry[1] / total) >= minShare)
    .slice(0, maxValues)
    .map((entry) => entry[0]);
}

function inferImportedAllValues(values = []) {
  const counts = new Map();
  (Array.isArray(values) ? values : []).forEach((value) => {
    const normalized = String(value || '').trim();
    if (!normalized) return;
    counts.set(normalized, (counts.get(normalized) || 0) + 1);
  });
  if (!counts.size) return [];
  return Array.from(counts.entries())
    .sort((a, b) => (b[1] - a[1]) || a[0].localeCompare(b[0]))
    .map((entry) => entry[0]);
}

function inferImportedWizardPrefill(db, userPlexId, playlist) {
  const matchedTracks = db.prepare(`
    SELECT
      m.rating_key, m.artist_name, m.genres, m.moods,
      m.album_genres, m.album_styles, m.album_moods,
      e.bpm, e.energy, e.danceability, e.track_year, e.original_release_date
    FROM playlist_tracks pt
    INNER JOIN master_tracks m ON m.rating_key = pt.rating_key
    LEFT JOIN track_enrichment e ON e.rating_key = m.rating_key
    WHERE pt.user_plex_id = ? AND pt.playlist_key = ?
  `).all(userPlexId, playlist.playlistKey).map((r) => ({
    ratingKey: r.rating_key,
    artistName: r.artist_name,
    genres: JSON.parse(r.genres || '[]'),
    moods: JSON.parse(r.moods || '[]'),
    albumGenres: JSON.parse(r.album_genres || '[]'),
    albumStyles: JSON.parse(r.album_styles || '[]'),
    albumMoods: JSON.parse(r.album_moods || '[]'),
    bpm: r.bpm,
    energy: r.energy,
    danceability: r.danceability,
    trackYear: r.track_year == null ? null : Number(r.track_year || 0),
    originalReleaseDate: String(r.original_release_date || '').trim(),
  }));
  if (!matchedTracks.length) return null;

  const featurePreset = inferImportedFeaturePreset(matchedTracks);
  const bpmRange = inferImportedValueRange(matchedTracks.map((track) => track?.bpm), { round: (value) => Math.round(value) });
  const energyRange = inferImportedValueRange(matchedTracks.map((track) => track?.energy), { round: (value) => Number(value.toFixed(2)) });
  const danceabilityRange = inferImportedValueRange(matchedTracks.map((track) => track?.danceability), { round: (value) => Number(value.toFixed(2)) });
  const topGenres = inferImportedTopValues(matchedTracks.flatMap((track) => Array.isArray(track?.genres) ? track.genres : []), { minShare: 0.18, maxValues: 4 });
  const topMoods = inferImportedTopValues(matchedTracks.flatMap((track) => Array.isArray(track?.moods) ? track.moods : []), { minShare: 0.18, maxValues: 4 });
  const topAlbumGenres = inferImportedTopValues(matchedTracks.flatMap((track) => Array.isArray(track?.albumGenres) ? track.albumGenres : []), { minShare: 0.18, maxValues: 4 });
  const topAlbumStyles = inferImportedTopValues(matchedTracks.flatMap((track) => Array.isArray(track?.albumStyles) ? track.albumStyles : []), { minShare: 0.18, maxValues: 4 });
  const topAlbumMoods = inferImportedTopValues(matchedTracks.flatMap((track) => Array.isArray(track?.albumMoods) ? track.albumMoods : []), { minShare: 0.18, maxValues: 4 });

  const artistTagMap = getArtistTagMap(db);
  const topTags = inferImportedTopValues(
    matchedTracks.flatMap((track) => getEffectiveTrackTags(track, artistTagMap)),
    { minShare: 0.3, maxValues: 3 },
  );
  const topDecades = inferImportedTopValues(
    matchedTracks.map((track) => getTrackDecadeTag(track)).filter(Boolean),
    { minShare: 0.18, maxValues: 3 },
  );
  const allDetectedGenres = inferImportedAllValues(
    matchedTracks.flatMap((track) => Array.isArray(track?.genres) ? track.genres : []),
  );
  const allDetectedMoods = inferImportedAllValues(
    matchedTracks.flatMap((track) => Array.isArray(track?.moods) ? track.moods : []),
  );
  const allDetectedAlbumGenres = inferImportedAllValues(
    matchedTracks.flatMap((track) => Array.isArray(track?.albumGenres) ? track.albumGenres : []),
  );
  const allDetectedAlbumStyles = inferImportedAllValues(
    matchedTracks.flatMap((track) => Array.isArray(track?.albumStyles) ? track.albumStyles : []),
  );
  const allDetectedAlbumMoods = inferImportedAllValues(
    matchedTracks.flatMap((track) => Array.isArray(track?.albumMoods) ? track.albumMoods : []),
  );
  const allDetectedTags = inferImportedAllValues(
    matchedTracks.flatMap((track) => getEffectiveTrackTags(track, artistTagMap)),
  );
  const allDetectedDecades = inferImportedAllValues(
    matchedTracks.map((track) => getTrackDecadeTag(track)).filter(Boolean),
  );

  const importSource = sanitizeImportedSourceInput({
    playlistKey: playlist.playlistKey,
    sourceType: playlist.sourceType,
    sourceRef: playlist.sourceRef,
    sourceTitle: playlist.sourceTitle,
    sourceOwner: playlist.sourceOwner,
    playlistTitle: playlist.playlistTitle,
  });

  return {
    templateId: 'blank',
    startingPointId: 'blank',
    name: `${String(playlist.playlistTitle || importSource?.sourceTitle || 'Imported Playlist').trim()} Smart`,
    nameManual: true,
    playlistTarget: 'personal',
    rebuildSchedule: 'daily',
    genres: { include: topGenres, exclude: [], includeMode: 'any' },
    moods: { include: topMoods, exclude: [], includeMode: 'any' },
    albumGenres: { include: topAlbumGenres, exclude: [], includeMode: 'any' },
    albumStyles: { include: topAlbumStyles, exclude: [], includeMode: 'any' },
    albumMoods: { include: topAlbumMoods, exclude: [], includeMode: 'any' },
    tags: { include: topTags, exclude: [], includeMode: 'any' },
    decades: { include: topDecades, exclude: [], includeMode: 'any' },
    artistTiers: { include: [], exclude: [], includeMode: 'any' },
    trackTiers: { include: [], exclude: [], includeMode: 'any' },
    featurePreset,
    bpmMin: bpmRange.min,
    bpmMax: bpmRange.max,
    energyMin: energyRange.min,
    energyMax: energyRange.max,
    danceabilityMin: danceabilityRange.min,
    danceabilityMax: danceabilityRange.max,
    camelotFocus: '',
    camelotMode: 'exact',
    albumPopularityMode: 'all',
    popularityMode: 'all',
    popularityPercent: null,
    topNPerArtist: null,
    maxTracksPerAlbum: null,
    maxTracks: null,
    sortBy: 'ratingCount',
    finalOrdering: 'none',
    blendUsers: [],
    blendMode: 'average',
    seasonalGenres: [],
    seasonalKeywords: [],
    seasonalExcludeGenres: [],
    seasonalExcludeKeywords: [],
    seasonalGenresMode: 'any',
    seasonalKeywordsMode: 'any',
    lastPlayedMode: 'any',
    lastPlayedDays: null,
    includeFolders: [],
    excludeFolders: [],
    advancedRules: [],
    deduplicateByMbid: false,
    deduplicateByArtistTitle: false,
    deduplicateByDuration: false,
    deduplicateIgnoreLikelyVariants: false,
    deduplicateIgnoreLiveAlbums: false,
    importSource,
    importSuggestedContent: {
      genres: { include: topGenres, exclude: [], includeMode: 'any' },
      moods: { include: topMoods, exclude: [], includeMode: 'any' },
      albumGenres: { include: topAlbumGenres, exclude: [], includeMode: 'any' },
      albumStyles: { include: topAlbumStyles, exclude: [], includeMode: 'any' },
      albumMoods: { include: topAlbumMoods, exclude: [], includeMode: 'any' },
      tags: { include: topTags, exclude: [], includeMode: 'any' },
      decades: { include: topDecades, exclude: [], includeMode: 'any' },
    },
    importDetectedContent: {
      genres: { include: allDetectedGenres, exclude: [], includeMode: 'any' },
      moods: { include: allDetectedMoods, exclude: [], includeMode: 'any' },
      albumGenres: { include: allDetectedAlbumGenres, exclude: [], includeMode: 'any' },
      albumStyles: { include: allDetectedAlbumStyles, exclude: [], includeMode: 'any' },
      albumMoods: { include: allDetectedAlbumMoods, exclude: [], includeMode: 'any' },
      tags: { include: allDetectedTags, exclude: [], includeMode: 'any' },
      decades: { include: allDetectedDecades, exclude: [], includeMode: 'any' },
    },
    keepImportedSource: true,
    inferenceSummary: {
      matchedTrackCount: matchedTracks.length,
      missingCount: Number(playlist?.missingCount || 0),
      sourceType: importSource?.sourceType || '',
      sourceTitle: importSource?.sourceTitle || playlist?.playlistTitle || '',
      sourceOwner: importSource?.sourceOwner || '',
      featurePreset,
      detectedGenreCount: allDetectedGenres.length,
      detectedMoodCount: allDetectedMoods.length,
      detectedTagCount: allDetectedTags.length,
      detectedDecadeCount: allDetectedDecades.length,
    },
  };
}

async function removeExistingPersonalGeneratedPlaylist({
  db,
  loadConfig,
  resolveUserPlexServerToken,
  buildPlexAuthHeaders,
  userPlexId,
  playlistId,
}) {
  const playlistKey = `personal:${playlistId}`;
  const existing = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
    .find((entry) => String(entry?.playlistKey || '') === playlistKey);
  if (!existing) return;

  const config = loadConfig();
  const { url } = config.plex || {};
  const token = resolveUserPlexServerToken(config, userPlexId);
  const base = String(url || '').replace(/\/$/, '');
  if (base && token && existing.plexPlaylistId) {
    try {
      await fetch(`${base}/playlists/${existing.plexPlaylistId}`, {
        method: 'DELETE',
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
    } catch (_err) { /* best-effort */ }
  }

  deleteUserGeneratedPlaylist(db, userPlexId, playlistKey);
}

async function deleteGeneratedPlaylistWithRemote({
  db,
  loadConfig,
  resolveUserPlexServerToken,
  buildPlexAuthHeaders,
  userPlexId,
  playlistKey,
}) {
  const existing = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
    .find((entry) => String(entry?.playlistKey || '') === String(playlistKey || ''));
  if (!existing) return null;

  const config = loadConfig();
  const { url } = config.plex || {};
  const token = resolveUserPlexServerToken(config, userPlexId);
  const base = String(url || '').replace(/\/$/, '');
  if (base && token && existing.plexPlaylistId) {
    try {
      await fetch(`${base}/playlists/${existing.plexPlaylistId}`, {
        method: 'DELETE',
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
    } catch (_err) { /* best-effort */ }
  }

  deleteUserGeneratedPlaylist(db, userPlexId, existing.playlistKey);
  return existing;
}

function stripArtistSuffix(title, artist) {
  if (!title || !artist) return title || '';
  const suffix = ' - ' + artist;
  return title.endsWith(suffix) ? title.slice(0, -suffix.length) : title;
}

function normalizeTierKey(value) {
  const key = String(value || '').trim().toLowerCase();
  if (key === 'half decent') return 'half-decent';
  return key;
}

function buildTierBadge(key = 'decent') {
  const normalized = normalizeTierKey(key);
  if (normalized === 'skip') return { key: 'skip', label: 'Skip', tone: 'skip' };
  if (normalized === 'half-decent') return { key: 'half-decent', label: 'Half Decent', tone: 'half-decent' };
  if (normalized === 'belter') return { key: 'belter', label: 'Belter', tone: 'belter' };
  if (normalized === 'decent') return { key: 'decent', label: 'Decent', tone: 'decent' };
  if (normalized === 'curatorr') return { key: 'curatorr', label: 'Curatorr', tone: 'curatorr' };
  return { key: 'decent', label: 'Decent', tone: 'decent' };
}

function deriveTrackTier(track) {
  if (!track || typeof track !== 'object') return null;
  if (track.excluded) return buildTierBadge('skip');
  const tier = normalizeTierKey(track.tier);
  if (['skip', 'half-decent', 'decent', 'belter'].includes(tier)) {
    return buildTierBadge(tier);
  }
  if (Number(track.total_skips || 0) > 0) return buildTierBadge('half-decent');
  if (Number(track.total_plays || 0) > 0) return buildTierBadge('decent');
  return null;
}

function deriveHistoryTier(event, config = {}) {
  if (!event || typeof event !== 'object') return buildTierBadge('decent');
  const listenedMs = Number(event.duration_ms || 0);
  const trackDurationMs = Number(event.track_duration_ms || 0);
  const smartSettings = config?.smartPlaylist || {};
  const skipThresholdMs = (Number(smartSettings.skipThresholdSeconds) || 30) * 1000;
  if (trackDurationMs > 0) {
    return buildTierBadge(classifyTier(listenedMs, trackDurationMs, smartSettings));
  }
  if (listenedMs > 0 && listenedMs < skipThresholdMs) {
    return buildTierBadge('skip');
  }
  return deriveTrackTier({
    excluded: Boolean(event.current_excluded),
    force_included: Boolean(event.current_force_included),
    tier: event.current_tier,
  });
}

function getDiscoveryArtCache(key) {
  const entry = discoveryArtCache.get(key);
  if (!entry) return null;
  if (entry.expiresAt <= Date.now()) {
    discoveryArtCache.delete(key);
    return null;
  }
  discoveryArtCache.delete(key);
  discoveryArtCache.set(key, entry);
  return entry;
}

function setDiscoveryArtCache(key, entry) {
  discoveryArtCache.set(key, entry);
  while (discoveryArtCache.size > DISCOVERY_ART_CACHE_MAX) {
    const oldestKey = discoveryArtCache.keys().next().value;
    if (!oldestKey) break;
    discoveryArtCache.delete(oldestKey);
  }
}

function getThumbCache(key) {
  const entry = thumbCache.get(key);
  if (!entry) return null;
  if (entry.expiresAt <= Date.now()) {
    thumbCache.delete(key);
    return null;
  }
  thumbCache.delete(key);
  thumbCache.set(key, entry);
  return entry;
}

function setThumbCache(key, entry) {
  if (!key) return;
  thumbCache.delete(key);
  thumbCache.set(key, entry);
  while (thumbCache.size > THUMB_CACHE_MAX) {
    const oldestKey = thumbCache.keys().next().value;
    if (!oldestKey) break;
    thumbCache.delete(oldestKey);
  }
}

function sendCachedThumbResponse(res, entry) {
  if (!entry) return false;
  if (entry.contentType) res.set('Content-Type', entry.contentType);
  if (entry.cacheControl) res.set('Cache-Control', entry.cacheControl);
  if (entry.location) return res.redirect(entry.status || 302, entry.location);
  if (entry.body) return res.status(entry.status || 200).send(Buffer.from(entry.body));
  return res.status(entry.status || 404).end();
}

function sendThumbImage(res, cacheKey, contentType, buffer, cacheControl = 'public, max-age=86400') {
  const body = Buffer.from(buffer);
  setThumbCache(cacheKey, {
    status: 200,
    contentType: contentType || 'image/jpeg',
    cacheControl,
    body,
    expiresAt: Date.now() + THUMB_CACHE_TTL_MS,
  });
  res.set('Content-Type', contentType || 'image/jpeg');
  res.set('Cache-Control', cacheControl);
  return res.send(body);
}

function sendThumbNotFound(res, cacheKey) {
  setThumbCache(cacheKey, {
    status: 404,
    cacheControl: 'no-store',
    expiresAt: Date.now() + THUMB_NEGATIVE_CACHE_TTL_MS,
  });
  res.set('Cache-Control', 'no-store');
  return res.status(404).end();
}

function sendThumbRedirect(res, cacheKey, location) {
  setThumbCache(cacheKey, {
    status: 302,
    location,
    cacheControl: 'public, max-age=3600',
    expiresAt: Date.now() + THUMB_CACHE_TTL_MS,
  });
  res.set('Cache-Control', 'public, max-age=3600');
  return res.redirect(302, location);
}

function buildDiscoveryArtistArtUrl(name) {
  return `/api/discovery/artist-art/${encodeURIComponent(String(name || '').trim())}?v=${DISCOVERY_ART_URL_VERSION}`;
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

const ARTIST_NAME_ALIASES = new Map([
  ['kanye west', ['Ye']],
  ['ye', ['Kanye West']],
]);

function getArtistLookupTerms(value) {
  const queue = [String(value || '').trim()];
  const terms = [];
  const seen = new Set();
  while (queue.length) {
    const term = String(queue.shift() || '').trim();
    const key = normalizeArtistMatchText(term);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    terms.push(term);
    const aliases = ARTIST_NAME_ALIASES.get(key) || [];
    for (const alias of aliases) queue.push(alias);
  }
  return terms;
}

function artistNamesMatch(left, right) {
  const leftKeys = new Set(getArtistLookupTerms(left).map((item) => normalizeArtistMatchText(item)).filter(Boolean));
  if (!leftKeys.size) return false;
  return getArtistLookupTerms(right)
    .map((item) => normalizeArtistMatchText(item))
    .filter(Boolean)
    .some((item) => leftKeys.has(item));
}

function isKnownPlaceholderImageUrl(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return true;
  return raw.includes('/2a96cbd8b46e442fc41c2b86b821562f.png');
}

async function lookupDeezerArtistArtUrl(artistName) {
  const deezerUrl = new URL('https://api.deezer.com/search/artist');
  deezerUrl.searchParams.set('q', artistName);
  const deezerRes = await fetch(deezerUrl.toString(), {
    headers: {
      Accept: 'application/json',
      'User-Agent': 'Curatorr/1.0',
    },
    signal: AbortSignal.timeout(8000),
  });
  if (!deezerRes.ok) return '';
  const deezerJson = await deezerRes.json();
  const candidates = Array.isArray(deezerJson?.data) ? deezerJson.data : [];
  const picked = candidates.find((item) => artistNamesMatch(item?.name, artistName))
    || candidates.find((item) => getArtistLookupTerms(artistName).some((term) => normalizeArtistMatchText(item?.name).startsWith(normalizeArtistMatchText(term))))
    || candidates[0];
  const remoteThumb = String(picked?.picture_big || picked?.picture_medium || picked?.picture || '').trim();
  return isKnownPlaceholderImageUrl(remoteThumb) ? '' : remoteThumb;
}

async function lookupLastfmArtistArtUrl(artistName, apiKey) {
  if (!apiKey) return '';
  const infoUrl = new URL('https://ws.audioscrobbler.com/2.0/');
  infoUrl.searchParams.set('method', 'artist.getinfo');
  infoUrl.searchParams.set('artist', artistName);
  infoUrl.searchParams.set('api_key', apiKey);
  infoUrl.searchParams.set('format', 'json');
  const infoRes = await fetch(infoUrl.toString(), {
    headers: {
      Accept: 'application/json',
      'User-Agent': 'Curatorr/1.0',
    },
    signal: AbortSignal.timeout(12000),
  });
  if (!infoRes.ok) return '';
  const infoJson = await infoRes.json();
  const images = Array.isArray(infoJson?.artist?.image) ? infoJson.artist.image : [];
  const remoteThumb =
    images.find((img) => String(img?.size || '').toLowerCase() === 'extralarge')?.['#text']
    || images.find((img) => String(img?.size || '').toLowerCase() === 'large')?.['#text']
    || images.find((img) => String(img?.size || '').toLowerCase() === 'medium')?.['#text']
    || images.find((img) => String(img?.size || '').toLowerCase() === 'small')?.['#text']
    || '';
  return isKnownPlaceholderImageUrl(remoteThumb) ? '' : String(remoteThumb || '').trim();
}


function enrichDiscoverRequests(db, userPlexId, requests = []) {
  function resolveDiscoverAlbumImageUrl(album = {}) {
    const directUrl = String(
      album?.selectedAlbumImageUrl
      || album?.preferredAlbumImageUrl
      || album?.albumImageUrl
      || album?.imageUrl
      || ''
    ).trim();
    if (directUrl) return directUrl;
    const imagePath = String(album?.imagePath || '').trim();
    if (imagePath) return `/api/music/lidarr/image?path=${encodeURIComponent(imagePath)}`;
    const foreignAlbumId = String(album?.foreignAlbumId || '').trim();
    if (foreignAlbumId) return `/api/music/cover/release-group/${encodeURIComponent(foreignAlbumId)}`;
    return '';
  }
  function toDiscoverAlbumSelection(album = {}) {
    return {
      albumTitle: String(album?.albumTitle || album?.title || '').trim(),
      albumImageUrl: resolveDiscoverAlbumImageUrl(album),
    };
  }
  const addedAlbumMap = new Map();
  for (const status of ['added_to_lidarr', 'already_monitored']) {
    for (const album of listSuggestedAlbums(db, userPlexId, { status, limit: 500 })) {
      const key = String(album?.artistName || '').trim().toLowerCase();
      if (!key || addedAlbumMap.has(key)) continue;
      addedAlbumMap.set(key, toDiscoverAlbumSelection(album));
    }
  }
  const suggestedAlbumMap = new Map();
  for (const album of listSuggestedAlbums(db, userPlexId, { limit: 500 })) {
    const key = String(album?.artistName || '').trim().toLowerCase();
    if (!key || suggestedAlbumMap.has(key)) continue;
    suggestedAlbumMap.set(key, toDiscoverAlbumSelection(album));
  }
  return (Array.isArray(requests) ? requests : []).map((request) => {
    const detail = request?.detail && typeof request.detail === 'object' ? { ...request.detail } : {};
    const artistKey = String(request?.artistName || '').trim().toLowerCase();
    const suggestion = getSuggestedArtist(db, userPlexId, String(request?.artistName || '').trim());
    const reason = suggestion?.reason && typeof suggestion.reason === 'object' ? suggestion.reason : {};
    const starterAlbum = reason?.starterAlbum && typeof reason.starterAlbum === 'object' ? reason.starterAlbum : null;
    const latestAlbum = reason?.latestAlbum && typeof reason.latestAlbum === 'object' ? reason.latestAlbum : null;
    const starterSelection = toDiscoverAlbumSelection(starterAlbum);
    const latestSelection = toDiscoverAlbumSelection(latestAlbum);
    const addedSelection = addedAlbumMap.get(artistKey) || { albumTitle: '', albumImageUrl: '' };
    const suggestedSelection = suggestedAlbumMap.get(artistKey) || { albumTitle: '', albumImageUrl: '' };
    const currentAlbumTitle = String(
      request?.albumTitle
      || detail.selectedAlbumTitle
      || detail.starterAlbumTitle
      || detail.latestAlbumTitle
      || detail.preferredAlbumTitle
      || ''
    ).trim();
    const selectedAlbumTitle = currentAlbumTitle
      || addedSelection.albumTitle
      || starterSelection.albumTitle
      || latestSelection.albumTitle
      || suggestedSelection.albumTitle
      || '';
    let albumMatch = resolveLibraryAlbumMatch(db, {
      artistName: request?.artistName,
      albumTitle: selectedAlbumTitle,
      alternateTitles: [
        request?.albumTitle,
        detail.selectedAlbumTitle,
        detail.starterAlbumTitle,
        detail.latestAlbumTitle,
        detail.preferredAlbumTitle,
      ],
    });
    if (detail.manualAvailabilityOverride === true) {
      albumMatch = {
        ...albumMatch,
        inLibrary: true,
        kind: 'manual_override',
        matchedAlbumTitle: String(detail.matchedAlbumTitle || selectedAlbumTitle || request?.albumTitle || '').trim(),
      };
    }
    const selectedAlbumImageUrl = String(
      detail.selectedAlbumImageUrl
      || detail.preferredAlbumImageUrl
      || (selectedAlbumTitle && selectedAlbumTitle === starterSelection.albumTitle ? starterSelection.albumImageUrl : '')
      || (selectedAlbumTitle && selectedAlbumTitle === latestSelection.albumTitle ? latestSelection.albumImageUrl : '')
      || (selectedAlbumTitle && selectedAlbumTitle === addedSelection.albumTitle ? addedSelection.albumImageUrl : '')
      || (selectedAlbumTitle && selectedAlbumTitle === suggestedSelection.albumTitle ? suggestedSelection.albumImageUrl : '')
      || ''
    ).trim();
    if (!selectedAlbumTitle) {
      return {
        ...request,
        detail,
        inLibrary: albumMatch.inLibrary,
        inLibraryKind: albumMatch.kind,
        matchedAlbumTitle: albumMatch.matchedAlbumTitle,
      };
    }
    return {
      ...request,
      detail: {
        ...detail,
        selectedAlbumTitle: detail.selectedAlbumTitle || selectedAlbumTitle,
        selectedAlbumImageUrl: detail.selectedAlbumImageUrl || selectedAlbumImageUrl,
        starterAlbumTitle: detail.starterAlbumTitle || starterSelection.albumTitle,
        starterAlbumImageUrl: detail.starterAlbumImageUrl || starterSelection.albumImageUrl,
        latestAlbumTitle: detail.latestAlbumTitle || latestSelection.albumTitle,
        latestAlbumImageUrl: detail.latestAlbumImageUrl || latestSelection.albumImageUrl,
      },
      inLibrary: albumMatch.inLibrary,
      inLibraryKind: albumMatch.kind,
      matchedAlbumTitle: albumMatch.matchedAlbumTitle,
    };
  });
}

function isDiscoverRequestReconciledElsewhere(request) {
  return String(request?.detail?.reconciledAction || '').trim().toLowerCase() === 'already_in_lidarr';
}

function isDiscoverRequestAddedToLibrary(request) {
  return String(request?.status || '').trim().toLowerCase() === 'completed' && request?.inLibrary === true;
}

function splitDiscoverRequestBuckets(requests = []) {
  const queue = [];
  const history = [];
  (Array.isArray(requests) ? requests : []).forEach((request) => {
    if (!request || isDiscoverRequestReconciledElsewhere(request)) return;
    if (isDiscoverRequestAddedToLibrary(request)) {
      history.push(request);
    } else {
      queue.push(request);
    }
  });
  return { queue, history };
}

function normalizeManualAlbumTitle(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function getResolvedRequestAlbumTitle(request) {
  const detail = request?.detail && typeof request.detail === 'object' ? request.detail : {};
  return String(
    request?.albumTitle
    || detail.selectedAlbumTitle
    || detail.starterAlbumTitle
    || detail.latestAlbumTitle
    || detail.preferredAlbumTitle
    || ''
  ).trim();
}

function getManualAlbumStatusMeta(statusKey) {
  if (statusKey === 'available') {
    return { key: 'available', label: 'In library', selectable: false };
  }
  if (statusKey === 'pending') {
    return { key: 'pending', label: 'Monitored in Lidarr', selectable: false };
  }
  return { key: 'missing', label: 'Not in library', selectable: true };
}

function buildManualAlbumStateMap(db, userPlexId, artistName) {
  const map = new Map();
  const artistKey = String(artistName || '').trim().toLowerCase();
  if (!artistKey) return map;
  const libraryAlbumSet = new Set(
    db.prepare('SELECT DISTINCT album_name FROM master_tracks WHERE LOWER(artist_name) = LOWER(?)')
      .all(String(artistName || '').trim())
      .map((row) => normalizeManualAlbumTitle(row?.album_name))
      .filter(Boolean),
  );
  const requests = enrichDiscoverRequests(
    db,
    userPlexId,
    listLidarrRequests(db, userPlexId, { statuses: ['queued', 'processing', 'completed'], limit: 500 }),
  );
  const suggestedAlbums = listSuggestedAlbums(db, userPlexId, { limit: 1000 })
    .filter((album) => String(album?.artistName || '').trim().toLowerCase() === artistKey);
  suggestedAlbums.forEach((album) => {
    const albumKey = normalizeManualAlbumTitle(album?.albumTitle);
    if (!albumKey) return;
    const current = map.get(albumKey) || { excluded: false, status: 'missing' };
    map.set(albumKey, {
      excluded: String(album?.status || '').trim().toLowerCase() === 'dismissed',
      status: current.status,
      suggestion: album,
    });
  });
  requests.forEach((request) => {
    if (String(request?.artistName || '').trim().toLowerCase() !== artistKey) return;
    const albumKey = normalizeManualAlbumTitle(getResolvedRequestAlbumTitle(request));
    if (!albumKey) return;
    const requestStatus = String(request?.status || '').trim().toLowerCase();
    const detail = request?.detail && typeof request.detail === 'object' ? request.detail : {};
    const monitoringConfirmed = detail.monitoredConfirmed === true || detail.alreadyMonitored === true;
    const nextStatus = libraryAlbumSet.has(albumKey) || request?.inLibrary === true
      ? 'available'
      : ((requestStatus === 'queued' || requestStatus === 'processing' || (requestStatus === 'completed' && monitoringConfirmed)) ? 'pending' : 'missing');
    if (nextStatus === 'missing') return;
    const current = map.get(albumKey) || { excluded: false, status: 'missing' };
    if (current.status === 'available') return;
    if (nextStatus === 'available' || current.status === 'missing') {
      map.set(albumKey, { ...current, status: nextStatus });
    }
  });
  libraryAlbumSet.forEach((albumKey) => {
    const current = map.get(albumKey) || { excluded: false, status: 'missing' };
    map.set(albumKey, { ...current, status: 'available' });
  });
  return map;
}

function applyManualPreviewStatuses(albums = [], stateMap = new Map()) {
  return (Array.isArray(albums) ? albums : []).map((album) => {
    const previewStatus = String(album?.statusKey || '').trim().toLowerCase();
    const state = stateMap.get(normalizeManualAlbumTitle(album?.title)) || { excluded: false, status: 'missing' };
    const requestedStatus = state.status;
    const status = requestedStatus === 'available'
      ? 'available'
      : (previewStatus === 'available'
        ? 'available'
        : ((requestedStatus === 'pending' || previewStatus === 'pending') ? 'pending' : 'missing'));
    const meta = getManualAlbumStatusMeta(status);
    return {
      ...album,
      excluded: state.excluded === true,
      statusKey: meta.key,
      statusLabel: meta.label,
      requestable: meta.selectable && state.excluded !== true,
    };
  });
}

function findSuggestedAlbumRecord(db, userPlexId, artistName, albumTitle) {
  const artistKey = String(artistName || '').trim().toLowerCase();
  const albumKey = normalizeManualAlbumTitle(albumTitle);
  if (!artistKey || !albumKey) return null;
  return listSuggestedAlbums(db, userPlexId, { limit: 1000 }).find((album) => {
    return String(album?.artistName || '').trim().toLowerCase() === artistKey
      && normalizeManualAlbumTitle(album?.albumTitle) === albumKey;
  }) || null;
}

function findLidarrRequestForManualAlbum(db, userPlexId, { artistName = '', albumTitle = '', albumId = 0 } = {}) {
  const normalizedArtist = String(artistName || '').trim().toLowerCase();
  const normalizedAlbum = normalizeManualAlbumTitle(albumTitle);
  const numericAlbumId = Number(albumId || 0) || 0;
  return listLidarrRequests(db, userPlexId, { statuses: ['queued', 'processing', 'completed', 'failed'], limit: 500 })
    .find((request) => {
      const detail = request?.detail && typeof request.detail === 'object' ? request.detail : {};
      const requestAlbumId = Number(request?.lidarrAlbumId || detail.albumId || detail.lidarrAlbumId || 0) || 0;
      if (numericAlbumId > 0 && requestAlbumId === numericAlbumId) return true;
      if (!normalizedArtist || !normalizedAlbum) return false;
      return String(request?.artistName || '').trim().toLowerCase() === normalizedArtist
        && normalizeManualAlbumTitle(getResolvedRequestAlbumTitle(request)) === normalizedAlbum;
    }) || null;
}

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
  return /^\/(?:api\/v\d+\/)?(?:MediaCover|MediaCoverProxy)\//.test(raw);
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
  const previousSyncCount = getLastPlaylistSync(db, userPlexId)?.track_count ?? null;
  const { mustIncludeArtists, neverIncludeArtists } = getResolvedUserArtistFilters(db, config, userPlexId);
  const ignoredArtistSet = new Set(neverIncludeArtists.map((a) => cleanMasterArtistName(a).toLowerCase()));
  const likedArtistSet = new Set(mustIncludeArtists.map((a) => cleanMasterArtistName(a).toLowerCase()));

  try {
    // Use master cache — no Plex API call needed
    const masterTracks = getMasterTracks(db);
    if (!masterTracks.length) return; // cache not ready

    const smartSettings = config.smartPlaylist || {};
    const artistSkipRankThreshold = smartSettings.artistSkipRank ?? 2;
    const maxDurationMs = Number(smartSettings.maxTrackDurationMins ?? 0) > 0
      ? Number(smartSettings.maxTrackDurationMins) * 60 * 1000
      : 0;
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
      // Track exceeds max duration setting (0 = no limit)
      if (maxDurationMs > 0 && Number(t.durationMs || 0) > maxDurationMs) { excludedTrackCount++; return false; }
      return true;
    });

    const seenTitles = new Set();
    const ratingKeys = included.filter((t) => {
      const key = `${cleanMasterArtistName(t.artistName).toLowerCase()}|${String(t.trackTitle || '').trim().toLowerCase()}`;
      if (seenTitles.has(key)) return false;
      seenTitles.add(key);
      return true;
    }).map((t) => t.ratingKey);

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
      const addRes = await fetch(addUrl.toString(), {
        method: 'PUT',
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
      if (!addRes.ok) {
        const body = await addRes.text().catch(() => '');
        pushLog({ level: 'warn', app: 'playlist', action: 'sync.add_items_failed', message: `Add playlist items failed: HTTP ${addRes.status}${body ? ` — ${body.slice(0, 200)}` : ''}` });
        break;
      }
    }

    const newCount = ratingKeys.length;
    recordPlaylistSync(db, {
      userPlexId,
      plexPlaylistId: playlistId,
      playlistTitle: playlistRow.playlist_title,
      trackCount: newCount,
      excludedTracks: excludedTrackCount,
      excludedArtists: excludedArtistNames.size,
      trigger: 'auto',
      tracksAdded: previousSyncCount !== null ? Math.max(0, newCount - previousSyncCount) : 0,
      tracksRemoved: previousSyncCount !== null ? Math.max(0, previousSyncCount - newCount) : 0,
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
    spotifyService,
    youtubeService,
    lidarrService,
    canUserAccessLidarrAutomation,
    getPreviewUserId,
    resolveUserPlexServerToken,
    buildAppApiUrl,
    buildPlexAuthHeaders,
    resolveLocalUsers,
    normalizeStoredAvatarPath,
  } = ctx;

  function resolveOverviewUserId(req) {
    const previewUserId = String(getPreviewUserId(req) || '').trim();
    if (previewUserId) return previewUserId;
    const user = req.session?.user || {};
    const role = String(user.role || '').trim().toLowerCase();
    if (role === 'admin') return String(req.query?.user || user.username || '').trim();
    return String(user.username || '').trim();
  }

  function normalizeImportMatchText(value) {
    return String(value || '')
      .trim()
      .toLowerCase()
      .replace(/\([^)]*\)/g, ' ')
      .replace(/\[[^\]]*\]/g, ' ')
      .replace(/\b(feat|featuring|ft)\.? .+$/i, ' ')
      .replace(/[^a-z0-9]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function buildSpotifyTrackLookups(masterTracks) {
    const byArtistTitle = new Map();
    const byTitle = new Map();
    for (const track of Array.isArray(masterTracks) ? masterTracks : []) {
      const ratingKey = String(track?.ratingKey || '').trim();
      if (!ratingKey) continue;
      const artistKey = normalizeImportMatchText(track?.artistName);
      const titleKey = normalizeImportMatchText(track?.trackTitle);
      if (!titleKey) continue;
      const artistTitleKey = `${artistKey}::${titleKey}`;
      const entry = {
        ratingKey,
        artistName: String(track?.artistName || '').trim(),
        trackTitle: String(track?.trackTitle || '').trim(),
        albumName: String(track?.albumName || '').trim(),
        durationMs: Number(track?.durationMs || 0),
      };
      if (!byArtistTitle.has(artistTitleKey)) byArtistTitle.set(artistTitleKey, []);
      byArtistTitle.get(artistTitleKey).push(entry);
      if (!byTitle.has(titleKey)) byTitle.set(titleKey, []);
      byTitle.get(titleKey).push(entry);
    }
    return { byArtistTitle, byTitle };
  }

  function pickSpotifyTrackMatch(trackLookups, spotifyItem) {
    const artists = Array.isArray(spotifyItem?.artists) ? spotifyItem.artists : [];
    const primaryArtist = artists.length ? artists[0].name : '';
    const titleKey = normalizeImportMatchText(spotifyItem?.title);
    const artistKey = normalizeImportMatchText(primaryArtist);
    const durationMs = Number(spotifyItem?.durationMs || 0);
    if (!titleKey) return { method: 'unmatched', match: null, candidates: [] };

    const artistTitleCandidates = trackLookups.byArtistTitle.get(`${artistKey}::${titleKey}`) || [];
    const titleCandidates = trackLookups.byTitle.get(titleKey) || [];
    const candidates = artistTitleCandidates.length ? artistTitleCandidates : titleCandidates;
    if (!candidates.length) return { method: 'unmatched', match: null, candidates: [] };

    let best = null;
    let bestScore = -Infinity;
    candidates.forEach((candidate) => {
      let score = 0;
      if (normalizeImportMatchText(candidate.artistName) === artistKey) score += 100;
      if (normalizeImportMatchText(candidate.trackTitle) === titleKey) score += 100;
      if (durationMs > 0 && Number(candidate.durationMs || 0) > 0) {
        const durationDelta = Math.abs(Number(candidate.durationMs || 0) - durationMs);
        if (durationDelta <= 1500) score += 40;
        else if (durationDelta <= 4000) score += 24;
        else if (durationDelta <= 8000) score += 8;
        else score -= Math.min(30, Math.floor(durationDelta / 1000));
      }
      if (score > bestScore) {
        best = candidate;
        bestScore = score;
      }
    });
    if (!best) return { method: 'unmatched', match: null, candidates };
    return {
      method: artistTitleCandidates.length ? 'artistTitle' : 'title',
      match: best,
      candidates,
    };
  }

  function buildSpotifyUnmatchedArtistGroups(unmatchedTracks, options = {}) {
    const groupLimit = Math.max(1, Number(options.groupLimit || 100));
    const sampleLimit = Math.max(1, Number(options.sampleLimit || 3));
    const groups = new Map();
    (Array.isArray(unmatchedTracks) ? unmatchedTracks : []).forEach((track) => {
      const artists = (Array.isArray(track?.artists) ? track.artists : [])
        .map((artist) => String(artist || '').trim())
        .filter(Boolean);
      const artistName = artists[0] || 'Unknown artist';
      if (!groups.has(artistName)) {
        groups.set(artistName, {
          artistName,
          trackCount: 0,
          sampleTracks: [],
          albumTitles: new Set(),
          albumTypes: new Set(),
        });
      }
      const group = groups.get(artistName);
      group.trackCount += 1;
      if (group.sampleTracks.length < sampleLimit) {
        group.sampleTracks.push({
          title: String(track?.title || '').trim(),
          albumTitle: String(track?.albumTitle || '').trim(),
        });
      }
      const albumTitle = String(track?.albumTitle || '').trim();
      const albumType = String(track?.albumType || '').trim();
      if (albumTitle) group.albumTitles.add(albumTitle);
      if (albumType) group.albumTypes.add(albumType);
    });
    return Array.from(groups.values())
      .sort((a, b) => {
        if (b.trackCount !== a.trackCount) return b.trackCount - a.trackCount;
        return a.artistName.localeCompare(b.artistName);
      })
      .slice(0, groupLimit)
      .map((group) => ({
        artistName: group.artistName,
        trackCount: group.trackCount,
        sampleTracks: group.sampleTracks,
        albumTitles: Array.from(group.albumTitles).slice(0, sampleLimit),
        albumTypes: Array.from(group.albumTypes),
      }));
  }

  function buildGenericImportMatchResult(items = [], trackLookups) {
    const trackRefs = [];
    const unmatched = [];
    const duplicateMatches = [];
    const seenRatingKeys = new Set();
    (Array.isArray(items) ? items : []).forEach((item) => {
      const result = pickSpotifyTrackMatch(trackLookups, item);
      const ratingKey = String(result?.match?.ratingKey || '').trim();
      if (ratingKey && !seenRatingKeys.has(ratingKey)) {
        seenRatingKeys.add(ratingKey);
        trackRefs.push({
          ratingKey,
          artistName: String(result.match.artistName || '').trim(),
        });
        return;
      }
      const row = {
        sourceTrackId: String(item?.id || '').trim(),
        position: Number(item?.position || 0),
        title: String(item?.title || '').trim(),
        artistName: String((Array.isArray(item?.artists) && item.artists[0]?.name) || '').trim(),
        artists: (Array.isArray(item?.artists) ? item.artists : []).map((artist) => ({ name: String(artist?.name || artist || '').trim() })).filter((artist) => artist.name),
        albumTitle: String(item?.album?.title || '').trim(),
        albumType: String(item?.album?.albumType || '').trim(),
        albumImageUrl: String(item?.album?.imageUrl || '').trim(),
        durationMs: Number(item?.durationMs || 0),
      };
      if (ratingKey) {
        duplicateMatches.push(row);
      } else {
        unmatched.push({
          ...row,
          artists: row.artists.map((artist) => artist.name),
        });
      }
    });
    return { trackRefs, unmatched, duplicateMatches };
  }

  function buildGenericImportPreview(items = [], resolveMatch) {
    const matched = [];
    const unmatched = [];
    const duplicateMatches = [];
    const seenRatingKeys = new Set();
    (Array.isArray(items) ? items : []).forEach((item, index) => {
      const result = typeof resolveMatch === 'function' ? resolveMatch(item, index) : { method: 'unmatched', match: null };
      const ratingKey = String(result?.match?.ratingKey || '').trim();
      const summary = {
        position: Number(item?.position || 0),
        ratingKey,
        artistName: String(result?.match?.artistName || '').trim(),
        trackTitle: String(result?.match?.trackTitle || item?.title || '').trim(),
        albumName: String(result?.match?.albumName || '').trim(),
        spotifyTitle: String(item?.title || '').trim(),
        spotifyArtists: (Array.isArray(item?.artists) ? item.artists : []).map((artist) => String(artist?.name || artist || '').trim()).filter(Boolean),
        durationMs: Number(item?.durationMs || 0),
        matchMethod: String(result?.method || '').trim(),
      };
      if (ratingKey) {
        if (seenRatingKeys.has(ratingKey)) duplicateMatches.push(summary);
        else {
          seenRatingKeys.add(ratingKey);
          matched.push(summary);
        }
        return;
      }
      unmatched.push({
        sourceTrackId: String(item?.id || '').trim(),
        position: Number(item?.position || 0),
        title: String(item?.title || '').trim(),
        artistName: String((Array.isArray(item?.artists) && item.artists[0]?.name) || '').trim(),
        artists: (Array.isArray(item?.artists) ? item.artists : []).map((artist) => String(artist?.name || artist || '').trim()).filter(Boolean),
        albumTitle: String(item?.album?.title || '').trim(),
        albumType: String(item?.album?.albumType || '').trim(),
        albumImageUrl: String(item?.album?.imageUrl || '').trim(),
        durationMs: Number(item?.durationMs || 0),
      });
    });
    return {
      matched,
      unmatched,
      duplicateMatches,
      unmatchedArtists: buildSpotifyUnmatchedArtistGroups(unmatched, {
        groupLimit: 100,
        sampleLimit: 3,
      }),
    };
  }

  function normalizeLastfmImportSourceKey(value) {
    const raw = String(value || '').trim();
    if (LASTFM_IMPORT_SOURCE_KEYS.includes(raw)) return raw;
    if (raw.startsWith('topTracks:')) {
      const period = raw.slice('topTracks:'.length);
      if (LASTFM_TOP_TRACKS_PERIODS.includes(period)) return `topTracks:${period}`;
    }
    throw Object.assign(new Error('Unsupported Last.fm source.'), { status: 400 });
  }

  function buildLastfmSourceTitle(sourceKey) {
    if (String(sourceKey || '').startsWith('topTracks:')) {
      const period = String(sourceKey || '').slice('topTracks:'.length);
      return `Last.fm Top Tracks (${TOP_TRACKS_PERIOD_LABELS[period] || period})`;
    }
    return `Last.fm ${LASTFM_STATION_LABELS[sourceKey] || sourceKey}`;
  }

  async function fetchJson(url, options = {}) {
    const response = await fetch(String(url || ''), options);
    if (!response.ok) throw Object.assign(new Error(`HTTP ${response.status}`), { status: response.status });
    return response.json();
  }

  async function fetchLastfmImportSource(userPlexId, rawSourceKey) {
    const sourceKey = normalizeLastfmImportSourceKey(rawSourceKey);
    const prefs = getUserPreferences(db, userPlexId);
    const lastfmUsername = String(prefs?.lastfmUsername || '').trim();
    if (!lastfmUsername) throw Object.assign(new Error('Last.fm username is not configured.'), { status: 400 });
    const apiKey = String(loadConfig()?.discovery?.lastfmApiKey || '').trim();
    let items = [];
    if (sourceKey === 'loved' || sourceKey.startsWith('topTracks:')) {
      if (!apiKey) throw Object.assign(new Error('Last.fm API key is not configured.'), { status: 400 });
      const url = new URL('https://ws.audioscrobbler.com/2.0/');
      url.searchParams.set('method', sourceKey === 'loved' ? 'user.getLovedTracks' : 'user.getTopTracks');
      url.searchParams.set('user', lastfmUsername);
      url.searchParams.set('api_key', apiKey);
      url.searchParams.set('format', 'json');
      url.searchParams.set('limit', '500');
      if (sourceKey.startsWith('topTracks:')) {
        url.searchParams.set('period', sourceKey.slice('topTracks:'.length));
      }
      const data = await fetchJson(url.toString());
      const rawTracks = sourceKey === 'loved'
        ? (Array.isArray(data?.lovedtracks?.track) ? data.lovedtracks.track : [])
        : (Array.isArray(data?.toptracks?.track) ? data.toptracks.track : []);
      items = rawTracks.map((track, index) => ({
        id: String(track?.mbid || `${sourceKey}-${index}`),
        position: index + 1,
        title: String(track?.name || '').trim(),
        artists: [{ name: String(track?.artist?.name || track?.artist || '').trim() }],
        album: { title: '' },
        durationMs: Number(track?.duration || 0) > 0 ? Number(track.duration) * 1000 : 0,
      })).filter((track) => track.title);
    } else {
      const data = await fetchJson(`https://www.last.fm/player/station/user/${encodeURIComponent(lastfmUsername)}/${encodeURIComponent(sourceKey)}`);
      const rawTracks = Array.isArray(data?.playlist) ? data.playlist : [];
      items = rawTracks.map((track, index) => ({
        id: String(track?.identifier || `${sourceKey}-${index}`),
        position: index + 1,
        title: String(track?._name || '').trim(),
        artists: [{ name: String(track?.artists?.[0]?._name || '').trim() }],
        album: { title: String(track?.album?._name || '').trim() },
        durationMs: Number(track?.duration || 0) > 0 ? Number(track.duration) * 1000 : 0,
      })).filter((track) => track.title);
    }
    const trackLookups = buildSpotifyTrackLookups(getMasterTracks(db));
    const matchResult = buildGenericImportMatchResult(items, trackLookups);
    return {
      sourceKey,
      sourceType: 'lastfm-station',
      sourceTitle: buildLastfmSourceTitle(sourceKey),
      sourceOwner: lastfmUsername,
      items,
      matchResult,
    };
  }

  function normalizeListenbrainzImportSourceKey(value) {
    const raw = String(value || '').trim();
    if (!LISTENBRAINZ_IMPORT_SOURCE_KEYS.includes(raw)) {
      throw Object.assign(new Error('Unsupported ListenBrainz playlist source.'), { status: 400 });
    }
    return raw;
  }

  function buildListenbrainzHeaders(token = '') {
    const headers = { Accept: 'application/json' };
    if (token) headers.Authorization = `Token ${token}`;
    return headers;
  }

  function extractMusicbrainzUuid(value) {
    const match = String(value || '').match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
    return match ? match[0].toLowerCase() : '';
  }

  function extractListenbrainzPlaylistUuid(playlistEntry) {
    const identifier = Array.isArray(playlistEntry?.playlist?.identifier)
      ? playlistEntry.playlist.identifier[0]
      : playlistEntry?.playlist?.identifier;
    return extractMusicbrainzUuid(identifier);
  }

  function getListenbrainzSourcePatch(playlistEntry) {
    return String(
      playlistEntry?.playlist?.extension?.['https://musicbrainz.org/doc/jspf#playlist']?.additional_metadata?.algorithm_metadata?.source_patch
      || '',
    ).trim();
  }

  function getListenbrainzTrackArtist(track) {
    const artists = track?.extension?.['https://musicbrainz.org/doc/jspf#track']?.additional_metadata?.artists;
    const firstArtist = Array.isArray(artists) ? artists[0] : null;
    return String(
      firstArtist?.artist_credit_name
      || firstArtist?.artist_name
      || track?.creator
      || '',
    ).trim();
  }

  function getListenbrainzRecordingMbid(track) {
    const identifiers = Array.isArray(track?.identifier) ? track.identifier : [track?.identifier];
    for (const identifier of identifiers) {
      const mbid = extractMusicbrainzUuid(identifier);
      if (mbid) return mbid;
    }
    return '';
  }

  function buildListenbrainzTrackLookups(masterTracks) {
    const byText = buildSpotifyTrackLookups(masterTracks);
    const byRecordingMbid = new Map();
    for (const track of Array.isArray(masterTracks) ? masterTracks : []) {
      const recordingMbid = String(track?.recordingMbid || '').trim().toLowerCase();
      const ratingKey = String(track?.ratingKey || '').trim();
      if (!recordingMbid || !ratingKey || !byRecordingMbid.has(recordingMbid)) {
        if (recordingMbid && ratingKey) byRecordingMbid.set(recordingMbid, {
          ratingKey,
          artistName: String(track?.artistName || '').trim(),
          trackTitle: String(track?.trackTitle || '').trim(),
          albumName: String(track?.albumName || '').trim(),
        });
      }
    }
    return { byText, byRecordingMbid };
  }

  function buildListenbrainzImportMatchResult(tracks = [], trackLookups) {
    const trackRefs = [];
    const unmatched = [];
    const seenRatingKeys = new Set();
    (Array.isArray(tracks) ? tracks : []).forEach((track, index) => {
      const recordingMbid = getListenbrainzRecordingMbid(track);
      const mbidMatch = recordingMbid ? trackLookups.byRecordingMbid.get(recordingMbid) : null;
      const item = {
        id: String(track?.identifier?.[0] || track?.identifier || `${index}`),
        position: index + 1,
        title: String(track?.title || '').trim(),
        artists: [{ name: getListenbrainzTrackArtist(track) }],
        album: { title: String(track?.album || '').trim() },
        durationMs: Number(track?.duration || 0) > 0 ? Number(track.duration) : 0,
      };
      const ratingKey = String(mbidMatch?.ratingKey || '').trim();
      if (ratingKey && !seenRatingKeys.has(ratingKey)) {
        seenRatingKeys.add(ratingKey);
        trackRefs.push({ ratingKey, artistName: String(mbidMatch?.artistName || '').trim() });
        return;
      }
      if (ratingKey) return;
      const textResult = pickSpotifyTrackMatch(trackLookups.byText, item);
      const textRatingKey = String(textResult?.match?.ratingKey || '').trim();
      if (textRatingKey && !seenRatingKeys.has(textRatingKey)) {
        seenRatingKeys.add(textRatingKey);
        trackRefs.push({
          ratingKey: textRatingKey,
          artistName: String(textResult.match.artistName || '').trim(),
        });
        return;
      }
      if (textRatingKey) return;
      unmatched.push({
        sourceTrackId: item.id,
        position: item.position,
        title: item.title,
        artistName: item.artists[0]?.name || '',
        artists: item.artists.map((artist) => artist.name).filter(Boolean),
        albumTitle: item.album.title,
        albumType: '',
        albumImageUrl: '',
        durationMs: item.durationMs,
      });
    });
    return { trackRefs, unmatched };
  }

  function pickListenbrainzTrackMatch(trackLookups, track, index = 0) {
    const recordingMbid = getListenbrainzRecordingMbid(track);
    const mbidMatch = recordingMbid ? trackLookups.byRecordingMbid.get(recordingMbid) : null;
    if (mbidMatch?.ratingKey) {
      return {
        method: 'recordingMbid',
        match: mbidMatch,
      };
    }
    const item = {
      id: String(track?.identifier?.[0] || track?.identifier || `${index}`),
      position: index + 1,
      title: String(track?.title || '').trim(),
      artists: [{ name: getListenbrainzTrackArtist(track) }],
      album: { title: String(track?.album || '').trim() },
      durationMs: Number(track?.duration || 0) > 0 ? Number(track.duration) : 0,
    };
    return pickSpotifyTrackMatch(trackLookups.byText, item);
  }

  async function fetchListenbrainzImportSource(userPlexId, rawSourceKey) {
    const sourceKey = normalizeListenbrainzImportSourceKey(rawSourceKey);
    const prefs = getUserPreferences(db, userPlexId);
    const listenbrainzUsername = String(prefs?.listenbrainzUsername || '').trim();
    if (!listenbrainzUsername) throw Object.assign(new Error('ListenBrainz username is not configured.'), { status: 400 });
    const listenbrainzToken = String(prefs?.listenbrainzToken || '').trim();
    const createdForJson = await fetchJson(
      `https://api.listenbrainz.org/1/user/${encodeURIComponent(listenbrainzUsername)}/playlists/createdfor`,
      { headers: buildListenbrainzHeaders(listenbrainzToken) },
    );
    const playlistEntry = (Array.isArray(createdForJson?.playlists) ? createdForJson.playlists : [])
      .find((entry) => getListenbrainzSourcePatch(entry) === sourceKey);
    const playlistUuid = extractListenbrainzPlaylistUuid(playlistEntry);
    if (!playlistUuid) throw Object.assign(new Error('That ListenBrainz playlist is not currently available.'), { status: 404 });
    const payload = await fetchJson(
      `https://api.listenbrainz.org/1/playlist/${encodeURIComponent(playlistUuid)}`,
      { headers: buildListenbrainzHeaders(listenbrainzToken) },
    );
    const tracks = Array.isArray(payload?.playlist?.track) ? payload.playlist.track : [];
    const trackLookups = buildListenbrainzTrackLookups(getMasterTracks(db));
    const matchResult = buildListenbrainzImportMatchResult(tracks, trackLookups);
    return {
      sourceKey,
      sourceType: 'listenbrainz-playlist',
      sourceTitle: String(playlistEntry?.playlist?.title || `ListenBrainz ${LISTENBRAINZ_PLAYLIST_LABELS[sourceKey] || sourceKey}`).trim(),
      sourceOwner: listenbrainzUsername,
      items: tracks,
      matchResult,
    };
  }

  async function getSpotifyAuthForUser(userPlexId) {
    if (!spotifyService?.isConfigured?.()) {
      const err = new Error('Spotify integration is not configured.');
      err.status = 400;
      throw err;
    }
    const prefs = getUserPreferences(db, userPlexId);
    const refreshToken = String(prefs?.spotifyRefreshToken || '').trim();
    const accessToken = String(prefs?.spotifyAccessToken || '').trim();
    if (!refreshToken && !accessToken) {
      const err = new Error('Connect Spotify in User Settings first.');
      err.status = 400;
      throw err;
    }
    const auth = await spotifyService.ensureAccessToken({
      accessToken,
      refreshToken,
      expiresAt: Number(prefs?.spotifyTokenExpiresAt || 0),
    });
    if (auth.refreshed) {
      saveUserPreferences(db, userPlexId, {
        ...prefs,
        spotifyAccessToken: auth.accessToken,
        spotifyRefreshToken: auth.refreshToken,
        spotifyTokenExpiresAt: auth.expiresAt,
      });
    }
    return auth;
  }

  // Like getSpotifyAuthForUser but returns null instead of throwing when the
  // user has not connected Spotify. Used for URL imports where the public-page
  // scraper works without a token.
  async function tryGetSpotifyAuthForUser(userPlexId) {
    try {
      return await getSpotifyAuthForUser(userPlexId);
    } catch (err) {
      if (Number(err?.status || 0) === 400) return null;
      throw err;
    }
  }

  async function fetchSpotifyPlaylistFromPublicPage(playlistId) {
    if (typeof spotifyService?.getPlaylistFromPublicPage !== 'function') {
      const err = new Error('Spotify integration is not configured.');
      err.status = 400;
      throw err;
    }
    const fallback = await spotifyService.getPlaylistFromPublicPage(playlistId);
    return {
      playlistMeta: fallback.playlist,
      playlistItems: {
        total: Number(fallback.total || 0),
        items: Array.isArray(fallback.items) ? fallback.items : [],
      },
      warning: String(fallback.warning || '').trim(),
      partial: fallback.partial === true,
      source: 'public-page',
    };
  }

  function isSpotifySharedPlaylistFallbackError(err) {
    const status = Number(err?.status || 0);
    return status === 403 || status === 404;
  }

  function normalizeYouTubeImportTitle(value) {
    return String(value || '')
      .replace(/\b(official music video|official video|official audio|official lyric video|official lyrics video|lyrics video|lyric video|visualizer|audio|video|hd|4k)\b/ig, ' ')
      .replace(/["'`]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function parseYouTubeTrackCandidates(item) {
    const rawTitle = normalizeYouTubeImportTitle(item?.title || '');
    const channelTitle = String((Array.isArray(item?.artists) && item.artists[0]?.name) || '').trim();
    const titleCandidates = new Set();
    const artistCandidates = new Set();
    if (channelTitle) artistCandidates.add(channelTitle);
    if (rawTitle) titleCandidates.add(rawTitle);
    const separators = [' - ', ' – ', ' — ', ' | ', ': '];
    separators.forEach((separator) => {
      const index = rawTitle.indexOf(separator);
      if (index <= 0) return;
      const left = rawTitle.slice(0, index).trim();
      const right = rawTitle.slice(index + separator.length).trim();
      if (left && right) {
        artistCandidates.add(left);
        titleCandidates.add(right);
      }
    });
    return {
      titleCandidates: Array.from(titleCandidates).map((value) => normalizeImportMatchText(value)).filter(Boolean),
      artistCandidates: Array.from(artistCandidates).map((value) => normalizeImportMatchText(value)).filter(Boolean),
    };
  }

  function pickYouTubeTrackMatch(trackLookups, item) {
    const parsed = parseYouTubeTrackCandidates(item);
    const seenCandidates = new Set();
    const candidates = [];
    parsed.artistCandidates.forEach((artistKey) => {
      parsed.titleCandidates.forEach((titleKey) => {
        const key = `${artistKey}::${titleKey}`;
        if (seenCandidates.has(key)) return;
        seenCandidates.add(key);
        (trackLookups.byArtistTitle.get(key) || []).forEach((entry) => candidates.push({ entry, method: 'artistTitle', score: 220 }));
      });
    });
    parsed.titleCandidates.forEach((titleKey) => {
      (trackLookups.byTitle.get(titleKey) || []).forEach((entry) => candidates.push({
        entry,
        method: 'title',
        score: parsed.artistCandidates.length && parsed.artistCandidates.some((artistKey) => normalizeImportMatchText(entry.artistName) === artistKey) ? 180 : 120,
      }));
    });
    if (!candidates.length) return { method: 'unmatched', match: null, candidates: [] };
    candidates.sort((a, b) => b.score - a.score);
    return {
      method: candidates[0].method,
      match: candidates[0].entry,
      candidates: candidates.map((candidate) => candidate.entry),
    };
  }

  async function fetchSpotifyPlaylistImportSource(accessToken, playlistId) {
    try {
      const [playlistMeta, playlistItems] = await Promise.all([
        spotifyService.getPlaylist(accessToken, playlistId),
        spotifyService.getPlaylistItems(accessToken, playlistId, { limit: 100 }),
      ]);
      return {
        playlistMeta,
        playlistItems,
        warning: '',
        partial: false,
        source: 'api',
      };
    } catch (err) {
      if (
        isSpotifySharedPlaylistFallbackError(err)
        && typeof spotifyService?.getClientCredentialsToken === 'function'
      ) {
        try {
          const clientToken = await spotifyService.getClientCredentialsToken();
          const [playlistMeta, playlistItems] = await Promise.all([
            spotifyService.getPlaylist(clientToken, playlistId),
            spotifyService.getPlaylistItems(clientToken, playlistId, { limit: 100 }),
          ]);
          return {
            playlistMeta,
            playlistItems,
            warning: '',
            partial: false,
            source: 'api',
          };
        } catch (clientErr) {
          if (!isSpotifySharedPlaylistFallbackError(clientErr)) throw clientErr;
        }
      }
      if (!isSpotifySharedPlaylistFallbackError(err) || typeof spotifyService?.getPlaylistFromPublicPage !== 'function') throw err;
      const fallback = await spotifyService.getPlaylistFromPublicPage(playlistId);
      return {
        playlistMeta: fallback.playlist,
        playlistItems: {
          total: Number(fallback.total || 0),
          items: Array.isArray(fallback.items) ? fallback.items : [],
        },
        warning: String(fallback.warning || '').trim(),
        partial: fallback.partial === true,
        source: 'public-page',
        playlistTrackCount: Number(fallback.totalCount || 0),
      };
    }
  }

  function resolveSpotifyPlaylistId(value) {
    const parsed = spotifyService?.parsePlaylistReference?.(value) || null;
    if (!parsed?.id) {
      const err = new Error('Enter a valid Spotify playlist URL or playlist id.');
      err.status = 400;
      throw err;
    }
    return parsed.id;
  }

  async function fetchYouTubePlaylistImportSource(playlistId) {
    if (!youtubeService?.isConfigured?.()) {
      const err = new Error('YouTube integration is not configured.');
      err.status = 400;
      throw err;
    }
    const [playlistMeta, playlistItems] = await Promise.all([
      youtubeService.getPlaylist(playlistId),
      youtubeService.getPlaylistItems(playlistId, { limit: 250 }),
    ]);
    return {
      playlistMeta,
      playlistItems,
      warning: '',
      partial: false,
      source: 'api',
      playlistTrackCount: Number(playlistMeta?.trackCount || playlistItems?.total || 0),
    };
  }

  function resolveYouTubePlaylistId(value) {
    const parsed = youtubeService?.parsePlaylistReference?.(value) || null;
    if (!parsed?.id) {
      const err = new Error('Enter a valid YouTube playlist URL or playlist id.');
      err.status = 400;
      throw err;
    }
    return parsed.id;
  }

  function buildYouTubeImportMatchResult(items = [], trackLookups) {
    const trackRefs = [];
    const unmatched = [];
    const duplicateMatches = [];
    const seenRatingKeys = new Set();
    (Array.isArray(items) ? items : []).forEach((item) => {
      const result = pickYouTubeTrackMatch(trackLookups, item);
      const ratingKey = String(result?.match?.ratingKey || '').trim();
      if (ratingKey && !seenRatingKeys.has(ratingKey)) {
        seenRatingKeys.add(ratingKey);
        trackRefs.push({
          ratingKey,
          artistName: String(result.match.artistName || '').trim(),
        });
        return;
      }
      if (ratingKey) {
        duplicateMatches.push({
          sourceTrackId: String(item?.id || '').trim(),
          position: Number(item?.position || 0),
          title: String(item?.title || '').trim(),
          artistName: String((Array.isArray(item?.artists) && item.artists[0]?.name) || '').trim(),
          artists: (Array.isArray(item?.artists) ? item.artists : []).map((artist) => String(artist?.name || '').trim()).filter(Boolean),
          albumTitle: '',
          durationMs: Number(item?.durationMs || 0),
        });
        return;
      }
      unmatched.push({
        sourceTrackId: String(item?.id || '').trim(),
        position: Number(item?.position || 0),
        title: String(item?.title || '').trim(),
        artistName: String((Array.isArray(item?.artists) && item.artists[0]?.name) || '').trim(),
        artists: (Array.isArray(item?.artists) ? item.artists : []).map((artist) => String(artist?.name || '').trim()).filter(Boolean),
        albumTitle: '',
        albumType: '',
        albumImageUrl: String(item?.album?.imageUrl || '').trim(),
        durationMs: Number(item?.durationMs || 0),
      });
    });
    return { trackRefs, unmatched, duplicateMatches };
  }

  function makeImportedCustomPlaylistKey() {
    return 'custom-import-' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
  }

  function isAdminRole(req) {
    const role = String(req.session?.user?.role || '').trim().toLowerCase();
    return ['admin', 'co-admin'].includes(role);
  }

  function normalizeArtworkMode(value) {
    const raw = String(value || '').trim().toLowerCase();
    return ['auto', 'preserve', 'custom'].includes(raw) ? raw : 'auto';
  }

  function normalizePlaylistArtworkState(value = {}) {
    return {
      mode: normalizeArtworkMode(value?.mode || value?.artworkMode || 'auto'),
      customArtworkAsset: String(value?.customArtworkAsset || '').trim(),
      preservedArtworkAsset: String(value?.preservedArtworkAsset || '').trim(),
    };
  }

  function parsePlaylistArtworkInput(rawArtwork, fallbackArtwork = {}) {
    const fallback = normalizePlaylistArtworkState(fallbackArtwork);
    if (rawArtwork === undefined) return fallback;
    if (!rawArtwork || typeof rawArtwork !== 'object') return { mode: 'auto', customArtworkAsset: '', preservedArtworkAsset: '' };
    const mode = normalizeArtworkMode(rawArtwork.mode || rawArtwork.artworkMode || fallback.mode || 'auto');
    let customArtworkAsset = String(rawArtwork.customArtworkAsset || fallback.customArtworkAsset || '').trim();
    let preservedArtworkAsset = String(rawArtwork.preservedArtworkAsset || fallback.preservedArtworkAsset || '').trim();

    if (rawArtwork.customArtworkData !== undefined) {
      const artworkData = String(rawArtwork.customArtworkData || '').trim();
      if (artworkData) {
        const parsed = parsePlaylistArtworkDataUrl(artworkData);
        if (!parsed.ok) throw new Error(parsed.error || 'Artwork image is invalid.');
        const saved = savePlaylistArtworkBuffer(parsed.buffer, parsed.ext, rawArtwork.nameHint || 'playlist', 'custom');
        if (!saved) throw new Error('Artwork image could not be saved.');
        customArtworkAsset = saved;
      }
    }

    if (mode === 'auto') preservedArtworkAsset = '';
    if (mode === 'custom' && !customArtworkAsset) throw new Error('Custom artwork is required.');

    return {
      mode,
      customArtworkAsset,
      preservedArtworkAsset,
    };
  }

  function scheduleGlobalImportSync(userPlexId, playlist) {
    setCustomPlaylistAudience(db, userPlexId, playlist.playlistKey, 'global');
    const updatedPlaylist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((p) => p.playlistKey === playlist.playlistKey);
    if (updatedPlaylist) {
      setImmediate(async () => {
        await playlistService.syncGlobalCustomPlaylist(userPlexId, updatedPlaylist).catch(() => {});
      });
    }
  }

  app.get('/api/music/playlists/artwork/:asset', (req, res) => {
    const info = getStoredPlaylistArtworkInfo(req.params.asset);
    if (!info?.filePath) return res.status(404).end();
    res.set('Cache-Control', 'public, max-age=300');
    res.type(info.mime || 'application/octet-stream');
    return res.sendFile(info.filePath);
  });

  function normaliseImportedPlaylistTitle(value, fallback) {
    const title = String(value || '').trim();
    if (title) return title.slice(0, 120);
    return String(fallback || 'Imported Playlist').trim().slice(0, 120) || 'Imported Playlist';
  }

  function findExistingImportedCustomPlaylist(userPlexId, sourceType, sourceRef) {
    const normalizedType = String(sourceType || '').trim().toLowerCase();
    const normalizedRef = String(sourceRef || '').trim();
    if (!normalizedType || !normalizedRef) return null;
    return listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false }).find((entry) => (
      String(entry?.playlistType || '').trim().toLowerCase() === 'custom'
      && String(entry?.sourceType || '').trim().toLowerCase() === normalizedType
      && String(entry?.sourceRef || '').trim() === normalizedRef
    )) || null;
  }

  function normalizeImportedSyncPeriod(value) {
    const normalized = String(value || '').trim().toLowerCase();
    return Object.prototype.hasOwnProperty.call(IMPORTED_SYNC_PERIOD_MS, normalized)
      ? normalized
      : 'disabled';
  }

  function isImportedCustomSourceType(sourceType) {
    return ['spotify-playlist', 'youtube-playlist', 'plex-playlist', 'plex-collection', 'lastfm-station', 'listenbrainz-playlist']
      .includes(String(sourceType || '').trim().toLowerCase());
  }

  function getImportedPlaylistRefreshIntervalMs(period) {
    return Number(IMPORTED_SYNC_PERIOD_MS[normalizeImportedSyncPeriod(period)] || 0);
  }

  async function fetchPlexMusicLibrariesForUser(userPlexId, tokenOverride = '') {
    const config = loadConfig();
    const { url } = config.plex || {};
    const token = tokenOverride || resolveUserPlexServerToken(config, userPlexId);
    if (!url || !token) return [];
    const target = buildAppApiUrl(url, 'library/sections');
    const response = await fetch(target.toString(), {
      headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
    });
    if (!response.ok) throw new Error(`Plex library fetch failed (${response.status})`);
    const json = await response.json();
    return (json?.MediaContainer?.Directory || [])
      .filter((entry) => String(entry?.type || '').trim().toLowerCase() === 'artist')
      .map((entry) => ({
        key: String(entry?.key || '').trim(),
        title: String(entry?.title || '').trim(),
      }))
      .filter((entry) => entry.key);
  }

  async function fetchPlexImportPlaylists(userPlexId) {
    const config = loadConfig();
    const { url } = config.plex || {};
    const token = resolveUserPlexServerToken(config, userPlexId);
    if (!url || !token) return [];
    const blockedIds = new Set(
      listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
        .map((entry) => String(entry?.plexPlaylistId || '').trim())
        .filter(Boolean),
    );
    const legacyPlaylistId = String(getUserPlaylist(db, userPlexId)?.playlist_id || '').trim();
    if (legacyPlaylistId) blockedIds.add(legacyPlaylistId);
    const target = buildAppApiUrl(url, 'playlists?playlistType=audio');
    const response = await fetch(target.toString(), {
      headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
    });
    if (!response.ok) throw new Error(`Plex playlist fetch failed (${response.status})`);
    const json = await response.json();
    return (json?.MediaContainer?.Metadata || [])
      .map((entry) => ({
        sourceType: 'plex-playlist',
        id: String(entry?.ratingKey || '').trim(),
        title: String(entry?.title || '').trim(),
        trackCount: Number(entry?.leafCount || 0),
        updatedAt: Number(entry?.updatedAt || 0),
      }))
      .filter((entry) => entry.id && entry.title && !blockedIds.has(entry.id));
  }

  async function fetchPlexImportCollections(userPlexId) {
    const config = loadConfig();
    const { url } = config.plex || {};
    const token = resolveUserPlexServerToken(config, userPlexId);
    if (!url || !token) return [];
    const libraries = await fetchPlexMusicLibrariesForUser(userPlexId);
    const collections = [];
    for (const library of libraries) {
      const target = buildAppApiUrl(url, `library/sections/${encodeURIComponent(library.key)}/collections`);
      const response = await fetch(target.toString(), {
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
      if (!response.ok) continue;
      const json = await response.json();
      const items = Array.isArray(json?.MediaContainer?.Metadata)
        ? json.MediaContainer.Metadata
        : (Array.isArray(json?.MediaContainer?.Directory) ? json.MediaContainer.Directory : []);
      items.forEach((entry) => {
        const id = String(entry?.ratingKey || entry?.key || '').trim();
        const title = String(entry?.title || '').trim();
        if (!id || !title) return;
        collections.push({
          sourceType: 'plex-collection',
          id,
          title,
          libraryKey: library.key,
          libraryTitle: library.title,
          trackCount: Number(entry?.childCount || entry?.leafCount || 0),
          updatedAt: Number(entry?.updatedAt || 0),
        });
      });
    }
    collections.sort((a, b) => String(a.title || '').localeCompare(String(b.title || '')));
    return collections;
  }

  async function fetchPlexPlaylistImportTracks(userPlexId, plexPlaylistId) {
    const config = loadConfig();
    const { url } = config.plex || {};
    const token = resolveUserPlexServerToken(config, userPlexId);
    if (!url || !token || !plexPlaylistId) return [];
    const tracks = [];
    let offset = 0;
    const pageSize = 1000;
    while (true) {
      const target = buildAppApiUrl(url, `playlists/${encodeURIComponent(plexPlaylistId)}/items`);
      target.searchParams.set('X-Plex-Container-Start', String(offset));
      target.searchParams.set('X-Plex-Container-Size', String(pageSize));
      const response = await fetch(target.toString(), {
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
      if (!response.ok) throw new Error(`Plex playlist track fetch failed (${response.status})`);
      const json = await response.json();
      const batch = (json?.MediaContainer?.Metadata || [])
        .map((track) => ({
          ratingKey: String(track?.ratingKey || '').trim(),
          artistName: String(track?.originalTitle || track?.grandparentTitle || '').trim(),
        }))
        .filter((track) => track.ratingKey);
      tracks.push(...batch);
      if (batch.length < pageSize) break;
      offset += batch.length;
    }
    return tracks;
  }

  async function fetchPlexCollectionImportTracks(userPlexId, collectionId) {
    const config = loadConfig();
    const { url } = config.plex || {};
    const token = resolveUserPlexServerToken(config, userPlexId);
    if (!url || !token || !collectionId) return [];
    const attempts = [
      buildAppApiUrl(url, `library/metadata/${encodeURIComponent(collectionId)}/children`),
      buildAppApiUrl(url, `library/collections/${encodeURIComponent(collectionId)}/children`),
    ];
    for (const target of attempts) {
      const tracks = [];
      let offset = 0;
      const pageSize = 1000;
      while (true) {
        target.searchParams.set('X-Plex-Container-Start', String(offset));
        target.searchParams.set('X-Plex-Container-Size', String(pageSize));
        const response = await fetch(target.toString(), {
          headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
        });
        if (!response.ok) break;
        const json = await response.json();
        const batch = (json?.MediaContainer?.Metadata || [])
          .map((track) => ({
            ratingKey: String(track?.ratingKey || '').trim(),
            artistName: String(track?.originalTitle || track?.grandparentTitle || '').trim(),
          }))
          .filter((track) => track.ratingKey);
        tracks.push(...batch);
        if (batch.length < pageSize) break;
        offset += batch.length;
      }
      if (tracks.length) return tracks;
    }
    return [];
  }

  async function createImportedCustomPlaylist(userPlexId, playlistTitle, trackRefs, sourceMeta = {}) {
    const sourceType = String(sourceMeta?.sourceType || '').trim();
    const sourceRef = String(sourceMeta?.sourceRef || '').trim();
    const existing = findExistingImportedCustomPlaylist(userPlexId, sourceType, sourceRef);
    if (existing) {
      const now = Date.now();
      const unmatchedTracks = Array.isArray(sourceMeta?.unmatchedTracks) ? sourceMeta.unmatchedTracks : [];
      saveUserGeneratedPlaylist(db, userPlexId, {
        ...existing,
        playlistTitle,
        sourceType,
        sourceRef,
        sourceTitle: String(sourceMeta?.sourceTitle || playlistTitle).trim(),
        sourceOwner: String(sourceMeta?.sourceOwner || '').trim(),
        importedSyncPeriod: existing.importedSyncPeriod || 'disabled',
        trackCount: Array.isArray(trackRefs) ? trackRefs.length : 0,
        missingCount: unmatchedTracks.length,
        lastBuiltAt: now,
        updatedAt: now,
      });
      setPlaylistTracks(db, userPlexId, existing.playlistKey, Array.isArray(trackRefs) ? trackRefs : []);
      setImportedPlaylistUnmatched(db, userPlexId, existing.playlistKey, unmatchedTracks);
      const updated = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
        .find((entry) => String(entry?.playlistKey || '') === String(existing.playlistKey || ''));
      if (!updated) return null;
      const synced = updated.active === false ? updated : await playlistService?.syncCustomPlaylist(userPlexId, updated);
      pushLog({
        level: 'info',
        app: 'playlist',
        action: 'import.playlist.refresh',
        message: `Refreshed imported ${sourceType || 'source'} "${sourceMeta?.sourceTitle || playlistTitle}" for ${userPlexId}`,
      });
      return synced || updated;
    }
    const playlistKey = makeImportedCustomPlaylistKey();
    const now = Date.now();
    const unmatchedTracks = Array.isArray(sourceMeta?.unmatchedTracks) ? sourceMeta.unmatchedTracks : [];
    saveUserGeneratedPlaylist(db, userPlexId, {
      playlistKey,
      playlistTitle,
      playlistType: 'custom',
      plexPlaylistId: '',
      sourceType,
      sourceRef,
      sourceTitle: String(sourceMeta?.sourceTitle || playlistTitle).trim(),
      sourceOwner: String(sourceMeta?.sourceOwner || '').trim(),
      importedSyncPeriod: 'disabled',
      active: false,
      trackCount: Array.isArray(trackRefs) ? trackRefs.length : 0,
      missingCount: unmatchedTracks.length,
      lastBuiltAt: now,
      lastSyncedAt: 0,
      updatedAt: now,
    });
    setPlaylistTracks(db, userPlexId, playlistKey, Array.isArray(trackRefs) ? trackRefs : []);
    setImportedPlaylistUnmatched(db, userPlexId, playlistKey, unmatchedTracks);
    const synced = await playlistService?.setGeneratedActive(userPlexId, playlistKey, true);
    pushLog({
      level: 'info',
      app: 'playlist',
      action: 'import.playlist',
      message: `Imported ${sourceMeta.sourceType || 'source'} "${sourceMeta.sourceTitle || playlistTitle}" as ${playlistTitle} for ${userPlexId}`,
    });
    return synced || listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => String(entry?.playlistKey || '') === playlistKey) || null;
  }

  function buildSpotifyImportMatchResult() {
    const trackLookups = buildSpotifyTrackLookups(getMasterTracks(db));
    return function mapPlaylistItems(playlistItems = []) {
      const trackRefs = [];
      const unmatched = [];
      const seenRatingKeys = new Set();
      (playlistItems || []).forEach((item) => {
        const result = pickSpotifyTrackMatch(trackLookups, item);
        const ratingKey = String(result?.match?.ratingKey || '').trim();
        if (ratingKey && !seenRatingKeys.has(ratingKey)) {
          seenRatingKeys.add(ratingKey);
          trackRefs.push({
            ratingKey,
            artistName: String(result.match.artistName || '').trim(),
          });
          return;
        }
        if (result.match?.ratingKey) return;
        unmatched.push({
          sourceTrackId: String(item?.id || '').trim(),
          position: Number(item.position || 0),
          title: String(item.title || '').trim(),
          artistName: String((Array.isArray(item.artists) && item.artists[0]?.name) || '').trim(),
          artists: (Array.isArray(item.artists) ? item.artists : []).map((artist) => String(artist?.name || '').trim()).filter(Boolean),
          albumTitle: String(item?.album?.title || '').trim(),
          albumType: String(item?.album?.albumType || '').trim(),
          durationMs: Number(item.durationMs || 0),
        });
      });
      return { trackRefs, unmatched };
    };
  }

  async function resolveImportedPlexSourceId(userPlexId, playlist) {
    const sourceRef = String(playlist?.sourceRef || '').trim();
    if (sourceRef) return sourceRef;
    const sourceTitle = String(playlist?.sourceTitle || '').trim().toLowerCase();
    const sourceType = String(playlist?.sourceType || '').trim().toLowerCase();
    if (!sourceTitle || !sourceType.startsWith('plex-')) return '';
    const kind = sourceType.replace(/^plex-/, '');
    const sources = kind === 'collection'
      ? await fetchPlexImportCollections(userPlexId)
      : await fetchPlexImportPlaylists(userPlexId);
    const matches = (sources || []).filter((entry) => String(entry?.title || '').trim().toLowerCase() === sourceTitle);
    return matches.length === 1 ? String(matches[0]?.id || '').trim() : '';
  }

  async function resolveImportedSpotifySourceId(userPlexId, playlist, auth) {
    const sourceRef = String(playlist?.sourceRef || '').trim();
    if (sourceRef) return sourceRef;
    const sourceTitle = String(playlist?.sourceTitle || '').trim().toLowerCase();
    const sourceOwner = String(playlist?.sourceOwner || '').trim().toLowerCase();
    if (!sourceTitle) return '';
    const playlists = await spotifyService.listCurrentUserPlaylists(auth.accessToken, { limit: 50 });
    const matches = (playlists || []).filter((entry) => {
      const titleMatch = String(entry?.name || '').trim().toLowerCase() === sourceTitle;
      const ownerMatch = !sourceOwner || String(entry?.ownerName || '').trim().toLowerCase() === sourceOwner;
      return titleMatch && ownerMatch;
    });
    return matches.length === 1 ? String(matches[0]?.id || '').trim() : '';
  }

  async function refreshImportedCustomPlaylist(userPlexId, playlist) {
    const sourceType = String(playlist?.sourceType || '').trim().toLowerCase();
    const now = Date.now();
    let trackRefs = [];
    let unmatched = [];
    let sourceRef = String(playlist?.sourceRef || '').trim();
    let sourceTitle = String(playlist?.sourceTitle || '').trim();
    let sourceOwner = String(playlist?.sourceOwner || '').trim();

    if (sourceType === 'plex-playlist' || sourceType === 'plex-collection') {
      sourceRef = await resolveImportedPlexSourceId(userPlexId, playlist);
      if (!sourceRef) throw new Error('Original Plex source could not be resolved.');
      trackRefs = sourceType === 'plex-playlist'
        ? await fetchPlexPlaylistImportTracks(userPlexId, sourceRef)
        : await fetchPlexCollectionImportTracks(userPlexId, sourceRef);
    } else if (sourceType === 'spotify-playlist') {
      const auth = await getSpotifyAuthForUser(userPlexId);
      sourceRef = await resolveImportedSpotifySourceId(userPlexId, playlist, auth);
      if (!sourceRef) throw new Error('Original Spotify playlist could not be resolved.');
      const [playlistMeta, playlistItems] = await Promise.all([
        spotifyService.getPlaylist(auth.accessToken, sourceRef),
        spotifyService.getPlaylistItems(auth.accessToken, sourceRef, { limit: 100 }),
      ]);
      sourceTitle = String(playlistMeta?.name || sourceTitle || playlist.playlistTitle || '').trim();
      sourceOwner = String(playlistMeta?.ownerName || sourceOwner || '').trim();
      const mapSpotifyPlaylistItems = buildSpotifyImportMatchResult();
      const matchResult = mapSpotifyPlaylistItems(playlistItems.items || []);
      trackRefs = matchResult.trackRefs;
      unmatched = matchResult.unmatched;
    } else if (sourceType === 'youtube-playlist') {
      if (!youtubeService?.isConfigured?.()) throw new Error('YouTube integration is not configured.');
      if (!sourceRef) throw new Error('Original YouTube playlist could not be resolved.');
      const [playlistMeta, playlistItems] = await Promise.all([
        youtubeService.getPlaylist(sourceRef),
        youtubeService.getPlaylistItems(sourceRef, { limit: 250 }),
      ]);
      sourceTitle = String(playlistMeta?.name || sourceTitle || playlist.playlistTitle || '').trim();
      sourceOwner = String(playlistMeta?.ownerName || sourceOwner || '').trim();
      const trackLookups = buildSpotifyTrackLookups(getMasterTracks(db));
      const matchResult = buildYouTubeImportMatchResult(playlistItems.items || [], trackLookups);
      trackRefs = matchResult.trackRefs;
      unmatched = matchResult.unmatched;
    } else if (sourceType === 'lastfm-station') {
      const source = await fetchLastfmImportSource(userPlexId, sourceRef);
      sourceTitle = String(source?.sourceTitle || sourceTitle || playlist.playlistTitle || '').trim();
      sourceOwner = String(source?.sourceOwner || sourceOwner || '').trim();
      trackRefs = source?.matchResult?.trackRefs || [];
      unmatched = source?.matchResult?.unmatched || [];
    } else if (sourceType === 'listenbrainz-playlist') {
      const source = await fetchListenbrainzImportSource(userPlexId, sourceRef);
      sourceTitle = String(source?.sourceTitle || sourceTitle || playlist.playlistTitle || '').trim();
      sourceOwner = String(source?.sourceOwner || sourceOwner || '').trim();
      trackRefs = source?.matchResult?.trackRefs || [];
      unmatched = source?.matchResult?.unmatched || [];
    } else {
      throw new Error('This imported playlist source cannot be refreshed.');
    }

    saveUserGeneratedPlaylist(db, userPlexId, {
      ...playlist,
      sourceRef,
      sourceTitle,
      sourceOwner,
      importedSyncPeriod: playlist.importedSyncPeriod || 'disabled',
      trackCount: trackRefs.length,
      missingCount: unmatched.length,
      lastBuiltAt: now,
      updatedAt: now,
    });
    setPlaylistTracks(db, userPlexId, playlist.playlistKey, trackRefs);
    setImportedPlaylistUnmatched(db, userPlexId, playlist.playlistKey, unmatched);
    const updated = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => String(entry?.playlistKey || '') === String(playlist?.playlistKey || ''));
    if (!updated) return null;
    if (updated.active === false) return updated;
    return playlistService?.syncCustomPlaylist(userPlexId, updated);
  }

  async function refreshScheduledImportedPlaylistsForUser(userPlexId, options = {}) {
    const now = Number(options?.now || Date.now());
    const playlists = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .filter((playlist) => String(playlist?.playlistType || '').trim().toLowerCase() === 'custom')
      .filter((playlist) => isImportedCustomSourceType(playlist?.sourceType))
      .filter((playlist) => playlist?.active !== false)
      .filter((playlist) => normalizeImportedSyncPeriod(playlist?.importedSyncPeriod) !== 'disabled');
    let refreshed = 0;
    let skipped = 0;
    for (const playlist of playlists) {
      const intervalMs = getImportedPlaylistRefreshIntervalMs(playlist.importedSyncPeriod);
      const baseline = Number(playlist?.lastBuiltAt || playlist?.lastSyncedAt || playlist?.updatedAt || playlist?.createdAt || 0);
      if (!intervalMs || (baseline > 0 && (now - baseline) < intervalMs)) {
        skipped += 1;
        continue;
      }
      try {
        await refreshImportedCustomPlaylist(userPlexId, playlist);
        refreshed += 1;
      } catch (err) {
        pushLog({
          level: 'warn',
          app: 'playlist',
          action: 'import.refresh.error',
          message: `Scheduled import refresh failed for ${playlist.playlistTitle || playlist.playlistKey} (${userPlexId}): ${safeMessage(err)}`,
        });
      }
    }
    return { refreshed, skipped };
  }

  ctx.refreshScheduledImportedPlaylistsForUser = refreshScheduledImportedPlaylistsForUser;

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

  function normalizeAlbumMatchText(value) {
    return String(value || '')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function scoreLookupArtistResult(item, term) {
    const query = normalizeLookupText(term);
    const artistName = normalizeLookupText(item?.artistName);
    const sortName = normalizeLookupText(item?.sortName);
    const disambiguation = normalizeLookupText(item?.disambiguation);
    const queryArtistKeys = new Set(getArtistLookupTerms(term).map((item) => normalizeArtistMatchText(item)).filter(Boolean));
    const artistNameKey = normalizeArtistMatchText(item?.artistName);
    const sortNameKey = normalizeArtistMatchText(item?.sortName);
    let score = 0;
    if (!query) return score;
    if (queryArtistKeys.has(artistNameKey)) score += 1100;
    if (queryArtistKeys.has(sortNameKey)) score += 1020;
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
    const seen = new Set();
    const items = [];
    for (const term of getArtistLookupTerms(name)) {
      try {
        const results = await lidarrService.lookupArtist(term);
        for (const item of (Array.isArray(results) ? results : [])) {
          const key = String(item?.foreignArtistId || '').trim().toLowerCase()
            || normalizeArtistMatchText(item?.artistName)
            || normalizeArtistMatchText(item?.sortName);
          if (!key || seen.has(key)) continue;
          seen.add(key);
          items.push(item);
        }
      } catch {
        // Ignore individual lookup failures and fall through to other aliases.
      }
    }
    const best = items
      .map((item) => ({ item, score: scoreLookupArtistResult(item, name) }))
      .sort((a, b) => b.score - a.score)[0]?.item;
    const imagePath = Array.isArray(best?.images)
      ? (
          best.images.find((img) => /poster/i.test(String(img?.coverType || '')))?.url
          || best.images.find((img) => /fanart|banner/i.test(String(img?.coverType || '')))?.url
          || best.images[0]?.url
          || ''
        )
      : '';
    return imagePath ? `/api/music/lidarr/image?path=${encodeURIComponent(imagePath)}` : null;
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

  function buildArtistActivityOverview(status = '', progressStage = '') {
    const normalizedStatus = String(status || '').trim().toLowerCase();
    const normalizedStage = String(progressStage || '').trim().toLowerCase();
    if (normalizedStage === 'awaiting_belter') return 'Starter release acquired. Curatorr is waiting for a stronger listening signal before expanding this artist further.';
    if (normalizedStage === 'catalog_expanded') return 'Curatorr has already expanded this artist beyond the starter release.';
    if (normalizedStage === 'catalog_complete') return 'Curatorr has completed the planned artist expansion.';
    if (normalizedStage === 'starter_album_added' || normalizedStage === 'album_acquired') return 'Starter release acquired and now available in your library.';
    if (normalizedStage === 'queued_for_lidarr' || normalizedStatus === 'queued_for_lidarr') return 'Queued for Lidarr processing.';
    if (normalizedStage === 'queued') return 'Curatorr is currently handing this artist off to Lidarr.';
    if (normalizedStage === 'manual_grab_queued') return 'A manual fallback release grab has been queued for this artist.';
    if (normalizedStage === 'search_retry_queued') return 'Curatorr has queued another search attempt for this artist.';
    if (normalizedStatus === 'added_to_lidarr' || normalizedStage === 'added') return 'Artist added to Lidarr and awaiting the next progression step.';
    if (normalizedStatus === 'already_in_lidarr') return 'This artist is already in Lidarr.';
    return 'Curatorr is tracking this artist as part of your discovery pipeline.';
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
    const config = loadConfig();
    const userPlexId = resolveQueryUserId(req);
    const limit = Math.min(200, Number(req.query?.limit || 100));
    const offset = Number(req.query?.offset || 0);
    const { history, hasMore } = paginateRolledHistory(
      (chunkLimit, chunkOffset) => getRecentHistory(db, userPlexId, chunkLimit, chunkOffset).map((event) => ({
        ...event,
        track_title: stripArtistSuffix(event.track_title, event.artist_name),
      })),
      { limit, offset },
    );
    const popularityByKey = getAlbumPopularTrackRanks(db, history.map((event) => event.plex_rating_key));
    const decoratedHistory = attachAlbumPopularity(history, popularityByKey, 'plex_rating_key').map((event) => ({
      ...event,
      curatorrTier: deriveHistoryTier(event, config),
    }));
    return res.json({ ok: true, history: decoratedHistory, hasMore });
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
      const userPlexId = resolveQueryUserId(req);
      const artistName = String(req.body?.artistName || '').trim();
      const foreignArtistId = String(req.body?.foreignArtistId || '').trim();
      const preview = await lidarrService.previewManualArtistAlbums({ artistName, foreignArtistId });
      const stateMap = buildManualAlbumStateMap(db, userPlexId, String(preview?.artist?.artistName || artistName || ''));
      return res.json({
        ok: true,
        artist: {
          artistName: String(preview?.artist?.artistName || artistName || ''),
          foreignArtistId: String(preview?.artist?.foreignArtistId || foreignArtistId || ''),
          lidarrArtistId: Number(preview?.artist?.id || 0) || null,
          added: Boolean(preview?.artist?.added),
        },
        source: String(preview?.source || ''),
        albums: applyManualPreviewStatuses((Array.isArray(preview?.albums) ? preview.albums : []).map((album) => ({
          ...album,
          image: album?.imagePath
            ? `/api/music/lidarr/image?path=${encodeURIComponent(String(album.imagePath))}`
            : (album?.imageUrl ? String(album.imageUrl) : ''),
        })), stateMap),
        curatorrPickPreview: (Array.isArray(preview?.curatorrPickPreview) ? preview.curatorrPickPreview : []).map((album) => ({
          ...album,
          image: album?.imagePath
            ? `/api/music/lidarr/image?path=${encodeURIComponent(String(album.imagePath))}`
            : (album?.imageUrl ? String(album.imageUrl) : ''),
        })),
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/lidarr/manual/album-overview', requireUser, async (req, res) => {
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    if (!lidarrService?.isConfigured()) {
      return res.status(400).json({ error: 'Lidarr is not configured.' });
    }
    try {
      const userPlexId = resolveQueryUserId(req);
      const artistName = String(req.query?.artist || '').trim();
      const albumTitle = String(req.query?.album || '').trim();
      const albumId = Number(req.query?.albumId || 0) || null;
      const foreignAlbumId = String(req.query?.foreignAlbumId || '').trim();
      const source = String(req.query?.source || '').trim().toLowerCase();
      const hintedStatusKey = String(req.query?.statusKey || '').trim().toLowerCase();
      const hintedStatusLabel = String(req.query?.statusLabel || '').trim();
      const hintedThumb = String(req.query?.thumb || '').trim();
      if (!artistName || !albumTitle) return res.status(400).json({ error: 'artist and album are required.' });

      const overview = await lidarrService.previewManualAlbumOverview({
        artistName,
        albumTitle,
        albumId,
        foreignAlbumId,
        source,
        albumType: String(req.query?.albumType || ''),
        releaseDate: String(req.query?.releaseDate || ''),
      });
      const stateMap = buildManualAlbumStateMap(db, userPlexId, artistName);
      const existingState = stateMap.get(normalizeManualAlbumTitle(albumTitle)) || { excluded: false, status: 'missing' };
      const requestedStatus = existingState.status;
      const resolvedStatus = requestedStatus === 'available'
        ? 'available'
        : (String(overview?.statusKey || '') === 'available'
          ? 'available'
          : ((requestedStatus === 'pending' || String(overview?.statusKey || '') === 'pending') ? 'pending' : 'missing'));
      const statusKey = ['available', 'pending', 'missing'].includes(hintedStatusKey) ? hintedStatusKey : resolvedStatus;
      const statusMeta = getManualAlbumStatusMeta(statusKey);
      const statusLabel = hintedStatusLabel || statusMeta.label;
      const tracks = (Array.isArray(overview?.trackList) ? overview.trackList : [])
        .map((track, index) => ({
          title: String(track?.title || '').trim(),
          mediumNumber: Number(track?.mediumNumber || 0) || 0,
          trackNumber: Number(track?.trackNumber || 0) || 0,
          absoluteNumber: Number(track?.absoluteNumber || index + 1) || (index + 1),
        }))
        .filter((track) => track.title)
        .sort((left, right) => {
          return (left.mediumNumber - right.mediumNumber)
            || (left.trackNumber - right.trackNumber)
            || (left.absoluteNumber - right.absoluteNumber)
            || left.title.localeCompare(right.title);
        });
      const overviewText = statusKey === 'available'
        ? `${albumTitle} is already in your library.`
        : (statusKey === 'pending'
          ? `${albumTitle} is already added in Lidarr and is waiting to arrive in your library.`
          : String(overview?.overview || `${albumTitle} by ${artistName}`));
      return res.json({
        ok: true,
        item: {
          kind: 'album',
          title: String(overview?.albumTitle || albumTitle || '').trim(),
          subtitle: artistName,
          overview: overviewText,
          thumb: String(overview?.thumb || hintedThumb || '').trim(),
          art: artistName ? `/api/music/thumb/artist/${encodeURIComponent(artistName)}` : String(overview?.art || '').trim(),
          posterRatio: 'square',
          pills: [
            'Album',
            statusLabel,
            String(overview?.albumType || '').trim() || '',
            String(overview?.source || source || '').trim() || '',
          ].filter(Boolean),
          stats: [
            { label: 'Tracks', value: Number(overview?.trackCount || tracks.length || 0) },
            { label: 'Available', value: statusKey === 'available' ? 'Yes' : 'No' },
            overview?.releaseDate ? { label: 'Released', value: formatOverviewReleaseDate(overview.releaseDate) } : null,
          ].filter(Boolean),
          trackList: tracks.map((track, index) => ({
            mediumNumber: Number(track?.mediumNumber || 0) || 0,
            index: Number(track?.trackNumber || track?.absoluteNumber || index + 1) || (index + 1),
            title: String(track?.title || '').trim(),
          })).filter((track) => track.title),
          actions: [{
            kind: 'manual-discovery-add-album',
            label: existingState.excluded
              ? 'Excluded from adds'
              : (statusKey === 'available' ? 'Already in library' : (statusKey === 'pending' ? 'Already added' : 'Add album')),
            disabled: statusKey !== 'missing' || existingState.excluded,
            payload: {
              artistName,
              albumTitle: String(overview?.albumTitle || albumTitle || '').trim(),
            },
          }],
        },
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/lidarr/manual/curatorr-pick-overview', requireUser, async (req, res) => {
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    if (!lidarrService?.isConfigured()) {
      return res.status(400).json({ error: 'Lidarr is not configured.' });
    }
    try {
      const userPlexId = resolveQueryUserId(req);
      const artistName = String(req.query?.artist || '').trim();
      const foreignArtistId = String(req.query?.foreignArtistId || '').trim();
      if (!artistName) return res.status(400).json({ error: 'artist is required.' });

      const preview = await lidarrService.previewManualArtistAlbums({ artistName, foreignArtistId });
      const resolvedArtistName = String(preview?.artist?.artistName || artistName || '').trim();
      const stateMap = buildManualAlbumStateMap(db, userPlexId, resolvedArtistName);
      const plan = applyManualPreviewStatuses((Array.isArray(preview?.curatorrPickPlan) ? preview.curatorrPickPlan : []).map((album) => ({
        ...album,
        image: album?.imagePath
          ? `/api/music/lidarr/image?path=${encodeURIComponent(String(album.imagePath))}`
          : (album?.imageUrl ? String(album.imageUrl) : ''),
      })), stateMap);
      const firstImage = String(plan.find((album) => String(album?.image || '').trim())?.image || '').trim();
      return res.json({
        ok: true,
        item: {
          kind: 'album',
          title: 'Let Curatorr choose for you',
          subtitle: resolvedArtistName,
          overview: 'Curatorr will try these albums in priority order. Greatest hits are preferred first, then the highest-ranked fallback albums.',
          thumb: firstImage,
          art: resolvedArtistName ? `/api/music/thumb/artist/${encodeURIComponent(resolvedArtistName)}` : firstImage,
          posterRatio: 'square',
          pills: [
            'Curatorr pick',
            String(preview?.source || '').trim() || '',
          ].filter(Boolean),
          stats: [
            { label: 'Albums', value: plan.length },
          ],
          trackSectionTitle: 'Albums',
          trackList: plan.map((album, index) => ({
            index: index + 1,
            title: String(album?.title || '').trim(),
            thumb: String(album?.image || '').trim(),
            meta: [
              album?.statusLabel ? String(album.statusLabel).trim() : '',
              String(album?.albumType || '').trim() || '',
              String(album?.releaseDate || '').trim() || '',
            ].filter(Boolean).join(' · '),
          })).filter((album) => album.title),
          actions: [{
            kind: 'manual-discovery-add-curatorr-pick',
            label: 'Use Curatorr pick',
            disabled: !resolvedArtistName || !plan.length,
            payload: {
              artistName: resolvedArtistName,
              foreignArtistId: String(preview?.artist?.foreignArtistId || foreignArtistId || '').trim(),
            },
          }],
        },
      });
    } catch (err) {
      const artistName = String(req.query?.artist || '').trim();
      return res.json({
        ok: true,
        item: {
          kind: 'album',
          title: 'Selection unavailable',
          subtitle: artistName,
          overview: 'Curatorr could not build an album selection preview for this request. This usually means the request failed before a candidate album plan was saved.',
          thumb: '',
          art: artistName ? `/api/music/thumb/artist/${encodeURIComponent(artistName)}` : '',
          posterRatio: 'square',
          pills: [
            'Curatorr pick',
            'Unavailable',
          ],
          stats: [],
          trackSectionTitle: 'Albums',
          trackList: [],
          actions: [],
        },
      });
    }
  });

  app.post('/api/music/lidarr/manual/album-exclusion', requireUser, (req, res) => {
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    try {
      const userPlexId = resolveQueryUserId(req);
      const artistName = String(req.body?.artistName || '').trim();
      const albumTitle = String(req.body?.albumTitle || '').trim();
      const excluded = req.body?.excluded === true || req.body?.excluded === 'true';
      if (!artistName || !albumTitle) return res.status(400).json({ error: 'artistName and albumTitle are required.' });
      const existing = findSuggestedAlbumRecord(db, userPlexId, artistName, albumTitle);
      upsertSuggestedAlbum(db, userPlexId, {
        artistName,
        albumTitle,
        albumType: String(req.body?.albumType || existing?.albumType || ''),
        releaseDate: String(req.body?.releaseDate || existing?.releaseDate || ''),
        selectionReason: String(req.body?.selectionReason || existing?.selectionReason || ''),
        rankScore: Number(req.body?.rankScore ?? existing?.rankScore ?? 0),
        lidarrAlbumId: req.body?.lidarrAlbumId ?? existing?.lidarrAlbumId ?? null,
        status: excluded ? 'dismissed' : (existing?.status === 'added_to_lidarr' || existing?.status === 'already_monitored' ? existing.status : 'candidate'),
        updatedAt: Date.now(),
      });
      return res.json({ ok: true, excluded });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/lidarr/manual/force-search', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    if (!lidarrService?.isConfigured()) {
      return res.status(400).json({ error: 'Lidarr is not configured.' });
    }
    const albumId = Number(req.body?.lidarrAlbumId || req.body?.albumId || 0) || 0;
    if (!albumId) return res.status(400).json({ error: 'lidarrAlbumId is required.' });

    try {
      const album = await lidarrService.getAlbum(albumId, { timeoutMs: 12000 });
      if (!album) return res.status(404).json({ error: 'Album not found in Lidarr.' });

      const artistName = String(
        req.body?.artistName
        || album?.artist?.artistName
        || album?.artistMetadata?.artistName
        || ''
      ).trim();
      const albumTitle = String(req.body?.albumTitle || album?.title || '').trim();
      const matchingRequest = findLidarrRequestForManualAlbum(db, userPlexId, { artistName, albumTitle, albumId });
      if (!album.monitored && !matchingRequest) {
        return res.status(400).json({ error: 'Use Add album first so Curatorr can monitor it and track the request.' });
      }

      if (!album.monitored) {
        await lidarrService.setAlbumMonitoredAndVerify(albumId, true);
      }
      const command = await lidarrService.triggerAlbumSearch([albumId]);
      const now = Date.now();
      let updatedRequest = null;
      if (matchingRequest) {
        updatedRequest = updateLidarrRequest(db, matchingRequest.id, {
          albumTitle: albumTitle || matchingRequest.albumTitle,
          lidarrArtistId: Number(album?.artistId || matchingRequest.lidarrArtistId || 0) || matchingRequest.lidarrArtistId || null,
          lidarrAlbumId: albumId,
          status: 'completed',
          processedAt: matchingRequest.processedAt || now,
          updatedAt: now,
          detail: {
            ...(matchingRequest.detail || {}),
            selectedAlbumTitle: albumTitle || matchingRequest.detail?.selectedAlbumTitle || '',
            forcedManualSearch: true,
            forcedManualSearchAt: now,
            forcedSearchCommandId: Number(command?.id || 0) || null,
            searchCommandId: Number(command?.id || matchingRequest.detail?.searchCommandId || 0) || null,
            lastManualSearchStatus: 'queued',
            monitoredConfirmed: true,
            monitoredConfirmedAt: now,
            lastError: '',
            lastErrorCode: '',
          },
        }, matchingRequest.userPlexId);
        updatedRequest = enrichDiscoverRequests(db, matchingRequest.userPlexId, [updatedRequest])[0] || updatedRequest;
      }

      if (artistName && albumTitle) {
        upsertSuggestedAlbum(db, userPlexId, {
          artistName,
          albumTitle,
          albumType: String(req.body?.albumType || album?.albumType || ''),
          releaseDate: String(req.body?.releaseDate || album?.releaseDate || ''),
          selectionReason: 'Manual force search from Discover.',
          rankScore: Number(album?.ratings?.value || 0),
          status: album.monitored ? 'already_monitored' : 'added_to_lidarr',
          lidarrAlbumId: albumId,
          updatedAt: now,
        });

        const existingProgress = getLidarrArtistProgress(db, userPlexId, artistName);
        saveLidarrArtistProgress(db, userPlexId, {
          artistName,
          lidarrArtistId: Number(album?.artistId || existingProgress?.lidarrArtistId || 0) || null,
          currentStage: 'search_queued',
          albumsAddedCount: Number(existingProgress?.albumsAddedCount || 0),
          highestObservedRank: Number(existingProgress?.highestObservedRank || 0),
          lastAlbumAddedAt: existingProgress?.lastAlbumAddedAt ?? null,
          nextReviewAt: now + DAY_MS,
          lastManualSearchAt: now,
          lastManualSearchStatus: 'queued',
          updatedAt: now,
        });
      }

      return res.json({
        ok: true,
        request: updatedRequest,
        command,
        album: {
          albumId,
          albumTitle: albumTitle || String(album?.title || ''),
          monitored: true,
        },
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/lidarr/requests', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const buckets = splitDiscoverRequestBuckets(
      await promoteCompletedRequestsFromLidarr(
        enrichDiscoverRequests(
          db,
          userPlexId,
          listLidarrRequests(db, userPlexId, { statuses: ['queued', 'processing', 'completed', 'failed'], limit: 250 }),
        ),
        lidarrService,
      ),
    );
    return res.json({
      ok: true,
      queued: buckets.queue,
      history: buckets.history,
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

  app.post('/api/music/lidarr/requests/:id/retry', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    if (!lidarrService?.isConfigured()) {
      return res.status(400).json({ error: 'Lidarr is not configured.' });
    }
    const existing = getLidarrRequest(db, req.params.id, userPlexId);
    if (!existing) return res.status(404).json({ error: 'Request not found.' });
    if (!['failed', 'completed'].includes(String(existing.status || '').trim().toLowerCase())) {
      return res.status(400).json({ error: 'Only failed or completed requests can be retried.' });
    }
    updateLidarrRequest(db, existing.id, {
      status: 'queued',
      processedAt: null,
      updatedAt: Date.now(),
      detail: {
        ...(existing.detail || {}),
        retried: true,
        retriedAt: Date.now(),
      },
    }, existing.userPlexId);
    lidarrService.processQueuedRequests({ userPlexId: existing.userPlexId, limit: 1 }).catch(() => {});
    return res.json({ ok: true, request: getLidarrRequest(db, existing.id, existing.userPlexId) });
  });

  app.post('/api/music/lidarr/requests/:id/force-search', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    if (!lidarrService?.isConfigured()) {
      return res.status(400).json({ error: 'Lidarr is not configured.' });
    }
    const existing = getLidarrRequest(db, req.params.id, userPlexId);
    if (!existing) return res.status(404).json({ error: 'Request not found.' });
    const albumId = Number(existing.lidarrAlbumId || existing.detail?.albumId || 0) || 0;
    if (!albumId) return res.status(400).json({ error: 'This request does not have a Lidarr album id yet.' });

    try {
      const album = await lidarrService.getAlbum(albumId, { timeoutMs: 12000 });
      if (!album) return res.status(404).json({ error: 'Album not found in Lidarr.' });
      if (!album.monitored) {
        await lidarrService.setAlbumMonitoredAndVerify(albumId, true);
      }
      const command = await lidarrService.triggerAlbumSearch([albumId]);
      const now = Date.now();
      const updated = updateLidarrRequest(db, existing.id, {
        status: 'completed',
        processedAt: existing.processedAt || now,
        updatedAt: now,
        detail: {
          ...(existing.detail || {}),
          forcedManualSearch: true,
          forcedManualSearchAt: now,
          forcedSearchCommandId: Number(command?.id || 0) || null,
          searchCommandId: Number(command?.id || existing.detail?.searchCommandId || 0) || null,
          lastManualSearchStatus: 'queued',
          monitoredConfirmed: true,
          monitoredConfirmedAt: now,
          lastError: '',
          lastErrorCode: '',
        },
      }, existing.userPlexId);

      const existingProgress = getLidarrArtistProgress(db, existing.userPlexId, existing.artistName);
      saveLidarrArtistProgress(db, existing.userPlexId, {
        artistName: existing.artistName,
        lidarrArtistId: existingProgress?.lidarrArtistId ?? existing.lidarrArtistId ?? null,
        currentStage: 'search_queued',
        albumsAddedCount: Number(existingProgress?.albumsAddedCount || 0),
        highestObservedRank: Number(existingProgress?.highestObservedRank || 0),
        lastAlbumAddedAt: existingProgress?.lastAlbumAddedAt ?? null,
        nextReviewAt: now + DAY_MS,
        lastManualSearchAt: now,
        lastManualSearchStatus: 'queued',
        updatedAt: now,
      });

      const responseRequest = enrichDiscoverRequests(db, existing.userPlexId, [updated])[0] || updated;
      return res.json({ ok: true, request: responseRequest, command });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/lidarr/requests/:id/delete', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const existing = getLidarrRequest(db, req.params.id, userPlexId);
    if (!existing) return res.status(404).json({ error: 'Request not found.' });
    const removed = updateLidarrRequest(db, existing.id, {
      status: 'removed',
      processedAt: Date.now(),
      updatedAt: Date.now(),
      detail: {
        ...(existing.detail || {}),
        deleted: true,
        deletedAt: Date.now(),
      },
    }, existing.userPlexId);
    // If the artist suggestion is stuck at queued_for_lidarr (set when the request was
    // enqueued but never cleared on failure), reset it so the artist doesn't stay
    // permanently stuck in the pipeline with a Queued badge.
    const suggestion = getSuggestedArtist(db, existing.userPlexId, existing.artistName);
    if (suggestion && String(suggestion.status || '').trim() === 'queued_for_lidarr') {
      const resetStatus = Number(existing.lidarrArtistId || 0) > 0 ? 'added_to_lidarr' : 'suggested';
      setSuggestedArtistStatus(db, existing.userPlexId, existing.artistName, resetStatus, {
        reason: {
          ...(suggestion.reason || {}),
          lastFailedRequestId: existing.id,
          lastFailedAt: Date.now(),
        },
      });
    }
    return res.json({ ok: true, request: removed });
  });

  app.post('/api/music/lidarr/requests/:id/select-album', requireUser, async (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    if (!lidarrService?.isConfigured()) {
      return res.status(400).json({ error: 'Lidarr is not configured.' });
    }
    const existing = getLidarrRequest(db, req.params.id, userPlexId);
    if (!existing) return res.status(404).json({ error: 'Request not found.' });

    const albumTitle = String(req.body?.albumTitle || '').trim();
    const foreignArtistId = String(req.body?.foreignArtistId || existing.foreignArtistId || '').trim();
    const lidarrAlbumId = Number(req.body?.lidarrAlbumId || 0) || null;
    const albumImageUrl = String(req.body?.albumImageUrl || '').trim();
    if (!String(existing.artistName || '').trim() || !albumTitle) {
      return res.status(400).json({ error: 'artist and album are required.' });
    }

    const updated = updateLidarrRequest(db, existing.id, {
      albumTitle,
      foreignArtistId,
      lidarrAlbumId,
      status: 'queued',
      processedAt: null,
      updatedAt: Date.now(),
      detail: {
        ...(existing.detail || {}),
        preferredAlbumTitle: albumTitle,
        selectedAlbumTitle: albumTitle,
        selectedAlbumImageUrl: albumImageUrl || String(existing.detail?.selectedAlbumImageUrl || ''),
        useCuratorrPick: false,
        retried: true,
        retriedAt: Date.now(),
        manualSelectionOverride: true,
        manualSelectionOverrideAt: Date.now(),
        manualAvailabilityOverride: false,
        matchedAlbumTitle: '',
        lastError: '',
        lastErrorCode: '',
      },
    }, existing.userPlexId);

    lidarrService.processQueuedRequests({ userPlexId: existing.userPlexId, limit: 1 }).catch(() => {});
    return res.json({ ok: true, request: updated });
  });

  app.post('/api/music/lidarr/requests/:id/mark-available', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    const existing = getLidarrRequest(db, req.params.id, userPlexId);
    if (!existing) return res.status(404).json({ error: 'Request not found.' });
    const now = Date.now();
    const matchedAlbumTitle = String(
      req.body?.matchedAlbumTitle
      || existing.detail?.selectedAlbumTitle
      || existing.albumTitle
      || ''
    ).trim();

    const request = updateLidarrRequest(db, existing.id, {
      status: 'completed',
      processedAt: existing.processedAt || now,
      updatedAt: now,
      detail: {
        ...(existing.detail || {}),
        manualAvailabilityOverride: true,
        manualAvailabilityMarkedAt: now,
        matchedAlbumTitle,
        lastError: '',
        lastErrorCode: '',
      },
    }, existing.userPlexId);

    const existingProgress = getLidarrArtistProgress(db, existing.userPlexId, existing.artistName);
    saveLidarrArtistProgress(db, existing.userPlexId, {
      artistName: existing.artistName,
      lidarrArtistId: existingProgress?.lidarrArtistId ?? existing.lidarrArtistId ?? null,
      currentStage: 'album_acquired',
      albumsAddedCount: Number(existingProgress?.albumsAddedCount || 0),
      highestObservedRank: Number(existingProgress?.highestObservedRank || 0),
      lastAlbumAddedAt: existingProgress?.lastAlbumAddedAt ?? now,
      nextReviewAt: now + (7 * DAY_MS),
      lastManualSearchAt: existingProgress?.lastManualSearchAt ?? now,
      lastManualSearchStatus: 'completed',
      updatedAt: now,
    });

    return res.json({ ok: true, request: getLidarrRequest(db, existing.id, existing.userPlexId), progress: getLidarrArtistProgress(db, existing.userPlexId, existing.artistName) });
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

  // ── Profile page: now-playing via media server sessions ──────────────────

  app.get('/api/music/overview/now-playing', requireUser, async (req, res) => {
    try {
      const config = loadConfig();
      const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
      const currentUsername = String(req.session?.user?.username || '').trim().toLowerCase();

      let sessions = [];

      if (msType === 'plex') {
        const { url, token } = config.plex || {};
        if (!url || !token) return res.json({ nowPlaying: null });
        const sessionsUrl = buildAppApiUrl(url, 'status/sessions');
        const r = await fetch(sessionsUrl.toString(), {
          headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
          signal: AbortSignal.timeout(8000),
        });
        if (!r.ok) return res.json({ nowPlaying: null });
        const json = await r.json();
        const all = json?.MediaContainer?.Metadata || [];
        sessions = all
          .filter((s) => s.type === 'track')
          .map((s) => ({
            title:      String(s.title || ''),
            artist:     String(s.grandparentTitle || ''),
            album:      String(s.parentTitle || ''),
            albumThumb: String(s.parentThumb || s.thumb || ''),
            state:      String(s.Player?.state || 'playing'),
            userName:   String(s.User?.title || '').toLowerCase(),
          }));
      } else {
        const { getAdapter } = await import('../services/media-servers/index.js');
        const adapter = getAdapter(msType);
        const { url, apiKey } = config[msType] || {};
        if (!url || !apiKey) return res.json({ nowPlaying: null });
        const raw = await adapter.getActiveSessions(url, apiKey);
        sessions = raw.map((s) => ({
          title:      s.trackTitle,
          artist:     s.artist,
          album:      s.album,
          albumThumb: s.albumId ? `/api/media-server/art?itemId=${encodeURIComponent(s.albumId)}` : '',
          state:      s.isPaused ? 'paused' : 'playing',
          userName:   String(s.username || '').toLowerCase(),
        }));
      }

      // Match the current logged-in user — include paused sessions too
      const session = sessions.find((s) => s.userName === currentUsername);
      if (!session) return res.json({ nowPlaying: null });

      return res.json({
        nowPlaying: {
          trackTitle:     session.title,
          artistName:     session.artist,
          albumName:      session.album,
          albumThumbPath: session.albumThumb,
          isPaused:       session.state === 'paused',
        },
      });
    } catch {
      return res.json({ nowPlaying: null });
    }
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
    const suggestion = userPlexId ? getSuggestedArtist(db, userPlexId, artistName) : null;
    const progress = userPlexId ? getLidarrArtistProgress(db, userPlexId, artistName) : null;
    if (!sampleTrack?.rating_key && !suggestion && !progress) return res.status(404).json({ error: 'Artist not found.' });
    try {
      const sampleMeta = sampleTrack?.rating_key ? await fetchPlexMetadata(base, token, sampleTrack.rating_key) : null;
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
      const suggestionScore = Number(suggestion?.total_score || 0);
      const statusLabel = String(progress?.currentStage || suggestion?.status || '').trim();
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
          overview: buildOverviewText(
            artistMeta?.summary,
            sampleTrack?.rating_key
              ? `${artistName} currently has ${Number(stats.play_count || 0)} plays and ${Number(stats.skip_count || 0)} skips in Curatorr.`
              : buildArtistActivityOverview(suggestion?.status, progress?.currentStage),
          ),
          thumb: `/api/music/thumb/artist/${encodeURIComponent(artistName)}`,
          art: `/api/music/thumb/artist/${encodeURIComponent(artistName)}`,
          pills: ['Artist', formatTierLabel(tier), statusLabel ? statusLabel.replace(/_/g, ' ') : ''].filter(Boolean),
          stats: [
            { label: 'Plays', value: Number(stats.play_count || 0) },
            { label: 'Skips', value: Number(stats.skip_count || 0) },
            { label: 'Ranking', value: `${Number(Number(stats.ranking_score || 5).toFixed(1))}/10` },
            { label: 'Albums', value: Number(libraryStats.album_count || 0) },
            ...(suggestionScore > 0 ? [{ label: 'Score', value: Number(suggestionScore.toFixed(1)) }] : []),
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
          art: `/api/music/thumb/artist/${encodeURIComponent(artistName)}`,
          posterRatio: 'square',
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
      const masterRow = db.prepare(`
        SELECT
          m.rating_key,
          m.genres,
          m.moods,
          m.file_path,
          m.duration_ms,
          m.rating_count,
          e.track_year,
          e.original_release_date,
          e.bpm,
          e.musical_key,
          e.camelot_key,
          e.energy,
          e.danceability,
          e.loudness,
          e.loudness_range,
          e.peak,
          e.track_gain,
          e.album_gain,
          e.album_peak,
          e.album_range,
          e.analysis_source,
          e.analysis_confidence
        FROM master_tracks m
        LEFT JOIN track_enrichment e ON e.rating_key = m.rating_key
        WHERE m.rating_key = ?
        LIMIT 1
      `).get(ratingKey) || ((trackTitle && artistName)
        ? db.prepare(`
          SELECT
            m.rating_key,
            m.genres,
            m.moods,
            m.file_path,
            m.duration_ms,
            m.rating_count,
            e.track_year,
            e.original_release_date,
            e.bpm,
            e.musical_key,
            e.camelot_key,
            e.energy,
            e.danceability,
            e.loudness,
            e.loudness_range,
            e.peak,
            e.track_gain,
            e.album_gain,
            e.album_peak,
            e.album_range,
            e.analysis_source,
            e.analysis_confidence
          FROM master_tracks m
          LEFT JOIN track_enrichment e ON e.rating_key = m.rating_key
          WHERE LOWER(m.track_title) = LOWER(?)
            AND LOWER(m.artist_name) = LOWER(?)
            AND (? = '' OR LOWER(m.album_name) = LOWER(?))
          ORDER BY m.rating_count DESC, m.updated_at DESC
          LIMIT 1
        `).get(trackTitle, artistName, albumName, albumName) || {}
        : {});
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
      let genres = [];
      let moods = [];
      try { genres = JSON.parse(masterRow.genres || '[]'); } catch { genres = []; }
      try { moods = JSON.parse(masterRow.moods || '[]'); } catch { moods = []; }
      genres = Array.isArray(genres) ? genres.map((value) => String(value || '').trim()).filter(Boolean) : [];
      moods = Array.isArray(moods) ? moods.map((value) => String(value || '').trim()).filter(Boolean) : [];
      const popularity = getAlbumPopularTrackRanks(db, [ratingKey]).get(String(ratingKey || '')) || null;
      const topAlbumTrackPill = popularity?.rank && Number(popularity.rank || 0) > 0 && Number(popularity.rank || 0) <= 3
        ? `🔥 Top ${Number(popularity.rank || 0)} on album`
        : '';
      const detailSections = [];
      const metadataRows = [
        genres.length ? { label: 'Genre', value: genres.join(', ') } : null,
        moods.length ? { label: 'Mood', value: moods.join(', ') } : null,
        Number(masterRow.track_year || 0) > 0 ? { label: 'Year', value: String(Number(masterRow.track_year || 0)) } : null,
        formatOverviewReleaseDate(masterRow.original_release_date) ? { label: 'Original release', value: formatOverviewReleaseDate(masterRow.original_release_date) } : null,
        formatOverviewDuration(masterRow.duration_ms || trackMeta?.duration || 0) ? { label: 'Duration', value: formatOverviewDuration(masterRow.duration_ms || trackMeta?.duration || 0) } : null,
        Number(masterRow.rating_count || 0) > 0 ? { label: 'Plex rating count', value: formatOverviewNumber(masterRow.rating_count || 0) } : null,
        popularity?.rank && Number(popularity.rank || 0) > 0 ? { label: 'Album popularity', value: `Top ${Number(popularity.rank || 0)} on album` } : null,
      ].filter(Boolean);
      if (metadataRows.length) detailSections.push({ title: 'Metadata', rows: metadataRows });
      const audioRows = [
        Number.isFinite(Number(masterRow.bpm)) ? { label: 'BPM', value: formatOverviewMetric(masterRow.bpm, 0) } : null,
        String(masterRow.musical_key || '').trim() ? { label: 'Musical key', value: String(masterRow.musical_key || '').trim() } : null,
        String(masterRow.camelot_key || '').trim() ? { label: 'Camelot', value: String(masterRow.camelot_key || '').trim() } : null,
        Number.isFinite(Number(masterRow.energy)) ? { label: 'Energy', value: formatOverviewMetric(masterRow.energy, 2) } : null,
        Number.isFinite(Number(masterRow.danceability)) ? { label: 'Danceability', value: formatOverviewMetric(masterRow.danceability, 2) } : null,
        Number.isFinite(Number(masterRow.loudness)) ? { label: 'Loudness', value: `${formatOverviewMetric(masterRow.loudness, 1)} LUFS` } : null,
        Number.isFinite(Number(masterRow.loudness_range)) ? { label: 'Loudness range', value: formatOverviewMetric(masterRow.loudness_range, 1) } : null,
        Number.isFinite(Number(masterRow.peak)) ? { label: 'Peak', value: `${formatOverviewMetric(masterRow.peak, 2)} dB` } : null,
        Number.isFinite(Number(masterRow.track_gain)) ? { label: 'Track gain', value: `${formatOverviewMetric(masterRow.track_gain, 1)} dB` } : null,
        Number.isFinite(Number(masterRow.album_gain)) ? { label: 'Album gain', value: `${formatOverviewMetric(masterRow.album_gain, 1)} dB` } : null,
        Number.isFinite(Number(masterRow.album_peak)) ? { label: 'Album peak', value: `${formatOverviewMetric(masterRow.album_peak, 2)} dB` } : null,
        Number.isFinite(Number(masterRow.album_range)) ? { label: 'Album range', value: formatOverviewMetric(masterRow.album_range, 1) } : null,
        String(masterRow.analysis_source || '').trim() ? { label: 'Analysis source', value: String(masterRow.analysis_source || '').trim() } : null,
      ].filter(Boolean);
      if (audioRows.length) detailSections.push({ title: 'Audio Profile', rows: audioRows });
      const libraryRows = [
        String(masterRow.rating_key || ratingKey || '').trim() ? { label: 'Rating key', value: String(masterRow.rating_key || ratingKey || '').trim() } : null,
        String(masterRow.file_path || '').trim() ? { label: 'File', value: String(masterRow.file_path || '').trim() } : null,
      ].filter(Boolean);
      if (libraryRows.length) detailSections.push({ title: 'Library', rows: libraryRows });
      return res.json({
        ok: true,
        item: {
          kind: 'track',
          title: trackTitle || 'Unknown track',
          subtitle: [artistName, albumName].filter(Boolean).join(' · '),
          overview: buildOverviewText(trackMeta?.summary, 'No overview available for this item yet.'),
          thumb: `/api/music/thumb/track/${encodeURIComponent(ratingKey)}`,
          art: artistName ? `/api/music/thumb/artist/${encodeURIComponent(artistName)}` : `/api/music/thumb/track/${encodeURIComponent(ratingKey)}`,
          posterRatio: 'square',
          pills: ['Track', formatTierLabel(tier), topAlbumTrackPill].filter(Boolean),
          detailSections,
          actions: [
            {
              kind: 'track-pin-toggle',
              label: Number(effectiveStats.manually_included || 0) ? 'Unpin track' : 'Pin track',
              payload: {
                ratingKey,
                included: Number(effectiveStats.manually_included || 0) ? false : true,
              },
            },
          ],
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
    const userPlexId = resolveSuggestionUserId(req);
    const limit = Math.min(100, Math.max(1, Number(req.query?.limit || 25)));
    const artists = recommendationService.listCachedSuggestions(userPlexId, { artistLimit: limit }).artists;
    return res.json({ ok: true, artists });
  });

  app.get('/api/music/suggestions/albums', requireUser, (req, res) => {
    const userPlexId = resolveSuggestionUserId(req);
    const limit = Math.min(100, Math.max(1, Number(req.query?.limit || 25)));
    const albums = recommendationService.listCachedSuggestions(userPlexId, { albumLimit: limit }).albums;
    return res.json({ ok: true, albums });
  });

  app.get('/api/music/suggestions/tracks', requireUser, (req, res) => {
    const userPlexId = resolveSuggestionUserId(req);
    const limit = Math.min(200, Math.max(1, Number(req.query?.limit || 50)));
    const tracks = recommendationService.listCachedSuggestions(userPlexId, { trackLimit: limit }).tracks;
    return res.json({ ok: true, tracks });
  });

  app.post('/api/music/suggestions/rebuild', requireUser, async (req, res) => {
    const userPlexId = resolveSuggestionUserId(req);
    try {
      if (!userPlexId) {
        return res.json({
          ok: true,
          generatedAt: Date.now(),
          counts: { artists: 0, albums: 0, tracks: 0 },
          cached: { artists: [], albums: [], tracks: [] },
        });
      }
      const rebuilt = await recommendationService.rebuildSuggestionsForUser(userPlexId, {
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
        await lidarrService.tagCuratorrManagedItems({
          sourceKind: 'manual',
          artistId: lidarrResult.artistId,
          tagArtist: true,
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
            sourceKind: 'manual',
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

            await lidarrService.tagCuratorrManagedItems({
              sourceKind: 'manual',
              albumId,
              tagAlbum: true,
            });
            if (!alreadyMonitored) {
              albumQuota = lidarrService.assertQuotaAvailable(role, latestUsage, { albums: 1 });
              await lidarrService.setAlbumMonitoredAndVerify(albumId, true);
              recordLidarrUsage(db, userPlexId, { roleName: role, usageKey: 'albums', amount: 1 });
              const albumTrackCount = await lidarrService.getAlbumTrackCount(album, { timeoutMs: 12000 });
              if (albumTrackCount > 0) recordLidarrUsage(db, userPlexId, { roleName: role, usageKey: 'tracks', amount: albumTrackCount });
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
              sourceKind: 'manual',
              addedByCuratorr: !alreadyMonitored,
              monitoredConfirmed: true,
              monitoredConfirmedAt: Date.now(),
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
        requestSourceKind: 'manual',
        lidarrExisting: Boolean(lidarrResult.existing),
        artistAddedByCuratorr: !lidarrResult.existing,
        artistAddedSourceKind: !lidarrResult.existing ? 'manual' : (existing.reason?.artistAddedSourceKind || ''),
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
        sourceKind: 'manual',
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
          monitoredConfirmed: Boolean(starterAlbum?.monitoredConfirmed || starterAlbum?.alreadyMonitored || starterAlbum?.addedByCuratorr),
          monitoredConfirmedAt: starterAlbum?.monitoredConfirmed || starterAlbum?.alreadyMonitored || starterAlbum?.addedByCuratorr
            ? Number(starterAlbum?.monitoredConfirmedAt || Date.now())
            : null,
          requestSource: 'manual',
          note: 'Completed from manual suggested artist add.',
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
    const userPlexId = resolveCanonicalUserId(req);
    const config = loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();

    const offset = Math.max(0, Number(req.query.offset || 0));
    const limit = Math.min(200, Math.max(1, Number(req.query.limit || 100)));

    // Resolve playlist ownership from DB (shared by Plex and Jellyfin/Emby paths)
    // Uses case-insensitive user_plex_id comparison to tolerate login-time casing differences.
    let playlistId, playlistTitle;
    const requestedPlexId = String(req.query.plexPlaylistId || '').trim();
    if (requestedPlexId) {
      // Case-insensitive lookup for generated playlists
      const generatedRow = db.prepare(
        `SELECT * FROM user_generated_playlists WHERE plex_playlist_id = ? AND LOWER(user_plex_id) = LOWER(?) LIMIT 1`
      ).get(requestedPlexId, userPlexId);
      // Also check legacy (user_playlists) table
      const legacyRow = db.prepare(
        `SELECT * FROM user_playlists WHERE playlist_id = ? AND LOWER(user_plex_id) = LOWER(?) LIMIT 1`
      ).get(requestedPlexId, userPlexId);
      if (generatedRow) {
        playlistId = String(generatedRow.plex_playlist_id || '');
        playlistTitle = String(generatedRow.playlist_title || generatedRow.playlist_key || 'Playlist');
      } else if (legacyRow) {
        playlistId = String(legacyRow.playlist_id || '');
        playlistTitle = String(legacyRow.playlist_title || 'Playlist');
      } else {
        return res.status(403).json({ error: 'Not authorized to view this playlist.' });
      }
    } else {
      const playlistRow = db.prepare(
        `SELECT * FROM user_playlists WHERE LOWER(user_plex_id) = LOWER(?) LIMIT 1`
      ).get(userPlexId);
      if (!playlistRow?.playlist_id) return res.json({ tracks: [], total: 0, playlistTitle: null });
      playlistId = playlistRow.playlist_id;
      playlistTitle = String(playlistRow.playlist_title || 'Playlist');
    }

    // ── Jellyfin / Emby path ──────────────────────────────────────────────────
    if (msType === 'jellyfin' || msType === 'emby') {
      const { url: msUrl, apiKey } = config[msType] || {};
      if (!msUrl || !apiKey) return res.json({ tracks: [], total: 0, playlistTitle: null });
      try {
        // Resolve Jellyfin userId for this user — cached to avoid a /Users round-trip on every page
        const userCacheKey = `${String(userPlexId || '').toLowerCase()}@${msUrl}`;
        let jellyfinUserId = '';
        const cachedUser = msUserIdCache.get(userCacheKey);
        if (cachedUser && cachedUser.expiresAt > Date.now()) {
          jellyfinUserId = cachedUser.id;
        } else {
          const usersRes = await fetch(new URL('/Users', msUrl).toString(), {
            headers: { 'X-Emby-Token': apiKey, Accept: 'application/json' },
            signal: AbortSignal.timeout(8000),
          });
          const usersList = usersRes.ok ? ((await usersRes.json()) || []) : [];
          const usersArray = Array.isArray(usersList) ? usersList : (usersList?.Items || []);
          const lower = String(userPlexId || '').toLowerCase();
          const jellyfinUser = usersArray.find((u) => String(u.Name || '').toLowerCase() === lower);
          jellyfinUserId = jellyfinUser?.Id || '';
          msUserIdCache.set(userCacheKey, { id: jellyfinUserId, expiresAt: Date.now() + MS_USERID_CACHE_TTL_MS });
        }

        const itemsUrl = new URL(`/Playlists/${encodeURIComponent(playlistId)}/Items`, msUrl);
        if (jellyfinUserId) itemsUrl.searchParams.set('UserId', jellyfinUserId);
        itemsUrl.searchParams.set('Fields', 'RunTimeTicks,AlbumArtist,Album,Artists');
        itemsUrl.searchParams.set('StartIndex', String(offset));
        itemsUrl.searchParams.set('Limit', String(limit));

        const r = await fetch(itemsUrl.toString(), {
          headers: { 'X-Emby-Token': apiKey, Accept: 'application/json' },
          signal: AbortSignal.timeout(15000),
        });
        if (!r.ok) return res.status(502).json({ error: `${msType} returned ${r.status}` });
        const json = await r.json();
        const tracks = (json?.Items || []).map((t) => {
          const artistName = String(t.AlbumArtist || (Array.isArray(t.Artists) ? t.Artists[0] : '') || '');
          const trackId = String(t.Id || '');
          return {
            ratingKey: trackId,
            title: String(t.Name || ''),
            artistName,
            albumName: String(t.Album || ''),
            duration: Math.round((t.RunTimeTicks || 0) / 10000),
            albumThumb: trackId ? `/api/music/thumb/track/${encodeURIComponent(trackId)}` : '',
            artistThumb: artistName ? `/api/music/thumb/artist/${encodeURIComponent(artistName)}` : '',
            playlistItemID: String(t.PlaylistItemId || ''),
          };
        });
        return res.json({ ok: true, tracks, total: Number(json?.TotalRecordCount || 0), playlistTitle, playlistId });
      } catch (err) {
        return res.status(500).json({ error: safeMessage(err) });
      }
    }

    // ── Plex path ─────────────────────────────────────────────────────────────
    const { url } = config.plex || {};
    const token = resolveUserPlexServerToken(config, req.session?.user || userPlexId, req.session?.plexServerToken || '');
    if (!url || !token) return res.json({ tracks: [], total: 0, playlistTitle: null });

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
        albumThumb: t.parentThumb || t.thumb || '',
        artistThumb: t.grandparentThumb || '',
        playlistItemID: t.playlistItemID,
      }));
      const popularityByKey = getAlbumPopularTrackRanks(db, tracks.map((track) => track.ratingKey));
      return res.json({
        ok: true,
        tracks: attachAlbumPopularity(tracks, popularityByKey),
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
    const userPlexId = resolveCanonicalUserId(req);
    try {
      await rebuildSmartPlaylist(ctx, userPlexId);
      const lastSync = getLastPlaylistSync(db, userPlexId);
      return res.json({ ok: true, lastSync });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/playlists/rebuild', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKind = String(req.body?.playlistKind || '').trim().toLowerCase();
    const playlistKey = String(req.body?.playlistKey || '').trim();

    try {
      if (playlistKind === 'legacy' || !playlistKey) {
        await rebuildSmartPlaylist(ctx, userPlexId);
        return res.json({ ok: true });
      }

      const generated = playlistService?.getGeneratedByKey(userPlexId, playlistKey);
      if (generated?.active === false) return res.status(409).json({ error: 'Playlist is disabled. Enable it first.' });

      const playlistType = String(
        generated?.playlistType
        || (playlistKey.startsWith('personal:') ? 'personal' : '')
        || (playlistKey.startsWith('global:') ? 'global' : ''),
      ).trim().toLowerCase();
      if (!playlistType) return res.status(404).json({ error: 'Playlist not found.' });
      if (playlistType === 'curatorred') {
        await rebuildSmartPlaylist(ctx, userPlexId);
        return res.json({ ok: true });
      }
      if (playlistType === 'crescive') {
        await playlistService.syncCrescive(userPlexId, { forceFullRebuild: true });
        return res.json({ ok: true });
      }
      if (playlistType === 'curative') {
        await playlistService.syncCurative(userPlexId, { forceFullRebuild: true });
        return res.json({ ok: true });
      }
      if (playlistType === 'daily-mix') {
        await playlistService.syncDailyMix(userPlexId);
        return res.json({ ok: true });
      }
      if (playlistType === 'curatorr') {
        await playlistService.syncCuratorr(userPlexId);
        return res.json({ ok: true });
      }
      if (playlistType === 'global') {
        const globalId = playlistKey.replace(/^global:/, '');
        const playlistDef = (loadConfig().globalPlaylists || []).find((entry) => String(entry?.id || '') === globalId);
        if (!playlistDef) return res.status(404).json({ error: 'Global playlist definition not found.' });
        await playlistService.syncGlobalPlaylist(userPlexId, playlistDef);
        return res.json({ ok: true });
      }
      if (playlistType === 'personal') {
        const personalId = playlistKey.replace(/^personal:/, '');
        const personalDef = getUserPersonalPlaylist(db, personalId, userPlexId);
        if (!personalDef) return res.status(404).json({ error: 'Personal playlist definition not found.' });
        await playlistService.syncPersonalPlaylist(userPlexId, personalDef);
        return res.json({ ok: true });
      }
      if (playlistType === 'custom') {
        return res.status(400).json({ error: 'Custom playlists do not have an automatic rebuild.' });
      }
      return res.status(400).json({ error: 'This playlist does not support rebuilding.' });
    } catch (err) {
      pushLog({ level: 'error', app: 'playlist', action: 'rebuild.error', message: safeMessage(err) });
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/playlists/imported-refresh', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKey = String(req.body?.playlistKey || '').trim();
    if (!playlistKey) return res.status(400).json({ error: 'playlistKey is required.' });

    const playlist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => entry.playlistKey === playlistKey);
    if (!playlist) return res.status(404).json({ error: 'Playlist not found.' });
    if (String(playlist.playlistType || '').trim().toLowerCase() !== 'custom') {
      return res.status(400).json({ error: 'Only imported custom playlists can be refreshed.' });
    }
    const sourceType = String(playlist.sourceType || '').trim().toLowerCase();
    if (!['spotify-playlist', 'youtube-playlist', 'plex-playlist', 'plex-collection', 'lastfm-station', 'listenbrainz-playlist'].includes(sourceType)) {
      return res.status(400).json({ error: 'This playlist is not linked to an import source.' });
    }

    try {
      const updated = await refreshImportedCustomPlaylist(userPlexId, playlist);
      if (!updated) throw new Error('Could not refresh imported playlist.');
      pushLog({
        level: 'info',
        app: 'playlist',
        action: 'import.refresh',
        message: `Refreshed imported playlist ${playlistKey} for ${userPlexId}`,
      });
      return res.json({
        ok: true,
        playlistKey: updated.playlistKey,
        trackCount: Number(updated.trackCount || 0),
        missingCount: Number(updated.missingCount || 0),
      });
    } catch (err) {
      return res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/playlists/imported-convert', requireUser, (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKey = String(req.query?.playlistKey || '').trim();
    if (!playlistKey) return res.status(400).json({ error: 'playlistKey is required.' });

    const playlist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => entry.playlistKey === playlistKey);
    if (!playlist) return res.status(404).json({ error: 'Playlist not found.' });
    if (String(playlist.playlistType || '').trim().toLowerCase() !== 'custom') {
      return res.status(400).json({ error: 'Only imported custom playlists can be converted.' });
    }
    const sourceType = String(playlist.sourceType || '').trim().toLowerCase();
    if (!['spotify-playlist', 'youtube-playlist', 'plex-playlist', 'plex-collection', 'lastfm-station', 'listenbrainz-playlist'].includes(sourceType)) {
      return res.status(400).json({ error: 'This playlist is not linked to an import source.' });
    }

    const prefill = inferImportedWizardPrefill(db, userPlexId, playlist);
    if (!prefill) {
      return res.status(422).json({ error: 'This imported playlist has no matched Curatorr tracks yet, so there is nothing to infer from.' });
    }

    return res.json({ ok: true, prefill });
  });

  app.post('/api/music/playlists/imported-settings', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKey = String(req.body?.playlistKey || '').trim();
    if (!playlistKey) return res.status(400).json({ error: 'playlistKey is required.' });

    const playlist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => entry.playlistKey === playlistKey);
    if (!playlist) return res.status(404).json({ error: 'Playlist not found.' });
    if (String(playlist.playlistType || '').trim().toLowerCase() !== 'custom' || !isImportedCustomSourceType(playlist.sourceType)) {
      return res.status(400).json({ error: 'Only imported custom playlists can be edited here.' });
    }

    const nextTitle = normaliseImportedPlaylistTitle(
      req.body?.title,
      playlist.playlistTitle || playlist.sourceTitle || 'Imported Playlist',
    );
    const nextImportedSyncPeriod = normalizeImportedSyncPeriod(req.body?.importedSyncPeriod || playlist.importedSyncPeriod);
    const requestedAudience = String(req.body?.audience || playlist.audience || 'personal').trim().toLowerCase();
    const nextAudience = ['personal', 'global'].includes(requestedAudience) ? requestedAudience : 'personal';
    if (nextAudience === 'global' && !isAdminRole(req)) {
      return res.status(403).json({ error: 'Admin access required to make an imported playlist global.' });
    }

    try {
      let updated = playlist;
      let changed = false;

      if (nextTitle !== String(updated.playlistTitle || '').trim()) {
        updated = await playlistService?.renameGeneratedPlaylistTitle(userPlexId, playlistKey, nextTitle) || updated;
        changed = true;
      }

      updated = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
        .find((entry) => entry.playlistKey === playlistKey) || updated;

      if (nextImportedSyncPeriod !== normalizeImportedSyncPeriod(updated.importedSyncPeriod)) {
        saveUserGeneratedPlaylist(db, userPlexId, {
          ...updated,
          importedSyncPeriod: nextImportedSyncPeriod,
          updatedAt: Date.now(),
        });
        updated = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
          .find((entry) => entry.playlistKey === playlistKey) || updated;
        changed = true;
      }

      const currentAudience = String(updated.audience || 'personal').trim().toLowerCase() || 'personal';
      if (nextAudience !== currentAudience) {
        if (nextAudience === 'global') {
          setCustomPlaylistAudience(db, userPlexId, playlistKey, 'global');
        } else {
          setAllCopiesPlaylistAudience(db, playlistKey, 'personal');
        }
        updated = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
          .find((entry) => entry.playlistKey === playlistKey) || updated;
        changed = true;
      }

      if (String(updated.audience || 'personal').trim().toLowerCase() === 'global' && changed) {
        await playlistService?.syncGlobalCustomPlaylist(userPlexId, updated).catch(() => {});
        updated = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
          .find((entry) => entry.playlistKey === playlistKey) || updated;
      }

      pushLog({
        level: 'info',
        app: 'playlist',
        action: 'import.settings',
        message: `Updated imported playlist settings for ${playlistKey} for ${userPlexId}`,
      });
      return res.json({
        ok: true,
        playlistKey,
        playlistTitle: String(updated.playlistTitle || nextTitle).trim(),
        importedSyncPeriod: normalizeImportedSyncPeriod(updated.importedSyncPeriod || nextImportedSyncPeriod),
        audience: String(updated.audience || nextAudience || 'personal').trim().toLowerCase() || 'personal',
        active: updated.active !== false,
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/playlists/generated/rename', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKey = String(req.body?.playlistKey || '').trim();
    if (!playlistKey) return res.status(400).json({ error: 'playlistKey is required' });

    const existing = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((playlist) => playlist.playlistKey === playlistKey);
    if (!existing) return res.status(404).json({ error: 'Playlist not found.' });

    const playlistType = String(existing.playlistType || '').trim().toLowerCase();
    if (!['daily-mix', 'curatorr', 'crescive', 'curative'].includes(playlistType)) {
      return res.status(403).json({ error: 'This playlist type cannot be renamed here.' });
    }

    const nextTitle = String(req.body?.title || '').trim();

    try {
      const updated = await playlistService?.renameGeneratedPlaylistTitle(userPlexId, playlistKey, nextTitle);
      if (!updated) return res.status(500).json({ error: 'Could not update playlist title.' });
      pushLog({
        level: 'info',
        app: 'playlist',
        action: 'generated.rename',
        message: `${playlistKey} renamed to "${updated.playlistTitle}" for ${userPlexId}`,
      });
      return res.json({
        ok: true,
        playlistKey: updated.playlistKey,
        playlistTitle: updated.playlistTitle,
        titleOverride: updated.titleOverride || '',
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/playlists/generated/artwork', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKey = String(req.body?.playlistKey || '').trim();
    if (!playlistKey) return res.status(400).json({ error: 'playlistKey is required' });

    const playlist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => entry.playlistKey === playlistKey);
    if (!playlist) return res.status(404).json({ error: 'Playlist not found.' });
    if (String(playlist.playlistType || '').trim().toLowerCase() === 'global' && !isAdminRole(req)) {
      return res.status(403).json({ error: 'Admin access required to edit global playlist artwork.' });
    }

    try {
      const artwork = parsePlaylistArtworkInput(req.body?.artwork, {
        mode: playlist.artworkMode || 'auto',
        customArtworkAsset: playlist.customArtworkAsset || '',
        preservedArtworkAsset: playlist.preservedArtworkAsset || '',
      });
      const updated = await playlistService?.updateGeneratedPlaylistArtwork(userPlexId, playlistKey, artwork);
      if (!updated) return res.status(500).json({ error: 'Could not update playlist artwork.' });
      pushLog({
        level: 'info',
        app: 'playlist',
        action: 'generated.artwork',
        message: `Updated artwork mode for ${playlistKey} to "${updated.artworkMode || artwork.mode}" for ${userPlexId}`,
      });
      return res.json({
        ok: true,
        playlistKey: updated.playlistKey,
        artworkMode: updated.artworkMode || artwork.mode,
        customArtworkAsset: updated.customArtworkAsset || '',
        preservedArtworkAsset: updated.preservedArtworkAsset || '',
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/playlists/generated/state', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKeys = Array.isArray(req.body?.playlistKeys)
      ? req.body.playlistKeys.map((value) => String(value || '').trim()).filter(Boolean)
      : [String(req.body?.playlistKey || '').trim()].filter(Boolean);
    const active = Boolean(req.body?.active);
    if (!playlistKeys.length) return res.status(400).json({ error: 'playlistKey is required' });

    try {
      const updated = [];
      for (const playlistKey of [...new Set(playlistKeys)]) {
        const playlist = await playlistService?.setGeneratedActive(userPlexId, playlistKey, active);
        if (playlist) {
          updated.push({
            playlistKey: playlist.playlistKey,
            plexPlaylistId: playlist.plexPlaylistId,
            active: Boolean(playlist.active),
            trackCount: Number(playlist.trackCount || 0),
          });
        }
      }
      pushLog({
        level: 'info',
        app: 'playlist',
        action: active ? 'generated.enable' : 'generated.disable',
        message: `${active ? 'Enabled' : 'Disabled'} ${updated.length} playlist(s) for ${userPlexId}`,
      });
      return res.json({ ok: true, playlists: updated });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/playlists/imported-sync-period', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKey = String(req.body?.playlistKey || '').trim();
    const importedSyncPeriod = normalizeImportedSyncPeriod(req.body?.importedSyncPeriod);
    if (!playlistKey) return res.status(400).json({ error: 'playlistKey is required' });

    const playlist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => entry.playlistKey === playlistKey);
    if (!playlist) return res.status(404).json({ error: 'Playlist not found.' });
    if (String(playlist.playlistType || '').trim().toLowerCase() !== 'custom' || !isImportedCustomSourceType(playlist.sourceType)) {
      return res.status(400).json({ error: 'Only imported custom playlists can set a sync period.' });
    }

    saveUserGeneratedPlaylist(db, userPlexId, {
      ...playlist,
      importedSyncPeriod,
      updatedAt: Date.now(),
    });
    const updated = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => entry.playlistKey === playlistKey);
    pushLog({
      level: 'info',
      app: 'playlist',
      action: 'import.sync-period',
      message: `Set imported sync period for ${playlistKey} to ${IMPORTED_SYNC_PERIOD_LABELS[importedSyncPeriod] || importedSyncPeriod} for ${userPlexId}`,
    });
    return res.json({
      ok: true,
      playlistKey,
      importedSyncPeriod,
      importedSyncPeriodLabel: IMPORTED_SYNC_PERIOD_LABELS[importedSyncPeriod] || importedSyncPeriod,
      active: updated?.active !== false,
    });
  });

  app.post('/api/music/playlists/imported-audience', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const role = String(req.session?.user?.role || '').trim().toLowerCase();
    if (!['admin', 'co-admin'].includes(role)) return res.status(403).json({ error: 'Admin access required to set playlist audience.' });

    const playlistKey = String(req.body?.playlistKey || '').trim();
    const audience = String(req.body?.audience || '').trim().toLowerCase();
    if (!playlistKey) return res.status(400).json({ error: 'playlistKey is required.' });
    if (!['personal', 'global'].includes(audience)) return res.status(400).json({ error: 'audience must be "personal" or "global".' });

    const playlist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => entry.playlistKey === playlistKey);
    if (!playlist) return res.status(404).json({ error: 'Playlist not found.' });
    if (String(playlist.playlistType || '').trim().toLowerCase() !== 'custom' || !isImportedCustomSourceType(playlist.sourceType)) {
      return res.status(400).json({ error: 'Only imported custom playlists can have their audience changed.' });
    }

    try {
      if (audience === 'global') {
        setCustomPlaylistAudience(db, userPlexId, playlistKey, 'global');
        const updatedPlaylist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
          .find((entry) => entry.playlistKey === playlistKey);
        if (updatedPlaylist) {
          setImmediate(async () => {
            await playlistService.syncGlobalCustomPlaylist(userPlexId, updatedPlaylist).catch(() => {});
          });
        }
      } else {
        setAllCopiesPlaylistAudience(db, playlistKey, 'personal');
      }
      pushLog({
        level: 'info',
        app: 'playlist',
        action: 'import.audience',
        message: `Set audience for imported playlist ${playlistKey} to "${audience}" by ${userPlexId}`,
      });
      return res.json({ ok: true, playlistKey, audience });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/playlist/job/dismiss', requireUser, (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    if (!userPlexId) return res.status(401).json({ error: 'Auth required.' });
    clearPlaylistJob(db, userPlexId);
    return res.json({ ok: true });
  });

  app.post('/api/music/playlists/daily-mix/sync', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    try {
      const config = loadConfig();
      const dailyMixConfig = config.smartPlaylist?.dailyMix || {};
      const result = await playlistService.syncDailyMix(userPlexId, {
        favoriteLimit: req.body?.favoriteLimit !== undefined ? Number(req.body.favoriteLimit) : dailyMixConfig.favoriteLimit,
        suggestedLimit: req.body?.suggestedLimit !== undefined ? Number(req.body.suggestedLimit) : dailyMixConfig.suggestedLimit,
        freshLimit: req.body?.freshLimit !== undefined ? Number(req.body.freshLimit) : dailyMixConfig.freshLimit,
        maxTracks: req.body?.maxTracks !== undefined ? Number(req.body.maxTracks) : dailyMixConfig.maxTracks,
        maxTracksPerArtist: req.body?.maxTracksPerArtist !== undefined ? Number(req.body.maxTracksPerArtist) : dailyMixConfig.maxTracksPerArtist,
        repeatCooldownDays: req.body?.repeatCooldownDays !== undefined ? Number(req.body.repeatCooldownDays) : dailyMixConfig.repeatCooldownDays,
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
    const userPlexId = resolveCanonicalUserId(req);
    const excludedKeys = getExcludedTrackKeys(db, userPlexId);
    return res.json({ ok: true, excludedTracks: excludedKeys });
  });

  // ── Playlist track removal ────────────────────────────────────────────────

  app.delete('/api/music/playlist/tracks', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
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
      if (ratingKey && playlistKey && generated && (['crescive', 'curative', 'curatorr', 'daily-mix'].includes(String(playlistKey)) || generated.playlistType === 'custom')) {
        removePlaylistTracks(db, userPlexId, playlistKey, [String(ratingKey)]);
      }
      return res.json({ ok: true });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Add all artist tracks to a playlist ──────────────────────────────────

  app.post('/api/music/playlist/tracks/add-artist', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
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

      if (playlistKey && generated && (['crescive', 'curative', 'curatorr', 'daily-mix'].includes(String(playlistKey)) || generated.playlistType === 'custom')) {
        addPlaylistTracks(db, userPlexId, playlistKey, artistTracks.map((t) => ({ ratingKey: t.ratingKey, artistName: t.artistName })));
      }
      return res.json({ ok: true, added: ratingKeys.length });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  // ── Import existing Plex playlists / collections ─────────────────────────

  app.get('/api/music/import/plex/sources', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const config = loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').trim().toLowerCase();
    if (msType !== 'plex') return res.status(400).json({ error: 'Plex import is only available when Plex is the active media server.' });
    try {
      const [playlists, collections] = await Promise.all([
        fetchPlexImportPlaylists(userPlexId),
        fetchPlexImportCollections(userPlexId),
      ]);
      return res.json({ ok: true, playlists, collections });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/import/plex', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const config = loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').trim().toLowerCase();
    if (msType !== 'plex') return res.status(400).json({ error: 'Plex import is only available when Plex is the active media server.' });

    const sourceType = String(req.body?.sourceType || '').trim().toLowerCase();
    const sourceId = String(req.body?.sourceId || '').trim();
    const sourceTitle = String(req.body?.sourceTitle || '').trim();
    const title = normaliseImportedPlaylistTitle(req.body?.title, sourceTitle || 'Imported Playlist');
    const makeGlobal = req.body?.audience === 'global' && isAdminRole(req);
    if (!['playlist', 'collection'].includes(sourceType)) return res.status(400).json({ error: 'sourceType must be playlist or collection.' });
    if (!sourceId) return res.status(400).json({ error: 'sourceId is required.' });

    try {
      const trackRefs = sourceType === 'playlist'
        ? await fetchPlexPlaylistImportTracks(userPlexId, sourceId)
        : await fetchPlexCollectionImportTracks(userPlexId, sourceId);
      if (!trackRefs.length) {
        return res.status(404).json({ error: `No tracks were found in that Plex ${sourceType}.` });
      }
      const playlist = await createImportedCustomPlaylist(userPlexId, title, trackRefs, {
        sourceType: `plex-${sourceType}`,
        sourceRef: sourceId,
        sourceTitle: sourceTitle || title,
      });
      if (makeGlobal && playlist) scheduleGlobalImportSync(userPlexId, playlist);
      return res.json({
        ok: true,
        playlist: playlist || null,
        importedTrackCount: trackRefs.length,
        audience: makeGlobal ? 'global' : 'personal',
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/import/spotify/playlists', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    try {
      const auth = await getSpotifyAuthForUser(userPlexId);
      const prefs = getUserPreferences(db, userPlexId);
      const spotifyUserId = String(prefs?.spotifyUserId || '').trim();
      const playlists = await spotifyService.listCurrentUserPlaylists(auth.accessToken, { limit: 50 });
      const ownedPlaylists = spotifyUserId
        ? playlists.filter((playlist) => String(playlist?.ownerId || '').trim() === spotifyUserId)
        : playlists;
      return res.json({ ok: true, playlists: ownedPlaylists });
    } catch (err) {
      return res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/import/spotify/preview', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    let playlistId = '';
    try {
      playlistId = resolveSpotifyPlaylistId(req.query?.playlistId || req.query?.playlistRef || '');
    } catch (err) {
      return res.status(Number(err?.status || 400)).json({ error: safeMessage(err) });
    }
    try {
      const forcePublic = req.query?.source === 'url';
      const auth = forcePublic ? null : await tryGetSpotifyAuthForUser(userPlexId);
      const playlistSource = auth
        ? await fetchSpotifyPlaylistImportSource(auth.accessToken, playlistId)
        : await fetchSpotifyPlaylistFromPublicPage(playlistId);
      const playlistMeta = playlistSource.playlistMeta;
      const playlistItems = playlistSource.playlistItems;
      const trackLookups = buildSpotifyTrackLookups(getMasterTracks(db));
      const matched = [];
      const unmatched = [];
      const duplicateMatches = [];
      const seenRatingKeys = new Set();
      for (const item of playlistItems.items || []) {
        const result = pickSpotifyTrackMatch(trackLookups, item);
        if (result.match?.ratingKey) {
          const summary = {
            position: Number(item.position || 0),
            ratingKey: String(result.match.ratingKey || ''),
            artistName: String(result.match.artistName || ''),
            trackTitle: String(result.match.trackTitle || item.title || ''),
            albumName: String(result.match.albumName || ''),
            spotifyTitle: String(item.title || ''),
            spotifyArtists: (Array.isArray(item.artists) ? item.artists : []).map((artist) => String(artist?.name || '').trim()).filter(Boolean),
            durationMs: Number(item.durationMs || 0),
            matchMethod: result.method,
          };
          if (seenRatingKeys.has(summary.ratingKey)) duplicateMatches.push(summary);
          else {
            seenRatingKeys.add(summary.ratingKey);
            matched.push(summary);
          }
        } else {
        unmatched.push({
          sourceTrackId: String(item?.id || '').trim(),
          position: Number(item.position || 0),
          title: String(item.title || ''),
          artistName: String((Array.isArray(item.artists) && item.artists[0]?.name) || '').trim(),
          artists: (Array.isArray(item.artists) ? item.artists : []).map((artist) => String(artist?.name || '').trim()).filter(Boolean),
          albumTitle: String(item?.album?.title || '').trim(),
          albumType: String(item?.album?.albumType || '').trim(),
          albumImageUrl: String(item?.album?.imageUrl || '').trim(),
          durationMs: Number(item.durationMs || 0),
        });
        }
      }
      const unmatchedArtists = buildSpotifyUnmatchedArtistGroups(unmatched, {
        groupLimit: 100,
        sampleLimit: 3,
      });
      return res.json({
        ok: true,
        playlist: playlistMeta,
        totalSourceTracks: Number(playlistItems.total || (playlistItems.items || []).length || 0),
        playlistTrackCount: Number(playlistSource.playlistTrackCount || playlistMeta?.trackCount || playlistItems.total || 0),
        matchedCount: matched.length,
        unmatchedCount: unmatched.length,
        unmatchedArtistCount: unmatchedArtists.length,
        duplicateCount: duplicateMatches.length,
        warning: String(playlistSource.warning || '').trim(),
        partial: playlistSource.partial === true,
        source: playlistSource.source || 'api',
        matched: matched.slice(0, 100),
        unmatchedArtists,
        unmatched: unmatched.slice(0, 100),
        duplicateMatches: duplicateMatches.slice(0, 100),
      });
    } catch (err) {
      return res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/import/youtube/preview', requireUser, async (req, res) => {
    let playlistId = '';
    try {
      playlistId = resolveYouTubePlaylistId(req.query?.playlistId || req.query?.playlistRef || '');
    } catch (err) {
      return res.status(Number(err?.status || 400)).json({ error: safeMessage(err) });
    }
    try {
      const playlistSource = await fetchYouTubePlaylistImportSource(playlistId);
      const playlistMeta = playlistSource.playlistMeta;
      const playlistItems = playlistSource.playlistItems;
      const trackLookups = buildSpotifyTrackLookups(getMasterTracks(db));
      const matched = [];
      const unmatched = [];
      const duplicateMatches = [];
      const seenRatingKeys = new Set();
      for (const item of playlistItems.items || []) {
        const result = pickYouTubeTrackMatch(trackLookups, item);
        if (result.match?.ratingKey) {
          const summary = {
            position: Number(item.position || 0),
            ratingKey: String(result.match.ratingKey || ''),
            artistName: String(result.match.artistName || ''),
            trackTitle: String(result.match.trackTitle || item.title || ''),
            albumName: String(result.match.albumName || ''),
            spotifyTitle: String(item.title || ''),
            spotifyArtists: (Array.isArray(item.artists) ? item.artists : []).map((artist) => String(artist?.name || '').trim()).filter(Boolean),
            durationMs: Number(item.durationMs || 0),
            matchMethod: result.method,
          };
          if (seenRatingKeys.has(summary.ratingKey)) duplicateMatches.push(summary);
          else {
            seenRatingKeys.add(summary.ratingKey);
            matched.push(summary);
          }
        } else {
          unmatched.push({
            sourceTrackId: String(item?.id || '').trim(),
            position: Number(item.position || 0),
            title: String(item.title || ''),
            artistName: String((Array.isArray(item.artists) && item.artists[0]?.name) || '').trim(),
            artists: (Array.isArray(item.artists) ? item.artists : []).map((artist) => String(artist?.name || '').trim()).filter(Boolean),
            albumTitle: '',
            albumType: '',
            albumImageUrl: String(item?.album?.imageUrl || '').trim(),
            durationMs: Number(item.durationMs || 0),
          });
        }
      }
      const unmatchedArtists = buildSpotifyUnmatchedArtistGroups(unmatched, {
        groupLimit: 100,
        sampleLimit: 3,
      });
      return res.json({
        ok: true,
        playlist: playlistMeta,
        totalSourceTracks: Number(playlistItems.total || (playlistItems.items || []).length || 0),
        playlistTrackCount: Number(playlistSource.playlistTrackCount || playlistMeta?.trackCount || playlistItems.total || 0),
        matchedCount: matched.length,
        unmatchedCount: unmatched.length,
        unmatchedArtistCount: unmatchedArtists.length,
        duplicateCount: duplicateMatches.length,
        warning: '',
        partial: false,
        source: playlistSource.source || 'api',
        matched: matched.slice(0, 100),
        unmatchedArtists,
        unmatched: unmatched.slice(0, 100),
        duplicateMatches: duplicateMatches.slice(0, 100),
      });
    } catch (err) {
      return res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/import/lastfm/preview', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const sourceKey = String(req.query?.sourceKey || '').trim();
    if (!sourceKey) return res.status(400).json({ error: 'sourceKey is required.' });
    try {
      const source = await fetchLastfmImportSource(userPlexId, sourceKey);
      const trackLookups = buildSpotifyTrackLookups(getMasterTracks(db));
      const preview = buildGenericImportPreview(source.items || [], (item) => pickSpotifyTrackMatch(trackLookups, item));
      return res.json({
        ok: true,
        playlist: {
          name: String(source.sourceTitle || 'Last.fm Playlist').trim(),
          ownerName: String(source.sourceOwner || '').trim(),
        },
        totalSourceTracks: Number((source.items || []).length || 0),
        playlistTrackCount: Number((source.items || []).length || 0),
        matchedCount: preview.matched.length,
        unmatchedCount: preview.unmatched.length,
        unmatchedArtistCount: preview.unmatchedArtists.length,
        duplicateCount: preview.duplicateMatches.length,
        warning: '',
        partial: false,
        source: 'api',
        matched: preview.matched.slice(0, 100),
        unmatchedArtists: preview.unmatchedArtists,
        unmatched: preview.unmatched.slice(0, 100),
        duplicateMatches: preview.duplicateMatches.slice(0, 100),
      });
    } catch (err) {
      return res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/import/listenbrainz/preview', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const sourceKey = String(req.query?.sourceKey || '').trim();
    if (!sourceKey) return res.status(400).json({ error: 'sourceKey is required.' });
    try {
      const source = await fetchListenbrainzImportSource(userPlexId, sourceKey);
      const trackLookups = buildListenbrainzTrackLookups(getMasterTracks(db));
      const previewItems = (Array.isArray(source.items) ? source.items : []).map((track, index) => ({
        id: String(track?.identifier?.[0] || track?.identifier || `${index}`),
        position: index + 1,
        title: String(track?.title || '').trim(),
        artists: [{ name: getListenbrainzTrackArtist(track) }],
        album: { title: String(track?.album || '').trim() },
        durationMs: Number(track?.duration || 0) > 0 ? Number(track.duration) : 0,
      }));
      const preview = buildGenericImportPreview(previewItems, (_item, index) => pickListenbrainzTrackMatch(trackLookups, source.items[index], index));
      return res.json({
        ok: true,
        playlist: {
          name: String(source.sourceTitle || 'ListenBrainz Playlist').trim(),
          ownerName: String(source.sourceOwner || '').trim(),
        },
        totalSourceTracks: Number((source.items || []).length || 0),
        playlistTrackCount: Number((source.items || []).length || 0),
        matchedCount: preview.matched.length,
        unmatchedCount: preview.unmatched.length,
        unmatchedArtistCount: preview.unmatchedArtists.length,
        duplicateCount: preview.duplicateMatches.length,
        warning: '',
        partial: false,
        source: 'api',
        matched: preview.matched.slice(0, 100),
        unmatchedArtists: preview.unmatchedArtists,
        unmatched: preview.unmatched.slice(0, 100),
        duplicateMatches: preview.duplicateMatches.slice(0, 100),
      });
    } catch (err) {
      return res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/import/spotify', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    let playlistId = '';
    const title = normaliseImportedPlaylistTitle(req.body?.title, 'Imported Spotify Playlist');
    const makeGlobal = req.body?.audience === 'global' && isAdminRole(req);
    try {
      playlistId = resolveSpotifyPlaylistId(req.body?.playlistId || req.body?.playlistRef || '');
    } catch (err) {
      return res.status(Number(err?.status || 400)).json({ error: safeMessage(err) });
    }
    try {
      const forcePublic = req.body?.source === 'url';
      const auth = forcePublic ? null : await tryGetSpotifyAuthForUser(userPlexId);
      const playlistSource = auth
        ? await fetchSpotifyPlaylistImportSource(auth.accessToken, playlistId)
        : await fetchSpotifyPlaylistFromPublicPage(playlistId);
      const playlistMeta = playlistSource.playlistMeta;
      const playlistItems = playlistSource.playlistItems;
      const trackLookups = buildSpotifyTrackLookups(getMasterTracks(db));
      const trackRefs = [];
      const unmatched = [];
      const seenRatingKeys = new Set();
      (playlistItems.items || []).forEach((item) => {
        const result = pickSpotifyTrackMatch(trackLookups, item);
        const ratingKey = String(result?.match?.ratingKey || '').trim();
        if (ratingKey && !seenRatingKeys.has(ratingKey)) {
          seenRatingKeys.add(ratingKey);
          trackRefs.push({
            ratingKey,
            artistName: String(result.match.artistName || '').trim(),
          });
          return;
        }
        if (result.match?.ratingKey) return;
        unmatched.push({
          sourceTrackId: String(item?.id || '').trim(),
          position: Number(item.position || 0),
          title: String(item.title || '').trim(),
          artistName: String((Array.isArray(item.artists) && item.artists[0]?.name) || '').trim(),
          artists: (Array.isArray(item.artists) ? item.artists : []).map((artist) => String(artist?.name || '').trim()).filter(Boolean),
          albumTitle: String(item?.album?.title || '').trim(),
          albumType: String(item?.album?.albumType || '').trim(),
          albumImageUrl: String(item?.album?.imageUrl || '').trim(),
          durationMs: Number(item.durationMs || 0),
        });
      });
      if (!trackRefs.length) return res.status(404).json({ error: 'No Spotify tracks matched your local library.' });
      const playlist = await createImportedCustomPlaylist(userPlexId, title, trackRefs, {
        sourceType: 'spotify-playlist',
        sourceRef: playlistId,
        sourceTitle: String(playlistMeta?.name || title).trim(),
        sourceOwner: String(playlistMeta?.ownerName || '').trim(),
        unmatchedTracks: unmatched,
      });
      if (makeGlobal && playlist) scheduleGlobalImportSync(userPlexId, playlist);
      return res.json({
        ok: true,
        playlist: playlist || null,
        importedTrackCount: trackRefs.length,
        unmatchedCount: unmatched.length,
        importedMissingCount: unmatched.length,
        audience: makeGlobal ? 'global' : 'personal',
        warning: String(playlistSource.warning || '').trim(),
        partial: playlistSource.partial === true,
      });
    } catch (err) {
      return res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/import/youtube', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    let playlistId = '';
    const title = normaliseImportedPlaylistTitle(req.body?.title, 'Imported YouTube Playlist');
    const makeGlobal = req.body?.audience === 'global' && isAdminRole(req);
    try {
      playlistId = resolveYouTubePlaylistId(req.body?.playlistId || req.body?.playlistRef || '');
    } catch (err) {
      return res.status(Number(err?.status || 400)).json({ error: safeMessage(err) });
    }
    try {
      const playlistSource = await fetchYouTubePlaylistImportSource(playlistId);
      const playlistMeta = playlistSource.playlistMeta;
      const playlistItems = playlistSource.playlistItems;
      const trackLookups = buildSpotifyTrackLookups(getMasterTracks(db));
      const matchResult = buildYouTubeImportMatchResult(playlistItems.items || [], trackLookups);
      if (!matchResult.trackRefs.length) return res.status(404).json({ error: 'No YouTube tracks matched your local library.' });
      const playlist = await createImportedCustomPlaylist(userPlexId, title, matchResult.trackRefs, {
        sourceType: 'youtube-playlist',
        sourceRef: playlistId,
        sourceTitle: String(playlistMeta?.name || title).trim(),
        sourceOwner: String(playlistMeta?.ownerName || '').trim(),
        unmatchedTracks: matchResult.unmatched,
      });
      if (makeGlobal && playlist) scheduleGlobalImportSync(userPlexId, playlist);
      return res.json({
        ok: true,
        playlist: playlist || null,
        importedTrackCount: matchResult.trackRefs.length,
        unmatchedCount: matchResult.unmatched.length,
        importedMissingCount: matchResult.unmatched.length,
        audience: makeGlobal ? 'global' : 'personal',
      });
    } catch (err) {
      return res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/import/lastfm', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    let sourceKey = '';
    const makeGlobal = req.body?.audience === 'global' && isAdminRole(req);
    try {
      sourceKey = normalizeLastfmImportSourceKey(req.body?.sourceKey || '');
    } catch (err) {
      return res.status(Number(err?.status || 400)).json({ error: safeMessage(err) });
    }
    try {
      const source = await fetchLastfmImportSource(userPlexId, sourceKey);
      if (!source.matchResult.trackRefs.length) {
        return res.status(404).json({ error: 'No Last.fm tracks matched your local library.' });
      }
      const title = normaliseImportedPlaylistTitle(req.body?.title, source.sourceTitle);
      const playlist = await createImportedCustomPlaylist(userPlexId, title, source.matchResult.trackRefs, {
        sourceType: source.sourceType,
        sourceRef: source.sourceKey,
        sourceTitle: source.sourceTitle,
        sourceOwner: source.sourceOwner,
        unmatchedTracks: source.matchResult.unmatched,
      });
      if (makeGlobal && playlist) scheduleGlobalImportSync(userPlexId, playlist);
      return res.json({
        ok: true,
        playlist: playlist || null,
        importedTrackCount: source.matchResult.trackRefs.length,
        unmatchedCount: source.matchResult.unmatched.length,
        importedMissingCount: source.matchResult.unmatched.length,
        audience: makeGlobal ? 'global' : 'personal',
      });
    } catch (err) {
      return res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/import/listenbrainz', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    let sourceKey = '';
    const makeGlobal = req.body?.audience === 'global' && isAdminRole(req);
    try {
      sourceKey = normalizeListenbrainzImportSourceKey(req.body?.sourceKey || '');
    } catch (err) {
      return res.status(Number(err?.status || 400)).json({ error: safeMessage(err) });
    }
    try {
      const source = await fetchListenbrainzImportSource(userPlexId, sourceKey);
      if (!source.matchResult.trackRefs.length) {
        return res.status(404).json({ error: 'No ListenBrainz tracks matched your local library.' });
      }
      const title = normaliseImportedPlaylistTitle(req.body?.title, source.sourceTitle);
      const playlist = await createImportedCustomPlaylist(userPlexId, title, source.matchResult.trackRefs, {
        sourceType: source.sourceType,
        sourceRef: source.sourceKey,
        sourceTitle: source.sourceTitle,
        sourceOwner: source.sourceOwner,
        unmatchedTracks: source.matchResult.unmatched,
      });
      if (makeGlobal && playlist) scheduleGlobalImportSync(userPlexId, playlist);
      return res.json({
        ok: true,
        playlist: playlist || null,
        importedTrackCount: source.matchResult.trackRefs.length,
        unmatchedCount: source.matchResult.unmatched.length,
        importedMissingCount: source.matchResult.unmatched.length,
        audience: makeGlobal ? 'global' : 'personal',
      });
    } catch (err) {
      return res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/playlists/imported-unmatched', requireUser, (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKey = String(req.query?.playlistKey || '').trim();
    if (!playlistKey) return res.status(400).json({ error: 'playlistKey is required.' });
    const playlist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => String(entry?.playlistKey || '') === playlistKey);
    if (!playlist) return res.status(404).json({ error: 'Playlist not found.' });
    const rows = listImportedPlaylistUnmatched(db, userPlexId, playlistKey);
    return res.json({
      ok: true,
      playlistKey,
      sourceType: String(playlist.sourceType || '').trim(),
      sourceTitle: String(playlist.sourceTitle || '').trim(),
      missingCount: Number(playlist.missingCount || rows.length || 0),
      selectedCount: rows.filter((row) => row.selected).length,
      rows,
    });
  });

  app.post('/api/music/playlists/imported-unmatched/selection', requireUser, (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKey = String(req.body?.playlistKey || '').trim();
    if (!playlistKey) return res.status(400).json({ error: 'playlistKey is required.' });
    const playlist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => String(entry?.playlistKey || '') === playlistKey);
    if (!playlist) return res.status(404).json({ error: 'Playlist not found.' });
    setImportedPlaylistUnmatchedSelection(db, userPlexId, playlistKey, {
      ids: Array.isArray(req.body?.ids) ? req.body.ids : [],
      selected: req.body?.selected !== false,
      artistName: String(req.body?.artistName || '').trim(),
      selectAll: req.body?.selectAll === true,
    });
    const rows = listImportedPlaylistUnmatched(db, userPlexId, playlistKey);
    return res.json({
      ok: true,
      playlistKey,
      missingCount: rows.length,
      selectedCount: rows.filter((row) => row.selected).length,
      rows,
    });
  });

  app.post('/api/music/playlists/imported-unmatched/lidarr-seed', requireUser, (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKey = String(req.body?.playlistKey || '').trim();
    if (!playlistKey) return res.status(400).json({ error: 'playlistKey is required.' });
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    if (!lidarrService?.isConfigured()) {
      return res.status(400).json({ error: 'Lidarr is not configured.' });
    }

    const playlist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => String(entry?.playlistKey || '') === playlistKey);
    if (!playlist) return res.status(404).json({ error: 'Playlist not found.' });

    const rows = listImportedPlaylistUnmatched(db, userPlexId, playlistKey);
    const selectedRows = rows.filter((row) => row.selected);
    if (!selectedRows.length) return res.status(400).json({ error: 'No selected missing tracks to send to Lidarr.' });

    const artistGroups = new Map();
    selectedRows.forEach((row) => {
      const artistName = String(row.artistName || (Array.isArray(row.artists) ? row.artists[0] : '') || '').trim();
      if (!artistName) return;
      if (!artistGroups.has(artistName)) artistGroups.set(artistName, []);
      artistGroups.get(artistName).push(row);
    });

    const now = Date.now();
    let queuedArtists = 0;
    let preservedArtists = 0;
    for (const [artistName, artistRows] of artistGroups.entries()) {
      const existing = getSuggestedArtist(db, userPlexId, artistName);
      const selectedTrackCount = artistRows.length;
      const sampleTracks = artistRows
        .map((row) => String(row.title || '').trim())
        .filter(Boolean)
        .slice(0, 5);
      const preservedStatus = new Set(['queued_for_lidarr', 'added_to_lidarr', 'already_monitored']).has(String(existing?.status || '').trim())
        ? String(existing.status).trim()
        : 'suggested';

      upsertSuggestedArtist(db, userPlexId, {
        artistName,
        source: 'imported_playlist_missing',
        similarityScore: Number(existing?.similarityScore || 0),
        behaviorScore: Number(existing?.behaviorScore || 0),
        editorialScore: Math.max(Number(existing?.editorialScore || 0), 2),
        totalScore: Math.max(Number(existing?.totalScore || 0), Math.min(10, 7 + Math.min(2, selectedTrackCount * 0.25))),
        status: preservedStatus,
        lidarrArtistId: existing?.lidarrArtistId ?? null,
        firstSuggestedAt: Number(existing?.firstSuggestedAt || now),
        lastEvaluatedAt: now,
        acceptedAt: existing?.acceptedAt ?? null,
        dismissedAt: preservedStatus === 'suggested' ? null : (existing?.dismissedAt ?? null),
        reason: {
          type: 'imported_playlist_missing',
          playlistKey,
          playlistTitle: String(playlist.playlistTitle || playlist.playlistKey || '').trim(),
          sourceType: String(playlist.sourceType || '').trim(),
          sourceTitle: String(playlist.sourceTitle || '').trim(),
          sourceOwner: String(playlist.sourceOwner || '').trim(),
          selectedTrackCount,
          sampleTracks,
        },
      });

      if (preservedStatus === 'suggested') queuedArtists += 1;
      else preservedArtists += 1;
    }

    setImportedPlaylistUnmatchedSelection(db, userPlexId, playlistKey, {
      ids: selectedRows.map((row) => Number(row.id || 0)).filter(Boolean),
      selected: false,
    });
    const updatedRows = listImportedPlaylistUnmatched(db, userPlexId, playlistKey);

    return res.json({
      ok: true,
      playlistKey,
      missingCount: updatedRows.length,
      selectedCount: updatedRows.filter((row) => row.selected).length,
      rows: updatedRows,
      queuedArtists,
      preservedArtists,
      message: queuedArtists
        ? `Queued ${queuedArtists.toLocaleString()} artist${queuedArtists === 1 ? '' : 's'} for Lidarr review.`
        : `Updated ${preservedArtists.toLocaleString()} artist${preservedArtists === 1 ? '' : 's'} already in the Lidarr pipeline.`,
    });
  });

  app.post('/api/music/playlists/imported-unmatched/lidarr-queue', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKey = String(req.body?.playlistKey || '').trim();
    if (!playlistKey) return res.status(400).json({ error: 'playlistKey is required.' });
    if (!canUserAccessLidarrAutomation(loadConfig(), req.session?.user)) {
      return res.status(403).json({ error: 'Lidarr automation is not enabled for this account.' });
    }
    if (!lidarrService?.isConfigured()) {
      return res.status(400).json({ error: 'Lidarr is not configured.' });
    }

    const playlist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((entry) => String(entry?.playlistKey || '') === playlistKey);
    if (!playlist) return res.status(404).json({ error: 'Playlist not found.' });

    const rows = listImportedPlaylistUnmatched(db, userPlexId, playlistKey);
    const selectedRows = rows.filter((row) => row.selected);
    if (!selectedRows.length) return res.status(400).json({ error: 'No selected missing tracks to add to the Lidarr queue.' });

    const artistGroups = new Map();
    selectedRows.forEach((row) => {
      const artistName = String(row.artistName || (Array.isArray(row.artists) ? row.artists[0] : '') || '').trim();
      if (!artistName) return;
      if (!artistGroups.has(artistName)) artistGroups.set(artistName, []);
      artistGroups.get(artistName).push(row);
    });

    const now = Date.now();
    const requests = [];
    let queuedAlbumRequests = 0;
    let queuedArtistFallbacks = 0;
    for (const [artistName, artistRows] of artistGroups.entries()) {
      const existing = getSuggestedArtist(db, userPlexId, artistName);
      const selectedTrackCount = artistRows.length;
      const sampleTracks = artistRows
        .map((row) => String(row.title || '').trim())
        .filter(Boolean)
        .slice(0, 5);

      upsertSuggestedArtist(db, userPlexId, {
        artistName,
        source: 'imported_playlist_missing',
        similarityScore: Number(existing?.similarityScore || 0),
        behaviorScore: Number(existing?.behaviorScore || 0),
        editorialScore: Math.max(Number(existing?.editorialScore || 0), 2),
        totalScore: Math.max(Number(existing?.totalScore || 0), Math.min(10, 7 + Math.min(2, selectedTrackCount * 0.25))),
        status: 'suggested',
        lidarrArtistId: existing?.lidarrArtistId ?? null,
        firstSuggestedAt: Number(existing?.firstSuggestedAt || now),
        lastEvaluatedAt: now,
        acceptedAt: existing?.acceptedAt ?? null,
        dismissedAt: null,
        reason: {
          type: 'imported_playlist_missing',
          playlistKey,
          playlistTitle: String(playlist.playlistTitle || playlist.playlistKey || '').trim(),
          sourceType: String(playlist.sourceType || '').trim(),
          sourceTitle: String(playlist.sourceTitle || '').trim(),
          sourceOwner: String(playlist.sourceOwner || '').trim(),
          selectedTrackCount,
          sampleTracks,
        },
      });

      const albumGroups = new Map();
      artistRows.forEach((row) => {
        const albumTitle = String(row.albumTitle || '').trim();
        const key = albumTitle.toLowerCase();
        if (!albumGroups.has(key)) {
          albumGroups.set(key, {
            albumTitle,
            albumImageUrl: String(row.albumImageUrl || '').trim(),
            rows: [],
          });
        }
        if (!albumGroups.get(key).albumImageUrl) albumGroups.get(key).albumImageUrl = String(row.albumImageUrl || '').trim();
        albumGroups.get(key).rows.push(row);
      });

      for (const group of albumGroups.values()) {
        const preferredAlbumTitle = String(group.albumTitle || '').trim();
        const request = await lidarrService.queueArtistAlbumRequest({
          userPlexId,
          artistName,
          preferredAlbumTitle,
          preferredAlbumImageUrl: String(group.albumImageUrl || '').trim(),
          sourceKind: 'manual',
          allowCuratorrFallback: true,
          note: `Queued from imported playlist missing tracks: ${String(playlist.playlistTitle || playlist.playlistKey || '').trim()}`,
        });
        requests.push(request);
        if (preferredAlbumTitle) queuedAlbumRequests += 1;
        else queuedArtistFallbacks += 1;
      }
    }

    setImportedPlaylistUnmatchedSelection(db, userPlexId, playlistKey, {
      ids: selectedRows.map((row) => Number(row.id || 0)).filter(Boolean),
      selected: false,
    });
    const updatedRows = listImportedPlaylistUnmatched(db, userPlexId, playlistKey);

    return res.json({
      ok: true,
      playlistKey,
      missingCount: updatedRows.length,
      selectedCount: updatedRows.filter((row) => row.selected).length,
      rows: updatedRows,
      queuedArtists: requests.length,
      queuedAlbumRequests,
      queuedArtistFallbacks,
      requestIds: requests.map((request) => request?.id).filter(Boolean),
      message: queuedAlbumRequests > 0 && queuedArtistFallbacks > 0
        ? `Queued ${queuedAlbumRequests.toLocaleString()} album request${queuedAlbumRequests === 1 ? '' : 's'} and ${queuedArtistFallbacks.toLocaleString()} artist fallback${queuedArtistFallbacks === 1 ? '' : 's'} in Lidarr.`
        : (queuedAlbumRequests > 0
          ? `Queued ${queuedAlbumRequests.toLocaleString()} album request${queuedAlbumRequests === 1 ? '' : 's'} in Lidarr.`
          : `Queued ${queuedArtistFallbacks.toLocaleString()} artist fallback${queuedArtistFallbacks === 1 ? '' : 's'} in Lidarr.`),
    });
  });

  // ── Create a custom playlist ──────────────────────────────────────────────

  app.post('/api/music/playlists/custom', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
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
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    if (msType === 'jellyfin' || msType === 'emby') {
      const { url, apiKey } = config[msType] || {};
      if (!url || !apiKey) return res.status(404).end();
      const key = req.params.key;
      try {
        const imgUrl = new URL(`/Items/${encodeURIComponent(key)}/Images/Primary`, url);
        imgUrl.searchParams.set('maxWidth', '600');
        const ir = await fetch(imgUrl.toString(), { headers: { 'X-Emby-Token': apiKey }, signal: AbortSignal.timeout(8000) });
        if (!ir.ok) return res.status(404).end();
        const buf = await ir.arrayBuffer();
        res.set('Content-Type', ir.headers.get('Content-Type') || 'image/jpeg');
        res.set('Cache-Control', 'public, max-age=86400');
        return res.send(Buffer.from(buf));
      } catch (_) { return res.status(404).end(); }
    }
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
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    const artistName = decodeURIComponent(req.params.name);
    const cacheKey = `artist:${normalizeArtistMatchText(artistName)}`;
    const cached = getThumbCache(cacheKey);
    if (cached) return sendCachedThumbResponse(res, cached);

    if (msType === 'jellyfin' || msType === 'emby') {
      const { url: msUrl, apiKey } = config[msType] || {};
      if (msUrl && apiKey) {
        try {
          // Search Jellyfin for the artist by name
          const searchUrl = new URL('/Items', msUrl);
          searchUrl.searchParams.set('IncludeItemTypes', 'MusicArtist');
          searchUrl.searchParams.set('SearchTerm', artistName);
          searchUrl.searchParams.set('Recursive', 'true');
          searchUrl.searchParams.set('Limit', '5');
          const searchRes = await fetch(searchUrl.toString(), { headers: { 'X-Emby-Token': apiKey }, signal: AbortSignal.timeout(8000) });
          if (searchRes.ok) {
            const searchJson = await searchRes.json();
            const artists = Array.isArray(searchJson?.Items) ? searchJson.Items : [];
            const match = artists.find((a) => artistNamesMatch(a.Name, artistName)) || artists[0];
            if (match?.Id) {
              const imgUrl = new URL(`/Items/${encodeURIComponent(match.Id)}/Images/Primary`, msUrl);
              imgUrl.searchParams.set('maxWidth', '600');
              const ir = await fetch(imgUrl.toString(), { headers: { 'X-Emby-Token': apiKey }, signal: AbortSignal.timeout(8000) });
              if (ir.ok) {
                const buf = await ir.arrayBuffer();
                return sendThumbImage(res, cacheKey, ir.headers.get('Content-Type') || 'image/jpeg', Buffer.from(buf));
              }
            }
          }
          // Fall back: use album art from a track by this artist
          const trackRow = db.prepare('SELECT rating_key FROM master_tracks WHERE artist_name = ? LIMIT 1').get(artistName);
          if (trackRow?.rating_key) {
            const imgUrl = new URL(`/Items/${encodeURIComponent(trackRow.rating_key)}/Images/Primary`, msUrl);
            imgUrl.searchParams.set('maxWidth', '600');
            const ir = await fetch(imgUrl.toString(), { headers: { 'X-Emby-Token': apiKey }, signal: AbortSignal.timeout(8000) });
            if (ir.ok) {
              const buf = await ir.arrayBuffer();
              return sendThumbImage(res, cacheKey, ir.headers.get('Content-Type') || 'image/jpeg', Buffer.from(buf));
            }
          }
        } catch (_) { /* fall through to Deezer */ }
      }
      // Deezer fallback for Jellyfin when no local art found
      try {
        const deezerUrl = new URL('https://api.deezer.com/search/artist');
        deezerUrl.searchParams.set('q', artistName);
        const deezerRes = await fetch(deezerUrl.toString(), { headers: { Accept: 'application/json', 'User-Agent': 'Curatorr/1.0' } });
        if (deezerRes.ok) {
          const deezerJson = await deezerRes.json();
          const candidates = Array.isArray(deezerJson?.data) ? deezerJson.data : [];
          const picked = candidates.find((item) => artistNamesMatch(item?.name, artistName)) || candidates[0];
          const remoteThumb = picked?.picture_big || picked?.picture_medium || picked?.picture;
          if (remoteThumb) {
            const ir = await fetch(remoteThumb, { headers: { Accept: 'image/*', 'User-Agent': 'Curatorr/1.0' } });
            if (ir.ok) {
              const buf = await ir.arrayBuffer();
              return sendThumbImage(res, cacheKey, ir.headers.get('Content-Type') || 'image/jpeg', Buffer.from(buf));
            }
          }
        }
      } catch (_) { /* ignore */ }
      return sendThumbNotFound(res, cacheKey);
    }

    const { url, token, libraries: selectedKeys = [] } = config.plex || {};
    if (!url || !token) return res.status(404).end();
    const base = url.replace(/\/$/, '');
    const trackRowsForArtist = db.prepare(`
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
    `);
    const trackRows = [];
    const seenTrackKeys = new Set();
    for (const candidate of getArtistLookupTerms(artistName)) {
      for (const row of trackRowsForArtist.all(candidate)) {
        const key = String(row?.rating_key || '').trim();
        if (!key || seenTrackKeys.has(key)) continue;
        seenTrackKeys.add(key);
        trackRows.push(row);
      }
      if (trackRows.length >= 12) break;
    }
    if (!trackRows.length) {
      // Artist not in library yet — try Deezer for queue/discovery artwork
      try {
        const deezerUrl = new URL('https://api.deezer.com/search/artist');
        deezerUrl.searchParams.set('q', artistName);
        const deezerRes = await fetch(deezerUrl.toString(), {
          headers: { Accept: 'application/json', 'User-Agent': 'Curatorr/1.0' },
        });
        if (deezerRes.ok) {
          const deezerJson = await deezerRes.json();
          const candidates = Array.isArray(deezerJson?.data) ? deezerJson.data : [];
          const picked = candidates.find((item) => artistNamesMatch(item?.name, artistName))
            || candidates.find((item) => getArtistLookupTerms(artistName).some((term) => normalizeArtistMatchText(item?.name).startsWith(normalizeArtistMatchText(term))))
            || candidates[0];
          const remoteThumb = picked?.picture_big || picked?.picture_medium || picked?.picture;
          if (remoteThumb) {
            const ir = await fetch(remoteThumb, { headers: { Accept: 'image/*', 'User-Agent': 'Curatorr/1.0' } });
            if (ir.ok) {
              const buf = await ir.arrayBuffer();
              return sendThumbImage(res, cacheKey, ir.headers.get('Content-Type') || 'image/jpeg', Buffer.from(buf));
            }
          }
        }
      } catch (_) {
        // Ignore and fall through to 404
      }
      return sendThumbNotFound(res, cacheKey);
    }

    try {
      for (const searchTerm of getArtistLookupTerms(artistName)) {
        for (const key of selectedKeys) {
          const searchUrl = buildAppApiUrl(url, `library/sections/${key}/all`);
          searchUrl.searchParams.set('type', '8');
          searchUrl.searchParams.set('title', searchTerm);
          const searchRes = await fetch(searchUrl.toString(), {
            headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
          });
          if (!searchRes.ok) continue;
          const searchJson = await searchRes.json();
          const artistMeta = (searchJson?.MediaContainer?.Metadata || []).find((item) => {
            return artistNamesMatch(item?.title, artistName);
          });
          const artistThumb = artistMeta?.thumb || artistMeta?.art;
          if (!artistThumb) continue;
          const ir = await fetch(`${base}${artistThumb}`, {
            headers: buildPlexAuthHeaders(token, { Accept: 'image/*,*/*' }),
          });
          if (!ir.ok) continue;
          const buf = await ir.arrayBuffer();
          return sendThumbImage(res, cacheKey, ir.headers.get('Content-Type') || 'image/jpeg', Buffer.from(buf));
        }
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
            const artistThumb = artistMeta?.thumb || artistMeta?.art;
            if (artistThumb && (!artistMeta?.title || artistNamesMatch(artistMeta?.title, artistName))) {
              const ir = await fetch(`${base}${artistThumb}`, {
                headers: buildPlexAuthHeaders(token, { Accept: 'image/*,*/*' }),
              });
              if (!ir.ok) continue;
              const buf = await ir.arrayBuffer();
              return sendThumbImage(res, cacheKey, ir.headers.get('Content-Type') || 'image/jpeg', Buffer.from(buf));
            }
          }
        }

        const fallbackThumb = trackMeta?.grandparentThumb;
        if (fallbackThumb && artistNamesMatch(trackMeta?.grandparentTitle, artistName)) {
          const ir = await fetch(`${base}${fallbackThumb}`, {
            headers: buildPlexAuthHeaders(token, { Accept: 'image/*,*/*' }),
          });
          if (!ir.ok) continue;
          const buf = await ir.arrayBuffer();
          return sendThumbImage(res, cacheKey, ir.headers.get('Content-Type') || 'image/jpeg', Buffer.from(buf));
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
          const picked = candidates.find((item) => artistNamesMatch(item?.name, artistName))
            || candidates.find((item) => getArtistLookupTerms(artistName).some((term) => normalizeArtistMatchText(item?.name).startsWith(normalizeArtistMatchText(term))))
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
              return sendThumbImage(res, cacheKey, ir.headers.get('Content-Type') || 'image/jpeg', Buffer.from(buf));
            }
          }
        }
      } catch (_) {
        // Ignore external fallback failures and keep the endpoint safe.
      }

      return sendThumbNotFound(res, cacheKey);
    } catch (_) {
      return sendThumbNotFound(res, cacheKey);
    }
  });

  app.get('/api/music/thumb/album', async (req, res) => {
    const config = loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    const artistName = String(req.query?.artist || '').trim();
    const albumName = String(req.query?.album || '').trim();
    const noArtistFallback = /^(1|true|yes)$/i.test(String(req.query?.noArtistFallback || '').trim());
    if (!artistName || !albumName) return res.status(404).end();
    const cacheKey = `album:${normalizeArtistMatchText(artistName)}::${normalizeAlbumMatchText(albumName)}`;
    const cached = getThumbCache(cacheKey);
    if (cached) return sendCachedThumbResponse(res, cached);

    if (msType === 'jellyfin' || msType === 'emby') {
      const { url: msUrl, apiKey } = config[msType] || {};
      const fallbackArtistLocation = `/api/music/thumb/artist/${encodeURIComponent(artistName)}?v=jf-album-fallback-1`;
      const fallbackThumb = () => (noArtistFallback
        ? sendThumbNotFound(res, cacheKey)
        : sendThumbRedirect(res, cacheKey, fallbackArtistLocation));
      if (msUrl && apiKey) {
        try {
          const trackRows = db.prepare('SELECT rating_key, album_name FROM master_tracks WHERE artist_name = ? LIMIT 500').all(artistName);
          const wanted = normalizeAlbumMatchText(albumName);
          const trackRow = trackRows.find((r) => normalizeAlbumMatchText(r?.album_name) === wanted)
            || trackRows.find((r) => normalizeAlbumMatchText(r?.album_name).includes(wanted) || wanted.includes(normalizeAlbumMatchText(r?.album_name)))
            || trackRows[0];
          if (trackRow?.rating_key) {
            const imgUrl = new URL(`/Items/${encodeURIComponent(trackRow.rating_key)}/Images/Primary`, msUrl);
            imgUrl.searchParams.set('maxWidth', '600');
            const ir = await fetch(imgUrl.toString(), { headers: { 'X-Emby-Token': apiKey }, signal: AbortSignal.timeout(8000) });
            if (ir.ok) {
              const buf = await ir.arrayBuffer();
              return sendThumbImage(res, cacheKey, ir.headers.get('Content-Type') || 'image/jpeg', Buffer.from(buf));
            }
          }
        } catch (_) { /* fall through */ }
      }
      return fallbackThumb();
    }

    const { url, token } = config.plex || {};
    if (!url || !token) return res.status(404).end();
    const base = url.replace(/\/$/, '');
    const fallbackArtistLocation = `/api/music/thumb/artist/${encodeURIComponent(artistName)}?v=discover-album-fallback-1`;
    const fallbackArtistThumb = () => (noArtistFallback
      ? sendThumbNotFound(res, cacheKey)
      : sendThumbRedirect(res, cacheKey, fallbackArtistLocation));
    const trackRows = db.prepare(
      'SELECT rating_key, album_name FROM master_tracks WHERE artist_name = ? LIMIT 500',
    ).all(artistName);
    const lidarrDbFallback = async () => {
      try {
        const lidarrDbPath = path.join(process.env.DATA_DIR || '/app/data', 'lidarr.db');
        const ldb = new Database(lidarrDbPath, { readonly: true, fileMustExist: true });
        const wanted = normalizeAlbumMatchText(albumName);
        const artistRow = ldb.prepare('SELECT Id FROM ArtistMetadata WHERE Name = ? COLLATE NOCASE LIMIT 1').get(artistName);
        if (!artistRow) { ldb.close(); return fallbackArtistThumb(); }
        const albumRows = ldb.prepare('SELECT Title, Images FROM Albums WHERE ArtistMetadataId = ?').all(artistRow.Id);
        ldb.close();
        const matched = albumRows.find((r) => normalizeAlbumMatchText(r.Title) === wanted)
          || albumRows.find((r) => normalizeAlbumMatchText(r.Title).includes(wanted) || wanted.includes(normalizeAlbumMatchText(r.Title)))
          || null;
        if (!matched) return fallbackArtistThumb();
        const images = JSON.parse(matched.Images || '[]');
        const coverUrl = (images.find((i) => i?.coverType === 'cover') || images[0])?.url || '';
        if (!coverUrl) return fallbackArtistThumb();
        const ir = await fetch(coverUrl, { headers: { Accept: 'image/*,*/*' } });
        if (!ir.ok) return fallbackArtistThumb();
        const buf = await ir.arrayBuffer();
        return sendThumbImage(res, cacheKey, ir.headers.get('Content-Type') || 'image/jpeg', Buffer.from(buf));
      } catch (_) {
        return fallbackArtistThumb();
      }
    };
    if (!trackRows.length) return lidarrDbFallback();
    const wanted = normalizeAlbumMatchText(albumName);
    const trackRow = trackRows.find((row) => normalizeAlbumMatchText(row?.album_name) === wanted)
      || trackRows.find((row) => normalizeAlbumMatchText(row?.album_name).startsWith(wanted))
      || trackRows.find((row) => normalizeAlbumMatchText(row?.album_name).includes(wanted) || wanted.includes(normalizeAlbumMatchText(row?.album_name)))
      || null;
    if (!trackRow?.rating_key) return lidarrDbFallback();

    try {
      const mr = await fetch(`${base}/library/metadata/${encodeURIComponent(trackRow.rating_key)}`, {
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
      if (!mr.ok) return fallbackArtistThumb();
      const meta = await mr.json();
      const trackMeta = (meta?.MediaContainer?.Metadata || [])[0];
      const thumb = trackMeta?.parentThumb || trackMeta?.thumb;
      if (!thumb) return fallbackArtistThumb();

      const ir = await fetch(`${base}${thumb}`, {
        headers: buildPlexAuthHeaders(token, { Accept: 'image/*,*/*' }),
      });
      if (!ir.ok) return fallbackArtistThumb();
      const buf = await ir.arrayBuffer();
      return sendThumbImage(res, cacheKey, ir.headers.get('Content-Type') || 'image/jpeg', Buffer.from(buf));
    } catch (_) {
      return fallbackArtistThumb();
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

  app.get('/api/music/lidarr/thumb/album/:id', requireUser, async (req, res) => {
    const albumId = Number(req.params.id || 0) || 0;
    if (!albumId || !lidarrService?.isConfigured()) return res.status(404).end();
    const cacheKey = `lidarr-album:${albumId}`;
    const cached = getThumbCache(cacheKey);
    if (cached) return sendCachedThumbResponse(res, cached);

    try {
      const album = await lidarrService.getAlbum(albumId, { timeoutMs: 12000 });
      if (!album) return sendThumbNotFound(res, cacheKey);

      const imagePath = String(
        album?.images?.find((img) => String(img?.coverType || '').trim().toLowerCase() === 'cover')?.url
        || album?.images?.find((img) => /cover/i.test(String(img?.coverType || '')))?.url
        || album?.images?.[0]?.url
        || album?.imagePath
        || ''
      ).trim();
      if (isAllowedLidarrImagePath(imagePath)) {
        return sendThumbRedirect(res, cacheKey, `/api/music/lidarr/image?path=${encodeURIComponent(imagePath)}`);
      }

      const directUrl = String(album?.imageUrl || '').trim();
      if (directUrl) {
        const upstream = await fetch(directUrl, { headers: { Accept: 'image/*,*/*' } });
        if (upstream.ok) {
          const buf = await upstream.arrayBuffer();
          return sendThumbImage(res, cacheKey, upstream.headers.get('Content-Type') || 'image/jpeg', Buffer.from(buf), 'public, max-age=3600');
        }
      }

      const foreignAlbumId = String(album?.foreignAlbumId || '').trim();
      if (foreignAlbumId) {
        return sendThumbRedirect(res, cacheKey, `/api/music/cover/release-group/${encodeURIComponent(foreignAlbumId)}`);
      }

      return sendThumbNotFound(res, cacheKey);
    } catch (_err) {
      return sendThumbNotFound(res, cacheKey);
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
    const cacheVersion = String(req.query?.v || DISCOVERY_ART_URL_VERSION).trim() || DISCOVERY_ART_URL_VERSION;
    const cacheKey = `${cacheVersion}:${artistName.toLowerCase()}`;
    const cached = getDiscoveryArtCache(cacheKey);
    if (cached) {
      if (cached.kind === 'image') {
        res.set('Content-Type', cached.contentType || 'image/jpeg');
        res.set('Cache-Control', 'public, max-age=21600');
        return res.send(cached.buffer);
      }
      if (cached.kind === 'redirect') {
        return res.redirect(302, cached.location);
      }
    }

    try {
      const deezerThumb = await lookupDeezerArtistArtUrl(artistName);
      if (deezerThumb) {
        setDiscoveryArtCache(cacheKey, {
          kind: 'redirect',
          location: deezerThumb,
          expiresAt: Date.now() + DISCOVERY_ART_CACHE_TTL_MS,
        });
        res.set('Cache-Control', 'public, max-age=21600');
        return res.redirect(302, deezerThumb);
      }
    } catch (_) {
      // Ignore and fall through.
    }

    try {
      const lastfmThumb = await lookupLastfmArtistArtUrl(artistName, disc.lastfmApiKey);
      if (lastfmThumb) {
        setDiscoveryArtCache(cacheKey, {
          kind: 'redirect',
          location: lastfmThumb,
          expiresAt: Date.now() + DISCOVERY_ART_CACHE_TTL_MS,
        });
        res.set('Cache-Control', 'public, max-age=21600');
        return res.redirect(302, lastfmThumb);
      }
    } catch (_) {
      // Ignore and fall through.
    }

    const fallbackLocation = `/api/music/thumb/artist/${encodeURIComponent(artistName)}?v=discover-artist-fallback-1`;
    setDiscoveryArtCache(cacheKey, {
      kind: 'redirect',
      location: fallbackLocation,
      expiresAt: Date.now() + THUMB_NEGATIVE_CACHE_TTL_MS,
    });
    return res.redirect(302, fallbackLocation);
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
      const r = await fetch(url.toString(), { headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(20000) });
      if (!r.ok) return res.status(502).json({ ok: false, error: 'Last.fm upstream error.' });
      const data = await r.json();
      let rawItems;
      if (type === 'artists') {
        rawItems = (data?.topartists?.artist || []).map((a) => ({
          name: a.name,
          listeners: Number(a.listeners || 0),
        }));
      } else {
        rawItems = (data?.tracks?.track || []).map((t) => ({
          name: t.name,
          artistName: t.artist?.name || '',
          listeners: Number(t.listeners || 0),
        }));
      }
      const items = rawItems.map((item) => ({
        ...item,
        image: buildDiscoveryArtistArtUrl(item.artistName || item.name),
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
    const userPlexId = resolveSuggestionUserId(req);
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
        return fetch(u.toString(), { headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(20000) })
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
            image: buildDiscoveryArtistArtUrl(a.name),
          });
        }
      }
      items.sort((a, b) => b.match - a.match);
      const top20 = items.slice(0, 20);
      return res.json({ ok: true, items: top20, basedOn: seedArtists });
    } catch (err) {
      return res.status(502).json({ ok: false, error: 'Failed to fetch similar artists.' });
    }
  });

  // ── Personal playlists ────────────────────────────────────────────────────

  function makePersonalPlaylistId() {
    return 'pp_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
  }

  // GET /api/music/playlists/personal/preview — must be before /:id to avoid route shadowing
  app.get('/api/music/playlists/personal/preview', requireUser, (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    let rules;
    try { rules = JSON.parse(String(req.query?.rules || '{}')); } catch { return res.status(400).json({ error: 'Invalid rules JSON' }); }
    let trackFilters;
    try { trackFilters = req.query?.trackFilters ? normaliseTrackFiltersInput(JSON.parse(String(req.query.trackFilters))) : null; } catch { trackFilters = null; }
    const config = loadConfig();
    const smartSettings = config.smartPlaylist || {};
    const preview = buildPlaylistPreviewSnapshot(db, userPlexId, rules, trackFilters, smartSettings, {
      dedupeReportLimit: Number(req.query?.dedupeReportLimit || 20),
    });
    const counts = preview.counts;
    res.json({
      ok: true,
      artistCount: counts.artistCount,
      trackCount: counts.trackCount,
      eligibleArtistCount: counts.eligibleArtistCount,
      eligibleTrackCount: counts.eligibleTrackCount,
      featureTrackCount: preview.featureTrackCount,
      dedupeDuplicateCount: preview.dedupeDuplicateCount,
      dedupeDuplicateMatches: preview.dedupeDuplicateMatches,
    });
  });

  // GET /api/music/playlists/personal/:id — must be after /preview to avoid route shadowing
  app.get('/api/music/playlists/personal/:id', requireUser, (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const id = String(req.params.id || '');
    const playlist = getUserPersonalPlaylist(db, id, userPlexId);
    if (!playlist) return res.status(404).json({ error: 'Not found' });
    res.json({ ok: true, playlist });
  });

  // POST /api/music/playlists/personal
  app.post('/api/music/playlists/personal', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const name = String(req.body?.name || '').trim();
    if (!name) return res.status(400).json({ error: 'Name is required' });
    if (findUserPersonalPlaylistByName(db, userPlexId, name)) {
      return res.status(409).json({ error: 'You already have a personal playlist with that name.' });
    }
    const BLEND_MODES = ['average', 'intersection', 'union', 'veto'];
    const REBUILD_SCHEDULES = ['daily', 'weekly', 'manual'];
    const blendUsers = Array.isArray(req.body?.blendUsers) ? req.body.blendUsers.filter(Boolean) : [];
    const trackFilters = normaliseTrackFiltersInput(req.body?.trackFilters);
    let artwork;
    try {
      artwork = parsePlaylistArtworkInput(req.body?.artwork, { mode: 'auto', customArtworkAsset: '', preservedArtworkAsset: '' });
    } catch (err) {
      return res.status(400).json({ error: safeMessage(err) });
    }
    const importedSource = sanitizeImportedSourceInput(req.body?.importSource);
    const importedSuggestedContent = sanitizeImportedContentSetInput(req.body?.importSuggestedContent);
    const importedDetectedContent = sanitizeImportedContentSetInput(req.body?.importDetectedContent);
    const rules = {
      artistTiers:     normaliseTriStateInput(req.body?.artistTiers),
      trackTiers:      normaliseTriStateInput(req.body?.trackTiers),
      genres:          normaliseTriStateInput(req.body?.genres),
      moods:           normaliseTriStateInput(req.body?.moods),
      albumGenres:     normaliseTriStateInput(req.body?.albumGenres),
      albumStyles:     normaliseTriStateInput(req.body?.albumStyles),
      albumMoods:      normaliseTriStateInput(req.body?.albumMoods),
      tags:            normaliseTriStateInput(req.body?.tags),
      decades:         normaliseTriStateInput(req.body?.decades),
      topNPerArtist:   req.body?.topNPerArtist ? Number(req.body.topNPerArtist) : null,
      maxTracksPerAlbum: req.body?.maxTracksPerAlbum ? Number(req.body.maxTracksPerAlbum) : null,
      maxTracks:       req.body?.maxTracks     ? Number(req.body.maxTracks)     : null,
      sortBy:          PLAYLIST_SORT_VALUES.includes(String(req.body?.sortBy || '').trim()) ? String(req.body.sortBy).trim() : 'ratingCount',
      finalOrdering:   PLAYLIST_FINAL_ORDERING_VALUES.includes(String(req.body?.finalOrdering || '').trim()) ? String(req.body.finalOrdering).trim() : 'none',
      blendUsers,
      blendMode:       blendUsers.length && BLEND_MODES.includes(req.body?.blendMode) ? req.body.blendMode : 'average',
      rebuildSchedule: REBUILD_SCHEDULES.includes(req.body?.rebuildSchedule) ? req.body.rebuildSchedule : 'daily',
      ...buildPlaylistFeatureRules(req.body || {}),
    };
    if (importedSource) rules.importSource = importedSource;
    if (importedSuggestedContent) rules.importSuggestedContent = importedSuggestedContent;
    if (importedDetectedContent) rules.importDetectedContent = importedDetectedContent;
    rules.artwork = artwork;
    const config = loadConfig();
    const smartSettings = config.smartPlaylist || {};
    const preview = buildPlaylistPreviewSnapshot(db, userPlexId, rules, trackFilters, smartSettings);
    const allowEmptyDraft = Boolean(req.body?.allowEmptyDraft) || Boolean(importedSource);
    const removeImportedSourcePlaylistKey = importedSource
      && String(req.body?.removeImportedSourcePlaylistKey || '').trim() === importedSource.playlistKey
      ? importedSource.playlistKey
      : '';
    if (Number(preview.counts.trackCount || 0) <= 0 && !allowEmptyDraft) {
      return res.status(422).json({
        error: 'This setup matches 0 tracks. Adjust the filters or save it as a draft.',
        code: 'ZERO_TRACK_PLAYLIST',
        canSaveDraft: true,
        counts: preview.counts,
      });
    }
    const playlistDef = { id: makePersonalPlaylistId(), name, rules, trackFilters };
    createUserPersonalPlaylist(db, userPlexId, playlistDef);
    pushLog({ level: 'info', app: 'playlist', action: 'personal.create', message: `Personal playlist created: ${name} for ${userPlexId}` });
    const isDraft = Number(preview.counts.trackCount || 0) <= 0;
    let removedSourcePlaylistKey = '';
    if ((!isDraft || importedSource) && removeImportedSourcePlaylistKey) {
      const importedPlaylist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
        .find((entry) => entry.playlistKey === removeImportedSourcePlaylistKey);
      if (importedPlaylist && String(importedPlaylist.playlistType || '').trim().toLowerCase() === 'custom') {
        const importedSourceType = String(importedPlaylist.sourceType || '').trim().toLowerCase();
        if (['spotify-playlist', 'youtube-playlist', 'plex-playlist', 'plex-collection', 'lastfm-station', 'listenbrainz-playlist'].includes(importedSourceType)) {
          await deleteGeneratedPlaylistWithRemote({
            db,
            loadConfig,
            resolveUserPlexServerToken,
            buildPlexAuthHeaders,
            userPlexId,
            playlistKey: removeImportedSourcePlaylistKey,
          });
          removedSourcePlaylistKey = removeImportedSourcePlaylistKey;
        }
      }
    }
    res.json({ ok: true, playlist: playlistDef, draft: isDraft, removedSourcePlaylistKey });
    if (isDraft) return;
    // Sync for owner + all blend users so playlist appears on everyone's account
    setImmediate(async () => {
      const syncIds = blendUsers.length ? [...new Set([userPlexId, ...blendUsers])] : [userPlexId];
      for (const uid of syncIds) {
        await playlistService?.syncPersonalPlaylist(uid, playlistDef).catch(() => {});
      }
    });
  });

  // PUT /api/music/playlists/personal/:id — update name and rules for a personal playlist
  app.put('/api/music/playlists/personal/:id', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const id = String(req.params.id || '');
    const existing = getUserPersonalPlaylist(db, id, userPlexId);
    if (!existing) return res.status(404).json({ error: 'Not found' });
    const name = String(req.body?.name || '').trim();
    if (!name) return res.status(400).json({ error: 'Name is required' });
    if (findUserPersonalPlaylistByName(db, userPlexId, name, { excludeId: id })) {
      return res.status(409).json({ error: 'You already have a personal playlist with that name.' });
    }
    const BLEND_MODES = ['average', 'intersection', 'union', 'veto'];
    const REBUILD_SCHEDULES = ['daily', 'weekly', 'manual'];
    const blendUsers = Array.isArray(req.body?.blendUsers) ? req.body.blendUsers.filter(Boolean) : [];
    const trackFilters = req.body?.trackFilters !== undefined ? normaliseTrackFiltersInput(req.body.trackFilters) : (existing.trackFilters || null);
    let artwork;
    try {
      artwork = parsePlaylistArtworkInput(
        req.body?.artwork,
        normalizePlaylistArtworkState(existing.rules?.artwork || {}),
      );
    } catch (err) {
      return res.status(400).json({ error: safeMessage(err) });
    }
    const importedSource = req.body?.importSource !== undefined
      ? sanitizeImportedSourceInput(req.body.importSource)
      : sanitizeImportedSourceInput(existing.rules?.importSource);
    const importedSuggestedContent = req.body?.importSuggestedContent !== undefined
      ? sanitizeImportedContentSetInput(req.body.importSuggestedContent)
      : sanitizeImportedContentSetInput(existing.rules?.importSuggestedContent);
    const importedDetectedContent = req.body?.importDetectedContent !== undefined
      ? sanitizeImportedContentSetInput(req.body.importDetectedContent)
      : sanitizeImportedContentSetInput(existing.rules?.importDetectedContent);
    const rules = {
      artistTiers:     normaliseTriStateInput(req.body?.artistTiers),
      trackTiers:      normaliseTriStateInput(req.body?.trackTiers),
      genres:          normaliseTriStateInput(req.body?.genres),
      moods:           normaliseTriStateInput(req.body?.moods),
      albumGenres:     normaliseTriStateInput(req.body?.albumGenres),
      albumStyles:     normaliseTriStateInput(req.body?.albumStyles),
      albumMoods:      normaliseTriStateInput(req.body?.albumMoods),
      tags:            normaliseTriStateInput(req.body?.tags),
      decades:         normaliseTriStateInput(req.body?.decades),
      topNPerArtist:   req.body?.topNPerArtist ? Number(req.body.topNPerArtist) : null,
      maxTracksPerAlbum: req.body?.maxTracksPerAlbum ? Number(req.body.maxTracksPerAlbum) : null,
      maxTracks:       req.body?.maxTracks     ? Number(req.body.maxTracks)     : null,
      sortBy:          PLAYLIST_SORT_VALUES.includes(String(req.body?.sortBy || '').trim()) ? String(req.body.sortBy).trim() : 'ratingCount',
      finalOrdering:   PLAYLIST_FINAL_ORDERING_VALUES.includes(String(req.body?.finalOrdering || '').trim()) ? String(req.body.finalOrdering).trim() : 'none',
      blendUsers,
      blendMode:       blendUsers.length && BLEND_MODES.includes(req.body?.blendMode) ? req.body.blendMode : 'average',
      rebuildSchedule: REBUILD_SCHEDULES.includes(req.body?.rebuildSchedule) ? req.body.rebuildSchedule : 'daily',
      ...buildPlaylistFeatureRules(req.body || {}),
    };
    if (importedSource) rules.importSource = importedSource;
    if (importedSuggestedContent) rules.importSuggestedContent = importedSuggestedContent;
    if (importedDetectedContent) rules.importDetectedContent = importedDetectedContent;
    rules.artwork = artwork;
    const updated = { ...existing, name, rules, trackFilters };
    const config = loadConfig();
    const smartSettings = config.smartPlaylist || {};
    const preview = buildPlaylistPreviewSnapshot(db, userPlexId, rules, trackFilters, smartSettings);
    const allowEmptyDraft = Boolean(req.body?.allowEmptyDraft);
    if (Number(preview.counts.trackCount || 0) <= 0 && !allowEmptyDraft) {
      return res.status(422).json({
        error: 'This setup matches 0 tracks. Adjust the filters or save it as a draft.',
        code: 'ZERO_TRACK_PLAYLIST',
        canSaveDraft: true,
        counts: preview.counts,
      });
    }
    updateUserPersonalPlaylist(db, userPlexId, updated);
    const isDraft = Number(preview.counts.trackCount || 0) <= 0;
    if (isDraft) {
      await removeExistingPersonalGeneratedPlaylist({
        db,
        loadConfig,
        resolveUserPlexServerToken,
        buildPlexAuthHeaders,
        userPlexId,
        playlistId: id,
      });
    }
    pushLog({ level: 'info', app: 'playlist', action: 'personal.update', message: `Personal playlist updated: ${id} for ${userPlexId}` });
    res.json({ ok: true, playlist: updated, draft: isDraft });
    if (isDraft) return;
    setImmediate(async () => {
      const syncIds = blendUsers.length ? [...new Set([userPlexId, ...blendUsers])] : [userPlexId];
      for (const uid of syncIds) {
        await playlistService?.syncPersonalPlaylist(uid, updated).catch(() => {});
      }
    });
  });

  // ── Playlist rule templates ───────────────────────────────────────────────

  // GET /api/music/playlist-templates
  app.get('/api/music/playlist-templates', requireUser, (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const templates = listRuleTemplates(db, userPlexId);
    res.json({ ok: true, templates });
  });

  // POST /api/music/playlist-templates — save current rules as a named template
  app.post('/api/music/playlist-templates', requireUser, (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const name = String(req.body?.name || '').trim();
    if (!name) return res.status(400).json({ error: 'Name is required' });
    const description = String(req.body?.description || '').trim();
    const rules = req.body?.rules && typeof req.body.rules === 'object' ? req.body.rules : {};
    const trackFilters = req.body?.trackFilters && typeof req.body.trackFilters === 'object' ? req.body.trackFilters : null;
    const startingPointId = String(req.body?.startingPointId || '').trim() || 'blank';
    const id = saveRuleTemplate(db, userPlexId, { name, description, rules, trackFilters, startingPointId });
    res.json({ ok: true, id });
  });

  // PUT /api/music/playlist-templates/:id — update a user-saved template
  app.put('/api/music/playlist-templates/:id', requireUser, (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const id = String(req.params.id || '').trim();
    const name = String(req.body?.name || '').trim();
    if (!id) return res.status(400).json({ error: 'Template id is required' });
    if (!name) return res.status(400).json({ error: 'Name is required' });
    const description = String(req.body?.description || '').trim();
    const rules = req.body?.rules && typeof req.body.rules === 'object' ? req.body.rules : {};
    const trackFilters = req.body?.trackFilters && typeof req.body.trackFilters === 'object' ? req.body.trackFilters : null;
    const startingPointId = String(req.body?.startingPointId || '').trim() || 'blank';
    const result = updateRuleTemplate(db, id, userPlexId, { name, description, rules, trackFilters, startingPointId });
    if (!result.changes) return res.status(404).json({ error: 'Template not found' });
    res.json({ ok: true });
  });

  // DELETE /api/music/playlist-templates/:id — delete a user-saved template
  app.delete('/api/music/playlist-templates/:id', requireUser, (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const id = String(req.params.id || '');
    deleteRuleTemplate(db, id, userPlexId);
    res.json({ ok: true });
  });

  // DELETE /api/music/playlists/generated — remove a generated playlist from Plex and Curatorr
  app.delete('/api/music/playlists/generated', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistKey = String(req.body?.playlistKey || req.query?.playlistKey || '').trim();
    if (!playlistKey) return res.status(400).json({ error: 'playlistKey is required' });

    const existing = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((p) => p.playlistKey === playlistKey);
    if (!existing) return res.status(404).json({ error: 'Playlist not found' });
    if (!['personal', 'global', 'custom', 'lastfm-station', 'listenbrainz-playlist'].includes(existing.playlistType)) {
      return res.status(403).json({ error: 'This playlist type cannot be deleted.' });
    }

    const config = loadConfig();
    const { url } = config.plex || {};
    const token = resolveUserPlexServerToken(config, userPlexId);
    const base = String(url || '').replace(/\/$/, '');

    if (base && token && existing.plexPlaylistId) {
      try {
        await fetch(`${base}/playlists/${existing.plexPlaylistId}`, {
          method: 'DELETE',
          headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
        });
      } catch (_err) { /* best-effort */ }
    }

    if (existing.playlistType === 'personal') {
      const personalId = playlistKey.replace(/^personal:/, '');
      if (personalId) deleteUserPersonalPlaylist(db, personalId, userPlexId);
    }
    deleteUserGeneratedPlaylist(db, userPlexId, playlistKey);
    pushLog({ level: 'info', app: 'playlist', action: 'generated.delete', message: `Generated playlist deleted: ${playlistKey} for ${userPlexId}` });
    res.json({ ok: true });
  });

  // DELETE /api/music/playlists/personal/:id
  app.delete('/api/music/playlists/personal/:id', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const id = String(req.params.id || '');
    const existing = getUserPersonalPlaylist(db, id, userPlexId);
    if (!existing) return res.status(404).json({ error: 'Not found' });

    const playlistKey = `personal:${id}`;
    const generatedEntry = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
      .find((p) => p.playlistKey === playlistKey);

    if (generatedEntry) {
      const config = loadConfig();
      const { url } = config.plex || {};
      const token = resolveUserPlexServerToken(config, userPlexId);
      const base = String(url || '').replace(/\/$/, '');
      if (base && token && generatedEntry.plexPlaylistId) {
        try {
          await fetch(`${base}/playlists/${generatedEntry.plexPlaylistId}`, {
            method: 'DELETE',
            headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
          });
        } catch (_err) { /* best-effort */ }
      }
      deleteUserGeneratedPlaylist(db, userPlexId, playlistKey);
    }

    deleteUserPersonalPlaylist(db, id, userPlexId);
    pushLog({ level: 'info', app: 'playlist', action: 'personal.delete', message: `Personal playlist deleted: ${id} for ${userPlexId}` });
    res.json({ ok: true });
  });

  // ── Blend ────────────────────────────────────────────────────────────────────

  // GET /api/blend/users
  // Returns users who have play history in artist_stats, with local avatar URLs where available.
  app.get('/api/blend/users', requireUser, (req, res) => {
    try {
      const rows = db.prepare(
        'SELECT DISTINCT user_plex_id FROM artist_stats WHERE TRIM(COALESCE(user_plex_id, \'\')) != \'\' ORDER BY user_plex_id COLLATE NOCASE',
      ).all();
      const cfg = loadConfig();
      const localUsers = resolveLocalUsers ? resolveLocalUsers(cfg) : [];
      const localIdentitySet = new Set(
        localUsers
          .flatMap((u) => [u?.username, u?.email])
          .map((value) => String(value || '').trim().toLowerCase())
          .filter(Boolean),
      );
      const userIds = rows
        .map((r) => String(r.user_plex_id || '').trim())
        .filter((id) => id && !localIdentitySet.has(id.toLowerCase()));
      const avatarMap = new Map(
        localUsers
          .filter((u) => u.username && u.avatar)
          .map((u) => [u.username.toLowerCase(), normalizeStoredAvatarPath ? normalizeStoredAvatarPath(u.avatar) : u.avatar]),
      );
      const users = userIds.map((id) => ({ id, avatar: avatarMap.get(id.toLowerCase()) || '' }));
      return res.json({ ok: true, users });
    } catch (err) {
      return res.status(500).json({ ok: false, error: String(err?.message || 'Failed to load blend users') });
    }
  });

  // GET /api/blend/stats?users[]=u1&users[]=u2[&limit=20]
  // Returns compatibility score, shared artist counts, and blended top artists/tracks.
  app.get('/api/music/report/weekly', requireUser, (req, res) => {
    const userPlexId = resolveQueryUserId(req);
    if (!userPlexId) return res.status(401).json({ error: 'Not authenticated' });

    const offset = Math.max(0, Math.min(104, Number(req.query.offset) || 0));

    const now = new Date();
    const dow = (now.getDay() + 6) % 7; // Mon=0, Sun=6
    const thisMon = new Date(now);
    thisMon.setHours(0, 0, 0, 0);
    thisMon.setDate(thisMon.getDate() - dow);

    const weekMs = 7 * 24 * 60 * 60 * 1000;
    const targetMon = new Date(thisMon.getTime() - offset * weekMs);
    const nextMon   = new Date(targetMon.getTime() + weekMs);
    const prevMon   = new Date(targetMon.getTime() - weekMs);

    function fetchWeek(startMs, endMs) {
      const rows = db.prepare(`
        SELECT date(started_at / 1000, 'unixepoch', 'localtime') AS day, COUNT(*) AS plays
        FROM play_events
        WHERE user_plex_id = ? AND is_skip = 0
          AND started_at >= ? AND started_at < ?
        GROUP BY day
      `).all(userPlexId, startMs, endMs);
      const map = new Map(rows.map((r) => [r.day, r.plays]));
      const DOW_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
      const days = [];
      let d = new Date(startMs);
      for (let i = 0; i < 7; i++, d = new Date(d.getTime() + 86400000)) {
        const key = d.toISOString().slice(0, 10);
        days.push({ date: key, dow: DOW_LABELS[i], plays: map.get(key) || 0 });
      }
      return { days, totalPlays: days.reduce((s, dd) => s + dd.plays, 0) };
    }

    function weekLabel(startMs) {
      const s = new Date(startMs);
      const e = new Date(startMs + weekMs - 86400000);
      const o1 = { day: 'numeric', month: 'short' };
      const o2 = { day: 'numeric', month: 'short', year: 'numeric' };
      return `${s.toLocaleDateString('en-GB', o1)} – ${e.toLocaleDateString('en-GB', o2)}`;
    }

    const week     = { label: weekLabel(targetMon.getTime()), ...fetchWeek(targetMon.getTime(), nextMon.getTime()) };
    const prevWeek = { label: weekLabel(prevMon.getTime()),   ...fetchWeek(prevMon.getTime(),   targetMon.getTime()) };

    res.json({ offset, canGoForward: offset > 0, week, prevWeek });
  });

  app.get('/api/blend/stats', requireUser, (req, res) => {
    const raw = req.query.users;
    const users = (Array.isArray(raw) ? raw : raw ? [raw] : []).map(String).filter(Boolean);
    if (users.length < 2) return res.status(400).json({ error: 'At least 2 users required' });

    const config = loadConfig();
    const smartSettings = config.smartPlaylist || {};
    const skipRank   = Number(smartSettings.artistSkipRank   ?? 2);
    const belterRank = Number(smartSettings.artistBelterRank ?? 8);
    const limit      = Math.min(50, Math.max(1, Number(req.query.limit) || 20));

    function classifyScore(s) {
      if (s === null || s === undefined) return 'unranked';
      if (s >= belterRank) return 'belter';
      if (s >= 5) return 'decent';
      if (s > skipRank) return 'halfDecent';
      return 'skip';
    }

    function applyBlendConfidence(rawScore, sharedArtists) {
      if (rawScore === null || rawScore === undefined) return null;
      const confidence = Math.max(0, Math.min(1, Number(sharedArtists || 0) / 30));
      return Math.round(50 + (Number(rawScore || 0) - 50) * confidence);
    }

    // Per-user artist maps: lowercase name → { artist_name, ranking_score, play_count }
    const userArtistMaps = users.map((uid) =>
      new Map(db.prepare('SELECT artist_name, ranking_score, play_count FROM artist_stats WHERE user_plex_id = ?').all(uid)
        .map((r) => [r.artist_name.toLowerCase(), r])),
    );

    // Per-user track maps: rating_key → { tier, tier_weight, play_count }
    const userTrackMaps = users.map((uid) =>
      new Map(db.prepare('SELECT plex_rating_key, tier, tier_weight, play_count FROM track_stats WHERE user_plex_id = ?').all(uid)
        .map((r) => [r.plex_rating_key, r])),
    );

    // Shared artists = artists that every user has play data for
    const allArtistKeys = new Set(userArtistMaps.flatMap((m) => [...m.keys()]));
    const sharedArtistKeys = [...allArtistKeys].filter((k) => userArtistMaps.every((m) => m.has(k)));

    // Compatibility score: mean score-agreement across shared artists (0–100)
    let compatibilityScore = null;
    let sharedBelters = 0;
    let agreedSkips = 0;

    if (sharedArtistKeys.length) {
      let totalSim = 0;
      for (const key of sharedArtistKeys) {
        const scores = userArtistMaps.map((m) => m.get(key).ranking_score);
        const mean   = scores.reduce((a, b) => a + b, 0) / scores.length;
        const avgDev = scores.reduce((a, s) => a + Math.abs(s - mean), 0) / scores.length;
        totalSim += 1 - avgDev / 10;
        if (scores.every((s) => s >= belterRank)) sharedBelters++;
        if (scores.every((s) => s <= skipRank))   agreedSkips++;
      }
      compatibilityScore = applyBlendConfidence(
        Math.round((totalSim / sharedArtistKeys.length) * 100),
        sharedArtistKeys.length,
      );
    }

    // Blended top artists: shared by all, sorted by averaged score
    const topArtists = sharedArtistKeys
      .map((key) => {
        const entries = userArtistMaps.map((m) => m.get(key));
        const avgScore = entries.reduce((a, e) => a + e.ranking_score, 0) / entries.length;
        const avgPlays = Math.round(entries.reduce((a, e) => a + (e.play_count || 0), 0) / entries.length);
        return { artist_name: entries[0].artist_name, avg_score: Math.round(avgScore * 10) / 10, avg_plays: avgPlays, tier: classifyScore(avgScore) };
      })
      .sort((a, b) => b.avg_score - a.avg_score || b.avg_plays - a.avg_plays)
      .slice(0, limit);

    // Blended top tracks: shared by all users, sorted by averaged tier_weight
    const masterTrackMap = new Map(getMasterTracks(db).map((t) => [t.ratingKey, t]));
    const allTrackKeys   = new Set(userTrackMaps.flatMap((m) => [...m.keys()]));
    const sharedTrackKeys = [...allTrackKeys].filter((k) => userTrackMaps.every((m) => m.has(k)));

    const topTrackPopularity = getAlbumPopularTrackRanks(db, sharedTrackKeys);
    const topTracks = sharedTrackKeys
      .map((key) => {
        const master = masterTrackMap.get(key);
        if (!master) return null;
        const entries   = userTrackMaps.map((m) => m.get(key));
        const avgWeight = entries.reduce((a, e) => a + (e.tier_weight || 0), 0) / entries.length;
        const avgPlays  = Math.round(entries.reduce((a, e) => a + (e.play_count || 0), 0) / entries.length);
        const tierCounts = {};
        for (const e of entries) tierCounts[e.tier || 'curatorr'] = (tierCounts[e.tier || 'curatorr'] || 0) + 1;
        const tier = Object.entries(tierCounts).sort((a, b) => b[1] - a[1])[0][0];
        const normTier = tier === 'half-decent' ? 'halfDecent' : tier === 'curatorr' ? 'unplayed' : tier;
        const popularity = topTrackPopularity.get(String(key || '')) || null;
        return {
          track_title: master.trackTitle,
          artist_name: master.artistName,
          rating_key: key,
          avg_tier_weight: Math.round(avgWeight * 10) / 10,
          avg_plays: avgPlays,
          tier,
          normTier,
          popularRank: popularity ? popularity.rank : null,
          ratingCount: popularity ? popularity.ratingCount : Number(master.ratingCount || 0),
        };
      })
      .filter(Boolean)
      .sort((a, b) => b.avg_tier_weight - a.avg_tier_weight || b.avg_plays - a.avg_plays)
      .slice(0, limit);

    res.json({
      ok: true,
      users,
      compatibilityScore,
      sharedArtists: sharedArtistKeys.length,
      totalArtists:  allArtistKeys.size,
      sharedBelters,
      agreedSkips,
      topArtists,
      topTracks,
    });
  });

}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function resolveQueryUserId(req) {
  const previewUserId = String(req.session?.previewUserId || '').trim();
  if (previewUserId) return previewUserId;
  const user = req.session?.user || {};
  return String(user.username || '').trim();
}

// For playlist/preference lookups — uses the canonical OAuth identity which may differ from
// the play_events identity used by resolveQueryUserId (e.g. webhook numeric ID vs username).
function resolveCanonicalUserId(req) {
  const previewCanonicalId = String(req.session?.previewCanonicalId || '').trim();
  if (previewCanonicalId) return previewCanonicalId;
  return resolveQueryUserId(req);
}

function resolveSuggestionUserId(req) {
  const previewUserId = String(req.session?.previewUserId || '').trim();
  if (previewUserId) return previewUserId;
  const user = req.session?.user || {};
  const source = String(user.source || '').trim().toLowerCase();
  const role = String(user.role || '').trim().toLowerCase();
  if (role === 'admin' && source === 'local') return '';
  return String(user.username || '').trim();
}
