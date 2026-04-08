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
  previewGlobalPlaylist,
  getAlbumPopularTrackRanks,
  getArtistTagMap,
  getEffectiveTrackTags,
  getTrackDecadeTag,
  listRuleTemplates,
  saveRuleTemplate,
  updateRuleTemplate,
  deleteRuleTemplate,
} from '../db.js';
import { paginateRolledHistory } from '../history-rollup.js';
import { promoteCompletedRequestsFromLidarr, resolveLibraryAlbumMatch } from '../services/album-reconciliation.js';
import { applyFeaturePresetFilters, applyTrackFiltersWithReport } from '../services/playlists.js';

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
const PLAYLIST_SORT_VALUES = ['default', 'source', 'ratingCount', 'tierWeight', 'playCount', 'random', 'bpmAsc', 'bpmDesc', 'energyAsc', 'energyDesc', 'danceabilityDesc', 'camelot', 'djFlow'];
const PLAYLIST_FINAL_ORDERING_VALUES = ['none', 'plexSonic', 'loudness', 'plexSonicLoudness'];
const PLAYLIST_ALBUM_POPULARITY_VALUES = ['all', 'top3Only', 'excludeTop3'];
const PLAYLIST_POPULARITY_VALUES = ['all', 'top50', 'top25', 'top10', 'top5', 'custom'];
// Cache Jellyfin/Emby userId lookups — keyed by "username@serverUrl", TTL 1 hour
const msUserIdCache = new Map();
const MS_USERID_CACHE_TTL_MS = 60 * 60 * 1000;

function parseNullablePlaylistNumber(value) {
  if (value === '' || value == null) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function formatOverviewReleaseDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const isoMatch = raw.match(/^(\d{4}-\d{2}-\d{2})T/);
  if (isoMatch) return isoMatch[1];
  return raw;
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
  if (!playlistKey || !['spotify-playlist', 'plex-playlist', 'plex-collection'].includes(sourceType)) return null;
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
  const kinds = ['genres', 'moods', 'tags', 'decades'];
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
  const masterTracks = getMasterTracks(db);
  const masterTrackMap = new Map(masterTracks.map((track) => [String(track?.ratingKey || '').trim(), track]));
  const trackRefs = getPlaylistTracks(db, userPlexId, playlist.playlistKey);
  const matchedTracks = trackRefs
    .map((trackRef) => masterTrackMap.get(String(trackRef?.ratingKey || '').trim()) || null)
    .filter(Boolean);
  if (!matchedTracks.length) return null;

  const featurePreset = inferImportedFeaturePreset(matchedTracks);
  const bpmRange = inferImportedValueRange(matchedTracks.map((track) => track?.bpm), { round: (value) => Math.round(value) });
  const energyRange = inferImportedValueRange(matchedTracks.map((track) => track?.energy), { round: (value) => Number(value.toFixed(2)) });
  const danceabilityRange = inferImportedValueRange(matchedTracks.map((track) => track?.danceability), { round: (value) => Number(value.toFixed(2)) });
  const topGenres = inferImportedTopValues(matchedTracks.flatMap((track) => Array.isArray(track?.genres) ? track.genres : []), { minShare: 0.18, maxValues: 4 });
  const topMoods = inferImportedTopValues(matchedTracks.flatMap((track) => Array.isArray(track?.moods) ? track.moods : []), { minShare: 0.18, maxValues: 4 });

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
      tags: { include: topTags, exclude: [], includeMode: 'any' },
      decades: { include: topDecades, exclude: [], includeMode: 'any' },
    },
    importDetectedContent: {
      genres: { include: allDetectedGenres, exclude: [], includeMode: 'any' },
      moods: { include: allDetectedMoods, exclude: [], includeMode: 'any' },
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
  if (event.is_skip) return buildTierBadge('skip');
  const listenedMs = Number(event.duration_ms || 0);
  const trackDurationMs = Number(event.track_duration_ms || 0);
  const completionThresholdMs = (Number(config?.smartPlaylist?.completionThresholdSeconds) || 30) * 1000;
  if (trackDurationMs > 0) {
    if (listenedMs >= Math.max(0, trackDurationMs - completionThresholdMs)) return buildTierBadge('belter');
    if (listenedMs >= trackDurationMs * 0.5) return buildTierBadge('decent');
    return buildTierBadge('half-decent');
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
      await fetch(addUrl.toString(), {
        method: 'PUT',
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      });
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
    if (role === 'admin') return String(req.query?.user || '').trim();
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

  function makeImportedCustomPlaylistKey() {
    return 'custom-import-' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
  }

  function normaliseImportedPlaylistTitle(value, fallback) {
    const title = String(value || '').trim();
    if (title) return title.slice(0, 120);
    return String(fallback || 'Imported Playlist').trim().slice(0, 120) || 'Imported Playlist';
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
    const playlistKey = makeImportedCustomPlaylistKey();
    const now = Date.now();
    const unmatchedTracks = Array.isArray(sourceMeta?.unmatchedTracks) ? sourceMeta.unmatchedTracks : [];
    saveUserGeneratedPlaylist(db, userPlexId, {
      playlistKey,
      playlistTitle,
      playlistType: 'custom',
      plexPlaylistId: '',
      sourceType: String(sourceMeta?.sourceType || '').trim(),
      sourceRef: String(sourceMeta?.sourceRef || '').trim(),
      sourceTitle: String(sourceMeta?.sourceTitle || playlistTitle).trim(),
      sourceOwner: String(sourceMeta?.sourceOwner || '').trim(),
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
    } else {
      throw new Error('This imported playlist source cannot be refreshed.');
    }

    saveUserGeneratedPlaylist(db, userPlexId, {
      ...playlist,
      sourceRef,
      sourceTitle,
      sourceOwner,
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
      const statusMeta = getManualAlbumStatusMeta(resolvedStatus);
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
      return res.json({
        ok: true,
        item: {
          kind: 'album',
          title: String(overview?.albumTitle || albumTitle || '').trim(),
          subtitle: artistName,
          overview: String(overview?.overview || `${albumTitle} by ${artistName}`),
          thumb: String(overview?.thumb || '').trim(),
          art: artistName ? `/api/music/thumb/artist/${encodeURIComponent(artistName)}` : String(overview?.art || '').trim(),
          posterRatio: 'square',
          pills: [
            'Album',
            statusMeta.label,
            String(overview?.albumType || '').trim() || '',
            String(overview?.source || source || '').trim() || '',
          ].filter(Boolean),
          stats: [
            { label: 'Tracks', value: Number(overview?.trackCount || tracks.length || 0) },
            { label: 'Available', value: Number(overview?.trackFileCount || 0) > 0 ? 'Yes' : 'No' },
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
              : (statusMeta.key === 'available' ? 'Already in library' : (statusMeta.key === 'pending' ? 'Already added' : 'Add album')),
            disabled: !statusMeta.selectable || existingState.excluded,
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
          art: artistName ? `/api/music/thumb/artist/${encodeURIComponent(artistName)}` : `/api/music/thumb/track/${encodeURIComponent(ratingKey)}`,
          posterRatio: 'square',
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

  app.post('/api/music/suggestions/rebuild', requireUser, (req, res) => {
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
              const _albumTrackCount3 = Number(album?.statistics?.trackCount || album?.trackCount || 0);
              if (_albumTrackCount3 > 0) recordLidarrUsage(db, userPlexId, { roleName: role, usageKey: 'tracks', amount: _albumTrackCount3 });
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
    if (!['spotify-playlist', 'plex-playlist', 'plex-collection'].includes(sourceType)) {
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
    if (!['spotify-playlist', 'plex-playlist', 'plex-collection'].includes(sourceType)) {
      return res.status(400).json({ error: 'This playlist is not linked to an import source.' });
    }

    const prefill = inferImportedWizardPrefill(db, userPlexId, playlist);
    if (!prefill) {
      return res.status(422).json({ error: 'This imported playlist has no matched Curatorr tracks yet, so there is nothing to infer from.' });
    }

    return res.json({ ok: true, prefill });
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
      return res.json({
        ok: true,
        playlist: playlist || null,
        importedTrackCount: trackRefs.length,
      });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/import/spotify/playlists', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    try {
      const auth = await getSpotifyAuthForUser(userPlexId);
      const playlists = await spotifyService.listCurrentUserPlaylists(auth.accessToken, { limit: 50 });
      return res.json({ ok: true, playlists });
    } catch (err) {
      return res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/import/spotify/preview', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistId = String(req.query?.playlistId || '').trim();
    if (!playlistId) return res.status(400).json({ error: 'playlistId is required.' });
    try {
      const auth = await getSpotifyAuthForUser(userPlexId);
      const [playlistMeta, playlistItems] = await Promise.all([
        spotifyService.getPlaylist(auth.accessToken, playlistId),
        spotifyService.getPlaylistItems(auth.accessToken, playlistId, { limit: 100 }),
      ]);
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
        matchedCount: matched.length,
        unmatchedCount: unmatched.length,
        unmatchedArtistCount: unmatchedArtists.length,
        duplicateCount: duplicateMatches.length,
        matched: matched.slice(0, 100),
        unmatchedArtists,
        unmatched: unmatched.slice(0, 100),
        duplicateMatches: duplicateMatches.slice(0, 100),
      });
    } catch (err) {
      return res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/import/spotify', requireUser, async (req, res) => {
    const userPlexId = resolveCanonicalUserId(req);
    const playlistId = String(req.body?.playlistId || '').trim();
    const title = normaliseImportedPlaylistTitle(req.body?.title, 'Imported Spotify Playlist');
    if (!playlistId) return res.status(400).json({ error: 'playlistId is required.' });
    try {
      const auth = await getSpotifyAuthForUser(userPlexId);
      const [playlistMeta, playlistItems] = await Promise.all([
        spotifyService.getPlaylist(auth.accessToken, playlistId),
        spotifyService.getPlaylistItems(auth.accessToken, playlistId, { limit: 100 }),
      ]);
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
      return res.json({
        ok: true,
        playlist: playlist || null,
        importedTrackCount: trackRefs.length,
        unmatchedCount: unmatched.length,
        importedMissingCount: unmatched.length,
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
    if (!artistName || !albumName) return res.status(404).end();
    const cacheKey = `album:${normalizeArtistMatchText(artistName)}::${normalizeAlbumMatchText(albumName)}`;
    const cached = getThumbCache(cacheKey);
    if (cached) return sendCachedThumbResponse(res, cached);

    if (msType === 'jellyfin' || msType === 'emby') {
      const { url: msUrl, apiKey } = config[msType] || {};
      const fallbackArtistLocation = `/api/music/thumb/artist/${encodeURIComponent(artistName)}?v=jf-album-fallback-1`;
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
      return sendThumbRedirect(res, cacheKey, fallbackArtistLocation);
    }

    const { url, token } = config.plex || {};
    if (!url || !token) return res.status(404).end();
    const base = url.replace(/\/$/, '');
    const fallbackArtistLocation = `/api/music/thumb/artist/${encodeURIComponent(artistName)}?v=discover-album-fallback-1`;
    const fallbackArtistThumb = () => sendThumbRedirect(res, cacheKey, fallbackArtistLocation);
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
    const importedSource = sanitizeImportedSourceInput(req.body?.importSource);
    const importedSuggestedContent = sanitizeImportedContentSetInput(req.body?.importSuggestedContent);
    const importedDetectedContent = sanitizeImportedContentSetInput(req.body?.importDetectedContent);
    const rules = {
      artistTiers:     normaliseTriStateInput(req.body?.artistTiers),
      trackTiers:      normaliseTriStateInput(req.body?.trackTiers),
      genres:          normaliseTriStateInput(req.body?.genres),
      moods:           normaliseTriStateInput(req.body?.moods),
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
    const config = loadConfig();
    const smartSettings = config.smartPlaylist || {};
    const preview = buildPlaylistPreviewSnapshot(db, userPlexId, rules, trackFilters, smartSettings);
    const allowEmptyDraft = Boolean(req.body?.allowEmptyDraft);
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
    if (!isDraft && removeImportedSourcePlaylistKey) {
      const importedPlaylist = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
        .find((entry) => entry.playlistKey === removeImportedSourcePlaylistKey);
      if (importedPlaylist && String(importedPlaylist.playlistType || '').trim().toLowerCase() === 'custom') {
        const importedSourceType = String(importedPlaylist.sourceType || '').trim().toLowerCase();
        if (['spotify-playlist', 'plex-playlist', 'plex-collection'].includes(importedSourceType)) {
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
