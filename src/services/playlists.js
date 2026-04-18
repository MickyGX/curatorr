import { execFile } from 'node:child_process';
import {
  getUserPersonalPlaylist,
  getUserPlaylist,
  listSuggestedTracks,
  listUserGeneratedPlaylists,
  listAllGeneratedPlaylists,
  recordPlaylistSync,
  saveUserGeneratedPlaylist,
  getAllUserIds,
  getUserPreferences,
  saveUserPreferences,
  getMasterTracks,
  setPlaylistTracks,
  getPlaylistTracks,
  updateUserPersonalPlaylist,
  getArtistTagMap,
  getEffectiveTrackTags,
  getTrackDecadeTag,
  parseTriStateFilter,
} from '../db.js';
import {
  detectPlaylistArtworkMimeFromBuffer,
  getStoredPlaylistArtworkInfo,
  savePlaylistArtworkBuffer,
} from './playlist-artwork.js';

const execFileAsync = (file, args, options = {}) => new Promise((resolve, reject) => {
  execFile(file, args, {
    ...options,
    encoding: 'utf8',
    maxBuffer: options.maxBuffer || 5 * 1024 * 1024,
  }, (err, stdout, stderr) => {
    if (err) {
      err.stdout = stdout;
      err.stderr = stderr;
      reject(err);
      return;
    }
    resolve({ stdout, stderr });
  });
});

const DAILY_MIX_PLAYLIST_KEY = 'daily-mix';
const DAILY_MIX_PLAYLIST_TYPE = 'daily-mix';
const CURATORR_PLAYLIST_KEY = 'curatorr';
const CURATORR_PLAYLIST_TYPE = 'curatorr';
const CRESCIVE_PLAYLIST_KEY  = 'crescive';
const CRESCIVE_PLAYLIST_TYPE = 'crescive';
const CURATIVE_PLAYLIST_KEY  = 'curative';
const CURATIVE_PLAYLIST_TYPE = 'curative';
const ALGORITHM_VERSION = 'crescive-curative-v1';
const LISTENBRAINZ_REQUEST_TIMEOUT_MS = 20_000;
const LASTFM_STATION_LABELS = {
  recommended: 'Recommended',
  mix: 'Mix',
  library: 'Library',
  neighbours: 'Neighbours',
  loved: 'Loved',
};

const LISTENBRAINZ_PLAYLIST_LABELS = {
  'daily-jams': 'Daily Jams',
  'weekly-jams': 'Weekly Jams',
  'weekly-exploration': 'Weekly Exploration',
};

const TOP_TRACKS_PERIOD_LABELS = {
  overall: 'All Time',
  '7day': 'Last 7 Days',
  '1month': 'Last Month',
  '3month': 'Last 3 Months',
  '6month': 'Last 6 Months',
  '12month': 'Last Year',
};
const PLAYLIST_BASE_SORT_MODES = new Set(['default', 'source', 'ratingCount', 'tierWeight', 'playCount', 'random', 'bpmAsc', 'bpmDesc', 'energyAsc', 'energyDesc', 'danceabilityDesc', 'camelot', 'djFlow']);
const PLAYLIST_FINAL_ORDERING_MODES = new Set(['none', 'plexSonic', 'loudness', 'plexSonicLoudness']);

function escapeRegex(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Normalize artist name for cross-source matching.
// Strips featured-artist credits that some sources append to the primary artist name.
function normalizeMatchArtist(value) {
  let a = String(value || '').trim();
  a = a.replace(/\s+(?:feat\.?|ft\.?|f\/|featuring)\b.+$/i, '');
  return a.toLowerCase().replace(/\s+/g, ' ').trim();
}

// Tier-1 (exact) title normalisation: strip only featured-artist credits.
// Preserves meaningful parens like "(acoustic)", "(Fade Out)", "(Oh Doctor Jesus)".
function normalizeMatchTitleExact(value) {
  let t = String(value || '').trim();
  // Strip featured-artist credits wrapped in parens: (feat. X), (ft. X), (f/ X), (featuring X)
  t = t.replace(/\s*\(\s*(?:feat\.?|ft\.?|f\/|featuring)\b[^)]*\)/gi, '');
  // Strip trailing featured-artist credits not in parens
  t = t.replace(/\s+(?:feat\.?|ft\.?|f\/|featuring)\b.+$/i, '');
  return t.toLowerCase().replace(/\s+/g, ' ').trim();
}

// Tier-2 (fuzzy) title normalisation: also strips trailing parenthetical/bracket version
// suffixes like (Remastered), (Live), [Remix], (from the series ...).
// Used as a fallback when the exact key finds no match.
function normalizeMatchTitleFuzzy(value) {
  let t = normalizeMatchTitleExact(value);
  // Strip trailing parenthetical suffixes — applied twice for back-to-back annotations.
  t = t.replace(/\s*\([^)]*\)\s*$/g, '').trim();
  t = t.replace(/\s*\([^)]*\)\s*$/g, '').trim();
  // Strip trailing square-bracket suffixes: [Remix], [Live], etc.
  t = t.replace(/\s*\[[^\]]*\]\s*$/g, '').trim();
  return t;
}

function buildExactLookupKey(artistName, trackTitle) {
  const artist = normalizeMatchArtist(artistName);
  const title = normalizeMatchTitleExact(trackTitle);
  return artist && title ? `${artist}|${title}` : '';
}

function buildFuzzyLookupKey(artistName, trackTitle) {
  const artist = normalizeMatchArtist(artistName);
  const title = normalizeMatchTitleFuzzy(trackTitle);
  return artist && title ? `${artist}|${title}` : '';
}

function isSyntheticPlaylistSeed(track) {
  const ratingKey = String(track?.ratingKey || '').trim().toLowerCase();
  const libraryKey = String(track?.libraryKey || '').trim().toLowerCase();
  return libraryKey === 'listenbrainz-seed' || ratingKey.startsWith('lb-seed-');
}

// Build two lookup Maps from master tracks:
//   exact — feat-stripped only; preserves (acoustic), (Fade Out), etc.
//   fuzzy — also strips trailing parens/brackets; catches remaster/version mismatches.
// Matching always tries exact first, then falls back to fuzzy.
// For tracks with comma-separated multi-artist originalTitle (e.g. "Jacob Collier, Shawn Mendes,
// Stormzy & Kirk Franklin"), extra entries are added under the primary artist alone so that
// external sources (Last.fm, ListenBrainz) that only send the primary artist still match.
function buildPlaylistTrackLookup(masterTracks) {
  const exact = new Map();
  const fuzzy = new Map();

  function indexArtist(artistName, track) {
    const ek = buildExactLookupKey(artistName, track.trackTitle);
    if (ek) {
      const prev = exact.get(ek);
      if (!prev || (isSyntheticPlaylistSeed(prev) && !isSyntheticPlaylistSeed(track))) {
        exact.set(ek, track);
      }
    }
    const fk = buildFuzzyLookupKey(artistName, track.trackTitle);
    if (fk && fk !== ek) {
      const prev = fuzzy.get(fk);
      if (!prev || (isSyntheticPlaylistSeed(prev) && !isSyntheticPlaylistSeed(track))) {
        fuzzy.set(fk, track);
      }
    }
  }

  for (const track of masterTracks || []) {
    if (!track?.ratingKey) continue;
    indexArtist(track.artistName, track);
    // If artistName is a comma-separated joint credit (from Plex originalTitle), also index
    // by the primary artist so lookups using only the first artist still resolve.
    const primaryArtist = String(track.artistName || '').split(/\s*,\s*/)[0].trim();
    if (primaryArtist && primaryArtist !== track.artistName) {
      indexArtist(primaryArtist, track);
    }
  }

  return {
    exact: new Map([...exact.entries()].map(([k, t]) => [k, t.ratingKey])),
    fuzzy: new Map([...fuzzy.entries()].map(([k, t]) => [k, t.ratingKey])),
  };
}

function lookupTrackRatingKey(lookup, artistName, trackTitle, { strictMatch = false } = {}) {
  const ek = buildExactLookupKey(artistName, trackTitle);
  if (ek) {
    const hit = lookup.exact.get(ek);
    if (hit) return hit;
  }
  if (strictMatch) return '';
  const fk = buildFuzzyLookupKey(artistName, trackTitle);
  return (fk ? lookup.fuzzy.get(fk) : '') || '';
}

function collectMatchedRatingKeys(trackLookup, tracks, getArtistName, getTrackTitle, { strictMatch = false } = {}) {
  const ratingKeys = [];
  for (const track of tracks || []) {
    const ratingKey = lookupTrackRatingKey(trackLookup, getArtistName(track), getTrackTitle(track), { strictMatch });
    if (ratingKey) ratingKeys.push(ratingKey);
  }
  return ratingKeys;
}

function extractMusicbrainzUuid(value) {
  const match = String(value || '').match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
  return match ? match[0].toLowerCase() : '';
}

function buildListenbrainzHeaders(token = '') {
  const headers = { Accept: 'application/json' };
  if (token) headers.Authorization = `Token ${token}`;
  return headers;
}

function collectListenbrainzErrorCodes(err, codes = new Set()) {
  if (!err || typeof err !== 'object') return codes;
  const code = String(err.code || '').trim();
  if (code) codes.add(code);
  if (Array.isArray(err.errors)) {
    for (const nested of err.errors) collectListenbrainzErrorCodes(nested, codes);
  }
  if (err.cause && err.cause !== err) collectListenbrainzErrorCodes(err.cause, codes);
  return codes;
}

function summarizeListenbrainzError(err) {
  const parts = [];
  const message = String(err?.message || '').trim();
  const codes = [...collectListenbrainzErrorCodes(err)];
  if (message) parts.push(message);
  if (codes.length) parts.push(`codes=${codes.join(',')}`);
  return parts.join(' | ') || 'Unknown error';
}

function isListenbrainzNetworkError(err) {
  const message = String(err?.message || '').toLowerCase();
  if (message.includes('fetch failed')) return true;
  const codes = collectListenbrainzErrorCodes(err);
  return ['ETIMEDOUT', 'ENETUNREACH', 'ECONNRESET', 'ECONNREFUSED', 'EHOSTUNREACH', 'EAI_AGAIN'].some((code) => codes.has(code));
}

async function fetchListenbrainzJsonViaWget(url, headers = {}) {
  const args = [
    '-qO-',
    '-T',
    String(Math.max(5, Math.ceil(LISTENBRAINZ_REQUEST_TIMEOUT_MS / 1000))),
    '-t',
    '1',
  ];
  for (const [name, value] of Object.entries(headers || {})) {
    if (!value) continue;
    args.push(`--header=${name}: ${value}`);
  }
  args.push(String(url || ''));
  const { stdout } = await execFileAsync('wget', args, { timeout: LISTENBRAINZ_REQUEST_TIMEOUT_MS });
  return JSON.parse(stdout);
}

async function requestListenbrainzJson(ctx, url, headers = {}, meta = {}) {
  if (typeof ctx.listenbrainzFetchJson === 'function') {
    return ctx.listenbrainzFetchJson(url, { headers, meta });
  }
  try {
    const response = await fetch(String(url || ''), {
      headers,
      signal: AbortSignal.timeout(LISTENBRAINZ_REQUEST_TIMEOUT_MS),
    });
    if (!response.ok) throw new Error(`ListenBrainz HTTP ${response.status}`);
    return await response.json();
  } catch (fetchErr) {
    if (!isListenbrainzNetworkError(fetchErr)) throw fetchErr;

    try {
      const payload = typeof ctx.listenbrainzFetchJsonFallback === 'function'
        ? await ctx.listenbrainzFetchJsonFallback(url, { headers, meta, error: fetchErr })
        : await fetchListenbrainzJsonViaWget(url, headers);

      if (typeof ctx.pushLog === 'function' && meta.userId) {
        ctx.pushLog({
          level: 'warn',
          app: 'listenbrainz',
          action: 'fetch.fallback',
          message: `ListenBrainz ${meta.requestLabel || 'request'} fell back to wget for ${meta.userId}.`,
          meta: {
            playlistType: meta.playlistType || '',
            uuid: meta.uuid || '',
            error: summarizeListenbrainzError(fetchErr),
          },
        });
      }
      return payload;
    } catch (fallbackErr) {
      throw new Error(
        `ListenBrainz ${meta.requestLabel || 'request'} failed via fetch (${summarizeListenbrainzError(fetchErr)}) and wget (${summarizeListenbrainzError(fallbackErr)})`,
      );
    }
  }
}

function extractListenbrainzPlaylistUuid(playlistEntry) {
  const identifier = Array.isArray(playlistEntry?.playlist?.identifier)
    ? playlistEntry.playlist.identifier[0]
    : playlistEntry?.playlist?.identifier;
  const match = String(identifier || '').match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
  return match ? match[0] : '';
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
  const byText = buildPlaylistTrackLookup(masterTracks);
  const byRecordingMbid = new Map();
  for (const track of masterTracks || []) {
    const recordingMbid = String(track?.recordingMbid || '').trim().toLowerCase();
    if (!recordingMbid || !track?.ratingKey) continue;
    const existing = byRecordingMbid.get(recordingMbid);
    if (!existing) {
      byRecordingMbid.set(recordingMbid, track);
      continue;
    }
    if (isSyntheticPlaylistSeed(existing) && !isSyntheticPlaylistSeed(track)) {
      byRecordingMbid.set(recordingMbid, track);
    }
  }
  return {
    byText,
    byRecordingMbid: new Map([...byRecordingMbid.entries()].map(([mbid, track]) => [mbid, track.ratingKey])),
  };
}

function collectMatchedListenbrainzTracks(trackLookups, tracks, { strictMatch = false } = {}) {
  const ratingKeys = [];
  let mbidMatched = 0;
  let textMatched = 0;
  for (const track of tracks || []) {
    const recordingMbid = getListenbrainzRecordingMbid(track);
    let ratingKey = recordingMbid ? trackLookups.byRecordingMbid.get(recordingMbid) : '';
    if (ratingKey) {
      mbidMatched += 1;
      ratingKeys.push(ratingKey);
      continue;
    }
    ratingKey = lookupTrackRatingKey(trackLookups.byText, getListenbrainzTrackArtist(track), track?.title, { strictMatch });
    if (!ratingKey) continue;
    textMatched += 1;
    ratingKeys.push(ratingKey);
  }
  return { ratingKeys, mbidMatched, textMatched };
}

function shouldAppendUsernameToPlaylistTitles(smartConfig = {}) {
  return smartConfig?.appendUsernameToPlaylistTitles !== false;
}

function stripPlaylistUserSuffix(title, userId) {
  const rawTitle = String(title || '').trim();
  const rawUserId = String(userId || '').trim();
  if (!rawTitle) return '';
  if (!rawUserId) return rawTitle;
  const suffixPattern = new RegExp(`\\s*\\(${escapeRegex(rawUserId)}\\)$`, 'i');
  const possessivePattern = new RegExp(`^${escapeRegex(rawUserId)}'s\\s+`, 'i');
  return rawTitle.replace(suffixPattern, '').replace(possessivePattern, '').trim();
}

function getGeneratedPlaylistBaseTitle(config, playlistType, playlistKey, userId, fallbackTitle = '') {
  const rawKey = String(playlistKey || '').trim();
  const cleanFallbackTitle = stripPlaylistUserSuffix(fallbackTitle, userId);
  if (playlistType === DAILY_MIX_PLAYLIST_TYPE || rawKey === DAILY_MIX_PLAYLIST_KEY) return 'Daily Mix';
  if (playlistType === CURATORR_PLAYLIST_TYPE || rawKey === CURATORR_PLAYLIST_KEY) return 'Curatorr';
  if (playlistType === CRESCIVE_PLAYLIST_TYPE || rawKey === CRESCIVE_PLAYLIST_KEY) return 'Crescive Playlist';
  if (playlistType === CURATIVE_PLAYLIST_TYPE || rawKey === CURATIVE_PLAYLIST_KEY) return 'Curative Playlist';
  if (playlistType === 'lastfm-station' || rawKey.startsWith('lastfm:')) {
    const stationKey = rawKey.slice('lastfm:'.length);
    if (stationKey.startsWith('topTracks:')) {
      const period = stationKey.slice('topTracks:'.length);
      return `Last.fm Top Tracks (${TOP_TRACKS_PERIOD_LABELS[period] || period})`;
    }
    return LASTFM_STATION_LABELS[stationKey] ? `Last.fm ${LASTFM_STATION_LABELS[stationKey]}` : cleanFallbackTitle;
  }
  if (playlistType === 'listenbrainz-playlist' || rawKey.startsWith('listenbrainz:')) {
    const playlistKey = rawKey.slice('listenbrainz:'.length);
    return LISTENBRAINZ_PLAYLIST_LABELS[playlistKey] ? `ListenBrainz ${LISTENBRAINZ_PLAYLIST_LABELS[playlistKey]}` : cleanFallbackTitle;
  }
  if (playlistType === 'global' || rawKey.startsWith('global:')) {
    const playlistId = rawKey.slice('global:'.length);
    const match = Array.isArray(config?.globalPlaylists)
      ? config.globalPlaylists.find((entry) => String(entry?.id || '').trim() === playlistId)
      : null;
    return String(match?.name || '').trim() || cleanFallbackTitle;
  }
  if (playlistType === 'personal' || rawKey.startsWith('personal:')) {
    const playlistId = rawKey.slice('personal:'.length);
    const match = Array.isArray(config?.personalPlaylists)
      ? config.personalPlaylists.find((entry) => String(entry?.id || '').trim() === playlistId)
      : null;
    return String(match?.name || '').trim() || cleanFallbackTitle;
  }
  return cleanFallbackTitle;
}

function buildGeneratedPlaylistTitle(config, playlistType, playlistKey, userId, fallbackTitle = '') {
  const smartConfig = config?.smartPlaylist || {};
  const baseTitle = getGeneratedPlaylistBaseTitle(config, playlistType, playlistKey, userId, fallbackTitle);
  if (!baseTitle) return stripPlaylistUserSuffix(fallbackTitle, userId);
  return shouldAppendUsernameToPlaylistTitles(smartConfig)
    ? `${baseTitle} (${userId})`
    : baseTitle;
}

function resolveGeneratedPlaylistTitle(config, playlistType, playlistKey, userId, existing = null, fallbackTitle = '') {
  const titleOverride = String(existing?.titleOverride || '').trim();
  if (titleOverride) return titleOverride;
  return buildGeneratedPlaylistTitle(
    config,
    playlistType,
    playlistKey,
    userId,
    fallbackTitle || existing?.playlistTitle || '',
  );
}

function buildGeneratedPlaylistSearchTitles(config, playlistType, playlistKey, userId, fallbackTitle = '') {
  const titles = new Set();
  const baseTitle = getGeneratedPlaylistBaseTitle(config, playlistType, playlistKey, userId, fallbackTitle);
  if (baseTitle) {
    titles.add(baseTitle);
    if (userId) titles.add(`${baseTitle} (${userId})`);
  }
  if (playlistType === DAILY_MIX_PLAYLIST_TYPE || playlistKey === DAILY_MIX_PLAYLIST_KEY) {
    titles.add(`${userId}'s Daily Mix`);
  }
  if (playlistType === CURATORR_PLAYLIST_TYPE || playlistKey === CURATORR_PLAYLIST_KEY) {
    titles.add(`${userId}'s Curatorr`);
    titles.add(`${userId}'s Curatorr Playlist`);
  }
  if (playlistType === CRESCIVE_PLAYLIST_TYPE || playlistKey === CRESCIVE_PLAYLIST_KEY) {
    titles.add(`${userId}'s Crescive Playlist`);
  }
  if (playlistType === CURATIVE_PLAYLIST_TYPE || playlistKey === CURATIVE_PLAYLIST_KEY) {
    titles.add(`${userId}'s Curative Playlist`);
  }
  if (fallbackTitle) titles.add(String(fallbackTitle).trim());
  return [...titles].map((title) => String(title || '').trim()).filter(Boolean);
}

function normalizeArtworkMode(value) {
  const raw = String(value || '').trim().toLowerCase();
  return ['auto', 'preserve', 'custom'].includes(raw) ? raw : 'auto';
}

function normalizeArtworkState(value = {}) {
  const mode = normalizeArtworkMode(value?.artworkMode || value?.mode || 'auto');
  return {
    artworkMode: mode,
    customArtworkAsset: String(value?.customArtworkAsset || value?.customAsset || '').trim(),
    preservedArtworkAsset: String(value?.preservedArtworkAsset || value?.preservedAsset || '').trim(),
  };
}

function extractPlaylistDefinitionArtworkState(ctx, userPlexId, playlistType, playlistKey) {
  const config = ctx.loadConfig();
  if (playlistType === 'personal' || String(playlistKey || '').startsWith('personal:')) {
    const personalId = String(playlistKey || '').replace(/^personal:/, '').trim();
    const playlist = personalId ? getUserPersonalPlaylist(ctx.db, personalId, userPlexId) : null;
    return normalizeArtworkState(playlist?.rules?.artwork || {});
  }
  if (playlistType === 'global' || String(playlistKey || '').startsWith('global:')) {
    const globalId = String(playlistKey || '').replace(/^global:/, '').trim();
    const playlist = Array.isArray(config?.globalPlaylists)
      ? config.globalPlaylists.find((entry) => String(entry?.id || '').trim() === globalId)
      : null;
    return normalizeArtworkState(playlist?.rules?.artwork || {});
  }
  return normalizeArtworkState({});
}

function resolveSyncArtworkState(ctx, userPlexId, playlistType, playlistKey, existing = null) {
  if (playlistType === 'personal' || playlistType === 'global'
      || String(playlistKey || '').startsWith('personal:')
      || String(playlistKey || '').startsWith('global:')) {
    return extractPlaylistDefinitionArtworkState(ctx, userPlexId, playlistType, playlistKey);
  }
  return normalizeArtworkState(existing || {});
}

function buildLocalPlaylistArtworkAppUrl(ctx, assetName) {
  const info = getStoredPlaylistArtworkInfo(assetName);
  if (!info?.url) return '';
  const baseUrl = String(process.env.BASE_URL || `http://localhost:${process.env.PORT || 7676}`).trim();
  try {
    return ctx.buildAppApiUrl(baseUrl, info.url.replace(/^\//, '')).toString();
  } catch (_err) {
    return '';
  }
}

async function fetchPlexPlaylistMetadata(ctx, userPlexId, plexPlaylistId) {
  const config = ctx.loadConfig();
  const base = String(config?.plex?.url || '').trim().replace(/\/$/, '');
  const token = ctx.resolveUserPlexServerToken(config, userPlexId);
  if (!base || !token || !plexPlaylistId) return null;
  const response = await fetch(`${base}/playlists/${encodeURIComponent(String(plexPlaylistId || ''))}?X-Plex-Container-Start=0&X-Plex-Container-Size=1`, {
    headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
    signal: AbortSignal.timeout(PLEX_ENSURE_TIMEOUT_MS),
  });
  if (!response.ok) return null;
  const json = await response.json().catch(() => null);
  return json?.MediaContainer?.Metadata?.[0] || null;
}

async function snapshotPlexPlaylistArtworkAsset(ctx, userPlexId, plexPlaylistId, nameHint = '') {
  const config = ctx.loadConfig();
  const base = String(config?.plex?.url || '').trim().replace(/\/$/, '');
  const token = ctx.resolveUserPlexServerToken(config, userPlexId);
  if (!base || !token || !plexPlaylistId) return '';
  const playlistMeta = await fetchPlexPlaylistMetadata(ctx, userPlexId, plexPlaylistId).catch(() => null);
  const artworkPath = String(playlistMeta?.thumb || playlistMeta?.art || playlistMeta?.composite || '').trim();
  if (!artworkPath) return '';
  const response = await fetch(`${base}${artworkPath}`, {
    headers: ctx.buildPlexAuthHeaders(token),
    signal: AbortSignal.timeout(20_000),
  });
  if (!response.ok) return '';
  const arrayBuffer = await response.arrayBuffer();
  const buffer = Buffer.from(arrayBuffer);
  const mime = detectPlaylistArtworkMimeFromBuffer(buffer);
  if (!mime) return '';
  const ext = mime === 'image/png' ? 'png' : (mime === 'image/webp' ? 'webp' : 'jpg');
  return savePlaylistArtworkBuffer(buffer, ext, nameHint || plexPlaylistId, 'preserve');
}

async function applyPlexPlaylistArtworkAsset(ctx, userPlexId, plexPlaylistId, assetName) {
  const config = ctx.loadConfig();
  const base = String(config?.plex?.url || '').trim().replace(/\/$/, '');
  const token = ctx.resolveUserPlexServerToken(config, userPlexId);
  const artworkUrl = buildLocalPlaylistArtworkAppUrl(ctx, assetName);
  if (!base || !token || !plexPlaylistId || !artworkUrl) return false;
  const requestUrl = ctx.buildAppApiUrl(base, `library/metadata/${encodeURIComponent(String(plexPlaylistId || ''))}/thumb`);
  requestUrl.searchParams.set('url', artworkUrl);
  const response = await fetch(requestUrl.toString(), {
    method: 'PUT',
    headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
    signal: AbortSignal.timeout(20_000),
  });
  return response.ok;
}

function canOverrideDisabledPlaylist(options = {}, playlistKey = '') {
  if (options?.forceEnable || options?.ignoreDisabled) return true;
  const allowed = options?.allowPlaylistKeys;
  if (allowed instanceof Set) return allowed.has(playlistKey);
  if (Array.isArray(allowed)) return allowed.includes(playlistKey);
  return false;
}

function shouldSkipGeneratedPlaylistSync(db, userPlexId, playlistKey, options = {}) {
  if (!playlistKey || canOverrideDisabledPlaylist(options, playlistKey)) return false;
  const existing = listUserGeneratedPlaylists(db, userPlexId, { activeOnly: false })
    .find((entry) => entry.playlistKey === playlistKey);
  return Boolean(existing && existing.active === false);
}

function dedupeByRatingKey(items) {
  const seen = new Set();
  return items.filter((item) => {
    const key = String(item?.ratingKey || '').trim();
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function clampNumber(value, min, max, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.max(min, Math.min(max, num));
}

function createSelectionState(maxTracksPerArtist = 1) {
  return {
    maxTracksPerArtist: Math.max(1, Number(maxTracksPerArtist) || 1),
    seenRatingKeys: new Set(),
    seenLookupKeys: new Set(),
    artistCounts: new Map(),
  };
}

function tryAddSelectionTrack(state, selected, track) {
  const ratingKey = String(track?.ratingKey || '').trim();
  const artistName = String(track?.artistName || '').trim();
  if (!ratingKey || !artistName) return false;
  if (state.seenRatingKeys.has(ratingKey)) return false;
  const lookupKey = buildExactLookupKey(artistName, track?.trackTitle);
  if (lookupKey && state.seenLookupKeys.has(lookupKey)) return false;
  const artistKey = normalizeMatchArtist(artistName);
  const artistCount = state.artistCounts.get(artistKey) || 0;
  if (artistKey && artistCount >= state.maxTracksPerArtist) return false;
  state.seenRatingKeys.add(ratingKey);
  if (lookupKey) state.seenLookupKeys.add(lookupKey);
  if (artistKey) state.artistCounts.set(artistKey, artistCount + 1);
  selected.push(track);
  return true;
}

function addTracksFromPool(pool, targetCount, state, selected) {
  let added = 0;
  for (const track of pool || []) {
    if (selected.length >= targetCount) break;
    if (!tryAddSelectionTrack(state, selected, track)) continue;
    added += 1;
  }
  return added;
}

function buildTrackStatMap(db, userPlexId) {
  return new Map(
    db.prepare(`
      SELECT plex_rating_key, artist_name, play_count, tier, tier_weight,
             last_played_at, excluded_from_smart, manually_included
      FROM track_stats
      WHERE user_plex_id = ?
    `).all(userPlexId).map((row) => [
      String(row.plex_rating_key || ''),
      {
        artistName: String(row.artist_name || ''),
        playCount: Number(row.play_count || 0),
        tier: String(row.tier || 'curatorr'),
        tierWeight: Number(row.tier_weight || 0),
        lastPlayedAt: Number(row.last_played_at || 0),
        excludedFromSmart: Boolean(row.excluded_from_smart),
        manuallyIncluded: Boolean(row.manually_included),
      },
    ]),
  );
}

function buildArtistScoreMap(db, userPlexId) {
  return new Map(
    db.prepare('SELECT artist_name, ranking_score FROM artist_stats WHERE user_plex_id = ?').all(userPlexId)
      .map((row) => [String(row.artist_name || '').trim().toLowerCase(), Number(row.ranking_score || 5)]),
  );
}

function buildRecentPenalty(lastPlayedAt, cooldownDays = 0) {
  const cooldownMs = Math.max(0, Number(cooldownDays || 0)) * 24 * 60 * 60 * 1000;
  if (!cooldownMs || !lastPlayedAt) return 0;
  const age = Date.now() - Number(lastPlayedAt || 0);
  if (!Number.isFinite(age) || age >= cooldownMs) return 0;
  return (1 - (age / cooldownMs)) * 40;
}

function scoreSelectionTrack(track, extras = {}) {
  const stat = extras.stat || {};
  const artistScore = Number(extras.artistScore ?? 5);
  const playCount = Number(stat.playCount || 0);
  const tierWeight = Number(stat.tierWeight || 0);
  const ratingCount = Number(track?.ratingCount || 0);
  let score = 0;
  score += artistScore * Number(extras.artistWeight || 0);
  score += tierWeight * Number(extras.tierWeight || 0);
  score += Math.min(playCount, 25) * Number(extras.playWeight || 0);
  score += Math.min(ratingCount, 100) * Number(extras.ratingWeight || 0);
  score += Number(extras.sourceBonus || 0);
  if (extras.inLikedArtist) score += Number(extras.likedArtistBonus || 0);
  if (extras.inLikedGenre) score += Number(extras.likedGenreBonus || 0);
  if (extras.inPreviousPlaylist) score -= Number(extras.previousPenalty || 0);
  score -= buildRecentPenalty(stat.lastPlayedAt, extras.repeatCooldownDays);
  return score;
}

function normalizeDailyMixOptions(rawOptions = {}) {
  return {
    sortBy: normalizePlaylistBaseSort(rawOptions.sortBy, 'default'),
    favoriteLimit: clampNumber(rawOptions.favoriteLimit, 1, 100, 12),
    suggestedLimit: clampNumber(rawOptions.suggestedLimit, 0, 100, 8),
    freshLimit: clampNumber(rawOptions.freshLimit, 0, 100, 10),
    maxTracks: clampNumber(rawOptions.maxTracks, 1, 200, 24),
    maxTracksPerArtist: clampNumber(rawOptions.maxTracksPerArtist, 1, 5, 1),
    repeatCooldownDays: clampNumber(rawOptions.repeatCooldownDays, 0, 60, 5),
    featureProfile: normalizeFeaturePreset(rawOptions.featurePreset || rawOptions.featureProfile),
    camelotFocus: normalizeCamelotFocusInput(rawOptions.camelotFocus || ''),
    camelotMode: ['exact', 'adjacent', 'relative', 'harmonic'].includes(rawOptions.camelotMode) ? rawOptions.camelotMode : 'exact',
    useSonicOrdering: Boolean(rawOptions.useSonicOrdering),
    useLoudnessOrdering: Boolean(rawOptions.useLoudnessOrdering),
    loudnessLookahead: clampNumber(rawOptions.loudnessLookahead, 2, 10, 4),
    maxLoudnessStepDb: clampNumber(rawOptions.maxLoudnessStepDb, 1, 20, 6),
    sonicSeedCount: clampNumber(rawOptions.sonicSeedCount, 1, 10, 3),
    sonicExpansionLimit: clampNumber(rawOptions.sonicExpansionLimit, 1, 50, 12),
    sonicMaxDistance: clampNumber(rawOptions.sonicMaxDistance, 0, 1, 0.35),
    finalOrdering: resolvePlaylistFinalOrdering(rawOptions),
    sonicStrategy: ['balanced', 'favor-favorites', 'favor-discovery'].includes(rawOptions.sonicStrategy)
      ? rawOptions.sonicStrategy
      : 'balanced',
    trackFilters: rawOptions.trackFilters || null,
  };
}

function normalizeCuratorrOptions(rawOptions = {}) {
  return {
    sortBy: normalizePlaylistBaseSort(rawOptions.sortBy, 'default'),
    targetTracks: clampNumber(rawOptions.targetTracks, 1, 200, 48),
    discoveryRatio: clampNumber(rawOptions.discoveryRatio, 0, 0.8, 0.35),
    maxTracksPerArtist: clampNumber(rawOptions.maxTracksPerArtist, 1, 5, 2),
    repeatCooldownDays: clampNumber(rawOptions.repeatCooldownDays, 0, 90, 14),
    featureProfile: normalizeFeaturePreset(rawOptions.featurePreset || rawOptions.featureProfile),
    camelotFocus: normalizeCamelotFocusInput(rawOptions.camelotFocus || ''),
    camelotMode: ['exact', 'adjacent', 'relative', 'harmonic'].includes(rawOptions.camelotMode) ? rawOptions.camelotMode : 'exact',
    useSonicOrdering: Boolean(rawOptions.useSonicOrdering),
    useLoudnessOrdering: Boolean(rawOptions.useLoudnessOrdering),
    loudnessLookahead: clampNumber(rawOptions.loudnessLookahead, 2, 10, 4),
    maxLoudnessStepDb: clampNumber(rawOptions.maxLoudnessStepDb, 1, 20, 6),
    sonicSeedCount: clampNumber(rawOptions.sonicSeedCount, 1, 10, 4),
    sonicExpansionLimit: clampNumber(rawOptions.sonicExpansionLimit, 1, 50, 16),
    sonicMaxDistance: clampNumber(rawOptions.sonicMaxDistance, 0, 1, 0.35),
    finalOrdering: resolvePlaylistFinalOrdering(rawOptions),
    sonicStrategy: ['balanced', 'favor-favorites', 'favor-discovery'].includes(rawOptions.sonicStrategy)
      ? rawOptions.sonicStrategy
      : 'balanced',
    trackFilters: rawOptions.trackFilters || null,
  };
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

const FEATURE_PRESET_KEYS = ['none', 'club', 'driving', 'workout', 'chill', 'harmonic', 'wakeup', 'downtempo'];
const FEATURE_PROFILE_RULES = {
  none: [],
  club: [
    { field: 'bpm', operator: 'between', value: '118,132' },
    { field: 'energy', operator: 'gte', value: '0.70' },
    { field: 'danceability', operator: 'gte', value: '0.55' },
  ],
  driving: [
    { field: 'bpm', operator: 'between', value: '110,145' },
    { field: 'energy', operator: 'gte', value: '0.60' },
  ],
  workout: [
    { field: 'bpm', operator: 'between', value: '124,150' },
    { field: 'energy', operator: 'gte', value: '0.72' },
    { field: 'danceability', operator: 'gte', value: '0.55' },
  ],
  harmonic: [],
  wakeup: [
    { field: 'bpm', operator: 'between', value: '88,116' },
    { field: 'energy', operator: 'between', value: '0.38,0.68' },
    { field: 'danceability', operator: 'between', value: '0.45,0.78' },
  ],
  chill: [
    { field: 'bpm', operator: 'lte', value: '105' },
    { field: 'energy', operator: 'lte', value: '0.45' },
  ],
  downtempo: [
    { field: 'bpm', operator: 'between', value: '70,100' },
    { field: 'energy', operator: 'lte', value: '0.40' },
    { field: 'danceability', operator: 'lte', value: '0.65' },
  ],
};

function normalizeCamelotKey(value) {
  const raw = String(value || '').trim().toUpperCase();
  const match = raw.match(/^([1-9]|1[0-2])([AB])$/);
  return match ? `${match[1]}${match[2]}` : '';
}

function parseCamelotKeys(value) {
  return Array.from(new Set(String(value || '')
    .split(/[\s,;/|]+/)
    .map((entry) => normalizeCamelotKey(entry))
    .filter(Boolean)));
}

function normalizeCamelotFocusInput(value) {
  return parseCamelotKeys(value).join(', ');
}

function normalizePlaylistBaseSort(value, fallback = 'default') {
  const raw = String(value || '').trim();
  return PLAYLIST_BASE_SORT_MODES.has(raw) ? raw : fallback;
}

function normalizePlaylistFinalOrdering(value, fallback = 'none') {
  const raw = String(value || '').trim();
  return PLAYLIST_FINAL_ORDERING_MODES.has(raw) ? raw : fallback;
}

function resolvePlaylistFinalOrdering(options = {}) {
  const explicit = normalizePlaylistFinalOrdering(options.finalOrdering, '');
  if (explicit) return explicit;
  if (options.useSonicOrdering && options.useLoudnessOrdering) return 'plexSonicLoudness';
  if (options.useSonicOrdering) return 'plexSonic';
  if (options.useLoudnessOrdering) return 'loudness';
  return 'none';
}

function normalizeFeaturePreset(value) {
  const raw = String(value || '').trim().toLowerCase();
  return FEATURE_PRESET_KEYS.includes(raw) ? raw : 'none';
}

function buildCamelotSet(camelotKeys, mode = 'exact') {
  const focusKeys = parseCamelotKeys(camelotKeys);
  if (!focusKeys.length) return new Set();
  const result = new Set();
  for (const normalized of focusKeys) {
    result.add(normalized);
    const match = normalized.match(/^([1-9]|1[0-2])([AB])$/);
    if (!match) continue;
    const num = Number(match[1]);
    const letter = match[2];
    const otherLetter = letter === 'A' ? 'B' : 'A';
    if (mode === 'relative' || mode === 'harmonic') {
      result.add(`${num}${otherLetter}`);
    }
    if (mode === 'adjacent' || mode === 'harmonic') {
      const prev = num === 1 ? 12 : num - 1;
      const next = num === 12 ? 1 : num + 1;
      result.add(`${prev}${letter}`);
      result.add(`${next}${letter}`);
    }
  }
  return result;
}

const ANALYSIS_SORT_MODES = new Set(['bpmAsc', 'bpmDesc', 'energyAsc', 'energyDesc', 'danceabilityDesc', 'camelot', 'djFlow']);

function isAnalysisSortMode(sortBy = '') {
  return ANALYSIS_SORT_MODES.has(String(sortBy || '').trim());
}

function compareNullableAsc(a, b) {
  const aMissing = a == null || Number.isNaN(a);
  const bMissing = b == null || Number.isNaN(b);
  if (aMissing && bMissing) return 0;
  if (aMissing) return 1;
  if (bMissing) return -1;
  return a - b;
}

function compareNullableDesc(a, b) {
  const aMissing = a == null || Number.isNaN(a);
  const bMissing = b == null || Number.isNaN(b);
  if (aMissing && bMissing) return 0;
  if (aMissing) return 1;
  if (bMissing) return -1;
  return b - a;
}

function compareCamelotSort(a = '', b = '') {
  const aKey = normalizeCamelotKey(a);
  const bKey = normalizeCamelotKey(b);
  if (!aKey && !bKey) return 0;
  if (!aKey) return 1;
  if (!bKey) return -1;
  const aNum = Number.parseInt(aKey, 10);
  const bNum = Number.parseInt(bKey, 10);
  if (aNum !== bNum) return aNum - bNum;
  return aKey.localeCompare(bKey);
}

function baseRatingComparator(a, b) {
  return (b.rc - a.rc)
    || (b.tw - a.tw)
    || (b.pc - a.pc)
    || String(a.artistName || '').localeCompare(String(b.artistName || ''))
    || String(a.trackTitle || '').localeCompare(String(b.trackTitle || ''));
}

function shufflePlaylistTracks(tracks = []) {
  const items = Array.isArray(tracks) ? [...tracks] : [];
  for (let i = items.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [items[i], items[j]] = [items[j], items[i]];
  }
  return items;
}

function playlistAlbumLimitKey(track) {
  return String(track?.albumName || '').trim().toLowerCase();
}

function playlistArtistLimitKey(track) {
  return String(track?.artistName || '').trim().toLowerCase();
}

function selectLimitedPlaylistTracks(tracks = [], {
  sortBy = 'ratingCount',
  topNPerArtist = null,
  maxTracksPerAlbum = null,
  maxTracks = null,
  preserveOrder = false,
} = {}) {
  const artistLimit = topNPerArtist ? Math.max(1, Number(topNPerArtist)) : null;
  const albumLimit = maxTracksPerAlbum ? Math.max(1, Number(maxTracksPerAlbum)) : null;
  const trackLimit = maxTracks ? Math.max(1, Number(maxTracks)) : null;
  const orderedTracks = sortBy === 'random'
    ? shufflePlaylistTracks(tracks)
    : (preserveOrder ? [...tracks] : [...tracks].sort((a, b) => {
        if (sortBy === 'tierWeight') return (b.tw - a.tw) || (b.rc - a.rc) || (b.pc - a.pc);
        if (sortBy === 'playCount')  return (b.pc - a.pc) || (b.rc - a.rc) || (b.tw - a.tw);
        return baseRatingComparator(a, b);
      }));
  const artistCounts = new Map();
  const albumCounts = new Map();
  const selected = [];
  for (const track of orderedTracks) {
    const artistKey = playlistArtistLimitKey(track);
    if (artistLimit && artistKey && (artistCounts.get(artistKey) || 0) >= artistLimit) continue;
    const albumKey = playlistAlbumLimitKey(track);
    if (albumLimit && albumKey && (albumCounts.get(albumKey) || 0) >= albumLimit) continue;
    selected.push(track);
    if (artistLimit && artistKey) artistCounts.set(artistKey, (artistCounts.get(artistKey) || 0) + 1);
    if (albumLimit && albumKey) albumCounts.set(albumKey, (albumCounts.get(albumKey) || 0) + 1);
    if (trackLimit && selected.length >= trackLimit) break;
  }
  return selected;
}

function djFlowScore(current, candidate) {
  let score = 0;
  const currentCamelot = normalizeCamelotKey(current?.camelotKey || '');
  const candidateCamelot = normalizeCamelotKey(candidate?.camelotKey || '');
  if (!currentCamelot || !candidateCamelot) {
    score += 2;
  } else if (currentCamelot === candidateCamelot) {
    score += 0;
  } else if (buildCamelotSet(currentCamelot, 'harmonic').has(candidateCamelot)) {
    score += 0.75;
  } else {
    score += 4;
  }

  if (current?.bpm == null || candidate?.bpm == null) {
    score += 3;
  } else {
    score += Math.abs(candidate.bpm - current.bpm) / 8;
    if (candidate.bpm < current.bpm) score += 0.5;
  }

  if (current?.energy != null && candidate?.energy != null) {
    score += Math.abs(candidate.energy - current.energy) * 2;
  } else {
    score += 0.5;
  }
  return score;
}

export function sortPlaylistTracksByAnalysis(tracks = [], sortBy = 'bpmAsc') {
  const items = Array.isArray(tracks) ? [...tracks] : [];
  if (!items.length) return [];

  if (sortBy === 'bpmAsc') {
    return items.sort((a, b) => compareNullableAsc(a?.bpm, b?.bpm) || baseRatingComparator(a, b));
  }
  if (sortBy === 'bpmDesc') {
    return items.sort((a, b) => compareNullableDesc(a?.bpm, b?.bpm) || baseRatingComparator(a, b));
  }
  if (sortBy === 'energyAsc') {
    return items.sort((a, b) => compareNullableAsc(a?.energy, b?.energy) || compareNullableAsc(a?.bpm, b?.bpm) || baseRatingComparator(a, b));
  }
  if (sortBy === 'energyDesc') {
    return items.sort((a, b) => compareNullableDesc(a?.energy, b?.energy) || compareNullableAsc(a?.bpm, b?.bpm) || baseRatingComparator(a, b));
  }
  if (sortBy === 'danceabilityDesc') {
    return items.sort((a, b) => compareNullableDesc(a?.danceability, b?.danceability) || compareNullableDesc(a?.energy, b?.energy) || baseRatingComparator(a, b));
  }
  if (sortBy === 'camelot') {
    return items.sort((a, b) => compareCamelotSort(a?.camelotKey, b?.camelotKey) || compareNullableAsc(a?.bpm, b?.bpm) || baseRatingComparator(a, b));
  }
  if (sortBy !== 'djFlow') return items;

  const withAnalysis = [];
  const withoutAnalysis = [];
  for (const track of items) {
    if (track?.bpm != null || normalizeCamelotKey(track?.camelotKey || '')) withAnalysis.push(track);
    else withoutAnalysis.push(track);
  }
  if (withAnalysis.length < 2) {
    return withAnalysis.sort(baseRatingComparator).concat(withoutAnalysis.sort(baseRatingComparator));
  }

  const remaining = [...withAnalysis].sort((a, b) => compareNullableAsc(a?.bpm, b?.bpm) || compareNullableAsc(a?.energy, b?.energy) || baseRatingComparator(a, b));
  const ordered = [remaining.shift()];
  while (remaining.length) {
    const current = ordered[ordered.length - 1];
    remaining.sort((a, b) => djFlowScore(current, a) - djFlowScore(current, b) || compareNullableAsc(a?.bpm, b?.bpm) || baseRatingComparator(a, b));
    ordered.push(remaining.shift());
  }
  return ordered.concat(withoutAnalysis.sort(baseRatingComparator));
}

function hydratePlaylistTracks(masterTracks, tracks = [], fallbackTrackKeys = []) {
  const masterTrackMap = new Map((masterTracks || []).map((track) => [String(track?.ratingKey || '').trim(), track]));
  const sourceTracks = Array.isArray(tracks) && tracks.length
    ? tracks
    : fallbackTrackKeys.map((ratingKey) => ({ ratingKey }));
  return sourceTracks
    .map((track) => {
      const ratingKey = String(track?.ratingKey || '').trim();
      if (!ratingKey) return null;
      const masterTrack = masterTrackMap.get(ratingKey) || {};
      return { ...masterTrack, ...track, ratingKey };
    })
    .filter(Boolean);
}

function sortPlaylistTracksByMode(db, userPlexId, tracks = [], sortBy = 'default') {
  const mode = normalizePlaylistBaseSort(sortBy, 'default');
  if (mode === 'default' || mode === 'source') return Array.isArray(tracks) ? [...tracks] : [];
  if (mode === 'random') return shufflePlaylistTracks(tracks);
  const trackStats = buildTrackStatMap(db, userPlexId);
  const items = (Array.isArray(tracks) ? tracks : []).map((track) => {
    const stat = trackStats.get(String(track?.ratingKey || '').trim()) || {};
    return {
      ...track,
      rc: Number(track?.ratingCount || 0),
      tw: Number(stat?.tierWeight || 0),
      pc: Number(stat?.playCount || 0),
    };
  });
  if (isAnalysisSortMode(mode)) return sortPlaylistTracksByAnalysis(items, mode);
  return items.sort((a, b) => {
    if (mode === 'tierWeight') return (b.tw - a.tw) || (b.rc - a.rc) || (b.pc - a.pc);
    if (mode === 'playCount') return (b.pc - a.pc) || (b.rc - a.rc) || (b.tw - a.tw);
    return (b.rc - a.rc) || (b.tw - a.tw) || (b.pc - a.pc);
  });
}

function parseOptionalFeatureNumber(value) {
  if (value === '' || value == null) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function buildFeatureSelectionRules(options = {}) {
  const preset = normalizeFeaturePreset(options.featurePreset || options.featureProfile || 'none');
  const presetRules = [...(FEATURE_PROFILE_RULES[preset] || [])];
  const explicitRules = [];
  const pushRangeRule = (field, minValue, maxValue) => {
    const min = parseOptionalFeatureNumber(minValue);
    const max = parseOptionalFeatureNumber(maxValue);
    if (min == null && max == null) return;
    if (min != null && max != null) {
      explicitRules.push({ field, operator: 'between', value: `${Math.min(min, max)},${Math.max(min, max)}` });
      return;
    }
    if (min != null) {
      explicitRules.push({ field, operator: 'gte', value: String(min) });
      return;
    }
    explicitRules.push({ field, operator: 'lte', value: String(max) });
  };

  pushRangeRule('bpm', options.bpmMin, options.bpmMax);
  pushRangeRule('energy', options.energyMin, options.energyMax);
  pushRangeRule('danceability', options.danceabilityMin, options.danceabilityMax);

  if (!explicitRules.length) return presetRules;
  const explicitFields = new Set(explicitRules.map((rule) => rule.field));
  return [
    ...presetRules.filter((rule) => !explicitFields.has(rule.field)),
    ...explicitRules,
  ];
}

export function applyFeaturePresetFilters(tracks, options = {}) {
  let result = Array.isArray(tracks) ? tracks : [];
  const rules = buildFeatureSelectionRules(options);
  if (rules.length) {
    result = result.filter((track) => rules.every((rule) => trackMatchesRule(track, rule)));
  }
  const preset = normalizeFeaturePreset(options.featurePreset || options.featureProfile || 'none');
  const camelotFocus = options.camelotFocus || (preset === 'harmonic' ? '8A' : '');
  const camelotMode = options.camelotMode || (preset === 'harmonic' ? 'harmonic' : 'exact');
  const camelotSet = buildCamelotSet(camelotFocus, camelotMode);
  if (camelotSet.size) {
    result = result.filter((track) => camelotSet.has(normalizeCamelotKey(track?.camelotKey || '')));
  }
  return result;
}

export function buildFeaturePresetAvailability(tracks = []) {
  const sourceTracks = Array.isArray(tracks) ? tracks : [];
  const totalTracks = sourceTracks.length;
  const presets = {};
  for (const preset of FEATURE_PRESET_KEYS) {
    if (preset === 'none') {
      presets[preset] = {
        key: preset,
        readyTrackCount: totalTracks,
        enabled: totalTracks > 0,
        reason: totalTracks > 0 ? '' : 'No library tracks available yet.',
      };
      continue;
    }
    const readyTrackCount = applyFeaturePresetFilters(sourceTracks, { featurePreset: preset }).length;
    presets[preset] = {
      key: preset,
      readyTrackCount,
      enabled: readyTrackCount > 0,
      reason: readyTrackCount > 0 ? '' : (preset === 'harmonic'
        ? 'Camelot key data is not available yet.'
        : 'BPM and feature analysis data is not available yet.'),
    };
  }
  return {
    totalTracks,
    presets,
  };
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

function isDiscoveryLikeSource(source = '') {
  const value = String(source || '').trim().toLowerCase();
  return !['favorite', 'familiar'].includes(value);
}

function pickSonicSeeds(tracks, { seedCount = 3, strategy = 'balanced' } = {}) {
  const ordered = [...(tracks || [])]
    .filter((track) => track?.ratingKey)
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0)
      || String(a.artistName || '').localeCompare(String(b.artistName || ''))
      || String(a.trackTitle || '').localeCompare(String(b.trackTitle || '')));
  const familiar = ordered.filter((track) => !isDiscoveryLikeSource(track.source));
  const discovery = ordered.filter((track) => isDiscoveryLikeSource(track.source));
  const seeds = [];
  const seen = new Set();
  const take = (track) => {
    const key = String(track?.ratingKey || '').trim();
    if (!key || seen.has(key) || seeds.length >= seedCount) return false;
    seen.add(key);
    seeds.push(track);
    return true;
  };
  if (strategy === 'favor-favorites') {
    for (const track of [...familiar, ...discovery]) take(track);
    return seeds;
  }
  if (strategy === 'favor-discovery') {
    for (const track of [...discovery, ...familiar]) take(track);
    return seeds;
  }
  const maxLen = Math.max(familiar.length, discovery.length);
  for (let i = 0; i < maxLen && seeds.length < seedCount; i += 1) {
    if (familiar[i]) take(familiar[i]);
    if (discovery[i]) take(discovery[i]);
  }
  for (const track of ordered) {
    if (seeds.length >= seedCount) break;
    take(track);
  }
  return seeds;
}

function reorderTracksByRatingKeys(tracks, orderedKeys = []) {
  const order = new Map();
  for (const key of orderedKeys || []) {
    const cleanKey = String(key || '').trim();
    if (!cleanKey || order.has(cleanKey)) continue;
    order.set(cleanKey, order.size);
  }
  const ranked = [];
  const remaining = [];
  for (const track of tracks || []) {
    if (order.has(track?.ratingKey)) ranked.push(track);
    else remaining.push(track);
  }
  ranked.sort((a, b) => order.get(a.ratingKey) - order.get(b.ratingKey));
  return [...ranked, ...remaining];
}

function resolveTrackLoudness(track) {
  const value = Number(track?.loudness);
  return Number.isFinite(value) ? value : null;
}

function applyLoudnessSmoothing(tracks, options = {}) {
  const ordered = Array.isArray(tracks) ? [...tracks] : [];
  if (ordered.length < 3) return ordered;
  const lookahead = Math.max(2, Math.min(10, Number(options.loudnessLookahead || 4)));
  const maxStepDb = Math.max(1, Math.min(20, Number(options.maxLoudnessStepDb || 6)));
  const result = [ordered.shift()];

  while (ordered.length) {
    const previous = result[result.length - 1];
    const previousLoudness = resolveTrackLoudness(previous);
    if (previousLoudness == null) {
      result.push(ordered.shift());
      continue;
    }

    const windowSize = Math.min(lookahead, ordered.length);
    let bestIndex = -1;
    let bestDiff = Number.POSITIVE_INFINITY;
    for (let i = 0; i < windowSize; i += 1) {
      const candidateLoudness = resolveTrackLoudness(ordered[i]);
      if (candidateLoudness == null) continue;
      const diff = Math.abs(candidateLoudness - previousLoudness);
      if (diff < bestDiff) {
        bestDiff = diff;
        bestIndex = i;
      }
    }

    if (bestIndex <= 0 || !Number.isFinite(bestDiff) || bestDiff > maxStepDb) {
      result.push(ordered.shift());
      continue;
    }
    result.push(ordered.splice(bestIndex, 1)[0]);
  }

  return result;
}

function resolveTrackLibraryKey(track, masterTrackMap = new Map()) {
  const direct = String(track?.libraryKey || '').trim();
  if (direct) return direct;
  return String(masterTrackMap.get(String(track?.ratingKey || '').trim())?.libraryKey || '').trim();
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

const PLEX_ENSURE_TIMEOUT_MS = 15_000;
const PLEX_PLAYLIST_SEARCH_PAGE_SIZE = 200;
const plexPlaylistEnsureLocks = new Map();

function normalizePlexPlaylistTitle(title = '') {
  return String(title || '').trim().toLowerCase();
}

function withPlexPlaylistEnsureLock(lockKey, task) {
  const existing = plexPlaylistEnsureLocks.get(lockKey);
  if (existing) return existing;
  const promise = Promise.resolve()
    .then(task)
    .finally(() => {
      if (plexPlaylistEnsureLocks.get(lockKey) === promise) {
        plexPlaylistEnsureLocks.delete(lockKey);
      }
    });
  plexPlaylistEnsureLocks.set(lockKey, promise);
  return promise;
}

async function listPlexAudioPlaylists(ctx, base, token, { timeoutMs = PLEX_ENSURE_TIMEOUT_MS } = {}) {
  const playlists = [];
  let offset = 0;

  while (true) {
    const searchUrl = new URL(`${base}/playlists`);
    searchUrl.searchParams.set('playlistType', 'audio');
    searchUrl.searchParams.set('X-Plex-Container-Start', String(offset));
    searchUrl.searchParams.set('X-Plex-Container-Size', String(PLEX_PLAYLIST_SEARCH_PAGE_SIZE));

    const response = await fetch(searchUrl.toString(), {
      headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!response.ok) throw new Error(`List playlists failed: HTTP ${response.status}`);

    const json = await response.json();
    const container = json?.MediaContainer || {};
    const batch = Array.isArray(container?.Metadata) ? container.Metadata : [];
    playlists.push(...batch);

    const totalSize = Number(container?.totalSize || container?.size || 0);
    if (!batch.length) break;
    offset += batch.length;
    if ((totalSize > 0 && offset >= totalSize) || batch.length < PLEX_PLAYLIST_SEARCH_PAGE_SIZE) break;
  }

  return playlists;
}

function findPlexPlaylistByTitle(playlists = [], searchTitles = []) {
  const normalizedTitles = new Set(
    (Array.isArray(searchTitles) ? searchTitles : [])
      .map((title) => normalizePlexPlaylistTitle(title))
      .filter(Boolean),
  );
  if (!normalizedTitles.size) return null;
  return (Array.isArray(playlists) ? playlists : [])
    .find((playlist) => normalizedTitles.has(normalizePlexPlaylistTitle(playlist?.title || ''))) || null;
}

async function ensurePlexPlaylist(ctx, userPlexId, playlistKey, playlistTitle, machineId) {
  return withPlexPlaylistEnsureLock(`daily-mix:${userPlexId}:${playlistKey}`, async () => {
    const { loadConfig } = ctx;
    const config = loadConfig();
    const { url } = config.plex || {};
    const token = ctx.resolveUserPlexServerToken(config, userPlexId);
    if (!url || !token || !machineId) throw new Error('Plex is not configured for playlist sync');

    const existing = listUserGeneratedPlaylists(ctx.db, userPlexId, { activeOnly: false })
      .find((entry) => entry.playlistKey === playlistKey);

    const base = url.replace(/\/$/, '');

    // Existing DB record with a Plex ID — ensure Plex title matches (handles legacy format migration)
    if (existing?.plexPlaylistId) {
      try {
        await fetch(`${base}/playlists/${existing.plexPlaylistId}?title=${encodeURIComponent(playlistTitle)}`, {
          method: 'PUT',
          headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
          signal: AbortSignal.timeout(PLEX_ENSURE_TIMEOUT_MS),
        });
        if (existing.playlistTitle !== playlistTitle) {
          saveUserGeneratedPlaylist(ctx.db, userPlexId, { ...existing, playlistTitle, updatedAt: Date.now() });
        }
      } catch { /* rename is best-effort */ }
      return listUserGeneratedPlaylists(ctx.db, userPlexId, { activeOnly: false })
        .find((entry) => entry.playlistKey === playlistKey);
    }

    // DB record exists but has no Plex ID — search Plex by title (or legacy title) to avoid creating a duplicate
    const searchTitles = buildGeneratedPlaylistSearchTitles(
      config,
      DAILY_MIX_PLAYLIST_TYPE,
      playlistKey,
      userPlexId,
      playlistTitle,
    );
    if (existing && !existing.plexPlaylistId) {
      const all = await listPlexAudioPlaylists(ctx, base, token);
      const match = findPlexPlaylistByTitle(all, searchTitles);
      if (match?.ratingKey) {
        const matchedId = String(match.ratingKey);
        if (match.title !== playlistTitle) {
          try {
            await fetch(`${base}/playlists/${matchedId}?title=${encodeURIComponent(playlistTitle)}`, {
              method: 'PUT',
              headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
              signal: AbortSignal.timeout(PLEX_ENSURE_TIMEOUT_MS),
            });
          } catch { /* rename is best-effort */ }
        }
        saveUserGeneratedPlaylist(ctx.db, userPlexId, {
          playlistType: DAILY_MIX_PLAYLIST_TYPE, playlistKey,
          plexPlaylistId: matchedId,
          playlistTitle, algorithmVersion: 'phase2c-daily-mix',
          active: true, updatedAt: Date.now(),
        });
        return listUserGeneratedPlaylists(ctx.db, userPlexId, { activeOnly: false })
          .find((entry) => entry.playlistKey === playlistKey) || null;
      }
    }

    const createUrl = new URL(`${base}/playlists`);
    createUrl.searchParams.set('type', 'audio');
    createUrl.searchParams.set('title', playlistTitle);
    createUrl.searchParams.set('smart', '0');
    createUrl.searchParams.set('uri', `server://${machineId}/com.plexapp.plugins.library`);
    const response = await fetch(createUrl.toString(), {
      method: 'POST',
      headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      signal: AbortSignal.timeout(PLEX_ENSURE_TIMEOUT_MS),
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
  });
}

async function replacePlexPlaylistItems(ctx, userPlexId, plexPlaylistId, machineId, ratingKeys) {
  const config = ctx.loadConfig();
  const { url } = config.plex || {};
  const token = ctx.resolveUserPlexServerToken(config, userPlexId);
  const base = String(url || '').replace(/\/$/, '');
  if (!base || !token) throw new Error('Plex is not configured for playlist sync');

  const PLEX_PLAYLIST_TIMEOUT_MS = 30_000;
  const clearUrl = new URL(`${base}/playlists/${plexPlaylistId}/items`);
  const clearRes = await fetch(clearUrl.toString(), {
    method: 'DELETE',
    headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
    signal: AbortSignal.timeout(PLEX_PLAYLIST_TIMEOUT_MS),
  });
  if (!clearRes.ok) throw new Error(`Clear playlist items failed: HTTP ${clearRes.status}`);

  for (let i = 0; i < ratingKeys.length; i += 100) {
    const batch = ratingKeys.slice(i, i + 100);
    const uri = `server://${machineId}/com.plexapp.plugins.library/library/metadata/${batch.join(',')}`;
    const addUrl = new URL(`${base}/playlists/${plexPlaylistId}/items`);
    addUrl.searchParams.set('uri', uri);
    const response = await fetch(addUrl.toString(), {
      method: 'PUT',
      headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      signal: AbortSignal.timeout(PLEX_PLAYLIST_TIMEOUT_MS),
    });
    if (!response.ok) throw new Error(`Add playlist items failed: HTTP ${response.status}`);
  }
}

function collectListenbrainzUnmatchedSamples(trackLookups, tracks, limit = 5) {
  const samples = [];
  for (const track of tracks || []) {
    const artistName = getListenbrainzTrackArtist(track);
    const trackTitle = String(track?.title || '').trim();
    const recordingMbid = getListenbrainzRecordingMbid(track);
    if (recordingMbid && trackLookups.byRecordingMbid.get(recordingMbid)) continue;
    if (lookupTrackRatingKey(trackLookups.byText, artistName, trackTitle)) continue;
    samples.push(`${artistName || 'Unknown Artist'} - ${trackTitle || 'Unknown Title'}`);
    if (samples.length >= limit) break;
  }
  return samples;
}

// ─── Track exclusion filters ──────────────────────────────────────────────────

function trackMatchesRule(track, rule) {
  const { field, operator, value, caseSensitive } = rule || {};
  if (!field || !operator || value == null || value === '') return false;
  if (field === 'trackYear') return compareTrackYearRule(track, operator, value);
  if (field === 'originalReleaseDate') return compareTrackDateRule(track, operator, value);
  if (field === 'ratingCount') return compareTrackNumericRule(track?.ratingCount, operator, value);
  if (field === 'bpm') return compareTrackNumericRule(track?.bpm, operator, value);
  if (field === 'energy') return compareTrackNumericRule(track?.energy, operator, value);
  if (field === 'danceability') return compareTrackNumericRule(track?.danceability, operator, value);
  const fieldMap = {
    albumName:  track.albumName,
    trackTitle: track.trackTitle,
    artistName: track.artistName,
    filePath:   String(track.filePath || '').replace(/\\/g, '/'),
    musicalKey: track.musicalKey,
    camelotKey: track.camelotKey,
  };
  const raw = String(fieldMap[field] || '');
  const cs = Boolean(caseSensitive);
  const haystack = cs ? raw : raw.toLowerCase();
  const needle    = cs ? String(value) : String(value).toLowerCase();
  switch (operator) {
    case 'contains':         return haystack.includes(needle);
    case 'not_contains':     return !haystack.includes(needle);
    case 'starts_with':      return haystack.startsWith(needle);
    case 'not_starts_with':  return !haystack.startsWith(needle);
    case 'ends_with':        return haystack.endsWith(needle);
    case 'not_ends_with':    return !haystack.endsWith(needle);
    case 'equals':           return haystack === needle;
    case 'not_equals':       return haystack !== needle;
    case 'regex':            try { return new RegExp(value, cs ? '' : 'i').test(raw); } catch { return false; }
    case 'not_regex':        try { return !new RegExp(value, cs ? '' : 'i').test(raw); } catch { return false; }
    default:                 return false;
  }
}

function matchesSeasonalRule(track, rules) {
  const seasonalGenres = Array.isArray(rules?.seasonalGenres) ? rules.seasonalGenres.filter(Boolean) : [];
  const seasonalKeywords = Array.isArray(rules?.seasonalKeywords) ? rules.seasonalKeywords.filter(Boolean) : [];
  const seasonalExcludeGenres = Array.isArray(rules?.seasonalExcludeGenres) ? rules.seasonalExcludeGenres.filter(Boolean) : [];
  const seasonalExcludeKeywords = Array.isArray(rules?.seasonalExcludeKeywords) ? rules.seasonalExcludeKeywords.filter(Boolean) : [];
  const seasonalGenresMode = rules?.seasonalGenresMode === 'all' ? 'all' : 'any';
  const seasonalKeywordsMode = rules?.seasonalKeywordsMode === 'all' ? 'all' : 'any';
  if (!seasonalGenres.length && !seasonalKeywords.length && !seasonalExcludeGenres.length && !seasonalExcludeKeywords.length) return true;

  const genreMatch = seasonalGenres.length
    ? (seasonalGenresMode === 'all'
      ? seasonalGenres.every((genre) => (track?.genres || []).includes(genre))
      : (track?.genres || []).some((genre) => seasonalGenres.includes(genre)))
    : false;
  const genreExcluded = seasonalExcludeGenres.length
    ? (track?.genres || []).some((genre) => seasonalExcludeGenres.includes(genre))
    : false;

  const haystacks = [track?.trackTitle, track?.albumName]
    .map((value) => String(value || '').trim().toLowerCase())
    .filter(Boolean);
  const keywordMatch = seasonalKeywords.length ? seasonalKeywords[seasonalKeywordsMode === 'all' ? 'every' : 'some']((keyword) => {
    const needle = String(keyword || '').trim().toLowerCase();
    return needle && haystacks.some((haystack) => haystack.includes(needle));
  }) : false;
  const keywordExcluded = seasonalExcludeKeywords.some((keyword) => {
    const needle = String(keyword || '').trim().toLowerCase();
    return needle && haystacks.some((haystack) => haystack.includes(needle));
  });

  if (genreExcluded || keywordExcluded) return false;
  if (!seasonalGenres.length && !seasonalKeywords.length) return true;
  return genreMatch || keywordMatch;
}

function parseNumberRuleValue(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const num = Number(raw);
  return Number.isFinite(num) ? num : null;
}

function resolveTrackNumericValue(actualValue) {
  if (actualValue === '' || actualValue == null) return null;
  const actual = Number(actualValue);
  return Number.isFinite(actual) ? actual : null;
}

function parseNumericRange(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const parts = raw.split(/\s*\.\.\s*|\s*,\s*/).map((part) => part.trim()).filter(Boolean);
  if (parts.length !== 2) return null;
  const min = Number(parts[0]);
  const max = Number(parts[1]);
  if (!Number.isFinite(min) || !Number.isFinite(max)) return null;
  return { min: Math.min(min, max), max: Math.max(min, max) };
}

function compareTrackYearRule(track, operator, value) {
  const actual = resolveTrackNumericValue(track?.trackYear);
  if (actual == null) return false;
  return compareTrackNumericRule(actual, operator, value);
}

function compareTrackNumericRule(actualValue, operator, value) {
  const actual = resolveTrackNumericValue(actualValue);
  if (actual == null) return false;
  if (operator === 'between') {
    const range = parseNumericRange(value);
    return Boolean(range && actual >= range.min && actual <= range.max);
  }
  const expected = parseNumberRuleValue(value);
  if (!Number.isFinite(expected)) return false;
  switch (operator) {
    case 'equals': return actual === expected;
    case 'not_equals': return actual !== expected;
    case 'gt': return actual > expected;
    case 'gte': return actual >= expected;
    case 'lt': return actual < expected;
    case 'lte': return actual <= expected;
    default: return false;
  }
}

function parseComparableDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const match = raw.match(/^(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?$/);
  if (!match) return null;
  const year = Number(match[1] || 0);
  const month = Number(match[2] || 1);
  const day = Number(match[3] || 1);
  const dateValue = Date.UTC(year, Math.max(0, month - 1), day);
  if (!Number.isFinite(dateValue)) return null;
  return {
    raw,
    value: dateValue,
  };
}

function parseDateRange(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const parts = raw.split(/\s*\.\.\s*|\s*,\s*/).map((part) => part.trim()).filter(Boolean);
  if (parts.length !== 2) return null;
  const start = parseComparableDate(parts[0]);
  const end = parseComparableDate(parts[1]);
  if (!start || !end) return null;
  return start.value <= end.value
    ? { min: start.value, max: end.value }
    : { min: end.value, max: start.value };
}

function compareTrackDateRule(track, operator, value) {
  const actual = parseComparableDate(track?.originalReleaseDate || '');
  if (!actual) return false;
  if (operator === 'between') {
    const range = parseDateRange(value);
    return Boolean(range && actual.value >= range.min && actual.value <= range.max);
  }
  const expected = parseComparableDate(value);
  if (!expected) return false;
  switch (operator) {
    case 'equals': return actual.value === expected.value;
    case 'not_equals': return actual.value !== expected.value;
    case 'gt': return actual.value > expected.value;
    case 'gte': return actual.value >= expected.value;
    case 'lt': return actual.value < expected.value;
    case 'lte': return actual.value <= expected.value;
    default: return false;
  }
}

function compareTrackPreferenceForDedupe(a, b) {
  return (Number(b?.ratingCount || 0) - Number(a?.ratingCount || 0))
    || (Number(b?.viewCount || 0) - Number(a?.viewCount || 0))
    || (Number(b?.durationMs || 0) - Number(a?.durationMs || 0))
    || String(a?.albumName || '').localeCompare(String(b?.albumName || ''))
    || String(a?.ratingKey || '').localeCompare(String(b?.ratingKey || ''));
}

function buildReleaseDedupeTitleKey(track) {
  const artistKey = primaryArtistKey(track?.artistName || '');
  let titleKey = normalizeMatchTitleExact(track?.trackTitle || '');
  const releaseTagPattern = /(?:live(?:\s+at.*)?|radio edit|single edit|single version|album version|remaster(?:ed)?(?:\s+\d{4})?|mono|stereo|acoustic|demo|instrumental|mix|remix|version|edit|session(?:\s+version)?)/i;
  let previous = '';
  while (titleKey && titleKey !== previous) {
    previous = titleKey;
    titleKey = titleKey
      .replace(/\s*\([^)]*\)\s*$/g, '')
      .replace(/\s*\[[^\]]*\]\s*$/g, '')
      .trim();
    titleKey = titleKey
      .replace(new RegExp(`\\s+[-:–]\\s+${releaseTagPattern.source}.*$`, 'i'), '')
      .replace(new RegExp(`\\s+${releaseTagPattern.source}\\s*$`, 'i'), '')
      .trim();
  }
  return artistKey && titleKey ? `${artistKey}|${titleKey}` : '';
}

const LIKELY_DISTINCT_RECORDING_PATTERN = /\b(?:live|demo|reprise|instrumental|commentary|acoustic|remix|radio\s+edit|single\s+edit|session\s+version)\b/i;

function hasLikelyDistinctRecordingMarker(track) {
  return [track?.trackTitle, track?.albumName]
    .map((value) => String(value || '').trim())
    .some((value) => value && LIKELY_DISTINCT_RECORDING_PATTERN.test(value));
}

function hasLiveAlbumType(track) {
  const albumType = String(track?.albumType || track?.albumSubformat || track?.albumSubformats || '').trim();
  return /\blive\b/i.test(albumType);
}

function canDedupeByTrackDuration(left, right, toleranceMs) {
  const a = Number(left?.durationMs || 0) || 0;
  const b = Number(right?.durationMs || 0) || 0;
  return a > 0 && b > 0 && Math.abs(a - b) <= toleranceMs;
}

function summarizeDedupeTrack(track = {}) {
  return {
    ratingKey: String(track?.ratingKey || ''),
    artistName: String(track?.artistName || ''),
    trackTitle: String(track?.trackTitle || ''),
    albumName: String(track?.albumName || ''),
    albumType: String(track?.albumType || track?.albumSubformat || track?.albumSubformats || ''),
    recordingMbid: String(track?.recordingMbid || ''),
    durationMs: Number(track?.durationMs || 0),
    ratingCount: Number(track?.ratingCount || 0),
    viewCount: Number(track?.viewCount || 0),
    filePath: String(track?.filePath || ''),
  };
}

export function applyTrackFiltersWithReport(tracks, filterConfig, options = {}) {
  if (!filterConfig) {
    return { tracks, duplicateCount: 0, duplicateMatches: [] };
  }
  const {
    rules = [],
    excludeLibraryKeys = [],
    deduplicateByMbid = false,
    deduplicateByArtistTitle = false,
    deduplicateByDuration = false,
    deduplicateIgnoreLikelyVariants = false,
    deduplicateIgnoreLiveAlbums = false,
    includeFolders = [],
    excludeFolders = [],
  } = filterConfig;
  const duplicateLimit = Math.max(0, Math.min(10000, Number(options.duplicateLimit || 0) || 0));
  const duplicateMatches = [];
  let duplicateCount = 0;

  let result = tracks;

  if (excludeLibraryKeys.length) {
    const excludeSet = new Set(excludeLibraryKeys.map(String));
    result = result.filter((t) => !excludeSet.has(String(t.libraryKey || '')));
  }

  if (rules.length) {
    result = result.filter((t) => !rules.some((rule) => trackMatchesRule(t, rule)));
  }

  // Folder whitelist — keep only tracks whose file path contains one of the included sub-paths
  // Uses path-segment boundary matching: "/folder/" so "1" won't match "2021" or "Vol.1"
  if (includeFolders.length) {
    const folderNorms = includeFolders.map((f) => f.replace(/\\/g, '/').toLowerCase());
    result = result.filter((t) => {
      const norm = '/' + t.filePath.replace(/\\/g, '/').split('/').filter(Boolean).join('/') + '/';
      const lc = norm.toLowerCase();
      return folderNorms.some((f) => lc.includes('/' + f + '/'));
    });
  }

  // Folder blacklist — remove tracks whose file path contains any excluded sub-path
  if (excludeFolders.length) {
    const folderNorms = excludeFolders.map((f) => f.replace(/\\/g, '/').toLowerCase());
    result = result.filter((t) => {
      const norm = '/' + t.filePath.replace(/\\/g, '/').split('/').filter(Boolean).join('/') + '/';
      const lc = norm.toLowerCase();
      return !folderNorms.some((f) => lc.includes('/' + f + '/'));
    });
  }

  if (deduplicateByMbid || deduplicateByArtistTitle) {
    const keepKeys = new Set();
    const keptByRecordingMbid = new Set();
    const keptTrackByRecordingMbid = new Map();
    const keptByTitle = new Map();
    const durationToleranceMs = 5000;

    // Prefer the strongest library copy first, then suppress the duplicate keys the
    // caller has asked for.
    for (const track of [...result].sort(compareTrackPreferenceForDedupe)) {
      const recordingMbid = String(track?.recordingMbid || '').trim().toLowerCase();
      if (deduplicateByMbid && recordingMbid && keptByRecordingMbid.has(recordingMbid)) {
        duplicateCount += 1;
        if (duplicateMatches.length < duplicateLimit) {
          duplicateMatches.push({
            method: 'mbid',
            key: recordingMbid,
            reason: 'MusicBrainz recording ID',
            kept: summarizeDedupeTrack(keptTrackByRecordingMbid.get(recordingMbid) || {}),
            duplicate: summarizeDedupeTrack(track),
          });
        }
        continue;
      }

      let titleKey = deduplicateByArtistTitle ? buildReleaseDedupeTitleKey(track) : '';
      if (titleKey && deduplicateIgnoreLikelyVariants && hasLikelyDistinctRecordingMarker(track)) titleKey = '';
      if (titleKey && deduplicateIgnoreLiveAlbums && hasLiveAlbumType(track)) titleKey = '';
      let duplicateTitleTrack = null;
      if (titleKey) {
        const titleMatches = keptByTitle.get(titleKey) || [];
        duplicateTitleTrack = titleMatches.find((keptTrack) => {
          if (deduplicateIgnoreLikelyVariants && hasLikelyDistinctRecordingMarker(keptTrack)) return false;
          if (deduplicateIgnoreLiveAlbums && hasLiveAlbumType(keptTrack)) return false;
          if (deduplicateByDuration && !canDedupeByTrackDuration(track, keptTrack, durationToleranceMs)) return false;
          return true;
        }) || null;
        if (duplicateTitleTrack) {
          duplicateCount += 1;
          if (duplicateMatches.length < duplicateLimit) {
            duplicateMatches.push({
              method: 'artist_title',
              key: titleKey,
              reason: deduplicateByDuration ? 'Artist/title fuzzy match within 5 seconds' : 'Artist/title fuzzy match',
              kept: summarizeDedupeTrack(duplicateTitleTrack),
              duplicate: summarizeDedupeTrack(track),
            });
          }
          continue;
        }
      }

      if (deduplicateByMbid && recordingMbid) {
        keptByRecordingMbid.add(recordingMbid);
        keptTrackByRecordingMbid.set(recordingMbid, track);
      }
      if (titleKey) {
        const titleMatches = keptByTitle.get(titleKey) || [];
        titleMatches.push(track);
        keptByTitle.set(titleKey, titleMatches);
      }
      keepKeys.add(String(track?.ratingKey || ''));
    }
    result = result.filter((track) => keepKeys.has(String(track?.ratingKey || '')));
  }

  return {
    tracks: result,
    duplicateCount,
    duplicateMatches,
  };
}

export function applyTrackFilters(tracks, filterConfig) {
  return applyTrackFiltersWithReport(tracks, filterConfig).tracks;
}

function normalizeAlbumPopularityMode(value) {
  const normalized = String(value || '').trim();
  if (normalized === 'top3Only' || normalized === 'excludeTop3') return normalized;
  return 'all';
}

function normalizePopularityMode(value) {
  const normalized = String(value || '').trim();
  if (['top50', 'top25', 'top10', 'top5', 'custom'].includes(normalized)) return normalized;
  return 'all';
}

function normalizePopularityPercent(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.max(1, Math.min(100, Math.round(num)));
}

function resolvePopularityPercent(mode, value) {
  const normalized = normalizePopularityMode(mode);
  if (normalized === 'top50') return 50;
  if (normalized === 'top25') return 25;
  if (normalized === 'top10') return 10;
  if (normalized === 'top5') return 5;
  if (normalized === 'custom') return normalizePopularityPercent(value);
  return null;
}

function albumPopularityGroupKey(track) {
  const artistKey = String(track?.artistName || '').trim().toLowerCase();
  const albumKey = String(track?.albumName || '').trim().toLowerCase();
  return artistKey && albumKey ? `${artistKey}|${albumKey}` : '';
}

function buildAlbumPopularTopTrackKeySet(tracks, limit = 3) {
  const groups = new Map();
  for (const track of tracks || []) {
    const groupKey = albumPopularityGroupKey(track);
    const ratingKey = String(track?.ratingKey || '').trim();
    if (!groupKey || !ratingKey) continue;
    if (!groups.has(groupKey)) groups.set(groupKey, []);
    groups.get(groupKey).push(track);
  }
  const keepKeys = new Set();
  for (const groupTracks of groups.values()) {
    groupTracks
      .slice()
      .sort((a, b) => (Number(b?.ratingCount || 0) - Number(a?.ratingCount || 0))
        || (Number(b?.viewCount || 0) - Number(a?.viewCount || 0))
        || String(a?.trackTitle || '').localeCompare(String(b?.trackTitle || ''))
        || String(a?.ratingKey || '').localeCompare(String(b?.ratingKey || '')))
      .slice(0, Math.max(1, Number(limit) || 3))
      .forEach((track) => {
        if (Number(track?.ratingCount || 0) > 0) keepKeys.add(String(track.ratingKey || ''));
      });
  }
  return keepKeys;
}

function applyAlbumPopularityMode(tracks, mode, allTracks = tracks) {
  const normalized = normalizeAlbumPopularityMode(mode);
  if (normalized === 'all') return tracks;
  const popularKeys = buildAlbumPopularTopTrackKeySet(allTracks, 3);
  if (!popularKeys.size) return normalized === 'excludeTop3' ? tracks : [];
  return tracks.filter((track) => {
    const isPopular = popularKeys.has(String(track?.ratingKey || ''));
    return normalized === 'top3Only' ? isPopular : !isPopular;
  });
}

function applyAbsolutePopularityMode(tracks, mode, percentValue) {
  const percent = resolvePopularityPercent(mode, percentValue);
  if (!percent || percent >= 100) return tracks;
  const sorted = (tracks || []).slice().sort((a, b) => (Number(b?.ratingCount || 0) - Number(a?.ratingCount || 0))
    || (Number(b?.viewCount || 0) - Number(a?.viewCount || 0))
    || String(a?.trackTitle || '').localeCompare(String(b?.trackTitle || ''))
    || String(a?.ratingKey || '').localeCompare(String(b?.ratingKey || '')));
  const keepCount = Math.max(1, Math.ceil(sorted.length * (percent / 100)));
  const keepKeys = new Set(sorted.slice(0, keepCount).map((track) => String(track?.ratingKey || '')).filter(Boolean));
  return tracks.filter((track) => keepKeys.has(String(track?.ratingKey || '')));
}

// ─── Smart playlist build pipeline ───────────────────────────────────────────

// Strip featured-artist suffixes so "Eminem" and "Eminem f/ Eye-Kyu" resolve to the same primary key.
function primaryArtistKey(name) {
  return String(name || '').toLowerCase().replace(/\s+(?:f\/|feat\.?|ft\.?|featuring)\s.*/i, '').trim();
}

function buildSmartPlaylistPayload(db, userId, playlistCfg, masterTracks) {
  const albumEligibleTracks = applyAlbumPopularityMode(masterTracks, playlistCfg?.albumPopularityMode, getMasterTracks(db));
  const eligibleTracks = applyAbsolutePopularityMode(albumEligibleTracks, playlistCfg?.popularityMode, playlistCfg?.popularityPercent);
  if (!eligibleTracks.length) return { ratingKeys: [], trackList: [] };
  const capMultiplier   = Math.max(0, Math.min(1, playlistCfg.capMultiplier ?? 1.0));
  const favArtistCap    = Math.max(0, Math.min(1, playlistCfg.favouriteArtistTrackPct  ?? 1.0));
  const favGenreArtPct  = Math.max(0, Math.min(1, playlistCfg.favouriteGenreArtistPct  ?? 1.0));
  const favGenreTrackCap= Math.max(0, Math.min(1, playlistCfg.favouriteGenreTrackPct   ?? 1.0));
  const otherArtPct     = Math.max(0, Math.min(1, playlistCfg.otherGenreArtistPct      ?? 1.0));
  const otherTrackCap   = Math.max(0, Math.min(1, playlistCfg.otherGenreTrackPct       ?? 1.0));

  // User preferences: liked artists (fav artist bucket) and liked genres (fav genre bucket)
  const prefs = getUserPreferences(db, userId) || {};
  const likedArtistsSet = new Set((prefs.likedArtists || []).map((a) => a.toLowerCase()));
  const likedGenresSet  = new Set((prefs.likedGenres  || []).map((g) => g.toLowerCase()));

  // Artist scores (0–10). Defaults to 5 for artists with no play history.
  const artistScoreRows = db.prepare('SELECT artist_name, ranking_score FROM artist_stats WHERE user_plex_id = ?').all(userId);
  const artistScoreMap  = new Map(artistScoreRows.map((r) => [r.artist_name.toLowerCase(), r.ranking_score]));

  // Per-track stats: Curatorr tier weight (belter > decent > half-decent), exclusion flags
  const statRows = db.prepare('SELECT plex_rating_key, tier_weight, excluded_from_smart, manually_included FROM track_stats WHERE user_plex_id = ?').all(userId);
  const statMap  = new Map(statRows.map((r) => [r.plex_rating_key, r]));

  // Group master tracks by artist and determine each artist's genres
  const artistMap = new Map();
  for (const t of eligibleTracks) {
    const lower = t.artistName.toLowerCase();
    if (!artistMap.has(lower)) artistMap.set(lower, { name: t.artistName, tracks: [], genres: new Set() });
    const entry = artistMap.get(lower);
    entry.tracks.push(t);
    for (const g of (t.genres || [])) entry.genres.add(g.toLowerCase());
  }

  // Categorise each artist: favArtist > favGenre > other
  // For genre bucket we apply an artist-count filter using ranking_score rank
  const favGenreArtists = [], otherArtists = [];
  for (const [lower, { genres }] of artistMap) {
    if (likedArtistsSet.has(lower)) continue; // handled separately
    const inFavGenre = likedGenresSet.size > 0 && [...genres].some((g) => likedGenresSet.has(g));
    if (inFavGenre) favGenreArtists.push(lower);
    else otherArtists.push(lower);
  }
  // Sort each bucket by score descending and apply the artist-count cap
  // Unplayed artists (no score) sit at the bottom of each bucket's ranking
  const byScore = (a, b) => (artistScoreMap.get(b) ?? 0) - (artistScoreMap.get(a) ?? 0);
  const favGenreIncluded = new Set(favGenreArtists.sort(byScore).slice(0, Math.ceil(favGenreArtists.length * favGenreArtPct)));
  const otherIncluded    = new Set(otherArtists.sort(byScore).slice(0, Math.ceil(otherArtists.length * otherArtPct)));

  const seen = new Set();       // by ratingKey
  const seenTitles = new Set(); // by artist|title — prevents the same song appearing multiple times from different albums
  const ratingKeys = [], trackList = [];

  const addArtistTracks = (lower, name, tracks, trackCap) => {
    const rawScore = artistScoreMap.get(lower);
    // Unplayed artists (no history) use the category pct directly as the starting position.
    // Artists with play history are scaled by their score so inclusions rise/fall over time.
    const scoreFactor = rawScore != null ? Math.max(0, rawScore / 10) : 1.0;
    if (scoreFactor <= 0) return;
    const pct   = scoreFactor * trackCap * capMultiplier;
    const count = Math.max(1, Math.ceil(tracks.length * pct));
    const sorted = tracks.slice().sort((a, b) => {
      const twa = statMap.get(a.ratingKey)?.tier_weight ?? 0;
      const twb = statMap.get(b.ratingKey)?.tier_weight ?? 0;
      if (twb !== twa) return twb - twa;
      return (b.ratingCount || 0) - (a.ratingCount || 0);
    });
    let added = 0;
    for (const t of sorted) {
      if (added >= count) break;
      if (seen.has(t.ratingKey)) continue;
      const titleKey = `${primaryArtistKey(lower)}|${String(t.trackTitle || '').trim().toLowerCase()}`;
      if (seenTitles.has(titleKey)) continue;
      const stat = statMap.get(t.ratingKey);
      if (stat?.excluded_from_smart === 1 && stat?.manually_included !== 1) continue;
      seen.add(t.ratingKey);
      seenTitles.add(titleKey);
      ratingKeys.push(t.ratingKey);
      trackList.push({ ratingKey: t.ratingKey, artistName: name });
      added++;
    }
  };

  for (const [lower, { name, tracks }] of artistMap) {
    if (likedArtistsSet.has(lower)) {
      addArtistTracks(lower, name, tracks, favArtistCap);
    } else if (favGenreIncluded.has(lower)) {
      addArtistTracks(lower, name, tracks, favGenreTrackCap);
    } else if (otherIncluded.has(lower)) {
      addArtistTracks(lower, name, tracks, otherTrackCap);
    }
    // If no preferences at all, fall through to the fallback below
  }

  // Fallback: if user has no liked artists/genres, treat every artist as "other" at full cap
  if (!likedArtistsSet.size && !likedGenresSet.size) {
    for (const [lower, { name, tracks }] of artistMap) {
      addArtistTracks(lower, name, tracks, otherTrackCap);
    }
  }

  // Always include manually pinned tracks regardless of artist score or bucket
  for (const [ratingKey, stat] of statMap) {
    if (stat.manually_included !== 1 || seen.has(ratingKey)) continue;
    const master = eligibleTracks.find((t) => t.ratingKey === ratingKey);
    if (!master) continue;
    seen.add(ratingKey);
    ratingKeys.push(ratingKey);
    trackList.push({ ratingKey, artistName: master.artistName });
  }

  return { ratingKeys, trackList };
}


// ─── Plex playlist helper (generic, reusable) ─────────────────────────────────

async function ensureGeneratedPlaylist(ctx, userId, playlistType, playlistKey, playlistTitle, machineId) {
  return withPlexPlaylistEnsureLock(`generated:${userId}:${playlistKey}`, async () => {
    const existing = listUserGeneratedPlaylists(ctx.db, userId, { activeOnly: false })
      .find((e) => e.playlistKey === playlistKey);

    const config = ctx.loadConfig();
    const { url } = config.plex || {};
    const token = ctx.resolveUserPlexServerToken(config, userId);
    if (!url || !token || !machineId) throw new Error('Plex is not configured for playlist creation');

    const base = url.replace(/\/$/, '');

    // If we have an existing record, ensure the Plex playlist title matches (handles legacy name migration).
    if (existing?.plexPlaylistId) {
      let verifyRes;
      try {
        verifyRes = await fetch(`${base}/playlists/${existing.plexPlaylistId}?X-Plex-Container-Start=0&X-Plex-Container-Size=1`, {
          headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
          signal: AbortSignal.timeout(PLEX_ENSURE_TIMEOUT_MS),
        });
      } catch (err) {
        ctx.pushLog?.({
          level: 'warn',
          app: 'playlist',
          action: 'generated.verify',
          message: `Generated playlist "${playlistTitle}" for ${userId} could not be verified; keeping existing Plex id ${existing.plexPlaylistId}.`,
          meta: { playlistKey, plexPlaylistId: existing.plexPlaylistId, error: err?.message || String(err || '') },
        });
        return listUserGeneratedPlaylists(ctx.db, userId, { activeOnly: false }).find((e) => e.playlistKey === playlistKey) || existing;
      }

      if (verifyRes.ok) {
        try {
          await fetch(`${base}/playlists/${existing.plexPlaylistId}?title=${encodeURIComponent(playlistTitle)}`, {
            method: 'PUT',
            headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
            signal: AbortSignal.timeout(PLEX_ENSURE_TIMEOUT_MS),
          });
          if (existing.playlistTitle !== playlistTitle) {
            saveUserGeneratedPlaylist(ctx.db, userId, { ...existing, playlistTitle, updatedAt: Date.now() });
          }
        } catch { /* rename is best-effort */ }
        return listUserGeneratedPlaylists(ctx.db, userId, { activeOnly: false }).find((e) => e.playlistKey === playlistKey);
      }

      if (verifyRes.status === 404) {
        saveUserGeneratedPlaylist(ctx.db, userId, {
          ...existing,
          plexPlaylistId: '',
          updatedAt: Date.now(),
        });
        ctx.pushLog?.({
          level: 'warn',
          app: 'playlist',
          action: 'generated.stale',
          message: `Generated playlist "${playlistTitle}" for ${userId} had a stale Plex id (${existing.plexPlaylistId}); recreating.`,
          meta: { playlistKey, stalePlexPlaylistId: existing.plexPlaylistId },
        });
      } else {
        throw new Error(`Verify playlist failed: HTTP ${verifyRes.status}`);
      }
    }

    // Search Plex by title before creating — prevents duplicates when the DB has been reset.
    // Also checks the legacy name format (e.g. "TJWhiteStar's Crescive Playlist") for migration.
    const searchTitles = buildGeneratedPlaylistSearchTitles(
      config,
      playlistType,
      playlistKey,
      userId,
      playlistTitle,
    );
    const all = await listPlexAudioPlaylists(ctx, base, token);
    const match = findPlexPlaylistByTitle(all, searchTitles);
    if (match?.ratingKey) {
      const matchedId = String(match.ratingKey);
      // Rename in Plex if found under the legacy title
      if (match.title !== playlistTitle) {
        try {
          await fetch(`${base}/playlists/${matchedId}?title=${encodeURIComponent(playlistTitle)}`, {
            method: 'PUT',
            headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
            signal: AbortSignal.timeout(PLEX_ENSURE_TIMEOUT_MS),
          });
        } catch { /* rename is best-effort */ }
      }
      saveUserGeneratedPlaylist(ctx.db, userId, {
        playlistType, playlistKey, plexPlaylistId: matchedId,
        playlistTitle, algorithmVersion: ALGORITHM_VERSION, active: true, updatedAt: Date.now(),
      });
      return listUserGeneratedPlaylists(ctx.db, userId, { activeOnly: false }).find((e) => e.playlistKey === playlistKey);
    }

    const createUrl = new URL(`${base}/playlists`);
    createUrl.searchParams.set('type', 'audio');
    createUrl.searchParams.set('title', playlistTitle);
    createUrl.searchParams.set('smart', '0');
    createUrl.searchParams.set('uri', `server://${machineId}/com.plexapp.plugins.library`);
    const res = await fetch(createUrl.toString(), {
      method: 'POST',
      headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
      signal: AbortSignal.timeout(PLEX_ENSURE_TIMEOUT_MS),
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
  });
}

// ─── Shared rule-based playlist filtering ────────────────────────────────────
// Used by both Plex and Jellyfin paths for global/personal playlist rule eval.

function _getUserStatMaps(db, userId) {
  const artistMap = new Map(
    db.prepare('SELECT artist_name, ranking_score FROM artist_stats WHERE user_plex_id = ?').all(userId)
      .map((r) => [r.artist_name.toLowerCase(), r.ranking_score]),
  );
  const trackMap = new Map(
    db.prepare('SELECT plex_rating_key, tier, tier_weight, play_count FROM track_stats WHERE user_plex_id = ?').all(userId)
      .map((r) => [r.plex_rating_key, r]),
  );
  return { artistMap, trackMap };
}

// Blend — average: aggregate numeric scores across all blend users.
// Artist score = mean ranking_score; track tier = majority vote; tier_weight = mean.
function _buildAverageStatMaps(db, userIds) {
  const artistGroups = new Map();
  const trackGroups  = new Map();
  for (const uid of userIds) {
    db.prepare('SELECT artist_name, ranking_score FROM artist_stats WHERE user_plex_id = ?').all(uid)
      .forEach((r) => {
        const key = r.artist_name.toLowerCase();
        if (!artistGroups.has(key)) artistGroups.set(key, []);
        artistGroups.get(key).push(r.ranking_score);
      });
    db.prepare('SELECT plex_rating_key, tier, tier_weight, play_count FROM track_stats WHERE user_plex_id = ?').all(uid)
      .forEach((r) => {
        if (!trackGroups.has(r.plex_rating_key)) trackGroups.set(r.plex_rating_key, { weights: [], tiers: [], playCounts: [] });
        const d = trackGroups.get(r.plex_rating_key);
        d.weights.push(r.tier_weight || 0);
        d.tiers.push(r.tier || 'curatorr');
        d.playCounts.push(r.play_count || 0);
      });
  }
  const artistMap = new Map();
  for (const [name, scores] of artistGroups) {
    artistMap.set(name, scores.reduce((a, b) => a + b, 0) / scores.length);
  }
  const trackMap = new Map();
  for (const [key, data] of trackGroups) {
    const avgWeight    = data.weights.reduce((a, b) => a + b, 0) / data.weights.length;
    const avgPlayCount = Math.round(data.playCounts.reduce((a, b) => a + b, 0) / data.playCounts.length);
    const tierCounts   = {};
    for (const t of data.tiers) tierCounts[t] = (tierCounts[t] || 0) + 1;
    const tier = Object.entries(tierCounts).sort((a, b) => b[1] - a[1])[0][0];
    trackMap.set(key, { tier, tier_weight: avgWeight, play_count: avgPlayCount });
  }
  return { artistMap, trackMap };
}

// Core filter: given pre-built artist/track maps, apply all rules and return rating keys.
function _applyFilterRules(db, masterTracks, artistMap, trackMap, rules, config) {
  const smartSettings = config.smartPlaylist || {};
  const skipRank   = Number(smartSettings.artistSkipRank   ?? 2);
  const belterRank = Number(smartSettings.artistBelterRank ?? 8);

  function classifyArtist(score) {
    if (score === null || score === undefined) return 'unranked';
    if (score >= belterRank) return 'belter';
    if (score >= 5) return 'decent';
    if (score > skipRank) return 'halfDecent';
    return 'skip';
  }

  const artistTierFilter = parseTriStateFilter(rules.artistTiers);
  const trackTierFilter  = parseTriStateFilter(rules.trackTiers);
  const gf = parseTriStateFilter(rules.genres);
  const mf = parseTriStateFilter(rules.moods);
  const agf = parseTriStateFilter(rules.albumGenres);
  const asf = parseTriStateFilter(rules.albumStyles);
  const amf = parseTriStateFilter(rules.albumMoods);
  const tf = parseTriStateFilter(rules.tags);
  const df = parseTriStateFilter(rules.decades);
  const artistTagMap = (tf.include || tf.exclude) ? getArtistTagMap(db) : null;
  const topN = rules.topNPerArtist ? Math.max(1, Number(rules.topNPerArtist)) : null;
  const maxT = rules.maxTracks     ? Math.max(1, Number(rules.maxTracks))     : null;
  const maxPerAlbum = rules.maxTracksPerAlbum ? Math.max(1, Number(rules.maxTracksPerAlbum)) : null;
  const sortBy = rules.sortBy || 'ratingCount';
  const eligibleTracks = applyFeaturePresetFilters(masterTracks, rules);
  const eligibleTrackMap = new Map(eligibleTracks.map((track) => [track.ratingKey, track]));

  function matchesTriStateValues(values, filter) {
    const items = Array.isArray(values) ? values : [];
    if (filter.include && !(filter.includeMode === 'all'
      ? Array.from(filter.include).every((value) => items.includes(value))
      : items.some((value) => filter.include.has(value)))) return false;
    if (filter.exclude && items.some((value) => filter.exclude.has(value))) return false;
    return true;
  }

  const matchedTracks = [];
  for (const t of eligibleTracks) {
    const score = artistMap.get((t.artistName || '').toLowerCase()) ?? null;
    const artistTier = classifyArtist(score);
    if (artistTierFilter.include && !artistTierFilter.include.has(artistTier)) continue;
    if (artistTierFilter.exclude && artistTierFilter.exclude.has(artistTier)) continue;

    const stat = trackMap.get(t.ratingKey);
    const rawTier = stat?.tier || 'curatorr';
    const normTier = rawTier === 'half-decent' ? 'halfDecent' : rawTier === 'curatorr' ? 'unplayed' : rawTier;
    if (trackTierFilter.include && !trackTierFilter.include.has(normTier)) continue;
    if (trackTierFilter.exclude && trackTierFilter.exclude.has(normTier)) continue;

    if (!matchesTriStateValues(t.genres || [], gf)) continue;
    if (!matchesTriStateValues(t.moods || [], mf)) continue;
    if (!matchesTriStateValues(t.albumGenres || [], agf)) continue;
    if (!matchesTriStateValues(t.albumStyles || [], asf)) continue;
    if (!matchesTriStateValues(t.albumMoods || [], amf)) continue;
    if (tf.include || tf.exclude) {
      const artistTags = getEffectiveTrackTags(t, artistTagMap);
      if (tf.include && !(tf.includeMode === 'all'
        ? Array.from(tf.include).every((tag) => artistTags.includes(tag))
        : artistTags.some((tag) => tf.include.has(tag)))) continue;
      if (tf.exclude && artistTags.some((tag) => tf.exclude.has(tag))) continue;
    }
    if (df.include || df.exclude) {
      const trackDecade = getTrackDecadeTag(t);
      if (df.include && !df.include.has(trackDecade)) continue;
      if (df.exclude && trackDecade && df.exclude.has(trackDecade)) continue;
    }
    if (!matchesSeasonalRule(t, rules)) continue;
    matchedTracks.push({
      ratingKey: t.ratingKey,
      artistName: t.artistName || '',
      trackTitle: t.trackTitle || '',
      albumName: t.albumName || '',
      ratingCount: Number(t.ratingCount || 0),
      viewCount: Number(t.viewCount || 0),
      rc: t.ratingCount || 0,
      tw: stat?.tier_weight || 0,
      pc: stat?.play_count || 0,
    });
  }

  const albumFilteredTracks = applyAlbumPopularityMode(matchedTracks, rules?.albumPopularityMode, masterTracks);
  const filteredTracks = applyAbsolutePopularityMode(albumFilteredTracks, rules?.popularityMode, rules?.popularityPercent);

  let ratingKeys = [];
  if (isAnalysisSortMode(sortBy)) {
    const selectedTracks = filteredTracks.map((track) => {
      const fullTrack = eligibleTrackMap.get(track.ratingKey) || {};
      return {
        ...track,
        artistName: fullTrack.artistName || '',
        trackTitle: fullTrack.trackTitle || '',
        albumName: fullTrack.albumName || '',
        bpm: fullTrack.bpm ?? null,
        camelotKey: fullTrack.camelotKey || '',
        energy: fullTrack.energy ?? null,
        danceability: fullTrack.danceability ?? null,
      };
    });
    const orderedTracks = sortPlaylistTracksByAnalysis(selectedTracks, sortBy);
    const limitedTracks = selectLimitedPlaylistTracks(orderedTracks, {
      sortBy,
      topNPerArtist: topN,
      maxTracksPerAlbum: maxPerAlbum,
      maxTracks: maxT,
      preserveOrder: true,
    });
    return limitedTracks.map((track) => track.ratingKey);
  }

  ratingKeys = selectLimitedPlaylistTracks(filteredTracks, {
    sortBy,
    topNPerArtist: topN,
    maxTracksPerAlbum: maxPerAlbum,
    maxTracks: maxT,
  }).map((track) => track.ratingKey);
  return ratingKeys;
}

function buildPlaylistRatingKeysFromRules(db, userId, playlistDef, config) {
  const masterTracks = applyTrackFilters(getMasterTracks(db), playlistDef.trackFilters);
  if (!masterTracks.length) return [];

  const rules      = playlistDef.rules || {};
  const blendUsers = Array.isArray(rules.blendUsers) && rules.blendUsers.length ? rules.blendUsers : null;
  const blendMode  = blendUsers ? (rules.blendMode || 'average') : null;

  if (blendMode === 'intersection' || blendMode === 'union' || blendMode === 'veto') {
    const perUserSets = blendUsers.map((uid) => {
      const { artistMap, trackMap } = _getUserStatMaps(db, uid);
      return new Set(_applyFilterRules(db, masterTracks, artistMap, trackMap, rules, config));
    });

    if (blendMode === 'intersection') {
      if (!perUserSets.length) return [];
      const [first, ...rest] = perUserSets;
      return [...first].filter((k) => rest.every((s) => s.has(k)));
    }

    // union and veto both start with the full union
    const result = new Set();
    for (const s of perUserSets) for (const k of s) result.add(k);
    if (blendMode === 'union') return [...result];

    // veto: remove any track that ANY blend user has explicitly skipped
    const skipKeys = new Set(
      blendUsers.flatMap((uid) =>
        db.prepare("SELECT plex_rating_key FROM track_stats WHERE user_plex_id = ? AND tier = 'skip'").all(uid)
          .map((r) => r.plex_rating_key),
      ),
    );
    return [...result].filter((k) => !skipKeys.has(k));
  }

  // average blend or standard single-user
  const { artistMap, trackMap } = (blendMode === 'average' && blendUsers)
    ? _buildAverageStatMaps(db, blendUsers)
    : _getUserStatMaps(db, userId);
  return _applyFilterRules(db, masterTracks, artistMap, trackMap, rules, config);
}

// ─── Full crescive / curative sync ───────────────────────────────────────────

async function syncSmartPlaylistForUser(ctx, userId, playlistType, playlistKey, playlistCfg, smartConfig, options = {}) {
  const { db, loadConfig, pushLog } = ctx;
  if (shouldSkipGeneratedPlaylistSync(db, userId, playlistKey, options)) {
    return listUserGeneratedPlaylists(db, userId, { activeOnly: false }).find((entry) => entry.playlistKey === playlistKey) || null;
  }
  const config = loadConfig();
  const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();

  if (msType === 'jellyfin' || msType === 'emby') {
    return syncSmartPlaylistForUserJellyfin(ctx, userId, playlistType, playlistKey, playlistCfg, smartConfig, options);
  }

  // ── Plex path ─────────────────────────────────────────────────────────────
  // Skip local-only users — they have no personal Plex token so any playlist
  // operation would fall back to the admin token and appear on the wrong account.
  if (!ctx.userHasOwnPlexToken(config, userId)) return;
  const { url, machineId: storedMid = '' } = config.plex || {};
  const token = ctx.resolveUserPlexServerToken(config, userId);
  if (!url || !token) return;

  let masterTracks = applyTrackFilters(getMasterTracks(db), playlistCfg.trackFilters);
  const maxDurationMs = Number(smartConfig?.maxTrackDurationMins ?? 0) > 0
    ? Number(smartConfig.maxTrackDurationMins) * 60 * 1000
    : 0;
  if (maxDurationMs > 0) masterTracks = masterTracks.filter((t) => !t.durationMs || t.durationMs <= maxDurationMs);
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

  const existing = listUserGeneratedPlaylists(db, userId, { activeOnly: false })
    .find((entry) => entry.playlistKey === playlistKey) || null;
  const title = resolveGeneratedPlaylistTitle(config, playlistType, playlistKey, userId, existing);
  const artworkState = resolveSyncArtworkState(ctx, userId, playlistType, playlistKey, existing);
  const playlistRow = await ensureGeneratedPlaylist(ctx, userId, playlistType, playlistKey, title, machineId);

  // Rebuild from current artist scores on every sync
  const { ratingKeys, trackList } = buildSmartPlaylistPayload(db, userId, playlistCfg, masterTracks);
  setPlaylistTracks(db, userId, playlistKey, trackList);
  pushLog({ level: 'info', app: 'playlist', action: `${playlistKey}.build`, message: `${playlistType} rebuilt: ${ratingKeys.length} tracks for ${userId}` });

  await replacePlexPlaylistItems(ctx, userId, playlistRow.plexPlaylistId, machineId, ratingKeys);
  const syncedArtwork = await syncGeneratedPlaylistArtwork(userId, {
    ...playlistRow,
    playlistKey,
    plexPlaylistId: playlistRow.plexPlaylistId,
  }, artworkState, { snapshotIfNeeded: artworkState.artworkMode === 'preserve' });

  const now = Date.now();
  saveUserGeneratedPlaylist(db, userId, {
    playlistType, playlistKey,
    plexPlaylistId: playlistRow.plexPlaylistId,
    playlistTitle: title,
    titleOverride: existing?.titleOverride || '',
    artworkMode: syncedArtwork.artworkMode,
    customArtworkAsset: syncedArtwork.customArtworkAsset,
    preservedArtworkAsset: syncedArtwork.preservedArtworkAsset,
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

async function syncSmartPlaylistForUserJellyfin(ctx, userId, playlistType, playlistKey, playlistCfg, smartConfig, _options = {}) {
  const { db, loadConfig, pushLog } = ctx;
  if (shouldSkipGeneratedPlaylistSync(db, userId, playlistKey, _options)) {
    return listUserGeneratedPlaylists(db, userId, { activeOnly: false }).find((entry) => entry.playlistKey === playlistKey) || null;
  }
  const config = loadConfig();
  const msType = String(config?.mediaServer?.type || 'jellyfin').toLowerCase();

  const serverCfg = config[msType] || {};
  const { url, apiKey } = serverCfg;
  if (!url || !apiKey) return;

  let masterTracks = applyTrackFilters(getMasterTracks(db), playlistCfg.trackFilters);
  const maxDurationMsJf = Number(smartConfig?.maxTrackDurationMins ?? 0) > 0
    ? Number(smartConfig.maxTrackDurationMins) * 60 * 1000
    : 0;
  if (maxDurationMsJf > 0) masterTracks = masterTracks.filter((t) => !t.durationMs || t.durationMs <= maxDurationMsJf);
  if (!masterTracks.length) return;

  // Resolve the Jellyfin user ID for this Curatorr user
  const { getAdapter } = await import('./media-servers/index.js');
  const adapter = getAdapter(msType);

  let jellyfinUserId;
  try {
    jellyfinUserId = await adapter.getUserIdByName(url, apiKey, userId);
  } catch (err) {
    pushLog({ level: 'warn', app: 'playlist', action: `${playlistKey}.jellyfin.user`, message: `Could not resolve Jellyfin user "${userId}": ${err.message}` });
    return;
  }

  // Rebuild from current artist scores on every sync
  const { ratingKeys, trackList } = buildSmartPlaylistPayload(db, userId, playlistCfg, masterTracks);
  setPlaylistTracks(db, userId, playlistKey, trackList);
  pushLog({ level: 'info', app: 'playlist', action: `${playlistKey}.build`, message: `${playlistType} rebuilt: ${ratingKeys.length} tracks for ${userId}` });
  const existing = listUserGeneratedPlaylists(db, userId, { activeOnly: false })
    .find((entry) => entry.playlistKey === playlistKey) || null;
  const title = resolveGeneratedPlaylistTitle(config, playlistType, playlistKey, userId, existing);

  // Always resolve the playlist via ensurePlaylist (searches by name first, creates if missing).
  // This handles stale UUIDs from externally-deleted playlists.
  const { playlistId } = await adapter.ensurePlaylist(url, apiKey, jellyfinUserId, title);

  await adapter.replacePlaylistItems(url, apiKey, playlistId, ratingKeys, jellyfinUserId);

  const now = Date.now();
  saveUserGeneratedPlaylist(db, userId, {
    playlistType, playlistKey,
    plexPlaylistId: playlistId,
    playlistTitle: title,
    titleOverride: existing?.titleOverride || '',
    artworkMode: artworkState.artworkMode,
    customArtworkAsset: artworkState.customArtworkAsset,
    preservedArtworkAsset: artworkState.preservedArtworkAsset,
    algorithmVersion: ALGORITHM_VERSION,
    lastBuiltAt: now, lastSyncedAt: now,
    trackCount: ratingKeys.length,
    active: true, updatedAt: now,
  });
  recordPlaylistSync(db, {
    userPlexId: userId, plexPlaylistId: playlistId,
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

  function updatePlaylistDefinitionArtworkState(userPlexId, playlistKey, artworkState) {
    const normalized = normalizeArtworkState(artworkState);
    if (String(playlistKey || '').startsWith('personal:')) {
      const personalId = String(playlistKey || '').replace(/^personal:/, '').trim();
      const existing = personalId ? getUserPersonalPlaylist(db, personalId, userPlexId) : null;
      if (!existing) return null;
      updateUserPersonalPlaylist(db, userPlexId, {
        ...existing,
        rules: {
          ...(existing.rules || {}),
          artwork: {
            mode: normalized.artworkMode,
            customArtworkAsset: normalized.customArtworkAsset,
            preservedArtworkAsset: normalized.preservedArtworkAsset,
          },
        },
      });
      return getUserPersonalPlaylist(db, personalId, userPlexId);
    }
    if (String(playlistKey || '').startsWith('global:')) {
      const globalId = String(playlistKey || '').replace(/^global:/, '').trim();
      const config = ctx.loadConfig();
      const entries = Array.isArray(config?.globalPlaylists) ? [...config.globalPlaylists] : [];
      const index = entries.findIndex((entry) => String(entry?.id || '').trim() === globalId);
      if (index < 0) return null;
      entries[index] = {
        ...entries[index],
        rules: {
          ...(entries[index].rules || {}),
          artwork: {
            mode: normalized.artworkMode,
            customArtworkAsset: normalized.customArtworkAsset,
            preservedArtworkAsset: normalized.preservedArtworkAsset,
          },
        },
        updatedAt: Date.now(),
      };
      ctx.saveConfig({ ...config, globalPlaylists: entries });
      return entries[index];
    }
    return null;
  }

  async function syncGeneratedPlaylistArtwork(userPlexId, playlist, artworkState, { snapshotIfNeeded = false } = {}) {
    const config = ctx.loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    if (msType !== 'plex') return normalizeArtworkState(artworkState);
    const normalized = normalizeArtworkState(artworkState);
    const plexPlaylistId = String(playlist?.plexPlaylistId || '').trim();
    if (!plexPlaylistId) return normalized;

    if (normalized.artworkMode === 'preserve' && !normalized.preservedArtworkAsset && snapshotIfNeeded) {
      const captured = await snapshotPlexPlaylistArtworkAsset(ctx, userPlexId, plexPlaylistId, `${userPlexId}-${playlist.playlistKey || plexPlaylistId}`).catch(() => '');
      if (captured) normalized.preservedArtworkAsset = captured;
    }

    const assetToApply = normalized.artworkMode === 'custom'
      ? normalized.customArtworkAsset
      : (normalized.artworkMode === 'preserve' ? normalized.preservedArtworkAsset : '');
    if (!assetToApply) return normalized;
    await applyPlexPlaylistArtworkAsset(ctx, userPlexId, plexPlaylistId, assetToApply).catch((err) => {
      ctx.pushLog?.({
        level: 'warn',
        app: 'playlist',
        action: 'artwork.apply',
        message: `Failed to apply artwork for ${playlist?.playlistKey || 'playlist'} for ${userPlexId}: ${err?.message || err}`,
      });
    });
    return normalized;
  }

  async function updateGeneratedPlaylistArtwork(userPlexId, playlistKey, artworkInput = {}) {
    const existing = getGeneratedByKey(userPlexId, playlistKey);
    if (!existing) throw new Error('Playlist not found.');
    const nextMode = normalizeArtworkMode(artworkInput?.artworkMode || artworkInput?.mode || existing.artworkMode || 'auto');
    let customArtworkAsset = String(existing.customArtworkAsset || '').trim();
    let preservedArtworkAsset = String(existing.preservedArtworkAsset || '').trim();

    if (nextMode === 'custom') {
      const explicitCustomAsset = String(artworkInput?.customArtworkAsset || '').trim();
      if (explicitCustomAsset) {
        customArtworkAsset = explicitCustomAsset;
      } else if (!customArtworkAsset) {
        throw new Error('Custom artwork is required.');
      }
    }
    if (nextMode === 'auto') {
      preservedArtworkAsset = '';
    }

    let updated = upsertGenerated(userPlexId, {
      ...existing,
      artworkMode: nextMode,
      customArtworkAsset,
      preservedArtworkAsset,
      updatedAt: Date.now(),
    });

    if (playlistKey.startsWith('personal:') || playlistKey.startsWith('global:')) {
      updatePlaylistDefinitionArtworkState(userPlexId, playlistKey, {
        artworkMode: nextMode,
        customArtworkAsset,
        preservedArtworkAsset,
      });
    }

    const syncedArtwork = await syncGeneratedPlaylistArtwork(userPlexId, updated, {
      artworkMode: nextMode,
      customArtworkAsset,
      preservedArtworkAsset,
    }, { snapshotIfNeeded: nextMode === 'preserve' }).catch(() => ({
      artworkMode: nextMode,
      customArtworkAsset,
      preservedArtworkAsset,
    }));

    updated = upsertGenerated(userPlexId, {
      ...updated,
      artworkMode: syncedArtwork.artworkMode,
      customArtworkAsset: syncedArtwork.customArtworkAsset,
      preservedArtworkAsset: syncedArtwork.preservedArtworkAsset,
      updatedAt: Date.now(),
    });
    return updated;
  }

  function syncExternalPlaylistPreference(userPlexId, playlistType, playlistKey, shouldEnable) {
    const type = String(playlistType || '').trim().toLowerCase();
    const rawKey = String(playlistKey || '').trim();
    if (!rawKey) return;
    const prefs = getUserPreferences(db, userPlexId);

    if (type === 'lastfm-station') {
      const stationKey = rawKey.replace(/^lastfm:/, '').trim();
      if (!stationKey) return;
      const current = Array.isArray(prefs.lastfmEnabledStations) ? prefs.lastfmEnabledStations.map((value) => String(value || '').trim()).filter(Boolean) : [];
      const next = shouldEnable
        ? [...current.filter((value) => value !== stationKey), stationKey]
        : current.filter((value) => value !== stationKey);
      if (next.length === current.length && next.every((value, idx) => value === current[idx])) return;
      saveUserPreferences(db, userPlexId, { ...prefs, lastfmEnabledStations: next });
      return;
    }

    if (type === 'listenbrainz-playlist') {
      const sourceKey = rawKey.replace(/^listenbrainz:/, '').trim();
      if (!sourceKey) return;
      const current = Array.isArray(prefs.listenbrainzEnabledPlaylists) ? prefs.listenbrainzEnabledPlaylists.map((value) => String(value || '').trim()).filter(Boolean) : [];
      const next = shouldEnable
        ? [...current.filter((value) => value !== sourceKey), sourceKey]
        : current.filter((value) => value !== sourceKey);
      if (next.length === current.length && next.every((value, idx) => value === current[idx])) return;
      saveUserPreferences(db, userPlexId, { ...prefs, listenbrainzEnabledPlaylists: next });
    }
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

  async function fetchPlexPlaylistTrackRefs(userPlexId, plexPlaylistId) {
    const config = ctx.loadConfig();
    const { url } = config.plex || {};
    const token = ctx.resolveUserPlexServerToken(config, userPlexId);
    const base = String(url || '').replace(/\/$/, '');
    if (!base || !token || !plexPlaylistId) return [];

    const tracks = [];
    let offset = 0;
    const pageSize = 1000;
    while (true) {
      const playlistUrl = new URL(`${base}/playlists/${plexPlaylistId}/items`);
      playlistUrl.searchParams.set('X-Plex-Container-Start', String(offset));
      playlistUrl.searchParams.set('X-Plex-Container-Size', String(pageSize));
      const response = await fetch(playlistUrl.toString(), {
        headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
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
    return tracks;
  }

  async function fetchJellyfinPlaylistTrackRefs(userPlexId, playlistId, msType) {
    const { getAdapter } = await import('./media-servers/index.js');
    const adapter = getAdapter(msType);
    const config = ctx.loadConfig();
    const { url, apiKey } = config[msType] || {};
    if (!url || !apiKey || !playlistId) return [];

    const remoteUserId = await adapter.getUserIdByName(url, apiKey, userPlexId);
    const tracks = [];
    let offset = 0;
    const pageSize = 1000;
    while (true) {
      const playlistUrl = new URL(`/Playlists/${encodeURIComponent(playlistId)}/Items`, url);
      playlistUrl.searchParams.set('UserId', remoteUserId);
      playlistUrl.searchParams.set('Fields', 'AlbumArtist,Artists');
      playlistUrl.searchParams.set('StartIndex', String(offset));
      playlistUrl.searchParams.set('Limit', String(pageSize));
      const response = await fetch(playlistUrl.toString(), {
        headers: { 'X-Emby-Token': apiKey, Accept: 'application/json' },
      });
      if (!response.ok) break;
      const json = await response.json();
      const batch = (json?.Items || [])
        .map((track) => ({
          ratingKey: String(track?.Id || '').trim(),
          artistName: String(track?.AlbumArtist || (Array.isArray(track?.Artists) ? track.Artists[0] : '') || '').trim(),
        }))
        .filter((track) => track.ratingKey);
      tracks.push(...batch);
      if (batch.length < pageSize) break;
      offset += batch.length;
    }
    return tracks;
  }

  async function snapshotCustomPlaylistTracks(userPlexId, playlist) {
    if (!playlist?.plexPlaylistId || playlist.playlistType !== 'custom') return;
    const config = ctx.loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    let tracks = [];
    if (msType === 'jellyfin' || msType === 'emby') {
      tracks = await fetchJellyfinPlaylistTrackRefs(userPlexId, playlist.plexPlaylistId, msType).catch(() => []);
    } else {
      tracks = await fetchPlexPlaylistTrackRefs(userPlexId, playlist.plexPlaylistId).catch(() => []);
    }
    setPlaylistTracks(db, userPlexId, playlist.playlistKey, tracks);
  }

  async function deleteGeneratedPlaylistFromMediaServer(userPlexId, playlist) {
    if (!playlist?.plexPlaylistId) return;
    const config = ctx.loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    if (msType === 'jellyfin' || msType === 'emby') {
      const { getAdapter } = await import('./media-servers/index.js');
      const adapter = getAdapter(msType);
      const { url, apiKey } = config[msType] || {};
      if (!url || !apiKey) return;
      const remoteUserId = await adapter.getUserIdByName(url, apiKey, userPlexId).catch(() => '');
      const deleteUrl = new URL(`/Items/${encodeURIComponent(playlist.plexPlaylistId)}`, url);
      if (remoteUserId) deleteUrl.searchParams.set('UserId', remoteUserId);
      await fetch(deleteUrl.toString(), {
        method: 'DELETE',
        headers: { 'X-Emby-Token': apiKey, Accept: 'application/json' },
      }).catch(() => {});
      return;
    }

    const { url } = config.plex || {};
    const token = ctx.resolveUserPlexServerToken(config, userPlexId);
    const base = String(url || '').replace(/\/$/, '');
    if (!base || !token) return;
    await fetch(`${base}/playlists/${playlist.plexPlaylistId}`, {
      method: 'DELETE',
      headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
    }).catch(() => {});
  }

  async function syncCustomPlaylist(userPlexId, playlist) {
    if (!playlist) throw new Error('Playlist not found.');
    const trackRefs = getPlaylistTracks(db, userPlexId, playlist.playlistKey);
    const ratingKeys = trackRefs.map((track) => String(track.ratingKey || '').trim()).filter(Boolean);
    const config = ctx.loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    const now = Date.now();
    const artworkState = normalizeArtworkState(playlist);

    if (msType === 'jellyfin' || msType === 'emby') {
      const { getAdapter } = await import('./media-servers/index.js');
      const adapter = getAdapter(msType);
      const { url, apiKey } = config[msType] || {};
      if (!url || !apiKey) throw new Error(`${msType} is not configured`);
      const remoteUserId = await adapter.getUserIdByName(url, apiKey, userPlexId);
      const { playlistId } = await adapter.ensurePlaylist(url, apiKey, remoteUserId, String(playlist.playlistTitle || 'Playlist'));
      await adapter.replacePlaylistItems(url, apiKey, playlistId, ratingKeys, remoteUserId);
      saveUserGeneratedPlaylist(db, userPlexId, {
        ...playlist,
        plexPlaylistId: playlistId,
        artworkMode: artworkState.artworkMode,
        customArtworkAsset: artworkState.customArtworkAsset,
        preservedArtworkAsset: artworkState.preservedArtworkAsset,
        trackCount: ratingKeys.length,
        active: true,
        lastSyncedAt: now,
        updatedAt: now,
      });
      return getGeneratedByKey(userPlexId, playlist.playlistKey);
    }

    if (!ctx.userHasOwnPlexToken(config, userPlexId)) {
      throw new Error('User has no Plex account — playlist sync is not available for local-only users.');
    }
    const machineId = await resolveMachineId(ctx, config);
    if (!machineId) throw new Error('Could not determine Plex machine ID');
    const playlistRow = await ensureGeneratedPlaylist(
      ctx,
      userPlexId,
      'custom',
      playlist.playlistKey,
      String(playlist.playlistTitle || 'Playlist'),
      machineId,
    );
    await replacePlexPlaylistItems(ctx, userPlexId, playlistRow.plexPlaylistId, machineId, ratingKeys);
    const syncedArtwork = await syncGeneratedPlaylistArtwork(userPlexId, {
      ...playlistRow,
      playlistKey: playlist.playlistKey,
      plexPlaylistId: playlistRow.plexPlaylistId,
    }, artworkState, { snapshotIfNeeded: artworkState.artworkMode === 'preserve' });
    saveUserGeneratedPlaylist(db, userPlexId, {
      ...playlist,
      plexPlaylistId: playlistRow.plexPlaylistId,
      artworkMode: syncedArtwork.artworkMode,
      customArtworkAsset: syncedArtwork.customArtworkAsset,
      preservedArtworkAsset: syncedArtwork.preservedArtworkAsset,
      trackCount: ratingKeys.length,
      active: true,
      lastSyncedAt: now,
      updatedAt: now,
    });
    return getGeneratedByKey(userPlexId, playlist.playlistKey);
  }

  async function syncRuleBasedSystemPlaylist(userPlexId, playlistType, playlistKey, playlistCfg, options = {}) {
    if (shouldSkipGeneratedPlaylistSync(db, userPlexId, playlistKey, options)) {
      return getGeneratedByKey(userPlexId, playlistKey);
    }
    const config = ctx.loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    if (msType === 'plex' && !ctx.userHasOwnPlexToken(config, userPlexId)) return;
    const masterTracks = applyTrackFilters(getMasterTracks(db), playlistCfg.trackFilters);
    if (!masterTracks.length) return;
    const { ratingKeys, trackList } = buildSmartPlaylistPayload(db, userPlexId, playlistCfg, masterTracks);
    const existing = getGeneratedByKey(userPlexId, playlistKey);
    const builtPlaylist = {
      playlistKey,
      playlistType,
      playlistTitle: resolveGeneratedPlaylistTitle(config, playlistType, playlistKey, userPlexId, existing),
      titleOverride: existing?.titleOverride || '',
      algorithmVersion: ALGORITHM_VERSION,
      trackCount: ratingKeys.length,
      trackKeys: ratingKeys,
      tracks: hydratePlaylistTracks(masterTracks, trackList, ratingKeys),
      options: {
        sortBy: normalizePlaylistBaseSort(playlistCfg.sortBy, 'default'),
        finalOrdering: normalizePlaylistFinalOrdering(playlistCfg.finalOrdering, 'none'),
      },
      builtAt: Date.now(),
    };
    ctx.pushLog?.({
      level: 'info',
      app: 'playlist',
      action: `${playlistKey}.build`,
      message: `${playlistType} rebuilt: ${ratingKeys.length} tracks for ${userPlexId}`,
    });
    return syncBuiltSelectionPlaylist(userPlexId, builtPlaylist, options);
  }

  async function setGeneratedActive(userPlexId, playlistKey, active) {
    const existing = getGeneratedByKey(userPlexId, playlistKey);
    if (!existing) throw new Error('Playlist not found.');
    const playlistType = String(existing.playlistType || '').trim().toLowerCase();
    const shouldEnable = Boolean(active);
    if (shouldEnable && existing.active) return existing;
    if (!shouldEnable && !existing.active) return existing;

    if (!shouldEnable) {
      syncExternalPlaylistPreference(userPlexId, playlistType, playlistKey, false);
      if (existing.playlistType === 'custom') await snapshotCustomPlaylistTracks(userPlexId, existing).catch(() => {});
      await deleteGeneratedPlaylistFromMediaServer(userPlexId, existing).catch(() => {});
      saveUserGeneratedPlaylist(db, userPlexId, {
        ...existing,
        plexPlaylistId: '',
        active: false,
        updatedAt: Date.now(),
      });
      ctx.pushLog?.({
        level: 'info',
        app: 'playlist',
        action: 'generated.disable',
        message: `Generated playlist disabled: ${playlistKey} for ${userPlexId}`,
      });
      return getGeneratedByKey(userPlexId, playlistKey);
    }

    if (playlistType === 'daily-mix') return syncDailyMix(userPlexId, { forceEnable: true });
    if (playlistType === 'curatorr') return syncCuratorr(userPlexId, { forceEnable: true });
    if (playlistType === 'crescive') return syncCrescive(userPlexId, { forceFullRebuild: true, forceEnable: true });
    if (playlistType === 'curative') return syncCurative(userPlexId, { forceFullRebuild: true, forceEnable: true });
    if (playlistType === 'global') {
      const globalId = String(playlistKey || '').replace(/^global:/, '');
      const playlistDef = (ctx.loadConfig().globalPlaylists || []).find((entry) => String(entry?.id || '') === globalId);
      if (!playlistDef) throw new Error('Global playlist definition not found.');
      return syncGlobalPlaylist(userPlexId, playlistDef, { forceEnable: true });
    }
    if (playlistType === 'personal') {
      const personalId = String(playlistKey || '').replace(/^personal:/, '');
      const playlistDef = getUserPersonalPlaylist(db, personalId, userPlexId);
      if (!playlistDef) throw new Error('Personal playlist definition not found.');
      return syncPersonalPlaylist(userPlexId, playlistDef, { forceEnable: true });
    }
    if (playlistType === 'lastfm-station') {
      syncExternalPlaylistPreference(userPlexId, playlistType, playlistKey, true);
      await syncLastfmStations(userPlexId, { allowPlaylistKeys: [playlistKey] });
      return getGeneratedByKey(userPlexId, playlistKey);
    }
    if (playlistType === 'listenbrainz-playlist') {
      syncExternalPlaylistPreference(userPlexId, playlistType, playlistKey, true);
      await syncListenbrainzPlaylists(userPlexId, { allowPlaylistKeys: [playlistKey] });
      return getGeneratedByKey(userPlexId, playlistKey);
    }
    if (playlistType === 'custom') return syncCustomPlaylist(userPlexId, existing);
    if (playlistType === 'curatorred') throw new Error('Legacy Curatorred playlists cannot be toggled here.');
    throw new Error('This playlist cannot be enabled.');
  }

  function resolveDailyMixOptions(options = {}) {
    const config = ctx.loadConfig();
    return normalizeDailyMixOptions({
      ...(config.smartPlaylist?.dailyMix || {}),
      ...options,
    });
  }

  function buildDailyMix(userPlexId, options = {}) {
    const config = ctx.loadConfig();
    const mixOptions = resolveDailyMixOptions(options);
    const masterTracks = getMasterTracks(db);
    const masterTrackMap = new Map(masterTracks.map((track) => [String(track.ratingKey || '').trim(), track]));
    const trackStatMap = buildTrackStatMap(db, userPlexId);
    const previousKeys = new Set(getPlaylistTracks(db, userPlexId, DAILY_MIX_PLAYLIST_KEY).map((track) => String(track.ratingKey || '')));
    const filteredMasterTracks = applyFeaturePresetFilters(
      mixOptions.trackFilters
        ? applyTrackFilters(masterTracks, mixOptions.trackFilters)
        : masterTracks,
      mixOptions,
    );
    const shouldRestrictToMasterTrackKeys = masterTracks.length > 0;
    const allowedTrackKeys = new Set(filteredMasterTracks.map((track) => track.ratingKey));

    const scorePool = (tracks, sourceBonus) => dedupeByRatingKey(tracks)
      .map((track) => ({
        ...(masterTrackMap.get(String(track?.ratingKey || '').trim()) || {}),
        ...track,
      }))
      .filter((track) => !shouldRestrictToMasterTrackKeys
        || !masterTrackMap.has(String(track?.ratingKey || '').trim())
        || allowedTrackKeys.has(track.ratingKey))
      .map((track) => {
        const stat = trackStatMap.get(track.ratingKey) || {};
        return {
          ...track,
          score: scoreSelectionTrack(track, {
            stat,
            tierWeight: 8,
            playWeight: 0.75,
            ratingWeight: 0.03,
            sourceBonus,
            previousPenalty: 16,
            repeatCooldownDays: mixOptions.repeatCooldownDays,
            inPreviousPlaylist: previousKeys.has(track.ratingKey),
          }),
        };
      })
      .sort((a, b) => b.score - a.score || (b.ratingCount || 0) - (a.ratingCount || 0));

    const favorites = scorePool(pickFavoriteTracks(db, userPlexId, mixOptions.favoriteLimit), 30);
    const suggestions = scorePool(pickSuggestedTracks(db, userPlexId, mixOptions.suggestedLimit), 22);
    const fresh = scorePool(pickFreshLibraryTracks(db, userPlexId, mixOptions.freshLimit), 14);
    const pools = [favorites, suggestions, fresh];
    const combined = [];
    const state = createSelectionState(mixOptions.maxTracksPerArtist);

    let madeProgress = true;
    while (combined.length < mixOptions.maxTracks && madeProgress) {
      madeProgress = false;
      for (const pool of pools) {
        while (pool.length) {
          const next = pool.shift();
          if (!tryAddSelectionTrack(state, combined, next)) continue;
          madeProgress = true;
          break;
        }
        if (combined.length >= mixOptions.maxTracks) break;
      }
    }

    return {
      playlistKey: DAILY_MIX_PLAYLIST_KEY,
      playlistType: DAILY_MIX_PLAYLIST_TYPE,
      playlistTitle: resolveGeneratedPlaylistTitle(
        config,
        DAILY_MIX_PLAYLIST_TYPE,
        DAILY_MIX_PLAYLIST_KEY,
        userPlexId,
        getGeneratedByKey(userPlexId, DAILY_MIX_PLAYLIST_KEY),
      ),
      algorithmVersion: 'phase3-daily-mix',
      trackCount: combined.length,
      trackKeys: combined.map((track) => track.ratingKey),
      tracks: combined,
      sourceBreakdown: {
        favorites: favorites.length,
        suggestions: suggestions.length,
        fresh: fresh.length,
      },
      options: mixOptions,
      builtAt: Date.now(),
    };
  }

  function buildCuratorr(userPlexId, options = {}) {
    const config = ctx.loadConfig();
    const curatorrOptions = normalizeCuratorrOptions({
      ...(config.smartPlaylist?.curatorr || {}),
      ...options,
    });
    const prefs = getUserPreferences(db, userPlexId) || {};
    const likedArtists = new Set((prefs.likedArtists || []).map((artist) => String(artist || '').trim().toLowerCase()).filter(Boolean));
    const likedGenres = new Set((prefs.likedGenres || []).map((genre) => String(genre || '').trim().toLowerCase()).filter(Boolean));
    const previousKeys = new Set(getPlaylistTracks(db, userPlexId, CURATORR_PLAYLIST_KEY).map((track) => String(track.ratingKey || '')));
    const trackStatMap = buildTrackStatMap(db, userPlexId);
    const artistScoreMap = buildArtistScoreMap(db, userPlexId);
    const suggestedKeys = new Set(pickSuggestedTracks(db, userPlexId, Math.max(curatorrOptions.targetTracks, 24)).map((track) => track.ratingKey));
    const freshKeys = new Set(pickFreshLibraryTracks(db, userPlexId, Math.max(curatorrOptions.targetTracks * 2, 40)).map((track) => track.ratingKey));
    const masterTracks = applyFeaturePresetFilters(
      applyTrackFilters(getMasterTracks(db), curatorrOptions.trackFilters),
      curatorrOptions,
    );
    const familiar = [];
    const discovery = [];
    const skipRank = Number(config.smartPlaylist?.artistSkipRank ?? 2);

    for (const track of masterTracks) {
      const stat = trackStatMap.get(track.ratingKey) || {};
      if (stat.excludedFromSmart && !stat.manuallyIncluded) continue;
      if (stat.tier === 'skip' && !stat.manuallyIncluded) continue;

      const artistKey = String(track.artistName || '').trim().toLowerCase();
      const artistScore = artistScoreMap.get(artistKey) ?? 5;
      const inLikedArtist = likedArtists.has(artistKey);
      const inLikedGenre = (track.genres || []).some((genre) => likedGenres.has(String(genre || '').trim().toLowerCase()));
      const inSuggested = suggestedKeys.has(track.ratingKey);
      const inFresh = freshKeys.has(track.ratingKey);
      const isHeard = Number(stat.playCount || 0) > 0 || (stat.tier && stat.tier !== 'curatorr');
      const inPreviousPlaylist = previousKeys.has(track.ratingKey);
      const commonExtras = {
        stat,
        artistScore,
        inLikedArtist,
        inLikedGenre,
        inPreviousPlaylist,
        repeatCooldownDays: curatorrOptions.repeatCooldownDays,
        likedArtistBonus: 16,
        likedGenreBonus: 8,
        previousPenalty: 22,
      };

      if (isHeard) {
        familiar.push({
          ratingKey: track.ratingKey,
          artistName: track.artistName,
          trackTitle: track.trackTitle,
          albumName: track.albumName,
          score: scoreSelectionTrack(track, {
            ...commonExtras,
            artistWeight: 6,
            tierWeight: 11,
            playWeight: 1.2,
            ratingWeight: 0.02,
            sourceBonus: inSuggested ? 6 : 0,
          }),
          source: 'familiar',
        });
      }

      const qualifiesDiscovery = !isHeard || inSuggested || inFresh || inLikedArtist || inLikedGenre;
      if (!qualifiesDiscovery) continue;
      if (!inSuggested && !inFresh && artistScore <= skipRank && !inLikedArtist) continue;
      discovery.push({
        ratingKey: track.ratingKey,
        artistName: track.artistName,
        trackTitle: track.trackTitle,
        albumName: track.albumName,
        score: scoreSelectionTrack(track, {
          ...commonExtras,
          artistWeight: 4.5,
          tierWeight: 3,
          playWeight: 0.2,
          ratingWeight: 0.04,
          sourceBonus: (inSuggested ? 18 : 0) + (inFresh ? 12 : 0) + (!isHeard ? 6 : 0),
        }),
        source: inSuggested ? 'suggested' : inFresh ? 'fresh-library' : 'discovery',
      });
    }

    familiar.sort((a, b) => b.score - a.score || a.artistName.localeCompare(b.artistName) || a.trackTitle.localeCompare(b.trackTitle));
    discovery.sort((a, b) => b.score - a.score || a.artistName.localeCompare(b.artistName) || a.trackTitle.localeCompare(b.trackTitle));

    const familiarTarget = Math.max(0, Math.round(curatorrOptions.targetTracks * (1 - curatorrOptions.discoveryRatio)));
    const selected = [];
    const state = createSelectionState(curatorrOptions.maxTracksPerArtist);
    addTracksFromPool(familiar, familiarTarget, state, selected);
    addTracksFromPool(discovery, curatorrOptions.targetTracks, state, selected);
    if (selected.length < curatorrOptions.targetTracks) {
      const fallbackPool = [...familiar, ...discovery].sort((a, b) => b.score - a.score);
      addTracksFromPool(fallbackPool, curatorrOptions.targetTracks, state, selected);
    }

    return {
      playlistKey: CURATORR_PLAYLIST_KEY,
      playlistType: CURATORR_PLAYLIST_TYPE,
      playlistTitle: resolveGeneratedPlaylistTitle(
        config,
        CURATORR_PLAYLIST_TYPE,
        CURATORR_PLAYLIST_KEY,
        userPlexId,
        getGeneratedByKey(userPlexId, CURATORR_PLAYLIST_KEY),
      ),
      algorithmVersion: 'phase1-rotating-curatorr',
      trackCount: selected.length,
      trackKeys: selected.map((track) => track.ratingKey),
      tracks: selected,
      sourceBreakdown: {
        familiar: selected.filter((track) => track.source === 'familiar').length,
        discovery: selected.filter((track) => track.source !== 'familiar').length,
      },
      options: curatorrOptions,
      builtAt: Date.now(),
    };
  }

  async function fetchPlexSonicNeighbors(userPlexId, seedRatingKey, { limit = 12, maxDistance = 0.35 } = {}) {
    const config = ctx.loadConfig();
    const { url } = config?.plex || {};
    const token = ctx.resolveUserPlexServerToken(config, userPlexId);
    const base = String(url || '').replace(/\/$/, '');
    if (!base || !token || !seedRatingKey) return [];
    const nearestUrl = new URL(`${base}/library/metadata/${encodeURIComponent(seedRatingKey)}/nearest`);
    nearestUrl.searchParams.set('limit', String(Math.max(1, Number(limit || 1))));
    nearestUrl.searchParams.set('maxDistance', String(Math.max(0, Math.min(1, Number(maxDistance || 0.35)))));
    const response = await fetch(nearestUrl.toString(), {
      headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
    });
    if (!response.ok) return [];
    const json = await response.json();
    return (json?.MediaContainer?.Metadata || []).map((track) => ({
      ratingKey: String(track?.ratingKey || '').trim(),
      distance: Number(track?.distance ?? Number.NaN),
    })).filter((track) => track.ratingKey);
  }

  async function fetchPlexSonicPath(userPlexId, sectionId, startRatingKey, endRatingKey, { count = 12, maxDistance = 0.35 } = {}) {
    const config = ctx.loadConfig();
    const { url } = config?.plex || {};
    const token = ctx.resolveUserPlexServerToken(config, userPlexId);
    const base = String(url || '').replace(/\/$/, '');
    if (!base || !token || !sectionId || !startRatingKey || !endRatingKey) return [];
    const pathUrl = new URL(`${base}/library/sections/${encodeURIComponent(sectionId)}/computePath`);
    pathUrl.searchParams.set('startID', String(startRatingKey));
    pathUrl.searchParams.set('endID', String(endRatingKey));
    pathUrl.searchParams.set('count', String(Math.max(1, Number(count || 1))));
    pathUrl.searchParams.set('maxDistance', String(Math.max(0, Math.min(1, Number(maxDistance || 0.35)))));
    const response = await fetch(pathUrl.toString(), {
      headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
    });
    if (!response.ok) return [];
    const json = await response.json();
    const rawItems = json?.MediaContainer?.Path || json?.MediaContainer?.Metadata || json?.MediaContainer?.path || [];
    return rawItems.map((track) => ({
      ratingKey: String(track?.ratingKey || track?.id || '').trim(),
    })).filter((track) => track.ratingKey);
  }

  async function buildPlexSonicPathOrder(userPlexId, tracks, options = {}, masterTrackMap = new Map()) {
    const selectedKeys = new Set((tracks || []).map((track) => String(track?.ratingKey || '').trim()).filter(Boolean));
    const libraryGroups = new Map();
    for (const track of tracks || []) {
      const libraryKey = resolveTrackLibraryKey(track, masterTrackMap);
      if (!libraryKey) continue;
      if (!libraryGroups.has(libraryKey)) libraryGroups.set(libraryKey, []);
      libraryGroups.get(libraryKey).push(track);
    }
    const dominantLibraryEntry = [...libraryGroups.entries()]
      .sort((a, b) => b[1].length - a[1].length)[0];
    const sectionId = String(dominantLibraryEntry?.[0] || '').trim();
    const eligibleTracks = dominantLibraryEntry?.[1] || [];
    if (!sectionId || eligibleTracks.length < 3) return [];

    const seeds = pickSonicSeeds(eligibleTracks, {
      seedCount: options.sonicSeedCount,
      strategy: options.sonicStrategy,
    });
    if (seeds.length < 2) return [];

    const orderedKeys = [];
    const seen = new Set();
    const pushKey = (key) => {
      const cleanKey = String(key || '').trim();
      if (!cleanKey || seen.has(cleanKey) || !selectedKeys.has(cleanKey)) return;
      seen.add(cleanKey);
      orderedKeys.push(cleanKey);
    };

    let matchedPath = false;
    const perSegmentCount = Math.max(1, Math.ceil(Number(options.sonicExpansionLimit || 12) / Math.max(1, seeds.length - 1)));
    pushKey(seeds[0].ratingKey);
    for (let i = 0; i < seeds.length - 1; i += 1) {
      const startSeed = seeds[i];
      const endSeed = seeds[i + 1];
      const path = await fetchPlexSonicPath(userPlexId, sectionId, startSeed.ratingKey, endSeed.ratingKey, {
        count: perSegmentCount,
        maxDistance: options.sonicMaxDistance,
      });
      const matched = path
        .map((track) => String(track?.ratingKey || '').trim())
        .filter((key) => key && selectedKeys.has(key));
      if (!matched.length) {
        pushKey(endSeed.ratingKey);
        continue;
      }
      matchedPath = true;
      for (const key of matched) pushKey(key);
      pushKey(endSeed.ratingKey);
    }
    return matchedPath ? orderedKeys : [];
  }

  async function buildPlexNearestOrder(userPlexId, tracks, options = {}) {
    const selectedKeys = new Set((tracks || []).map((track) => String(track?.ratingKey || '').trim()).filter(Boolean));
    const seeds = pickSonicSeeds(tracks, {
      seedCount: options.sonicSeedCount,
      strategy: options.sonicStrategy,
    });
    const orderedKeys = [];
    const seen = new Set();
    const pushKey = (key) => {
      const cleanKey = String(key || '').trim();
      if (!cleanKey || seen.has(cleanKey) || !selectedKeys.has(cleanKey)) return;
      seen.add(cleanKey);
      orderedKeys.push(cleanKey);
    };
    let matchedNeighbor = false;
    for (const seed of seeds) {
      const neighbors = await fetchPlexSonicNeighbors(userPlexId, seed.ratingKey, {
        limit: options.sonicExpansionLimit,
        maxDistance: options.sonicMaxDistance,
      });
      const matched = neighbors
        .map((neighbor) => String(neighbor?.ratingKey || '').trim())
        .filter((key) => key && key !== seed.ratingKey && selectedKeys.has(key));
      if (!matched.length) continue;
      matchedNeighbor = true;
      pushKey(seed.ratingKey);
      for (const key of matched) pushKey(key);
    }
    return matchedNeighbor ? orderedKeys : [];
  }

  async function buildPlexSonicOrder(userPlexId, tracks, options = {}) {
    const masterTrackMap = new Map(getMasterTracks(db).map((track) => [String(track.ratingKey || '').trim(), track]));
    const pathKeys = await buildPlexSonicPathOrder(userPlexId, tracks, options, masterTrackMap);
    if (pathKeys.length) return { orderedKeys: pathKeys, method: 'path' };
    const nearestKeys = await buildPlexNearestOrder(userPlexId, tracks, options);
    if (nearestKeys.length) return { orderedKeys: nearestKeys, method: 'nearest' };
    return { orderedKeys: [], method: 'none' };
  }

  async function applyOptionalSonicOrdering(userPlexId, builtPlaylist, options = {}) {
    const useSonicOrdering = Boolean(options.useSonicOrdering);
    if (!useSonicOrdering) return builtPlaylist;

    const config = ctx.loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    const fallback = (reason, err = null) => {
      if (typeof ctx.pushLog === 'function') {
        ctx.pushLog({
          level: err ? 'warn' : 'info',
          app: 'playlist',
          action: 'sonic.fallback',
          message: `Skipped Plex sonic ordering for ${builtPlaylist?.playlistKey || 'playlist'} for ${userPlexId}: ${reason}${err?.message ? ` (${err.message})` : ''}`,
        });
      }
      return { ...builtPlaylist, sonicApplied: false, sonicFallbackReason: reason };
    };

    if (msType !== 'plex') return fallback('unsupported-server');
    if (!Array.isArray(builtPlaylist?.tracks) || builtPlaylist.tracks.length < 3) return fallback('too-few-tracks');

    try {
      const { orderedKeys, method } = await buildPlexSonicOrder(userPlexId, builtPlaylist.tracks, options);
      const reorderedTracks = reorderTracksByRatingKeys(builtPlaylist.tracks, orderedKeys);
      const changed = reorderedTracks.some((track, index) => track?.ratingKey !== builtPlaylist.tracks[index]?.ratingKey);
      if (!changed) return fallback('no-sonic-data');
      if (typeof ctx.pushLog === 'function') {
        const seeds = pickSonicSeeds(builtPlaylist.tracks, {
          seedCount: options.sonicSeedCount,
          strategy: options.sonicStrategy,
        });
        ctx.pushLog({
          level: 'info',
          app: 'playlist',
          action: 'sonic.ordering',
          message: `Applied Plex sonic ordering to ${builtPlaylist.playlistKey} for ${userPlexId}`,
          meta: {
            playlistKey: builtPlaylist.playlistKey,
            seedCount: seeds.length,
            strategy: options.sonicStrategy || 'balanced',
            trackCount: reorderedTracks.length,
            method,
          },
        });
      }
      return {
        ...builtPlaylist,
        tracks: reorderedTracks,
        trackKeys: reorderedTracks.map((track) => track.ratingKey),
        trackCount: reorderedTracks.length,
        sonicApplied: true,
        sonicFallbackReason: '',
      };
    } catch (err) {
      return fallback('plex-error', err);
    }
  }

  function applyOptionalLoudnessOrdering(userPlexId, builtPlaylist, options = {}) {
    const useLoudnessOrdering = Boolean(options.useLoudnessOrdering);
    if (!useLoudnessOrdering) return builtPlaylist;

    const fallback = (reason) => {
      if (typeof ctx.pushLog === 'function') {
        ctx.pushLog({
          level: 'info',
          app: 'playlist',
          action: 'loudness.fallback',
          message: `Skipped loudness smoothing for ${builtPlaylist?.playlistKey || 'playlist'} for ${userPlexId}: ${reason}`,
        });
      }
      return { ...builtPlaylist, loudnessApplied: false, loudnessFallbackReason: reason };
    };

    if (!Array.isArray(builtPlaylist?.tracks) || builtPlaylist.tracks.length < 3) return fallback('too-few-tracks');
    const withLoudness = builtPlaylist.tracks.filter((track) => resolveTrackLoudness(track) != null).length;
    if (withLoudness < 3) return fallback('no-loudness-data');

    const reorderedTracks = applyLoudnessSmoothing(builtPlaylist.tracks, options);
    const changed = reorderedTracks.some((track, index) => track?.ratingKey !== builtPlaylist.tracks[index]?.ratingKey);
    if (!changed) return fallback('no-loudness-benefit');

    if (typeof ctx.pushLog === 'function') {
      ctx.pushLog({
        level: 'info',
        app: 'playlist',
        action: 'loudness.ordering',
        message: `Applied loudness smoothing to ${builtPlaylist.playlistKey} for ${userPlexId}`,
        meta: {
          playlistKey: builtPlaylist.playlistKey,
          lookahead: options.loudnessLookahead || 4,
          maxLoudnessStepDb: options.maxLoudnessStepDb || 6,
          trackCount: reorderedTracks.length,
          loudnessTracks: withLoudness,
        },
      });
    }

    return {
      ...builtPlaylist,
      tracks: reorderedTracks,
      trackKeys: reorderedTracks.map((track) => track.ratingKey),
      trackCount: reorderedTracks.length,
      loudnessApplied: true,
      loudnessFallbackReason: '',
    };
  }

  async function finalizeBuiltPlaylistOrdering(userPlexId, builtPlaylist, options = {}) {
    const masterTracks = getMasterTracks(db);
    const hydratedTracks = hydratePlaylistTracks(masterTracks, builtPlaylist?.tracks, builtPlaylist?.trackKeys);
    const baseSort = normalizePlaylistBaseSort(options.sortBy, 'default');
    let orderedPlaylist = {
      ...builtPlaylist,
      tracks: sortPlaylistTracksByMode(db, userPlexId, hydratedTracks, baseSort),
    };
    orderedPlaylist.trackKeys = orderedPlaylist.tracks.map((track) => track.ratingKey);
    orderedPlaylist.trackCount = orderedPlaylist.trackKeys.length;

    const finalOrdering = resolvePlaylistFinalOrdering(options);
    if (finalOrdering === 'plexSonic' || finalOrdering === 'plexSonicLoudness') {
      orderedPlaylist = await applyOptionalSonicOrdering(userPlexId, orderedPlaylist, { ...options, useSonicOrdering: true });
    }
    if (finalOrdering === 'loudness' || finalOrdering === 'plexSonicLoudness') {
      orderedPlaylist = applyOptionalLoudnessOrdering(userPlexId, orderedPlaylist, { ...options, useLoudnessOrdering: true });
    }
    return orderedPlaylist;
  }

  async function syncBuiltSelectionPlaylist(userPlexId, builtPlaylist, options = {}) {
    const config = ctx.loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    const trigger = String(options.trigger || 'manual');
    const playlistToSync = await finalizeBuiltPlaylistOrdering(userPlexId, builtPlaylist, builtPlaylist?.options || {});
    const existing = getGeneratedByKey(userPlexId, playlistToSync?.playlistKey || builtPlaylist?.playlistKey || '');
    const artworkState = resolveSyncArtworkState(
      ctx,
      userPlexId,
      String(playlistToSync?.playlistType || builtPlaylist?.playlistType || ''),
      String(playlistToSync?.playlistKey || builtPlaylist?.playlistKey || ''),
      existing,
    );
    if (!playlistToSync?.trackKeys?.length) {
      throw new Error(`No ${builtPlaylist?.playlistTitle || 'playlist'} tracks are available yet`);
    }

    if (msType === 'jellyfin' || msType === 'emby') {
      const { getAdapter } = await import('./media-servers/index.js');
      const adapter = getAdapter(msType);
      const serverCfg = config[msType] || {};
      const { url, apiKey } = serverCfg;
      if (!url || !apiKey) throw new Error(`${msType} is not configured`);
      const remoteUserId = await adapter.getUserIdByName(url, apiKey, userPlexId);
      const { playlistId } = await adapter.ensurePlaylist(url, apiKey, remoteUserId, playlistToSync.playlistTitle);
      await adapter.replacePlaylistItems(url, apiKey, playlistId, playlistToSync.trackKeys, remoteUserId);
      setPlaylistTracks(db, userPlexId, playlistToSync.playlistKey, playlistToSync.tracks);
      const syncedAt = Date.now();
      saveUserGeneratedPlaylist(db, userPlexId, {
        playlistType: playlistToSync.playlistType,
        playlistKey: playlistToSync.playlistKey,
        plexPlaylistId: playlistId,
        playlistTitle: playlistToSync.playlistTitle,
        artworkMode: artworkState.artworkMode,
        customArtworkAsset: artworkState.customArtworkAsset,
        preservedArtworkAsset: artworkState.preservedArtworkAsset,
        algorithmVersion: playlistToSync.algorithmVersion,
        lastBuiltAt: playlistToSync.builtAt,
        lastSyncedAt: syncedAt,
        trackCount: playlistToSync.trackCount,
        active: true,
        updatedAt: syncedAt,
      });
      recordPlaylistSync(db, {
        userPlexId,
        plexPlaylistId: playlistId,
        playlistTitle: playlistToSync.playlistTitle,
        trackCount: playlistToSync.trackCount,
        excludedTracks: 0,
        excludedArtists: 0,
        trigger,
      });
      return { ...playlistToSync, syncedAt, plexPlaylistId: playlistId };
    }

    if (!ctx.userHasOwnPlexToken(config, userPlexId)) {
      throw new Error('User has no Plex account — playlist sync is not available for local-only users.');
    }
    const { url } = config.plex || {};
    const token = ctx.resolveUserPlexServerToken(config, userPlexId);
    if (!url || !token) throw new Error('Plex is not configured');

    const machineId = await resolveMachineId(ctx, config);
    if (!machineId) throw new Error('Could not determine Plex machine ID');

    const playlistRow = await ensureGeneratedPlaylist(
      ctx,
      userPlexId,
      playlistToSync.playlistType,
      playlistToSync.playlistKey,
      playlistToSync.playlistTitle,
      machineId,
    );
    await replacePlexPlaylistItems(ctx, userPlexId, playlistRow.plexPlaylistId, machineId, playlistToSync.trackKeys);
    const syncedArtwork = await syncGeneratedPlaylistArtwork(userPlexId, {
      ...playlistRow,
      playlistKey: playlistToSync.playlistKey,
      plexPlaylistId: playlistRow.plexPlaylistId,
    }, artworkState, { snapshotIfNeeded: artworkState.artworkMode === 'preserve' });
    setPlaylistTracks(db, userPlexId, playlistToSync.playlistKey, playlistToSync.tracks);

    const syncedAt = Date.now();
    saveUserGeneratedPlaylist(db, userPlexId, {
      playlistType: playlistToSync.playlistType,
      playlistKey: playlistToSync.playlistKey,
      plexPlaylistId: playlistRow.plexPlaylistId,
      playlistTitle: playlistToSync.playlistTitle,
      artworkMode: syncedArtwork.artworkMode,
      customArtworkAsset: syncedArtwork.customArtworkAsset,
      preservedArtworkAsset: syncedArtwork.preservedArtworkAsset,
      algorithmVersion: playlistToSync.algorithmVersion,
      lastBuiltAt: playlistToSync.builtAt,
      lastSyncedAt: syncedAt,
      trackCount: playlistToSync.trackCount,
      active: true,
      updatedAt: syncedAt,
    });
    recordPlaylistSync(db, {
      userPlexId,
      plexPlaylistId: playlistRow.plexPlaylistId,
      playlistTitle: playlistToSync.playlistTitle,
      trackCount: playlistToSync.trackCount,
      excludedTracks: 0,
      excludedArtists: 0,
      trigger,
    });
    return { ...playlistToSync, syncedAt, plexPlaylistId: playlistRow.plexPlaylistId };
  }

  async function syncDailyMix(userPlexId, options = {}) {
    if (shouldSkipGeneratedPlaylistSync(db, userPlexId, DAILY_MIX_PLAYLIST_KEY, options)) {
      return getGeneratedByKey(userPlexId, DAILY_MIX_PLAYLIST_KEY);
    }
    return syncBuiltSelectionPlaylist(userPlexId, buildDailyMix(userPlexId, options), options);
  }

  async function syncCuratorr(userPlexId, options = {}) {
    if (shouldSkipGeneratedPlaylistSync(db, userPlexId, CURATORR_PLAYLIST_KEY, options)) {
      return getGeneratedByKey(userPlexId, CURATORR_PLAYLIST_KEY);
    }
    return syncBuiltSelectionPlaylist(userPlexId, buildCuratorr(userPlexId, options), options);
  }

  async function syncGlobalPlaylist(userId, playlistDef, options = {}) {
    const config = ctx.loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    const playlistKey = `global:${playlistDef.id}`;
    if (shouldSkipGeneratedPlaylistSync(db, userId, playlistKey, options)) return getGeneratedByKey(userId, playlistKey);
    // Jellyfin/Emby: use the generic adapter path
    if (msType === 'jellyfin' || msType === 'emby') return syncGlobalPlaylistJellyfin(userId, playlistDef, config, msType, options);
    // Blend playlists only sync for users listed in blendUsers
    const blendUsersList = Array.isArray(playlistDef.rules?.blendUsers) ? playlistDef.rules.blendUsers : null;
    if (blendUsersList?.length && !blendUsersList.includes(userId)) return;
    if (!ctx.userHasOwnPlexToken(config, userId)) return;
    const ratingKeys = buildPlaylistRatingKeysFromRules(db, userId, playlistDef, config);
    if (!ratingKeys.length) return;
    const builtPlaylist = {
      playlistKey,
      playlistType: 'global',
      playlistTitle: buildGeneratedPlaylistTitle(config, 'global', playlistKey, userId, playlistDef.name),
      algorithmVersion: 'global-playlist-v1',
      trackCount: ratingKeys.length,
      trackKeys: ratingKeys,
      tracks: hydratePlaylistTracks(getMasterTracks(db), [], ratingKeys),
      options: {
        sortBy: 'default',
        finalOrdering: normalizePlaylistFinalOrdering(playlistDef.rules?.finalOrdering, 'none'),
      },
      builtAt: Date.now(),
    };
    await syncBuiltSelectionPlaylist(userId, builtPlaylist, options);
    ctx.pushLog({ level: 'info', app: 'playlist', action: 'global.sync', message: `Global playlist "${playlistDef.name}" synced: ${ratingKeys.length} tracks for ${userId}` });
  }

  async function syncGlobalPlaylistJellyfin(userId, playlistDef, config, msType, options = {}) {
    // Blend playlists only sync for users listed in blendUsers
    const blendUsersList = Array.isArray(playlistDef.rules?.blendUsers) ? playlistDef.rules.blendUsers : null;
    if (blendUsersList?.length && !blendUsersList.includes(userId)) return;
    const playlistKey = `global:${playlistDef.id}`;
    if (shouldSkipGeneratedPlaylistSync(db, userId, playlistKey, options)) return getGeneratedByKey(userId, playlistKey);
    const { getAdapter } = await import('./media-servers/index.js');
    const adapter = getAdapter(msType);
    const { url, apiKey } = config[msType] || {};
    if (!url || !apiKey) return;

    const ratingKeys = buildPlaylistRatingKeysFromRules(db, userId, playlistDef, config);
    if (!ratingKeys.length) return;
    const builtPlaylist = {
      playlistKey,
      playlistType: 'global',
      playlistTitle: buildGeneratedPlaylistTitle(config, 'global', playlistKey, userId, playlistDef.name),
      algorithmVersion: 'global-playlist-v1',
      trackCount: ratingKeys.length,
      trackKeys: ratingKeys,
      tracks: hydratePlaylistTracks(getMasterTracks(db), [], ratingKeys),
      options: {
        sortBy: 'default',
        finalOrdering: normalizePlaylistFinalOrdering(playlistDef.rules?.finalOrdering, 'none'),
      },
      builtAt: Date.now(),
    };
    await syncBuiltSelectionPlaylist(userId, builtPlaylist, options);
    ctx.pushLog({ level: 'info', app: 'playlist', action: 'global.sync', message: `Global playlist "${playlistDef.name}" synced (${msType}): ${ratingKeys.length} tracks for ${userId}` });
  }

  async function syncPersonalPlaylist(userId, playlistDef, options = {}) {
    const config = ctx.loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    const playlistKey = `personal:${playlistDef.id}`;
    if (shouldSkipGeneratedPlaylistSync(db, userId, playlistKey, options)) return getGeneratedByKey(userId, playlistKey);
    if (msType === 'jellyfin' || msType === 'emby') return syncPersonalPlaylistJellyfin(userId, playlistDef, config, msType, options);
    if (!ctx.userHasOwnPlexToken(config, userId)) return;
    const ratingKeys = buildPlaylistRatingKeysFromRules(db, userId, playlistDef, config);
    if (!ratingKeys.length) return;
    const builtPlaylist = {
      playlistKey,
      playlistType: 'personal',
      playlistTitle: buildGeneratedPlaylistTitle(config, 'personal', playlistKey, userId, playlistDef.name),
      algorithmVersion: 'personal-playlist-v1',
      trackCount: ratingKeys.length,
      trackKeys: ratingKeys,
      tracks: hydratePlaylistTracks(getMasterTracks(db), [], ratingKeys),
      options: {
        sortBy: 'default',
        finalOrdering: normalizePlaylistFinalOrdering(playlistDef.rules?.finalOrdering, 'none'),
      },
      builtAt: Date.now(),
    };
    await syncBuiltSelectionPlaylist(userId, builtPlaylist, options);
    ctx.pushLog({ level: 'info', app: 'playlist', action: 'personal.sync', message: `Personal playlist "${playlistDef.name}" synced: ${ratingKeys.length} tracks for ${userId}` });
  }

  async function syncPersonalPlaylistJellyfin(userId, playlistDef, config, msType, options = {}) {
    const { getAdapter } = await import('./media-servers/index.js');
    const adapter = getAdapter(msType);
    const { url, apiKey } = config[msType] || {};
    if (!url || !apiKey) return;
    const playlistKey = `personal:${playlistDef.id}`;
    if (shouldSkipGeneratedPlaylistSync(db, userId, playlistKey, options)) return getGeneratedByKey(userId, playlistKey);

    const ratingKeys = buildPlaylistRatingKeysFromRules(db, userId, playlistDef, config);
    if (!ratingKeys.length) return;
    const builtPlaylist = {
      playlistKey,
      playlistType: 'personal',
      playlistTitle: buildGeneratedPlaylistTitle(config, 'personal', playlistKey, userId, playlistDef.name),
      algorithmVersion: 'personal-playlist-v1',
      trackCount: ratingKeys.length,
      trackKeys: ratingKeys,
      tracks: hydratePlaylistTracks(getMasterTracks(db), [], ratingKeys),
      options: {
        sortBy: 'default',
        finalOrdering: normalizePlaylistFinalOrdering(playlistDef.rules?.finalOrdering, 'none'),
      },
      builtAt: Date.now(),
    };
    await syncBuiltSelectionPlaylist(userId, builtPlaylist, options);
    ctx.pushLog({ level: 'info', app: 'playlist', action: 'personal.sync', message: `Personal playlist "${playlistDef.name}" synced (${msType}): ${ratingKeys.length} tracks for ${userId}` });
  }

  async function syncLastfmStations(userId, options = {}) {
    const prefs = getUserPreferences(db, userId);
    if (!prefs.lastfmUsername) return;
    const config = ctx.loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    // Jellyfin/Emby: Last.fm station playlist sync not yet implemented
    if (msType === 'jellyfin' || msType === 'emby') return;
    if (!ctx.userHasOwnPlexToken(config, userId)) return;

    // Cleanup: delete Plex playlists for stations that have been disabled
    const enabledSet = new Set(prefs.lastfmEnabledStations || []);
    const strictMatchSet = new Set(prefs.lastfmStrictMatchStations || []);
    const existingLastfm = listGenerated(userId, { activeOnly: false })
      .filter((p) => p.playlistKey.startsWith('lastfm:') && p.active);
    for (const existing of existingLastfm) {
      const stationKey = existing.playlistKey.slice('lastfm:'.length);
      if (!enabledSet.has(stationKey)) {
        try {
          const { url } = config.plex || {};
          const token = ctx.resolveUserPlexServerToken(config, userId);
          const base = String(url || '').replace(/\/$/, '');
          if (base && token && existing.plexPlaylistId) {
            await fetch(`${base}/playlists/${existing.plexPlaylistId}`, {
              method: 'DELETE',
              headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
            });
          }
        } catch (_err) {}
        saveUserGeneratedPlaylist(db, userId, { ...existing, active: false, plexPlaylistId: '', updatedAt: Date.now() });
        ctx.pushLog({ level: 'info', app: 'playlist', action: 'lastfm.station.removed', message: `Last.fm ${stationKey} playlist removed for ${userId}` });
      }
    }

    if (!enabledSet.size) return;

    const apiKey = String(config.discovery?.lastfmApiKey || '').trim();
    const masterTracks = getMasterTracks(db);
    const masterTrackLookup = buildPlaylistTrackLookup(masterTracks);

    for (const stationKey of prefs.lastfmEnabledStations) {
      // ── Official Last.fm API: Loved tracks and Top Tracks ─────────────────
      if (stationKey === 'loved' || stationKey.startsWith('topTracks:')) {
        if (!apiKey) {
          ctx.pushLog({ level: 'warn', app: 'playlist', action: 'lastfm.station', message: `Last.fm ${stationKey} sync skipped for ${userId}: no API key configured in Discovery settings` });
          continue;
        }
        let label, apiUrl;
        if (stationKey === 'loved') {
          label = 'Loved';
          apiUrl = `https://ws.audioscrobbler.com/2.0/?method=user.getLovedTracks&user=${encodeURIComponent(prefs.lastfmUsername)}&api_key=${encodeURIComponent(apiKey)}&format=json&limit=500`;
        } else {
          const period = stationKey.slice('topTracks:'.length);
          label = `Top Tracks (${TOP_TRACKS_PERIOD_LABELS[period] || period})`;
          apiUrl = `https://ws.audioscrobbler.com/2.0/?method=user.getTopTracks&user=${encodeURIComponent(prefs.lastfmUsername)}&period=${encodeURIComponent(period)}&api_key=${encodeURIComponent(apiKey)}&format=json&limit=500`;
        }
        const playlistKey = `lastfm:${stationKey}`;
        if (shouldSkipGeneratedPlaylistSync(db, userId, playlistKey, options)) continue;
        const playlistTitle = buildGeneratedPlaylistTitle(config, 'lastfm-station', playlistKey, userId, `Last.fm ${label}`);
        try {
          const res = await fetch(apiUrl);
          if (!res.ok) throw new Error(`Last.fm API HTTP ${res.status}`);
          const data = await res.json();
          const rawTracks = stationKey === 'loved'
            ? (data?.lovedtracks?.track || [])
            : (data?.toptracks?.track || []);

          const ratingKeys = collectMatchedRatingKeys(
            masterTrackLookup,
            rawTracks,
            (track) => track?.artist?.name,
            (track) => track?.name,
            { strictMatch: strictMatchSet.has(stationKey) },
          );

          await syncBuiltSelectionPlaylist(userId, {
            playlistKey,
            playlistType: 'lastfm-station',
            playlistTitle,
            algorithmVersion: 'lastfm-station-v1',
            trackCount: ratingKeys.length,
            trackKeys: ratingKeys,
            tracks: hydratePlaylistTracks(masterTracks, [], ratingKeys),
            options: {
              sortBy: normalizePlaylistBaseSort(prefs.lastfmStationSorts?.[stationKey], 'source'),
              finalOrdering: normalizePlaylistFinalOrdering(prefs.lastfmStationFinalOrderings?.[stationKey], 'none'),
            },
            builtAt: Date.now(),
          }, options);
          ctx.pushLog({ level: 'info', app: 'playlist', action: 'lastfm.station', message: `Last.fm ${label} synced: ${ratingKeys.length}/${rawTracks.length} matched for ${userId}` });
        } catch (err) {
          ctx.pushLog({ level: 'warn', app: 'playlist', action: 'lastfm.station', message: `Last.fm ${label} sync failed for ${userId}: ${err.message}` });
        }
        continue;
      }

      // ── Undocumented station endpoint: Recommended, Mix, Library, Neighbours
      const label = LASTFM_STATION_LABELS[stationKey];
      if (!label) continue;
      const playlistKey = `lastfm:${stationKey}`;
      if (shouldSkipGeneratedPlaylistSync(db, userId, playlistKey, options)) continue;
      const playlistTitle = buildGeneratedPlaylistTitle(config, 'lastfm-station', playlistKey, userId, `Last.fm ${label}`);
      try {
        const res = await fetch(`https://www.last.fm/player/station/user/${encodeURIComponent(prefs.lastfmUsername)}/${stationKey}`);
        if (!res.ok) throw new Error(`Last.fm station HTTP ${res.status}`);
        const data = await res.json();
        const tracks = (data.playlist || []);

        const ratingKeys = collectMatchedRatingKeys(
          masterTrackLookup,
          tracks,
          (track) => track?.artists?.[0]?._name,
          (track) => track?._name,
          { strictMatch: strictMatchSet.has(stationKey) },
        );

        await syncBuiltSelectionPlaylist(userId, {
          playlistKey,
          playlistType: 'lastfm-station',
          playlistTitle,
          algorithmVersion: 'lastfm-station-v1',
          trackCount: ratingKeys.length,
          trackKeys: ratingKeys,
          tracks: hydratePlaylistTracks(masterTracks, [], ratingKeys),
          options: {
            sortBy: normalizePlaylistBaseSort(prefs.lastfmStationSorts?.[stationKey], 'source'),
            finalOrdering: normalizePlaylistFinalOrdering(prefs.lastfmStationFinalOrderings?.[stationKey], 'none'),
          },
          builtAt: Date.now(),
        }, options);
        ctx.pushLog({ level: 'info', app: 'playlist', action: 'lastfm.station', message: `Last.fm ${label} synced: ${ratingKeys.length}/${tracks.length} matched for ${userId}` });
      } catch (err) {
        ctx.pushLog({ level: 'warn', app: 'playlist', action: 'lastfm.station', message: `Last.fm ${label} sync failed for ${userId}: ${err.message}` });
      }
    }
  }

  async function syncListenbrainzPlaylists(userId, options = {}) {
    const prefs = getUserPreferences(db, userId);
    if (!prefs.listenbrainzUsername) {
      ctx.pushLog({
        level: 'info',
        app: 'listenbrainz',
        action: 'sync.skip',
        message: `ListenBrainz sync skipped for ${userId}: no username configured.`,
      });
      return;
    }
    const config = ctx.loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    if (msType === 'jellyfin' || msType === 'emby') {
      ctx.pushLog({
        level: 'info',
        app: 'listenbrainz',
        action: 'sync.skip',
        message: `ListenBrainz playlist sync skipped for ${userId}: media server "${msType}" is not supported for playlist export.`,
      });
      return;
    }
    if (!ctx.userHasOwnPlexToken(config, userId)) {
      ctx.pushLog({
        level: 'warn',
        app: 'listenbrainz',
        action: 'sync.skip',
        message: `ListenBrainz playlist sync skipped for ${userId}: no personal Plex server token is configured.`,
      });
      return;
    }

    const enabledSet = new Set(prefs.listenbrainzEnabledPlaylists || []);
    const existingListenbrainz = listGenerated(userId, { activeOnly: false })
      .filter((playlist) => playlist.playlistKey.startsWith('listenbrainz:') && playlist.active);
    for (const existing of existingListenbrainz) {
      const playlistKey = existing.playlistKey.slice('listenbrainz:'.length);
      if (!enabledSet.has(playlistKey)) {
        try {
          const { url } = config.plex || {};
          const token = ctx.resolveUserPlexServerToken(config, userId);
          const base = String(url || '').replace(/\/$/, '');
          if (base && token && existing.plexPlaylistId) {
            await fetch(`${base}/playlists/${existing.plexPlaylistId}`, {
              method: 'DELETE',
              headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
            });
          }
        } catch (_err) {}
        saveUserGeneratedPlaylist(db, userId, { ...existing, active: false, plexPlaylistId: '', updatedAt: Date.now() });
        ctx.pushLog({ level: 'info', app: 'listenbrainz', action: 'playlist.removed', message: `ListenBrainz ${playlistKey} playlist removed for ${userId}` });
      }
    }

    if (!enabledSet.size) {
      ctx.pushLog({
        level: 'info',
        app: 'listenbrainz',
        action: 'sync.skip',
        message: `ListenBrainz sync skipped for ${userId}: no playlist types are enabled.`,
      });
      return;
    }

    const listenbrainzToken = String(prefs.listenbrainzToken || '').trim();
    ctx.pushLog({
      level: 'info',
      app: 'listenbrainz',
      action: 'sync.start',
      message: `ListenBrainz playlist sync started for ${userId}.`,
      meta: {
        username: prefs.listenbrainzUsername,
        enabledPlaylists: [...enabledSet],
      },
    });
    let createdForJson;
    try {
      createdForJson = await requestListenbrainzJson(
        ctx,
        `https://api.listenbrainz.org/1/user/${encodeURIComponent(prefs.listenbrainzUsername)}/playlists/createdfor`,
        buildListenbrainzHeaders(listenbrainzToken),
        {
          userId,
          requestLabel: 'created-for fetch',
        },
      );
    } catch (err) {
      ctx.pushLog({
        level: 'warn',
        app: 'listenbrainz',
        action: 'sync.error',
        message: `ListenBrainz created-for fetch failed for ${userId}: ${err.message}`,
      });
      return;
    }
    const createdForPlaylists = Array.isArray(createdForJson?.playlists) ? createdForJson.playlists : [];
    const playlistsByType = new Map();
    for (const entry of createdForPlaylists) {
      const sourcePatch = getListenbrainzSourcePatch(entry);
      if (!enabledSet.has(sourcePatch) || playlistsByType.has(sourcePatch)) continue;
      const playlistUuid = extractListenbrainzPlaylistUuid(entry);
      if (!playlistUuid) continue;
      playlistsByType.set(sourcePatch, {
        uuid: playlistUuid,
        title: String(entry?.playlist?.title || '').trim(),
      });
    }
    ctx.pushLog({
      level: 'info',
      app: 'listenbrainz',
      action: 'sync.discovered',
      message: `ListenBrainz discovered ${createdForPlaylists.length} created-for playlists for ${userId}; ${playlistsByType.size} matched enabled types.`,
      meta: {
        username: prefs.listenbrainzUsername,
        discoveredTypes: [...playlistsByType.keys()],
      },
    });

    const masterTrackLookups = buildListenbrainzTrackLookups(getMasterTracks(db));
    const lbStrictMatchSet = new Set(prefs.listenbrainzStrictMatchPlaylists || []);
    for (const playlistType of prefs.listenbrainzEnabledPlaylists || []) {
      const label = LISTENBRAINZ_PLAYLIST_LABELS[playlistType];
      if (!label) continue;
      const playlistMeta = playlistsByType.get(playlistType);
      if (!playlistMeta?.uuid) {
        ctx.pushLog({ level: 'info', app: 'listenbrainz', action: 'playlist.unavailable', message: `ListenBrainz ${playlistType} not available for ${userId}` });
        continue;
      }
      const playlistKey = `listenbrainz:${playlistType}`;
      if (shouldSkipGeneratedPlaylistSync(db, userId, playlistKey, options)) continue;
      const playlistTitle = buildGeneratedPlaylistTitle(
        config,
        'listenbrainz-playlist',
        playlistKey,
        userId,
        playlistMeta.title || `ListenBrainz ${label}`,
      );

      try {
        const payload = await requestListenbrainzJson(
          ctx,
          `https://api.listenbrainz.org/1/playlist/${encodeURIComponent(playlistMeta.uuid)}`,
          buildListenbrainzHeaders(listenbrainzToken),
          {
            userId,
            playlistType,
            uuid: playlistMeta.uuid,
            requestLabel: `${label} playlist fetch`,
          },
        );
        const tracks = Array.isArray(payload?.playlist?.track) ? payload.playlist.track : [];
        ctx.pushLog({
          level: 'info',
          app: 'listenbrainz',
          action: 'playlist.fetched',
          message: `ListenBrainz ${label} fetched ${tracks.length} source tracks for ${userId}.`,
          meta: {
            playlistType,
            uuid: playlistMeta.uuid,
            sourceTracks: tracks.length,
          },
        });
        const { ratingKeys, mbidMatched, textMatched } = collectMatchedListenbrainzTracks(masterTrackLookups, tracks, { strictMatch: lbStrictMatchSet.has(playlistType) });
        if (!ratingKeys.length && tracks.length) {
          ctx.pushLog({
            level: 'warn',
            app: 'listenbrainz',
            action: 'playlist.nomatch',
            message: `ListenBrainz ${label} matched 0/${tracks.length} tracks for ${userId}.`,
            meta: {
              playlistType,
              unmatchedSamples: collectListenbrainzUnmatchedSamples(masterTrackLookups, tracks),
            },
          });
        }

        await syncBuiltSelectionPlaylist(userId, {
          playlistKey,
          playlistType: 'listenbrainz-playlist',
          playlistTitle,
          algorithmVersion: 'listenbrainz-playlist-v2',
          trackCount: ratingKeys.length,
          trackKeys: ratingKeys,
          tracks: hydratePlaylistTracks(getMasterTracks(db), [], ratingKeys),
          options: {
            sortBy: normalizePlaylistBaseSort(prefs.listenbrainzPlaylistSorts?.[playlistType], 'source'),
            finalOrdering: normalizePlaylistFinalOrdering(prefs.listenbrainzPlaylistFinalOrderings?.[playlistType], 'none'),
          },
          builtAt: Date.now(),
        }, options);
        ctx.pushLog({
          level: 'info',
          app: 'listenbrainz',
          action: 'playlist.synced',
          message: `ListenBrainz ${label} synced: ${ratingKeys.length}/${tracks.length} matched for ${userId}`,
          meta: {
            playlistType,
            matched: ratingKeys.length,
            mbidMatched,
            sourceTracks: tracks.length,
            textMatched,
          },
        });
      } catch (err) {
        ctx.pushLog({ level: 'warn', app: 'listenbrainz', action: 'playlist.error', message: `ListenBrainz ${label} sync failed for ${userId}: ${err.message}` });
      }
    }
  }

  async function syncCrescive(userId, options = {}) {
    const smartConfig = ctx.loadConfig().smartPlaylist || {};
    const playlistCfg = {
      capMultiplier:           smartConfig.crescive?.capMultiplier           ?? 1.0,
      favouriteArtistTrackPct: smartConfig.crescive?.favouriteArtistTrackPct ?? 0.80,
      favouriteGenreArtistPct: smartConfig.crescive?.favouriteGenreArtistPct ?? 0.80,
      favouriteGenreTrackPct:  smartConfig.crescive?.favouriteGenreTrackPct  ?? 0.20,
      otherGenreArtistPct:     smartConfig.crescive?.otherGenreArtistPct     ?? 0.20,
      otherGenreTrackPct:      smartConfig.crescive?.otherGenreTrackPct      ?? 0.20,
      sortBy:                  smartConfig.crescive?.sortBy                  ?? 'default',
      finalOrdering:           smartConfig.crescive?.finalOrdering           ?? 'none',
      trackFilters:            smartConfig.crescive?.trackFilters,
    };
    return syncRuleBasedSystemPlaylist(userId, CRESCIVE_PLAYLIST_TYPE, CRESCIVE_PLAYLIST_KEY, playlistCfg, options);
  }

  async function syncCurative(userId, options = {}) {
    const smartConfig = ctx.loadConfig().smartPlaylist || {};
    const playlistCfg = {
      capMultiplier:           smartConfig.curative?.capMultiplier           ?? 1.0,
      favouriteArtistTrackPct: smartConfig.curative?.favouriteArtistTrackPct ?? 1.00,
      favouriteGenreArtistPct: smartConfig.curative?.favouriteGenreArtistPct ?? 1.00,
      favouriteGenreTrackPct:  smartConfig.curative?.favouriteGenreTrackPct  ?? 0.80,
      otherGenreArtistPct:     smartConfig.curative?.otherGenreArtistPct     ?? 0.50,
      otherGenreTrackPct:      smartConfig.curative?.otherGenreTrackPct      ?? 0.50,
      sortBy:                  smartConfig.curative?.sortBy                  ?? 'default',
      finalOrdering:           smartConfig.curative?.finalOrdering           ?? 'none',
      trackFilters:            smartConfig.curative?.trackFilters,
    };
    return syncRuleBasedSystemPlaylist(userId, CURATIVE_PLAYLIST_TYPE, CURATIVE_PLAYLIST_KEY, playlistCfg, options);
  }

  async function syncBothForUser(userId) {
    const smartConfig = ctx.loadConfig().smartPlaylist || {};
    if (smartConfig.enableCrescive !== false) await syncCrescive(userId);
    if (smartConfig.enableCurative !== false) await syncCurative(userId);
  }

  // Remove all playlists of a given type (crescive/curative) from Plex/Jellyfin and the DB for every user.
  // Called when the admin disables the playlist type toggle in settings.
  async function removeSmartPlaylistType(playlistType) {
    const config = ctx.loadConfig();
    const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
    const rows = db.prepare(
      `SELECT user_plex_id, playlist_key, plex_playlist_id FROM user_generated_playlists WHERE playlist_type = ? AND plex_playlist_id IS NOT NULL AND plex_playlist_id != ''`
    ).all(playlistType);

    for (const row of rows) {
      try {
        if (msType === 'jellyfin' || msType === 'emby') {
          const { getAdapter } = await import('./media-servers/index.js');
          const adapter = getAdapter(msType);
          const { url, apiKey } = config[msType] || {};
          if (url && apiKey) {
            const jellyfinUserId = await adapter.getUserIdByName(url, apiKey, row.user_plex_id).catch(() => null);
            if (jellyfinUserId) await adapter.deletePlaylist(url, apiKey, row.plex_playlist_id, jellyfinUserId).catch(() => {});
          }
        } else {
          const { url } = config.plex || {};
          const token = ctx.resolveUserPlexServerToken(config, row.user_plex_id);
          if (url && token) {
            await fetch(`${String(url).replace(/\/$/, '')}/playlists/${row.plex_playlist_id}`, {
              method: 'DELETE',
              headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
            }).catch(() => {});
          }
        }
      } catch (_err) { /* best-effort — DB cleanup always runs */ }

      saveUserGeneratedPlaylist(db, row.user_plex_id, {
        playlistType,
        playlistKey: row.playlist_key,
        plexPlaylistId: '',
        active: false,
        updatedAt: Date.now(),
      });
      ctx.pushLog({ level: 'info', app: 'playlist', action: `${playlistType}.remove`, message: `${playlistType} playlist removed for ${row.user_plex_id}` });
    }
  }

  async function renameGeneratedPlaylistTitle(userPlexId, playlistKey, titleOverride = null, configOverride = null) {
    const existing = getGeneratedByKey(userPlexId, playlistKey);
    if (!existing) return null;
    const config = configOverride || ctx.loadConfig();
    const nextOverride = titleOverride == null ? String(existing.titleOverride || '').trim() : String(titleOverride || '').trim();
    const playlistTitle = nextOverride || buildGeneratedPlaylistTitle(
      config,
      existing.playlistType,
      existing.playlistKey,
      userPlexId,
      existing.playlistTitle,
    );
    if (!playlistTitle) return existing;

    saveUserGeneratedPlaylist(db, userPlexId, {
      ...existing,
      playlistTitle,
      titleOverride: nextOverride,
      updatedAt: Date.now(),
    });

    const updated = getGeneratedByKey(userPlexId, playlistKey);
    if (!updated?.plexPlaylistId) return updated;
    if (!ctx.userHasOwnPlexToken(config, userPlexId)) return updated;
    const base = String(config?.plex?.url || '').trim().replace(/\/$/, '');
    const token = ctx.resolveUserPlexServerToken(config, userPlexId);
    if (!base || !token) return updated;

    await fetch(`${base}/playlists/${updated.plexPlaylistId}?title=${encodeURIComponent(playlistTitle)}`, {
      method: 'PUT',
      headers: ctx.buildPlexAuthHeaders(token, { Accept: 'application/json' }),
    });
    return updated;
  }

  async function renameAllGeneratedPlaylistTitles(configOverride = null) {
    const config = configOverride || ctx.loadConfig();
    const all = listAllGeneratedPlaylists(db);
    let renamed = 0;

    for (const entry of all) {
      try {
        const existing = getGeneratedByKey(entry.userPlexId, entry.playlistKey);
        const nextTitle = resolveGeneratedPlaylistTitle(
          config,
          existing?.playlistType || entry.playlistType,
          entry.playlistKey,
          entry.userPlexId,
          existing,
          entry.playlistTitle,
        );
        if (!nextTitle) continue;
        await renameGeneratedPlaylistTitle(entry.userPlexId, entry.playlistKey, null, config);
        if (nextTitle !== String(existing?.playlistTitle || entry.playlistTitle || '').trim()) renamed += 1;
      } catch (err) {
        ctx.pushLog({
          level: 'warn',
          app: 'playlist',
          action: 'playlist.rename',
          message: `Failed to rename playlist ${entry.playlistKey} for ${entry.userPlexId}: ${err.message}`,
        });
      }
    }

    return { processed: all.length, renamed };
  }

  async function syncGlobalCustomPlaylist(ownerUserId, playlist, options = {}) {
    const trackRefs = getPlaylistTracks(db, ownerUserId, playlist.playlistKey);
    if (!trackRefs.length) return;
    const allUserIds = getAllUserIds(db).filter((uid) => {
      if (!uid) return false;
      const prefs = getUserPreferences(db, uid);
      return Boolean(prefs?.userWizardCompleted);
    });
    for (const userId of allUserIds) {
      try {
        saveUserGeneratedPlaylist(db, userId, {
          ...playlist,
          userPlexId: userId,
          audience: 'global',
          importedSyncPeriod: userId === ownerUserId ? (playlist.importedSyncPeriod || 'disabled') : 'disabled',
          plexPlaylistId: userId === ownerUserId ? (playlist.plexPlaylistId || '') : '',
        });
        setPlaylistTracks(db, userId, playlist.playlistKey, trackRefs);
        const userRecord = listUserGeneratedPlaylists(db, userId, { activeOnly: false })
          .find((p) => p.playlistKey === playlist.playlistKey);
        if (userRecord) {
          await syncCustomPlaylist(userId, userRecord).catch(() => {});
        }
      } catch (err) {
        ctx.pushLog({
          level: 'warn',
          app: 'playlist',
          action: 'import.global.sync-user',
          message: `Failed to sync global imported playlist ${playlist.playlistKey} for ${userId}: ${err?.message || err}`,
        });
      }
    }
    ctx.pushLog({
      level: 'info',
      app: 'playlist',
      action: 'import.global.sync',
      message: `Synced global imported playlist "${playlist.playlistTitle}" (${playlist.playlistKey}) to ${allUserIds.length} user(s)`,
    });
  }

  return {
    listGenerated,
    getGeneratedByKey,
    upsertGenerated,
    getCanonicalPlaylist,
    syncCustomPlaylist,
    syncGlobalCustomPlaylist,
    buildDailyMix,
    buildCuratorr,
    syncDailyMix,
    syncCuratorr,
    syncGlobalPlaylist,
    syncPersonalPlaylist,
    syncLastfmStations,
    syncListenbrainzPlaylists,
    syncCrescive,
    syncCurative,
    syncBothForUser,
    setGeneratedActive,
    updateGeneratedPlaylistArtwork,
    removeSmartPlaylistType,
    renameGeneratedPlaylistTitle,
    renameAllGeneratedPlaylistTitles,
    buildFeaturePresetAvailability,
    CURATORR_PLAYLIST_KEY,
    CRESCIVE_PLAYLIST_KEY,
    CURATIVE_PLAYLIST_KEY,
  };
}
