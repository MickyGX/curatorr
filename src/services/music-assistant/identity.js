// Maps Music Assistant tracks onto Curatorr's canonical track identity: the
// primary media server's rating key in master_tracks. Resolution order:
//   1. MA provider mapping that points at Curatorr's own server
//   2. MusicBrainz recording id
//   3. normalised artist + title (+ duration) via the playlist import matcher
//   4. unmatched (rating_key '')
// Results are cached in ma_track_map, keyed by MA URI (library://track/<id>).

import { getMasterTracks } from '../../db.js';
import { buildSpotifyTrackLookups, pickSpotifyTrackMatch } from '../import-matching.js';

// Full (summary:false) pages are slow while MA is syncing a large library: keep them
// small and give each one a generous timeout.
const LIBRARY_PAGE_SIZE = 200;
const LIBRARY_PAGE_TIMEOUT_MS = 180_000;
const PLEX_ITEM_PREFIX = /^\/library\/metadata\//;

export function ensureMaTables(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS ma_track_map (
      ma_uri        TEXT NOT NULL PRIMARY KEY,
      rating_key    TEXT NOT NULL DEFAULT '',
      match_method  TEXT NOT NULL DEFAULT 'none',
      updated_at    INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
    );
    CREATE INDEX IF NOT EXISTS idx_ma_track_map_rating_key ON ma_track_map(rating_key);
  `);
}

// Plex provider item ids are "/library/metadata/<ratingKey>"; Jellyfin/Emby use the item Id as-is.
export function ratingKeyFromProviderItemId(itemId, providerDomain) {
  const value = String(itemId || '').trim();
  if (!value) return '';
  if (String(providerDomain || '').toLowerCase() === 'plex') return value.replace(PLEX_ITEM_PREFIX, '');
  return value;
}

export function pickServerMapping(providerMappings, { providerInstance = '', serverType = 'plex' } = {}) {
  const mappings = Array.isArray(providerMappings) ? providerMappings : [];
  const instance = String(providerInstance || '').trim();
  if (instance) {
    const exact = mappings.find((m) => m?.provider_instance === instance);
    if (exact) return exact;
  }
  const domain = String(serverType || 'plex').toLowerCase();
  return mappings.find((m) => String(m?.provider_domain || '').toLowerCase() === domain) || null;
}

function recordingMbidOf(track) {
  const ids = Array.isArray(track?.external_ids) ? track.external_ids : [];
  const hit = ids.find((pair) => Array.isArray(pair) && pair[0] === 'musicbrainz_recordingid');
  return String(hit?.[1] || '').trim().toLowerCase();
}

function artistNamesOf(track) {
  if (Array.isArray(track?.artists) && track.artists.length) {
    return track.artists.map((a) => (typeof a === 'string' ? a : a?.name)).filter(Boolean);
  }
  return track?.artist ? [String(track.artist)] : [];
}

const lookupCache = new WeakMap(); // masterTracks array -> lookups (rebuilt when the master cache is refreshed)

export function buildMaLookups(masterTracks) {
  const byRatingKey = new Map();
  const byMbid = new Map();
  for (const track of masterTracks) {
    byRatingKey.set(String(track.ratingKey), track);
    const mbid = String(track.recordingMbid || '').toLowerCase();
    if (mbid && !byMbid.has(mbid)) byMbid.set(mbid, track);
  }
  return { byRatingKey, byMbid, text: buildSpotifyTrackLookups(masterTracks) };
}

function getLookups(db) {
  const masterTracks = getMasterTracks(db);
  let lookups = lookupCache.get(masterTracks);
  if (!lookups) {
    lookups = buildMaLookups(masterTracks);
    lookupCache.set(masterTracks, lookups);
  }
  return lookups;
}

// Pure matcher: `track` is either a full MA Track (provider_mappings, external_ids,
// artists[{name}], duration) or a media_item_played report (mbid, artists[], name, duration).
export function matchMaTrack(lookups, track, { providerInstance = '', serverType = 'plex' } = {}) {
  const mapping = pickServerMapping(track?.provider_mappings, { providerInstance, serverType });
  if (mapping) {
    const ratingKey = ratingKeyFromProviderItemId(mapping.item_id, mapping.provider_domain);
    if (ratingKey && lookups.byRatingKey.has(ratingKey)) return { ratingKey, method: 'provider', track: lookups.byRatingKey.get(ratingKey) };
  }
  const mbid = recordingMbidOf(track) || String(track?.mbid || '').trim().toLowerCase();
  if (mbid && lookups.byMbid.has(mbid)) {
    const hit = lookups.byMbid.get(mbid);
    return { ratingKey: String(hit.ratingKey), method: 'mbid', track: hit };
  }
  const result = pickSpotifyTrackMatch(lookups.text, {
    title: track?.name,
    artists: artistNamesOf(track).map((name) => ({ name })),
    durationMs: Math.round(Number(track?.duration || 0) * 1000),
  });
  if (result?.match?.ratingKey) {
    const ratingKey = String(result.match.ratingKey);
    return { ratingKey, method: 'text', track: lookups.byRatingKey.get(ratingKey) || result.match };
  }
  return { ratingKey: '', method: 'none', track: null };
}

export function createMaIdentity({ db, getOptions, send }) {
  ensureMaTables(db);
  const upsert = db.prepare(`
    INSERT INTO ma_track_map (ma_uri, rating_key, match_method, updated_at) VALUES (?, ?, ?, ?)
    ON CONFLICT(ma_uri) DO UPDATE SET rating_key = excluded.rating_key, match_method = excluded.match_method, updated_at = excluded.updated_at
  `);
  const selectByUri = db.prepare('SELECT rating_key, match_method FROM ma_track_map WHERE ma_uri = ?');
  const deleteByUri = db.prepare('DELETE FROM ma_track_map WHERE ma_uri = ?');

  function store(uri, match) {
    upsert.run(uri, match.ratingKey || '', match.method, Date.now());
  }

  async function fetchTrack(uri) {
    const libraryMatch = /^library:\/\/track\/(.+)$/.exec(uri);
    if (libraryMatch) {
      // allow_update_metadata:false — otherwise every lookup queues a metadata refresh task in MA.
      return send('music/tracks/get', { item_id: libraryMatch[1], provider_instance_id_or_domain: 'library', allow_update_metadata: false });
    }
    return send('music/item_by_uri', { uri, allow_update_metadata: false });
  }

  // Resolve a finished play. `report` is the media_item_played data, used as the
  // fallback when MA can't return the full item.
  async function resolve(uri, report = {}) {
    const lookups = getLookups(db);
    const cached = uri ? selectByUri.get(uri) : null;
    if (cached && cached.rating_key && lookups.byRatingKey.has(cached.rating_key)) {
      return { ratingKey: cached.rating_key, method: cached.match_method, track: lookups.byRatingKey.get(cached.rating_key) };
    }
    let full = null;
    try { full = uri ? await fetchTrack(uri) : null; } catch { full = null; }
    const match = matchMaTrack(lookups, full || report, getOptions());
    if (match.method === 'none' && full) {
      // The report carries the names MA displayed; try them if the full item didn't match.
      const fallback = matchMaTrack(lookups, report, getOptions());
      if (fallback.method !== 'none') {
        if (uri) store(uri, fallback);
        return fallback;
      }
    }
    if (uri) store(uri, match);
    return match;
  }

  // Page through the MA library for the configured provider and pre-fill the map,
  // so finished plays resolve locally. Returns counts.
  async function rebuildIndex({ shouldStop = () => false } = {}) {
    const { providerInstance } = getOptions();
    const lookups = getLookups(db);
    const counts = { scanned: 0, matched: 0, unmatched: 0 };
    let offset = 0;
    for (;;) {
      if (shouldStop()) break;
      const page = await send('music/tracks/library_items', {
        limit: LIBRARY_PAGE_SIZE,
        offset,
        summary: false,
        ...(providerInstance ? { provider: providerInstance } : {}),
      }, { timeoutMs: LIBRARY_PAGE_TIMEOUT_MS });
      const items = Array.isArray(page) ? page : [];
      const write = db.transaction((rows) => {
        for (const item of rows) {
          if (!item?.uri) continue;
          const match = matchMaTrack(lookups, item, getOptions());
          store(item.uri, match);
          counts.scanned += 1;
          if (match.ratingKey) counts.matched += 1;
          else counts.unmatched += 1;
        }
      });
      write(items);
      if (items.length < LIBRARY_PAGE_SIZE) break;
      offset += LIBRARY_PAGE_SIZE;
    }
    return counts;
  }

  function forget(uri) {
    if (uri) deleteByUri.run(uri);
  }

  function lastIndexedAt() {
    return Number(db.prepare('SELECT MAX(updated_at) AS at FROM ma_track_map').get()?.at || 0);
  }

  return { resolve, rebuildIndex, forget, lastIndexedAt };
}
