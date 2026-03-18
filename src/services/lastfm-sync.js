// Last.fm history sync: optional scheduled job that fetches recent scrobbles
// for users who have configured a Last.fm username and inserts plays not already
// present in play_events (deduped by user + artist + track title within ±90s of
// the scrobble timestamp).
//
// Track matching: scrobbles are matched against master_tracks by artist+title.
// Matched tracks get full track_stats updates (using the known track duration
// from prior play_events). Unmatched tracks are recorded in play_events and
// only update artist-level stats — they still contribute to history and artist
// ranking but don't affect per-track tiers.
//
// Duration: Last.fm scrobbles carry no duration. If a prior play_event exists
// for the matched plex_rating_key we use its track_duration_ms, treating the
// scrobble as a completed play. Without duration, track_stats are skipped.

import {
  recordPlayEvent,
  updateTrackStats,
  updateArtistStats,
  resolveUserSmartConfig,
} from '../db.js';

const LASTFM_API_BASE = 'https://ws.audioscrobbler.com/2.0/';
const DEDUP_FORWARD_MS  = 90_000;        // allow 90s after scrobble time
const DEDUP_FALLBACK_MS = 15 * 60_000;  // fallback lookback when duration unknown
const PAGE_SIZE = 200;           // Last.fm max results per page
const MAX_PAGES = 50;            // safety cap per user per run
const REQUEST_TIMEOUT_MS = 15_000;
const PAGE_DELAY_MS = 250;       // respect Last.fm 5 req/sec rate limit

// ─── DB helpers ──────────────────────────────────────────────────────────────

// Last.fm scrobbles at ~50% through a track; Curatorr records started_at at
// track start. The gap is up to trackDurationMs. Use the known duration (plus
// a small buffer) as the backward window so we don't create duplicates.
// Normalise Unicode dash variants (U+2010–U+2015, minus sign, etc.) to ASCII hyphen.
// Tautulli sometimes stores artist names with U+2010 non-breaking hyphens.
function normaliseDashes(s) {
  return String(s || '').replace(/[\u2010\u2011\u2012\u2013\u2014\u2015\u2212\uFE58\uFE63\uFF0D]/g, '-');
}

function findExistingPlay(db, userPlexId, trackTitle, artistName, scrobbleMs, knownDurationMs) {
  const lookback = knownDurationMs > 0
    ? knownDurationMs + DEDUP_FORWARD_MS
    : DEDUP_FALLBACK_MS;
  // Normalise dashes on both sides via SQLite REPLACE for U+2010, then compare.
  // Also match title with a " - Artist" suffix that Tautulli sometimes appends.
  return db.prepare(`
    SELECT id FROM play_events
    WHERE user_plex_id = ?
      AND REPLACE(LOWER(artist_name), char(8208), '-') = LOWER(?)
      AND (
        LOWER(track_title) = LOWER(?)
        OR LOWER(track_title) LIKE LOWER(?) || ' -%'
      )
      AND started_at BETWEEN ? AND ?
    LIMIT 1
  `).get(userPlexId, normaliseDashes(artistName).toLowerCase(), trackTitle, trackTitle, scrobbleMs - lookback, scrobbleMs + DEDUP_FORWARD_MS);
}

function findMasterTrackMatch(db, artistName, trackTitle) {
  return db.prepare(`
    SELECT rating_key, album_name FROM master_tracks
    WHERE LOWER(artist_name) = LOWER(?) AND LOWER(track_title) = LOWER(?)
    LIMIT 1
  `).get(artistName, trackTitle);
}

// Look up the best known track duration from prior play_events for this rating key.
function lookupKnownTrackDuration(db, plexRatingKey) {
  const row = db.prepare(`
    SELECT MAX(track_duration_ms) AS dur FROM play_events
    WHERE plex_rating_key = ? AND track_duration_ms > 0
  `).get(plexRatingKey);
  return Number(row?.dur || 0);
}

// ─── Last.fm API ─────────────────────────────────────────────────────────────

async function fetchRecentTracks(username, apiKey, fromTs, page) {
  const url = new URL(LASTFM_API_BASE);
  url.searchParams.set('method', 'user.getRecentTracks');
  url.searchParams.set('user', username);
  url.searchParams.set('api_key', apiKey);
  url.searchParams.set('format', 'json');
  url.searchParams.set('limit', String(PAGE_SIZE));
  url.searchParams.set('page', String(page));
  if (fromTs > 0) url.searchParams.set('from', String(fromTs));
  const r = await fetch(url.toString(), {
    headers: { Accept: 'application/json', 'User-Agent': 'Curatorr/1.0' },
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!r.ok) throw new Error(`Last.fm API returned HTTP ${r.status}`);
  return r.json();
}

// ─── Per-user sync ────────────────────────────────────────────────────────────

async function syncUserHistory(ctx, { userPlexId, lastfmUsername, watermark, apiKey, smartSettings, songSkipLimit, syncedAt }) {
  const { db, pushLog } = ctx;
  let inserted = 0;
  let skipped = 0;
  let page = 1;
  let totalPages = 1;

  while (page <= totalPages && page <= MAX_PAGES) {
    const json = await fetchRecentTracks(lastfmUsername, apiKey, watermark, page);
    const tracks = json?.recenttracks?.track || [];
    const attr = json?.recenttracks?.['@attr'] || {};
    totalPages = Math.min(Number(attr.totalPages || 1), MAX_PAGES);

    for (const track of tracks) {
      // Skip the currently-playing marker (no date field)
      if (track['@attr']?.nowplaying === 'true') continue;

      const dateUts = Number(track?.date?.uts || 0);
      if (!dateUts) continue;

      const startedAtMs = dateUts * 1000;
      const trackTitle = String(track?.name || '').trim();
      const artistName = String(track?.artist?.['#text'] || '').trim();
      const albumName  = String(track?.album?.['#text'] || '').trim();

      if (!trackTitle || !artistName) continue;

      // Resolve master track first so we have the known duration for dedup window
      const masterMatch = findMasterTrackMatch(db, artistName, trackTitle);
      const plexRatingKey   = masterMatch?.rating_key ? String(masterMatch.rating_key) : '';
      const resolvedAlbum   = albumName || masterMatch?.album_name || '';
      const trackDurationMs = plexRatingKey ? lookupKnownTrackDuration(db, plexRatingKey) : 0;

      // Already recorded? Use track duration to widen lookback window —
      // Last.fm scrobbles at ~50% through, Curatorr stores started_at at track start.
      if (findExistingPlay(db, userPlexId, trackTitle, artistName, startedAtMs, trackDurationMs)) {
        skipped++;
        continue;
      }

      // Last.fm scrobbles = completed plays; treat listened = full duration if known
      const durationMs  = trackDurationMs;
      const sessionKey  = `lastfm-${userPlexId}-${artistName}-${trackTitle}-${dateUts}`
        .toLowerCase().replace(/[^\w-]/g, '-');

      recordPlayEvent(db, {
        userPlexId,
        plexRatingKey: plexRatingKey || '',
        trackTitle, artistName,
        albumName: resolvedAlbum,
        libraryKey: '',
        startedAt: startedAtMs,
        endedAt: startedAtMs + (durationMs || 0),
        durationMs,
        trackDurationMs,
        isSkip: false,
        eventSource: 'lastfm_sync',
        sessionKey,
      });

      if (plexRatingKey && trackDurationMs > 0) {
        // Full stats update — we know the track and its duration
        const trackResult = updateTrackStats(db, {
          userPlexId, plexRatingKey,
          trackTitle, artistName, albumName: resolvedAlbum,
          listenedMs: durationMs,
          trackDurationMs,
          playedAt: startedAtMs,
          songSkipLimit,
          smartConfig: smartSettings,
        });
        updateArtistStats(db, {
          userPlexId, artistName,
          isSkip: false,
          playedAt: startedAtMs,
          scoreDelta: trackResult.scoreDelta,
        });
      } else {
        // Artist-only update — track unmatched or duration unknown
        updateArtistStats(db, {
          userPlexId, artistName,
          isSkip: false,
          playedAt: startedAtMs,
          scoreDelta: 0.05,
        });
      }

      inserted++;
    }

    if (tracks.length < PAGE_SIZE) break;
    page++;
    if (page <= totalPages) await new Promise((resolve) => setTimeout(resolve, PAGE_DELAY_MS));
  }

  // Advance watermark so next run only fetches new scrobbles
  db.prepare('UPDATE user_preferences SET lastfm_sync_watermark = ? WHERE user_plex_id = ?')
    .run(syncedAt, userPlexId);

  pushLog({
    level: 'info', app: 'lastfm-sync', action: 'sync.user.complete',
    message: `Last.fm sync for ${lastfmUsername}: ${inserted} new play${inserted !== 1 ? 's' : ''} backfilled, ${skipped} already recorded`,
  });

  return { inserted, skipped };
}

// ─── Main export ─────────────────────────────────────────────────────────────

export async function runLastfmHistorySync(ctx) {
  const { db, loadConfig, pushLog, safeMessage } = ctx;
  const config = loadConfig();
  const globalApiKey = String(config.discovery?.lastfmApiKey || '').trim();
  if (!globalApiKey) {
    pushLog({ level: 'info', app: 'lastfm-sync', action: 'sync.skip', message: 'Last.fm History Sync skipped: no API key configured in Discovery settings' });
    return { inserted: 0, skipped: 0 };
  }

  const usersWithLastfm = db.prepare(`
    SELECT user_plex_id, lastfm_username, lastfm_sync_watermark
    FROM user_preferences
    WHERE lastfm_username IS NOT NULL AND lastfm_username != ''
  `).all();

  if (!usersWithLastfm.length) {
    pushLog({ level: 'info', app: 'lastfm-sync', action: 'sync.skip', message: 'Last.fm History Sync skipped: no users have a Last.fm username configured' });
    return { inserted: 0, skipped: 0 };
  }

  let totalInserted = 0;
  let totalSkipped = 0;
  const syncedAt = Math.floor(Date.now() / 1000);

  for (const userRow of usersWithLastfm) {
    const userPlexId     = String(userRow.user_plex_id || '').trim();
    const lastfmUsername = String(userRow.lastfm_username || '').trim();
    const watermark      = Number(userRow.lastfm_sync_watermark || 0);

    if (!userPlexId || !lastfmUsername) continue;

    const smartSettings = resolveUserSmartConfig(db, config, userPlexId);
    const songSkipLimit = Number(smartSettings.songSkipLimit) || 3;

    try {
      const { inserted, skipped } = await syncUserHistory(ctx, {
        userPlexId, lastfmUsername, watermark, apiKey: globalApiKey, smartSettings, songSkipLimit, syncedAt,
      });
      totalInserted += inserted;
      totalSkipped  += skipped;
    } catch (err) {
      pushLog({
        level: 'error', app: 'lastfm-sync', action: 'sync.user.error',
        message: `Last.fm sync failed for ${lastfmUsername}: ${safeMessage(err)}`,
      });
    }
  }

  pushLog({
    level: 'info', app: 'lastfm-sync', action: 'sync.complete',
    message: `Last.fm History Sync complete: ${totalInserted} total play${totalInserted !== 1 ? 's' : ''} backfilled across ${usersWithLastfm.length} user${usersWithLastfm.length !== 1 ? 's' : ''}`,
  });

  return { inserted: totalInserted, skipped: totalSkipped };
}
