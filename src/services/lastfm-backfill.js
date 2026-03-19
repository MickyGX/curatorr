// Last.fm History Backfill: manually triggered job that pages backwards through
// a user's full Last.fm scrobble history and inserts plays not already recorded.
//
// Each run fetches up to MAX_PAGES pages working backwards from the current
// cursor (oldest timestamp reached so far). Run multiple times to process a
// large history. The cursor persists between runs so each batch picks up where
// the previous one left off.
//
// Cursor semantics (lastfm_backfill_cursor per user):
//   0   = not started — first run starts from the regular sync watermark going back
//   > 0 = oldest scrobble timestamp processed so far; next run fetches before this
//  -1   = complete — all available history has been imported

import {
  recordPlayEvent,
  updateTrackStats,
  updateArtistStats,
  resolveUserSmartConfig,
  updateLastfmBackfillCursor,
  getLastfmUsers,
} from '../db.js';

const LASTFM_API_BASE    = 'https://ws.audioscrobbler.com/2.0/';
const DEDUP_FORWARD_MS   = 90_000;
const DEDUP_FALLBACK_MS  = 15 * 60_000;
const PAGE_SIZE          = 200;
const MAX_PAGES          = 50;
const REQUEST_TIMEOUT_MS = 15_000;
const PAGE_DELAY_MS      = 250;

function normaliseDashes(s) {
  return String(s || '').replace(/[\u2010\u2011\u2012\u2013\u2014\u2015\u2212\uFE58\uFE63\uFF0D]/g, '-');
}

function findExistingPlay(db, userPlexId, trackTitle, artistName, scrobbleMs, knownDurationMs) {
  const lookback = knownDurationMs > 0 ? knownDurationMs + DEDUP_FORWARD_MS : DEDUP_FALLBACK_MS;
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

function lookupKnownTrackDuration(db, plexRatingKey) {
  const row = db.prepare(`
    SELECT MAX(track_duration_ms) AS dur FROM play_events
    WHERE plex_rating_key = ? AND track_duration_ms > 0
  `).get(plexRatingKey);
  return Number(row?.dur || 0);
}

async function fetchPage(username, apiKey, toTs, page) {
  const url = new URL(LASTFM_API_BASE);
  url.searchParams.set('method', 'user.getRecentTracks');
  url.searchParams.set('user', username);
  url.searchParams.set('api_key', apiKey);
  url.searchParams.set('format', 'json');
  url.searchParams.set('limit', String(PAGE_SIZE));
  url.searchParams.set('page', String(page));
  if (toTs != null) url.searchParams.set('to', String(toTs));
  const r = await fetch(url.toString(), {
    headers: { Accept: 'application/json', 'User-Agent': 'Curatorr/1.0' },
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!r.ok) throw new Error(`Last.fm API returned HTTP ${r.status}`);
  return r.json();
}

async function backfillUser(ctx, { userPlexId, lastfmUsername, cursor, watermark, apiKey, smartSettings, songSkipLimit }) {
  const { db, pushLog } = ctx;

  if (cursor === -1) {
    pushLog({ level: 'info', app: 'lastfm-backfill', action: 'backfill.user.skip', message: `${lastfmUsername}: already complete` });
    return { inserted: 0, skipped: 0, done: true };
  }

  // Start from watermark going back on first run; otherwise continue from cursor
  const toTs = cursor > 0 ? cursor - 1 : (watermark > 0 ? watermark : Math.floor(Date.now() / 1000));

  let inserted = 0;
  let skipped  = 0;
  let page     = 1;
  let totalPages  = 1;
  let oldestSeen  = toTs;
  let anyFound    = false;
  let reachedEnd  = false;  // true when Last.fm returns a partial page (no more history)

  while (page <= totalPages && page <= MAX_PAGES) {
    const json   = await fetchPage(lastfmUsername, apiKey, toTs, page);
    const tracks = json?.recenttracks?.track || [];
    const attr   = json?.recenttracks?.['@attr'] || {};
    totalPages   = Math.min(Number(attr.totalPages || 1), MAX_PAGES);

    for (const track of tracks) {
      if (track['@attr']?.nowplaying === 'true') continue;
      const dateUts = Number(track?.date?.uts || 0);
      if (!dateUts) continue;

      anyFound = true;
      if (dateUts < oldestSeen) oldestSeen = dateUts;

      const startedAtMs = dateUts * 1000;
      const trackTitle  = String(track?.name || '').trim();
      const artistName  = String(track?.artist?.['#text'] || '').trim();
      const albumName   = String(track?.album?.['#text'] || '').trim();
      if (!trackTitle || !artistName) continue;

      const masterMatch     = findMasterTrackMatch(db, artistName, trackTitle);
      const plexRatingKey   = masterMatch?.rating_key ? String(masterMatch.rating_key) : '';
      const resolvedAlbum   = albumName || masterMatch?.album_name || '';
      const trackDurationMs = plexRatingKey ? lookupKnownTrackDuration(db, plexRatingKey) : 0;

      if (findExistingPlay(db, userPlexId, trackTitle, artistName, startedAtMs, trackDurationMs)) {
        skipped++;
        continue;
      }

      const durationMs = trackDurationMs;
      const sessionKey = `lastfm-backfill-${userPlexId}-${artistName}-${trackTitle}-${dateUts}`
        .toLowerCase().replace(/[^\w-]/g, '-');

      recordPlayEvent(db, {
        userPlexId,
        plexRatingKey: plexRatingKey || '',
        trackTitle, artistName,
        albumName: resolvedAlbum,
        libraryKey: '',
        startedAt: startedAtMs,
        endedAt:   startedAtMs + (durationMs || 0),
        durationMs,
        trackDurationMs,
        isSkip:      false,
        eventSource: 'lastfm_backfill',
        sessionKey,
      });

      if (plexRatingKey && trackDurationMs > 0) {
        const trackResult = updateTrackStats(db, {
          userPlexId, plexRatingKey,
          trackTitle, artistName, albumName: resolvedAlbum,
          listenedMs:      durationMs,
          trackDurationMs,
          playedAt:        startedAtMs,
          songSkipLimit,
          smartConfig:     smartSettings,
        });
        updateArtistStats(db, { userPlexId, artistName, isSkip: false, playedAt: startedAtMs, scoreDelta: trackResult.scoreDelta });
      } else {
        updateArtistStats(db, { userPlexId, artistName, isSkip: false, playedAt: startedAtMs, scoreDelta: 0.05 });
      }

      inserted++;
    }

    if (tracks.length < PAGE_SIZE) { reachedEnd = true; break; }
    page++;
    if (page <= totalPages) await new Promise((r) => setTimeout(r, PAGE_DELAY_MS));
  }

  // Done if no tracks were found at all, Last.fm returned a partial page (end of history),
  // or we walked past all available pages
  const exhausted = !anyFound || reachedEnd || page > Number(totalPages);
  const newCursor = exhausted ? -1 : oldestSeen;

  updateLastfmBackfillCursor(db, userPlexId, newCursor);

  const oldestDate = oldestSeen > 0 && oldestSeen < toTs
    ? new Date(oldestSeen * 1000).toISOString().slice(0, 10)
    : null;

  pushLog({
    level: 'info', app: 'lastfm-backfill', action: 'backfill.user.complete',
    message: `${lastfmUsername}: ${inserted} plays added, ${skipped} already recorded${oldestDate ? ` — reached ${oldestDate}` : ''}${exhausted ? ' — complete' : ''}`,
  });

  return { inserted, skipped, done: exhausted, oldestDate };
}

// ─── Main export ─────────────────────────────────────────────────────────────

export async function runLastfmHistoryBackfill(ctx) {
  const { db, loadConfig, pushLog, safeMessage } = ctx;
  const config     = loadConfig();
  const globalApiKey = String(config.discovery?.lastfmApiKey || '').trim();

  if (!globalApiKey) {
    pushLog({ level: 'info', app: 'lastfm-backfill', action: 'backfill.skip', message: 'Last.fm Backfill skipped: no API key configured in Discovery settings' });
    return { inserted: 0, skipped: 0 };
  }

  const users = getLastfmUsers(db);
  if (!users.length) {
    pushLog({ level: 'info', app: 'lastfm-backfill', action: 'backfill.skip', message: 'Last.fm Backfill skipped: no users have a Last.fm username configured' });
    return { inserted: 0, skipped: 0 };
  }

  let totalInserted = 0;
  let totalSkipped  = 0;
  let allDone       = true;
  let oldestReached = null;

  for (const row of users) {
    const userPlexId     = String(row.user_plex_id || '').trim();
    const lastfmUsername = String(row.lastfm_username || '').trim();
    const cursor         = Number(row.lastfm_backfill_cursor ?? 0);
    const watermark      = Number(row.lastfm_sync_watermark || 0);

    if (!userPlexId || !lastfmUsername) continue;

    const smartSettings = resolveUserSmartConfig(db, config, userPlexId);
    const songSkipLimit = Number(smartSettings.songSkipLimit) || 3;

    try {
      const result = await backfillUser(ctx, { userPlexId, lastfmUsername, cursor, watermark, apiKey: globalApiKey, smartSettings, songSkipLimit });
      totalInserted += result.inserted;
      totalSkipped  += result.skipped;
      if (!result.done) allDone = false;
      if (result.oldestDate && (!oldestReached || result.oldestDate < oldestReached)) oldestReached = result.oldestDate;
    } catch (err) {
      allDone = false;
      pushLog({ level: 'error', app: 'lastfm-backfill', action: 'backfill.user.error', message: `Backfill failed for ${lastfmUsername}: ${safeMessage(err)}` });
    }
  }

  const summary = allDone
    ? `Last.fm Backfill complete — all history imported. ${totalInserted} plays added.`
    : `Last.fm Backfill batch done — ${totalInserted} plays added${oldestReached ? `, reached ${oldestReached}` : ''}. Run again to continue.`;

  pushLog({ level: 'info', app: 'lastfm-backfill', action: 'backfill.complete', message: summary });

  return { inserted: totalInserted, skipped: totalSkipped, message: summary };
}
