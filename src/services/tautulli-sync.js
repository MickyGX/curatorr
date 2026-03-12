// Tautulli daily sync: backfills any play history missed by real-time webhooks.
// Runs periodically (default: every 24h) and fetches recent Tautulli history,
// inserting only plays not already present in play_events (deduped by user +
// rating_key + started_at within a ±90s window).
//
// Split-session handling: a network blip can cause Tautulli to record the same
// listen as two separate sessions (e.g. 1:30 + 3:25 for a single 4:55 play).
// When we encounter a Tautulli session for a track that ended recently (within
// RESUME_WINDOW_MS before this session started), we treat it as a continuation
// and accumulate the durations rather than inserting a duplicate entry.

import {
  recordPlayEvent,
  updateTrackStats,
  updateArtistStats,
  updateTrackTierOnly,
  adjustArtistScore,
  resolveUserSmartConfig,
} from '../db.js';

const DEDUP_WINDOW_MS = 90_000;    // ±90 seconds around started_at
const RESUME_WINDOW_MS = 5 * 60_000; // a session is a "resume" if previous ended within 5 min

// Returns the existing play_events row whose started_at is within ±90s of startedAtMs.
function findExistingPlay(db, userPlexId, plexRatingKey, startedAtMs) {
  return db.prepare(`
    SELECT id, duration_ms, ended_at FROM play_events
    WHERE user_plex_id = ? AND plex_rating_key = ?
      AND started_at BETWEEN ? AND ?
    LIMIT 1
  `).get(userPlexId, plexRatingKey, startedAtMs - DEDUP_WINDOW_MS, startedAtMs + DEDUP_WINDOW_MS);
}

// Returns an existing play_events row that ended shortly before startedAtMs,
// indicating a resumed/continued session for the same track after a blip.
function findResumedPlay(db, userPlexId, plexRatingKey, startedAtMs) {
  return db.prepare(`
    SELECT id, duration_ms FROM play_events
    WHERE user_plex_id = ? AND plex_rating_key = ?
      AND ended_at BETWEEN ? AND ?
    ORDER BY ended_at DESC
    LIMIT 1
  `).get(userPlexId, plexRatingKey, startedAtMs - RESUME_WINDOW_MS, startedAtMs + 30_000);
}

export async function runTautulliDailySync(ctx, { lookbackHours = 26 } = {}) {
  const { db, loadConfig, pushLog, safeMessage } = ctx;
  const config = loadConfig();
  const tautulliUrl = config.tautulli?.url;
  const apiKey = config.tautulli?.apiKey;

  if (!tautulliUrl || !apiKey) {
    pushLog({ level: 'info', app: 'tautulli-sync', action: 'sync.skip', message: 'Tautulli not configured — skipping sync' });
    return { inserted: 0, skipped: 0 };
  }

  const api = `${tautulliUrl.replace(/\/$/, '')}/api/v2`;
  const afterTs = Math.floor((Date.now() - lookbackHours * 60 * 60 * 1000) / 1000);

  let inserted = 0;
  let skipped = 0;
  let start = 0;
  const pageSize = 100;

  try {
    while (true) {
      const body = new URLSearchParams({
        apikey: apiKey,
        cmd: 'get_history',
        media_type: 'track',
        length: String(pageSize),
        start: String(start),
        order_column: 'date',
        order_dir: 'desc',
        after: String(afterTs),
      });
      const res = await fetch(api, {
        method: 'POST',
        headers: { Accept: 'application/json' },
        body,
      });
      if (!res.ok) throw new Error(`Tautulli API returned HTTP ${res.status}`);
      const json = await res.json();

      const rows = json?.response?.data?.data || [];
      if (!rows.length) break;

      for (const row of rows) {
        if (row.media_type !== 'track') continue;

        const userPlexId = String(row.user || row.user_id || '').trim();
        const plexRatingKey = String(row.rating_key || '').trim();
        if (!userPlexId || !plexRatingKey) continue;

        const startedAtMs = Number(row.started) * 1000;
        const stoppedAtMs = Number(row.stopped) * 1000;
        // Tautulli history `duration` is wall-clock session time (stopped - started),
        // which includes pause time. Subtract `paused_counter` to get active listen time.
        const rawDurationMs = Number(row.duration || 0) * 1000;
        const pausedMs = Number(row.paused_counter || 0) * 1000;
        const listenedMs = Math.max(0, rawDurationMs - pausedMs);
        const trackDurationMs = Number(row.full_duration || 0) * 1000;

        if (!startedAtMs) continue;

        const trackTitle = String(row.title || '').trim();
        const artistName = String(row.original_title || row.grandparent_title || '').trim();
        const albumName = String(row.parent_title || '').trim();
        const libraryKey = String(row.section_id || '').trim();

        const smartSettings = resolveUserSmartConfig(db, config, userPlexId);
        const skipThresholdMs = (Number(smartSettings.skipThresholdSeconds) || 20) * 1000;
        const songSkipLimit = Number(smartSettings.songSkipLimit) || 3;

        // watched_status 1 = watched/completed in Tautulli
        const isWatched = row.watched_status === 1 || row.watched_status === '1';
        const isSkip = Boolean(
          trackDurationMs > 0
          && listenedMs < skipThresholdMs
          && !isWatched,
        );

        // 1. Exact match: a play already recorded near the same started_at.
        const existing = findExistingPlay(db, userPlexId, plexRatingKey, startedAtMs);

        if (existing) {
          // Tautulli's history can be more accurate (pauses excluded) than the
          // viewOffset the webhook captured — update if it shows more listen time.
          if (listenedMs > existing.duration_ms) {
            db.prepare('UPDATE play_events SET duration_ms = ?, is_skip = ? WHERE id = ?')
              .run(listenedMs, isSkip ? 1 : 0, existing.id);
            if (artistName) {
              const { scoreDelta } = updateTrackTierOnly(db, {
                userPlexId, plexRatingKey, listenedMs, trackDurationMs, smartConfig: smartSettings,
              });
              if (scoreDelta) adjustArtistScore(db, { userPlexId, artistName, scoreDelta });
            }
            inserted++;
          } else {
            skipped++;
          }
          continue;
        }

        // 2. Resume match: a play ended shortly before this session started, meaning a
        //    network blip split one listen into two Tautulli sessions. Accumulate durations.
        const resumed = findResumedPlay(db, userPlexId, plexRatingKey, startedAtMs);

        if (resumed) {
          const combined = resumed.duration_ms + listenedMs;
          // Cap at full track duration if known, to guard against drift/overlap
          const cappedMs = trackDurationMs > 0 ? Math.min(combined, trackDurationMs) : combined;
          const combinedIsSkip = Boolean(trackDurationMs > 0 && cappedMs < skipThresholdMs && !isWatched);
          db.prepare('UPDATE play_events SET duration_ms = ?, ended_at = ?, is_skip = ? WHERE id = ?')
            .run(cappedMs, stoppedAtMs || (startedAtMs + listenedMs), combinedIsSkip ? 1 : 0, resumed.id);
          if (artistName) {
            const { scoreDelta } = updateTrackTierOnly(db, {
              userPlexId, plexRatingKey, listenedMs: cappedMs, trackDurationMs, smartConfig: smartSettings,
            });
            if (scoreDelta) adjustArtistScore(db, { userPlexId, artistName, scoreDelta });
          }
          pushLog({
            level: 'info', app: 'tautulli-sync', action: 'sync.resume-merge',
            message: `Merged resumed session for "${trackTitle}" — ${resumed.duration_ms}ms + ${listenedMs}ms = ${cappedMs}ms`,
          });
          inserted++;
          continue;
        }

        const sessionKey = `tautulli-sync-${userPlexId}-${plexRatingKey}-${startedAtMs}`;

        recordPlayEvent(db, {
          userPlexId, plexRatingKey,
          trackTitle, artistName, albumName, libraryKey,
          startedAt: startedAtMs,
          endedAt: stoppedAtMs || (startedAtMs + listenedMs),
          durationMs: listenedMs,
          trackDurationMs,
          isSkip,
          eventSource: 'tautulli_sync',
          sessionKey,
        });

        if (artistName) {
          const trackResult = updateTrackStats(db, {
            userPlexId, plexRatingKey,
            trackTitle, artistName, albumName,
            listenedMs, trackDurationMs,
            playedAt: startedAtMs,
            songSkipLimit,
            smartConfig: smartSettings,
          });
          updateArtistStats(db, {
            userPlexId, artistName,
            isSkip: trackResult.isSkip,
            playedAt: startedAtMs,
            scoreDelta: trackResult.scoreDelta,
          });
        }

        inserted++;
      }

      start += pageSize;
      if (rows.length < pageSize) break;
    }
  } catch (err) {
    pushLog({ level: 'error', app: 'tautulli-sync', action: 'sync.error', message: safeMessage(err) });
    throw err;
  }

  pushLog({
    level: 'info', app: 'tautulli-sync', action: 'sync.complete',
    message: `Tautulli sync: ${inserted} new play${inserted !== 1 ? 's' : ''} backfilled, ${skipped} already recorded`,
  });
  return { inserted, skipped };
}
