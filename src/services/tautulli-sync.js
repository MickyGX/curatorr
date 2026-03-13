// Tautulli daily sync: optional backup job that fills gaps missed by Plex
// webhooks. It inserts plays not already present in play_events (deduped by
// user + rating_key + started_at within a ±90s window) and only refines rows
// that were already sourced from Tautulli.
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
  rebuildTrackStatsFromEvents,
  rebuildArtistStatsFromEvents,
  resolveUserSmartConfig,
} from '../db.js';

const DEDUP_WINDOW_MS = 90_000;    // ±90 seconds around started_at
const RESUME_WINDOW_MS = 5 * 60_000; // a session is a "resume" if previous ended within 5 min
const NEARBY_WINDOW_MS = 20 * 60_000; // broader fallback window for drifted Tautulli history rows

function inferTrackDurationMs(row, listenedMs) {
  const fullDurationMs = Number(row.full_duration || 0) * 1000;
  if (fullDurationMs > 0) return fullDurationMs;
  const pct = Number(row.percent_complete || 0);
  if (listenedMs > 0 && pct > 0 && pct <= 100) {
    return Math.round(listenedMs / (pct / 100));
  }
  return 0;
}

// Returns the existing play_events row whose started_at is within ±90s of startedAtMs.
function findExistingPlay(db, userPlexId, plexRatingKey, startedAtMs) {
  return db.prepare(`
    SELECT id, duration_ms, ended_at, event_source FROM play_events
    WHERE user_plex_id = ? AND plex_rating_key = ?
      AND started_at BETWEEN ? AND ?
    LIMIT 1
  `).get(userPlexId, plexRatingKey, startedAtMs - DEDUP_WINDOW_MS, startedAtMs + DEDUP_WINDOW_MS);
}

// Returns an existing play_events row that ended shortly before startedAtMs,
// indicating a resumed/continued session for the same track after a blip.
function findResumedPlay(db, userPlexId, plexRatingKey, startedAtMs) {
  return db.prepare(`
    SELECT id, duration_ms, event_source FROM play_events
    WHERE user_plex_id = ? AND plex_rating_key = ?
      AND ended_at BETWEEN ? AND ?
    ORDER BY ended_at DESC
    LIMIT 1
  `).get(userPlexId, plexRatingKey, startedAtMs - RESUME_WINDOW_MS, startedAtMs + 30_000);
}

function findNearbyRecordedPlay(db, userPlexId, plexRatingKey, startedAtMs, endedAtMs) {
  const effectiveEndedAtMs = Number(endedAtMs || startedAtMs || 0);
  return db.prepare(`
    SELECT id, duration_ms, track_duration_ms, started_at, ended_at, event_source
    FROM play_events
    WHERE user_plex_id = ? AND plex_rating_key = ?
      AND event_source NOT IN ('tautulli_sync', 'tautulli_repair')
      AND (
        started_at BETWEEN ? AND ?
        OR COALESCE(ended_at, started_at + duration_ms, started_at) BETWEEN ? AND ?
      )
    ORDER BY ABS(started_at - ?) ASC, id DESC
    LIMIT 1
  `).get(
    userPlexId,
    plexRatingKey,
    startedAtMs - NEARBY_WINDOW_MS,
    effectiveEndedAtMs + NEARBY_WINDOW_MS,
    startedAtMs - NEARBY_WINDOW_MS,
    effectiveEndedAtMs + NEARBY_WINDOW_MS,
    startedAtMs,
  );
}

function isPlausibleNearbyRefinement(existing, listenedMs, trackDurationMs) {
  const existingDurationMs = Math.max(0, Number(existing?.duration_ms || 0));
  const existingTrackDurationMs = Math.max(0, Number(existing?.track_duration_ms || 0));
  const nextListenedMs = Math.max(0, Number(listenedMs || 0));
  const nextTrackDurationMs = Math.max(0, Number(trackDurationMs || 0));

  if (nextListenedMs <= existingDurationMs) return false;
  if (nextTrackDurationMs > 0 && nextListenedMs > (nextTrackDurationMs * 1.15)) return false;
  if (existingTrackDurationMs > 0 && nextListenedMs > (existingTrackDurationMs * 1.15)) return false;
  if (existingTrackDurationMs > 0 && nextTrackDurationMs > 0) {
    const ratio = Math.max(existingTrackDurationMs, nextTrackDurationMs) / Math.max(1, Math.min(existingTrackDurationMs, nextTrackDurationMs));
    if (ratio > 1.5) return false;
  }
  return true;
}

function isPlexRecordedSource(eventSource) {
  return String(eventSource || '').trim().toLowerCase() === 'plex_webhook';
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
        const listenedMs = Number(row.play_duration || row.duration || 0) * 1000;
        const trackDurationMs = inferTrackDurationMs(row, listenedMs);

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
          if (isPlexRecordedSource(existing.event_source)) {
            skipped++;
            continue;
          }
          // Tautulli's history can be more accurate (pauses excluded) than the
          // viewOffset the webhook captured — update if it shows more listen time.
          if (listenedMs > existing.duration_ms) {
            db.prepare('UPDATE play_events SET duration_ms = ?, is_skip = ? WHERE id = ?')
              .run(listenedMs, isSkip ? 1 : 0, existing.id);
            if (artistName) {
              rebuildTrackStatsFromEvents(db, {
                userPlexId, plexRatingKey, songSkipLimit, smartConfig: smartSettings,
              });
              rebuildArtistStatsFromEvents(db, {
                userPlexId, artistName, smartConfig: smartSettings,
              });
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
          if (isPlexRecordedSource(resumed.event_source)) {
            skipped++;
            continue;
          }
          const combined = resumed.duration_ms + listenedMs;
          // Cap at full track duration if known, to guard against drift/overlap
          const cappedMs = trackDurationMs > 0 ? Math.min(combined, trackDurationMs) : combined;
          const combinedIsSkip = Boolean(trackDurationMs > 0 && cappedMs < skipThresholdMs && !isWatched);
          db.prepare('UPDATE play_events SET duration_ms = ?, ended_at = ?, is_skip = ? WHERE id = ?')
            .run(cappedMs, stoppedAtMs || (startedAtMs + listenedMs), combinedIsSkip ? 1 : 0, resumed.id);
          if (artistName) {
            rebuildTrackStatsFromEvents(db, {
              userPlexId, plexRatingKey, songSkipLimit, smartConfig: smartSettings,
            });
            rebuildArtistStatsFromEvents(db, {
              userPlexId, artistName, smartConfig: smartSettings,
            });
          }
          pushLog({
            level: 'info', app: 'tautulli-sync', action: 'sync.resume-merge',
            message: `Merged resumed session for "${trackTitle}" — ${resumed.duration_ms}ms + ${listenedMs}ms = ${cappedMs}ms`,
          });
          inserted++;
          continue;
        }

        // 3. Broader nearby match: Tautulli history can sometimes drift enough that the
        //    same play lands outside the exact window. If we already have a nearby
        //    non-sync row for this track, prefer refining it or skipping the sync row
        //    rather than inserting a duplicate entry.
        const nearby = findNearbyRecordedPlay(
          db,
          userPlexId,
          plexRatingKey,
          startedAtMs,
          stoppedAtMs || (startedAtMs + listenedMs),
        );

        if (nearby) {
          if (isPlausibleNearbyRefinement(nearby, listenedMs, trackDurationMs)) {
            const nextDurationMs = listenedMs;
            const nextTrackDurationMs = Number(nearby.track_duration_ms || 0) > 0
              ? Number(nearby.track_duration_ms || 0)
              : trackDurationMs;
            db.prepare('UPDATE play_events SET duration_ms = ?, ended_at = ?, is_skip = ?, track_duration_ms = COALESCE(NULLIF(?, 0), track_duration_ms) WHERE id = ?')
              .run(
                nextDurationMs,
                stoppedAtMs || (startedAtMs + listenedMs),
                isSkip ? 1 : 0,
                nextTrackDurationMs,
                nearby.id,
              );
            if (artistName) {
              rebuildTrackStatsFromEvents(db, {
                userPlexId, plexRatingKey, songSkipLimit, smartConfig: smartSettings,
              });
              rebuildArtistStatsFromEvents(db, {
                userPlexId, artistName, smartConfig: smartSettings,
              });
            }
            pushLog({
              level: 'info', app: 'tautulli-sync', action: 'sync.nearby-refine',
              message: `Refined nearby recorded play for "${trackTitle}" from Tautulli history`,
            });
            inserted++;
          } else {
            pushLog({
              level: 'info', app: 'tautulli-sync', action: 'sync.nearby-skip',
              message: `Skipped drifted Tautulli history row for "${trackTitle}" because a nearby recorded play already exists`,
            });
            skipped++;
          }
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
