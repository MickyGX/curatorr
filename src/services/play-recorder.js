// Shared play finalisation for every playback source (Plex/Tautulli webhooks,
// the Jellyfin/Emby poller and Music Assistant). Turns a settled session into a
// play_events row plus track/artist stat updates, reusing or merging recent rows
// so that pause/resume and cross-source duplicates don't double count.

import {
  closeSession,
  recordPlayEvent,
  updateTrackStats,
  updateArtistStats,
  rebuildTrackStatsFromEvents,
  rebuildArtistStatsFromEvents,
  classifyTier,
} from '../db.js';

// Debounce map: after a skip event, schedule a smart-playlist rebuild
const rebuildTimers = new Map(); // userId → timeout handle
const RECENT_PLAY_CONSOLIDATION_WINDOW = 10;
// Same nudge the Last.fm sync gives an artist when the scrobbled track isn't in the library.
const UNMATCHED_PLAY_ARTIST_SCORE_DELTA = 0.05;

export function getPendingRebuildCount() {
  return rebuildTimers.size;
}

export function scheduleRebuild(ctx, userPlexId) {
  const existing = rebuildTimers.get(userPlexId);
  if (existing) clearTimeout(existing);
  const handle = setTimeout(() => {
    rebuildTimers.delete(userPlexId);
    triggerSmartPlaylistRebuild(ctx, userPlexId).catch(() => {});
  }, 30_000); // 30s debounce
  handle.unref?.();
  rebuildTimers.set(userPlexId, handle);
}

async function triggerSmartPlaylistRebuild(ctx, userPlexId) {
  // Imported lazily to avoid circular dependency
  const { rebuildSmartPlaylist } = await import('../routes/api-music.js');
  await rebuildSmartPlaylist(ctx, userPlexId);
}

export function settleSessionProgress(session, endedAt, observedPositionMs = 0) {
  const observed = Math.max(0, Number(observedPositionMs || 0));
  const currentStart = Math.max(0, Number(session?.last_position_ms || 0));
  const currentAccumulated = Math.max(0, Number(session?.accumulated_ms || 0));
  const currentMax = Math.max(0, Number(session?.max_position_ms || 0));
  const playingSince = Number(session?.playing_since || 0) > 0 ? Number(session.playing_since) : null;

  let accumulatedMs = currentAccumulated;
  if (playingSince) {
    if (observed > 0 || currentStart > 0) {
      accumulatedMs += Math.max(0, observed - currentStart);
    } else {
      accumulatedMs += Math.max(0, Number(endedAt || 0) - playingSince);
    }
  }

  const maxPositionMs = Math.max(currentMax, observed, accumulatedMs);
  return {
    accumulatedMs,
    maxPositionMs,
    lastPositionMs: observed > 0 ? observed : currentStart,
  };
}

export function createPlayRecorder(ctx) {
  const { db } = ctx;

  function normalizeHistoryText(value) {
    return String(value || '')
      .normalize('NFKD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[\u2018\u2019\u201A\u201B\u2032\u2035`´]/g, "'")
      .replace(/[\u201C\u201D\u201E\u201F]/g, '"')
      .replace(/\u2026/g, '...')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, ' ')
      .trim();
  }

  function buildPlayEventStrictKey(event) {
    const userPlexId = String(event?.user_plex_id || '').trim();
    const plexRatingKey = String(event?.plex_rating_key || '').trim();
    if (plexRatingKey) return `${userPlexId}::${plexRatingKey}`;
    return '';
  }

  function buildPlayEventLooseKey(event) {
    const userPlexId = String(event?.user_plex_id || '').trim();
    const trackTitle = normalizeHistoryText(event?.track_title);
    const artistName = normalizeHistoryText(event?.artist_name);
    if (trackTitle && artistName) return `${userPlexId}::${trackTitle}::${artistName}`;
    return [
      userPlexId,
      trackTitle,
      artistName,
      normalizeHistoryText(event?.album_name),
    ].join('::');
  }

  function findRecentPlayMergeCandidate(session) {
    const recentEvents = db.prepare(`
      SELECT id, user_plex_id, plex_rating_key, track_title, artist_name, album_name, library_key,
             started_at, ended_at, duration_ms, track_duration_ms, is_skip, event_source, session_key
      FROM play_events
      WHERE user_plex_id = ?
      ORDER BY started_at DESC, id DESC
      LIMIT ?
    `).all(session.user_plex_id, RECENT_PLAY_CONSOLIDATION_WINDOW);
    const targetStrictKey = buildPlayEventStrictKey(session);
    const targetLooseKey = buildPlayEventLooseKey(session);
    const strictMatch = targetStrictKey
      ? recentEvents.find((event) => buildPlayEventStrictKey(event) === targetStrictKey)
      : null;
    if (strictMatch) return strictMatch;
    return targetLooseKey
      ? recentEvents.find((event) => buildPlayEventLooseKey(event) === targetLooseKey)
      : null;
  }

  function rebuildAffectedPlayStats({ userPlexId, plexRatingKey, artistName, smartSettings }) {
    const songSkipLimit = Number(smartSettings.songSkipLimit) || 3;
    const trackSnapshot = rebuildTrackStatsFromEvents(db, {
      userPlexId,
      plexRatingKey,
      songSkipLimit,
      smartConfig: smartSettings,
    });
    const artistSnapshot = artistName
      ? rebuildArtistStatsFromEvents(db, { userPlexId, artistName, smartConfig: smartSettings })
      : null;
    return { trackSnapshot, artistSnapshot };
  }

  function recordOrUpdateSessionPlay({
    session,
    endedAt,
    playbackPositionMs = 0,
    smartSettings,
    eventSource = 'plex_webhook',
    allowRecentPlayReuse = true,
    // Sources that already know exact play boundaries (Music Assistant) keep
    // session_key continuation but opt out of merging into a recent same-track play.
    allowMergeCandidate = allowRecentPlayReuse,
    mergeIncremental = false,
  }) {
    if (!session) return { duplicate: true, listenedMs: 0, isSkip: false };
    const skipThresholdMs = (Number(smartSettings.skipThresholdSeconds) || 20) * 1000;
    const songSkipLimit = Number(smartSettings.songSkipLimit) || 3;
    const completionThresholdMs = (Number(smartSettings.completionThresholdSeconds) || 20) * 1000;

    const settled = settleSessionProgress(session, endedAt, playbackPositionMs);
    let listenedMs = Math.max(
      settled.accumulatedMs,
      settled.maxPositionMs,
      Math.max(0, Number(playbackPositionMs || 0)),
    );
    const resolvedTrackDuration = Math.max(0, Number(session.track_duration_ms || 0));
    if (resolvedTrackDuration > 0) listenedMs = Math.min(listenedMs, resolvedTrackDuration);

    const isSkip = Boolean(
      resolvedTrackDuration > 0
      && listenedMs < skipThresholdMs,
    );

    const recentCutoff = Date.now() - 10 * 60 * 1000;

    const existing = allowRecentPlayReuse ? db.prepare(`SELECT id, duration_ms, started_at, ended_at FROM play_events WHERE session_key = ? AND plex_rating_key = ? AND ended_at > ? AND started_at >= ? ORDER BY ended_at DESC, id DESC LIMIT 1`).get(session.session_key, session.plex_rating_key, recentCutoff, Number(session.started_at || endedAt) - 60 * 1000) : null;

    let finalizedListenedMs = listenedMs;
    let finalizedTrackDurationMs = resolvedTrackDuration;
    let effectiveIsSkip = isSkip;
    let usedRebuildPath = false;
    let rebuildRatingKey = session.plex_rating_key;

    const isExistingRowContinuation = existing
      ? Number(session.started_at || 0) < Number(existing.ended_at || 0)
      : false;

    if (existing && isExistingRowContinuation) {
      if (listenedMs <= Number(existing.duration_ms || 0)) {
        closeSession(db, session.session_key);
        return { duplicate: true, listenedMs, isSkip };
      }
      db.prepare('UPDATE play_events SET duration_ms = ?, ended_at = ?, is_skip = ?, track_duration_ms = COALESCE(NULLIF(?, 0), track_duration_ms) WHERE id = ?')
        .run(listenedMs, endedAt, isSkip ? 1 : 0, resolvedTrackDuration, existing.id);
      usedRebuildPath = true;
    } else {

      const mergeCandidate = allowMergeCandidate ? findRecentPlayMergeCandidate(session) : null;
      if (mergeCandidate) {
        const mergeIncrementMs = mergeIncremental
          ? Math.max(0, Number(settled.accumulatedMs || 0))
          : listenedMs;
        finalizedTrackDurationMs = Math.max(
          Number(mergeCandidate.track_duration_ms || 0),
          resolvedTrackDuration,
        );
        finalizedListenedMs = Math.max(0, Number(mergeCandidate.duration_ms || 0)) + mergeIncrementMs;
        if (finalizedTrackDurationMs > 0) {
          finalizedListenedMs = Math.min(finalizedListenedMs, finalizedTrackDurationMs);
        }
        effectiveIsSkip = classifyTier(finalizedListenedMs, finalizedTrackDurationMs, smartSettings) === 'skip';
        db.prepare(`
          UPDATE play_events SET
            track_title = ?, artist_name = ?, album_name = ?, library_key = ?,
            started_at = ?, ended_at = ?, duration_ms = ?, track_duration_ms = ?, is_skip = ?,
            event_source = ?, session_key = ?
          WHERE id = ?
        `).run(
          session.track_title || mergeCandidate.track_title || '',
          session.artist_name || mergeCandidate.artist_name || '',
          session.album_name || mergeCandidate.album_name || '',
          session.library_key || mergeCandidate.library_key || '',
          Number(session.started_at || endedAt),
          endedAt,
          finalizedListenedMs,
          finalizedTrackDurationMs,
          effectiveIsSkip ? 1 : 0,
          eventSource,
          session.session_key,
          mergeCandidate.id,
        );
        usedRebuildPath = true;
        rebuildRatingKey = mergeCandidate.plex_rating_key || session.plex_rating_key;
      } else {
        recordPlayEvent(db, {
          userPlexId: session.user_plex_id,
          plexRatingKey: session.plex_rating_key,
          trackTitle: session.track_title || '',
          artistName: session.artist_name || '',
          albumName: session.album_name || '',
          libraryKey: session.library_key || '',
          startedAt: Number(session.started_at || endedAt),
          endedAt,
          durationMs: listenedMs,
          trackDurationMs: resolvedTrackDuration,
          isSkip,
          eventSource,
          sessionKey: session.session_key,
        });
      }
    }
    if (session.artist_name && !String(rebuildRatingKey || '').trim()) {
      // Unmatched track (no library rating key, e.g. a Music Assistant play from a
      // streaming provider): there is no track_stats row to maintain, so only the
      // artist is credited — mirrors the Last.fm sync artist-only path.
      if (usedRebuildPath) {
        rebuildArtistStatsFromEvents(db, {
          userPlexId: session.user_plex_id,
          artistName: session.artist_name,
          smartConfig: smartSettings,
        });
      } else {
        updateArtistStats(db, {
          userPlexId: session.user_plex_id,
          artistName: session.artist_name,
          isSkip,
          playedAt: endedAt,
          scoreDelta: isSkip ? 0 : UNMATCHED_PLAY_ARTIST_SCORE_DELTA,
        });
      }
    } else if (session.artist_name) {
      if (usedRebuildPath) {
        const { trackSnapshot } = rebuildAffectedPlayStats({
          userPlexId: session.user_plex_id,
          plexRatingKey: rebuildRatingKey,
          artistName: session.artist_name || '',
          smartSettings,
        });
        effectiveIsSkip = trackSnapshot?.tier === 'skip';
      } else {
        const trackResult = updateTrackStats(db, {
          userPlexId: session.user_plex_id,
          plexRatingKey: session.plex_rating_key,
          trackTitle: session.track_title || '',
          artistName: session.artist_name || '',
          albumName: session.album_name || '',
          listenedMs,
          trackDurationMs: resolvedTrackDuration,
          playedAt: endedAt,
          songSkipLimit,
          smartConfig: smartSettings,
        });
        effectiveIsSkip = trackResult.isSkip;
        updateArtistStats(db, {
          userPlexId: session.user_plex_id,
          artistName: session.artist_name || '',
          isSkip: effectiveIsSkip,
          playedAt: endedAt,
          scoreDelta: trackResult.scoreDelta,
        });
      }
    }

    closeSession(db, session.session_key);
    const isCompletion = !effectiveIsSkip
      && finalizedTrackDurationMs > 0
      && finalizedListenedMs >= finalizedTrackDurationMs - completionThresholdMs;
    if (effectiveIsSkip || isCompletion) scheduleRebuild(ctx, session.user_plex_id);
    return { duplicate: false, listenedMs: finalizedListenedMs, isSkip: effectiveIsSkip };
  }

  return { recordOrUpdateSessionPlay };
}
