// Webhook receiver for Tautulli and Plex notifications.
// Note: Plex webhooks use multipart/form-data which express.json() doesn't parse.
// readRawBody captures the raw body for multipart routes before normal parsing.

import crypto from 'crypto';

// Middleware: capture raw body buffer (only when body not already parsed)
function readRawBody(req, res, next) {
  const contentType = String(req.headers?.['content-type'] || '');
  const isMultipart = /multipart\/form-data/i.test(contentType);
  const parsedBody = req.body;
  const hasParsedPayload = Boolean(
    parsedBody
    && typeof parsedBody === 'object'
    && !Buffer.isBuffer(parsedBody)
    && Object.keys(parsedBody).length > 0,
  );
  if (!isMultipart && parsedBody !== undefined) return next();
  if (isMultipart && hasParsedPayload) return next();
  if (req.readableEnded) {
    req.rawBody = Buffer.alloc(0);
    return next();
  }
  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', () => { req.rawBody = Buffer.concat(chunks); next(); });
  req.on('error', next);
}

// Extract a named field value from a multipart/form-data buffer
function extractMultipartField(buf, contentType, fieldName) {
  const m = contentType.match(/boundary=([^\s;]+)/i);
  if (!m) return null;
  const boundary = '--' + m[1].replace(/^"(.*)"$/, '$1');
  const text = buf.toString('utf8');
  const parts = text.split(boundary);
  for (const part of parts) {
    const sep = part.indexOf('\r\n\r\n');
    if (sep === -1) continue;
    const headers = part.slice(0, sep);
    if (!headers.includes(`name="${fieldName}"`)) continue;
    const body = part.slice(sep + 4);
    const end = body.lastIndexOf('\r\n');
    return end === -1 ? body : body.slice(0, end);
  }
  return null;
}

function timingSafeEqual(left, right) {
  const a = Buffer.from(String(left || ''), 'utf8');
  const b = Buffer.from(String(right || ''), 'utf8');
  return a.length === b.length && a.length > 0 && crypto.timingSafeEqual(a, b);
}

function resolveWebhookKey(req) {
  return String(
    req.query?.key
      || req.headers?.['x-curatorr-webhook-key']
      || req.headers?.['x-webhook-secret']
      || '',
  ).trim();
}

function inferTautulliTrackDurationMs(body) {
  const durationMinutesMs = Number(body.duration || 0) * 60 * 1000;
  const viewOffsetMs = Number(body.view_offset || 0);
  const progressPct = Number(body.progress_percent || 0);
  const inferredFromProgressMs = (
    viewOffsetMs > 0
    && progressPct > 0
    && progressPct <= 100
  ) ? Math.round(viewOffsetMs / (progressPct / 100)) : 0;
  if (durationMinutesMs > 0 && inferredFromProgressMs > 0) {
    return Math.min(durationMinutesMs, inferredFromProgressMs);
  }
  return inferredFromProgressMs || durationMinutesMs || 0;
}

function resolvePlexSessionKey(payload, userPlexId, plexRatingKey) {
  const explicitSessionKey = String(
    payload?.PlaySessionStateNotification?.sessionKey
    || payload?.Session?.id
    || payload?.sessionKey
    || '',
  ).trim();
  if (explicitSessionKey) return `plex-${explicitSessionKey}`;

  const playerUuid = String(payload?.Player?.uuid || '').trim();
  if (playerUuid) return `plex-${userPlexId}-${playerUuid}-${plexRatingKey}`;

  return `plex-${userPlexId}-${plexRatingKey}`;
}

function resolvePlexPlayerScope(payload, userPlexId) {
  const playerUuid = String(payload?.Player?.uuid || '').trim();
  return playerUuid ? `plex-${userPlexId}-${playerUuid}-` : '';
}

function resolveTautulliPlayerScope(body, userPlexId) {
  const playerIdentity = String(
    body?.machine_id
      || body?.machineId
      || body?.player
      || body?.player_title
      || body?.device
      || '',
  ).trim();
  return playerIdentity
    ? `tautulli-${userPlexId}-${playerIdentity}-`
    : `tautulli-${userPlexId}-`;
}
// Both sources emit play/stop/pause/resume events; we deduplicate by session_key.
// Skip detection: if a track stops before skipThresholdSeconds, it's a skip.

import {
  openSession,
  getOpenSession,
  closeSession,
  recordPlayEvent,
  updateTrackStats,
  updateArtistStats,
  expireOldSessions,
  resolveUserSmartConfig,
} from '../db.js';

// Debounce map: after a skip event, schedule a smart-playlist rebuild
const rebuildTimers = new Map(); // userId → timeout handle

function scheduleRebuild(ctx, userPlexId) {
  const existing = rebuildTimers.get(userPlexId);
  if (existing) clearTimeout(existing);
  const handle = setTimeout(() => {
    rebuildTimers.delete(userPlexId);
    triggerSmartPlaylistRebuild(ctx, userPlexId).catch(() => {});
  }, 30_000); // 30s debounce
  rebuildTimers.set(userPlexId, handle);
}

async function triggerSmartPlaylistRebuild(ctx, userPlexId) {
  // Imported lazily to avoid circular dependency
  const { rebuildSmartPlaylist } = await import('./api-music.js');
  await rebuildSmartPlaylist(ctx, userPlexId);
}

// ─── Tautulli webhook ─────────────────────────────────────────────────────────
//
// Configure in Tautulli → Notification Agents → Webhook:
//   URL: http://curatorr:7676/webhook/tautulli
//   Method: POST
//   Triggers: Playback Start, Playback Stop, Playback Pause, Playback Resume, Watched
//
// JSON body parameters (set in Tautulli notification agent body):
// {
//   "action":        "{action}",
//   "session_key":   "{session_key}",
//   "user":          "{username}",
//   "user_id":       "{user_id}",
//   "media_type":    "{media_type}",
//   "rating_key":    "{rating_key}",
//   "title":         "{title}",
//   "parent_title":  "{parent_title}",
//   "grandparent_title": "{grandparent_title}",
//   "original_title": "{original_title}",
//   "library_name":  "{library_name}",
//   "section_id":    "{section_id}",
//   "duration":      "{duration}",
//   "progress_percent": "{progress_percent}",
//   "view_offset":   "{view_offset}"
// }

export function registerWebhooks(app, ctx) {
  const {
    db,
    loadConfig,
    pushLog,
    safeMessage,
    getWebhookSharedSecret,
    buildAppApiUrl,
    buildPlexAuthHeaders,
  } = ctx;
  const plexDurationCache = new Map();

  function resolveLivePlaybackSource() {
    const config = loadConfig();
    return String(config?.general?.playbackSource || 'plex').trim().toLowerCase() === 'tautulli'
      ? 'tautulli'
      : 'plex';
  }

  function purgeInactiveSourceSessions(activeSource = resolveLivePlaybackSource()) {
    const activeEventSource = activeSource === 'tautulli' ? 'tautulli' : 'plex_webhook';
    const stale = db.prepare(
      'SELECT session_key, event_source FROM open_sessions WHERE event_source != ?',
    ).all(activeEventSource);
    if (!stale.length) return 0;
    db.prepare('DELETE FROM open_sessions WHERE event_source != ?').run(activeEventSource);
    pushLog({
      level: 'info',
      app: 'webhook',
      action: 'sessions.purged',
      message: `Purged ${stale.length} stale open session(s) for inactive playback sources.`,
      meta: { activeSource, purged: stale.map((row) => ({ sessionKey: row.session_key, eventSource: row.event_source })) },
    });
    return stale.length;
  }

  function requireWebhookKey(req, res, source) {
    const config = loadConfig();
    const expectedKey = String(getWebhookSharedSecret(config) || '').trim();
    const providedKey = resolveWebhookKey(req);
    if (timingSafeEqual(expectedKey, providedKey)) return true;
    pushLog({
      level: 'warn',
      app: 'webhook',
      action: `${source}.auth`,
      message: `Rejected ${source} webhook with invalid or missing key.`,
    });
    res.status(401).json({ error: 'Invalid webhook key.' });
    return false;
  }

  async function resolvePlexTrackDurationMs(plexRatingKey, fallbackDurationMs = 0) {
    const fallback = Math.max(0, Number(fallbackDurationMs || 0));
    if (fallback > 0) return fallback;
    const ratingKey = String(plexRatingKey || '').trim();
    if (!ratingKey) return 0;

    const cached = plexDurationCache.get(ratingKey);
    if (cached && cached.expiresAt > Date.now()) return cached.durationMs;

    const config = loadConfig();
    const plexUrl = String(config?.plex?.localUrl || config?.plex?.url || '').trim();
    const token = String(config?.plex?.token || '').trim();
    if (!plexUrl || !token) return 0;

    try {
      const metaUrl = buildAppApiUrl(plexUrl, `library/metadata/${encodeURIComponent(ratingKey)}`);
      const response = await fetch(metaUrl.toString(), {
        headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
        signal: AbortSignal.timeout(8000),
      });
      if (!response.ok) return 0;
      const json = await response.json();
      const durationMs = Math.max(0, Number(json?.MediaContainer?.Metadata?.[0]?.duration || 0));
      if (durationMs > 0) {
        plexDurationCache.set(ratingKey, {
          durationMs,
          expiresAt: Date.now() + (12 * 60 * 60 * 1000),
        });
      }
      return durationMs;
    } catch (_err) {
      return 0;
    }
  }

  function saveSessionState(session, fallbackEventSource = 'plex_webhook') {
    openSession(db, {
      sessionKey: session.session_key,
      userPlexId: session.user_plex_id,
      plexRatingKey: session.plex_rating_key,
      trackTitle: session.track_title || '',
      artistName: session.artist_name || '',
      albumName: session.album_name || '',
      libraryKey: session.library_key || '',
      trackDurationMs: Number(session.track_duration_ms || 0),
      startedAt: Number(session.started_at || Date.now()),
      eventSource: session.event_source || fallbackEventSource,
      playerScope: session.player_scope || '',
      playbackState: session.playback_state || 'playing',
      lastPositionMs: Number(session.last_position_ms || 0),
      maxPositionMs: Number(session.max_position_ms || 0),
      accumulatedMs: Number(session.accumulated_ms || 0),
      playingSince: Number(session.playing_since || 0) > 0 ? Number(session.playing_since) : null,
      lastEventAt: Number(session.last_event_at || Date.now()),
    });
  }

  function settleSessionProgress(session, endedAt, observedPositionMs = 0) {
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

  function continueSession(session, {
    endedAt,
    observedPositionMs = 0,
    playbackState = 'playing',
    trackDurationMs = 0,
    trackTitle = '',
    artistName = '',
    albumName = '',
  }) {
    const observed = Math.max(0, Number(observedPositionMs || 0));
    const settled = settleSessionProgress(session, endedAt, observed);
    const nextTrackDurationMs = Math.max(
      Number(session?.track_duration_ms || 0),
      Number(trackDurationMs || 0),
    );
    const nextMaxPositionMs = Math.max(settled.maxPositionMs, observed);
    const nextLastPositionMs = observed > 0
      ? observed
      : Math.max(0, Number(session?.last_position_ms || 0));

    return {
      ...session,
      track_title: trackTitle || session?.track_title || '',
      artist_name: artistName || session?.artist_name || '',
      album_name: albumName || session?.album_name || '',
      track_duration_ms: nextTrackDurationMs,
      playback_state: playbackState,
      accumulated_ms: settled.accumulatedMs,
      max_position_ms: nextMaxPositionMs,
      last_position_ms: nextLastPositionMs,
      playing_since: playbackState === 'playing' ? Number(endedAt || Date.now()) : null,
      last_event_at: Number(endedAt || Date.now()),
    };
  }

  function recordOrUpdateSessionPlay({
    session,
    endedAt,
    playbackPositionMs = 0,
    smartSettings,
    eventSource = 'plex_webhook',
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
    const existing = db.prepare(`
      SELECT id, duration_ms
      FROM play_events
      WHERE session_key = ?
        AND plex_rating_key = ?
        AND ended_at > ?
        AND started_at >= ?
      ORDER BY ended_at DESC, id DESC
      LIMIT 1
    `).get(
      session.session_key,
      session.plex_rating_key,
      recentCutoff,
      Number(session.started_at || endedAt) - 60 * 1000,
    );

    if (existing) {
      if (listenedMs <= Number(existing.duration_ms || 0)) {
        closeSession(db, session.session_key);
        return { duplicate: true, listenedMs, isSkip };
      }
      db.prepare('UPDATE play_events SET duration_ms = ?, ended_at = ?, is_skip = ?, track_duration_ms = COALESCE(NULLIF(?, 0), track_duration_ms) WHERE id = ?')
        .run(listenedMs, endedAt, isSkip ? 1 : 0, resolvedTrackDuration, existing.id);
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

    let effectiveIsSkip = isSkip;
    if (session.artist_name) {
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

    closeSession(db, session.session_key);
    const isCompletion = !effectiveIsSkip
      && resolvedTrackDuration > 0
      && listenedMs >= resolvedTrackDuration - completionThresholdMs;
    if (effectiveIsSkip || isCompletion) scheduleRebuild(ctx, session.user_plex_id);
    return { duplicate: false, listenedMs, isSkip: effectiveIsSkip };
  }

  // Expire stale open sessions on startup and periodically
  purgeInactiveSourceSessions();
  expireOldSessions(db);
  setInterval(() => {
    purgeInactiveSourceSessions();
    expireOldSessions(db);
  }, 30 * 60 * 1000).unref();

  // ── Tautulli ─────────────────────────────────────────────────────────────

  app.post('/webhook/tautulli', (req, res) => {
    try {
      if (!requireWebhookKey(req, res, 'tautulli')) return;
      if (resolveLivePlaybackSource() !== 'tautulli') {
        purgeInactiveSourceSessions('plex');
        pushLog({
          level: 'info',
          app: 'webhook',
          action: 'tautulli.ignored',
          message: 'Ignored Tautulli webhook because live playback source is set to Plex.',
        });
        return res.json({ ok: true, ignored: 'disabled_source' });
      }
      const body = req.body || {};

      // Only process music tracks
      const mediaType = String(body.media_type || '').toLowerCase();
      if (mediaType !== 'track') return res.json({ ok: true, ignored: true });

      const action = String(body.action || '').toLowerCase();
      const sessionKey = String(body.session_key || '').trim();
      // Prefer username (matches local account names) over numeric user_id
      const userPlexId = String(body.user || body.user_id || '').trim();
      const plexRatingKey = String(body.rating_key || '').trim();

      if (!userPlexId || !plexRatingKey) return res.json({ ok: true, ignored: true });

      const trackTitle = String(body.title || '').trim();
      const artistName = String(body.original_title || body.grandparent_title || '').trim();
      const albumName = String(body.parent_title || '').trim();
      const libraryKey = String(body.section_id || '').trim();

      // Tautulli's duration field is coarse; progress_percent + view_offset gives a
      // better real-time estimate for near-complete tracks, so prefer the smaller
      // plausible value when both are present.
      const trackDurationMs = inferTautulliTrackDurationMs(body);
      // view_offset is current position in ms
      const viewOffsetMs = Number(body.view_offset || 0);

      const config = loadConfig();
      const smartSettings = resolveUserSmartConfig(db, config, userPlexId);
      const now = Date.now();
      const effectiveSessionKey = sessionKey || `tautulli-${userPlexId}-${plexRatingKey}-${Math.floor(now / 60000)}`;
      const playerScope = resolveTautulliPlayerScope(body, userPlexId);

      if (action === 'play' && playerScope) {
        const previous = db.prepare(`
          SELECT *
          FROM open_sessions
          WHERE user_plex_id = ?
            AND event_source = 'tautulli'
            AND player_scope LIKE ?
            AND plex_rating_key != ?
          ORDER BY started_at DESC
          LIMIT 1
        `).get(userPlexId, `${playerScope}%`, plexRatingKey);

        if (previous) {
          const result = recordOrUpdateSessionPlay({
            session: previous,
            endedAt: now,
            smartSettings,
            eventSource: 'tautulli',
          });
          if (result.duplicate) {
            pushLog({
              level: 'info',
              app: 'webhook',
              action: 'tautulli.play-finalized-previous',
              message: `Skipped duplicate finalize for previous Tautulli session "${previous.track_title}" [session=${previous.session_key}]`,
            });
          }
        }
      }

      if (action === 'play' || action === 'resume') {
        const existingSession = getOpenSession(db, effectiveSessionKey);
        const resumedPositionMs = viewOffsetMs > 0
          ? viewOffsetMs
          : Math.max(0, Number(existingSession?.last_position_ms || 0));
        const nextSession = existingSession ? {
          ...existingSession,
          track_title: trackTitle || existingSession.track_title || '',
          artist_name: artistName || existingSession.artist_name || '',
          album_name: albumName || existingSession.album_name || '',
          library_key: libraryKey || existingSession.library_key || '',
          track_duration_ms: Math.max(
            Number(existingSession.track_duration_ms || 0),
            trackDurationMs,
          ),
          player_scope: playerScope || existingSession.player_scope || '',
          playback_state: 'playing',
          last_position_ms: resumedPositionMs,
          max_position_ms: Math.max(
            Math.max(0, Number(existingSession.max_position_ms || 0)),
            resumedPositionMs,
          ),
          playing_since: now,
          last_event_at: now,
        } : {
          session_key: effectiveSessionKey,
          user_plex_id: userPlexId,
          plex_rating_key: plexRatingKey,
          track_title: trackTitle,
          artist_name: artistName,
          album_name: albumName,
          library_key: libraryKey,
          track_duration_ms: trackDurationMs,
          player_scope: playerScope,
          playback_state: 'playing',
          last_position_ms: viewOffsetMs,
          max_position_ms: viewOffsetMs,
          accumulated_ms: 0,
          playing_since: now,
          last_event_at: now,
          started_at: viewOffsetMs > 0 ? now - viewOffsetMs : now,
          event_source: 'tautulli',
        };
        saveSessionState(nextSession, 'tautulli');
      } else if (action === 'pause') {
        const session = getOpenSession(db, effectiveSessionKey);
        if (session) {
          saveSessionState(continueSession(session, {
            endedAt: now,
            observedPositionMs: viewOffsetMs,
            playbackState: 'paused',
            trackDurationMs,
            trackTitle,
            artistName,
            albumName,
          }), 'tautulli');
        }
      } else if (action === 'scrobble' || action === 'watched') {
        const session = getOpenSession(db, effectiveSessionKey);
        if (session) {
          const nextSession = viewOffsetMs > 0
            ? continueSession(session, {
              endedAt: now,
              observedPositionMs: viewOffsetMs,
              playbackState: 'playing',
              trackDurationMs,
              trackTitle,
              artistName,
              albumName,
            })
            : {
              ...session,
              track_title: trackTitle || session.track_title || '',
              artist_name: artistName || session.artist_name || '',
              album_name: albumName || session.album_name || '',
              library_key: libraryKey || session.library_key || '',
              track_duration_ms: Math.max(Number(session.track_duration_ms || 0), trackDurationMs),
              last_event_at: now,
            };
          saveSessionState(nextSession, 'tautulli');
        } else if (action === 'watched') {
          const fallbackSession = {
            session_key: effectiveSessionKey,
            user_plex_id: userPlexId,
            plex_rating_key: plexRatingKey,
            track_title: trackTitle,
            artist_name: artistName,
            album_name: albumName,
            library_key: libraryKey,
            track_duration_ms: trackDurationMs,
            player_scope: playerScope,
            playback_state: 'playing',
            last_position_ms: viewOffsetMs,
            max_position_ms: viewOffsetMs,
            accumulated_ms: 0,
            playing_since: viewOffsetMs > 0 ? now - viewOffsetMs : now,
            last_event_at: now,
            started_at: viewOffsetMs > 0 ? now - viewOffsetMs : now,
            event_source: 'tautulli',
          };
          recordOrUpdateSessionPlay({
            session: fallbackSession,
            endedAt: now,
            playbackPositionMs: viewOffsetMs,
            smartSettings,
            eventSource: 'tautulli',
          });
        } else {
          pushLog({
            level: 'info',
            app: 'webhook',
            action: 'tautulli.scrobble-without-session',
            message: `Observed Tautulli scrobble without an open session for "${trackTitle}" [user=${userPlexId}]`,
          });
        }
      } else if (action === 'stop') {
        const session = getOpenSession(db, effectiveSessionKey);
        const fallbackSession = session || {
          session_key: effectiveSessionKey,
          user_plex_id: userPlexId,
          plex_rating_key: plexRatingKey,
          track_title: trackTitle,
          artist_name: artistName,
          album_name: albumName,
          library_key: libraryKey,
          track_duration_ms: trackDurationMs,
          player_scope: playerScope,
          playback_state: 'playing',
          last_position_ms: viewOffsetMs,
          max_position_ms: viewOffsetMs,
          accumulated_ms: 0,
          playing_since: viewOffsetMs > 0 ? now - viewOffsetMs : now,
          last_event_at: now,
          started_at: viewOffsetMs > 0 ? now - viewOffsetMs : now,
          event_source: 'tautulli',
        };
        const result = recordOrUpdateSessionPlay({
          session: {
            ...fallbackSession,
            track_title: trackTitle || fallbackSession.track_title || '',
            artist_name: artistName || fallbackSession.artist_name || '',
            album_name: albumName || fallbackSession.album_name || '',
            library_key: libraryKey || fallbackSession.library_key || '',
            track_duration_ms: Math.max(Number(fallbackSession.track_duration_ms || 0), trackDurationMs),
          },
          endedAt: now,
          playbackPositionMs: viewOffsetMs,
          smartSettings,
          eventSource: 'tautulli',
        });
        if (result.duplicate) return res.json({ ok: true, ignored: 'duplicate' });
      }

      pushLog({
        level: 'info', app: 'webhook',
        action: `tautulli.${action}`,
        message: `${action} — "${trackTitle}" by ${artistName} [user=${userPlexId}]`,
        meta: { plexRatingKey, sessionKey: effectiveSessionKey },
      });

      return res.json({ ok: true });
    } catch (err) {
      pushLog({ level: 'error', app: 'webhook', action: 'tautulli.error', message: safeMessage(err) });
      return res.status(500).json({ error: 'Webhook processing failed.' });
    }
  });

  // ── Plex webhook ──────────────────────────────────────────────────────────
  //
  // Configure in Plex Server Settings → Webhooks (requires Plex Pass):
  //   URL: http://curatorr:7676/webhook/plex
  //
  // Plex sends multipart/form-data with a "payload" field containing JSON.

  app.post('/webhook/plex', readRawBody, async (req, res) => {
    try {
      if (!requireWebhookKey(req, res, 'plex')) return;
      if (resolveLivePlaybackSource() !== 'plex') {
        purgeInactiveSourceSessions('tautulli');
        pushLog({
          level: 'info',
          app: 'webhook',
          action: 'plex.ignored',
          message: 'Ignored Plex webhook because live playback source is set to Tautulli.',
        });
        return res.json({ ok: true, ignored: 'disabled_source' });
      }
      let payload = null;

      // Plex sends multipart/form-data. We read the raw body and parse it.
      // Also check req.body.payload for urlencoded fallback.
      const rawPayload = req.body?.payload;
      if (rawPayload) {
        try { payload = typeof rawPayload === 'string' ? JSON.parse(rawPayload) : rawPayload; } catch (e) { /* ignore */ }
      }
      if (!payload && req.rawBody) {
        const ct = req.headers['content-type'] || '';
        const payloadText = extractMultipartField(req.rawBody, ct, 'payload');
        if (payloadText) {
          try { payload = JSON.parse(payloadText); } catch (e) { /* ignore */ }
        }
      }

      if (!payload) {
        pushLog({
          level: 'info',
          app: 'webhook',
          action: 'plex.ignored',
          message: 'Ignored Plex webhook without a parseable payload.',
          meta: {
            contentType: String(req.headers?.['content-type'] || ''),
            bodyKeys: Object.keys(req.body || {}),
            rawBytes: Number(req.rawBody?.length || 0),
          },
        });
        return res.json({ ok: true, ignored: 'missing_payload' });
      }

      const event = String(payload.event || '').toLowerCase();
      const metadata = payload.Metadata || {};
      const account = payload.Account || {};

      // Only process music tracks
      if (metadata.type !== 'track') {
        pushLog({
          level: 'info',
          app: 'webhook',
          action: 'plex.ignored',
          message: 'Ignored Plex webhook for a non-track item.',
          meta: { event, mediaType: metadata.type || '' },
        });
        return res.json({ ok: true, ignored: 'non_track' });
      }
      if (!['media.play', 'media.pause', 'media.resume', 'media.stop', 'media.scrobble'].includes(event)) {
        pushLog({
          level: 'info',
          app: 'webhook',
          action: 'plex.ignored',
          message: 'Ignored unsupported Plex webhook event.',
          meta: { event },
        });
        return res.json({ ok: true, ignored: 'unsupported_event' });
      }

      const userPlexId = String(account.title || account.id || '').trim();
      const plexRatingKey = String(metadata.ratingKey || '').trim();
      if (!userPlexId || !plexRatingKey) {
        pushLog({
          level: 'info',
          app: 'webhook',
          action: 'plex.ignored',
          message: 'Ignored Plex webhook without a user or rating key.',
          meta: {
            event,
            userPlexId,
            plexRatingKey,
            accountId: String(account.id || ''),
            accountTitle: String(account.title || ''),
          },
        });
        return res.json({ ok: true, ignored: 'missing_identity' });
      }

      const trackTitle = String(metadata.title || '').trim();
      const artistName = String(metadata.originalTitle || metadata.grandparentTitle || '').trim();
      const albumName = String(metadata.parentTitle || '').trim();
      const trackDurationMs = Number(metadata.duration || 0);
      const viewOffsetMs = Number(metadata.viewOffset || 0);
      const observedPositionMs = Math.max(0, viewOffsetMs);

      const config = loadConfig();
      const smartSettings = resolveUserSmartConfig(db, config, userPlexId);

      const now = Date.now();
      const sessionKey = resolvePlexSessionKey(payload, userPlexId, plexRatingKey);
      const playerScope = resolvePlexPlayerScope(payload, userPlexId);

      if (event === 'media.play' && playerScope) {
        const previous = db.prepare(`
          SELECT *
          FROM open_sessions
          WHERE user_plex_id = ?
            AND event_source = 'plex_webhook'
            AND session_key LIKE ?
            AND plex_rating_key != ?
          ORDER BY started_at DESC
          LIMIT 1
        `).get(userPlexId, `${playerScope}%`, plexRatingKey);

        if (previous) {
          const previousTrackDurationMs = await resolvePlexTrackDurationMs(
            previous.plex_rating_key,
            previous.track_duration_ms || 0,
          );
          recordOrUpdateSessionPlay({
            session: {
              ...previous,
              track_duration_ms: previousTrackDurationMs,
            },
            endedAt: now,
            smartSettings,
            eventSource: 'plex_webhook',
          });
        }
      }

      if (event === 'media.play' || event === 'media.resume') {
        const existingSession = getOpenSession(db, sessionKey);
        const hydratedTrackDurationMs = await resolvePlexTrackDurationMs(
          plexRatingKey,
          trackDurationMs || existingSession?.track_duration_ms || 0,
        );
        const resumedPositionMs = observedPositionMs > 0
          ? observedPositionMs
          : Math.max(0, Number(existingSession?.last_position_ms || 0));

        const nextSession = existingSession ? {
          ...existingSession,
          track_title: trackTitle || existingSession.track_title || '',
          artist_name: artistName || existingSession.artist_name || '',
          album_name: albumName || existingSession.album_name || '',
          track_duration_ms: hydratedTrackDurationMs,
          player_scope: playerScope || existingSession.player_scope || '',
          playback_state: 'playing',
          last_position_ms: resumedPositionMs,
          max_position_ms: Math.max(
            Math.max(0, Number(existingSession.max_position_ms || 0)),
            resumedPositionMs,
          ),
          playing_since: now,
          last_event_at: now,
        } : {
          session_key: sessionKey,
          user_plex_id: userPlexId,
          plex_rating_key: plexRatingKey,
          track_title: trackTitle,
          artist_name: artistName,
          album_name: albumName,
          library_key: '',
          track_duration_ms: hydratedTrackDurationMs,
          player_scope: playerScope,
          playback_state: 'playing',
          last_position_ms: observedPositionMs,
          max_position_ms: observedPositionMs,
          accumulated_ms: 0,
          playing_since: now,
          last_event_at: now,
          started_at: observedPositionMs > 0 ? now - observedPositionMs : now,
          event_source: 'plex_webhook',
        };
        saveSessionState(nextSession, 'plex_webhook');
      } else if (event === 'media.pause') {
        const session = getOpenSession(db, sessionKey);
        if (session) {
          const hydratedTrackDurationMs = await resolvePlexTrackDurationMs(
            plexRatingKey,
            trackDurationMs || session.track_duration_ms || 0,
          );
          saveSessionState(continueSession(session, {
            endedAt: now,
            observedPositionMs,
            playbackState: 'paused',
            trackDurationMs: hydratedTrackDurationMs,
            trackTitle,
            artistName,
            albumName,
          }), 'plex_webhook');
        }
      } else if (event === 'media.scrobble') {
        const session = getOpenSession(db, sessionKey);
        if (session) {
          const hydratedTrackDurationMs = await resolvePlexTrackDurationMs(
            plexRatingKey,
            trackDurationMs || session.track_duration_ms || 0,
          );
          const nextSession = observedPositionMs > 0
            ? continueSession(session, {
              endedAt: now,
              observedPositionMs,
              playbackState: 'playing',
              trackDurationMs: hydratedTrackDurationMs,
              trackTitle,
              artistName,
              albumName,
            })
            : {
              ...session,
              track_title: trackTitle || session.track_title || '',
              artist_name: artistName || session.artist_name || '',
              album_name: albumName || session.album_name || '',
              track_duration_ms: hydratedTrackDurationMs,
              last_event_at: now,
            };
          saveSessionState(nextSession, 'plex_webhook');
        } else {
          pushLog({
            level: 'info',
            app: 'webhook',
            action: 'plex.scrobble-without-session',
            message: `Observed Plex scrobble without an open session for "${trackTitle}" [user=${userPlexId}]`,
          });
        }
      } else if (event === 'media.stop') {
        const session = getOpenSession(db, sessionKey);
        const fallbackSession = session || {
          session_key: sessionKey,
          user_plex_id: userPlexId,
          plex_rating_key: plexRatingKey,
          track_title: trackTitle,
          artist_name: artistName,
          album_name: albumName,
          library_key: '',
          track_duration_ms: 0,
          player_scope: playerScope,
          playback_state: 'playing',
          last_position_ms: observedPositionMs,
          max_position_ms: observedPositionMs,
          accumulated_ms: 0,
          playing_since: observedPositionMs > 0 ? now - observedPositionMs : now,
          last_event_at: now,
          started_at: observedPositionMs > 0 ? now - observedPositionMs : now,
          event_source: 'plex_webhook',
        };
        const resolvedTrackDuration = await resolvePlexTrackDurationMs(
          plexRatingKey,
          trackDurationMs || fallbackSession.track_duration_ms || 0,
        );
        const result = recordOrUpdateSessionPlay({
          session: {
            ...fallbackSession,
            track_title: trackTitle || fallbackSession.track_title || '',
            artist_name: artistName || fallbackSession.artist_name || '',
            album_name: albumName || fallbackSession.album_name || '',
            track_duration_ms: resolvedTrackDuration,
          },
          endedAt: now,
          playbackPositionMs: observedPositionMs,
          smartSettings,
          eventSource: 'plex_webhook',
        });
        if (result.duplicate) return res.json({ ok: true, ignored: 'duplicate' });
      }

      pushLog({
        level: 'info', app: 'webhook',
        action: `plex.${event}`,
        message: `${event} — "${trackTitle}" by ${artistName} [user=${userPlexId}]`,
      });

      return res.json({ ok: true });
    } catch (err) {
      pushLog({ level: 'error', app: 'webhook', action: 'plex.error', message: safeMessage(err) });
      return res.status(500).json({ error: 'Webhook processing failed.' });
    }
  });

  // ── Status endpoint ───────────────────────────────────────────────────────

  app.get('/api/webhooks/status', (req, res) => {
    if (!req.session?.user) return res.status(401).json({ error: 'Authentication required.' });
    const openCount = db.prepare('SELECT COUNT(*) AS n FROM open_sessions').get().n;
    const totalEvents = db.prepare('SELECT COUNT(*) AS n FROM play_events').get().n;
    return res.json({
      ok: true,
      openSessions: openCount,
      totalEvents,
      pendingRebuilds: rebuildTimers.size,
    });
  });
}
