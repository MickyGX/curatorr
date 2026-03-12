// Webhook receiver for Tautulli and Plex notifications.
// Note: Plex webhooks use multipart/form-data which express.json() doesn't parse.
// readRawBody captures the raw body for multipart routes before normal parsing.

import crypto from 'crypto';

// Middleware: capture raw body buffer (only when body not already parsed)
function readRawBody(req, res, next) {
  if (req.body !== undefined) return next(); // already parsed by express.json/urlencoded
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
// Both sources emit play/stop/pause/resume events; we deduplicate by session_key.
// Skip detection: if a track stops before skipThresholdSeconds, it's a skip.

import {
  openSession,
  getOpenSession,
  closeSession,
  recordPlayEvent,
  updateTrackStats,
  updateArtistStats,
  updateTrackTierOnly,
  adjustArtistScore,
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
  const { db, loadConfig, pushLog, safeMessage, getWebhookSharedSecret } = ctx;

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

  // Expire stale open sessions on startup and periodically
  expireOldSessions(db);
  setInterval(() => expireOldSessions(db), 30 * 60 * 1000).unref();

  // ── Tautulli ─────────────────────────────────────────────────────────────

  app.post('/webhook/tautulli', (req, res) => {
    try {
      if (!requireWebhookKey(req, res, 'tautulli')) return;
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

      // Tautulli {duration} returns duration in minutes (integer); convert to ms
      const trackDurationMs = Number(body.duration || 0) * 60 * 1000;
      // view_offset is current position in ms
      const viewOffsetMs = Number(body.view_offset || 0);

      const config = loadConfig();
      const smartSettings = resolveUserSmartConfig(db, config, userPlexId);
      const skipThresholdMs = (Number(smartSettings.skipThresholdSeconds) || 20) * 1000;
      const songSkipLimit = Number(smartSettings.songSkipLimit) || 3;

      const now = Date.now();
      const effectiveSessionKey = sessionKey || `tautulli-${userPlexId}-${plexRatingKey}-${Math.floor(now / 60000)}`;

      if (action === 'play' || action === 'resume') {
        // On resume, adjust startedAt backwards by the current viewOffset so that the
        // "now - startedAt" fallback at stop time reflects accumulated playtime rather
        // than just time-since-resume. This matches the Plex webhook's approach and
        // handles clients that send view_offset=0 on the stop event.
        openSession(db, {
          sessionKey: effectiveSessionKey,
          userPlexId, plexRatingKey,
          trackTitle, artistName, albumName, libraryKey,
          trackDurationMs,
          startedAt: action === 'resume' ? now - viewOffsetMs : now,
          eventSource: 'tautulli',
        });
      } else if (action === 'stop' || action === 'watched' || action === 'scrobble') {
        // Deduplicate: stop + watched both fire for the same track in the same session — only record once per (session, track)
        const alreadyRecorded = db.prepare('SELECT id FROM play_events WHERE session_key = ? AND plex_rating_key = ? LIMIT 1').get(effectiveSessionKey, plexRatingKey);
        if (alreadyRecorded) {
          pushLog({ level: 'info', app: 'webhook', action: `tautulli.${action}`, message: `duplicate skipped — "${trackTitle}" [session=${effectiveSessionKey}]` });
          return res.json({ ok: true, ignored: 'duplicate' });
        }

        const session = getOpenSession(db, effectiveSessionKey);
        const startedAt = session ? session.started_at : now - viewOffsetMs;
        const listenedMs = viewOffsetMs || (now - startedAt);
        const resolvedTrackDuration = trackDurationMs || (session?.track_duration_ms) || 0;

        // A track is a skip if: we know the full duration AND the user listened for < threshold
        const isSkip = Boolean(
          resolvedTrackDuration > 0
          && listenedMs < skipThresholdMs
          && action !== 'watched'
          && action !== 'scrobble',
        );

        recordPlayEvent(db, {
          userPlexId, plexRatingKey,
          trackTitle: trackTitle || session?.track_title || '',
          artistName: artistName || session?.artist_name || '',
          albumName: albumName || session?.album_name || '',
          libraryKey: libraryKey || session?.library_key || '',
          startedAt, endedAt: now,
          durationMs: listenedMs,
          trackDurationMs: resolvedTrackDuration,
          isSkip,
          eventSource: 'tautulli',
          sessionKey: effectiveSessionKey,
        });

        const resolvedArtist = artistName || session?.artist_name || '';
        let effectiveIsSkip = isSkip;
        if (resolvedArtist) {
          const trackResult = updateTrackStats(db, {
            userPlexId, plexRatingKey,
            trackTitle: trackTitle || session?.track_title || '',
            artistName: resolvedArtist,
            albumName: albumName || session?.album_name || '',
            listenedMs, trackDurationMs: resolvedTrackDuration,
            playedAt: now, songSkipLimit, smartConfig: smartSettings,
          });
          effectiveIsSkip = trackResult.isSkip;
          updateArtistStats(db, {
            userPlexId, artistName: resolvedArtist,
            isSkip: effectiveIsSkip, playedAt: now,
            scoreDelta: trackResult.scoreDelta,
          });
        }

        closeSession(db, effectiveSessionKey);

        // Trigger rebuild on skip OR on completion (listened to within threshold of end)
        const completionThresholdMs = (Number(smartSettings.completionThresholdSeconds) || 20) * 1000;
        const isCompletion = !effectiveIsSkip && resolvedTrackDuration > 0
          && listenedMs >= resolvedTrackDuration - completionThresholdMs;
        if (effectiveIsSkip || isCompletion) scheduleRebuild(ctx, userPlexId);
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

  app.post('/webhook/plex', readRawBody, (req, res) => {
    try {
      if (!requireWebhookKey(req, res, 'plex')) return;
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

      if (!payload) return res.json({ ok: true, ignored: true });

      const event = String(payload.event || '').toLowerCase();
      const metadata = payload.Metadata || {};
      const account = payload.Account || {};

      // Only process music tracks
      if (metadata.type !== 'track') return res.json({ ok: true, ignored: true });
      if (!['media.play', 'media.pause', 'media.resume', 'media.stop', 'media.scrobble'].includes(event)) {
        return res.json({ ok: true, ignored: true });
      }

      const userPlexId = String(account.title || account.id || '').trim();
      const plexRatingKey = String(metadata.ratingKey || '').trim();
      if (!userPlexId || !plexRatingKey) return res.json({ ok: true, ignored: true });

      const trackTitle = String(metadata.title || '').trim();
      const artistName = String(metadata.originalTitle || metadata.grandparentTitle || '').trim();
      const albumName = String(metadata.parentTitle || '').trim();
      const trackDurationMs = Number(metadata.duration || 0);
      const viewOffsetMs = Number(metadata.viewOffset || 0);

      const config = loadConfig();
      const smartSettings = resolveUserSmartConfig(db, config, userPlexId);
      const skipThresholdMs = (Number(smartSettings.skipThresholdSeconds) || 20) * 1000;
      const songSkipLimit = Number(smartSettings.songSkipLimit) || 3;

      const now = Date.now();
      const sessionKey = `plex-${userPlexId}-${plexRatingKey}`;

      if (event === 'media.play' || event === 'media.resume') {
        openSession(db, {
          sessionKey, userPlexId, plexRatingKey,
          trackTitle, artistName, albumName, libraryKey: '',
          trackDurationMs, startedAt: now - viewOffsetMs,
          eventSource: 'plex_webhook',
        });
      } else if (event === 'media.stop' || event === 'media.scrobble') {
        const session = getOpenSession(db, sessionKey);
        const startedAt = session ? session.started_at : now - viewOffsetMs;
        const listenedMs = viewOffsetMs || (now - startedAt);
        const resolvedTrackDuration = trackDurationMs || session?.track_duration_ms || 0;
        const resolvedArtist = artistName || session?.artist_name || '';

        const isSkip = Boolean(
          resolvedTrackDuration > 0
          && listenedMs < skipThresholdMs
          && event !== 'media.scrobble',
        );

        // Deduplicate within a short window to catch stop+scrobble double-fires.
        // However, if a network blip caused an earlier stop to be recorded with a lower
        // listenedMs, accept the new event and update the record rather than ignoring it.
        const recentCutoff = Date.now() - 10 * 60 * 1000;
        const existing = db.prepare('SELECT id, duration_ms FROM play_events WHERE session_key = ? AND ended_at > ? LIMIT 1').get(sessionKey, recentCutoff);

        if (existing) {
          if (listenedMs <= existing.duration_ms) {
            // Same or shorter — genuine stop+scrobble double-fire; ignore.
            return res.json({ ok: true, ignored: 'duplicate' });
          }
          // Longer — a resumed play went further (e.g. after a network blip).
          // Update the existing record with the better measurement.
          db.prepare('UPDATE play_events SET duration_ms = ?, ended_at = ?, is_skip = ? WHERE id = ?')
            .run(listenedMs, now, isSkip ? 1 : 0, existing.id);
          if (resolvedArtist) {
            const { scoreDelta } = updateTrackTierOnly(db, {
              userPlexId, plexRatingKey, listenedMs,
              trackDurationMs: resolvedTrackDuration, smartConfig: smartSettings,
            });
            if (scoreDelta) adjustArtistScore(db, { userPlexId, artistName: resolvedArtist, scoreDelta });
          }
          closeSession(db, sessionKey);
          pushLog({
            level: 'info', app: 'webhook', action: `plex.${event}`,
            message: `${event} updated (longer) — "${trackTitle}" by ${resolvedArtist} [user=${userPlexId}] ${existing.duration_ms}ms→${listenedMs}ms`,
          });
          return res.json({ ok: true });
        }

        recordPlayEvent(db, {
          userPlexId, plexRatingKey,
          trackTitle: trackTitle || session?.track_title || '',
          artistName: resolvedArtist,
          albumName: albumName || session?.album_name || '',
          libraryKey: '',
          startedAt, endedAt: now,
          durationMs: listenedMs,
          trackDurationMs: resolvedTrackDuration,
          isSkip,
          eventSource: 'plex_webhook',
          sessionKey,
        });

        let effectiveIsSkip = isSkip;
        if (resolvedArtist) {
          const trackResult = updateTrackStats(db, {
            userPlexId, plexRatingKey,
            trackTitle: trackTitle || session?.track_title || '',
            artistName: resolvedArtist,
            albumName: albumName || session?.album_name || '',
            listenedMs, trackDurationMs: resolvedTrackDuration,
            playedAt: now, songSkipLimit, smartConfig: smartSettings,
          });
          effectiveIsSkip = trackResult.isSkip;
          updateArtistStats(db, {
            userPlexId, artistName: resolvedArtist,
            isSkip: effectiveIsSkip, playedAt: now,
            scoreDelta: trackResult.scoreDelta,
          });
        }

        closeSession(db, sessionKey);
        if (effectiveIsSkip) scheduleRebuild(ctx, userPlexId);
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
