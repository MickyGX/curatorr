// Plex History Sync: scheduled job that reads Plex's own play history and adds
// plays Curatorr never saw. The main case is offline listening: Plexamp plays
// downloaded tracks without a server connection, so no webhook fires and
// Tautulli sees no stream, but Plexamp reports those plays to the server once
// it reconnects and they show up in /status/sessions/history/all.
//
// What a history row gives us: user (accountID), track (ratingKey), library and
// viewedAt, the moment Plex marked the track played. On this server viewedAt
// lands about two minutes before the end of the track (the 90% scrobble point).
// There is no listen duration, so every imported row counts as a full listen,
// the same way Last.fm scrobbles are treated.
//
// Deduplication (online plays appear in history too, already recorded by
// webhooks or Tautulli):
//   1. A row whose historyKey was imported before is skipped (reruns are safe).
//   2. Plex sometimes logs one play twice a few seconds apart; rows for the same
//      user and track within DOUBLE_SCROBBLE_MS of each other collapse to one.
//   3. A row matches an existing play for the same user and track that started
//      between (viewedAt - track length - MATCH_SLACK_MS) and
//      (viewedAt + MATCH_SLACK_MS). Each existing play can match only one row,
//      so a track played twice in a row is not collapsed.
//   4. Rows newer than SETTLE_MS, or with an open webhook session for the same
//      track, are left for the next run so a live play is not recorded twice.

import {
  recordPlayEvent,
  updateTrackStats,
  updateArtistStats,
  resolveUserSmartConfig,
} from '../db.js';
import { isPlexLibrarySelected } from './library-selection.js';

export const PLEX_HISTORY_EVENT_SOURCE = 'plex_history';

const DEFAULT_LOOKBACK_DAYS = 7;
const PAGE_SIZE = 200;
const MAX_PAGES = 200;
const REQUEST_TIMEOUT_MS = 20_000;
const MATCH_SLACK_MS = 15 * 60_000;
const DOUBLE_SCROBBLE_MS = 45_000;
const SETTLE_MS = 20 * 60_000;
// Where viewedAt falls within the track, used to estimate when the play started.
const SCROBBLE_POINT = 0.9;

function plexHeaders(token) {
  return { Accept: 'application/json', 'X-Plex-Token': token };
}

function buildPlexUrl(baseUrl, path, params = {}) {
  const u = new URL(String(baseUrl).replace(/\/+$/, '') + path);
  for (const [key, value] of Object.entries(params)) u.searchParams.set(key, String(value));
  return u;
}

async function fetchJson(url, token) {
  const href = String(url);
  const res = await fetch(href, {
    headers: plexHeaders(token),
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`Plex returned HTTP ${res.status} for ${new URL(href).pathname}`);
  return res.json();
}

// accountID → the name webhooks store as user_plex_id (Account.title).
async function fetchAccountNames(plexUrl, token) {
  const json = await fetchJson(buildPlexUrl(plexUrl, '/accounts'), token);
  const accounts = json?.MediaContainer?.Account || [];
  const names = new Map();
  for (const account of accounts) {
    const id = String(account?.id ?? '').trim();
    const name = String(account?.name || '').trim();
    if (id && name) names.set(id, name);
  }
  return names;
}

async function fetchHistoryRows(plexUrl, token, sinceSec) {
  const rows = [];
  for (let page = 0; page < MAX_PAGES; page += 1) {
    const url = buildPlexUrl(plexUrl, '/status/sessions/history/all', {
      sort: 'viewedAt:desc',
      'X-Plex-Container-Start': page * PAGE_SIZE,
      'X-Plex-Container-Size': PAGE_SIZE,
    });
    // Plex filter syntax: the ">=" must reach the server unencoded.
    const json = await fetchJson(`${url}&viewedAt>=${sinceSec}`, token);
    const batch = json?.MediaContainer?.Metadata || [];
    rows.push(...batch);
    if (batch.length < PAGE_SIZE) break;
    // Stop early if the server ignored the viewedAt filter.
    const oldest = Math.min(...batch.map((row) => Number(row?.viewedAt || 0)));
    if (oldest && oldest <= sinceSec) break;
  }
  return rows;
}

function historyId(row) {
  const key = String(row?.historyKey || '').trim();
  const match = key.match(/(\d+)\s*$/);
  return match ? match[1] : key;
}

// Drops the second of two rows for the same user and track logged within a few
// seconds of each other. Rows must be sorted by viewedAt ascending.
export function collapseDoubleScrobbles(rows) {
  const lastKept = new Map();
  const kept = [];
  let collapsed = 0;
  for (const row of rows) {
    const key = `${row.userPlexId}\u0000${row.plexRatingKey}`;
    const previous = lastKept.get(key);
    if (previous != null && row.viewedAtMs - previous < DOUBLE_SCROBBLE_MS) {
      collapsed += 1;
      continue;
    }
    lastKept.set(key, row.viewedAtMs);
    kept.push(row);
  }
  return { kept, collapsed };
}

function lookupTrackDurationMs(db, plexRatingKey) {
  const master = db.prepare('SELECT duration_ms FROM master_tracks WHERE rating_key = ?').get(plexRatingKey);
  if (Number(master?.duration_ms) > 0) return Number(master.duration_ms);
  const prior = db.prepare('SELECT MAX(track_duration_ms) AS dur FROM play_events WHERE plex_rating_key = ?').get(plexRatingKey);
  return Number(prior?.dur) > 0 ? Number(prior.dur) : 0;
}

async function fetchTrackDurationMs(plexUrl, token, plexRatingKey) {
  try {
    const json = await fetchJson(buildPlexUrl(plexUrl, `/library/metadata/${encodeURIComponent(plexRatingKey)}`), token);
    return Number(json?.MediaContainer?.Metadata?.[0]?.duration || 0);
  } catch {
    return 0;
  }
}

function findMatchingPlay(db, { userPlexId, plexRatingKey, viewedAtMs, trackDurationMs, usedIds }) {
  const lengthMs = trackDurationMs > 0 ? trackDurationMs : MATCH_SLACK_MS;
  const expectedStartMs = viewedAtMs - Math.round(lengthMs * SCROBBLE_POINT);
  const candidates = db.prepare(`
    SELECT id FROM play_events
    WHERE user_plex_id = ? AND plex_rating_key = ?
      AND started_at BETWEEN ? AND ?
    ORDER BY ABS(started_at - ?) ASC, id ASC
  `).all(
    userPlexId,
    plexRatingKey,
    viewedAtMs - lengthMs - MATCH_SLACK_MS,
    viewedAtMs + MATCH_SLACK_MS,
    expectedStartMs,
  );
  return candidates.find((candidate) => !usedIds.has(candidate.id)) || null;
}

function hasOpenSession(db, userPlexId, plexRatingKey) {
  return Boolean(db.prepare(
    'SELECT 1 FROM open_sessions WHERE user_plex_id = ? AND plex_rating_key = ? LIMIT 1',
  ).get(userPlexId, plexRatingKey));
}

function isAlreadyImported(db, sessionKey) {
  return Boolean(db.prepare('SELECT 1 FROM play_events WHERE session_key = ? LIMIT 1').get(sessionKey));
}

export async function runPlexHistorySync(ctx, { lookbackDays = DEFAULT_LOOKBACK_DAYS, backfillDays = 0, now = Date.now() } = {}) {
  const { db, loadConfig, pushLog, safeMessage } = ctx;
  const config = loadConfig();
  const serverType = String(config?.mediaServer?.type || 'plex').trim().toLowerCase();
  const plexUrl = String(config?.plex?.url || '').trim();
  const token = String(config?.plex?.token || '').trim();

  if (serverType !== 'plex' || !plexUrl || !token) {
    const message = 'Plex is not configured, skipping Plex history sync';
    pushLog({ level: 'info', app: 'plex-history', action: 'sync.skip', message });
    return { inserted: 0, skipped: 0, message };
  }

  const days = backfillDays > 0 ? backfillDays : lookbackDays;
  const sinceSec = Math.floor((now - days * 86_400_000) / 1000);
  const settleBeforeMs = now - SETTLE_MS;

  const counts = { inserted: 0, recorded: 0, collapsed: 0, pending: 0, libraryFiltered: 0, unknownUser: 0 };

  try {
    const accountNames = await fetchAccountNames(plexUrl, token);
    const rawRows = await fetchHistoryRows(plexUrl, token, sinceSec);

    const rows = [];
    for (const row of rawRows) {
      if (row?.type !== 'track') continue;
      const viewedAtMs = Number(row.viewedAt || 0) * 1000;
      const plexRatingKey = String(row.ratingKey || '').trim();
      if (!viewedAtMs || !plexRatingKey || viewedAtMs < sinceSec * 1000) continue;
      const libraryKey = String(row.librarySectionID || '').trim();
      if (!isPlexLibrarySelected(config, { libraryKey })) {
        counts.libraryFiltered += 1;
        continue;
      }
      const userPlexId = accountNames.get(String(row.accountID ?? '').trim()) || '';
      if (!userPlexId) {
        counts.unknownUser += 1;
        continue;
      }
      rows.push({
        id: historyId(row),
        userPlexId,
        plexRatingKey,
        libraryKey,
        viewedAtMs,
        trackTitle: String(row.title || '').trim(),
        artistName: String(row.originalTitle || row.grandparentTitle || '').trim(),
        albumName: String(row.parentTitle || '').trim(),
      });
    }
    rows.sort((a, b) => a.viewedAtMs - b.viewedAtMs);

    const { kept, collapsed } = collapseDoubleScrobbles(rows);
    counts.collapsed = collapsed;

    const usedIds = new Set();
    const durationCache = new Map();

    for (const row of kept) {
      const sessionKey = `plex-history-${row.id || `${row.userPlexId}-${row.plexRatingKey}-${row.viewedAtMs}`}`;
      if (isAlreadyImported(db, sessionKey)) {
        counts.recorded += 1;
        continue;
      }

      let trackDurationMs = durationCache.get(row.plexRatingKey);
      if (trackDurationMs == null) {
        trackDurationMs = lookupTrackDurationMs(db, row.plexRatingKey)
          || await fetchTrackDurationMs(plexUrl, token, row.plexRatingKey);
        durationCache.set(row.plexRatingKey, trackDurationMs);
      }

      const match = findMatchingPlay(db, { ...row, trackDurationMs, usedIds });
      if (match) {
        usedIds.add(match.id);
        counts.recorded += 1;
        continue;
      }

      if (row.viewedAtMs > settleBeforeMs || hasOpenSession(db, row.userPlexId, row.plexRatingKey)) {
        counts.pending += 1;
        continue;
      }

      const startedAt = trackDurationMs > 0
        ? row.viewedAtMs - Math.round(trackDurationMs * SCROBBLE_POINT)
        : row.viewedAtMs;
      const endedAt = trackDurationMs > 0 ? startedAt + trackDurationMs : row.viewedAtMs;

      const eventId = recordPlayEvent(db, {
        userPlexId: row.userPlexId,
        plexRatingKey: row.plexRatingKey,
        trackTitle: row.trackTitle,
        artistName: row.artistName,
        albumName: row.albumName,
        libraryKey: row.libraryKey,
        startedAt,
        endedAt,
        durationMs: trackDurationMs,
        trackDurationMs,
        isSkip: false,
        eventSource: PLEX_HISTORY_EVENT_SOURCE,
        sessionKey,
      });
      usedIds.add(Number(eventId));

      if (row.artistName && trackDurationMs > 0) {
        const smartSettings = resolveUserSmartConfig(db, config, row.userPlexId);
        const trackResult = updateTrackStats(db, {
          userPlexId: row.userPlexId,
          plexRatingKey: row.plexRatingKey,
          trackTitle: row.trackTitle,
          artistName: row.artistName,
          albumName: row.albumName,
          listenedMs: trackDurationMs,
          trackDurationMs,
          playedAt: startedAt,
          songSkipLimit: Number(smartSettings.songSkipLimit) || 3,
          smartConfig: smartSettings,
        });
        updateArtistStats(db, {
          userPlexId: row.userPlexId,
          artistName: row.artistName,
          isSkip: trackResult.isSkip,
          playedAt: startedAt,
          scoreDelta: trackResult.scoreDelta,
        });
      }

      counts.inserted += 1;
      pushLog({
        level: 'info', app: 'plex-history', action: 'sync.insert',
        message: `Added play from Plex history: "${row.trackTitle}" by ${row.artistName} [user=${row.userPlexId}]`,
      });
    }
  } catch (err) {
    pushLog({ level: 'error', app: 'plex-history', action: 'sync.error', message: safeMessage(err) });
    throw err;
  }

  const parts = [`${counts.inserted} new play${counts.inserted === 1 ? '' : 's'} added`, `${counts.recorded} already recorded`];
  if (counts.pending) parts.push(`${counts.pending} left for the next run`);
  if (counts.collapsed) parts.push(`${counts.collapsed} duplicate Plex entr${counts.collapsed === 1 ? 'y' : 'ies'} ignored`);
  if (counts.libraryFiltered) parts.push(`${counts.libraryFiltered} outside selected libraries`);
  if (counts.unknownUser) parts.push(`${counts.unknownUser} from unknown accounts`);
  const message = `Plex history sync (last ${days} day${days === 1 ? '' : 's'}): ${parts.join(', ')}`;
  pushLog({ level: 'info', app: 'plex-history', action: 'sync.complete', message });
  return { ...counts, skipped: counts.recorded, message };
}
