// Music Assistant integration: an additive play source. Curatorr connects to MA
// as a WebSocket client, turns media_item_played reports into finished plays,
// maps them onto library tracks and Curatorr users, and records them through the
// shared play recorder (same stats/skip/rebuild path as the webhooks).

import { resolveUserSmartConfig } from '../../db.js';
import { createPlayRecorder } from '../play-recorder.js';
import {
  connectMusicAssistant,
  decodeTokenExpiry,
  MusicAssistantError,
  normalizeMusicAssistantUrl,
} from './client.js';
import { createMaIdentity } from './identity.js';
import { createPlayTracker } from './play-tracker.js';

export const MA_EVENT_SOURCE = 'music_assistant';
const RECONNECT_MIN_MS = 5_000;
const RECONNECT_MAX_MS = 5 * 60_000;
// A full index pages the whole MA library, which is slow and loads MA; plays resolve
// on demand anyway, so refresh at most daily (and after an MA library sync).
const INDEX_REFRESH_MS = 24 * 60 * 60_000;
// Errors a retry can't fix; wait for a settings change instead.
const TERMINAL_ERRORS = new Set(['auth_failed', 'unsupported_version']);

export function getMaConfig(config) {
  const raw = config?.musicAssistant || {};
  const userMap = raw.userMap && typeof raw.userMap === 'object' ? raw.userMap : {};
  return {
    enabled: Boolean(raw.enabled),
    url: normalizeMusicAssistantUrl(raw.url),
    token: String(raw.token || ''),
    tokenSet: Boolean(raw.token),
    providerInstance: String(raw.providerInstance || '').trim(),
    userMap: Object.fromEntries(Object.entries(userMap).map(([k, v]) => [String(k), String(v || '').trim()])),
    defaultUser: String(raw.defaultUser || '').trim(),
  };
}

// MA user id -> Curatorr user_plex_id, or '' to drop the play.
export function resolveMaListener(maConfig, maUserId) {
  const id = String(maUserId || '').trim();
  if (!id) return maConfig.defaultUser || '';
  return Object.prototype.hasOwnProperty.call(maConfig.userMap, id) ? maConfig.userMap[id] : '';
}

function emptyCounters() {
  return { plays: 0, matched: 0, unmatched: 0, droppedNoUser: 0, errors: 0 };
}

let state = null;

function currentStatus() {
  if (!state) return { state: 'disabled' };
  return {
    state: state.status,
    serverVersion: state.serverInfo?.server_version || null,
    serverName: state.serverInfo?.name || null,
    user: state.user ? { userId: state.user.user_id, username: state.user.username, role: state.user.role } : null,
    tokenExpiresAt: state.tokenExpiresAt,
    connectedAt: state.connectedAt,
    lastEventAt: state.lastEventAt,
    lastPlayAt: state.lastPlayAt,
    lastError: state.lastError,
    index: state.indexInfo,
    counters: { ...state.counters },
  };
}

export function getMusicAssistantStatus() {
  return currentStatus();
}

// Snapshot of an MA queue for the overview "Now Playing" card.
export function summarizeMaQueue(queue) {
  const item = queue?.current_item;
  const media = item?.media_item || {};
  if (!item) return null;
  const artists = Array.isArray(media.artists) ? media.artists.map((a) => (typeof a === 'string' ? a : a?.name)).filter(Boolean) : [];
  const image = item.image || media.image || null;
  // Plex-provided artwork is a server path (/library/metadata/<id>/thumb/<ts>) that the
  // overview card already proxies for Plex sessions; other providers' art isn't reachable.
  const imagePath = image && String(image.provider || '').startsWith('plex') && String(image.path || '').startsWith('/library/')
    ? String(image.path)
    : '';
  return {
    queueId: String(queue.queue_id || ''),
    state: String(queue.state || ''),
    title: String(media.name || item.name || ''),
    artist: artists.join(', '),
    album: String(media.album?.name || ''),
    uri: String(media.uri || ''),
    imagePath,
  };
}

// Listener for a queue: the MA user seen in its latest play report, else the only
// mapped listener (single-user setups), else the default listener.
export function resolveQueueListener(maConfig, queueUserId) {
  if (queueUserId) return resolveMaListener(maConfig, queueUserId);
  const mapped = [...new Set(Object.values(maConfig.userMap).filter(Boolean))];
  if (mapped.length === 1) return mapped[0];
  return maConfig.defaultUser || '';
}

export function getMusicAssistantNowPlaying(ctx, listenerIds = []) {
  if (!state || state.status !== 'connected') return null;
  const wanted = new Set(listenerIds.map((id) => String(id || '').trim().toLowerCase()).filter(Boolean));
  if (!wanted.size) return null;
  const maConfig = getMaConfig(ctx.loadConfig());
  const candidates = [...state.queues.values()]
    .filter((q) => (q.state === 'playing' || q.state === 'paused') && q.title)
    .filter((q) => wanted.has(resolveQueueListener(maConfig, state.queueUsers.get(q.queueId)).toLowerCase()))
    .sort((a, b) => (a.state === b.state ? b.updatedAt - a.updatedAt : (a.state === 'playing' ? -1 : 1)));
  return candidates[0] || null;
}

export function startMusicAssistant(ctx, { WebSocketImpl } = {}) {
  stopMusicAssistant();
  const config = ctx.loadConfig();
  const maConfig = getMaConfig(config);
  if (!maConfig.enabled || !maConfig.url || !maConfig.token) return currentStatus();

  const { db, pushLog } = ctx;
  const recorder = createPlayRecorder(ctx);
  const warnedUsers = new Set();
  const local = {
    status: 'connecting',
    serverInfo: null,
    user: null,
    tokenExpiresAt: decodeTokenExpiry(maConfig.token),
    connectedAt: null,
    lastEventAt: null,
    lastPlayAt: null,
    lastError: null,
    indexInfo: null,
    counters: emptyCounters(),
    queues: new Map(), // queue_id -> summarizeMaQueue() + updatedAt
    queueUsers: new Map(), // queue_id -> MA user id from the latest play report
    connection: null,
    reconnectTimer: null,
    indexTimer: null,
    backoffMs: RECONNECT_MIN_MS,
    stopped: false,
  };
  state = local;

  const log = (level, action, message, meta) => pushLog?.({ level, app: 'music-assistant', action, message, ...(meta ? { meta } : {}) });

  const send = (command, args, options) => (local.connection
    ? local.connection.send(command, args, options)
    : Promise.reject(new MusicAssistantError('closed', 'Not connected.')));

  const identity = createMaIdentity({
    db,
    send,
    getOptions: () => ({
      providerInstance: getMaConfig(ctx.loadConfig()).providerInstance,
      serverType: String(ctx.loadConfig()?.mediaServer?.type || 'plex').toLowerCase(),
    }),
  });

  async function recordFinish(finish) {
    const liveConfig = ctx.loadConfig();
    const liveMa = getMaConfig(liveConfig);
    const listener = resolveMaListener(liveMa, finish.userId);
    if (!listener) {
      local.counters.droppedNoUser += 1;
      const warnKey = finish.userId || '(none)';
      if (!warnedUsers.has(warnKey)) {
        warnedUsers.add(warnKey);
        log('warn', 'play.unmapped-user', `Ignoring Music Assistant plays from MA user ${warnKey}: not mapped to a Curatorr user.`);
      }
      return;
    }
    const report = finish.report || {};
    const match = await identity.resolve(finish.uri, report);
    const libraryTrack = match.track || null;
    const artistName = String(libraryTrack?.artistName || report.artist || (Array.isArray(report.artists) ? report.artists[0] : '') || '').trim();
    const session = {
      session_key: `ma:${finish.instanceKey}:${finish.startedAt}`,
      user_plex_id: listener,
      plex_rating_key: match.ratingKey || '',
      track_title: String(libraryTrack?.trackTitle || report.name || '').trim(),
      artist_name: artistName,
      album_name: String(libraryTrack?.albumName || report.album || '').trim(),
      library_key: String(libraryTrack?.libraryKey || ''),
      started_at: finish.startedAt,
      track_duration_ms: finish.durationMs || Number(libraryTrack?.durationMs || 0),
      accumulated_ms: 0,
      max_position_ms: finish.listenedMs,
      last_position_ms: 0,
      playing_since: 0,
    };
    const smartSettings = resolveUserSmartConfig(db, liveConfig, listener);
    const result = recorder.recordOrUpdateSessionPlay({
      session,
      endedAt: finish.endedAt,
      playbackPositionMs: finish.listenedMs,
      smartSettings,
      eventSource: MA_EVENT_SOURCE,
      allowRecentPlayReuse: true,
      allowMergeCandidate: false,
    });
    if (result.duplicate) return;
    local.counters.plays += 1;
    if (match.ratingKey) local.counters.matched += 1;
    else local.counters.unmatched += 1;
    local.lastPlayAt = Date.now();
    log('info', 'play.recorded', `${result.isSkip ? 'Skip' : 'Play'}: "${session.track_title}" by ${session.artist_name} [user=${listener}, match=${match.method}]`);
  }

  const tracker = createPlayTracker({
    onFinish: (finish) => {
      recordFinish(finish).catch((err) => {
        local.counters.errors += 1;
        log('error', 'play.error', `Failed to record Music Assistant play: ${err?.message || err}`);
      });
    },
  });
  local.tracker = tracker;

  async function refreshIndex(reason) {
    if (local.stopped || local.status !== 'connected' || local.indexRunning) return;
    local.indexRunning = true;
    const startedAt = Date.now();
    try {
      const counts = await identity.rebuildIndex({ shouldStop: () => local.stopped || local.status !== 'connected' });
      local.indexInfo = { ...counts, finishedAt: Date.now(), durationMs: Date.now() - startedAt };
      log('info', 'index.done', `Music Assistant track index (${reason}): ${counts.matched}/${counts.scanned} tracks matched to the library.`);
    } catch (err) {
      log('warn', 'index.error', `Music Assistant track index failed: ${err?.message || err}`);
    } finally {
      local.indexRunning = false;
    }
  }

  function updateQueue(queue) {
    const summary = summarizeMaQueue(queue);
    const queueId = String(queue?.queue_id || '');
    if (!queueId) return;
    if (summary) local.queues.set(queueId, { ...summary, updatedAt: Date.now() });
    else local.queues.delete(queueId);
  }

  function handleEvent(msg) {
    local.lastEventAt = Date.now();
    switch (msg.event) {
      case 'media_item_played':
        if (msg.data?.player_id && msg.data?.userid) local.queueUsers.set(String(msg.data.player_id), String(msg.data.userid));
        tracker.ingest(msg.data);
        break;
      case 'queue_updated':
      case 'queue_added':
        updateQueue(msg.data);
        break;
      case 'queue_removed':
        local.queues.delete(String(msg.object_id || msg.data?.queue_id || ''));
        break;
      case 'media_item_updated':
      case 'media_item_deleted':
        if (msg.data?.media_type === 'track') identity.forget(msg.object_id || msg.data?.uri);
        break;
      case 'music_sync_completed':
        refreshIndex('library sync completed');
        break;
      default:
        break;
    }
  }

  function scheduleReconnect() {
    if (local.stopped) return;
    const delay = local.backoffMs;
    local.backoffMs = Math.min(local.backoffMs * 2, RECONNECT_MAX_MS);
    local.reconnectTimer = setTimeout(connect, delay);
    local.reconnectTimer.unref?.();
  }

  function connect() {
    if (local.stopped) return;
    local.status = 'connecting';
    const connection = connectMusicAssistant({
      url: maConfig.url,
      token: maConfig.token,
      ...(WebSocketImpl ? { WebSocketImpl } : {}),
      onEvent: handleEvent,
      onClose: (reason) => {
        if (local.connection !== connection) return;
        local.connection = null;
        if (local.stopped) return;
        tracker.flush();
        if (local.status === 'connected') {
          local.status = 'disconnected';
          local.lastError = reason?.message || 'Connection closed.';
          log('warn', 'disconnected', `Music Assistant connection lost: ${local.lastError}`);
          scheduleReconnect();
        }
      },
    });
    local.connection = connection;
    connection.ready.then(({ serverInfo, user }) => {
      if (local.stopped) { connection.close(); return; }
      local.status = 'connected';
      local.serverInfo = serverInfo;
      local.user = user;
      local.connectedAt = Date.now();
      local.lastError = null;
      local.backoffMs = RECONNECT_MIN_MS;
      log('info', 'connected', `Connected to Music Assistant ${serverInfo.server_version} as ${user?.username || 'unknown user'}.`);
      connection.send('player_queues/all', {}).then((queues) => {
        (Array.isArray(queues) ? queues : []).forEach(updateQueue);
      }).catch(() => {});
      if (Date.now() - identity.lastIndexedAt() > INDEX_REFRESH_MS) refreshIndex('connected');
    }, (err) => {
      if (local.connection === connection) local.connection = null;
      if (local.stopped) return;
      const code = err?.code || 'error';
      local.lastError = err?.message || String(err);
      local.status = TERMINAL_ERRORS.has(code) ? code : 'error';
      log(TERMINAL_ERRORS.has(code) ? 'error' : 'warn', 'connect.error', `Music Assistant: ${local.lastError}`);
      if (!TERMINAL_ERRORS.has(code)) scheduleReconnect();
    });
  }

  local.indexTimer = setInterval(() => refreshIndex('scheduled'), INDEX_REFRESH_MS);
  local.indexTimer.unref?.();
  connect();
  return currentStatus();
}

export function stopMusicAssistant() {
  if (!state) return;
  const local = state;
  local.stopped = true;
  clearTimeout(local.reconnectTimer);
  clearInterval(local.indexTimer);
  local.tracker?.clear();
  local.connection?.close();
  local.connection = null;
  state = null;
}

export function restartMusicAssistant(ctx, options) {
  return startMusicAssistant(ctx, options);
}

// One-shot connection for the settings "Test connection" button. Returns what the
// settings panel needs to populate its provider and user selects.
export async function probeMusicAssistant({ url, token, WebSocketImpl }) {
  const connection = connectMusicAssistant({ url, token, ...(WebSocketImpl ? { WebSocketImpl } : {}) });
  try {
    const { serverInfo, user } = await connection.ready;
    const [providers, users] = await Promise.all([
      connection.send('providers', {}).catch(() => []),
      connection.send('auth/users', {}).catch(() => null),
    ]);
    return {
      ok: true,
      serverVersion: serverInfo.server_version,
      serverName: serverInfo.name,
      user: user ? { userId: user.user_id, username: user.username, displayName: user.display_name, role: user.role } : null,
      tokenExpiresAt: decodeTokenExpiry(token),
      canListUsers: Array.isArray(users),
      users: (Array.isArray(users) ? users : (user ? [user] : [])).map((u) => ({
        userId: u.user_id, username: u.username, displayName: u.display_name || u.username, role: u.role,
      })),
      providers: (Array.isArray(providers) ? providers : [])
        .filter((p) => p?.type === 'music' && ['plex', 'jellyfin', 'emby'].includes(p.domain))
        .map((p) => ({ instanceId: p.instance_id, domain: p.domain, name: p.name, available: p.available !== false })),
    };
  } catch (err) {
    return { ok: false, code: err?.code || 'error', error: err?.message || String(err) };
  } finally {
    connection.close();
  }
}
