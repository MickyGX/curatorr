// Turns Music Assistant media_item_played progress reports into finished plays.
//
// MA reports on every state change and every 30s while playing; a finished or
// skipped track arrives as is_playing:false. Behaviour observed against MA 2.10.4
// (see src/test/fixtures/ma-events.ndjson):
//   - a pause also sends is_playing:false / fully_played:false, indistinguishable
//     from a skip, and seconds_played keeps counting cumulatively after resume;
//   - starting a new queue can re-send the previous item's final report with a
//     bogus duration, so durations only ever grow;
//   - seconds_played runs a few seconds short of wall time, so fully_played is the
//     completion signal.
// Plays are keyed by (queue, uri); a drop in seconds_played starts a new instance.

const RESTART_TOLERANCE_SECONDS = 5;
export const PROVISIONAL_FINISH_DELAY_MS = 60_000;
const EVICT_AFTER_MS = 30 * 60_000;
const MIN_EVICT_WINDOW_MS = 10 * 60_000;

export function createPlayTracker({
  onFinish,
  now = () => Date.now(),
  setTimer = (fn, ms) => { const t = setTimeout(fn, ms); t.unref?.(); return t; },
  clearTimer = (t) => clearTimeout(t),
  provisionalDelayMs = PROVISIONAL_FINISH_DELAY_MS,
}) {
  const instances = new Map();
  let nextInstanceId = 1;

  function emit(instance) {
    if (instance.pendingTimer) {
      clearTimer(instance.pendingTimer);
      instance.pendingTimer = null;
    }
    if (instance.maxSeconds <= instance.finalisedSeconds && instance.fullyPlayed === instance.finalisedFully) return;
    instance.finalisedSeconds = instance.maxSeconds;
    instance.finalisedFully = instance.fullyPlayed;
    const durationSeconds = instance.maxDuration;
    const listenedSeconds = instance.fullyPlayed && durationSeconds > 0
      ? Math.max(durationSeconds, instance.maxSeconds)
      : instance.maxSeconds;
    onFinish({
      instanceKey: `${instance.queueId}:${instance.instanceId}`,
      queueId: instance.queueId,
      uri: instance.uri,
      userId: instance.userId,
      startedAt: instance.startedAt,
      endedAt: now(),
      durationMs: Math.round(durationSeconds * 1000),
      listenedMs: Math.round(listenedSeconds * 1000),
      fullyPlayed: instance.fullyPlayed,
      report: instance.report,
    });
  }

  function evictStale() {
    const cutoff = now();
    for (const [key, instance] of instances) {
      const window = Math.max(instance.maxDuration * 1000, MIN_EVICT_WINDOW_MS) + EVICT_AFTER_MS;
      if (cutoff - instance.lastSeenAt > window) {
        if (instance.pendingTimer) clearTimer(instance.pendingTimer);
        instances.delete(key);
      }
    }
  }

  function ingest(data) {
    if (!data || data.media_type !== 'track' || !data.uri) return;
    const queueId = String(data.player_id || '');
    const key = `${queueId}|${data.uri}`;
    const seconds = Math.max(0, Number(data.seconds_played || 0));
    const duration = Math.max(0, Number(data.duration || 0));
    const at = now();

    let instance = instances.get(key);
    if (instance && seconds < instance.maxSeconds - RESTART_TOLERANCE_SECONDS) {
      // Restart or repeat of the same item: settle the previous instance first.
      if (instance.pendingTimer) emit(instance);
      instance = null;
    }
    if (!instance) {
      instance = {
        instanceId: nextInstanceId,
        queueId,
        uri: data.uri,
        userId: data.userid || null,
        startedAt: at - seconds * 1000,
        maxSeconds: 0,
        maxDuration: 0,
        fullyPlayed: false,
        finalisedSeconds: -1,
        finalisedFully: false,
        pendingTimer: null,
        lastSeenAt: at,
        report: data,
      };
      nextInstanceId += 1;
      instances.set(key, instance);
    }

    instance.maxSeconds = Math.max(instance.maxSeconds, seconds);
    instance.maxDuration = Math.max(instance.maxDuration, duration);
    instance.fullyPlayed = instance.fullyPlayed || Boolean(data.fully_played);
    instance.lastSeenAt = at;
    if (!instance.userId && data.userid) instance.userId = data.userid;
    instance.report = { ...instance.report, ...data, duration: instance.maxDuration };

    if (data.is_playing) {
      // Still playing (or resumed): a pending pause/skip decision is void.
      if (instance.pendingTimer) {
        clearTimer(instance.pendingTimer);
        instance.pendingTimer = null;
      }
    } else if (instance.fullyPlayed) {
      emit(instance);
    } else if (!instance.pendingTimer) {
      instance.pendingTimer = setTimer(() => {
        instance.pendingTimer = null;
        emit(instance);
      }, provisionalDelayMs);
    }
    evictStale();
  }

  function flush() {
    for (const instance of instances.values()) {
      if (instance.pendingTimer) emit(instance);
    }
  }

  function clear() {
    for (const instance of instances.values()) {
      if (instance.pendingTimer) clearTimer(instance.pendingTimer);
    }
    instances.clear();
  }

  return { ingest, flush, clear, size: () => instances.size };
}
