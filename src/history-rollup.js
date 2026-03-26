export const HISTORY_ROLLUP_WINDOW = 10;

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

function buildFallbackHistoryKey(event) {
  const trackTitle = normalizeHistoryText(event?.track_title);
  const artistName = normalizeHistoryText(event?.artist_name);
  if (trackTitle && artistName) return `${trackTitle}::${artistName}`;
  return [
    trackTitle,
    artistName,
    normalizeHistoryText(event?.album_name),
  ].join('::');
}

function buildHistoryEntryKey(event) {
  const userPlexId = String(event?.user_plex_id || '').trim();
  const fallback = buildFallbackHistoryKey(event);
  if (fallback) return `${userPlexId}::${fallback}`;
  const plexRatingKey = String(event?.plex_rating_key || '').trim();
  return plexRatingKey ? `${userPlexId}::${plexRatingKey}` : '';
}

function toHistoryRollupEntry(event) {
  const startedAt = Math.max(0, Number(event?.started_at || 0));
  const listenedMs = Math.max(0, Number(event?.duration_ms || 0));
  return {
    ...event,
    duration_ms: listenedMs,
    rollup_count: 1,
    rollup_total_duration_ms: listenedMs,
    rollup_latest_started_at: startedAt,
    rollup_oldest_started_at: startedAt,
  };
}

function mergeHistoryEntries(target, event) {
  const listenedMs = Math.max(0, Number(event?.duration_ms || 0));
  const startedAt = Math.max(0, Number(event?.started_at || 0));
  target.rollup_count = Math.max(1, Number(target.rollup_count || 1)) + 1;
  target.rollup_total_duration_ms = Math.max(0, Number(target.rollup_total_duration_ms || 0)) + listenedMs;
  target.duration_ms = target.rollup_total_duration_ms;
  target.rollup_latest_started_at = Math.max(
    Math.max(0, Number(target.rollup_latest_started_at || 0)),
    startedAt,
  );
  const currentOldest = Math.max(
    0,
    Number(target.rollup_oldest_started_at || target.rollup_latest_started_at || startedAt),
  );
  target.rollup_oldest_started_at = startedAt > 0
    ? Math.min(currentOldest, startedAt)
    : currentOldest;
  return target;
}

function createHistoryRollupState(windowSize = HISTORY_ROLLUP_WINDOW) {
  return {
    rolled: [],
    rawIndex: 0,
    windowSize: Math.max(0, Number(windowSize || 0)),
    lastSeenByKey: new Map(),
  };
}

function appendHistoryRollup(state, event) {
  const key = buildHistoryEntryKey(event);
  const lastSeen = key ? state.lastSeenByKey.get(key) : null;
  const shouldMerge = Boolean(
    key
    && lastSeen
    && (state.rawIndex - Number(lastSeen.rawIndex || 0)) <= state.windowSize,
  );

  if (shouldMerge && lastSeen?.entry) {
    mergeHistoryEntries(lastSeen.entry, event);
    state.lastSeenByKey.set(key, { rawIndex: state.rawIndex, entry: lastSeen.entry });
  } else {
    const entry = toHistoryRollupEntry(event);
    state.rolled.push(entry);
    if (key) state.lastSeenByKey.set(key, { rawIndex: state.rawIndex, entry });
  }

  state.rawIndex += 1;
}

export function rollupHistoryEntries(events, options = {}) {
  const state = createHistoryRollupState(options.windowSize);
  for (const event of Array.isArray(events) ? events : []) appendHistoryRollup(state, event);
  return state.rolled;
}

export function paginateRolledHistory(fetchChunk, options = {}) {
  const limit = Math.max(0, Number(options.limit || 0));
  const offset = Math.max(0, Number(options.offset || 0));
  const windowSize = Math.max(0, Number(options.windowSize || HISTORY_ROLLUP_WINDOW));
  const chunkSize = Math.max(
    limit + windowSize + 1,
    Math.max(50, Number(options.chunkSize || 0)),
  );
  const state = createHistoryRollupState(windowSize);
  const targetCount = offset + limit + 1;
  let rawOffset = 0;
  let rawExhausted = false;

  while (!rawExhausted && state.rolled.length < targetCount) {
    const nextChunk = fetchChunk(chunkSize, rawOffset);
    const chunk = Array.isArray(nextChunk) ? nextChunk : [];
    if (!chunk.length) {
      rawExhausted = true;
      break;
    }
    for (const event of chunk) appendHistoryRollup(state, event);
    rawOffset += chunk.length;
    if (chunk.length < chunkSize) rawExhausted = true;
  }

  return {
    history: state.rolled.slice(offset, offset + limit),
    hasMore: state.rolled.length > (offset + limit),
  };
}
