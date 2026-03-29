import fs from 'fs';
import path from 'path';
import {
  initDb,
  classifyTier,
  getAllUserIds,
  resolveUserSmartConfig,
  rebuildTrackStatsFromEvents,
  rebuildArtistStatsFromEvents,
} from '../db.js';

const CONSOLIDATION_WINDOW = 10;

function loadConfig(configPath) {
  try {
    return JSON.parse(fs.readFileSync(configPath, 'utf8'));
  } catch {
    return {};
  }
}

function buildPlayEventMatchKey(event) {
  const userPlexId = String(event?.user_plex_id || '').trim();
  const plexRatingKey = String(event?.plex_rating_key || '').trim();
  if (plexRatingKey) return `${userPlexId}::${plexRatingKey}`;
  return [
    userPlexId,
    String(event?.track_title || '').trim().toLowerCase(),
    String(event?.artist_name || '').trim().toLowerCase(),
    String(event?.album_name || '').trim().toLowerCase(),
  ].join('::');
}

function consolidateUserPlayEvents(db, userPlexId, smartConfig) {
  const rows = db.prepare(`
    SELECT id, user_plex_id, plex_rating_key, track_title, artist_name, album_name, library_key,
           started_at, ended_at, duration_ms, track_duration_ms, is_skip, event_source, session_key
    FROM play_events
    WHERE user_plex_id = ?
    ORDER BY started_at DESC, id DESC
  `).all(userPlexId);

  const keptByKey = new Map();
  const rowsToDelete = [];
  const rowsToUpdate = new Map();
  const affectedTracks = new Set();
  const affectedArtists = new Set();

  rows.forEach((row, index) => {
    const key = buildPlayEventMatchKey(row);
    const existing = keptByKey.get(key);
    if (existing && (index - existing.index) <= CONSOLIDATION_WINDOW) {
      const target = existing.row;
      const mergedTrackDurationMs = Math.max(
        Number(target.track_duration_ms || 0),
        Number(row.track_duration_ms || 0),
      );
      let mergedDurationMs = Math.max(0, Number(target.duration_ms || 0)) + Math.max(0, Number(row.duration_ms || 0));
      if (mergedTrackDurationMs > 0) mergedDurationMs = Math.min(mergedDurationMs, mergedTrackDurationMs);
      target.duration_ms = mergedDurationMs;
      target.track_duration_ms = mergedTrackDurationMs;
      target.is_skip = classifyTier(mergedDurationMs, mergedTrackDurationMs, smartConfig) === 'skip' ? 1 : 0;
      rowsToUpdate.set(target.id, target);
      rowsToDelete.push(row.id);
      keptByKey.set(key, { index, row: target });
      if (target.plex_rating_key) affectedTracks.add(`${userPlexId}::${target.plex_rating_key}`);
      if (row.plex_rating_key) affectedTracks.add(`${userPlexId}::${row.plex_rating_key}`);
      if (target.artist_name) affectedArtists.add(`${userPlexId}::${target.artist_name}`);
      if (row.artist_name) affectedArtists.add(`${userPlexId}::${row.artist_name}`);
      return;
    }
    keptByKey.set(key, { index, row: { ...row } });
  });

  if (!rowsToDelete.length) {
    return { mergedRows: 0, deletedRows: 0, affectedTracks: [], affectedArtists: [] };
  }

  const apply = db.transaction(() => {
    for (const row of rowsToUpdate.values()) {
      db.prepare(`
        UPDATE play_events
        SET duration_ms = ?, track_duration_ms = ?, is_skip = ?
        WHERE id = ?
      `).run(
        Number(row.duration_ms || 0),
        Number(row.track_duration_ms || 0),
        Number(row.is_skip || 0),
        row.id,
      );
    }
    for (const id of rowsToDelete) {
      db.prepare('DELETE FROM play_events WHERE id = ?').run(id);
    }
  });
  apply();

  const songSkipLimit = Number(smartConfig.songSkipLimit) || 3;
  for (const trackKey of affectedTracks) {
    const split = trackKey.indexOf('::');
    const affectedUserId = trackKey.slice(0, split);
    const plexRatingKey = trackKey.slice(split + 2);
    rebuildTrackStatsFromEvents(db, {
      userPlexId: affectedUserId,
      plexRatingKey,
      songSkipLimit,
      smartConfig,
    });
  }
  for (const artistKey of affectedArtists) {
    const split = artistKey.indexOf('::');
    const affectedUserId = artistKey.slice(0, split);
    const artistName = artistKey.slice(split + 2);
    rebuildArtistStatsFromEvents(db, {
      userPlexId: affectedUserId,
      artistName,
      smartConfig,
    });
  }

  return {
    mergedRows: rowsToUpdate.size,
    deletedRows: rowsToDelete.length,
    affectedTracks: [...affectedTracks],
    affectedArtists: [...affectedArtists],
  };
}

const dataDir = process.env.DATA_DIR || path.resolve(process.cwd(), 'data');
const configPath = process.env.CONFIG_PATH || path.resolve(process.cwd(), 'config', 'config.json');
const dbPath = path.join(dataDir, 'curatorr.db');
const db = initDb(dbPath);

try {
  const config = loadConfig(configPath);
  const requestedUser = String(process.argv[2] || '').trim();
  const userIds = requestedUser ? [requestedUser] : getAllUserIds(db);
  const summary = [];
  for (const userPlexId of userIds) {
    if (!userPlexId) continue;
    const smartConfig = resolveUserSmartConfig(db, config, userPlexId);
    const result = consolidateUserPlayEvents(db, userPlexId, smartConfig);
    summary.push({ userPlexId, ...result });
  }
  console.log(JSON.stringify(summary, null, 2));
} finally {
  db.close();
}
