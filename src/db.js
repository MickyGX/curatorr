import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

// ─── Schema ───────────────────────────────────────────────────────────────────

const SCHEMA = `
-- Raw event log: one row per play session
CREATE TABLE IF NOT EXISTS play_events (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  user_plex_id      TEXT NOT NULL,          -- Plex account ID or username
  plex_rating_key   TEXT NOT NULL,          -- Plex track ratingKey
  track_title       TEXT NOT NULL DEFAULT '',
  artist_name       TEXT NOT NULL DEFAULT '',
  album_name        TEXT NOT NULL DEFAULT '',
  library_key       TEXT NOT NULL DEFAULT '',
  started_at        INTEGER NOT NULL,       -- unix ms
  ended_at          INTEGER,               -- unix ms, null if session still open
  duration_ms       INTEGER DEFAULT 0,     -- how long they actually listened
  track_duration_ms INTEGER DEFAULT 0,     -- full track length from Plex metadata
  is_skip           INTEGER NOT NULL DEFAULT 0, -- 1 = skip
  event_source      TEXT NOT NULL DEFAULT 'tautulli', -- 'tautulli' | 'plex_webhook'
  session_key       TEXT DEFAULT '',       -- Plex session id for deduplication
  created_at        INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);

CREATE INDEX IF NOT EXISTS idx_play_events_user       ON play_events(user_plex_id);
CREATE INDEX IF NOT EXISTS idx_play_events_artist     ON play_events(artist_name);
CREATE INDEX IF NOT EXISTS idx_play_events_rating_key ON play_events(plex_rating_key);
CREATE INDEX IF NOT EXISTS idx_play_events_started_at ON play_events(started_at);
CREATE INDEX IF NOT EXISTS idx_play_events_session    ON play_events(session_key) WHERE session_key != '';

-- Per-user per-track aggregated counters (rebuilt from play_events)
CREATE TABLE IF NOT EXISTS track_stats (
  plex_rating_key       TEXT NOT NULL,
  user_plex_id          TEXT NOT NULL,
  track_title           TEXT NOT NULL DEFAULT '',
  artist_name           TEXT NOT NULL DEFAULT '',
  album_name            TEXT NOT NULL DEFAULT '',
  play_count            INTEGER NOT NULL DEFAULT 0,
  skip_count            INTEGER NOT NULL DEFAULT 0,
  consecutive_skips     INTEGER NOT NULL DEFAULT 0,
  excluded_from_smart   INTEGER NOT NULL DEFAULT 0, -- 1 = excluded
  manually_excluded     INTEGER NOT NULL DEFAULT 0, -- 1 = manual override
  manually_included     INTEGER NOT NULL DEFAULT 0, -- 1 = force include despite skips
  tier                  TEXT NOT NULL DEFAULT 'curatorr', -- curatorr|skip|decent|half-decent|belter
  tier_weight           REAL NOT NULL DEFAULT 0,
  last_played_at        INTEGER,
  last_skipped_at       INTEGER,
  updated_at            INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  PRIMARY KEY (plex_rating_key, user_plex_id)
);

CREATE INDEX IF NOT EXISTS idx_track_stats_artist ON track_stats(artist_name);
CREATE INDEX IF NOT EXISTS idx_track_stats_user   ON track_stats(user_plex_id);

-- Per-user per-artist aggregated counters
CREATE TABLE IF NOT EXISTS artist_stats (
  artist_name           TEXT NOT NULL,
  user_plex_id          TEXT NOT NULL,
  play_count            INTEGER NOT NULL DEFAULT 0,
  skip_count            INTEGER NOT NULL DEFAULT 0,
  consecutive_skips     INTEGER NOT NULL DEFAULT 0,
  excluded_from_smart   INTEGER NOT NULL DEFAULT 0,
  manually_excluded     INTEGER NOT NULL DEFAULT 0,
  manually_included     INTEGER NOT NULL DEFAULT 0,
  ranking_score         REAL NOT NULL DEFAULT 5.0, -- 0–10; start at 5; adjusted by track tier changes
  last_played_at        INTEGER,
  last_skipped_at       INTEGER,
  updated_at            INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  PRIMARY KEY (artist_name, user_plex_id)
);

CREATE INDEX IF NOT EXISTS idx_artist_stats_user ON artist_stats(user_plex_id);

-- Open sessions: tracks that started but haven't ended yet (prevents duplicate events)
CREATE TABLE IF NOT EXISTS open_sessions (
  session_key     TEXT PRIMARY KEY,
  user_plex_id    TEXT NOT NULL,
  plex_rating_key TEXT NOT NULL,
  track_title     TEXT NOT NULL DEFAULT '',
  artist_name     TEXT NOT NULL DEFAULT '',
  album_name      TEXT NOT NULL DEFAULT '',
  library_key     TEXT NOT NULL DEFAULT '',
  track_duration_ms INTEGER DEFAULT 0,
  started_at      INTEGER NOT NULL,
  event_source    TEXT NOT NULL DEFAULT 'tautulli',
  created_at      INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);

-- Playlist sync log: what was pushed to Plex and when
CREATE TABLE IF NOT EXISTS playlist_syncs (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  user_plex_id      TEXT NOT NULL,
  plex_playlist_id  TEXT NOT NULL,   -- Plex ratingKey of the playlist
  playlist_title    TEXT NOT NULL DEFAULT '',
  synced_at         INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  track_count       INTEGER NOT NULL DEFAULT 0,
  excluded_tracks   INTEGER NOT NULL DEFAULT 0,
  excluded_artists  INTEGER NOT NULL DEFAULT 0,
  trigger           TEXT NOT NULL DEFAULT 'auto'  -- 'auto' | 'manual'
);

CREATE INDEX IF NOT EXISTS idx_playlist_syncs_user ON playlist_syncs(user_plex_id);

-- Lidarr tag tracking: artists we've tagged in Lidarr
CREATE TABLE IF NOT EXISTS lidarr_tags (
  artist_name     TEXT NOT NULL,
  lidarr_artist_id INTEGER,
  tag_id          INTEGER,
  tag_name        TEXT NOT NULL DEFAULT '',
  reason          TEXT NOT NULL DEFAULT '',   -- 'high_skips' | 'manual'
  tagged_at       INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  removed_at      INTEGER,
  PRIMARY KEY (artist_name)
);

CREATE TABLE IF NOT EXISTS master_tracks (
  rating_key    TEXT NOT NULL PRIMARY KEY,
  artist_name   TEXT NOT NULL DEFAULT '',
  track_title   TEXT NOT NULL DEFAULT '',
  album_name    TEXT NOT NULL DEFAULT '',
  genres        TEXT NOT NULL DEFAULT '[]',
  library_key   TEXT NOT NULL DEFAULT '',
  updated_at    INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);

CREATE TABLE IF NOT EXISTS user_preferences (
  user_plex_id        TEXT NOT NULL PRIMARY KEY,
  liked_genres        TEXT NOT NULL DEFAULT '[]',   -- JSON string[]
  ignored_genres      TEXT NOT NULL DEFAULT '[]',
  liked_artists       TEXT NOT NULL DEFAULT '[]',
  ignored_artists     TEXT NOT NULL DEFAULT '[]',
  user_wizard_completed INTEGER NOT NULL DEFAULT 0,
  updated_at          INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);

CREATE TABLE IF NOT EXISTS user_playlists (
  user_plex_id        TEXT NOT NULL PRIMARY KEY,
  playlist_id         TEXT NOT NULL,
  playlist_title      TEXT NOT NULL DEFAULT '',
  created_at          INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);
`;

// ─── Init ─────────────────────────────────────────────────────────────────────

export function initDb(dbPath) {
  const dir = path.dirname(dbPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const db = new Database(dbPath);

  // WAL mode for better concurrent read performance
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  db.pragma('busy_timeout = 5000');

  // Apply schema
  db.exec(SCHEMA);

  // ── Migrations (idempotent) ──────────────────────────────────────────────
  const trackCols = db.prepare('PRAGMA table_info(track_stats)').all().map((c) => c.name);
  if (!trackCols.includes('tier'))
    db.exec("ALTER TABLE track_stats ADD COLUMN tier TEXT NOT NULL DEFAULT 'curatorr'");
  if (!trackCols.includes('tier_weight'))
    db.exec('ALTER TABLE track_stats ADD COLUMN tier_weight REAL NOT NULL DEFAULT 0');

  const artistCols = db.prepare('PRAGMA table_info(artist_stats)').all().map((c) => c.name);
  if (!artistCols.includes('ranking_score'))
    db.exec('ALTER TABLE artist_stats ADD COLUMN ranking_score REAL NOT NULL DEFAULT 5.0');

  return db;
}

// ─── Session helpers ──────────────────────────────────────────────────────────

export function openSession(db, {
  sessionKey, userPlexId, plexRatingKey,
  trackTitle, artistName, albumName, libraryKey,
  trackDurationMs, startedAt, eventSource,
}) {
  db.prepare(`
    INSERT OR REPLACE INTO open_sessions
      (session_key, user_plex_id, plex_rating_key, track_title, artist_name,
       album_name, library_key, track_duration_ms, started_at, event_source)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    sessionKey, userPlexId, plexRatingKey,
    trackTitle || '', artistName || '', albumName || '', libraryKey || '',
    trackDurationMs || 0, startedAt, eventSource || 'tautulli',
  );
}

export function getOpenSession(db, sessionKey) {
  return db.prepare('SELECT * FROM open_sessions WHERE session_key = ?').get(sessionKey) || null;
}

export function closeSession(db, sessionKey) {
  db.prepare('DELETE FROM open_sessions WHERE session_key = ?').run(sessionKey);
}

export function expireOldSessions(db, olderThanMs = 4 * 60 * 60 * 1000) {
  const cutoff = Date.now() - olderThanMs;
  const stale = db.prepare('SELECT * FROM open_sessions WHERE started_at < ?').all(cutoff);
  if (stale.length) {
    db.prepare('DELETE FROM open_sessions WHERE started_at < ?').run(cutoff);
  }
  return stale;
}

// ─── Event recording ──────────────────────────────────────────────────────────

export function recordPlayEvent(db, {
  userPlexId, plexRatingKey, trackTitle, artistName, albumName, libraryKey,
  startedAt, endedAt, durationMs, trackDurationMs, isSkip, eventSource, sessionKey,
}) {
  const result = db.prepare(`
    INSERT INTO play_events
      (user_plex_id, plex_rating_key, track_title, artist_name, album_name,
       library_key, started_at, ended_at, duration_ms, track_duration_ms,
       is_skip, event_source, session_key)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    userPlexId, plexRatingKey,
    trackTitle || '', artistName || '', albumName || '', libraryKey || '',
    startedAt, endedAt || null,
    durationMs || 0, trackDurationMs || 0,
    isSkip ? 1 : 0,
    eventSource || 'tautulli',
    sessionKey || '',
  );
  return result.lastInsertRowid;
}

// ─── Tier classification ──────────────────────────────────────────────────────

// Classify a play into a tier based on how long the user listened.
// Returns: 'skip' | 'decent' | 'half-decent' | 'belter'
// (Tracks with no play are implicitly 'curatorr' — the default column value.)
export function classifyTier(listenedMs, trackDurationMs, smartConfig) {
  const skipMs = (Number(smartConfig.skipThresholdSeconds) || 30) * 1000;
  const completionMs = (Number(smartConfig.completionThresholdSeconds) || 30) * 1000;
  if (listenedMs < skipMs) return 'skip';
  if (trackDurationMs > 0 && listenedMs >= trackDurationMs - completionMs) return 'belter';
  if (trackDurationMs > 0 && listenedMs >= trackDurationMs * 0.5) return 'half-decent';
  return 'decent';
}

// Map a tier name to its weight value.
// half-decent = skipWeight / 2; decent = belterWeight / 2 (always derived, never stored).
function _tierWeight(tier, smartConfig) {
  const skipW   = Number(smartConfig.skipWeight)   || -1;
  const belterW = Number(smartConfig.belterWeight) || 1;
  switch (tier) {
    case 'skip':        return skipW;
    case 'half-decent': return skipW / 2;
    case 'decent':      return belterW / 2;
    case 'belter':      return belterW;
    default:            return 0; // 'curatorr' (unheard)
  }
}

// ─── Stats update ─────────────────────────────────────────────────────────────

// Returns { scoreDelta, tier, isSkip }
// scoreDelta is the change to apply to the artist's ranking_score.
export function updateTrackStats(db, {
  userPlexId, plexRatingKey, trackTitle, artistName, albumName,
  listenedMs, trackDurationMs, playedAt, songSkipLimit, smartConfig,
}) {
  const now = Date.now();
  const tier = classifyTier(listenedMs, trackDurationMs, smartConfig);
  const weight = _tierWeight(tier, smartConfig);
  const isSkip = tier === 'skip';

  const existing = db.prepare(
    'SELECT * FROM track_stats WHERE plex_rating_key = ? AND user_plex_id = ?',
  ).get(plexRatingKey, userPlexId);

  let scoreDelta = 0;

  if (!existing) {
    // First play: previous tier was 'curatorr' (weight 0), so delta = new weight
    scoreDelta = weight;
    db.prepare(`
      INSERT INTO track_stats
        (plex_rating_key, user_plex_id, track_title, artist_name, album_name,
         play_count, skip_count, consecutive_skips, excluded_from_smart,
         tier, tier_weight,
         last_played_at, last_skipped_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      plexRatingKey, userPlexId,
      trackTitle || '', artistName || '', albumName || '',
      isSkip ? 0 : 1,
      isSkip ? 1 : 0,
      isSkip ? 1 : 0,
      0,
      tier, weight,
      isSkip ? null : playedAt,
      isSkip ? playedAt : null,
      now,
    );
  } else {
    const prevTier = existing.tier || 'curatorr';
    const prevWeight = existing.tier_weight ?? 0;
    // Only apply delta when the tier actually changes
    if (tier !== prevTier) scoreDelta = weight - prevWeight;

    const newPlayCount = isSkip ? existing.play_count : existing.play_count + 1;
    const newSkipCount = isSkip ? existing.skip_count + 1 : existing.skip_count;
    const newConsecutive = isSkip ? existing.consecutive_skips + 1 : 0;
    const newExcluded = existing.manually_included
      ? 0
      : (newConsecutive >= songSkipLimit ? 1 : existing.excluded_from_smart);

    db.prepare(`
      UPDATE track_stats SET
        track_title = ?, artist_name = ?, album_name = ?,
        play_count = ?, skip_count = ?, consecutive_skips = ?,
        excluded_from_smart = ?,
        tier = ?, tier_weight = ?,
        last_played_at = ?, last_skipped_at = ?, updated_at = ?
      WHERE plex_rating_key = ? AND user_plex_id = ?
    `).run(
      trackTitle || existing.track_title,
      artistName || existing.artist_name,
      albumName || existing.album_name,
      newPlayCount, newSkipCount, newConsecutive, newExcluded,
      tier, weight,
      isSkip ? existing.last_played_at : playedAt,
      isSkip ? playedAt : existing.last_skipped_at,
      now,
      plexRatingKey, userPlexId,
    );
  }

  return { scoreDelta, tier, isSkip };
}

export function updateArtistStats(db, {
  userPlexId, artistName, isSkip, playedAt, scoreDelta,
}) {
  const now = Date.now();
  const existing = db.prepare(
    'SELECT * FROM artist_stats WHERE artist_name = ? AND user_plex_id = ?',
  ).get(artistName, userPlexId);

  if (!existing) {
    const newScore = Math.min(10, Math.max(0, 5.0 + (scoreDelta || 0)));
    db.prepare(`
      INSERT INTO artist_stats
        (artist_name, user_plex_id, play_count, skip_count, consecutive_skips,
         excluded_from_smart, ranking_score, last_played_at, last_skipped_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      artistName, userPlexId,
      isSkip ? 0 : 1,
      isSkip ? 1 : 0,
      isSkip ? 1 : 0,
      0,
      newScore,
      isSkip ? null : playedAt,
      isSkip ? playedAt : null,
      now,
    );
  } else {
    const newPlayCount = isSkip ? existing.play_count : existing.play_count + 1;
    const newSkipCount = isSkip ? existing.skip_count + 1 : existing.skip_count;
    const newConsecutive = isSkip ? existing.consecutive_skips + 1 : 0;
    const currentScore = existing.ranking_score ?? 5.0;
    const newScore = Math.min(10, Math.max(0, currentScore + (scoreDelta || 0)));
    // manually_excluded is respected; auto-exclusion now driven by ranking_score at playlist build time
    const newExcluded = existing.manually_excluded ? 1 : 0;

    db.prepare(`
      UPDATE artist_stats SET
        play_count = ?, skip_count = ?, consecutive_skips = ?,
        excluded_from_smart = ?, ranking_score = ?,
        last_played_at = ?, last_skipped_at = ?, updated_at = ?
      WHERE artist_name = ? AND user_plex_id = ?
    `).run(
      newPlayCount, newSkipCount, newConsecutive, newExcluded, newScore,
      isSkip ? existing.last_played_at : playedAt,
      isSkip ? playedAt : existing.last_skipped_at,
      now,
      artistName, userPlexId,
    );
  }
}

// ─── Query helpers ────────────────────────────────────────────────────────────

export function getExcludedTrackKeys(db, userPlexId) {
  return db.prepare(
    'SELECT plex_rating_key FROM track_stats WHERE user_plex_id = ? AND excluded_from_smart = 1 AND manually_included = 0',
  ).all(userPlexId).map((r) => r.plex_rating_key);
}

export function getExcludedArtists(db, userPlexId) {
  return db.prepare(
    'SELECT artist_name FROM artist_stats WHERE user_plex_id = ? AND excluded_from_smart = 1 AND manually_included = 0',
  ).all(userPlexId).map((r) => r.artist_name);
}

// Artists whose ranking_score is at or below the skip threshold (default ≤ 2).
export function getSkipTierArtists(db, userPlexId, threshold = 2) {
  return db.prepare(
    'SELECT artist_name FROM artist_stats WHERE user_plex_id = ? AND ranking_score <= ? AND manually_included = 0',
  ).all(userPlexId, threshold).map((r) => r.artist_name);
}

// Rating keys of tracks that have been heard at least once (tier != 'curatorr').
export function getPlayedTrackKeys(db, userPlexId) {
  return new Set(
    db.prepare(
      "SELECT plex_rating_key FROM track_stats WHERE user_plex_id = ? AND tier != 'curatorr'",
    ).all(userPlexId).map((r) => r.plex_rating_key),
  );
}

export function getTopArtists(db, userPlexId, limit = 20) {
  const filter = userPlexId
    ? "WHERE user_plex_id = ? AND LOWER(artist_name) != 'various artists'"
    : "WHERE LOWER(artist_name) != 'various artists'";
  const params = userPlexId ? [userPlexId, limit] : [limit];
  return db.prepare(`
    SELECT artist_name,
           play_count AS total_plays,
           skip_count AS total_skips,
           consecutive_skips AS skip_streak,
           CAST(play_count AS REAL) / MAX(play_count + skip_count, 1) AS play_ratio,
           excluded_from_smart AS excluded,
           manually_excluded,
           last_played_at
    FROM artist_stats
    ${filter}
    ORDER BY play_count DESC
    LIMIT ?
  `).all(...params);
}

export function getTopTracks(db, userPlexId, limit = 20) {
  const filter = userPlexId
    ? "WHERE user_plex_id = ? AND LOWER(artist_name) != 'various artists'"
    : "WHERE LOWER(artist_name) != 'various artists'";
  const params = userPlexId ? [userPlexId, limit] : [limit];
  return db.prepare(`
    SELECT plex_rating_key AS rating_key,
           track_title, artist_name, album_name,
           play_count AS total_plays,
           skip_count AS total_skips,
           consecutive_skips AS skip_streak,
           excluded_from_smart AS excluded,
           manually_excluded,
           manually_included AS force_included,
           last_played_at
    FROM track_stats
    ${filter}
    ORDER BY play_count DESC
    LIMIT ?
  `).all(...params);
}

export function getRecentHistory(db, userPlexId, limit = 50, offset = 0) {
  const filter = userPlexId ? 'WHERE user_plex_id = ?' : 'WHERE 1=1';
  const params = userPlexId ? [userPlexId, limit, offset] : [limit, offset];
  return db.prepare(`
    SELECT id, plex_rating_key, track_title, artist_name, album_name,
           started_at, ended_at, duration_ms, track_duration_ms,
           is_skip, event_source
    FROM play_events
    ${filter}
    ORDER BY started_at DESC
    LIMIT ? OFFSET ?
  `).all(...params);
}

// Returns a Set of plex_rating_key for tracks listened through to within completionThresholdMs of the end.
export function getCompletedTrackKeys(db, userPlexId, completionThresholdMs = 20000) {
  const filter = userPlexId
    ? 'WHERE user_plex_id = ? AND track_duration_ms > 0 AND duration_ms >= track_duration_ms - ?'
    : 'WHERE track_duration_ms > 0 AND duration_ms >= track_duration_ms - ?';
  const params = userPlexId ? [userPlexId, completionThresholdMs] : [completionThresholdMs];
  const rows = db.prepare(`SELECT DISTINCT plex_rating_key FROM play_events ${filter}`).all(...params);
  return new Set(rows.map((r) => r.plex_rating_key));
}

export function getPlayStats(db, userPlexId, sinceMs = 0) {
  const filter = userPlexId ? 'WHERE user_plex_id = ? AND started_at >= ?' : 'WHERE started_at >= ?';
  const params = userPlexId ? [userPlexId, sinceMs] : [sinceMs];
  return db.prepare(`
    SELECT
      COUNT(*) AS total_plays,
      SUM(is_skip) AS total_skips,
      COUNT(DISTINCT artist_name) AS unique_artists,
      COUNT(DISTINCT plex_rating_key) AS unique_tracks,
      SUM(duration_ms) AS total_listen_ms
    FROM play_events
    ${filter}
  `).get(...params);
}

export function getPlayStatsByDay(db, userPlexId, days = 30) {
  const since = Date.now() - days * 24 * 60 * 60 * 1000;
  const filter = userPlexId ? 'WHERE user_plex_id = ? AND started_at >= ?' : 'WHERE started_at >= ?';
  const params = userPlexId ? [userPlexId, since] : [since];
  return db.prepare(`
    SELECT
      date(started_at / 1000, 'unixepoch') AS day,
      COUNT(*) AS plays,
      SUM(is_skip) AS skips
    FROM play_events
    ${filter}
    GROUP BY day
    ORDER BY day ASC
  `).all(...params);
}

export function getAllUserIds(db) {
  return db.prepare('SELECT DISTINCT user_plex_id FROM play_events').all().map((r) => r.user_plex_id);
}

export function recordPlaylistSync(db, {
  userPlexId, plexPlaylistId, playlistTitle, trackCount, excludedTracks, excludedArtists, trigger,
}) {
  return db.prepare(`
    INSERT INTO playlist_syncs
      (user_plex_id, plex_playlist_id, playlist_title, track_count,
       excluded_tracks, excluded_artists, trigger, synced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    userPlexId, plexPlaylistId, playlistTitle || '',
    trackCount || 0, excludedTracks || 0, excludedArtists || 0,
    trigger || 'auto', Date.now(),
  );
}

export function getLastPlaylistSync(db, userPlexId) {
  return db.prepare(
    'SELECT * FROM playlist_syncs WHERE user_plex_id = ? ORDER BY synced_at DESC LIMIT 1',
  ).get(userPlexId) || null;
}

// ─── Manual override helpers ──────────────────────────────────────────────────

export function setTrackExclusion(db, userPlexId, plexRatingKey, excluded) {
  const existing = db.prepare(
    'SELECT * FROM track_stats WHERE plex_rating_key = ? AND user_plex_id = ?',
  ).get(plexRatingKey, userPlexId);

  if (existing) {
    db.prepare(`
      UPDATE track_stats SET
        manually_excluded = ?,
        manually_included = CASE WHEN ? = 1 THEN 0 ELSE manually_included END,
        excluded_from_smart = ?,
        updated_at = ?
      WHERE plex_rating_key = ? AND user_plex_id = ?
    `).run(excluded ? 1 : 0, excluded ? 1 : 0, excluded ? 1 : 0, Date.now(), plexRatingKey, userPlexId);
  } else {
    db.prepare(`
      INSERT INTO track_stats
        (plex_rating_key, user_plex_id, manually_excluded, excluded_from_smart, updated_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(plexRatingKey, userPlexId, excluded ? 1 : 0, excluded ? 1 : 0, Date.now());
  }
}

export function setTrackInclusion(db, userPlexId, plexRatingKey, included) {
  const existing = db.prepare(
    'SELECT * FROM track_stats WHERE plex_rating_key = ? AND user_plex_id = ?',
  ).get(plexRatingKey, userPlexId);

  if (existing) {
    db.prepare(`
      UPDATE track_stats SET
        manually_included = ?,
        manually_excluded = CASE WHEN ? = 1 THEN 0 ELSE manually_excluded END,
        excluded_from_smart = CASE WHEN ? = 1 THEN 0 ELSE excluded_from_smart END,
        updated_at = ?
      WHERE plex_rating_key = ? AND user_plex_id = ?
    `).run(included ? 1 : 0, included ? 1 : 0, included ? 1 : 0, Date.now(), plexRatingKey, userPlexId);
  } else {
    db.prepare(`
      INSERT INTO track_stats
        (plex_rating_key, user_plex_id, manually_included, updated_at)
      VALUES (?, ?, ?, ?)
    `).run(plexRatingKey, userPlexId, included ? 1 : 0, Date.now());
  }
}

export function setArtistExclusion(db, userPlexId, artistName, excluded) {
  const existing = db.prepare(
    'SELECT * FROM artist_stats WHERE artist_name = ? AND user_plex_id = ?',
  ).get(artistName, userPlexId);

  if (existing) {
    db.prepare(`
      UPDATE artist_stats SET
        manually_excluded = ?,
        manually_included = CASE WHEN ? = 1 THEN 0 ELSE manually_included END,
        excluded_from_smart = ?,
        updated_at = ?
      WHERE artist_name = ? AND user_plex_id = ?
    `).run(excluded ? 1 : 0, excluded ? 1 : 0, excluded ? 1 : 0, Date.now(), artistName, userPlexId);
  } else {
    db.prepare(`
      INSERT INTO artist_stats
        (artist_name, user_plex_id, manually_excluded, excluded_from_smart, updated_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(artistName, userPlexId, excluded ? 1 : 0, excluded ? 1 : 0, Date.now());
  }
}

export function resetTrackSkipStreak(db, userPlexId, plexRatingKey) {
  db.prepare(`
    UPDATE track_stats SET
      consecutive_skips = 0,
      excluded_from_smart = CASE WHEN manually_excluded = 0 THEN 0 ELSE excluded_from_smart END,
      updated_at = ?
    WHERE plex_rating_key = ? AND user_plex_id = ?
  `).run(Date.now(), plexRatingKey, userPlexId);
}

export function resetArtistSkipStreak(db, userPlexId, artistName) {
  db.prepare(`
    UPDATE artist_stats SET
      consecutive_skips = 0,
      excluded_from_smart = CASE WHEN manually_excluded = 0 THEN 0 ELSE excluded_from_smart END,
      updated_at = ?
    WHERE artist_name = ? AND user_plex_id = ?
  `).run(Date.now(), artistName, userPlexId);
}

// ─── User preferences ─────────────────────────────────────────────────────────

export function getUserPreferences(db, userPlexId) {
  const row = db.prepare('SELECT * FROM user_preferences WHERE user_plex_id = ?').get(userPlexId);
  if (!row) return { likedGenres: [], ignoredGenres: [], likedArtists: [], ignoredArtists: [], userWizardCompleted: false };
  return {
    likedGenres: JSON.parse(row.liked_genres || '[]'),
    ignoredGenres: JSON.parse(row.ignored_genres || '[]'),
    likedArtists: JSON.parse(row.liked_artists || '[]'),
    ignoredArtists: JSON.parse(row.ignored_artists || '[]'),
    userWizardCompleted: Boolean(row.user_wizard_completed),
  };
}

export function saveUserPreferences(db, userPlexId, { likedGenres = [], ignoredGenres = [], likedArtists = [], ignoredArtists = [], userWizardCompleted = false }) {
  db.prepare(`
    INSERT INTO user_preferences (user_plex_id, liked_genres, ignored_genres, liked_artists, ignored_artists, user_wizard_completed, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_plex_id) DO UPDATE SET
      liked_genres = excluded.liked_genres,
      ignored_genres = excluded.ignored_genres,
      liked_artists = excluded.liked_artists,
      ignored_artists = excluded.ignored_artists,
      user_wizard_completed = excluded.user_wizard_completed,
      updated_at = excluded.updated_at
  `).run(userPlexId, JSON.stringify(likedGenres), JSON.stringify(ignoredGenres), JSON.stringify(likedArtists), JSON.stringify(ignoredArtists), userWizardCompleted ? 1 : 0, Date.now());
}

export function getUserPlaylist(db, userPlexId) {
  return db.prepare('SELECT * FROM user_playlists WHERE user_plex_id = ?').get(userPlexId) || null;
}

export function saveUserPlaylist(db, userPlexId, playlistId, playlistTitle) {
  db.prepare(`
    INSERT INTO user_playlists (user_plex_id, playlist_id, playlist_title)
    VALUES (?, ?, ?)
    ON CONFLICT(user_plex_id) DO UPDATE SET playlist_id = excluded.playlist_id, playlist_title = excluded.playlist_title
  `).run(userPlexId, playlistId, playlistTitle);
}

// ─── Master track cache ───────────────────────────────────────────────────────

export function refreshMasterTracks(db, tracks) {
  const upsert = db.prepare(`
    INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name, genres, library_key, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(rating_key) DO UPDATE SET
      artist_name = excluded.artist_name, track_title = excluded.track_title,
      album_name = excluded.album_name, genres = excluded.genres,
      library_key = excluded.library_key, updated_at = excluded.updated_at
  `);
  const run = db.transaction((rows) => { for (const r of rows) upsert.run(r.ratingKey, r.artistName, r.trackTitle, r.albumName, JSON.stringify(r.genres || []), r.libraryKey, Date.now()); });
  run(tracks);
}

export function getMasterTracks(db) {
  return db.prepare('SELECT * FROM master_tracks').all().map((r) => ({
    ratingKey: r.rating_key, artistName: r.artist_name, trackTitle: r.track_title,
    albumName: r.album_name, genres: JSON.parse(r.genres || '[]'), libraryKey: r.library_key,
  }));
}

export function getMasterTrackCount(db) {
  return db.prepare('SELECT COUNT(*) as n FROM master_tracks').get().n;
}

export function getGenresFromMaster(db) {
  const rows = db.prepare('SELECT DISTINCT value FROM master_tracks, json_each(master_tracks.genres) ORDER BY value').all();
  return rows.map((r) => r.value).filter(Boolean);
}

export function getArtistsFromMaster(db, filterGenres = []) {
  if (!filterGenres.length) {
    return db.prepare('SELECT DISTINCT artist_name FROM master_tracks ORDER BY artist_name').all().map((r) => r.artist_name);
  }
  // Return artists who have at least one track in any of the specified genres
  const placeholders = filterGenres.map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT DISTINCT m.artist_name FROM master_tracks m
    WHERE EXISTS (
      SELECT 1 FROM json_each(m.genres) g WHERE g.value IN (${placeholders})
    )
    ORDER BY m.artist_name
  `).all(...filterGenres);
  return rows.map((r) => r.artist_name);
}
