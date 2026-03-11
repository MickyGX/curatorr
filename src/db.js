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
  rating_count  INTEGER NOT NULL DEFAULT 0,
  view_count    INTEGER NOT NULL DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS playlist_jobs (
  user_plex_id        TEXT NOT NULL PRIMARY KEY,
  status              TEXT NOT NULL DEFAULT 'idle',
  trigger             TEXT NOT NULL DEFAULT 'wizard',
  message             TEXT NOT NULL DEFAULT '',
  playlist_id         TEXT NOT NULL DEFAULT '',
  playlist_title      TEXT NOT NULL DEFAULT '',
  track_count         INTEGER NOT NULL DEFAULT 0,
  error_message       TEXT NOT NULL DEFAULT '',
  started_at          INTEGER,
  completed_at        INTEGER,
  updated_at          INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);

CREATE TABLE IF NOT EXISTS system_job_runs (
  job_id      TEXT NOT NULL PRIMARY KEY,
  status      TEXT NOT NULL DEFAULT 'idle',
  last_run_at INTEGER,
  message     TEXT NOT NULL DEFAULT '',
  updated_at  INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);

CREATE TABLE IF NOT EXISTS suggested_artists (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  user_plex_id        TEXT NOT NULL,
  artist_name         TEXT NOT NULL,
  source              TEXT NOT NULL DEFAULT 'curatorr',
  similarity_score    REAL NOT NULL DEFAULT 0,
  behavior_score      REAL NOT NULL DEFAULT 0,
  editorial_score     REAL NOT NULL DEFAULT 0,
  total_score         REAL NOT NULL DEFAULT 0,
  status              TEXT NOT NULL DEFAULT 'suggested',
  reason_json         TEXT NOT NULL DEFAULT '{}',
  lidarr_artist_id    INTEGER,
  first_suggested_at  INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  last_evaluated_at   INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  accepted_at         INTEGER,
  dismissed_at        INTEGER,
  UNIQUE (user_plex_id, artist_name)
);

CREATE INDEX IF NOT EXISTS idx_suggested_artists_user_status
  ON suggested_artists(user_plex_id, status, total_score DESC);

CREATE TABLE IF NOT EXISTS suggested_albums (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  user_plex_id        TEXT NOT NULL,
  artist_name         TEXT NOT NULL,
  album_title         TEXT NOT NULL,
  album_type          TEXT NOT NULL DEFAULT '',
  release_date        TEXT NOT NULL DEFAULT '',
  selection_reason    TEXT NOT NULL DEFAULT '',
  rank_score          REAL NOT NULL DEFAULT 0,
  status              TEXT NOT NULL DEFAULT 'candidate',
  lidarr_album_id     INTEGER,
  created_at          INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  updated_at          INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  UNIQUE (user_plex_id, artist_name, album_title)
);

CREATE INDEX IF NOT EXISTS idx_suggested_albums_user_status
  ON suggested_albums(user_plex_id, status, rank_score DESC);

CREATE TABLE IF NOT EXISTS suggested_tracks (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  user_plex_id        TEXT NOT NULL,
  suggestion_key      TEXT NOT NULL,
  rating_key          TEXT NOT NULL DEFAULT '',
  artist_name         TEXT NOT NULL DEFAULT '',
  track_title         TEXT NOT NULL DEFAULT '',
  album_name          TEXT NOT NULL DEFAULT '',
  source              TEXT NOT NULL DEFAULT 'curatorr',
  total_score         REAL NOT NULL DEFAULT 0,
  reason_json         TEXT NOT NULL DEFAULT '{}',
  created_at          INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  expires_at          INTEGER,
  UNIQUE (user_plex_id, suggestion_key)
);

CREATE INDEX IF NOT EXISTS idx_suggested_tracks_user_score
  ON suggested_tracks(user_plex_id, total_score DESC, created_at DESC);

CREATE TABLE IF NOT EXISTS user_generated_playlists (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  user_plex_id        TEXT NOT NULL,
  playlist_type       TEXT NOT NULL DEFAULT 'curatorred',
  playlist_key        TEXT NOT NULL,
  plex_playlist_id    TEXT NOT NULL DEFAULT '',
  playlist_title      TEXT NOT NULL DEFAULT '',
  algorithm_version   TEXT NOT NULL DEFAULT 'phase2a',
  last_built_at       INTEGER,
  last_synced_at      INTEGER,
  track_count         INTEGER NOT NULL DEFAULT 0,
  active              INTEGER NOT NULL DEFAULT 1,
  created_at          INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  updated_at          INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  UNIQUE (user_plex_id, playlist_key)
);

CREATE INDEX IF NOT EXISTS idx_user_generated_playlists_user_active
  ON user_generated_playlists(user_plex_id, active, playlist_type);

CREATE TABLE IF NOT EXISTS playlist_tracks (
  playlist_key   TEXT NOT NULL,
  user_plex_id   TEXT NOT NULL,
  rating_key     TEXT NOT NULL,
  artist_name    TEXT NOT NULL DEFAULT '',
  added_at       INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  PRIMARY KEY (playlist_key, user_plex_id, rating_key)
);

CREATE INDEX IF NOT EXISTS idx_playlist_tracks_user_playlist
  ON playlist_tracks(user_plex_id, playlist_key);

CREATE TABLE IF NOT EXISTS playlist_artist_state (
  playlist_key      TEXT NOT NULL,
  user_plex_id      TEXT NOT NULL,
  artist_name       TEXT NOT NULL,
  thresholds_fired  TEXT NOT NULL DEFAULT '[]',
  updated_at        INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  PRIMARY KEY (playlist_key, user_plex_id, artist_name)
);

CREATE TABLE IF NOT EXISTS lidarr_artist_progress (
  id                      INTEGER PRIMARY KEY AUTOINCREMENT,
  user_plex_id            TEXT NOT NULL,
  artist_name             TEXT NOT NULL,
  lidarr_artist_id        INTEGER,
  current_stage           TEXT NOT NULL DEFAULT 'suggested',
  albums_added_count      INTEGER NOT NULL DEFAULT 0,
  last_album_added_at     INTEGER,
  next_review_at          INTEGER,
  highest_observed_rank   REAL NOT NULL DEFAULT 0,
  last_manual_search_at   INTEGER,
  last_manual_search_status TEXT NOT NULL DEFAULT '',
  created_at              INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  updated_at              INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  UNIQUE (user_plex_id, artist_name)
);

CREATE INDEX IF NOT EXISTS idx_lidarr_artist_progress_user_stage
  ON lidarr_artist_progress(user_plex_id, current_stage, next_review_at);

CREATE TABLE IF NOT EXISTS lidarr_usage (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  user_plex_id        TEXT NOT NULL,
  role_name           TEXT NOT NULL DEFAULT 'user',
  usage_key           TEXT NOT NULL,
  amount              INTEGER NOT NULL DEFAULT 0,
  period_start        INTEGER NOT NULL,
  created_at          INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);

CREATE INDEX IF NOT EXISTS idx_lidarr_usage_user_period
  ON lidarr_usage(user_plex_id, period_start, usage_key);

CREATE TABLE IF NOT EXISTS lidarr_requests (
  id                    INTEGER PRIMARY KEY AUTOINCREMENT,
  user_plex_id          TEXT NOT NULL,
  source_kind           TEXT NOT NULL DEFAULT 'manual',
  request_kind          TEXT NOT NULL DEFAULT 'artist_album',
  artist_name           TEXT NOT NULL,
  album_title           TEXT NOT NULL DEFAULT '',
  foreign_artist_id     TEXT NOT NULL DEFAULT '',
  status                TEXT NOT NULL DEFAULT 'queued',
  priority_order        INTEGER NOT NULL DEFAULT 0,
  lidarr_artist_id      INTEGER,
  lidarr_album_id       INTEGER,
  detail_json           TEXT NOT NULL DEFAULT '{}',
  created_at            INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  updated_at            INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  processed_at          INTEGER
);

CREATE INDEX IF NOT EXISTS idx_lidarr_requests_user_status
  ON lidarr_requests(user_plex_id, status, priority_order, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_lidarr_requests_user_active
  ON lidarr_requests(user_plex_id, artist_name, album_title, status)
  WHERE status IN ('queued', 'processing');
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

  const prefCols = db.prepare('PRAGMA table_info(user_preferences)').all().map((c) => c.name);
  if (!prefCols.includes('smart_config'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN smart_config TEXT NOT NULL DEFAULT 'null'");

  const masterCols = db.prepare('PRAGMA table_info(master_tracks)').all().map((c) => c.name);
  if (!masterCols.includes('rating_count'))
    db.exec('ALTER TABLE master_tracks ADD COLUMN rating_count INTEGER NOT NULL DEFAULT 0');
  if (!masterCols.includes('view_count'))
    db.exec('ALTER TABLE master_tracks ADD COLUMN view_count INTEGER NOT NULL DEFAULT 0');

  // system_job_runs has no migrations needed — created fresh via SCHEMA above

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

// ─── Preset definitions ───────────────────────────────────────────────────────
// Single source of truth — imported by wizard.js, webhooks.js, and settings pages.

export const PRESET_VALUES = {
  cautious:   { skipThresholdSeconds: 20, completionThresholdSeconds: 20, skipWeight: -0.5, belterWeight: 0.5,  artistSkipRank: 1, artistBelterRank: 9, songSkipLimit: 3 },
  measured:   { skipThresholdSeconds: 30, completionThresholdSeconds: 30, skipWeight: -1,   belterWeight: 1,    artistSkipRank: 2, artistBelterRank: 8, songSkipLimit: 2 },
  aggressive: { skipThresholdSeconds: 40, completionThresholdSeconds: 40, skipWeight: -1.5, belterWeight: 1.5,  artistSkipRank: 3, artistBelterRank: 7, songSkipLimit: 1 },
};

// Resolve the effective smart config for a user.
// Priority: user's chosen preset → admin's default preset → admin's custom values.
export function resolveUserSmartConfig(db, config, userId) {
  if (userId) {
    const prefs = getUserPreferences(db, userId);
    const userPreset = prefs?.smartConfig?.preset;
    if (userPreset && PRESET_VALUES[userPreset]) return { ...PRESET_VALUES[userPreset] };
  }
  const adminDefaultPreset = config?.smartPlaylist?.defaultPreset;
  if (adminDefaultPreset && PRESET_VALUES[adminDefaultPreset]) return { ...PRESET_VALUES[adminDefaultPreset] };
  return config?.smartPlaylist || {};
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
  if (trackDurationMs > 0 && listenedMs >= trackDurationMs * 0.5) return 'decent';
  return 'half-decent';
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
    // Once excluded, a play cannot redeem the track — only manual inclusion can. While still active,
    // a play decrements consecutive skips by 1 (rather than resetting) so recovery is gradual.
    const alreadyExcluded = existing.excluded_from_smart === 1 && existing.manually_included !== 1;
    const newConsecutive = isSkip
      ? existing.consecutive_skips + 1
      : alreadyExcluded ? existing.consecutive_skips : Math.max(0, existing.consecutive_skips - 1);
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

// Update only the tier/tier_weight on an existing track_stats row (no counter changes).
// Used when a play event is retroactively corrected to a longer listened duration.
// Returns { tier, scoreDelta } where scoreDelta is the change to apply to artist ranking_score.
export function updateTrackTierOnly(db, { userPlexId, plexRatingKey, listenedMs, trackDurationMs, smartConfig }) {
  const tier = classifyTier(listenedMs, trackDurationMs, smartConfig);
  const weight = _tierWeight(tier, smartConfig);
  const existing = db.prepare(
    'SELECT tier, tier_weight FROM track_stats WHERE plex_rating_key = ? AND user_plex_id = ?',
  ).get(plexRatingKey, userPlexId);
  if (!existing) return { tier, scoreDelta: 0 };
  const prevTier = existing.tier || 'curatorr';
  const prevWeight = existing.tier_weight ?? 0;
  const scoreDelta = tier !== prevTier ? weight - prevWeight : 0;
  if (tier !== prevTier) {
    db.prepare(
      'UPDATE track_stats SET tier = ?, tier_weight = ?, updated_at = ? WHERE plex_rating_key = ? AND user_plex_id = ?',
    ).run(tier, weight, Date.now(), plexRatingKey, userPlexId);
  }
  return { tier, scoreDelta };
}

// Adjust an artist's ranking_score by a delta without touching any counters.
// Used alongside updateTrackTierOnly when a tier change requires propagating the score delta.
export function adjustArtistScore(db, { userPlexId, artistName, scoreDelta }) {
  if (!scoreDelta) return;
  const existing = db.prepare(
    'SELECT ranking_score FROM artist_stats WHERE artist_name = ? AND user_plex_id = ?',
  ).get(artistName, userPlexId);
  if (!existing) return;
  const newScore = Math.min(10, Math.max(0, (existing.ranking_score ?? 5.0) + scoreDelta));
  db.prepare(
    'UPDATE artist_stats SET ranking_score = ?, updated_at = ? WHERE artist_name = ? AND user_plex_id = ?',
  ).run(newScore, Date.now(), artistName, userPlexId);
}

// ─── Query helpers ────────────────────────────────────────────────────────────

export function getExcludedTrackKeys(db, userPlexId) {
  return db.prepare(
    'SELECT plex_rating_key FROM track_stats WHERE user_plex_id = ? AND excluded_from_smart = 1 AND manually_included = 0',
  ).all(userPlexId).map((r) => r.plex_rating_key);
}

export function getManuallyIncludedArtists(db, userPlexId) {
  return db.prepare(
    'SELECT artist_name FROM artist_stats WHERE user_plex_id = ? AND manually_included = 1',
  ).all(userPlexId).map((r) => r.artist_name);
}

export function getResolvedUserArtistFilters(db, config, userPlexId) {
  const prefs = userPlexId ? getUserPreferences(db, userPlexId) : { likedArtists: [], ignoredArtists: [] };
  let mustIncludeArtists = dedupeMasterArtistNames(Array.isArray(prefs?.likedArtists) ? prefs.likedArtists : []);
  let neverIncludeArtists = dedupeMasterArtistNames(Array.isArray(prefs?.ignoredArtists) ? prefs.ignoredArtists : []);

  if (!mustIncludeArtists.length && !neverIncludeArtists.length) {
    mustIncludeArtists = dedupeMasterArtistNames(Array.isArray(config?.filters?.mustIncludeArtists) ? config.filters.mustIncludeArtists : []);
    neverIncludeArtists = dedupeMasterArtistNames(Array.isArray(config?.filters?.neverIncludeArtists) ? config.filters.neverIncludeArtists : []);
  }

  const curatorMustIncludeArtists = userPlexId ? getManuallyIncludedArtists(db, userPlexId) : [];

  return {
    mustIncludeArtists: dedupeMasterArtistNames([...mustIncludeArtists, ...curatorMustIncludeArtists]),
    neverIncludeArtists: dedupeMasterArtistNames([...neverIncludeArtists]),
  };
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
           manually_included,
           ranking_score,
           last_played_at
    FROM artist_stats
    ${filter}
    ORDER BY play_count DESC
    LIMIT ?
  `).all(...params);
}

export function getArtistRankSnapshot(db, userPlexId, artistName) {
  const row = db.prepare(`
    SELECT artist_name,
           play_count,
           skip_count,
           ranking_score,
           consecutive_skips,
           last_played_at,
           last_skipped_at,
           updated_at
    FROM artist_stats
    WHERE user_plex_id = ? AND artist_name = ?
    LIMIT 1
  `).get(userPlexId, artistName);
  if (!row) return null;
  return {
    artistName: row.artist_name,
    playCount: Number(row.play_count || 0),
    skipCount: Number(row.skip_count || 0),
    rankingScore: Number(row.ranking_score || 0),
    consecutiveSkips: Number(row.consecutive_skips || 0),
    lastPlayedAt: row.last_played_at,
    lastSkippedAt: row.last_skipped_at,
    updatedAt: row.updated_at,
  };
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
           tier,
           last_played_at
    FROM track_stats
    ${filter}
    ORDER BY play_count DESC
    LIMIT ?
  `).all(...params);
}

export function getRecentHistory(db, userPlexId, limit = 50, offset = 0) {
  const filter = userPlexId ? 'WHERE pe.user_plex_id = ?' : 'WHERE 1=1';
  const params = userPlexId ? [userPlexId, limit, offset] : [limit, offset];
  return db.prepare(`
    SELECT pe.id, pe.user_plex_id, pe.plex_rating_key, pe.track_title, pe.artist_name, pe.album_name,
           pe.started_at, pe.ended_at, pe.duration_ms, pe.track_duration_ms,
           pe.is_skip, pe.event_source,
           COALESCE(ts.tier, 'curatorr') AS current_tier,
           COALESCE(ts.excluded_from_smart, 0) AS current_excluded,
           COALESCE(ts.manually_included, 0) AS current_force_included
    FROM play_events pe
    LEFT JOIN track_stats ts
      ON ts.user_plex_id = pe.user_plex_id
     AND ts.plex_rating_key = pe.plex_rating_key
    ${filter}
    ORDER BY pe.started_at DESC
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
  if (!row) return { likedGenres: [], ignoredGenres: [], likedArtists: [], ignoredArtists: [], userWizardCompleted: false, smartConfig: null };
  return {
    likedGenres: JSON.parse(row.liked_genres || '[]'),
    ignoredGenres: JSON.parse(row.ignored_genres || '[]'),
    likedArtists: JSON.parse(row.liked_artists || '[]'),
    ignoredArtists: JSON.parse(row.ignored_artists || '[]'),
    userWizardCompleted: Boolean(row.user_wizard_completed),
    smartConfig: JSON.parse(row.smart_config || 'null'),
  };
}

export function saveUserPreferences(db, userPlexId, { likedGenres = [], ignoredGenres = [], likedArtists = [], ignoredArtists = [], userWizardCompleted = false, smartConfig = undefined }) {
  const existing = getUserPreferences(db, userPlexId);
  const resolvedSmartConfig = smartConfig !== undefined ? smartConfig : existing.smartConfig;
  db.prepare(`
    INSERT INTO user_preferences (user_plex_id, liked_genres, ignored_genres, liked_artists, ignored_artists, user_wizard_completed, smart_config, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_plex_id) DO UPDATE SET
      liked_genres = excluded.liked_genres,
      ignored_genres = excluded.ignored_genres,
      liked_artists = excluded.liked_artists,
      ignored_artists = excluded.ignored_artists,
      user_wizard_completed = excluded.user_wizard_completed,
      smart_config = excluded.smart_config,
      updated_at = excluded.updated_at
  `).run(userPlexId, JSON.stringify(likedGenres), JSON.stringify(ignoredGenres), JSON.stringify(likedArtists), JSON.stringify(ignoredArtists), userWizardCompleted ? 1 : 0, JSON.stringify(resolvedSmartConfig ?? null), Date.now());
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

export function getPlaylistJob(db, userPlexId) {
  return db.prepare('SELECT * FROM playlist_jobs WHERE user_plex_id = ?').get(userPlexId) || null;
}

export function savePlaylistJob(db, userPlexId, job = {}) {
  const now = Date.now();
  db.prepare(`
    INSERT INTO playlist_jobs (
      user_plex_id, status, trigger, message, playlist_id, playlist_title,
      track_count, error_message, started_at, completed_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_plex_id) DO UPDATE SET
      status = excluded.status,
      trigger = excluded.trigger,
      message = excluded.message,
      playlist_id = excluded.playlist_id,
      playlist_title = excluded.playlist_title,
      track_count = excluded.track_count,
      error_message = excluded.error_message,
      started_at = excluded.started_at,
      completed_at = excluded.completed_at,
      updated_at = excluded.updated_at
  `).run(
    userPlexId,
    String(job.status || 'queued'),
    String(job.trigger || 'wizard'),
    String(job.message || ''),
    String(job.playlistId || ''),
    String(job.playlistTitle || ''),
    Number(job.trackCount || 0),
    String(job.errorMessage || ''),
    job.startedAt ?? null,
    job.completedAt ?? null,
    Number(job.updatedAt || now),
  );
}

export function clearPlaylistJob(db, userPlexId) {
  db.prepare(`
    INSERT INTO playlist_jobs (
      user_plex_id, status, trigger, message, playlist_id, playlist_title,
      track_count, error_message, started_at, completed_at, updated_at
    ) VALUES (?, 'idle', '', '', '', '', 0, '', NULL, NULL, ?)
    ON CONFLICT(user_plex_id) DO UPDATE SET
      status = 'idle',
      trigger = '',
      message = '',
      playlist_id = '',
      playlist_title = '',
      track_count = 0,
      error_message = '',
      started_at = NULL,
      completed_at = NULL,
      updated_at = excluded.updated_at
  `).run(userPlexId, Date.now());
}

// ─── Master track cache ───────────────────────────────────────────────────────

export function refreshMasterTracks(db, tracks) {
  const upsert = db.prepare(`
    INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name, genres, library_key, rating_count, view_count, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(rating_key) DO UPDATE SET
      artist_name = excluded.artist_name, track_title = excluded.track_title,
      album_name = excluded.album_name, genres = excluded.genres,
      library_key = excluded.library_key, rating_count = excluded.rating_count,
      view_count = excluded.view_count, updated_at = excluded.updated_at
  `);
  const run = db.transaction((rows) => {
    for (const r of rows)
      upsert.run(r.ratingKey, r.artistName, r.trackTitle, r.albumName, JSON.stringify(r.genres || []), r.libraryKey, r.ratingCount ?? 0, r.viewCount ?? 0, Date.now());
  });
  run(tracks);
}

export function getMasterTracks(db) {
  return db.prepare('SELECT * FROM master_tracks').all().map((r) => ({
    ratingKey: r.rating_key, artistName: r.artist_name, trackTitle: r.track_title,
    albumName: r.album_name, genres: JSON.parse(r.genres || '[]'), libraryKey: r.library_key,
    ratingCount: r.rating_count, viewCount: r.view_count,
  }));
}

export function getMasterTrackCount(db) {
  return db.prepare('SELECT COUNT(*) as n FROM master_tracks').get().n;
}

// ─── System job runs ──────────────────────────────────────────────────────────

export function getSystemJobRun(db, jobId) {
  return db.prepare('SELECT * FROM system_job_runs WHERE job_id = ?').get(jobId) || null;
}

export function setSystemJobRun(db, jobId, { status, lastRunAt, message }) {
  const now = Date.now();
  db.prepare(`
    INSERT INTO system_job_runs (job_id, status, last_run_at, message, updated_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(job_id) DO UPDATE SET
      status = excluded.status, last_run_at = excluded.last_run_at,
      message = excluded.message, updated_at = excluded.updated_at
  `).run(jobId, String(status || 'idle'), lastRunAt ?? null, String(message || ''), now);
}

export function getAllSystemJobRuns(db) {
  return db.prepare('SELECT * FROM system_job_runs').all();
}

// ─── Playlist tracks (crescive / curative local state) ────────────────────────

export function getPlaylistTracks(db, userId, playlistKey) {
  return db.prepare('SELECT rating_key, artist_name FROM playlist_tracks WHERE user_plex_id = ? AND playlist_key = ?')
    .all(userId, playlistKey)
    .map((r) => ({ ratingKey: r.rating_key, artistName: r.artist_name }));
}

export function setPlaylistTracks(db, userId, playlistKey, tracks) {
  const del = db.prepare('DELETE FROM playlist_tracks WHERE user_plex_id = ? AND playlist_key = ?');
  const ins = db.prepare('INSERT OR IGNORE INTO playlist_tracks (playlist_key, user_plex_id, rating_key, artist_name, added_at) VALUES (?, ?, ?, ?, ?)');
  const now = Date.now();
  db.transaction(() => {
    del.run(userId, playlistKey);
    for (const t of tracks) ins.run(playlistKey, userId, t.ratingKey, t.artistName || '', now);
  })();
}

export function addPlaylistTracks(db, userId, playlistKey, tracks) {
  const ins = db.prepare('INSERT OR IGNORE INTO playlist_tracks (playlist_key, user_plex_id, rating_key, artist_name, added_at) VALUES (?, ?, ?, ?, ?)');
  const now = Date.now();
  db.transaction(() => {
    for (const t of tracks) ins.run(playlistKey, userId, t.ratingKey, t.artistName || '', now);
  })();
}

export function removePlaylistTracks(db, userId, playlistKey, ratingKeys) {
  if (!ratingKeys.length) return;
  const del = db.prepare('DELETE FROM playlist_tracks WHERE user_plex_id = ? AND playlist_key = ? AND rating_key = ?');
  db.transaction(() => {
    for (const k of ratingKeys) del.run(userId, playlistKey, k);
  })();
}

// ─── Playlist artist state (fired thresholds) ─────────────────────────────────

export function getPlaylistArtistState(db, userId, playlistKey, artistName) {
  const row = db.prepare('SELECT thresholds_fired FROM playlist_artist_state WHERE playlist_key = ? AND user_plex_id = ? AND artist_name = ?')
    .get(playlistKey, userId, artistName);
  if (!row) return [];
  try { return JSON.parse(row.thresholds_fired || '[]'); } catch { return []; }
}

export function setPlaylistArtistState(db, userId, playlistKey, artistName, thresholdsFired) {
  db.prepare(`
    INSERT INTO playlist_artist_state (playlist_key, user_plex_id, artist_name, thresholds_fired, updated_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(playlist_key, user_plex_id, artist_name) DO UPDATE SET
      thresholds_fired = excluded.thresholds_fired, updated_at = excluded.updated_at
  `).run(playlistKey, userId, artistName, JSON.stringify(thresholdsFired || []), Date.now());
}

export function clearPlaylistState(db, userId, playlistKey) {
  db.prepare('DELETE FROM playlist_tracks WHERE user_plex_id = ? AND playlist_key = ?').run(userId, playlistKey);
  db.prepare('DELETE FROM playlist_artist_state WHERE user_plex_id = ? AND playlist_key = ?').run(userId, playlistKey);
}

export function getGenresFromMaster(db) {
  const rows = db.prepare('SELECT DISTINCT value FROM master_tracks, json_each(master_tracks.genres) ORDER BY value').all();
  return rows.map((r) => r.value).filter(Boolean);
}

export function cleanMasterArtistName(value) {
  let name = String(value || '').trim();
  if (!name) return '';
  name = name.replace(/\s+(feat\.?|featuring|ft\.?|with)\s+.+$/i, '').trim();
  name = name.replace(/\s+/g, ' ').trim();
  return name;
}

export function shouldDropMasterArtist(value) {
  const normalized = String(cleanMasterArtistName(value) || '').trim().toLowerCase();
  if (!normalized) return true;
  return [
    '[dialogue]',
    'dialogue',
    '[unknown]',
    'unknown',
    'va',
    'v/a',
    'various artists',
  ].includes(normalized);
}

export function buildMasterArtistKey(value) {
  return cleanMasterArtistName(value)
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .filter((token) => token !== 'and' && token !== 'the')
    .map((token) => (token.length > 3 && token.endsWith('s') ? token.slice(0, -1) : token))
    .join(' ');
}

function pickPreferredMasterArtistName(current, candidate) {
  const currentName = cleanMasterArtistName(current);
  const candidateName = cleanMasterArtistName(candidate);
  if (!currentName) return candidateName;
  if (!candidateName) return currentName;
  if (candidateName.length < currentName.length) return candidateName;
  if (candidateName.length === currentName.length && candidateName.localeCompare(currentName) < 0) return candidateName;
  return currentName;
}

export function dedupeMasterArtistNames(values = []) {
  const artistMap = new Map();
  values.forEach((entry) => {
    const cleaned = cleanMasterArtistName(entry);
    if (shouldDropMasterArtist(cleaned)) return;
    const key = buildMasterArtistKey(cleaned);
    if (!key) return;
    const preferred = pickPreferredMasterArtistName(artistMap.get(key), cleaned);
    artistMap.set(key, preferred);
  });
  return [...artistMap.values()].sort((a, b) => a.localeCompare(b));
}

export function getArtistsFromMaster(db, filterGenres = []) {
  if (!filterGenres.length) {
    const rows = db.prepare('SELECT DISTINCT artist_name FROM master_tracks ORDER BY artist_name').all();
    return dedupeMasterArtistNames(rows.map((r) => r.artist_name));
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
  return dedupeMasterArtistNames(rows.map((r) => r.artist_name));
}

function _safeParseJson(value, fallback) {
  try { return JSON.parse(value); } catch (err) { return fallback; }
}

function _normalizeSuggestedArtistRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    userPlexId: row.user_plex_id,
    artistName: row.artist_name,
    source: row.source,
    similarityScore: Number(row.similarity_score || 0),
    behaviorScore: Number(row.behavior_score || 0),
    editorialScore: Number(row.editorial_score || 0),
    totalScore: Number(row.total_score || 0),
    status: row.status,
    reason: _safeParseJson(row.reason_json || '{}', {}),
    lidarrArtistId: row.lidarr_artist_id,
    firstSuggestedAt: row.first_suggested_at,
    lastEvaluatedAt: row.last_evaluated_at,
    acceptedAt: row.accepted_at,
    dismissedAt: row.dismissed_at,
  };
}

function _normalizeSuggestedTrackRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    userPlexId: row.user_plex_id,
    suggestionKey: row.suggestion_key,
    ratingKey: row.rating_key,
    artistName: row.artist_name,
    trackTitle: row.track_title,
    albumName: row.album_name,
    source: row.source,
    totalScore: Number(row.total_score || 0),
    reason: _safeParseJson(row.reason_json || '{}', {}),
    createdAt: row.created_at,
    expiresAt: row.expires_at,
  };
}

export function listSuggestedArtists(db, userPlexId, { status = '', limit = 25 } = {}) {
  const clauses = ['user_plex_id = ?'];
  const params = [userPlexId];
  if (status) {
    clauses.push('status = ?');
    params.push(status);
  }
  params.push(Math.max(1, Number(limit) || 25));
  return db.prepare(`
    SELECT * FROM suggested_artists
    WHERE ${clauses.join(' AND ')}
    ORDER BY total_score DESC, artist_name ASC
    LIMIT ?
  `).all(...params).map(_normalizeSuggestedArtistRow);
}

export function listUsersWithSuggestedArtists(db) {
  return db.prepare(`
    SELECT DISTINCT user_plex_id FROM suggested_artists WHERE status = 'suggested'
  `).all().map((row) => row.user_plex_id);
}

export function getSuggestedArtist(db, userPlexId, artistName) {
  const row = db.prepare(`
    SELECT * FROM suggested_artists
    WHERE user_plex_id = ? AND artist_name = ?
  `).get(userPlexId, artistName);
  return _normalizeSuggestedArtistRow(row);
}

export function upsertSuggestedArtist(db, userPlexId, artist) {
  const now = Date.now();
  const artistName = String(artist?.artistName || '').trim();
  if (!artistName) throw new Error('artistName is required');
  db.prepare(`
    INSERT INTO suggested_artists (
      user_plex_id, artist_name, source,
      similarity_score, behavior_score, editorial_score, total_score,
      status, reason_json, lidarr_artist_id,
      first_suggested_at, last_evaluated_at, accepted_at, dismissed_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_plex_id, artist_name) DO UPDATE SET
      source = excluded.source,
      similarity_score = excluded.similarity_score,
      behavior_score = excluded.behavior_score,
      editorial_score = excluded.editorial_score,
      total_score = excluded.total_score,
      status = excluded.status,
      reason_json = excluded.reason_json,
      lidarr_artist_id = COALESCE(excluded.lidarr_artist_id, suggested_artists.lidarr_artist_id),
      last_evaluated_at = excluded.last_evaluated_at,
      accepted_at = COALESCE(excluded.accepted_at, suggested_artists.accepted_at),
      dismissed_at = excluded.dismissed_at
  `).run(
    userPlexId,
    artistName,
    String(artist?.source || 'curatorr'),
    Number(artist?.similarityScore || 0),
    Number(artist?.behaviorScore || 0),
    Number(artist?.editorialScore || 0),
    Number(artist?.totalScore || 0),
    String(artist?.status || 'suggested'),
    JSON.stringify(artist?.reason || {}),
    artist?.lidarrArtistId ?? null,
    Number(artist?.firstSuggestedAt || now),
    Number(artist?.lastEvaluatedAt || now),
    artist?.acceptedAt ?? null,
    artist?.dismissedAt ?? null,
  );
  return db.prepare(`
    SELECT * FROM suggested_artists
    WHERE user_plex_id = ? AND artist_name = ?
  `).get(userPlexId, artistName);
}

export function setSuggestedArtistStatus(db, userPlexId, artistName, status, extra = {}) {
  const existing = db.prepare(`
    SELECT * FROM suggested_artists
    WHERE user_plex_id = ? AND artist_name = ?
  `).get(userPlexId, artistName);
  if (!existing) return null;

  const now = Date.now();
  const nextReason = extra.reason ? JSON.stringify(extra.reason) : existing.reason_json;
  const nextStatus = String(status || existing.status || 'suggested');
  const acceptedAt = Object.prototype.hasOwnProperty.call(extra, 'acceptedAt')
    ? extra.acceptedAt
    : (nextStatus === 'queued_for_lidarr' || nextStatus === 'added_to_lidarr' ? (existing.accepted_at || now) : existing.accepted_at);
  const dismissedAt = Object.prototype.hasOwnProperty.call(extra, 'dismissedAt')
    ? extra.dismissedAt
    : (nextStatus === 'dismissed' ? now : existing.dismissed_at);

  db.prepare(`
    UPDATE suggested_artists SET
      status = ?,
      reason_json = ?,
      lidarr_artist_id = COALESCE(?, lidarr_artist_id),
      accepted_at = ?,
      dismissed_at = ?,
      last_evaluated_at = ?
    WHERE user_plex_id = ? AND artist_name = ?
  `).run(
    nextStatus,
    nextReason,
    extra.lidarrArtistId ?? null,
    acceptedAt ?? null,
    dismissedAt ?? null,
    now,
    userPlexId,
    artistName,
  );

  return getSuggestedArtist(db, userPlexId, artistName);
}

export function listSuggestedAlbums(db, userPlexId, { status = '', limit = 25 } = {}) {
  const clauses = ['user_plex_id = ?'];
  const params = [userPlexId];
  if (status) {
    clauses.push('status = ?');
    params.push(status);
  }
  params.push(Math.max(1, Number(limit) || 25));
  return db.prepare(`
    SELECT * FROM suggested_albums
    WHERE ${clauses.join(' AND ')}
    ORDER BY rank_score DESC, artist_name ASC, album_title ASC
    LIMIT ?
  `).all(...params).map((row) => ({
    id: row.id,
    userPlexId: row.user_plex_id,
    artistName: row.artist_name,
    albumTitle: row.album_title,
    albumType: row.album_type,
    releaseDate: row.release_date,
    selectionReason: row.selection_reason,
    rankScore: Number(row.rank_score || 0),
    status: row.status,
    lidarrAlbumId: row.lidarr_album_id,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }));
}

export function upsertSuggestedAlbum(db, userPlexId, album) {
  const now = Date.now();
  const artistName = String(album?.artistName || '').trim();
  const albumTitle = String(album?.albumTitle || '').trim();
  if (!artistName || !albumTitle) throw new Error('artistName and albumTitle are required');
  db.prepare(`
    INSERT INTO suggested_albums (
      user_plex_id, artist_name, album_title, album_type,
      release_date, selection_reason, rank_score, status,
      lidarr_album_id, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_plex_id, artist_name, album_title) DO UPDATE SET
      album_type = excluded.album_type,
      release_date = excluded.release_date,
      selection_reason = excluded.selection_reason,
      rank_score = excluded.rank_score,
      status = excluded.status,
      lidarr_album_id = COALESCE(excluded.lidarr_album_id, suggested_albums.lidarr_album_id),
      updated_at = excluded.updated_at
  `).run(
    userPlexId,
    artistName,
    albumTitle,
    String(album?.albumType || ''),
    String(album?.releaseDate || ''),
    String(album?.selectionReason || ''),
    Number(album?.rankScore || 0),
    String(album?.status || 'candidate'),
    album?.lidarrAlbumId ?? null,
    Number(album?.createdAt || now),
    Number(album?.updatedAt || now),
  );
}

export function listSuggestedTracks(db, userPlexId, { limit = 50 } = {}) {
  return db.prepare(`
    SELECT * FROM suggested_tracks
    WHERE user_plex_id = ?
    ORDER BY total_score DESC, created_at DESC
    LIMIT ?
  `).all(userPlexId, Math.max(1, Number(limit) || 50)).map(_normalizeSuggestedTrackRow);
}

export function upsertSuggestedTrack(db, userPlexId, track) {
  const now = Date.now();
  const suggestionKey = String(track?.suggestionKey || track?.ratingKey || '').trim();
  if (!suggestionKey) throw new Error('suggestionKey or ratingKey is required');
  db.prepare(`
    INSERT INTO suggested_tracks (
      user_plex_id, suggestion_key, rating_key, artist_name,
      track_title, album_name, source, total_score, reason_json,
      created_at, expires_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_plex_id, suggestion_key) DO UPDATE SET
      rating_key = excluded.rating_key,
      artist_name = excluded.artist_name,
      track_title = excluded.track_title,
      album_name = excluded.album_name,
      source = excluded.source,
      total_score = excluded.total_score,
      reason_json = excluded.reason_json,
      expires_at = excluded.expires_at
  `).run(
    userPlexId,
    suggestionKey,
    String(track?.ratingKey || ''),
    String(track?.artistName || ''),
    String(track?.trackTitle || ''),
    String(track?.albumName || ''),
    String(track?.source || 'curatorr'),
    Number(track?.totalScore || 0),
    JSON.stringify(track?.reason || {}),
    Number(track?.createdAt || now),
    track?.expiresAt ?? null,
  );
}

export function listUserGeneratedPlaylists(db, userPlexId, { activeOnly = true } = {}) {
  const clauses = ['user_plex_id = ?'];
  const params = [userPlexId];
  if (activeOnly) clauses.push('active = 1');
  return db.prepare(`
    SELECT * FROM user_generated_playlists
    WHERE ${clauses.join(' AND ')}
    ORDER BY playlist_type ASC, playlist_title ASC
  `).all(...params).map((row) => ({
    id: row.id,
    userPlexId: row.user_plex_id,
    playlistType: row.playlist_type,
    playlistKey: row.playlist_key,
    plexPlaylistId: row.plex_playlist_id,
    playlistTitle: row.playlist_title,
    algorithmVersion: row.algorithm_version,
    lastBuiltAt: row.last_built_at,
    lastSyncedAt: row.last_synced_at,
    trackCount: Number(row.track_count || 0),
    active: Boolean(row.active),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }));
}

export function listAllGeneratedPlaylists(db) {
  return db.prepare('SELECT user_plex_id, playlist_key, playlist_type, plex_playlist_id, playlist_title FROM user_generated_playlists WHERE plex_playlist_id IS NOT NULL AND plex_playlist_id != \'\'').all()
    .map((r) => ({ userPlexId: r.user_plex_id, playlistKey: r.playlist_key, playlistType: r.playlist_type, plexPlaylistId: r.plex_playlist_id, playlistTitle: r.playlist_title }));
}

export function clearGeneratedPlaylistPlexId(db, userPlexId, playlistKey) {
  db.prepare('UPDATE user_generated_playlists SET plex_playlist_id = \'\', updated_at = ? WHERE user_plex_id = ? AND playlist_key = ?')
    .run(Date.now(), userPlexId, playlistKey);
}

export function saveUserGeneratedPlaylist(db, userPlexId, playlist) {
  const now = Date.now();
  const playlistKey = String(playlist?.playlistKey || '').trim();
  if (!playlistKey) throw new Error('playlistKey is required');
  db.prepare(`
    INSERT INTO user_generated_playlists (
      user_plex_id, playlist_type, playlist_key, plex_playlist_id,
      playlist_title, algorithm_version, last_built_at, last_synced_at,
      track_count, active, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_plex_id, playlist_key) DO UPDATE SET
      playlist_type = excluded.playlist_type,
      plex_playlist_id = excluded.plex_playlist_id,
      playlist_title = excluded.playlist_title,
      algorithm_version = excluded.algorithm_version,
      last_built_at = COALESCE(excluded.last_built_at, user_generated_playlists.last_built_at),
      last_synced_at = COALESCE(excluded.last_synced_at, user_generated_playlists.last_synced_at),
      track_count = excluded.track_count,
      active = excluded.active,
      updated_at = excluded.updated_at
  `).run(
    userPlexId,
    String(playlist?.playlistType || 'curatorred'),
    playlistKey,
    String(playlist?.plexPlaylistId || ''),
    String(playlist?.playlistTitle || ''),
    String(playlist?.algorithmVersion || 'phase2a'),
    playlist?.lastBuiltAt ?? null,
    playlist?.lastSyncedAt ?? null,
    Number(playlist?.trackCount || 0),
    playlist?.active === false ? 0 : 1,
    Number(playlist?.createdAt || now),
    Number(playlist?.updatedAt || now),
  );
}

export function getLidarrArtistProgress(db, userPlexId, artistName) {
  const row = db.prepare(`
    SELECT * FROM lidarr_artist_progress
    WHERE user_plex_id = ? AND artist_name = ?
  `).get(userPlexId, artistName);
  if (!row) return null;
  return {
    id: row.id,
    userPlexId: row.user_plex_id,
    artistName: row.artist_name,
    lidarrArtistId: row.lidarr_artist_id,
    currentStage: row.current_stage,
    albumsAddedCount: Number(row.albums_added_count || 0),
    lastAlbumAddedAt: row.last_album_added_at,
    nextReviewAt: row.next_review_at,
    highestObservedRank: Number(row.highest_observed_rank || 0),
    lastManualSearchAt: row.last_manual_search_at,
    lastManualSearchStatus: row.last_manual_search_status || '',
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export function listLidarrArtistProgress(db, userPlexId, { limit = 25 } = {}) {
  return db.prepare(`
    SELECT * FROM lidarr_artist_progress
    WHERE user_plex_id = ?
    ORDER BY updated_at DESC, artist_name ASC
    LIMIT ?
  `).all(userPlexId, Math.max(1, Number(limit) || 25)).map((row) => ({
    id: row.id,
    userPlexId: row.user_plex_id,
    artistName: row.artist_name,
    lidarrArtistId: row.lidarr_artist_id,
    currentStage: row.current_stage,
    albumsAddedCount: Number(row.albums_added_count || 0),
    lastAlbumAddedAt: row.last_album_added_at,
    nextReviewAt: row.next_review_at,
    highestObservedRank: Number(row.highest_observed_rank || 0),
    lastManualSearchAt: row.last_manual_search_at,
    lastManualSearchStatus: row.last_manual_search_status || '',
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }));
}

export function listDueLidarrArtistReviews(db, { now = Date.now(), limit = 25 } = {}) {
  return db.prepare(`
    SELECT * FROM lidarr_artist_progress
    WHERE current_stage != 'catalog_complete'
      AND (next_review_at IS NULL OR next_review_at <= ?)
    ORDER BY COALESCE(next_review_at, 0) ASC, updated_at ASC, artist_name ASC
    LIMIT ?
  `).all(Number(now || Date.now()), Math.max(1, Number(limit) || 25)).map((row) => ({
    id: row.id,
    userPlexId: row.user_plex_id,
    artistName: row.artist_name,
    lidarrArtistId: row.lidarr_artist_id,
    currentStage: row.current_stage,
    albumsAddedCount: Number(row.albums_added_count || 0),
    lastAlbumAddedAt: row.last_album_added_at,
    nextReviewAt: row.next_review_at,
    highestObservedRank: Number(row.highest_observed_rank || 0),
    lastManualSearchAt: row.last_manual_search_at,
    lastManualSearchStatus: row.last_manual_search_status || '',
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }));
}

export function saveLidarrArtistProgress(db, userPlexId, artist) {
  const now = Date.now();
  const artistName = String(artist?.artistName || '').trim();
  if (!artistName) throw new Error('artistName is required');
  db.prepare(`
    INSERT INTO lidarr_artist_progress (
      user_plex_id, artist_name, lidarr_artist_id, current_stage,
      albums_added_count, last_album_added_at, next_review_at,
      highest_observed_rank, last_manual_search_at, last_manual_search_status,
      created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_plex_id, artist_name) DO UPDATE SET
      lidarr_artist_id = COALESCE(excluded.lidarr_artist_id, lidarr_artist_progress.lidarr_artist_id),
      current_stage = excluded.current_stage,
      albums_added_count = excluded.albums_added_count,
      last_album_added_at = COALESCE(excluded.last_album_added_at, lidarr_artist_progress.last_album_added_at),
      next_review_at = excluded.next_review_at,
      highest_observed_rank = excluded.highest_observed_rank,
      last_manual_search_at = excluded.last_manual_search_at,
      last_manual_search_status = excluded.last_manual_search_status,
      updated_at = excluded.updated_at
  `).run(
    userPlexId,
    artistName,
    artist?.lidarrArtistId ?? null,
    String(artist?.currentStage || 'suggested'),
    Number(artist?.albumsAddedCount || 0),
    artist?.lastAlbumAddedAt ?? null,
    artist?.nextReviewAt ?? null,
    Number(artist?.highestObservedRank || 0),
    artist?.lastManualSearchAt ?? null,
    String(artist?.lastManualSearchStatus || ''),
    Number(artist?.createdAt || now),
    Number(artist?.updatedAt || now),
  );
}

function _weekStartTimestamp(timestamp = Date.now()) {
  const date = new Date(Number(timestamp || Date.now()));
  const day = date.getUTCDay();
  const diff = (day + 6) % 7;
  date.setUTCHours(0, 0, 0, 0);
  date.setUTCDate(date.getUTCDate() - diff);
  return date.getTime();
}

export function getLidarrUsageSummary(db, userPlexId, periodStart = _weekStartTimestamp()) {
  const rows = db.prepare(`
    SELECT usage_key, SUM(amount) AS total
    FROM lidarr_usage
    WHERE user_plex_id = ? AND period_start = ?
    GROUP BY usage_key
  `).all(userPlexId, Number(periodStart || 0));
  return rows.reduce((acc, row) => {
    acc[String(row.usage_key || '')] = Number(row.total || 0);
    return acc;
  }, {});
}

export function recordLidarrUsage(db, userPlexId, { roleName = 'user', usageKey = '', amount = 1, createdAt = Date.now() } = {}) {
  const key = String(usageKey || '').trim().toLowerCase();
  if (!key) throw new Error('usageKey is required');
  const value = Math.max(0, Number(amount || 0));
  if (!value) return;
  const now = Number(createdAt || Date.now());
  db.prepare(`
    INSERT INTO lidarr_usage (user_plex_id, role_name, usage_key, amount, period_start, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(
    userPlexId,
    String(roleName || 'user'),
    key,
    value,
    _weekStartTimestamp(now),
    now,
  );
}

export function getCurrentLidarrUsage(db, userPlexId) {
  const periodStart = _weekStartTimestamp();
  return {
    periodStart,
    usage: getLidarrUsageSummary(db, userPlexId, periodStart),
  };
}

function _normalizeLidarrRequestRow(row) {
  if (!row) return null;
  let detail = {};
  try {
    detail = row.detail_json ? JSON.parse(row.detail_json) : {};
  } catch (_err) {
    detail = {};
  }
  return {
    id: row.id,
    userPlexId: row.user_plex_id,
    sourceKind: row.source_kind || 'manual',
    requestKind: row.request_kind || 'artist_album',
    artistName: row.artist_name || '',
    albumTitle: row.album_title || '',
    foreignArtistId: row.foreign_artist_id || '',
    status: row.status || 'queued',
    priorityOrder: Number(row.priority_order || 0),
    lidarrArtistId: row.lidarr_artist_id,
    lidarrAlbumId: row.lidarr_album_id,
    detail,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    processedAt: row.processed_at,
  };
}

export function listLidarrRequests(db, userPlexId, { statuses = [], limit = 50 } = {}) {
  const where = ['user_plex_id = ?'];
  const params = [userPlexId];
  const wanted = Array.isArray(statuses)
    ? statuses.map((value) => String(value || '').trim()).filter(Boolean)
    : [];
  if (wanted.length) {
    where.push(`status IN (${wanted.map(() => '?').join(', ')})`);
    params.push(...wanted);
  }
  params.push(Math.max(1, Number(limit) || 50));
  return db.prepare(`
    SELECT *
    FROM lidarr_requests
    WHERE ${where.join(' AND ')}
    ORDER BY
      CASE
        WHEN status = 'queued' THEN 0
        WHEN status = 'processing' THEN 1
        WHEN status = 'completed' THEN 2
        WHEN status = 'failed' THEN 3
        ELSE 4
      END,
      priority_order ASC,
      updated_at DESC,
      created_at DESC
    LIMIT ?
  `).all(...params).map(_normalizeLidarrRequestRow);
}

export function getLidarrRequest(db, requestId, userPlexId = '') {
  const id = Number(requestId || 0);
  if (!id) return null;
  let row = null;
  if (userPlexId) {
    row = db.prepare('SELECT * FROM lidarr_requests WHERE id = ? AND user_plex_id = ?').get(id, userPlexId);
  } else {
    row = db.prepare('SELECT * FROM lidarr_requests WHERE id = ?').get(id);
  }
  return _normalizeLidarrRequestRow(row);
}

export function enqueueLidarrRequest(db, userPlexId, request = {}) {
  const artistName = String(request.artistName || '').trim();
  if (!artistName) throw new Error('artistName is required');
  const albumTitle = String(request.albumTitle || '').trim();
  const existing = db.prepare(`
    SELECT *
    FROM lidarr_requests
    WHERE user_plex_id = ? AND artist_name = ? AND album_title = ? AND status IN ('queued', 'processing')
    ORDER BY updated_at DESC
    LIMIT 1
  `).get(userPlexId, artistName, albumTitle);
  const now = Date.now();
  const nextDetail = request.detail && typeof request.detail === 'object' ? request.detail : {};
  if (existing) {
    db.prepare(`
      UPDATE lidarr_requests
      SET source_kind = ?,
          request_kind = ?,
          foreign_artist_id = ?,
          detail_json = ?,
          updated_at = ?
      WHERE id = ?
    `).run(
      String(request.sourceKind || existing.source_kind || 'manual'),
      String(request.requestKind || existing.request_kind || 'artist_album'),
      String(request.foreignArtistId || existing.foreign_artist_id || ''),
      JSON.stringify(nextDetail),
      now,
      existing.id,
    );
    return getLidarrRequest(db, existing.id, userPlexId);
  }
  const nextPriority = Number(
    db.prepare('SELECT COALESCE(MAX(priority_order), 0) AS max_priority FROM lidarr_requests WHERE user_plex_id = ? AND status = ?')
      .get(userPlexId, 'queued')?.max_priority || 0,
  ) + 1;
  const result = db.prepare(`
    INSERT INTO lidarr_requests (
      user_plex_id, source_kind, request_kind, artist_name, album_title,
      foreign_artist_id, status, priority_order, lidarr_artist_id, lidarr_album_id,
      detail_json, created_at, updated_at, processed_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    userPlexId,
    String(request.sourceKind || 'manual'),
    String(request.requestKind || 'artist_album'),
    artistName,
    albumTitle,
    String(request.foreignArtistId || ''),
    String(request.status || 'queued'),
    Number(request.priorityOrder || nextPriority),
    request.lidarrArtistId ?? null,
    request.lidarrAlbumId ?? null,
    JSON.stringify(nextDetail),
    Number(request.createdAt || now),
    Number(request.updatedAt || now),
    request.processedAt ?? null,
  );
  return getLidarrRequest(db, result.lastInsertRowid, userPlexId);
}

export function updateLidarrRequest(db, requestId, changes = {}, userPlexId = '') {
  const existing = getLidarrRequest(db, requestId, userPlexId);
  if (!existing) return null;
  const mergedDetail = Object.prototype.hasOwnProperty.call(changes, 'detail')
    ? (changes.detail && typeof changes.detail === 'object' ? changes.detail : {})
    : existing.detail;
  db.prepare(`
    UPDATE lidarr_requests
    SET source_kind = ?,
        request_kind = ?,
        artist_name = ?,
        album_title = ?,
        foreign_artist_id = ?,
        status = ?,
        priority_order = ?,
        lidarr_artist_id = ?,
        lidarr_album_id = ?,
        detail_json = ?,
        updated_at = ?,
        processed_at = ?
    WHERE id = ?
  `).run(
    String(changes.sourceKind || existing.sourceKind || 'manual'),
    String(changes.requestKind || existing.requestKind || 'artist_album'),
    String(changes.artistName || existing.artistName || ''),
    String(Object.prototype.hasOwnProperty.call(changes, 'albumTitle') ? changes.albumTitle : existing.albumTitle || ''),
    String(Object.prototype.hasOwnProperty.call(changes, 'foreignArtistId') ? changes.foreignArtistId : existing.foreignArtistId || ''),
    String(changes.status || existing.status || 'queued'),
    Number(Object.prototype.hasOwnProperty.call(changes, 'priorityOrder') ? changes.priorityOrder : existing.priorityOrder || 0),
    Object.prototype.hasOwnProperty.call(changes, 'lidarrArtistId') ? changes.lidarrArtistId : existing.lidarrArtistId,
    Object.prototype.hasOwnProperty.call(changes, 'lidarrAlbumId') ? changes.lidarrAlbumId : existing.lidarrAlbumId,
    JSON.stringify(mergedDetail),
    Number(changes.updatedAt || Date.now()),
    Object.prototype.hasOwnProperty.call(changes, 'processedAt') ? changes.processedAt : existing.processedAt,
    existing.id,
  );
  return getLidarrRequest(db, existing.id, existing.userPlexId);
}

export function removeQueuedLidarrRequest(db, requestId, userPlexId = '') {
  const existing = getLidarrRequest(db, requestId, userPlexId);
  if (!existing) return null;
  if (!['queued', 'processing'].includes(String(existing.status || ''))) return existing;
  return updateLidarrRequest(db, existing.id, {
    status: 'removed',
    processedAt: Date.now(),
    updatedAt: Date.now(),
  }, existing.userPlexId);
}

export function reorderQueuedLidarrRequests(db, userPlexId, requestIds = []) {
  const ids = Array.isArray(requestIds)
    ? requestIds.map((value) => Number(value || 0)).filter((value) => value > 0)
    : [];
  if (!ids.length) return [];
  const queued = listLidarrRequests(db, userPlexId, { statuses: ['queued'], limit: 500 });
  const queuedIds = new Set(queued.map((item) => item.id));
  const filtered = ids.filter((id) => queuedIds.has(id));
  const remainder = queued.map((item) => item.id).filter((id) => !filtered.includes(id));
  const ordered = [...filtered, ...remainder];
  const stmt = db.prepare('UPDATE lidarr_requests SET priority_order = ?, updated_at = ? WHERE id = ? AND user_plex_id = ?');
  const now = Date.now();
  const tx = db.transaction(() => {
    ordered.forEach((id, index) => {
      stmt.run(index + 1, now, id, userPlexId);
    });
  });
  tx();
  return listLidarrRequests(db, userPlexId, { statuses: ['queued'], limit: 500 });
}

// Returns { forUser: {artistCount, trackCount} | null, average: {artistCount, trackCount} }
// rules: { artistTiers: string[], trackTiers: string[], topNPerArtist: number|null, maxTracks: number|null }
// smartSettings: { artistSkipRank, artistBelterRank }
export function previewGlobalPlaylist(db, rules, userIdFilter, smartSettings) {
  const masterTracks = getMasterTracks(db);
  const skipRank = Number(smartSettings?.artistSkipRank ?? 2);
  const belterRank = Number(smartSettings?.artistBelterRank ?? 8);

  function classifyArtist(score) {
    if (score === null || score === undefined) return 'unranked';
    if (score >= belterRank) return 'belter';
    if (score >= 5) return 'decent';
    if (score > skipRank) return 'halfDecent';
    return 'skip';
  }

  const artistTierFilter = Array.isArray(rules?.artistTiers) && rules.artistTiers.length ? new Set(rules.artistTiers) : null;
  const trackTierFilter  = Array.isArray(rules?.trackTiers)  && rules.trackTiers.length  ? new Set(rules.trackTiers)  : null;
  const topN = rules?.topNPerArtist ? Math.max(1, Number(rules.topNPerArtist)) : null;
  const maxT = rules?.maxTracks     ? Math.max(1, Number(rules.maxTracks))     : null;

  function runForUser(uid) {
    const artistMap = new Map(
      db.prepare('SELECT artist_name, ranking_score FROM artist_stats WHERE user_plex_id = ?').all(uid)
        .map((r) => [r.artist_name.toLowerCase(), r.ranking_score]),
    );
    const trackMap = new Map(
      db.prepare('SELECT plex_rating_key, tier, tier_weight FROM track_stats WHERE user_plex_id = ?').all(uid)
        .map((r) => [r.plex_rating_key, r]),
    );

    const byArtist = new Map();
    for (const t of masterTracks) {
      const score = artistMap.get((t.artistName || '').toLowerCase()) ?? null;
      const artistTier = classifyArtist(score);
      if (artistTierFilter && !artistTierFilter.has(artistTier)) continue;

      const stat = trackMap.get(t.ratingKey);
      const rawTier = stat?.tier || 'curatorr';
      const normTier = rawTier === 'half-decent' ? 'halfDecent' : rawTier === 'curatorr' ? 'unplayed' : rawTier;
      if (trackTierFilter && !trackTierFilter.has(normTier)) continue;

      if (!byArtist.has(t.artistName)) byArtist.set(t.artistName, []);
      byArtist.get(t.artistName).push({ rc: t.ratingCount || 0, tw: stat?.tier_weight || 0 });
    }

    let totalTracks = 0;
    for (const [, tracks] of byArtist) {
      totalTracks += topN ? Math.min(tracks.length, topN) : tracks.length;
    }
    if (maxT && totalTracks > maxT) totalTracks = maxT;
    return { artistCount: byArtist.size, trackCount: totalTracks };
  }

  if (userIdFilter) {
    return { forUser: runForUser(userIdFilter), average: null };
  }

  const allIds = db.prepare('SELECT DISTINCT user_plex_id FROM artist_stats').all().map((r) => r.user_plex_id);
  if (!allIds.length) return { forUser: null, average: { artistCount: 0, trackCount: 0 } };
  const results = allIds.map(runForUser);
  return {
    forUser: null,
    average: {
      artistCount: Math.round(results.reduce((s, r) => s + r.artistCount, 0) / results.length),
      trackCount:  Math.round(results.reduce((s, r) => s + r.trackCount,  0) / results.length),
    },
  };
}
