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
  player_scope    TEXT NOT NULL DEFAULT '',
  playback_state  TEXT NOT NULL DEFAULT 'playing',
  last_position_ms INTEGER NOT NULL DEFAULT 0,
  max_position_ms INTEGER NOT NULL DEFAULT 0,
  accumulated_ms  INTEGER NOT NULL DEFAULT 0,
  playing_since   INTEGER,
  last_event_at   INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
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
  recording_mbid TEXT NOT NULL DEFAULT '',
  genres        TEXT NOT NULL DEFAULT '[]',
  library_key   TEXT NOT NULL DEFAULT '',
  file_path     TEXT NOT NULL DEFAULT '',
  duration_ms   INTEGER NOT NULL DEFAULT 0,
  rating_count  INTEGER NOT NULL DEFAULT 0,
  view_count    INTEGER NOT NULL DEFAULT 0,
  updated_at    INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);

CREATE TABLE IF NOT EXISTS track_enrichment (
  rating_key             TEXT NOT NULL PRIMARY KEY,
  recording_mbid         TEXT NOT NULL DEFAULT '',
  track_year             INTEGER,
  original_release_date  TEXT NOT NULL DEFAULT '',
  bpm                    REAL,
  musical_key            TEXT NOT NULL DEFAULT '',
  camelot_key            TEXT NOT NULL DEFAULT '',
  energy                 REAL,
  danceability           REAL,
  loudness               REAL,
  loudness_range         REAL,
  peak                   REAL,
  track_gain             REAL,
  album_gain             REAL,
  album_peak             REAL,
  album_range            REAL,
  analysis_source        TEXT NOT NULL DEFAULT '',
  analysis_confidence    REAL NOT NULL DEFAULT 0,
  payload_json           TEXT NOT NULL DEFAULT '{}',
  updated_at             INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);

CREATE INDEX IF NOT EXISTS idx_track_enrichment_track_year
  ON track_enrichment(track_year)
  WHERE track_year IS NOT NULL;

CREATE TABLE IF NOT EXISTS user_preferences (
  user_plex_id        TEXT NOT NULL PRIMARY KEY,
  liked_genres        TEXT NOT NULL DEFAULT '[]',   -- JSON string[]
  ignored_genres      TEXT NOT NULL DEFAULT '[]',
  liked_artists       TEXT NOT NULL DEFAULT '[]',
  ignored_artists     TEXT NOT NULL DEFAULT '[]',
  user_wizard_completed INTEGER NOT NULL DEFAULT 0,
  smart_config        TEXT NOT NULL DEFAULT 'null',
  lastfm_username     TEXT NOT NULL DEFAULT '',
  lastfm_api_key      TEXT NOT NULL DEFAULT '',
  lastfm_sync_watermark INTEGER NOT NULL DEFAULT 0,
  lastfm_backfill_cursor INTEGER NOT NULL DEFAULT 0,
  lastfm_enabled_stations TEXT NOT NULL DEFAULT '[]',
  lastfm_strict_match_stations TEXT NOT NULL DEFAULT '[]',
  lastfm_station_sorts TEXT NOT NULL DEFAULT '{}',
  lastfm_station_final_orderings TEXT NOT NULL DEFAULT '{}',
  listenbrainz_username TEXT NOT NULL DEFAULT '',
  listenbrainz_token  TEXT NOT NULL DEFAULT '',
  listenbrainz_enabled_playlists TEXT NOT NULL DEFAULT '[]',
  listenbrainz_strict_match_playlists TEXT NOT NULL DEFAULT '[]',
  listenbrainz_playlist_sorts TEXT NOT NULL DEFAULT '{}',
  listenbrainz_playlist_final_orderings TEXT NOT NULL DEFAULT '{}',
  spotify_user_id     TEXT NOT NULL DEFAULT '',
  spotify_display_name TEXT NOT NULL DEFAULT '',
  spotify_access_token TEXT NOT NULL DEFAULT '',
  spotify_refresh_token TEXT NOT NULL DEFAULT '',
  spotify_token_expires_at INTEGER NOT NULL DEFAULT 0,
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
  title_override      TEXT NOT NULL DEFAULT '',
  source_type         TEXT NOT NULL DEFAULT '',
  source_ref          TEXT NOT NULL DEFAULT '',
  source_title        TEXT NOT NULL DEFAULT '',
  source_owner        TEXT NOT NULL DEFAULT '',
  imported_sync_period TEXT NOT NULL DEFAULT 'disabled',
  algorithm_version   TEXT NOT NULL DEFAULT 'phase2a',
  last_built_at       INTEGER,
  last_synced_at      INTEGER,
  track_count         INTEGER NOT NULL DEFAULT 0,
  missing_count       INTEGER NOT NULL DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS imported_playlist_unmatched (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  playlist_key    TEXT NOT NULL,
  user_plex_id    TEXT NOT NULL,
  source_track_id TEXT NOT NULL DEFAULT '',
  position        INTEGER NOT NULL DEFAULT 0,
  track_title     TEXT NOT NULL DEFAULT '',
  artist_name     TEXT NOT NULL DEFAULT '',
  artists_json    TEXT NOT NULL DEFAULT '[]',
  album_title     TEXT NOT NULL DEFAULT '',
  album_type      TEXT NOT NULL DEFAULT '',
  album_image_url TEXT NOT NULL DEFAULT '',
  duration_ms     INTEGER NOT NULL DEFAULT 0,
  selected        INTEGER NOT NULL DEFAULT 1,
  created_at      INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  updated_at      INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);

CREATE INDEX IF NOT EXISTS idx_imported_playlist_unmatched_user_playlist
  ON imported_playlist_unmatched(user_plex_id, playlist_key, position);

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

CREATE TABLE IF NOT EXISTS artist_tags (
  artist_name  TEXT NOT NULL PRIMARY KEY,
  tags         TEXT NOT NULL DEFAULT '[]',
  updated_at   INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);

CREATE TABLE IF NOT EXISTS user_personal_playlists (
  id           TEXT NOT NULL PRIMARY KEY,
  user_plex_id TEXT NOT NULL,
  name         TEXT NOT NULL,
  rules        TEXT NOT NULL DEFAULT '{}',
  track_filters TEXT NOT NULL DEFAULT 'null',
  enabled      INTEGER NOT NULL DEFAULT 1,
  created_at   INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  updated_at   INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);
CREATE INDEX IF NOT EXISTS idx_user_personal_playlists_user ON user_personal_playlists(user_plex_id);

CREATE TABLE IF NOT EXISTS signal_events (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  user_plex_id    TEXT NOT NULL,
  plex_rating_key TEXT NOT NULL,
  artist_name     TEXT NOT NULL DEFAULT '',
  signal_type     TEXT NOT NULL,
  signal_value    REAL NOT NULL DEFAULT 1.0,
  source_context  TEXT NOT NULL DEFAULT '',
  created_at      INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);
CREATE INDEX IF NOT EXISTS idx_signal_events_user_track
  ON signal_events(user_plex_id, plex_rating_key);
CREATE INDEX IF NOT EXISTS idx_signal_events_user_type
  ON signal_events(user_plex_id, signal_type, created_at DESC);

CREATE TABLE IF NOT EXISTS playlist_rule_templates (
  id           TEXT NOT NULL PRIMARY KEY,
  user_plex_id TEXT NOT NULL DEFAULT '',
  name         TEXT NOT NULL,
  description  TEXT NOT NULL DEFAULT '',
  rules        TEXT NOT NULL DEFAULT '{}',
  is_builtin   INTEGER NOT NULL DEFAULT 0,
  created_at   INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000),
  updated_at   INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
);
CREATE INDEX IF NOT EXISTS idx_playlist_rule_templates_user
  ON playlist_rule_templates(user_plex_id, is_builtin);
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
  // Checkpoint every 200 pages (~800 KB) to keep the WAL small.
  // Default is 1000 pages which can grow to 4+ MB on a NAS, causing slow startup.
  db.pragma('wal_autocheckpoint = 200');

  // Apply schema
  db.exec(SCHEMA);

  // ── Migrations (idempotent) ──────────────────────────────────────────────
  const playCols = db.prepare('PRAGMA table_info(play_events)').all().map((c) => c.name);
  if (!playCols.includes('session_key'))
    db.exec("ALTER TABLE play_events ADD COLUMN session_key TEXT DEFAULT ''");
  const playColsNow = db.prepare('PRAGMA table_info(play_events)').all().map((c) => c.name);
  if (playColsNow.includes('session_key'))
    db.exec(`CREATE INDEX IF NOT EXISTS idx_play_events_session ON play_events(session_key) WHERE session_key != ''`);

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
  if (!prefCols.includes('lastfm_username'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN lastfm_username TEXT NOT NULL DEFAULT ''");
  if (!prefCols.includes('lastfm_api_key'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN lastfm_api_key TEXT NOT NULL DEFAULT ''");
  if (!prefCols.includes('lastfm_sync_watermark'))
    db.exec('ALTER TABLE user_preferences ADD COLUMN lastfm_sync_watermark INTEGER NOT NULL DEFAULT 0');
  if (!prefCols.includes('lastfm_backfill_cursor'))
    db.exec('ALTER TABLE user_preferences ADD COLUMN lastfm_backfill_cursor INTEGER NOT NULL DEFAULT 0');
  if (!prefCols.includes('lastfm_enabled_stations'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN lastfm_enabled_stations TEXT NOT NULL DEFAULT '[]'");
  if (!prefCols.includes('listenbrainz_username'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN listenbrainz_username TEXT NOT NULL DEFAULT ''");
  if (!prefCols.includes('listenbrainz_token'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN listenbrainz_token TEXT NOT NULL DEFAULT ''");
  if (!prefCols.includes('listenbrainz_enabled_playlists'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN listenbrainz_enabled_playlists TEXT NOT NULL DEFAULT '[]'");
  if (!prefCols.includes('lastfm_strict_match_stations'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN lastfm_strict_match_stations TEXT NOT NULL DEFAULT '[]'");
  if (!prefCols.includes('lastfm_station_sorts'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN lastfm_station_sorts TEXT NOT NULL DEFAULT '{}'");
  if (!prefCols.includes('lastfm_station_final_orderings'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN lastfm_station_final_orderings TEXT NOT NULL DEFAULT '{}'");
  if (!prefCols.includes('listenbrainz_strict_match_playlists'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN listenbrainz_strict_match_playlists TEXT NOT NULL DEFAULT '[]'");
  if (!prefCols.includes('listenbrainz_playlist_sorts'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN listenbrainz_playlist_sorts TEXT NOT NULL DEFAULT '{}'");
  if (!prefCols.includes('listenbrainz_playlist_final_orderings'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN listenbrainz_playlist_final_orderings TEXT NOT NULL DEFAULT '{}'");
  if (!prefCols.includes('spotify_user_id'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN spotify_user_id TEXT NOT NULL DEFAULT ''");
  if (!prefCols.includes('spotify_display_name'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN spotify_display_name TEXT NOT NULL DEFAULT ''");
  if (!prefCols.includes('spotify_access_token'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN spotify_access_token TEXT NOT NULL DEFAULT ''");
  if (!prefCols.includes('spotify_refresh_token'))
    db.exec("ALTER TABLE user_preferences ADD COLUMN spotify_refresh_token TEXT NOT NULL DEFAULT ''");
  if (!prefCols.includes('spotify_token_expires_at'))
    db.exec('ALTER TABLE user_preferences ADD COLUMN spotify_token_expires_at INTEGER NOT NULL DEFAULT 0');
  const generatedCols = db.prepare('PRAGMA table_info(user_generated_playlists)').all().map((c) => c.name);
  if (!generatedCols.includes('title_override'))
    db.exec("ALTER TABLE user_generated_playlists ADD COLUMN title_override TEXT NOT NULL DEFAULT ''");
  if (!generatedCols.includes('source_type'))
    db.exec("ALTER TABLE user_generated_playlists ADD COLUMN source_type TEXT NOT NULL DEFAULT ''");
  if (!generatedCols.includes('source_ref'))
    db.exec("ALTER TABLE user_generated_playlists ADD COLUMN source_ref TEXT NOT NULL DEFAULT ''");
  if (!generatedCols.includes('source_title'))
    db.exec("ALTER TABLE user_generated_playlists ADD COLUMN source_title TEXT NOT NULL DEFAULT ''");
  if (!generatedCols.includes('source_owner'))
    db.exec("ALTER TABLE user_generated_playlists ADD COLUMN source_owner TEXT NOT NULL DEFAULT ''");
  if (!generatedCols.includes('imported_sync_period'))
    db.exec("ALTER TABLE user_generated_playlists ADD COLUMN imported_sync_period TEXT NOT NULL DEFAULT 'disabled'");
  if (!generatedCols.includes('missing_count'))
    db.exec('ALTER TABLE user_generated_playlists ADD COLUMN missing_count INTEGER NOT NULL DEFAULT 0');

  const importedUnmatchedCols = db.prepare('PRAGMA table_info(imported_playlist_unmatched)').all().map((c) => c.name);
  if (!importedUnmatchedCols.includes('album_image_url'))
    db.exec("ALTER TABLE imported_playlist_unmatched ADD COLUMN album_image_url TEXT NOT NULL DEFAULT ''");

  const masterCols = db.prepare('PRAGMA table_info(master_tracks)').all().map((c) => c.name);
  if (!masterCols.includes('rating_count'))
    db.exec('ALTER TABLE master_tracks ADD COLUMN rating_count INTEGER NOT NULL DEFAULT 0');
  if (!masterCols.includes('view_count'))
    db.exec('ALTER TABLE master_tracks ADD COLUMN view_count INTEGER NOT NULL DEFAULT 0');
  if (!masterCols.includes('moods'))
    db.exec("ALTER TABLE master_tracks ADD COLUMN moods TEXT NOT NULL DEFAULT '[]'");
  if (!masterCols.includes('recording_mbid'))
    db.exec("ALTER TABLE master_tracks ADD COLUMN recording_mbid TEXT NOT NULL DEFAULT ''");
  const masterColsNow = db.prepare('PRAGMA table_info(master_tracks)').all().map((c) => c.name);
  if (masterColsNow.includes('recording_mbid'))
    db.exec(`CREATE INDEX IF NOT EXISTS idx_master_tracks_recording_mbid ON master_tracks(recording_mbid) WHERE recording_mbid != ''`);
  if (!masterCols.includes('file_path'))
    db.exec("ALTER TABLE master_tracks ADD COLUMN file_path TEXT NOT NULL DEFAULT ''");
  if (!masterCols.includes('duration_ms'))
    db.exec('ALTER TABLE master_tracks ADD COLUMN duration_ms INTEGER NOT NULL DEFAULT 0');

  const personalPlaylistCols = db.prepare('PRAGMA table_info(user_personal_playlists)').all().map((c) => c.name);
  if (!personalPlaylistCols.includes('track_filters'))
    db.exec("ALTER TABLE user_personal_playlists ADD COLUMN track_filters TEXT NOT NULL DEFAULT 'null'");

  const enrichmentCols = db.prepare('PRAGMA table_info(track_enrichment)').all().map((c) => c.name);
  if (!enrichmentCols.includes('recording_mbid'))
    db.exec("ALTER TABLE track_enrichment ADD COLUMN recording_mbid TEXT NOT NULL DEFAULT ''");
  const enrichmentColsNow = db.prepare('PRAGMA table_info(track_enrichment)').all().map((c) => c.name);
  if (enrichmentColsNow.includes('recording_mbid'))
    db.exec(`CREATE INDEX IF NOT EXISTS idx_track_enrichment_recording_mbid ON track_enrichment(recording_mbid) WHERE recording_mbid != ''`);
  if (!enrichmentCols.includes('track_year'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN track_year INTEGER');
  if (!enrichmentCols.includes('original_release_date'))
    db.exec("ALTER TABLE track_enrichment ADD COLUMN original_release_date TEXT NOT NULL DEFAULT ''");
  if (!enrichmentCols.includes('bpm'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN bpm REAL');
  if (!enrichmentCols.includes('musical_key'))
    db.exec("ALTER TABLE track_enrichment ADD COLUMN musical_key TEXT NOT NULL DEFAULT ''");
  if (!enrichmentCols.includes('camelot_key'))
    db.exec("ALTER TABLE track_enrichment ADD COLUMN camelot_key TEXT NOT NULL DEFAULT ''");
  if (!enrichmentCols.includes('energy'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN energy REAL');
  if (!enrichmentCols.includes('danceability'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN danceability REAL');
  if (!enrichmentCols.includes('loudness'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN loudness REAL');
  if (!enrichmentCols.includes('loudness_range'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN loudness_range REAL');
  if (!enrichmentCols.includes('peak'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN peak REAL');
  if (!enrichmentCols.includes('track_gain'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN track_gain REAL');
  if (!enrichmentCols.includes('album_gain'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN album_gain REAL');
  if (!enrichmentCols.includes('album_peak'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN album_peak REAL');
  if (!enrichmentCols.includes('album_range'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN album_range REAL');
  if (!enrichmentCols.includes('analysis_source'))
    db.exec("ALTER TABLE track_enrichment ADD COLUMN analysis_source TEXT NOT NULL DEFAULT ''");
  if (!enrichmentCols.includes('analysis_confidence'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN analysis_confidence REAL NOT NULL DEFAULT 0');
  if (!enrichmentCols.includes('payload_json'))
    db.exec("ALTER TABLE track_enrichment ADD COLUMN payload_json TEXT NOT NULL DEFAULT '{}'");
  if (!enrichmentCols.includes('updated_at'))
    db.exec('ALTER TABLE track_enrichment ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0');
  if (db.prepare('SELECT 1 FROM track_enrichment WHERE updated_at = 0 LIMIT 1').get())
    db.exec(`UPDATE track_enrichment SET updated_at = COALESCE(NULLIF(updated_at, 0), unixepoch('now') * 1000) WHERE updated_at = 0`);

  const openSessionCols = db.prepare('PRAGMA table_info(open_sessions)').all().map((c) => c.name);
  if (!openSessionCols.includes('player_scope'))
    db.exec("ALTER TABLE open_sessions ADD COLUMN player_scope TEXT NOT NULL DEFAULT ''");
  if (!openSessionCols.includes('playback_state'))
    db.exec("ALTER TABLE open_sessions ADD COLUMN playback_state TEXT NOT NULL DEFAULT 'playing'");
  if (!openSessionCols.includes('last_position_ms'))
    db.exec('ALTER TABLE open_sessions ADD COLUMN last_position_ms INTEGER NOT NULL DEFAULT 0');
  if (!openSessionCols.includes('max_position_ms'))
    db.exec('ALTER TABLE open_sessions ADD COLUMN max_position_ms INTEGER NOT NULL DEFAULT 0');
  if (!openSessionCols.includes('accumulated_ms'))
    db.exec('ALTER TABLE open_sessions ADD COLUMN accumulated_ms INTEGER NOT NULL DEFAULT 0');
  if (!openSessionCols.includes('playing_since'))
    db.exec('ALTER TABLE open_sessions ADD COLUMN playing_since INTEGER');
  if (!openSessionCols.includes('last_event_at'))
    db.exec('ALTER TABLE open_sessions ADD COLUMN last_event_at INTEGER NOT NULL DEFAULT 0');
  if (db.prepare('SELECT 1 FROM open_sessions WHERE last_event_at = 0 LIMIT 1').get())
    db.exec('UPDATE open_sessions SET last_event_at = COALESCE(NULLIF(last_event_at, 0), created_at, started_at) WHERE last_event_at = 0');

  // system_job_runs has no migrations needed — created fresh via SCHEMA above

  const syncCols = db.prepare('PRAGMA table_info(playlist_syncs)').all().map((c) => c.name);
  if (!syncCols.includes('tracks_added'))
    db.exec('ALTER TABLE playlist_syncs ADD COLUMN tracks_added INTEGER NOT NULL DEFAULT 0');
  if (!syncCols.includes('tracks_removed'))
    db.exec('ALTER TABLE playlist_syncs ADD COLUMN tracks_removed INTEGER NOT NULL DEFAULT 0');

  // ── Rule template seeds (idempotent) ────────────────────────────────────────
  const BUILTIN_TEMPLATES = [
    { id: 'tmpl_favourites', name: 'Favourites',  description: 'Your highest-rated tracks and artists', rules: { artistTiers: { include: ['belter'], exclude: [] }, trackTiers: { include: ['belter'], exclude: [] } } },
    { id: 'tmpl_discovery',  name: 'Discovery',   description: 'Unplayed tracks from unranked artists — surface new favourites', rules: { artistTiers: { include: ['unranked'], exclude: [] }, trackTiers: { include: ['unplayed'], exclude: [] } } },
    { id: 'tmpl_random_library', name: 'Random Library Mix', description: '50 random library tracks with artist and album variety', rules: { topNPerArtist: 1, maxTracksPerAlbum: 1, maxTracks: 50, sortBy: 'random', finalOrdering: 'none', rebuildSchedule: 'daily' } },
    { id: 'tmpl_workout',    name: 'Workout',      description: 'High BPM, high energy, built for momentum', rules: { featurePreset: 'workout', artistTiers: { include: ['belter', 'decent'], exclude: [] }, trackTiers: { include: [], exclude: [] } } },
    { id: 'tmpl_focus',      name: 'Focus',        description: 'Slow pace, low energy, easy background listening', rules: { featurePreset: 'chill', trackTiers: { include: ['belter', 'decent'], exclude: [] } } },
    { id: 'tmpl_latenight',  name: 'Late Night',   description: 'Club vibes — fast, energetic, dancefloor-ready', rules: { featurePreset: 'club', artistTiers: { include: ['belter', 'decent'], exclude: [] }, trackTiers: { include: [], exclude: [] } } },
  ];
  const existingTemplateIds = new Set(
    db.prepare('SELECT id FROM playlist_rule_templates WHERE is_builtin = 1').all().map((r) => r.id),
  );
  const insertTemplate = db.prepare(`
    INSERT OR IGNORE INTO playlist_rule_templates (id, user_plex_id, name, description, rules, is_builtin)
    VALUES (?, '', ?, ?, ?, 1)
  `);
  for (const t of BUILTIN_TEMPLATES) {
    if (!existingTemplateIds.has(t.id)) {
      insertTemplate.run(t.id, t.name, t.description, JSON.stringify(t.rules));
    }
  }

  return db;
}

// ─── Session helpers ──────────────────────────────────────────────────────────

export function openSession(db, {
  sessionKey, userPlexId, plexRatingKey,
  trackTitle, artistName, albumName, libraryKey,
  trackDurationMs, startedAt, eventSource,
  playerScope = '',
  playbackState = 'playing',
  lastPositionMs = 0,
  maxPositionMs = 0,
  accumulatedMs = 0,
  playingSince = null,
  lastEventAt = Date.now(),
}) {
  db.prepare(`
    INSERT OR REPLACE INTO open_sessions
      (session_key, user_plex_id, plex_rating_key, track_title, artist_name,
       album_name, library_key, track_duration_ms, player_scope, playback_state,
       last_position_ms, max_position_ms, accumulated_ms, playing_since, last_event_at,
       started_at, event_source)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    sessionKey, userPlexId, plexRatingKey,
    trackTitle || '', artistName || '', albumName || '', libraryKey || '',
    trackDurationMs || 0,
    playerScope || '',
    playbackState || 'playing',
    Math.max(0, Number(lastPositionMs || 0)),
    Math.max(0, Number(maxPositionMs || 0)),
    Math.max(0, Number(accumulatedMs || 0)),
    playingSince || null,
    lastEventAt || Date.now(),
    startedAt,
    eventSource || 'tautulli',
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

// ─── Tri-state filter parser ──────────────────────────────────────────────────

// Normalises genres/moods/tags into { include: Set|null, exclude: Set|null, includeMode }.
// Accepts legacy flat arrays (treated as include-only) and new { include, exclude, includeMode } objects.
export function parseTriStateFilter(value) {
  if (!value) return { include: null, exclude: null, includeMode: 'any' };
  if (Array.isArray(value)) {
    return value.length ? { include: new Set(value), exclude: null, includeMode: 'any' } : { include: null, exclude: null, includeMode: 'any' };
  }
  const inc = Array.isArray(value.include) && value.include.length ? new Set(value.include) : null;
  const exc = Array.isArray(value.exclude) && value.exclude.length ? new Set(value.exclude) : null;
  return { include: inc, exclude: exc, includeMode: value.includeMode === 'all' ? 'all' : 'any' };
}

// ─── Signal events ────────────────────────────────────────────────────────────

export function recordSignalEvent(db, { userPlexId, plexRatingKey, artistName = '', signalType, signalValue = 1.0, sourceContext = '' }) {
  db.prepare(`
    INSERT INTO signal_events (user_plex_id, plex_rating_key, artist_name, signal_type, signal_value, source_context)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(userPlexId, plexRatingKey, artistName, signalType, signalValue, sourceContext);
}

export function getSignalBonus(db, userPlexId, plexRatingKey) {
  const WINDOW_MS = 90 * 24 * 60 * 60 * 1000;
  const rows = db.prepare(`
    SELECT signal_type, signal_value FROM signal_events
    WHERE user_plex_id = ? AND plex_rating_key = ? AND created_at > ?
    ORDER BY created_at DESC LIMIT 50
  `).all(userPlexId, plexRatingKey, Date.now() - WINDOW_MS);
  let bonus = 0;
  for (const row of rows) {
    if (row.signal_type === 'playlist_add')    bonus += 0.5;
    if (row.signal_type === 'playlist_remove') bonus -= 0.3;
    if (row.signal_type === 'session_replay')  bonus += 0.4;
    if (row.signal_type === 'plex_rating')     bonus += (row.signal_value / 5) * 1.5;
  }
  return Math.max(-2, Math.min(4, bonus));
}

// ─── Rule templates ───────────────────────────────────────────────────────────

export function listRuleTemplates(db, userPlexId) {
  function parseTemplatePayload(raw) {
    let parsed = {};
    try {
      parsed = JSON.parse(raw || '{}') || {};
    } catch {
      parsed = {};
    }
    const hasEnvelope = parsed && typeof parsed === 'object'
      && !Array.isArray(parsed)
      && (Object.prototype.hasOwnProperty.call(parsed, 'rules')
        || Object.prototype.hasOwnProperty.call(parsed, 'trackFilters')
        || Object.prototype.hasOwnProperty.call(parsed, 'startingPointId'));
    const rules = hasEnvelope && parsed.rules && typeof parsed.rules === 'object' && !Array.isArray(parsed.rules)
      ? parsed.rules
      : (parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {});
    const trackFilters = hasEnvelope && parsed.trackFilters && typeof parsed.trackFilters === 'object'
      ? parsed.trackFilters
      : null;
    const startingPointId = hasEnvelope && typeof parsed.startingPointId === 'string'
      ? parsed.startingPointId
      : 'blank';
    return { rules, trackFilters, startingPointId };
  }
  return db.prepare(`
    SELECT * FROM playlist_rule_templates
    WHERE is_builtin = 1 OR user_plex_id = ?
    ORDER BY is_builtin DESC, created_at ASC
  `).all(userPlexId).map((r) => {
    const payload = parseTemplatePayload(r.rules);
    return {
      ...r,
      rules: payload.rules,
      trackFilters: payload.trackFilters,
      startingPointId: payload.startingPointId,
    };
  });
}

export function saveRuleTemplate(db, userPlexId, { name, description, rules, trackFilters, startingPointId }) {
  const now = Date.now();
  const id = 'utmpl_' + now.toString(36) + Math.random().toString(36).slice(2, 6);
  const payload = JSON.stringify({
    rules: rules || {},
    trackFilters: trackFilters && typeof trackFilters === 'object' ? trackFilters : null,
    startingPointId: typeof startingPointId === 'string' && startingPointId.trim() ? startingPointId.trim() : 'blank',
  });
  db.prepare(`
    INSERT INTO playlist_rule_templates (id, user_plex_id, name, description, rules, is_builtin, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, 0, ?, ?)
  `).run(id, userPlexId, name, description || '', payload, now, now);
  return id;
}

export function updateRuleTemplate(db, id, userPlexId, { name, description, rules, trackFilters, startingPointId }) {
  const now = Date.now();
  const payload = JSON.stringify({
    rules: rules || {},
    trackFilters: trackFilters && typeof trackFilters === 'object' ? trackFilters : null,
    startingPointId: typeof startingPointId === 'string' && startingPointId.trim() ? startingPointId.trim() : 'blank',
  });
  return db.prepare(`
    UPDATE playlist_rule_templates
    SET name = ?, description = ?, rules = ?, updated_at = ?
    WHERE id = ? AND user_plex_id = ? AND is_builtin = 0
  `).run(name, description || '', payload, now, id, userPlexId);
}

export function deleteRuleTemplate(db, id, userPlexId) {
  db.prepare(`
    DELETE FROM playlist_rule_templates WHERE id = ? AND user_plex_id = ? AND is_builtin = 0
  `).run(id, userPlexId);
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

function _eventTime(event) {
  return Number(event?.ended_at || event?.started_at || Date.now());
}

function _classifyReplayedEvent(event, smartConfig) {
  const eventSource = String(event?.event_source || '').trim().toLowerCase();
  const isLastfmScrobble = eventSource === 'lastfm_sync' || eventSource === 'lastfm_backfill';
  if (isLastfmScrobble && Number(event?.is_skip || 0) === 0) {
    const trackDurationMs = Number(event?.track_duration_ms || 0);
    return trackDurationMs > 0
      ? classifyTier(trackDurationMs, trackDurationMs, smartConfig)
      : 'belter';
  }
  return classifyTier(event?.duration_ms || 0, event?.track_duration_ms || 0, smartConfig);
}

function _rebuildTrackSnapshot(existing, events, { songSkipLimit, smartConfig }) {
  const manualExcluded = Number(existing?.manually_excluded || 0);
  const manualIncluded = Number(existing?.manually_included || 0);
  let playCount = 0;
  let skipCount = 0;
  let consecutiveSkips = 0;
  let autoExcluded = 0;
  let lastPlayedAt = null;
  let lastSkippedAt = null;
  let trackTitle = String(existing?.track_title || '').trim();
  let artistName = String(existing?.artist_name || '').trim();
  let albumName = String(existing?.album_name || '').trim();
  let tier = existing?.tier || 'curatorr';
  let tierWeight = existing?.tier_weight ?? 0;

  for (const event of events) {
    const eventTier = _classifyReplayedEvent(event, smartConfig);
    const isSkip = eventTier === 'skip';
    const eventAt = _eventTime(event);

    if (event.track_title) trackTitle = event.track_title;
    if (event.artist_name) artistName = event.artist_name;
    if (event.album_name) albumName = event.album_name;

    if (isSkip) {
      skipCount += 1;
      consecutiveSkips += 1;
      lastSkippedAt = eventAt;
      if (!manualIncluded && consecutiveSkips >= songSkipLimit) autoExcluded = 1;
    } else {
      playCount += 1;
      lastPlayedAt = eventAt;
      const alreadyExcluded = (manualExcluded || autoExcluded) === 1 && manualIncluded !== 1;
      consecutiveSkips = alreadyExcluded ? consecutiveSkips : Math.max(0, consecutiveSkips - 1);
    }

    tier = eventTier;
    tierWeight = _tierWeight(eventTier, smartConfig);
  }

  return {
    trackTitle,
    artistName,
    albumName,
    playCount,
    skipCount,
    consecutiveSkips,
    excludedFromSmart: manualIncluded ? 0 : (manualExcluded ? 1 : autoExcluded),
    manuallyExcluded: manualExcluded,
    manuallyIncluded: manualIncluded,
    tier,
    tierWeight,
    lastPlayedAt,
    lastSkippedAt,
  };
}

export function rebuildTrackStatsFromEvents(db, {
  userPlexId, plexRatingKey, songSkipLimit, smartConfig,
}) {
  const existing = db.prepare(
    'SELECT * FROM track_stats WHERE plex_rating_key = ? AND user_plex_id = ?',
  ).get(plexRatingKey, userPlexId);
  const events = db.prepare(`
    SELECT id, track_title, artist_name, album_name, started_at, ended_at, duration_ms, track_duration_ms, is_skip, event_source
    FROM play_events
    WHERE user_plex_id = ? AND plex_rating_key = ?
    ORDER BY started_at ASC, id ASC
  `).all(userPlexId, plexRatingKey);

  if (!events.length) {
    if (existing && !existing.manually_excluded && !existing.manually_included) {
      db.prepare('DELETE FROM track_stats WHERE plex_rating_key = ? AND user_plex_id = ?').run(plexRatingKey, userPlexId);
    }
    return null;
  }

  const snapshot = _rebuildTrackSnapshot(existing, events, { songSkipLimit, smartConfig });
  const now = Date.now();

  if (existing) {
    db.prepare(`
      UPDATE track_stats SET
        track_title = ?, artist_name = ?, album_name = ?,
        play_count = ?, skip_count = ?, consecutive_skips = ?,
        excluded_from_smart = ?, manually_excluded = ?, manually_included = ?,
        tier = ?, tier_weight = ?,
        last_played_at = ?, last_skipped_at = ?, updated_at = ?
      WHERE plex_rating_key = ? AND user_plex_id = ?
    `).run(
      snapshot.trackTitle,
      snapshot.artistName,
      snapshot.albumName,
      snapshot.playCount,
      snapshot.skipCount,
      snapshot.consecutiveSkips,
      snapshot.excludedFromSmart,
      snapshot.manuallyExcluded,
      snapshot.manuallyIncluded,
      snapshot.tier,
      snapshot.tierWeight,
      snapshot.lastPlayedAt,
      snapshot.lastSkippedAt,
      now,
      plexRatingKey,
      userPlexId,
    );
  } else {
    db.prepare(`
      INSERT INTO track_stats
        (plex_rating_key, user_plex_id, track_title, artist_name, album_name,
         play_count, skip_count, consecutive_skips, excluded_from_smart,
         manually_excluded, manually_included, tier, tier_weight,
         last_played_at, last_skipped_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      plexRatingKey,
      userPlexId,
      snapshot.trackTitle,
      snapshot.artistName,
      snapshot.albumName,
      snapshot.playCount,
      snapshot.skipCount,
      snapshot.consecutiveSkips,
      snapshot.excludedFromSmart,
      snapshot.manuallyExcluded,
      snapshot.manuallyIncluded,
      snapshot.tier,
      snapshot.tierWeight,
      snapshot.lastPlayedAt,
      snapshot.lastSkippedAt,
      now,
    );
  }

  return snapshot;
}

export function rebuildArtistStatsFromEvents(db, { userPlexId, artistName, smartConfig }) {
  const existing = db.prepare(
    'SELECT * FROM artist_stats WHERE artist_name = ? AND user_plex_id = ?',
  ).get(artistName, userPlexId);
  const events = db.prepare(`
    SELECT id, started_at, ended_at, duration_ms, track_duration_ms, is_skip, event_source
    FROM play_events
    WHERE user_plex_id = ? AND artist_name = ?
    ORDER BY started_at ASC, id ASC
  `).all(userPlexId, artistName);

  if (!events.length) {
    if (existing && !existing.manually_excluded && !existing.manually_included) {
      db.prepare('DELETE FROM artist_stats WHERE artist_name = ? AND user_plex_id = ?').run(artistName, userPlexId);
    }
    return null;
  }

  const manualExcluded = Number(existing?.manually_excluded || 0);
  const manualIncluded = Number(existing?.manually_included || 0);
  let playCount = 0;
  let skipCount = 0;
  let consecutiveSkips = 0;
  let rankingScore = 5.0;
  let lastPlayedAt = null;
  let lastSkippedAt = null;

  for (const event of events) {
    const tier = _classifyReplayedEvent(event, smartConfig);
    const weight = _tierWeight(tier, smartConfig);
    const isSkip = tier === 'skip';
    const eventAt = _eventTime(event);
    rankingScore = Math.min(10, Math.max(0, rankingScore + weight));
    if (isSkip) {
      skipCount += 1;
      consecutiveSkips += 1;
      lastSkippedAt = eventAt;
    } else {
      playCount += 1;
      consecutiveSkips = 0;
      lastPlayedAt = eventAt;
    }
  }

  const now = Date.now();
  if (existing) {
    db.prepare(`
      UPDATE artist_stats SET
        play_count = ?, skip_count = ?, consecutive_skips = ?,
        excluded_from_smart = ?, manually_excluded = ?, manually_included = ?,
        ranking_score = ?, last_played_at = ?, last_skipped_at = ?, updated_at = ?
      WHERE artist_name = ? AND user_plex_id = ?
    `).run(
      playCount,
      skipCount,
      consecutiveSkips,
      manualExcluded ? 1 : 0,
      manualExcluded,
      manualIncluded,
      rankingScore,
      lastPlayedAt,
      lastSkippedAt,
      now,
      artistName,
      userPlexId,
    );
  } else {
    db.prepare(`
      INSERT INTO artist_stats
        (artist_name, user_plex_id, play_count, skip_count, consecutive_skips,
         excluded_from_smart, manually_excluded, manually_included,
         ranking_score, last_played_at, last_skipped_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      artistName,
      userPlexId,
      playCount,
      skipCount,
      consecutiveSkips,
      manualExcluded ? 1 : 0,
      manualExcluded,
      manualIncluded,
      rankingScore,
      lastPlayedAt,
      lastSkippedAt,
      now,
    );
  }

  return {
    playCount,
    skipCount,
    consecutiveSkips,
    rankingScore,
    lastPlayedAt,
    lastSkippedAt,
  };
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
    ? "WHERE ts.user_plex_id = ? AND LOWER(ts.artist_name) != 'various artists'"
    : "WHERE LOWER(ts.artist_name) != 'various artists'";
  const params = userPlexId ? [userPlexId, limit] : [limit];
  return db.prepare(`
    SELECT ts.plex_rating_key AS rating_key,
           ts.track_title, ts.artist_name, ts.album_name,
           ts.play_count AS total_plays,
           ts.skip_count AS total_skips,
           ts.consecutive_skips AS skip_streak,
           ts.excluded_from_smart AS excluded,
           ts.manually_excluded,
           ts.manually_included AS force_included,
           ts.tier,
           ts.last_played_at,
           COALESCE(mt.rating_count, 0) AS rating_count
    FROM track_stats ts
    LEFT JOIN master_tracks mt ON mt.rating_key = ts.plex_rating_key
    ${filter}
    ORDER BY ts.play_count DESC
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
           COALESCE(ts.manually_included, 0) AS current_force_included,
           COALESCE(mt.rating_count, 0) AS rating_count
    FROM play_events pe
    LEFT JOIN track_stats ts
      ON ts.user_plex_id = pe.user_plex_id
     AND ts.plex_rating_key = pe.plex_rating_key
    LEFT JOIN master_tracks mt
      ON mt.rating_key = pe.plex_rating_key
    ${filter}
    ORDER BY pe.started_at DESC
    LIMIT ? OFFSET ?
  `).all(...params);
}

export function getAlbumPopularTrackRanks(db, ratingKeys = []) {
  const keys = [...new Set((Array.isArray(ratingKeys) ? ratingKeys : [])
    .map((value) => String(value || '').trim())
    .filter(Boolean))];
  if (!keys.length) return new Map();
  const placeholders = keys.map(() => '?').join(', ');
  const rows = db.prepare(`
    WITH ranked AS (
      SELECT
        rating_key,
        rating_count,
        ROW_NUMBER() OVER (
          PARTITION BY LOWER(TRIM(artist_name)), LOWER(TRIM(album_name))
          ORDER BY rating_count DESC, view_count DESC, LOWER(TRIM(track_title)) ASC, rating_key ASC
        ) AS popularity_rank
      FROM master_tracks
      WHERE TRIM(album_name) != ''
        AND rating_count > 0
    )
    SELECT rating_key, rating_count, popularity_rank
    FROM ranked
    WHERE popularity_rank <= 3
      AND rating_key IN (${placeholders})
  `).all(...keys);
  return new Map(rows.map((row) => [
    String(row.rating_key || ''),
    {
      rank: Number(row.popularity_rank || 0),
      ratingCount: Number(row.rating_count || 0),
    },
  ]));
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
  const rows = db.prepare(`
    SELECT user_plex_id FROM play_events
    UNION
    SELECT user_plex_id FROM user_preferences WHERE user_wizard_completed = 1
  `).all();
  return [...new Set(rows.map((r) => r.user_plex_id))];
}

export function recordPlaylistSync(db, {
  userPlexId, plexPlaylistId, playlistTitle, trackCount, excludedTracks, excludedArtists, trigger,
  tracksAdded, tracksRemoved,
}) {
  return db.prepare(`
    INSERT INTO playlist_syncs
      (user_plex_id, plex_playlist_id, playlist_title, track_count,
       excluded_tracks, excluded_artists, trigger, synced_at, tracks_added, tracks_removed)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    userPlexId, plexPlaylistId, playlistTitle || '',
    trackCount || 0, excludedTracks || 0, excludedArtists || 0,
    trigger || 'auto', Date.now(),
    tracksAdded ?? 0, tracksRemoved ?? 0,
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
  if (!row) return {
    likedGenres: [],
    ignoredGenres: [],
    likedArtists: [],
    ignoredArtists: [],
    userWizardCompleted: false,
    smartConfig: null,
    lastfmUsername: '',
    lastfmSyncWatermark: 0,
    lastfmBackfillCursor: 0,
    lastfmEnabledStations: [],
    lastfmStrictMatchStations: [],
    lastfmStationSorts: {},
    lastfmStationFinalOrderings: {},
    listenbrainzUsername: '',
    listenbrainzToken: '',
    listenbrainzEnabledPlaylists: [],
    listenbrainzStrictMatchPlaylists: [],
    listenbrainzPlaylistSorts: {},
    listenbrainzPlaylistFinalOrderings: {},
    spotifyUserId: '',
    spotifyDisplayName: '',
    spotifyAccessToken: '',
    spotifyRefreshToken: '',
    spotifyTokenExpiresAt: 0,
  };
  return {
    likedGenres: JSON.parse(row.liked_genres || '[]'),
    ignoredGenres: JSON.parse(row.ignored_genres || '[]'),
    likedArtists: JSON.parse(row.liked_artists || '[]'),
    ignoredArtists: JSON.parse(row.ignored_artists || '[]'),
    userWizardCompleted: Boolean(row.user_wizard_completed),
    smartConfig: JSON.parse(row.smart_config || 'null'),
    lastfmUsername: String(row.lastfm_username || ''),
    lastfmSyncWatermark: Number(row.lastfm_sync_watermark || 0),
    lastfmBackfillCursor: Number(row.lastfm_backfill_cursor ?? 0),
    lastfmEnabledStations: JSON.parse(row.lastfm_enabled_stations || '[]'),
    lastfmStrictMatchStations: JSON.parse(row.lastfm_strict_match_stations || '[]'),
    lastfmStationSorts: JSON.parse(row.lastfm_station_sorts || '{}'),
    lastfmStationFinalOrderings: JSON.parse(row.lastfm_station_final_orderings || '{}'),
    listenbrainzUsername: String(row.listenbrainz_username || ''),
    listenbrainzToken: String(row.listenbrainz_token || ''),
    listenbrainzEnabledPlaylists: JSON.parse(row.listenbrainz_enabled_playlists || '[]'),
    listenbrainzStrictMatchPlaylists: JSON.parse(row.listenbrainz_strict_match_playlists || '[]'),
    listenbrainzPlaylistSorts: JSON.parse(row.listenbrainz_playlist_sorts || '{}'),
    listenbrainzPlaylistFinalOrderings: JSON.parse(row.listenbrainz_playlist_final_orderings || '{}'),
    spotifyUserId: String(row.spotify_user_id || ''),
    spotifyDisplayName: String(row.spotify_display_name || ''),
    spotifyAccessToken: String(row.spotify_access_token || ''),
    spotifyRefreshToken: String(row.spotify_refresh_token || ''),
    spotifyTokenExpiresAt: Number(row.spotify_token_expires_at || 0),
  };
}

export function saveUserPreferences(db, userPlexId, {
  likedGenres = [],
  ignoredGenres = [],
  likedArtists = [],
  ignoredArtists = [],
  userWizardCompleted = false,
  smartConfig = undefined,
  lastfmUsername = undefined,
  lastfmSyncWatermark = undefined,
  lastfmEnabledStations = undefined,
  lastfmStrictMatchStations = undefined,
  lastfmStationSorts = undefined,
  lastfmStationFinalOrderings = undefined,
  listenbrainzUsername = undefined,
  listenbrainzToken = undefined,
  listenbrainzEnabledPlaylists = undefined,
  listenbrainzStrictMatchPlaylists = undefined,
  listenbrainzPlaylistSorts = undefined,
  listenbrainzPlaylistFinalOrderings = undefined,
  spotifyUserId = undefined,
  spotifyDisplayName = undefined,
  spotifyAccessToken = undefined,
  spotifyRefreshToken = undefined,
  spotifyTokenExpiresAt = undefined,
}) {
  const existing = getUserPreferences(db, userPlexId);
  const resolvedSmartConfig = smartConfig !== undefined ? smartConfig : existing.smartConfig;
  const resolvedLastfmUsername = lastfmUsername !== undefined ? String(lastfmUsername).trim() : existing.lastfmUsername;
  const resolvedWatermark = lastfmSyncWatermark !== undefined ? Number(lastfmSyncWatermark) : existing.lastfmSyncWatermark;
  const resolvedStations = lastfmEnabledStations !== undefined ? lastfmEnabledStations : existing.lastfmEnabledStations;
  const resolvedStrictStations = lastfmStrictMatchStations !== undefined ? lastfmStrictMatchStations : existing.lastfmStrictMatchStations;
  const resolvedLastfmStationSorts = lastfmStationSorts !== undefined ? lastfmStationSorts : existing.lastfmStationSorts;
  const resolvedLastfmStationFinalOrderings = lastfmStationFinalOrderings !== undefined ? lastfmStationFinalOrderings : existing.lastfmStationFinalOrderings;
  const resolvedListenbrainzUsername = listenbrainzUsername !== undefined ? String(listenbrainzUsername).trim() : existing.listenbrainzUsername;
  const resolvedListenbrainzToken = listenbrainzToken !== undefined ? String(listenbrainzToken).trim() : existing.listenbrainzToken;
  const resolvedListenbrainzPlaylists = listenbrainzEnabledPlaylists !== undefined ? listenbrainzEnabledPlaylists : existing.listenbrainzEnabledPlaylists;
  const resolvedStrictPlaylists = listenbrainzStrictMatchPlaylists !== undefined ? listenbrainzStrictMatchPlaylists : existing.listenbrainzStrictMatchPlaylists;
  const resolvedListenbrainzPlaylistSorts = listenbrainzPlaylistSorts !== undefined ? listenbrainzPlaylistSorts : existing.listenbrainzPlaylistSorts;
  const resolvedListenbrainzPlaylistFinalOrderings = listenbrainzPlaylistFinalOrderings !== undefined ? listenbrainzPlaylistFinalOrderings : existing.listenbrainzPlaylistFinalOrderings;
  const resolvedSpotifyUserId = spotifyUserId !== undefined ? String(spotifyUserId).trim() : existing.spotifyUserId;
  const resolvedSpotifyDisplayName = spotifyDisplayName !== undefined ? String(spotifyDisplayName).trim() : existing.spotifyDisplayName;
  const resolvedSpotifyAccessToken = spotifyAccessToken !== undefined ? String(spotifyAccessToken).trim() : existing.spotifyAccessToken;
  const resolvedSpotifyRefreshToken = spotifyRefreshToken !== undefined ? String(spotifyRefreshToken).trim() : existing.spotifyRefreshToken;
  const resolvedSpotifyTokenExpiresAt = spotifyTokenExpiresAt !== undefined ? Number(spotifyTokenExpiresAt || 0) : existing.spotifyTokenExpiresAt;
  db.prepare(`
    INSERT INTO user_preferences (
      user_plex_id, liked_genres, ignored_genres, liked_artists, ignored_artists, user_wizard_completed,
      smart_config, lastfm_username, lastfm_sync_watermark, lastfm_enabled_stations, lastfm_strict_match_stations,
      lastfm_station_sorts, lastfm_station_final_orderings,
      listenbrainz_username, listenbrainz_token, listenbrainz_enabled_playlists, listenbrainz_strict_match_playlists,
      listenbrainz_playlist_sorts, listenbrainz_playlist_final_orderings,
      spotify_user_id, spotify_display_name, spotify_access_token, spotify_refresh_token, spotify_token_expires_at,
      updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_plex_id) DO UPDATE SET
      liked_genres = excluded.liked_genres,
      ignored_genres = excluded.ignored_genres,
      liked_artists = excluded.liked_artists,
      ignored_artists = excluded.ignored_artists,
      user_wizard_completed = excluded.user_wizard_completed,
      smart_config = excluded.smart_config,
      lastfm_username = excluded.lastfm_username,
      lastfm_sync_watermark = excluded.lastfm_sync_watermark,
      lastfm_enabled_stations = excluded.lastfm_enabled_stations,
      lastfm_strict_match_stations = excluded.lastfm_strict_match_stations,
      lastfm_station_sorts = excluded.lastfm_station_sorts,
      lastfm_station_final_orderings = excluded.lastfm_station_final_orderings,
      listenbrainz_username = excluded.listenbrainz_username,
      listenbrainz_token = excluded.listenbrainz_token,
      listenbrainz_enabled_playlists = excluded.listenbrainz_enabled_playlists,
      listenbrainz_strict_match_playlists = excluded.listenbrainz_strict_match_playlists,
      listenbrainz_playlist_sorts = excluded.listenbrainz_playlist_sorts,
      listenbrainz_playlist_final_orderings = excluded.listenbrainz_playlist_final_orderings,
      spotify_user_id = excluded.spotify_user_id,
      spotify_display_name = excluded.spotify_display_name,
      spotify_access_token = excluded.spotify_access_token,
      spotify_refresh_token = excluded.spotify_refresh_token,
      spotify_token_expires_at = excluded.spotify_token_expires_at,
      updated_at = excluded.updated_at
  `).run(
    userPlexId,
    JSON.stringify(likedGenres),
    JSON.stringify(ignoredGenres),
    JSON.stringify(likedArtists),
    JSON.stringify(ignoredArtists),
    userWizardCompleted ? 1 : 0,
    JSON.stringify(resolvedSmartConfig ?? null),
    resolvedLastfmUsername,
    resolvedWatermark,
    JSON.stringify(resolvedStations),
    JSON.stringify(resolvedStrictStations),
    JSON.stringify(resolvedLastfmStationSorts || {}),
    JSON.stringify(resolvedLastfmStationFinalOrderings || {}),
    resolvedListenbrainzUsername,
    resolvedListenbrainzToken,
    JSON.stringify(resolvedListenbrainzPlaylists),
    JSON.stringify(resolvedStrictPlaylists),
    JSON.stringify(resolvedListenbrainzPlaylistSorts || {}),
    JSON.stringify(resolvedListenbrainzPlaylistFinalOrderings || {}),
    resolvedSpotifyUserId,
    resolvedSpotifyDisplayName,
    resolvedSpotifyAccessToken,
    resolvedSpotifyRefreshToken,
    resolvedSpotifyTokenExpiresAt,
    Date.now(),
  );
}

export function updateLastfmBackfillCursor(db, userPlexId, cursor) {
  db.prepare('UPDATE user_preferences SET lastfm_backfill_cursor = ? WHERE user_plex_id = ?').run(cursor, userPlexId);
}

export function getLastfmUsers(db) {
  return db.prepare(`
    SELECT user_plex_id, lastfm_username, lastfm_sync_watermark, lastfm_backfill_cursor
    FROM user_preferences
    WHERE lastfm_username IS NOT NULL AND lastfm_username != ''
  `).all();
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
    INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name, recording_mbid, genres, moods, library_key, file_path, duration_ms, rating_count, view_count, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(rating_key) DO UPDATE SET
      artist_name = excluded.artist_name, track_title = excluded.track_title,
      album_name = excluded.album_name, recording_mbid = excluded.recording_mbid,
      genres = excluded.genres, moods = excluded.moods,
      library_key = excluded.library_key, file_path = excluded.file_path,
      duration_ms = excluded.duration_ms, rating_count = excluded.rating_count,
      view_count = excluded.view_count, updated_at = excluded.updated_at
  `);
  const upsertYear = db.prepare(`
    INSERT INTO track_enrichment (
      rating_key, recording_mbid, track_year, original_release_date, payload_json, analysis_source, updated_at
    ) VALUES (?, ?, ?, ?, '{}', 'media-server', ?)
    ON CONFLICT(rating_key) DO UPDATE SET
      recording_mbid = COALESCE(NULLIF(excluded.recording_mbid, ''), track_enrichment.recording_mbid),
      track_year = COALESCE(excluded.track_year, track_enrichment.track_year),
      original_release_date = COALESCE(NULLIF(excluded.original_release_date, ''), track_enrichment.original_release_date),
      updated_at = excluded.updated_at
  `);
  const run = db.transaction((rows) => {
    for (const r of rows) {
      upsert.run(
        r.ratingKey,
        r.artistName,
        r.trackTitle,
        r.albumName,
        String(r.recordingMbid || '').trim(),
        JSON.stringify(r.genres || []),
        JSON.stringify(r.moods || []),
        r.libraryKey,
        String(r.filePath || ''),
        Number(r.durationMs ?? 0),
        r.ratingCount ?? 0,
        r.viewCount ?? 0,
        Date.now(),
      );
      const trackYear = Number(r.trackYear || 0);
      const originalReleaseDate = String(r.originalReleaseDate || '').trim();
      if ((Number.isInteger(trackYear) && trackYear > 0) || originalReleaseDate) {
        upsertYear.run(
          r.ratingKey,
          String(r.recordingMbid || '').trim(),
          Number.isInteger(trackYear) && trackYear > 0 ? trackYear : null,
          originalReleaseDate,
          Date.now(),
        );
      }
    }
  });
  run(tracks);
}

export function getMasterTracks(db) {
  return db.prepare(`
    SELECT
      m.*,
      e.track_year,
      e.original_release_date,
      e.bpm,
      e.musical_key,
      e.camelot_key,
      e.energy,
      e.danceability,
      e.loudness,
      e.loudness_range,
      e.peak,
      e.track_gain,
      e.album_gain,
      e.album_peak,
      e.album_range,
      e.analysis_source,
      e.analysis_confidence
    FROM master_tracks m
    LEFT JOIN track_enrichment e ON e.rating_key = m.rating_key
  `).all().map((r) => ({
    ratingKey: r.rating_key,
    artistName: r.artist_name,
    trackTitle: r.track_title,
    albumName: r.album_name,
    genres: JSON.parse(r.genres || '[]'),
    moods: JSON.parse(r.moods || '[]'),
    libraryKey: r.library_key,
    filePath: String(r.file_path || ''),
    recordingMbid: String(r.recording_mbid || '').trim(),
    durationMs: Number(r.duration_ms || 0),
    ratingCount: r.rating_count,
    viewCount: r.view_count,
    trackYear: r.track_year == null ? null : Number(r.track_year || 0),
    originalReleaseDate: String(r.original_release_date || '').trim(),
    bpm: r.bpm == null ? null : Number(r.bpm),
    musicalKey: String(r.musical_key || '').trim(),
    camelotKey: String(r.camelot_key || '').trim(),
    energy: r.energy == null ? null : Number(r.energy),
    danceability: r.danceability == null ? null : Number(r.danceability),
    loudness: r.loudness == null ? null : Number(r.loudness),
    loudnessRange: r.loudness_range == null ? null : Number(r.loudness_range),
    peak: r.peak == null ? null : Number(r.peak),
    trackGain: r.track_gain == null ? null : Number(r.track_gain),
    albumGain: r.album_gain == null ? null : Number(r.album_gain),
    albumPeak: r.album_peak == null ? null : Number(r.album_peak),
    albumRange: r.album_range == null ? null : Number(r.album_range),
    enrichmentSource: String(r.analysis_source || '').trim(),
    enrichmentConfidence: Number(r.analysis_confidence || 0),
  }));
}

export function getDistinctPathSegments(db) {
  const rows = db.prepare(`SELECT DISTINCT file_path FROM master_tracks WHERE file_path != ''`).all();
  if (!rows.length) return [];

  // Normalise all paths and split into directory parts (strip filename)
  const allDirParts = rows.map(({ file_path }) =>
    file_path.replace(/\\/g, '/').split('/').filter(Boolean).slice(0, -1),
  );

  // Find the common root prefix depth shared by all paths
  let commonDepth = allDirParts[0].length;
  for (const parts of allDirParts) {
    let d = 0;
    while (d < commonDepth && d < parts.length && parts[d] === allDirParts[0][d]) d++;
    commonDepth = d;
  }

  // Collect all distinct sub-paths below the common root
  const subpaths = new Set();
  for (const parts of allDirParts) {
    const meaningful = parts.slice(commonDepth);
    for (let len = 1; len <= meaningful.length; len++) {
      subpaths.add(meaningful.slice(0, len).join('/'));
    }
  }

  return [...subpaths].sort((a, b) => a.localeCompare(b));
}

export function getDistinctLibraryKeys(db) {
  return db.prepare(`SELECT DISTINCT library_key FROM master_tracks WHERE library_key != '' ORDER BY library_key`).all().map((r) => ({ key: r.library_key }));
}

export function getMasterTrackCount(db) {
  return db.prepare('SELECT COUNT(*) as n FROM master_tracks').get().n;
}

export function getArtistMasterTrackCount(db, artistName) {
  const name = String(artistName || '').trim();
  if (!name) return 0;
  return db.prepare('SELECT COUNT(*) AS n FROM master_tracks WHERE LOWER(artist_name) = LOWER(?)').get(name)?.n ?? 0;
}

export function getMasterArtistCount(db) {
  return db.prepare(`
    SELECT COUNT(DISTINCT LOWER(TRIM(artist_name))) AS n
    FROM master_tracks
    WHERE TRIM(artist_name) != ''
      AND LOWER(TRIM(artist_name)) NOT IN ('various artists', 'va', 'v/a', 'unknown')
  `).get().n;
}

function parseJsonObject(value, fallback = {}) {
  try {
    const parsed = JSON.parse(value || '{}');
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : fallback;
  } catch {
    return fallback;
  }
}

function normalizeTrackEnrichmentRow(row) {
  if (!row) return null;
  return {
    ratingKey: String(row.rating_key || ''),
    recordingMbid: String(row.recording_mbid || '').trim(),
    trackYear: row.track_year == null ? null : Number(row.track_year || 0),
    originalReleaseDate: String(row.original_release_date || '').trim(),
    bpm: row.bpm == null ? null : Number(row.bpm),
    musicalKey: String(row.musical_key || '').trim(),
    camelotKey: String(row.camelot_key || '').trim(),
    energy: row.energy == null ? null : Number(row.energy),
    danceability: row.danceability == null ? null : Number(row.danceability),
    loudness: row.loudness == null ? null : Number(row.loudness),
    loudnessRange: row.loudness_range == null ? null : Number(row.loudness_range),
    peak: row.peak == null ? null : Number(row.peak),
    trackGain: row.track_gain == null ? null : Number(row.track_gain),
    albumGain: row.album_gain == null ? null : Number(row.album_gain),
    albumPeak: row.album_peak == null ? null : Number(row.album_peak),
    albumRange: row.album_range == null ? null : Number(row.album_range),
    analysisSource: String(row.analysis_source || '').trim(),
    analysisConfidence: Number(row.analysis_confidence || 0),
    payload: parseJsonObject(row.payload_json, {}),
    updatedAt: Number(row.updated_at || 0),
  };
}

export function upsertTrackEnrichment(db, entries) {
  const items = Array.isArray(entries) ? entries : [entries];
  const upsert = db.prepare(`
    INSERT INTO track_enrichment (
      rating_key, recording_mbid, track_year, original_release_date, bpm,
      musical_key, camelot_key, energy, danceability, loudness, loudness_range,
      peak, track_gain, album_gain, album_peak, album_range, analysis_source,
      analysis_confidence, payload_json, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(rating_key) DO UPDATE SET
      recording_mbid = excluded.recording_mbid,
      track_year = excluded.track_year,
      original_release_date = excluded.original_release_date,
      bpm = excluded.bpm,
      musical_key = excluded.musical_key,
      camelot_key = excluded.camelot_key,
      energy = excluded.energy,
      danceability = excluded.danceability,
      loudness = excluded.loudness,
      loudness_range = excluded.loudness_range,
      peak = excluded.peak,
      track_gain = excluded.track_gain,
      album_gain = excluded.album_gain,
      album_peak = excluded.album_peak,
      album_range = excluded.album_range,
      analysis_source = excluded.analysis_source,
      analysis_confidence = excluded.analysis_confidence,
      payload_json = excluded.payload_json,
      updated_at = excluded.updated_at
  `);
  const run = db.transaction((rows) => {
    for (const row of rows) {
      if (!row || !row.ratingKey) continue;
      upsert.run(
        String(row.ratingKey || ''),
        String(row.recordingMbid || '').trim(),
        row.trackYear == null ? null : Number(row.trackYear),
        String(row.originalReleaseDate || '').trim(),
        row.bpm == null ? null : Number(row.bpm),
        String(row.musicalKey || '').trim(),
        String(row.camelotKey || '').trim(),
        row.energy == null ? null : Number(row.energy),
        row.danceability == null ? null : Number(row.danceability),
        row.loudness == null ? null : Number(row.loudness),
        row.loudnessRange == null ? null : Number(row.loudnessRange),
        row.peak == null ? null : Number(row.peak),
        row.trackGain == null ? null : Number(row.trackGain),
        row.albumGain == null ? null : Number(row.albumGain),
        row.albumPeak == null ? null : Number(row.albumPeak),
        row.albumRange == null ? null : Number(row.albumRange),
        String(row.analysisSource || '').trim(),
        Number(row.analysisConfidence || 0),
        JSON.stringify(row.payload && typeof row.payload === 'object' ? row.payload : {}),
        Number(row.updatedAt || Date.now()),
      );
    }
  });
  run(items);
}

export function getTrackEnrichmentByRatingKeys(db, ratingKeys) {
  const keys = Array.isArray(ratingKeys)
    ? ratingKeys.map((value) => String(value || '').trim()).filter(Boolean)
    : [];
  if (!keys.length) return [];
  const placeholders = keys.map(() => '?').join(', ');
  return db.prepare(`SELECT * FROM track_enrichment WHERE rating_key IN (${placeholders})`).all(...keys)
    .map(normalizeTrackEnrichmentRow)
    .filter(Boolean);
}

export function listTracksMissingEnrichment(db, options = {}) {
  const limit = Math.max(1, Math.min(500, Number(options.limit || 100)));
  const requireRecordingMbid = options.requireRecordingMbid !== false;
  const where = requireRecordingMbid
    ? `WHERE m.recording_mbid != '' AND e.rating_key IS NULL`
    : `WHERE e.rating_key IS NULL`;
  return db.prepare(`
    SELECT
      m.rating_key,
      m.artist_name,
      m.track_title,
      m.album_name,
      m.recording_mbid,
      m.library_key,
      m.updated_at
    FROM master_tracks m
    LEFT JOIN track_enrichment e ON e.rating_key = m.rating_key
    ${where}
    ORDER BY m.updated_at DESC, m.rating_key ASC
    LIMIT ?
  `).all(limit).map((row) => ({
    ratingKey: String(row.rating_key || ''),
    artistName: String(row.artist_name || ''),
    trackTitle: String(row.track_title || ''),
    albumName: String(row.album_name || ''),
    recordingMbid: String(row.recording_mbid || '').trim(),
    libraryKey: String(row.library_key || ''),
    updatedAt: Number(row.updated_at || 0),
  }));
}

export function countTracksMissingEnrichment(db, options = {}) {
  const requireRecordingMbid = options.requireRecordingMbid !== false;
  const where = requireRecordingMbid
    ? `WHERE m.recording_mbid != '' AND e.rating_key IS NULL`
    : `WHERE e.rating_key IS NULL`;
  return Number(db.prepare(`
    SELECT COUNT(*) AS n
    FROM master_tracks m
    LEFT JOIN track_enrichment e ON e.rating_key = m.rating_key
    ${where}
  `).get()?.n || 0);
}

export function listTracksMissingPlexLoudness(db, options = {}) {
  const limit = Math.max(1, Math.min(500, Number(options.limit || 100)));
  const rawCursorUpdatedAt = options.cursorUpdatedAt;
  const cursorUpdatedAt = rawCursorUpdatedAt == null || rawCursorUpdatedAt === ''
    ? null
    : (Number.isFinite(Number(rawCursorUpdatedAt)) ? Number(rawCursorUpdatedAt) : null);
  const cursorRatingKey = String(options.cursorRatingKey || '');
  return db.prepare(`
    SELECT
      m.rating_key,
      m.artist_name,
      m.track_title,
      m.album_name,
      m.recording_mbid,
      m.library_key,
      m.updated_at
    FROM master_tracks m
    LEFT JOIN track_enrichment e ON e.rating_key = m.rating_key
    WHERE (
         e.rating_key IS NULL
      OR e.loudness IS NULL
      OR e.loudness_range IS NULL
      OR e.peak IS NULL
    )
      AND lower(trim(m.track_title)) NOT IN ('[silence]', 'silence')
      AND (
        ? IS NULL
        OR m.updated_at < ?
        OR (m.updated_at = ? AND m.rating_key > ?)
      )
    ORDER BY m.updated_at DESC, m.rating_key ASC
    LIMIT ?
  `).all(cursorUpdatedAt, cursorUpdatedAt, cursorUpdatedAt, cursorRatingKey, limit).map((row) => ({
    ratingKey: String(row.rating_key || ''),
    artistName: String(row.artist_name || ''),
    trackTitle: String(row.track_title || ''),
    albumName: String(row.album_name || ''),
    recordingMbid: String(row.recording_mbid || '').trim(),
    libraryKey: String(row.library_key || ''),
    updatedAt: Number(row.updated_at || 0),
  }));
}

export function countTracksMissingPlexLoudness(db) {
  return Number(db.prepare(`
    SELECT COUNT(*) AS n
    FROM master_tracks m
    LEFT JOIN track_enrichment e ON e.rating_key = m.rating_key
    WHERE (
         e.rating_key IS NULL
      OR e.loudness IS NULL
      OR e.loudness_range IS NULL
      OR e.peak IS NULL
    )
      AND lower(trim(m.track_title)) NOT IN ('[silence]', 'silence')
  `).get()?.n || 0);
}

export function mergeMasterTracksWithEnrichment(tracks, enrichmentRows = []) {
  const enrichByKey = new Map((Array.isArray(enrichmentRows) ? enrichmentRows : [])
    .map((row) => [String(row?.ratingKey || ''), row]));
  return (Array.isArray(tracks) ? tracks : []).map((track) => {
    const enrichment = enrichByKey.get(String(track?.ratingKey || '')) || null;
    return enrichment ? { ...track, enrichment } : { ...track };
  });
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

export function listImportedPlaylistUnmatched(db, userId, playlistKey) {
  return db.prepare(`
    SELECT id, source_track_id, position, track_title, artist_name, artists_json, album_title, album_type, album_image_url, duration_ms, selected
    FROM imported_playlist_unmatched
    WHERE user_plex_id = ? AND playlist_key = ?
    ORDER BY position ASC, artist_name COLLATE NOCASE ASC, track_title COLLATE NOCASE ASC, id ASC
  `).all(userId, playlistKey).map((row) => {
    let artists = [];
    try { artists = JSON.parse(row.artists_json || '[]'); } catch { artists = []; }
    return {
      id: Number(row.id || 0),
      sourceTrackId: String(row.source_track_id || '').trim(),
      position: Number(row.position || 0),
      title: String(row.track_title || '').trim(),
      artistName: String(row.artist_name || '').trim(),
      artists: Array.isArray(artists) ? artists.map((artist) => String(artist || '').trim()).filter(Boolean) : [],
      albumTitle: String(row.album_title || '').trim(),
      albumType: String(row.album_type || '').trim(),
      albumImageUrl: String(row.album_image_url || '').trim(),
      durationMs: Number(row.duration_ms || 0),
      selected: Boolean(row.selected),
    };
  });
}

export function setImportedPlaylistUnmatched(db, userId, playlistKey, rows) {
  const del = db.prepare('DELETE FROM imported_playlist_unmatched WHERE user_plex_id = ? AND playlist_key = ?');
  const ins = db.prepare(`
    INSERT INTO imported_playlist_unmatched (
      playlist_key, user_plex_id, source_track_id, position, track_title,
      artist_name, artists_json, album_title, album_type, album_image_url, duration_ms,
      selected, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const now = Date.now();
  db.transaction(() => {
    del.run(userId, playlistKey);
    for (const row of (Array.isArray(rows) ? rows : [])) {
      const artists = Array.isArray(row?.artists) ? row.artists.map((artist) => String(artist || '').trim()).filter(Boolean) : [];
      ins.run(
        playlistKey,
        userId,
        String(row?.sourceTrackId || '').trim(),
        Number(row?.position || 0),
        String(row?.title || '').trim(),
        String(row?.artistName || artists[0] || '').trim(),
        JSON.stringify(artists),
        String(row?.albumTitle || '').trim(),
        String(row?.albumType || '').trim(),
        String(row?.albumImageUrl || '').trim(),
        Number(row?.durationMs || 0),
        row?.selected === false ? 0 : 1,
        now,
        now,
      );
    }
  })();
}

export function setImportedPlaylistUnmatchedSelection(db, userId, playlistKey, { ids = [], selected = true, artistName = '', selectAll = false } = {}) {
  const now = Date.now();
  if (selectAll) {
    db.prepare('UPDATE imported_playlist_unmatched SET selected = ?, updated_at = ? WHERE user_plex_id = ? AND playlist_key = ?')
      .run(selected ? 1 : 0, now, userId, playlistKey);
    return;
  }
  const safeArtistName = String(artistName || '').trim();
  if (safeArtistName) {
    db.prepare('UPDATE imported_playlist_unmatched SET selected = ?, updated_at = ? WHERE user_plex_id = ? AND playlist_key = ? AND artist_name = ?')
      .run(selected ? 1 : 0, now, userId, playlistKey, safeArtistName);
    return;
  }
  const safeIds = Array.isArray(ids) ? ids.map((value) => Number(value || 0)).filter((value) => Number.isFinite(value) && value > 0) : [];
  if (!safeIds.length) return;
  const placeholders = safeIds.map(() => '?').join(',');
  db.prepare(`UPDATE imported_playlist_unmatched SET selected = ?, updated_at = ? WHERE user_plex_id = ? AND playlist_key = ? AND id IN (${placeholders})`)
    .run(selected ? 1 : 0, now, userId, playlistKey, ...safeIds);
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
  // Jellyfin may store genres as semicolon-joined strings; split and deduplicate
  const genres = new Set();
  for (const row of rows) {
    if (!row.value) continue;
    for (const g of String(row.value).split(';').map((s) => s.trim()).filter(Boolean)) {
      if (/^\d+$/.test(g)) continue; // skip bare numeric IDs
      genres.add(g);
    }
  }
  return [...genres].sort();
}

export function getMoodsFromMaster(db) {
  const rows = db.prepare('SELECT DISTINCT value FROM master_tracks, json_each(master_tracks.moods) ORDER BY value').all();
  return rows.map((r) => r.value).filter(Boolean);
}

// ─── Artist tags (Last.fm) ────────────────────────────────────────────────────

export function saveArtistTags(db, artistName, tags) {
  db.prepare(`
    INSERT INTO artist_tags (artist_name, tags, updated_at)
    VALUES (?, ?, ?)
    ON CONFLICT(artist_name) DO UPDATE SET tags = excluded.tags, updated_at = excluded.updated_at
  `).run(artistName, JSON.stringify(tags || []), Date.now());
}

export function getArtistTagMap(db) {
  return new Map(
    db.prepare('SELECT artist_name, tags FROM artist_tags').all()
      .map((r) => [r.artist_name.toLowerCase(), JSON.parse(r.tags || '[]')]),
  );
}

function normalizeTagValue(value) {
  return String(value || '').trim().toLowerCase();
}

function resolveTrackYearValue(track = {}) {
  const direct = Number(track?.trackYear ?? track?.track_year ?? 0);
  if (Number.isInteger(direct) && direct > 0) return direct;
  const releaseDate = String(track?.originalReleaseDate || track?.original_release_date || '').trim();
  const match = releaseDate.match(/^(\d{4})/);
  return match ? Number(match[1]) : 0;
}

export function getTrackDecadeTag(track = {}) {
  const year = resolveTrackYearValue(track);
  if (!Number.isInteger(year) || year < 1900 || year > 2099) return '';
  return `${Math.floor(year / 10) * 10}s`;
}

export function getAllTrackDecadeTags(db) {
  const rows = db.prepare(`
    SELECT DISTINCT year FROM (
      SELECT track_year AS year
      FROM track_enrichment
      WHERE track_year IS NOT NULL
      UNION
      SELECT CAST(substr(original_release_date, 1, 4) AS INTEGER) AS year
      FROM track_enrichment
      WHERE original_release_date GLOB '[0-9][0-9][0-9][0-9]*'
    )
    WHERE year BETWEEN 1900 AND 2099
    ORDER BY year
  `).all();
  return [...new Set(rows.map((row) => getTrackDecadeTag({ trackYear: Number(row.year || 0) })).filter(Boolean))].sort();
}

export function getEffectiveTrackTags(track = {}, artistTagMap = new Map()) {
  const artistKey = String(track?.artistName || track?.artist_name || '').trim().toLowerCase();
  return (artistTagMap?.get?.(artistKey) || [])
    .map(normalizeTagValue)
    .filter(Boolean);
}

export function getAllLastfmTags(db) {
  const rows = db.prepare('SELECT DISTINCT value FROM artist_tags, json_each(artist_tags.tags) ORDER BY value').all();
  return [...new Set(rows.map((r) => normalizeTagValue(r.value)).filter(Boolean))].sort();
}

export function getLastfmTagSyncStats(db) {
  const total = db.prepare('SELECT COUNT(*) as n FROM artist_tags').get().n;
  const withTags = db.prepare("SELECT COUNT(*) as n FROM artist_tags WHERE tags != '[]'").get().n;
  return { total, withTags };
}

// ─── User personal playlists ──────────────────────────────────────────────────

function parsePersonalPlaylist(r) {
  let trackFilters = null;
  try { trackFilters = JSON.parse(r.track_filters || 'null'); } catch { trackFilters = null; }
  return {
    id: r.id,
    userPlexId: r.user_plex_id,
    name: r.name,
    rules: JSON.parse(r.rules || '{}'),
    trackFilters,
    enabled: Boolean(r.enabled),
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

export function listUserPersonalPlaylists(db, userPlexId) {
  return db.prepare('SELECT * FROM user_personal_playlists WHERE user_plex_id = ? ORDER BY created_at ASC').all(userPlexId).map(parsePersonalPlaylist);
}

export function getUserPersonalPlaylist(db, id, userPlexId) {
  const row = db.prepare('SELECT * FROM user_personal_playlists WHERE id = ? AND user_plex_id = ?').get(id, userPlexId);
  return row ? parsePersonalPlaylist(row) : null;
}

export function findUserPersonalPlaylistByName(db, userPlexId, name, { excludeId = '' } = {}) {
  const normalizedName = String(name || '').trim();
  if (!normalizedName) return null;
  const excludedId = String(excludeId || '').trim();
  const row = excludedId
    ? db.prepare(`
      SELECT * FROM user_personal_playlists
      WHERE user_plex_id = ?
        AND LOWER(TRIM(name)) = LOWER(TRIM(?))
        AND id != ?
      LIMIT 1
    `).get(userPlexId, normalizedName, excludedId)
    : db.prepare(`
      SELECT * FROM user_personal_playlists
      WHERE user_plex_id = ?
        AND LOWER(TRIM(name)) = LOWER(TRIM(?))
      LIMIT 1
    `).get(userPlexId, normalizedName);
  return row ? parsePersonalPlaylist(row) : null;
}

export function createUserPersonalPlaylist(db, userPlexId, { id, name, rules, trackFilters = null }) {
  db.prepare(`
    INSERT INTO user_personal_playlists (id, user_plex_id, name, rules, track_filters, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(id, userPlexId, name, JSON.stringify(rules || {}), JSON.stringify(trackFilters), Date.now(), Date.now());
}

export function updateUserPersonalPlaylist(db, userPlexId, { id, name, rules, trackFilters = null }) {
  db.prepare('UPDATE user_personal_playlists SET name = ?, rules = ?, track_filters = ?, updated_at = ? WHERE id = ? AND user_plex_id = ?')
    .run(name, JSON.stringify(rules || {}), JSON.stringify(trackFilters), Date.now(), id, userPlexId);
}

export function deleteUserPersonalPlaylist(db, id, userPlexId) {
  db.prepare('DELETE FROM user_personal_playlists WHERE id = ? AND user_plex_id = ?').run(id, userPlexId);
}

export function deleteUserGeneratedPlaylist(db, userPlexId, playlistKey) {
  const deleteGenerated = db.prepare('DELETE FROM user_generated_playlists WHERE user_plex_id = ? AND playlist_key = ?');
  const deleteTracks = db.prepare('DELETE FROM playlist_tracks WHERE user_plex_id = ? AND playlist_key = ?');
  const deleteImportedUnmatched = db.prepare('DELETE FROM imported_playlist_unmatched WHERE user_plex_id = ? AND playlist_key = ?');
  const deleteArtistState = db.prepare('DELETE FROM playlist_artist_state WHERE user_plex_id = ? AND playlist_key = ?');
  db.transaction(() => {
    deleteTracks.run(userPlexId, playlistKey);
    deleteImportedUnmatched.run(userPlexId, playlistKey);
    deleteArtistState.run(userPlexId, playlistKey);
    deleteGenerated.run(userPlexId, playlistKey);
  })();
}

export function cleanMasterArtistName(value) {
  let name = String(value || '').trim();
  if (!name) return '';
  name = name.replace(/\s+(f\/|feat\.?|featuring|ft\.?|with)\s+.+$/i, '').trim();
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
  const hasLidarrArtistId = Object.prototype.hasOwnProperty.call(extra, 'lidarrArtistId');
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
      lidarr_artist_id = CASE WHEN ? THEN ? ELSE lidarr_artist_id END,
      accepted_at = ?,
      dismissed_at = ?,
      last_evaluated_at = ?
    WHERE user_plex_id = ? AND artist_name = ?
  `).run(
    nextStatus,
    nextReason,
    hasLidarrArtistId ? 1 : 0,
    hasLidarrArtistId ? (extra.lidarrArtistId ?? null) : null,
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
    titleOverride: row.title_override,
    sourceType: row.source_type,
    sourceRef: row.source_ref,
    sourceTitle: row.source_title,
    sourceOwner: row.source_owner,
    importedSyncPeriod: String(row.imported_sync_period || 'disabled').trim() || 'disabled',
    algorithmVersion: row.algorithm_version,
    lastBuiltAt: row.last_built_at,
    lastSyncedAt: row.last_synced_at,
    trackCount: Number(row.track_count || 0),
    missingCount: Number(row.missing_count || 0),
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
  const existing = db.prepare(`
    SELECT imported_sync_period
    FROM user_generated_playlists
    WHERE user_plex_id = ? AND playlist_key = ?
  `).get(userPlexId, playlistKey);
  const rawImportedSyncPeriod = Object.prototype.hasOwnProperty.call(playlist || {}, 'importedSyncPeriod')
    ? String(playlist?.importedSyncPeriod || '').trim().toLowerCase()
    : String(existing?.imported_sync_period || 'disabled').trim().toLowerCase();
  const importedSyncPeriod = ['disabled', 'daily', 'weekly', 'monthly'].includes(rawImportedSyncPeriod)
    ? rawImportedSyncPeriod
    : 'disabled';
  db.prepare(`
    INSERT INTO user_generated_playlists (
      user_plex_id, playlist_type, playlist_key, plex_playlist_id,
      playlist_title, title_override, source_type, source_ref, source_title, source_owner, imported_sync_period,
      algorithm_version, last_built_at, last_synced_at,
      track_count, missing_count, active, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_plex_id, playlist_key) DO UPDATE SET
      playlist_type = excluded.playlist_type,
      plex_playlist_id = excluded.plex_playlist_id,
      playlist_title = excluded.playlist_title,
      title_override = excluded.title_override,
      source_type = excluded.source_type,
      source_ref = excluded.source_ref,
      source_title = excluded.source_title,
      source_owner = excluded.source_owner,
      imported_sync_period = excluded.imported_sync_period,
      algorithm_version = excluded.algorithm_version,
      last_built_at = COALESCE(excluded.last_built_at, user_generated_playlists.last_built_at),
      last_synced_at = COALESCE(excluded.last_synced_at, user_generated_playlists.last_synced_at),
      track_count = excluded.track_count,
      missing_count = excluded.missing_count,
      active = excluded.active,
      updated_at = excluded.updated_at
  `).run(
    userPlexId,
    String(playlist?.playlistType || 'curatorred'),
    playlistKey,
    String(playlist?.plexPlaylistId || ''),
    String(playlist?.playlistTitle || ''),
    String(playlist?.titleOverride || ''),
    String(playlist?.sourceType || ''),
    String(playlist?.sourceRef || ''),
    String(playlist?.sourceTitle || ''),
    String(playlist?.sourceOwner || ''),
    importedSyncPeriod,
    String(playlist?.algorithmVersion || 'phase2a'),
    playlist?.lastBuiltAt ?? null,
    playlist?.lastSyncedAt ?? null,
    Number(playlist?.trackCount || 0),
    Number(playlist?.missingCount || 0),
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
  const hasLidarrArtistId = Object.prototype.hasOwnProperty.call(artist || {}, 'lidarrArtistId');
  db.prepare(`
    INSERT INTO lidarr_artist_progress (
      user_plex_id, artist_name, lidarr_artist_id, current_stage,
      albums_added_count, last_album_added_at, next_review_at,
      highest_observed_rank, last_manual_search_at, last_manual_search_status,
      created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_plex_id, artist_name) DO UPDATE SET
      lidarr_artist_id = CASE WHEN ? THEN excluded.lidarr_artist_id ELSE lidarr_artist_progress.lidarr_artist_id END,
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
    hasLidarrArtistId ? 1 : 0,
  );
}

export function deleteLidarrArtistProgress(db, userPlexId, artistName) {
  const existing = getLidarrArtistProgress(db, userPlexId, artistName);
  if (!existing) return null;
  db.prepare(`
    DELETE FROM lidarr_artist_progress
    WHERE user_plex_id = ? AND artist_name = ?
  `).run(userPlexId, artistName);
  return existing;
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
  const where = [];
  const params = [];
  const normalizedUserPlexId = String(userPlexId || '').trim();
  if (normalizedUserPlexId) {
    where.push('user_plex_id = ?');
    params.push(normalizedUserPlexId);
  }
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
    ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
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
  if (!['queued', 'processing', 'failed', 'completed'].includes(String(existing.status || ''))) return existing;
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
// rules: { artistTiers, trackTiers, topNPerArtist: number|null, maxTracks: number|null }
// smartSettings: { artistSkipRank, artistBelterRank }
function normalizeAlbumPopularityMode(value) {
  const normalized = String(value || '').trim();
  if (normalized === 'top3Only' || normalized === 'excludeTop3') return normalized;
  return 'all';
}

function normalizePopularityMode(value) {
  const normalized = String(value || '').trim();
  if (['top50', 'top25', 'top10', 'top5', 'custom'].includes(normalized)) return normalized;
  return 'all';
}

function normalizePopularityPercent(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.max(1, Math.min(100, Math.round(num)));
}

function resolvePopularityPercent(mode, value) {
  const normalized = normalizePopularityMode(mode);
  if (normalized === 'top50') return 50;
  if (normalized === 'top25') return 25;
  if (normalized === 'top10') return 10;
  if (normalized === 'top5') return 5;
  if (normalized === 'custom') return normalizePopularityPercent(value);
  return null;
}

function albumPopularityGroupKey(track) {
  const artistKey = String(track?.artistName || '').trim().toLowerCase();
  const albumKey = String(track?.albumName || '').trim().toLowerCase();
  return artistKey && albumKey ? `${artistKey}|${albumKey}` : '';
}

function buildAlbumPopularTopTrackKeySet(tracks, limit = 3) {
  const groups = new Map();
  for (const track of tracks || []) {
    const groupKey = albumPopularityGroupKey(track);
    const ratingKey = String(track?.ratingKey || '').trim();
    if (!groupKey || !ratingKey) continue;
    if (!groups.has(groupKey)) groups.set(groupKey, []);
    groups.get(groupKey).push(track);
  }
  const keepKeys = new Set();
  for (const groupTracks of groups.values()) {
    groupTracks
      .slice()
      .sort((a, b) => (Number(b?.ratingCount || 0) - Number(a?.ratingCount || 0))
        || (Number(b?.viewCount || 0) - Number(a?.viewCount || 0))
        || String(a?.trackTitle || '').localeCompare(String(b?.trackTitle || ''))
        || String(a?.ratingKey || '').localeCompare(String(b?.ratingKey || '')))
      .slice(0, Math.max(1, Number(limit) || 3))
      .forEach((track) => {
        if (Number(track?.ratingCount || 0) > 0) keepKeys.add(String(track.ratingKey || ''));
      });
  }
  return keepKeys;
}

function applyAbsolutePopularityMode(tracks, mode, percentValue) {
  const percent = resolvePopularityPercent(mode, percentValue);
  if (!percent || percent >= 100) return tracks;
  const sorted = (tracks || []).slice().sort((a, b) => (Number(b?.ratingCount || b?.rc || 0) - Number(a?.ratingCount || a?.rc || 0))
    || (Number(b?.viewCount || 0) - Number(a?.viewCount || 0))
    || String(a?.trackTitle || '').localeCompare(String(b?.trackTitle || ''))
    || String(a?.ratingKey || '').localeCompare(String(b?.ratingKey || '')));
  const keepCount = Math.max(1, Math.ceil(sorted.length * (percent / 100)));
  const keepKeys = new Set(sorted.slice(0, keepCount).map((track) => String(track?.ratingKey || '')).filter(Boolean));
  return tracks.filter((track) => keepKeys.has(String(track?.ratingKey || '')));
}

function shufflePreviewTracks(tracks = []) {
  const items = Array.isArray(tracks) ? [...tracks] : [];
  for (let i = items.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [items[i], items[j]] = [items[j], items[i]];
  }
  return items;
}

function previewTrackComparator(sortBy) {
  return (a, b) => {
    if (sortBy === 'tierWeight') return (Number(b?.tw || 0) - Number(a?.tw || 0)) || (Number(b?.rc || 0) - Number(a?.rc || 0));
    if (sortBy === 'playCount') return (Number(b?.pc || 0) - Number(a?.pc || 0)) || (Number(b?.rc || 0) - Number(a?.rc || 0));
    return (Number(b?.rc || 0) - Number(a?.rc || 0))
      || (Number(b?.tw || 0) - Number(a?.tw || 0))
      || String(a?.artistName || '').localeCompare(String(b?.artistName || ''))
      || String(a?.trackTitle || '').localeCompare(String(b?.trackTitle || ''));
  };
}

function selectPreviewTracks(tracks = [], { sortBy = 'ratingCount', topNPerArtist = null, maxTracksPerAlbum = null, maxTracks = null } = {}) {
  const artistLimit = topNPerArtist ? Math.max(1, Number(topNPerArtist)) : null;
  const albumLimit = maxTracksPerAlbum ? Math.max(1, Number(maxTracksPerAlbum)) : null;
  const trackLimit = maxTracks ? Math.max(1, Number(maxTracks)) : null;
  const orderedTracks = sortBy === 'random'
    ? shufflePreviewTracks(tracks)
    : [...tracks].sort(previewTrackComparator(sortBy));
  const artistCounts = new Map();
  const albumCounts = new Map();
  const selected = [];
  for (const track of orderedTracks) {
    const artistKey = String(track?.artistName || '').trim().toLowerCase();
    if (artistLimit && artistKey && (artistCounts.get(artistKey) || 0) >= artistLimit) continue;
    const albumKey = String(track?.albumName || '').trim().toLowerCase();
    if (albumLimit && albumKey && (albumCounts.get(albumKey) || 0) >= albumLimit) continue;
    selected.push(track);
    if (artistLimit && artistKey) artistCounts.set(artistKey, (artistCounts.get(artistKey) || 0) + 1);
    if (albumLimit && albumKey) albumCounts.set(albumKey, (albumCounts.get(albumKey) || 0) + 1);
    if (trackLimit && selected.length >= trackLimit) break;
  }
  return selected;
}

export function previewGlobalPlaylist(db, rules, userIdFilter, smartSettings, filteredMasterTracks) {
  const masterTracks = filteredMasterTracks ?? getMasterTracks(db);
  const albumPopularityMode = normalizeAlbumPopularityMode(rules?.albumPopularityMode);
  const popularityMode = normalizePopularityMode(rules?.popularityMode);
  const albumPopularTrackKeys = albumPopularityMode === 'all'
    ? null
    : buildAlbumPopularTopTrackKeySet(getMasterTracks(db), 3);
  const artistTagMap = getArtistTagMap(db);
  const skipRank = Number(smartSettings?.artistSkipRank ?? 2);
  const belterRank = Number(smartSettings?.artistBelterRank ?? 8);
  const sortBy = String(rules?.sortBy || 'ratingCount');

  function classifyArtist(score) {
    if (score === null || score === undefined) return 'unranked';
    if (score >= belterRank) return 'belter';
    if (score >= 5) return 'decent';
    if (score > skipRank) return 'halfDecent';
    return 'skip';
  }

  const artistTierFilter = parseTriStateFilter(rules?.artistTiers);
  const trackTierFilter  = parseTriStateFilter(rules?.trackTiers);
  const gf = parseTriStateFilter(rules?.genres);
  const mf = parseTriStateFilter(rules?.moods);
  const tf = parseTriStateFilter(rules?.tags);
  const df = parseTriStateFilter(rules?.decades);
  const topN = rules?.topNPerArtist ? Math.max(1, Number(rules.topNPerArtist)) : null;
  const maxT = rules?.maxTracks     ? Math.max(1, Number(rules.maxTracks))     : null;
  const maxPerAlbum = rules?.maxTracksPerAlbum ? Math.max(1, Number(rules.maxTracksPerAlbum)) : null;

  function matchesSeasonalRule(track) {
    const seasonalGenres = Array.isArray(rules?.seasonalGenres) ? rules.seasonalGenres.filter(Boolean) : [];
    const seasonalKeywords = Array.isArray(rules?.seasonalKeywords) ? rules.seasonalKeywords.filter(Boolean) : [];
    const seasonalExcludeGenres = Array.isArray(rules?.seasonalExcludeGenres) ? rules.seasonalExcludeGenres.filter(Boolean) : [];
    const seasonalExcludeKeywords = Array.isArray(rules?.seasonalExcludeKeywords) ? rules.seasonalExcludeKeywords.filter(Boolean) : [];
    const seasonalGenresMode = rules?.seasonalGenresMode === 'all' ? 'all' : 'any';
    const seasonalKeywordsMode = rules?.seasonalKeywordsMode === 'all' ? 'all' : 'any';
    if (!seasonalGenres.length && !seasonalKeywords.length && !seasonalExcludeGenres.length && !seasonalExcludeKeywords.length) return true;

    const genreMatch = seasonalGenres.length
      ? (seasonalGenresMode === 'all'
        ? seasonalGenres.every((genre) => (track?.genres || []).includes(genre))
        : (track?.genres || []).some((genre) => seasonalGenres.includes(genre)))
      : false;
    const genreExcluded = seasonalExcludeGenres.length
      ? (track?.genres || []).some((genre) => seasonalExcludeGenres.includes(genre))
      : false;

    const haystacks = [track?.trackTitle, track?.albumName]
      .map((value) => String(value || '').trim().toLowerCase())
      .filter(Boolean);
    const keywordMatch = seasonalKeywords.length ? seasonalKeywords[seasonalKeywordsMode === 'all' ? 'every' : 'some']((keyword) => {
      const needle = String(keyword || '').trim().toLowerCase();
      return needle && haystacks.some((haystack) => haystack.includes(needle));
    }) : false;
    const keywordExcluded = seasonalExcludeKeywords.some((keyword) => {
      const needle = String(keyword || '').trim().toLowerCase();
      return needle && haystacks.some((haystack) => haystack.includes(needle));
    });

    if (genreExcluded || keywordExcluded) return false;
    if (!seasonalGenres.length && !seasonalKeywords.length) return true;
    return genreMatch || keywordMatch;
  }

  // Returns both the eligible pool and the final output estimate for a given pre-built artistMap + trackMap.
  function runWithMaps(artistMap, trackMap) {
    const matchedTracks = [];
    for (const t of masterTracks) {
      const score = artistMap.get((t.artistName || '').toLowerCase()) ?? null;
      const artistTier = classifyArtist(score);
      if (artistTierFilter.include && !artistTierFilter.include.has(artistTier)) continue;
      if (artistTierFilter.exclude && artistTierFilter.exclude.has(artistTier)) continue;

      const stat = trackMap.get(t.ratingKey);
      const rawTier = stat?.tier || 'curatorr';
      const normTier = rawTier === 'half-decent' ? 'halfDecent' : rawTier === 'curatorr' ? 'unplayed' : rawTier;
      if (trackTierFilter.include && !trackTierFilter.include.has(normTier)) continue;
      if (trackTierFilter.exclude && trackTierFilter.exclude.has(normTier)) continue;

      if (gf.include && !(gf.includeMode === 'all'
        ? Array.from(gf.include).every((g) => (t.genres || []).includes(g))
        : (t.genres || []).some((g) => gf.include.has(g)))) continue;
      if (gf.exclude && (t.genres || []).some((g) => gf.exclude.has(g))) continue;
      if (mf.include && !(mf.includeMode === 'all'
        ? Array.from(mf.include).every((m) => (t.moods || []).includes(m))
        : (t.moods || []).some((m) => mf.include.has(m)))) continue;
      if (mf.exclude && (t.moods || []).some((m) => mf.exclude.has(m))) continue;
      if (tf.include || tf.exclude) {
        const artistTags = getEffectiveTrackTags(t, artistTagMap);
        if (tf.include && !(tf.includeMode === 'all'
          ? Array.from(tf.include).every((tag) => artistTags.includes(tag))
          : artistTags.some((tag) => tf.include.has(tag)))) continue;
        if (tf.exclude && artistTags.some((tag) => tf.exclude.has(tag))) continue;
      }
      if (df.include || df.exclude) {
        const trackDecade = getTrackDecadeTag(t);
        if (df.include && !df.include.has(trackDecade)) continue;
        if (df.exclude && trackDecade && df.exclude.has(trackDecade)) continue;
      }
      if (!matchesSeasonalRule(t)) continue;
      if (albumPopularTrackKeys) {
        const isPopular = albumPopularTrackKeys.has(String(t.ratingKey || ''));
        if (albumPopularityMode === 'top3Only' && !isPopular) continue;
        if (albumPopularityMode === 'excludeTop3' && isPopular) continue;
      }
      matchedTracks.push({
        ratingKey: t.ratingKey,
        artistName: t.artistName || '',
        trackTitle: t.trackTitle || '',
        albumName: t.albumName || '',
        ratingCount: Number(t.ratingCount || 0),
        viewCount: Number(t.viewCount || 0),
        rc: t.ratingCount || 0,
        tw: stat?.tier_weight || 0,
        pc: stat?.play_count || 0,
      });
    }

    const popularityFilteredTracks = applyAbsolutePopularityMode(matchedTracks, popularityMode, rules?.popularityPercent);
    const eligibleArtistCount = new Set(popularityFilteredTracks.map((track) => String(track.artistName || '').trim()).filter(Boolean)).size;
    const eligibleTrackCount = popularityFilteredTracks.length;
    const finalTracks = selectPreviewTracks(popularityFilteredTracks, {
      sortBy,
      topNPerArtist: topN,
      maxTracksPerAlbum: maxPerAlbum,
      maxTracks: maxT,
    });
    const artistCount = new Set(finalTracks.map((track) => String(track.artistName || '').trim()).filter(Boolean)).size;
    return {
      artistCount,
      trackCount: finalTracks.length,
      eligibleArtistCount,
      eligibleTrackCount,
    };
  }

  // Returns the Set of qualifying ratingKeys for a single user (used by blend set ops)
  function runKeysForUser(uid) {
    const artistMap = new Map(
      db.prepare('SELECT artist_name, ranking_score FROM artist_stats WHERE user_plex_id = ?').all(uid)
        .map((r) => [r.artist_name.toLowerCase(), r.ranking_score]),
    );
    const trackMap = new Map(
      db.prepare('SELECT plex_rating_key, tier, tier_weight, play_count FROM track_stats WHERE user_plex_id = ?').all(uid)
        .map((r) => [r.plex_rating_key, r]),
    );
    const matchedTracks = [];
    for (const t of masterTracks) {
      const score = artistMap.get((t.artistName || '').toLowerCase()) ?? null;
      const artistTier = classifyArtist(score);
      if (artistTierFilter.include && !artistTierFilter.include.has(artistTier)) continue;
      if (artistTierFilter.exclude && artistTierFilter.exclude.has(artistTier)) continue;
      const stat = trackMap.get(t.ratingKey);
      const rawTier = stat?.tier || 'curatorr';
      const normTier = rawTier === 'half-decent' ? 'halfDecent' : rawTier === 'curatorr' ? 'unplayed' : rawTier;
      if (trackTierFilter.include && !trackTierFilter.include.has(normTier)) continue;
      if (trackTierFilter.exclude && trackTierFilter.exclude.has(normTier)) continue;
      if (gf.include && !(gf.includeMode === 'all'
        ? Array.from(gf.include).every((g) => (t.genres || []).includes(g))
        : (t.genres || []).some((g) => gf.include.has(g)))) continue;
      if (gf.exclude && (t.genres || []).some((g) => gf.exclude.has(g))) continue;
      if (mf.include && !(mf.includeMode === 'all'
        ? Array.from(mf.include).every((m) => (t.moods || []).includes(m))
        : (t.moods || []).some((m) => mf.include.has(m)))) continue;
      if (mf.exclude && (t.moods || []).some((m) => mf.exclude.has(m))) continue;
      if (tf.include || tf.exclude) {
        const artistTags = getEffectiveTrackTags(t, artistTagMap);
        if (tf.include && !(tf.includeMode === 'all'
          ? Array.from(tf.include).every((tag) => artistTags.includes(tag))
          : artistTags.some((tag) => tf.include.has(tag)))) continue;
        if (tf.exclude && artistTags.some((tag) => tf.exclude.has(tag))) continue;
      }
      if (df.include || df.exclude) {
        const trackDecade = getTrackDecadeTag(t);
        if (df.include && !df.include.has(trackDecade)) continue;
        if (df.exclude && trackDecade && df.exclude.has(trackDecade)) continue;
      }
      if (!matchesSeasonalRule(t)) continue;
      if (albumPopularTrackKeys) {
        const isPopular = albumPopularTrackKeys.has(String(t.ratingKey || ''));
        if (albumPopularityMode === 'top3Only' && !isPopular) continue;
        if (albumPopularityMode === 'excludeTop3' && isPopular) continue;
      }
      matchedTracks.push({
        ratingKey: t.ratingKey,
        artistName: t.artistName || '',
        trackTitle: t.trackTitle || '',
        albumName: t.albumName || '',
        ratingCount: Number(t.ratingCount || 0),
        viewCount: Number(t.viewCount || 0),
        rc: t.ratingCount || 0,
        tw: stat?.tier_weight || 0,
        pc: stat?.play_count || 0,
      });
    }
    const keys = new Set(selectPreviewTracks(applyAbsolutePopularityMode(matchedTracks, popularityMode, rules?.popularityPercent), {
      sortBy,
      topNPerArtist: topN,
      maxTracksPerAlbum: maxPerAlbum,
      maxTracks: maxT,
    }).map((track) => String(track.ratingKey || ''))
      .filter(Boolean));
    return keys;
  }

  function runForUser(uid) {
    const artistMap = new Map(
      db.prepare('SELECT artist_name, ranking_score FROM artist_stats WHERE user_plex_id = ?').all(uid)
        .map((r) => [r.artist_name.toLowerCase(), r.ranking_score]),
    );
    const trackMap = new Map(
      db.prepare('SELECT plex_rating_key, tier, tier_weight, play_count FROM track_stats WHERE user_plex_id = ?').all(uid)
        .map((r) => [r.plex_rating_key, r]),
    );
    return runWithMaps(artistMap, trackMap);
  }

  // ── Blend preview ──────────────────────────────────────────────────────────
  const blendUsers = Array.isArray(rules?.blendUsers) && rules.blendUsers.length ? rules.blendUsers : null;
  const blendMode  = blendUsers ? (rules?.blendMode || 'average') : null;

  if (blendMode === 'average') {
    const artistGroups = new Map();
    const trackGroups  = new Map();
    for (const uid of blendUsers) {
      db.prepare('SELECT artist_name, ranking_score FROM artist_stats WHERE user_plex_id = ?').all(uid)
        .forEach((r) => {
          const key = r.artist_name.toLowerCase();
          if (!artistGroups.has(key)) artistGroups.set(key, []);
          artistGroups.get(key).push(r.ranking_score);
        });
      db.prepare('SELECT plex_rating_key, tier, tier_weight, play_count FROM track_stats WHERE user_plex_id = ?').all(uid)
        .forEach((r) => {
          if (!trackGroups.has(r.plex_rating_key)) trackGroups.set(r.plex_rating_key, { weights: [], tiers: [] });
          const d = trackGroups.get(r.plex_rating_key);
          d.weights.push(r.tier_weight || 0);
          d.tiers.push(r.tier || 'curatorr');
        });
    }
    const avgArtistMap = new Map();
    for (const [name, scores] of artistGroups) {
      avgArtistMap.set(name, scores.reduce((a, b) => a + b, 0) / scores.length);
    }
    const avgTrackMap = new Map();
    for (const [key, data] of trackGroups) {
      const tierCounts = {};
      for (const t of data.tiers) tierCounts[t] = (tierCounts[t] || 0) + 1;
      const tier = Object.entries(tierCounts).sort((a, b) => b[1] - a[1])[0][0];
      avgTrackMap.set(key, { tier, tier_weight: data.weights.reduce((a, b) => a + b, 0) / data.weights.length });
    }
    return { forUser: runWithMaps(avgArtistMap, avgTrackMap), average: null };
  }

  if (blendMode === 'intersection' || blendMode === 'union' || blendMode === 'veto') {
    const perUserSets = blendUsers.map(runKeysForUser);
    let resultKeys;
    if (blendMode === 'intersection') {
      if (!perUserSets.length) return { forUser: { artistCount: 0, trackCount: 0, eligibleArtistCount: 0, eligibleTrackCount: 0 }, average: null };
      const [first, ...rest] = perUserSets;
      resultKeys = new Set([...first].filter((k) => rest.every((s) => s.has(k))));
    } else {
      resultKeys = new Set();
      for (const s of perUserSets) for (const k of s) resultKeys.add(k);
      if (blendMode === 'veto') {
        for (const uid of blendUsers) {
          db.prepare("SELECT plex_rating_key FROM track_stats WHERE user_plex_id = ? AND tier = 'skip'").all(uid)
            .forEach((r) => resultKeys.delete(r.plex_rating_key));
        }
      }
    }
    const eligibleTracks = masterTracks.filter((t) => resultKeys.has(t.ratingKey));
    const eligibleArtistCount = new Set(eligibleTracks.map((t) => t.artistName)).size;
    const finalTracks = selectPreviewTracks(eligibleTracks.map((track) => ({
      ...track,
      rc: Number(track?.ratingCount || 0),
      tw: 0,
      pc: 0,
    })), {
      sortBy,
      topNPerArtist: topN,
      maxTracksPerAlbum: maxPerAlbum,
      maxTracks: maxT,
    });
    const artistCount = new Set(finalTracks.map((t) => t.artistName)).size;
    return {
      forUser: {
        artistCount,
        trackCount: finalTracks.length,
        eligibleArtistCount,
        eligibleTrackCount: eligibleTracks.length,
      },
      average: null,
    };
  }

  // ── Standard single-user or all-users average ──────────────────────────────
  if (userIdFilter) {
    return { forUser: runForUser(userIdFilter), average: null };
  }

  const allIds = db.prepare('SELECT DISTINCT user_plex_id FROM artist_stats').all().map((r) => r.user_plex_id);
  if (!allIds.length) return { forUser: null, average: { artistCount: 0, trackCount: 0, eligibleArtistCount: 0, eligibleTrackCount: 0 } };
  const results = allIds.map(runForUser);
  return {
    forUser: null,
    average: {
      artistCount: Math.round(results.reduce((s, r) => s + r.artistCount, 0) / results.length),
      trackCount:  Math.round(results.reduce((s, r) => s + r.trackCount,  0) / results.length),
      eligibleArtistCount: Math.round(results.reduce((s, r) => s + r.eligibleArtistCount, 0) / results.length),
      eligibleTrackCount: Math.round(results.reduce((s, r) => s + r.eligibleTrackCount, 0) / results.length),
    },
  };
}
