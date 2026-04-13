import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import {
  enqueueLidarrRequest,
  getLidarrRequest,
  getSuggestedArtist,
  removeQueuedLidarrRequest,
  setSuggestedArtistStatus,
  updateLidarrRequest,
  upsertSuggestedArtist,
} from '../db.js';

const SCHEMA = `
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
`;

function createDb() {
  const db = new Database(':memory:');
  db.exec(SCHEMA);
  return db;
}

function seedRequest(db, overrides = {}) {
  const db2 = db;
  const req = enqueueLidarrRequest(db2, overrides.userPlexId || 'user1', {
    artistName: overrides.artistName || 'Test Artist',
    albumTitle: overrides.albumTitle || 'Test Album',
    sourceKind: 'manual',
    requestKind: 'artist_album',
  });
  if (overrides.status && overrides.status !== 'queued') {
    return updateLidarrRequest(db2, req.id, {
      status: overrides.status,
      lidarrAlbumId: overrides.lidarrAlbumId ?? null,
      processedAt: overrides.status === 'completed' || overrides.status === 'failed' ? Date.now() : null,
    }, req.userPlexId);
  }
  return req;
}

describe('removeQueuedLidarrRequest', () => {
  it('removes a queued request', () => {
    const db = createDb();
    const req = seedRequest(db, { status: 'queued' });
    const result = removeQueuedLidarrRequest(db, req.id, 'user1');
    assert.equal(result.status, 'removed');
    db.close();
  });

  it('removes a processing request', () => {
    const db = createDb();
    const req = seedRequest(db, { status: 'processing' });
    const result = removeQueuedLidarrRequest(db, req.id, 'user1');
    assert.equal(result.status, 'removed');
    db.close();
  });

  it('removes a failed request', () => {
    const db = createDb();
    const req = seedRequest(db, { status: 'failed' });
    const result = removeQueuedLidarrRequest(db, req.id, 'user1');
    assert.equal(result.status, 'removed');
    db.close();
  });

  it('removes a completed request whose album was deleted from Lidarr', () => {
    const db = createDb();
    // Simulates: request completed, lidarrAlbumId set, but album later deleted from Lidarr.
    // promoteCompletedRequestsFromLidarr returns it with inLibrary=false so it sits in the queue bucket.
    const req = seedRequest(db, { status: 'completed', lidarrAlbumId: 9866 });
    const result = removeQueuedLidarrRequest(db, req.id, 'user1');
    assert.equal(result.status, 'removed');
    db.close();
  });

  it('does not remove an already-removed request', () => {
    const db = createDb();
    const req = seedRequest(db, { status: 'queued' });
    removeQueuedLidarrRequest(db, req.id, 'user1');
    // Second remove attempt should not change anything
    const second = removeQueuedLidarrRequest(db, req.id, 'user1');
    // status is 'removed', not in the allowed list, so it returns existing unchanged
    assert.equal(second.status, 'removed');
    db.close();
  });

  it('returns null when the request does not exist', () => {
    const db = createDb();
    const result = removeQueuedLidarrRequest(db, 99999, 'user1');
    assert.equal(result, null);
    db.close();
  });

  it('returns null when the user does not own the request', () => {
    const db = createDb();
    const req = seedRequest(db, { status: 'queued', userPlexId: 'user1' });
    const result = removeQueuedLidarrRequest(db, req.id, 'other-user');
    assert.equal(result, null);
    db.close();
  });

  it('removed item is no longer returned as queued', () => {
    const db = createDb();
    const req = seedRequest(db, { status: 'queued' });
    removeQueuedLidarrRequest(db, req.id, 'user1');
    const fetched = getLidarrRequest(db, req.id, 'user1');
    assert.equal(fetched.status, 'removed');
    db.close();
  });

  it('failed item previously returned false success — now actually removes', () => {
    // Regression: before the fix, removeQueuedLidarrRequest returned `existing`
    // unchanged for 'failed' status, and the route sent { ok: true } falsely.
    const db = createDb();
    const req = seedRequest(db, { status: 'failed' });
    const before = getLidarrRequest(db, req.id, 'user1');
    assert.equal(before.status, 'failed');
    const result = removeQueuedLidarrRequest(db, req.id, 'user1');
    assert.equal(result.status, 'removed', 'failed request must be removed, not returned unchanged');
    db.close();
  });

  it('completed item with stale lidarrAlbumId previously returned false success — now actually removes', () => {
    // Regression: completed items stuck in queue because their Lidarr album was
    // deleted. promoteCompletedRequestsFromLidarr returns them with inLibrary=false,
    // so they appear in the queue bucket permanently. Before the fix, remove
    // returned the existing completed record as { ok: true } without removing it.
    const db = createDb();
    const req = seedRequest(db, { status: 'completed', lidarrAlbumId: 9866 });
    const before = getLidarrRequest(db, req.id, 'user1');
    assert.equal(before.status, 'completed');
    const result = removeQueuedLidarrRequest(db, req.id, 'user1');
    assert.equal(result.status, 'removed', 'completed request must be removed, not returned unchanged');
    db.close();
  });
});

// ── Helper: manufacture the exact stuck state from issue #85 ─────────────────
//
// Seeds a suggested_artist at queued_for_lidarr (set when the request was
// enqueued) and a lidarr_request at the given status (failed / completed).
// This is the state the user sees: artist stuck as "Queued" in the pipeline,
// request either already failed or completed with a stale album ID.
function seedStuckPipelineState(db, { requestStatus = 'failed', lidarrArtistId = null } = {}) {
  upsertSuggestedArtist(db, 'user1', {
    artistName: 'Stuck Artist',
    source: 'curatorr',
    similarityScore: 10,
    behaviorScore: 10,
    editorialScore: 0,
    totalScore: 10,
    reason: {},
  });
  setSuggestedArtistStatus(db, 'user1', 'Stuck Artist', 'queued_for_lidarr', { reason: {} });
  const req = enqueueLidarrRequest(db, 'user1', { artistName: 'Stuck Artist', albumTitle: 'Some Album' });
  return updateLidarrRequest(db, req.id, {
    status: requestStatus,
    lidarrArtistId: lidarrArtistId ?? null,
    lidarrAlbumId: requestStatus === 'completed' ? 9866 : null,
    processedAt: Date.now(),
  }, 'user1');
}

describe('Artist pipeline stuck at queued_for_lidarr — delete route fix', () => {
  // These tests reproduce the exact state reported in issue #85:
  // the artist shows "Queued" in the Artist Pipeline even though the underlying
  // request has already failed (or completed with a deleted album ID), and the
  // user has no way to clear it because there is no dismiss button for pipeline
  // artists that are not in "suggested" state.

  it('manufacturing the stuck state: suggestion stays queued_for_lidarr after request fails', () => {
    // Confirm the broken state exists — this is what oemino53 was seeing.
    const db = createDb();
    seedStuckPipelineState(db, { requestStatus: 'failed' });
    const suggestion = getSuggestedArtist(db, 'user1', 'Stuck Artist');
    assert.equal(suggestion.status, 'queued_for_lidarr', 'stuck state should be reproducible');
    db.close();
  });

  it('deleting the request resets the pipeline status to suggested (no lidarrArtistId)', () => {
    // Simulates the delete route logic after the fix.
    // Artist was never added to Lidarr (no lidarrArtistId) so goes back to suggested.
    const db = createDb();
    const req = seedStuckPipelineState(db, { requestStatus: 'failed', lidarrArtistId: null });

    // Simulate what the /delete route now does after the fix.
    updateLidarrRequest(db, req.id, { status: 'removed', processedAt: Date.now(), updatedAt: Date.now() }, 'user1');
    const suggestion = getSuggestedArtist(db, 'user1', 'Stuck Artist');
    if (suggestion && String(suggestion.status || '').trim() === 'queued_for_lidarr') {
      const resetStatus = Number(req.lidarrArtistId || 0) > 0 ? 'added_to_lidarr' : 'suggested';
      setSuggestedArtistStatus(db, 'user1', 'Stuck Artist', resetStatus, { reason: { ...(suggestion.reason || {}), lastFailedAt: Date.now() } });
    }

    const after = getSuggestedArtist(db, 'user1', 'Stuck Artist');
    assert.equal(after.status, 'suggested', 'pipeline should reset to suggested so artist can be re-requested or dismissed');
    db.close();
  });

  it('deleting the request resets the pipeline status to added_to_lidarr when artist exists in Lidarr', () => {
    // Artist was already added to Lidarr (lidarrArtistId set) — album failed but artist remains.
    const db = createDb();
    const req = seedStuckPipelineState(db, { requestStatus: 'failed', lidarrArtistId: 42 });

    updateLidarrRequest(db, req.id, { status: 'removed', processedAt: Date.now(), updatedAt: Date.now() }, 'user1');
    const suggestion = getSuggestedArtist(db, 'user1', 'Stuck Artist');
    if (suggestion && String(suggestion.status || '').trim() === 'queued_for_lidarr') {
      const resetStatus = Number(req.lidarrArtistId || 0) > 0 ? 'added_to_lidarr' : 'suggested';
      setSuggestedArtistStatus(db, 'user1', 'Stuck Artist', resetStatus, { reason: { ...(suggestion.reason || {}), lastFailedAt: Date.now() } });
    }

    const after = getSuggestedArtist(db, 'user1', 'Stuck Artist');
    assert.equal(after.status, 'added_to_lidarr', 'pipeline should show added_to_lidarr since artist is still in Lidarr');
    db.close();
  });

  it('deleting a completed request with stale album ID also resets pipeline status', () => {
    // The completed + stale lidarrAlbumId case (issue #85 exact scenario).
    const db = createDb();
    const req = seedStuckPipelineState(db, { requestStatus: 'completed', lidarrArtistId: null });

    updateLidarrRequest(db, req.id, { status: 'removed', processedAt: Date.now(), updatedAt: Date.now() }, 'user1');
    const suggestion = getSuggestedArtist(db, 'user1', 'Stuck Artist');
    if (suggestion && String(suggestion.status || '').trim() === 'queued_for_lidarr') {
      const resetStatus = Number(req.lidarrArtistId || 0) > 0 ? 'added_to_lidarr' : 'suggested';
      setSuggestedArtistStatus(db, 'user1', 'Stuck Artist', resetStatus, { reason: { ...(suggestion.reason || {}), lastFailedAt: Date.now() } });
    }

    const after = getSuggestedArtist(db, 'user1', 'Stuck Artist');
    assert.notEqual(after.status, 'queued_for_lidarr', 'pipeline must not stay stuck at queued_for_lidarr after request is deleted');
    db.close();
  });

  it('does not reset pipeline status if suggestion is already past queued_for_lidarr', () => {
    // Guard: if someone dismisses or the automation already resolved the status,
    // the delete route should not touch it.
    const db = createDb();
    const req = seedStuckPipelineState(db, { requestStatus: 'failed' });
    // Pre-advance the suggestion to dismissed.
    setSuggestedArtistStatus(db, 'user1', 'Stuck Artist', 'dismissed', { reason: {} });

    updateLidarrRequest(db, req.id, { status: 'removed', processedAt: Date.now(), updatedAt: Date.now() }, 'user1');
    const suggestion = getSuggestedArtist(db, 'user1', 'Stuck Artist');
    // Only reset if still queued_for_lidarr — dismissed should be untouched.
    if (suggestion && String(suggestion.status || '').trim() === 'queued_for_lidarr') {
      setSuggestedArtistStatus(db, 'user1', 'Stuck Artist', 'suggested', { reason: {} });
    }

    const after = getSuggestedArtist(db, 'user1', 'Stuck Artist');
    assert.equal(after.status, 'dismissed', 'dismissed status must not be overwritten');
    db.close();
  });
});

// ── Helper: seed a suggested artist at a given status ────────────────────────
function seedSuggestion(db, { artistName = 'Test Artist', status = 'suggested' } = {}) {
  upsertSuggestedArtist(db, 'user1', {
    artistName,
    source: 'curatorr',
    similarityScore: 5,
    behaviorScore: 5,
    editorialScore: 0,
    totalScore: 5,
    reason: {},
  });
  if (status !== 'suggested') {
    setSuggestedArtistStatus(db, 'user1', artistName, status, { reason: {} });
  }
  return getSuggestedArtist(db, 'user1', artistName);
}

describe('Dismiss suggested artist from pipeline', () => {
  it('dismisses a suggested artist', () => {
    const db = createDb();
    seedSuggestion(db, { status: 'suggested' });
    setSuggestedArtistStatus(db, 'user1', 'Test Artist', 'dismissed', { reason: { manualAction: 'dismissed' } });
    const after = getSuggestedArtist(db, 'user1', 'Test Artist');
    assert.equal(after.status, 'dismissed');
    db.close();
  });

  it('dismisses a quota_blocked artist', () => {
    const db = createDb();
    seedSuggestion(db, { status: 'quota_blocked' });
    const before = getSuggestedArtist(db, 'user1', 'Test Artist');
    assert.equal(before.status, 'quota_blocked');
    setSuggestedArtistStatus(db, 'user1', 'Test Artist', 'dismissed', { reason: { manualAction: 'dismissed' } });
    const after = getSuggestedArtist(db, 'user1', 'Test Artist');
    assert.equal(after.status, 'dismissed');
    db.close();
  });

  it('dismisses a queued_for_lidarr artist (stuck-state escape hatch)', () => {
    // This is the fix for issue #85: artists stuck at queued_for_lidarr could not be
    // dismissed because the UI showed no action button. The Dismiss button now appears
    // for queued_for_lidarr status so users can manually unblock the pipeline.
    const db = createDb();
    seedStuckPipelineState(db, { requestStatus: 'failed' });
    const before = getSuggestedArtist(db, 'user1', 'Stuck Artist');
    assert.equal(before.status, 'queued_for_lidarr');
    setSuggestedArtistStatus(db, 'user1', 'Stuck Artist', 'dismissed', { reason: { manualAction: 'dismissed' } });
    const after = getSuggestedArtist(db, 'user1', 'Stuck Artist');
    assert.equal(after.status, 'dismissed', 'queued_for_lidarr artist must be dismissable');
    db.close();
  });

  it('dismissing an already-dismissed artist is idempotent', () => {
    const db = createDb();
    seedSuggestion(db, { status: 'suggested' });
    setSuggestedArtistStatus(db, 'user1', 'Test Artist', 'dismissed', { reason: {} });
    setSuggestedArtistStatus(db, 'user1', 'Test Artist', 'dismissed', { reason: {} });
    const after = getSuggestedArtist(db, 'user1', 'Test Artist');
    assert.equal(after.status, 'dismissed');
    db.close();
  });

  it('dismiss does not affect a different user\'s suggestion for the same artist', () => {
    const db = createDb();
    upsertSuggestedArtist(db, 'user2', {
      artistName: 'Test Artist',
      source: 'curatorr',
      similarityScore: 5,
      behaviorScore: 5,
      editorialScore: 0,
      totalScore: 5,
      reason: {},
    });
    seedSuggestion(db, { status: 'suggested' });
    setSuggestedArtistStatus(db, 'user1', 'Test Artist', 'dismissed', { reason: {} });
    const user2After = getSuggestedArtist(db, 'user2', 'Test Artist');
    assert.equal(user2After.status, 'suggested', 'other user\'s suggestion must be unaffected');
    db.close();
  });
});

// ── in_library aging filter ───────────────────────────────────────────────────
// The pipeline hides artists with lidarrStatusKey === 'in_library' once they
// have been settled for more than 14 days. This is a pure function test of
// the filter predicate used inside buildLidarrStatusBundle.

const IN_LIBRARY_GRACE_MS = 14 * 24 * 60 * 60 * 1000;

function isActionable(artist, now = Date.now()) {
  if (artist.lidarrStatusKey !== 'in_library') return true;
  const settledAt = Number(artist.lidarrProgress?.updatedAt || artist.lidarrProgress?.lastAlbumAddedAt || 0);
  return settledAt > 0 && (now - settledAt) < IN_LIBRARY_GRACE_MS;
}

describe('in_library aging filter', () => {
  it('keeps a non-in_library artist regardless of age', () => {
    const artist = { lidarrStatusKey: 'in_progress', lidarrProgress: { updatedAt: 0 } };
    assert.equal(isActionable(artist), true);
  });

  it('keeps an in_library artist settled less than 14 days ago', () => {
    const thirteenDaysAgo = Date.now() - (13 * 24 * 60 * 60 * 1000);
    const artist = { lidarrStatusKey: 'in_library', lidarrProgress: { updatedAt: thirteenDaysAgo } };
    assert.equal(isActionable(artist), true);
  });

  it('drops an in_library artist settled more than 14 days ago', () => {
    const fifteenDaysAgo = Date.now() - (15 * 24 * 60 * 60 * 1000);
    const artist = { lidarrStatusKey: 'in_library', lidarrProgress: { updatedAt: fifteenDaysAgo } };
    assert.equal(isActionable(artist), false);
  });

  it('drops an in_library artist with no progress timestamp', () => {
    const artist = { lidarrStatusKey: 'in_library', lidarrProgress: null };
    assert.equal(isActionable(artist), false);
  });

  it('falls back to lastAlbumAddedAt when updatedAt is missing', () => {
    const tenDaysAgo = Date.now() - (10 * 24 * 60 * 60 * 1000);
    const artist = { lidarrStatusKey: 'in_library', lidarrProgress: { lastAlbumAddedAt: tenDaysAgo } };
    assert.equal(isActionable(artist), true);
  });

  it('is exactly at the grace boundary — kept on day 13, dropped on day 15', () => {
    const now = Date.now();
    const onDay13 = { lidarrStatusKey: 'in_library', lidarrProgress: { updatedAt: now - (13 * 24 * 60 * 60 * 1000) } };
    const onDay15 = { lidarrStatusKey: 'in_library', lidarrProgress: { updatedAt: now - (15 * 24 * 60 * 60 * 1000) } };
    assert.equal(isActionable(onDay13, now), true, 'day 13 should still be visible');
    assert.equal(isActionable(onDay15, now), false, 'day 15 should be hidden');
  });
});
