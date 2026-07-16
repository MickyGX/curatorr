import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import {
  enqueueLidarrRequest,
  listLidarrRequests,
  requeueFailedLidarrRequests,
  updateLidarrRequest,
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
`;

function createDb() {
  const db = new Database(':memory:');
  db.exec(SCHEMA);
  return db;
}

// Insert a raw `failed` row. The partial unique index only covers queued/processing,
// so multiple failed rows for the same artist/album are permitted — exactly the
// state that used to wedge the retry job.
function insertFailed(db, { userPlexId = 'user1', artistName = 'ABBA', albumTitle = 'Arrival', retryCount = 0 } = {}) {
  const info = db.prepare(`
    INSERT INTO lidarr_requests (user_plex_id, artist_name, album_title, status, detail_json, processed_at)
    VALUES (?, ?, ?, 'failed', ?, ?)
  `).run(userPlexId, artistName, albumTitle, JSON.stringify({ retryCount }), Date.now());
  return info.lastInsertRowid;
}

function insertActive(db, { userPlexId = 'user1', artistName = 'ABBA', albumTitle = 'Arrival', status = 'queued' } = {}) {
  const info = db.prepare(`
    INSERT INTO lidarr_requests (user_plex_id, artist_name, album_title, status, detail_json)
    VALUES (?, ?, ?, ?, '{}')
  `).run(userPlexId, artistName, albumTitle, status);
  return info.lastInsertRowid;
}

const countByStatus = (db, status) =>
  db.prepare(`SELECT COUNT(*) AS n FROM lidarr_requests WHERE status = ?`).get(status).n;

describe('requeueFailedLidarrRequests (issue #142)', () => {
  it('drains 28 duplicate failed rows and re-queues exactly one — no UNIQUE constraint throw', () => {
    const db = createDb();
    try {
      for (let i = 0; i < 28; i++) insertFailed(db);
      assert.equal(countByStatus(db, 'failed'), 28, 'precondition: 28 duplicate failed rows');

      const res = requeueFailedLidarrRequests(db, 'user1', { maxRetries: 3, now: Date.now() });

      assert.equal(res.requeued, 1, 'exactly one row re-queued');
      assert.equal(res.collapsed, 27, 'the other 27 duplicates drained');
      assert.equal(countByStatus(db, 'queued'), 1, 'one queued row remains');
      assert.equal(countByStatus(db, 'removed'), 27, 'duplicates marked removed');
      assert.equal(countByStatus(db, 'failed'), 0, 'no failed rows left for this key');

      const queued = listLidarrRequests(db, 'user1', { statuses: ['queued'], limit: 100 });
      assert.equal(Number(queued[0].detail?.retryCount || 0), 1, 'retry count incremented');
    } finally {
      db.close();
    }
  });

  it('does not re-queue rows at or over the retry limit, but still drains their duplicates', () => {
    const db = createDb();
    try {
      insertFailed(db, { retryCount: 3 });
      insertFailed(db, { retryCount: 3 });
      insertFailed(db, { retryCount: 4 });

      const res = requeueFailedLidarrRequests(db, 'user1', { maxRetries: 3, now: Date.now() });

      assert.equal(res.requeued, 0, 'nothing re-queued past the retry limit');
      assert.equal(countByStatus(db, 'queued'), 0);
      assert.equal(countByStatus(db, 'failed'), 1, 'one survivor left as failed for inspection');
      assert.equal(countByStatus(db, 'removed'), 2, 'duplicates drained');
    } finally {
      db.close();
    }
  });

  it('drains failed rows whose artist/album already has an active row (no collision)', () => {
    const db = createDb();
    try {
      insertActive(db, { status: 'queued' });
      insertFailed(db);
      insertFailed(db);

      const res = requeueFailedLidarrRequests(db, 'user1', { maxRetries: 3, now: Date.now() });

      assert.equal(res.requeued, 0, 'must not promote into an already-active key');
      assert.equal(countByStatus(db, 'queued'), 1, 'still exactly one queued row');
      assert.equal(countByStatus(db, 'failed'), 0, 'redundant failed rows drained');
      assert.equal(res.collapsed, 2);
    } finally {
      db.close();
    }
  });

  it('re-queues one survivor per distinct artist/album', () => {
    const db = createDb();
    try {
      insertFailed(db, { artistName: 'ABBA', albumTitle: 'Arrival' });
      insertFailed(db, { artistName: 'ABBA', albumTitle: 'Arrival' });
      insertFailed(db, { artistName: 'Racoon', albumTitle: 'Liverpool' });

      const res = requeueFailedLidarrRequests(db, 'user1', { maxRetries: 3, now: Date.now() });

      assert.equal(res.requeued, 2, 'one per distinct key');
      assert.equal(countByStatus(db, 'queued'), 2);
      assert.equal(countByStatus(db, 'failed'), 0);
    } finally {
      db.close();
    }
  });

  it('is a no-op when there are no failed rows', () => {
    const db = createDb();
    try {
      const res = requeueFailedLidarrRequests(db, 'user1', { maxRetries: 3, now: Date.now() });
      assert.deepEqual(res, { requeued: 0, collapsed: 0 });
    } finally {
      db.close();
    }
  });
});

describe('enqueueLidarrRequest dedup against failed rows (issue #142)', () => {
  it('revives an existing failed row instead of inserting a duplicate', () => {
    const db = createDb();
    try {
      const first = enqueueLidarrRequest(db, 'user1', { artistName: 'ABBA', albumTitle: 'Arrival' });
      updateLidarrRequest(db, first.id, { status: 'failed', processedAt: Date.now() }, 'user1');
      assert.equal(countByStatus(db, 'failed'), 1);

      const again = enqueueLidarrRequest(db, 'user1', { artistName: 'ABBA', albumTitle: 'Arrival' });

      assert.equal(again.id, first.id, 'same row revived, not a new insert');
      assert.equal(again.status, 'queued');
      assert.equal(countByStatus(db, 'queued'), 1);
      assert.equal(countByStatus(db, 'failed'), 0, 'no leftover failed duplicate');
    } finally {
      db.close();
    }
  });

  it('collapses older failed duplicates when reviving on enqueue', () => {
    const db = createDb();
    try {
      insertFailed(db);
      insertFailed(db);
      insertFailed(db);
      assert.equal(countByStatus(db, 'failed'), 3);

      const req = enqueueLidarrRequest(db, 'user1', { artistName: 'ABBA', albumTitle: 'Arrival' });

      assert.equal(req.status, 'queued');
      assert.equal(countByStatus(db, 'queued'), 1, 'exactly one active row');
      assert.equal(countByStatus(db, 'failed'), 0, 'all failed dupes revived/drained');
      assert.equal(countByStatus(db, 'removed'), 2, 'older dupes removed');
    } finally {
      db.close();
    }
  });
});
