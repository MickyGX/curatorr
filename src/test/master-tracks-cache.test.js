import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { unlinkSync } from 'node:fs';

import {
  getMasterTracks,
  initDb,
  refreshMasterTracks,
  updateMasterTrackTagMetadata,
  invalidateMasterTracksCache,
} from '../db.js';

function makeTestDb() {
  const dbPath = join(tmpdir(), `curatorr-cache-test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
  const db = initDb(dbPath);
  db._testPath = dbPath;
  return db;
}

function closeTestDb(db) {
  try { db.close(); } catch (_) {}
  try { unlinkSync(db._testPath); } catch (_) {}
  try { unlinkSync(`${db._testPath}-wal`); } catch (_) {}
  try { unlinkSync(`${db._testPath}-shm`); } catch (_) {}
}

function insertTrack(db, ratingKey, artistName = 'Artist', title = 'Track') {
  db.prepare(
    `INSERT OR REPLACE INTO master_tracks
     (rating_key, artist_name, track_title, album_name, genres, moods, album_genres, album_styles, album_moods, library_key, file_path, duration_ms, rating_count, view_count, updated_at)
     VALUES (?, ?, ?, 'Album', '[]', '[]', '[]', '[]', '[]', '/library', '/file.mp3', 180000, 0, 0, ?)`
  ).run(ratingKey, artistName, title, Date.now());
}

describe('getMasterTracks cache', () => {
  it('returns the same array instance on repeated calls (cache hit)', () => {
    const db = makeTestDb();
    try {
      insertTrack(db, '1');
      const first = getMasterTracks(db);
      const second = getMasterTracks(db);
      assert.strictEqual(first, second, 'cache hit should return the same array reference');
    } finally {
      closeTestDb(db);
    }
  });

  it('invalidates on refreshMasterTracks', () => {
    const db = makeTestDb();
    try {
      insertTrack(db, '10', 'Artist A', 'Track A');
      const before = getMasterTracks(db);
      assert.equal(before.length, 1);

      refreshMasterTracks(db, [
        { ratingKey: '11', artistName: 'Artist B', trackTitle: 'Track B', albumName: 'Album B',
          genres: [], moods: [], albumGenres: [], albumStyles: [], albumMoods: [],
          libraryKey: '/library', filePath: '/b.mp3', durationMs: 120000, ratingCount: 0, viewCount: 0 },
      ]);

      const after = getMasterTracks(db);
      assert.ok(after !== before, 'cache should have been invalidated by refreshMasterTracks');
      assert.ok(after.length >= 1, 'should return tracks after cache invalidation');
    } finally {
      closeTestDb(db);
    }
  });

  it('invalidates on updateMasterTrackTagMetadata', () => {
    const db = makeTestDb();
    try {
      insertTrack(db, '20', 'Artist C', 'Track C');
      const before = getMasterTracks(db);

      updateMasterTrackTagMetadata(db, [
        { ratingKey: '20', genres: ['jazz'], moods: [], albumGenres: [], albumStyles: [], albumMoods: [] },
      ]);

      const after = getMasterTracks(db);
      assert.ok(after !== before, 'cache should have been invalidated by updateMasterTrackTagMetadata');
    } finally {
      closeTestDb(db);
    }
  });

  it('invalidates on explicit invalidateMasterTracksCache call', () => {
    const db = makeTestDb();
    try {
      insertTrack(db, '30', 'Artist D', 'Track D');
      const before = getMasterTracks(db);
      invalidateMasterTracksCache(db);
      const after = getMasterTracks(db);
      assert.ok(after !== before, 'cache should have been invalidated by explicit call');
    } finally {
      closeTestDb(db);
    }
  });

  it('caches independently per database instance', () => {
    const db1 = makeTestDb();
    const db2 = makeTestDb();
    try {
      insertTrack(db1, '40', 'Artist E', 'Track E');
      insertTrack(db2, '50', 'Artist F', 'Track F');

      const tracks1 = getMasterTracks(db1);
      const tracks2 = getMasterTracks(db2);

      assert.notEqual(tracks1, tracks2, 'different db instances should have independent caches');
      assert.equal(tracks1.length, 1);
      assert.equal(tracks2.length, 1);
      assert.equal(tracks1[0].ratingKey, '40');
      assert.equal(tracks2[0].ratingKey, '50');
    } finally {
      closeTestDb(db1);
      closeTestDb(db2);
    }
  });
});
