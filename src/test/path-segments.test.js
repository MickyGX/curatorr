import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { unlinkSync } from 'node:fs';

import { getDistinctPathSegments, initDb } from '../db.js';

function makeTestDb() {
  const dbPath = join(tmpdir(), `curatorr-paths-test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
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

function insertTrack(db, ratingKey, filePath, artistName = 'Artist') {
  db.prepare(
    `INSERT OR REPLACE INTO master_tracks
     (rating_key, artist_name, track_title, album_name, genres, moods, album_genres, album_styles, album_moods, library_key, file_path, duration_ms, rating_count, view_count, updated_at)
     VALUES (?, ?, 'Track', 'Album', '[]', '[]', '[]', '[]', '[]', '/library', ?, 180000, 0, 0, ?)`
  ).run(ratingKey, artistName, filePath, Date.now());
}

describe('getDistinctPathSegments', () => {
  it('lists every artist folder in a per-artist library larger than the old 2000-track cap (issue #141)', () => {
    const db = makeTestDb();
    try {
      // 30 artist folders, 2 albums each, 60 tracks per album = 3600 tracks.
      // That is well past the historic 2000 *file-path* cap that used to truncate
      // the folder list alphabetically around "ABBA".
      const ARTISTS = Array.from({ length: 30 }, (_, i) => `Artist-${String(i + 1).padStart(3, '0')}`);
      let rk = 0;
      for (const artist of ARTISTS) {
        for (const album of ['Album-A', 'Album-B']) {
          for (let t = 1; t <= 60; t++) {
            insertTrack(db, String(++rk), `/music/${artist}/${album}/track-${t}.mp3`, artist);
          }
        }
      }
      assert.ok(rk > 2000, 'sanity: dataset must exceed the old 2000-track cap');

      // Passing limit: 2000 (the historic value) must NOT truncate folders now,
      // because the limit bounds distinct *directories* (60 here), not tracks.
      const segments = getDistinctPathSegments(db, { limit: 2000 });

      // Common root "/music" is stripped, so top-level segments are the artists.
      for (const artist of ARTISTS) {
        assert.ok(segments.includes(artist), `expected top-level folder "${artist}" to be present`);
      }
      // The last artist alphabetically is exactly what the old code dropped.
      assert.ok(segments.includes('Artist-030'), 'late-alphabet artist folder must be present');
      // Nested album folders resolve too.
      assert.ok(segments.includes('Artist-030/Album-B'), 'nested album folder must be present');
    } finally {
      closeTestDb(db);
    }
  });

  it('normalises Windows backslash separators', () => {
    const db = makeTestDb();
    try {
      insertTrack(db, '1', 'C:\\Music\\ABBA\\Arrival\\track01.mp3', 'ABBA');
      insertTrack(db, '2', 'C:\\Music\\Racoon\\Liverpool\\track01.mp3', 'Racoon');
      const segments = getDistinctPathSegments(db, { limit: 2000 });
      assert.ok(segments.includes('ABBA'), 'ABBA folder from a backslash path');
      assert.ok(segments.includes('Racoon'), 'Racoon folder from a backslash path');
      assert.ok(segments.includes('Racoon/Liverpool'), 'nested folder from a backslash path');
    } finally {
      closeTestDb(db);
    }
  });

  it('returns an empty array when there are no tracks', () => {
    const db = makeTestDb();
    try {
      assert.deepEqual(getDistinctPathSegments(db, { limit: 2000 }), []);
    } finally {
      closeTestDb(db);
    }
  });
});
