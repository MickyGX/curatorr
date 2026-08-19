import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { unlinkSync } from 'node:fs';

const { getMasterTracks, initDb, pruneStaleMasterTracks } = await import('../db.js');

function makeTestDb() {
  const dbPath = join(tmpdir(), `curatorr-prune-test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
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

describe('pruneStaleMasterTracks', () => {
  it('prunes stale master tracks only from refreshed libraries', () => {
    const db = makeTestDb();
    try {
      const cutoff = Date.now();
      db.prepare(`
        INSERT OR REPLACE INTO master_tracks
          (rating_key, artist_name, track_title, album_name, genres, moods, album_genres, album_styles, album_moods, library_key, file_path, duration_ms, rating_count, view_count, updated_at)
        VALUES
          ('live-1', 'Artist A', 'Live Track', 'Album', '[]', '[]', '[]', '[]', '[]', '1', '/music/live.flac', 180000, 0, 0, ?),
          ('stale-1', 'Artist B', 'Stale Track', 'Album', '[]', '[]', '[]', '[]', '[]', '1', '/music/stale.flac', 180000, 0, 0, ?),
          ('other-lib-stale', 'Artist C', 'Other Track', 'Album', '[]', '[]', '[]', '[]', '[]', '2', '/music/other.flac', 180000, 0, 0, ?)
      `).run(cutoff + 10, cutoff - 10, cutoff - 10);
      db.prepare(`
        INSERT OR REPLACE INTO track_enrichment (rating_key, payload_json, updated_at)
        VALUES ('stale-1', '{}', ?)
      `).run(cutoff - 10);
      db.prepare(`
        INSERT OR REPLACE INTO playlist_tracks (playlist_key, user_plex_id, rating_key, artist_name, added_at)
        VALUES ('personal:test', 'alice', 'stale-1', 'Artist B', ?)
      `).run(cutoff - 10);

      const removed = pruneStaleMasterTracks(db, ['1'], cutoff);

      assert.equal(removed, 1);
      assert.deepEqual(getMasterTracks(db).map((track) => track.ratingKey).sort(), ['live-1', 'other-lib-stale']);
      assert.equal(db.prepare('SELECT COUNT(*) AS n FROM track_enrichment WHERE rating_key = ?').get('stale-1').n, 0);
      assert.equal(db.prepare('SELECT COUNT(*) AS n FROM playlist_tracks WHERE rating_key = ?').get('stale-1').n, 0);
    } finally {
      closeTestDb(db);
    }
  });
});
