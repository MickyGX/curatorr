import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { unlinkSync } from 'node:fs';

import {
  addPlaylistTracks,
  getPlaylistTracks,
  initDb,
  setPlaylistTracks,
} from '../db.js';
import {
  buildM3uPathLookups,
  parseM3uPlaylist,
  pickM3uPathMatch,
} from '../services/m3u-import.js';

function makeTestDb() {
  const dbPath = join(tmpdir(), `curatorr-m3u-test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
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

describe('M3U import parsing and matching', () => {
  it('parses extended metadata and plain path entries in source order', () => {
    const entries = parseM3uPlaylist([
      '#EXTM3U',
      '#EXTINF:184,Artist One - Song One',
      'C:\\Music\\Artist One\\Song One.flac',
      '/music/Artist Two/Song Two.mp3',
    ].join('\n'));

    assert.equal(entries.length, 2);
    assert.equal(entries[0].position, 1);
    assert.equal(entries[0].artistName, 'Artist One');
    assert.equal(entries[0].title, 'Song One');
    assert.equal(entries[0].durationMs, 184000);
    assert.equal(entries[1].position, 2);
    assert.equal(entries[1].title, 'Song Two');
  });

  it('matches Plex file paths with normalized separators and URI escaping', () => {
    const lookups = buildM3uPathLookups([
      {
        ratingKey: 'rk-1',
        artistName: 'Artist One',
        trackTitle: 'Song One',
        albumName: 'Album',
        filePath: '/music/Artist One/Song One.flac',
        durationMs: 184000,
      },
    ]);

    const result = pickM3uPathMatch(lookups, { filePath: 'file:///music/Artist%20One/Song%20One.flac' });
    assert.equal(result.method, 'path');
    assert.equal(result.match.ratingKey, 'rk-1');
  });

  it('keeps playlist track source order when storage order differs', () => {
    const db = makeTestDb();
    try {
      setPlaylistTracks(db, 'user-a', 'm3u:test', [
        { ratingKey: 'rk-2', artistName: 'Artist Two', sourcePosition: 2 },
        { ratingKey: 'rk-1', artistName: 'Artist One', sourcePosition: 1 },
      ]);
      addPlaylistTracks(db, 'user-a', 'm3u:test', [
        { ratingKey: 'rk-3', artistName: 'Artist Three' },
      ]);

      assert.deepEqual(
        getPlaylistTracks(db, 'user-a', 'm3u:test').map((track) => track.ratingKey),
        ['rk-1', 'rk-2', 'rk-3'],
      );
    } finally {
      closeTestDb(db);
    }
  });
});
