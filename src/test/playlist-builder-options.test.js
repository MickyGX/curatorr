import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { unlinkSync } from 'node:fs';
import Database from 'better-sqlite3';

import {
  initDb,
  bumpLibraryDataGeneration,
  getLibraryDataGeneration,
  invalidateMasterTracksCache,
  saveArtistTags,
} from '../db.js';
import { getPlaylistBuilderOptions, warmPlaylistBuilderOptions } from '../services/playlist-builder-options.js';

// A stand-in for a file-backed better-sqlite3 handle; the service only reads
// `name`/`memory` before handing the path to the (injected) worker runner.
function fakeFileDb() {
  return { name: `/data/fake-${Math.random().toString(36).slice(2)}.db`, memory: false };
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

describe('playlist builder options cache', () => {
  it('waits briefly for a fast rebuild, then serves the cached lists without rebuilding', async () => {
    const db = fakeFileDb();
    let runs = 0;
    const runWorker = async () => { runs++; return { allGenres: ['Rock'] }; };

    const first = await getPlaylistBuilderOptions(db, { runWorker });
    assert.deepEqual(first.allGenres, ['Rock']);
    assert.equal(first.builderOptionsPending, false);
    assert.deepEqual(first.allMoods, [], 'missing lists are filled with empty defaults');
    assert.deepEqual(first.playlistFeatureCoverage, { totalTracks: 0, presets: {} });

    const second = await getPlaylistBuilderOptions(db, { runWorker });
    assert.deepEqual(second.allGenres, ['Rock']);
    assert.equal(runs, 1);
  });

  it('renders empty lists with a pending flag on a cold start that outlasts the wait', async () => {
    const db = fakeFileDb();
    const job = deferred();
    let runs = 0;
    const runWorker = () => { runs++; return job.promise; };

    const cold = await getPlaylistBuilderOptions(db, { runWorker, waitMs: 5 });
    assert.equal(cold.builderOptionsPending, true);
    assert.deepEqual(cold.allGenres, []);

    const stillCold = await getPlaylistBuilderOptions(db, { runWorker, waitMs: 5 });
    assert.equal(stillCold.builderOptionsPending, true);
    assert.equal(runs, 1, 'concurrent requests share one rebuild');

    job.resolve({ allGenres: ['Jazz'] });
    await flush();
    const ready = await getPlaylistBuilderOptions(db, { runWorker, waitMs: 5 });
    assert.equal(ready.builderOptionsPending, false);
    assert.deepEqual(ready.allGenres, ['Jazz']);
    assert.equal(runs, 1);
  });

  it('serves the previous lists while a stale cache rebuilds after library data changes', async () => {
    const db = fakeFileDb();
    let job = deferred();
    let runs = 0;
    const runWorker = () => { runs++; return job.promise; };

    job.resolve({ allGenres: ['Rock'] });
    await getPlaylistBuilderOptions(db, { runWorker });

    job = deferred();
    bumpLibraryDataGeneration(db);
    const stale = await getPlaylistBuilderOptions(db, { runWorker, waitMs: 5 });
    assert.deepEqual(stale.allGenres, ['Rock']);
    assert.equal(stale.builderOptionsPending, false);
    assert.equal(runs, 2);

    job.resolve({ allGenres: ['Rock', 'Soul'] });
    await flush();
    const fresh = await getPlaylistBuilderOptions(db, { runWorker, waitMs: 5 });
    assert.deepEqual(fresh.allGenres, ['Rock', 'Soul']);
    assert.equal(runs, 2);
  });

  it('keeps the last good lists when a rebuild fails', async () => {
    const db = fakeFileDb();
    let fail = false;
    const runWorker = async () => {
      if (fail) throw new Error('disk busy');
      return { allGenres: ['Rock'] };
    };
    await getPlaylistBuilderOptions(db, { runWorker });

    fail = true;
    bumpLibraryDataGeneration(db);
    const originalWarn = console.warn;
    console.warn = () => {};
    try {
      const result = await getPlaylistBuilderOptions(db, { runWorker });
      assert.deepEqual(result.allGenres, ['Rock']);
      assert.equal(result.builderOptionsPending, false);
    } finally {
      console.warn = originalWarn;
    }
  });

  it('spaces out rebuilds of a slow library in proportion to how long the last one took', async () => {
    const db = fakeFileDb();
    let clock = 0;
    const now = () => clock;
    let runs = 0;
    const runWorker = async () => {
      runs++;
      clock += 60_000; // each rebuild "takes" a minute
      return { allGenres: [`run-${runs}`] };
    };

    await getPlaylistBuilderOptions(db, { runWorker, now });
    assert.equal(runs, 1);

    bumpLibraryDataGeneration(db);
    clock += 1_000;
    const cooling = await getPlaylistBuilderOptions(db, { runWorker, now });
    assert.equal(runs, 1, 'no rebuild inside the cooldown');
    assert.deepEqual(cooling.allGenres, ['run-1'], 'stale lists are served meanwhile');

    clock += 4 * 60_000;
    const rebuilt = await getPlaylistBuilderOptions(db, { runWorker, now });
    assert.equal(runs, 2);
    assert.deepEqual(rebuilt.allGenres, ['run-2']);
  });

  it('warm only starts a rebuild when the cache is missing or stale', async () => {
    const db = fakeFileDb();
    let runs = 0;
    const runWorker = async () => { runs++; return { allGenres: ['Rock'] }; };

    await warmPlaylistBuilderOptions(db, { runWorker });
    assert.equal(runs, 1);
    assert.equal(warmPlaylistBuilderOptions(db, { runWorker }), null);
    assert.equal(warmPlaylistBuilderOptions(new Database(':memory:')), null, 'in-memory dbs are never warmed');
  });

  it('computes in-memory databases inline', async () => {
    const db = new Database(':memory:');
    db.exec(`CREATE TABLE master_tracks (genres TEXT DEFAULT '[]', moods TEXT DEFAULT '[]')`);
    db.prepare(`INSERT INTO master_tracks (genres, moods) VALUES (?, ?)`).run('["Rock; Pop"]', '["Happy"]');
    const result = await getPlaylistBuilderOptions(db);
    assert.deepEqual(result.allGenres, ['Pop', 'Rock']);
    assert.deepEqual(result.allMoods, ['Happy']);
    assert.equal(result.builderOptionsPending, false);
    db.close();
  });
});

describe('library data generation', () => {
  it('advances on master track invalidation and on artist tag writes', () => {
    const dbPath = join(tmpdir(), `curatorr-gen-test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
    const db = initDb(dbPath);
    try {
      const start = getLibraryDataGeneration(db);
      invalidateMasterTracksCache(db);
      assert.equal(getLibraryDataGeneration(db), start + 1);
      saveArtistTags(db, 'Artist', ['indie']);
      assert.equal(getLibraryDataGeneration(db), start + 2);
    } finally {
      db.close();
      for (const suffix of ['', '-wal', '-shm']) { try { unlinkSync(`${dbPath}${suffix}`); } catch (_) {} }
    }
  });
});

describe('playlist builder options worker', () => {
  it('builds the lists from a real database file in a worker thread', async () => {
    const dbPath = join(tmpdir(), `curatorr-builder-opts-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
    const db = initDb(dbPath);
    try {
      db.prepare(
        `INSERT INTO master_tracks
         (rating_key, artist_name, track_title, album_name, genres, moods, album_genres, album_styles, album_moods, library_key, file_path, duration_ms, rating_count, view_count, updated_at)
         VALUES ('1', 'Artist', 'Track', 'Album', '["Rock; Pop"]', '["Happy"]', '[]', '[]', '[]', '1', '/music/Artist/Album/01.flac', 180000, 0, 0, ?)`
      ).run(Date.now());
      invalidateMasterTracksCache(db);

      const result = await getPlaylistBuilderOptions(db, { waitMs: 30_000 });
      assert.equal(result.builderOptionsPending, false);
      assert.deepEqual(result.allGenres, ['Pop', 'Rock']);
      assert.deepEqual(result.allMoods, ['Happy']);
      assert.equal(result.playlistFeatureCoverage.totalTracks, 1);
    } finally {
      db.close();
      for (const suffix of ['', '-wal', '-shm']) { try { unlinkSync(`${dbPath}${suffix}`); } catch (_) {} }
    }
  });
});
