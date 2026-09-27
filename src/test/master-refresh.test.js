import { afterEach, beforeEach, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { initDb, refreshMasterTracks, getMasterTracks, getSystemJobRun } from '../db.js';
import { refreshMasterTrackCache } from '../routes/wizard.js';
import { createJobService } from '../services/jobs.js';

describe('Master track refresh failure reporting and pruning', () => {
  const originalFetch = global.fetch;
  let db;
  let ctx;
  beforeEach(() => {
    db = initDb(':memory:');
    refreshMasterTracks(db, [{ ratingKey: 'old-id', artistName: 'Artist', trackTitle: 'Song', albumName: 'Album', libraryKey: '3' }]);
    db.prepare('UPDATE master_tracks SET updated_at = 1').run();
    ctx = {
      db,
      loadConfig: () => ({ plex: { url: 'http://plex.local', token: 'token', libraries: ['3'] } }),
      pushLog() {},
      safeMessage: (error) => error.message,
    };
  });
  afterEach(() => { global.fetch = originalFetch; db.close(); });

  function mockPages(pages) {
    global.fetch = async (input) => {
      const url = new URL(input);
      if (url.pathname === '/library/sections/3/all') return pages.shift()();
      return Response.json({ MediaContainer: { size: 0 } });
    };
  }

  it('reports HTTP failures as failed jobs and retains the previous cache', async () => {
    mockPages([() => new Response('', { status: 503 })]);
    const jobs = createJobService(ctx, { masterTrackRefresh: () => refreshMasterTrackCache(ctx) });
    await jobs.runJob('masterTrackRefresh');
    const run = getSystemJobRun(db, 'masterTrackRefresh');
    assert.equal(run.status, 'error');
    assert.match(run.message, /library 3 at offset 0: HTTP 503/);
    assert.equal(getMasterTracks(db)[0].ratingKey, 'old-id');
  });

  for (const nextPage of [
    () => new Response('', { status: 500 }),
    () => Response.json({ MediaContainer: { totalSize: 2, Metadata: [] } }),
    () => Response.json({ unexpected: true }),
  ]) {
    it('does not prune stale entries after an incomplete paginated scan', async () => {
      mockPages([
        () => Response.json({ MediaContainer: { totalSize: 2, Metadata: [{ ratingKey: 'new-id', title: 'Song' }] } }),
        nextPage,
      ]);
      await assert.rejects(refreshMasterTrackCache(ctx), /Plex track cache refresh/);
      assert.deepEqual(getMasterTracks(db).map((t) => t.ratingKey).sort(), ['new-id', 'old-id']);
    });
  }

  it('removes an obsolete ID after a complete refresh adds the replacement ID', async () => {
    mockPages([() => Response.json({ MediaContainer: { totalSize: 1, Metadata: [{ ratingKey: 'new-id', title: 'Song' }] } })]);
    assert.equal(await refreshMasterTrackCache(ctx), 1);
    assert.deepEqual(getMasterTracks(db).map((t) => t.ratingKey), ['new-id']);
  });

  it('accepts a genuinely empty library and removes obsolete cached entries', async () => {
    mockPages([() => Response.json({ MediaContainer: { size: 0 } })]);
    assert.equal(await refreshMasterTrackCache(ctx), 0);
    assert.deepEqual(getMasterTracks(db), []);
  });

  it('does not report missing configuration as a successful refresh', async () => {
    ctx.loadConfig = () => ({});
    await assert.rejects(refreshMasterTrackCache(ctx), /requires a configured media server/);
  });
});
