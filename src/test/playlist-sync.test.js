import { after, beforeEach, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const testDir = fs.mkdtempSync(path.join(os.tmpdir(), 'curatorr-playlist-sync-'));
process.env.DATA_DIR = testDir;

const {
  getLastPlaylistSync,
  getPlaylistTracks,
  initDb,
  listUserGeneratedPlaylists,
  refreshMasterTracks,
  saveUserGeneratedPlaylist,
} = await import('../db.js');
const { createPlaylistService, replacePlexPlaylistItems } = await import('../services/playlists.js');

function buildAppApiUrl(base, relativePath) {
  return new URL(String(relativePath || '').replace(/^\/+/, '/'), `${String(base || '').replace(/\/+$/, '')}/`);
}

function createTestContext(db, logs) {
  return {
    db,
    loadConfig() {
      return {
        mediaServer: { type: 'plex' },
        plex: {
          url: 'http://plex.local',
          token: 'admin-token',
          machineId: 'machine-1',
        },
        smartPlaylist: {},
      };
    },
    saveConfig() {},
    resolveUserPlexServerToken() {
      return 'user-token';
    },
    buildPlexAuthHeaders(token, extraHeaders = {}) {
      return {
        'X-Plex-Token': String(token || ''),
        ...extraHeaders,
      };
    },
    buildAppApiUrl,
    pushLog(entry) {
      logs.push(entry);
    },
    userHasOwnPlexToken() {
      return true;
    },
  };
}

describe('Plex generated playlist sync verification', () => {
  const originalFetch = global.fetch;
  let db;

  beforeEach(() => {
    if (db) db.close();
    const dbPath = path.join(testDir, `playlist-sync-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
    db = initDb(dbPath);
  });

  after(() => {
    global.fetch = originalFetch;
    if (db) db.close();
    fs.rmSync(testDir, { recursive: true, force: true });
  });

  it('stores confirmed Plex item count and warns when Plex silently drops stale ratingKeys', async () => {
    const userId = 'alice';
    const logs = [];
    refreshMasterTracks(db, [
      {
        ratingKey: 'rk-live',
        artistName: 'Artist A',
        trackTitle: 'Live Track',
        albumName: 'Album A',
        libraryKey: '1',
        ratingCount: 30,
      },
      {
        ratingKey: 'rk-stale-1',
        artistName: 'Artist B',
        trackTitle: 'Stale Track 1',
        albumName: 'Album B',
        libraryKey: '1',
        ratingCount: 20,
      },
      {
        ratingKey: 'rk-stale-2',
        artistName: 'Artist C',
        trackTitle: 'Stale Track 2',
        albumName: 'Album C',
        libraryKey: '1',
        ratingCount: 10,
      },
    ]);
    saveUserGeneratedPlaylist(db, userId, {
      playlistType: 'personal',
      playlistKey: 'personal:upbeat',
      plexPlaylistId: 'plex-playlist-1',
      playlistTitle: 'Upbeat Smart',
      active: true,
    });

    const requests = [];
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      const method = String(options.method || 'GET').toUpperCase();
      requests.push({ target, method });
      if (target.startsWith('http://plex.local/playlists/plex-playlist-1?title=') && method === 'PUT') {
        return new Response('', { status: 200 });
      }
      if (target === 'http://plex.local/playlists/plex-playlist-1/items' && method === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-playlist-1/items?uri=') && method === 'PUT') {
        assert.match(decodeURIComponent(target), /rk-live/);
        assert.match(decodeURIComponent(target), /rk-stale-1/);
        assert.match(decodeURIComponent(target), /rk-stale-2/);
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-playlist-1/items?X-Plex-Container-Start=') && method === 'GET') {
        return Response.json({
          MediaContainer: {
            size: 1,
            totalSize: 1,
            Metadata: [{ ratingKey: 'rk-live' }],
          },
        });
      }
      throw new Error(`Unexpected fetch: ${method} ${target}`);
    };

    const service = createPlaylistService(createTestContext(db, logs));
    const synced = await service.syncPersonalPlaylist(userId, {
      id: 'upbeat',
      name: 'Upbeat Smart',
      rules: { maxTracks: 3, sortBy: 'ratingCount' },
    });

    assert.equal(synced.trackCount, 1);
    assert.equal(synced.missingCount, 2);
    assert.deepEqual(getPlaylistTracks(db, userId, 'personal:upbeat'), [
      { ratingKey: 'rk-live', artistName: 'Artist A' },
    ]);

    const generated = listUserGeneratedPlaylists(db, userId, { activeOnly: false })
      .find((playlist) => playlist.playlistKey === 'personal:upbeat');
    assert.equal(generated.trackCount, 1);
    assert.equal(generated.missingCount, 2);

    const sync = getLastPlaylistSync(db, userId);
    assert.equal(sync.track_count, 1);
    assert.equal(sync.excluded_tracks, 2);

    const verifyWarning = logs.find((entry) => entry.action === 'plex.playlist.verify_missing');
    assert.equal(verifyWarning?.level, 'warn');
    assert.equal(verifyWarning?.meta?.requestedCount, 3);
    assert.equal(verifyWarning?.meta?.confirmedCount, 1);
    assert.deepEqual(verifyWarning?.meta?.missingKeys, ['rk-stale-1', 'rk-stale-2']);

    const finalLog = logs.find((entry) => entry.action === 'personal.sync');
    assert.equal(finalLog?.level, 'warn');
    assert.match(finalLog?.message || '', /synced: 1 tracks/);
    assert.match(finalLog?.message || '', /2 missing in Plex/);

    assert.ok(requests.some((request) => request.method === 'GET' && request.target.includes('/items?X-Plex-Container-Start=')));
  });
});


describe('Plex rejected playlist batch recovery', () => {
  const originalFetch = global.fetch;
  after(() => { global.fetch = originalFetch; });

  function mockPlex({ rejected = 'stale', status = 400, metadataStatus = 404, metadataType = 'track', freshId = 'machine-1', verifyStatus = 200, clearStatus = 200 } = {}) {
    const added = [];
    const calls = [];
    global.fetch = async (url, options = {}) => {
      const target = new URL(url);
      const method = options.method || 'GET';
      calls.push({ target, method });
      if (target.pathname === '/') return Response.json({ MediaContainer: { machineIdentifier: freshId } });
      if (target.pathname.startsWith('/library/metadata/')) {
        if (metadataStatus !== 200) return new Response('', { status: metadataStatus });
        return Response.json({ MediaContainer: { Metadata: [{ ratingKey: rejected, type: metadataType }] } });
      }
      if (method === 'DELETE') return new Response('', { status: clearStatus });
      if (method === 'PUT') {
        const uri = target.searchParams.get('uri');
        const keys = uri.split('/').at(-1).split(',');
        if (keys.includes(rejected) || !uri.startsWith(`server://${freshId}/`)) {
          return new Response('Bad Request', { status });
        }
        added.push(...keys);
        return new Response('', { status: 200 });
      }
      if (verifyStatus !== 200) return new Response('', { status: verifyStatus });
      return Response.json({ MediaContainer: { Metadata: added.map((ratingKey) => ({ ratingKey })) } });
    };
    return { added, calls };
  }

  it('isolates missing tracks in a rejected batch and preserves valid track order and duplicates', async () => {
    const { added, calls } = mockPlex();
    const logs = [];
    const result = await replacePlexPlaylistItems(createTestContext(null, logs), 'alice', 'playlist', 'machine-1', ['live-1', 'stale', 'live-2', 'live-1']);
    assert.deepEqual(added, ['live-1', 'live-2', 'live-1']);
    assert.deepEqual(result.confirmedKeys, added);
    assert.deepEqual(result.missingKeys, ['stale']);
    assert.equal(result.confirmedCount, 3);
    assert.equal(result.verified, true);
    assert.equal(calls.filter((call) => call.method === 'DELETE').length, 1);
    assert.equal(calls.filter((call) => call.target.pathname === '/').length, 1);
    assert.ok(logs.some((entry) => entry.action === 'plex.playlist.unavailable_track'));
  });

  it('skips an old music ID reassigned to a TV episode after a Plex database repair', async () => {
    const { added } = mockPlex({ metadataStatus: 200, metadataType: 'episode' });
    const result = await replacePlexPlaylistItems(createTestContext(null, []), 'alice', 'playlist', 'machine-1', ['live', 'stale']);
    assert.deepEqual(added, ['live']);
    assert.deepEqual(result.missingKeys, ['stale']);
    assert.equal(result.confirmedCount, 1);
  });

  it('keeps known missing tracks out of counts when verification is unavailable', async () => {
    mockPlex({ verifyStatus: 503 });
    const result = await replacePlexPlaylistItems(createTestContext(null, []), 'alice', 'playlist', 'machine-1', ['live', 'stale']);
    assert.equal(result.verified, false);
    assert.equal(result.confirmedCount, 1);
    assert.deepEqual(result.confirmedKeys, ['live']);
    assert.deepEqual(result.missingKeys, ['stale']);
  });

  it('refreshes a stale machine identifier without clearing the playlist again', async () => {
    const { added, calls } = mockPlex({ freshId: 'machine-2' });
    const ctx = createTestContext(null, []);
    let saved;
    ctx.saveConfig = (config) => { saved = config; };
    const result = await replacePlexPlaylistItems(ctx, 'alice', 'playlist', 'machine-1', ['live']);
    assert.deepEqual(added, ['live']);
    assert.equal(result.confirmedCount, 1);
    assert.equal(saved.plex.machineId, 'machine-2');
    assert.equal(calls.filter((call) => call.method === 'DELETE').length, 1);
  });

  for (const metadataStatus of [200, 401, 403, 500]) {
    it(`does not hide a rejected track when metadata returns ${metadataStatus}`, async () => {
      mockPlex({ metadataStatus });
      await assert.rejects(
        replacePlexPlaylistItems(createTestContext(null, []), 'alice', 'playlist', 'machine-1', ['stale']),
        /Add playlist items failed: HTTP 400/,
      );
    });
  }

  for (const status of [401, 403, 404, 500]) {
    it(`does not split batches on HTTP ${status}`, async () => {
      const { calls } = mockPlex({ status });
      await assert.rejects(
        replacePlexPlaylistItems(createTestContext(null, []), 'alice', 'playlist', 'machine-1', ['live', 'stale']),
        new RegExp(`Add playlist items failed: HTTP ${status}`),
      );
      assert.equal(calls.filter((call) => call.method === 'PUT').length, 1);
    });
  }

  it('does not append tracks when clearing the playlist fails', async () => {
    const { calls } = mockPlex({ clearStatus: 500 });
    await assert.rejects(
      replacePlexPlaylistItems(createTestContext(null, []), 'alice', 'playlist', 'machine-1', ['live']),
      /Clear playlist items failed: HTTP 500/,
    );
    assert.equal(calls.length, 1);
  });
});
