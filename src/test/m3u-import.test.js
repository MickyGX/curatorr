import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { unlinkSync } from 'node:fs';
import express from 'express';
import request from 'supertest';

import {
  addPlaylistTracks,
  getPlaylistTracks,
  initDb,
  listUserGeneratedPlaylists,
  refreshMasterTracks,
  setPlaylistTracks,
} from '../db.js';
import { registerApiMusic } from '../routes/api-music.js';
import {
  buildM3uPathLookups,
  parseM3uPlaylist,
  pickM3uPathMatch,
} from '../services/m3u-import.js';
import { createPlaylistService } from '../services/playlists.js';

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

  it('imports and refreshes stored M3U playlists through the HTTP API', async () => {
    const db = makeTestDb();
    const userId = 'm3u-route-user';
    const now = Date.now();
    const firstKey = `m3u-route-1-${now}`;
    const secondKey = `m3u-route-2-${now}`;
    const thirdKey = `m3u-route-3-${now}`;
    const m3uContent = [
      '#EXTM3U',
      '#EXTINF:180,Route Artist - First Song',
      `/music/route-${now}/first.flac`,
      '#EXTINF:181,Route Artist - Second Song',
      `/music/route-${now}/second.flac`,
      '#EXTINF:182,Route Artist - Third Song',
      `/music/route-${now}/third.flac`,
    ].join('\n');
    const config = {
      mediaServer: { type: 'plex' },
      plex: {
        url: 'http://plex.local',
        token: 'plex-admin-token',
        machineId: 'machine-route-test',
        userServerTokens: { [userId]: 'plex-user-token' },
      },
      smartPlaylist: {},
    };
    const logs = [];
    const ctx = {
      db,
      requireUser(req, _res, next) {
        req.session = { user: { username: userId, role: 'user', source: 'plex' } };
        next();
      },
      requireAdmin(_req, res) {
        res.status(403).json({ error: 'Admin access required.' });
      },
      loadConfig: () => config,
      saveConfig() {},
      pushLog: (entry) => logs.push(entry),
      safeMessage: (err) => String(err?.message || err || ''),
      getPreviewUserId: () => '',
      resolveUserPlexServerToken: () => 'plex-user-token',
      buildAppApiUrl(base, relativePath) {
        return new URL(String(relativePath || '').replace(/^\/+/, '/'), `${String(base || '').replace(/\/+$/, '')}/`);
      },
      buildPlexAuthHeaders(token, extraHeaders = {}) {
        return { ...extraHeaders, 'X-Plex-Token': String(token || '') };
      },
      userHasOwnPlexToken: () => true,
      resolveLocalUsers: () => [],
      normalizeStoredAvatarPath: (value) => String(value || ''),
    };
    ctx.playlistService = createPlaylistService(ctx);

    const app = express();
    app.use(express.json());
    registerApiMusic(app, ctx);

    refreshMasterTracks(db, [
      {
        ratingKey: firstKey,
        artistName: 'Route Artist',
        trackTitle: 'First Song',
        albumName: 'Route Album',
        libraryKey: '1',
        filePath: `/music/route-${now}/first.flac`,
        durationMs: 180000,
      },
      {
        ratingKey: secondKey,
        artistName: 'Route Artist',
        trackTitle: 'Second Song',
        albumName: 'Route Album',
        libraryKey: '1',
        filePath: `/music/route-${now}/second.flac`,
        durationMs: 181000,
      },
    ]);

    const originalFetch = global.fetch;
    const addBatches = [];
    let remoteKeys = [];
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      const method = String(options.method || 'GET').toUpperCase();
      if (target.startsWith('http://plex.local/playlists?playlistType=audio') && method === 'GET') {
        return Response.json({ MediaContainer: { Metadata: [] } });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&') && method === 'POST') {
        return Response.json({ MediaContainer: { Metadata: [{ ratingKey: 'plex-m3u-route' }] } });
      }
      if (target.startsWith('http://plex.local/playlists/plex-m3u-route?title=') && method === 'PUT') {
        return new Response('', { status: 200 });
      }
      if (target === 'http://plex.local/playlists/plex-m3u-route/items' && method === 'DELETE') {
        remoteKeys = [];
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-m3u-route/items?uri=') && method === 'PUT') {
        const decoded = decodeURIComponent(target);
        const match = decoded.match(/library\/metadata\/([^&]+)/);
        remoteKeys = match ? match[1].split(',').filter(Boolean) : [];
        addBatches.push([...remoteKeys]);
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-m3u-route/items?X-Plex-Container-Start=') && method === 'GET') {
        return Response.json({
          MediaContainer: {
            Metadata: remoteKeys.map((ratingKey) => ({ ratingKey })),
          },
        });
      }
      throw new Error(`Unexpected fetch in M3U route test: ${method} ${target}`);
    };

    try {
      const imported = await request(app)
        .post('/api/music/import/m3u')
        .send({
          filename: `route-${now}.m3u8`,
          title: `Route M3U ${now}`,
          content: m3uContent,
        })
        .expect(200);
      assert.equal(imported.body.ok, true);
      assert.equal(imported.body.importedTrackCount, 2);
      assert.equal(imported.body.unmatchedCount, 1);
      const playlistKey = String(imported.body.playlist?.playlistKey || '');
      assert.ok(playlistKey);

      const stored = listUserGeneratedPlaylists(db, userId, { activeOnly: false })
        .find((playlist) => playlist.playlistKey === playlistKey);
      assert.equal(stored?.sourceType, 'm3u-file');
      assert.equal(stored?.sourceContent, m3uContent);
      assert.equal(stored?.sourceFilename, `route-${now}.m3u8`);
      assert.equal(stored?.trackCount, 2);
      assert.equal(stored?.missingCount, 1);
      assert.deepEqual(addBatches[0], [firstKey, secondKey]);

      refreshMasterTracks(db, [{
        ratingKey: thirdKey,
        artistName: 'Route Artist',
        trackTitle: 'Third Song',
        albumName: 'Route Album',
        libraryKey: '1',
        filePath: `/music/route-${now}/third.flac`,
        durationMs: 182000,
      }]);

      const refreshed = await request(app)
        .post('/api/music/playlists/imported-refresh')
        .send({ playlistKey })
        .expect(200);
      assert.equal(refreshed.body.ok, true);
      assert.equal(refreshed.body.trackCount, 3);
      assert.equal(refreshed.body.missingCount, 0);
      assert.deepEqual(addBatches[1], [firstKey, secondKey, thirdKey]);
      assert.deepEqual(
        getPlaylistTracks(db, userId, playlistKey).map((track) => track.ratingKey),
        [firstKey, secondKey, thirdKey],
      );
    } finally {
      global.fetch = originalFetch;
      closeTestDb(db);
    }
  });
});
