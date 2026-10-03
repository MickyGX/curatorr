import { afterEach, beforeEach, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { unlinkSync } from 'node:fs';
import express from 'express';
import request from 'supertest';

import {
  getPlaylistTracks,
  initDb,
  listImportedPlaylistUnmatched,
  listUserGeneratedPlaylists,
  refreshMasterTracks,
  saveUserGeneratedPlaylist,
  setImportedPlaylistUnmatched,
  setPlaylistTracks,
} from '../db.js';
import { registerApiMusic } from '../routes/api-music.js';

const USER = 'plex-backup-user';

function makeTestDb() {
  const dbPath = join(tmpdir(), `curatorr-plex-backup-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
  const db = initDb(dbPath);
  db._testPath = dbPath;
  return db;
}

function closeTestDb(db) {
  try { db.close(); } catch (_) {}
  for (const suffix of ['', '-wal', '-shm']) {
    try { unlinkSync(`${db._testPath}${suffix}`); } catch (_) {}
  }
}

function libraryTrack(ratingKey, title) {
  return {
    ratingKey,
    artistName: 'Artist',
    trackTitle: title,
    albumName: 'Album',
    libraryKey: '1',
    filePath: `/music/${ratingKey}.flac`,
    durationMs: 200000,
  };
}

describe('Plex playlist backups and stored-track refresh', () => {
  let db;
  let app;
  let ctx;
  let plex;
  let syncs;
  let originalFetch;

  beforeEach(() => {
    db = makeTestDb();
    syncs = [];
    plex = { playlists: [], items: {}, itemRequests: [] };
    const config = {
      mediaServer: { type: 'plex' },
      plex: { url: 'http://plex.local', token: 'admin-token', machineId: 'machine' },
      smartPlaylist: {},
    };
    ctx = {
      db,
      requireUser(req, _res, next) {
        req.session = { user: { username: USER, role: 'user', source: 'plex' } };
        next();
      },
      requireAdmin(_req, res) { res.status(403).json({ error: 'Admin access required.' }); },
      loadConfig: () => config,
      saveConfig() {},
      pushLog() {},
      safeMessage: (err) => String(err?.message || err || ''),
      getPreviewUserId: () => '',
      resolveUserPlexServerToken: () => 'user-token',
      buildAppApiUrl(base, relativePath) {
        return new URL(String(relativePath || '').replace(/^\/+/, '/'), `${String(base || '').replace(/\/+$/, '')}/`);
      },
      buildPlexAuthHeaders: (token, extra = {}) => ({ ...extra, 'X-Plex-Token': String(token || '') }),
      userHasOwnPlexToken: () => true,
      resolveLocalUsers: () => [],
      normalizeStoredAvatarPath: (value) => String(value || ''),
      playlistService: {
        async syncCustomPlaylist(userId, playlist) {
          syncs.push(playlist.playlistKey);
          return listUserGeneratedPlaylists(db, userId, { activeOnly: false })
            .find((entry) => entry.playlistKey === playlist.playlistKey);
        },
      },
    };
    app = express();
    app.use(express.json());
    registerApiMusic(app, ctx);

    originalFetch = global.fetch;
    global.fetch = async (url) => {
      const target = String(url || '');
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
        return Response.json({
          MediaContainer: {
            Metadata: plex.playlists.map((entry) => ({ ratingKey: entry.id, title: entry.title, leafCount: (plex.items[entry.id] || []).length })),
          },
        });
      }
      const itemsMatch = target.match(/^http:\/\/plex\.local\/playlists\/([^/]+)\/items\?/);
      if (itemsMatch) {
        plex.itemRequests.push(itemsMatch[1]);
        return Response.json({
          MediaContainer: {
            Metadata: (plex.items[itemsMatch[1]] || []).map((ratingKey) => ({ ratingKey, grandparentTitle: 'Artist' })),
          },
        });
      }
      throw new Error(`Unexpected fetch: ${target}`);
    };
  });

  afterEach(() => {
    global.fetch = originalFetch;
    closeTestDb(db);
  });

  it('backs up Plex playlists without creating them in Plex, and refreshes saved copies', async () => {
    refreshMasterTracks(db, [libraryTrack('t1', 'One'), libraryTrack('t2', 'Two')]);
    saveUserGeneratedPlaylist(db, USER, {
      playlistKey: 'personal:pp_x', playlistType: 'personal', playlistTitle: 'Curatorr Mix', plexPlaylistId: '900',
    });
    plex.playlists = [{ id: '100', title: 'Road Trip' }, { id: '900', title: 'Curatorr Mix' }];
    plex.items = { 100: ['t1', 't2'] };

    const listed = await request(app).get('/api/music/import/plex/backup').expect(200);
    assert.deepEqual(listed.body.playlists.map((entry) => [entry.id, entry.backedUp]), [['100', false]]);

    const first = await request(app).post('/api/music/import/plex/backup').send({}).expect(200);
    assert.equal(first.body.created, 1);
    const saved = listUserGeneratedPlaylists(db, USER, { activeOnly: false }).find((entry) => entry.sourceRef === '100');
    assert.equal(saved.playlistTitle, 'Road Trip');
    assert.equal(saved.active, false);
    assert.equal(saved.backupOnly, true);
    assert.equal(saved.importedSyncPeriod, 'weekly');
    assert.deepEqual(getPlaylistTracks(db, USER, saved.playlistKey).map((t) => t.ratingKey), ['t1', 't2']);
    assert.deepEqual(syncs, []);

    plex.items = { 100: ['t2'] };
    const second = await request(app).post('/api/music/import/plex/backup').send({ sourceIds: ['100'] }).expect(200);
    assert.equal(second.body.updated, 1);
    const updated = listUserGeneratedPlaylists(db, USER, { activeOnly: false }).find((entry) => entry.sourceRef === '100');
    assert.equal(updated.playlistKey, saved.playlistKey);
    assert.equal(updated.backupOnly, true);
    assert.deepEqual(getPlaylistTracks(db, USER, saved.playlistKey).map((t) => t.ratingKey), ['t2']);

    const relisted = await request(app).get('/api/music/import/plex/backup').expect(200);
    assert.equal(relisted.body.playlists[0].backedUp, true);
  });

  it('keeps backup-only playlists current on their refresh schedule without syncing them', async () => {
    refreshMasterTracks(db, [libraryTrack('t1', 'One'), libraryTrack('t2', 'Two')]);
    plex.playlists = [{ id: '100', title: 'Road Trip' }];
    plex.items = { 100: ['t1'] };
    await request(app).post('/api/music/import/plex/backup').send({}).expect(200);
    const saved = listUserGeneratedPlaylists(db, USER, { activeOnly: false })[0];
    saveUserGeneratedPlaylist(db, USER, { ...saved, lastBuiltAt: 1 });
    db.prepare('UPDATE user_generated_playlists SET last_built_at = 1 WHERE playlist_key = ?').run(saved.playlistKey);

    plex.items = { 100: ['t1', 't2'] };
    const result = await ctx.refreshScheduledImportedPlaylistsForUser(USER);
    assert.equal(result.refreshed, 1);
    assert.deepEqual(getPlaylistTracks(db, USER, saved.playlistKey).map((t) => t.ratingKey), ['t1', 't2']);
    assert.deepEqual(syncs, []);
    assert.equal(listUserGeneratedPlaylists(db, USER, { activeOnly: false })[0].backupOnly, true);
  });

  it('refreshes from stored tracks when the Plex source is gone, even if a new server reused its id', async () => {
    refreshMasterTracks(db, [libraryTrack('n1', 'One'), libraryTrack('n2', 'Two')]);
    saveUserGeneratedPlaylist(db, USER, {
      playlistKey: 'custom-import-x',
      playlistType: 'custom',
      playlistTitle: 'Road Trip',
      sourceType: 'plex-playlist',
      sourceRef: '100',
      sourceTitle: 'Road Trip',
      active: true,
      plexPlaylistId: '555',
    });
    setPlaylistTracks(db, USER, 'custom-import-x', [{ ratingKey: 'n1' }]);
    setImportedPlaylistUnmatched(db, USER, 'custom-import-x', [
      { position: 2, title: 'Two', artistName: 'Artist', artists: ['Artist'], albumTitle: 'Album', durationMs: 200000 },
    ]);
    // The new server gave id 100 to an unrelated playlist.
    plex.playlists = [{ id: '100', title: 'Someone Else' }];
    plex.items = { 100: ['zzz'] };

    const refreshed = await request(app).post('/api/music/playlists/imported-refresh').send({ playlistKey: 'custom-import-x' }).expect(200);
    assert.equal(refreshed.body.trackCount, 2);
    assert.equal(refreshed.body.missingCount, 0);
    assert.deepEqual(plex.itemRequests, []);
    assert.deepEqual(getPlaylistTracks(db, USER, 'custom-import-x').map((t) => t.ratingKey), ['n1', 'n2']);
    assert.equal(listImportedPlaylistUnmatched(db, USER, 'custom-import-x').length, 0);
    assert.deepEqual(syncs, ['custom-import-x']);
  });

  it('follows a Plex source playlist that now has a different id but the same title', async () => {
    refreshMasterTracks(db, [libraryTrack('n1', 'One'), libraryTrack('n2', 'Two')]);
    saveUserGeneratedPlaylist(db, USER, {
      playlistKey: 'custom-import-y',
      playlistType: 'custom',
      playlistTitle: 'My Copy',
      sourceType: 'plex-playlist',
      sourceRef: '100',
      sourceTitle: 'Road Trip',
      active: false,
    });
    plex.playlists = [{ id: '42', title: 'Road Trip' }];
    plex.items = { 42: ['n2', 'n1'] };

    await request(app).post('/api/music/playlists/imported-refresh').send({ playlistKey: 'custom-import-y' }).expect(200);
    const row = listUserGeneratedPlaylists(db, USER, { activeOnly: false }).find((entry) => entry.playlistKey === 'custom-import-y');
    assert.equal(row.sourceRef, '42');
    assert.deepEqual(getPlaylistTracks(db, USER, 'custom-import-y').map((t) => t.ratingKey), ['n2', 'n1']);
  });

  it('ends backup-only mode when the playlist is enabled', () => {
    saveUserGeneratedPlaylist(db, USER, {
      playlistKey: 'custom-import-z', playlistType: 'custom', playlistTitle: 'Saved', active: false, backupOnly: true,
    });
    const saved = listUserGeneratedPlaylists(db, USER, { activeOnly: false })[0];
    // Unrelated updates keep the flag.
    saveUserGeneratedPlaylist(db, USER, { ...saved, playlistTitle: 'Renamed', backupOnly: undefined });
    assert.equal(listUserGeneratedPlaylists(db, USER, { activeOnly: false })[0].backupOnly, true);
    saveUserGeneratedPlaylist(db, USER, { ...saved, active: true });
    assert.equal(listUserGeneratedPlaylists(db, USER, { activeOnly: false })[0].backupOnly, false);
  });
});
