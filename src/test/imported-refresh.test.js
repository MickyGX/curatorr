import { after, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { unlinkSync } from 'node:fs';
import express from 'express';
import request from 'supertest';

import { initDb, listUserGeneratedPlaylists, saveUserGeneratedPlaylist } from '../db.js';
import { registerApiMusic } from '../routes/api-music.js';
import { createPlaylistService } from '../services/playlists.js';

const USER = 'imported-refresh-user';
const DAY_MS = 24 * 60 * 60 * 1000;

describe('imported playlist auto-refresh at scale', () => {
  let db;
  let dbPath;
  let app;
  let ctx;

  before(() => {
    dbPath = join(tmpdir(), `curatorr-imported-refresh-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
    db = initDb(dbPath);
    ctx = {
      db,
      requireUser(req, _res, next) {
        req.session = { user: { username: USER, role: 'user', source: 'plex' } };
        next();
      },
      requireAdmin(_req, res) {
        res.status(403).json({ error: 'Admin access required.' });
      },
      loadConfig: () => ({ mediaServer: { type: 'plex' }, plex: { url: 'http://plex.local' }, smartPlaylist: {} }),
      saveConfig() {},
      pushLog() {},
      safeMessage: (err) => String(err?.message || err || ''),
      getPreviewUserId: () => '',
      resolveUserPlexServerToken: () => 'plex-user-token',
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': String(token || '') }),
      userHasOwnPlexToken: () => true,
      resolveLocalUsers: () => [],
      normalizeStoredAvatarPath: (value) => String(value || ''),
    };
    ctx.playlistService = createPlaylistService(ctx);
    app = express();
    app.use(express.json());
    registerApiMusic(app, ctx);

    // Backup-only restored playlists refresh from stored tracks without touching Plex.
    const base = Date.now() - 10 * DAY_MS;
    for (let i = 0; i < 30; i += 1) {
      saveUserGeneratedPlaylist(db, USER, {
        playlistKey: `custom-import-${String(i).padStart(2, '0')}`,
        playlistTitle: `Imported ${i}`,
        playlistType: 'custom',
        sourceType: 'curatorr-backup',
        sourceRef: `backup-${i}`,
        importedSyncPeriod: 'disabled',
        active: false,
        backupOnly: true,
        lastBuiltAt: base + i * 1000,
        updatedAt: base + i * 1000,
      });
    }
    saveUserGeneratedPlaylist(db, USER, {
      playlistKey: 'personal-not-imported',
      playlistTitle: 'Personal',
      playlistType: 'personal',
      active: false,
    });
  });

  after(() => {
    try { db.close(); } catch (_) {}
    for (const suffix of ['', '-wal', '-shm']) {
      try { unlinkSync(`${dbPath}${suffix}`); } catch (_) {}
    }
  });

  const importedRows = () => listUserGeneratedPlaylists(db, USER, { activeOnly: false })
    .filter((playlist) => playlist.playlistType === 'custom');

  it('sets auto-refresh for many imported playlists at once and skips other playlists', async () => {
    const keys = [...importedRows().map((playlist) => playlist.playlistKey), 'personal-not-imported'];
    await request(app)
      .post('/api/music/playlists/imported-sync-period/bulk')
      .send({ playlistKeys: keys, importedSyncPeriod: 'bogus' })
      .expect(400);

    const res = await request(app)
      .post('/api/music/playlists/imported-sync-period/bulk')
      .send({ playlistKeys: keys, importedSyncPeriod: 'daily' })
      .expect(200);
    assert.equal(res.body.updated.length, 30);
    assert.equal(res.body.skipped, 1);
    assert.ok(importedRows().every((playlist) => playlist.importedSyncPeriod === 'daily'));
  });

  it('refreshes at most one batch per run, oldest due first', async () => {
    const before = new Map(importedRows().map((playlist) => [playlist.playlistKey, playlist.lastBuiltAt]));
    const result = await ctx.refreshScheduledImportedPlaylistsForUser(USER);
    assert.deepEqual(result, { refreshed: 25, skipped: 0, deferred: 5 });
    const untouched = importedRows()
      .filter((playlist) => playlist.lastBuiltAt === before.get(playlist.playlistKey))
      .map((playlist) => playlist.playlistKey)
      .sort();
    assert.deepEqual(untouched, ['custom-import-25', 'custom-import-26', 'custom-import-27', 'custom-import-28', 'custom-import-29']);

    const next = await ctx.refreshScheduledImportedPlaylistsForUser(USER);
    assert.deepEqual(next, { refreshed: 5, skipped: 25, deferred: 0 });
  });

  it('queues "refresh all" and works through it in staggered batches', async () => {
    await request(app)
      .post('/api/music/playlists/imported-sync-period/bulk')
      .send({ playlistKeys: importedRows().map((playlist) => playlist.playlistKey), importedSyncPeriod: 'disabled' })
      .expect(200);

    const res = await request(app).post('/api/music/playlists/imported/refresh-all').send({}).expect(200);
    assert.equal(res.body.queued, 30);

    const pending = () => importedRows().filter((playlist) => playlist.importedRefreshRequestedAt > 0).length;
    for (let i = 0; i < 100 && pending() > 5; i += 1) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(pending(), 5, 'expected the background run to take one batch');

    let result = await ctx.refreshScheduledImportedPlaylistsForUser(USER);
    for (let i = 0; i < 100 && result.busy; i += 1) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      result = await ctx.refreshScheduledImportedPlaylistsForUser(USER);
    }
    assert.equal(result.refreshed, 5);
    assert.equal(pending(), 0);
  });

  it('backs off playlists whose refresh keeps failing so others still get a turn', async () => {
    saveUserGeneratedPlaylist(db, USER, {
      playlistKey: 'custom-import-broken',
      playlistTitle: 'Broken source',
      playlistType: 'custom',
      sourceType: 'spotify-playlist',
      sourceRef: '',
      importedSyncPeriod: 'daily',
      active: true,
      lastBuiltAt: 1,
      updatedAt: 1,
    });
    const first = await ctx.refreshScheduledImportedPlaylistsForUser(USER, { limit: 1 });
    assert.equal(first.failed, 1);
    const second = await ctx.refreshScheduledImportedPlaylistsForUser(USER, { limit: 1 });
    assert.equal(second.failed, undefined, 'expected the failing playlist to wait before retrying');
  });
});
