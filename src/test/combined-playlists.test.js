import { after, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { unlinkSync } from 'node:fs';
import express from 'express';
import request from 'supertest';

import {
  createUserPersonalPlaylist,
  initDb,
  listUserPersonalPlaylists,
  refreshMasterTracks,
  saveUserGeneratedPlaylist,
  setPlaylistTracks,
} from '../db.js';
import { registerApiMusic } from '../routes/api-music.js';
import { buildCombinedPlaylistRatingKeys, createPlaylistService } from '../services/playlists.js';

const USER = 'combined-user';

function track(ratingKey, artistName, ratingCount = 0) {
  return { ratingKey, artistName, ratingCount, trackTitle: `Song ${ratingKey}`, albumName: `${artistName} Album`, libraryKey: '1' };
}

describe('combined playlists', () => {
  let db;
  let dbPath;
  let app;

  before(() => {
    dbPath = join(tmpdir(), `curatorr-combined-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
    db = initDb(dbPath);
    refreshMasterTracks(db, [
      track('n1', 'Artist A', 50), track('n2', 'Artist B', 10), track('n3', 'Artist C', 70),
      track('t1', 'Artist D', 90), track('t2', 'Artist A', 30), track('t3', 'Artist E', 60),
      track('x1', 'Artist F', 100),
    ]);
    // "Newest" and "Top played" sources; n1 appears in both to test dedupe.
    const sources = {
      'personal:newest': ['n1', 'n2', 'n3'],
      'custom-import-top': ['t1', 'n1', 't2', 't3'],
      'personal:nested': ['x1'],
    };
    for (const [key, keys] of Object.entries(sources)) {
      setPlaylistTracks(db, USER, key, keys.map((ratingKey, index) => ({ ratingKey, sourcePosition: index + 1 })));
      saveUserGeneratedPlaylist(db, USER, {
        playlistKey: key,
        playlistTitle: key,
        playlistType: key.startsWith('personal:') ? 'personal' : 'custom',
        active: true,
        trackCount: keys.length,
      });
    }
    createUserPersonalPlaylist(db, USER, { id: 'newest', name: 'Newest', rules: { sortBy: 'libraryAddedDesc' } });
    // An existing combined playlist can't be used as a source.
    createUserPersonalPlaylist(db, USER, {
      id: 'nested',
      name: 'Nested',
      rules: { combined: { sources: ['personal:newest', 'custom-import-top'] } },
    });

    const ctx = {
      db,
      requireUser(req, _res, next) {
        req.session = { user: { username: USER, role: 'user', source: 'plex' } };
        next();
      },
      requireAdmin(_req, res) { res.status(403).json({ error: 'Admin access required.' }); },
      loadConfig: () => ({ mediaServer: { type: 'plex' }, smartPlaylist: {} }),
      saveConfig() {},
      pushLog() {},
      safeMessage: (err) => String(err?.message || err || ''),
      getPreviewUserId: () => '',
      resolveUserPlexServerToken: () => '',
      buildPlexAuthHeaders: (token, extra = {}) => ({ ...extra, 'X-Plex-Token': String(token || '') }),
      userHasOwnPlexToken: () => false,
      resolveLocalUsers: () => [],
      normalizeStoredAvatarPath: (value) => String(value || ''),
    };
    ctx.playlistService = createPlaylistService(ctx);
    app = express();
    app.use(express.json());
    registerApiMusic(app, ctx);
  });

  after(() => {
    try { db.close(); } catch (_) {}
    for (const suffix of ['', '-wal', '-shm']) {
      try { unlinkSync(`${dbPath}${suffix}`); } catch (_) {}
    }
  });

  const build = (combined, extraRules = {}) => buildCombinedPlaylistRatingKeys(db, USER, {
    id: 'mix',
    rules: { ...extraRules, combined },
  });

  it('pools the sources without duplicates and sorts them like any playlist', () => {
    // Default sort is global rating count, across both sources.
    assert.deepEqual(
      build({ sources: ['personal:newest', 'custom-import-top'] }),
      ['t1', 'n3', 't3', 'n1', 't2', 'n2'],
    );
    assert.deepEqual(
      build({ sources: ['personal:newest', 'custom-import-top'] }, { sortBy: 'ratingCount' }),
      build({ sources: ['custom-import-top', 'personal:newest'] }, { sortBy: 'ratingCount' }),
      'source order does not change a sorted result',
    );
  });

  it('applies the per-artist and total caps after sorting', () => {
    // Artist A has n1 (50) and t2 (30); with one per artist, t2 is dropped.
    assert.deepEqual(
      build({ sources: ['personal:newest', 'custom-import-top'] }, { topNPerArtist: 1, maxTracks: 4 }),
      ['t1', 'n3', 't3', 'n1'],
    );
  });

  it('ignores combined playlists and itself as sources', () => {
    assert.deepEqual(build({ sources: ['personal:nested', 'personal:mix', 'personal:newest'] }), ['n3', 'n1', 'n2']);
  });

  it('validates sources when saving and previews the result', async () => {
    const tooFew = await request(app)
      .post('/api/music/playlists/personal')
      .send({ name: 'Too few', combined: { sources: ['personal:newest', 'personal:nested'] } })
      .expect(400);
    assert.match(tooFew.body.error, /at least two/);

    const notMine = await request(app)
      .post('/api/music/playlists/personal')
      .send({ name: 'Not mine', combined: { sources: ['personal:newest', 'custom-import-someone-else'] } })
      .expect(400);
    assert.match(notMine.body.error, /at least two/);

    const preview = await request(app)
      .get('/api/music/playlists/personal/preview')
      .query({ rules: JSON.stringify({ combined: { sources: ['personal:newest', 'custom-import-top'] } }) })
      .expect(200);
    assert.equal(preview.body.trackCount, 6);
    assert.equal(preview.body.artistCount, 5);

    const created = await request(app)
      .post('/api/music/playlists/personal')
      .send({
        name: 'Newest and top played',
        blendUsers: ['someone'],
        combined: { sources: ['personal:newest', 'custom-import-top'] },
      })
      .expect(200);
    assert.equal(created.body.draft, false);
    const saved = listUserPersonalPlaylists(db, USER).find((entry) => entry.name === 'Newest and top played');
    assert.deepEqual(saved.rules.combined, { sources: ['personal:newest', 'custom-import-top'] });
    assert.deepEqual(saved.rules.blendUsers, [], 'combined playlists are never blended');
  });
});
