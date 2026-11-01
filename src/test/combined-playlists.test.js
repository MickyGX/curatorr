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

function track(ratingKey, artistName) {
  return { ratingKey, artistName, trackTitle: `Song ${ratingKey}`, albumName: `${artistName} Album`, libraryKey: '1' };
}

describe('combined playlists', () => {
  let db;
  let dbPath;
  let app;

  before(() => {
    dbPath = join(tmpdir(), `curatorr-combined-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
    db = initDb(dbPath);
    refreshMasterTracks(db, [
      track('n1', 'Artist A'), track('n2', 'Artist B'), track('n3', 'Artist C'),
      track('t1', 'Artist D'), track('t2', 'Artist A'), track('t3', 'Artist E'),
      track('x1', 'Artist F'),
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
      rules: { combined: { sources: ['personal:newest', 'custom-import-top'], mode: 'interleave' } },
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
    rules: { sortBy: 'source', ...extraRules, combined },
  });

  it('interleaves sources by default and drops duplicates', () => {
    assert.deepEqual(
      build({ sources: ['personal:newest', 'custom-import-top'] }),
      ['n1', 't1', 'n2', 'n3', 't2', 't3'],
    );
  });

  it('appends sources one after another when asked', () => {
    assert.deepEqual(
      build({ sources: ['custom-import-top', 'personal:newest'], mode: 'append' }),
      ['t1', 'n1', 't2', 't3', 'n2', 'n3'],
    );
  });

  it('applies the per-artist and total caps after mixing', () => {
    // Artist A has n1 and t2; with one per artist, t2 is dropped.
    assert.deepEqual(
      build({ sources: ['personal:newest', 'custom-import-top'] }, { topNPerArtist: 1, maxTracks: 4 }),
      ['n1', 't1', 'n2', 'n3'],
    );
  });

  it('ignores combined playlists and itself as sources', () => {
    assert.deepEqual(build({ sources: ['personal:nested', 'personal:mix', 'personal:newest'] }), ['n1', 'n2', 'n3']);
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
      .query({ rules: JSON.stringify({ sortBy: 'source', combined: { sources: ['personal:newest', 'custom-import-top'] } }) })
      .expect(200);
    assert.equal(preview.body.trackCount, 6);
    assert.equal(preview.body.artistCount, 5);

    const created = await request(app)
      .post('/api/music/playlists/personal')
      .send({
        name: 'Newest and top played',
        sortBy: 'source',
        blendUsers: ['someone'],
        combined: { sources: ['personal:newest', 'custom-import-top'], mode: 'append' },
      })
      .expect(200);
    assert.equal(created.body.draft, false);
    const saved = listUserPersonalPlaylists(db, USER).find((entry) => entry.name === 'Newest and top played');
    assert.deepEqual(saved.rules.combined, { sources: ['personal:newest', 'custom-import-top'], mode: 'append' });
    assert.deepEqual(saved.rules.blendUsers, [], 'combined playlists are never blended');
  });
});
