import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const testDir = fs.mkdtempSync(path.join(os.tmpdir(), 'curatorr-playlist-backup-'));
process.env.DATA_DIR = testDir;

const { default: express } = await import('express');
const { default: request } = await import('supertest');
const {
  createUserPersonalPlaylist,
  getPlaylistTrackSnapshots,
  getPlaylistTracks,
  getUserPersonalPlaylist,
  getUserPreferences,
  initDb,
  listImportedPlaylistUnmatched,
  listUserGeneratedPlaylists,
  listUserPersonalPlaylists,
  pruneStaleMasterTracks,
  refreshMasterTracks,
  remapPlaylistTracks,
  saveUserGeneratedPlaylist,
  saveUserPreferences,
  setPlaylistTracks,
} = await import('../db.js');
const { savePlaylistArtworkBuffer, getStoredPlaylistArtworkInfo } = await import('../services/playlist-artwork.js');
const {
  buildPlaylistBackup,
  buildPlaylistM3u,
  parsePlaylistBackup,
  previewPlaylistBackup,
  restorePlaylistBackup,
} = await import('../services/playlist-backup.js');
const { buildTrackIdentityLookups, resolveTrackIdentity } = await import('../services/track-identity.js');
const { registerPlaylistBackup, PLAYLIST_BACKUP_CONTENT_TYPE } = await import('../routes/api-playlist-backup.js');

after(() => {
  fs.rmSync(testDir, { recursive: true, force: true });
});

let dbCounter = 0;
function makeDb() {
  dbCounter += 1;
  return initDb(path.join(testDir, `backup-${dbCounter}.db`));
}

function track(ratingKey, overrides = {}) {
  return {
    ratingKey,
    artistName: 'Artist',
    trackTitle: `Song ${ratingKey}`,
    albumName: 'Album',
    libraryKey: '1',
    filePath: `/music/Artist/Album/${ratingKey}.flac`,
    durationMs: 200000,
    ...overrides,
  };
}

function customPlaylist(db, userId, playlistKey, title, extra = {}) {
  saveUserGeneratedPlaylist(db, userId, {
    playlistKey,
    playlistType: 'custom',
    playlistTitle: title,
    plexPlaylistId: 'plex-1',
    active: true,
    ...extra,
  });
}

function createPng() {
  return Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48]);
}

function makeCtx(db, { config = {}, userId = 'owner' } = {}) {
  const state = {
    config: { mediaServer: { type: 'plex' }, globalPlaylists: [], ...config },
    syncs: [],
  };
  const ctx = {
    db,
    APP_VERSION: '9.9.9',
    loadConfig: () => state.config,
    saveConfig: (next) => { state.config = next; },
    pushLog: () => {},
    safeMessage: (err) => String(err?.message || err),
    requireUser(req, _res, next) {
      req.session = { user: { username: req.headers['x-test-user'] || userId, role: req.headers['x-test-role'] || 'user' } };
      next();
    },
    playlistService: {
      syncCustomPlaylist: async (uid, playlist) => state.syncs.push(['custom', uid, playlist.playlistKey]),
      syncGlobalCustomPlaylist: async (uid, playlist) => state.syncs.push(['global-custom', uid, playlist.playlistKey]),
      syncPersonalPlaylist: async (uid, def) => state.syncs.push(['personal', uid, def.id]),
      syncGlobalPlaylist: async (uid, def) => state.syncs.push(['global', uid, def.id]),
      renameGeneratedPlaylistTitle: async (uid, key, title) => state.syncs.push(['rename', uid, key, title]),
      fetchGeneratedPlaylistTrackRefs: async () => [],
    },
  };
  return { ctx, state };
}

describe('track identity matching', () => {
  const library = [
    track('a1', { artistName: 'Band', trackTitle: 'Anthem', albumName: 'Live', filePath: '/m/Band/Live/01 Anthem.flac', durationMs: 300000 }),
    track('a2', { artistName: 'Band', trackTitle: 'Anthem', albumName: 'Studio', filePath: '/m/Band/Studio/01 Anthem.flac', durationMs: 240000 }),
    track('b1', { artistName: 'Other', trackTitle: 'Tune', albumName: 'One', recordingMbid: 'MBID-1' }),
  ];
  const lookups = buildTrackIdentityLookups(library);

  it('prefers an exact file path', () => {
    const result = resolveTrackIdentity(lookups, { title: 'Anthem', artistName: 'Band', filePath: '/m/Band/Studio/01 Anthem.flac' });
    assert.equal(result.method, 'path');
    assert.equal(result.match.ratingKey, 'a2');
  });

  it('matches a MusicBrainz recording id', () => {
    const result = resolveTrackIdentity(lookups, { title: 'Renamed', artistName: 'Nobody', recordingMbid: 'mbid-1' });
    assert.equal(result.method, 'mbid');
    assert.equal(result.match.ratingKey, 'b1');
  });

  it('chooses the same album when artist and title have several copies', () => {
    const result = resolveTrackIdentity(lookups, { title: 'Anthem', artistName: 'Band', albumName: 'Live', durationMs: 240000 });
    assert.equal(result.match.ratingKey, 'a1');
  });

  it('does not match a same-titled song by another artist', () => {
    const result = resolveTrackIdentity(lookups, { title: 'Anthem', artistName: 'Cover Band' });
    assert.equal(result.match, null);
  });
});

describe('playlist tracks survive re-keyed libraries', () => {
  it('stores identity from the library cache with each playlist track', () => {
    const db = makeDb();
    refreshMasterTracks(db, [track('k1', { recordingMbid: 'mb-k1' })]);
    setPlaylistTracks(db, 'owner', 'custom-a', [{ ratingKey: 'k1', artistName: 'Artist' }]);
    const [snapshot] = getPlaylistTrackSnapshots(db, 'owner', 'custom-a');
    assert.equal(snapshot.title, 'Song k1');
    assert.equal(snapshot.albumName, 'Album');
    assert.equal(snapshot.filePath, '/music/Artist/Album/k1.flac');
    assert.equal(snapshot.recordingMbid, 'mb-k1');
    assert.equal(snapshot.durationMs, 200000);
  });

  it('re-points playlists after a cache refresh finds every track under a new id', () => {
    const db = makeDb();
    refreshMasterTracks(db, [track('old-1'), track('old-2'), track('old-3')]);
    customPlaylist(db, 'owner', 'custom-a', 'Road Trip');
    setPlaylistTracks(db, 'owner', 'custom-a', ['old-1', 'old-2', 'old-3'].map((ratingKey) => ({ ratingKey })));
    saveUserGeneratedPlaylist(db, 'owner', { playlistKey: 'crescive', playlistType: 'crescive', playlistTitle: 'Crescive' });
    setPlaylistTracks(db, 'owner', 'crescive', [{ ratingKey: 'old-1' }, { ratingKey: 'old-3' }]);

    const refreshStartedAt = Date.now() + 1;
    // A new Plex install: same files, new ids. old-3 was not carried over.
    refreshMasterTracks(db, [
      { ...track('new-1', { filePath: '/music/Artist/Album/old-1.flac', trackTitle: 'Song old-1' }), updatedAt: refreshStartedAt + 1 },
      { ...track('new-2', { filePath: '/other/mount/old-2.flac', trackTitle: 'Song old-2' }), updatedAt: refreshStartedAt + 1 },
    ].map(({ updatedAt: _u, ...rest }) => rest));
    db.prepare('UPDATE master_tracks SET updated_at = ? WHERE rating_key LIKE ?').run(refreshStartedAt + 1, 'new-%');
    db.prepare('UPDATE master_tracks SET updated_at = ? WHERE rating_key LIKE ?').run(refreshStartedAt - 1000, 'old-%');

    const remaps = [];
    const removed = pruneStaleMasterTracks(db, ['1'], refreshStartedAt, { onPlaylistRemap: (result) => remaps.push(result) });
    assert.equal(removed, 3);
    assert.deepEqual(getPlaylistTracks(db, 'owner', 'custom-a').map((t) => t.ratingKey), ['new-1', 'new-2']);
    const missing = listImportedPlaylistUnmatched(db, 'owner', 'custom-a');
    assert.deepEqual(missing.map((row) => row.title), ['Song old-3']);
    assert.equal(missing[0].filePath, '/music/Artist/Album/old-3.flac');
    const custom = listUserGeneratedPlaylists(db, 'owner', { activeOnly: false }).find((p) => p.playlistKey === 'custom-a');
    assert.equal(custom.trackCount, 2);
    assert.equal(custom.missingCount, 1);
    // Generated playlists regenerate, so a gone track simply leaves them.
    assert.deepEqual(getPlaylistTracks(db, 'owner', 'crescive').map((t) => t.ratingKey), ['new-1']);
    assert.equal(remaps[0].remapped, 3);
    assert.equal(remaps[0].missing, 1);
    assert.equal(remaps[0].removed, 1);
  });

  it('re-points tracks orphaned by a library switch once the new library loads', () => {
    const db = makeDb();
    refreshMasterTracks(db, [track('lib5-1', { libraryKey: '5' })]);
    customPlaylist(db, 'owner', 'custom-a', 'Mix');
    setPlaylistTracks(db, 'owner', 'custom-a', [{ ratingKey: 'lib5-1' }, { ratingKey: 'unknown-library-track', artistName: 'X' }]);
    // Deselecting library 5 removes its cache rows; the new server's library 2 is loaded.
    db.prepare("DELETE FROM master_tracks WHERE library_key = '5'").run();
    refreshMasterTracks(db, [track('lib2-9', { libraryKey: '2', filePath: '/music/Artist/Album/lib5-1.flac', trackTitle: 'Song lib5-1' })]);

    const result = remapPlaylistTracks(db);
    assert.equal(result.remapped, 1);
    // A row Curatorr never had identity for (an uncached library) is left untouched.
    assert.deepEqual(getPlaylistTracks(db, 'owner', 'custom-a').map((t) => t.ratingKey), ['lib2-9', 'unknown-library-track']);
    assert.equal(listImportedPlaylistUnmatched(db, 'owner', 'custom-a').length, 0);
  });

  it('keeps unmatched orphans until their id is confirmed gone', () => {
    const db = makeDb();
    refreshMasterTracks(db, [track('k1'), track('other')]);
    customPlaylist(db, 'owner', 'custom-a', 'Mix');
    setPlaylistTracks(db, 'owner', 'custom-a', [{ ratingKey: 'k1' }]);
    db.prepare("DELETE FROM master_tracks WHERE rating_key = 'k1'").run();
    assert.equal(remapPlaylistTracks(db).remapped, 0);
    assert.deepEqual(getPlaylistTracks(db, 'owner', 'custom-a').map((t) => t.ratingKey), ['k1']);
    const confirmed = remapPlaylistTracks(db, { goneKeys: ['k1'] });
    assert.equal(confirmed.missing, 1);
    assert.equal(getPlaylistTracks(db, 'owner', 'custom-a').length, 0);
  });

  it('does nothing while the library cache is empty', () => {
    const db = makeDb();
    refreshMasterTracks(db, [track('k1')]);
    customPlaylist(db, 'owner', 'custom-a', 'Mix');
    setPlaylistTracks(db, 'owner', 'custom-a', [{ ratingKey: 'k1' }]);
    db.prepare('DELETE FROM master_tracks').run();
    const result = remapPlaylistTracks(db, { goneKeys: ['k1'] });
    assert.equal(result.missing, 0);
    assert.equal(getPlaylistTracks(db, 'owner', 'custom-a').length, 1);
  });
});

describe('Curatorr playlist backup', () => {
  function seedLibrary(db) {
    refreshMasterTracks(db, [
      track('t1', { trackTitle: 'First', recordingMbid: 'mb-first' }),
      track('t2', { trackTitle: 'Second' }),
      track('t3', { trackTitle: 'Third' }),
    ]);
  }

  function seedPlaylists(db) {
    const artworkAsset = savePlaylistArtworkBuffer(createPng(), 'png', 'road', 'custom');
    createUserPersonalPlaylist(db, 'owner', {
      id: 'pp_one',
      name: 'Night Drive',
      rules: { genres: { include: ['rock'], exclude: [] }, maxTracks: 25, sortBy: 'random', rebuildSchedule: 'weekly', artwork: { mode: 'custom', customArtworkAsset: artworkAsset } },
      trackFilters: { rules: [], includeFolders: ['/music/Artist'], excludeFolders: [] },
    });
    customPlaylist(db, 'owner', 'custom-static', 'Road Trip', {
      artworkMode: 'custom',
      customArtworkAsset: artworkAsset,
    });
    setPlaylistTracks(db, 'owner', 'custom-static', [{ ratingKey: 't2' }, { ratingKey: 't1' }]);
    customPlaylist(db, 'owner', 'custom-import', 'Plex Faves', {
      sourceType: 'plex-playlist',
      sourceRef: '123',
      sourceTitle: 'Faves',
      importedSyncPeriod: 'weekly',
      active: false,
      backupOnly: true,
    });
    setPlaylistTracks(db, 'owner', 'custom-import', [{ ratingKey: 't3' }]);
    saveUserGeneratedPlaylist(db, 'owner', {
      playlistKey: 'crescive', playlistType: 'crescive', playlistTitle: 'Crescive', titleOverride: 'My Rising',
    });
    saveUserPreferences(db, 'owner', {
      ...getUserPreferences(db, 'owner'),
      likedArtists: ['Artist'],
      smartConfig: { songSkipLimit: 4 },
      userWizardCompleted: true,
      lastfmUsername: 'me',
    });
    return { artworkAsset };
  }

  it('exports smart rules, track identity, artwork, system playlists and settings', async () => {
    const db = makeDb();
    seedLibrary(db);
    seedPlaylists(db);
    const { ctx } = makeCtx(db);
    const backup = await buildPlaylistBackup(ctx, { userIds: ['owner'] });

    assert.equal(backup.format, 'curatorr-playlists');
    assert.equal(backup.version, 1);
    assert.equal(backup.curatorrVersion, '9.9.9');
    const byName = Object.fromEntries(backup.playlists.map((entry) => [entry.name, entry]));
    assert.equal(byName['Night Drive'].type, 'smart');
    assert.equal(byName['Night Drive'].smart.rules.maxTracks, 25);
    assert.equal(byName['Night Drive'].smart.rules.artwork, undefined);
    assert.equal(byName['Night Drive'].artwork.mode, 'custom');
    assert.equal(byName['Night Drive'].artwork.custom.mime, 'image/png');
    assert.deepEqual(byName['Night Drive'].smart.trackFilters.includeFolders, ['/music/Artist']);
    assert.equal(byName['Road Trip'].type, 'static');
    assert.deepEqual(byName['Road Trip'].tracks.map((t) => t.title), ['Second', 'First']);
    assert.equal(byName['Road Trip'].tracks[1].mbid, 'mb-first');
    assert.equal(byName['Road Trip'].tracks[1].path, '/music/Artist/Album/t1.flac');
    assert.equal(byName['Plex Faves'].type, 'imported');
    assert.equal(byName['Plex Faves'].backupOnly, true);
    assert.equal(byName['Plex Faves'].source.type, 'plex-playlist');
    assert.equal(byName.Crescive.type, 'system');
    assert.equal(byName.Crescive.titleOverride, 'My Rising');
    assert.equal(backup.settings.length, 1);
    assert.deepEqual(backup.settings[0].likedArtists, ['Artist']);
    assert.equal(backup.settings[0].lastfmApiKey, undefined);
    assert.equal(backup.settings[0].spotifyAccessToken, undefined);

    const withoutArt = await buildPlaylistBackup(ctx, { userIds: ['owner'], includeArtwork: false });
    assert.equal(withoutArt.playlists.find((entry) => entry.name === 'Night Drive').artwork.custom, undefined);
  });

  it('exports selected playlists only', async () => {
    const db = makeDb();
    seedLibrary(db);
    seedPlaylists(db);
    const { ctx } = makeCtx(db);
    const backup = await buildPlaylistBackup(ctx, { userIds: ['owner'], playlistKeys: ['personal:pp_one', 'custom-static'] });
    assert.deepEqual(backup.playlists.map((entry) => entry.name).sort(), ['Night Drive', 'Road Trip']);
    assert.equal(backup.settings.length, 0);
  });

  it('writes an M3U with paths and extended info', async () => {
    const m3u = buildPlaylistM3u({
      name: 'Road Trip',
      tracks: [
        { artist: 'Artist', title: 'First', album: 'Album', durationMs: 200400, path: '/music/a.flac' },
        { artist: 'Artist', title: 'No File', durationMs: 0, path: '' },
      ],
    });
    assert.equal(m3u, [
      '#EXTM3U',
      '#PLAYLIST:Road Trip',
      '#EXTINF:200,Artist - First',
      '#EXTALB:Album',
      '/music/a.flac',
      '#EXTINF:-1,Artist - No File',
      'Artist - No File',
      '',
    ].join('\n'));
  });

  it('rejects files that are not backups or come from a newer format', () => {
    assert.throws(() => parsePlaylistBackup('nope'), /not a Curatorr playlist backup/);
    assert.throws(() => parsePlaylistBackup({ format: 'other', version: 1, playlists: [] }), /not a Curatorr playlist backup/);
    assert.throws(() => parsePlaylistBackup({ format: 'curatorr-playlists', version: 2, playlists: [] }), /newer Curatorr/);
  });

  it('restores every playlist type onto a new server with different track ids', async () => {
    const source = makeDb();
    seedLibrary(source);
    seedPlaylists(source);
    const backup = parsePlaylistBackup(JSON.stringify(await buildPlaylistBackup(makeCtx(source).ctx, { userIds: ['owner'] })));

    // The new install: same files under new ids, the third track is gone, and setup has
    // created the user's system playlists.
    const target = makeDb();
    refreshMasterTracks(target, [
      track('n1', { trackTitle: 'First', filePath: '/music/Artist/Album/t1.flac' }),
      track('n2', { trackTitle: 'Second', filePath: '/new/path/t2.flac' }),
    ]);
    saveUserGeneratedPlaylist(target, 'owner', { playlistKey: 'crescive', playlistType: 'crescive', playlistTitle: 'Crescive' });
    const { ctx, state } = makeCtx(target);
    const jobs = [];

    const preview = previewPlaylistBackup(ctx, backup, { userId: 'owner', isAdmin: false, ownerMode: 'self' });
    const roadTrip = preview.playlists.find((entry) => entry.name === 'Road Trip');
    assert.equal(roadTrip.matched, 2);
    assert.equal(roadTrip.missing, 0);
    assert.equal(preview.playlists.find((entry) => entry.name === 'Plex Faves').missing, 1);
    assert.equal(preview.playlists.find((entry) => entry.name === 'Crescive').restorable, true);

    const result = restorePlaylistBackup(ctx, backup, { userId: 'owner', queueSync: (queued) => jobs.push(...queued) });
    assert.equal(result.skipped.length, 0);
    assert.equal(result.settingsRestored, 1);

    const smart = listUserPersonalPlaylists(target, 'owner');
    assert.equal(smart.length, 1);
    assert.equal(smart[0].name, 'Night Drive');
    assert.equal(smart[0].rules.maxTracks, 25);
    assert.equal(smart[0].rules.rebuildSchedule, 'weekly');
    assert.equal(smart[0].rules.artwork.mode, 'custom');
    assert.ok(getStoredPlaylistArtworkInfo(smart[0].rules.artwork.customArtworkAsset));
    assert.deepEqual(smart[0].trackFilters.includeFolders, ['/music/Artist']);

    const rows = listUserGeneratedPlaylists(target, 'owner', { activeOnly: false });
    const restoredStatic = rows.find((row) => row.playlistTitle === 'Road Trip');
    assert.equal(restoredStatic.sourceType, 'curatorr-backup');
    assert.equal(restoredStatic.active, true);
    assert.equal(restoredStatic.artworkMode, 'custom');
    assert.deepEqual(getPlaylistTracks(target, 'owner', restoredStatic.playlistKey).map((t) => t.ratingKey), ['n2', 'n1']);

    const restoredImport = rows.find((row) => row.playlistTitle === 'Plex Faves');
    assert.equal(restoredImport.sourceType, 'plex-playlist');
    assert.equal(restoredImport.active, false);
    assert.equal(restoredImport.backupOnly, true);
    assert.equal(restoredImport.importedSyncPeriod, 'weekly');
    assert.deepEqual(listImportedPlaylistUnmatched(target, 'owner', restoredImport.playlistKey).map((row) => row.title), ['Third']);

    const prefs = getUserPreferences(target, 'owner');
    assert.deepEqual(prefs.likedArtists, ['Artist']);
    assert.deepEqual(prefs.smartConfig, { songSkipLimit: 4 });

    for (const job of jobs) await job();
    const syncKinds = state.syncs.map((entry) => entry[0]).sort();
    // Backup-only playlists stay out of Plex; the smart and static ones sync, and the system
    // playlist gets its custom name back.
    assert.deepEqual(syncKinds, ['custom', 'personal', 'rename']);
    assert.deepEqual(state.syncs.find((entry) => entry[0] === 'rename').slice(2), ['crescive', 'My Rising']);
  });

  it('renames or skips playlists whose names are already taken', async () => {
    const source = makeDb();
    seedLibrary(source);
    seedPlaylists(source);
    const backup = parsePlaylistBackup(await buildPlaylistBackup(makeCtx(source).ctx, { userIds: ['owner'], playlistKeys: ['personal:pp_one'] }));
    const target = makeDb();
    seedLibrary(target);
    createUserPersonalPlaylist(target, 'owner', { id: 'pp_existing', name: 'Night Drive', rules: {} });
    const { ctx } = makeCtx(target);

    const skipped = restorePlaylistBackup(ctx, backup, { userId: 'owner', onConflict: 'skip', queueSync: () => {} });
    assert.equal(skipped.restored.length, 0);
    assert.equal(skipped.skipped.length, 1);
    const renamed = restorePlaylistBackup(ctx, backup, { userId: 'owner', onConflict: 'rename', queueSync: () => {} });
    assert.equal(renamed.restored[0].name, 'Night Drive (restored)');
    assert.ok(getUserPersonalPlaylist(target, 'pp_existing', 'owner'));
  });

  it('restores global playlists as global for admins and as personal smart playlists for users', async () => {
    const db = makeDb();
    seedLibrary(db);
    const config = { globalPlaylists: [{ id: 'gp_one', name: 'House Party', rules: { maxTracks: 40, artwork: { mode: 'auto' } }, enabled: true }] };
    const backup = parsePlaylistBackup(await buildPlaylistBackup(makeCtx(db, { config }).ctx, { userIds: ['admin'], includeGlobal: true }));
    assert.equal(backup.playlists[0].type, 'global');

    const target = makeDb();
    seedLibrary(target);
    const admin = makeCtx(target);
    restorePlaylistBackup(admin.ctx, backup, { userId: 'admin', isAdmin: true, queueSync: () => {} });
    assert.equal(admin.state.config.globalPlaylists.length, 1);
    assert.equal(admin.state.config.globalPlaylists[0].name, 'House Party');

    const user = makeCtx(target);
    restorePlaylistBackup(user.ctx, backup, { userId: 'listener', isAdmin: false, queueSync: () => {} });
    assert.equal(user.state.config.globalPlaylists.length, 0);
    const personal = listUserPersonalPlaylists(target, 'listener');
    assert.equal(personal[0].name, 'House Party');
    assert.equal(personal[0].rules.rebuildSchedule, 'daily');
  });

  it('restores an admin backup to each original owner', async () => {
    const source = makeDb();
    seedLibrary(source);
    customPlaylist(source, 'alice', 'a-1', 'Alice Mix');
    setPlaylistTracks(source, 'alice', 'a-1', [{ ratingKey: 't1' }]);
    customPlaylist(source, 'bob', 'b-1', 'Bob Mix');
    setPlaylistTracks(source, 'bob', 'b-1', [{ ratingKey: 't2' }]);
    const backup = parsePlaylistBackup(await buildPlaylistBackup(makeCtx(source).ctx, { userIds: ['alice', 'bob'] }));

    const target = makeDb();
    seedLibrary(target);
    const { ctx } = makeCtx(target);
    restorePlaylistBackup(ctx, backup, { userId: 'admin', isAdmin: true, ownerMode: 'original', queueSync: () => {} });
    assert.equal(listUserGeneratedPlaylists(target, 'alice', { activeOnly: false })[0].playlistTitle, 'Alice Mix');
    assert.equal(listUserGeneratedPlaylists(target, 'bob', { activeOnly: false })[0].playlistTitle, 'Bob Mix');

    // A regular user cannot direct a restore at other accounts.
    const other = makeDb();
    seedLibrary(other);
    restorePlaylistBackup(makeCtx(other).ctx, backup, { userId: 'carol', isAdmin: false, ownerMode: 'original', queueSync: () => {} });
    assert.equal(listUserGeneratedPlaylists(other, 'alice', { activeOnly: false }).length, 0);
    assert.equal(listUserGeneratedPlaylists(other, 'carol', { activeOnly: false }).length, 2);
  });
});

describe('playlist backup routes', () => {
  function makeApp(db) {
    const { ctx, state } = makeCtx(db);
    const app = express();
    app.use(express.json({ limit: '100kb' }));
    registerPlaylistBackup(app, ctx);
    return { app, state };
  }

  it('downloads a backup and an M3U, and restores a body larger than the JSON limit', async () => {
    const db = makeDb();
    refreshMasterTracks(db, [track('t1', { trackTitle: 'First' })]);
    customPlaylist(db, 'owner', 'custom-a', 'Road Trip');
    setPlaylistTracks(db, 'owner', 'custom-a', [{ ratingKey: 't1' }]);
    const { app } = makeApp(db);

    const exported = await request(app).get('/api/music/playlists/backup/export').expect(200);
    assert.match(exported.headers['content-disposition'], /curatorr-playlists-\d{4}-\d{2}-\d{2}\.curatorr\.json/);
    const backup = JSON.parse(exported.text);
    assert.equal(backup.playlists[0].name, 'Road Trip');

    const m3u = await request(app)
      .get('/api/music/playlists/backup/export.m3u?key=custom-a')
      .buffer(true)
      .parse((res, done) => {
        let data = '';
        res.on('data', (chunk) => { data += chunk; });
        res.on('end', () => done(null, data));
      })
      .expect(200);
    assert.match(m3u.headers['content-disposition'], /road-trip\.m3u8/);
    assert.match(m3u.body, /#EXTINF:200,Artist - First\n#EXTALB:Album\n\/music\/Artist\/Album\/t1\.flac/);

    await request(app).get('/api/music/playlists/backup/export?scope=all').expect(403);

    // Pad the backup past the app-wide JSON limit, as embedded artwork would.
    backup.padding = 'x'.repeat(200 * 1024);
    const body = JSON.stringify({ backup, options: { onConflict: 'rename' } });
    const preview = await request(app)
      .post('/api/music/playlists/backup/preview')
      .set('Content-Type', PLAYLIST_BACKUP_CONTENT_TYPE)
      .send(body)
      .expect(200);
    assert.equal(preview.body.playlists[0].conflict, true);

    const restored = await request(app)
      .post('/api/music/playlists/backup/restore')
      .set('Content-Type', PLAYLIST_BACKUP_CONTENT_TYPE)
      .send(body)
      .expect(200);
    assert.equal(restored.body.restored[0].name, 'Road Trip (restored)');

    const invalid = await request(app)
      .post('/api/music/playlists/backup/preview')
      .set('Content-Type', PLAYLIST_BACKUP_CONTENT_TYPE)
      .send(JSON.stringify({ backup: { format: 'nope' } }))
      .expect(400);
    assert.match(invalid.body.error, /not a Curatorr playlist backup/);
  });
});
