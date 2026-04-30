import { after, beforeEach, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const testDir = fs.mkdtempSync(path.join(os.tmpdir(), 'curatorr-playlist-artwork-'));
process.env.DATA_DIR = testDir;
process.env.BASE_URL = 'http://curatorr.local';

const { initDb, saveUserGeneratedPlaylist } = await import('../db.js');
const { savePlaylistArtworkBuffer } = await import('../services/playlist-artwork.js');
const { createPlaylistService } = await import('../services/playlists.js');

function buildAppApiUrl(base, relativePath) {
  return new URL(String(relativePath || '').replace(/^\/+/, '/'), `${String(base || '').replace(/\/+$/, '')}/`);
}

function createPngFixture() {
  return Buffer.from([
    0x89, 0x50, 0x4e, 0x47,
    0x0d, 0x0a, 0x1a, 0x0a,
    0x00, 0x00, 0x00, 0x0d,
  ]);
}

function createTestContext(db) {
  return {
    db,
    loadConfig() {
      return {
        mediaServer: { type: 'plex' },
        plex: { url: 'http://plex.local' },
      };
    },
    resolveUserPlexServerToken() {
      return 'plex-token';
    },
    buildPlexAuthHeaders(token, extraHeaders = {}) {
      return {
        'X-Plex-Token': String(token || ''),
        ...extraHeaders,
      };
    },
    buildAppApiUrl,
    pushLog() {},
    userHasOwnPlexToken() {
      return true;
    },
  };
}

describe('playlist artwork sync', () => {
  const originalFetch = global.fetch;
  let db;

  beforeEach(() => {
    if (db) db.close();
    const dbPath = path.join(testDir, `playlist-artwork-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
    db = initDb(dbPath);
  });

  after(() => {
    global.fetch = originalFetch;
    if (db) db.close();
    fs.rmSync(testDir, { recursive: true, force: true });
  });

  it('uploads stored playlist artwork to Plex as binary data before falling back to a URL', async () => {
    const assetName = savePlaylistArtworkBuffer(createPngFixture(), 'png', 'direct-upload', 'test');
    assert.ok(assetName);

    saveUserGeneratedPlaylist(db, 'alice', {
      playlistType: 'custom',
      playlistKey: 'custom:direct-upload',
      plexPlaylistId: 'playlist-1',
      playlistTitle: 'Direct Upload',
      artworkMode: 'auto',
      customArtworkAsset: '',
      preservedArtworkAsset: '',
      active: true,
    });

    const requests = [];
    global.fetch = async (url, options = {}) => {
      requests.push({ url: String(url || ''), options });
      return new Response('', { status: 200 });
    };

    const playlistService = createPlaylistService(createTestContext(db));
    await playlistService.updateGeneratedPlaylistArtwork('alice', 'custom:direct-upload', {
      mode: 'custom',
      customArtworkAsset: assetName,
    });

    assert.equal(requests.length, 1);
    assert.equal(requests[0].url, 'http://plex.local/library/metadata/playlist-1/thumb');
    assert.equal(requests[0].options.method, 'POST');
    assert.equal(requests[0].options.headers['Content-Type'], 'image/png');
    assert.ok(Buffer.isBuffer(requests[0].options.body));
    assert.ok(requests[0].options.body.length > 0);
  });

  it('falls back to Plex url-based artwork updates when binary upload fails', async () => {
    const assetName = savePlaylistArtworkBuffer(createPngFixture(), 'png', 'fallback-upload', 'test');
    assert.ok(assetName);

    saveUserGeneratedPlaylist(db, 'alice', {
      playlistType: 'custom',
      playlistKey: 'custom:fallback-upload',
      plexPlaylistId: 'playlist-2',
      playlistTitle: 'Fallback Upload',
      artworkMode: 'auto',
      customArtworkAsset: '',
      preservedArtworkAsset: '',
      active: true,
    });

    const requests = [];
    global.fetch = async (url, options = {}) => {
      requests.push({ url: String(url || ''), options });
      if (options.method === 'POST') return new Response('', { status: 500 });
      return new Response('', { status: 200 });
    };

    const playlistService = createPlaylistService(createTestContext(db));
    await playlistService.updateGeneratedPlaylistArtwork('alice', 'custom:fallback-upload', {
      mode: 'custom',
      customArtworkAsset: assetName,
    });

    assert.equal(requests.length, 2);
    assert.equal(requests[0].url, 'http://plex.local/library/metadata/playlist-2/thumb');
    assert.equal(requests[0].options.method, 'POST');
    assert.equal(requests[1].options.method, 'PUT');
    const fallbackUrl = new URL(requests[1].url);
    assert.equal(fallbackUrl.origin + fallbackUrl.pathname, 'http://plex.local/library/metadata/playlist-2/thumb');
    assert.match(fallbackUrl.searchParams.get('url') || '', /^http:\/\/curatorr\.local\/api\/music\/playlists\/artwork\//);
  });
});
