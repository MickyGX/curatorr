import { afterEach, beforeEach, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {
  createTidalPkcePair,
  createTidalService,
  parseTidalDurationMs,
  parseTidalPlaylistReference,
} from '../services/tidal.js';

const PLAYLIST_ID = '0f6a2b4c-1d2e-4f30-8a9b-112233445566';

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'Content-Type': 'application/vnd.api+json' },
  });
}

describe('TIDAL playlist reference parsing', () => {
  it('accepts tidal.com and listen.tidal.com playlist links', () => {
    assert.deepEqual(parseTidalPlaylistReference(`https://tidal.com/playlist/${PLAYLIST_ID}`), {
      id: PLAYLIST_ID,
      kind: 'url',
      raw: `https://tidal.com/playlist/${PLAYLIST_ID}`,
    });
    assert.equal(parseTidalPlaylistReference(`https://listen.tidal.com/playlist/${PLAYLIST_ID}?u`)?.id, PLAYLIST_ID);
    assert.equal(parseTidalPlaylistReference(`https://tidal.com/browse/playlist/${PLAYLIST_ID.toUpperCase()}`)?.id, PLAYLIST_ID);
  });

  it('accepts raw playlist ids', () => {
    assert.deepEqual(parseTidalPlaylistReference(PLAYLIST_ID), { id: PLAYLIST_ID, kind: 'id', raw: PLAYLIST_ID });
  });

  it('rejects non-playlist and non-TIDAL links', () => {
    assert.equal(parseTidalPlaylistReference('https://tidal.com/album/12345'), null);
    assert.equal(parseTidalPlaylistReference(`https://example.com/playlist/${PLAYLIST_ID}`), null);
    assert.equal(parseTidalPlaylistReference(`https://nottidal.com/playlist/${PLAYLIST_ID}`), null);
    assert.equal(parseTidalPlaylistReference('37i9dQZF1DXaVgr4Tx5kRF'), null);
  });
});

describe('TIDAL helpers', () => {
  it('parses ISO 8601 durations', () => {
    assert.equal(parseTidalDurationMs('PT3M25S'), 205000);
    assert.equal(parseTidalDurationMs('PT1H2M3.5S'), 3723500);
    assert.equal(parseTidalDurationMs('PT45S'), 45000);
    assert.equal(parseTidalDurationMs(''), 0);
    assert.equal(parseTidalDurationMs('3:25'), 0);
  });

  it('creates an S256 PKCE pair', () => {
    const { codeVerifier, codeChallenge } = createTidalPkcePair();
    assert.match(codeVerifier, /^[A-Za-z0-9_-]{43,128}$/);
    assert.equal(codeChallenge, crypto.createHash('sha256').update(codeVerifier).digest('base64url'));
  });
});

describe('TIDAL service', () => {
  const originalFetch = globalThis.fetch;
  const originalEnv = { id: process.env.TIDAL_CLIENT_ID, secret: process.env.TIDAL_CLIENT_SECRET };

  beforeEach(() => {
    process.env.TIDAL_CLIENT_ID = 'tidal-client-id';
    process.env.TIDAL_CLIENT_SECRET = 'tidal-client-secret';
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    if (originalEnv.id === undefined) delete process.env.TIDAL_CLIENT_ID;
    else process.env.TIDAL_CLIENT_ID = originalEnv.id;
    if (originalEnv.secret === undefined) delete process.env.TIDAL_CLIENT_SECRET;
    else process.env.TIDAL_CLIENT_SECRET = originalEnv.secret;
  });

  it('is not configured without client credentials', () => {
    delete process.env.TIDAL_CLIENT_SECRET;
    assert.equal(createTidalService().isConfigured(), false);
  });

  it('builds a PKCE authorize URL with the playlist scope', () => {
    const service = createTidalService();
    const { url, redirectUri } = service.getAuthorizationUrl({
      baseUrl: 'https://curatorr.example.com/',
      state: 'state-1',
      codeChallenge: 'challenge-1',
    });
    const parsed = new URL(url);
    assert.equal(parsed.origin + parsed.pathname, 'https://login.tidal.com/authorize');
    assert.equal(parsed.searchParams.get('client_id'), 'tidal-client-id');
    assert.equal(parsed.searchParams.get('response_type'), 'code');
    assert.equal(parsed.searchParams.get('code_challenge'), 'challenge-1');
    assert.equal(parsed.searchParams.get('code_challenge_method'), 'S256');
    assert.equal(parsed.searchParams.get('state'), 'state-1');
    assert.ok(parsed.searchParams.get('scope').split(' ').includes('playlists.read'));
    assert.equal(redirectUri, 'https://curatorr.example.com/user-settings/tidal/callback');
    assert.equal(parsed.searchParams.get('redirect_uri'), redirectUri);
  });

  it('sends the PKCE verifier when exchanging the authorization code', async () => {
    let tokenBody = null;
    globalThis.fetch = async (url, options) => {
      assert.equal(String(url), 'https://auth.tidal.com/v1/oauth2/token');
      tokenBody = new URLSearchParams(String(options.body));
      return jsonResponse({ access_token: 'access-1', refresh_token: 'refresh-1', expires_in: 3600, user_id: 42 });
    };
    const token = await createTidalService().exchangeCode({
      code: 'code-1',
      redirectUri: 'https://curatorr.example.com/user-settings/tidal/callback',
      codeVerifier: 'verifier-1',
    });
    assert.equal(tokenBody.get('grant_type'), 'authorization_code');
    assert.equal(tokenBody.get('code'), 'code-1');
    assert.equal(tokenBody.get('code_verifier'), 'verifier-1');
    assert.equal(tokenBody.get('client_id'), 'tidal-client-id');
    assert.equal(tokenBody.get('client_secret'), 'tidal-client-secret');
    assert.equal(token.accessToken, 'access-1');
    assert.equal(token.refreshToken, 'refresh-1');
    assert.equal(token.userId, '42');
    assert.ok(token.expiresAt > Date.now());
  });

  it('reuses the cached client credentials token', async () => {
    let tokenRequests = 0;
    globalThis.fetch = async () => {
      tokenRequests += 1;
      return jsonResponse({ access_token: 'client-token', expires_in: 3600 });
    };
    const service = createTidalService();
    assert.equal(await service.getClientCredentialsToken(), 'client-token');
    assert.equal(await service.getClientCredentialsToken(), 'client-token');
    assert.equal(tokenRequests, 1);
  });

  it('lists the current user playlists across cursor pages', async () => {
    const requests = [];
    globalThis.fetch = async (url) => {
      const parsed = new URL(String(url));
      requests.push(parsed);
      const cursor = parsed.searchParams.get('page[cursor]');
      if (!cursor) {
        return jsonResponse({
          data: [{ id: 'aaaaaaaa-0000-4000-8000-000000000001', type: 'playlists', attributes: { name: 'Road Trip', numberOfTrackItems: 12, accessType: 'PUBLIC' } }],
          links: { self: '/playlists', meta: { nextCursor: 'page-2' } },
        });
      }
      return jsonResponse({
        data: [{ id: 'aaaaaaaa-0000-4000-8000-000000000002', type: 'playlists', attributes: { name: 'Focus', numberOfItems: 3, accessType: 'UNLISTED' } }],
        links: { self: '/playlists' },
      });
    };
    const playlists = await createTidalService().listCurrentUserPlaylists('user-token', { countryCode: 'gb', ownerName: 'Me' });
    assert.equal(requests.length, 2);
    assert.equal(requests[0].pathname, '/v2/playlists');
    assert.equal(requests[0].searchParams.get('filter[owners.id]'), 'me');
    assert.equal(requests[0].searchParams.get('countryCode'), 'GB');
    assert.equal(requests[1].searchParams.get('page[cursor]'), 'page-2');
    assert.deepEqual(playlists.map((p) => [p.name, p.trackCount, p.public, p.ownerName]), [
      ['Road Trip', 12, true, 'Me'],
      ['Focus', 3, false, 'Me'],
    ]);
    assert.equal(playlists[0].externalUrl, 'https://tidal.com/playlist/aaaaaaaa-0000-4000-8000-000000000001');
  });

  it('joins track artists and albums, keeps playlist order, and skips videos', async () => {
    const trackRequests = [];
    // 25 tracks (forces two /tracks batches), plus a video and a repeated track.
    const trackIds = Array.from({ length: 25 }, (_, index) => String(1000 + index));
    const itemRefs = [
      ...trackIds.slice(0, 10).map((id) => ({ id, type: 'tracks' })),
      { id: 'video-1', type: 'videos' },
      ...trackIds.slice(10).map((id) => ({ id, type: 'tracks' })),
      { id: trackIds[0], type: 'tracks' },
    ];
    globalThis.fetch = async (url) => {
      const parsed = new URL(String(url));
      if (parsed.pathname === `/v2/playlists/${PLAYLIST_ID}/relationships/items`) {
        const cursor = parsed.searchParams.get('page[cursor]');
        return jsonResponse(cursor
          ? { data: itemRefs.slice(20), links: { self: '' } }
          : { data: itemRefs.slice(0, 20), links: { self: '', meta: { nextCursor: 'next' } } });
      }
      if (parsed.pathname === '/v2/tracks') {
        const ids = parsed.searchParams.getAll('filter[id]');
        trackRequests.push({ ids, include: parsed.searchParams.getAll('include') });
        return jsonResponse({
          // Returned out of order, as the API does not promise filter order.
          data: [...ids].reverse().map((id) => ({
            id,
            type: 'tracks',
            attributes: { title: `Song ${id}`, duration: 'PT3M', isrc: `ISRC${id}` },
            relationships: {
              artists: { data: [{ id: `artist-${id}`, type: 'artists' }, { id: 'artist-shared', type: 'artists' }] },
              albums: { data: [{ id: 'album-1', type: 'albums' }] },
            },
          })),
          included: [
            ...ids.map((id) => ({ id: `artist-${id}`, type: 'artists', attributes: { name: `Artist ${id}` } })),
            { id: 'artist-shared', type: 'artists', attributes: { name: 'Guest' } },
            { id: 'album-1', type: 'albums', attributes: { title: 'The Album', albumType: 'ALBUM' } },
          ],
        });
      }
      throw new Error(`Unexpected request: ${parsed}`);
    };

    const result = await createTidalService().getPlaylistItems('token', PLAYLIST_ID, { countryCode: 'US' });

    assert.deepEqual(trackRequests.map((request) => request.ids.length), [20, 5]);
    assert.deepEqual(trackRequests[0].include, ['artists', 'albums']);
    assert.equal(result.total, 26);
    assert.deepEqual(result.items.slice(0, 2).map((item) => item.id), ['1000', '1001']);
    assert.equal(result.items[10].id, '1010', 'video is skipped without breaking order');
    assert.equal(result.items[25].id, '1000', 'repeated tracks are kept for duplicate reporting');
    assert.deepEqual(result.items.map((item) => item.position), Array.from({ length: 26 }, (_, index) => index + 1));
    assert.deepEqual(result.items[0], {
      id: '1000',
      title: 'Song 1000',
      artists: [{ name: 'Artist 1000' }, { name: 'Guest' }],
      album: { title: 'The Album', albumType: 'album', imageUrl: '' },
      durationMs: 180000,
      isrc: 'ISRC1000',
      position: 1,
    });
  });

  it('retries a rate-limited request after Retry-After', async () => {
    let calls = 0;
    globalThis.fetch = async () => {
      calls += 1;
      if (calls === 1) return new Response('', { status: 429, headers: { 'Retry-After': '0.01' } });
      return jsonResponse({ data: { id: PLAYLIST_ID, type: 'playlists', attributes: { name: 'Retry' } } });
    };
    const playlist = await createTidalService().getPlaylist('token', PLAYLIST_ID);
    assert.equal(calls, 2);
    assert.equal(playlist.name, 'Retry');
  });

  it('surfaces JSON:API error details with the HTTP status', async () => {
    globalThis.fetch = async () => jsonResponse({ errors: [{ status: '404', detail: 'Playlist not found' }] }, 404);
    await assert.rejects(
      () => createTidalService().getPlaylist('token', PLAYLIST_ID),
      (err) => err.status === 404 && err.message === 'Playlist not found',
    );
  });
});
