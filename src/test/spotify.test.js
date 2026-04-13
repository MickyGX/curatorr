import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createSpotifyService, parseSpotifyPlaylistReference, parseSpotifyPublicPlaylistPage, parseSpotifyEmbedPlaylistPage } from '../services/spotify.js';

describe('Spotify playlist reference parsing', () => {
  it('accepts Spotify share links with query strings', () => {
    const parsed = parseSpotifyPlaylistReference('https://open.spotify.com/playlist/37i9dQZF1DXaVgr4Tx5kRF?si=tmXwDy1-QcKmiB2osuS33Q');
    assert.deepEqual(parsed, {
      id: '37i9dQZF1DXaVgr4Tx5kRF',
      kind: 'url',
      raw: 'https://open.spotify.com/playlist/37i9dQZF1DXaVgr4Tx5kRF?si=tmXwDy1-QcKmiB2osuS33Q',
    });
  });

  it('accepts spotify playlist uris', () => {
    const parsed = parseSpotifyPlaylistReference('spotify:playlist:37i9dQZF1DXaVgr4Tx5kRF');
    assert.deepEqual(parsed, {
      id: '37i9dQZF1DXaVgr4Tx5kRF',
      kind: 'uri',
      raw: 'spotify:playlist:37i9dQZF1DXaVgr4Tx5kRF',
    });
  });

  it('accepts raw playlist ids', () => {
    const parsed = parseSpotifyPlaylistReference('37i9dQZF1DXaVgr4Tx5kRF');
    assert.deepEqual(parsed, {
      id: '37i9dQZF1DXaVgr4Tx5kRF',
      kind: 'id',
      raw: '37i9dQZF1DXaVgr4Tx5kRF',
    });
  });

  it('rejects non-playlist spotify links', () => {
    assert.equal(parseSpotifyPlaylistReference('https://open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT'), null);
  });
});

describe('Spotify playlist API requests', () => {
  it('uses the playlist tracks endpoint for playlist previews', async () => {
    const originalFetch = global.fetch;
    const originalClientId = process.env.SPOTIFY_CLIENT_ID;
    const originalClientSecret = process.env.SPOTIFY_CLIENT_SECRET;
    process.env.SPOTIFY_CLIENT_ID = 'client-id';
    process.env.SPOTIFY_CLIENT_SECRET = 'client-secret';
    const requests = [];
    global.fetch = async (url) => {
      requests.push(String(url));
      return {
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({
            id: '37i9dQZF1DXaVgr4Tx5kRF',
            name: 'Playlist',
            description: '',
            public: true,
            collaborative: false,
            owner: { id: 'spotify', display_name: 'Spotify' },
            images: [{ url: 'https://i.scdn.co/image/playlist' }],
            external_urls: { spotify: 'https://open.spotify.com/playlist/37i9dQZF1DXaVgr4Tx5kRF' },
            tracks: { total: 1 },
            snapshot_id: 'snap',
            items: {
              total: 1,
              items: [{
                added_at: '2025-04-12T00:00:00Z',
                is_local: false,
                item: {
                  id: 'track-1',
                  name: 'Song',
                  duration_ms: 180000,
                  explicit: false,
                  popularity: 42,
                  preview_url: null,
                  external_urls: { spotify: 'https://open.spotify.com/track/track-1' },
                  external_ids: { isrc: 'GBABC1234567' },
                  artists: [{ id: 'artist-1', name: 'Artist' }],
                  album: {
                    id: 'album-1',
                    name: 'Album',
                    album_type: 'album',
                    release_date: '2025-01-01',
                    images: [{ url: 'https://i.scdn.co/image/test' }],
                    external_urls: { spotify: 'https://open.spotify.com/album/album-1' },
                  },
                },
              }],
              next: null,
            },
          });
        },
      };
    };

    try {
      const spotify = createSpotifyService();
      const playlist = await spotify.getPlaylist('access-token', '37i9dQZF1DXaVgr4Tx5kRF');
      const items = await spotify.getPlaylistItems('access-token', '37i9dQZF1DXaVgr4Tx5kRF', { limit: 100 });
      assert.equal(playlist.trackCount, 1);
      assert.equal(items.total, 1);
      assert.equal(items.items[0].trackId, 'track-1');
      assert.match(requests[0], /\/v1\/playlists\/37i9dQZF1DXaVgr4Tx5kRF\?/);
      assert.match(requests[0], /tracks%28total%29/);
      assert.match(requests[1], /\/v1\/playlists\/37i9dQZF1DXaVgr4Tx5kRF\?/);
      assert.match(requests[1], /items%28/);
    } finally {
      global.fetch = originalFetch;
      if (originalClientId === undefined) delete process.env.SPOTIFY_CLIENT_ID;
      else process.env.SPOTIFY_CLIENT_ID = originalClientId;
      if (originalClientSecret === undefined) delete process.env.SPOTIFY_CLIENT_SECRET;
      else process.env.SPOTIFY_CLIENT_SECRET = originalClientSecret;
    }
  });
});

describe('Spotify client credentials token', () => {
  it('requests an app token with the client_credentials grant', async () => {
    const originalFetch = global.fetch;
    const originalClientId = process.env.SPOTIFY_CLIENT_ID;
    const originalClientSecret = process.env.SPOTIFY_CLIENT_SECRET;
    process.env.SPOTIFY_CLIENT_ID = 'client-id';
    process.env.SPOTIFY_CLIENT_SECRET = 'client-secret';
    let request = null;
    global.fetch = async (url, options = {}) => {
      request = {
        url: String(url),
        method: String(options.method || 'GET').toUpperCase(),
        auth: String(options.headers?.Authorization || ''),
        body: String(options.body || ''),
      };
      return {
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({
            access_token: 'client-token',
            token_type: 'Bearer',
            expires_in: 3600,
          });
        },
      };
    };

    try {
      const spotify = createSpotifyService();
      const token = await spotify.getClientCredentialsToken();
      assert.equal(token, 'client-token');
      assert.equal(request.url, 'https://accounts.spotify.com/api/token');
      assert.equal(request.method, 'POST');
      assert.match(request.auth, /^Basic /);
      assert.match(request.body, /grant_type=client_credentials/);
    } finally {
      global.fetch = originalFetch;
      if (originalClientId === undefined) delete process.env.SPOTIFY_CLIENT_ID;
      else process.env.SPOTIFY_CLIENT_ID = originalClientId;
      if (originalClientSecret === undefined) delete process.env.SPOTIFY_CLIENT_SECRET;
      else process.env.SPOTIFY_CLIENT_SECRET = originalClientSecret;
    }
  });
});

describe('Spotify embed playlist page parser', () => {
  it('parses __NEXT_DATA__ from the embed URL and extracts tracks', () => {
    const nextData = {
      props: {
        pageProps: {
          state: {
            data: {
              entity: {
                name: 'Hot Hits UK',
                description: 'The biggest songs right now.',
                author: 'Spotify',
                author_url: 'https://open.spotify.com/user/spotify',
                cover_url: 'https://i.scdn.co/image/playlist',
                totalCount: 2,
                trackList: [
                  {
                    id: 'track1',
                    uri: 'spotify:track:track1',
                    title: 'Song One',
                    duration: 180000,
                    subtitle: 'Artist One, Artist Two',
                  },
                  {
                    id: 'track2',
                    uri: 'spotify:track:track2',
                    title: 'Song Two',
                    duration: 210000,
                    subtitle: 'Artist Three',
                  },
                ],
              },
            },
          },
        },
      },
    };
    const html = `<html><head><script id="__NEXT_DATA__" type="application/json">${JSON.stringify(nextData)}</script></head></html>`;
    const parsed = parseSpotifyEmbedPlaylistPage(html, '37i9dQZF1DXcBWIGoYBM5M');
    assert.equal(parsed.playlist.id, '37i9dQZF1DXcBWIGoYBM5M');
    assert.equal(parsed.playlist.name, 'Hot Hits UK');
    assert.equal(parsed.playlist.ownerName, 'Spotify');
    assert.equal(parsed.playlist.ownerId, 'spotify');
    assert.equal(parsed.playlist.imageUrl, 'https://i.scdn.co/image/playlist');
    assert.equal(parsed.items.length, 2);
    assert.equal(parsed.items[0].trackId, 'track1');
    assert.equal(parsed.items[0].title, 'Song One');
    assert.equal(parsed.items[0].durationMs, 180000);
    assert.equal(parsed.items[0].artists.length, 2);
    assert.equal(parsed.items[0].artists[0].name, 'Artist One');
    assert.equal(parsed.items[0].artists[1].name, 'Artist Two');
    assert.equal(parsed.items[1].artists[0].name, 'Artist Three');
    assert.equal(parsed.totalCount, 2);
    assert.equal(parsed.partial, false);
    assert.equal(parsed.warning, '');
  });

  it('marks result as partial when totalCount exceeds the trackList length', () => {
    const nextData = {
      props: {
        pageProps: {
          state: {
            data: {
              entity: {
                name: 'Big Playlist',
                author: 'Spotify',
                author_url: 'https://open.spotify.com/user/spotify',
                totalCount: 100,
                trackList: [
                  { id: 'track1', uri: 'spotify:track:track1', title: 'Song One', duration: 180000, subtitle: 'Artist' },
                ],
              },
            },
          },
        },
      },
    };
    const html = `<html><head><script id="__NEXT_DATA__" type="application/json">${JSON.stringify(nextData)}</script></head></html>`;
    const parsed = parseSpotifyEmbedPlaylistPage(html, 'abc123');
    assert.equal(parsed.items.length, 1);
    assert.equal(parsed.totalCount, 100);
    assert.equal(parsed.partial, true);
    assert.match(parsed.warning, /first 1 of 100 tracks/i);
  });

  it('returns null when __NEXT_DATA__ script is absent', () => {
    const parsed = parseSpotifyEmbedPlaylistPage('<html><body>Login</body></html>', 'abc123');
    assert.equal(parsed, null);
  });

  it('returns null when entity path is missing in __NEXT_DATA__', () => {
    const html = `<html><head><script id="__NEXT_DATA__" type="application/json">${JSON.stringify({ props: {} })}</script></head></html>`;
    const parsed = parseSpotifyEmbedPlaylistPage(html, 'abc123');
    assert.equal(parsed, null);
  });
});

describe('Spotify public playlist page fallback', () => {
  it('parses the public page state and marks partial previews', () => {
    const state = {
      entities: {
        items: {
          'spotify:playlist:37i9dQZF1DXaVgr4Tx5kRF': {
            name: 'Britpop, Etc.',
            description: 'Desc',
            images: {
              items: [{ sources: [{ url: 'https://i.scdn.co/image/playlist' }] }],
            },
            ownerV2: {
              data: {
                name: 'Spotify',
                uri: 'spotify:user:spotify',
                username: 'spotify',
              },
            },
            content: {
              totalCount: 5,
              pagingInfo: { nextOffset: 2 },
              items: [
                {
                  itemV2: {
                    data: {
                      uri: 'spotify:track:track1',
                      name: 'Song One',
                      duration: { totalMilliseconds: 180000 },
                      previews: { audioPreviews: { items: [{ url: 'https://p.scdn.co/mp3-preview/test1' }] } },
                      artists: { items: [{ uri: 'spotify:artist:artist1', profile: { name: 'Artist One' } }] },
                      albumOfTrack: {
                        uri: 'spotify:album:album1',
                        name: 'Album One',
                        coverArt: { sources: [{ url: 'https://i.scdn.co/image/album1' }] },
                      },
                    },
                  },
                },
                {
                  itemV2: {
                    data: {
                      uri: 'spotify:track:track2',
                      name: 'Song Two',
                      duration: { totalMilliseconds: 200000 },
                      artists: { items: [{ uri: 'spotify:artist:artist2', profile: { name: 'Artist Two' } }] },
                      albumOfTrack: {
                        uri: 'spotify:album:album2',
                        name: 'Album Two',
                        coverArt: { sources: [{ url: 'https://i.scdn.co/image/album2' }] },
                      },
                    },
                  },
                },
              ],
            },
          },
        },
      },
    };
    const html = `<html><body><script id="initialState" type="text/plain">${Buffer.from(JSON.stringify(state), 'utf8').toString('base64')}</script></body></html>`;
    const parsed = parseSpotifyPublicPlaylistPage(html, '37i9dQZF1DXaVgr4Tx5kRF');
    assert.equal(parsed.playlist.name, 'Britpop, Etc.');
    assert.equal(parsed.playlist.ownerName, 'Spotify');
    assert.equal(parsed.items.length, 2);
    assert.equal(parsed.items[0].trackId, 'track1');
    assert.equal(parsed.items[0].album.id, 'album1');
    assert.equal(parsed.totalCount, 5);
    assert.equal(parsed.partial, true);
    assert.match(parsed.warning, /first 2 of 5 tracks/i);
  });
});
