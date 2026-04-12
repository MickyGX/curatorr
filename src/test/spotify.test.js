import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createSpotifyService, parseSpotifyPlaylistReference, parseSpotifyPublicPlaylistPage } from '../services/spotify.js';

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
          if (String(url).includes('/tracks?')) {
            return JSON.stringify({
              total: 1,
              items: [{
                added_at: '2025-04-12T00:00:00Z',
                is_local: false,
                track: {
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
            });
          }
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
      assert.match(requests[1], /\/v1\/playlists\/37i9dQZF1DXaVgr4Tx5kRF\/tracks\?/);
      assert.match(requests[1], /track%28/);
    } finally {
      global.fetch = originalFetch;
      if (originalClientId === undefined) delete process.env.SPOTIFY_CLIENT_ID;
      else process.env.SPOTIFY_CLIENT_ID = originalClientId;
      if (originalClientSecret === undefined) delete process.env.SPOTIFY_CLIENT_SECRET;
      else process.env.SPOTIFY_CLIENT_SECRET = originalClientSecret;
    }
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
