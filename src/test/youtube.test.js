import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createYouTubeService, parseYouTubePlaylistReference } from '../services/youtube.js';

describe('YouTube playlist reference parsing', () => {
  it('accepts playlist URLs', () => {
    const parsed = parseYouTubePlaylistReference('https://www.youtube.com/playlist?list=PL1234567890abcdef');
    assert.deepEqual(parsed, {
      id: 'PL1234567890abcdef',
      kind: 'url',
      raw: 'https://www.youtube.com/playlist?list=PL1234567890abcdef',
    });
  });

  it('accepts watch URLs with playlist ids', () => {
    const parsed = parseYouTubePlaylistReference('https://www.youtube.com/watch?v=abc123&list=PL1234567890abcdef');
    assert.deepEqual(parsed, {
      id: 'PL1234567890abcdef',
      kind: 'url',
      raw: 'https://www.youtube.com/watch?v=abc123&list=PL1234567890abcdef',
    });
  });

  it('accepts raw playlist ids', () => {
    const parsed = parseYouTubePlaylistReference('PL1234567890abcdef');
    assert.deepEqual(parsed, {
      id: 'PL1234567890abcdef',
      kind: 'id',
      raw: 'PL1234567890abcdef',
    });
  });

  it('rejects non-playlist URLs', () => {
    assert.equal(parseYouTubePlaylistReference('https://www.youtube.com/watch?v=abc123'), null);
  });
});

describe('YouTube playlist API requests', () => {
  it('uses playlist and playlistItems endpoints with the API key', async () => {
    const originalFetch = global.fetch;
    const originalApiKey = process.env.YOUTUBE_API_KEY;
    process.env.YOUTUBE_API_KEY = 'youtube-key';
    const requests = [];
    global.fetch = async (url) => {
      const rawUrl = String(url);
      requests.push(rawUrl);
      if (rawUrl.indexOf('/playlists?') !== -1) {
        return {
          ok: true,
          status: 200,
          async text() {
            return JSON.stringify({
              items: [{
                id: 'PL1234567890abcdef',
                snippet: {
                  title: 'Britpop',
                  description: 'Desc',
                  channelId: 'channel-1',
                  channelTitle: 'Curatorr',
                  thumbnails: {
                    high: { url: 'https://i.ytimg.com/test.jpg' },
                  },
                },
                contentDetails: {
                  itemCount: 2,
                },
              }],
            });
          },
        };
      }
      return {
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({
            items: [{
              snippet: {
                position: 0,
                title: 'Blur - Girls & Boys',
                publishedAt: '2025-04-12T00:00:00Z',
                channelId: 'channel-1',
                channelTitle: 'Blur',
                videoOwnerChannelTitle: 'Blur',
                resourceId: { videoId: 'video-1' },
                thumbnails: {
                  medium: { url: 'https://i.ytimg.com/video-1.jpg' },
                },
              },
              contentDetails: {
                videoId: 'video-1',
              },
            }],
          });
        },
      };
    };

    try {
      const youtube = createYouTubeService();
      const playlist = await youtube.getPlaylist('PL1234567890abcdef');
      const items = await youtube.getPlaylistItems('PL1234567890abcdef', { limit: 50 });
      assert.equal(playlist.trackCount, 2);
      assert.equal(items.total, 1);
      assert.equal(items.items[0].trackId, 'video-1');
      assert.match(requests[0], /\/playlists\?/);
      assert.match(requests[0], /part=snippet%2CcontentDetails/);
      assert.match(requests[0], /key=youtube-key/);
      assert.match(requests[1], /\/playlistItems\?/);
      assert.match(requests[1], /playlistId=PL1234567890abcdef/);
    } finally {
      global.fetch = originalFetch;
      if (originalApiKey === undefined) delete process.env.YOUTUBE_API_KEY;
      else process.env.YOUTUBE_API_KEY = originalApiKey;
    }
  });
});
