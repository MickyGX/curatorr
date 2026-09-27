import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  getMaConfig,
  resolveMaListener,
  resolveQueueListener,
  summarizeMaQueue,
} from '../services/music-assistant/index.js';

// Trimmed from player_queues/get on the MA 2.10.4 test install.
const QUEUE = {
  queue_id: 'd5c5e6f7',
  state: 'playing',
  current_item: {
    name: '3 Doors Down - Let Me Go',
    duration: 225,
    image: { type: 'thumb', path: '/library/metadata/541986/thumb/1774581684', provider: 'plex--D5gauxcg', remotely_accessible: false },
    media_item: { uri: 'library://track/14', name: 'Let Me Go', artists: [{ name: '3 Doors Down' }], album: { name: 'Acoustic Back Porch Jam' } },
  },
};

describe('Music Assistant service helpers', () => {
  it('summarises a queue for the Now Playing card', () => {
    assert.deepEqual(summarizeMaQueue(QUEUE), {
      queueId: 'd5c5e6f7',
      state: 'playing',
      title: 'Let Me Go',
      artist: '3 Doors Down',
      album: 'Acoustic Back Porch Jam',
      uri: 'library://track/14',
      imagePath: '/library/metadata/541986/thumb/1774581684',
    });
    const spotifyArt = { ...QUEUE, current_item: { ...QUEUE.current_item, image: { path: 'https://i.scdn.co/x', provider: 'spotify' } } };
    assert.equal(summarizeMaQueue(spotifyArt).imagePath, '', 'only Plex server art paths are passed through');
    assert.equal(summarizeMaQueue({ queue_id: 'q', state: 'idle', current_item: null }), null);
  });

  it('maps MA users to listeners, dropping unmapped users', () => {
    const cfg = getMaConfig({ musicAssistant: { userMap: { u1: 'MickyGX', u2: '' }, defaultUser: 'HouseHold' } });
    assert.equal(resolveMaListener(cfg, 'u1'), 'MickyGX');
    assert.equal(resolveMaListener(cfg, 'u2'), '', 'explicitly ignored');
    assert.equal(resolveMaListener(cfg, 'u3'), '', 'unknown MA user');
    assert.equal(resolveMaListener(cfg, null), 'HouseHold', 'no MA user -> default listener');
  });

  it('attributes a queue with no play report yet to the only mapped listener', () => {
    const single = getMaConfig({ musicAssistant: { userMap: { u1: 'MickyGX', u2: '' } } });
    assert.equal(resolveQueueListener(single, undefined), 'MickyGX');
    const multi = getMaConfig({ musicAssistant: { userMap: { u1: 'MickyGX', u2: 'Other' }, defaultUser: '' } });
    assert.equal(resolveQueueListener(multi, undefined), '');
    assert.equal(resolveQueueListener(multi, 'u2'), 'Other');
  });
});
