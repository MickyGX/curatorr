import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  buildMaLookups,
  matchMaTrack,
  pickServerMapping,
  ratingKeyFromProviderItemId,
} from '../services/music-assistant/identity.js';

// Shapes taken from the MA 2.10.4 test install (music/tracks/library_items summary:false).
const LIBRARY = [
  { ratingKey: '512405', artistName: 'The Beatles', trackTitle: 'Cayenne', albumName: 'Anthology 1', durationMs: 73000, recordingMbid: '1c4411ac-e515-3a8e-9b4c-224b67ba40ae' },
  { ratingKey: '528625', artistName: 'Alice Cooper', trackTitle: '$1000 High Heel Shoes', durationMs: 209000, recordingMbid: '' },
  { ratingKey: '600001', artistName: 'Jorn', trackTitle: "Don't Stop Believin'", durationMs: 260000, recordingMbid: '' },
];

function maTrack({ itemId, instance = 'plex--D5gauxcg', domain = 'plex', mbid = '', name, artists, duration = 0 }) {
  return {
    uri: 'library://track/1',
    name,
    duration,
    artists: artists.map((artistName) => ({ name: artistName })),
    external_ids: mbid ? [['musicbrainz_recordingid', mbid]] : [],
    provider_mappings: itemId ? [{ item_id: itemId, provider_domain: domain, provider_instance: instance }] : [],
  };
}

describe('Music Assistant identity', () => {
  const lookups = buildMaLookups(LIBRARY);
  const options = { providerInstance: 'plex--D5gauxcg', serverType: 'plex' };

  it('strips the Plex /library/metadata/ prefix and passes Jellyfin ids through', () => {
    assert.equal(ratingKeyFromProviderItemId('/library/metadata/528625', 'plex'), '528625');
    assert.equal(ratingKeyFromProviderItemId('528625', 'plex'), '528625');
    assert.equal(ratingKeyFromProviderItemId('a1b2c3d4e5', 'jellyfin'), 'a1b2c3d4e5');
  });

  it('prefers the configured provider instance, then falls back to the server domain', () => {
    const mappings = [
      { item_id: '/library/metadata/1', provider_domain: 'plex', provider_instance: 'plex--other' },
      { item_id: '/library/metadata/2', provider_domain: 'plex', provider_instance: 'plex--D5gauxcg' },
    ];
    assert.equal(pickServerMapping(mappings, options).item_id, '/library/metadata/2');
    assert.equal(pickServerMapping(mappings, { serverType: 'plex' }).item_id, '/library/metadata/1');
    assert.equal(pickServerMapping(mappings, { serverType: 'jellyfin' }), null);
  });

  it('matches through the provider mapping first', () => {
    const result = matchMaTrack(lookups, maTrack({ itemId: '/library/metadata/528625', name: 'Wrong title', artists: ['Nobody'] }), options);
    assert.equal(result.method, 'provider');
    assert.equal(result.ratingKey, '528625');
  });

  it('falls back to the MusicBrainz recording id (full item or playback report)', () => {
    const full = matchMaTrack(lookups, maTrack({ mbid: '1C4411AC-e515-3a8e-9b4c-224b67ba40ae', name: 'x', artists: ['y'] }), options);
    assert.deepEqual([full.method, full.ratingKey], ['mbid', '512405']);
    const reportShape = { uri: 'library://track/9', name: 'x', artist: 'y', mbid: '1c4411ac-e515-3a8e-9b4c-224b67ba40ae', duration: 73 };
    assert.equal(matchMaTrack(lookups, reportShape, options).ratingKey, '512405');
  });

  it('falls back to artist + title, including the playback report shape (artists as strings)', () => {
    const result = matchMaTrack(lookups, { name: 'Cayenne', artists: ['The Beatles'], duration: 74 }, options);
    assert.deepEqual([result.method, result.ratingKey], ['text', '512405']);
  });

  it('does not match a same-titled track by a different artist', () => {
    const result = matchMaTrack(lookups, maTrack({ name: "Don't Stop Believin'", artists: ['Journey'], duration: 250 }), options);
    assert.equal(result.method, 'none');
    assert.equal(result.ratingKey, '');
  });

  it('ignores a provider mapping whose rating key is not in the library', () => {
    const result = matchMaTrack(lookups, maTrack({ itemId: '/library/metadata/999', name: 'Cayenne', artists: ['The Beatles'] }), options);
    assert.deepEqual([result.method, result.ratingKey], ['text', '512405']);
  });
});
