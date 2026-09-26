import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  buildImportArtistKeys,
  buildSpotifyTrackLookups,
  pickSpotifyTrackMatch,
} from '../services/import-matching.js';

const LIBRARY = [
  { ratingKey: 'jorn-dsb', artistName: 'Jorn', trackTitle: "Don't Stop Believin'", durationMs: 260000 },
  { ratingKey: 'apoc-drive', artistName: 'Apocalyptica', trackTitle: 'Drive', durationMs: 230000 },
  { ratingKey: 'killers-human', artistName: 'The Killers', trackTitle: 'Human', durationMs: 245000 },
  { ratingKey: 'sabbath-dz', artistName: 'Black Sabbath', trackTitle: 'Danger Zone', durationMs: 300000 },
  { ratingKey: 'cars-drive', artistName: 'Cars', trackTitle: 'Drive', durationMs: 235000 },
  { ratingKey: 'collier-mendes', artistName: 'Jacob Collier, Shawn Mendes', trackTitle: 'Witness Me', durationMs: 200000 },
  { ratingKey: 'sg-boxer', artistName: 'Simon and Garfunkel', trackTitle: 'The Boxer', durationMs: 308000 },
  { ratingKey: 'daft-glw', artistName: 'Daft Punk', trackTitle: 'Get Lucky', durationMs: 369000 },
];

function spotifyItem(title, artists, durationMs = 0) {
  return { title, artists: artists.map((name) => ({ name })), durationMs };
}

describe('import track matching', () => {
  const lookups = buildSpotifyTrackLookups(LIBRARY);

  it('leaves tracks unmatched when only a different artist has the same title', () => {
    const cases = [
      spotifyItem("Don't Stop Believin'", ['Journey']),
      spotifyItem('Human', ['The Human League']),
      spotifyItem('Danger Zone', ['Kenny Loggins']),
    ];
    for (const item of cases) {
      const result = pickSpotifyTrackMatch(lookups, item);
      assert.equal(result.method, 'unmatched', `${item.title} should not match another artist`);
      assert.equal(result.match, null);
    }
  });

  it('picks the right artist among same-titled tracks, ignoring a leading "The"', () => {
    const result = pickSpotifyTrackMatch(lookups, spotifyItem('Drive', ['The Cars'], 235000));
    assert.equal(result.match?.ratingKey, 'cars-drive');
  });

  it('matches on any listed artist, not just the first', () => {
    const result = pickSpotifyTrackMatch(lookups, spotifyItem('Get Lucky', ['Pharrell Williams', 'Daft Punk']));
    assert.equal(result.method, 'artistTitle');
    assert.equal(result.match?.ratingKey, 'daft-glw');
  });

  it('matches joint credits and & / and spellings', () => {
    const collier = pickSpotifyTrackMatch(lookups, spotifyItem('Witness Me', ['Jacob Collier', 'Shawn Mendes']));
    assert.equal(collier.match?.ratingKey, 'collier-mendes');
    const boxer = pickSpotifyTrackMatch(lookups, spotifyItem('The Boxer', ['Simon & Garfunkel']));
    assert.equal(boxer.match?.ratingKey, 'sg-boxer');
  });

  it('still matches by title when the source has no artist', () => {
    const result = pickSpotifyTrackMatch(lookups, { title: 'Human', artists: [] });
    assert.equal(result.method, 'title');
    assert.equal(result.match?.ratingKey, 'killers-human');
  });

  it('splits artist credits into comparable keys', () => {
    const featured = buildImportArtistKeys('The Weeknd feat. Daft Punk');
    assert.ok(featured.has('weeknd'));
    assert.ok(featured.has('daft punk'));
    assert.ok(buildImportArtistKeys('Florence + The Machine').has('florence and the machine'));
    assert.ok(buildImportArtistKeys('Florence and the Machine').has('florence and the machine'));
  });
});
