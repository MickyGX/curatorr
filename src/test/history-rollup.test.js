import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { paginateRolledHistory, rollupHistoryEntries } from '../history-rollup.js';

function buildEvent(trackKey, startedAt, durationMs, overrides = {}) {
  return {
    user_plex_id: 'mick',
    plex_rating_key: trackKey,
    track_title: `Track ${trackKey}`,
    artist_name: 'Artist',
    album_name: 'Album',
    started_at: startedAt,
    duration_ms: durationMs,
    is_skip: false,
    ...overrides,
  };
}

describe('history roll-up', () => {
  it('merges repeated tracks seen within the recent 10-event window', () => {
    const rolled = rollupHistoryEntries([
      buildEvent('a', 3000, 4000, { is_skip: true }),
      buildEvent('b', 2000, 2000),
      buildEvent('a', 1000, 3000),
    ]);

    assert.equal(rolled.length, 2);
    assert.equal(rolled[0].plex_rating_key, 'a');
    assert.equal(rolled[0].rollup_count, 2);
    assert.equal(rolled[0].duration_ms, 7000);
    assert.equal(rolled[0].started_at, 3000);
    assert.equal(rolled[0].is_skip, true);
    assert.equal(rolled[1].plex_rating_key, 'b');
  });

  it('does not merge repeats that fall outside the 10-event window', () => {
    const events = [buildEvent('a', 20000, 1000)];
    for (let idx = 0; idx < 10; idx += 1) {
      events.push(buildEvent(`gap-${idx}`, 19000 - idx, 1000));
    }
    events.push(buildEvent('a', 5000, 1000));

    const rolled = rollupHistoryEntries(events);
    const aRows = rolled.filter((entry) => entry.plex_rating_key === 'a');
    assert.equal(aRows.length, 2);
  });

  it('merges recent repeats when rating keys differ but title and artist match', () => {
    const rolled = rollupHistoryEntries([
      buildEvent('first-key', 3000, 4000, {
        track_title: 'If I Had A Gun...',
        artist_name: 'Noel Gallagher’s High Flying Birds',
        album_name: 'Album One',
      }),
      buildEvent('second-key', 1000, 3000, {
        track_title: 'If I Had A Gun...',
        artist_name: 'Noel Gallagher’s High Flying Birds',
        album_name: 'Album Two',
      }),
    ]);

    assert.equal(rolled.length, 1);
    assert.equal(rolled[0].rollup_count, 2);
    assert.equal(rolled[0].duration_ms, 7000);
  });

  it('merges recent repeats when title and artist only differ by punctuation variants', () => {
    const rolled = rollupHistoryEntries([
      buildEvent('first-key', 3000, 4000, {
        track_title: 'If I Had A Gun...',
        artist_name: "Noel Gallagher's High Flying Birds",
        album_name: 'Album One',
      }),
      buildEvent('second-key', 1000, 3000, {
        track_title: 'If I Had A Gun…',
        artist_name: 'Noel Gallagher’s High Flying Birds',
        album_name: 'Album Two',
      }),
    ]);

    assert.equal(rolled.length, 1);
    assert.equal(rolled[0].rollup_count, 2);
    assert.equal(rolled[0].duration_ms, 7000);
  });

  it('paginates by rolled rows rather than raw play events', () => {
    const events = [
      buildEvent('a', 5000, 1000),
      buildEvent('b', 4000, 1000),
      buildEvent('a', 3000, 1000),
      buildEvent('c', 2000, 1000),
    ];

    const fetchChunk = (limit, offset) => events.slice(offset, offset + limit);
    const pageOne = paginateRolledHistory(fetchChunk, { limit: 1, offset: 0 });
    const pageTwo = paginateRolledHistory(fetchChunk, { limit: 1, offset: 1 });
    const pageThree = paginateRolledHistory(fetchChunk, { limit: 1, offset: 2 });

    assert.equal(pageOne.history.length, 1);
    assert.equal(pageOne.history[0].plex_rating_key, 'a');
    assert.equal(pageOne.history[0].rollup_count, 2);
    assert.equal(pageOne.hasMore, true);

    assert.equal(pageTwo.history.length, 1);
    assert.equal(pageTwo.history[0].plex_rating_key, 'b');
    assert.equal(pageTwo.hasMore, true);

    assert.equal(pageThree.history.length, 1);
    assert.equal(pageThree.history[0].plex_rating_key, 'c');
    assert.equal(pageThree.hasMore, false);
  });
});
