import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  getDailyMixLastAppearances,
  initDb,
  purgeUserScopedMusicData,
  recordDailyMixAppearances,
  saveUserGeneratedPlaylist,
  setPlaylistTracks,
} from '../db.js';
import { createPlaylistService } from '../services/playlists.js';

const testDir = join(tmpdir(), `curatorr-daily-mix-repeat-${process.pid}`);
mkdirSync(testDir, { recursive: true });
after(() => rmSync(testDir, { recursive: true, force: true }));

const DAY_MS = 24 * 60 * 60 * 1000;
let dbCounter = 0;
const openDb = () => initDb(join(testDir, `mix-${dbCounter++}.db`));

function seedLibrary(db, userId, { artists, tracksPerArtist, favourites }) {
  const now = Date.now();
  const longAgo = now - 120 * DAY_MS;
  const insertMaster = db.prepare(`
    INSERT INTO master_tracks (
      rating_key, artist_name, track_title, album_name, recording_mbid,
      genres, library_key, rating_count, view_count, updated_at
    ) VALUES (?, ?, ?, ?, '', '[]', '1', 0, 0, ?)
  `);
  const insertStat = db.prepare(`
    INSERT INTO track_stats (
      plex_rating_key, user_plex_id, track_title, artist_name, album_name,
      play_count, skip_count, consecutive_skips, excluded_from_smart,
      tier, tier_weight, last_played_at, updated_at
    ) VALUES (?, ?, 'Track 0', ?, '', ?, 0, 0, 0, ?, ?, ?, ?)
  `);
  db.transaction(() => {
    for (let a = 0; a < artists; a += 1) {
      const artist = `Artist ${String(a).padStart(3, '0')}`;
      for (let t = 0; t < tracksPerArtist; t += 1) {
        insertMaster.run(`k-${a}-${t}`, artist, `Track ${t}`, `${artist} Album`, now);
      }
      if (a < favourites) {
        // A steep play-count curve: the first few favourites are played far more,
        // which is what used to win the favourite slots every other day.
        const plays = a < 6 ? 25 : 3 + (a % 5);
        const belter = a < 10;
        insertStat.run(`k-${a}-0`, userId, artist, plays, belter ? 'belter' : 'decent', belter ? 1 : 0.5, longAgo, longAgo);
      }
    }
  })();
}

// Builds the mix for `days` consecutive days, persisting each one the way a
// sync does (current tracks, build time and appearance history).
function simulateDays(service, db, userId, { days, start = Date.UTC(2026, 10, 1, 12) }) {
  const mixes = [];
  for (let day = 0; day < days; day += 1) {
    const now = start + day * DAY_MS;
    const mix = service.buildDailyMix(userId, { now });
    setPlaylistTracks(db, userId, 'daily-mix', mix.tracks);
    saveUserGeneratedPlaylist(db, userId, {
      playlistKey: 'daily-mix', playlistType: 'daily-mix', playlistTitle: 'Daily Mix',
      lastBuiltAt: now, updatedAt: now,
    });
    recordDailyMixAppearances(db, userId, mix.mixDate, mix.trackKeys);
    mixes.push(mix);
  }
  return mixes;
}

function serviceFor(db, dailyMix = {}) {
  return createPlaylistService({
    db,
    loadConfig: () => ({ mediaServer: { type: 'plex' }, smartPlaylist: { dailyMix } }),
  });
}

describe('Daily Mix repeat window', () => {
  it('does not repeat a favourite within the window when there are enough favourites', () => {
    const db = openDb();
    const userId = 'repeat-window-user';
    seedLibrary(db, userId, { artists: 200, tracksPerArtist: 2, favourites: 120 });
    const service = serviceFor(db, { favoriteLimit: 12, suggestedLimit: 0, freshLimit: 12, maxTracks: 24, favoriteRepeatDays: 7 });
    try {
      const mixes = simulateDays(service, db, userId, { days: 14 });
      const favouriteDays = new Map();
      mixes.forEach((mix, day) => {
        assert.equal(mix.trackCount, 24, `day ${day} should be full`);
        assert.equal(mix.sourceBreakdown.favorites, 12, `day ${day} should have its full favourite share`);
        assert.equal(mix.repeatWindowFills, 0, `day ${day} should not need repeats`);
        mix.tracks.filter((track) => track.source === 'favorite').forEach((track) => {
          const last = favouriteDays.get(track.ratingKey);
          if (last !== undefined) {
            assert.ok(day - last >= 7, `favourite ${track.ratingKey} repeated after ${day - last} day(s)`);
          }
          favouriteDays.set(track.ratingKey, day);
        });
      });
      // The most-played favourites still come back once their window has passed.
      const topFavouriteDays = mixes
        .map((mix, day) => (mix.trackKeys.some((key) => /^k-[0-5]-0$/.test(key)) ? day : -1))
        .filter((day) => day >= 0);
      assert.ok(topFavouriteDays.length >= 2, 'expected top favourites to return after the window');
    } finally {
      db.close();
    }
  });

  it('keeps the mix full when there are too few tracks, repeating the longest-ago tracks first', () => {
    const db = openDb();
    const userId = 'repeat-window-small';
    // 30 tracks total, all favourites: a 24-track mix can't avoid repeats.
    seedLibrary(db, userId, { artists: 30, tracksPerArtist: 1, favourites: 30 });
    const service = serviceFor(db, { favoriteLimit: 24, suggestedLimit: 0, freshLimit: 0, maxTracks: 24, favoriteRepeatDays: 7 });
    try {
      const [dayOne, dayTwo, dayThree] = simulateDays(service, db, userId, { days: 3 });
      assert.equal(dayOne.trackCount, 24);
      assert.equal(dayTwo.trackCount, 24);
      assert.equal(dayThree.trackCount, 24);
      assert.ok(dayTwo.repeatWindowFills > 0, 'expected day two to report repeats');
      // Day three: the 6 tracks left out on day one were used on day two, so the
      // tracks from day one (two days ago) must be preferred over yesterday's.
      const fromDayOneOnly = dayOne.trackKeys.filter((key) => !dayTwo.trackKeys.includes(key));
      for (const key of fromDayOneOnly) {
        assert.ok(dayThree.trackKeys.includes(key), `expected ${key} (last seen two days ago) to be reused before yesterday's tracks`);
      }
    } finally {
      db.close();
    }
  });

  it('only avoids yesterday when the windows are 0', () => {
    const db = openDb();
    const userId = 'repeat-window-zero';
    seedLibrary(db, userId, { artists: 60, tracksPerArtist: 1, favourites: 30 });
    const service = serviceFor(db, { favoriteLimit: 12, suggestedLimit: 0, freshLimit: 0, maxTracks: 12, favoriteRepeatDays: 0, otherRepeatDays: 0 });
    try {
      const [dayOne, dayTwo, dayThree] = simulateDays(service, db, userId, { days: 3 });
      assert.equal(dayTwo.trackKeys.filter((key) => dayOne.trackKeys.includes(key)).length, 0);
      assert.ok(dayThree.trackKeys.some((key) => dayOne.trackKeys.includes(key)), 'expected day-one tracks to be allowed back on day three');
    } finally {
      db.close();
    }
  });

  it('keeps a same-day rebuild unchanged once history is recorded', () => {
    const db = openDb();
    const userId = 'repeat-window-same-day';
    seedLibrary(db, userId, { artists: 120, tracksPerArtist: 2, favourites: 60 });
    const service = serviceFor(db, { favoriteRepeatDays: 7 });
    const start = Date.UTC(2026, 10, 1, 12);
    try {
      const [, dayTwo] = simulateDays(service, db, userId, { days: 2, start });
      const rebuilt = service.buildDailyMix(userId, { now: start + DAY_MS + 3 * 60 * 60 * 1000 });
      assert.deepEqual([...rebuilt.trackKeys].sort(), [...dayTwo.trackKeys].sort());
    } finally {
      db.close();
    }
  });
});

describe('Daily Mix history storage', () => {
  it('replaces a rebuilt day, prunes old rows and filters by date range', () => {
    const db = openDb();
    const userId = 'history-user';
    try {
      recordDailyMixAppearances(db, userId, '2026-06-01', ['old']);
      recordDailyMixAppearances(db, userId, '2026-11-01', ['a', 'b']);
      recordDailyMixAppearances(db, userId, '2026-11-02', ['a', 'c']);
      recordDailyMixAppearances(db, userId, '2026-11-02', ['a', 'd']); // same-day rebuild

      const all = getDailyMixLastAppearances(db, userId, { sinceDate: '2026-01-01' });
      assert.equal(all.has('old'), false, 'rows older than 90 days should be pruned');
      assert.equal(all.has('c'), false, 'a same-day rebuild should replace that day');
      assert.equal(all.get('a'), '2026-11-02');
      assert.equal(all.get('b'), '2026-11-01');
      assert.equal(all.get('d'), '2026-11-02');

      const beforeToday = getDailyMixLastAppearances(db, userId, { sinceDate: '2026-10-01', beforeDate: '2026-11-02' });
      assert.deepEqual([...beforeToday.entries()].sort(), [['a', '2026-11-01'], ['b', '2026-11-01']]);

      assert.equal(recordDailyMixAppearances(db, userId, 'not-a-date', ['x']), 0);

      const purge = purgeUserScopedMusicData(db, [userId]);
      assert.equal(purge.tableCounts.daily_mix_history, 4);
    } finally {
      db.close();
    }
  });
});
