import { after, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { initDb } from '../db.js';
import { createPlayRecorder } from '../services/play-recorder.js';

const SMART = { skipThresholdSeconds: 20, songSkipLimit: 3, completionThresholdSeconds: 20 };

// Mirrors the synthetic session built by services/music-assistant/index.js for a finished MA play.
function maSession({ key, ratingKey = 'rk-1', startedAt, listenedMs, durationMs = 96_000, artist = 'The Beatles', title = 'Carry That Weight' }) {
  return {
    session: {
      session_key: key,
      user_plex_id: 'listener',
      plex_rating_key: ratingKey,
      track_title: title,
      artist_name: artist,
      album_name: 'Abbey Road',
      library_key: '',
      started_at: startedAt,
      track_duration_ms: durationMs,
      accumulated_ms: 0,
      max_position_ms: listenedMs,
      last_position_ms: 0,
      playing_since: 0,
    },
    endedAt: startedAt + listenedMs,
    playbackPositionMs: listenedMs,
    smartSettings: SMART,
    eventSource: 'music_assistant',
    allowRecentPlayReuse: true,
    allowMergeCandidate: false,
  };
}

describe('play recorder with Music Assistant sessions', () => {
  let dir;
  let db;
  let recorder;

  before(() => {
    dir = mkdtempSync(join(tmpdir(), 'curatorr-ma-'));
    db = initDb(join(dir, 'curatorr.db'));
    recorder = createPlayRecorder({ db });
  });

  after(() => {
    db.close();
    rmSync(dir, { recursive: true, force: true });
  });

  const rows = (sql, ...params) => db.prepare(sql).all(...params);

  it('turns a provisional pause-skip into one full play when the same instance finishes', () => {
    const startedAt = Date.now() - 120_000;
    const first = recorder.recordOrUpdateSessionPlay(maSession({ key: 'ma:q:1:a', startedAt, listenedMs: 14_000 }));
    assert.equal(first.isSkip, true);
    const second = recorder.recordOrUpdateSessionPlay(maSession({ key: 'ma:q:1:a', startedAt, listenedMs: 96_000 }));
    assert.equal(second.duplicate, false);
    assert.equal(second.isSkip, false);

    const events = rows("SELECT duration_ms, is_skip, event_source FROM play_events WHERE session_key = 'ma:q:1:a'");
    assert.deepEqual(events, [{ duration_ms: 96_000, is_skip: 0, event_source: 'music_assistant' }]);
    const [stats] = rows("SELECT play_count, skip_count FROM track_stats WHERE plex_rating_key = 'rk-1' AND user_plex_id = 'listener'");
    assert.deepEqual({ ...stats }, { play_count: 1, skip_count: 0 });
  });

  it('ignores a repeated final report for the same instance', () => {
    const startedAt = Date.now() - 100_000;
    recorder.recordOrUpdateSessionPlay(maSession({ key: 'ma:q:2:b', ratingKey: 'rk-2', startedAt, listenedMs: 96_000 }));
    const again = recorder.recordOrUpdateSessionPlay(maSession({ key: 'ma:q:2:b', ratingKey: 'rk-2', startedAt, listenedMs: 96_000 }));
    assert.equal(again.duplicate, true);
    assert.equal(rows("SELECT id FROM play_events WHERE plex_rating_key = 'rk-2'").length, 1);
  });

  it('keeps back-to-back repeats of one track as separate plays', () => {
    const startedAt = Date.now() - 300_000;
    recorder.recordOrUpdateSessionPlay(maSession({ key: 'ma:q:3:c', ratingKey: 'rk-3', startedAt, listenedMs: 73_000, durationMs: 73_000 }));
    recorder.recordOrUpdateSessionPlay(maSession({ key: 'ma:q:4:d', ratingKey: 'rk-3', startedAt: startedAt + 80_000, listenedMs: 73_000, durationMs: 73_000 }));
    assert.equal(rows("SELECT id FROM play_events WHERE plex_rating_key = 'rk-3'").length, 2);
    const [stats] = rows("SELECT play_count FROM track_stats WHERE plex_rating_key = 'rk-3'");
    assert.equal(stats.play_count, 2);
  });

  it('credits only the artist for a track that is not in the library', () => {
    const startedAt = Date.now() - 400_000;
    const result = recorder.recordOrUpdateSessionPlay(maSession({
      key: 'ma:q:5:e', ratingKey: '', startedAt, listenedMs: 200_000, durationMs: 200_000, artist: 'Streaming Only', title: 'Not In Plex',
    }));
    assert.equal(result.duplicate, false);
    assert.equal(rows("SELECT id FROM play_events WHERE session_key = 'ma:q:5:e' AND plex_rating_key = ''").length, 1);
    assert.equal(rows("SELECT * FROM track_stats WHERE plex_rating_key = ''").length, 0, 'no track_stats row for an empty rating key');
    const [artist] = rows("SELECT play_count FROM artist_stats WHERE artist_name = 'Streaming Only' AND user_plex_id = 'listener'");
    assert.equal(artist.play_count, 1);
  });
});
