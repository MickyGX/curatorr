import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { createPlayTracker, PROVISIONAL_FINISH_DELAY_MS } from '../services/music-assistant/play-tracker.js';

const FIXTURE = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures', 'ma-events.ndjson');

// Deterministic clock + timers so the provisional-finish debounce can be driven from event timestamps.
function createHarness() {
  let clock = 0;
  const timers = [];
  const finishes = [];
  const tracker = createPlayTracker({
    onFinish: (finish) => finishes.push(finish),
    now: () => clock,
    setTimer: (fn, ms) => { const t = { at: clock + ms, fn, done: false }; timers.push(t); return t; },
    clearTimer: (t) => { if (t) t.done = true; },
  });
  function advanceTo(ms) {
    for (;;) {
      const due = timers.filter((t) => !t.done && t.at <= ms).sort((a, b) => a.at - b.at)[0];
      if (!due) break;
      clock = due.at;
      due.done = true;
      due.fn();
    }
    clock = Math.max(clock, ms);
  }
  return { tracker, finishes, advanceTo, now: () => clock };
}

function report(overrides) {
  return {
    uri: 'library://track/1', media_type: 'track', name: 'Song', artist: 'Artist', duration: 100,
    seconds_played: 0, fully_played: false, is_playing: true, userid: 'u1', player_id: 'p1', ...overrides,
  };
}

function loadFixture() {
  return fs.readFileSync(FIXTURE, 'utf8').trim().split('\n').map((line) => JSON.parse(line));
}

describe('Music Assistant play tracker', () => {
  it('replays the captured MA 2.10.4 session into the expected plays', () => {
    const { tracker, finishes, advanceTo } = createHarness();
    const lines = loadFixture();
    const t0 = Date.parse(lines[0].t);
    for (const line of lines) {
      advanceTo(Date.parse(line.t) - t0);
      if (line.kind === 'media_item_played') tracker.ingest(line.data);
    }
    advanceTo(Date.parse(lines.at(-1).t) - t0 + PROVISIONAL_FINISH_DELAY_MS + 1);

    const summary = finishes.map((f) => `${f.uri}|${f.fullyPlayed ? 'full' : 'partial'}|${Math.round(f.listenedMs / 1000)}`);
    assert.deepEqual(summary, [
      'library://track/2748|full|73', // A: full play; the duration-11 duplicate after it is ignored
      'library://track/1428|partial|11', // B: skipped at ~11s (emitted after the debounce)
      'library://track/2685|partial|14', // C: 70s pause outlived the debounce -> provisional finish
      'library://track/2685|full|96', // C: same instance resumes and completes (updates the same row)
      'library://track/2748|full|73', // D: first of two back-to-back plays
      'library://track/2748|full|73', // D: repeat is a new instance
    ]);
    const cFinishes = finishes.filter((f) => f.uri === 'library://track/2685');
    assert.equal(cFinishes[0].instanceKey, cFinishes[1].instanceKey, 'pause/resume keeps one play instance');
    assert.equal(cFinishes[0].startedAt, cFinishes[1].startedAt);
    const dFinishes = finishes.slice(4);
    assert.notEqual(dFinishes[0].instanceKey, dFinishes[1].instanceKey, 'a repeat is a separate play');
    assert.ok(finishes.every((f) => f.userId === 'ma-user-1' && f.queueId === 'ma-player-1'));
    assert.equal(finishes[0].durationMs, 73_000, 'bogus shorter duration never shrinks the track');
  });

  it('absorbs a short pause without a provisional finish', () => {
    const { tracker, finishes, advanceTo } = createHarness();
    tracker.ingest(report({ seconds_played: 30 }));
    advanceTo(10_000);
    tracker.ingest(report({ seconds_played: 40, is_playing: false }));
    advanceTo(40_000);
    tracker.ingest(report({ seconds_played: 60 }));
    advanceTo(90_000);
    tracker.ingest(report({ seconds_played: 98, is_playing: false, fully_played: true }));
    advanceTo(90_000 + PROVISIONAL_FINISH_DELAY_MS * 2);
    assert.equal(finishes.length, 1);
    assert.equal(finishes[0].fullyPlayed, true);
    assert.equal(finishes[0].listenedMs, 100_000, 'fully played counts the whole duration');
  });

  it('keys instances by queue so the same track on two players is two plays', () => {
    const { tracker, finishes } = createHarness();
    tracker.ingest(report({ player_id: 'p1', seconds_played: 99, is_playing: false, fully_played: true }));
    tracker.ingest(report({ player_id: 'p2', seconds_played: 99, is_playing: false, fully_played: true }));
    assert.equal(finishes.length, 2);
    assert.notEqual(finishes[0].instanceKey, finishes[1].instanceKey);
  });

  it('settles a pending skip immediately when the same item restarts', () => {
    const { tracker, finishes } = createHarness();
    tracker.ingest(report({ seconds_played: 30, is_playing: false }));
    assert.equal(finishes.length, 0, 'partial finish waits for the debounce');
    tracker.ingest(report({ seconds_played: 2 }));
    assert.equal(finishes.length, 1);
    assert.equal(finishes[0].listenedMs, 30_000);
  });

  it('ignores non-track media and reports without a uri', () => {
    const { tracker, finishes } = createHarness();
    tracker.ingest(report({ media_type: 'radio', is_playing: false, fully_played: true }));
    tracker.ingest(report({ uri: '', is_playing: false, fully_played: true }));
    assert.equal(finishes.length, 0);
    assert.equal(tracker.size(), 0);
  });
});
