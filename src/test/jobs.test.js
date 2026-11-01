import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { unlinkSync } from 'node:fs';

import { initDb } from '../db.js';
import {
  JOB_DEFS,
  JOB_GROUPS,
  buildJobRunSequence,
  createJobService,
  resolveJobAvailability,
} from '../services/jobs.js';

const configured = {
  mediaServer: { type: 'plex' },
  lidarr: { url: 'http://lidarr.local', apiKey: 'key' },
  tautulli: { url: 'http://tautulli.local', apiKey: 'key' },
  discovery: { lastfmApiKey: 'key' },
  playlistWatchFolder: { enabled: true },
};

describe('job groups and availability', () => {
  it('puts every job in exactly one group', () => {
    const grouped = JOB_GROUPS.flatMap((group) => group.jobs);
    assert.deepEqual([...grouped].sort(), Object.keys(JOB_DEFS).sort());
    assert.equal(new Set(grouped).size, grouped.length);
  });

  it('marks jobs for integrations that are not set up as unavailable', () => {
    const availability = resolveJobAvailability({ mediaServer: { type: 'plex' } });
    for (const jobId of ['lidarrReviewArtists', 'lidarrProcessQueue', 'lidarrRetryFailed']) {
      assert.match(availability[jobId], /Lidarr/);
    }
    assert.match(availability.tautulliDailySync, /Tautulli/);
    assert.match(availability.lastfmTagSync, /Last\.fm/);
    assert.match(availability.playlistWatchFolder, /watch folder/);
    assert.equal(availability.masterTrackRefresh, '');
    assert.equal(availability.trackAnalysisPipeline, '');
    assert.match(resolveJobAvailability({ mediaServer: { type: 'jellyfin' } }).trackPlexLoudnessSync, /Plex/);
    assert.ok(Object.values(resolveJobAvailability(configured)).every((reason) => reason === ''));
  });

  it('runs configured, enabled, scheduled jobs in group order with long-running jobs last', () => {
    const all = buildJobRunSequence(configured);
    assert.ok(!all.includes('lastfmHistoryBackfill'), 'manual-only jobs are skipped');
    assert.equal(all.at(-1), 'trackAnalysisPipeline');
    assert.ok(all.indexOf('masterTrackRefresh') < all.indexOf('trackEnrichmentSync'));
    assert.ok(all.indexOf('suggestionRebuild') < all.indexOf('dailyMixSync'));

    const trimmed = buildJobRunSequence({
      mediaServer: { type: 'plex' },
      jobs: { smartPlaylistSync: { enabled: false } },
    });
    for (const jobId of ['smartPlaylistSync', 'lidarrReviewArtists', 'tautulliDailySync', 'playlistWatchFolder']) {
      assert.ok(!trimmed.includes(jobId), `expected ${jobId} to be left out`);
    }
    assert.deepEqual(buildJobRunSequence(configured, 'playlists'), ['smartPlaylistSync', 'dailyMixSync']);
  });
});

describe('job run sequence', () => {
  const dbPath = join(tmpdir(), `curatorr-jobs-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
  const db = initDb(dbPath);
  after(() => {
    db.close();
    for (const suffix of ['', '-wal', '-shm']) {
      try { unlinkSync(`${dbPath}${suffix}`); } catch (_) {}
    }
  });

  it('runs jobs one at a time and records each result', async () => {
    const order = [];
    let active = 0;
    const step = (jobId, fail = false) => async () => {
      active += 1;
      assert.equal(active, 1, 'jobs must not overlap');
      order.push(jobId);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active -= 1;
      if (fail) throw new Error(`${jobId} broke`);
      return { message: `${jobId} done` };
    };
    const service = createJobService(
      { db, loadConfig: () => ({}), pushLog: () => {}, safeMessage: (err) => String(err?.message || err) },
      { masterTrackRefresh: step('masterTrackRefresh'), trackEnrichmentSync: step('trackEnrichmentSync', true), smartPlaylistSync: step('smartPlaylistSync') },
    );
    const done = service.runSequence(['masterTrackRefresh', 'trackEnrichmentSync', 'smartPlaylistSync']);
    assert.equal(service.runSequence(['masterTrackRefresh']), false, 'only one sequence at a time');
    assert.equal(service.getSequenceStatus().running, true);
    await done;
    const status = service.getSequenceStatus();
    assert.deepEqual(order, ['masterTrackRefresh', 'trackEnrichmentSync', 'smartPlaylistSync']);
    assert.equal(status.running, false);
    assert.deepEqual(status.results.map((result) => result.status), ['success', 'error', 'success']);
    assert.match(status.results[1].message, /broke/);
  });

  it('waits for a job that is already running instead of starting it twice', async () => {
    let calls = 0;
    let release;
    const service = createJobService(
      { db, loadConfig: () => ({}), pushLog: () => {}, safeMessage: (err) => String(err?.message || err) },
      { masterTrackRefresh: () => { calls += 1; return new Promise((resolve) => { release = resolve; }); } },
    );
    const scheduled = service.runJob('masterTrackRefresh');
    const done = service.runSequence(['masterTrackRefresh'], { pollMs: 5 });
    setTimeout(() => release({}), 20);
    await Promise.all([scheduled, done]);
    assert.equal(calls, 1);
    assert.equal(service.getSequenceStatus().results[0].status, 'skipped');
  });
});
