import { getSystemJobRun, setSystemJobRun, getAllSystemJobRuns } from '../db.js';

// ─── Job definitions (metadata only — no functions) ───────────────────────────

export const JOB_DEFS = {
  masterTrackRefresh: {
    label: 'Master Track Cache Refresh',
    description: 'Fetches all tracks from your media server music library and updates the local cache with genres, rating counts, and view counts.',
    defaultIntervalMinutes: 360,
  },
  smartPlaylistSync: {
    label: 'Smart Playlist Sync',
    description: 'Rebuilds each user\'s smart playlist based on their listening habits and syncs it to your media server.',
    defaultIntervalMinutes: 30,
  },
  lidarrReviewArtists: {
    label: 'Lidarr: Review Due Artists',
    description: 'Reviews suggested artists and queues Lidarr searches for artists that are due for evaluation.',
    defaultIntervalMinutes: 30,
  },
  lidarrProcessQueue: {
    label: 'Lidarr: Process Queued Requests',
    description: 'Processes pending Lidarr add and monitor requests from the automation queue.',
    defaultIntervalMinutes: 20,
  },
  dailyMixSync: {
    label: 'Rotating Playlist Sync',
    description: 'Builds each user\'s Daily Mix and Curatorr rotating playlists from recent favourites, discovery candidates, and stored playlist settings, then syncs them to your media server.',
    defaultIntervalMinutes: 1440,
  },
  trackEnrichmentSync: {
    label: 'Track Enrichment Sync',
    description: 'Backfills stored track metadata such as release year and original release date from MusicBrainz for tracks with a recording MBID.',
    defaultIntervalMinutes: 360,
  },
  trackFeatureImportSync: {
    label: 'Track Feature Import',
    description: 'Imports BPM, musical key, Camelot key, energy, and danceability from a local JSON manifest file into track enrichment.',
    defaultIntervalMinutes: 360,
  },
  trackPlexLoudnessSync: {
    label: 'Plex Loudness Sync',
    description: 'Fetches finished track loudness metrics from the Plex public API and stores them in track enrichment for loudness-aware playlist sequencing.',
    defaultIntervalMinutes: 180,
  },
  trackAnalysisPipeline: {
    label: 'Track Analysis Pipeline',
    description: 'Exports a Curatorr feature template, runs either the analyzer sidecar or a configured external analyzer command, merges the analyzer output, and imports the results back into track enrichment.',
    defaultIntervalMinutes: 1440,
  },
  tautulliDailySync: {
    label: 'Tautulli Gap-Fill Sync',
    description: 'Optional backup job that fetches recent Tautulli history, fills in plays missed by webhooks, and can optionally perform guarded repairs on shorter Plex-recorded listens.',
    defaultIntervalMinutes: 1440,
  },
  lastfmTagSync: {
    label: 'Last.fm Tag Sync',
    description: 'Fetches genre/mood tags from Last.fm for every artist in your library and stores them for use in global playlist filters.',
    defaultIntervalMinutes: 10080,
  },
  lastfmHistorySync: {
    label: 'Last.fm History Sync',
    description: 'Fetches recent scrobbles from Last.fm for each user who has configured a Last.fm username and backfills plays not already recorded. Requires a Last.fm API key and a username set in User Profile.',
    defaultIntervalMinutes: 60,
  },
  lastfmHistoryBackfill: {
    label: 'Last.fm Full History Backfill',
    description: 'Manually import your complete Last.fm scrobble history. Each run fetches one batch (~10,000 scrobbles) working backwards in time — run multiple times until complete. Progress is saved between runs per user.',
    manualOnly: true,
  },
  lidarrRetryFailed: {
    label: 'Lidarr: Retry Failed Requests',
    description: 'Re-queues failed Lidarr add requests so they are picked up by the next queue processing run. Requests that have already been retried 3 times are skipped.',
    defaultIntervalMinutes: 1440,
  },
};

// ─── Service factory ──────────────────────────────────────────────────────────

export function createJobService(ctx, jobFunctions) {
  const { db, loadConfig, pushLog, safeMessage } = ctx;
  const handles = {}; // jobId → timer handle
  const running = new Set(); // jobIds currently executing

  function clearInterruptedRuns() {
    const rows = getAllSystemJobRuns(db);
    for (const row of rows) {
      if (row?.status !== 'running') continue;
      const jobId = String(row.job_id || '').trim();
      if (!jobId) continue;
      const message = `Interrupted by app restart at ${new Date().toISOString()}.`;
      setSystemJobRun(db, jobId, {
        status: 'error',
        lastRunAt: Number(row.last_run_at || Date.now()),
        message,
      });
      pushLog({
        level: 'warn',
        app: 'jobs',
        action: 'job.interrupted',
        message: `Marked interrupted job as failed on startup: ${jobId}`,
      });
    }
  }

  async function runJob(jobId) {
    const fn = jobFunctions[jobId];
    if (!fn) return;
    if (running.has(jobId)) return; // prevent overlapping runs
    running.add(jobId);
    setSystemJobRun(db, jobId, { status: 'running', lastRunAt: Date.now(), message: '' });
    pushLog({ level: 'info', app: 'jobs', action: 'job.start', message: `Job started: ${jobId}` });
    try {
      const result = await fn();
      const message = typeof result?.message === 'string' ? result.message : 'Completed successfully';
      setSystemJobRun(db, jobId, { status: 'success', lastRunAt: Date.now(), message });
      pushLog({ level: 'info', app: 'jobs', action: 'job.success', message: `Job completed: ${jobId}` });
    } catch (err) {
      const msg = safeMessage(err);
      setSystemJobRun(db, jobId, { status: 'error', lastRunAt: Date.now(), message: msg });
      pushLog({ level: 'error', app: 'jobs', action: 'job.error', message: `Job failed: ${jobId} — ${msg}` });
    } finally {
      running.delete(jobId);
    }
  }

  function _scheduleOne(jobId, intervalMinutes) {
    if (handles[jobId]) { clearInterval(handles[jobId]); delete handles[jobId]; }
    handles[jobId] = setInterval(() => runJob(jobId).catch(() => {}), intervalMinutes * 60 * 1000);
    handles[jobId].unref();
  }

  function startAll(options = false) {
    clearInterruptedRuns();
    const runImmediately = typeof options === 'boolean'
      ? options
      : options?.runImmediately === true;
    const skipImmediate = new Set(
      Array.isArray(options?.skipImmediate) ? options.skipImmediate : [],
    );
    const jobsCfg = loadConfig().jobs || {};
    for (const jobId of Object.keys(JOB_DEFS)) {
      if (!jobFunctions[jobId]) continue;
      if (JOB_DEFS[jobId].manualOnly) continue;
      const cfg = jobsCfg[jobId] || {};
      const enabled = cfg.enabled !== false;
      const intervalMinutes = Number(cfg.intervalMinutes) || JOB_DEFS[jobId].defaultIntervalMinutes;
      if (enabled) {
        _scheduleOne(jobId, intervalMinutes);
        if (runImmediately && !skipImmediate.has(jobId)) runJob(jobId).catch(() => {});
      }
    }
  }

  function reschedule(jobId, intervalMinutes, enabled) {
    if (handles[jobId]) { clearInterval(handles[jobId]); delete handles[jobId]; }
    if (enabled && jobFunctions[jobId]) _scheduleOne(jobId, intervalMinutes);
  }

  function getStatus() {
    const rows = getAllSystemJobRuns(db);
    const byId = Object.fromEntries(rows.map((r) => [r.job_id, r]));
    return Object.fromEntries(
      Object.entries(JOB_DEFS).map(([jobId, def]) => {
        const row = byId[jobId] || null;
        return [jobId, {
          label: def.label,
          description: def.description,
          manualOnly: def.manualOnly || false,
          status: row?.status || 'idle',
          lastRunAt: row?.last_run_at || null,
          message: row?.message || '',
          isRunning: running.has(jobId),
        }];
      })
    );
  }

  return { runJob, startAll, reschedule, getStatus };
}
