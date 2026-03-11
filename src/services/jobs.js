import { getSystemJobRun, setSystemJobRun, getAllSystemJobRuns } from '../db.js';

// ─── Job definitions (metadata only — no functions) ───────────────────────────

export const JOB_DEFS = {
  masterTrackRefresh: {
    label: 'Master Track Cache Refresh',
    description: 'Fetches all tracks from your Plex music library and updates the local cache with genres, rating counts, and view counts.',
    defaultIntervalMinutes: 360,
  },
  smartPlaylistSync: {
    label: 'Smart Playlist Sync',
    description: 'Rebuilds each user\'s smart playlist based on their listening habits and syncs it to Plex.',
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
    label: 'Daily Mix Sync',
    description: 'Builds each user\'s Daily Mix playlist based on recent favourites, suggestions, and fresh library tracks, then syncs it to Plex.',
    defaultIntervalMinutes: 1440,
  },
  tautulliDailySync: {
    label: 'Tautulli History Sync',
    description: 'Fetches recent play history from Tautulli and backfills any plays missed by real-time webhooks.',
    defaultIntervalMinutes: 1440,
  },
};

// ─── Service factory ──────────────────────────────────────────────────────────

export function createJobService(ctx, jobFunctions) {
  const { db, loadConfig, pushLog, safeMessage } = ctx;
  const handles = {}; // jobId → timer handle
  const running = new Set(); // jobIds currently executing

  async function runJob(jobId) {
    const fn = jobFunctions[jobId];
    if (!fn) return;
    if (running.has(jobId)) return; // prevent overlapping runs
    running.add(jobId);
    setSystemJobRun(db, jobId, { status: 'running', lastRunAt: Date.now(), message: '' });
    pushLog({ level: 'info', app: 'jobs', action: 'job.start', message: `Job started: ${jobId}` });
    try {
      await fn();
      setSystemJobRun(db, jobId, { status: 'success', lastRunAt: Date.now(), message: 'Completed successfully' });
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

  function startAll(runImmediately = false) {
    const jobsCfg = loadConfig().jobs || {};
    for (const jobId of Object.keys(JOB_DEFS)) {
      if (!jobFunctions[jobId]) continue;
      const cfg = jobsCfg[jobId] || {};
      const enabled = cfg.enabled !== false;
      const intervalMinutes = Number(cfg.intervalMinutes) || JOB_DEFS[jobId].defaultIntervalMinutes;
      if (enabled) {
        _scheduleOne(jobId, intervalMinutes);
        if (runImmediately) runJob(jobId).catch(() => {});
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
