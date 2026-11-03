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
    shortLabel: 'Review Due Artists',
    description: 'Reviews suggested artists and queues Lidarr searches for artists that are due for evaluation.',
    defaultIntervalMinutes: 30,
  },
  lidarrProcessQueue: {
    label: 'Lidarr: Process Queued Requests',
    shortLabel: 'Process Queued Requests',
    description: 'Processes pending Lidarr add and monitor requests from the automation queue.',
    defaultIntervalMinutes: 20,
  },
  dailyMixSync: {
    label: 'Daily Mix and Curatorr Playlist Sync',
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
    // Can run for hours, so "Run all" leaves it until last.
    longRunning: true,
  },
  tautulliDailySync: {
    label: 'Tautulli Gap-Fill Sync',
    description: 'Optional backup job that fetches recent Tautulli history, fills in plays missed by webhooks, and can optionally perform guarded repairs on shorter Plex-recorded listens.',
    defaultIntervalMinutes: 1440,
  },
  plexHistorySync: {
    label: 'Plex History Sync',
    description: 'Reads Plex\'s own play history and adds plays Curatorr never saw, such as tracks played offline in Plexamp and reported to Plex when it reconnects. Plex history has no listen time, so these count as full plays.',
    defaultIntervalMinutes: 360,
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
  suggestionRebuild: {
    label: 'Artist Pipeline Rebuild',
    description: 'Rebuilds artist, album, and track suggestions for all users using listening habits, genre affinity, and Last.fm similar artist data. Runs automatically every 6 hours; can also be triggered manually.',
    defaultIntervalMinutes: 360,
  },
  lidarrRetryFailed: {
    label: 'Lidarr: Retry Failed Requests',
    shortLabel: 'Retry Failed Requests',
    description: 'Re-queues failed Lidarr add requests so they are picked up by the next queue processing run. Requests that have already been retried 3 times are skipped.',
    defaultIntervalMinutes: 1440,
  },
  playlistWatchFolder: {
    label: 'Playlist Watch Folder',
    description: 'Imports new M3U/M3U8 files from the playlist watch folder, refreshes playlists whose file changed, and applies matching artwork. Configure the folder in Settings → Playlists.',
    defaultIntervalMinutes: 15,
  },
};

// ─── Groups ───────────────────────────────────────────────────────────────────

// Jobs are shown, and run by "Run all", in this order: library first, then the
// history and metadata it feeds, then discovery, then the playlists built from
// all of it.
export const JOB_GROUPS = [
  {
    key: 'library',
    label: 'Library',
    description: 'Keeps Curatorr\'s copy of your music library and watch-folder playlists up to date.',
    jobs: ['masterTrackRefresh', 'playlistWatchFolder'],
  },
  {
    key: 'history',
    label: 'Listening history',
    description: 'Fills in plays that webhooks missed, from Tautulli, Plex history (including offline plays) and Last.fm.',
    jobs: ['tautulliDailySync', 'plexHistorySync', 'lastfmHistorySync', 'lastfmHistoryBackfill'],
  },
  {
    key: 'metadata',
    label: 'Track metadata and analysis',
    description: 'Adds release dates, tags, loudness and audio features used by playlist filters and ordering.',
    jobs: ['trackEnrichmentSync', 'lastfmTagSync', 'trackFeatureImportSync', 'trackPlexLoudnessSync', 'trackAnalysisPipeline'],
  },
  {
    key: 'discovery',
    label: 'Discovery and Lidarr',
    description: 'Builds artist and track suggestions and works through Lidarr requests.',
    jobs: ['suggestionRebuild', 'lidarrReviewArtists', 'lidarrProcessQueue', 'lidarrRetryFailed'],
  },
  {
    key: 'playlists',
    label: 'Playlists',
    description: 'Rebuilds smart and rotating playlists and syncs them to your media server.',
    jobs: ['smartPlaylistSync', 'dailyMixSync'],
  },
];

// Why a job is not available with the current configuration, or '' when it is.
// Such jobs are tagged "Not configured" and left out of Run all.
export function resolveJobAvailability(config = {}) {
  const lidarr = config?.lidarr || {};
  const hasLidarr = Boolean(String(lidarr.url || lidarr.localUrl || '').trim() && String(lidarr.apiKey || '').trim());
  const isPlex = String(config?.mediaServer?.type || 'plex').trim().toLowerCase() === 'plex';
  const hasTautulli = isPlex && Boolean(String(config?.tautulli?.url || '').trim() && String(config?.tautulli?.apiKey || '').trim());
  const hasLastfm = Boolean(String(config?.discovery?.lastfmApiKey || '').trim());
  const hasWatchFolder = config?.playlistWatchFolder?.enabled === true;
  const reasons = {
    lidarrReviewArtists: hasLidarr ? '' : 'Lidarr is not configured.',
    lidarrProcessQueue: hasLidarr ? '' : 'Lidarr is not configured.',
    lidarrRetryFailed: hasLidarr ? '' : 'Lidarr is not configured.',
    tautulliDailySync: hasTautulli ? '' : 'Tautulli is not configured.',
    lastfmTagSync: hasLastfm ? '' : 'No Last.fm API key is set.',
    lastfmHistorySync: hasLastfm ? '' : 'No Last.fm API key is set.',
    lastfmHistoryBackfill: hasLastfm ? '' : 'No Last.fm API key is set.',
    trackPlexLoudnessSync: isPlex ? '' : 'Only available with Plex.',
    plexHistorySync: isPlex ? '' : 'Only available with Plex.',
    playlistWatchFolder: hasWatchFolder ? '' : 'The playlist watch folder is turned off.',
  };
  return Object.fromEntries(Object.keys(JOB_DEFS).map((jobId) => [jobId, reasons[jobId] || '']));
}

// Jobs "Run all" (or "Run group") would run, in order: configured, enabled,
// scheduled jobs; long-running jobs go last.
export function buildJobRunSequence(config = {}, groupKey = '') {
  const groups = groupKey ? JOB_GROUPS.filter((group) => group.key === groupKey) : JOB_GROUPS;
  const availability = resolveJobAvailability(config);
  const jobsCfg = config?.jobs || {};
  const ids = groups.flatMap((group) => group.jobs).filter((jobId) => {
    const def = JOB_DEFS[jobId];
    const cfg = jobsCfg[jobId] || {};
    return def && !def.manualOnly && !availability[jobId] && cfg.enabled !== false;
  });
  return [...ids.filter((jobId) => !JOB_DEFS[jobId].longRunning), ...ids.filter((jobId) => JOB_DEFS[jobId].longRunning)];
}

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

  // options are passed through to the job function (e.g. a manual Tautulli
  // backfill window). Returns false when the job is unknown or already running.
  async function runJob(jobId, options = undefined) {
    const fn = jobFunctions[jobId];
    if (!fn) return false;
    if (running.has(jobId)) return false; // prevent overlapping runs
    running.add(jobId);
    setSystemJobRun(db, jobId, { status: 'running', lastRunAt: Date.now(), message: '' });
    pushLog({ level: 'info', app: 'jobs', action: 'job.start', message: `Job started: ${jobId}` });
    try {
      const result = await fn(options);
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
    return true;
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
    // Boot catch-up: timers reset to zero on every restart and there is no
    // run-on-boot, so a job whose interval is longer than the time between
    // restarts (e.g. the 24h dailyMixSync on a box that restarts daily) never
    // fires. When catchUp is on, any job whose last run is older than its
    // interval is run after startup — staggered so a restart-prone NAS isn't
    // slammed during warm-up.
    const catchUp = typeof options === 'object' && options?.catchUp === true;
    const catchUpDelayMs = Number.isFinite(Number(options?.catchUpInitialDelayMs))
      ? Number(options.catchUpInitialDelayMs) : 2 * 60 * 1000;
    const catchUpStaggerMs = Number.isFinite(Number(options?.catchUpStaggerMs))
      ? Number(options.catchUpStaggerMs) : 90 * 1000;
    const jobsCfg = loadConfig().jobs || {};
    let catchUpSlot = 0;
    for (const jobId of Object.keys(JOB_DEFS)) {
      if (!jobFunctions[jobId]) continue;
      if (JOB_DEFS[jobId].manualOnly) continue;
      const cfg = jobsCfg[jobId] || {};
      const enabled = cfg.enabled !== false;
      if (!enabled) continue;
      const intervalMinutes = Number(cfg.intervalMinutes) || JOB_DEFS[jobId].defaultIntervalMinutes;
      _scheduleOne(jobId, intervalMinutes);
      if (runImmediately && !skipImmediate.has(jobId)) {
        runJob(jobId).catch(() => {});
        continue;
      }
      if (catchUp) {
        const lastRunAt = Number(getSystemJobRun(db, jobId)?.last_run_at || 0);
        if (Date.now() - lastRunAt >= intervalMinutes * 60 * 1000) {
          const delay = catchUpDelayMs + catchUpSlot * catchUpStaggerMs;
          catchUpSlot += 1;
          const handle = setTimeout(() => runJob(jobId).catch(() => {}), delay);
          if (typeof handle.unref === 'function') handle.unref();
          pushLog({
            level: 'info',
            app: 'jobs',
            action: 'job.catchup',
            message: `Scheduled catch-up for overdue job ${jobId} (in ${Math.round(delay / 1000)}s)`,
          });
        }
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

  // One "Run all" / "Run group" sequence at a time. Jobs run one after another;
  // a job that is already running (e.g. on its schedule) is waited for rather
  // than started twice.
  let sequence = { running: false, group: '', jobIds: [], index: 0, current: '', startedAt: 0, finishedAt: 0, results: [] };

  async function waitWhileRunning(jobId, { pollMs = 1000 } = {}) {
    while (running.has(jobId)) await new Promise((resolve) => setTimeout(resolve, pollMs));
  }

  function runSequence(jobIds, { group = '', pollMs = 1000 } = {}) {
    if (sequence.running) return false;
    const ids = (Array.isArray(jobIds) ? jobIds : []).filter((jobId) => jobFunctions[jobId]);
    sequence = { running: true, group, jobIds: ids, index: 0, current: '', startedAt: Date.now(), finishedAt: 0, results: [] };
    pushLog({ level: 'info', app: 'jobs', action: 'sequence.start', message: `Running ${ids.length} job(s) in sequence${group ? ` (${group})` : ''}` });
    const done = (async () => {
      for (let i = 0; i < ids.length; i += 1) {
        const jobId = ids[i];
        sequence.index = i;
        sequence.current = jobId;
        if (running.has(jobId)) {
          await waitWhileRunning(jobId, { pollMs });
          sequence.results.push({ jobId, status: 'skipped', message: 'Already running; waited for it to finish.' });
          continue;
        }
        await runJob(jobId);
        const row = getSystemJobRun(db, jobId);
        sequence.results.push({ jobId, status: row?.status || 'success', message: row?.message || '' });
      }
      sequence.running = false;
      sequence.current = '';
      sequence.index = ids.length;
      sequence.finishedAt = Date.now();
      const failed = sequence.results.filter((result) => result.status === 'error').length;
      pushLog({ level: failed ? 'warn' : 'info', app: 'jobs', action: 'sequence.complete', message: `Job sequence finished: ${ids.length - failed} succeeded, ${failed} failed` });
    })();
    done.catch(() => { sequence.running = false; });
    return done;
  }

  function getSequenceStatus() {
    return { ...sequence, jobIds: [...sequence.jobIds], results: sequence.results.map((result) => ({ ...result })) };
  }

  return { runJob, startAll, reschedule, getStatus, runSequence, getSequenceStatus };
}
