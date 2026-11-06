// Cached playlist builder option lists (genres, moods, tags, decades, folders,
// feature coverage). Building them scans the whole library, which takes minutes
// on very large libraries (issue #196), so the result is kept in memory per db
// and rebuilt in a read-only worker thread whenever library data changes.
//
// Small libraries see no difference: a rebuild finishes inside the short wait
// window and the page renders fresh lists. Large libraries render straight away
// with the previous lists (or empty lists plus a notice on a cold start) while
// the rebuild runs in the background.

import { Worker } from 'node:worker_threads';
import {
  computePlaylistBuilderOptions,
  emptyPlaylistBuilderOptions,
  getLibraryDataGeneration,
} from '../db.js';

const DEFAULT_WAIT_MS = 1500;
const WORKER_TIMEOUT_MS = 15 * 60 * 1000;
// After a rebuild, wait a multiple of its duration before starting another one
// for a stale cache, so frequent small changes (tag backfill, analyzer batches)
// can't keep a large library rebuilding back to back. Capped so lists never
// lag by more than a few minutes.
const COOLDOWN_FACTOR = 4;
const MAX_COOLDOWN_MS = 5 * 60 * 1000;

const cacheByDb = new WeakMap();

function getEntry(db) {
  let entry = cacheByDb.get(db);
  if (!entry) {
    entry = { value: null, generation: -1, refreshing: null, finishedAt: 0, durationMs: 0 };
    cacheByDb.set(db, entry);
  }
  return entry;
}

function runOptionsWorker(dbPath) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL('./playlist-builder-options-worker.js', import.meta.url), {
      workerData: { dbPath },
    });
    let settled = false;
    const finish = (fn, arg) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      fn(arg);
    };
    const timer = setTimeout(() => {
      void worker.terminate();
      finish(reject, new Error('Playlist builder options worker timed out'));
    }, WORKER_TIMEOUT_MS);
    timer.unref?.();
    worker.once('message', (value) => finish(resolve, value));
    worker.once('error', (err) => finish(reject, err));
    worker.once('exit', (code) => finish(reject, new Error(`Playlist builder options worker exited (${code}) without a result`)));
  });
}

function canUseWorker(db) {
  return Boolean(db?.name) && !db.memory;
}

function startRefresh(db, entry, { runWorker = runOptionsWorker, now = Date.now } = {}) {
  if (entry.refreshing) return entry.refreshing;
  if (entry.value) {
    const cooldown = Math.min(MAX_COOLDOWN_MS, entry.durationMs * COOLDOWN_FACTOR);
    if (now() - entry.finishedAt < cooldown) return null;
  }
  const generation = getLibraryDataGeneration(db);
  const startedAt = now();
  entry.refreshing = Promise.resolve()
    .then(() => runWorker(db.name))
    .then((value) => {
      entry.value = { ...emptyPlaylistBuilderOptions(), ...(value || {}) };
      entry.generation = generation;
      return entry.value;
    })
    .catch((err) => {
      console.warn(`[playlist-builder-options] refresh failed: ${err?.message || err}`);
      return null;
    })
    .finally(() => {
      entry.finishedAt = now();
      entry.durationMs = entry.finishedAt - startedAt;
      entry.refreshing = null;
    });
  return entry.refreshing;
}

function delay(ms) {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    timer.unref?.();
  });
}

/**
 * Returns the builder option lists plus `builderOptionsPending`, which is true
 * when no lists have been built yet and the caller is rendering empty ones.
 */
export async function getPlaylistBuilderOptions(db, { waitMs = DEFAULT_WAIT_MS, runWorker, now } = {}) {
  const entry = getEntry(db);
  const isFresh = () => entry.value && entry.generation === getLibraryDataGeneration(db);
  if (isFresh()) return { ...entry.value, builderOptionsPending: false };

  // In-memory databases (tests) can't be opened from a worker; they are small, so build inline.
  if (!canUseWorker(db)) {
    entry.value = computePlaylistBuilderOptions(db);
    entry.generation = getLibraryDataGeneration(db);
    return { ...entry.value, builderOptionsPending: false };
  }

  const refresh = startRefresh(db, entry, { runWorker, now });
  if (refresh && waitMs > 0) await Promise.race([refresh, delay(waitMs)]);
  if (entry.value) return { ...entry.value, builderOptionsPending: false };
  return { ...emptyPlaylistBuilderOptions(), builderOptionsPending: true };
}

/** Starts a background rebuild if the cached lists are missing or stale. */
export function warmPlaylistBuilderOptions(db, options = {}) {
  if (!canUseWorker(db)) return null;
  const entry = getEntry(db);
  if (entry.value && entry.generation === getLibraryDataGeneration(db)) return null;
  return startRefresh(db, entry, options);
}
