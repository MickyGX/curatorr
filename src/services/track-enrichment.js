import fs from 'fs';
import http from 'http';
import https from 'https';
import path from 'path';
import { execFile } from 'node:child_process';
import {
  countTracksMissingPlexLoudness,
  countTracksMissingEnrichment,
  getMasterTracks,
  getTrackEnrichmentByRatingKeys,
  listTracksMissingEnrichment,
  listTracksMissingPlexLoudness,
  setSystemJobRun,
  upsertTrackEnrichment,
} from '../db.js';
import {
  buildTrackFeatureManifest,
  mergeAnalyzerResultsIntoManifest,
  parseAnalyzerFeatureInput,
} from './track-feature-manifest.js';

function clampNumber(value, min, max, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, num));
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const execFileAsync = (file, args, options = {}) => new Promise((resolve, reject) => {
  execFile(file, args, {
    ...options,
    encoding: 'utf8',
    maxBuffer: options.maxBuffer || 10 * 1024 * 1024,
  }, (err, stdout, stderr) => {
    if (err) {
      err.stdout = stdout;
      err.stderr = stderr;
      reject(err);
      return;
    }
    resolve({ stdout, stderr });
  });
});

function resolveManifestPath(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  return path.isAbsolute(raw) ? raw : path.resolve(process.cwd(), raw);
}

function shellQuote(value) {
  return `'${String(value || '').replace(/'/g, `'\"'\"'`)}'`;
}

function normalizeFeatureNumber(value, { min = null, max = null } = {}) {
  if (value == null || value === '') return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  if (min != null && num < min) return null;
  if (max != null && num > max) return null;
  return num;
}

function normalizeOptionalNumber(value) {
  if (value == null || value === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function buildChunkFilePath(basePath, chunkIndex, totalChunks) {
  const parsed = path.parse(String(basePath || '').trim());
  const width = Math.max(3, String(totalChunks || 1).length);
  const suffix = `.chunk-${String(chunkIndex + 1).padStart(width, '0')}`;
  return path.join(parsed.dir || '.', `${parsed.name}${suffix}${parsed.ext || '.json'}`);
}

function splitIntoChunks(items, chunkSize) {
  const size = Math.max(1, Math.floor(Number(chunkSize) || 1));
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function shouldLogChunkProgress(chunkIndex, totalChunks) {
  const current = chunkIndex + 1;
  if (current === 1 || current === totalChunks) return true;
  if (totalChunks <= 20) return true;
  if (totalChunks <= 200) return current % 10 === 0;
  return current % 50 === 0;
}

function accumulateImportTotals(target, result = {}) {
  target.processed += Number(result.processed || 0);
  target.imported += Number(result.imported || 0);
  target.skipped += Number(result.skipped || 0);
  target.unmatched += Number(result.unmatched || 0);
  return target;
}

function updateJobProgress(db, jobId, message, lastRunAt = Date.now()) {
  if (!db || !jobId) return;
  setSystemJobRun(db, jobId, {
    status: 'running',
    lastRunAt,
    message: String(message || '').trim(),
  });
}

function normalizeFeatureEntry(entry) {
  if (!entry || typeof entry !== 'object') return null;
  const ratingKey = String(entry.ratingKey || entry.rating_key || '').trim();
  const recordingMbid = String(entry.recordingMbid || entry.recording_mbid || '').trim();
  const filePath = String(entry.filePath || entry.file_path || '').trim();
  const musicalKey = String(entry.musicalKey || entry.musical_key || '').trim();
  const camelotKey = String(entry.camelotKey || entry.camelot_key || '').trim();
  const bpm = normalizeFeatureNumber(entry.bpm, { min: 1, max: 400 });
  const energy = normalizeFeatureNumber(entry.energy, { min: 0, max: 1 });
  const danceability = normalizeFeatureNumber(entry.danceability, { min: 0, max: 1 });
  if (!ratingKey && !recordingMbid && !filePath) return null;
  if (bpm == null && !musicalKey && !camelotKey && energy == null && danceability == null) return null;
  return {
    ratingKey,
    recordingMbid,
    filePath,
    bpm,
    musicalKey,
    camelotKey,
    energy,
    danceability,
    analysisSource: String(entry.analysisSource || entry.analysis_source || entry.source || 'feature-import').trim() || 'feature-import',
    analysisConfidence: normalizeFeatureNumber(
      entry.analysisConfidence ?? entry.analysis_confidence ?? entry.confidence,
      { min: 0, max: 1 },
    ) ?? 0,
  };
}

function parseFeatureManifest(rawValue) {
  const parsed = JSON.parse(String(rawValue || '').trim() || '[]');
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed?.tracks)) return parsed.tracks;
  if (parsed && typeof parsed === 'object') {
    return Object.entries(parsed).map(([ratingKey, value]) => {
      if (value && typeof value === 'object' && !Array.isArray(value)) return { ratingKey, ...value };
      return null;
    }).filter(Boolean);
  }
  return [];
}

function parseDatePrecision(value) {
  const raw = String(value || '').trim();
  if (!raw) return { raw: '', rank: 0, time: Number.POSITIVE_INFINITY };
  const match = raw.match(/^(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?$/);
  if (!match) return { raw: '', rank: 0, time: Number.POSITIVE_INFINITY };
  const year = Number(match[1] || 0);
  const month = Number(match[2] || 1);
  const day = Number(match[3] || 1);
  const rank = match[3] ? 3 : (match[2] ? 2 : 1);
  const time = Date.UTC(year, Math.max(0, month - 1), day);
  return {
    raw,
    rank,
    time: Number.isFinite(time) ? time : Number.POSITIVE_INFINITY,
  };
}

function pickBestDatedEntry(entries = []) {
  return [...entries]
    .map((entry) => {
      const parsed = parseDatePrecision(
        entry?.date
        || entry?.firstReleaseDate
        || entry?.first_release_date
        || entry?.['first-release-date']
        || '',
      );
      const isOfficial = String(entry?.status || '').trim().toLowerCase() === 'official' ? 1 : 0;
      return { entry, parsed, isOfficial };
    })
    .filter((candidate) => candidate.parsed.raw)
    .sort((left, right) => {
      if (left.parsed.time !== right.parsed.time) return left.parsed.time - right.parsed.time;
      if (left.isOfficial !== right.isOfficial) return right.isOfficial - left.isOfficial;
      return right.parsed.rank - left.parsed.rank;
    })[0] || null;
}

function normalizeReleasePayload(recording = {}) {
  const releases = Array.isArray(recording?.releases) ? recording.releases : [];
  const releaseGroups = Array.isArray(recording?.['release-groups']) ? recording['release-groups'] : [];
  const bestRelease = pickBestDatedEntry(releases);
  const bestReleaseGroup = pickBestDatedEntry(releaseGroups);
  const chosen = bestRelease || bestReleaseGroup;
  if (!chosen) {
    return {
      trackYear: null,
      originalReleaseDate: '',
      confidence: 0.1,
      payload: {
        recordingId: String(recording?.id || '').trim(),
        releasesScanned: releases.length,
        releaseGroupsScanned: releaseGroups.length,
      },
    };
  }
  const originalReleaseDate = chosen.parsed.raw;
  const yearMatch = originalReleaseDate.match(/^(\d{4})/);
  const trackYear = yearMatch ? Number(yearMatch[1]) : null;
  const confidence = bestRelease
    ? (chosen.parsed.rank >= 3 ? 0.95 : (chosen.parsed.rank === 2 ? 0.85 : 0.75))
    : (chosen.parsed.rank >= 3 ? 0.8 : 0.7);
  return {
    trackYear,
    originalReleaseDate,
    confidence,
    payload: {
      recordingId: String(recording?.id || '').trim(),
      releasesScanned: releases.length,
      releaseGroupsScanned: releaseGroups.length,
      matchedKind: bestRelease ? 'release' : 'release-group',
      matchedId: String(chosen.entry?.id || '').trim(),
      matchedStatus: String(chosen.entry?.status || '').trim(),
      matchedDate: originalReleaseDate,
    },
  };
}

async function requestExternalJson(url, options = {}) {
  const timeoutMs = Number(options.timeoutMs || 15000);
  let controller = null;
  let timeoutId = null;
  const hasBody = options.body != null;
  const init = {
    method: options.method || 'GET',
    headers: {
      Accept: 'application/json',
      'User-Agent': 'Curatorr/phase3 (+https://github.com/MickyGX/curatorr)',
      ...(hasBody ? { 'Content-Type': 'application/json' } : {}),
      ...(options.headers || {}),
    },
  };
  if (hasBody) init.body = typeof options.body === 'string' ? options.body : JSON.stringify(options.body);
  if (timeoutMs > 0 && typeof AbortController !== 'undefined') {
    controller = new AbortController();
    init.signal = controller.signal;
    timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  }
  try {
    const response = await fetch(String(url || ''), init);
    if (!response.ok) {
      const err = new Error(`External request failed (${response.status}).`);
      err.status = response.status;
      throw err;
    }
    return await response.json();
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
}

async function requestLongRunningJson(url, options = {}) {
  const timeoutMs = Number(options.timeoutMs || 15000);
  const hasBody = options.body != null;
  const parsedUrl = new URL(String(url || ''));
  const body = hasBody ? (typeof options.body === 'string' ? options.body : JSON.stringify(options.body)) : '';
  const headers = {
    Accept: 'application/json',
    'User-Agent': 'Curatorr/phase3 (+https://github.com/MickyGX/curatorr)',
    ...(hasBody ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) } : {}),
    ...(options.headers || {}),
  };
  const transport = parsedUrl.protocol === 'https:' ? https : http;

  return new Promise((resolve, reject) => {
    const req = transport.request(parsedUrl, {
      method: options.method || 'GET',
      headers,
    }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        const rawBody = Buffer.concat(chunks).toString('utf8');
        const statusCode = Number(res.statusCode || 0);
        if (statusCode < 200 || statusCode >= 300) {
          const err = new Error(`External request failed (${statusCode}).`);
          err.status = statusCode;
          err.responseBody = rawBody;
          reject(err);
          return;
        }
        try {
          resolve(rawBody ? JSON.parse(rawBody) : {});
        } catch (err) {
          reject(err);
        }
      });
    });

    req.on('error', reject);
    if (timeoutMs > 0) {
      req.setTimeout(timeoutMs, () => {
        req.destroy(new Error(`External request timed out after ${timeoutMs}ms.`));
      });
    }
    if (body) req.write(body);
    req.end();
  });
}

function buildPlexUrl(baseUrl, suffixPath) {
  let normalized = String(baseUrl || '').trim().replace(/\/+$/, '');
  if (!normalized) return '';
  if (!/^https?:\/\//i.test(normalized)) normalized = `http://${normalized}`;
  const suffix = String(suffixPath || '').replace(/^\/+/, '');
  return `${normalized}/${suffix}`;
}

function plexHeaders(token, extraHeaders = {}) {
  return {
    ...(token ? { 'X-Plex-Token': String(token).trim() } : {}),
    ...extraHeaders,
  };
}

function pickPrimaryAudioStream(metadata = {}) {
  const streams = metadata?.Media?.[0]?.Part?.[0]?.Stream || [];
  const audioStreams = streams.filter((stream) => Number(stream?.streamType || 0) === 2);
  return audioStreams.find((stream) => stream?.selected) || audioStreams[0] || null;
}

function extractPlexLoudnessValues(metadata = {}) {
  const media = metadata?.Media?.[0] || {};
  const part = media?.Part?.[0] || {};
  const stream = pickPrimaryAudioStream(metadata);
  return {
    mediaItemId: media?.id != null ? Number(media.id) : null,
    partId: part?.id != null ? Number(part.id) : null,
    streamId: stream?.id != null ? Number(stream.id) : null,
    filePath: String(part?.file || '').trim(),
    loudness: normalizeOptionalNumber(stream?.loudness),
    loudnessRange: normalizeOptionalNumber(stream?.lra),
    peak: normalizeOptionalNumber(stream?.peak),
    trackGain: normalizeOptionalNumber(stream?.gain),
    albumGain: normalizeOptionalNumber(stream?.albumGain),
    albumPeak: normalizeOptionalNumber(stream?.albumPeak),
    albumRange: normalizeOptionalNumber(stream?.albumRange),
  };
}

function resolveAnalyzerExecution(options = {}) {
  const rawMode = String(options.analyzerMode || options.mode || 'custom').trim().toLowerCase();
  const mode = (rawMode === 'sidecar' || rawMode === 'builtin')
    ? 'sidecar'
    : 'custom';
  if (mode === 'custom') {
    return {
      mode,
      command: String(options.command || '').trim(),
      workingDir: resolveManifestPath(options.workingDir || process.cwd()) || process.cwd(),
    };
  }
  if (mode === 'sidecar') {
    return {
      mode,
      sidecarUrl: String(options.analyzerSidecarUrl || options.sidecarUrl || '').trim(),
      timeoutMs: clampNumber(options.timeoutMs, 1000, 24 * 60 * 60 * 1000, 12 * 60 * 60 * 1000),
    };
  }

}

async function invokeAnalyzerSidecar(sidecarUrl, payload = {}, options = {}) {
  const base = String(sidecarUrl || '').trim().replace(/\/+$/, '');
  if (!base) throw new Error('No analyzer sidecar URL is configured.');
  return requestLongRunningJson(`${base}/analyze`, {
    method: 'POST',
    timeoutMs: clampNumber(options.timeoutMs, 1000, 24 * 60 * 60 * 1000, 12 * 60 * 60 * 1000),
    body: payload,
  });
}

export function createTrackEnrichmentService(ctx) {
  const { db, pushLog, safeMessage } = ctx;
  const execCommand = typeof ctx.execCommand === 'function'
    ? ctx.execCommand
    : async ({ command, cwd, env }) => {
      return execFileAsync('/bin/sh', ['-lc', command], { cwd, env });
    };

  async function fetchMusicBrainzRecordingYear(recordingMbid, options = {}) {
    const mbid = String(recordingMbid || '').trim();
    if (!mbid) return null;
    const url = new URL(`https://musicbrainz.org/ws/2/recording/${encodeURIComponent(mbid)}`);
    url.searchParams.set('inc', 'releases+release-groups');
    url.searchParams.set('fmt', 'json');
    const recording = await requestExternalJson(url.toString(), { timeoutMs: Number(options.timeoutMs || 20000) });
    return normalizeReleasePayload(recording || {});
  }

  async function runSync(options = {}) {
    const limit = clampNumber(options.limit, 1, 100, 20);
    const requestDelayMs = clampNumber(options.requestDelayMs, 0, 5000, 1100);
    const timeoutMs = clampNumber(options.timeoutMs, 1000, 60000, 20000);
    const batch = listTracksMissingEnrichment(db, {
      limit,
      requireRecordingMbid: true,
    });
    let processed = 0;
    let enriched = 0;
    let skipped = 0;
    let failed = 0;

    if (!batch.length) {
      return {
        processed,
        enriched,
        skipped,
        failed,
        remaining: 0,
        message: 'No tracks are waiting for enrichment.',
      };
    }

    pushLog?.({
      level: 'info',
      app: 'enrichment',
      action: 'track-enrichment.start',
      message: `Track enrichment started for ${batch.length} track${batch.length === 1 ? '' : 's'}.`,
      meta: { limit, requestDelayMs, timeoutMs },
    });

    for (let index = 0; index < batch.length; index += 1) {
      const track = batch[index];
      try {
        const result = await fetchMusicBrainzRecordingYear(track.recordingMbid, { timeoutMs });
        upsertTrackEnrichment(db, {
          ratingKey: track.ratingKey,
          recordingMbid: track.recordingMbid,
          trackYear: result?.trackYear ?? null,
          originalReleaseDate: result?.originalReleaseDate || '',
          analysisSource: 'musicbrainz',
          analysisConfidence: Number(result?.confidence || 0),
          payload: {
            artistName: track.artistName,
            trackTitle: track.trackTitle,
            albumName: track.albumName,
            ...(result?.payload || {}),
          },
          updatedAt: Date.now(),
        });
        processed += 1;
        if (result?.trackYear || result?.originalReleaseDate) enriched += 1;
        else skipped += 1;
      } catch (err) {
        if (Number(err?.status || 0) === 404) {
          upsertTrackEnrichment(db, {
            ratingKey: track.ratingKey,
            recordingMbid: track.recordingMbid,
            trackYear: null,
            originalReleaseDate: '',
            analysisSource: 'musicbrainz',
            analysisConfidence: 0,
            payload: {
              artistName: track.artistName,
              trackTitle: track.trackTitle,
              albumName: track.albumName,
              error: 'not-found',
              status: 404,
            },
            updatedAt: Date.now(),
          });
          processed += 1;
          skipped += 1;
          continue;
        }
        failed += 1;
        pushLog?.({
          level: 'warn',
          app: 'enrichment',
          action: 'track-enrichment.track.error',
          message: `Track enrichment failed for ${track.artistName} - ${track.trackTitle}: ${safeMessage?.(err) || String(err?.message || err || 'Unknown error')}`,
          meta: {
            ratingKey: track.ratingKey,
            recordingMbid: track.recordingMbid,
          },
        });
      }
      if (requestDelayMs > 0 && index < batch.length - 1) await wait(requestDelayMs);
    }

    const remaining = countTracksMissingEnrichment(db, { requireRecordingMbid: true });
    const message = `Processed ${processed} track${processed === 1 ? '' : 's'}; enriched ${enriched}, skipped ${skipped}, failed ${failed}, remaining ${remaining}.`;
    pushLog?.({
      level: failed > 0 ? 'warn' : 'info',
      app: 'enrichment',
      action: 'track-enrichment.finish',
      message,
      meta: { processed, enriched, skipped, failed, remaining },
    });
    return { processed, enriched, skipped, failed, remaining, message };
  }

  async function runPlexLoudnessSync(options = {}) {
    const jobId = String(options.jobId || 'trackPlexLoudnessSync').trim() || 'trackPlexLoudnessSync';
    const startedAt = Date.now();
    const loadConfig = typeof ctx.loadConfig === 'function' ? ctx.loadConfig : null;
    const config = loadConfig ? loadConfig() : {};
    const mediaServerType = String(config?.mediaServer?.type || 'plex').trim().toLowerCase();
    if (mediaServerType !== 'plex') {
      return {
        processed: 0,
        synced: 0,
        skipped: 0,
        failed: 0,
        message: 'Plex loudness sync is only available when Plex is the active media server.',
      };
    }

    const baseUrl = String(config?.plex?.url || '').trim();
    const token = String(config?.plex?.token || '').trim();
    if (!baseUrl || !token) {
      return {
        processed: 0,
        synced: 0,
        skipped: 0,
        failed: 0,
        message: 'Plex loudness sync requires a saved Plex URL and token.',
      };
    }

    const pageSize = clampNumber(options.limit, 1, 500, 100);
    const batchSize = clampNumber(options.batchSize, 1, 100, 25);
    const totalCandidates = countTracksMissingPlexLoudness(db);
    if (!totalCandidates) {
      return {
        processed: 0,
        synced: 0,
        skipped: 0,
        failed: 0,
        message: 'No tracks are waiting for Plex loudness sync.',
      };
    }

    const fetchedAt = Date.now();
    let processed = 0;
    let synced = 0;
    let skipped = 0;
    let failed = 0;
    let cursorUpdatedAt = null;
    let cursorRatingKey = '';
    let pageIndex = 0;
    const totalPages = Math.max(1, Math.ceil(totalCandidates / pageSize));

    pushLog?.({
      level: 'info',
      app: 'enrichment',
      action: 'plex-loudness.start',
      message: `Plex loudness sync started for ${totalCandidates} queued track${totalCandidates === 1 ? '' : 's'}.`,
      meta: { totalCandidates, pageSize, batchSize },
    });
    updateJobProgress(
      db,
      jobId,
      `Preparing ${totalCandidates} loudness candidate${totalCandidates === 1 ? '' : 's'} in ${totalPages} page${totalPages === 1 ? '' : 's'}…`,
      startedAt,
    );

    while (true) {
      const page = listTracksMissingPlexLoudness(db, {
        limit: pageSize,
        cursorUpdatedAt,
        cursorRatingKey,
      });
      if (!page.length) break;

      pageIndex += 1;
      const existing = new Map(
        getTrackEnrichmentByRatingKeys(db, page.map((track) => track.ratingKey))
          .map((row) => [row.ratingKey, row]),
      );

      updateJobProgress(
        db,
        jobId,
        `Scanning loudness page ${pageIndex}/${totalPages} (${processed + 1}-${Math.min(totalCandidates, processed + page.length)} of ${totalCandidates} candidates)… Synced ${synced}, skipped ${skipped}, failed ${failed}.`,
        startedAt,
      );

      for (let index = 0; index < page.length; index += batchSize) {
        const slice = page.slice(index, index + batchSize);
        const url = buildPlexUrl(baseUrl, `library/metadata/${slice.map((track) => encodeURIComponent(track.ratingKey)).join(',')}`);
        processed += slice.length;
        try {
          const response = await fetch(url, {
            headers: plexHeaders(token, { Accept: 'application/json' }),
          });
          if (!response.ok) throw new Error(`Plex metadata request failed (${response.status}).`);

          const json = await response.json();
          const metadataItems = Array.isArray(json?.MediaContainer?.Metadata) ? json.MediaContainer.Metadata : [];
          const byRatingKey = new Map(metadataItems.map((item) => [String(item?.ratingKey || '').trim(), item]));
          const updates = [];

          for (const track of slice) {
            const metadata = byRatingKey.get(track.ratingKey);
            if (!metadata) {
              skipped += 1;
              continue;
            }
            const current = existing.get(track.ratingKey) || {};
            const values = extractPlexLoudnessValues(metadata);
            const hasLoudness = [
              values.loudness,
              values.loudnessRange,
              values.peak,
              values.trackGain,
              values.albumGain,
              values.albumPeak,
              values.albumRange,
            ].some((value) => value != null);
            if (!hasLoudness) {
              skipped += 1;
              continue;
            }
            updates.push({
              ratingKey: track.ratingKey,
              recordingMbid: current.recordingMbid || track.recordingMbid || '',
              trackYear: current.trackYear ?? null,
              originalReleaseDate: current.originalReleaseDate || '',
              bpm: current.bpm ?? null,
              musicalKey: current.musicalKey || '',
              camelotKey: current.camelotKey || '',
              energy: current.energy ?? null,
              danceability: current.danceability ?? null,
              loudness: values.loudness,
              loudnessRange: values.loudnessRange,
              peak: values.peak,
              trackGain: values.trackGain,
              albumGain: values.albumGain,
              albumPeak: values.albumPeak,
              albumRange: values.albumRange,
              analysisSource: current.analysisSource || 'plex-loudness',
              analysisConfidence: Math.max(Number(current.analysisConfidence || 0), 1),
              payload: {
                ...(current.payload && typeof current.payload === 'object' ? current.payload : {}),
                plexLoudness: {
                  fetchedAt,
                  mediaItemId: values.mediaItemId,
                  partId: values.partId,
                  streamId: values.streamId,
                  filePath: values.filePath || track.filePath || '',
                },
              },
              updatedAt: fetchedAt,
            });
          }

          if (updates.length) {
            upsertTrackEnrichment(db, updates);
            for (const row of updates) existing.set(row.ratingKey, row);
            synced += updates.length;
          }
        } catch (err) {
          failed += slice.length;
          pushLog?.({
            level: 'warn',
            app: 'enrichment',
            action: 'plex-loudness.batch.error',
            message: `Plex loudness sync failed for ${slice.length} track${slice.length === 1 ? '' : 's'}: ${safeMessage?.(err) || String(err?.message || err || 'Unknown error')}`,
            meta: {
              ratingKeys: slice.map((track) => track.ratingKey),
            },
          });
        }
      }

      const lastTrack = page[page.length - 1];
      cursorUpdatedAt = Number(lastTrack?.updatedAt || 0);
      cursorRatingKey = String(lastTrack?.ratingKey || '');

      updateJobProgress(
        db,
        jobId,
        `Completed loudness page ${pageIndex}/${totalPages}. Processed ${Math.min(processed, totalCandidates)}/${totalCandidates}; synced ${synced}, skipped ${skipped}, failed ${failed}.`,
        startedAt,
      );
    }

    const remaining = countTracksMissingPlexLoudness(db);
    const message = `Processed ${processed} queued track${processed === 1 ? '' : 's'}; synced ${synced}, skipped ${skipped}, failed ${failed}. Remaining ${remaining}.`;
    pushLog?.({
      level: failed > 0 ? 'warn' : 'info',
      app: 'enrichment',
      action: 'plex-loudness.finish',
      message,
      meta: { processed, synced, skipped, failed, remaining, totalCandidates, pageSize },
    });
    return { processed, synced, skipped, failed, remaining, message };
  }

  async function importFeatureManifest(options = {}) {
    const manifestPath = resolveManifestPath(options.manifestPath);
    if (!manifestPath) {
      return {
        processed: 0,
        imported: 0,
        skipped: 0,
        unmatched: 0,
        message: 'No feature manifest path is configured.',
      };
    }
    if (!fs.existsSync(manifestPath)) {
      return {
        processed: 0,
        imported: 0,
        skipped: 0,
        unmatched: 0,
        message: `Feature manifest not found: ${manifestPath}`,
      };
    }

    const masterTracks = getMasterTracks(db);
    const byRatingKey = new Map(masterTracks.map((track) => [String(track.ratingKey || ''), track]));
    const byRecordingMbid = new Map(masterTracks
      .filter((track) => track.recordingMbid)
      .map((track) => [String(track.recordingMbid || '').trim(), track]));
    const byFilePath = new Map(masterTracks
      .filter((track) => track.filePath)
      .map((track) => [String(track.filePath || '').trim(), track]));

    const rawManifest = fs.readFileSync(manifestPath, 'utf8');
    const entries = parseFeatureManifest(rawManifest).map(normalizeFeatureEntry).filter(Boolean);
    const matchedKeys = new Set();
    const updates = [];
    let skipped = 0;
    let unmatched = 0;

    for (const entry of entries) {
      const matchedTrack = byRatingKey.get(entry.ratingKey)
        || byRecordingMbid.get(entry.recordingMbid)
        || byFilePath.get(entry.filePath);
      if (!matchedTrack) {
        unmatched += 1;
        continue;
      }
      if (matchedKeys.has(matchedTrack.ratingKey)) {
        skipped += 1;
        continue;
      }
      matchedKeys.add(matchedTrack.ratingKey);
      updates.push({ ...entry, matchedTrack });
    }

    const existing = new Map(getTrackEnrichmentByRatingKeys(db, updates.map((entry) => entry.matchedTrack.ratingKey))
      .map((row) => [row.ratingKey, row]));

    const importedAt = Date.now();
    upsertTrackEnrichment(db, updates.map((entry) => {
      const current = existing.get(entry.matchedTrack.ratingKey) || {};
      return {
        ratingKey: entry.matchedTrack.ratingKey,
        recordingMbid: entry.matchedTrack.recordingMbid || current.recordingMbid || entry.recordingMbid,
        trackYear: current.trackYear ?? null,
        originalReleaseDate: current.originalReleaseDate || '',
        bpm: entry.bpm ?? current.bpm ?? null,
        musicalKey: entry.musicalKey || current.musicalKey || '',
        camelotKey: entry.camelotKey || current.camelotKey || '',
        energy: entry.energy ?? current.energy ?? null,
        danceability: entry.danceability ?? current.danceability ?? null,
        analysisSource: entry.analysisSource || current.analysisSource || 'feature-import',
        analysisConfidence: entry.analysisConfidence ?? current.analysisConfidence ?? 0,
        payload: {
          ...(current.payload && typeof current.payload === 'object' ? current.payload : {}),
          featureImport: {
            manifestPath,
            importedAt,
          },
        },
        updatedAt: importedAt,
      };
    }));

    const message = `Imported features for ${updates.length} track${updates.length === 1 ? '' : 's'} from ${manifestPath}; skipped ${skipped}, unmatched ${unmatched}.`;
    if (options.suppressLog !== true) {
      pushLog?.({
        level: 'info',
        app: 'enrichment',
        action: 'track-features.import',
        message,
        meta: { manifestPath, processed: entries.length, imported: updates.length, skipped, unmatched },
      });
    }
    return {
      processed: entries.length,
      imported: updates.length,
      skipped,
      unmatched,
      message,
    };
  }

  async function runAutomatedAnalysis(options = {}) {
    const jobId = String(options.jobId || 'trackAnalysisPipeline').trim() || 'trackAnalysisPipeline';
    const startedAt = Date.now();
    const manifestPath = resolveManifestPath(options.manifestPath);
    const resultsPath = resolveManifestPath(options.resultsPath);
    const execution = resolveAnalyzerExecution(options);
    const workingDir = execution.workingDir;
    const command = execution.command;
    const inputFormat = String(options.inputFormat || 'auto').trim().toLowerCase() || 'auto';
    const exportLimit = clampNumber(options.exportLimit, 1, 100000, 0);
    const exportMissingOnly = options.exportMissingOnly !== false;
    const overwriteExisting = options.overwriteExisting === true;
    const defaultChunkSize = execution.mode === 'sidecar' ? 100 : 250;
    const chunkSize = clampNumber(options.chunkSize, 1, 5000, defaultChunkSize);

    if (!manifestPath) {
      return { ok: false, message: 'No feature manifest path is configured.' };
    }
    if (!resultsPath) {
      return { ok: false, message: 'No analyzer results path is configured.' };
    }
    if (execution.mode === 'custom' && !command) {
      return { ok: false, message: 'No analyzer command is configured.' };
    }
    if (execution.mode === 'sidecar' && !execution.sidecarUrl) {
      return { ok: false, message: 'No analyzer sidecar URL is configured.' };
    }

    const sourceTracks = getMasterTracks(db);
    const manifest = buildTrackFeatureManifest(sourceTracks, {
      missingOnly: exportMissingOnly,
      includeExisting: false,
      limit: exportLimit > 0 ? exportLimit : undefined,
    });
    fs.mkdirSync(path.dirname(manifestPath), { recursive: true });
    fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
    fs.writeFileSync(resultsPath, JSON.stringify({ tracks: [] }, null, 2));

    if (!manifest.trackCount) {
      const message = 'Automated track analysis skipped because no tracks need feature analysis.';
      pushLog?.({
        level: 'info',
        app: 'enrichment',
        action: 'track-analysis.finish',
        message,
        meta: { manifestPath, resultsPath, analyzerMode: execution.mode, chunkSize, chunkCount: 0 },
      });
      return {
        ok: true,
        manifestPath,
        resultsPath,
        analyzerMode: execution.mode,
        analyzerRows: 0,
        chunkCount: 0,
        chunkSize,
        importResult: { processed: 0, imported: 0, skipped: 0, unmatched: 0, message },
        sidecarResult: null,
        commandStdout: '',
        commandStderr: '',
        message,
      };
    }

    const env = {
      ...process.env,
      CURATORR_FEATURE_TEMPLATE: manifestPath,
      CURATORR_FEATURE_MANIFEST: manifestPath,
      CURATORR_ANALYZER_OUTPUT: resultsPath,
      CURATORR_DB_PATH: String(ctx?.DB_PATH || process.env.DB_PATH || ''),
    };

    pushLog?.({
      level: 'info',
      app: 'enrichment',
      action: 'track-analysis.start',
      message: `Starting automated track analysis for ${manifest.trackCount} track${manifest.trackCount === 1 ? '' : 's'}.`,
      meta: { manifestPath, resultsPath, workingDir, analyzerMode: execution.mode, chunkSize },
    });
    updateJobProgress(db, jobId, `Preparing ${manifest.trackCount} tracks for analysis in ${Math.ceil(manifest.trackCount / chunkSize)} chunk${Math.ceil(manifest.trackCount / chunkSize) === 1 ? '' : 's'}…`, startedAt);

    const manifestChunks = splitIntoChunks(manifest.tracks, chunkSize);
    const totalChunks = manifestChunks.length;
    const aggregateImport = { processed: 0, imported: 0, skipped: 0, unmatched: 0 };
    const aggregateRows = [];
    let commandStdout = '';
    let commandStderr = '';
    let sidecarResult = null;

    for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex += 1) {
      const chunkTracks = manifestChunks[chunkIndex];
      const chunkManifest = {
        ...manifest,
        trackCount: chunkTracks.length,
        tracks: chunkTracks,
      };
      const usePrimaryPaths = totalChunks === 1;
      const chunkManifestPath = usePrimaryPaths ? manifestPath : buildChunkFilePath(manifestPath, chunkIndex, totalChunks);
      const chunkResultsPath = usePrimaryPaths ? resultsPath : buildChunkFilePath(resultsPath, chunkIndex, totalChunks);
      fs.writeFileSync(chunkManifestPath, JSON.stringify(chunkManifest, null, 2));
      if (!usePrimaryPaths) fs.writeFileSync(chunkResultsPath, JSON.stringify({ tracks: [] }, null, 2));

      const chunkEnv = {
        ...env,
        CURATORR_FEATURE_TEMPLATE: chunkManifestPath,
        CURATORR_FEATURE_MANIFEST: chunkManifestPath,
        CURATORR_ANALYZER_OUTPUT: chunkResultsPath,
      };

      try {
        updateJobProgress(
          db,
          jobId,
          `Analyzing chunk ${chunkIndex + 1}/${totalChunks} (${Math.min(manifest.trackCount, (chunkIndex * chunkSize) + 1)}-${Math.min(manifest.trackCount, (chunkIndex + 1) * chunkSize)} of ${manifest.trackCount} tracks)… Imported ${aggregateImport.imported} so far.`,
          startedAt,
        );
        let chunkCommandResult = { stdout: '', stderr: '' };
        if (execution.mode === 'sidecar') {
          sidecarResult = await invokeAnalyzerSidecar(execution.sidecarUrl, {
            inputPath: chunkManifestPath,
            outputPath: chunkResultsPath,
          }, {
            timeoutMs: execution.timeoutMs,
          });
        } else {
          chunkCommandResult = await execCommand({ command, cwd: workingDir, env: chunkEnv });
          if (chunkCommandResult.stdout) commandStdout += String(chunkCommandResult.stdout);
          if (chunkCommandResult.stderr) commandStderr += String(chunkCommandResult.stderr);
        }

        if (!fs.existsSync(chunkResultsPath)) {
          throw new Error(`Analyzer finished without writing results: ${chunkResultsPath}`);
        }

        const analyzerRaw = fs.readFileSync(chunkResultsPath, 'utf8');
        const analyzerRows = parseAnalyzerFeatureInput(analyzerRaw, inputFormat);
        aggregateRows.push(...analyzerRows);
        fs.writeFileSync(resultsPath, JSON.stringify({ tracks: aggregateRows }, null, 2));

        const mergedChunkManifest = mergeAnalyzerResultsIntoManifest(chunkManifest, analyzerRows, { overwriteExisting });
        fs.writeFileSync(chunkManifestPath, JSON.stringify(mergedChunkManifest, null, 2));
        const chunkImportResult = await importFeatureManifest({
          manifestPath: chunkManifestPath,
          suppressLog: true,
        });
        accumulateImportTotals(aggregateImport, chunkImportResult);
        const percentComplete = Math.min(100, Math.round(((chunkIndex + 1) / totalChunks) * 100));
        updateJobProgress(
          db,
          jobId,
          `Chunk ${chunkIndex + 1}/${totalChunks} complete (${percentComplete}%). Imported ${aggregateImport.imported}/${aggregateImport.processed} analyzed rows so far.`,
          startedAt,
        );

        if (shouldLogChunkProgress(chunkIndex, totalChunks)) {
          pushLog?.({
            level: 'info',
            app: 'enrichment',
            action: 'track-analysis.chunk',
            message: `Processed analysis chunk ${chunkIndex + 1}/${totalChunks}.`,
            meta: {
              chunkIndex: chunkIndex + 1,
              totalChunks,
              chunkTracks: chunkTracks.length,
              analyzerRows: analyzerRows.length,
              imported: aggregateImport.imported,
            },
          });
        }
      } catch (err) {
        err.message = `Chunk ${chunkIndex + 1}/${totalChunks} failed: ${err.message}`;
        throw err;
      } finally {
        if (!usePrimaryPaths) {
          try { fs.unlinkSync(chunkManifestPath); } catch (_err) {}
          try { fs.unlinkSync(chunkResultsPath); } catch (_err) {}
        }
      }
    }

    const mergedManifest = mergeAnalyzerResultsIntoManifest(manifest, aggregateRows, { overwriteExisting });
    fs.writeFileSync(manifestPath, JSON.stringify(mergedManifest, null, 2));

    pushLog?.({
      level: 'info',
      app: 'enrichment',
      action: 'track-features.import',
      message: `Imported features for ${aggregateImport.imported} track${aggregateImport.imported === 1 ? '' : 's'} from automated chunked analysis.`,
      meta: {
        manifestPath,
        processed: aggregateImport.processed,
        imported: aggregateImport.imported,
        skipped: aggregateImport.skipped,
        unmatched: aggregateImport.unmatched,
        chunkCount: totalChunks,
      },
    });

    const message = `Automated track analysis completed: ${aggregateImport.imported} imported from ${aggregateRows.length} analyzer row${aggregateRows.length === 1 ? '' : 's'} across ${totalChunks} chunk${totalChunks === 1 ? '' : 's'}.`;
    pushLog?.({
      level: 'info',
      app: 'enrichment',
      action: 'track-analysis.finish',
      message,
      meta: {
        manifestPath,
        resultsPath,
        analyzerRows: aggregateRows.length,
        imported: aggregateImport.imported,
        chunkCount: totalChunks,
        chunkSize,
      },
    });
    return {
      ok: true,
      manifestPath,
      resultsPath,
      analyzerMode: execution.mode,
      analyzerRows: aggregateRows.length,
      chunkCount: totalChunks,
      chunkSize,
      mergeSummary: mergedManifest.mergeSummary || null,
      importResult: aggregateImport,
      sidecarResult,
      commandStdout: String(commandStdout || ''),
      commandStderr: String(commandStderr || ''),
      message,
    };
  }

  return {
    fetchMusicBrainzRecordingYear,
    importFeatureManifest,
    resolveAnalyzerExecution,
    runPlexLoudnessSync,
    runAutomatedAnalysis,
    runSync,
  };
}
