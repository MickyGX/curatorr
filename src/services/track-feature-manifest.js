const FEATURE_KEYS = ['bpm', 'musicalKey', 'camelotKey', 'energy', 'danceability'];

function normalizeNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function normalizeTrackEntry(track) {
  if (!track || typeof track !== 'object') return null;
  return {
    ratingKey: String(track.ratingKey || '').trim(),
    recordingMbid: String(track.recordingMbid || '').trim(),
    filePath: String(track.filePath || '').trim(),
    artistName: String(track.artistName || '').trim(),
    trackTitle: String(track.trackTitle || '').trim(),
    albumName: String(track.albumName || '').trim(),
    durationMs: normalizeNumber(track.durationMs),
    trackYear: normalizeNumber(track.trackYear),
    originalReleaseDate: String(track.originalReleaseDate || '').trim(),
    bpm: normalizeNumber(track.bpm),
    musicalKey: String(track.musicalKey || '').trim(),
    camelotKey: String(track.camelotKey || '').trim(),
    energy: normalizeNumber(track.energy),
    danceability: normalizeNumber(track.danceability),
    analysisSource: String(track.analysisSource || track.analysis_source || '').trim(),
    analysisConfidence: normalizeNumber(track.analysisConfidence ?? track.analysis_confidence),
  };
}

function normalizeText(value) {
  return String(value || '').trim();
}

function normalizeLower(value) {
  return normalizeText(value).toLowerCase();
}

function splitCsvLine(line) {
  const values = [];
  let current = '';
  let inQuotes = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (char === '"') {
      const next = line[index + 1];
      if (inQuotes && next === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (char === ',' && !inQuotes) {
      values.push(current);
      current = '';
      continue;
    }
    current += char;
  }
  values.push(current);
  return values.map((value) => value.trim());
}

function parseCsvRecords(raw) {
  const lines = String(raw || '').split(/\r?\n/).filter((line) => line.trim());
  if (!lines.length) return [];
  const header = splitCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cells = splitCsvLine(line);
    const record = {};
    header.forEach((key, index) => {
      record[key] = cells[index] ?? '';
    });
    return record;
  });
}

function getFirstValue(record, keys = []) {
  for (const key of keys) {
    if (record[key] != null && record[key] !== '') return record[key];
  }
  return '';
}

function normalizeAnalyzerRecord(record) {
  if (!record || typeof record !== 'object') return null;
  const ratingKey = normalizeText(getFirstValue(record, ['ratingKey', 'rating_key', 'plex_rating_key']));
  const recordingMbid = normalizeText(getFirstValue(record, ['recordingMbid', 'recording_mbid', 'mbid', 'musicbrainz_recording_id']));
  const filePath = normalizeText(getFirstValue(record, ['filePath', 'file_path', 'path', 'filename']));
  const artistName = normalizeText(getFirstValue(record, ['artistName', 'artist_name', 'artist']));
  const trackTitle = normalizeText(getFirstValue(record, ['trackTitle', 'track_title', 'title', 'name']));
  const albumName = normalizeText(getFirstValue(record, ['albumName', 'album_name', 'album']));
  const bpm = normalizeNumber(getFirstValue(record, ['bpm', 'tempo']));
  const musicalKey = normalizeText(getFirstValue(record, ['musicalKey', 'musical_key', 'key', 'initialKey']));
  const camelotKey = normalizeText(getFirstValue(record, ['camelotKey', 'camelot_key', 'camelot']));
  const energy = normalizeNumber(getFirstValue(record, ['energy']));
  const danceability = normalizeNumber(getFirstValue(record, ['danceability']));
  const analysisSource = normalizeText(getFirstValue(record, ['analysisSource', 'analysis_source', 'source']));
  const analysisConfidence = normalizeNumber(getFirstValue(record, ['analysisConfidence', 'analysis_confidence', 'confidence']));
  if (!ratingKey && !recordingMbid && !filePath && !(artistName && trackTitle)) return null;
  if (bpm == null && !musicalKey && !camelotKey && energy == null && danceability == null) return null;
  return {
    ratingKey,
    recordingMbid,
    filePath,
    artistName,
    trackTitle,
    albumName,
    bpm,
    musicalKey,
    camelotKey,
    energy,
    danceability,
    analysisSource,
    analysisConfidence,
  };
}

function normalizeManifestPayload(payload) {
  if (Array.isArray(payload)) return { ...buildTrackFeatureManifest(payload, { missingOnly: false }), tracks: payload.map(normalizeTrackEntry).filter(Boolean) };
  if (payload && typeof payload === 'object' && Array.isArray(payload.tracks)) {
    return {
      version: Number(payload.version || 1),
      source: String(payload.source || 'curatorr-track-feature-export'),
      generatedAt: String(payload.generatedAt || new Date().toISOString()),
      missingOnly: payload.missingOnly !== false,
      includeExisting: payload.includeExisting === true,
      trackCount: Array.isArray(payload.tracks) ? payload.tracks.length : 0,
      tracks: payload.tracks.map(normalizeTrackEntry).filter(Boolean),
    };
  }
  return buildTrackFeatureManifest([], { missingOnly: false });
}

export function trackNeedsFeatureImport(track) {
  const entry = normalizeTrackEntry(track);
  if (!entry) return false;
  return FEATURE_KEYS.some((key) => {
    const value = entry[key];
    return value == null || value === '';
  });
}

export function buildTrackFeatureManifest(tracks, options = {}) {
  const missingOnly = options.missingOnly !== false;
  const includeExisting = options.includeExisting === true;
  const limit = Number(options.limit);
  const generatedAt = String(options.generatedAt || new Date().toISOString());

  let items = (Array.isArray(tracks) ? tracks : [])
    .map(normalizeTrackEntry)
    .filter((track) => track && track.ratingKey);

  if (missingOnly) items = items.filter(trackNeedsFeatureImport);

  items = items.sort((left, right) => {
    return String(left.artistName || '').localeCompare(String(right.artistName || ''))
      || String(left.albumName || '').localeCompare(String(right.albumName || ''))
      || String(left.trackTitle || '').localeCompare(String(right.trackTitle || ''))
      || String(left.ratingKey || '').localeCompare(String(right.ratingKey || ''));
  });

  if (Number.isFinite(limit) && limit > 0) items = items.slice(0, Math.floor(limit));

  const payloadTracks = items.map((track) => {
    const payload = {
      ratingKey: track.ratingKey,
      recordingMbid: track.recordingMbid,
      filePath: track.filePath,
      artistName: track.artistName,
      trackTitle: track.trackTitle,
      albumName: track.albumName,
      durationMs: track.durationMs,
      trackYear: track.trackYear,
      originalReleaseDate: track.originalReleaseDate,
    };
    if (includeExisting) {
      payload.bpm = track.bpm;
      payload.musicalKey = track.musicalKey;
      payload.camelotKey = track.camelotKey;
      payload.energy = track.energy;
      payload.danceability = track.danceability;
      payload.analysisSource = track.analysisSource;
      payload.analysisConfidence = track.analysisConfidence;
    }
    return payload;
  });

  return {
    version: 1,
    source: 'curatorr-track-feature-export',
    generatedAt,
    missingOnly,
    includeExisting,
    trackCount: payloadTracks.length,
    tracks: payloadTracks,
  };
}

export function parseAnalyzerFeatureInput(raw, format = 'auto') {
  const selected = normalizeLower(format || 'auto');
  if (selected === 'csv') return parseCsvRecords(raw).map(normalizeAnalyzerRecord).filter(Boolean);
  const text = String(raw || '').trim();
  if (!text) return [];
  if (selected === 'json' || (selected === 'auto' && /^[\[{]/.test(text))) {
    const parsed = JSON.parse(text);
    const rows = Array.isArray(parsed)
      ? parsed
      : (Array.isArray(parsed?.tracks) ? parsed.tracks : Object.values(parsed || {}));
    return rows.map(normalizeAnalyzerRecord).filter(Boolean);
  }
  return parseCsvRecords(text).map(normalizeAnalyzerRecord).filter(Boolean);
}

export function mergeAnalyzerResultsIntoManifest(manifestPayload, analyzerRows, options = {}) {
  const manifest = normalizeManifestPayload(manifestPayload);
  const overwriteExisting = options.overwriteExisting === true;
  const records = (Array.isArray(analyzerRows) ? analyzerRows : []).map(normalizeAnalyzerRecord).filter(Boolean);

  const byRatingKey = new Map(records.filter((row) => row.ratingKey).map((row) => [row.ratingKey, row]));
  const byRecordingMbid = new Map(records.filter((row) => row.recordingMbid).map((row) => [row.recordingMbid, row]));
  const byFilePath = new Map(records.filter((row) => row.filePath).map((row) => [row.filePath, row]));
  const byArtistTitle = new Map(records
    .filter((row) => row.artistName && row.trackTitle)
    .map((row) => [`${normalizeLower(row.artistName)}::${normalizeLower(row.trackTitle)}`, row]));

  let matched = 0;
  let unchanged = 0;
  const mergedTracks = manifest.tracks.map((track) => {
    const match = byRatingKey.get(track.ratingKey)
      || byRecordingMbid.get(track.recordingMbid)
      || byFilePath.get(track.filePath)
      || byArtistTitle.get(`${normalizeLower(track.artistName)}::${normalizeLower(track.trackTitle)}`);
    if (!match) {
      unchanged += 1;
      return track;
    }
    matched += 1;
    const next = { ...track };
    for (const key of FEATURE_KEYS) {
      const incoming = match[key];
      const current = next[key];
      if (incoming == null || incoming === '') continue;
      if (!overwriteExisting && current != null && current !== '') continue;
      next[key] = incoming;
    }
    if (match.analysisSource) next.analysisSource = match.analysisSource;
    if (match.analysisConfidence != null) next.analysisConfidence = match.analysisConfidence;
    return next;
  });

  return {
    ...manifest,
    trackCount: mergedTracks.length,
    tracks: mergedTracks,
    mergeSummary: {
      analyzerRows: records.length,
      matched,
      unchanged,
      overwriteExisting,
    },
  };
}
