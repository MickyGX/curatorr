import path from 'node:path';

const EXTINF_RE = /^#EXTINF\s*:\s*([^,]*),(.*)$/i;
const AUDIO_EXT_RE = /\.(?:aac|aif|aiff|alac|flac|m4a|mp3|ogg|opus|wav|wma)$/i;

export function normalizeM3uPath(value) {
  let raw = String(value || '').trim();
  if (!raw) return '';
  raw = raw.replace(/^file:\/+/i, '/');
  try { raw = decodeURIComponent(raw); } catch { /* keep original */ }
  raw = raw.replace(/\\/g, '/').replace(/\/+/g, '/').trim();
  return raw.toLowerCase();
}

function parseExtinfPayload(payload) {
  const raw = String(payload || '').trim();
  const split = raw.match(/^(.+?)\s+-\s+(.+)$/);
  if (!split) return { artistName: '', title: raw };
  return {
    artistName: String(split[1] || '').trim(),
    title: String(split[2] || '').trim(),
  };
}

function parseDurationMs(value) {
  const seconds = Number(String(value || '').trim());
  return Number.isFinite(seconds) && seconds > 0 ? Math.round(seconds * 1000) : 0;
}

export function parseM3uPlaylist(content, options = {}) {
  const maxEntries = Math.max(1, Number(options.maxEntries || 5000));
  const lines = String(content || '').replace(/^\uFEFF/, '').split(/\r?\n/);
  const entries = [];
  let pendingExtinf = null;

  for (const line of lines) {
    const raw = String(line || '').trim();
    if (!raw) continue;
    const extinfMatch = raw.match(EXTINF_RE);
    if (extinfMatch) {
      pendingExtinf = {
        durationMs: parseDurationMs(extinfMatch[1]),
        ...parseExtinfPayload(extinfMatch[2]),
      };
      continue;
    }
    if (raw.startsWith('#')) continue;

    const filePath = raw;
    const basename = path.basename(filePath.replace(/\\/g, '/')).replace(AUDIO_EXT_RE, '').trim();
    entries.push({
      id: `m3u:${entries.length + 1}`,
      position: entries.length + 1,
      filePath,
      title: pendingExtinf?.title || basename,
      artistName: pendingExtinf?.artistName || '',
      artists: pendingExtinf?.artistName ? [{ name: pendingExtinf.artistName }] : [],
      album: { title: '' },
      durationMs: Number(pendingExtinf?.durationMs || 0),
    });
    pendingExtinf = null;
    if (entries.length >= maxEntries) break;
  }

  return entries;
}

export function buildM3uPathLookups(masterTracks = []) {
  const byPath = new Map();
  const byBasename = new Map();
  for (const track of Array.isArray(masterTracks) ? masterTracks : []) {
    const ratingKey = String(track?.ratingKey || '').trim();
    const filePath = String(track?.filePath || '').trim();
    if (!ratingKey || !filePath) continue;
    const entry = {
      ratingKey,
      artistName: String(track?.artistName || '').trim(),
      trackTitle: String(track?.trackTitle || '').trim(),
      albumName: String(track?.albumName || '').trim(),
      durationMs: Number(track?.durationMs || 0),
      filePath,
    };
    const normalized = normalizeM3uPath(filePath);
    if (normalized && !byPath.has(normalized)) byPath.set(normalized, entry);
    const base = path.basename(normalized);
    if (!base) continue;
    if (!byBasename.has(base)) byBasename.set(base, []);
    byBasename.get(base).push(entry);
  }
  return { byPath, byBasename };
}

export function pickM3uPathMatch(pathLookups, item) {
  const normalized = normalizeM3uPath(item?.filePath || '');
  if (!normalized) return { method: 'unmatched', match: null };
  const exact = pathLookups?.byPath?.get(normalized);
  if (exact) return { method: 'path', match: exact };
  const basename = path.basename(normalized);
  const basenameMatches = pathLookups?.byBasename?.get(basename) || [];
  if (basenameMatches.length === 1) return { method: 'filename', match: basenameMatches[0] };
  return { method: 'unmatched', match: null, candidates: basenameMatches };
}
