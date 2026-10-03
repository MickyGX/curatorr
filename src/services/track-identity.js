// Resolves a stored track identity (title, artist, album, duration, file path, MBID) to a track
// in the current library. Used to re-point playlists when their media-server ids change, and to
// rebuild playlists from a Curatorr backup on a different server.

import { buildM3uPathLookups, pickM3uPathMatch } from './m3u-import.js';
import {
  buildSpotifyTrackLookups,
  normalizeImportMatchText,
  pickSpotifyTrackMatch,
} from './import-matching.js';

// The library cache hands out one array until it is invalidated, so lookups built from it can be
// reused across playlist syncs.
const lookupCache = new WeakMap();

export function buildTrackIdentityLookups(masterTracks) {
  const tracks = Array.isArray(masterTracks) ? masterTracks : [];
  const cached = lookupCache.get(tracks);
  if (cached) return cached;
  const byMbid = new Map();
  for (const track of tracks) {
    const ratingKey = String(track?.ratingKey || '').trim();
    const mbid = String(track?.recordingMbid || '').trim().toLowerCase();
    if (!ratingKey || !mbid) continue;
    if (!byMbid.has(mbid)) byMbid.set(mbid, []);
    byMbid.get(mbid).push(track);
  }
  const lookups = {
    byMbid,
    byKey: new Map(tracks.map((track) => [String(track?.ratingKey || '').trim(), track])),
    paths: buildM3uPathLookups(tracks),
    text: buildSpotifyTrackLookups(tracks),
  };
  lookupCache.set(tracks, lookups);
  return lookups;
}

// Matchers return trimmed entries; the full library track carries the file path and MBID.
function toMatch(lookups, track) {
  const ratingKey = String(track?.ratingKey || '').trim();
  const full = lookups.byKey.get(ratingKey) || track;
  return {
    ratingKey,
    artistName: String(full?.artistName || '').trim(),
    trackTitle: String(full?.trackTitle || '').trim(),
    albumName: String(full?.albumName || '').trim(),
    durationMs: Number(full?.durationMs || 0),
    filePath: String(full?.filePath || '').trim(),
    recordingMbid: String(full?.recordingMbid || '').trim(),
  };
}

function titleAgrees(match, title) {
  const wanted = normalizeImportMatchText(title);
  return !wanted || normalizeImportMatchText(match?.trackTitle) === wanted;
}

function closestByDuration(candidates, durationMs) {
  if (!(durationMs > 0) || candidates.length < 2) return candidates[0] || null;
  return [...candidates].sort((a, b) => (
    Math.abs(Number(a.durationMs || 0) - durationMs) - Math.abs(Number(b.durationMs || 0) - durationMs)
  ))[0];
}

// identity: { title, artistName, albumName, durationMs, filePath, recordingMbid }
export function resolveTrackIdentity(lookups, identity = {}) {
  const title = String(identity.title || identity.trackTitle || '').trim();
  const artistName = String(identity.artistName || identity.artist || '').trim();
  const albumName = String(identity.albumName || identity.album || '').trim();
  const durationMs = Number(identity.durationMs || 0);
  const filePath = String(identity.filePath || identity.path || '').trim();
  const mbid = String(identity.recordingMbid || identity.mbid || '').trim().toLowerCase();

  let filenameMatch = null;
  if (filePath) {
    const byPath = pickM3uPathMatch(lookups.paths, { filePath });
    if (byPath.method === 'path' && byPath.match) return { method: 'path', match: toMatch(lookups, byPath.match) };
    if (byPath.method === 'filename' && byPath.match) filenameMatch = byPath.match;
  }

  if (mbid) {
    const candidates = lookups.byMbid.get(mbid) || [];
    const albumKey = normalizeImportMatchText(albumName);
    const sameAlbum = albumKey
      ? candidates.filter((track) => normalizeImportMatchText(track.albumName) === albumKey)
      : [];
    const best = closestByDuration(sameAlbum.length ? sameAlbum : candidates, durationMs);
    if (best) return { method: 'mbid', match: toMatch(lookups, best) };
  }

  if (filenameMatch && titleAgrees(filenameMatch, title)) {
    return { method: 'filename', match: toMatch(lookups, filenameMatch) };
  }

  if (!title) return { method: 'unmatched', match: null };
  const picked = pickSpotifyTrackMatch(lookups.text, {
    title,
    artists: artistName ? [{ name: artistName }] : [],
    durationMs,
  });
  if (!picked.match) return { method: 'unmatched', match: null };
  // Several library copies (album, compilation, single) can share artist and title: prefer the
  // one from the same album before falling back to the matcher's duration-based choice.
  const albumKey = normalizeImportMatchText(albumName);
  if (albumKey && picked.candidates?.length > 1) {
    const sameAlbum = picked.candidates.filter((track) => normalizeImportMatchText(track.albumName) === albumKey);
    const best = closestByDuration(sameAlbum, durationMs);
    if (best) return { method: picked.method, match: toMatch(lookups, best) };
  }
  return { method: picked.method, match: toMatch(lookups, picked.match) };
}
