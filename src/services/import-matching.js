// Text matching of imported playlist items (Spotify, Last.fm, ListenBrainz, M3U #EXTINF, ...)
// against the master track cache.

// Source types for custom playlists imported from an external source. These can
// be refreshed from their source and edited through the imported-playlist modal.
export const IMPORTED_PLAYLIST_SOURCE_TYPES = Object.freeze([
  'spotify-playlist',
  'tidal-playlist',
  'youtube-playlist',
  'plex-playlist',
  'plex-collection',
  'lastfm-station',
  'listenbrainz-playlist',
  'm3u-file',
  'curatorr-backup',
]);

export function isImportedPlaylistSourceType(sourceType) {
  return IMPORTED_PLAYLIST_SOURCE_TYPES.includes(String(sourceType || '').trim().toLowerCase());
}

const ARTIST_CREDIT_SPLIT_RE = /\s*(?:,|;|\/|&|\+|\bfeat\.?\s|\bft\.?\s|\bfeaturing\b|\bvs\.?\s)\s*/i;

export function normalizeImportMatchText(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\([^)]*\)/g, ' ')
    .replace(/\[[^\]]*\]/g, ' ')
    .replace(/\b(feat|featuring|ft)\.? .+$/i, ' ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeImportArtistKey(value) {
  return normalizeImportMatchText(String(value || '').replace(/[&+]/g, ' and '))
    .replace(/^the /, '');
}

// All the ways an artist credit can be compared: the full credit ("Simon & Garfunkel" and
// "Simon and Garfunkel" agree; a leading "The" is ignored) plus each individual artist in a
// joint credit, so "Jacob Collier, Shawn Mendes" agrees with a source that lists either artist.
export function buildImportArtistKeys(names) {
  const keys = new Set();
  (Array.isArray(names) ? names : [names]).forEach((name) => {
    const raw = String(name || '').trim();
    if (!raw) return;
    const full = normalizeImportArtistKey(raw);
    if (full) keys.add(full);
    raw.split(ARTIST_CREDIT_SPLIT_RE).forEach((part) => {
      const key = normalizeImportArtistKey(part);
      if (key) keys.add(key);
    });
  });
  return keys;
}

function sharesArtistKey(left, right) {
  for (const key of left) {
    if (right.has(key)) return true;
  }
  return false;
}

export function buildSpotifyTrackLookups(masterTracks) {
  const byArtistTitle = new Map();
  const byTitle = new Map();
  for (const track of Array.isArray(masterTracks) ? masterTracks : []) {
    const ratingKey = String(track?.ratingKey || '').trim();
    if (!ratingKey) continue;
    const artistKey = normalizeImportMatchText(track?.artistName);
    const titleKey = normalizeImportMatchText(track?.trackTitle);
    if (!titleKey) continue;
    const artistTitleKey = `${artistKey}::${titleKey}`;
    const entry = {
      ratingKey,
      artistName: String(track?.artistName || '').trim(),
      trackTitle: String(track?.trackTitle || '').trim(),
      albumName: String(track?.albumName || '').trim(),
      durationMs: Number(track?.durationMs || 0),
    };
    if (!byArtistTitle.has(artistTitleKey)) byArtistTitle.set(artistTitleKey, []);
    byArtistTitle.get(artistTitleKey).push(entry);
    if (!byTitle.has(titleKey)) byTitle.set(titleKey, []);
    byTitle.get(titleKey).push(entry);
  }
  return { byArtistTitle, byTitle };
}

// When the source names an artist, a library track must share that artist to match: a
// same-titled song by someone else (usually a cover) is left unmatched. Title-only matching is
// kept for sources that carry no artist at all, such as bare M3U file entries.
export function pickSpotifyTrackMatch(trackLookups, spotifyItem) {
  const artistNames = (Array.isArray(spotifyItem?.artists) ? spotifyItem.artists : [])
    .map((artist) => String(artist?.name || '').trim())
    .filter(Boolean);
  const titleKey = normalizeImportMatchText(spotifyItem?.title);
  const primaryArtistKeys = buildImportArtistKeys(artistNames[0] || '');
  const itemArtistKeys = buildImportArtistKeys(artistNames);
  const durationMs = Number(spotifyItem?.durationMs || 0);
  if (!titleKey) return { method: 'unmatched', match: null, candidates: [] };

  const exactCandidates = [];
  const seenExact = new Set();
  artistNames.forEach((name) => {
    const key = `${normalizeImportMatchText(name)}::${titleKey}`;
    (trackLookups.byArtistTitle.get(key) || []).forEach((entry) => {
      if (seenExact.has(entry.ratingKey)) return;
      seenExact.add(entry.ratingKey);
      exactCandidates.push(entry);
    });
  });
  const titleCandidates = trackLookups.byTitle.get(titleKey) || [];

  let method = 'title';
  let candidates = titleCandidates;
  if (exactCandidates.length) {
    method = 'artistTitle';
    candidates = exactCandidates;
  } else if (itemArtistKeys.size) {
    method = 'artistCredit';
    candidates = titleCandidates.filter((entry) => (
      sharesArtistKey(itemArtistKeys, buildImportArtistKeys(entry.artistName))
    ));
  }
  if (!candidates.length) return { method: 'unmatched', match: null, candidates: [] };

  let best = null;
  let bestScore = -Infinity;
  candidates.forEach((candidate) => {
    let score = 0;
    const candidateArtistKeys = buildImportArtistKeys(candidate.artistName);
    if (sharesArtistKey(primaryArtistKeys, candidateArtistKeys)) score += 100;
    else if (sharesArtistKey(itemArtistKeys, candidateArtistKeys)) score += 60;
    if (normalizeImportMatchText(candidate.trackTitle) === titleKey) score += 100;
    if (durationMs > 0 && Number(candidate.durationMs || 0) > 0) {
      const durationDelta = Math.abs(Number(candidate.durationMs || 0) - durationMs);
      if (durationDelta <= 1500) score += 40;
      else if (durationDelta <= 4000) score += 24;
      else if (durationDelta <= 8000) score += 8;
      else score -= Math.min(30, Math.floor(durationDelta / 1000));
    }
    if (score > bestScore) {
      best = candidate;
      bestScore = score;
    }
  });
  if (!best) return { method: 'unmatched', match: null, candidates };
  return { method, match: best, candidates };
}
