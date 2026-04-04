function normalizeArtistName(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function normalizeAlbumStrictTitle(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function normalizeAlbumVariantTitle(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\([^)]*\)/g, ' ')
    .replace(/\[[^\]]*\]/g, ' ')
    .replace(/\b(deluxe|expanded|complete|collector s|collector|special|anniversary|bonus|edition|versions?|version|remaster(?:ed)?|reissue|mono|stereo)\b/g, ' ')
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function listArtistLibraryAlbums(db, artistName) {
  const artistKey = normalizeArtistName(artistName);
  if (!db || !artistKey) return [];
  return db.prepare(`
    SELECT album_name, COUNT(*) AS track_count
    FROM master_tracks
    WHERE LOWER(TRIM(artist_name)) = ?
      AND TRIM(album_name) != ''
    GROUP BY album_name
    ORDER BY COUNT(*) DESC, album_name ASC
  `).all(artistKey).map((row) => ({
    albumTitle: String(row?.album_name || '').trim(),
    trackCount: Number(row?.track_count || 0),
  }));
}

export function resolveLibraryAlbumMatch(db, options = {}) {
  const artistName = String(options?.artistName || '').trim();
  const requestedTitles = [
    options?.albumTitle,
    ...(Array.isArray(options?.alternateTitles) ? options.alternateTitles : []),
  ]
    .map((value) => String(value || '').trim())
    .filter(Boolean);
  const albums = listArtistLibraryAlbums(db, artistName);
  const exactTitles = new Set(requestedTitles.map((title) => normalizeAlbumStrictTitle(title)).filter(Boolean));
  const variantTitles = new Set(requestedTitles.map((title) => normalizeAlbumVariantTitle(title)).filter(Boolean));

  const exact = albums.find((album) => exactTitles.has(normalizeAlbumStrictTitle(album.albumTitle)));
  if (exact) {
    return {
      inLibrary: true,
      kind: 'exact',
      matchedAlbumTitle: exact.albumTitle,
      requestedAlbumTitle: requestedTitles[0] || '',
      artistAlbumCount: albums.length,
      artistTrackCount: albums.reduce((sum, album) => sum + Number(album.trackCount || 0), 0),
    };
  }

  const variant = albums.find((album) => variantTitles.has(normalizeAlbumVariantTitle(album.albumTitle)));
  if (variant) {
    return {
      inLibrary: true,
      kind: 'variant',
      matchedAlbumTitle: variant.albumTitle,
      requestedAlbumTitle: requestedTitles[0] || '',
      artistAlbumCount: albums.length,
      artistTrackCount: albums.reduce((sum, album) => sum + Number(album.trackCount || 0), 0),
    };
  }

  return {
    inLibrary: false,
    kind: albums.length ? 'artist_only' : 'not_found',
    matchedAlbumTitle: '',
    requestedAlbumTitle: requestedTitles[0] || '',
    artistAlbumCount: albums.length,
    artistTrackCount: albums.reduce((sum, album) => sum + Number(album.trackCount || 0), 0),
  };
}

export async function promoteCompletedRequestsFromLidarr(requests = [], lidarrService = null) {
  if (!lidarrService?.isConfigured?.()) return Array.isArray(requests) ? requests : [];
  const items = Array.isArray(requests) ? requests : [];
  const cache = new Map();
  return Promise.all(items.map(async (request) => {
    const status = String(request?.status || '').trim().toLowerCase();
    const albumId = Number(request?.lidarrAlbumId || 0) || 0;
    if (request?.inLibrary || status !== 'completed' || !albumId) return request;

    if (!cache.has(albumId)) {
      cache.set(albumId, lidarrService.getAlbum(albumId, { timeoutMs: 12000 }).catch(() => null));
    }
    const album = await cache.get(albumId);
    const trackFileCount = Number(album?.statistics?.trackFileCount || album?.trackFileCount || 0) || 0;
    if (trackFileCount <= 0) return request;

    return {
      ...request,
      inLibrary: true,
      inLibraryKind: 'lidarr_downloaded',
      matchedAlbumTitle: String(album?.title || request?.matchedAlbumTitle || '').trim(),
    };
  }));
}
