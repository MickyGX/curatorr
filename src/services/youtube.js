function parsePositiveNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function trimText(value) {
  return String(value || '').trim();
}

function buildThumbUrl(thumbnails = {}) {
  const keys = ['maxres', 'standard', 'high', 'medium', 'default'];
  for (const key of keys) {
    const url = trimText(thumbnails?.[key]?.url || '');
    if (url) return url;
  }
  return '';
}

export function parseYouTubePlaylistReference(value) {
  const raw = trimText(value);
  if (!raw) return null;

  if (/^[A-Za-z0-9_-]{10,}$/i.test(raw)) {
    return {
      id: raw,
      kind: 'id',
      raw,
    };
  }

  let parsedUrl = null;
  try {
    parsedUrl = new URL(raw);
  } catch (_err) {
    parsedUrl = null;
  }
  if (!parsedUrl) return null;

  const host = trimText(parsedUrl.hostname).toLowerCase();
  if (!(
    host.endsWith('youtube.com')
    || host.endsWith('youtube-nocookie.com')
    || host === 'youtu.be'
  )) return null;

  const listId = trimText(parsedUrl.searchParams.get('list'));
  if (!listId) return null;
  return {
    id: listId,
    kind: 'url',
    raw,
  };
}

export function createYouTubeService() {
  const apiKey = trimText(process.env.YOUTUBE_API_KEY || '');
  const requestTimeoutMs = Math.max(5000, parsePositiveNumber(process.env.YOUTUBE_TIMEOUT_MS, 15000));

  function isConfigured() {
    return Boolean(apiKey);
  }

  async function fetchJson(endpoint, params = {}) {
    if (!isConfigured()) throw new Error('YouTube integration is not configured.');
    const url = new URL(`https://www.googleapis.com/youtube/v3/${endpoint}`);
    Object.entries(params || {}).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') url.searchParams.set(key, String(value));
    });
    url.searchParams.set('key', apiKey);

    const response = await fetch(url.toString(), {
      signal: AbortSignal.timeout(requestTimeoutMs),
    });
    const text = await response.text();
    let payload = null;
    try {
      payload = text ? JSON.parse(text) : null;
    } catch (_err) {
      payload = null;
    }
    if (!response.ok) {
      const err = new Error(trimText(payload?.error?.message || text || `YouTube HTTP ${response.status}`));
      err.status = response.status;
      err.payload = payload;
      throw err;
    }
    return payload || {};
  }

  async function getPlaylist(playlistId) {
    const id = trimText(playlistId);
    if (!id) throw new Error('YouTube playlist id is required.');
    const payload = await fetchJson('playlists', {
      part: 'snippet,contentDetails',
      id,
      maxResults: 1,
    });
    const playlist = Array.isArray(payload?.items) ? payload.items[0] : null;
    if (!playlist) {
      const err = new Error('YouTube playlist not found.');
      err.status = 404;
      throw err;
    }
    const snippet = playlist?.snippet || {};
    const contentDetails = playlist?.contentDetails || {};
    return {
      id,
      name: trimText(snippet.title || ''),
      description: trimText(snippet.description || ''),
      ownerId: trimText(snippet.channelId || ''),
      ownerName: trimText(snippet.channelTitle || ''),
      public: true,
      collaborative: false,
      trackCount: Number(contentDetails.itemCount || 0),
      snapshotId: '',
      imageUrl: buildThumbUrl(snippet.thumbnails || {}),
      externalUrl: `https://www.youtube.com/playlist?list=${encodeURIComponent(id)}`,
    };
  }

  async function getPlaylistItems(playlistId, options = {}) {
    const id = trimText(playlistId);
    if (!id) throw new Error('YouTube playlist id is required.');
    const limit = Math.max(1, Math.min(500, Number(options.limit || 250)));
    let nextPageToken = trimText(options.pageToken || '');
    const items = [];
    do {
      const payload = await fetchJson('playlistItems', {
        part: 'snippet,contentDetails',
        playlistId: id,
        maxResults: Math.min(50, limit - items.length),
        pageToken: nextPageToken,
      });
      const pageItems = Array.isArray(payload?.items) ? payload.items : [];
      pageItems.forEach((entry, index) => {
        const snippet = entry?.snippet || {};
        const contentDetails = entry?.contentDetails || {};
        const videoId = trimText(snippet?.resourceId?.videoId || contentDetails?.videoId || '');
        const title = trimText(snippet.title || '');
        if (!title) return;
        const artistName = trimText(snippet.videoOwnerChannelTitle || snippet.channelTitle || '');
        items.push({
          position: Number(snippet.position || items.length + index || 0),
          addedAt: trimText(snippet.publishedAt || ''),
          isLocal: false,
          id: videoId,
          trackId: videoId,
          title,
          durationMs: 0,
          explicit: false,
          popularity: 0,
          previewUrl: '',
          externalUrl: videoId ? `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}&list=${encodeURIComponent(id)}` : '',
          isrc: '',
          artists: artistName ? [{ id: trimText(snippet.videoOwnerChannelId || snippet.channelId || ''), name: artistName }] : [],
          album: {
            id: '',
            title: '',
            albumType: '',
            releaseDate: '',
            imageUrl: buildThumbUrl(snippet.thumbnails || {}),
            externalUrl: '',
          },
        });
      });
      nextPageToken = trimText(payload?.nextPageToken || '');
    } while (nextPageToken && items.length < limit);

    return {
      total: items.length,
      items,
      next: nextPageToken,
    };
  }

  return {
    isConfigured,
    parsePlaylistReference: parseYouTubePlaylistReference,
    getPlaylist,
    getPlaylistItems,
  };
}
