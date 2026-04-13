function parsePositiveNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeScopeList(scopes = []) {
  return [...new Set((Array.isArray(scopes) ? scopes : []).map((scope) => String(scope || '').trim()).filter(Boolean))];
}

function normalizeBaseUrl(value) {
  const raw = String(value || '').trim().replace(/\/+$/, '');
  if (!raw) return '';
  return raw;
}

function getSpotifyPlaylistEntryMedia(item) {
  return item?.track || item?.item || null;
}

function parseSpotifyUriId(value, kind) {
  const match = String(value || '').trim().match(new RegExp(`^spotify:${kind}:([A-Za-z0-9]+)$`, 'i'));
  return match ? String(match[1] || '').trim() : '';
}

export function parseSpotifyEmbedPlaylistPage(html, playlistId) {
  const body = String(html || '');
  const id = String(playlistId || '').trim();
  if (!body || !id) return null;
  const scriptMatch = body.match(/<script id="__NEXT_DATA__"[^>]*>([^<]+)<\/script>/i);
  if (!scriptMatch) return null;
  let data = null;
  try {
    data = JSON.parse(String(scriptMatch[1] || '').trim());
  } catch (_err) {
    data = null;
  }
  const entity = data?.props?.pageProps?.state?.data?.entity;
  if (!entity) return null;
  const rawTracks = Array.isArray(entity.trackList) ? entity.trackList : [];
  const items = rawTracks.map((track, index) => {
    const trackUri = String(track?.uri || '').trim();
    const trackId = String(track?.id || parseSpotifyUriId(trackUri, 'track') || '').trim();
    const artistNames = String(track?.subtitle || '').trim().split(',').map((s) => s.trim()).filter(Boolean);
    return {
      position: index,
      addedAt: '',
      isLocal: false,
      trackId,
      title: String(track?.title || '').trim(),
      durationMs: Number(track?.duration || 0),
      explicit: false,
      popularity: 0,
      previewUrl: '',
      externalUrl: trackId ? `https://open.spotify.com/track/${encodeURIComponent(trackId)}` : '',
      isrc: '',
      artists: artistNames.map((name) => ({ id: '', name })),
      album: {
        id: '',
        title: '',
        albumType: '',
        releaseDate: '',
        imageUrl: '',
        externalUrl: '',
      },
    };
  }).filter((item) => item.title);
  const totalCount = Math.max(items.length, Number(entity.totalCount || entity.trackCount || 0));
  const ownerUrl = String(entity.author_url || '').trim();
  const ownerIdMatch = ownerUrl.match(/\/user\/([A-Za-z0-9_]+)/i);
  const ownerId = ownerIdMatch ? ownerIdMatch[1] : '';
  const imageUrl = String(entity.cover_url || '').trim();
  const partial = totalCount > items.length;
  const warning = partial
    ? `Spotify only exposed the first ${items.length} of ${totalCount} tracks for this shared playlist URL. Add or copy it in Spotify and import it from your Library for the full set.`
    : '';
  return {
    playlist: {
      id,
      name: String(entity.name || entity.title || '').trim(),
      description: String(entity.description || entity.playlist_description || '').trim(),
      ownerId,
      ownerName: String(entity.author || '').trim(),
      public: true,
      collaborative: false,
      trackCount: totalCount,
      snapshotId: '',
      imageUrl,
      externalUrl: `https://open.spotify.com/playlist/${encodeURIComponent(id)}`,
    },
    items,
    total: items.length,
    totalCount,
    partial,
    warning,
    nextOffset: 0,
  };
}

export function parseSpotifyPublicPlaylistPage(html, playlistId) {
  const body = String(html || '');
  const id = String(playlistId || '').trim();
  if (!body || !id) return null;
  const stateMatch = body.match(/<script id="initialState" type="text\/plain">([^<]+)<\/script>/i);
  if (!stateMatch) return null;
  let state = null;
  try {
    state = JSON.parse(Buffer.from(String(stateMatch[1] || '').trim(), 'base64').toString('utf8'));
  } catch (_err) {
    state = null;
  }
  const playlist = state?.entities?.items?.[`spotify:playlist:${id}`];
  const page = playlist?.content;
  const rawItems = Array.isArray(page?.items) ? page.items : [];
  const items = rawItems.map((entry, index) => {
    const track = entry?.itemV2?.data;
    const trackUri = String(track?.uri || '').trim();
    const albumUri = String(track?.albumOfTrack?.uri || '').trim();
    const trackId = parseSpotifyUriId(trackUri, 'track');
    const albumId = parseSpotifyUriId(albumUri, 'album');
    return {
      position: index,
      addedAt: '',
      isLocal: false,
      trackId,
      title: String(track?.name || '').trim(),
      durationMs: Number(track?.duration?.totalMilliseconds || 0),
      explicit: String(track?.contentRating?.label || '').trim().toUpperCase() === 'EXPLICIT',
      popularity: 0,
      previewUrl: String(track?.previews?.audioPreviews?.items?.[0]?.url || '').trim(),
      externalUrl: trackId ? `https://open.spotify.com/track/${encodeURIComponent(trackId)}` : '',
      isrc: '',
      artists: (Array.isArray(track?.artists?.items) ? track.artists.items : []).map((artist) => ({
        id: parseSpotifyUriId(artist?.uri, 'artist'),
        name: String(artist?.profile?.name || '').trim(),
      })).filter((artist) => artist.name),
      album: {
        id: albumId,
        title: String(track?.albumOfTrack?.name || '').trim(),
        albumType: '',
        releaseDate: '',
        imageUrl: String(track?.albumOfTrack?.coverArt?.sources?.[0]?.url || '').trim(),
        externalUrl: albumId ? `https://open.spotify.com/album/${encodeURIComponent(albumId)}` : '',
      },
    };
  }).filter((item) => item.title);
  const totalCount = Math.max(items.length, Number(page?.totalCount || 0));
  const owner = playlist?.ownerV2?.data || {};
  const partial = totalCount > items.length;
  const warning = partial
    ? `Spotify only exposed the first ${items.length} of ${totalCount} tracks for this shared playlist URL. Add or copy it in Spotify and import it from your Library for the full set.`
    : '';
  return {
    playlist: {
      id,
      name: String(playlist?.name || '').trim(),
      description: String(playlist?.description || '').trim(),
      ownerId: parseSpotifyUriId(owner?.uri, 'user') || String(owner?.username || '').trim(),
      ownerName: String(owner?.name || owner?.username || '').trim(),
      public: true,
      collaborative: false,
      trackCount: totalCount,
      snapshotId: '',
      imageUrl: String(playlist?.images?.items?.[0]?.sources?.[0]?.url || '').trim(),
      externalUrl: `https://open.spotify.com/playlist/${encodeURIComponent(id)}`,
    },
    items,
    total: items.length,
    totalCount,
    partial,
    warning,
    nextOffset: Number(page?.pagingInfo?.nextOffset || 0),
  };
}

export function parseSpotifyPlaylistReference(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;

  const uriMatch = raw.match(/^spotify:playlist:([A-Za-z0-9]+)$/i);
  if (uriMatch) {
    return {
      id: String(uriMatch[1] || '').trim(),
      kind: 'uri',
      raw,
    };
  }

  if (/^[A-Za-z0-9]{10,}$/i.test(raw)) {
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

  const hostname = String(parsedUrl.hostname || '').trim().toLowerCase();
  if (!hostname.endsWith('spotify.com')) return null;
  const match = String(parsedUrl.pathname || '').match(/\/playlist\/([A-Za-z0-9]+)/i);
  if (!match) return null;
  return {
    id: String(match[1] || '').trim(),
    kind: 'url',
    raw,
  };
}

export function createSpotifyService(ctx = {}) {
  const clientId = String(process.env.SPOTIFY_CLIENT_ID || '').trim();
  const clientSecret = String(process.env.SPOTIFY_CLIENT_SECRET || '').trim();
  const requestTimeoutMs = Math.max(5000, parsePositiveNumber(process.env.SPOTIFY_TIMEOUT_MS, 15000));
  const defaultScopes = normalizeScopeList([
    'playlist-read-private',
    'playlist-read-collaborative',
  ]);
  const pushLog = typeof ctx.pushLog === 'function' ? ctx.pushLog : null;

  function log(level, action, message, meta = null) {
    if (!pushLog) return;
    pushLog({
      level,
      app: 'spotify',
      action,
      message,
      meta,
    });
  }

  function isConfigured() {
    return Boolean(clientId && clientSecret);
  }

  function buildRedirectUri(baseUrl) {
    const normalized = normalizeBaseUrl(baseUrl);
    if (!normalized) throw new Error('Spotify redirect base URL is not configured.');
    return `${normalized}/user-settings/spotify/callback`;
  }

  function getAuthorizationUrl({ baseUrl, state, scopes = defaultScopes, showDialog = false } = {}) {
    if (!isConfigured()) throw new Error('Spotify integration is not configured.');
    const redirectUri = buildRedirectUri(baseUrl);
    const url = new URL('https://accounts.spotify.com/authorize');
    url.searchParams.set('client_id', clientId);
    url.searchParams.set('response_type', 'code');
    url.searchParams.set('redirect_uri', redirectUri);
    url.searchParams.set('scope', normalizeScopeList(scopes).join(' '));
    if (state) url.searchParams.set('state', String(state));
    if (showDialog) url.searchParams.set('show_dialog', 'true');
    return { url: url.toString(), redirectUri };
  }

  async function fetchForm(url, body) {
    const response = await fetch(String(url), {
      method: 'POST',
      headers: {
        Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams(body),
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
      const err = new Error(String(payload?.error_description || payload?.error || text || `Spotify HTTP ${response.status}`).trim());
      err.status = response.status;
      err.payload = payload;
      throw err;
    }
    return payload || {};
  }

  async function fetchJson(url, accessToken) {
    const response = await fetch(String(url), {
      headers: {
        Authorization: `Bearer ${String(accessToken || '').trim()}`,
      },
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
      const err = new Error(String(payload?.error?.message || payload?.error_description || text || `Spotify HTTP ${response.status}`).trim());
      err.status = response.status;
      err.payload = payload;
      throw err;
    }
    return payload || {};
  }

  async function fetchText(url) {
    const response = await fetch(String(url), {
      headers: {
        'User-Agent': 'Mozilla/5.0',
      },
      signal: AbortSignal.timeout(requestTimeoutMs),
    });
    const text = await response.text();
    if (!response.ok) {
      const err = new Error(String(text || `Spotify HTTP ${response.status}`).trim());
      err.status = response.status;
      throw err;
    }
    return text;
  }

  function normalizeTokenPayload(payload = {}, fallbackRefreshToken = '') {
    return {
      accessToken: String(payload.access_token || '').trim(),
      refreshToken: String(payload.refresh_token || fallbackRefreshToken || '').trim(),
      tokenType: String(payload.token_type || 'Bearer').trim(),
      scope: String(payload.scope || '').trim(),
      expiresAt: Date.now() + (Math.max(1, parsePositiveNumber(payload.expires_in, 3600)) * 1000),
    };
  }

  async function getClientCredentialsToken() {
    if (!isConfigured()) throw new Error('Spotify integration is not configured.');
    const payload = await fetchForm('https://accounts.spotify.com/api/token', {
      grant_type: 'client_credentials',
    });
    return String(payload.access_token || '').trim();
  }

  async function exchangeCode({ code, redirectUri } = {}) {
    if (!isConfigured()) throw new Error('Spotify integration is not configured.');
    const payload = await fetchForm('https://accounts.spotify.com/api/token', {
      grant_type: 'authorization_code',
      code: String(code || '').trim(),
      redirect_uri: String(redirectUri || '').trim(),
    });
    return normalizeTokenPayload(payload);
  }

  async function refreshAccessToken(refreshToken) {
    if (!isConfigured()) throw new Error('Spotify integration is not configured.');
    const payload = await fetchForm('https://accounts.spotify.com/api/token', {
      grant_type: 'refresh_token',
      refresh_token: String(refreshToken || '').trim(),
    });
    return normalizeTokenPayload(payload, refreshToken);
  }

  async function ensureAccessToken(auth = {}) {
    const accessToken = String(auth.accessToken || '').trim();
    const refreshToken = String(auth.refreshToken || '').trim();
    const expiresAt = Number(auth.expiresAt || 0);
    if (accessToken && expiresAt > (Date.now() + 60 * 1000)) return {
      accessToken,
      refreshToken,
      expiresAt,
      refreshed: false,
    };
    if (!refreshToken) {
      const err = new Error('Spotify connection is missing a refresh token.');
      err.code = 'SPOTIFY_REFRESH_TOKEN_MISSING';
      throw err;
    }
    const refreshed = await refreshAccessToken(refreshToken);
    return {
      accessToken: refreshed.accessToken,
      refreshToken: refreshed.refreshToken,
      expiresAt: refreshed.expiresAt,
      refreshed: true,
    };
  }

  async function getCurrentUserProfile(accessToken) {
    return fetchJson('https://api.spotify.com/v1/me', accessToken);
  }

  async function listCurrentUserPlaylists(accessToken, { limit = 50 } = {}) {
    const pageSize = Math.max(1, Math.min(50, parsePositiveNumber(limit, 50)));
    let nextUrl = new URL('https://api.spotify.com/v1/me/playlists');
    nextUrl.searchParams.set('limit', String(pageSize));
    const items = [];
    while (nextUrl) {
      const payload = await fetchJson(nextUrl.toString(), accessToken);
      items.push(...(Array.isArray(payload.items) ? payload.items : []));
      nextUrl = payload.next ? new URL(String(payload.next)) : null;
    }
    return items.map((item) => ({
      id: String(item?.id || '').trim(),
      name: String(item?.name || '').trim(),
      description: String(item?.description || '').trim(),
      ownerId: String(item?.owner?.id || '').trim(),
      ownerName: String(item?.owner?.display_name || item?.owner?.id || '').trim(),
      public: item?.public === true,
      collaborative: item?.collaborative === true,
      trackCount: Number(item?.items?.total || item?.tracks?.total || 0),
      snapshotId: String(item?.snapshot_id || '').trim(),
      imageUrl: Array.isArray(item?.images) && item.images[0]?.url ? String(item.images[0].url) : '',
      externalUrl: String(item?.external_urls?.spotify || '').trim(),
    })).filter((item) => item.id);
  }

  async function getPlaylist(accessToken, playlistId) {
    const id = String(playlistId || '').trim();
    if (!id) throw new Error('playlistId is required.');
    const url = new URL(`https://api.spotify.com/v1/playlists/${encodeURIComponent(id)}`);
    url.searchParams.set('fields', [
      'id',
      'name',
      'description',
      'snapshot_id',
      'public',
      'collaborative',
      'owner(id,display_name)',
      'images(url)',
      'external_urls(spotify)',
      'tracks(total)',
    ].join(','));
    const item = await fetchJson(url.toString(), accessToken);
    return {
      id: String(item?.id || '').trim(),
      name: String(item?.name || '').trim(),
      description: String(item?.description || '').trim(),
      ownerId: String(item?.owner?.id || '').trim(),
      ownerName: String(item?.owner?.display_name || item?.owner?.id || '').trim(),
      public: item?.public === true,
      collaborative: item?.collaborative === true,
      trackCount: Number(item?.items?.total || item?.tracks?.total || 0),
      snapshotId: String(item?.snapshot_id || '').trim(),
      imageUrl: Array.isArray(item?.images) && item.images[0]?.url ? String(item.images[0].url) : '',
      externalUrl: String(item?.external_urls?.spotify || '').trim(),
    };
  }

  async function getPlaylistItems(accessToken, playlistId, { limit = 25 } = {}) {
    const id = String(playlistId || '').trim();
    if (!id) throw new Error('playlistId is required.');
    const pageSize = Math.max(1, Math.min(100, parsePositiveNumber(limit, 25)));
    const playlistFields = [
      'items(',
      'total,limit,offset,next,',
      'items(',
      'added_at,is_local,',
      'item(',
      'id,name,duration_ms,explicit,popularity,preview_url,external_urls(spotify),external_ids(isrc),',
      'artists(id,name),',
      'album(id,name,album_type,release_date,images(url),external_urls(spotify))',
      ')',
      ')',
      ')',
    ].join('');
    const itemFields = [
      'items(',
      'added_at,is_local,',
      'item(',
      'id,name,duration_ms,explicit,popularity,preview_url,external_urls(spotify),external_ids(isrc),',
      'artists(id,name),',
      'album(id,name,album_type,release_date,images(url),external_urls(spotify))',
      ')',
      ')',
    ].join('');
    let nextUrl = new URL(`https://api.spotify.com/v1/playlists/${encodeURIComponent(id)}`);
    nextUrl.searchParams.set('limit', String(pageSize));
    nextUrl.searchParams.set('fields', playlistFields);
    const allItems = [];
    let total = 0;
    let isFirstPage = true;
    while (nextUrl) {
      const payload = await fetchJson(nextUrl.toString(), accessToken);
      const page = isFirstPage ? payload?.items : payload;
      total = Math.max(total, Number(page?.total || 0));
      allItems.push(...(Array.isArray(page?.items) ? page.items : []));
      nextUrl = page?.next ? new URL(String(page.next)) : null;
      if (nextUrl) nextUrl.searchParams.set('fields', itemFields);
      isFirstPage = false;
    }
    return {
      total,
      items: allItems.map((item, index) => {
        const media = getSpotifyPlaylistEntryMedia(item);
        return {
          position: index,
          addedAt: String(item?.added_at || '').trim(),
          isLocal: item?.is_local === true,
          trackId: String(media?.id || '').trim(),
          title: String(media?.name || '').trim(),
          durationMs: Number(media?.duration_ms || 0),
          explicit: media?.explicit === true,
          popularity: Number(media?.popularity || 0),
          previewUrl: String(media?.preview_url || '').trim(),
          externalUrl: String(media?.external_urls?.spotify || '').trim(),
          isrc: String(media?.external_ids?.isrc || '').trim().toUpperCase(),
          artists: (Array.isArray(media?.artists) ? media.artists : []).map((artist) => ({
            id: String(artist?.id || '').trim(),
            name: String(artist?.name || '').trim(),
          })).filter((artist) => artist.name),
          album: {
            id: String(media?.album?.id || '').trim(),
            title: String(media?.album?.name || '').trim(),
            albumType: String(media?.album?.album_type || '').trim(),
            releaseDate: String(media?.album?.release_date || '').trim(),
            imageUrl: Array.isArray(media?.album?.images) && media.album.images[0]?.url ? String(media.album.images[0].url) : '',
            externalUrl: String(media?.album?.external_urls?.spotify || '').trim(),
          },
        };
      }).filter((item) => item.title),
    };
  }

  async function getPlaylistFromPublicPage(playlistId) {
    const id = String(playlistId || '').trim();
    if (!id) throw new Error('playlistId is required.');
    // Try the embed URL first — it is always publicly accessible without auth and
    // embeds the full track list in a __NEXT_DATA__ JSON script tag.
    try {
      const embedHtml = await fetchText(`https://open.spotify.com/embed/playlist/${encodeURIComponent(id)}`);
      const embedParsed = parseSpotifyEmbedPlaylistPage(embedHtml, id);
      if (embedParsed?.playlist?.id) return embedParsed;
    } catch (_embedErr) {
      // fall through to main page
    }
    // Fallback: scrape the main playlist page for the initialState blob.
    const html = await fetchText(`https://open.spotify.com/playlist/${encodeURIComponent(id)}`);
    const parsed = parseSpotifyPublicPlaylistPage(html, id);
    if (!parsed?.playlist?.id) {
      const err = new Error('Spotify did not expose public playlist data for this URL.');
      err.code = 'SPOTIFY_PUBLIC_PLAYLIST_UNAVAILABLE';
      throw err;
    }
    return parsed;
  }

  return {
    isConfigured,
    parsePlaylistReference: parseSpotifyPlaylistReference,
    parsePublicPlaylistPage: parseSpotifyPublicPlaylistPage,
    parseEmbedPlaylistPage: parseSpotifyEmbedPlaylistPage,
    buildRedirectUri,
    getAuthorizationUrl,
    getDefaultScopes: () => defaultScopes.slice(),
    getClientCredentialsToken,
    exchangeCode,
    refreshAccessToken,
    ensureAccessToken,
    getCurrentUserProfile,
    listCurrentUserPlaylists,
    getPlaylist,
    getPlaylistItems,
    getPlaylistFromPublicPage,
    log,
  };
}
