import crypto from 'crypto';

// TIDAL developer API (https://developer.tidal.com). Users connect with OAuth
// authorization code + PKCE; public playlists can also be read with an app-level
// client credentials token. The API is JSON:API, so track artists/albums arrive
// as separate `included` resources that have to be joined back onto each track.

const TIDAL_AUTHORIZE_URL = 'https://login.tidal.com/authorize';
const TIDAL_TOKEN_URL = 'https://auth.tidal.com/v1/oauth2/token';
const TIDAL_API_BASE = 'https://openapi.tidal.com/v2';
const TIDAL_TRACK_BATCH_SIZE = 20;
const TIDAL_MAX_PAGES = 200;
const TIDAL_MAX_RATE_LIMIT_RETRIES = 3;
const TIDAL_PLAYLIST_ID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function parsePositiveNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeBaseUrl(value) {
  return String(value || '').trim().replace(/\/+$/, '');
}

function normalizeCountryCode(value, fallback = 'US') {
  const code = String(value || '').trim().toUpperCase();
  return /^[A-Z]{2}$/.test(code) ? code : fallback;
}

function base64Url(buffer) {
  return Buffer.from(buffer).toString('base64url');
}

export function createTidalPkcePair() {
  const codeVerifier = base64Url(crypto.randomBytes(48));
  const codeChallenge = base64Url(crypto.createHash('sha256').update(codeVerifier).digest());
  return { codeVerifier, codeChallenge };
}

// ISO 8601 durations as used by TIDAL, e.g. "PT3M25S" or "PT1H2M3.5S".
export function parseTidalDurationMs(value) {
  const match = String(value || '').trim().match(/^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/i);
  if (!match) return 0;
  const [, days, hours, minutes, seconds] = match;
  const totalSeconds = (Number(days || 0) * 86400)
    + (Number(hours || 0) * 3600)
    + (Number(minutes || 0) * 60)
    + Number(seconds || 0);
  return Math.round(totalSeconds * 1000);
}

export function parseTidalPlaylistReference(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (TIDAL_PLAYLIST_ID_RE.test(raw)) return { id: raw.toLowerCase(), kind: 'id', raw };
  try {
    const parsed = new URL(raw);
    const host = String(parsed.hostname || '').toLowerCase();
    if (host !== 'tidal.com' && !host.endsWith('.tidal.com')) return null;
    const match = String(parsed.pathname || '').match(/\/playlist\/([0-9a-f-]{36})(?:\/|$)/i);
    if (!match || !TIDAL_PLAYLIST_ID_RE.test(match[1])) return null;
    return { id: match[1].toLowerCase(), kind: 'url', raw };
  } catch (_err) {
    return null;
  }
}

function indexIncluded(included) {
  const map = new Map();
  (Array.isArray(included) ? included : []).forEach((resource) => {
    const type = String(resource?.type || '').trim();
    const id = String(resource?.id || '').trim();
    if (type && id) map.set(`${type}:${id}`, resource);
  });
  return map;
}

function relationshipIds(resource, name) {
  const data = resource?.relationships?.[name]?.data;
  return (Array.isArray(data) ? data : (data ? [data] : []))
    .map((entry) => ({ type: String(entry?.type || '').trim(), id: String(entry?.id || '').trim() }))
    .filter((entry) => entry.type && entry.id);
}

export function mapTidalPlaylist(resource, { ownerName = '' } = {}) {
  const attrs = resource?.attributes || {};
  const id = String(resource?.id || '').trim();
  return {
    id,
    name: String(attrs.name || '').trim(),
    description: String(attrs.description || '').trim(),
    ownerId: relationshipIds(resource, 'owners').map((ref) => ref.id)[0] || '',
    ownerName: String(ownerName || '').trim(),
    public: String(attrs.accessType || '').trim().toUpperCase() === 'PUBLIC',
    collaborative: false,
    trackCount: Number(attrs.numberOfTrackItems ?? attrs.numberOfItems ?? 0),
    snapshotId: String(attrs.lastModifiedAt || '').trim(),
    imageUrl: '',
    externalUrl: id ? `https://tidal.com/playlist/${id}` : '',
  };
}

export function mapTidalTrack(resource, includedIndex) {
  const attrs = resource?.attributes || {};
  const artists = relationshipIds(resource, 'artists')
    .map((ref) => includedIndex.get(`${ref.type}:${ref.id}`))
    .map((artist) => String(artist?.attributes?.name || '').trim())
    .filter(Boolean)
    .map((name) => ({ name }));
  const albumRef = relationshipIds(resource, 'albums')[0];
  const album = albumRef ? includedIndex.get(`${albumRef.type}:${albumRef.id}`) : null;
  return {
    id: String(resource?.id || '').trim(),
    title: String(attrs.title || '').trim(),
    artists,
    album: {
      title: String(album?.attributes?.title || '').trim(),
      albumType: String(album?.attributes?.albumType || '').trim().toLowerCase(),
      imageUrl: '',
    },
    durationMs: parseTidalDurationMs(attrs.duration),
    isrc: String(attrs.isrc || '').trim(),
  };
}

export function createTidalService(ctx = {}) {
  const clientId = String(process.env.TIDAL_CLIENT_ID || '').trim();
  const clientSecret = String(process.env.TIDAL_CLIENT_SECRET || '').trim();
  const defaultCountryCode = normalizeCountryCode(process.env.TIDAL_COUNTRY_CODE, 'US');
  const requestTimeoutMs = Math.max(5000, parsePositiveNumber(process.env.TIDAL_TIMEOUT_MS, 15000));
  const scopes = ['playlists.read', 'user.read'];
  const pushLog = typeof ctx.pushLog === 'function' ? ctx.pushLog : null;
  let clientToken = { accessToken: '', expiresAt: 0 };

  function log(level, action, message, meta = null) {
    if (!pushLog) return;
    pushLog({ level, app: 'tidal', action, message, meta });
  }

  function isConfigured() {
    return Boolean(clientId && clientSecret);
  }

  function assertConfigured() {
    if (!isConfigured()) throw new Error('TIDAL integration is not configured.');
  }

  function buildRedirectUri(baseUrl) {
    const normalized = normalizeBaseUrl(baseUrl);
    if (!normalized) throw new Error('TIDAL redirect base URL is not configured.');
    return `${normalized}/user-settings/tidal/callback`;
  }

  function getAuthorizationUrl({ baseUrl, state, codeChallenge } = {}) {
    assertConfigured();
    if (!codeChallenge) throw new Error('TIDAL authorization requires a PKCE code challenge.');
    const redirectUri = buildRedirectUri(baseUrl);
    const url = new URL(TIDAL_AUTHORIZE_URL);
    url.searchParams.set('client_id', clientId);
    url.searchParams.set('response_type', 'code');
    url.searchParams.set('redirect_uri', redirectUri);
    url.searchParams.set('scope', scopes.join(' '));
    url.searchParams.set('code_challenge', String(codeChallenge));
    url.searchParams.set('code_challenge_method', 'S256');
    if (state) url.searchParams.set('state', String(state));
    return { url: url.toString(), redirectUri };
  }

  async function readJsonResponse(response) {
    const text = await response.text();
    try {
      return text ? JSON.parse(text) : null;
    } catch (_err) {
      return { rawText: text };
    }
  }

  function buildHttpError(response, payload) {
    const apiError = Array.isArray(payload?.errors) ? payload.errors[0] : null;
    const message = String(
      apiError?.detail
      || apiError?.title
      || payload?.error_description
      || payload?.userMessage
      || payload?.error
      || payload?.rawText
      || `TIDAL HTTP ${response.status}`,
    ).trim();
    const err = new Error(message);
    err.status = response.status;
    err.payload = payload;
    return err;
  }

  async function postTokenForm(body) {
    const response = await fetch(TIDAL_TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
      body: new URLSearchParams(body),
      signal: AbortSignal.timeout(requestTimeoutMs),
    });
    const payload = await readJsonResponse(response);
    if (!response.ok) throw buildHttpError(response, payload);
    return payload || {};
  }

  async function fetchApi(path, accessToken, params = {}) {
    const url = new URL(`${TIDAL_API_BASE}${path}`);
    // Array params use the OpenAPI default form/explode style: ?key=a&key=b
    Object.entries(params).forEach(([key, value]) => {
      if (value === undefined || value === null || value === '') return;
      (Array.isArray(value) ? value : [value]).forEach((entry) => url.searchParams.append(key, String(entry)));
    });
    for (let attempt = 0; ; attempt += 1) {
      const response = await fetch(url.toString(), {
        headers: {
          Authorization: `Bearer ${String(accessToken || '').trim()}`,
          Accept: 'application/vnd.api+json',
        },
        signal: AbortSignal.timeout(requestTimeoutMs),
      });
      if (response.status === 429 && attempt < TIDAL_MAX_RATE_LIMIT_RETRIES) {
        const retryAfterSeconds = parsePositiveNumber(response.headers?.get?.('retry-after'), attempt + 1);
        await new Promise((resolve) => setTimeout(resolve, Math.min(10, retryAfterSeconds) * 1000));
        continue;
      }
      const payload = await readJsonResponse(response);
      if (!response.ok) throw buildHttpError(response, payload);
      return payload || {};
    }
  }

  // Follows JSON:API cursor pagination, collecting `data` and `included`.
  async function fetchAllPages(path, accessToken, params = {}) {
    const data = [];
    const included = [];
    let cursor = '';
    for (let page = 0; page < TIDAL_MAX_PAGES; page += 1) {
      const payload = await fetchApi(path, accessToken, cursor ? { ...params, 'page[cursor]': cursor } : params);
      if (Array.isArray(payload?.data)) data.push(...payload.data);
      else if (payload?.data) data.push(payload.data);
      if (Array.isArray(payload?.included)) included.push(...payload.included);
      cursor = String(payload?.links?.meta?.nextCursor || '').trim();
      if (!cursor) break;
    }
    return { data, included };
  }

  function normalizeTokenPayload(payload = {}, fallbackRefreshToken = '') {
    return {
      accessToken: String(payload.access_token || '').trim(),
      refreshToken: String(payload.refresh_token || fallbackRefreshToken || '').trim(),
      scope: String(payload.scope || '').trim(),
      userId: String(payload.user_id || '').trim(),
      expiresAt: Date.now() + (Math.max(1, parsePositiveNumber(payload.expires_in, 3600)) * 1000),
    };
  }

  async function exchangeCode({ code, redirectUri, codeVerifier } = {}) {
    assertConfigured();
    const payload = await postTokenForm({
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: 'authorization_code',
      code: String(code || '').trim(),
      redirect_uri: String(redirectUri || '').trim(),
      code_verifier: String(codeVerifier || '').trim(),
      scope: scopes.join(' '),
    });
    return normalizeTokenPayload(payload);
  }

  async function refreshAccessToken(refreshToken) {
    assertConfigured();
    const payload = await postTokenForm({
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: 'refresh_token',
      refresh_token: String(refreshToken || '').trim(),
      scope: scopes.join(' '),
    });
    return normalizeTokenPayload(payload, refreshToken);
  }

  async function ensureAccessToken(auth = {}) {
    const accessToken = String(auth.accessToken || '').trim();
    const refreshToken = String(auth.refreshToken || '').trim();
    const expiresAt = Number(auth.expiresAt || 0);
    if (accessToken && expiresAt > (Date.now() + 60 * 1000)) {
      return { accessToken, refreshToken, expiresAt, refreshed: false };
    }
    if (!refreshToken) {
      const err = new Error('TIDAL connection is missing a refresh token. Reconnect TIDAL in User Settings.');
      err.code = 'TIDAL_REFRESH_TOKEN_MISSING';
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

  async function getClientCredentialsToken() {
    assertConfigured();
    if (clientToken.accessToken && clientToken.expiresAt > (Date.now() + 60 * 1000)) return clientToken.accessToken;
    const payload = await postTokenForm({
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: 'client_credentials',
    });
    const token = normalizeTokenPayload(payload);
    clientToken = { accessToken: token.accessToken, expiresAt: token.expiresAt };
    return clientToken.accessToken;
  }

  async function getCurrentUser(accessToken) {
    const payload = await fetchApi('/users/me', accessToken);
    const attrs = payload?.data?.attributes || {};
    return {
      id: String(payload?.data?.id || '').trim(),
      username: String(attrs.username || '').trim(),
      displayName: [attrs.firstName, attrs.lastName].map((part) => String(part || '').trim()).filter(Boolean).join(' ')
        || String(attrs.username || '').trim(),
      countryCode: normalizeCountryCode(attrs.country, defaultCountryCode),
    };
  }

  async function listCurrentUserPlaylists(accessToken, { countryCode, ownerName = '' } = {}) {
    const { data } = await fetchAllPages('/playlists', accessToken, {
      'filter[owners.id]': 'me',
      countryCode: normalizeCountryCode(countryCode, defaultCountryCode),
      sort: 'name',
    });
    return data
      .filter((resource) => String(resource?.type || '') === 'playlists')
      .map((resource) => mapTidalPlaylist(resource, { ownerName }))
      .filter((playlist) => playlist.id);
  }

  async function getPlaylist(accessToken, playlistId, { countryCode } = {}) {
    const id = String(playlistId || '').trim();
    if (!id) throw new Error('playlistId is required.');
    const payload = await fetchApi(`/playlists/${encodeURIComponent(id)}`, accessToken, {
      countryCode: normalizeCountryCode(countryCode, defaultCountryCode),
      includeLinkage: ['owners'],
    });
    return mapTidalPlaylist(payload?.data);
  }

  async function getPlaylistItems(accessToken, playlistId, { countryCode } = {}) {
    const id = String(playlistId || '').trim();
    if (!id) throw new Error('playlistId is required.');
    const country = normalizeCountryCode(countryCode, defaultCountryCode);
    const { data: itemRefs } = await fetchAllPages(`/playlists/${encodeURIComponent(id)}/relationships/items`, accessToken, {
      countryCode: country,
    });
    // Playlists can contain videos as well as tracks; only tracks can match the library.
    const trackIds = itemRefs
      .filter((ref) => String(ref?.type || '') === 'tracks')
      .map((ref) => String(ref?.id || '').trim())
      .filter(Boolean);
    const tracksById = new Map();
    const uniqueTrackIds = [...new Set(trackIds)];
    for (let index = 0; index < uniqueTrackIds.length; index += TIDAL_TRACK_BATCH_SIZE) {
      const batch = uniqueTrackIds.slice(index, index + TIDAL_TRACK_BATCH_SIZE);
      const payload = await fetchApi('/tracks', accessToken, {
        'filter[id]': batch,
        include: ['artists', 'albums'],
        countryCode: country,
      });
      const includedIndex = indexIncluded(payload?.included);
      (Array.isArray(payload?.data) ? payload.data : []).forEach((resource) => {
        const track = mapTidalTrack(resource, includedIndex);
        if (track.id) tracksById.set(track.id, track);
      });
    }
    const items = [];
    trackIds.forEach((trackId) => {
      const track = tracksById.get(trackId);
      if (!track) return;
      items.push({ ...track, position: items.length + 1 });
    });
    const skipped = trackIds.length - items.length;
    if (skipped > 0) {
      log('warn', 'playlist.items.unavailable', `${skipped} TIDAL track(s) in playlist ${id} were unavailable.`, { playlistId: id, skipped });
    }
    return { total: items.length, items };
  }

  return {
    isConfigured,
    buildRedirectUri,
    getAuthorizationUrl,
    createPkcePair: createTidalPkcePair,
    exchangeCode,
    refreshAccessToken,
    ensureAccessToken,
    getClientCredentialsToken,
    getCurrentUser,
    listCurrentUserPlaylists,
    getPlaylist,
    getPlaylistItems,
    parsePlaylistReference: parseTidalPlaylistReference,
  };
}
