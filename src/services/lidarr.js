import {
  getArtistRankSnapshot,
  getCurrentLidarrUsage,
  getLidarrArtistProgress,
  getLidarrRequest,
  getSuggestedArtist,
  enqueueLidarrRequest,
  listDueLidarrArtistReviews,
  listLidarrRequests,
  listSuggestedArtists,
  listUsersWithSuggestedArtists,
  recordLidarrUsage,
  removeQueuedLidarrRequest,
  saveLidarrArtistProgress,
  setSuggestedArtistStatus,
  updateLidarrRequest,
  upsertSuggestedAlbum,
} from '../db.js';

export const DEFAULT_LIDARR_AUTOMATION_SETTINGS = {
  autoAddArtists: false,
  autoAddQuotas: {
    weeklyArtists: 1,
    weeklyAlbums: 1,
  },
  autoTriggerManualSearch: false,
  manualSearchFallbackAttempts: 2,
  manualSearchFallbackHours: 24,
  minimumReleasePeers: 2,
  preferApprovedReleases: true,
  automationEnabled: false,
  automationScope: 'global',
  enabledUsers: [],
  roleQuotas: {
    admin: { weeklyArtists: -1, weeklyAlbums: -1 },
    'co-admin': { weeklyArtists: 3, weeklyAlbums: 6 },
    'power-user': { weeklyArtists: 1, weeklyAlbums: 2 },
    user: { weeklyArtists: 0, weeklyAlbums: 0 },
  },
};

const CURATORR_LIDARR_TAGS = {
  manual: {
    artist: 'curatorr-manual-artist',
    album: 'curatorr-manual-album',
  },
  automatic: {
    artist: 'curatorr-auto-artist',
    album: 'curatorr-auto-album',
  },
};

function normalizeNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeTitle(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeSourceKind(value) {
  return String(value || '').trim().toLowerCase() === 'automatic' ? 'automatic' : 'manual';
}

function parseDateMs(value) {
  const ts = Date.parse(String(value || ''));
  return Number.isFinite(ts) ? ts : 0;
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const DAY_MS = 24 * 60 * 60 * 1000;
const WEEK_MS = 7 * DAY_MS;

function normalizeStoredRank(value, fallback = 0) {
  const rank = Number(value);
  if (!Number.isFinite(rank)) return fallback;
  if (rank < 0) return 0;
  if (rank > 10) return fallback;
  return rank;
}

function summarizeLidarrErrorBody(body, status) {
  const raw = String(body || '').trim();
  if (!raw) return { message: `Lidarr request failed (${status}).`, detail: '' };
  let parsed = null;
  try {
    parsed = JSON.parse(raw);
  } catch (_err) {
    parsed = null;
  }
  const text = parsed && typeof parsed === 'object'
    ? [
        parsed.title,
        parsed.message,
        parsed.description,
        parsed.detail,
      ].filter(Boolean).join(' ')
    : raw;
  const normalized = String(text || raw).replace(/\s+/g, ' ').trim();
  if (/database is locked|sqliteexception|code\s*=\s*busy|message\s*=\s*busy/i.test(normalized)) {
    return {
      message: 'Lidarr is busy updating its database. Try again in a few seconds.',
      detail: normalized,
      code: 'LIDARR_BUSY',
      retryable: true,
    };
  }
  if (status >= 500) {
    return {
      message: 'Lidarr returned a server error. Check Curatorr logs for details.',
      detail: normalized,
      code: 'LIDARR_SERVER_ERROR',
      retryable: false,
    };
  }
  return {
    message: normalized || `Lidarr request failed (${status}).`,
    detail: normalized,
    code: 'LIDARR_REQUEST_ERROR',
    retryable: false,
  };
}

function scoreRelease(release, settings) {
  const peers = Math.max(0, normalizeNumber(release?.peers, normalizeNumber(release?.seeders, 0)));
  const ageHours = Math.max(0, normalizeNumber(release?.ageHours, normalizeNumber(release?.age, 0)));
  const customFormatScore = normalizeNumber(release?.customFormatScore, 0);
  const approved = release?.approved === true ? 1 : 0;
  const protocol = normalizeTitle(release?.protocol);
  const quality = normalizeTitle(release?.quality?.quality?.name || release?.quality?.name || release?.qualityName);

  let score = (peers * 10) + customFormatScore - ageHours;
  if (settings.preferApprovedReleases && approved) score += 25;
  if (protocol === 'usenet') score += 5;
  if (quality.includes('flac')) score += 8;
  if (quality.includes('mp3')) score += 3;
  if (peers < settings.minimumReleasePeers) score -= 50;
  return score;
}

export function createLidarrService(ctx) {
  const {
    db,
    loadConfig,
    safeMessage,
    slugifyId,
    pushLog,
    resolveRole,
    resolveLocalUsers,
  } = ctx;

  function logEvent(level, action, message, meta = null) {
    if (typeof pushLog !== 'function') return;
    pushLog({
      level,
      app: 'lidarr',
      action,
      message,
      meta,
    });
  }

  function normalizeRoleQuotas(value = {}) {
    const source = value && typeof value === 'object' ? value : {};
    return {
      admin: {
        ...DEFAULT_LIDARR_AUTOMATION_SETTINGS.roleQuotas.admin,
        ...(source.admin && typeof source.admin === 'object' ? source.admin : {}),
      },
      'co-admin': {
        ...DEFAULT_LIDARR_AUTOMATION_SETTINGS.roleQuotas['co-admin'],
        ...(source['co-admin'] && typeof source['co-admin'] === 'object' ? source['co-admin'] : {}),
      },
      'power-user': {
        ...DEFAULT_LIDARR_AUTOMATION_SETTINGS.roleQuotas['power-user'],
        ...(source['power-user'] && typeof source['power-user'] === 'object' ? source['power-user'] : {}),
      },
      user: {
        ...DEFAULT_LIDARR_AUTOMATION_SETTINGS.roleQuotas.user,
        ...(source.user && typeof source.user === 'object' ? source.user : {}),
      },
    };
  }

  function normalizeAutoAddQuotas(value = {}) {
    const source = value && typeof value === 'object' ? value : {};
    const normalizeLimit = (input, fallback) => {
      const parsed = Number(input);
      if (!Number.isFinite(parsed)) return fallback;
      return Math.max(-1, Math.min(999, Math.round(parsed)));
    };
    return {
      weeklyArtists: normalizeLimit(source.weeklyArtists, DEFAULT_LIDARR_AUTOMATION_SETTINGS.autoAddQuotas.weeklyArtists),
      weeklyAlbums: normalizeLimit(source.weeklyAlbums, DEFAULT_LIDARR_AUTOMATION_SETTINGS.autoAddQuotas.weeklyAlbums),
    };
  }

  function getSettings() {
    const config = loadConfig();
    const source = (config?.lidarr && typeof config.lidarr === 'object') ? config.lidarr : {};
    return {
      url: String(source.url || source.localUrl || '').trim(),
      localUrl: String(source.localUrl || '').trim(),
      remoteUrl: String(source.remoteUrl || '').trim(),
      apiKey: String(source.apiKey || '').trim(),
      ...DEFAULT_LIDARR_AUTOMATION_SETTINGS,
      ...source,
      autoAddQuotas: normalizeAutoAddQuotas(source.autoAddQuotas),
      roleQuotas: normalizeRoleQuotas(source.roleQuotas),
    };
  }

  function isConfigured() {
    const settings = getSettings();
    return Boolean(settings.url && settings.apiKey);
  }

  function buildRequest(pathname, init = {}) {
    const settings = getSettings();
    if (!settings.url || !settings.apiKey) throw new Error('Lidarr is not configured');
    const baseUrl = String(settings.url || '').replace(/\/+$/, '');
    const path = String(pathname || '').startsWith('/') ? pathname : `/${pathname}`;
    const headers = {
      'Content-Type': 'application/json',
      'X-Api-Key': settings.apiKey,
      ...(init.headers || {}),
    };
    return {
      url: `${baseUrl}/api/v1${path}`,
      init: { ...init, headers },
    };
  }

  async function request(pathname, init = {}) {
    const timeoutMs = Number(init?.timeoutMs || 0);
    const fetchInit = { ...init };
    delete fetchInit.timeoutMs;
    const { url, init: requestInitBase } = buildRequest(pathname, fetchInit);
    const requestInit = { ...requestInitBase };
    let controller = null;
    let timeoutId = null;
    if (timeoutMs > 0 && !requestInit.signal && typeof AbortController !== 'undefined') {
      controller = new AbortController();
      requestInit.signal = controller.signal;
      timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    }
    const retryDelays = [0, 700, 1500];
    let lastErr = null;
    try {
      for (let attempt = 0; attempt < retryDelays.length; attempt += 1) {
        if (attempt > 0) await wait(retryDelays[attempt]);
        const response = await fetch(url, requestInit);
        if (!response.ok) {
          const body = await response.text().catch(() => '');
          const summary = summarizeLidarrErrorBody(body, response.status);
          const err = new Error(summary.message);
          err.status = response.status;
          err.pathname = pathname;
          err.code = summary.code || '';
          err.detail = summary.detail || '';
          lastErr = err;
          if (summary.retryable && attempt < retryDelays.length - 1 && String(fetchInit?.method || 'GET').toUpperCase() !== 'GET') {
            logEvent('warn', 'request.retry', `Retrying Lidarr request for ${pathname}`, {
              attempt: attempt + 1,
              method: String(fetchInit?.method || 'GET').toUpperCase(),
              status: response.status,
              error: summary.detail || summary.message,
            });
            continue;
          }
          throw err;
        }
        if (response.status === 204) return null;
        return response.json();
      }
      if (lastErr) throw lastErr;
      throw new Error('Lidarr request failed.');
    } catch (err) {
      if (err?.name === 'AbortError') {
        err.code = err.code || 'REQUEST_TIMEOUT';
        err.message = 'Lidarr timed out while processing the request.';
      }
      logEvent('error', 'request.error', `Lidarr request failed for ${pathname}`, {
        method: String(fetchInit?.method || 'GET').toUpperCase(),
        error: safeMessage(err),
        detail: String(err?.detail || '').slice(0, 2000),
      });
      throw err;
    } finally {
      if (timeoutId) clearTimeout(timeoutId);
    }
  }

  async function requestExternalJson(url, options = {}) {
    const timeoutMs = Number(options.timeoutMs || 15000);
    let controller = null;
    let timeoutId = null;
    const headers = {
      Accept: 'application/json',
      'User-Agent': 'Curatorr/phase2 (+https://github.com/MickyGX/curatorr)',
      ...(options.headers || {}),
    };
    const init = {
      method: options.method || 'GET',
      headers,
    };
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

  function pickBestRelease(releases = [], options = {}) {
    const settings = { ...getSettings(), ...(options.settings || {}) };
    return [...releases]
      .map((release) => ({ release, _score: scoreRelease(release, settings) }))
      .sort((a, b) => b._score - a._score)[0]?.release || null;
  }

  function normalizeRole(role) {
    const raw = String(role || '').trim().toLowerCase();
    if (raw === 'admin' || raw === 'co-admin' || raw === 'power-user') return raw;
    return 'user';
  }

  function resolveAutomationRoleForUserId(userPlexId) {
    const value = String(userPlexId || '').trim();
    if (!value) return 'user';
    const config = loadConfig();
    const localUsers = typeof resolveLocalUsers === 'function' ? resolveLocalUsers(config) : [];
    const key = value.toLowerCase();
    const localMatch = localUsers.find((user) => {
      const ids = [user?.username, user?.email].map((entry) => String(entry || '').trim().toLowerCase()).filter(Boolean);
      return ids.includes(key);
    });
    if (localMatch?.role) return normalizeRole(localMatch.role);
    if (typeof resolveRole === 'function') {
      return normalizeRole(resolveRole({
        username: value,
        email: value,
        title: value,
      }));
    }
    return 'user';
  }

  function getBelterThreshold() {
    const config = loadConfig();
    const raw = Number(config?.smartPlaylist?.artistBelterRank);
    if (Number.isFinite(raw)) return Math.max(5, Math.min(10, raw));
    return 8;
  }

  function getManualFallbackAttempts() {
    const raw = Number(getSettings().manualSearchFallbackAttempts);
    if (!Number.isFinite(raw)) return DEFAULT_LIDARR_AUTOMATION_SETTINGS.manualSearchFallbackAttempts;
    return Math.max(1, Math.min(10, Math.round(raw)));
  }

  function getManualFallbackDelayMs() {
    const raw = Number(getSettings().manualSearchFallbackHours);
    if (!Number.isFinite(raw)) return DEFAULT_LIDARR_AUTOMATION_SETTINGS.manualSearchFallbackHours * 60 * 60 * 1000;
    return Math.max(1, Math.min(168, raw)) * 60 * 60 * 1000;
  }

  function getReviewCooldownMs(progress = null) {
    const stage = String(progress?.currentStage || '').trim().toLowerCase();
    if (stage === 'awaiting_belter' || stage === 'quota_blocked' || stage === 'search_failed') return DAY_MS;
    if (stage === 'catalog_complete') return WEEK_MS;
    return WEEK_MS;
  }

  function resolveProgressReviewAt(progress = null, now = Date.now()) {
    const explicit = Number(progress?.nextReviewAt || 0);
    if (explicit > 0) return explicit;
    const lastAlbumAddedAt = Number(progress?.lastAlbumAddedAt || 0);
    if (lastAlbumAddedAt > 0) return lastAlbumAddedAt + getReviewCooldownMs(progress);
    return now;
  }

  function getRoleQuota(role, usage = {}) {
    const settings = getSettings();
    const roleKey = normalizeRole(role);
    const quota = settings.roleQuotas?.[roleKey] || { weeklyArtists: 0, weeklyAlbums: 0 };
    const artistUsed = Math.max(0, Number(usage.artists || 0));
    const albumUsed = Math.max(0, Number(usage.albums || 0));
    const buildMetric = (limit, used) => {
      const normalizedLimit = Number(limit);
      const unlimited = normalizedLimit < 0;
      return {
        used,
        limit: unlimited ? -1 : normalizedLimit,
        unlimited,
        remaining: unlimited ? -1 : Math.max(0, normalizedLimit - used),
      };
    };
    return {
      role: roleKey,
      weeklyArtists: buildMetric(quota.weeklyArtists, artistUsed),
      weeklyAlbums: buildMetric(quota.weeklyAlbums, albumUsed),
    };
  }

  function getAutoAddQuota(usage = {}) {
    const settings = getSettings();
    const autoQuotas = settings.autoAddQuotas || DEFAULT_LIDARR_AUTOMATION_SETTINGS.autoAddQuotas;
    const autoArtistUsed = Math.max(0, Number(usage.auto_artists || 0));
    const autoAlbumUsed = Math.max(0, Number(usage.auto_albums || 0));
    const buildMetric = (limit, used) => {
      const normalizedLimit = Number(limit);
      const unlimited = normalizedLimit < 0;
      return {
        used,
        limit: unlimited ? -1 : normalizedLimit,
        unlimited,
        remaining: unlimited ? -1 : Math.max(0, normalizedLimit - used),
      };
    };
    return {
      weeklyArtists: buildMetric(autoQuotas.weeklyArtists, autoArtistUsed),
      weeklyAlbums: buildMetric(autoQuotas.weeklyAlbums, autoAlbumUsed),
    };
  }

  function assertAutoAddQuotaAvailable(usage = {}, requested = {}) {
    const quota = getAutoAddQuota(usage);
    const artistsRequested = Math.max(0, Number(requested.artists || 0));
    const albumsRequested = Math.max(0, Number(requested.albums || 0));
    if (!quota.weeklyArtists.unlimited && quota.weeklyArtists.used + artistsRequested > quota.weeklyArtists.limit) {
      const err = new Error(`Automatic artist quota reached (${quota.weeklyArtists.used}/${quota.weeklyArtists.limit}).`);
      err.code = 'AUTO_ARTIST_QUOTA_REACHED';
      err.quota = quota;
      throw err;
    }
    if (!quota.weeklyAlbums.unlimited && quota.weeklyAlbums.used + albumsRequested > quota.weeklyAlbums.limit) {
      const err = new Error(`Automatic album quota reached (${quota.weeklyAlbums.used}/${quota.weeklyAlbums.limit}).`);
      err.code = 'AUTO_ALBUM_QUOTA_REACHED';
      err.quota = quota;
      throw err;
    }
    return quota;
  }

  function getPendingAutoAddReservations(userPlexId) {
    return listLidarrRequests(db, userPlexId, { statuses: ['queued', 'processing'], limit: 500 })
      .filter((request) => request.requestKind === 'artist_album' && request.detail?.autoAdd === true)
      .reduce((acc, request) => {
        acc.artists += 1;
        acc.albums += 1;
        return acc;
      }, { artists: 0, albums: 0 });
  }

  function assertQuotaAvailable(role, usage = {}, requested = {}) {
    const quota = getRoleQuota(role, usage);
    const artistsRequested = Math.max(0, Number(requested.artists || 0));
    const albumsRequested = Math.max(0, Number(requested.albums || 0));
    if (!quota.weeklyArtists.unlimited && quota.weeklyArtists.used + artistsRequested > quota.weeklyArtists.limit) {
      const err = new Error(`Weekly artist quota reached (${quota.weeklyArtists.used}/${quota.weeklyArtists.limit}).`);
      err.code = 'ARTIST_QUOTA_REACHED';
      err.quota = quota;
      throw err;
    }
    if (!quota.weeklyAlbums.unlimited && quota.weeklyAlbums.used + albumsRequested > quota.weeklyAlbums.limit) {
      const err = new Error(`Weekly album quota reached (${quota.weeklyAlbums.used}/${quota.weeklyAlbums.limit}).`);
      err.code = 'ALBUM_QUOTA_REACHED';
      err.quota = quota;
      throw err;
    }
    return quota;
  }

  async function getRootFolders() {
    const list = await request('/rootfolder', { method: 'GET' });
    return Array.isArray(list) ? list : [];
  }

  async function getQualityProfiles() {
    const list = await request('/qualityprofile', { method: 'GET' });
    return Array.isArray(list) ? list : [];
  }

  async function lookupArtist(term) {
    const query = encodeURIComponent(String(term || '').trim());
    if (!query) return [];
    logEvent('info', 'artist.lookup.start', `Looking up artist in Lidarr: ${term}`);
    try {
      const list = await request(`/artist/lookup?term=${query}`, { method: 'GET' });
      const normalized = Array.isArray(list) ? list : [];
      logEvent('info', 'artist.lookup.result', `Lidarr lookup returned ${normalized.length} result(s) for ${term}`, {
        results: normalized.length,
      });
      return normalized;
    } catch (err) {
      logEvent('error', 'artist.lookup.error', `Artist lookup failed for ${term}`, {
        error: safeMessage(err),
      });
      throw err;
    }
  }

  async function listArtists(options = {}) {
    const pageSize = Math.max(100, Math.min(5000, Number(options.pageSize || 2000)));
    const timeoutMs = Number(options.timeoutMs || 15000);
    logEvent('info', 'artist.list.start', 'Fetching Lidarr artist list', { pageSize, timeoutMs });
    try {
      const list = await request(`/artist?page=1&pageSize=${pageSize}`, {
        method: 'GET',
        timeoutMs,
      });
      const artists = Array.isArray(list) ? list : [];
      logEvent('info', 'artist.list.result', `Fetched ${artists.length} artist(s) from Lidarr`, {
        artists: artists.length,
      });
      return artists;
    } catch (err) {
      logEvent('error', 'artist.list.error', 'Failed to fetch Lidarr artist list', {
        error: safeMessage(err),
        code: err?.code || '',
      });
      throw err;
    }
  }

  function pickLookupArtist(results = [], artistName = '', options = {}) {
    const target = normalizeTitle(artistName);
    const targetForeignId = normalizeTitle(options.foreignArtistId);
    if (!Array.isArray(results) || !results.length) return null;
    if (targetForeignId) {
      const byForeignId = results.find((item) => normalizeTitle(item?.foreignArtistId) === targetForeignId);
      if (byForeignId) return byForeignId;
    }
    return results.find((item) => normalizeTitle(item?.artistName) === target)
      || results.find((item) => normalizeTitle(item?.sortName) === target)
      || results[0]
      || null;
  }

  function normalizeAlbumMatchTitle(title) {
    return String(title || '')
      .trim()
      .toLowerCase()
      .replace(/\([^)]*\)/g, ' ')
      .replace(/\[[^\]]*\]/g, ' ')
      .replace(/[^a-z0-9]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function buildArtistPath(rootFolderPath, lookupArtistResult) {
    const root = String(rootFolderPath || '').replace(/\/+$/, '');
    const folder = String(lookupArtistResult?.folder || '').trim() || slugifyId(lookupArtistResult?.artistName || 'artist');
    return `${root}/${folder}`.replace(/\/{2,}/g, '/');
  }

  async function listArtistAlbums(artistId, options = {}) {
    const id = Number(artistId || 0);
    if (!id) return [];
    const timeoutMs = Number(options.timeoutMs || 15000);
    const pageSize = Math.max(10, Math.min(200, Number(options.pageSize || 100)));
    logEvent('info', 'album.list.start', `Fetching Lidarr albums for artist ${id}`, { timeoutMs, pageSize });
    try {
      const list = await request(`/album?artistId=${encodeURIComponent(id)}&page=1&pageSize=${pageSize}`, {
        method: 'GET',
        timeoutMs,
      });
      const albums = Array.isArray(list) ? list : [];
      logEvent('info', 'album.list.result', `Fetched ${albums.length} Lidarr album(s) for artist ${id}`, {
        artistId: id,
        albums: albums.length,
      });
      return albums;
    } catch (err) {
      logEvent('error', 'album.list.error', `Failed to fetch Lidarr albums for artist ${id}`, {
        artistId: id,
        error: safeMessage(err),
        code: err?.code || '',
      });
      throw err;
    }
  }

  function isExplicitGreatestHitsTitle(album) {
    const title = String(album?.title || '').trim();
    return /\b(greatest hits|best of|essentials|anthology|collection|number ones|the hits|hits)\b/i.test(title)
      && !/\b(box set|catalogue set|catalog set|discography|complete works|deluxe edition|super deluxe)\b/i.test(title);
  }

  function isStandardAlbumCandidate(album) {
    const albumType = normalizeTitle(album?.albumType);
    const title = String(album?.title || '').trim();
    if (/\b(box set|catalogue set|catalog set|discography|complete works)\b/i.test(title)) return false;
    if (/\s\/\s|\s\+\s/.test(title)) return false;
    return albumType === 'album' || albumType === 'ep';
  }

  function scoreAlbumQuality(album) {
    const ratings = album?.ratings && typeof album.ratings === 'object' ? album.ratings : {};
    const statistics = album?.statistics && typeof album.statistics === 'object' ? album.statistics : {};
    const ratingValue = normalizeNumber(ratings.value, 0);
    const votes = normalizeNumber(ratings.votes, 0);
    const trackCount = normalizeNumber(statistics.trackCount, 0);
    const releaseMs = parseDateMs(album?.releaseDate);
    return (ratingValue * 100) + Math.min(60, votes * 1.5) + Math.min(30, trackCount) + (releaseMs / 1e11);
  }

  function pickStarterAlbum(albums = []) {
    const candidates = (Array.isArray(albums) ? albums : [])
      .filter((album) => album && String(album.title || '').trim());
    if (!candidates.length) return null;
    if (candidates.length === 1) {
      return { album: candidates[0], selectionReason: 'only_album' };
    }

    const preferredAlbumTypes = new Set(['album', 'compilation', 'ep']);
    const filtered = candidates.filter((album) => preferredAlbumTypes.has(normalizeTitle(album.albumType) || 'album'));
    const pool = filtered.length ? filtered : candidates;

    const greatestHits = pool
      .filter((album) => isExplicitGreatestHitsTitle(album))
      .sort((a, b) => parseDateMs(b?.releaseDate) - parseDateMs(a?.releaseDate) || scoreAlbumQuality(b) - scoreAlbumQuality(a));
    if (greatestHits.length) {
      return { album: greatestHits[0], selectionReason: 'greatest_hits_recent' };
    }

    const standardAlbums = pool.filter((album) => isStandardAlbumCandidate(album));
    const ranked = [...(standardAlbums.length ? standardAlbums : pool)]
      .sort((a, b) => scoreAlbumQuality(b) - scoreAlbumQuality(a) || parseDateMs(b?.releaseDate) - parseDateMs(a?.releaseDate));
    return { album: ranked[0], selectionReason: 'highest_rated_album' };
  }

  function pickNextExpansionAlbum(albums = []) {
    const candidates = (Array.isArray(albums) ? albums : [])
      .filter((album) => album && String(album.title || '').trim() && !album.monitored);
    if (!candidates.length) return null;

    const standardAlbums = candidates.filter((album) => isStandardAlbumCandidate(album) && !isExplicitGreatestHitsTitle(album));
    const fallbackStandard = candidates.filter((album) => isStandardAlbumCandidate(album));
    const pool = standardAlbums.length
      ? standardAlbums
      : (fallbackStandard.length ? fallbackStandard : candidates);

    const ranked = [...pool]
      .sort((a, b) => scoreAlbumQuality(b) - scoreAlbumQuality(a) || parseDateMs(b?.releaseDate) - parseDateMs(a?.releaseDate));
    if (!ranked.length) return null;
    return {
      album: ranked[0],
      selectionReason: standardAlbums.length ? 'catalog_unlock' : 'catalog_fallback',
    };
  }

  function pickPreferredAlbum(albums = [], preferredAlbumTitle = '') {
    const preferred = normalizeAlbumMatchTitle(preferredAlbumTitle);
    if (!preferred) return pickStarterAlbum(albums);
    const candidates = Array.isArray(albums) ? albums : [];
    const exact = candidates.find((album) => normalizeAlbumMatchTitle(album?.title) === preferred);
    if (exact) return { album: exact, selectionReason: 'manual_exact' };
    const fuzzy = candidates.find((album) => {
      const title = normalizeAlbumMatchTitle(album?.title);
      return title.includes(preferred) || preferred.includes(title);
    });
    if (fuzzy) return { album: fuzzy, selectionReason: 'manual_fuzzy' };
    const fallback = pickStarterAlbum(candidates);
    return fallback ? { ...fallback, selectionReason: `fallback_${fallback.selectionReason}` } : null;
  }

  async function listMusicBrainzReleaseGroups(foreignArtistId, options = {}) {
    const mbid = String(foreignArtistId || '').trim();
    if (!mbid) return [];
    const limit = Math.max(25, Math.min(100, Number(options.limit || 100)));
    const url = new URL('https://musicbrainz.org/ws/2/release-group');
    url.searchParams.set('artist', mbid);
    url.searchParams.set('limit', String(limit));
    url.searchParams.set('offset', String(Math.max(0, Number(options.offset || 0))));
    url.searchParams.set('fmt', 'json');
    logEvent('info', 'musicbrainz.release_groups.start', `Fetching MusicBrainz release groups for ${mbid}`, {
      foreignArtistId: mbid,
      limit,
    });
    const data = await requestExternalJson(url.toString(), { timeoutMs: Number(options.timeoutMs || 20000) });
    const groups = Array.isArray(data?.['release-groups']) ? data['release-groups'] : [];
    return groups
      .map((group) => {
        const secondaryTypes = Array.isArray(group?.['secondary-types']) ? group['secondary-types'] : [];
        const primaryType = String(group?.['primary-type'] || '').trim();
        const inferredType = secondaryTypes.some((value) => normalizeTitle(value) === 'compilation')
          ? 'compilation'
          : normalizeTitle(primaryType || 'album');
        return {
          albumId: String(group?.id || ''),
          title: String(group?.title || '').trim(),
          albumType: inferredType,
          releaseDate: String(group?.['first-release-date'] || ''),
          secondaryTypes,
          imageUrl: group?.id ? `/api/music/cover/release-group/${encodeURIComponent(String(group.id))}` : '',
          source: 'musicbrainz',
        };
      })
      .filter((album) => album.title)
      .sort((a, b) => {
        if (isExplicitGreatestHitsTitle(a) !== isExplicitGreatestHitsTitle(b)) {
          return isExplicitGreatestHitsTitle(a) ? -1 : 1;
        }
        return parseDateMs(b.releaseDate) - parseDateMs(a.releaseDate) || a.title.localeCompare(b.title);
      });
  }

  async function previewManualArtistAlbums(options = {}) {
    const artistName = String(options.artistName || '').trim();
    const foreignArtistId = String(options.foreignArtistId || '').trim();
    if (!artistName && !foreignArtistId) throw new Error('artistName or foreignArtistId is required');
    const results = await lookupArtist(artistName || foreignArtistId);
    const match = pickLookupArtist(results, artistName, { foreignArtistId });
    if (!match) {
      const err = new Error(`No Lidarr match found for ${artistName || foreignArtistId}.`);
      err.code = 'ARTIST_NOT_FOUND';
      throw err;
    }
    if (match.added && Number(match.id || 0) > 0) {
      const albums = await listArtistAlbums(match.id, { timeoutMs: 15000, pageSize: 200 });
      return {
        artist: match,
        albums: albums.map((album) => ({
          albumId: Number(album?.id || 0) || null,
          title: String(album?.title || '').trim(),
          albumType: String(album?.albumType || ''),
          releaseDate: String(album?.releaseDate || ''),
          monitored: Boolean(album?.monitored),
          imagePath: Array.isArray(album?.images)
            ? (album.images.find((img) => /cover|poster/i.test(String(img?.coverType || '')))?.url || '')
            : '',
          source: 'lidarr',
        })).filter((album) => album.title),
        source: 'lidarr',
      };
    }
    const albums = await listMusicBrainzReleaseGroups(match.foreignArtistId || foreignArtistId, { limit: 100, timeoutMs: 20000 });
    return {
      artist: match,
      albums,
      source: 'musicbrainz',
    };
  }

  async function setAlbumMonitored(albumId, monitored = true) {
    const id = Number(albumId || 0);
    if (!id) throw new Error('albumId is required');
    logEvent('info', 'album.monitor.start', `${monitored ? 'Monitoring' : 'Unmonitoring'} Lidarr album ${id}`, {
      albumId: id,
      monitored: Boolean(monitored),
    });
    const result = await request('/album/monitor', {
      method: 'PUT',
      body: JSON.stringify({ albumIds: [id], monitored: Boolean(monitored) }),
    });
    logEvent('info', 'album.monitor.success', `${monitored ? 'Monitored' : 'Unmonitored'} Lidarr album ${id}`, {
      albumId: id,
      monitored: Boolean(monitored),
    });
    return result;
  }

  async function listTags(options = {}) {
    const timeoutMs = Number(options.timeoutMs || 10000);
    const list = await request('/tag', {
      method: 'GET',
      timeoutMs,
    });
    return Array.isArray(list) ? list : [];
  }

  async function ensureTag(tagName, options = {}) {
    const label = String(tagName || '').trim();
    if (!label) return null;
    const timeoutMs = Number(options.timeoutMs || 10000);
    const existingTags = await listTags({ timeoutMs });
    const existing = existingTags.find((tag) => normalizeTitle(tag?.label) === normalizeTitle(label));
    if (existing) return Number(existing.id || 0) || null;
    const created = await request('/tag', {
      method: 'POST',
      timeoutMs,
      body: JSON.stringify({ label }),
    });
    return Number(created?.id || 0) || null;
  }

  async function ensureTagIds(tagNames = [], options = {}) {
    const uniqueNames = [...new Set((Array.isArray(tagNames) ? tagNames : [])
      .map((name) => String(name || '').trim())
      .filter(Boolean))];
    if (!uniqueNames.length) return [];
    const ids = [];
    for (const tagName of uniqueNames) {
      const id = await ensureTag(tagName, options);
      if (Number(id || 0) > 0) ids.push(Number(id));
    }
    return [...new Set(ids)];
  }

  async function addArtistTags(artistId, tagNames = [], options = {}) {
    const id = Number(artistId || 0);
    if (!id) return null;
    const timeoutMs = Number(options.timeoutMs || 15000);
    const tagIds = await ensureTagIds(tagNames, { timeoutMs });
    if (!tagIds.length) return null;
    try {
      return await request('/artist/editor', {
        method: 'PUT',
        timeoutMs,
        body: JSON.stringify({
          artistIds: [id],
          tags: tagIds,
          applyTags: 'add',
        }),
      });
    } catch (editorErr) {
      const artist = options.artist && typeof options.artist === 'object'
        ? options.artist
        : await getArtist(id, { timeoutMs });
      if (!artist) throw editorErr;
      const existingTags = Array.isArray(artist?.tags)
        ? artist.tags.map((value) => Number(value || 0)).filter((value) => value > 0)
        : [];
      return request('/artist', {
        method: 'PUT',
        timeoutMs,
        body: JSON.stringify({
          ...artist,
          tags: [...new Set([...existingTags, ...tagIds])],
        }),
      });
    }
  }

  async function addAlbumTags(albumId, tagNames = [], options = {}) {
    const id = Number(albumId || 0);
    if (!id) return null;
    const timeoutMs = Number(options.timeoutMs || 15000);
    const tagIds = await ensureTagIds(tagNames, { timeoutMs });
    if (!tagIds.length) return null;
    try {
      return await request('/album/editor', {
        method: 'PUT',
        timeoutMs,
        body: JSON.stringify({
          albumIds: [id],
          tags: tagIds,
          applyTags: 'add',
        }),
      });
    } catch (editorErr) {
      const album = options.album && typeof options.album === 'object'
        ? options.album
        : await getAlbum(id, { timeoutMs });
      if (!album) throw editorErr;
      const existingTags = Array.isArray(album?.tags)
        ? album.tags.map((value) => Number(value || 0)).filter((value) => value > 0)
        : [];
      return request('/album', {
        method: 'PUT',
        timeoutMs,
        body: JSON.stringify({
          ...album,
          tags: [...new Set([...existingTags, ...tagIds])],
        }),
      });
    }
  }

  async function tagCuratorrManagedItems(options = {}) {
    const sourceKind = normalizeSourceKind(options.sourceKind);
    const tagNames = CURATORR_LIDARR_TAGS[sourceKind] || CURATORR_LIDARR_TAGS.manual;
    const artistId = Number(options.artistId || 0);
    const albumId = Number(options.albumId || 0);
    const tagArtist = options.tagArtist === true && artistId > 0;
    const tagAlbum = options.tagAlbum === true && albumId > 0;
    if (!tagArtist && !tagAlbum) return { ok: true, taggedArtist: false, taggedAlbum: false };

    const tasks = [];
    if (tagArtist) {
      tasks.push(
        addArtistTags(artistId, [tagNames.artist], { timeoutMs: 15000 }).then(() => ({ kind: 'artist', ok: true })).catch((err) => ({ kind: 'artist', ok: false, err }))
      );
    }
    if (tagAlbum) {
      tasks.push(
        addAlbumTags(albumId, [tagNames.album], { timeoutMs: 15000 }).then(() => ({ kind: 'album', ok: true })).catch((err) => ({ kind: 'album', ok: false, err }))
      );
    }
    const results = await Promise.all(tasks);
    results.filter((result) => !result.ok).forEach((result) => {
      logEvent('warn', 'tag.apply.error', `Failed to apply Curatorr ${result.kind} tag in Lidarr`, {
        sourceKind,
        artistId: tagArtist ? artistId : null,
        albumId: tagAlbum ? albumId : null,
        error: safeMessage(result.err),
        code: result.err?.code || '',
      });
    });
    return {
      ok: results.every((result) => result.ok),
      taggedArtist: results.some((result) => result.kind === 'artist' && result.ok),
      taggedAlbum: results.some((result) => result.kind === 'album' && result.ok),
    };
  }

  async function triggerAlbumSearch(albumIds = []) {
    const ids = (Array.isArray(albumIds) ? albumIds : [])
      .map((value) => Number(value || 0))
      .filter((value) => value > 0);
    if (!ids.length) throw new Error('albumIds are required');
    logEvent('info', 'album.search.start', `Triggering Lidarr album search for ${ids.length} album(s)`, {
      albumIds: ids,
    });
    const result = await request('/command', {
      method: 'POST',
      body: JSON.stringify({ name: 'AlbumSearch', albumIds: ids }),
    });
    logEvent('info', 'album.search.success', `Queued Lidarr album search for ${ids.length} album(s)`, {
      albumIds: ids,
      commandId: Number(result?.id || 0) || null,
      commandName: result?.name || 'AlbumSearch',
    });
    return result;
  }

  async function getCommand(commandId, options = {}) {
    const id = Number(commandId || 0);
    if (!id) return null;
    const timeoutMs = Number(options.timeoutMs || 10000);
    logEvent('info', 'command.get.start', `Fetching Lidarr command ${id}`, {
      commandId: id,
      timeoutMs,
    });
    try {
      const command = await request(`/command/${encodeURIComponent(id)}`, {
        method: 'GET',
        timeoutMs,
      });
      logEvent('info', 'command.get.result', `Fetched Lidarr command ${id}`, {
        commandId: id,
        status: String(command?.status || ''),
        name: String(command?.name || ''),
      });
      return command && typeof command === 'object' ? command : null;
    } catch (err) {
      logEvent('error', 'command.get.error', `Failed to fetch Lidarr command ${id}`, {
        commandId: id,
        error: safeMessage(err),
        code: err?.code || '',
      });
      throw err;
    }
  }

  async function getAlbum(albumId, options = {}) {
    const id = Number(albumId || 0);
    if (!id) return null;
    const timeoutMs = Number(options.timeoutMs || 10000);
    logEvent('info', 'album.get.start', `Fetching Lidarr album ${id}`, {
      albumId: id,
      timeoutMs,
    });
    try {
      const album = await request(`/album/${encodeURIComponent(id)}`, {
        method: 'GET',
        timeoutMs,
      });
      logEvent('info', 'album.get.result', `Fetched Lidarr album ${id}`, {
        albumId: id,
        monitored: Boolean(album?.monitored),
        trackFileCount: Number(album?.statistics?.trackFileCount || 0),
      });
      return album && typeof album === 'object' ? album : null;
    } catch (err) {
      logEvent('error', 'album.get.error', `Failed to fetch Lidarr album ${id}`, {
        albumId: id,
        error: safeMessage(err),
        code: err?.code || '',
      });
      throw err;
    }
  }

  async function getArtist(artistId, options = {}) {
    const id = Number(artistId || 0);
    if (!id) return null;
    const timeoutMs = Number(options.timeoutMs || 10000);
    logEvent('info', 'artist.get.start', `Fetching Lidarr artist ${id}`, {
      artistId: id,
      timeoutMs,
    });
    try {
      const artist = await request(`/artist/${encodeURIComponent(id)}`, {
        method: 'GET',
        timeoutMs,
      });
      logEvent('info', 'artist.get.result', `Fetched Lidarr artist ${id}`, {
        artistId: id,
        monitored: Boolean(artist?.monitored),
        monitorNewItems: String(artist?.monitorNewItems || ''),
      });
      return artist && typeof artist === 'object' ? artist : null;
    } catch (err) {
      logEvent('error', 'artist.get.error', `Failed to fetch Lidarr artist ${id}`, {
        artistId: id,
        error: safeMessage(err),
        code: err?.code || '',
      });
      throw err;
    }
  }

  async function setArtistMonitored(artistId, monitored = true, options = {}) {
    const id = Number(artistId || 0);
    if (!id) throw new Error('artistId is required');
    const timeoutMs = Number(options.timeoutMs || 15000);
    const artist = options.artist && typeof options.artist === 'object'
      ? options.artist
      : await getArtist(id, { timeoutMs });
    if (!artist) throw new Error('Artist not found in Lidarr');
    const payload = {
      ...artist,
      monitored: Boolean(monitored),
      monitorNewItems: String(artist?.monitorNewItems || 'none') || 'none',
    };
    logEvent('info', 'artist.monitor.start', `${monitored ? 'Monitoring' : 'Unmonitoring'} Lidarr artist ${id}`, {
      artistId: id,
      monitored: Boolean(monitored),
      monitorNewItems: payload.monitorNewItems,
    });
    const result = await request('/artist', {
      method: 'PUT',
      timeoutMs,
      body: JSON.stringify(payload),
    });
    logEvent('info', 'artist.monitor.success', `${monitored ? 'Monitored' : 'Unmonitored'} Lidarr artist ${id}`, {
      artistId: id,
      monitored: Boolean(monitored),
      monitorNewItems: payload.monitorNewItems,
    });
    return result;
  }

  async function searchAlbumReleases(albumId, options = {}) {
    const id = Number(albumId || 0);
    if (!id) return [];
    const timeoutMs = Number(options.timeoutMs || 60000);
    logEvent('info', 'release.search.start', `Searching Lidarr releases for album ${id}`, {
      albumId: id,
      timeoutMs,
    });
    try {
      const releases = await request(`/release?albumId=${encodeURIComponent(id)}`, {
        method: 'GET',
        timeoutMs,
      });
      const list = Array.isArray(releases) ? releases : [];
      logEvent('info', 'release.search.result', `Lidarr returned ${list.length} release candidate(s) for album ${id}`, {
        albumId: id,
        results: list.length,
      });
      return list;
    } catch (err) {
      logEvent('error', 'release.search.error', `Release lookup failed for album ${id}`, {
        albumId: id,
        error: safeMessage(err),
        code: err?.code || '',
      });
      throw err;
    }
  }

  async function grabRelease(release, options = {}) {
    const body = release && typeof release === 'object' ? release : {};
    const timeoutMs = Number(options.timeoutMs || 20000);
    logEvent('info', 'release.grab.start', `Sending release to Lidarr download client: ${String(body?.title || '').trim() || 'unknown release'}`, {
      guid: String(body?.guid || ''),
      indexerId: Number(body?.indexerId || 0) || null,
      protocol: String(body?.protocol || ''),
      peers: normalizeNumber(body?.peers, normalizeNumber(body?.seeders, 0)),
    });
    try {
      const result = await request('/release', {
        method: 'POST',
        timeoutMs,
        body: JSON.stringify(body),
      });
      logEvent('info', 'release.grab.success', `Queued release download in Lidarr: ${String(body?.title || '').trim() || 'unknown release'}`, {
        guid: String(body?.guid || ''),
        indexerId: Number(body?.indexerId || 0) || null,
      });
      return result;
    } catch (err) {
      logEvent('error', 'release.grab.error', `Failed to queue Lidarr release download: ${String(body?.title || '').trim() || 'unknown release'}`, {
        guid: String(body?.guid || ''),
        indexerId: Number(body?.indexerId || 0) || null,
        error: safeMessage(err),
        code: err?.code || '',
      });
      throw err;
    }
  }

  function resolveTrackedAlbum(reason = {}) {
    const latestAlbum = reason?.latestAlbum && typeof reason.latestAlbum === 'object' ? reason.latestAlbum : null;
    const starterAlbum = reason?.starterAlbum && typeof reason.starterAlbum === 'object' ? reason.starterAlbum : null;
    return latestAlbum || starterAlbum || null;
  }

  async function reconcileArtistAcquisition(options = {}) {
    const userPlexId = String(options.userPlexId || '').trim();
    const artistName = String(options.artistName || '').trim();
    const role = normalizeRole(options.role || resolveAutomationRoleForUserId(userPlexId));
    const force = options.force === true;
    const now = Date.now();
    if (!db || !userPlexId || !artistName) return { ok: false, status: 'invalid_request' };

    const existingSuggestion = getSuggestedArtist(db, userPlexId, artistName);
    const existingProgress = getLidarrArtistProgress(db, userPlexId, artistName);
    const persistedSuggestionStatus = existingSuggestion?.status === 'dismissed' ? 'dismissed' : 'added_to_lidarr';
    const baseReason = existingSuggestion?.reason && typeof existingSuggestion.reason === 'object' ? existingSuggestion.reason : {};
    const trackedAlbum = resolveTrackedAlbum(baseReason);
    const albumId = Number(trackedAlbum?.albumId || 0);
    if (!albumId) {
      return { ok: false, status: 'no_tracked_album', progress: existingProgress };
    }

    const album = await getAlbum(albumId, { timeoutMs: 12000 });
    if (!album) return { ok: false, status: 'album_not_found', albumId };
    const lidarrArtistId = Number(existingProgress?.lidarrArtistId || existingSuggestion?.lidarrArtistId || album?.artistId || 0);
    if (lidarrArtistId > 0) {
      const liveArtist = await getArtist(lidarrArtistId, { timeoutMs: 12000 });
      if (liveArtist && !liveArtist.monitored) {
        await setArtistMonitored(lidarrArtistId, true, { artist: liveArtist, timeoutMs: 15000 });
      }
    }

    const liveTrackFileCount = Number(album?.statistics?.trackFileCount || 0);
    const liveMonitored = Boolean(album?.monitored);
    const acquisition = baseReason?.acquisition && typeof baseReason.acquisition === 'object' ? baseReason.acquisition : {};
    const searchAttempts = Math.max(0, Number(acquisition.searchAttempts || 0));
    const monitorRepairCount = Math.max(0, Number(acquisition.monitorRepairCount || 0));
    const fallbackAttempts = getManualFallbackAttempts();
    const fallbackDelayMs = getManualFallbackDelayMs();
    const commandId = Number(trackedAlbum?.commandId || acquisition.lastCommandId || 0);
    let liveCommand = null;

    if (liveTrackFileCount > 0) {
      const nextProgress = {
        artistName,
        lidarrArtistId: lidarrArtistId || null,
        currentStage: 'album_acquired',
        albumsAddedCount: Number(existingProgress?.albumsAddedCount || 0),
        highestObservedRank: normalizeStoredRank(existingProgress?.highestObservedRank, 0),
        lastAlbumAddedAt: existingProgress?.lastAlbumAddedAt ?? now,
        nextReviewAt: now + WEEK_MS,
        lastManualSearchAt: existingProgress?.lastManualSearchAt ?? null,
        lastManualSearchStatus: 'completed',
        updatedAt: now,
      };
      saveLidarrArtistProgress(db, userPlexId, nextProgress);
      if (existingSuggestion) {
        setSuggestedArtistStatus(db, userPlexId, artistName, persistedSuggestionStatus, {
          reason: {
            ...baseReason,
            acquisition: {
              ...acquisition,
              lastRecoveryStatus: 'downloaded',
              searchAttempts,
              monitorRepairCount,
              lastCheckedAt: now,
            },
          },
          lidarrArtistId: existingProgress?.lidarrArtistId || existingSuggestion?.lidarrArtistId || null,
        });
      }
      return { ok: true, status: 'downloaded', album, progress: nextProgress };
    }

    if (commandId > 0) {
      try {
        liveCommand = await getCommand(commandId, { timeoutMs: 10000 });
      } catch (_err) {
        liveCommand = null;
      }
    }
    const liveCommandStatus = String(liveCommand?.status || '').trim().toLowerCase();
    if (liveCommandStatus === 'queued' || liveCommandStatus === 'started') {
      return {
        ok: true,
        status: liveCommandStatus === 'started' ? 'search_running' : 'search_queued',
        album,
        command: liveCommand,
        progress: existingProgress,
      };
    }

    let repairedMonitoring = false;
    if (!liveMonitored) {
      await setAlbumMonitored(albumId, true);
      await tagCuratorrManagedItems({
        sourceKind: trackedAlbum?.sourceKind || baseReason?.artistAddedSourceKind || baseReason?.requestSourceKind || 'automatic',
        albumId,
        tagAlbum: true,
      });
      repairedMonitoring = true;
    }

    const canRetrySearch = force || searchAttempts < fallbackAttempts;
    const canUseFallback = force || searchAttempts >= fallbackAttempts;
    let searchCommand = null;
    let grabbedRelease = null;
    let nextStatus = repairedMonitoring ? 'monitor_repaired' : 'no_files_found';
    let nextReviewAt = now + fallbackDelayMs;

    if (canRetrySearch) {
      try {
        searchCommand = await triggerAlbumSearch([albumId]);
        nextStatus = repairedMonitoring ? 'monitor_repaired_search_queued' : 'search_retry_queued';
      } catch (err) {
        nextStatus = 'search_failed';
        logEvent('error', 'acquisition.retry.error', `Retry AlbumSearch failed for ${userPlexId}: ${artistName}`, {
          albumId,
          error: safeMessage(err),
          code: err?.code || '',
        });
      }
    } else if (canUseFallback) {
      try {
        const releases = await searchAlbumReleases(albumId, { timeoutMs: 60000 });
        const bestRelease = pickBestRelease(releases);
        if (bestRelease) {
          await grabRelease(bestRelease, { timeoutMs: 20000 });
          grabbedRelease = bestRelease;
          nextStatus = 'manual_grab_queued';
        } else {
          nextStatus = 'manual_search_no_results';
        }
      } catch (err) {
        nextStatus = 'manual_search_failed';
        logEvent('error', 'acquisition.manual.error', `Manual fallback release grab failed for ${userPlexId}: ${artistName}`, {
          albumId,
          error: safeMessage(err),
          code: err?.code || '',
        });
      }
    }

    const nextProgress = {
      artistName,
      lidarrArtistId: lidarrArtistId || null,
      currentStage: nextStatus,
      albumsAddedCount: Number(existingProgress?.albumsAddedCount || 0),
      highestObservedRank: normalizeStoredRank(existingProgress?.highestObservedRank, 0),
      lastAlbumAddedAt: existingProgress?.lastAlbumAddedAt ?? null,
      nextReviewAt,
      lastManualSearchAt: (searchCommand || grabbedRelease) ? now : (existingProgress?.lastManualSearchAt ?? null),
      lastManualSearchStatus: searchCommand
        ? String(searchCommand?.status || 'queued').toLowerCase()
        : (grabbedRelease ? 'manual_grab_queued' : (existingProgress?.lastManualSearchStatus || '')),
      updatedAt: now,
    };
    saveLidarrArtistProgress(db, userPlexId, nextProgress);

    if (existingSuggestion) {
      setSuggestedArtistStatus(db, userPlexId, artistName, persistedSuggestionStatus, {
        reason: {
          ...baseReason,
          acquisition: {
            ...acquisition,
            searchAttempts: searchCommand ? (searchAttempts + 1) : searchAttempts,
            monitorRepairCount: repairedMonitoring ? (monitorRepairCount + 1) : monitorRepairCount,
            lastRecoveryStatus: nextStatus,
            lastCheckedAt: now,
            lastCommandId: Number(searchCommand?.id || 0) || null,
            manualFallbackReleaseTitle: grabbedRelease?.title || '',
            manualFallbackGuid: grabbedRelease?.guid || '',
          },
          latestAlbum: {
            ...(baseReason?.latestAlbum && typeof baseReason.latestAlbum === 'object' ? baseReason.latestAlbum : trackedAlbum),
            albumId,
            albumTitle: trackedAlbum?.albumTitle || album?.title || '',
            commandId: Number(searchCommand?.id || 0) || null,
          },
        },
          lidarrArtistId: lidarrArtistId || null,
      });
    }

    logEvent('info', 'acquisition.reconcile', `Reconciled Lidarr acquisition for ${userPlexId}: ${artistName}`, {
      albumId,
      repairedMonitoring,
      searchAttempts: searchCommand ? (searchAttempts + 1) : searchAttempts,
      nextStatus,
      manualFallbackRelease: grabbedRelease?.title || '',
      commandId: Number(searchCommand?.id || 0) || commandId || null,
    });

    return {
      ok: true,
      status: nextStatus,
      album,
      command: searchCommand || liveCommand,
      grabbedRelease,
      progress: nextProgress,
    };
  }

  async function reviewArtistProgression(options = {}) {
    const userPlexId = String(options.userPlexId || '').trim();
    const artistName = String(options.artistName || '').trim();
    const forcedRole = String(options.role || '').trim();
    const sourceKind = normalizeSourceKind(options.sourceKind || 'automatic');
    const force = options.force === true;
    const now = Date.now();
    if (!db) throw new Error('Database is not initialized');
    if (!userPlexId || !artistName) throw new Error('userPlexId and artistName are required');

    const settings = getSettings();
    if (!settings.automationEnabled || settings.automationScope === 'off') {
      return { ok: false, status: 'automation_disabled' };
    }

    const role = normalizeRole(forcedRole || resolveAutomationRoleForUserId(userPlexId));
    if (!['admin', 'co-admin', 'power-user'].includes(role)) {
      return { ok: false, status: 'role_ineligible', role };
    }

    const existingSuggestion = getSuggestedArtist(db, userPlexId, artistName);
    const existingProgress = getLidarrArtistProgress(db, userPlexId, artistName);
    const persistedSuggestionStatus = existingSuggestion?.status === 'dismissed' ? 'dismissed' : 'added_to_lidarr';
    const lidarrArtistId = Number(options.lidarrArtistId || existingProgress?.lidarrArtistId || existingSuggestion?.lidarrArtistId || 0);
    if (!lidarrArtistId) {
      return { ok: false, status: 'artist_not_linked', role };
    }

    const dueAt = resolveProgressReviewAt(existingProgress, now);
    if (!force && dueAt > now) {
      return {
        ok: true,
        status: 'not_due',
        role,
        lidarrArtistId,
        nextReviewAt: dueAt,
        progress: existingProgress,
      };
    }

    const acquisitionStage = String(existingProgress?.currentStage || '').trim().toLowerCase();
    if ([
      'starter_album_added',
      'starter_album_linked',
      'catalog_expanded',
      'search_finished',
      'search_failed',
      'manual_grab_queued',
      'search_retry_queued',
      'monitor_repaired',
      'monitor_repaired_search_queued',
      'no_files_found',
      'album_acquired',
    ].includes(acquisitionStage)) {
      const recovery = await reconcileArtistAcquisition({ userPlexId, artistName, role, force });
      if (recovery?.status && recovery.status !== 'no_tracked_album' && recovery.status !== 'downloaded') {
        return { ...recovery, role, lidarrArtistId };
      }
    }

    const rankSnapshot = getArtistRankSnapshot(db, userPlexId, artistName);
    const existingObservedRank = normalizeStoredRank(existingProgress?.highestObservedRank, 0);
    const rankingScore = normalizeStoredRank(rankSnapshot?.rankingScore, existingObservedRank);
    const highestObservedRank = Math.max(
      existingObservedRank,
      rankingScore,
    );
    const belterThreshold = getBelterThreshold();
    const baseReason = existingSuggestion?.reason && typeof existingSuggestion.reason === 'object' ? existingSuggestion.reason : {};
    const baseProgress = {
      artistName,
      lidarrArtistId,
      albumsAddedCount: Number(existingProgress?.albumsAddedCount || 0),
      highestObservedRank,
      lastAlbumAddedAt: existingProgress?.lastAlbumAddedAt ?? null,
      lastManualSearchAt: existingProgress?.lastManualSearchAt ?? null,
      lastManualSearchStatus: existingProgress?.lastManualSearchStatus || '',
      updatedAt: now,
    };

    logEvent('info', 'review.start', `Reviewing Lidarr progression for ${userPlexId}: ${artistName}`, {
      role,
      lidarrArtistId,
      force,
      rankingScore,
      belterThreshold,
      albumsAddedCount: baseProgress.albumsAddedCount,
    });

    if (rankingScore < belterThreshold) {
      const nextProgress = {
        ...baseProgress,
        currentStage: 'awaiting_belter',
        nextReviewAt: now + DAY_MS,
      };
      saveLidarrArtistProgress(db, userPlexId, nextProgress);
      if (existingSuggestion) {
        setSuggestedArtistStatus(db, userPlexId, artistName, persistedSuggestionStatus, {
          reason: {
            ...baseReason,
            albumWarning: null,
            catalogProgress: {
              currentStage: 'awaiting_belter',
              nextReviewAt: nextProgress.nextReviewAt,
              highestObservedRank,
              albumsAddedCount: baseProgress.albumsAddedCount,
            },
          },
          lidarrArtistId,
        });
      }
      return {
        ok: true,
        status: 'awaiting_belter',
        role,
        rankingScore,
        belterThreshold,
        nextReviewAt: nextProgress.nextReviewAt,
        progress: nextProgress,
      };
    }

    const albumList = await listArtistAlbums(lidarrArtistId, { timeoutMs: 15000, pageSize: 200 });
    const nextAlbumPick = pickNextExpansionAlbum(albumList);
    if (!nextAlbumPick?.album) {
      const nextProgress = {
        ...baseProgress,
        currentStage: 'catalog_complete',
        nextReviewAt: null,
      };
      saveLidarrArtistProgress(db, userPlexId, nextProgress);
      if (existingSuggestion) {
        setSuggestedArtistStatus(db, userPlexId, artistName, persistedSuggestionStatus, {
          reason: {
            ...baseReason,
            albumWarning: null,
            catalogProgress: {
              currentStage: 'catalog_complete',
              nextReviewAt: null,
              highestObservedRank,
              albumsAddedCount: baseProgress.albumsAddedCount,
            },
          },
          lidarrArtistId,
        });
      }
      return {
        ok: true,
        status: 'catalog_complete',
        role,
        rankingScore,
        progress: nextProgress,
      };
    }

    const album = nextAlbumPick.album;
    const albumId = Number(album.id || 0);
    const albumTitle = String(album.title || '').trim();
    const usage = getCurrentLidarrUsage(db, userPlexId).usage || {};
    let quota = getRoleQuota(role, usage);
    let searchCommand = null;
    try {
      quota = assertQuotaAvailable(role, usage, { albums: 1 });
      await setAlbumMonitored(albumId, true);
      await tagCuratorrManagedItems({
        sourceKind,
        albumId,
        tagAlbum: true,
      });
      recordLidarrUsage(db, userPlexId, { roleName: role, usageKey: 'albums', amount: 1, createdAt: now });
      quota = getRoleQuota(role, getCurrentLidarrUsage(db, userPlexId).usage || {});
      if (settings.autoTriggerManualSearch) {
        searchCommand = await triggerAlbumSearch([albumId]);
      }
    } catch (err) {
      if (err?.code === 'ALBUM_QUOTA_REACHED') {
        const nextProgress = {
          ...baseProgress,
          currentStage: 'quota_blocked',
          nextReviewAt: now + DAY_MS,
        };
        saveLidarrArtistProgress(db, userPlexId, nextProgress);
        if (existingSuggestion) {
          setSuggestedArtistStatus(db, userPlexId, artistName, 'quota_blocked', {
            reason: {
              ...baseReason,
              albumWarning: { type: 'album_quota', message: safeMessage(err) },
              catalogProgress: {
                currentStage: 'quota_blocked',
                nextReviewAt: nextProgress.nextReviewAt,
                highestObservedRank,
                albumsAddedCount: baseProgress.albumsAddedCount,
              },
            },
            lidarrArtistId,
          });
        }
        return {
          ok: false,
          status: 'quota_blocked',
          role,
          quota: err.quota || quota,
          nextReviewAt: nextProgress.nextReviewAt,
          progress: nextProgress,
        };
      }
      throw err;
    }

    upsertSuggestedAlbum(db, userPlexId, {
      artistName,
      albumTitle,
      albumType: String(album.albumType || ''),
      releaseDate: String(album.releaseDate || ''),
      selectionReason: nextAlbumPick.selectionReason,
      rankScore: Number(album?.ratings?.value || 0),
      status: 'added_to_lidarr',
      lidarrAlbumId: albumId || null,
      updatedAt: now,
    });

    const nextProgress = {
      ...baseProgress,
      currentStage: 'catalog_expanded',
      albumsAddedCount: baseProgress.albumsAddedCount + 1,
      lastAlbumAddedAt: now,
      nextReviewAt: now + getManualFallbackDelayMs(),
      lastManualSearchAt: searchCommand ? now : (existingProgress?.lastManualSearchAt ?? null),
      lastManualSearchStatus: searchCommand ? String(searchCommand?.status || 'queued').toLowerCase() : (existingProgress?.lastManualSearchStatus || ''),
    };
    saveLidarrArtistProgress(db, userPlexId, nextProgress);

    if (existingSuggestion) {
      setSuggestedArtistStatus(db, userPlexId, artistName, persistedSuggestionStatus, {
        reason: {
          ...baseReason,
          manualAction: 'catalog_review',
          manualActionAt: now,
          requestSourceKind: sourceKind,
          albumWarning: null,
          latestAlbum: {
            albumId,
            albumTitle,
            albumType: String(album.albumType || ''),
            releaseDate: String(album.releaseDate || ''),
            selectionReason: nextAlbumPick.selectionReason,
            commandId: Number(searchCommand?.id || 0) || null,
            sourceKind,
            addedByCuratorr: true,
          },
          catalogProgress: {
            currentStage: nextProgress.currentStage,
            nextReviewAt: nextProgress.nextReviewAt,
            highestObservedRank,
            albumsAddedCount: nextProgress.albumsAddedCount,
          },
        },
        lidarrArtistId,
      });
    }

    logEvent('info', 'review.success', `Expanded Lidarr catalog for ${userPlexId}: ${artistName} — ${albumTitle}`, {
      role,
      lidarrArtistId,
      lidarrAlbumId: albumId,
      selectionReason: nextAlbumPick.selectionReason,
      searchCommandId: Number(searchCommand?.id || 0) || null,
      quota,
      albumsAddedCount: nextProgress.albumsAddedCount,
    });

    return {
      ok: true,
      status: searchCommand ? 'search_queued' : 'album_added',
      role,
      quota,
      album: {
        albumId,
        albumTitle,
        albumType: String(album.albumType || ''),
        releaseDate: String(album.releaseDate || ''),
        selectionReason: nextAlbumPick.selectionReason,
      },
      commandId: Number(searchCommand?.id || 0) || null,
      rankingScore,
      progress: nextProgress,
    };
  }

  async function reviewDueArtists(options = {}) {
    if (!db) return [];
    const settings = getSettings();
    if (!settings.automationEnabled || settings.automationScope === 'off') return [];
    const now = Date.now();
    let due = listDueLidarrArtistReviews(db, {
      now,
      limit: Math.max(1, Math.min(100, Number(options.limit || 20))),
    });
    const requestedUserPlexId = String(options.userPlexId || '').trim();
    if (requestedUserPlexId) {
      due = due.filter((item) => String(item.userPlexId || '').trim() === requestedUserPlexId);
    }
    const results = [];
    for (const item of due) {
      try {
        const role = resolveAutomationRoleForUserId(item.userPlexId);
        if (!['admin', 'co-admin', 'power-user'].includes(role)) continue;
        const result = await reviewArtistProgression({
          userPlexId: item.userPlexId,
          artistName: item.artistName,
          role,
          force: false,
          lidarrArtistId: item.lidarrArtistId,
        });
        results.push({
          userPlexId: item.userPlexId,
          artistName: item.artistName,
          ...result,
        });
      } catch (err) {
        logEvent('error', 'review.error', `Background Lidarr review failed for ${item.userPlexId}: ${item.artistName}`, {
          error: safeMessage(err),
          lidarrArtistId: item.lidarrArtistId || null,
        });
      }
    }
    return results;
  }

  async function addArtistFromSuggestion(artistName, options = {}) {
    logEvent('info', 'artist.add.start', `Starting Lidarr artist add flow for ${artistName}`, {
      searchForMissingAlbums: Boolean(options.searchForMissingAlbums),
    });
    try {
      const match = options.lookupArtistResult && typeof options.lookupArtistResult === 'object'
        ? options.lookupArtistResult
        : pickLookupArtist(await lookupArtist(artistName), artistName, { foreignArtistId: options.foreignArtistId });
      if (!match) {
        const err = new Error(`No Lidarr match found for ${artistName}.`);
        err.code = 'ARTIST_NOT_FOUND';
        throw err;
      }

      if (match.added && Number(match.id || 0) > 0) {
        const existingArtistId = Number(match.id || 0);
        if (existingArtistId > 0) {
          try {
            const liveArtist = await getArtist(existingArtistId, { timeoutMs: 12000 });
            if (liveArtist && !liveArtist.monitored) {
              await setArtistMonitored(existingArtistId, true, { artist: liveArtist, timeoutMs: 15000 });
            }
          } catch (err) {
            logEvent('warn', 'artist.monitor.repair.error', `Failed to repair monitoring for existing Lidarr artist ${artistName}`, {
              lidarrArtistId: existingArtistId,
              error: safeMessage(err),
              code: err?.code || '',
            });
          }
        }
        logEvent('info', 'artist.add.exists', `Artist already exists in Lidarr: ${artistName}`, {
          lidarrArtistId: existingArtistId,
        });
        return {
          created: false,
          existing: true,
          artistId: existingArtistId,
          artistName: String(match.artistName || artistName),
          payload: match,
        };
      }

      const rootFolders = await getRootFolders();
      const rootFolder = rootFolders.find((item) => item && item.accessible !== false) || rootFolders[0];
      if (!rootFolder?.path) {
        const err = new Error('No accessible Lidarr root folder is configured.');
        err.code = 'ROOT_FOLDER_MISSING';
        throw err;
      }

      const qualityProfiles = await getQualityProfiles();
      const qualityProfileId = Number(match.qualityProfileId || rootFolder.defaultQualityProfileId || qualityProfiles[0]?.id || 0);
      const metadataProfileId = Number(match.metadataProfileId || rootFolder.defaultMetadataProfileId || 1);
      if (!qualityProfileId) {
        const err = new Error('No Lidarr quality profile is configured.');
        err.code = 'QUALITY_PROFILE_MISSING';
        throw err;
      }

      const payload = {
        ...match,
        qualityProfileId,
        metadataProfileId,
        rootFolderPath: String(rootFolder.path || ''),
        path: buildArtistPath(rootFolder.path, match),
        monitored: true,
        monitorNewItems: 'none',
        addOptions: {
          monitor: 'none',
          searchForMissingAlbums: Boolean(options.searchForMissingAlbums),
        },
      };
      logEvent('info', 'artist.add.prepare', `Prepared Lidarr add payload for ${artistName}`, {
        qualityProfileId,
        metadataProfileId,
        rootFolderPath: payload.rootFolderPath,
        path: payload.path,
      });
      const created = await request('/artist', {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      const result = {
        created: true,
        existing: false,
        artistId: Number(created?.id || payload.id || 0),
        artistName: String(created?.artistName || match.artistName || artistName),
        payload: created || payload,
      };
      if (result.artistId > 0) {
        try {
          const liveArtist = await getArtist(result.artistId, { timeoutMs: 12000 });
          if (liveArtist && !liveArtist.monitored) {
            await setArtistMonitored(result.artistId, true, { artist: liveArtist, timeoutMs: 15000 });
          }
        } catch (err) {
          logEvent('warn', 'artist.monitor.repair.error', `Failed to confirm monitoring for newly added Lidarr artist ${result.artistName}`, {
            lidarrArtistId: result.artistId,
            error: safeMessage(err),
            code: err?.code || '',
          });
        }
      }
      logEvent('info', 'artist.add.success', `Added artist to Lidarr: ${result.artistName}`, {
        lidarrArtistId: result.artistId,
      });
      return result;
    } catch (err) {
      logEvent('error', 'artist.add.error', `Lidarr artist add failed for ${artistName}`, {
        error: safeMessage(err),
        code: err?.code || '',
      });
      throw err;
    }
  }

  async function executeArtistAlbumRequest(options = {}) {
    const userPlexId = String(options.userPlexId || '').trim();
    const artistName = String(options.artistName || '').trim();
    const foreignArtistId = String(options.foreignArtistId || '').trim();
    const preferredAlbumTitle = String(options.preferredAlbumTitle || '').trim();
    const role = normalizeRole(options.role || resolveAutomationRoleForUserId(userPlexId));
    const sourceKind = normalizeSourceKind(options.sourceKind);
    const autoAdd = options.autoAdd === true;
    const requestId = Number(options.requestId || 0) || null;
    if (!userPlexId || !artistName) throw new Error('userPlexId and artistName are required');

    const existingSuggestion = getSuggestedArtist(db, userPlexId, artistName);
    const existingProgress = getLidarrArtistProgress(db, userPlexId, artistName);
    const persistedSuggestionStatus = existingSuggestion?.status === 'dismissed' ? 'dismissed' : 'added_to_lidarr';
    const now = Date.now();
    const autoTriggerManualSearch = Boolean(getSettings().autoTriggerManualSearch);
    const fallbackDelayMs = getManualFallbackDelayMs();
    const rankSnapshot = getArtistRankSnapshot(db, userPlexId, artistName);
    const normalizedObservedRank = (() => {
      const current = Number(rankSnapshot?.rankingScore);
      if (Number.isFinite(current) && current >= 0 && current <= 10) return current;
      const previous = Number(existingProgress?.highestObservedRank);
      return Number.isFinite(previous) && previous >= 0 && previous <= 10 ? previous : 0;
    })();
    let quota = getRoleQuota(role, getCurrentLidarrUsage(db, userPlexId).usage || {});
    let lookupMatch = options.lookupArtistResult && typeof options.lookupArtistResult === 'object'
      ? options.lookupArtistResult
      : pickLookupArtist(await lookupArtist(artistName), artistName, { foreignArtistId });

    if (!lookupMatch) {
      const err = new Error(`No Lidarr match found for ${artistName}.`);
      err.code = 'ARTIST_NOT_FOUND';
      throw err;
    }

    const alreadyExists = Boolean(lookupMatch?.added && Number(lookupMatch?.id || 0) > 0);
    const usage = getCurrentLidarrUsage(db, userPlexId).usage || {};
    if (!alreadyExists) quota = assertQuotaAvailable(role, usage, { artists: 1 });
    if (autoAdd && !alreadyExists) assertAutoAddQuotaAvailable(usage, { artists: 1 });

    const lidarrResult = await addArtistFromSuggestion(artistName, {
      searchForMissingAlbums: false,
      foreignArtistId,
      lookupArtistResult: lookupMatch,
    });
    if (!lidarrResult.existing) {
      await tagCuratorrManagedItems({
        sourceKind,
        artistId: lidarrResult.artistId,
        tagArtist: true,
      });
      recordLidarrUsage(db, userPlexId, { roleName: role, usageKey: 'artists', amount: 1, createdAt: now });
      if (autoAdd) {
        recordLidarrUsage(db, userPlexId, { roleName: role, usageKey: 'auto_artists', amount: 1, createdAt: now });
      }
      quota = getRoleQuota(role, getCurrentLidarrUsage(db, userPlexId).usage || {});
    }

    let albumList = [];
    try {
      albumList = await listArtistAlbums(lidarrResult.artistId, { timeoutMs: 15000, pageSize: 200 });
    } catch (err) {
      logEvent('warn', 'album.list.retry', `Initial album list failed for ${artistName}, retrying`, {
        lidarrArtistId: lidarrResult.artistId,
        error: safeMessage(err),
      });
      await wait(1200);
      albumList = await listArtistAlbums(lidarrResult.artistId, { timeoutMs: 20000, pageSize: 200 });
    }

    const pickedAlbum = pickPreferredAlbum(albumList, preferredAlbumTitle);
    if (!pickedAlbum?.album) {
      const err = new Error('No starter album could be selected.');
      err.code = 'ALBUM_NOT_FOUND';
      throw err;
    }

    const album = pickedAlbum.album;
    const albumId = Number(album.id || 0);
    const albumTitle = String(album.title || '').trim();
    const alreadyMonitored = Boolean(album.monitored);
    const albumSourceKind = pickedAlbum.selectionReason.startsWith('fallback_') ? 'automatic' : sourceKind;
    if (!alreadyMonitored) {
      quota = assertQuotaAvailable(role, getCurrentLidarrUsage(db, userPlexId).usage || {}, { albums: 1 });
      if (autoAdd) assertAutoAddQuotaAvailable(getCurrentLidarrUsage(db, userPlexId).usage || {}, { albums: 1 });
      await setAlbumMonitored(albumId, true);
      await tagCuratorrManagedItems({
        sourceKind: albumSourceKind,
        albumId,
        tagAlbum: true,
      });
      recordLidarrUsage(db, userPlexId, { roleName: role, usageKey: 'albums', amount: 1, createdAt: Date.now() });
      if (autoAdd) {
        recordLidarrUsage(db, userPlexId, { roleName: role, usageKey: 'auto_albums', amount: 1, createdAt: Date.now() });
      }
      quota = getRoleQuota(role, getCurrentLidarrUsage(db, userPlexId).usage || {});
    }

    let searchCommand = null;
    if (autoTriggerManualSearch) {
      searchCommand = await triggerAlbumSearch([albumId]);
    }

    upsertSuggestedAlbum(db, userPlexId, {
      artistName,
      albumTitle,
      albumType: String(album.albumType || ''),
      releaseDate: String(album.releaseDate || ''),
      selectionReason: pickedAlbum.selectionReason,
      rankScore: Number(album?.ratings?.value || 0),
      status: alreadyMonitored ? 'already_monitored' : 'added_to_lidarr',
      lidarrAlbumId: albumId || null,
      updatedAt: Date.now(),
    });

    const nextProgress = {
      artistName,
      lidarrArtistId: lidarrResult.artistId || existingProgress?.lidarrArtistId || existingSuggestion?.lidarrArtistId || null,
      currentStage: alreadyMonitored ? 'starter_album_linked' : 'starter_album_added',
      albumsAddedCount: alreadyMonitored
        ? Math.max(Number(existingProgress?.albumsAddedCount || 0), 1)
        : Number(existingProgress?.albumsAddedCount || 0) + 1,
      highestObservedRank: normalizedObservedRank,
      lastAlbumAddedAt: alreadyMonitored ? (existingProgress?.lastAlbumAddedAt ?? null) : Date.now(),
      nextReviewAt: Date.now() + fallbackDelayMs,
      lastManualSearchAt: searchCommand ? Date.now() : (existingProgress?.lastManualSearchAt ?? null),
      lastManualSearchStatus: searchCommand ? 'queued' : (existingProgress?.lastManualSearchStatus || ''),
      updatedAt: Date.now(),
    };
    saveLidarrArtistProgress(db, userPlexId, nextProgress);

    const baseReason = existingSuggestion?.reason && typeof existingSuggestion.reason === 'object' ? existingSuggestion.reason : {};
    const nextReason = {
      ...baseReason,
      manualAction: sourceKind === 'automatic' ? 'queued_to_lidarr' : 'manual_add',
      manualActionAt: Date.now(),
      requestSourceKind: sourceKind,
      lidarrExisting: Boolean(lidarrResult.existing),
      artistAddedByCuratorr: !lidarrResult.existing,
      artistAddedSourceKind: !lidarrResult.existing ? sourceKind : (baseReason?.artistAddedSourceKind || ''),
      starterAlbum: {
        albumId,
        albumTitle,
        albumType: String(album.albumType || ''),
        releaseDate: String(album.releaseDate || ''),
        selectionReason: pickedAlbum.selectionReason,
        alreadyMonitored,
        commandId: Number(searchCommand?.id || 0) || null,
        sourceKind: albumSourceKind,
        addedByCuratorr: !alreadyMonitored,
        requestedAlbumTitle: preferredAlbumTitle || '',
        fellBackFromManualChoice: Boolean(preferredAlbumTitle) && albumSourceKind === 'automatic',
      },
      albumWarning: null,
    };
    if (existingSuggestion) {
      setSuggestedArtistStatus(db, userPlexId, artistName, persistedSuggestionStatus, {
        reason: nextReason,
        lidarrArtistId: lidarrResult.artistId || null,
      });
    }

    if (requestId) {
      updateLidarrRequest(db, requestId, {
        status: 'completed',
        lidarrArtistId: lidarrResult.artistId || null,
        lidarrAlbumId: albumId || null,
        processedAt: Date.now(),
        detail: {
          preferredAlbumTitle,
          selectedAlbumTitle: albumTitle,
          selectionReason: pickedAlbum.selectionReason,
          searchCommandId: Number(searchCommand?.id || 0) || null,
          alreadyMonitored,
          fallbackUsed: pickedAlbum.selectionReason.startsWith('fallback_'),
          requestSource: sourceKind,
          albumSource: albumSourceKind,
        },
      }, userPlexId);
    }

    return {
      ok: true,
      status: 'completed',
      lidarr: lidarrResult,
      album: {
        albumId,
        albumTitle,
        albumType: String(album.albumType || ''),
        releaseDate: String(album.releaseDate || ''),
        selectionReason: pickedAlbum.selectionReason,
        alreadyMonitored,
      },
      commandId: Number(searchCommand?.id || 0) || null,
      quota,
      progress: nextProgress,
    };
  }

  async function queueArtistAlbumRequest(options = {}) {
    const userPlexId = String(options.userPlexId || '').trim();
    const artistName = String(options.artistName || '').trim();
    const sourceKind = normalizeSourceKind(options.sourceKind);
    if (!userPlexId || !artistName) throw new Error('userPlexId and artistName are required');
    const request = enqueueLidarrRequest(db, userPlexId, {
      sourceKind,
      requestKind: 'artist_album',
      artistName,
      albumTitle: String(options.preferredAlbumTitle || ''),
      foreignArtistId: String(options.foreignArtistId || ''),
      status: 'queued',
      detail: {
        preferredAlbumTitle: String(options.preferredAlbumTitle || ''),
        allowCuratorrFallback: options.allowCuratorrFallback !== false,
        autoAdd: options.autoAdd === true,
        note: String(options.note || ''),
      },
    });
    const existingSuggestion = getSuggestedArtist(db, userPlexId, artistName);
    if (existingSuggestion) {
      setSuggestedArtistStatus(db, userPlexId, artistName, 'queued_for_lidarr', {
        reason: {
          ...(existingSuggestion.reason || {}),
          queuedRequestId: request.id,
          manualAction: 'queued_for_lidarr',
          manualActionAt: Date.now(),
          requestSourceKind: sourceKind,
          queuedSourceKind: sourceKind,
        },
      });
    }
    return request;
  }

  // Minimum totalScore an artist must have to qualify for automatic queuing.
  const AUTO_ADD_MIN_SCORE = 7.0;

  async function autoQueueSuggestedArtists(options = {}) {
    if (!db) return [];
    const settings = getSettings();
    if (!settings.automationEnabled || !settings.autoAddArtists || settings.automationScope === 'off' || !isConfigured()) return [];
    const perUserLimit = Math.max(1, Math.min(5, Number(options.perUserLimit || 1)));
    const userIds = listUsersWithSuggestedArtists(db);
    const results = [];
    let existingArtistsByName = null;
    try {
      const currentArtists = await listArtists({ pageSize: 2000, timeoutMs: 15000 });
      existingArtistsByName = new Map(
        currentArtists
          .map((artist) => [normalizeTitle(artist?.artistName), artist])
          .filter(([name]) => Boolean(name))
      );
    } catch (err) {
      logEvent('warn', 'auto_add.artist_list_failed', 'Failed to fetch current Lidarr artist list before auto-queueing.', {
        error: safeMessage(err),
      });
    }
    for (const userPlexId of userIds) {
      const role = resolveAutomationRoleForUserId(userPlexId);
      if (!['admin', 'co-admin', 'power-user'].includes(role)) continue;
      const usage = getCurrentLidarrUsage(db, userPlexId).usage || {};
      const quota = getRoleQuota(role, usage);
      const pendingReservations = getPendingAutoAddReservations(userPlexId);
      const autoQuotaUsage = {
        ...usage,
        auto_artists: Math.max(0, Number(usage.auto_artists || 0)) + pendingReservations.artists,
        auto_albums: Math.max(0, Number(usage.auto_albums || 0)) + pendingReservations.albums,
      };
      const autoQuota = getAutoAddQuota(autoQuotaUsage);
      if (!quota.weeklyArtists.unlimited && quota.weeklyArtists.used >= quota.weeklyArtists.limit) {
        logEvent('info', 'auto_add.quota_reached', `Auto-add skipped for ${userPlexId}: artist quota reached (${quota.weeklyArtists.used}/${quota.weeklyArtists.limit})`);
        continue;
      }
      if (!autoQuota.weeklyArtists.unlimited && autoQuota.weeklyArtists.used >= autoQuota.weeklyArtists.limit) {
        logEvent('info', 'auto_add.auto_quota_reached', `Auto-add skipped for ${userPlexId}: automatic artist quota reached (${autoQuota.weeklyArtists.used}/${autoQuota.weeklyArtists.limit})`);
        continue;
      }
      if (!autoQuota.weeklyAlbums.unlimited && autoQuota.weeklyAlbums.used >= autoQuota.weeklyAlbums.limit) {
        logEvent('info', 'auto_add.auto_album_quota_reached', `Auto-add skipped for ${userPlexId}: automatic album quota reached (${autoQuota.weeklyAlbums.used}/${autoQuota.weeklyAlbums.limit})`);
        continue;
      }
      const candidates = listSuggestedArtists(db, userPlexId, { status: 'suggested', limit: 25 })
        .filter((s) => Number(s.totalScore || 0) >= AUTO_ADD_MIN_SCORE);
      let queued = 0;
      for (const candidate of candidates) {
        if (queued >= perUserLimit) break;
        try {
          const existingArtist = existingArtistsByName?.get(normalizeTitle(candidate.artistName)) || null;
          if (existingArtist) {
            setSuggestedArtistStatus(db, userPlexId, candidate.artistName, 'already_in_lidarr', {
              reason: {
                ...(candidate.reason || {}),
                manualAction: 'already_in_lidarr',
                manualActionAt: Date.now(),
                lidarrExisting: true,
              },
              lidarrArtistId: Number(existingArtist.id || 0) || null,
            });
            logEvent('info', 'auto_add.skip_existing', `Auto-queue skipped for ${userPlexId}: ${candidate.artistName} already exists in Lidarr.`, {
              lidarrArtistId: Number(existingArtist.id || 0) || null,
            });
            continue;
          }
          const request = await queueArtistAlbumRequest({
            userPlexId,
            artistName: candidate.artistName,
            sourceKind: 'automatic',
            autoAdd: true,
            allowCuratorrFallback: true,
          });
          logEvent('info', 'auto_add.queued', `Auto-queued artist for ${userPlexId}: ${candidate.artistName}`, {
            totalScore: candidate.totalScore,
            requestId: request.id,
          });
          results.push({ userPlexId, artistName: candidate.artistName, requestId: request.id, totalScore: candidate.totalScore });
          queued++;
        } catch (err) {
          logEvent('warn', 'auto_add.error', `Auto-queue failed for ${userPlexId}: ${candidate.artistName}`, {
            error: safeMessage(err),
          });
        }
      }
    }
    return results;
  }

  async function processQueuedRequests(options = {}) {
    if (!db) return [];
    const settings = getSettings();
    if (!settings.automationEnabled || settings.automationScope === 'off' || !isConfigured()) return [];
    const requestedUserPlexId = String(options.userPlexId || '').trim();
    const limit = Math.max(1, Math.min(50, Number(options.limit || 10)));
    let queued = listLidarrRequests(db, requestedUserPlexId, { statuses: ['queued'], limit: requestedUserPlexId ? limit : 500 });
    if (!requestedUserPlexId) queued = queued.slice(0, limit);
    const results = [];
    for (const request of queued) {
      const role = resolveAutomationRoleForUserId(request.userPlexId);
      if (!['admin', 'co-admin', 'power-user'].includes(role)) continue;
      try {
        updateLidarrRequest(db, request.id, { status: 'processing' }, request.userPlexId);
        const result = await executeArtistAlbumRequest({
          userPlexId: request.userPlexId,
          role,
          artistName: request.artistName,
          foreignArtistId: request.foreignArtistId,
          preferredAlbumTitle: request.albumTitle,
          sourceKind: request.sourceKind,
          autoAdd: request.detail?.autoAdd === true,
          requestId: request.id,
        });
        results.push({ requestId: request.id, ...result, artistName: request.artistName });
      } catch (err) {
        if ([
          'ARTIST_QUOTA_REACHED',
          'ALBUM_QUOTA_REACHED',
          'AUTO_ARTIST_QUOTA_REACHED',
          'AUTO_ALBUM_QUOTA_REACHED',
        ].includes(String(err?.code || ''))) {
          updateLidarrRequest(db, request.id, {
            status: 'queued',
            detail: {
              ...(request.detail || {}),
              lastError: safeMessage(err),
              lastErrorCode: err?.code || '',
            },
          }, request.userPlexId);
          continue;
        }
        updateLidarrRequest(db, request.id, {
          status: 'failed',
          processedAt: Date.now(),
          detail: {
            ...(request.detail || {}),
            lastError: safeMessage(err),
            lastErrorCode: err?.code || '',
          },
        }, request.userPlexId);
        results.push({
          requestId: request.id,
          ok: false,
          status: 'failed',
          artistName: request.artistName,
          error: safeMessage(err),
        });
      }
    }
    return results;
  }

  return {
    getSettings,
    isConfigured,
    buildRequest,
    request,
    pickBestRelease,
    getRoleQuota,
    assertQuotaAvailable,
    getRootFolders,
    getQualityProfiles,
    lookupArtist,
    listArtists,
    pickLookupArtist,
    listArtistAlbums,
    pickStarterAlbum,
    pickPreferredAlbum,
    previewManualArtistAlbums,
    setAlbumMonitored,
    triggerAlbumSearch,
    getCommand,
    getAlbum,
    getArtist,
    setArtistMonitored,
    searchAlbumReleases,
    grabRelease,
    resolveAutomationRoleForUserId,
    reconcileArtistAcquisition,
    reviewArtistProgression,
    reviewDueArtists,
    tagCuratorrManagedItems,
    executeArtistAlbumRequest,
    queueArtistAlbumRequest,
    processQueuedRequests,
    autoQueueSuggestedArtists,
    addArtistFromSuggestion,
  };
}
