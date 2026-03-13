// Page routes — HTML views

import {
  getPlayStats,
  getTopArtists,
  getTopTracks,
  getRecentHistory,
  getLastPlaylistSync,
  getPlaylistJob,
  getPlayStatsByDay,
  getCompletedTrackKeys,
  getCurrentLidarrUsage,
  listLidarrArtistProgress,
  listLidarrRequests,
  getArtistsFromMaster,
  dedupeMasterArtistNames,
  getResolvedUserArtistFilters,
  getUserPreferences,
} from '../db.js';

// Returns the DB filter key for a user:
// - local admin accounts can inspect global activity
// - Plex-backed accounts, including admins, stay scoped to their Plex username
export function resolveUserFilter(user, role) {
  const source = String(user?.source || '').trim().toLowerCase();
  if (role === 'admin' && source === 'local') return '';
  return String(user.username || '').trim();
}

function stripArtistSuffix(title, artist) {
  if (!title || !artist) return title || '';
  const suffix = ' - ' + artist;
  return title.endsWith(suffix) ? title.slice(0, -suffix.length) : title;
}

export function registerPages(app, ctx) {
  const {
    requireUser,
    requireAdmin,
    requireWizardComplete,
    requireUserWizardComplete,
    loadConfig,
    getActualRole,
    getEffectiveRole,
    canUserAccessLidarrAutomation,
    db,
    recommendationService,
    playlistService,
    lidarrService,
  } = ctx;

  // Root redirect
  app.get('/', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    return res.redirect('/dashboard');
  });

  // ── Dashboard ─────────────────────────────────────────────────────────────

  app.get('/dashboard', requireUser, requireWizardComplete, requireUserWizardComplete, async (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const role = getEffectiveRole(req);
    const userPlexId = resolveUserFilter(user, role);
    const suggestionUserId = String(user.username || '').trim();

    const now = Date.now();
    const since7d = now - 7 * 24 * 60 * 60 * 1000;
    const since30d = now - 30 * 24 * 60 * 60 * 1000;

    const normalizeStats = (r) => ({
      plays: r?.total_plays || 0,
      skips: r?.total_skips || 0,
      skipRate: r?.total_plays ? (r.total_skips || 0) / r.total_plays : 0,
      uniqueArtists: r?.unique_artists || 0,
      uniqueTracks: r?.unique_tracks || 0,
      totalListenMs: r?.total_listen_ms || 0,
    });
    const stats7d = normalizeStats(getPlayStats(db, userPlexId, since7d));
    const stats30d = normalizeStats(getPlayStats(db, userPlexId, since30d));
    const byDayRaw = getPlayStatsByDay(db, userPlexId, 14);
    const byDayMap = Object.fromEntries(byDayRaw.map((r) => [r.day, r]));
    const byDay = Array.from({ length: 14 }, (_, i) => {
      const d = new Date(now - (13 - i) * 24 * 60 * 60 * 1000);
      const key = d.toISOString().slice(0, 10);
      const label = d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
      return byDayMap[key] ? { ...byDayMap[key], label } : { day: key, plays: 0, skips: 0, label };
    });
    const topArtists = getTopArtists(db, userPlexId, 5).map((artist) => ({
      ...artist,
      curatorrTier: deriveArtistTier(artist, config),
    }));
    const topTracks = getTopTracks(db, userPlexId, 5).map((track) => ({
      ...track,
      track_title: stripArtistSuffix(track.track_title, track.artist_name),
      curatorrTier: deriveTrackTier(track),
    }));
    const recentHistory = getRecentHistory(db, userPlexId, 10).map((event) => ({
      ...event,
      track_title: stripArtistSuffix(event.track_title, event.artist_name),
      curatorrTier: deriveHistoryTier(event, config),
    }));
    const lastSync = getLastPlaylistSync(db, userPlexId);
    const dashboardSuggestions = loadSuggestionBundle(recommendationService, suggestionUserId, { artistLimit: 8 });
    const lidarrStatus = await buildLidarrStatusBundle(db, lidarrService, suggestionUserId, dashboardSuggestions.artists);
    const lidarrAutomationEligible = canUserAccessLidarrAutomation(loadConfig(), { ...req.session.user, role });
    const lidarrQuota = lidarrAutomationEligible && lidarrService
      ? lidarrService.getRoleQuota(role, getCurrentLidarrUsage(db, suggestionUserId).usage || {})
      : null;

    res.render('dashboard', {
      title: 'Dashboard — Curatorr',
      user,
      role,
      actualRole: getActualRole(req),
      config: safeConfig(config),
      stats7d,
      stats30d,
      byDay,
      topArtists,
      topTracks,
      recentHistory,
      lastSync,
      lidarrStatus,
      lidarrQuota,
      lidarrAutomationEligible,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── History ───────────────────────────────────────────────────────────────

  app.get('/history', requireUser, requireWizardComplete, requireUserWizardComplete, (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const userPlexId = resolveUserFilter(user, getEffectiveRole(req));
    const page = Math.max(0, Number(req.query?.page || 0));
    const limit = 50;
    const history = getRecentHistory(db, userPlexId, limit, page * limit).map((event) => ({
      ...event,
      track_title: stripArtistSuffix(event.track_title, event.artist_name),
      curatorrTier: deriveHistoryTier(event, config),
    }));

    res.render('history', {
      title: 'Play History — Curatorr',
      user,
      role: getEffectiveRole(req),
      actualRole: getActualRole(req),
      config: safeConfig(config),
      history,
      page,
      limit,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── Artists ───────────────────────────────────────────────────────────────

  app.get('/artists', requireUser, requireWizardComplete, requireUserWizardComplete, async (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const role = getEffectiveRole(req);
    const userPlexId = resolveUserFilter(user, role);
    const suggestionUserId = String(user.username || '').trim();
    const artists = getTopArtists(db, userPlexId, 500).map((artist) => ({
      ...artist,
      curatorrTier: deriveArtistTier(artist, config),
    }));
    let suggestions = loadSuggestionBundle(recommendationService, suggestionUserId, { artistLimit: 16 });
    if (recommendationService && suggestionUserId) {
      try {
        const rebuilt = recommendationService.rebuildSuggestionsForUser(suggestionUserId, { artistLimit: 16 });
        suggestions = rebuilt?.cached || suggestions;
      } catch (_err) {
        // Fall back to cached suggestions if the automatic rebuild fails.
      }
    }
    const lidarrStatus = await buildLidarrStatusBundle(db, lidarrService, suggestionUserId, suggestions.artists);
    const lidarrAutomationEligible = canUserAccessLidarrAutomation(loadConfig(), { ...req.session.user, role });
    const lidarrQuota = lidarrAutomationEligible && lidarrService
      ? lidarrService.getRoleQuota(role, getCurrentLidarrUsage(db, suggestionUserId).usage || {})
      : null;

    res.render('artists', {
      title: 'Artists — Curatorr',
      user,
      role,
      actualRole: getActualRole(req),
      config: safeConfig(config),
      artists,
      suggestedArtists: lidarrStatus.actionableSuggestions,
      lidarrStatus,
      lidarrAutomationEligible,
      lidarrQuota,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── Discover ─────────────────────────────────────────────────────────────

  app.get('/discover', requireUser, requireWizardComplete, requireUserWizardComplete, async (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const role = getEffectiveRole(req);
    const userPlexId = String(user.username || '').trim();
    const lidarrAutomationEligible = canUserAccessLidarrAutomation(loadConfig(), { ...req.session.user, role });
    const lidarrQuota = lidarrService?.isConfigured() && lidarrAutomationEligible
      ? lidarrService.getRoleQuota(role, getCurrentLidarrUsage(db, userPlexId).usage || {})
      : null;
    const queuedRequests = listLidarrRequests(db, userPlexId, { statuses: ['queued', 'processing'], limit: 200 });
    const requestHistory = listLidarrRequests(db, userPlexId, { statuses: ['completed', 'failed'], limit: 50 });
    const lidarrStatus = await buildLidarrStatusBundle(db, lidarrService, userPlexId, []);
    const disc = config.discovery || {};
    const discoveryConfig = {
      enabled: Boolean(disc.lastfmApiKey),
      showTrendingArtists: disc.lastfmApiKey ? (disc.showTrendingArtists ?? true) : false,
      showTrendingTracks:  disc.lastfmApiKey ? (disc.showTrendingTracks  ?? true) : false,
      showSimilarArtists:  disc.lastfmApiKey ? (disc.showSimilarArtists  ?? true) : false,
    };

    res.render('discover', {
      title: 'Discover — Curatorr',
      user,
      role,
      actualRole: getActualRole(req),
      config: safeConfig(config),
      lidarrAutomationEligible,
      lidarrQuota,
      queuedRequests,
      requestHistory,
      lidarrStatus,
      discoveryConfig,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── Tracks ────────────────────────────────────────────────────────────────

  app.get('/tracks', requireUser, requireWizardComplete, requireUserWizardComplete, (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const role = getEffectiveRole(req);
    const userPlexId = resolveUserFilter(user, role);
    const suggestionUserId = String(user.username || '').trim();
    const tracks = getTopTracks(db, userPlexId, 500).map((track) => ({
      ...track,
      track_title: stripArtistSuffix(track.track_title, track.artist_name),
      curatorrTier: deriveTrackTier(track),
    }));
    const smartSettings = config.smartPlaylist || {};
    const completionThresholdMs = (Number(smartSettings.completionThresholdSeconds) || 20) * 1000;
    const completedKeys = getCompletedTrackKeys(db, userPlexId, completionThresholdMs);
    const suggestions = loadSuggestionBundle(recommendationService, suggestionUserId, {
      trackLimit: 10,
      albumLimit: 8,
    });

    res.render('tracks', {
      title: 'Tracks — Curatorr',
      user,
      role,
      actualRole: getActualRole(req),
      config: safeConfig(config),
      tracks,
      completedKeys: [...completedKeys],
      suggestedTracks: suggestions.tracks,
      suggestedAlbums: suggestions.albums,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── Playlists ─────────────────────────────────────────────────────────────

  app.get('/playlists', requireUser, requireWizardComplete, requireUserWizardComplete, (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const userPlexId = String(user.username || '').trim();
    const role = getEffectiveRole(req);
    const lastSync = getLastPlaylistSync(db, userPlexId);
    const playlistJob = getPlaylistJob(db, userPlexId);
    const suggestions = loadSuggestionBundle(recommendationService, userPlexId, {
      artistLimit: 6,
      albumLimit: 6,
      trackLimit: 12,
    });
    const generatedPlaylists = playlistService?.listGenerated(userPlexId, { activeOnly: false }) || [];
    const canonicalPlaylists = playlistService?.getCanonicalPlaylist(userPlexId) || { legacy: null, generated: [], curatorred: null };

    res.render('playlists', {
      title: 'Playlists — Curatorr',
      user,
      role,
      actualRole: getActualRole(req),
      config: safeConfig(config),
      lastSync,
      playlistJob,
      suggestionSummary: {
        artists: suggestions.artists.length,
        albums: suggestions.albums.length,
        tracks: suggestions.tracks.length,
      },
      generatedPlaylists,
      canonicalPlaylists,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── User settings ─────────────────────────────────────────────────────────

  app.get('/user-settings', requireUser, (req, res) => {
    const config = loadConfig();
    const userPlexId = String(req.session?.user?.username || '').trim();
    const { mustIncludeArtists, neverIncludeArtists } = getResolvedUserArtistFilters(db, config, userPlexId);
    const filterArtists = dedupeMasterArtistNames([
      ...getArtistsFromMaster(db),
      ...mustIncludeArtists,
      ...neverIncludeArtists,
    ]).map((artistName) => ({
      name: artistName,
      thumb: `/api/music/thumb/artist/${encodeURIComponent(artistName)}?v=user-settings-artist-thumb-1`,
    }));
    const userPrefs = userPlexId ? getUserPreferences(db, userPlexId) : null;
    const userPreset = userPrefs?.smartConfig?.preset || null;
    res.render('user-settings', {
      title: 'My Settings — Curatorr',
      user: req.session.user,
      role: getEffectiveRole(req),
      actualRole: getActualRole(req),
      config: safeConfig(config),
      filterArtists,
      mustIncludeArtists,
      neverIncludeArtists,
      userPreset,
      error: String(req.query?.error || '').trim() || null,
      success: String(req.query?.success || '').trim() || null,
      extraCss: ['/styles-layout.css', '/styles-settings.css'],
    });
  });
}

function safeConfig(config) {
  return {
    general: config.general || {},
    plex: { url: config.plex?.url || '', tokenSet: Boolean(config.plex?.token), libraries: config.plex?.libraries || [] },
    tautulli: { url: config.tautulli?.url || '', configured: Boolean(config.tautulli?.url) },
    lidarr: { url: config.lidarr?.url || '', configured: Boolean(config.lidarr?.url) },
    theme: config.theme || {},
    smartPlaylist: config.smartPlaylist || {},
    filters: config.filters || {},
    wizard: config.wizard || {},
  };
}

function normalizeTierKey(value) {
  const key = String(value || '').trim().toLowerCase();
  if (key === 'half decent') return 'half-decent';
  return key;
}

function buildTierBadge(key = 'decent') {
  const normalized = normalizeTierKey(key);
  if (normalized === 'skip') return { key: 'skip', label: 'Skip', tone: 'skip' };
  if (normalized === 'half-decent') return { key: 'half-decent', label: 'Half Decent', tone: 'half-decent' };
  if (normalized === 'belter') return { key: 'belter', label: 'Belter', tone: 'belter' };
  if (normalized === 'decent') return { key: 'decent', label: 'Decent', tone: 'decent' };
  if (normalized === 'curatorr') return { key: 'curatorr', label: 'Curatorr', tone: 'curatorr' };
  return { key: 'decent', label: 'Decent', tone: 'decent' };
}

function deriveArtistTier(artist, config = {}) {
  if (!artist || typeof artist !== 'object') return buildTierBadge('decent');
  if (artist.excluded) return buildTierBadge('skip');
  const smartSettings = config?.smartPlaylist || {};
  const skipThreshold = Number(smartSettings.artistSkipRank ?? 2);
  const belterThreshold = Number(smartSettings.artistBelterRank ?? 8);
  const score = Number(artist.ranking_score);
  if (Number.isFinite(score)) {
    if (score <= skipThreshold) return buildTierBadge('skip');
    if (score < 5) return buildTierBadge('half-decent');
    if (score >= belterThreshold) return buildTierBadge('belter');
    return buildTierBadge('decent');
  }
  if (Number(artist.total_skips || 0) > 0) return buildTierBadge('half-decent');
  if (Number(artist.total_plays || 0) > 0) return buildTierBadge('decent');
  return buildTierBadge('curatorr');
}

function deriveTrackTier(track) {
  if (!track || typeof track !== 'object') return null;
  if (track.excluded) return buildTierBadge('skip');
  const tier = normalizeTierKey(track.tier);
  // Only use explicitly set tiers (not the DB default 'curatorr')
  if (['skip', 'half-decent', 'decent', 'belter'].includes(tier)) {
    return buildTierBadge(tier);
  }
  // Derive from observed behaviour
  if (Number(track.total_skips || 0) > 0) return buildTierBadge('half-decent');
  if (Number(track.total_plays || 0) > 0) return buildTierBadge('decent');
  return null; // never played — show nothing
}

function deriveHistoryTier(event, config = {}) {
  if (!event || typeof event !== 'object') return buildTierBadge('decent');
  if (event.is_skip) return buildTierBadge('skip');
  const listenedMs = Number(event.duration_ms || 0);
  const trackDurationMs = Number(event.track_duration_ms || 0);
  const completionThresholdMs = (Number(config?.smartPlaylist?.completionThresholdSeconds) || 30) * 1000;
  if (trackDurationMs > 0) {
    if (listenedMs >= Math.max(0, trackDurationMs - completionThresholdMs)) return buildTierBadge('belter');
    if (listenedMs >= trackDurationMs * 0.5) return buildTierBadge('decent');
    return buildTierBadge('half-decent');
  }
  return deriveTrackTier({
    excluded: Boolean(event.current_excluded),
    force_included: Boolean(event.current_force_included),
    tier: event.current_tier,
  });
}

function loadSuggestionBundle(recommendationService, userPlexId, options = {}) {
  if (!recommendationService || !userPlexId) return { artists: [], albums: [], tracks: [] };
  let cached = recommendationService.listCachedSuggestions(userPlexId, options);
  const count = (cached.artists?.length || 0) + (cached.albums?.length || 0) + (cached.tracks?.length || 0);
  if (count > 0) return cached;
  try {
    const rebuilt = recommendationService.rebuildSuggestionsForUser(userPlexId, options);
    cached = rebuilt?.cached || cached;
  } catch (err) {
    return cached;
  }
  return cached;
}

function normalizeName(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeLidarrSourceKind(value) {
  return String(value || '').trim().toLowerCase() === 'automatic' ? 'automatic' : '';
}

function formatLidarrSourceLabel(value) {
  return normalizeLidarrSourceKind(value) === 'automatic' ? 'Automatic' : (String(value || '').trim() ? 'Manual' : '');
}

function resolveLidarrSourceLabel(suggestion, statusKey = '') {
  const reason = suggestion?.reason && typeof suggestion.reason === 'object' ? suggestion.reason : {};
  const starterAlbum = reason?.starterAlbum && typeof reason.starterAlbum === 'object' ? reason.starterAlbum : null;
  const latestAlbum = reason?.latestAlbum && typeof reason.latestAlbum === 'object' ? reason.latestAlbum : null;
  const normalizedStatusKey = String(statusKey || '').trim().toLowerCase();
  if (normalizedStatusKey === 'suggested' || normalizedStatusKey === 'already_in_lidarr') return '';
  if (normalizedStatusKey === 'queued_for_lidarr' || normalizedStatusKey === 'adding_to_lidarr') {
    return formatLidarrSourceLabel(reason?.requestSourceKind || reason?.queuedSourceKind || '');
  }
  return formatLidarrSourceLabel(
    latestAlbum?.sourceKind
    || starterAlbum?.sourceKind
    || reason?.artistAddedSourceKind
    || reason?.requestSourceKind
    || ''
  );
}

function resolveLidarrSourceFromRequest(request) {
  if (!request || typeof request !== 'object') return '';
  const detail = request.detail && typeof request.detail === 'object' ? request.detail : {};
  return formatLidarrSourceLabel(detail.albumSource || detail.requestSource || request.sourceKind || '');
}

function appendLidarrSourceDetail(detail, sourceLabel) {
  const normalizedDetail = String(detail || '').trim();
  const normalizedSource = String(sourceLabel || '').trim();
  if (!normalizedSource) return normalizedDetail;
  return normalizedDetail ? `${normalizedDetail} · ${normalizedSource}` : normalizedSource;
}

function isProgressReviewable(progress, statusKey = '') {
  if (!progress || typeof progress !== 'object') return false;
  const currentStage = String(progress.currentStage || '').trim().toLowerCase();
  const normalizedStatusKey = String(statusKey || '').trim().toLowerCase();
  if (!currentStage || currentStage === 'catalog_complete') return false;
  if (normalizedStatusKey === 'already_in_lidarr') return false;
  return true;
}

function deriveLidarrStateLabel(suggestion, progress, liveCommand = null, liveAlbum = null) {
  const reason = suggestion?.reason && typeof suggestion.reason === 'object' ? suggestion.reason : {};
  const currentStage = String(progress?.currentStage || '').trim().toLowerCase();
  const suggestionStatus = String(suggestion?.status || '').trim().toLowerCase();
  const lastManualSearchStatus = String(progress?.lastManualSearchStatus || '').trim().toLowerCase();
  const starterAlbum = reason?.starterAlbum && typeof reason.starterAlbum === 'object' ? reason.starterAlbum : null;
  const latestAlbum = reason?.latestAlbum && typeof reason.latestAlbum === 'object' ? reason.latestAlbum : null;
  const albumWarning = reason?.albumWarning && typeof reason.albumWarning === 'object' ? reason.albumWarning : null;
  const liveCommandStatus = String(liveCommand?.status || '').trim().toLowerCase();
  const liveCommandResult = String(liveCommand?.result || '').trim().toLowerCase();
  const albumLabel = latestAlbum?.albumTitle || starterAlbum?.albumTitle || '';
  const liveTrackFileCount = Number(liveAlbum?.statistics?.trackFileCount || 0);
  const acquisition = reason?.acquisition && typeof reason.acquisition === 'object' ? reason.acquisition : {};
  if (suggestionStatus === 'already_in_lidarr') {
    return { key: 'already_in_lidarr', label: 'Already in Lidarr', tone: 'neutral', detail: '' };
  }
  if (suggestionStatus === 'queued_for_lidarr' || currentStage === 'queued_for_lidarr') {
    return { key: 'queued_for_lidarr', label: 'Queued', tone: 'half-decent', detail: 'Queued for Lidarr processing.' };
  }
  if (suggestionStatus === 'quota_blocked' || albumWarning?.type === 'album_quota') {
    return { key: 'quota_blocked', label: 'Quota blocked', tone: 'warn', detail: albumWarning?.message || 'Weekly quota reached.' };
  }
  if (liveTrackFileCount > 0 || currentStage === 'album_acquired') {
    return { key: 'downloaded', label: 'Downloaded', tone: 'belter', detail: albumLabel };
  }
  if (liveCommandStatus === 'queued') {
    return { key: 'search_queued', label: 'Search queued', tone: 'ok', detail: albumLabel };
  }
  if (liveCommandStatus === 'started') {
    return { key: 'search_running', label: 'Search running', tone: 'half-decent', detail: albumLabel };
  }
  if (liveCommandStatus === 'completed') {
    if (liveTrackFileCount > 0 || liveCommandResult === 'successful') {
      return { key: 'search_complete', label: 'Search complete', tone: 'belter', detail: albumLabel };
    }
    return { key: 'search_finished', label: 'Search finished', tone: 'neutral', detail: albumLabel ? `${albumLabel} · no files found yet` : 'No files found yet' };
  }
  if (liveCommandStatus === 'failed') {
    return { key: 'search_failed', label: 'Search failed', tone: 'warn', detail: albumLabel };
  }
  if (lastManualSearchStatus === 'queued') {
    return { key: 'search_queued', label: 'Search queued', tone: 'ok', detail: albumLabel };
  }
  if (lastManualSearchStatus === 'started') {
    return { key: 'search_running', label: 'Search running', tone: 'half-decent', detail: albumLabel };
  }
  if (lastManualSearchStatus === 'completed') {
    return { key: 'search_complete', label: 'Search complete', tone: 'belter', detail: albumLabel };
  }
  if (lastManualSearchStatus === 'failed') {
    return { key: 'search_failed', label: 'Search failed', tone: 'warn', detail: albumLabel };
  }
  if (currentStage === 'manual_grab_queued') {
    return { key: 'manual_grab_queued', label: 'Manual grab queued', tone: 'curatorr', detail: acquisition?.manualFallbackReleaseTitle || albumLabel };
  }
  if (currentStage === 'search_retry_queued' || currentStage === 'monitor_repaired_search_queued') {
    return { key: 'search_retry_queued', label: 'Search retry queued', tone: 'ok', detail: albumLabel };
  }
  if (currentStage === 'monitor_repaired') {
    return { key: 'monitor_repaired', label: 'Monitoring repaired', tone: 'neutral', detail: albumLabel };
  }
  if (currentStage === 'manual_search_no_results' || currentStage === 'no_files_found') {
    return { key: 'search_finished', label: 'Search finished', tone: 'neutral', detail: albumLabel ? `${albumLabel} · no files found yet` : 'No files found yet' };
  }
  if (currentStage === 'manual_search_failed') {
    return { key: 'manual_search_failed', label: 'Manual fallback failed', tone: 'warn', detail: albumLabel || 'Release lookup or grab failed.' };
  }
  if (currentStage === 'starter_album_added') {
    return { key: 'starter_album_added', label: 'Starter album added', tone: 'belter', detail: albumLabel };
  }
  if (currentStage === 'starter_album_linked') {
    return { key: 'starter_album_linked', label: 'Starter album linked', tone: 'neutral', detail: albumLabel };
  }
  if (currentStage === 'catalog_expanded') {
    return { key: 'catalog_expanded', label: 'Next album added', tone: 'curatorr', detail: albumLabel };
  }
  if (currentStage === 'awaiting_belter') {
    return { key: 'awaiting_belter', label: 'Awaiting belter', tone: 'neutral', detail: 'Waiting for a stronger listening signal.' };
  }
  if (currentStage === 'catalog_complete') {
    return { key: 'catalog_complete', label: 'Catalog complete', tone: 'neutral', detail: 'No further album unlocks pending.' };
  }
  if (currentStage === 'added' || String(suggestion?.status || '').trim().toLowerCase() === 'added_to_lidarr') {
    return { key: 'artist_added', label: 'Artist added', tone: 'curatorr', detail: albumLabel };
  }
  if (currentStage === 'queued') {
    return { key: 'adding_to_lidarr', label: 'Adding to Lidarr', tone: 'half-decent', detail: '' };
  }
  return { key: 'suggested', label: 'Suggested', tone: 'ok', detail: '' };
}

async function buildLidarrStatusBundle(db, lidarrService, userPlexId, suggestedArtists = []) {
  const suggestions = Array.isArray(suggestedArtists) ? suggestedArtists : [];
  const progressItems = listLidarrArtistProgress(db, userPlexId, { limit: 12 });
  const requestHistory = listLidarrRequests(db, userPlexId, { statuses: ['queued', 'processing', 'completed', 'failed'], limit: 250 });
  const progressMap = new Map(progressItems.map((item) => [normalizeName(item.artistName), item]));
  const requestMap = new Map();
  requestHistory.forEach((request) => {
    const key = normalizeName(request.artistName);
    if (!key || requestMap.has(key)) return;
    requestMap.set(key, request);
  });
  const lidarrNames = new Set(progressItems.map((item) => normalizeName(item.artistName)).filter(Boolean));
  const commandIds = new Set();
  const albumIds = new Set();

  suggestions.forEach((artist) => {
    const reason = artist?.reason && typeof artist.reason === 'object' ? artist.reason : {};
    const starterAlbum = reason?.starterAlbum && typeof reason.starterAlbum === 'object' ? reason.starterAlbum : null;
    const latestAlbum = reason?.latestAlbum && typeof reason.latestAlbum === 'object' ? reason.latestAlbum : null;
    const commandId = Number(starterAlbum?.commandId || 0);
    const starterAlbumId = Number(starterAlbum?.albumId || 0);
    const latestAlbumId = Number(latestAlbum?.albumId || 0);
    if (commandId > 0) commandIds.add(commandId);
    if (starterAlbumId > 0) albumIds.add(starterAlbumId);
    if (latestAlbumId > 0) albumIds.add(latestAlbumId);
  });

  if (lidarrService?.isConfigured()) {
    try {
      const currentArtists = await lidarrService.listArtists({ pageSize: 2000, timeoutMs: 15000 });
      currentArtists.forEach((artist) => {
        const name = normalizeName(artist?.artistName);
        if (name) lidarrNames.add(name);
      });
    } catch (_err) {
      // Ignore Lidarr list failures here; the page can still render from local progress data.
    }
  }

  const commandMap = new Map();
  const albumMap = new Map();
  if (lidarrService?.isConfigured() && commandIds.size) {
    await Promise.all([...commandIds].map(async (commandId) => {
      try {
        const command = await lidarrService.getCommand(commandId, { timeoutMs: 8000 });
        if (command) commandMap.set(commandId, command);
      } catch (_err) {
        // Ignore command lookup failures here; the page can still render from cached progress data.
      }
    }));
  }
  if (lidarrService?.isConfigured() && albumIds.size) {
    await Promise.all([...albumIds].map(async (albumId) => {
      try {
        const album = await lidarrService.getAlbum(albumId, { timeoutMs: 8000 });
        if (album) albumMap.set(albumId, album);
      } catch (_err) {
        // Ignore album lookup failures here; the page can still render from cached progress data.
      }
    }));
  }

  const enrichedSuggestions = suggestions.map((artist) => {
    const key = normalizeName(artist.artistName);
    const progress = progressMap.get(key) || null;
    const reason = artist?.reason && typeof artist.reason === 'object' ? artist.reason : {};
    const starterAlbum = reason?.starterAlbum && typeof reason.starterAlbum === 'object' ? reason.starterAlbum : null;
    const latestAlbum = reason?.latestAlbum && typeof reason.latestAlbum === 'object' ? reason.latestAlbum : null;
    const liveCommand = commandMap.get(Number(starterAlbum?.commandId || 0)) || null;
    const liveAlbum = albumMap.get(Number(latestAlbum?.albumId || starterAlbum?.albumId || 0)) || null;
    const isInLidarr = Boolean(key && lidarrNames.has(key))
      || Boolean(artist?.lidarrArtistId)
      || Boolean(progress?.lidarrArtistId)
      || Boolean(reason?.lidarrExisting);
    let derived = deriveLidarrStateLabel(artist, progress, liveCommand, liveAlbum);
    if (isInLidarr && ['suggested', 'queued_for_lidarr', 'quota_blocked', 'adding_to_lidarr'].includes(derived.key)) {
      derived = { key: 'already_in_lidarr', label: 'Already in Lidarr', tone: 'neutral', detail: '' };
    }
    const sourceLabel = resolveLidarrSourceLabel(artist, derived.key) || resolveLidarrSourceFromRequest(requestMap.get(key));
    return {
      ...artist,
      isInLidarr,
      lidarrProgress: progress,
      lidarrCommand: liveCommand,
      lidarrAlbum: liveAlbum,
      reviewable: isProgressReviewable(progress, derived.key),
      lidarrStatusKey: derived.key,
      lidarrStatusLabel: derived.label,
      lidarrStatusTone: derived.tone,
      lidarrStatusDetail: appendLidarrSourceDetail(derived.detail, sourceLabel),
      lidarrStatusSourceLabel: sourceLabel,
    };
  });

  const actionableSuggestions = enrichedSuggestions.filter((artist) => !artist.isInLidarr);
  const activityMap = new Map();

  enrichedSuggestions.forEach((artist) => {
    if (!artist.isInLidarr && artist.lidarrStatusKey === 'suggested') return;
    const key = normalizeName(artist.artistName);
    if (!key) return;
    activityMap.set(key, {
      artistName: artist.artistName,
      label: artist.lidarrStatusLabel,
      tone: artist.lidarrStatusTone,
      detail: artist.lidarrStatusDetail,
      reviewable: Boolean(artist.reviewable),
      updatedAt: artist.lidarrProgress?.updatedAt || artist.reason?.manualActionAt || artist.lastEvaluatedAt || 0,
    });
  });

  progressItems.forEach((progress) => {
    const key = normalizeName(progress.artistName);
    if (!key || activityMap.has(key)) return;
    const derived = deriveLidarrStateLabel(null, progress);
    activityMap.set(key, {
      artistName: progress.artistName,
      label: derived.label,
      tone: derived.tone,
      detail: derived.detail,
      reviewable: isProgressReviewable(progress, derived.key),
      updatedAt: progress.updatedAt || 0,
    });
  });

  const items = [...activityMap.values()]
    .sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0) || a.artistName.localeCompare(b.artistName))
    .slice(0, 8);

  const counts = items.reduce((acc, item) => {
    acc[item.label] = Number(acc[item.label] || 0) + 1;
    return acc;
  }, {});

  return {
    actionableSuggestions,
    allSuggestions: enrichedSuggestions,
    items,
    counts,
  };
}
