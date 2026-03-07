// Page routes — HTML views

import { getPlayStats, getTopArtists, getTopTracks, getRecentHistory, getLastPlaylistSync, getPlayStatsByDay, getCompletedTrackKeys } from '../db.js';

// Returns the DB filter key for a user:
// - admin → '' (all events)
// - others → username (matches Tautulli {username} macro and wizard storage key)
function resolveUserFilter(user, role) {
  if (role === 'admin') return '';
  return String(user.username || '').trim();
}

export function registerPages(app, ctx) {
  const {
    requireUser,
    requireAdmin,
    requireWizardComplete,
    requireUserWizardComplete,
    loadConfig,
    getEffectiveRole,
    db,
  } = ctx;

  // Root redirect
  app.get('/', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    return res.redirect('/dashboard');
  });

  // ── Dashboard ─────────────────────────────────────────────────────────────

  app.get('/dashboard', requireUser, requireWizardComplete, requireUserWizardComplete, (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const role = getEffectiveRole(req);
    const userPlexId = resolveUserFilter(user, role);

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
    const topArtists = getTopArtists(db, userPlexId, 5);
    const topTracks = getTopTracks(db, userPlexId, 5);
    const recentHistory = getRecentHistory(db, userPlexId, 10);
    const lastSync = getLastPlaylistSync(db, userPlexId);

    res.render('dashboard', {
      title: 'Dashboard — Curatorr',
      user,
      role,
      config: safeConfig(config),
      stats7d,
      stats30d,
      byDay,
      topArtists,
      topTracks,
      recentHistory,
      lastSync,
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
    const history = getRecentHistory(db, userPlexId, limit, page * limit);

    res.render('history', {
      title: 'Play History — Curatorr',
      user,
      role: getEffectiveRole(req),
      config: safeConfig(config),
      history,
      page,
      limit,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── Artists ───────────────────────────────────────────────────────────────

  app.get('/artists', requireUser, requireWizardComplete, requireUserWizardComplete, (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const userPlexId = resolveUserFilter(user, getEffectiveRole(req));
    const artists = getTopArtists(db, userPlexId, 500);

    res.render('artists', {
      title: 'Artists — Curatorr',
      user,
      role: getEffectiveRole(req),
      config: safeConfig(config),
      artists,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── Tracks ────────────────────────────────────────────────────────────────

  app.get('/tracks', requireUser, requireWizardComplete, requireUserWizardComplete, (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const userPlexId = resolveUserFilter(user, getEffectiveRole(req));
    const tracks = getTopTracks(db, userPlexId, 500);
    const smartSettings = config.smartPlaylist || {};
    const completionThresholdMs = (Number(smartSettings.completionThresholdSeconds) || 20) * 1000;
    const completedKeys = getCompletedTrackKeys(db, userPlexId, completionThresholdMs);

    res.render('tracks', {
      title: 'Tracks — Curatorr',
      user,
      role: getEffectiveRole(req),
      config: safeConfig(config),
      tracks,
      completedKeys: [...completedKeys],
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── Playlists ─────────────────────────────────────────────────────────────

  app.get('/playlists', requireUser, requireWizardComplete, requireUserWizardComplete, (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const userPlexId = String(user.username || '').trim();
    const lastSync = getLastPlaylistSync(db, userPlexId);

    res.render('playlists', {
      title: 'Playlists — Curatorr',
      user,
      role: getEffectiveRole(req),
      config: safeConfig(config),
      lastSync,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── User settings ─────────────────────────────────────────────────────────

  app.get('/user-settings', requireUser, (req, res) => {
    const config = loadConfig();
    res.render('user-settings', {
      title: 'My Settings — Curatorr',
      user: req.session.user,
      role: getEffectiveRole(req),
      config: safeConfig(config),
      error: null,
      success: null,
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
    smartPlaylist: config.smartPlaylist || {},
    filters: config.filters || {},
    wizard: config.wizard || {},
  };
}
