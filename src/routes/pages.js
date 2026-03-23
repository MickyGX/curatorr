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
  getSuggestedArtist,
  getArtistsFromMaster,
  dedupeMasterArtistNames,
  getResolvedUserArtistFilters,
  getUserPreferences,
  listSuggestedAlbums,
  getGenresFromMaster,
  getMoodsFromMaster,
  getAllLastfmTags,
  listUserPersonalPlaylists,
  listUserGeneratedPlaylists,
  getPlaylistTracks,
  getMasterTrackCount,
  getMasterArtistCount,
  getExcludedTrackKeys,
  getSkipTierArtists,
  getAllUserIds,
} from '../db.js';
import { paginateRolledHistory } from '../history-rollup.js';
import * as jellyfinAdapter from '../services/media-servers/jellyfin.js';
import * as embyAdapter from '../services/media-servers/emby.js';

// Returns the DB filter key for a user:
// - local admin accounts can inspect global activity
// - Plex-backed accounts, including admins, stay scoped to their Plex username
export function resolveUserFilter(user, role) {
  const source = String(user?.source || '').trim().toLowerCase();
  if (role === 'admin' && source === 'local') return '';
  return String(user.username || '').trim();
}

function resolveSuggestionUser(user, role) {
  const source = String(user?.source || '').trim().toLowerCase();
  if (role === 'admin' && source === 'local') return '';
  return String(user?.username || '').trim();
}

function stripArtistSuffix(title, artist) {
  if (!title || !artist) return title || '';
  const suffix = ' - ' + artist;
  return title.endsWith(suffix) ? title.slice(0, -suffix.length) : title;
}

function enrichDiscoverRequests(db, userPlexId, requests = []) {
  const libraryArtistSet = new Set(
    db.prepare('SELECT DISTINCT artist_name FROM master_tracks').all().map((r) => String(r.artist_name || '').trim().toLowerCase()),
  );
  const addedAlbumMap = new Map();
  for (const status of ['added_to_lidarr', 'already_monitored']) {
    for (const album of listSuggestedAlbums(db, userPlexId, { status, limit: 500 })) {
      const key = String(album?.artistName || '').trim().toLowerCase();
      if (!key || addedAlbumMap.has(key)) continue;
      addedAlbumMap.set(key, String(album?.albumTitle || '').trim());
    }
  }
  const suggestedAlbumMap = new Map();
  for (const album of listSuggestedAlbums(db, userPlexId, { limit: 500 })) {
    const key = String(album?.artistName || '').trim().toLowerCase();
    if (!key || suggestedAlbumMap.has(key)) continue;
    suggestedAlbumMap.set(key, String(album?.albumTitle || '').trim());
  }
  return (Array.isArray(requests) ? requests : []).map((request) => {
    const detail = request?.detail && typeof request.detail === 'object' ? { ...request.detail } : {};
    const currentAlbumTitle = String(
      request?.albumTitle
      || detail.selectedAlbumTitle
      || detail.starterAlbumTitle
      || detail.latestAlbumTitle
      || detail.preferredAlbumTitle
      || ''
    ).trim();
    const artistKey = String(request?.artistName || '').trim().toLowerCase();
    const inLibrary = libraryArtistSet.has(artistKey);
    if (currentAlbumTitle) return { ...request, detail, inLibrary };
    const addedAlbumTitle = addedAlbumMap.get(artistKey) || '';
    if (addedAlbumTitle) {
      return {
        ...request,
        detail: { ...detail, selectedAlbumTitle: addedAlbumTitle },
        inLibrary,
      };
    }
    const suggestion = getSuggestedArtist(db, userPlexId, String(request?.artistName || '').trim());
    const reason = suggestion?.reason && typeof suggestion.reason === 'object' ? suggestion.reason : {};
    const starterAlbum = reason?.starterAlbum && typeof reason.starterAlbum === 'object' ? reason.starterAlbum : null;
    const latestAlbum = reason?.latestAlbum && typeof reason.latestAlbum === 'object' ? reason.latestAlbum : null;
    const fallbackAlbumTitle = String(
      starterAlbum?.albumTitle
      || latestAlbum?.albumTitle
      || suggestedAlbumMap.get(artistKey)
      || ''
    ).trim();
    if (!fallbackAlbumTitle) return { ...request, detail, inLibrary };
    return {
      ...request,
      detail: {
        ...detail,
        selectedAlbumTitle: detail.selectedAlbumTitle || fallbackAlbumTitle,
        starterAlbumTitle: detail.starterAlbumTitle || String(starterAlbum?.albumTitle || '').trim(),
        latestAlbumTitle: detail.latestAlbumTitle || String(latestAlbum?.albumTitle || '').trim(),
      },
      inLibrary,
    };
  });
}

function normalizeIdentitySet(values = [], normalizeIdentityList) {
  return new Set(
    normalizeIdentityList(values)
      .map((value) => String(value || '').trim().toLowerCase())
      .filter(Boolean),
  );
}

function resolveConfiguredPlexRole(identifier, roleSets) {
  const key = String(identifier || '').trim().toLowerCase();
  if (!key) return 'user';
  if (roleSets.disabled.has(key)) return 'disabled';
  if (roleSets.admin.has(key)) return 'admin';
  if (roleSets.coAdmin.has(key)) return 'co-admin';
  if (roleSets.powerUser.has(key)) return 'power-user';
  if (roleSets.guest.has(key)) return 'guest';
  return 'user';
}

function buildPlexIdentityLookup(entries, normalizeIdentityList) {
  const lookup = new Map();
  (Array.isArray(entries) ? entries : []).forEach((entry) => {
    normalizeIdentityList([
      entry?.email,
      entry?.username,
      entry?.title,
      String(entry?.id || ''),
      String(entry?.uuid || ''),
    ]).forEach((value) => {
      const key = String(value || '').trim().toLowerCase();
      if (!key || lookup.has(key)) return;
      lookup.set(key, entry);
    });
  });
  return lookup;
}

async function fetchLivePlexUsersWithHomeData({
  config,
  normalizeIdentityList,
  fetchPlexUser,
  fetchPlexHomeUsers,
  parsePlexUsers,
}) {
  const token = String(config?.plex?.token || '').trim();
  const machineId = String(config?.plex?.machineId || '').trim();
  if (!token || !fetchPlexUser || !parsePlexUsers) {
    return { liveUsers: [], homeUsers: [], ownerIdentitySet: new Set() };
  }

  const liveUsers = [];
  const homeUsers = [];
  const homeThumbByKey = new Map();
  let ownerIdentitySet = new Set();

  try {
    const [usersRes, ownerUser, fetchedHomeUsers] = await Promise.all([
      fetch('https://plex.tv/api/users', { headers: { Accept: 'application/xml', 'X-Plex-Token': token } }),
      fetchPlexUser(token).catch(() => null),
      fetchPlexHomeUsers ? fetchPlexHomeUsers(token).catch(() => []) : [],
    ]);

    (Array.isArray(fetchedHomeUsers) ? fetchedHomeUsers : []).forEach((homeUser) => {
      const normalizedUser = {
        id: String(homeUser?.id || '').trim(),
        uuid: String(homeUser?.uuid || '').trim(),
        username: String(homeUser?.username || '').trim(),
        email: String(homeUser?.email || '').trim(),
        title: String(homeUser?.title || homeUser?.username || homeUser?.email || homeUser?.id || 'User').trim() || 'User',
        thumb: String(homeUser?.thumb || '').trim(),
        admin: Boolean(homeUser?.admin),
      };
      homeUsers.push(normalizedUser);
      if (!normalizedUser.thumb) return;
      normalizeIdentityList([
        normalizedUser.title,
        normalizedUser.username,
        normalizedUser.email,
        normalizedUser.id,
        normalizedUser.uuid,
      ]).forEach((value) => {
        const key = String(value || '').trim().toLowerCase();
        if (!key || homeThumbByKey.has(key)) return;
        homeThumbByKey.set(key, normalizedUser.thumb);
      });
    });

    if (usersRes.ok) {
      const xmlText = await usersRes.text();
      const parsedUsers = parsePlexUsers(xmlText, { machineId }).map((entry) => {
        const thumb = String(entry?.thumb || '').trim();
        if (thumb) return entry;
        const ids = normalizeIdentityList([
          entry?.email,
          entry?.username,
          entry?.title,
          String(entry?.id || ''),
          String(entry?.uuid || ''),
        ]).map((value) => String(value || '').trim().toLowerCase());
        const matchedThumb = ids.map((id) => homeThumbByKey.get(id)).find(Boolean) || '';
        return matchedThumb ? { ...entry, thumb: matchedThumb } : entry;
      });
      liveUsers.push(...parsedUsers);
    }

    if (ownerUser) {
      ownerIdentitySet = new Set(
        normalizeIdentityList([
          ownerUser.email,
          ownerUser.username,
          ownerUser.title,
          String(ownerUser.id || ''),
          String(ownerUser.uuid || ''),
        ]).map((value) => String(value || '').trim().toLowerCase()),
      );
      const alreadyPresent = liveUsers.some((entry) => normalizeIdentityList([
        entry?.email,
        entry?.username,
        entry?.title,
        String(entry?.id || ''),
        String(entry?.uuid || ''),
      ]).some((value) => ownerIdentitySet.has(String(value || '').trim().toLowerCase())));
      if (!alreadyPresent) {
        liveUsers.unshift({
          id: String(ownerUser.id || ''),
          uuid: String(ownerUser.uuid || ''),
          username: ownerUser.username || '',
          email: ownerUser.email || '',
          title: ownerUser.title || ownerUser.username || ownerUser.email || '',
          thumb: String(ownerUser.thumb || '').trim(),
        });
      }
    }
  } catch (_err) {
    return { liveUsers: [], homeUsers: [], ownerIdentitySet: new Set() };
  }

  return { liveUsers, homeUsers, ownerIdentitySet };
}

async function buildAdminUsersPageData({
  db,
  config,
  resolveLocalUsers,
  normalizeIdentityList,
  loadAdmins,
  loadCoAdmins,
  loadPowerUsers,
  loadGuestUsers,
  loadDisabledUsers,
  fetchPlexUser,
  fetchPlexHomeUsers,
  parsePlexUsers,
  currentUser,
}) {
  const userLogins = config?.userLogins?.curatorr && typeof config.userLogins.curatorr === 'object'
    ? config.userLogins.curatorr
    : {};
  const plexLogins = config?.userLogins?.plex && typeof config.userLogins.plex === 'object'
    ? config.userLogins.plex
    : {};
  const normalizedLocalUsers = resolveLocalUsers(config);
  const localIdentitySet = normalizeIdentitySet(
    normalizedLocalUsers.flatMap((entry) => [entry?.username, entry?.email]),
    normalizeIdentityList,
  );
  const roleSets = {
    admin: normalizeIdentitySet(loadAdmins(), normalizeIdentityList),
    coAdmin: normalizeIdentitySet(loadCoAdmins(), normalizeIdentityList),
    powerUser: normalizeIdentitySet(loadPowerUsers(), normalizeIdentityList),
    guest: normalizeIdentitySet(loadGuestUsers(), normalizeIdentityList),
    disabled: normalizeIdentitySet(loadDisabledUsers(), normalizeIdentityList),
  };
  const ownerKey = [...roleSets.admin][0] || '';
  const hasRoleMatch = (set, ids = []) => ids.some((id) => set.has(id));
  const resolveLogin = (ids = []) => {
    for (const id of ids) {
      if (userLogins[id]) return userLogins[id];
    }
    for (const id of ids) {
      if (plexLogins[id]) return plexLogins[id];
    }
    return '';
  };
  const observedPlexUserIds = db.prepare(`
    SELECT DISTINCT user_plex_id FROM play_events WHERE TRIM(COALESCE(user_plex_id, '')) != ''
    UNION
    SELECT DISTINCT user_plex_id FROM artist_stats WHERE TRIM(COALESCE(user_plex_id, '')) != ''
    UNION
    SELECT DISTINCT user_plex_id FROM track_stats WHERE TRIM(COALESCE(user_plex_id, '')) != ''
    UNION
    SELECT DISTINCT user_plex_id FROM playlist_syncs WHERE TRIM(COALESCE(user_plex_id, '')) != ''
    UNION
    SELECT DISTINCT user_plex_id FROM user_generated_playlists WHERE TRIM(COALESCE(user_plex_id, '')) != ''
    UNION
    SELECT DISTINCT user_plex_id FROM user_personal_playlists WHERE TRIM(COALESCE(user_plex_id, '')) != ''
    UNION
    SELECT DISTINCT user_plex_id FROM lidarr_requests WHERE TRIM(COALESCE(user_plex_id, '')) != ''
  `).all().map((row) => String(row.user_plex_id || '').trim()).filter(Boolean);
  const plexLoginIds = Object.keys(plexLogins || {}).map((value) => String(value || '').trim()).filter(Boolean);
  const seenPlexIds = new Set();
  const fallbackPlexUserIds = [...observedPlexUserIds, ...plexLoginIds].filter((value) => {
    const key = String(value || '').trim().toLowerCase();
    if (!key || localIdentitySet.has(key) || seenPlexIds.has(key)) return false;
    seenPlexIds.add(key);
    return true;
  });
  const now = Date.now();
  const since7d = now - 7 * 24 * 60 * 60 * 1000;
  const since30d = now - 30 * 24 * 60 * 60 * 1000;
  const playlistCountStmt = db.prepare('SELECT COUNT(*) AS n FROM user_generated_playlists WHERE user_plex_id = ? AND active = 1');
  const personalPlaylistCountStmt = db.prepare('SELECT COUNT(*) AS n FROM user_personal_playlists WHERE user_plex_id = ?');
  const lastPlayStmt = db.prepare('SELECT MAX(started_at) AS last_play_at FROM play_events WHERE user_plex_id = ?');
  const lidarrUsageTotalsStmt = db.prepare(`
    SELECT
      COALESCE(SUM(CASE WHEN usage_key = 'artists' THEN amount ELSE 0 END), 0) AS artists_added,
      COALESCE(SUM(CASE WHEN usage_key = 'albums' THEN amount ELSE 0 END), 0) AS albums_added
    FROM lidarr_usage
    WHERE user_plex_id = ?
  `);
  const lidarrProgressTotalsStmt = db.prepare(`
    SELECT
      COUNT(DISTINCT CASE
        WHEN TRIM(COALESCE(artist_name, '')) != ''
          AND (
            lidarr_artist_id IS NOT NULL
            OR current_stage IN ('artist_added', 'starter_album_added', 'starter_album_linked', 'catalog_expanded', 'catalog_complete')
          )
        THEN LOWER(TRIM(artist_name))
        ELSE NULL
      END) AS artists_added,
      COALESCE(SUM(COALESCE(albums_added_count, 0)), 0) AS albums_added
    FROM lidarr_artist_progress
    WHERE user_plex_id = ?
  `);
  const resolveLidarrStats = (userId) => {
    const usage = lidarrUsageTotalsStmt.get(userId) || {};
    const progress = lidarrProgressTotalsStmt.get(userId) || {};
    return {
      artistsAdded: Math.max(Number(usage.artists_added || 0), Number(progress.artists_added || 0)),
      albumsAdded: Math.max(Number(usage.albums_added || 0), Number(progress.albums_added || 0)),
      tracksAdded: null,
    };
  };
  const livePlexUsers = [];
  const homeThumbByKey = new Map();
  const homeUserKeySet = new Set();
  const token = String(config?.plex?.token || '').trim();
  const machineId = String(config?.plex?.machineId || '').trim();
  if (token && fetchPlexUser && parsePlexUsers) {
    try {
      const [usersRes, ownerUser, homeUsers] = await Promise.all([
        fetch('https://plex.tv/api/users', { headers: { Accept: 'application/xml', 'X-Plex-Token': token } }),
        fetchPlexUser(token).catch(() => null),
        fetchPlexHomeUsers ? fetchPlexHomeUsers(token).catch(() => []) : [],
      ]);
      (Array.isArray(homeUsers) ? homeUsers : []).forEach((homeUser) => {
        const thumb = String(homeUser?.thumb || '').trim();
        normalizeIdentityList([
          homeUser?.title,
          homeUser?.username,
          homeUser?.email,
          String(homeUser?.id || ''),
          String(homeUser?.uuid || ''),
        ]).forEach((value) => {
          const key = String(value || '').trim().toLowerCase();
          if (!key) return;
          if (thumb && !homeThumbByKey.has(key)) homeThumbByKey.set(key, thumb);
          if (!homeUser?.admin) homeUserKeySet.add(key);
        });
      });
      if (usersRes.ok) {
        const xmlText = await usersRes.text();
        const users = parsePlexUsers(xmlText, { machineId }).map((user) => {
          const thumb = String(user?.thumb || '').trim();
          if (thumb) return user;
          const ids = normalizeIdentityList([
            user?.email,
            user?.username,
            user?.title,
            String(user?.id || ''),
            String(user?.uuid || ''),
          ]).map((value) => String(value || '').trim().toLowerCase());
          const matchedThumb = ids.map((id) => homeThumbByKey.get(id)).find(Boolean) || '';
          return matchedThumb ? { ...user, thumb: matchedThumb } : user;
        });
        users.forEach((user) => livePlexUsers.push(user));
      }
      if (ownerUser) {
        const ownerIds = new Set(
          normalizeIdentityList([ownerUser.email, ownerUser.username, ownerUser.title, String(ownerUser.id || ''), String(ownerUser.uuid || '')])
            .map((value) => value.toLowerCase()),
        );
        const alreadyPresent = livePlexUsers.some((user) => normalizeIdentityList([
          user?.email,
          user?.username,
          user?.title,
          String(user?.id || ''),
          String(user?.uuid || ''),
        ]).some((value) => ownerIds.has(String(value || '').toLowerCase())));
        if (!alreadyPresent) {
          livePlexUsers.unshift({
            id: String(ownerUser.id || ''),
            uuid: String(ownerUser.uuid || ''),
            username: ownerUser.username || '',
            email: ownerUser.email || '',
            title: ownerUser.title || ownerUser.username || ownerUser.email || '',
            thumb: String(ownerUser.thumb || ''),
            lastSeenAt: '',
          });
        }
      }
    } catch (_err) {
      // Fall back to observed IDs only when live Plex user lookup is unavailable.
    }
  }

  // ── Jellyfin / Emby: fetch live user list when no Plex token is configured ──
  const msType = String(config?.mediaServer?.type || 'plex').trim().toLowerCase();
  if (livePlexUsers.length === 0 && (msType === 'jellyfin' || msType === 'emby')) {
    const msCfg = config?.[msType] || {};
    const msUrl  = String(msCfg?.url    || '').trim();
    const msKey  = String(msCfg?.apiKey || '').trim();
    if (msUrl && msKey) {
      try {
        const adapter = msType === 'emby' ? embyAdapter : jellyfinAdapter;
        const users = await adapter.getUsers(msUrl, msKey);
        for (const u of users) {
          livePlexUsers.push({
            id:          u.name,
            uuid:        u.id,
            username:    u.name,
            email:       '',
            title:       u.name,
            thumb:       '',
            lastSeenAt:  '',
          });
        }
      } catch (_err) {
        // Fall back to observed IDs.
      }
    }
  }

  const livePlexRows = livePlexUsers.map((user) => {
    const ids = normalizeIdentityList([
      user?.email,
      user?.username,
      user?.title,
      String(user?.id || ''),
      String(user?.uuid || ''),
    ]).map((value) => value.toLowerCase());
    const userId = String(user?.username || user?.title || user?.email || user?.id || user?.uuid || '').trim();
    if (!userId) return null;
    const stats7d = getPlayStats(db, userId, since7d) || {};
    const stats30d = getPlayStats(db, userId, since30d) || {};
    const statsAll = getPlayStats(db, userId, 0) || {};
    const playlistCount = Number(playlistCountStmt.get(userId)?.n || 0);
    const personalPlaylistCount = Number(personalPlaylistCountStmt.get(userId)?.n || 0);
    const topArtist = getTopArtists(db, userId, 1)[0]?.artist_name || '';
    const lastSync = getLastPlaylistSync(db, userId);
    const lastPlayAt = Number(lastPlayStmt.get(userId)?.last_play_at || 0);
    const lidarrStats = resolveLidarrStats(userId);
    const plays7d = Number(stats7d.total_plays || 0);
    const plays30d = Number(stats30d.total_plays || 0);
    const playsAll = Number(statsAll.total_plays || 0);
    const skips7d = Number(stats7d.total_skips || 0);
    const skips30d = Number(stats30d.total_skips || 0);
    const skipsAll = Number(statsAll.total_skips || 0);
    let role = 'user';
    if (ownerKey && ids.includes(ownerKey)) role = 'admin';
    else if (hasRoleMatch(roleSets.disabled, ids)) role = 'disabled';
    else if (hasRoleMatch(roleSets.admin, ids)) role = 'admin';
    else if (hasRoleMatch(roleSets.coAdmin, ids)) role = 'co-admin';
    else if (hasRoleMatch(roleSets.powerUser, ids)) role = 'power-user';
    else if (hasRoleMatch(roleSets.guest, ids)) role = 'guest';
    return {
      id: userId,
      name: String(user?.title || user?.username || user?.email || userId).trim(),
      avatarUrl: String(user?.thumb || '').trim(),
      avatarLabel: String(user?.title || user?.username || userId).trim().charAt(0).toUpperCase() || 'P',
      isHomeUser: ids.some((id) => homeUserKeySet.has(id)),
      role,
      plays7d,
      plays30d,
      playsAll,
      skips7d,
      skips30d,
      skipsAll,
      skipRate7d: plays7d > 0 ? skips7d / plays7d : 0,
      skipRate30d: plays30d > 0 ? skips30d / plays30d : 0,
      skipRateAll: playsAll > 0 ? skipsAll / playsAll : 0,
      uniqueArtists: Number(statsAll.unique_artists || 0),
      uniqueTracks: Number(statsAll.unique_tracks || 0),
      totalListenMs: Number(statsAll.total_listen_ms || 0),
      playlistCount,
      personalPlaylistCount,
      lidarrArtistsAdded: lidarrStats.artistsAdded,
      lidarrAlbumsAdded: lidarrStats.albumsAdded,
      lidarrTracksAdded: lidarrStats.tracksAdded,
      topArtist,
      lastPlayAt,
      lastPlaylistSyncAt: Number(lastSync?.synced_at || 0),
      lastCuratorrLogin: resolveLogin(ids),
      lastPlexLogin: ids.find((id) => plexLogins[id]) ? plexLogins[ids.find((id) => plexLogins[id])] : '',
    };
  }).filter(Boolean);

  const livePlexKeySet = new Set(livePlexRows.map((entry) => String(entry.id || '').trim().toLowerCase()));
  const fallbackRows = livePlexRows.length ? [] : fallbackPlexUserIds
    .filter((userId) => !livePlexKeySet.has(String(userId || '').trim().toLowerCase()))
    .map((userId) => {
      const ids = normalizeIdentityList([userId]).map((value) => value.toLowerCase());
      const stats7d = getPlayStats(db, userId, since7d) || {};
      const stats30d = getPlayStats(db, userId, since30d) || {};
      const statsAll = getPlayStats(db, userId, 0) || {};
      const playlistCount = Number(playlistCountStmt.get(userId)?.n || 0);
      const personalPlaylistCount = Number(personalPlaylistCountStmt.get(userId)?.n || 0);
      const topArtist = getTopArtists(db, userId, 1)[0]?.artist_name || '';
      const lastSync = getLastPlaylistSync(db, userId);
      const lastPlayAt = Number(lastPlayStmt.get(userId)?.last_play_at || 0);
      const lidarrStats = resolveLidarrStats(userId);
      const plays7d = Number(stats7d.total_plays || 0);
      const plays30d = Number(stats30d.total_plays || 0);
      const playsAll = Number(statsAll.total_plays || 0);
      const skips7d = Number(stats7d.total_skips || 0);
      const skips30d = Number(stats30d.total_skips || 0);
      const skipsAll = Number(statsAll.total_skips || 0);
      return {
        id: userId,
        name: userId,
        avatarUrl: '',
        avatarLabel: userId.charAt(0).toUpperCase() || 'P',
        isHomeUser: false,
        role: resolveConfiguredPlexRole(userId, roleSets),
        plays7d,
        plays30d,
        playsAll,
        skips7d,
        skips30d,
        skipsAll,
        skipRate7d: plays7d > 0 ? skips7d / plays7d : 0,
        skipRate30d: plays30d > 0 ? skips30d / plays30d : 0,
        skipRateAll: playsAll > 0 ? skipsAll / playsAll : 0,
        uniqueArtists: Number(statsAll.unique_artists || 0),
        uniqueTracks: Number(statsAll.unique_tracks || 0),
        totalListenMs: Number(statsAll.total_listen_ms || 0),
        playlistCount,
        personalPlaylistCount,
        lidarrArtistsAdded: lidarrStats.artistsAdded,
        lidarrAlbumsAdded: lidarrStats.albumsAdded,
        lidarrTracksAdded: lidarrStats.tracksAdded,
        topArtist,
        lastPlayAt,
        lastPlaylistSyncAt: Number(lastSync?.synced_at || 0),
        lastCuratorrLogin: resolveLogin(ids),
        lastPlexLogin: ids.find((id) => plexLogins[id]) ? plexLogins[ids.find((id) => plexLogins[id])] : '',
      };
    });

  const plexUsers = [...livePlexRows, ...fallbackRows].sort((a, b) => {
    if (b.plays30d !== a.plays30d) return b.plays30d - a.plays30d;
    if (b.playsAll !== a.playsAll) return b.playsAll - a.playsAll;
    if (b.lastPlayAt !== a.lastPlayAt) return b.lastPlayAt - a.lastPlayAt;
    return a.name.localeCompare(b.name, undefined, { sensitivity: 'base', numeric: true });
  });

  const currentUserKey = String(currentUser?.username || '').trim().toLowerCase();
  const localUsers = normalizedLocalUsers.map((entry) => {
    const loginKey = String(entry.email || entry.username || '').trim().toLowerCase();
    return {
      username: String(entry.username || '').trim(),
      email: String(entry.email || '').trim(),
      role: String(entry.role || 'user').trim().toLowerCase(),
      lastCuratorrLogin: String(userLogins[loginKey] || '').trim(),
      isSetupAdmin: Boolean(entry.isSetupAdmin || entry.setupAccount),
      isCurrentSessionUser: currentUserKey && currentUserKey === String(entry.username || '').trim().toLowerCase(),
    };
  });

  const summary = {
    plexUserCount: plexUsers.length,
    activePlexUsers7d: plexUsers.filter((entry) => entry.plays7d > 0).length,
    activePlexUsers30d: plexUsers.filter((entry) => entry.plays30d > 0).length,
    activePlexUsersAll: plexUsers.filter((entry) => entry.playsAll > 0).length,
    totalPlexPlays7d: plexUsers.reduce((sum, entry) => sum + entry.plays7d, 0),
    totalPlexPlays30d: plexUsers.reduce((sum, entry) => sum + entry.plays30d, 0),
    totalPlexPlaysAll: plexUsers.reduce((sum, entry) => sum + entry.playsAll, 0),
    totalPlaylists: plexUsers.reduce((sum, entry) => sum + Number(entry.playlistCount || 0) + Number(entry.personalPlaylistCount || 0), 0),
    totalLidarrArtistsAdded: plexUsers.reduce((sum, entry) => sum + Number(entry.lidarrArtistsAdded || 0), 0),
    totalLidarrAlbumsAdded: plexUsers.reduce((sum, entry) => sum + Number(entry.lidarrAlbumsAdded || 0), 0),
    totalLidarrTracksAdded: plexUsers.some((entry) => entry.lidarrTracksAdded != null)
      ? plexUsers.reduce((sum, entry) => sum + Number(entry.lidarrTracksAdded || 0), 0)
      : null,
    localAccountCount: localUsers.length,
  };

  return { plexUsers, localUsers, summary };
}

async function buildLocalAdminPreviewData({
  req,
  db,
  getActualRole,
  getPreviewUserId,
  setPreviewUserId,
  resolveLocalUsers,
  normalizeIdentityList,
  fetchPlexUser,
  fetchPlexHomeUsers,
  parsePlexUsers,
  config,
}) {
  const currentUser = req.session?.user || {};
  const source = String(currentUser?.source || '').trim().toLowerCase();
  if (getActualRole(req) !== 'admin' || source !== 'local') return null;

  const localIdentitySet = new Set(
    resolveLocalUsers(config)
      .flatMap((entry) => [entry?.username, entry?.email])
      .map((value) => String(value || '').trim().toLowerCase())
      .filter(Boolean),
  );

  const observedRows = db.prepare(`
    SELECT user_plex_id, MAX(started_at) AS last_play_at
    FROM play_events
    WHERE TRIM(COALESCE(user_plex_id, '')) != ''
    GROUP BY user_plex_id
    ORDER BY last_play_at DESC, user_plex_id COLLATE NOCASE ASC
  `).all().map((row) => ({
    id: String(row.user_plex_id || '').trim(),
    lastPlayAt: Number(row.last_play_at || 0),
  })).filter((row) => {
    const id = String(row.id || '').trim();
    if (!id) return false;
    return !localIdentitySet.has(id.toLowerCase());
  });

  const msTypePreview = String(config?.mediaServer?.type || 'plex').trim().toLowerCase();
  const isNonPlexMs = msTypePreview === 'jellyfin' || msTypePreview === 'emby';

  // ── Jellyfin / Emby: always use the media server's user list as the source of truth ──
  if (isNonPlexMs) {
    const msCfg = config?.[msTypePreview] || {};
    const msUrl = String(msCfg?.url || '').trim();
    const msKey = String(msCfg?.apiKey || '').trim();
    const msLabelPreview = msTypePreview === 'emby' ? 'Emby' : 'Jellyfin';
    let options = [];
    if (msUrl && msKey) {
      try {
        const adapter = msTypePreview === 'emby' ? embyAdapter : jellyfinAdapter;
        const msUsers = await adapter.getUsers(msUrl, msKey);
        // Build a map from lowercase name → DB-stored user_plex_id (preserves login-time casing)
        const dbUserRows = db.prepare('SELECT user_plex_id FROM user_preferences').all();
        const dbUserMap = new Map(dbUserRows.map((r) => [String(r.user_plex_id || '').toLowerCase(), r.user_plex_id]));
        // Build the observed-user set for last-play lookups
        const observedMap = new Map(observedRows.map((r) => [r.id.toLowerCase(), r.lastPlayAt]));
        options = msUsers.map((u) => {
          const lower = u.name.toLowerCase();
          // Use the DB-stored ID (login-time casing) if this user has logged in before, else Jellyfin name
          const id = dbUserMap.get(lower) || u.name;
          return {
            id,
            name:        u.name,
            avatarUrl:   '',
            avatarLabel: u.name.charAt(0).toUpperCase() || 'U',
            isOwner:     false,
            lastPlayAt:  observedMap.get(lower) || 0,
          };
        });
      } catch (_err) { /* fall through to empty */ }
    }
    if (!options.length) {
      setPreviewUserId(req, '');
      return {
        enabled: true,
        selectedUserId: '',
        selectedName: `No ${msLabelPreview} users found`,
        selectedAvatarUrl: '',
        selectedAvatarLabel: 'C',
        options: [],
      };
    }
    const selectedKey = String(getPreviewUserId(req) || '').trim().toLowerCase();
    const selectedOption = options.find((o) => o.id.toLowerCase() === selectedKey) || options[0];
    setPreviewUserId(req, selectedOption?.id || '');
    return {
      enabled: true,
      selectedUserId: String(selectedOption?.id || '').trim(),
      selectedName: String(selectedOption?.name || '').trim(),
      selectedAvatarUrl: '',
      selectedAvatarLabel: selectedOption?.avatarLabel || 'U',
      options,
      returnTo: req.originalUrl,
    };
  }

  // ── Plex: fall back to empty state when no observed users ─────────────────
  if (!observedRows.length) {
    setPreviewUserId(req, '');
    return {
      enabled: true,
      selectedUserId: '',
      selectedName: 'No observed Plex users',
      selectedAvatarUrl: '',
      selectedAvatarLabel: 'C',
      options: [],
    };
  }

  const livePlexData = await fetchLivePlexUsersWithHomeData({
    config,
    normalizeIdentityList,
    fetchPlexUser,
    fetchPlexHomeUsers,
    parsePlexUsers,
  });
  const liveUsers = livePlexData.liveUsers;
  const ownerIdentitySet = livePlexData.ownerIdentitySet;
  const liveUserByObservedKey = buildPlexIdentityLookup(liveUsers, normalizeIdentityList);

  // Deduplicate: if two observed user_plex_ids resolve to the same live Plex identity (e.g. one is
  // username and the other is email for the same account), keep only the first occurrence.
  // Also filter managed home users (no real Plex account) when the Plex API was reachable.
  const plexApiAvailable = liveUsers.length > 0;
  const seenPlexEntries = new Set();
  const options = observedRows.reduce((acc, row) => {
    const entry = liveUserByObservedKey.get(String(row.id || '').trim().toLowerCase()) || null;
    // When Plex API was reachable, skip users with no real Plex identity (managed home accounts)
    if (plexApiAvailable && !entry) return acc;
    // Deduplicate by Plex numeric id or uuid — two rows that resolve to the same Plex user are merged
    const entryDedupeKey = entry ? String(entry?.uuid || entry?.id || '').trim() : null;
    if (entryDedupeKey && seenPlexEntries.has(entryDedupeKey)) return acc;
    if (entryDedupeKey) seenPlexEntries.add(entryDedupeKey);
    const ids = normalizeIdentityList([
      entry?.email,
      entry?.username,
      entry?.title,
      String(entry?.id || ''),
      String(entry?.uuid || ''),
      String(row.id || ''),
    ]).map((value) => String(value || '').trim().toLowerCase());
    const name = String(entry?.title || entry?.username || entry?.email || row.id).trim() || row.id;
    acc.push({
      id: row.id,
      name,
      avatarUrl: String(entry?.thumb || '').trim(),
      avatarLabel: name.charAt(0).toUpperCase() || 'P',
      isOwner: ids.some((id) => ownerIdentitySet.has(id)),
      lastPlayAt: row.lastPlayAt,
    });
    return acc;
  }, []);

  const selectedKey = String(getPreviewUserId(req) || '').trim().toLowerCase();
  const selectedOption = options.find((option) => String(option.id || '').trim().toLowerCase() === selectedKey)
    || options.find((option) => option.isOwner)
    || options[0]
    || null;

  setPreviewUserId(req, selectedOption?.id || '');

  return {
    enabled: true,
    selectedUserId: String(selectedOption?.id || '').trim(),
    selectedName: String(selectedOption?.name || '').trim(),
    selectedAvatarUrl: String(selectedOption?.avatarUrl || '').trim(),
    selectedAvatarLabel: String(selectedOption?.avatarLabel || 'P').trim() || 'P',
    options,
    returnTo: req.originalUrl,
  };
}

async function buildBlendableUsers(
  db,
  config,
  adminPreview,
  resolveLocalUsers,
  normalizeIdentityList,
  fetchPlexUser,
  fetchPlexHomeUsers,
  parsePlexUsers,
) {
  try {
    const localIdentitySet = new Set(
      (resolveLocalUsers ? resolveLocalUsers(config) : [])
        .flatMap((user) => [user?.username, user?.email])
        .map((value) => String(value || '').trim().toLowerCase())
        .filter(Boolean),
    );

    // When adminPreview is available, use its options as the canonical deduplicated user list
    // (already filtered for local accounts and managed Plex accounts), then cross-check artist_stats.
    const adminOptions = adminPreview?.options || [];
    if (adminOptions.length) {
      const statsIds = new Set(
        db.prepare('SELECT DISTINCT user_plex_id FROM artist_stats WHERE TRIM(COALESCE(user_plex_id, \'\')) != \'\'')
          .all().map((r) => String(r.user_plex_id || '').trim().toLowerCase()).filter(Boolean),
      );
      return adminOptions
        .filter((opt) => {
          const id = String(opt?.id || '').trim();
          return id && !localIdentitySet.has(id.toLowerCase()) && statsIds.has(id.toLowerCase());
        })
        .map((opt) => {
          const id = String(opt.id || '').trim();
          const name = String(opt?.name || id).trim() || id;
          return {
            id,
            name,
            avatarUrl: String(opt?.avatarUrl || '').trim(),
            avatarLabel: String(opt?.avatarLabel || name.charAt(0).toUpperCase() || 'P').trim() || 'P',
          };
        });
    }

    // Fallback when adminPreview is not available (non-admin or Plex-logged-in user)
    const userIds = db.prepare(
      'SELECT DISTINCT user_plex_id FROM artist_stats WHERE TRIM(COALESCE(user_plex_id, \'\')) != \'\' ORDER BY user_plex_id COLLATE NOCASE',
    ).all()
      .map((r) => String(r.user_plex_id || '').trim())
      .filter((id) => id && !localIdentitySet.has(id.toLowerCase()));

    if (!userIds.length) return [];

    const msType = String(config?.mediaServer?.type || 'plex').trim().toLowerCase();
    if (msType !== 'plex') {
      return userIds.map((id) => ({
        id,
        name: id,
        avatarUrl: '',
        avatarLabel: id.charAt(0).toUpperCase() || 'P',
      }));
    }

    const { liveUsers, homeUsers } = await fetchLivePlexUsersWithHomeData({
      config,
      normalizeIdentityList,
      fetchPlexUser,
      fetchPlexHomeUsers,
      parsePlexUsers,
    });
    const liveUserByObservedKey = buildPlexIdentityLookup(liveUsers, normalizeIdentityList);
    const homeUserByObservedKey = buildPlexIdentityLookup(homeUsers, normalizeIdentityList);
    const seenEntries = new Set();

    return userIds.reduce((acc, id) => {
      const key = String(id || '').trim().toLowerCase();
      if (!key) return acc;
      const liveEntry = liveUserByObservedKey.get(key) || null;
      const homeEntry = homeUserByObservedKey.get(key) || null;
      const canonicalEntry = liveEntry || homeEntry || null;
      const dedupeKey = canonicalEntry
        ? `${liveEntry ? 'plex' : 'home'}:${String(canonicalEntry.uuid || canonicalEntry.id || id).trim().toLowerCase()}`
        : `observed:${key}`;
      if (seenEntries.has(dedupeKey)) return acc;
      seenEntries.add(dedupeKey);
      const name = String(canonicalEntry?.title || canonicalEntry?.username || canonicalEntry?.email || id).trim() || id;
      acc.push({
        id,
        name,
        avatarUrl: String(canonicalEntry?.thumb || '').trim(),
        avatarLabel: name.charAt(0).toUpperCase() || 'P',
      });
      return acc;
    }, []);
  } catch { return []; }
}

function buildBlendArtistMap(db, userId) {
  return new Map(
    db.prepare('SELECT artist_name, ranking_score, play_count FROM artist_stats WHERE user_plex_id = ?').all(userId)
      .map((row) => [String(row.artist_name || '').trim().toLowerCase(), row]),
  );
}

function summarizeBlendArtistMaps(userArtistMaps, { skipRank, belterRank }) {
  const allArtistKeys = new Set(userArtistMaps.flatMap((map) => [...map.keys()]));
  const sharedArtistKeys = [...allArtistKeys].filter((key) => userArtistMaps.every((map) => map.has(key)));

  let compatibilityScore = null;
  let sharedBelters = 0;
  let agreedSkips = 0;

  if (sharedArtistKeys.length) {
    let totalSimilarity = 0;
    for (const key of sharedArtistKeys) {
      const scores = userArtistMaps.map((map) => Number(map.get(key)?.ranking_score || 0));
      const mean = scores.reduce((sum, score) => sum + score, 0) / scores.length;
      const avgDeviation = scores.reduce((sum, score) => sum + Math.abs(score - mean), 0) / scores.length;
      totalSimilarity += 1 - avgDeviation / 10;
      if (scores.every((score) => score >= belterRank)) sharedBelters++;
      if (scores.every((score) => score <= skipRank)) agreedSkips++;
    }
    compatibilityScore = Math.round((totalSimilarity / sharedArtistKeys.length) * 100);
  }

  return {
    rawCompatibilityScore: compatibilityScore,
    compatibilityScore: applyBlendConfidence(compatibilityScore, sharedArtistKeys.length),
    sharedArtists: sharedArtistKeys.length,
    totalArtists: allArtistKeys.size,
    sharedBelters,
    agreedSkips,
  };
}

function applyBlendConfidence(rawScore, sharedArtists) {
  if (rawScore === null || rawScore === undefined) return null;
  const confidence = Math.max(0, Math.min(1, Number(sharedArtists || 0) / 30));
  return Math.round(50 + (Number(rawScore || 0) - 50) * confidence);
}

function pickBlendShowcasePlaylist(playlists = []) {
  const ordered = [
    playlists.find((playlist) => String(playlist?.playlistType || '').trim() === 'curative' || String(playlist?.playlistKey || '').trim() === 'curative'),
    playlists.find((playlist) => String(playlist?.playlistType || '').trim() === 'crescive' || String(playlist?.playlistKey || '').trim() === 'crescive'),
    playlists.find((playlist) => String(playlist?.playlistType || '').trim() === 'daily-mix' || String(playlist?.playlistKey || '').trim() === 'daily-mix'),
    playlists[0] || null,
  ];
  return ordered.find(Boolean) || null;
}

async function attachPlaylistArtwork(cards, config, fetchPlexPlaylistsForToken) {
  const playlistCards = Array.isArray(cards) ? cards : [];
  if (!playlistCards.length) return playlistCards;

  const msType = String(config?.mediaServer?.type || 'plex').toLowerCase();
  if (msType === 'jellyfin' || msType === 'emby') {
    const { url: msUrl, apiKey: msApiKey } = config[msType] || {};
    const playlistIds = playlistCards.map((playlist) => String(playlist?.plexPlaylistId || '')).filter(Boolean);
    const imageTagMap = {};
    if (msUrl && msApiKey && playlistIds.length) {
      try {
        const batchUrl = new URL('/Items', msUrl);
        batchUrl.searchParams.set('Ids', playlistIds.join(','));
        batchUrl.searchParams.set('Fields', 'ImageTags');
        const batchRes = await fetch(batchUrl.toString(), {
          headers: { 'X-Emby-Token': msApiKey },
          signal: AbortSignal.timeout(6000),
        });
        if (batchRes.ok) {
          const batchJson = await batchRes.json();
          for (const item of batchJson?.Items || []) {
            const tag = item?.ImageTags?.Primary;
            if (item.Id && tag) imageTagMap[String(item.Id)] = String(tag);
          }
        }
      } catch {
        // Leave cards on fallback artwork if media-server metadata is unavailable.
      }
    }
    playlistCards.forEach((playlist) => {
      if (!playlist?.plexPlaylistId) return;
      playlist.artPath = String(playlist.plexPlaylistId);
      const tag = imageTagMap[String(playlist.plexPlaylistId || '')];
      if (tag) playlist.artTag = tag;
    });
    return playlistCards;
  }

  const { url: plexUrl, token: plexToken } = config.plex || {};
  if (plexUrl && plexToken && fetchPlexPlaylistsForToken) {
    try {
      const plexPlaylists = await fetchPlexPlaylistsForToken(plexUrl, plexToken);
      const plexPlaylistMap = new Map(
        plexPlaylists.map((playlist) => [String(playlist.ratingKey || ''), playlist]),
      );
      playlistCards.forEach((playlist) => {
        const plexPlaylist = plexPlaylistMap.get(String(playlist?.plexPlaylistId || '')) || null;
        playlist.artPath = String(plexPlaylist?.composite || plexPlaylist?.thumb || plexPlaylist?.art || '');
      });
    } catch {
      // Leave cards on fallback artwork if Plex metadata is unavailable.
    }
  }

  return playlistCards;
}

function finalizeBlendArtwork(cards, mediaServerType) {
  const msType = String(mediaServerType || 'plex').toLowerCase();
  for (const card of (Array.isArray(cards) ? cards : [])) {
    if (card?.artPath) {
      card.artUrl = msType === 'jellyfin' || msType === 'emby'
        ? `/api/ms/art?id=${encodeURIComponent(card.artPath)}${card.artTag ? `&tag=${encodeURIComponent(card.artTag)}` : ''}`
        : `/api/plex/art?path=${encodeURIComponent(card.artPath)}`;
    } else if (card?.fallbackTrackRatingKey) {
      card.artUrl = `/api/music/thumb/track/${encodeURIComponent(String(card.fallbackTrackRatingKey || ''))}`;
    } else {
      card.artUrl = '';
    }
  }
  return cards;
}

async function buildBlendCarouselUsers(db, config, currentUserId, blendableUsers, fetchPlexPlaylistsForToken) {
  const currentId = String(currentUserId || '').trim();
  if (!currentId) return [];

  const smartSettings = config.smartPlaylist || {};
  const skipRank = Number(smartSettings.artistSkipRank ?? 2);
  const belterRank = Number(smartSettings.artistBelterRank ?? 8);
  const currentArtistMap = buildBlendArtistMap(db, currentId);

  const cards = (Array.isArray(blendableUsers) ? blendableUsers : [])
    .filter((user) => String(user?.id || '').trim() && String(user.id).trim().toLowerCase() !== currentId.toLowerCase())
    .map((user) => {
      const userId = String(user.id || '').trim();
      const candidateArtistMap = buildBlendArtistMap(db, userId);
      const summary = summarizeBlendArtistMaps([currentArtistMap, candidateArtistMap], { skipRank, belterRank });
      const showcasePlaylist = pickBlendShowcasePlaylist(
        listUserGeneratedPlaylists(db, userId, { activeOnly: true }),
      );
      const fallbackTrack = showcasePlaylist?.playlistKey
        ? getPlaylistTracks(db, userId, showcasePlaylist.playlistKey)[0] || null
        : null;
      return {
        id: userId,
        name: String(user?.name || userId).trim() || userId,
        avatarUrl: String(user?.avatarUrl || '').trim(),
        avatarLabel: String(user?.avatarLabel || userId.charAt(0).toUpperCase() || 'P').trim() || 'P',
        compatibilityScore: summary.compatibilityScore,
        rawCompatibilityScore: summary.rawCompatibilityScore,
        sharedArtists: summary.sharedArtists,
        totalArtists: summary.totalArtists,
        sharedBelters: summary.sharedBelters,
        agreedSkips: summary.agreedSkips,
        plexPlaylistId: String(showcasePlaylist?.plexPlaylistId || '').trim(),
        playlistTitle: String(showcasePlaylist?.playlistTitle || '').trim(),
        playlistType: String(showcasePlaylist?.playlistType || '').trim(),
        artPath: '',
        artTag: '',
        fallbackTrackRatingKey: String(fallbackTrack?.ratingKey || '').trim(),
      };
    });

  await attachPlaylistArtwork(cards, config, fetchPlexPlaylistsForToken);
  finalizeBlendArtwork(cards, String(config?.mediaServer?.type || 'plex'));

  cards.sort((a, b) => {
    const aScore = a.compatibilityScore === null ? -1 : Number(a.compatibilityScore || 0);
    const bScore = b.compatibilityScore === null ? -1 : Number(b.compatibilityScore || 0);
    if (bScore !== aScore) return bScore - aScore;
    if (Number(b.sharedArtists || 0) !== Number(a.sharedArtists || 0)) {
      return Number(b.sharedArtists || 0) - Number(a.sharedArtists || 0);
    }
    return String(a.id || '').localeCompare(String(b.id || ''));
  });

  return cards;
}

export function registerPages(app, ctx) {
  const {
    requireUser,
    requireAdmin,
    requireWizardComplete,
    requireUserWizardComplete,
    loadConfig,
    resolveLocalUsers,
    getActualRole,
    getEffectiveRole,
    getPreviewUserId,
    setPreviewUserId,
    canUserAccessLidarrAutomation,
    normalizeIdentityList,
    loadAdmins,
    loadCoAdmins,
    loadPowerUsers,
    loadGuestUsers,
    loadDisabledUsers,
    db,
    recommendationService,
    playlistService,
    lidarrService,
    fetchPlexPlaylistsForToken,
    fetchPlexUser,
    fetchPlexHomeUsers,
    parsePlexUsers,
  } = ctx;

  // Root redirect
  app.get('/', (req, res) => {
    if (!req.session?.user) return res.redirect('/login');
    return res.redirect('/dashboard');
  });

  async function buildPageScope(req, config) {
    const adminPreview = await buildLocalAdminPreviewData({
      req,
      db,
      getActualRole,
      getPreviewUserId,
      setPreviewUserId,
      resolveLocalUsers,
      normalizeIdentityList,
      fetchPlexUser,
      fetchPlexHomeUsers,
      parsePlexUsers,
      config,
    });
    const previewUserId = String(adminPreview?.selectedUserId || '').trim();
    const user = req.session?.user || {};
    const role = getEffectiveRole(req);
    const scopedUserId = previewUserId || String(user.username || '').trim();
    return {
      adminPreview,
      role,
      userPlexId: scopedUserId,
      suggestionUserId: scopedUserId,
      personalUserId: scopedUserId,
    };
  }

  // ── Admin users ────────────────────────────────────────────────────────────

  app.get('/admin/users', requireAdmin, requireWizardComplete, async (req, res) => {
    const config = loadConfig();
    const adminUsersData = await buildAdminUsersPageData({
      db,
      config,
      resolveLocalUsers,
      normalizeIdentityList,
      loadAdmins,
      loadCoAdmins,
      loadPowerUsers,
      loadGuestUsers,
      loadDisabledUsers,
      fetchPlexUser: ctx.fetchPlexUser,
      fetchPlexHomeUsers: ctx.fetchPlexHomeUsers,
      parsePlexUsers: ctx.parsePlexUsers,
      currentUser: req.session?.user,
    });
    res.render('admin-users', {
      title: 'Users — Curatorr',
      user: req.session.user,
      role: getEffectiveRole(req),
      actualRole: getActualRole(req),
      config: safeConfig(config),
      plexUsers: adminUsersData.plexUsers,
      localUsers: adminUsersData.localUsers,
      summary: adminUsersData.summary,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css', '/styles-settings.css'],
    });
  });

  // ── Dashboard ─────────────────────────────────────────────────────────────

  app.get('/dashboard', requireUser, requireWizardComplete, requireUserWizardComplete, async (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const { adminPreview, role, userPlexId, suggestionUserId } = await buildPageScope(req, config);

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
    const stats7d  = normalizeStats(getPlayStats(db, userPlexId, since7d));
    const stats30d = normalizeStats(getPlayStats(db, userPlexId, since30d));
    const statsAll = normalizeStats(getPlayStats(db, userPlexId, 0));
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
    const generatedPlaylists = playlistService?.listGenerated(userPlexId, { activeOnly: true }) || [];
    const _dashLastSync = getLastPlaylistSync(db, suggestionUserId);
    let dashboardPlaylists = generatedPlaylists.map((playlist) => ({
      ...playlist,
      artPath: '',
      curatorrUpdatedAt: Number(playlist.lastBuiltAt || playlist.lastSyncedAt || playlist.updatedAt || playlist.createdAt || 0),
      tracksAdded: Number(_dashLastSync?.tracks_added || 0),
      tracksRemoved: Number(_dashLastSync?.tracks_removed || 0),
    }));
    const { url: plexUrl, token: plexToken } = config.plex || {};
    if (dashboardPlaylists.length && plexUrl && plexToken && fetchPlexPlaylistsForToken) {
      try {
        const plexPlaylists = await fetchPlexPlaylistsForToken(plexUrl, plexToken);
        const plexPlaylistMap = new Map(
          plexPlaylists.map((playlist) => [String(playlist.ratingKey || ''), playlist]),
        );
        dashboardPlaylists = dashboardPlaylists.map((playlist) => {
          const plexPlaylist = plexPlaylistMap.get(String(playlist.plexPlaylistId || '')) || null;
          return {
            ...playlist,
            artPath: String(plexPlaylist?.composite || plexPlaylist?.thumb || plexPlaylist?.art || ''),
            plexUpdatedAt: Number(plexPlaylist?.updatedAt || 0),
          };
        });
      } catch (_err) {
        // Fall back to placeholder art; dashboard playlist metadata still renders.
      }
    }
    dashboardPlaylists.sort((a, b) => {
      const byUpdated = Number(b.curatorrUpdatedAt || 0) - Number(a.curatorrUpdatedAt || 0);
      if (byUpdated) return byUpdated;
      return String(a.playlistTitle || '').localeCompare(String(b.playlistTitle || ''));
    });
    const dashboardSuggestions = loadSuggestionBundle(recommendationService, suggestionUserId, { artistLimit: 8 });
    const lidarrStatus = await buildLidarrStatusBundle(db, lidarrService, suggestionUserId, dashboardSuggestions.artists);
    const lidarrAutomationEligible = canUserAccessLidarrAutomation(loadConfig(), { ...req.session.user, role });
    const lidarrQuota = lidarrAutomationEligible && lidarrService
      ? lidarrService.getRoleQuota(role, getCurrentLidarrUsage(db, suggestionUserId).usage || {})
      : null;

    const masterTrackCount = getMasterTrackCount(db);
    const masterArtistCount = getMasterArtistCount(db);
    const lastPlaylistSync = getLastPlaylistSync(db, suggestionUserId);
    const excludedTrackCount = getExcludedTrackKeys(db, suggestionUserId).length;
    const skipTierArtistCount = getSkipTierArtists(db, suggestionUserId).length;
    const belterTrackCount = db.prepare("SELECT COUNT(*) AS n FROM track_stats WHERE user_plex_id = ? AND tier = 'belter'").get(suggestionUserId)?.n || 0;
    const heardTrackCount = db.prepare("SELECT COUNT(*) AS n FROM track_stats WHERE user_plex_id = ? AND tier != 'curatorr'").get(suggestionUserId)?.n || 0;

    res.render('dashboard', {
      title: 'Dashboard — Curatorr',
      user,
      role,
      actualRole: getActualRole(req),
      adminPreview,
      config: safeConfig(config),
      stats7d,
      stats30d,
      statsAll,
      byDay,
      topArtists,
      topTracks,
      recentHistory,
      dashboardPlaylists,
      lidarrStatus,
      lidarrQuota,
      lidarrAutomationEligible,
      masterTrackCount,
      masterArtistCount,
      lastPlaylistSync,
      excludedTrackCount,
      skipTierArtistCount,
      belterTrackCount,
      heardTrackCount,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── History ───────────────────────────────────────────────────────────────

  app.get('/history', requireUser, requireWizardComplete, requireUserWizardComplete, async (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const { adminPreview, role, userPlexId } = await buildPageScope(req, config);
    const offset = Math.max(0, Number(req.query?.offset || 0));
    const limit = 100;
    const { history, hasMore } = paginateRolledHistory(
      (chunkLimit, chunkOffset) => getRecentHistory(db, userPlexId, chunkLimit, chunkOffset).map((event) => ({
        ...event,
        track_title: stripArtistSuffix(event.track_title, event.artist_name),
        curatorrTier: deriveHistoryTier(event, config),
      })),
      { limit, offset },
    );

    res.render('history', {
      title: 'Play History — Curatorr',
      user,
      role,
      actualRole: getActualRole(req),
      adminPreview,
      config: safeConfig(config),
      history,
      hasMore,
      offset,
      limit,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── Artists ───────────────────────────────────────────────────────────────

  app.get('/artists', requireUser, requireWizardComplete, requireUserWizardComplete, async (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const { adminPreview, role, userPlexId, suggestionUserId } = await buildPageScope(req, config);
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
      adminPreview,
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
    const { adminPreview, role, userPlexId, suggestionUserId } = await buildPageScope(req, config);
    const lidarrAutomationEligible = canUserAccessLidarrAutomation(loadConfig(), { ...req.session.user, role });
    const lidarrQuota = lidarrService?.isConfigured() && lidarrAutomationEligible
      ? lidarrService.getRoleQuota(role, getCurrentLidarrUsage(db, userPlexId).usage || {})
      : null;
    const queuedRequests = enrichDiscoverRequests(
      db,
      userPlexId,
      listLidarrRequests(db, userPlexId, { statuses: ['queued', 'processing'], limit: 200 }),
    );
    const requestHistory = enrichDiscoverRequests(
      db,
      userPlexId,
      listLidarrRequests(db, userPlexId, { statuses: ['completed', 'failed'], limit: 50 }),
    ).filter((r) => r?.detail?.reconciledAction !== 'already_in_lidarr');
    const discoverSuggestions = loadSuggestionBundle(recommendationService, suggestionUserId, { artistLimit: 16 });
    const lidarrStatus = await buildLidarrStatusBundle(db, lidarrService, suggestionUserId, discoverSuggestions.artists);
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
      adminPreview,
      config: safeConfig(config),
      lidarrAutomationEligible,
      lidarrQuota,
      suggestedArtists: lidarrStatus.actionableSuggestions,
      queuedRequests,
      requestHistory,
      lidarrStatus,
      discoveryConfig,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── Tracks ────────────────────────────────────────────────────────────────

  app.get('/tracks', requireUser, requireWizardComplete, requireUserWizardComplete, async (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const { adminPreview, role, userPlexId, suggestionUserId } = await buildPageScope(req, config);
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
      albumLimit: 10,
    });

    res.render('tracks', {
      title: 'Tracks — Curatorr',
      user,
      role,
      actualRole: getActualRole(req),
      adminPreview,
      config: safeConfig(config),
      tracks,
      completedKeys: [...completedKeys],
      suggestedTracks: suggestions.tracks,
      suggestedAlbums: suggestions.albums,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── Playlists ─────────────────────────────────────────────────────────────

  app.get('/playlists', requireUser, requireWizardComplete, requireUserWizardComplete, async (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const { adminPreview, role, personalUserId: userPlexId } = await buildPageScope(req, config);
    const lastSync = getLastPlaylistSync(db, userPlexId);
    const generatedPlaylists = playlistService?.listGenerated(userPlexId, { activeOnly: true }) || [];
    const canonicalPlaylists = playlistService?.getCanonicalPlaylist(userPlexId) || { legacy: null, generated: [], curatorred: null };
    const generatedCards = generatedPlaylists
      .map((playlist) => ({
        playlistKind: 'generated',
        playlistKey: String(playlist.playlistKey || ''),
        playlistType: String(playlist.playlistType || ''),
        plexPlaylistId: String(playlist.plexPlaylistId || ''),
        playlistTitle: String(playlist.playlistTitle || playlist.playlistKey || 'Playlist'),
        trackCount: Number(playlist.trackCount || 0),
        curatorrUpdatedAt: Number(playlist.lastBuiltAt || playlist.lastSyncedAt || playlist.updatedAt || playlist.createdAt || 0),
        state: playlist.plexPlaylistId ? 'synced' : 'pending',
        description: String(playlist.playlistType || 'generated'),
        artPath: '',
        tracksAdded: Number(lastSync?.tracks_added || 0),
        tracksRemoved: Number(lastSync?.tracks_removed || 0),
      }))
      .sort((a, b) => Number(b.curatorrUpdatedAt || 0) - Number(a.curatorrUpdatedAt || 0) || a.playlistTitle.localeCompare(b.playlistTitle));
    const playlistCards = [];
    if (canonicalPlaylists.legacy) {
      playlistCards.push({
        playlistKind: 'legacy',
        playlistKey: '',
        playlistType: 'legacy',
        plexPlaylistId: String(canonicalPlaylists.legacy.playlist_id || ''),
        playlistTitle: String(canonicalPlaylists.legacy.playlist_title || 'Curatorred Playlist'),
        trackCount: Number(lastSync?.track_count || 0),
        curatorrUpdatedAt: Number(lastSync?.synced_at || 0),
        state: canonicalPlaylists.legacy.playlist_id ? 'synced' : 'pending',
        description: 'Current Curatorr playlist',
        artPath: '',
        tracksAdded: Number(lastSync?.tracks_added || 0),
        tracksRemoved: Number(lastSync?.tracks_removed || 0),
      });
    }
    playlistCards.push(...generatedCards);
    await attachPlaylistArtwork(playlistCards, config, fetchPlexPlaylistsForToken);

    res.render('playlists', {
      title: 'Playlists — Curatorr',
      user,
      role,
      actualRole: getActualRole(req),
      adminPreview,
      config: safeConfig(config),
      lastSync,
      playlistCards,
      plexMachineId: String(config.plex?.machineId || ''),
      allGenres:     (() => { try { return getGenresFromMaster(db); } catch { return []; } })(),
      allMoods:      (() => { try { return getMoodsFromMaster(db);  } catch { return []; } })(),
      allLastfmTags: (() => { try { return getAllLastfmTags(db);    } catch { return []; } })(),
      allUserIds:    (() => { try { return getAllUserIds(db);        } catch { return []; } })(),
      currentUserId: userPlexId,
      blendableUsers: await buildBlendableUsers(
        db,
        config,
        adminPreview,
        resolveLocalUsers,
        normalizeIdentityList,
        fetchPlexUser,
        fetchPlexHomeUsers,
        parsePlexUsers,
      ),
      userPersonalPlaylists: (() => { try { return listUserPersonalPlaylists(db, userPlexId); } catch { return []; } })(),
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── Blend ─────────────────────────────────────────────────────────────────

  app.get('/blend', requireUser, requireWizardComplete, requireUserWizardComplete, async (req, res) => {
    const config = loadConfig();
    const { adminPreview, role, userPlexId } = await buildPageScope(req, config);
    const blendableUsers = await buildBlendableUsers(
      db,
      config,
      adminPreview,
      resolveLocalUsers,
      normalizeIdentityList,
      fetchPlexUser,
      fetchPlexHomeUsers,
      parsePlexUsers,
    );
    const blendCarouselUsers = await buildBlendCarouselUsers(
      db,
      config,
      userPlexId,
      blendableUsers,
      fetchPlexPlaylistsForToken,
    );
    const currentBlendUser = blendableUsers.find((entry) => String(entry?.id || '').trim().toLowerCase() === String(userPlexId || '').trim().toLowerCase()) || null;
    res.render('blend', {
      title: 'Blend — Curatorr',
      user: req.session.user,
      role,
      actualRole: getActualRole(req),
      adminPreview,
      config: safeConfig(config),
      blendableUsers,
      blendCarouselUsers,
      currentUserId: userPlexId,
      currentUserName: String(currentBlendUser?.name || userPlexId).trim() || userPlexId,
      currentUserAvatarUrl: String(currentBlendUser?.avatarUrl || req.session?.user?.thumb || '').trim(),
      currentUserAvatarLabel: String(currentBlendUser?.avatarLabel || userPlexId).trim().charAt(0).toUpperCase() || 'P',
      mediaServerType: String(config?.mediaServer?.type || 'plex').toLowerCase(),
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
    const lastfmUsername = userPrefs?.lastfmUsername || '';
    const lastfmEnabledStations = userPrefs?.lastfmEnabledStations || [];
    const lastfmBackfillCursor = userPrefs?.lastfmBackfillCursor ?? 0;
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
      lastfmUsername,
      lastfmEnabledStations,
      lastfmBackfillCursor,
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
  let cached = { artists: [], albums: [], tracks: [] };
  try {
    cached = recommendationService.listCachedSuggestions(userPlexId, options) || cached;
  } catch (_err) {
    cached = { artists: [], albums: [], tracks: [] };
  }
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
