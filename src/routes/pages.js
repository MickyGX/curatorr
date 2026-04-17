// Page routes — HTML views

import { ARTIST_RECOMMENDATION_MODEL_VERSION } from '../services/recommendations.js';
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
  getGenresFromMaster,
  getMoodsFromMaster,
  getAllLastfmTags,
  getAllTrackDecadeTags,
  listUserPersonalPlaylists,
  listUserGeneratedPlaylists,
  getPlaylistTracks,
  getMasterTracks,
  getMasterTrackCount,
  getMasterArtistCount,
  getAlbumPopularTrackRanks,
  getExcludedTrackKeys,
  getSkipTierArtists,
  getAllUserIds,
  getDistinctPathSegments,
  classifyTier,
  listSuggestedAlbums,
} from '../db.js';
import { paginateRolledHistory } from '../history-rollup.js';
import { buildFeaturePresetAvailability } from '../services/playlists.js';
import { buildStoredPlaylistArtworkUrl } from '../services/playlist-artwork.js';
import { resolveLibraryAlbumMatch } from '../services/album-reconciliation.js';
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

function attachAlbumPopularity(items = [], popularityByKey = new Map(), keyField = 'rating_key') {
  return (Array.isArray(items) ? items : []).map((item) => {
    const popularity = popularityByKey.get(String(item?.[keyField] || '')) || null;
    return popularity
      ? { ...item, popularRank: popularity.rank, ratingCount: popularity.ratingCount }
      : { ...item, popularRank: null, ratingCount: Number(item?.ratingCount || item?.rating_count || 0) };
  });
}

function resolvePlaylistAudience(playlistType, playlistKey = '', personalPlaylistMap = new Map(), sourceType = '', audience = 'personal') {
  const type = String(playlistType || '').trim().toLowerCase();
  const source = String(sourceType || '').trim().toLowerCase();
  const aud = String(audience || 'personal').trim().toLowerCase();
  if ((['spotify-playlist', 'youtube-playlist', 'lastfm-station', 'listenbrainz-playlist'].includes(source) || source.startsWith('plex-')) && aud === 'global') return 'global';
  if (['spotify-playlist', 'youtube-playlist', 'lastfm-station', 'listenbrainz-playlist'].includes(source) || source.startsWith('plex-')) return 'imported';
  if (type === 'global') return 'global';
  if (['lastfm-station', 'listenbrainz-playlist'].includes(type)) return 'external';
  if (['legacy', 'curatorred', 'curatorr', 'curative', 'crescive', 'daily-mix'].includes(type)) return 'system';
  if (type === 'personal') {
    const personalId = String(playlistKey || '').replace(/^personal:/, '').trim();
    const personalDef = personalId ? personalPlaylistMap.get(personalId) : null;
    const blendUsers = Array.isArray(personalDef?.rules?.blendUsers) ? personalDef.rules.blendUsers.filter(Boolean) : [];
    return blendUsers.length ? 'blend' : 'personal';
  }
  return 'personal';
}

function getPlaylistAudienceSortRank(audience) {
  const kind = String(audience || '').trim().toLowerCase();
  if (kind === 'imported') return 1;
  if (kind === 'external') return 2;
  if (kind === 'blend') return 3;
  if (kind === 'global') return 4;
  if (kind === 'system') return 5;
  return 0;
}

function comparePlaylistCards(a, b) {
  const activeDelta = Number(Boolean(b?.active !== false)) - Number(Boolean(a?.active !== false));
  if (activeDelta) return activeDelta;
  const audienceDelta = getPlaylistAudienceSortRank(a?.playlistAudience) - getPlaylistAudienceSortRank(b?.playlistAudience);
  if (audienceDelta) return audienceDelta;
  const updatedDelta = Number(b?.curatorrUpdatedAt || 0) - Number(a?.curatorrUpdatedAt || 0);
  if (updatedDelta) return updatedDelta;
  return String(a?.playlistTitle || '').localeCompare(String(b?.playlistTitle || ''));
}

function normalizeIdentitySet(values = [], normalizeIdentityList) {
  return new Set(
    normalizeIdentityList(values)
      .map((value) => String(value || '').trim().toLowerCase())
      .filter(Boolean),
  );
}

const ROLE_RANK = { admin: 5, 'co-admin': 4, 'power-user': 3, user: 2, guest: 1, disabled: 0 };

function canUserSeeDiscoveryPipeline(config, userRole) {
  const disc = config?.discovery || {};
  const minRole = String(disc.similarArtistMinRole || 'power-user').trim().toLowerCase();
  if (minRole === 'disabled') return false;
  const userRank = ROLE_RANK[String(userRole || 'guest').trim().toLowerCase()] ?? 0;
  const minRank = ROLE_RANK[minRole] ?? 3;
  return userRank >= minRank;
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

function normalizeIdentityKeys(values = []) {
  return [...new Set(
    (Array.isArray(values) ? values : [values])
      .map((value) => String(value || '').trim().toLowerCase())
      .filter(Boolean),
  )];
}

function fetchGeneratedPlaylistsByIdentityKeys(db, identityKeys = []) {
  const keys = normalizeIdentityKeys(identityKeys);
  if (!keys.length) return [];
  const placeholders = keys.map(() => '?').join(',');
  return db.prepare(`
    SELECT playlist_key, playlist_type, source_type, active
    FROM user_generated_playlists
    WHERE active = 1 AND LOWER(user_plex_id) IN (${placeholders})
  `).all(...keys).map((row) => ({
    playlistKey: String(row.playlist_key || '').trim(),
    playlistType: String(row.playlist_type || '').trim(),
    sourceType: String(row.source_type || '').trim(),
    active: Boolean(row.active),
  }));
}

function fetchPersonalPlaylistsByIdentityKeys(db, identityKeys = []) {
  const keys = normalizeIdentityKeys(identityKeys);
  if (!keys.length) return [];
  const placeholders = keys.map(() => '?').join(',');
  return db.prepare(`
    SELECT id, rules
    FROM user_personal_playlists
    WHERE LOWER(user_plex_id) IN (${placeholders})
  `).all(...keys).map((row) => ({
    id: String(row.id || '').trim(),
    rules: (() => {
      try { return JSON.parse(String(row.rules || '{}')); } catch { return {}; }
    })(),
  }));
}

function fetchLidarrUsageByIdentityKeys(db, identityKeys = []) {
  const keys = normalizeIdentityKeys(identityKeys);
  if (!keys.length) return [];
  const placeholders = keys.map(() => '?').join(',');
  return db.prepare(`
    SELECT usage_key, COALESCE(SUM(amount), 0) AS total
    FROM lidarr_usage
    WHERE LOWER(user_plex_id) IN (${placeholders})
    GROUP BY usage_key
  `).all(...keys).map((row) => ({
    usageKey: String(row.usage_key || '').trim(),
    total: Number(row.total || 0),
  }));
}

export function summarizeAdminPlaylistCounts(generatedPlaylists = [], personalPlaylists = []) {
  const personalMap = new Map(
    (Array.isArray(personalPlaylists) ? personalPlaylists : [])
      .map((playlist) => [String(playlist?.id || '').trim(), playlist])
      .filter(([id]) => Boolean(id)),
  );
  const generatedByKey = new Map();
  (Array.isArray(generatedPlaylists) ? generatedPlaylists : []).forEach((playlist) => {
    if (!playlist || playlist.active === false) return;
    const playlistKey = String(playlist.playlistKey || '').trim();
    if (!playlistKey || generatedByKey.has(playlistKey)) return;
    generatedByKey.set(playlistKey, playlist);
  });

  let systemPlaylistCount = 0;
  let userPlaylistCount = 0;
  let otherPlaylistCount = 0;
  generatedByKey.forEach((playlist) => {
    const audience = resolvePlaylistAudience(
      playlist.playlistType,
      playlist.playlistKey,
      personalMap,
      playlist.sourceType,
    );
    if (audience === 'personal' || audience === 'blend') {
      userPlaylistCount += 1;
    } else if (audience === 'system') {
      systemPlaylistCount += 1;
    } else {
      otherPlaylistCount += 1;
    }
  });

  let draftPlaylistCount = 0;
  personalMap.forEach((_playlist, id) => {
    if (!generatedByKey.has(`personal:${id}`)) draftPlaylistCount += 1;
  });
  userPlaylistCount += draftPlaylistCount;

  return {
    playlistCount: systemPlaylistCount + otherPlaylistCount,
    systemPlaylistCount,
    userPlaylistCount,
    personalPlaylistCount: userPlaylistCount,
    otherPlaylistCount,
    draftPlaylistCount,
    playlistTotalCount: generatedByKey.size + draftPlaylistCount,
  };
}

export function summarizeAdminLidarrCounts(usageRows = []) {
  const totals = new Map();
  (Array.isArray(usageRows) ? usageRows : []).forEach((row) => {
    const usageKey = String(row?.usageKey || row?.usage_key || '').trim().toLowerCase();
    if (!usageKey) return;
    totals.set(usageKey, (totals.get(usageKey) || 0) + Number(row?.total || row?.amount || 0));
  });
  const tracksAdded = Number(totals.get('tracks') || 0);
  return {
    artistsAdded: Number(totals.get('artists') || 0),
    albumsAdded: Number(totals.get('albums') || 0),
    tracksAdded: tracksAdded > 0 ? tracksAdded : null,
  };
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
  const lastPlayStmt = db.prepare('SELECT MAX(started_at) AS last_play_at FROM play_events WHERE user_plex_id = ?');
  const resolveLidarrStats = (identityKeys) => summarizeAdminLidarrCounts(
    fetchLidarrUsageByIdentityKeys(db, identityKeys),
  );
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

  // Build lookup: lowercased Plex identity → live Plex user object.
  // Iterate observed DB IDs (not live Plex users) so that the exact stored identifier
  // is used for all stat queries — the same approach as buildLocalAdminPreviewData.
  const liveUserByKey = buildPlexIdentityLookup(livePlexUsers, normalizeIdentityList);
  const seenLiveUserUuids = new Set();
  const livePlexRows = observedPlexUserIds.map((dbId) => {
    const user = liveUserByKey.get(dbId.toLowerCase());
    if (!user) return null;
    // Deduplicate: same Plex user may appear under multiple DB IDs (e.g. username + title)
    const dedupeKey = String(user?.uuid || user?.id || '').trim().toLowerCase() || dbId.toLowerCase();
    if (seenLiveUserUuids.has(dedupeKey)) return null;
    seenLiveUserUuids.add(dedupeKey);
    const ids = normalizeIdentityList([
      user?.email,
      user?.username,
      user?.title,
      String(user?.id || ''),
      String(user?.uuid || ''),
    ]).map((value) => value.toLowerCase());
    const playlistIdentityKeys = normalizeIdentityKeys([...ids, dbId]);
    // dbId is the exact value stored in the DB — use it for all stat queries (correct case/format)
    const stats7d = getPlayStats(db, dbId, since7d) || {};
    const stats30d = getPlayStats(db, dbId, since30d) || {};
    const statsAll = getPlayStats(db, dbId, 0) || {};
    const playlistCounts = summarizeAdminPlaylistCounts(
      fetchGeneratedPlaylistsByIdentityKeys(db, playlistIdentityKeys),
      fetchPersonalPlaylistsByIdentityKeys(db, playlistIdentityKeys),
    );
    const topArtist = getTopArtists(db, dbId, 1)[0]?.artist_name || '';
    const lastSync = getLastPlaylistSync(db, dbId);
    const lastPlayAt = Number(lastPlayStmt.get(dbId)?.last_play_at || 0);
    const lidarrStats = resolveLidarrStats(playlistIdentityKeys);
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
    const displayName = String(user?.title || user?.username || user?.email || dbId).trim();
    return {
      id: dbId,
      name: displayName,
      avatarUrl: String(user?.thumb || '').trim(),
      avatarLabel: displayName.charAt(0).toUpperCase() || 'P',
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
      ...playlistCounts,
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
      const playlistCounts = summarizeAdminPlaylistCounts(
        fetchGeneratedPlaylistsByIdentityKeys(db, ids),
        fetchPersonalPlaylistsByIdentityKeys(db, ids),
      );
      const topArtist = getTopArtists(db, userId, 1)[0]?.artist_name || '';
      const lastSync = getLastPlaylistSync(db, userId);
      const lastPlayAt = Number(lastPlayStmt.get(userId)?.last_play_at || 0);
      const lidarrStats = resolveLidarrStats(ids);
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
        ...playlistCounts,
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
    totalPlaylists: plexUsers.reduce((sum, entry) => sum + Number(entry.playlistTotalCount || 0), 0),
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
  // Build a lookup so data lookups (playlists, preferences) use the canonical user_plex_id from
  // user_preferences rather than the raw play_events identifier (which may differ — e.g. numeric
  // Plex account ID in webhooks vs OAuth username stored at login).
  // We only match on username/email to avoid cross-user collisions with display names or numeric IDs.
  const prefRows = db.prepare('SELECT user_plex_id FROM user_preferences').all();
  const prefCanonicalById = new Map(prefRows.map((r) => {
    const id = String(r.user_plex_id || '').trim();
    return [id.toLowerCase(), id];
  }));
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
    // Resolve canonical ID via OAuth identifiers (username/email) only — avoids collisions with
    // display names or numeric IDs. Used for playlists/preferences lookups; the raw play_events
    // id is kept as the option id for history lookups and dropdown validation.
    const authIds = normalizeIdentityList([entry?.username, entry?.email]).map((v) => v.toLowerCase());
    const canonicalId = authIds.reduce((found, lid) => found || prefCanonicalById.get(lid) || null, null) || row.id;
    acc.push({
      id: row.id,
      canonicalId,
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
  // Cache id→canonicalId map so /admin/preview-user can resolve canonical without a Plex API call.
  if (req.session) {
    req.session.previewUserMap = Object.fromEntries(options.map((o) => [o.id, o.canonicalId]));
    req.session.previewCanonicalId = String(selectedOption?.canonicalId || selectedOption?.id || '').trim() || null;
  }

  return {
    enabled: true,
    selectedUserId: String(selectedOption?.id || '').trim(),
    selectedCanonicalId: String(selectedOption?.canonicalId || selectedOption?.id || '').trim(),
    selectedName: String(selectedOption?.name || '').trim(),
    selectedAvatarUrl: String(selectedOption?.avatarUrl || '').trim(),
    selectedAvatarLabel: String(selectedOption?.avatarLabel || 'P').trim() || 'P',
    options,
    returnTo: req.originalUrl,
  };
}

export async function buildBlendableUsers(
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
    const normalizeBlendIdentity = (values = []) => {
      if (typeof normalizeIdentityList === 'function') return normalizeIdentityList(values);
      return values
        .map((value) => String(value || '').trim().toLowerCase())
        .filter(Boolean);
    };
    const isHiddenBlendIdentity = (...values) => normalizeBlendIdentity(values).includes('local');
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
          return (
            id &&
            !localIdentitySet.has(id.toLowerCase()) &&
            !isHiddenBlendIdentity(id, opt?.name) &&
            statsIds.has(id.toLowerCase())
          );
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
      .filter((id) => id && !localIdentitySet.has(id.toLowerCase()) && !isHiddenBlendIdentity(id));

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
      if (
        isHiddenBlendIdentity(
          id,
          canonicalEntry?.title,
          canonicalEntry?.username,
          canonicalEntry?.email,
        )
      ) return acc;
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
    playlists.find((playlist) => String(playlist?.playlistType || '').trim() === 'curatorr' || String(playlist?.playlistKey || '').trim() === 'curatorr'),
    playlists.find((playlist) => String(playlist?.playlistType || '').trim() === 'curative' || String(playlist?.playlistKey || '').trim() === 'curative'),
    playlists.find((playlist) => String(playlist?.playlistType || '').trim() === 'crescive' || String(playlist?.playlistKey || '').trim() === 'crescive'),
    playlists.find((playlist) => String(playlist?.playlistType || '').trim() === 'daily-mix' || String(playlist?.playlistKey || '').trim() === 'daily-mix'),
    playlists[0] || null,
  ];
  return ordered.find(Boolean) || null;
}

async function attachPlaylistArtwork(cards, config, fetchPlexPlaylistsForToken, userToken = null) {
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
      if (!playlist.artUrl) {
        playlist.artUrl = `/api/ms/art?id=${encodeURIComponent(String(playlist.plexPlaylistId || ''))}${tag ? `&tag=${encodeURIComponent(tag)}` : ''}`;
      }
    });
    return playlistCards;
  }

  const { url: plexUrl, token: plexToken } = config.plex || {};
  const effectivePlexToken = userToken || plexToken;
  if (plexUrl && effectivePlexToken && fetchPlexPlaylistsForToken) {
    try {
      const plexPlaylists = await fetchPlexPlaylistsForToken(plexUrl, effectivePlexToken);
      const plexPlaylistMap = new Map(
        plexPlaylists.map((playlist) => [String(playlist.ratingKey || ''), playlist]),
      );
      playlistCards.forEach((playlist) => {
        const plexPlaylist = plexPlaylistMap.get(String(playlist?.plexPlaylistId || '')) || null;
        playlist.artPath = String(plexPlaylist?.composite || plexPlaylist?.thumb || plexPlaylist?.art || '');
        if (playlist.artPath && !playlist.artUrl) {
          playlist.artUrl = `/api/plex/art?path=${encodeURIComponent(playlist.artPath)}`;
        }
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
    spotifyService,
    youtubeService,
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
    const previewCanonicalId = String(adminPreview?.selectedCanonicalId || previewUserId).trim();
    const user = req.session?.user || {};
    const role = getEffectiveRole(req);
    // userPlexId: play_events identity — used for history/stats queries
    // personalUserId: OAuth canonical identity — used for playlists/preferences
    const scopedUserId = previewUserId || String(user.username || '').trim();
    const scopedCanonicalId = previewCanonicalId || scopedUserId;
    return {
      adminPreview,
      role,
      userPlexId: scopedUserId,
      suggestionUserId: scopedCanonicalId,
      personalUserId: scopedCanonicalId,
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
    const { adminPreview, role, userPlexId, suggestionUserId, personalUserId } = await buildPageScope(req, config);

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
    const topTracksBase = getTopTracks(db, userPlexId, 5).map((track) => ({
      ...track,
      track_title: stripArtistSuffix(track.track_title, track.artist_name),
      curatorrTier: deriveTrackTier(track),
    }));
    const topTrackPopularity = getAlbumPopularTrackRanks(db, topTracksBase.map((track) => track.rating_key));
    const topTracks = attachAlbumPopularity(topTracksBase, topTrackPopularity);
    const { history: recentHistory } = paginateRolledHistory(
      (chunkLimit, chunkOffset) => getRecentHistory(db, userPlexId, chunkLimit, chunkOffset).map((event) => ({
        ...event,
        track_title: stripArtistSuffix(event.track_title, event.artist_name),
      })),
      { limit: 10, offset: 0 },
    );
    const recentHistoryPopularity = getAlbumPopularTrackRanks(db, recentHistory.map((event) => event.plex_rating_key));
    const decoratedRecentHistory = attachAlbumPopularity(recentHistory, recentHistoryPopularity, 'plex_rating_key').map((event) => ({
      ...event,
      curatorrTier: deriveHistoryTier(event, config),
    }));
    const generatedPlaylists = playlistService?.listGenerated(personalUserId, { activeOnly: true }) || [];
    const _dashLastSync = getLastPlaylistSync(db, suggestionUserId);
    let dashboardPlaylists = generatedPlaylists.map((playlist) => ({
      ...playlist,
      artPath: '',
      curatorrUpdatedAt: Number(playlist.lastBuiltAt || playlist.lastSyncedAt || playlist.updatedAt || playlist.createdAt || 0),
      tracksAdded: Number(_dashLastSync?.tracks_added || 0),
      tracksRemoved: Number(_dashLastSync?.tracks_removed || 0),
    }));
    const { url: plexUrl, token: plexToken } = config.plex || {};
    const dashboardUserToken = ctx.resolveUserPlexServerToken(config, personalUserId) || plexToken;
    if (dashboardPlaylists.length && plexUrl && dashboardUserToken && fetchPlexPlaylistsForToken) {
      try {
        const plexPlaylists = await fetchPlexPlaylistsForToken(plexUrl, dashboardUserToken);
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
    const dashboardSuggestions = await loadSuggestionBundle(recommendationService, suggestionUserId, { artistLimit: 8 });
    const lidarrStatus = await buildLidarrStatusBundle(db, lidarrService, suggestionUserId, dashboardSuggestions.artists);
    const lidarrAutomationEligible = canUserAccessLidarrAutomation(loadConfig(), { ...req.session.user, role });
    const lidarrQuota = lidarrAutomationEligible && lidarrService
      ? lidarrService.getRoleQuota(role, getCurrentLidarrUsage(db, suggestionUserId).usage || {})
      : null;

    const masterTrackCount = getMasterTrackCount(db);
    const masterArtistCount = getMasterArtistCount(db);
    const lastPlaylistSync = getLastPlaylistSync(db, suggestionUserId);
    const excludedTrackCount = getExcludedTrackKeys(db, userPlexId).length;
    const skipTierArtistCount = getSkipTierArtists(db, userPlexId).length;
    const belterTrackCount = db.prepare("SELECT COUNT(*) AS n FROM track_stats WHERE user_plex_id = ? AND tier = 'belter'").get(userPlexId)?.n || 0;
    const heardTrackCount = db.prepare("SELECT COUNT(*) AS n FROM track_stats WHERE user_plex_id = ? AND tier != 'curatorr'").get(userPlexId)?.n || 0;

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
      recentHistory: decoratedRecentHistory,
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

  // ── Overview (Last.fm-style profile) ──────────────────────────────────────

  app.get('/overview', requireUser, requireWizardComplete, requireUserWizardComplete, async (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const { adminPreview, role, userPlexId } = await buildPageScope(req, config);

    const now = Date.now();
    const since7d  = now - 7  * 24 * 60 * 60 * 1000;
    const since30d = now - 30 * 24 * 60 * 60 * 1000;

    const normalizeStats = (r) => ({
      plays:         r?.total_plays    || 0,
      skips:         r?.total_skips    || 0,
      uniqueArtists: r?.unique_artists || 0,
      uniqueTracks:  r?.unique_tracks  || 0,
      totalListenMs: r?.total_listen_ms || 0,
    });

    const statsAll  = normalizeStats(getPlayStats(db, userPlexId, 0));
    const stats7d   = normalizeStats(getPlayStats(db, userPlexId, since7d));
    const stats30d  = normalizeStats(getPlayStats(db, userPlexId, since30d));

    const firstPlayRow = db.prepare(
      'SELECT MIN(started_at) AS first_play FROM play_events WHERE user_plex_id = ?',
    ).get(userPlexId);
    const firstPlayAt = firstPlayRow?.first_play || null;

    const prefs = getUserPreferences(db, userPlexId);
    const lastfmUsername = prefs?.lastfmUsername || null;

    function queryTopArtists(since, limit = 10) {
      const sinceClause = since > 0 ? 'AND started_at >= ?' : '';
      const params = since > 0 ? [userPlexId, since, limit] : [userPlexId, limit];
      return db.prepare(`
        SELECT artist_name, COUNT(*) AS play_count
        FROM play_events
        WHERE user_plex_id = ?
          AND is_skip = 0
          AND LOWER(TRIM(artist_name)) != 'various artists'
          AND TRIM(artist_name) != ''
          ${sinceClause}
        GROUP BY LOWER(TRIM(artist_name))
        ORDER BY play_count DESC
        LIMIT ?
      `).all(...params);
    }

    function queryTopAlbums(since, limit = 10) {
      const sinceClause = since > 0 ? 'AND started_at >= ?' : '';
      const params = since > 0 ? [userPlexId, since, limit] : [userPlexId, limit];
      return db.prepare(`
        SELECT album_name, artist_name, COUNT(*) AS play_count,
               MAX(plex_rating_key) AS sample_rating_key
        FROM play_events
        WHERE user_plex_id = ?
          AND is_skip = 0
          AND LOWER(TRIM(artist_name)) != 'various artists'
          AND TRIM(artist_name) != ''
          AND TRIM(album_name) != ''
          ${sinceClause}
        GROUP BY LOWER(TRIM(album_name)), LOWER(TRIM(artist_name))
        ORDER BY play_count DESC
        LIMIT ?
      `).all(...params);
    }

    function queryTopTracks(since, limit = 10) {
      const sinceClause = since > 0 ? 'AND started_at >= ?' : '';
      const params = since > 0 ? [userPlexId, since, limit] : [userPlexId, limit];
      return db.prepare(`
        SELECT
          MAX(CASE WHEN TRIM(plex_rating_key) != '' THEN plex_rating_key ELSE '' END) AS plex_rating_key,
          MAX(track_title) AS track_title,
          MAX(artist_name) AS artist_name,
          MAX(album_name) AS album_name,
          COUNT(*) AS play_count,
          MAX(started_at) AS last_played_at,
          MAX(CASE WHEN TRIM(plex_rating_key) != '' THEN 1 ELSE 0 END) AS has_rating_key
        FROM play_events
        WHERE user_plex_id = ?
          AND is_skip = 0
          AND LOWER(TRIM(artist_name)) != 'various artists'
          AND TRIM(artist_name) != ''
          ${sinceClause}
        GROUP BY
          LOWER(TRIM(REPLACE(REPLACE(track_title, '’', ''''), '‘', ''''))),
          LOWER(TRIM(REPLACE(REPLACE(artist_name, '’', ''''), '‘', '''')))
        ORDER BY play_count DESC, has_rating_key DESC, last_played_at DESC
        LIMIT ?
      `).all(...params);
    }

    const topArtists7d   = queryTopArtists(since7d);
    const topArtists30d  = queryTopArtists(since30d);
    const topArtistsAll  = queryTopArtists(0);

    const topAlbums7d    = queryTopAlbums(since7d);
    const topAlbums30d   = queryTopAlbums(since30d);
    const topAlbumsAll   = queryTopAlbums(0);

    const topTracks7dBase  = queryTopTracks(since7d);
    const topTracks30dBase = queryTopTracks(since30d);
    const topTracksAllBase = queryTopTracks(0);

    const allTopTrackKeys = [
      ...topTracks7dBase, ...topTracks30dBase, ...topTracksAllBase,
    ].map((t) => t.plex_rating_key).filter(Boolean);
    const topTrackPopularity = getAlbumPopularTrackRanks(db, allTopTrackKeys);

    const topTracks7d  = attachAlbumPopularity(topTracks7dBase,  topTrackPopularity, 'plex_rating_key');
    const topTracks30d = attachAlbumPopularity(topTracks30dBase, topTrackPopularity, 'plex_rating_key');
    const topTracksAll = attachAlbumPopularity(topTracksAllBase, topTrackPopularity, 'plex_rating_key');

    const { history: recentHistoryBase } = paginateRolledHistory(
      (chunkLimit, chunkOffset) => getRecentHistory(db, userPlexId, chunkLimit, chunkOffset).map((event) => ({
        ...event,
        track_title: stripArtistSuffix(event.track_title, event.artist_name),
      })),
      { limit: 10, offset: 0 },
    );
    const recentPopularity = getAlbumPopularTrackRanks(db, recentHistoryBase.map((e) => e.plex_rating_key).filter(Boolean));
    const recentHistory = attachAlbumPopularity(recentHistoryBase, recentPopularity, 'plex_rating_key');

    res.render('overview', {
      title: 'Overview — Curatorr',
      user,
      role,
      actualRole: getActualRole(req),
      adminPreview,
      config: safeConfig(config),
      firstPlayAt,
      lastfmUsername,
      statsAll,
      stats7d,
      stats30d,
      topArtists7d,
      topArtists30d,
      topArtistsAll,
      topAlbums7d,
      topAlbums30d,
      topAlbumsAll,
      topTracks7d,
      topTracks30d,
      topTracksAll,
      recentHistory,
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
      })),
      { limit, offset },
    );
    const historyPopularity = getAlbumPopularTrackRanks(db, history.map((event) => event.plex_rating_key));
    const decoratedHistory = attachAlbumPopularity(history, historyPopularity, 'plex_rating_key').map((event) => ({
      ...event,
      curatorrTier: deriveHistoryTier(event, config),
    }));

    res.render('history', {
      title: 'Play History — Curatorr',
      user,
      role,
      actualRole: getActualRole(req),
      adminPreview,
      config: safeConfig(config),
      history: decoratedHistory,
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
    let suggestions = await loadSuggestionBundle(recommendationService, suggestionUserId, { artistLimit: 16 });
    if (recommendationService && suggestionUserId) {
      try {
        const rebuilt = await recommendationService.rebuildSuggestionsForUser(suggestionUserId, { artistLimit: 16 });
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
    const discoverSuggestions = await loadSuggestionBundle(recommendationService, suggestionUserId, { artistLimit: 24 });
    const lidarrStatus = buildLidarrStatusBundle(db, lidarrService, suggestionUserId, discoverSuggestions.artists);
    const recentAlbumCarousels = buildDiscoverRecentAlbumCarousels(db, suggestionUserId, { addedLimit: 16, requestedLimit: 0 });
    const disc = config.discovery || {};
    const discoveryConfig = {
      enabled: Boolean(disc.lastfmApiKey),
      showTrendingArtists: disc.lastfmApiKey ? (disc.showTrendingArtists ?? true) : false,
      showTrendingTracks:  disc.lastfmApiKey ? (disc.showTrendingTracks  ?? true) : false,
      showSimilarArtists:  disc.lastfmApiKey ? (disc.showSimilarArtists  ?? true) : false,
    };
    const discoveryPipelineEligible = disc.lastfmApiKey && canUserSeeDiscoveryPipeline(config, role);
    const suggestedArtists = discoveryPipelineEligible
      ? lidarrStatus.actionableSuggestions
      : lidarrStatus.actionableSuggestions.filter((a) => a.source !== 'lastfm-similar');

    res.render('discover', {
      title: 'Discover — Curatorr',
      user,
      role,
      actualRole: getActualRole(req),
      adminPreview,
      config: safeConfig(config),
      lidarrAutomationEligible,
      lidarrQuota,
      suggestedArtists,
      lidarrStatus,
      recentAddedAlbums: recentAlbumCarousels.recentAdded,
      recentRequestedAlbums: recentAlbumCarousels.recentRequested,
      discoveryConfig,
      extraCss: ['/styles-layout.css', '/styles-curatorr.css'],
    });
  });

  // ── Tracks ────────────────────────────────────────────────────────────────

  app.get('/tracks', requireUser, requireWizardComplete, requireUserWizardComplete, async (req, res) => {
    const config = loadConfig();
    const user = req.session.user;
    const { adminPreview, role, userPlexId, suggestionUserId } = await buildPageScope(req, config);
    const trackRows = getTopTracks(db, userPlexId, 500).map((track) => ({
      ...track,
      track_title: stripArtistSuffix(track.track_title, track.artist_name),
      curatorrTier: deriveTrackTier(track),
    }));
    const trackPopularity = getAlbumPopularTrackRanks(db, trackRows.map((track) => track.rating_key));
    const tracks = attachAlbumPopularity(trackRows, trackPopularity);
    const smartSettings = config.smartPlaylist || {};
    const completionThresholdMs = (Number(smartSettings.completionThresholdSeconds) || 20) * 1000;
    const completedKeys = getCompletedTrackKeys(db, userPlexId, completionThresholdMs);
    const suggestions = await loadSuggestionBundle(recommendationService, suggestionUserId, {
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
    const userPrefs = userPlexId ? getUserPreferences(db, userPlexId) : null;
    const lidarrAutomationEligible = canUserAccessLidarrAutomation(loadConfig(), { ...req.session.user, role });
    const lidarrConfigured = Boolean(lidarrService?.isConfigured?.());
    const lastSync = getLastPlaylistSync(db, userPlexId);
    const userPersonalPlaylists = (() => { try { return listUserPersonalPlaylists(db, userPlexId); } catch { return []; } })();
    const personalPlaylistMap = new Map(userPersonalPlaylists.map((playlist) => [String(playlist?.id || '').trim(), playlist]));
    const generatedPlaylists = playlistService?.listGenerated(userPlexId, { activeOnly: false }) || [];
    const generatedPlaylistKeys = new Set(generatedPlaylists.map((playlist) => String(playlist?.playlistKey || '').trim()).filter(Boolean));
    const canonicalPlaylists = playlistService?.getCanonicalPlaylist(userPlexId) || { legacy: null, generated: [], curatorred: null };
    const generatedCards = generatedPlaylists
      .map((playlist) => ({
        playlistKind: 'generated',
        playlistKey: String(playlist.playlistKey || ''),
        playlistType: String(playlist.playlistType || ''),
        plexPlaylistId: String(playlist.plexPlaylistId || ''),
        playlistTitle: String(playlist.playlistTitle || playlist.playlistKey || 'Playlist'),
        artworkMode: String(playlist.artworkMode || 'auto'),
        customArtworkAsset: String(playlist.customArtworkAsset || ''),
        preservedArtworkAsset: String(playlist.preservedArtworkAsset || ''),
        sourceType: String(playlist.sourceType || ''),
        sourceTitle: String(playlist.sourceTitle || ''),
        sourceOwner: String(playlist.sourceOwner || ''),
        importedSyncPeriod: String(playlist.importedSyncPeriod || 'disabled'),
        trackCount: Number(playlist.trackCount || 0),
        missingCount: Number(playlist.missingCount || 0),
        curatorrUpdatedAt: Number(playlist.lastBuiltAt || playlist.lastSyncedAt || playlist.updatedAt || playlist.createdAt || 0),
        state: playlist.active === false ? 'disabled' : (playlist.plexPlaylistId ? 'synced' : 'pending'),
        active: playlist.active !== false,
        description: String(playlist.playlistType || 'generated'),
        playlistAudience: resolvePlaylistAudience(playlist.playlistType, playlist.playlistKey, personalPlaylistMap, playlist.sourceType, playlist.audience),
        artPath: '',
        artUrl: buildStoredPlaylistArtworkUrl(playlist.artworkMode === 'custom'
          ? playlist.customArtworkAsset
          : (playlist.artworkMode === 'preserve' ? playlist.preservedArtworkAsset : '')),
        tracksAdded: Number(lastSync?.tracks_added || 0),
        tracksRemoved: Number(lastSync?.tracks_removed || 0),
      }))
      .sort(comparePlaylistCards);
    const draftCards = userPersonalPlaylists
      .filter((playlist) => {
        const playlistId = String(playlist?.id || '').trim();
        return playlistId && !generatedPlaylistKeys.has(`personal:${playlistId}`);
      })
      .map((playlist) => ({
        playlistKind: 'draft',
        playlistKey: `personal:${String(playlist.id || '').trim()}`,
        playlistType: 'personal',
        plexPlaylistId: '',
        playlistTitle: String(playlist.name || 'Playlist'),
        sourceType: '',
        sourceTitle: '',
        sourceOwner: '',
        trackCount: 0,
        missingCount: 0,
        curatorrUpdatedAt: Number(playlist.updatedAt || playlist.createdAt || 0),
        state: 'draft',
        active: true,
        description: 'personal draft',
        playlistAudience: resolvePlaylistAudience('personal', `personal:${String(playlist.id || '').trim()}`, personalPlaylistMap),
        artPath: '',
        tracksAdded: 0,
        tracksRemoved: 0,
      }))
      .sort(comparePlaylistCards);
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
        active: true,
        description: 'Current Curatorr playlist',
        playlistAudience: 'system',
        artPath: '',
        tracksAdded: Number(lastSync?.tracks_added || 0),
        tracksRemoved: Number(lastSync?.tracks_removed || 0),
      });
    }
    playlistCards.push(...generatedCards);
    playlistCards.push(...draftCards);
    playlistCards.sort(comparePlaylistCards);
    const userPlexToken = ctx.resolveUserPlexServerToken(config, userPlexId);
    await attachPlaylistArtwork(playlistCards, config, fetchPlexPlaylistsForToken, userPlexToken);

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
      allTrackDecades: (() => { try { return getAllTrackDecadeTags(db); } catch { return []; } })(),
      allUserIds:    (() => { try { return getAllUserIds(db);        } catch { return []; } })(),
      allPathSegments: (() => { try { return getDistinctPathSegments(db); } catch { return []; } })(),
      playlistFeatureCoverage: (() => { try { return buildFeaturePresetAvailability(getMasterTracks(db)); } catch { return { totalTracks: 0, presets: {} }; } })(),
      currentUserId: userPlexId,
      lastfmUsername: String(userPrefs?.lastfmUsername || ''),
      listenbrainzUsername: String(userPrefs?.listenbrainzUsername || ''),
      spotifyUserId: String(userPrefs?.spotifyUserId || ''),
      spotifyConnected: Boolean(userPrefs?.spotifyRefreshToken || userPrefs?.spotifyAccessToken),
      spotifyConfigured: Boolean(spotifyService?.isConfigured?.()),
      youtubeConfigured: Boolean(youtubeService?.isConfigured?.()),
      spotifyDisplayName: String(userPrefs?.spotifyDisplayName || ''),
      lidarrAutomationEligible,
      lidarrConfigured,
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
      userPersonalPlaylists,
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
    const lastfmStationSorts = userPrefs?.lastfmStationSorts || {};
    const lastfmStationFinalOrderings = userPrefs?.lastfmStationFinalOrderings || {};
    const lastfmBackfillCursor = userPrefs?.lastfmBackfillCursor ?? 0;
    const listenbrainzUsername = userPrefs?.listenbrainzUsername || '';
    const listenbrainzToken = userPrefs?.listenbrainzToken || '';
    const listenbrainzEnabledPlaylists = userPrefs?.listenbrainzEnabledPlaylists || [];
    const listenbrainzPlaylistSorts = userPrefs?.listenbrainzPlaylistSorts || {};
    const listenbrainzPlaylistFinalOrderings = userPrefs?.listenbrainzPlaylistFinalOrderings || {};
    const spotifyUserId = userPrefs?.spotifyUserId || '';
    const spotifyDisplayName = userPrefs?.spotifyDisplayName || '';
    const spotifyConnected = Boolean(userPrefs?.spotifyRefreshToken || userPrefs?.spotifyAccessToken);
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
      lastfmStationSorts,
      lastfmStationFinalOrderings,
      lastfmBackfillCursor,
      listenbrainzUsername,
      listenbrainzToken,
      listenbrainzEnabledPlaylists,
      listenbrainzPlaylistSorts,
      listenbrainzPlaylistFinalOrderings,
      spotifyUserId,
      spotifyDisplayName,
      spotifyConnected,
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

export function deriveHistoryTier(event, config = {}) {
  if (!event || typeof event !== 'object') return buildTierBadge('decent');
  const listenedMs = Number(event.duration_ms || 0);
  const trackDurationMs = Number(event.track_duration_ms || 0);
  const smartSettings = config?.smartPlaylist || {};
  const skipThresholdMs = (Number(smartSettings.skipThresholdSeconds) || 30) * 1000;
  if (trackDurationMs > 0) {
    return buildTierBadge(classifyTier(listenedMs, trackDurationMs, smartSettings));
  }
  if (listenedMs > 0 && listenedMs < skipThresholdMs) {
    return buildTierBadge('skip');
  }
  return deriveTrackTier({
    excluded: Boolean(event.current_excluded),
    force_included: Boolean(event.current_force_included),
    tier: event.current_tier,
  });
}

async function loadSuggestionBundle(recommendationService, userPlexId, options = {}) {
  if (!recommendationService || !userPlexId) return { artists: [], albums: [], tracks: [] };
  let cached = { artists: [], albums: [], tracks: [] };
  try {
    cached = recommendationService.listCachedSuggestions(userPlexId, options) || cached;
  } catch (_err) {
    cached = { artists: [], albums: [], tracks: [] };
  }
  // Rebuild if cache is empty or any recommendation artist is from a stale/old model.
  const artistCacheNeedsRefresh = !Array.isArray(cached.artists)
    || cached.artists.length === 0
    || cached.artists.some((artist) => {
      const src = String(artist?.source || '');
      if (src !== 'library-affinity' && src !== 'lastfm-similar') return false;
      return String(artist?.reason?.modelVersion || '').trim() !== ARTIST_RECOMMENDATION_MODEL_VERSION;
    });
  const count = (cached.artists?.length || 0) + (cached.albums?.length || 0) + (cached.tracks?.length || 0);
  if (count > 0 && !artistCacheNeedsRefresh) return cached;
  try {
    const rebuilt = await recommendationService.rebuildSuggestionsForUser(userPlexId, options);
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
  if (normalizedStatusKey === 'in_library') return false;
  return true;
}

function deriveLidarrStateLabel(suggestion, progress) {
  const suggestionStatus = String(suggestion?.status || '').trim().toLowerCase();
  const currentStage = String(progress?.currentStage || '').trim().toLowerCase();
  const lastManualSearchStatus = String(progress?.lastManualSearchStatus || '').trim().toLowerCase();
  const reason = suggestion?.reason && typeof suggestion.reason === 'object' ? suggestion.reason : {};
  const albumWarning = reason?.albumWarning && typeof reason.albumWarning === 'object' ? reason.albumWarning : null;
  const starterAlbum = reason?.starterAlbum && typeof reason.starterAlbum === 'object' ? reason.starterAlbum : null;
  const latestAlbum = reason?.latestAlbum && typeof reason.latestAlbum === 'object' ? reason.latestAlbum : null;
  const albumLabel = latestAlbum?.albumTitle || starterAlbum?.albumTitle || '';

  // ── Four user-facing states ───────────────────────────────────────────────
  // in_library: stage shows acquisition complete
  const IN_LIBRARY_STAGES = new Set(['album_acquired', 'catalog_complete']);
  if (IN_LIBRARY_STAGES.has(currentStage)) {
    return { key: 'in_library', label: 'In your library', tone: 'ok', detail: albumLabel };
  }

  // stuck: quota blocked, or search exhausted with no result
  const STUCK_STAGES = new Set(['manual_search_no_results', 'no_files_found', 'manual_search_failed']);
  if (suggestionStatus === 'quota_blocked' || albumWarning?.type === 'album_quota') {
    return { key: 'stuck', label: 'Stuck', tone: 'warn', detail: albumWarning?.message || 'Quota reached — will retry when quota resets.' };
  }
  if (STUCK_STAGES.has(currentStage) || lastManualSearchStatus === 'failed') {
    return { key: 'stuck', label: 'Stuck', tone: 'warn', detail: `${albumLabel ? albumLabel + ' · ' : ''}No files found. Check Lidarr.` };
  }

  // in_progress: handed off to Lidarr and being processed
  const IN_PROGRESS_STAGES = new Set([
    'queued', 'added', 'starter_album_added', 'starter_album_linked',
    'awaiting_belter', 'catalog_expanded', 'search_retry_queued',
    'monitor_repaired_search_queued', 'manual_grab_queued', 'monitor_repaired',
  ]);
  if (
    suggestionStatus === 'queued_for_lidarr'
    || suggestionStatus === 'added_to_lidarr'
    || IN_PROGRESS_STAGES.has(currentStage)
  ) {
    return { key: 'in_progress', label: 'In progress', tone: 'half-decent', detail: albumLabel };
  }

  // suggested: recommended, no action taken yet
  return { key: 'suggested', label: 'Suggested', tone: 'curatorr', detail: '' };
}

function buildLidarrStatusBundle(db, lidarrService, userPlexId, suggestedArtists = []) {
  const suggestions = Array.isArray(suggestedArtists) ? suggestedArtists : [];
  const dismissedArtistKeys = new Set(
    suggestions
      .filter((artist) => String(artist?.status || '').trim().toLowerCase() === 'dismissed')
      .map((artist) => normalizeName(artist?.artistName || ''))
      .filter(Boolean),
  );
  const progressItems = listLidarrArtistProgress(db, userPlexId, { limit: 12 });
  const progressMap = new Map(progressItems.map((item) => [normalizeName(item.artistName), item]));

  // Source-kind labels only — local DB read, no Lidarr API calls on page load.
  // The background service keeps lidarr_artist_progress up to date; live API
  // calls were removed because they blocked every page render with up to 3
  // sequential/parallel Lidarr round-trips (listArtists + getCommand + getAlbum).
  const requestHistory = listLidarrRequests(db, userPlexId, { statuses: ['queued', 'processing', 'completed', 'failed'], limit: 250 });
  const requestMap = new Map();
  requestHistory.forEach((request) => {
    const key = normalizeName(request.artistName);
    if (!key || requestMap.has(key)) return;
    requestMap.set(key, request);
  });

  const enrichedSuggestions = suggestions
    .filter((artist) => !dismissedArtistKeys.has(normalizeName(artist?.artistName || '')))
    .map((artist) => {
    const key = normalizeName(artist.artistName);
    const progress = progressMap.get(key) || null;
    const reason = artist?.reason && typeof artist.reason === 'object' ? artist.reason : {};
    // isInLidarr from local DB only — lidarrArtistId is set by the background
    // service once the artist is confirmed in Lidarr.
    const isInLidarr = Boolean(artist?.lidarrArtistId)
      || Boolean(progress?.lidarrArtistId)
      || Boolean(reason?.lidarrExisting);
    let derived = deriveLidarrStateLabel(artist, progress);
    if (isInLidarr && derived.key === 'suggested') {
      derived = { key: 'in_progress', label: 'In progress', tone: 'half-decent', detail: '' };
    }
    const sourceLabel = resolveLidarrSourceLabel(artist, derived.key) || resolveLidarrSourceFromRequest(requestMap.get(key));
    return {
      ...artist,
      isInLidarr,
      lidarrProgress: progress,
      reviewable: isProgressReviewable(progress, derived.key),
      lidarrStatusKey: derived.key,
      lidarrStatusLabel: derived.label,
      lidarrStatusTone: derived.tone,
      lidarrStatusDetail: appendLidarrSourceDetail(derived.detail, sourceLabel),
      lidarrStatusSourceLabel: sourceLabel,
    };
  });

  const IN_LIBRARY_GRACE_MS = 14 * 24 * 60 * 60 * 1000; // 14 days
  const now = Date.now();
  const actionableSuggestions = enrichedSuggestions.filter((artist) => {
    if (artist.lidarrStatusKey !== 'in_library') return true;
    // Keep in pipeline during grace period so the user can see acquisition completed.
    // After 14 days, drop it — no longer needs attention.
    const settledAt = Number(artist.lidarrProgress?.updatedAt || artist.lidarrProgress?.lastAlbumAddedAt || 0);
    return settledAt > 0 && (now - settledAt) < IN_LIBRARY_GRACE_MS;
  }).sort((a, b) => {
    const aScore = Number(a?.totalScore || 0);
    const bScore = Number(b?.totalScore || 0);
    return bScore - aScore
      || String(a?.artistName || '').localeCompare(String(b?.artistName || ''));
  });
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
    if (!key || dismissedArtistKeys.has(key) || activityMap.has(key)) return;
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

function formatDiscoverAlbumStamp(timestamp) {
  const value = Number(timestamp || 0);
  if (!value) return '';
  try {
    return new Date(value).toISOString().slice(0, 10);
  } catch (_err) {
    return '';
  }
}

function normalizeDiscoverAlbumKey(artistName = '', albumTitle = '') {
  return `${normalizeName(artistName)}::${normalizeName(albumTitle)}`;
}

function resolveDiscoverAlbumImageUrl(album = {}) {
  const directUrl = String(
    album?.selectedAlbumImageUrl
    || album?.preferredAlbumImageUrl
    || album?.albumImageUrl
    || album?.imageUrl
    || ''
  ).trim();
  if (directUrl) return directUrl;
  const imagePath = String(album?.imagePath || '').trim();
  if (imagePath) return `/api/music/lidarr/image?path=${encodeURIComponent(imagePath)}`;
  const foreignAlbumId = String(
    album?.foreignAlbumId
    || album?.selectedForeignAlbumId
    || album?.releaseGroupMbid
    || ''
  ).trim();
  if (foreignAlbumId) return `/api/music/cover/release-group/${encodeURIComponent(foreignAlbumId)}`;
  return '';
}

function buildDiscoverRecentAlbumCarousels(db, userPlexId, { addedLimit = 16, requestedLimit = 0 } = {}) {
  const suggestedAlbumState = new Map(
    listSuggestedAlbums(db, userPlexId, { limit: 1000 }).map((album) => ([
      normalizeDiscoverAlbumKey(album.artistName, album.albumTitle),
      album,
    ])),
  );

  const requests = listLidarrRequests(db, userPlexId, {
    statuses: ['queued', 'processing', 'completed', 'failed'],
    limit: 250,
  })
    .slice()
    .sort((a, b) => Number(b.updatedAt || b.createdAt || 0) - Number(a.updatedAt || a.createdAt || 0));

  const items = requests.map((request) => {
    const detail = request?.detail && typeof request.detail === 'object' ? request.detail : {};
    const artistName = String(request?.artistName || '').trim();
    const albumTitle = String(
      request?.albumTitle
      || detail.selectedAlbumTitle
      || detail.starterAlbumTitle
      || detail.latestAlbumTitle
      || detail.preferredAlbumTitle
      || ''
    ).trim();
    if (!artistName || !albumTitle) return null;

    const suggestion = suggestedAlbumState.get(normalizeDiscoverAlbumKey(artistName, albumTitle)) || null;
    const albumMatch = resolveLibraryAlbumMatch(db, {
      artistName,
      albumTitle,
      alternateTitles: [
        detail.selectedAlbumTitle,
        detail.starterAlbumTitle,
        detail.latestAlbumTitle,
        detail.preferredAlbumTitle,
      ],
    });
    const inLibrary = detail.manualAvailabilityOverride === true || albumMatch.inLibrary === true;
    const requestStatus = String(request?.status || '').trim().toLowerCase();
    const monitoringConfirmed = detail.monitoredConfirmed === true || detail.alreadyMonitored === true;
    const statusKey = inLibrary
      ? 'available'
      : ((requestStatus === 'queued' || requestStatus === 'processing' || (requestStatus === 'completed' && (monitoringConfirmed || Number(request?.lidarrAlbumId || 0) > 0)))
        ? 'pending'
        : 'missing');
    const excluded = String(suggestion?.status || '').trim().toLowerCase() === 'dismissed';
    const sourceLabel = formatLidarrSourceLabel(detail.albumSource || detail.requestSource || request?.sourceKind || '');
    const stamp = formatDiscoverAlbumStamp(request?.updatedAt || request?.processedAt || request?.createdAt || 0);
    const metaParts = [artistName];
    metaParts.push((requestStatus === 'completed' && statusKey === 'available') ? `Added ${stamp || 'recently'}` : `Requested ${stamp || 'recently'}`);
    if (sourceLabel) metaParts.push(sourceLabel);

    return {
      key: `${request.id}:${normalizeDiscoverAlbumKey(artistName, albumTitle)}`,
      artistName,
      foreignArtistId: String(request?.foreignArtistId || '').trim(),
      albumTitle,
      albumId: Number(request?.lidarrAlbumId || detail.albumId || detail.lidarrAlbumId || 0) || 0,
      foreignAlbumId: String(detail.foreignAlbumId || detail.selectedForeignAlbumId || '').trim(),
      albumType: String(detail.albumType || suggestion?.albumType || '').trim(),
      releaseDate: String(detail.releaseDate || suggestion?.releaseDate || '').trim(),
      source: String(detail.source || detail.albumSource || '').trim(),
      image: resolveDiscoverAlbumImageUrl({
        selectedAlbumImageUrl: detail.selectedAlbumImageUrl,
        preferredAlbumImageUrl: detail.preferredAlbumImageUrl,
        albumImageUrl: detail.albumImageUrl,
        imageUrl: detail.imageUrl,
        imagePath: detail.imagePath,
        foreignAlbumId: detail.foreignAlbumId || detail.selectedForeignAlbumId,
      }),
      excluded,
      statusKey,
      requestable: statusKey === 'missing' && !excluded,
      statusLabel: statusKey === 'available' ? 'In library' : (statusKey === 'pending' ? 'Monitored in Lidarr' : 'Not in library'),
      meta: metaParts.join(' · '),
      createdAt: Number(request?.createdAt || 0),
      updatedAt: Number(request?.updatedAt || request?.createdAt || 0),
    };
  }).filter(Boolean);

  const unresolvedRequested = [];
  const seenRequested = new Set();
  for (const item of items) {
    if (!item || item.statusKey === 'available') continue;
    const key = normalizeDiscoverAlbumKey(item.artistName, item.albumTitle);
    if (seenRequested.has(key)) continue;
    seenRequested.add(key);
    unresolvedRequested.push(item);
  }
  const normalizedRequestedLimit = Number(requestedLimit);
  const recentRequested = Number.isFinite(normalizedRequestedLimit) && normalizedRequestedLimit > 0
    ? unresolvedRequested.slice(0, Math.max(1, Math.floor(normalizedRequestedLimit)))
    : unresolvedRequested;

  const seenAdded = new Set();
  const recentAdded = items
    .filter((item) => item.statusKey === 'available')
    .filter((item) => {
      const key = normalizeDiscoverAlbumKey(item.artistName, item.albumTitle);
      if (seenAdded.has(key)) return false;
      seenAdded.add(key);
      return true;
    })
    .slice(0, Math.max(1, Number(addedLimit) || 16));

  return { recentAdded, recentRequested };
}
