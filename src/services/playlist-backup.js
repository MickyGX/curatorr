// Curatorr playlist backups: a portable JSON file that records every kind of Curatorr playlist
// (smart rules, global rules, imported and static track lists, system playlist settings) with
// enough track identity to rebuild them against another library or a new media server.

import fs from 'node:fs';
import crypto from 'node:crypto';
import {
  createUserPersonalPlaylist,
  findUserPersonalPlaylistByName,
  getAllUserIds,
  getMasterTracks,
  getPlaylistTrackSnapshots,
  getUserPreferences,
  listImportedPlaylistUnmatched,
  listUserGeneratedPlaylists,
  listUserPersonalPlaylists,
  saveUserGeneratedPlaylist,
  saveUserPreferences,
  setCustomPlaylistAudience,
  setImportedPlaylistUnmatched,
  setPlaylistTracks,
} from '../db.js';
import { isImportedPlaylistSourceType } from './import-matching.js';
import {
  getStoredPlaylistArtworkInfo,
  parsePlaylistArtworkDataUrl,
  savePlaylistArtworkBuffer,
} from './playlist-artwork.js';
import { buildTrackIdentityLookups, resolveTrackIdentity } from './track-identity.js';

export const PLAYLIST_BACKUP_FORMAT = 'curatorr-playlists';
export const PLAYLIST_BACKUP_VERSION = 1;

const SYSTEM_PLAYLIST_TYPES = new Set(['daily-mix', 'curatorr', 'crescive', 'curative', 'lastfm-station', 'listenbrainz-playlist']);

// Preferences that shape a user's system playlists. Account links and credentials are left out.
const SYSTEM_SETTING_KEYS = [
  'likedGenres',
  'ignoredGenres',
  'likedArtists',
  'ignoredArtists',
  'smartConfig',
  'lastfmUsername',
  'lastfmEnabledStations',
  'lastfmStrictMatchStations',
  'lastfmStationSorts',
  'lastfmStationFinalOrderings',
  'listenbrainzUsername',
  'listenbrainzEnabledPlaylists',
  'listenbrainzStrictMatchPlaylists',
  'listenbrainzPlaylistSorts',
  'listenbrainzPlaylistFinalOrderings',
];

function backupError(message, status = 400) {
  const err = new Error(message);
  err.status = status;
  return err;
}

function makeId(prefix) {
  return prefix + Date.now().toString(36) + crypto.randomBytes(4).toString('hex').slice(0, 5);
}

function text(value) {
  return String(value ?? '').trim();
}

// ─── Export ───────────────────────────────────────────────────────────────────

function readArtworkAsset(assetName) {
  const info = assetName ? getStoredPlaylistArtworkInfo(assetName) : null;
  if (!info) return null;
  try {
    return { mime: info.mime, data: fs.readFileSync(info.filePath).toString('base64') };
  } catch {
    return null;
  }
}

function exportArtwork(state, includeArtwork) {
  const mode = ['auto', 'preserve', 'custom'].includes(text(state?.mode || state?.artworkMode))
    ? text(state?.mode || state?.artworkMode)
    : 'auto';
  if (!includeArtwork) return { mode };
  const custom = readArtworkAsset(text(state?.customArtworkAsset));
  const preserved = readArtworkAsset(text(state?.preservedArtworkAsset));
  return {
    mode,
    ...(custom ? { custom } : {}),
    ...(preserved ? { preserved } : {}),
  };
}

function withoutArtwork(rules) {
  const { artwork: _artwork, ...rest } = rules && typeof rules === 'object' ? rules : {};
  return rest;
}

function toBackupTrack(track, index) {
  return {
    position: Number(track.position || index + 1),
    artist: text(track.artistName),
    title: text(track.title),
    album: text(track.albumName),
    durationMs: Number(track.durationMs || 0),
    path: text(track.filePath),
    mbid: text(track.recordingMbid),
  };
}

async function snapshotTracks(ctx, userId, playlist) {
  const { db, playlistService } = ctx;
  if (!playlist?.playlistKey) return [];
  let tracks = getPlaylistTrackSnapshots(db, userId, playlist.playlistKey);
  // Smart playlists only keep their tracks in the media server.
  if (!tracks.length && playlist.plexPlaylistId && playlistService?.fetchGeneratedPlaylistTrackRefs) {
    const refs = await playlistService.fetchGeneratedPlaylistTrackRefs(userId, playlist).catch(() => []);
    const byKey = new Map(getMasterTracks(db).map((track) => [track.ratingKey, track]));
    tracks = refs.map((ref, index) => {
      const master = byKey.get(ref.ratingKey) || {};
      return {
        position: index + 1,
        artistName: master.artistName || ref.artistName,
        title: master.trackTitle || '',
        albumName: master.albumName || '',
        durationMs: master.durationMs || 0,
        filePath: master.filePath || '',
        recordingMbid: master.recordingMbid || '',
      };
    }).filter((track) => track.title);
  }
  return tracks.map(toBackupTrack);
}

function snapshotMissing(db, userId, playlistKey) {
  return listImportedPlaylistUnmatched(db, userId, playlistKey).map((row, index) => ({
    position: Number(row.position || index + 1),
    artist: text(row.artistName || row.artists[0]),
    artists: row.artists,
    title: text(row.title),
    album: text(row.albumTitle),
    durationMs: Number(row.durationMs || 0),
    path: text(row.filePath),
    mbid: text(row.recordingMbid),
    sourceTrackId: text(row.sourceTrackId),
  }));
}

function exportSystemSettings(db, userId) {
  const prefs = getUserPreferences(db, userId);
  const settings = {};
  for (const key of SYSTEM_SETTING_KEYS) settings[key] = prefs[key];
  return settings;
}

// Playlist key an export selection uses for a card: personal smart playlists are keyed by their
// definition so drafts (which have no generated row yet) can be exported too.
function wantsKey(keys, key) {
  return !keys || keys.has(key);
}

/**
 * @param {object} ctx  route context (db, loadConfig, playlistService, APP_VERSION)
 * @param {object} options
 *   userIds         users whose playlists to include
 *   playlistKeys    optional list restricting the export to these playlist keys
 *   includeArtwork  embed custom and preserved artwork images
 *   includeSettings include each user's system playlist settings
 *   includeGlobal   include global smart playlist definitions (admins)
 */
export async function buildPlaylistBackup(ctx, {
  userIds = [],
  playlistKeys = null,
  includeArtwork = true,
  includeSettings = true,
  includeGlobal = false,
} = {}) {
  const { db, loadConfig } = ctx;
  const keys = Array.isArray(playlistKeys) && playlistKeys.length ? new Set(playlistKeys.map(text)) : null;
  const config = loadConfig();
  const playlists = [];
  const settings = [];
  const exportedGlobalCustomKeys = new Set();

  if (includeGlobal) {
    const firstUser = userIds[0] || '';
    for (const def of Array.isArray(config.globalPlaylists) ? config.globalPlaylists : []) {
      const key = `global:${def.id}`;
      if (!wantsKey(keys, key)) continue;
      const generated = listUserGeneratedPlaylists(db, firstUser, { activeOnly: false })
        .find((entry) => entry.playlistKey === key);
      playlists.push({
        type: 'global',
        key,
        name: text(def.name),
        owner: null,
        audience: 'global',
        enabled: def.enabled !== false,
        smart: { rules: withoutArtwork(def.rules), trackFilters: def.trackFilters ?? null },
        artwork: exportArtwork(def.rules?.artwork, includeArtwork),
        tracks: generated ? await snapshotTracks(ctx, firstUser, generated) : [],
      });
    }
  }

  for (const userId of userIds) {
    const generated = listUserGeneratedPlaylists(db, userId, { activeOnly: false });
    const generatedByKey = new Map(generated.map((entry) => [entry.playlistKey, entry]));

    for (const def of listUserPersonalPlaylists(db, userId)) {
      const key = `personal:${def.id}`;
      if (!wantsKey(keys, key)) continue;
      const row = generatedByKey.get(key);
      playlists.push({
        type: 'smart',
        key,
        name: text(def.name),
        owner: userId,
        audience: 'personal',
        enabled: row ? row.active !== false : true,
        smart: { rules: withoutArtwork(def.rules), trackFilters: def.trackFilters ?? null },
        artwork: exportArtwork(def.rules?.artwork, includeArtwork),
        tracks: row ? await snapshotTracks(ctx, userId, row) : [],
      });
    }

    for (const row of generated) {
      const type = text(row.playlistType).toLowerCase();
      if (!wantsKey(keys, row.playlistKey)) continue;
      const artwork = exportArtwork(row, includeArtwork);

      if (type === 'custom') {
        // A global imported playlist is copied to every user; export it once.
        if (row.audience === 'global') {
          if (exportedGlobalCustomKeys.has(row.playlistKey)) continue;
          exportedGlobalCustomKeys.add(row.playlistKey);
        }
        const sourceType = text(row.sourceType).toLowerCase();
        const imported = isImportedPlaylistSourceType(sourceType) && sourceType !== 'curatorr-backup';
        playlists.push({
          type: imported ? 'imported' : 'static',
          key: row.playlistKey,
          name: text(row.playlistTitle),
          owner: userId,
          audience: row.audience === 'global' && includeGlobal ? 'global' : 'personal',
          enabled: row.active !== false,
          backupOnly: Boolean(row.backupOnly),
          source: imported ? {
            type: sourceType,
            ref: text(row.sourceRef),
            title: text(row.sourceTitle),
            owner: text(row.sourceOwner),
            filename: text(row.sourceFilename),
            content: sourceType === 'm3u-file' ? String(row.sourceContent || '') : '',
            refreshPeriod: text(row.importedSyncPeriod) || 'disabled',
          } : null,
          artwork,
          tracks: await snapshotTracks(ctx, userId, row),
          missing: snapshotMissing(db, userId, row.playlistKey),
        });
        continue;
      }

      // A non-admin's view of a global smart playlist is kept as a static copy.
      if (type === 'global' && !includeGlobal) {
        playlists.push({
          type: 'static',
          key: row.playlistKey,
          name: text(row.playlistTitle),
          owner: userId,
          audience: 'personal',
          enabled: row.active !== false,
          backupOnly: false,
          source: null,
          artwork,
          tracks: await snapshotTracks(ctx, userId, row),
          missing: [],
        });
        continue;
      }

      if (SYSTEM_PLAYLIST_TYPES.has(type)) {
        playlists.push({
          type: 'system',
          kind: type,
          key: row.playlistKey,
          name: text(row.playlistTitle),
          titleOverride: text(row.titleOverride),
          owner: userId,
          audience: 'personal',
          enabled: row.active !== false,
          artwork,
          tracks: await snapshotTracks(ctx, userId, row),
        });
      }
    }

    // Play history can name accounts that never used Curatorr; only real users carry settings.
    const hasCuratorrData = generated.length > 0 || getUserPreferences(db, userId).userWizardCompleted;
    if (includeSettings && !keys && hasCuratorrData) settings.push({ owner: userId, ...exportSystemSettings(db, userId) });
  }

  return {
    format: PLAYLIST_BACKUP_FORMAT,
    version: PLAYLIST_BACKUP_VERSION,
    exportedAt: new Date().toISOString(),
    curatorrVersion: text(ctx.APP_VERSION),
    mediaServer: text(config?.mediaServer?.type || 'plex'),
    owners: [...new Set(userIds)],
    settings,
    playlists,
  };
}

// ─── M3U ──────────────────────────────────────────────────────────────────────

function m3uLine(value) {
  return String(value || '').replace(/[\r\n]+/g, ' ').trim();
}

export function buildPlaylistM3u(entry) {
  const lines = ['#EXTM3U', `#PLAYLIST:${m3uLine(entry?.name || 'Playlist')}`];
  for (const track of Array.isArray(entry?.tracks) ? entry.tracks : []) {
    const seconds = Math.round(Number(track.durationMs || 0) / 1000) || -1;
    const label = [m3uLine(track.artist), m3uLine(track.title)].filter(Boolean).join(' - ');
    lines.push(`#EXTINF:${seconds},${label}`);
    if (track.album) lines.push(`#EXTALB:${m3uLine(track.album)}`);
    // M3U needs a location line; a track without a known file keeps its label so another
    // player (or Curatorr's M3U import) can still match it by name.
    lines.push(m3uLine(track.path) || label || 'Unknown track');
  }
  return `${lines.join('\n')}\n`;
}

// ─── Restore ──────────────────────────────────────────────────────────────────

export function parsePlaylistBackup(raw) {
  let backup = raw;
  if (typeof raw === 'string' || Buffer.isBuffer(raw)) {
    try {
      backup = JSON.parse(String(raw).replace(/^﻿/, ''));
    } catch {
      throw backupError('This file is not a Curatorr playlist backup (it is not valid JSON).');
    }
  }
  if (!backup || typeof backup !== 'object' || backup.format !== PLAYLIST_BACKUP_FORMAT) {
    throw backupError('This file is not a Curatorr playlist backup.');
  }
  const version = Number(backup.version);
  if (!Number.isInteger(version) || version < 1) throw backupError('This backup has an unknown format version.');
  if (version > PLAYLIST_BACKUP_VERSION) {
    throw backupError(`This backup was made by a newer Curatorr (format version ${version}). Update Curatorr to restore it.`);
  }
  if (!Array.isArray(backup.playlists)) throw backupError('This backup contains no playlists.');
  return {
    ...backup,
    settings: Array.isArray(backup.settings) ? backup.settings : [],
    playlists: backup.playlists
      .filter((entry) => entry && typeof entry === 'object')
      .map((entry, index) => ({ ...entry, entryId: String(index) })),
  };
}

function resolveTarget(entry, { userId, isAdmin, ownerMode }) {
  if (isAdmin && ownerMode === 'original' && text(entry.owner)) return text(entry.owner);
  return userId;
}

function existingNames(ctx, targetUserId) {
  const names = new Set();
  listUserPersonalPlaylists(ctx.db, targetUserId).forEach((def) => names.add(text(def.name).toLowerCase()));
  listUserGeneratedPlaylists(ctx.db, targetUserId, { activeOnly: false })
    .forEach((row) => names.add(text(row.playlistTitle).toLowerCase()));
  return names;
}

function globalNames(ctx) {
  return new Set((ctx.loadConfig().globalPlaylists || []).map((def) => text(def.name).toLowerCase()));
}

function matchTracks(lookups, entry) {
  const matched = [];
  const missing = [];
  const seen = new Set();
  const consider = (track, fromMissing) => {
    const { match } = resolveTrackIdentity(lookups, {
      title: track.title,
      artistName: track.artist || (Array.isArray(track.artists) ? track.artists[0] : ''),
      albumName: track.album,
      durationMs: track.durationMs,
      filePath: track.path,
      recordingMbid: track.mbid,
    });
    const position = Number(track.position || 0);
    if (match?.ratingKey) {
      if (seen.has(match.ratingKey)) return;
      seen.add(match.ratingKey);
      matched.push({ ratingKey: match.ratingKey, artistName: match.artistName, sourcePosition: position || matched.length + 1 });
      return;
    }
    if (!text(track.title)) return;
    const artists = Array.isArray(track.artists) && track.artists.length
      ? track.artists.map(text).filter(Boolean)
      : [text(track.artist)].filter(Boolean);
    missing.push({
      sourceTrackId: text(track.sourceTrackId || (fromMissing ? '' : track.path)),
      position,
      title: text(track.title),
      artistName: artists[0] || '',
      artists,
      albumTitle: text(track.album),
      durationMs: Number(track.durationMs || 0),
      filePath: text(track.path),
      recordingMbid: text(track.mbid),
    });
  };
  (Array.isArray(entry.tracks) ? entry.tracks : []).forEach((track) => consider(track, false));
  (Array.isArray(entry.missing) ? entry.missing : []).forEach((track) => consider(track, true));
  matched.sort((a, b) => a.sourcePosition - b.sourcePosition);
  return { matched, missing };
}

function describeEntry(ctx, entry, options, lookups) {
  const target = resolveTarget(entry, options);
  const type = text(entry.type);
  const base = {
    entryId: entry.entryId,
    type,
    kind: text(entry.kind),
    name: text(entry.name) || 'Playlist',
    owner: text(entry.owner),
    targetUser: type === 'global' && options.isAdmin ? '' : target,
    enabled: entry.enabled !== false,
    backupOnly: Boolean(entry.backupOnly),
    trackCount: Array.isArray(entry.tracks) ? entry.tracks.length : 0,
    restorable: true,
    note: '',
  };
  if (type === 'smart' || type === 'global') {
    if (!entry.smart?.rules || typeof entry.smart.rules !== 'object') {
      return { ...base, restorable: false, note: 'The playlist rules are missing from the backup.' };
    }
    const asGlobal = type === 'global' && options.isAdmin;
    const conflict = asGlobal
      ? globalNames(ctx).has(base.name.toLowerCase())
      : existingNames(ctx, target).has(base.name.toLowerCase());
    return {
      ...base,
      restoreAs: asGlobal ? 'global' : 'smart',
      conflict,
      note: type === 'global' && !options.isAdmin
        ? 'Restores as your own smart playlist and rebuilds from its rules.'
        : 'Rebuilds from its rules against the current library.',
    };
  }
  if (type === 'imported' || type === 'static') {
    const { matched, missing } = matchTracks(lookups, entry);
    return {
      ...base,
      restoreAs: 'custom',
      matched: matched.length,
      missing: missing.length,
      conflict: existingNames(ctx, target).has(base.name.toLowerCase()),
      note: missing.length ? 'Unmatched tracks are kept on the playlist\'s missing list.' : '',
    };
  }
  if (type === 'system') {
    const row = listUserGeneratedPlaylists(ctx.db, target, { activeOnly: false })
      .find((candidate) => candidate.playlistKey === text(entry.key));
    return {
      ...base,
      restoreAs: 'system',
      conflict: false,
      restorable: Boolean(row),
      note: row
        ? 'Restores its name and artwork; tracks regenerate from the user\'s settings.'
        : 'This user does not have this system playlist yet. Finish user setup, then restore again.',
    };
  }
  return { ...base, restorable: false, note: 'This playlist type is not supported by this version of Curatorr.' };
}

export function previewPlaylistBackup(ctx, backup, options) {
  const lookups = buildTrackIdentityLookups(getMasterTracks(ctx.db));
  const knownUsers = new Set(getAllUserIds(ctx.db));
  const playlists = backup.playlists.map((entry) => {
    const described = describeEntry(ctx, entry, options, lookups);
    return {
      ...described,
      unknownUser: Boolean(described.targetUser) && described.targetUser !== options.userId && !knownUsers.has(described.targetUser),
    };
  });
  const settings = backup.settings.map((entry, index) => ({
    index,
    owner: text(entry.owner),
    targetUser: resolveTarget(entry, options),
  }));
  return {
    exportedAt: text(backup.exportedAt),
    curatorrVersion: text(backup.curatorrVersion),
    version: Number(backup.version),
    owners: Array.isArray(backup.owners) ? backup.owners.map(text) : [],
    playlists,
    settings,
  };
}

function restoreArtworkAsset(asset, nameHint, prefix) {
  if (!asset?.data || !asset?.mime) return '';
  const parsed = parsePlaylistArtworkDataUrl(`data:${asset.mime};base64,${asset.data}`);
  if (!parsed.ok) return '';
  return savePlaylistArtworkBuffer(parsed.buffer, parsed.ext, nameHint, prefix);
}

function restoreArtwork(artwork, nameHint) {
  const customArtworkAsset = restoreArtworkAsset(artwork?.custom, nameHint, 'custom');
  const preservedArtworkAsset = restoreArtworkAsset(artwork?.preserved, nameHint, 'preserved');
  let mode = ['auto', 'preserve', 'custom'].includes(text(artwork?.mode)) ? text(artwork.mode) : 'auto';
  if (mode === 'custom' && !customArtworkAsset) mode = 'auto';
  if (mode === 'preserve' && !preservedArtworkAsset) mode = 'auto';
  return { mode, customArtworkAsset, preservedArtworkAsset };
}

function uniqueName(name, taken, onConflict) {
  const wanted = text(name) || 'Playlist';
  if (!taken.has(wanted.toLowerCase())) return wanted;
  if (onConflict === 'skip') return '';
  for (let n = 1; n < 1000; n += 1) {
    const candidate = n === 1 ? `${wanted} (restored)` : `${wanted} (restored ${n})`;
    if (!taken.has(candidate.toLowerCase())) return candidate;
  }
  return '';
}

function normalizeSmartRules(rules) {
  const next = { ...(rules && typeof rules === 'object' ? rules : {}) };
  // A personal smart playlist needs a rebuild schedule; global definitions do not carry one.
  if (!['daily', 'weekly', 'manual'].includes(next.rebuildSchedule)) next.rebuildSchedule = 'daily';
  return next;
}

/**
 * Restores the selected entries. Media-server syncs are queued through `queueSync` (by default
 * run in the background) so the request returns as soon as Curatorr's own records are written.
 */
export function restorePlaylistBackup(ctx, backup, {
  userId,
  isAdmin = false,
  ownerMode = 'self',
  entryIds = null,
  includeSettings = true,
  onConflict = 'rename',
  queueSync = null,
} = {}) {
  const { db, loadConfig, saveConfig, playlistService, pushLog } = ctx;
  const options = { userId, isAdmin, ownerMode };
  const selected = Array.isArray(entryIds) ? new Set(entryIds.map(String)) : null;
  const lookups = buildTrackIdentityLookups(getMasterTracks(db));
  const restored = [];
  const skipped = [];
  const syncJobs = [];
  const takenByUser = new Map();
  const takenFor = (target) => {
    if (!takenByUser.has(target)) takenByUser.set(target, existingNames(ctx, target));
    return takenByUser.get(target);
  };
  const takenGlobal = globalNames(ctx);

  for (const entry of backup.playlists) {
    if (selected && !selected.has(entry.entryId)) continue;
    const described = describeEntry(ctx, entry, options, lookups);
    if (!described.restorable) {
      skipped.push({ entryId: entry.entryId, name: described.name, reason: described.note });
      continue;
    }
    const target = resolveTarget(entry, options);

    if (described.restoreAs === 'global') {
      const name = uniqueName(entry.name, takenGlobal, onConflict);
      if (!name) { skipped.push({ entryId: entry.entryId, name: described.name, reason: 'A global playlist with this name already exists.' }); continue; }
      takenGlobal.add(name.toLowerCase());
      const def = {
        id: makeId('gp_'),
        name,
        rules: { ...withoutArtwork(entry.smart.rules), artwork: restoreArtwork(entry.artwork, name) },
        trackFilters: entry.smart.trackFilters ?? undefined,
        enabled: entry.enabled !== false,
        createdAt: Date.now(),
      };
      const config = loadConfig();
      saveConfig({ ...config, globalPlaylists: [...(config.globalPlaylists || []), def] });
      if (def.enabled) {
        const blendUsers = Array.isArray(def.rules.blendUsers) ? def.rules.blendUsers.filter(Boolean) : [];
        const syncIds = blendUsers.length
          ? blendUsers
          : getAllUserIds(db).filter((uid) => getUserPreferences(db, uid).userWizardCompleted);
        for (const uid of syncIds) syncJobs.push(() => playlistService?.syncGlobalPlaylist(uid, def));
      }
      restored.push({ entryId: entry.entryId, name, type: 'global', playlistKey: `global:${def.id}` });
      continue;
    }

    if (described.restoreAs === 'smart') {
      const taken = takenFor(target);
      const name = uniqueName(entry.name, taken, onConflict);
      if (!name || findUserPersonalPlaylistByName(db, target, name)) {
        skipped.push({ entryId: entry.entryId, name: described.name, reason: 'A playlist with this name already exists.' });
        continue;
      }
      taken.add(name.toLowerCase());
      const def = {
        id: makeId('pp_'),
        name,
        rules: { ...normalizeSmartRules(withoutArtwork(entry.smart.rules)), artwork: restoreArtwork(entry.artwork, name) },
        trackFilters: entry.smart.trackFilters ?? null,
      };
      createUserPersonalPlaylist(db, target, def);
      if (entry.enabled !== false) {
        const blendUsers = Array.isArray(def.rules.blendUsers) ? def.rules.blendUsers.filter(Boolean) : [];
        for (const uid of new Set([target, ...blendUsers])) {
          syncJobs.push(() => playlistService?.syncPersonalPlaylist(uid, def));
        }
      }
      restored.push({ entryId: entry.entryId, name, type: 'smart', owner: target, playlistKey: `personal:${def.id}` });
      continue;
    }

    if (described.restoreAs === 'custom') {
      const taken = takenFor(target);
      const name = uniqueName(entry.name, taken, onConflict);
      if (!name) { skipped.push({ entryId: entry.entryId, name: described.name, reason: 'A playlist with this name already exists.' }); continue; }
      taken.add(name.toLowerCase());
      const { matched, missing } = matchTracks(lookups, entry);
      const source = entry.type === 'imported' && entry.source && isImportedPlaylistSourceType(entry.source.type)
        ? entry.source
        : null;
      const backupOnly = Boolean(entry.backupOnly);
      const active = entry.enabled !== false && !backupOnly;
      const artwork = restoreArtwork(entry.artwork, name);
      const playlistKey = 'custom-import-' + Date.now().toString(36) + crypto.randomBytes(4).toString('hex').slice(0, 5);
      const now = Date.now();
      saveUserGeneratedPlaylist(db, target, {
        playlistKey,
        playlistType: 'custom',
        playlistTitle: name,
        plexPlaylistId: '',
        artworkMode: artwork.mode,
        customArtworkAsset: artwork.customArtworkAsset,
        preservedArtworkAsset: artwork.preservedArtworkAsset,
        sourceType: source ? text(source.type) : 'curatorr-backup',
        sourceRef: source ? text(source.ref) : '',
        sourceTitle: source ? text(source.title) : text(entry.name),
        sourceOwner: source ? text(source.owner) : 'Curatorr backup',
        sourceContent: source && text(source.type) === 'm3u-file' ? String(source.content || '') : '',
        sourceFilename: source ? text(source.filename) : '',
        importedSyncPeriod: source ? text(source.refreshPeriod) || 'disabled' : 'disabled',
        audience: 'personal',
        trackCount: matched.length,
        missingCount: missing.length,
        active,
        backupOnly,
        lastBuiltAt: now,
        createdAt: now,
        updatedAt: now,
      });
      setPlaylistTracks(db, target, playlistKey, matched);
      setImportedPlaylistUnmatched(db, target, playlistKey, missing);
      const asGlobal = isAdmin && entry.audience === 'global';
      if (asGlobal) setCustomPlaylistAudience(db, target, playlistKey, 'global');
      if (active) {
        syncJobs.push(() => {
          const row = listUserGeneratedPlaylists(db, target, { activeOnly: false })
            .find((candidate) => candidate.playlistKey === playlistKey);
          if (!row) return null;
          return asGlobal
            ? playlistService?.syncGlobalCustomPlaylist(target, row)
            : playlistService?.syncCustomPlaylist(target, row);
        });
      }
      restored.push({
        entryId: entry.entryId, name, type: entry.type, owner: target, playlistKey,
        trackCount: matched.length, missing: missing.length,
      });
      continue;
    }

    if (described.restoreAs === 'system') {
      const row = listUserGeneratedPlaylists(db, target, { activeOnly: false })
        .find((candidate) => candidate.playlistKey === text(entry.key));
      const artwork = restoreArtwork(entry.artwork, row.playlistTitle);
      const titleOverride = text(entry.titleOverride);
      saveUserGeneratedPlaylist(db, target, {
        ...row,
        artworkMode: artwork.mode,
        customArtworkAsset: artwork.customArtworkAsset,
        preservedArtworkAsset: artwork.preservedArtworkAsset,
        updatedAt: Date.now(),
      });
      if (titleOverride && titleOverride !== text(row.titleOverride)) {
        syncJobs.push(() => playlistService?.renameGeneratedPlaylistTitle(target, row.playlistKey, titleOverride));
      }
      restored.push({ entryId: entry.entryId, name: titleOverride || row.playlistTitle, type: 'system', owner: target, playlistKey: row.playlistKey });
    }
  }

  let settingsRestored = 0;
  if (includeSettings) {
    for (const entry of backup.settings) {
      const target = resolveTarget(entry, options);
      if (!target) continue;
      const next = {};
      for (const key of SYSTEM_SETTING_KEYS) {
        if (entry[key] !== undefined) next[key] = entry[key];
      }
      saveUserPreferences(db, target, { ...getUserPreferences(db, target), ...next });
      settingsRestored += 1;
    }
  }

  pushLog?.({
    level: 'info',
    app: 'playlist',
    action: 'backup.restore',
    message: `Restored ${restored.length} playlist(s) from backup for ${userId}${skipped.length ? ` (${skipped.length} skipped)` : ''}${settingsRestored ? `, ${settingsRestored} settings set(s)` : ''}`,
  });

  const runSyncs = queueSync || ((jobs) => setImmediate(async () => {
    for (const job of jobs) {
      try {
        await job();
      } catch (err) {
        pushLog?.({ level: 'warn', app: 'playlist', action: 'backup.restore.sync', message: `Restored playlist sync failed: ${err?.message || err}` });
      }
    }
  }));
  if (syncJobs.length) runSyncs(syncJobs);

  return { restored, skipped, settingsRestored };
}
