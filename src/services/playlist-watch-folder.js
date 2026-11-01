import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import {
  listPlaylistWatchFiles,
  savePlaylistWatchFile,
  deletePlaylistWatchFile,
} from '../db.js';
import {
  PLAYLIST_ARTWORK_MAX_BYTES,
  detectPlaylistArtworkMimeFromBuffer,
} from './playlist-artwork.js';

// Watch folder for externally generated playlists. Each .m3u/.m3u8 file becomes one
// imported playlist identified by its path relative to the folder; an image with the
// same base name becomes its artwork. Unchanged files are skipped by size + mtime and
// then by content hash, so a scan of an idle folder only stats files.

export const PLAYLIST_WATCH_SOURCE_TYPE = 'm3u-watch';
export const PLAYLIST_WATCH_MAX_BYTES = 2 * 1024 * 1024;
const PLAYLIST_FILE_RE = /\.m3u8?$/i;
const ARTWORK_EXTS = ['.jpg', '.jpeg', '.png', '.webp'];
const ARTWORK_EXT_BY_MIME = { 'image/jpeg': 'jpg', 'image/png': 'png', 'image/webp': 'webp' };
const MAX_DEPTH = 4;
const MAX_PLAYLISTS = 1000;
// Files modified this recently may still be being written by a non-atomic writer.
const SETTLE_MS = 3000;

export function normalizePlaylistWatchConfig(raw = {}) {
  const value = raw && typeof raw === 'object' ? raw : {};
  return {
    enabled: value.enabled === true,
    path: String(value.path || '').trim(),
    ownerUserId: String(value.ownerUserId || '').trim(),
    audience: String(value.audience || '').trim().toLowerCase() === 'global' ? 'global' : 'personal',
    removeMissing: value.removeMissing === true,
    // '' falls back to the global default for new imports (playlistImports).
    syncPeriod: ['disabled', 'daily', 'weekly', 'monthly'].includes(String(value.syncPeriod || '').trim().toLowerCase())
      ? String(value.syncPeriod).trim().toLowerCase()
      : '',
  };
}

export function buildPlaylistWatchSourceRef(relPath) {
  return `${PLAYLIST_WATCH_SOURCE_TYPE}:${relPath}`;
}

function sha1(buffer) {
  return crypto.createHash('sha1').update(buffer).digest('hex');
}

function statSignature(stat) {
  return `${Number(stat?.size || 0)}:${Math.round(Number(stat?.mtimeMs || 0))}`;
}

function findArtworkFile(dirPath, baseName, siblings) {
  const wanted = new Set(ARTWORK_EXTS.map((ext) => `${baseName}${ext}`.toLowerCase()));
  const match = siblings.find((name) => wanted.has(name.toLowerCase()));
  return match ? path.join(dirPath, match) : '';
}

// Lists playlist files (relative paths use "/" on every platform), with the
// matching artwork file when one exists. Dotfiles and symlinks are skipped.
export function listPlaylistWatchCandidates(rootDir) {
  const root = path.resolve(String(rootDir || ''));
  const results = [];
  const walk = (dirPath, depth) => {
    let entries;
    try {
      entries = fs.readdirSync(dirPath, { withFileTypes: true });
    } catch (_err) {
      return;
    }
    const fileNames = entries.filter((entry) => entry.isFile()).map((entry) => entry.name);
    for (const entry of entries) {
      if (results.length >= MAX_PLAYLISTS) return;
      if (entry.name.startsWith('.')) continue;
      const absPath = path.join(dirPath, entry.name);
      if (entry.isDirectory()) {
        if (depth < MAX_DEPTH) walk(absPath, depth + 1);
        continue;
      }
      if (!entry.isFile() || !PLAYLIST_FILE_RE.test(entry.name)) continue;
      const baseName = entry.name.replace(PLAYLIST_FILE_RE, '');
      results.push({
        relPath: path.relative(root, absPath).split(path.sep).join('/'),
        absPath,
        artworkPath: findArtworkFile(dirPath, baseName, fileNames),
      });
    }
  };
  walk(root, 0);
  return results.sort((a, b) => a.relPath.localeCompare(b.relPath));
}

// deps:
//   importPlaylist({ userPlexId, relPath, content }) -> playlist row (creates or updates by source ref)
//   playlistExists(userPlexId, playlistKey) -> boolean
//   applyArtwork({ userPlexId, playlistKey, buffer, ext, nameHint }) -> void; buffer null resets to auto
//   syncGlobal(userPlexId, playlistKey) -> copies the playlist to every user
//   removePlaylist(playlistKey) -> deletes every user's copy and its media-server playlist
export function createPlaylistWatchFolderService({ db, loadConfig, pushLog, safeMessage, ...deps }) {
  const log = (level, action, message) => pushLog?.({ level, app: 'playlist', action, message });
  const errorText = (err) => (typeof safeMessage === 'function' ? safeMessage(err) : String(err?.message || err));

  function readArtwork(artworkPath) {
    if (!artworkPath) return { status: 'none' };
    const stat = fs.statSync(artworkPath);
    if (stat.size > PLAYLIST_ARTWORK_MAX_BYTES) return { status: 'invalid', signature: statSignature(stat), message: 'artwork is larger than 5 MB' };
    const buffer = fs.readFileSync(artworkPath);
    const mime = detectPlaylistArtworkMimeFromBuffer(buffer);
    if (!ARTWORK_EXT_BY_MIME[mime]) return { status: 'invalid', signature: statSignature(stat), message: 'artwork is not a JPEG, PNG or WebP image' };
    return { status: 'ok', buffer, ext: ARTWORK_EXT_BY_MIME[mime], hash: `${statSignature(stat)}:${sha1(buffer)}` };
  }

  async function syncArtwork(candidate, tracked, userPlexId, playlistKey, { force = false } = {}) {
    const previous = String(tracked?.artworkHash || '');
    if (!candidate.artworkPath) {
      if (previous && !force) await deps.applyArtwork({ userPlexId, playlistKey, buffer: null });
      return { hash: '', changed: Boolean(previous) };
    }
    const stat = fs.statSync(candidate.artworkPath);
    if (!force && previous.startsWith(`${statSignature(stat)}:`)) return { hash: previous, changed: false };
    const artwork = readArtwork(candidate.artworkPath);
    if (artwork.status !== 'ok') return { hash: previous, changed: false, warning: artwork.message };
    // Same image with a new mtime: just remember the new signature.
    if (!force && previous && previous.split(':').pop() === artwork.hash.split(':').pop()) {
      return { hash: artwork.hash, changed: false };
    }
    const nameHint = path.basename(candidate.relPath).replace(PLAYLIST_FILE_RE, '');
    await deps.applyArtwork({ userPlexId, playlistKey, buffer: artwork.buffer, ext: artwork.ext, nameHint });
    return { hash: artwork.hash, changed: true };
  }

  async function processCandidate(candidate, tracked, settings, now) {
    const stat = fs.statSync(candidate.absPath);
    if (now - stat.mtimeMs < SETTLE_MS) return { outcome: 'pending' };
    const base = {
      relPath: candidate.relPath,
      userPlexId: tracked?.userPlexId || '',
      playlistKey: tracked?.playlistKey || '',
      fileSize: stat.size,
      fileMtimeMs: stat.mtimeMs,
      contentHash: tracked?.contentHash || '',
      artworkHash: tracked?.artworkHash || '',
      lastSeenAt: now,
      lastImportedAt: tracked?.lastImportedAt || 0,
    };
    if (stat.size > PLAYLIST_WATCH_MAX_BYTES) {
      savePlaylistWatchFile(db, { ...base, status: 'error', message: 'File is larger than the 2 MB import limit.' });
      return { outcome: 'error' };
    }

    const hasPlaylist = Boolean(tracked?.playlistKey && deps.playlistExists(tracked.userPlexId, tracked.playlistKey));
    const sameStat = tracked && tracked.fileSize === stat.size && tracked.fileMtimeMs === Math.round(stat.mtimeMs);
    let content = null;
    let contentHash = tracked?.contentHash || '';
    if (!sameStat || !hasPlaylist || tracked?.status === 'error' || tracked?.status === 'missing') {
      const buffer = fs.readFileSync(candidate.absPath);
      contentHash = sha1(buffer);
      content = buffer.toString('utf8');
    }
    const contentChanged = !tracked || contentHash !== tracked.contentHash;

    // A playlist deleted in Curatorr stays deleted until its file changes.
    if (tracked?.playlistKey && !hasPlaylist && !contentChanged) {
      savePlaylistWatchFile(db, {
        ...base,
        status: 'deleted',
        message: 'Playlist was deleted in Curatorr. Change the file to import it again.',
      });
      return { outcome: 'unchanged' };
    }

    // Existing files keep their owner; the configured owner applies to new files.
    const userPlexId = hasPlaylist ? tracked.userPlexId : settings.ownerUserId;
    let playlistKey = hasPlaylist ? tracked.playlistKey : '';
    let outcome = 'unchanged';
    if (contentChanged || !hasPlaylist) {
      if (!String(content || '').trim()) {
        savePlaylistWatchFile(db, { ...base, status: 'error', message: 'File is empty.' });
        return { outcome: 'error' };
      }
      const playlist = await deps.importPlaylist({ userPlexId, relPath: candidate.relPath, content });
      if (!playlist?.playlistKey) throw new Error('Import did not create a playlist.');
      playlistKey = playlist.playlistKey;
      outcome = hasPlaylist ? 'updated' : 'imported';
    }

    const artwork = await syncArtwork(candidate, hasPlaylist ? tracked : null, userPlexId, playlistKey, { force: !hasPlaylist });
    if (outcome === 'unchanged' && artwork.changed) outcome = 'updated';
    if (outcome !== 'unchanged' && settings.audience === 'global') await deps.syncGlobal(userPlexId, playlistKey);

    savePlaylistWatchFile(db, {
      ...base,
      userPlexId,
      playlistKey,
      contentHash,
      artworkHash: artwork.hash,
      status: 'ok',
      message: artwork.warning ? `Artwork skipped: ${artwork.warning}.` : '',
      lastImportedAt: outcome === 'unchanged' ? base.lastImportedAt : now,
    });
    if (outcome !== 'unchanged') {
      log('info', `watch.${outcome}`, `Watch folder ${outcome} "${candidate.relPath}" for ${userPlexId}`);
    }
    return { outcome };
  }

  async function handleMissing(tracked, settings, now) {
    if (settings.removeMissing) {
      if (tracked.playlistKey) await deps.removePlaylist(tracked.playlistKey);
      deletePlaylistWatchFile(db, tracked.relPath);
      log('info', 'watch.removed', `Watch folder file "${tracked.relPath}" was deleted; removed its playlist`);
      return 'removed';
    }
    if (tracked.status !== 'missing') {
      savePlaylistWatchFile(db, {
        ...tracked,
        status: 'missing',
        message: 'Source file is missing. The playlist is kept.',
        lastSeenAt: tracked.lastSeenAt || now,
      });
      log('warn', 'watch.missing', `Watch folder file "${tracked.relPath}" is missing; its playlist is kept`);
    }
    return 'missing';
  }

  async function scan() {
    const settings = normalizePlaylistWatchConfig(loadConfig().playlistWatchFolder);
    if (!settings.enabled) return { skipped: true, message: 'Playlist watch folder is disabled.' };
    if (!settings.path) throw new Error('Playlist watch folder path is not set.');
    if (!settings.ownerUserId) throw new Error('Playlist watch folder owner is not set.');
    let rootStat;
    try {
      rootStat = fs.statSync(settings.path);
    } catch (_err) {
      throw new Error(`Playlist watch folder not found: ${settings.path}`);
    }
    if (!rootStat.isDirectory()) throw new Error(`Playlist watch folder is not a directory: ${settings.path}`);

    const now = Date.now();
    const trackedByPath = new Map(listPlaylistWatchFiles(db).map((entry) => [entry.relPath, entry]));
    const counts = { imported: 0, updated: 0, unchanged: 0, pending: 0, missing: 0, removed: 0, error: 0 };
    const candidates = listPlaylistWatchCandidates(settings.path);
    for (const candidate of candidates) {
      const tracked = trackedByPath.get(candidate.relPath) || null;
      trackedByPath.delete(candidate.relPath);
      try {
        const { outcome } = await processCandidate(candidate, tracked, settings, now);
        counts[outcome] += 1;
      } catch (err) {
        counts.error += 1;
        const message = errorText(err);
        savePlaylistWatchFile(db, {
          ...(tracked || {}),
          relPath: candidate.relPath,
          status: 'error',
          message,
          lastSeenAt: now,
        });
        log('warn', 'watch.error', `Watch folder import failed for "${candidate.relPath}": ${message}`);
      }
    }
    // Anything still tracked was not found in this scan.
    for (const tracked of trackedByPath.values()) {
      try {
        counts[await handleMissing(tracked, settings, now)] += 1;
      } catch (err) {
        counts.error += 1;
        log('warn', 'watch.error', `Watch folder cleanup failed for "${tracked.relPath}": ${errorText(err)}`);
      }
    }
    const parts = [
      `${candidates.length} file(s)`,
      `${counts.imported} imported`,
      `${counts.updated} updated`,
      `${counts.unchanged} unchanged`,
    ];
    if (counts.pending) parts.push(`${counts.pending} still being written`);
    if (counts.missing) parts.push(`${counts.missing} missing`);
    if (counts.removed) parts.push(`${counts.removed} removed`);
    if (counts.error) parts.push(`${counts.error} failed`);
    return { ...counts, files: candidates.length, message: parts.join(', ') };
  }

  return { scan };
}
