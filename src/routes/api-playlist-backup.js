// Playlist backup export (Curatorr JSON and M3U) and restore.

import express from 'express';
import { getAllUserIds } from '../db.js';
import {
  buildPlaylistBackup,
  buildPlaylistM3u,
  parsePlaylistBackup,
  previewPlaylistBackup,
  restorePlaylistBackup,
} from '../services/playlist-backup.js';

// Backups can embed artwork, so they bypass the app-wide JSON body limit with their own type.
export const PLAYLIST_BACKUP_CONTENT_TYPE = 'application/x-curatorr-backup';
const BACKUP_BODY_LIMIT = String(process.env.PLAYLIST_BACKUP_BODY_LIMIT || '64mb').trim() || '64mb';

function resolveCanonicalUserId(req) {
  const previewCanonicalId = String(req.session?.previewCanonicalId || '').trim();
  if (previewCanonicalId) return previewCanonicalId;
  const previewUserId = String(req.session?.previewUserId || '').trim();
  if (previewUserId) return previewUserId;
  return String(req.session?.user?.username || '').trim();
}

function isAdminRole(req) {
  return ['admin', 'co-admin'].includes(String(req.session?.user?.role || '').trim().toLowerCase());
}

function flag(value, fallback) {
  if (value === undefined || value === null || value === '') return fallback;
  return !['0', 'false', 'no', 'off'].includes(String(value).trim().toLowerCase());
}

function fileSlug(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 60) || 'playlist';
}

function sendDownload(res, filename, contentType, body) {
  res.setHeader('Content-Type', contentType);
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.setHeader('Cache-Control', 'no-store');
  res.send(body);
}

export function registerPlaylistBackup(app, ctx) {
  const { requireUser, safeMessage, db } = ctx;
  const parseBackupBody = express.text({ type: PLAYLIST_BACKUP_CONTENT_TYPE, limit: BACKUP_BODY_LIMIT });

  function readRestoreRequest(req) {
    let payload;
    try {
      payload = JSON.parse(String(req.body || ''));
    } catch {
      const err = new Error('The backup file could not be read.');
      err.status = 400;
      throw err;
    }
    const backup = parsePlaylistBackup(payload?.backup);
    const options = payload?.options && typeof payload.options === 'object' ? payload.options : {};
    const isAdmin = isAdminRole(req);
    return {
      backup,
      options: {
        userId: resolveCanonicalUserId(req),
        isAdmin,
        ownerMode: isAdmin && options.ownerMode === 'original' ? 'original' : 'self',
        entryIds: Array.isArray(options.entryIds) ? options.entryIds.map(String) : null,
        includeSettings: options.includeSettings !== false,
        onConflict: options.onConflict === 'skip' ? 'skip' : 'rename',
      },
    };
  }

  app.get('/api/music/playlists/backup/export', requireUser, async (req, res) => {
    const userId = resolveCanonicalUserId(req);
    const isAdmin = isAdminRole(req);
    const allUsers = String(req.query?.scope || '').trim() === 'all';
    if (allUsers && !isAdmin) return res.status(403).json({ error: 'Only admins can back up every user\'s playlists.' });
    const playlistKeys = String(req.query?.keys || '')
      .split(',')
      .map((key) => key.trim())
      .filter(Boolean);
    try {
      const userIds = allUsers ? [...new Set([userId, ...getAllUserIds(db)])].filter(Boolean) : [userId];
      const backup = await buildPlaylistBackup(ctx, {
        userIds,
        playlistKeys: playlistKeys.length ? playlistKeys : null,
        includeArtwork: flag(req.query?.artwork, true),
        includeSettings: flag(req.query?.settings, true),
        includeGlobal: isAdmin,
      });
      if (!backup.playlists.length && playlistKeys.length) return res.status(404).json({ error: 'Playlist not found.' });
      const date = backup.exportedAt.slice(0, 10);
      const name = backup.playlists.length === 1 && playlistKeys.length
        ? fileSlug(backup.playlists[0].name)
        : (allUsers ? 'all-users' : 'playlists');
      sendDownload(res, `curatorr-${name}-${date}.curatorr.json`, 'application/json; charset=utf-8', JSON.stringify(backup, null, 2));
    } catch (err) {
      res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.get('/api/music/playlists/backup/export.m3u', requireUser, async (req, res) => {
    const userId = resolveCanonicalUserId(req);
    const playlistKey = String(req.query?.key || '').trim();
    if (!playlistKey) return res.status(400).json({ error: 'key is required.' });
    try {
      const backup = await buildPlaylistBackup(ctx, {
        userIds: [userId],
        playlistKeys: [playlistKey],
        includeArtwork: false,
        includeSettings: false,
        includeGlobal: isAdminRole(req),
      });
      const entry = backup.playlists[0];
      if (!entry) return res.status(404).json({ error: 'Playlist not found.' });
      sendDownload(res, `${fileSlug(entry.name)}.m3u8`, 'audio/x-mpegurl; charset=utf-8', buildPlaylistM3u(entry));
    } catch (err) {
      res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/playlists/backup/preview', requireUser, parseBackupBody, (req, res) => {
    try {
      const { backup, options } = readRestoreRequest(req);
      res.json({ ok: true, isAdmin: options.isAdmin, ...previewPlaylistBackup(ctx, backup, options) });
    } catch (err) {
      res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });

  app.post('/api/music/playlists/backup/restore', requireUser, parseBackupBody, (req, res) => {
    try {
      const { backup, options } = readRestoreRequest(req);
      res.json({ ok: true, ...restorePlaylistBackup(ctx, backup, options) });
    } catch (err) {
      res.status(Number(err?.status || 500)).json({ error: safeMessage(err) });
    }
  });
}
