import fs from 'fs';
import path from 'path';
import { initDb } from '../db.js';

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

function normalizeSourceKind(value) {
  return String(value || '').trim().toLowerCase() === 'automatic' ? 'automatic' : 'manual';
}

function loadConfig(configPath) {
  return JSON.parse(fs.readFileSync(configPath, 'utf8'));
}

function createLidarrClient(config) {
  const baseUrl = String(config?.lidarr?.localUrl || config?.lidarr?.url || '').replace(/\/+$/, '');
  const apiKey = String(config?.lidarr?.apiKey || '').trim();
  if (!baseUrl || !apiKey) throw new Error('Lidarr is not configured.');

  async function request(pathname, init = {}) {
    const response = await fetch(`${baseUrl}/api/v1${pathname}`, {
      ...init,
      headers: {
        'Content-Type': 'application/json',
        'X-Api-Key': apiKey,
        ...(init.headers || {}),
      },
    });
    if (!response.ok) {
      const body = await response.text().catch(() => '');
      throw new Error(`Lidarr request failed for ${pathname}: ${response.status} ${body}`);
    }
    if (response.status === 204) return null;
    return response.json();
  }

  async function listTags() {
    const list = await request('/tag', { method: 'GET' });
    return Array.isArray(list) ? list : [];
  }

  async function ensureTag(label) {
    const current = await listTags();
    const existing = current.find((tag) => String(tag?.label || '').trim().toLowerCase() === String(label || '').trim().toLowerCase());
    if (existing) return Number(existing.id || 0) || null;
    const created = await request('/tag', {
      method: 'POST',
      body: JSON.stringify({ label }),
    });
    return Number(created?.id || 0) || null;
  }

  async function addArtistTag(artistId, tagLabel) {
    const tagId = await ensureTag(tagLabel);
    if (!tagId) return false;
    await request('/artist/editor', {
      method: 'PUT',
      body: JSON.stringify({
        artistIds: [Number(artistId)],
        tags: [tagId],
        applyTags: 'add',
      }),
    });
    return true;
  }

  async function addAlbumTag(albumId, tagLabel) {
    const tagId = await ensureTag(tagLabel);
    if (!tagId) return false;
    const album = await request(`/album/${Number(albumId)}`, { method: 'GET' });
    if (!Object.prototype.hasOwnProperty.call(album, 'tags')) {
      const error = new Error('Lidarr album tags are not supported by this server version');
      error.code = 'LIDARR_ALBUM_TAGS_UNSUPPORTED';
      throw error;
    }
    try {
      await request('/album/editor', {
        method: 'PUT',
        body: JSON.stringify({
          albumIds: [Number(albumId)],
          tags: [tagId],
          applyTags: 'add',
        }),
      });
    } catch (error) {
      const message = String(error?.message || error);
      if (!message.includes('/album/editor: 404')) throw error;
      const existingTags = Array.isArray(album?.tags)
        ? album.tags.map((value) => Number(value || 0)).filter((value) => value > 0)
        : [];
      await request('/album', {
        method: 'PUT',
        body: JSON.stringify({
          ...album,
          tags: [...new Set([...existingTags, tagId])],
        }),
      });
    }
    return true;
  }

  return { addArtistTag, addAlbumTag };
}

function keyFor(userPlexId, artistName, albumTitle = '') {
  return `${String(userPlexId || '').trim()}::${String(artistName || '').trim().toLowerCase()}::${String(albumTitle || '').trim().toLowerCase()}`;
}

const dataDir = process.env.DATA_DIR || path.resolve(process.cwd(), 'data');
const configPath = process.env.CONFIG_PATH || path.resolve(process.cwd(), 'config', 'config.json');
const dbPath = path.join(dataDir, 'curatorr.db');

const config = loadConfig(configPath);
const db = initDb(dbPath);
const lidarr = createLidarrClient(config);

try {
  const completedRequests = db.prepare(`
    SELECT user_plex_id, artist_name, album_title, source_kind, lidarr_artist_id, lidarr_album_id, updated_at, created_at
    FROM lidarr_requests
    WHERE status = 'completed'
    ORDER BY updated_at DESC, created_at DESC, id DESC
  `).all();

  const sourceByArtist = new Map();
  const sourceByArtistAlbum = new Map();
  const artistIdByArtist = new Map();
  const albumIdByArtistAlbum = new Map();

  completedRequests.forEach((row) => {
    const artistKey = keyFor(row.user_plex_id, row.artist_name);
    if (!sourceByArtist.has(artistKey)) sourceByArtist.set(artistKey, normalizeSourceKind(row.source_kind));
    if (Number(row.lidarr_artist_id || 0) > 0 && !artistIdByArtist.has(artistKey)) {
      artistIdByArtist.set(artistKey, Number(row.lidarr_artist_id));
    }
    const albumTitle = String(row.album_title || '').trim();
    if (albumTitle) {
      const albumKey = keyFor(row.user_plex_id, row.artist_name, albumTitle);
      if (!sourceByArtistAlbum.has(albumKey)) sourceByArtistAlbum.set(albumKey, normalizeSourceKind(row.source_kind));
      if (Number(row.lidarr_album_id || 0) > 0 && !albumIdByArtistAlbum.has(albumKey)) {
        albumIdByArtistAlbum.set(albumKey, Number(row.lidarr_album_id));
      }
    }
  });

  db.prepare(`
    SELECT user_plex_id, artist_name, lidarr_artist_id
    FROM lidarr_artist_progress
    WHERE lidarr_artist_id IS NOT NULL AND lidarr_artist_id > 0
    ORDER BY updated_at DESC, id DESC
  `).all().forEach((row) => {
    const artistKey = keyFor(row.user_plex_id, row.artist_name);
    if (!artistIdByArtist.has(artistKey)) artistIdByArtist.set(artistKey, Number(row.lidarr_artist_id));
  });

  db.prepare(`
    SELECT user_plex_id, artist_name, album_title, lidarr_album_id
    FROM suggested_albums
    WHERE lidarr_album_id IS NOT NULL AND lidarr_album_id > 0
    ORDER BY updated_at DESC, id DESC
  `).all().forEach((row) => {
    const albumKey = keyFor(row.user_plex_id, row.artist_name, row.album_title);
    if (!albumIdByArtistAlbum.has(albumKey)) albumIdByArtistAlbum.set(albumKey, Number(row.lidarr_album_id));
  });

  const artistTasks = [...artistIdByArtist.entries()].map(([artistKey, artistId]) => {
    const sourceKind = sourceByArtist.get(artistKey) || 'automatic';
    return { artistKey, artistId, sourceKind };
  });

  const albumTasks = [...albumIdByArtistAlbum.entries()].map(([albumKey, albumId]) => {
    const sourceKind = sourceByArtistAlbum.get(albumKey) || sourceByArtist.get(albumKey.split('::').slice(0, 2).join('::')) || 'automatic';
    return { albumKey, albumId, sourceKind };
  });

  const summary = {
    artistsTagged: 0,
    albumsTagged: 0,
    albumUnsupported: 0,
    artistFailures: [],
    albumFailures: [],
  };

  for (const task of artistTasks) {
    try {
      await lidarr.addArtistTag(task.artistId, CURATORR_LIDARR_TAGS[task.sourceKind].artist);
      summary.artistsTagged += 1;
    } catch (error) {
      summary.artistFailures.push({ ...task, error: String(error?.message || error) });
    }
  }

  for (const task of albumTasks) {
    try {
      await lidarr.addAlbumTag(task.albumId, CURATORR_LIDARR_TAGS[task.sourceKind].album);
      summary.albumsTagged += 1;
    } catch (error) {
      if (error?.code === 'LIDARR_ALBUM_TAGS_UNSUPPORTED') {
        summary.albumUnsupported += 1;
      } else {
        summary.albumFailures.push({ ...task, error: String(error?.message || error) });
      }
    }
  }

  console.log(JSON.stringify(summary, null, 2));
} finally {
  db.close();
}
