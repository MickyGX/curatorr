import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const PLAYLIST_ARTWORK_DIR = path.join(DATA_DIR, 'playlist-artwork');
const PLAYLIST_ARTWORK_ROUTE_BASE = '/api/music/playlists/artwork';
const PLAYLIST_ARTWORK_MAX_BYTES = 5 * 1024 * 1024;
const PLAYLIST_ARTWORK_ALLOWED_MIME = new Set(['image/png', 'image/jpeg', 'image/webp']);

function slugify(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 48);
}

export function detectPlaylistArtworkMimeFromBuffer(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 12) return '';
  if (buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4e && buffer[3] === 0x47) return 'image/png';
  if (buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) return 'image/jpeg';
  if (buffer[0] === 0x52 && buffer[1] === 0x49 && buffer[2] === 0x46 && buffer[3] === 0x46
      && buffer[8] === 0x57 && buffer[9] === 0x45 && buffer[10] === 0x42 && buffer[11] === 0x50) return 'image/webp';
  return '';
}

export function parsePlaylistArtworkDataUrl(dataUrl) {
  const raw = String(dataUrl || '').trim();
  if (!raw) return { ok: false, error: 'Artwork image data is missing.' };
  const match = raw.match(/^data:(image\/[a-zA-Z0-9.+-]+);base64,([A-Za-z0-9+/=]+)$/);
  if (!match) return { ok: false, error: 'Artwork must be a valid PNG, JPG, or WEBP image.' };
  const requestedMime = String(match[1] || '').toLowerCase();
  const mime = requestedMime === 'image/jpg' ? 'image/jpeg' : requestedMime;
  if (!PLAYLIST_ARTWORK_ALLOWED_MIME.has(mime)) return { ok: false, error: 'Artwork type not allowed.' };
  let buffer;
  try {
    buffer = Buffer.from(String(match[2] || ''), 'base64');
  } catch (_err) {
    return { ok: false, error: 'Artwork image could not be decoded.' };
  }
  if (!buffer?.length) return { ok: false, error: 'Artwork image is empty.' };
  if (buffer.length > PLAYLIST_ARTWORK_MAX_BYTES) {
    return { ok: false, error: 'Artwork image is too large. Maximum size is 5 MB.' };
  }
  const decodedMime = detectPlaylistArtworkMimeFromBuffer(buffer);
  if (!decodedMime || decodedMime !== mime) return { ok: false, error: 'Artwork content does not match file type.' };
  const ext = mime === 'image/png' ? 'png' : (mime === 'image/webp' ? 'webp' : 'jpg');
  return { ok: true, mime, ext, buffer };
}

export function savePlaylistArtworkBuffer(buffer, ext, nameHint = '', prefix = 'playlist') {
  if (!Buffer.isBuffer(buffer) || !buffer.length) return '';
  const safeExt = String(ext || '').trim().toLowerCase();
  if (!['png', 'jpg', 'webp'].includes(safeExt)) return '';
  try {
    if (!fs.existsSync(PLAYLIST_ARTWORK_DIR)) fs.mkdirSync(PLAYLIST_ARTWORK_DIR, { recursive: true });
    const filename = `${prefix}-${slugify(nameHint) || 'artwork'}-${Date.now()}.${safeExt}`;
    fs.writeFileSync(path.join(PLAYLIST_ARTWORK_DIR, filename), buffer);
    return filename;
  } catch (_err) {
    return '';
  }
}

export function resolveStoredPlaylistArtworkPath(assetName) {
  const raw = String(assetName || '').trim();
  if (!raw) return '';
  const safe = path.basename(raw);
  if (!safe || safe !== raw) return '';
  return path.join(PLAYLIST_ARTWORK_DIR, safe);
}

export function buildStoredPlaylistArtworkUrl(assetName) {
  const safe = path.basename(String(assetName || '').trim());
  if (!safe) return '';
  return `${PLAYLIST_ARTWORK_ROUTE_BASE}/${encodeURIComponent(safe)}`;
}

export function getStoredPlaylistArtworkInfo(assetName) {
  const filePath = resolveStoredPlaylistArtworkPath(assetName);
  if (!filePath || !fs.existsSync(filePath)) return null;
  const ext = path.extname(filePath).toLowerCase();
  const mime = ext === '.png' ? 'image/png' : (ext === '.webp' ? 'image/webp' : 'image/jpeg');
  return {
    assetName: path.basename(filePath),
    filePath,
    mime,
    url: buildStoredPlaylistArtworkUrl(path.basename(filePath)),
  };
}
