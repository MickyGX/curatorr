import path from 'path';
import fs from 'fs';
import crypto from 'crypto';
import express from 'express';
import cookieSession from 'cookie-session';
import { exportJWK, calculateJwkThumbprint } from 'jose';
import { fileURLToPath } from 'url';
import { registerApiUtil } from './routes/api-util.js';
import { registerAuth } from './routes/auth.js';
import { registerApiPlex } from './routes/api-plex.js';
import { registerWizard, refreshMasterTrackCache } from './routes/wizard.js';
import { registerPages } from './routes/pages.js';
import { registerApiMusic } from './routes/api-music.js';
import { registerWebhooks } from './routes/webhooks.js';
import { registerSettings } from './routes/settings.js';
import { initDb, getUserPreferences, getAllUserIds } from './db.js';
import { createRecommendationService } from './services/recommendations.js';
import { createLidarrService, DEFAULT_LIDARR_AUTOMATION_SETTINGS } from './services/lidarr.js';
import { createPlaylistService } from './services/playlists.js';
import { createJobService } from './services/jobs.js';
import { rebuildSmartPlaylist } from './routes/api-music.js';
import { runTautulliDailySync } from './services/tautulli-sync.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

// ─── Environment ─────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 7676;
const BASE_URL = process.env.BASE_URL || `http://localhost:${PORT}`;
const CLIENT_ID = process.env.PLEX_CLIENT_ID || getOrCreatePlexClientId();
const PRODUCT = process.env.PLEX_PRODUCT || 'Curatorr';
const PLATFORM = process.env.PLEX_PLATFORM || 'Web';
const DEVICE_NAME = process.env.PLEX_DEVICE_NAME || 'Curatorr';
const SESSION_SECRET = process.env.SESSION_SECRET
  || (() => {
    console.warn('[security] SESSION_SECRET not set — generating random secret. Sessions will reset on restart. Set SESSION_SECRET for persistent sessions.');
    return crypto.randomBytes(32).toString('hex');
  })();
const LOCAL_AUTH_MIN_PASSWORD = 12;
const TRUST_PROXY_ENABLED = parseEnvFlag(process.env.TRUST_PROXY, false);
const TRUST_PROXY_HOPS = resolveProxyHopCount(process.env.TRUST_PROXY_HOPS, 1);
const TRUST_PROXY_SETTING = TRUST_PROXY_ENABLED ? TRUST_PROXY_HOPS : false;
const EMBED_ALLOWED_ORIGINS = resolveAllowedEmbedOrigins(process.env.EMBED_ALLOWED_ORIGINS || '');
const URLENCODED_BODY_LIMIT = String(process.env.URLENCODED_BODY_LIMIT || '8mb').trim() || '8mb';
const JSON_BODY_LIMIT = String(process.env.JSON_BODY_LIMIT || '4mb').trim() || '4mb';
const CONFIG_PATH = process.env.CONFIG_PATH || path.join(__dirname, '..', 'config', 'config.json');
const APP_VERSION = process.env.APP_VERSION || loadPackageVersion();
const ASSET_VERSION_BASE = normalizeVersionTag(APP_VERSION || '') || String(APP_VERSION || 'dev');
const ASSET_VERSION = `${ASSET_VERSION_BASE}-${String(process.env.ASSET_BUILD_ID || Date.now().toString(36))}`;
const ADMIN_USERS = parseCsv(process.env.ADMIN_USERS || '');
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const DB_PATH = process.env.DB_PATH || path.join(DATA_DIR, 'curatorr.db');
const PUBLIC_DIR = path.join(__dirname, '..', 'public');
const ICONS_DIR = path.join(PUBLIC_DIR, 'icons');
const USER_AVATAR_DIR = path.join(ICONS_DIR, 'custom', 'avatars');
const USER_AVATAR_BASE = '/icons/custom/avatars';
const MAX_USER_AVATAR_BYTES = 2 * 1024 * 1024;
const USER_AVATAR_ALLOWED_MIME = new Set(['image/png', 'image/jpeg', 'image/webp']);
const HTTP_ACCESS_LOGS = parseEnvFlag(process.env.HTTP_ACCESS_LOGS, true);
const HTTP_ACCESS_LOGS_SKIP_STATIC = parseEnvFlag(process.env.HTTP_ACCESS_LOGS_SKIP_STATIC, false);
const LOG_BUFFER = [];
const LOG_PATH = process.env.LOG_PATH || path.join(DATA_DIR, 'logs.json');
const VERSION_CACHE_TTL_MS = 10 * 60 * 1000;
let versionCache = { fetchedAt: 0, payload: null };

const DEFAULT_LOG_SETTINGS = { maxEntries: 500, maxDays: 30, visibleRows: 25 };

const ALLOWED_BRAND_THEMES = new Set(['custom', 'curatorr', 'pulsarr', 'plex', 'lidarr']);
const DEFAULT_THEME_SETTINGS = {
  mode: '',
  brandTheme: 'curatorr',
  customColor: '#8b5cf6',
  sidebarInvert: false,
  squareCorners: false,
  bgMotion: true,
  hideScrollbars: false,
};

const DEFAULT_SMART_PLAYLIST_SETTINGS = {
  skipThresholdSeconds: 20,
  songSkipLimit: 3,
  artistSkipLimit: 3,
  syncIntervalMinutes: 30,
  artistSkipRank: 2,
  artistBelterRank: 8,
  crescive: {
    favouriteArtistTrackPct: 0.80,
    favouriteGenreArtistPct: 0.80,
    favouriteGenreTrackPct:  0.20,
    otherGenreArtistPct:     0.20,
    otherGenreTrackPct:      0.20,
  },
  curative: {
    favouriteArtistTrackPct: 1.00,
    favouriteGenreArtistPct: 1.00,
    favouriteGenreTrackPct:  0.80,
    otherGenreArtistPct:     0.50,
    otherGenreTrackPct:      0.50,
  },
  additionRules: {
    belter:     { playedPct: 0.50, addCount: 15 },
    decent:     { playedPct: 0.80, addCount: 10 },
    halfDecent: { playedPct: 1.00, addCount:  5 },
  },
  subtractionRules: {
    skip: [
      { playedPct: 0.20, removeCount: 15 },
      { playedPct: 0.50, removeCount: 10 },
      { playedPct: 0.80, removeCount:  5 },
    ],
  },
};

const ROLE_ORDER = ['guest', 'user', 'power-user', 'co-admin', 'admin'];

// ─── Startup helpers ──────────────────────────────────────────────────────────

function parseEnvFlag(value, fallback = false) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'true' || raw === '1' || raw === 'yes') return true;
  if (raw === 'false' || raw === '0' || raw === 'no') return false;
  return fallback;
}

function resolveProxyHopCount(value, fallback = 1) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

function parseCsv(value) {
  return String(value || '').split(',').map((s) => s.trim()).filter(Boolean);
}

function normalizeHttpOrigin(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const url = new URL(raw);
    if (url.protocol !== 'http:' && url.protocol !== 'https:') return '';
    return url.origin;
  } catch (_err) {
    return '';
  }
}

function resolveAllowedEmbedOrigins(value) {
  const parts = String(value || '').split(/[\n,]/);
  return [...new Set(parts.map(normalizeHttpOrigin).filter(Boolean))];
}

function normalizeIdentityList(value) {
  const list = Array.isArray(value) ? value : String(value || '').split(/[\n,]/);
  return [...new Set(list.map((entry) => String(entry || '').trim()).filter(Boolean))];
}

function loadPackageVersion() {
  try {
    const raw = fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8');
    return JSON.parse(raw).version || '';
  } catch (err) { return ''; }
}

function normalizeVersionTag(value) {
  const raw = String(value || '').trim().replace(/^v/i, '');
  return raw ? `v${raw}` : '';
}

function isSecureEnv() {
  const proto = String(process.env.PROTOCOL || '').trim().toLowerCase();
  if (proto === 'https') return true;
  try { return new URL(BASE_URL).protocol === 'https:'; } catch (err) { return false; }
}

function getOrCreatePlexClientId() {
  // DATA_DIR may not exist yet at this point; use a temp dir
  const dir = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
  try {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const p = path.join(dir, 'plex_client_id');
    if (fs.existsSync(p)) {
      const existing = fs.readFileSync(p, 'utf8').trim();
      if (existing) return existing;
    }
    const id = crypto.randomUUID();
    fs.writeFileSync(p, id);
    return id;
  } catch (err) { return crypto.randomUUID(); }
}

function slugifyId(value) {
  return String(value || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 32);
}

function uniqueList(items) {
  return [...new Set((Array.isArray(items) ? items : []).filter(Boolean))];
}

function safeMessage(err) {
  if (!err) return 'Unknown error';
  const message = String(err.message || String(err) || '').trim();
  const cause = err && typeof err === 'object' ? err.cause : null;
  if (!cause || typeof cause !== 'object') return message || 'Unknown error';
  const parts = [message].filter(Boolean);
  if (cause.code) parts.push(`code=${cause.code}`);
  if (cause.address) parts.push(`address=${cause.address}`);
  if (cause.port) parts.push(`port=${cause.port}`);
  return parts.join(', ') || 'Unknown error';
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildAppApiUrl(baseUrl, suffixPath) {
  let normalized = String(baseUrl || '').trim().replace(/\/+$/, '');
  if (!normalized) return new URL('about:blank');
  if (!/^https?:\/\//i.test(normalized)) normalized = `http://${normalized}`;
  const suffix = String(suffixPath || '').replace(/^\/+/, '');
  return new URL(`${normalized}/${suffix}`);
}

function normalizeBasePath(value) {
  let raw = String(value || '').trim();
  if (!raw) return '';
  if (/^https?:\/\//i.test(raw)) {
    try { raw = String(new URL(raw).pathname || '').trim(); } catch (_) { raw = ''; }
  }
  if (!raw) return '';
  let p = raw.replace(/[#?].*$/, '').replace(/\/{2,}/g, '/').trim();
  if (!p) return '';
  if (!p.startsWith('/')) p = `/${p}`;
  p = p.replace(/\/+$/, '');
  return p === '/' ? '' : p;
}

function getCookieValue(header, name) {
  const source = String(header || '');
  const prefix = `${name}=`;
  for (const part of source.split(';')) {
    const trimmed = part.trim();
    if (trimmed.startsWith(prefix)) return decodeURIComponent(trimmed.slice(prefix.length));
  }
  return '';
}

function makeGlobalPlaylistId() {
  return 'gp_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
}

function normalizeBaseUrl(value, options = {}) {
  let raw = String(value || '').trim();
  if (!raw) return '';
  if (!/^https?:\/\//i.test(raw)) raw = `http://${raw}`;
  try {
    const parsed = new URL(raw);
    if (options.stripWeb) parsed.pathname = parsed.pathname.replace(/\/web\/?$/i, '');
    parsed.pathname = parsed.pathname.replace(/\/+$/, '');
    parsed.search = '';
    parsed.hash = '';
    return parsed.toString().replace(/\/$/, '');
  } catch (err) { return ''; }
}

function resolvePublicBaseUrl(req) {
  try {
    const proto = String(req?.headers?.['x-forwarded-proto'] || '').split(',')[0].trim() || req?.protocol || 'http';
    const host = String(req?.headers?.['x-forwarded-host'] || '').split(',')[0].trim() || req?.get?.('host') || '';
    if (host) return `${proto}://${host}`;
  } catch (err) { /* ignore */ }
  return BASE_URL;
}

// ─── Version ──────────────────────────────────────────────────────────────────

function buildReleaseNotesUrl(version) {
  const tag = normalizeVersionTag(version);
  return tag ? `https://github.com/MickyGX/curatorr/releases/tag/${tag}` : '';
}

function loadReleaseHighlights() { return []; }
async function fetchLatestDockerTag() { return ''; }

// ─── Logging ─────────────────────────────────────────────────────────────────

function applyLogRetention(entries, settings) {
  const maxEntries = settings?.maxEntries || DEFAULT_LOG_SETTINGS.maxEntries;
  const maxDays = settings?.maxDays || DEFAULT_LOG_SETTINGS.maxDays;
  const cutoff = Number.isFinite(maxDays) && maxDays > 0 ? Date.now() - (maxDays * 24 * 60 * 60 * 1000) : null;
  const filtered = Array.isArray(entries)
    ? entries.filter((e) => {
        if (!cutoff) return true;
        const ts = e?.ts ? Date.parse(e.ts) : NaN;
        return Number.isFinite(ts) ? ts >= cutoff : true;
      })
    : [];
  if (!Number.isFinite(maxEntries) || maxEntries <= 0) return filtered;
  return filtered.length <= maxEntries ? filtered : filtered.slice(filtered.length - maxEntries);
}

function persistLogsToDisk(settings) {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    const pruned = applyLogRetention(LOG_BUFFER, settings);
    const tmpPath = LOG_PATH + '.tmp';
    fs.writeFileSync(tmpPath, JSON.stringify({ items: pruned }, null, 2));
    fs.renameSync(tmpPath, LOG_PATH);
  } catch (err) { /* avoid crash on disk errors */ }
}

function loadLogsFromDisk(settings) {
  try {
    if (!fs.existsSync(LOG_PATH)) return;
    const parsed = JSON.parse(fs.readFileSync(LOG_PATH, 'utf8'));
    const items = Array.isArray(parsed?.items) ? parsed.items : [];
    const pruned = applyLogRetention(items, settings);
    LOG_BUFFER.splice(0, LOG_BUFFER.length, ...pruned);
  } catch (err) { /* ignore invalid log file */ }
}

function resolveLogSettings(config) {
  const raw = config?.logs && typeof config.logs === 'object' ? config.logs : {};
  const maxEntries = Number(raw.maxEntries);
  const maxDays = Number(raw.maxDays);
  const visibleRows = Number(raw.visibleRows);
  return {
    maxEntries: Number.isFinite(maxEntries) && maxEntries > 0 ? Math.floor(maxEntries) : DEFAULT_LOG_SETTINGS.maxEntries,
    maxDays: Number.isFinite(maxDays) && maxDays > 0 ? Math.floor(maxDays) : DEFAULT_LOG_SETTINGS.maxDays,
    visibleRows: Number.isFinite(visibleRows) && visibleRows > 0 ? Math.floor(visibleRows) : DEFAULT_LOG_SETTINGS.visibleRows,
  };
}

function pushLog(entry) {
  const settings = resolveLogSettings(loadConfig());
  const safeEntry = {
    ts: new Date().toISOString(),
    level: entry?.level || 'info',
    app: entry?.app || 'system',
    action: entry?.action || 'event',
    message: entry?.message || '',
    meta: entry?.meta || null,
  };
  LOG_BUFFER.push(safeEntry);
  const pruned = applyLogRetention(LOG_BUFFER, settings);
  if (pruned.length !== LOG_BUFFER.length) LOG_BUFFER.splice(0, LOG_BUFFER.length, ...pruned);
  persistLogsToDisk(settings);
}

// ─── HTTP access log ──────────────────────────────────────────────────────────

function httpAccessLogMiddleware(req, res, next) {
  if (!HTTP_ACCESS_LOGS) return next();
  const start = Date.now();
  res.on('finish', () => {
    if (HTTP_ACCESS_LOGS_SKIP_STATIC) {
      if (/\.(css|js|png|svg|ico|webp|jpg|jpeg|woff2?|ttf|map)$/i.test(req.path || '')) return;
    }
    console.log(`[http] ${req.method} ${req.path} ${res.statusCode} ${Date.now() - start}ms`);
  });
  next();
}

// ─── Request body size guard ──────────────────────────────────────────────────

function createRequestBodySizeGuard(maxBytes) {
  return (req, res, next) => {
    let bytes = 0;
    req.on('data', (chunk) => {
      bytes += chunk.length;
      if (bytes > maxBytes) { res.status(413).send('Request too large.'); req.destroy(); }
    });
    next();
  };
}

// ─── CSRF ─────────────────────────────────────────────────────────────────────

const CSRF_SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);
const CSRF_EXEMPT_PATHS = new Set(['/webhook/tautulli', '/webhook/plex', '/healthz', '/api/plex/pin']);

function csrfProtectionMiddleware(req, res, next) {
  if (CSRF_SAFE_METHODS.has(req.method)) return next();
  if (CSRF_EXEMPT_PATHS.has(req.path)) return next();
  const sessionToken = req.session?.csrfToken;
  const rawCsrf = req.body?._csrf;
  const headerToken = req.headers?.['x-csrf-token'] || (Array.isArray(rawCsrf) ? rawCsrf[0] : rawCsrf);
  if (!sessionToken || !headerToken || sessionToken !== headerToken) {
    return res.status(403).json({ error: 'Invalid CSRF token.' });
  }
  return next();
}

// ─── Config ───────────────────────────────────────────────────────────────────

const DEFAULT_JOBS_CONFIG = {
  masterTrackRefresh:  { intervalMinutes: 360, enabled: true },
  smartPlaylistSync:   { intervalMinutes: 30,  enabled: true },
  lidarrReviewArtists: { intervalMinutes: 30,  enabled: true },
  lidarrProcessQueue:  { intervalMinutes: 20,  enabled: true },
};

const DEFAULT_CONFIG = {
  wizard: { completed: false },
  plex: { url: '', token: '', machineId: '', libraries: [] },
  tautulli: { url: '', apiKey: '' },
  lidarr: { url: '', localUrl: '', remoteUrl: '', apiKey: '', ...DEFAULT_LIDARR_AUTOMATION_SETTINGS },
  smartPlaylist: { ...DEFAULT_SMART_PLAYLIST_SETTINGS },
  discovery: { lastfmApiKey: '', region: 'united states', showTrendingArtists: true, showTrendingTracks: true, showSimilarArtists: true },
  filters: { mustIncludeArtists: [], neverIncludeArtists: [] },
  general: { serverName: 'Curatorr', remoteUrl: '', localUrl: '', basePath: '', restrictGuests: false },
  users: [],
  theme: {},
  logs: { ...DEFAULT_LOG_SETTINGS },
  jobs: { ...DEFAULT_JOBS_CONFIG },
  globalPlaylists: [],
};

function loadConfig() {
  try {
    if (!fs.existsSync(CONFIG_PATH)) {
      const dir = path.dirname(CONFIG_PATH);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(CONFIG_PATH, JSON.stringify(DEFAULT_CONFIG, null, 2));
      return { ...DEFAULT_CONFIG };
    }
    const parsed = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
    return { ...DEFAULT_CONFIG, ...parsed };
  } catch (err) { return { ...DEFAULT_CONFIG }; }
}

function saveConfig(config) {
  const dir = path.dirname(CONFIG_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const tmpPath = CONFIG_PATH + '.tmp';
  fs.writeFileSync(tmpPath, JSON.stringify(config, null, 2));
  fs.renameSync(tmpPath, CONFIG_PATH);
}

// ─── User management (local) ─────────────────────────────────────────────────

function normalizeUserKey(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeLocalRole(value, fallback = 'user') {
  const role = String(value || '').trim().toLowerCase();
  if (['admin', 'co-admin', 'power-user', 'user', 'guest', 'disabled'].includes(role)) return role;
  const fb = String(fallback || '').trim().toLowerCase();
  return ['admin', 'co-admin', 'power-user', 'user', 'guest', 'disabled'].includes(fb) ? fb : 'user';
}

function normalizeStoredAvatarPath(value) {
  const avatar = String(value || '').trim();
  if (!avatar) return '';
  if (avatar.startsWith('/icons/') || avatar.startsWith('http://') || avatar.startsWith('https://')) return avatar;
  return '';
}

function resolveLocalAvatarFallback(user) {
  const isSetup = Boolean(user?.isSetupAdmin || user?.setupAccount === true || String(user?.createdBy || '').toLowerCase() === 'setup');
  return isSetup ? '/icons/role.svg' : '/icons/user-profile.svg';
}

function resolveSetupAdminUserKey(users) {
  if (!Array.isArray(users) || !users.length) return '';
  const explicit = users.find((u) => u?.setupAccount === true || u?.createdBy === 'setup');
  if (explicit) return normalizeUserKey(explicit.username || explicit.email || '');
  const admins = users.filter((u) => u?.role === 'admin');
  if (!admins.length) return '';
  const sorted = admins.slice().sort((a, b) => {
    const aT = Date.parse(String(a?.createdAt || ''));
    const bT = Date.parse(String(b?.createdAt || ''));
    const sA = Number.isNaN(aT) ? Number.MAX_SAFE_INTEGER : aT;
    const sB = Number.isNaN(bT) ? Number.MAX_SAFE_INTEGER : bT;
    return sA !== sB ? sA - sB : String(a?.username || '').localeCompare(String(b?.username || ''));
  });
  return normalizeUserKey(sorted[0]?.username || sorted[0]?.email || '');
}

function normalizeLocalUsers(items) {
  if (!Array.isArray(items)) return [];
  const normalized = items.map((entry) => {
    if (!entry || typeof entry !== 'object') return null;
    const username = String(entry.username || '').trim();
    const passwordHash = String(entry.passwordHash || '').trim();
    const salt = String(entry.salt || '').trim();
    if (!username || !passwordHash || !salt) return null;
    const createdBy = String(entry.createdBy || '').toLowerCase() === 'setup' ? 'setup' : 'system';
    return {
      username, email: String(entry.email || '').trim(),
      role: normalizeLocalRole(entry.role, 'admin'),
      passwordHash, salt,
      avatar: normalizeStoredAvatarPath(entry.avatar || ''),
      createdBy, setupAccount: entry.setupAccount === true || createdBy === 'setup',
      systemCreated: entry.systemCreated !== false,
      createdAt: entry.createdAt ? String(entry.createdAt) : new Date().toISOString(),
    };
  }).filter(Boolean);

  const setupAdminKey = resolveSetupAdminUserKey(normalized);
  return normalized.map((entry) => {
    const isSetupAdmin = Boolean(setupAdminKey && normalizeUserKey(entry.username) === setupAdminKey);
    return { ...entry, isSetupAdmin, avatarFallback: resolveLocalAvatarFallback({ ...entry, isSetupAdmin }) };
  });
}

function resolveLocalUsers(config) { return normalizeLocalUsers(config?.users); }

function serializeLocalUsers(users) {
  if (!Array.isArray(users)) return [];
  return users.map((entry) => {
    if (!entry || typeof entry !== 'object') return null;
    const username = String(entry.username || '').trim();
    const passwordHash = String(entry.passwordHash || '').trim();
    const salt = String(entry.salt || '').trim();
    if (!username || !passwordHash || !salt) return null;
    return {
      username, email: String(entry.email || '').trim(),
      role: normalizeLocalRole(entry.role, 'user'),
      passwordHash, salt,
      avatar: normalizeStoredAvatarPath(entry.avatar || ''),
      createdBy: String(entry.createdBy || '').toLowerCase() === 'setup' ? 'setup' : 'system',
      setupAccount: entry.setupAccount === true,
      systemCreated: entry.systemCreated !== false,
      createdAt: entry.createdAt ? String(entry.createdAt) : new Date().toISOString(),
    };
  }).filter(Boolean);
}

function hasLocalAdmin(config) { return resolveLocalUsers(config).some((u) => u.role === 'admin'); }

function roleRank(role) {
  const key = normalizeLocalRole(role, 'guest');
  const index = ROLE_ORDER.indexOf(key);
  return index >= 0 ? index : 0;
}

function findLocalUserIndex(users, identity = {}) {
  const un = normalizeUserKey(identity.username || '');
  const em = normalizeUserKey(identity.email || '');
  if (!Array.isArray(users) || !users.length) return -1;
  return users.findIndex((entry) => {
    const eu = normalizeUserKey(entry?.username || '');
    const ee = normalizeUserKey(entry?.email || '');
    return (un && eu === un) || (em && ee && ee === em);
  });
}

function isValidEmail(value) { return String(value || '').trim().includes('@'); }

// ─── Password ─────────────────────────────────────────────────────────────────

function hashPassword(password, salt) {
  return crypto.scryptSync(String(password || ''), salt, 64).toString('hex');
}

function verifyPassword(password, user) {
  if (!user?.passwordHash || !user?.salt) return false;
  try {
    return crypto.timingSafeEqual(
      Buffer.from(hashPassword(password, user.salt), 'hex'),
      Buffer.from(user.passwordHash, 'hex'),
    );
  } catch (err) { return false; }
}

function validateLocalPasswordStrength(password) {
  const pw = String(password || '');
  if (pw.length < LOCAL_AUTH_MIN_PASSWORD) return `Password must be at least ${LOCAL_AUTH_MIN_PASSWORD} characters.`;
  if (!/[A-Z]/.test(pw)) return 'Password must contain at least one uppercase letter.';
  if (!/[a-z]/.test(pw)) return 'Password must contain at least one lowercase letter.';
  if (!/[0-9]/.test(pw)) return 'Password must contain at least one number.';
  return null;
}

// ─── Session helpers ──────────────────────────────────────────────────────────

function setSessionUser(req, user, source = 'local') {
  const src = String(source || '').trim().toLowerCase() || 'local';
  req.session.user = {
    username: user.username,
    email: user.email || '',
    avatar: normalizeStoredAvatarPath(user?.avatar || ''),
    avatarFallback: src === 'local' ? resolveLocalAvatarFallback(user) : '/icons/user-profile.svg',
    role: user.role || 'admin',
    source: src,
  };
  req.session.viewRole = null;
}

function getActualRole(req) {
  return String(req.session?.user?.role || '').trim().toLowerCase() || 'guest';
}

function getEffectiveRole(req) {
  const viewRole = String(req.session?.viewRole || '').trim().toLowerCase();
  const actualRole = getActualRole(req);
  return (actualRole === 'admin' && viewRole) ? viewRole : actualRole;
}

function resolveRoleSwitchRedirectPath(req, _targetRole, _opts) {
  const referer = String(req.headers?.referer || '').trim();
  if (referer) {
    try {
      const url = new URL(referer);
      if (url.pathname.startsWith('/')) return url.pathname + url.search;
    } catch (err) { /* ignore */ }
  }
  return '/dashboard';
}

// ─── User login tracking ──────────────────────────────────────────────────────

function resolveUserLogins(config) {
  const raw = config?.userLogins && typeof config.userLogins === 'object' ? config.userLogins : {};
  return {
    plex: (typeof raw.plex === 'object' ? raw.plex : {}),
    curatorr: (typeof raw.curatorr === 'object' ? raw.curatorr : {}),
  };
}

function updateUserLogins(config, { identifier, plex, curatorr: appLogin }) {
  const key = normalizeUserKey(identifier);
  if (!key) return config;
  const store = resolveUserLogins(config);
  const now = new Date().toISOString();
  const next = { plex: { ...store.plex }, curatorr: { ...store.curatorr } };
  if (plex) next.plex[key] = now;
  if (appLogin) next.curatorr[key] = now;
  return { ...config, userLogins: next };
}

// ─── Plex admins ──────────────────────────────────────────────────────────────

function loadAdmins() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const p = path.join(DATA_DIR, 'admins.json');
  if (fs.existsSync(p)) {
    try {
      const data = JSON.parse(fs.readFileSync(p, 'utf8'));
      if (Array.isArray(data.admins)) return data.admins;
    } catch (err) { return []; }
  }
  if (ADMIN_USERS.length) { saveAdmins(ADMIN_USERS); return ADMIN_USERS; }
  return [];
}

function saveAdmins(admins) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(path.join(DATA_DIR, 'admins.json'), JSON.stringify({ admins }, null, 2));
}

function loadCoAdmins() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const p = path.join(DATA_DIR, 'coadmins.json');
  if (fs.existsSync(p)) {
    try {
      const data = JSON.parse(fs.readFileSync(p, 'utf8'));
      if (Array.isArray(data.coAdmins)) return data.coAdmins;
    } catch (err) { return []; }
  }
  return [];
}

function saveCoAdmins(coAdmins) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(path.join(DATA_DIR, 'coadmins.json'), JSON.stringify({ coAdmins }, null, 2));
}

function loadPowerUsers() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const p = path.join(DATA_DIR, 'powerusers.json');
  if (fs.existsSync(p)) {
    try {
      const data = JSON.parse(fs.readFileSync(p, 'utf8'));
      if (Array.isArray(data.powerUsers)) return data.powerUsers;
    } catch (err) { return []; }
  }
  return [];
}

function savePowerUsers(powerUsers) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(path.join(DATA_DIR, 'powerusers.json'), JSON.stringify({ powerUsers }, null, 2));
}

function loadGuestUsers() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const p = path.join(DATA_DIR, 'guestusers.json');
  if (fs.existsSync(p)) {
    try {
      const data = JSON.parse(fs.readFileSync(p, 'utf8'));
      if (Array.isArray(data.guests)) return data.guests;
    } catch (err) { return []; }
  }
  return [];
}

function saveGuestUsers(guests) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(path.join(DATA_DIR, 'guestusers.json'), JSON.stringify({ guests }, null, 2));
}

function loadDisabledUsers() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const p = path.join(DATA_DIR, 'disabledusers.json');
  if (fs.existsSync(p)) {
    try {
      const data = JSON.parse(fs.readFileSync(p, 'utf8'));
      if (Array.isArray(data.disabledUsers)) return data.disabledUsers;
    } catch (err) { return []; }
  }
  return [];
}

function saveDisabledUsers(disabledUsers) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(path.join(DATA_DIR, 'disabledusers.json'), JSON.stringify({ disabledUsers }, null, 2));
}

function matchesAdminList(list, identifiers) {
  const normalized = list.map((v) => v.toLowerCase());
  return identifiers.some((v) => normalized.includes(String(v || '').toLowerCase()));
}

function resolveRole(plexUser) {
  const ids = [plexUser.username, plexUser.email, plexUser.title].filter(Boolean);
  if (matchesAdminList(loadDisabledUsers(), ids)) return 'disabled';
  if (matchesAdminList(loadAdmins(), ids)) return 'admin';
  if (matchesAdminList(loadCoAdmins(), ids)) return 'co-admin';
  if (matchesAdminList(loadPowerUsers(), ids)) return 'power-user';
  if (matchesAdminList(loadGuestUsers(), ids)) return 'guest';
  if (!loadAdmins().length) {
    const key = ids[0];
    if (key) { saveAdmins([key]); return 'admin'; }
  }
  return 'user';
}

function normalizeLidarrAutomationScope(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'global') return 'global';
  if (raw === 'per-user' || raw === 'per_user' || raw === 'user') return 'per-user';
  return 'off';
}

function resolveLidarrAutomationSettings(config) {
  const source = (config?.lidarr && typeof config.lidarr === 'object') ? config.lidarr : {};
  const enabledUsers = normalizeIdentityList(source.enabledUsers || []);
  const normalizeRoleQuotas = (value = {}) => {
    const quotas = value && typeof value === 'object' ? value : {};
    return {
      admin: {
        ...DEFAULT_LIDARR_AUTOMATION_SETTINGS.roleQuotas.admin,
        ...(quotas.admin && typeof quotas.admin === 'object' ? quotas.admin : {}),
      },
      'co-admin': {
        ...DEFAULT_LIDARR_AUTOMATION_SETTINGS.roleQuotas['co-admin'],
        ...(quotas['co-admin'] && typeof quotas['co-admin'] === 'object' ? quotas['co-admin'] : {}),
      },
      'power-user': {
        ...DEFAULT_LIDARR_AUTOMATION_SETTINGS.roleQuotas['power-user'],
        ...(quotas['power-user'] && typeof quotas['power-user'] === 'object' ? quotas['power-user'] : {}),
      },
      user: {
        ...DEFAULT_LIDARR_AUTOMATION_SETTINGS.roleQuotas.user,
        ...(quotas.user && typeof quotas.user === 'object' ? quotas.user : {}),
      },
    };
  };
  let automationScope = source.automationEnabled === false
    ? 'off'
    : normalizeLidarrAutomationScope(source.automationScope);
  if (automationScope === 'per-user') automationScope = 'global';
  const automationEnabled = Boolean(source.automationEnabled) && automationScope !== 'off';
  return {
    ...DEFAULT_LIDARR_AUTOMATION_SETTINGS,
    ...source,
    automationEnabled,
    automationScope: automationEnabled ? automationScope : 'off',
    enabledUsers,
    roleQuotas: normalizeRoleQuotas(source.roleQuotas),
  };
}

function resolveUserIdentifiers(user) {
  return normalizeIdentityList([
    user?.username,
    user?.email,
    user?.plexId,
  ]).map((entry) => entry.toLowerCase());
}

function userHasOwnPlexToken(config, userOrId) {
  const source = (config?.plex && typeof config.plex === 'object') ? config.plex : {};
  const tokenMap = source.userServerTokens && typeof source.userServerTokens === 'object' ? source.userServerTokens : {};
  const ids = typeof userOrId === 'string'
    ? normalizeIdentityList([userOrId]).map((e) => e.toLowerCase())
    : resolveUserIdentifiers(userOrId);
  return ids.some((id) => String(tokenMap[id] || '').trim() !== '');
}

function resolveUserPlexServerToken(config, userOrId, fallbackToken = '') {
  const source = (config?.plex && typeof config.plex === 'object') ? config.plex : {};
  const tokenMap = source.userServerTokens && typeof source.userServerTokens === 'object'
    ? source.userServerTokens
    : {};
  const ids = typeof userOrId === 'string'
    ? normalizeIdentityList([userOrId]).map((entry) => entry.toLowerCase())
    : resolveUserIdentifiers(userOrId);
  for (const id of ids) {
    const token = String(tokenMap[id] || '').trim();
    if (token) return token;
  }
  return String(fallbackToken || source.token || '').trim();
}

function canUserAccessLidarrAutomation(config, user) {
  if (!user || typeof user !== 'object') return false;
  const role = normalizeLocalRole(user.role, 'guest');
  if (!['admin', 'co-admin', 'power-user'].includes(role)) return false;
  const lidarr = resolveLidarrAutomationSettings(config);
  if (!lidarr.automationEnabled || lidarr.automationScope === 'off') return false;
  if (role === 'admin' || role === 'co-admin') return true;
  if (lidarr.automationScope === 'global') return true;
  const enabled = new Set((lidarr.enabledUsers || []).map((entry) => entry.toLowerCase()));
  return resolveUserIdentifiers(user).some((entry) => enabled.has(entry));
}

// ─── Auth middleware ──────────────────────────────────────────────────────────

function requireUser(req, res, next) {
  if (!req.session?.user) {
    if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'Authentication required.' });
    return res.redirect(`/login?next=${encodeURIComponent(req.originalUrl)}`);
  }
  if (getActualRole(req) === 'disabled') {
    if (req.path.startsWith('/api/')) return res.status(403).json({ error: 'Account disabled.' });
    return res.status(403).send('Account disabled.');
  }
  const cfg = loadConfig();
  if (cfg.general?.restrictGuests && getActualRole(req) === 'guest') {
    if (req.path.startsWith('/api/')) return res.status(403).json({ error: 'Guest access restricted.' });
    return res.redirect('/login?restricted=1');
  }
  return next();
}

function requireAdmin(req, res, next) {
  if (!req.session?.user) {
    if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'Authentication required.' });
    return res.redirect(`/login?next=${encodeURIComponent(req.originalUrl)}`);
  }
  if (getActualRole(req) !== 'admin') {
    if (req.path.startsWith('/api/')) return res.status(403).json({ error: 'Admin access required.' });
    return res.status(403).send('Admin access required.');
  }
  return next();
}

function requireSettingsAdmin(req, res, next) {
  if (!req.session?.user) {
    if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'Authentication required.' });
    return res.redirect(`/login?next=${encodeURIComponent(req.originalUrl)}`);
  }
  const role = getActualRole(req);
  if (role !== 'admin' && role !== 'co-admin') {
    if (req.path.startsWith('/api/')) return res.status(403).json({ error: 'Admin access required.' });
    return res.status(403).send('Admin access required.');
  }
  return next();
}

function requireActualAdmin(req, res, next) { return requireAdmin(req, res, next); }

function requireWizardComplete(_req, res, next) {
  const config = loadConfig();
  if (!config.wizard?.completed) return res.redirect('/wizard');
  return next();
}

// ─── Theme ────────────────────────────────────────────────────────────────────

function normalizeThemeSettings(raw, fallback = DEFAULT_THEME_SETTINGS) {
  const source = (raw && typeof raw === 'object') ? raw : {};
  const fb = (fallback && typeof fallback === 'object') ? fallback : DEFAULT_THEME_SETTINGS;
  const brandTheme = String(source.brandTheme || fb.brandTheme || 'curatorr').trim().toLowerCase();
  return {
    mode: String(source.mode || fb.mode || '').trim().toLowerCase() || '',
    brandTheme: ALLOWED_BRAND_THEMES.has(brandTheme) ? brandTheme : (fb.brandTheme || 'curatorr'),
    customColor: String(source.customColor || fb.customColor || '#8b5cf6').trim(),
    sidebarInvert: source.sidebarInvert !== undefined ? Boolean(source.sidebarInvert) : Boolean(fb.sidebarInvert),
    squareCorners: source.squareCorners !== undefined ? Boolean(source.squareCorners) : Boolean(fb.squareCorners),
    bgMotion: source.bgMotion !== undefined ? Boolean(source.bgMotion) : Boolean(fb.bgMotion !== false),
    hideScrollbars: source.hideScrollbars !== undefined ? Boolean(source.hideScrollbars) : Boolean(fb.hideScrollbars),
  };
}

function resolveThemeDefaults(config) {
  return normalizeThemeSettings(config?.theme, DEFAULT_THEME_SETTINGS);
}

function resolveThemePreferenceKey(user) {
  const source = String(user?.source || '').toLowerCase() === 'plex' ? 'plex' : 'local';
  const un = normalizeUserKey(user?.username || '');
  const em = normalizeUserKey(user?.email || '');
  const identity = source === 'plex' ? (em || un) : (un || em);
  return identity ? `${source}:${identity}` : '';
}

function resolveUserThemePreferences(config, defaults) {
  const prefs = config?.themePreferences;
  if (!prefs || typeof prefs !== 'object') return {};
  const result = {};
  Object.entries(prefs).forEach(([key, val]) => {
    const k = String(key || '').trim().toLowerCase();
    if (k) result[k] = normalizeThemeSettings(val, defaults);
  });
  return result;
}

function resolveThemeSettingsForUser(config, user) {
  const defaults = resolveThemeDefaults(config);
  const key = resolveThemePreferenceKey(user);
  if (!key) return defaults;
  const prefs = resolveUserThemePreferences(config, defaults);
  return prefs[key] ? normalizeThemeSettings(prefs[key], defaults) : defaults;
}

function serializeUserThemePreferences(config, user, settings) {
  const key = resolveThemePreferenceKey(user);
  if (!key) return config;
  const prefs = config?.themePreferences && typeof config.themePreferences === 'object'
    ? { ...config.themePreferences } : {};
  prefs[key] = normalizeThemeSettings(settings, resolveThemeDefaults(config));
  return { ...config, themePreferences: prefs };
}

// ─── Avatar helpers ───────────────────────────────────────────────────────────

function detectAvatarMimeFromBuffer(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 12) return '';
  if (buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4E && buffer[3] === 0x47) return 'image/png';
  if (buffer[0] === 0xFF && buffer[1] === 0xD8 && buffer[2] === 0xFF) return 'image/jpeg';
  if (buffer[0] === 0x52 && buffer[1] === 0x49 && buffer[2] === 0x46 && buffer[3] === 0x46
      && buffer[8] === 0x57 && buffer[9] === 0x45 && buffer[10] === 0x42 && buffer[11] === 0x50) return 'image/webp';
  return '';
}

function parseUserAvatarDataUrl(dataUrl) {
  const raw = String(dataUrl || '').trim();
  if (!raw) return { ok: false, error: 'Avatar image data is missing.' };
  const match = raw.match(/^data:(image\/[a-zA-Z0-9.+-]+);base64,([A-Za-z0-9+/=]+)$/);
  if (!match) return { ok: false, error: 'Avatar must be a valid PNG, JPG, or WEBP image.' };
  const requestedMime = String(match[1] || '').toLowerCase();
  const mime = requestedMime === 'image/jpg' ? 'image/jpeg' : requestedMime;
  if (!USER_AVATAR_ALLOWED_MIME.has(mime)) return { ok: false, error: 'Avatar type not allowed.' };
  let buffer;
  try { buffer = Buffer.from(String(match[2] || ''), 'base64'); } catch (e) { return { ok: false, error: 'Avatar could not be decoded.' }; }
  if (!buffer?.length) return { ok: false, error: 'Avatar image is empty.' };
  if (buffer.length > MAX_USER_AVATAR_BYTES) return { ok: false, error: 'Avatar image is too large. Maximum size is 2 MB.' };
  const decodedMime = detectAvatarMimeFromBuffer(buffer);
  if (!decodedMime || decodedMime !== mime) return { ok: false, error: 'Avatar content does not match file type.' };
  const ext = mime === 'image/png' ? 'png' : (mime === 'image/webp' ? 'webp' : 'jpg');
  return { ok: true, mime, ext, buffer };
}

function saveCustomUserAvatar(buffer, ext, nameHint = '') {
  if (!Buffer.isBuffer(buffer) || !buffer.length) return '';
  const safeExt = String(ext || '').toLowerCase();
  if (!['png', 'jpg', 'webp'].includes(safeExt)) return '';
  try {
    if (!fs.existsSync(USER_AVATAR_DIR)) fs.mkdirSync(USER_AVATAR_DIR, { recursive: true });
    const filename = `${slugifyId(nameHint) || 'avatar'}-${Date.now()}.${safeExt}`;
    fs.writeFileSync(path.join(USER_AVATAR_DIR, filename), buffer);
    return `${USER_AVATAR_BASE}/${filename}`;
  } catch (err) { return ''; }
}

// ─── Plex OAuth ───────────────────────────────────────────────────────────────

function plexHeaders() {
  return {
    'X-Plex-Client-Identifier': CLIENT_ID,
    'X-Plex-Product': PRODUCT,
    'X-Plex-Platform': PLATFORM,
    'X-Plex-Device': PLATFORM,
    'X-Plex-Device-Name': DEVICE_NAME,
  };
}

async function exchangePin(pinId) {
  const res = await fetch(`https://plex.tv/api/v2/pins/${pinId}`, {
    method: 'GET',
    headers: { Accept: 'application/json', ...plexHeaders() },
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`PIN exchange failed (${res.status}): ${text}`);
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch (err) { throw new Error(`PIN exchange JSON parse failed: ${text.slice(0, 180)}`); }
  return data.authToken || null;
}

async function exchangePinWithRetry(pinId, attempts = 20, delayMs = 1000) {
  let lastError = '';
  for (let i = 0; i < attempts; i += 1) {
    try {
      const token = await exchangePin(pinId);
      if (token) return { token, attempts: i + 1, error: '' };
    } catch (err) { lastError = safeMessage(err) || ''; }
    await sleep(delayMs);
  }
  return { token: null, attempts, error: lastError };
}

async function fetchPlexUser(token) {
  const res = await fetch('https://plex.tv/api/v2/user', {
    method: 'GET',
    headers: { Accept: 'application/json', 'X-Plex-Token': token, ...plexHeaders() },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Plex user lookup failed (${res.status}): ${text}`);
  }
  return res.json();
}

async function fetchPlexResources(token) {
  const res = await fetch('https://plex.tv/api/v2/resources?includeHttps=1&includeRelay=1', {
    method: 'GET',
    headers: { Accept: 'application/json', 'X-Plex-Token': token, ...plexHeaders() },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Plex resources request failed (${res.status}): ${text.slice(0, 180)}`);
  }
  return res.json();
}

function resolvePlexServerResource(resources, { machineId, plexUrl }) {
  const list = Array.isArray(resources)
    ? resources
    : (resources?.MediaContainer?.Device || resources?.mediaContainer?.Device || []);
  const servers = (Array.isArray(list) ? list : []).filter((item) => String(item?.provides || '').includes('server'));
  const machine = String(machineId || '').trim();
  if (machine) {
    const match = servers.find((item) => String(item?.clientIdentifier || item?.clientidentifier || '').trim() === machine);
    if (match) return match;
  }
  if (plexUrl) {
    try {
      const host = new URL(plexUrl).hostname.toLowerCase();
      const match = servers.find((server) => {
        const conns = Array.isArray(server?.connections) ? server.connections : (Array.isArray(server?.Connection) ? server.Connection : []);
        return conns.some((c) => { try { return new URL(c?.uri || '').hostname.toLowerCase() === host; } catch (e) { return false; } });
      });
      if (match) return match;
    } catch (err) { /* ignore */ }
  }
  return servers.length === 1 ? servers[0] : null;
}

function resolvePlexServerToken(resources, opts) {
  return resolvePlexServerResource(resources, opts)?.accessToken || '';
}

function resolvePlexMachineIdentifier(resources, opts) {
  const server = resolvePlexServerResource(resources, opts);
  return String(server?.clientIdentifier || server?.clientidentifier || '').trim();
}

function isPlexServerOwner(server) {
  if (!server) return false;
  const value = server?.owned ?? server?.owner ?? server?.isOwner;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value === 1;
  if (typeof value === 'string') return value.toLowerCase() === 'true' || value === '1';
  return false;
}

function parsePlexUsers(xmlText, options = {}) {
  const machineId = String(options.machineId || '').trim();
  const users = [];
  const blocks = String(xmlText || '').match(/<User\b[^>]*>[\s\S]*?<\/User>/g) || [];
  blocks.forEach((block) => {
    const tagMatch = block.match(/<User\b[^>]*>/);
    if (!tagMatch) return;
    const attrs = {};
    tagMatch[0].replace(/(\w+)="([^"]*)"/g, (_m, k, v) => { attrs[k] = v; return ''; });
    const serverTags = block.match(/<Server\b[^>]*>/g) || [];
    const servers = serverTags.map((tag) => {
      const sa = {};
      tag.replace(/(\w+)="([^"]*)"/g, (_m, k, v) => { sa[k] = v; return ''; });
      return sa;
    });
    let serverMatch = machineId ? servers.find((s) => String(s.machineIdentifier || '') === machineId) : null;
    if (!serverMatch) serverMatch = servers.find((s) => String(s.owned || '') === '1') || servers[0] || null;
    users.push({
      id: attrs.id || attrs.uuid || '',
      uuid: attrs.uuid || '',
      username: attrs.username || '',
      email: attrs.email || '',
      title: attrs.title || '',
      lastSeenAt: serverMatch?.lastSeenAt || '',
    });
  });
  return users;
}

async function completePlexLogin(req, authToken) {
  const plexUser = await fetchPlexUser(authToken);
  if (!plexUser) throw new Error('Could not fetch Plex user info.');

  const role = resolveRole(plexUser);
  if (role === 'disabled') throw new Error('This Plex account is disabled.');
  const loginIdentifier = plexUser.email || plexUser.username || plexUser.title || plexUser.id || '';

  req.session.user = {
    username: plexUser.username || plexUser.title || loginIdentifier,
    email: plexUser.email || '',
    avatar: plexUser.thumb || '',
    avatarFallback: '/icons/user-profile.svg',
    role,
    source: 'plex',
    plexId: String(plexUser.id || '').trim(),
  };
  req.session.viewRole = null;
  req.session.pinId = null;

  let config = loadConfig();
  config = updateUserLogins(config, { identifier: loginIdentifier, plex: true });

  try {
    const resources = await fetchPlexResources(authToken);
    const plexCfg = config.plex || {};
    const serverToken = resolvePlexServerToken(resources, { machineId: plexCfg.machineId || '', plexUrl: plexCfg.url || '' });
    if (serverToken) {
      req.session.plexServerToken = serverToken;
      const nextUserServerTokens = {
        ...(plexCfg.userServerTokens && typeof plexCfg.userServerTokens === 'object' ? plexCfg.userServerTokens : {}),
      };
      for (const id of resolveUserIdentifiers(req.session.user)) nextUserServerTokens[id] = serverToken;
      config = {
        ...config,
        plex: {
          ...plexCfg,
          ...(role === 'admin' && serverToken !== plexCfg.token ? { token: serverToken } : {}),
          userServerTokens: nextUserServerTokens,
        },
      };
    }
  } catch (err) { /* non-fatal */ }

  saveConfig(config);
  pushLog({ level: 'info', app: 'plex', action: 'login.success', message: 'Plex login successful.', meta: { user: req.session.user.username, role } });
}

async function ensureKeypair() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const privatePath = path.join(DATA_DIR, 'plex_private.pem');
  const publicPath = path.join(DATA_DIR, 'plex_public.json');
  if (fs.existsSync(privatePath) && fs.existsSync(publicPath)) {
    const privatePem = fs.readFileSync(privatePath, 'utf8');
    const publicBundle = JSON.parse(fs.readFileSync(publicPath, 'utf8'));
    return { privatePem, publicJwk: publicBundle.jwk, kid: publicBundle.kid };
  }
  const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
  const privatePem = privateKey.export({ format: 'pem', type: 'pkcs8' });
  const publicJwk = await exportJWK(publicKey);
  publicJwk.alg = 'EdDSA';
  const kid = await calculateJwkThumbprint(publicJwk);
  publicJwk.kid = kid;
  fs.writeFileSync(privatePath, privatePem);
  fs.writeFileSync(publicPath, JSON.stringify({ jwk: publicJwk, kid }, null, 2));
  return { privatePem, publicJwk, kid };
}

// ─── Plex library helpers ─────────────────────────────────────────────────────

async function fetchPlexLibraries(plexUrl, token) {
  const url = buildAppApiUrl(plexUrl, 'library/sections');
  url.searchParams.set('X-Plex-Token', token);
  const res = await fetch(url.toString(), { headers: { Accept: 'application/json' } });
  if (!res.ok) throw new Error(`Plex library fetch failed (${res.status})`);
  const json = await res.json();
  return (json?.MediaContainer?.Directory || []).map((d) => ({
    key: String(d.key || ''),
    title: String(d.title || ''),
    type: String(d.type || ''),
    agent: String(d.agent || ''),
  }));
}

async function fetchPlexMusicLibraries(plexUrl, token) {
  const all = await fetchPlexLibraries(plexUrl, token);
  return all.filter((lib) => lib.type === 'artist');
}

async function fetchPlexPlaylistsForToken(plexUrl, token) {
  const url = buildAppApiUrl(plexUrl, 'playlists?playlistType=audio');
  url.searchParams.set('X-Plex-Token', token);
  const res = await fetch(url.toString(), { headers: { Accept: 'application/json' } });
  if (!res.ok) return [];
  const json = await res.json();
  return (json?.MediaContainer?.Metadata || []).map((pl) => ({
    ratingKey: String(pl.ratingKey || ''),
    title: String(pl.title || ''),
    smart: Boolean(pl.smart),
    leafCount: Number(pl.leafCount || 0),
  }));
}

// ─── Express setup ────────────────────────────────────────────────────────────

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.set('trust proxy', TRUST_PROXY_SETTING);

app.use(httpAccessLogMiddleware);

// Security headers
app.use((_req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('X-Permitted-Cross-Domain-Policies', 'none');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=(), payment=(), usb=()');
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin-allow-popups');
  res.setHeader('Cross-Origin-Resource-Policy', 'same-origin');
  if (EMBED_ALLOWED_ORIGINS.length) {
    res.removeHeader('X-Frame-Options');
  } else {
    res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  }
  const frameAncestors = ["'self'", ...EMBED_ALLOWED_ORIGINS].join(' ');
  res.setHeader('Content-Security-Policy', `frame-ancestors ${frameAncestors}; object-src 'none'; base-uri 'self'; form-action 'self'`);
  next();
});

app.use(express.static(path.join(__dirname, '..', 'public')));
app.use(express.urlencoded({ extended: false, limit: URLENCODED_BODY_LIMIT }));
app.use(express.json({ limit: JSON_BODY_LIMIT }));

// Asset version + app name locals
app.use((_req, res, next) => {
  res.locals.assetVersion = ASSET_VERSION;
  res.locals.APP_NAME = 'Curatorr';
  res.locals.APP_VERSION = APP_VERSION;
  next();
});

// Session
app.use(
  cookieSession({
    name: 'curatorr_session',
    secret: SESSION_SECRET,
    httpOnly: true,
    sameSite: 'lax',
    secure: isSecureEnv(),
    maxAge: 7 * 24 * 60 * 60 * 1000,
  })
);

// CSRF token injection
app.use((req, res, next) => {
  if (!req.session || typeof req.session !== 'object') return next();
  if (!req.session.csrfToken) req.session.csrfToken = crypto.randomBytes(24).toString('hex');
  res.locals.csrfToken = req.session.csrfToken;
  return next();
});

app.use(csrfProtectionMiddleware);

// Body size limits for auth routes
app.use('/login', createRequestBodySizeGuard(32 * 1024));
app.use('/wizard', createRequestBodySizeGuard(64 * 1024));
app.use('/setup', createRequestBodySizeGuard(32 * 1024));
app.use('/logout', createRequestBodySizeGuard(8 * 1024));

// Sync local user role changes into session on every request
  app.use((req, _res, next) => {
  const sessionUser = req.session?.user;
  if (!sessionUser || typeof sessionUser !== 'object') return next();
  if (String(sessionUser.source || '').toLowerCase() === 'local') {
    const config = loadConfig();
    const users = resolveLocalUsers(config);
    const index = findLocalUserIndex(users, { username: sessionUser.username, email: sessionUser.email });
    if (index >= 0) {
      const localUser = users[index];
      req.session.user = {
        ...sessionUser,
        username: localUser.username,
        email: localUser.email || '',
        role: localUser.role || sessionUser.role || 'user',
        avatar: normalizeStoredAvatarPath(localUser.avatar || ''),
        avatarFallback: resolveLocalAvatarFallback(localUser),
        source: 'local',
      };
    }
  }
  return next();
});

// Inject theme + server name into every response
app.use((req, res, next) => {
  const config = loadConfig();
  res.locals.themeDefaults = resolveThemeSettingsForUser(config, req.session?.user);
  res.locals.sidebarCollapsed = getCookieValue(req.headers?.cookie, 'curatorr_sidebar_collapsed') === 'true';
  res.locals.serverName = config?.general?.serverName || 'Curatorr';
  res.locals.wizardCompleted = Boolean(config?.wizard?.completed);
  res.locals.generalSettings = {
    serverName: config?.general?.serverName || 'Curatorr',
    remoteUrl: config?.general?.remoteUrl || '',
    localUrl: config?.general?.localUrl || '',
    basePath: normalizeBasePath(config?.general?.basePath || ''),
    restrictGuests: Boolean(config?.general?.restrictGuests),
  };
  next();
});

// ─── Route context ────────────────────────────────────────────────────────────

const _routeCtx = {
  // middleware
  requireUser,
  requireAdmin,
  requireSettingsAdmin,
  requireActualAdmin,
  requireWizardComplete,
  // config
  loadConfig,
  saveConfig,
  // auth helpers
  getActualRole,
  getEffectiveRole,
  resolveRoleSwitchRedirectPath,
  hasLocalAdmin,
  resolveLocalUsers,
  serializeLocalUsers,
  findLocalUserIndex,
  verifyPassword,
  hashPassword,
  validateLocalPasswordStrength,
  setSessionUser,
  updateUserLogins,
  resolvePublicBaseUrl,
  buildAppApiUrl,
  normalizeBaseUrl,
  normalizeIdentityList,
  normalizeLidarrAutomationScope,
  resolveLidarrAutomationSettings,
  canUserAccessLidarrAutomation,
  userHasOwnPlexToken,
  resolveUserPlexServerToken,
  roleRank,
  safeMessage,
  slugifyId,
  uniqueList,
  isValidEmail,
  // plex
  plexHeaders,
  exchangePin,
  exchangePinWithRetry,
  completePlexLogin,
  fetchPlexUser,
  fetchPlexResources,
  fetchPlexLibraries,
  fetchPlexMusicLibraries,
  fetchPlexPlaylistsForToken,
  resolvePlexServerToken,
  resolvePlexServerResource,
  resolvePlexMachineIdentifier,
  isPlexServerOwner,
  parsePlexUsers,
  resolveRole,
  loadAdmins,
  saveAdmins,
  loadCoAdmins,
  saveCoAdmins,
  loadPowerUsers,
  savePowerUsers,
  loadGuestUsers,
  saveGuestUsers,
  loadDisabledUsers,
  saveDisabledUsers,
  // logging
  pushLog,
  LOG_BUFFER,
  applyLogRetention,
  persistLogsToDisk,
  resolveLogSettings,
  // theme
  normalizeThemeSettings,
  resolveThemeDefaults,
  resolveThemeSettingsForUser,
  resolveThemePreferenceKey,
  serializeUserThemePreferences,
  resolveUserThemePreferences,
  // avatars
  parseUserAvatarDataUrl,
  saveCustomUserAvatar,
  normalizeStoredAvatarPath,
  USER_AVATAR_BASE,
  // version
  normalizeVersionTag,
  APP_VERSION,
  VERSION_CACHE_TTL_MS,
  buildReleaseNotesUrl,
  loadReleaseHighlights,
  fetchLatestDockerTag,
  // constants
  PRODUCT,
  PLATFORM,
  DEVICE_NAME,
  CLIENT_ID,
  LOCAL_AUTH_MIN_PASSWORD,
  DATA_DIR,
  DB_PATH,
  DEFAULT_SMART_PLAYLIST_SETTINGS,
  DEFAULT_LIDARR_AUTOMATION_SETTINGS,
  makeGlobalPlaylistId,
  // mutable let — accessed via getter/setter so route files see current value
  get versionCache() { return versionCache; },
  set versionCache(v) { versionCache = v; },
};

// ─── Startup ──────────────────────────────────────────────────────────────────

async function start() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  // Initialize SQLite DB
  const db = initDb(DB_PATH);
  _routeCtx.db = db;
  _routeCtx.recommendationService = createRecommendationService(_routeCtx);
  _routeCtx.lidarrService = createLidarrService(_routeCtx);
  _routeCtx.playlistService = createPlaylistService(_routeCtx);

  // Middleware: redirect non-admin users who haven't completed the user wizard.
  // Admins can always access pages — they may run the wizard later from their profile.
  _routeCtx.requireUserWizardComplete = (req, res, next) => {
    if (!req.session?.user) return next();
    const role = req.session.user.role || 'user';
    if (role === 'admin' || role === 'co-admin') return next(); // admins bypass
    const userId = req.session.user.username;
    const prefs = getUserPreferences(db, userId);
    if (!prefs.userWizardCompleted) return res.redirect('/wizard/user');
    return next();
  };

  // Ensure Ed25519 keypair for Plex OAuth
  await ensureKeypair();

  // Load persisted logs
  loadLogsFromDisk(resolveLogSettings(loadConfig()));

  // Create job service and start scheduled jobs (if wizard is complete)
  const _jobFunctions = {
    masterTrackRefresh: () => refreshMasterTrackCache(_routeCtx),
    smartPlaylistSync: async () => {
      const userIds = getAllUserIds(db);
      for (const userId of userIds) {
        const prefs = getUserPreferences(db, userId);
        if (!prefs.userWizardCompleted) continue;
        await rebuildSmartPlaylist(_routeCtx, userId);
        await _routeCtx.playlistService.syncCrescive(userId).catch(() => {});
        await _routeCtx.playlistService.syncCurative(userId).catch(() => {});
        const globalPlaylists = (loadConfig().globalPlaylists || []).filter((p) => p.enabled);
        for (const gp of globalPlaylists) {
          await _routeCtx.playlistService.syncGlobalPlaylist(userId, gp).catch(() => {});
        }
      }
    },
    tautulliDailySync: () => runTautulliDailySync(_routeCtx),
    lidarrReviewArtists: async () => {
      await _routeCtx.lidarrService?.autoQueueSuggestedArtists({ perUserLimit: 1 });
      return _routeCtx.lidarrService?.reviewDueArtists({ limit: 20 });
    },
    lidarrProcessQueue: () => _routeCtx.lidarrService?.processQueuedRequests({ limit: 10 }),
    dailyMixSync: async () => {
      const userIds = getAllUserIds(db);
      for (const userId of userIds) {
        const prefs = getUserPreferences(db, userId);
        if (!prefs.userWizardCompleted) continue;
        await _routeCtx.playlistService.syncDailyMix(userId).catch(() => {});
      }
    },
  };
  _routeCtx.jobService = createJobService(_routeCtx, _jobFunctions);

  const config0 = loadConfig();
  if (config0.wizard?.completed) {
    _routeCtx.jobService.startAll(true); // start intervals + run each job immediately once
  }

  // Register all routes
  registerApiUtil(app, _routeCtx);
  registerAuth(app, _routeCtx);
  registerApiPlex(app, _routeCtx);
  registerWizard(app, _routeCtx);
  registerPages(app, _routeCtx);
  registerApiMusic(app, _routeCtx);
  registerWebhooks(app, _routeCtx);
  registerSettings(app, _routeCtx);

  // 404 handler
  app.use((req, res) => {
    if (req.path.startsWith('/api/')) return res.status(404).json({ error: 'Not found.' });
    return res.status(404).render('error', { title: 'Not Found', message: 'Page not found.', status: 404 });
  });

  // Error handler
  app.use((err, req, res, _next) => {
    pushLog({ level: 'error', app: 'system', action: 'unhandled', message: safeMessage(err) });
    if (req.path.startsWith('/api/')) return res.status(500).json({ error: 'Internal server error.' });
    return res.status(500).render('error', { title: 'Error', message: 'An unexpected error occurred.', status: 500 });
  });

  app.listen(PORT, () => {
    const config = loadConfig();
    console.log(`[curatorr] v${APP_VERSION} listening on port ${PORT}`);
    console.log(`[curatorr] Base URL: ${BASE_URL}`);
    if (!config?.wizard?.completed) {
      console.log('[curatorr] Setup wizard not complete — visit /wizard to get started.');
    }
  });
}

start().catch((err) => {
  console.error('[curatorr] Fatal startup error:', err);
  process.exit(1);
});
