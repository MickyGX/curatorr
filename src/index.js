import path from 'path';
import fs from 'fs';
import crypto from 'crypto';
import express from 'express';
import cookieSession from 'cookie-session';
import {
  exportJWK,
  calculateJwkThumbprint,
} from 'jose';
import { format as formatConsoleArgs } from 'node:util';
import { fileURLToPath, pathToFileURL } from 'url';
import { registerApiUtil } from './routes/api-util.js';
import { registerAuth } from './routes/auth.js';
import { registerApiPlex } from './routes/api-plex.js';
import { registerApiMedia } from './routes/api-media.js';
import { registerPages } from './routes/pages.js';
import { registerApiArr } from './routes/api-arr.js';
import { registerApiSpecialty } from './routes/api-specialty.js';
import { registerSettings } from './routes/settings.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

const PORT = process.env.PORT || 3333;
const BASE_URL = process.env.BASE_URL || `http://localhost:${PORT}`;
const WIDGET_STATUS_INTERNAL_TOKEN = crypto.randomBytes(24).toString('hex');
const CLIENT_ID = process.env.PLEX_CLIENT_ID || getOrCreatePlexClientId();
const PRODUCT = process.env.PLEX_PRODUCT || 'Launcharr';
const PLATFORM = process.env.PLEX_PLATFORM || 'Web';
const DEVICE_NAME = process.env.PLEX_DEVICE_NAME || 'Launcharr';
const SESSION_SECRET = process.env.SESSION_SECRET
  || (() => {
    console.warn('[security] SESSION_SECRET is not set — generating a random secret. Sessions will reset on every restart. Set the SESSION_SECRET environment variable for persistent sessions.');
    return crypto.randomBytes(32).toString('hex');
  })();
const LOCAL_AUTH_MIN_PASSWORD = 12;
const TRUST_PROXY_ENABLED = parseEnvFlag(process.env.TRUST_PROXY, false);
const TRUST_PROXY_HOPS = resolveProxyHopCount(process.env.TRUST_PROXY_HOPS, 1);
const TRUST_PROXY_SETTING = TRUST_PROXY_ENABLED ? TRUST_PROXY_HOPS : false;
const URLENCODED_BODY_LIMIT = String(process.env.URLENCODED_BODY_LIMIT || '8mb').trim() || '8mb';
const JSON_BODY_LIMIT = String(process.env.JSON_BODY_LIMIT || '2mb').trim() || '2mb';
const CONFIG_PATH = process.env.CONFIG_PATH || path.join(__dirname, '..', 'config', 'config.json');
const APP_VERSION = process.env.APP_VERSION || loadPackageVersion();
const ASSET_VERSION_BASE = normalizeVersionTag(APP_VERSION || '') || String(APP_VERSION || 'dev');
const ASSET_VERSION = `${ASSET_VERSION_BASE}-${String(process.env.ASSET_BUILD_ID || Date.now().toString(36))}`;
const RELEASE_NOTES_BASE_URL = 'https://github.com/MickyGX/launcharr/releases/tag/';
const RELEASE_NOTES_DIR = path.join(__dirname, '..', 'docs', 'release', 'releases');
const RELEASE_HIGHLIGHT_SECTIONS = ['Added', 'Changed', 'Fixed'];
const RELEASE_HIGHLIGHT_LIMIT = 6;
const DEFAULT_APPS_PATH = process.env.DEFAULT_APPS_PATH || path.join(__dirname, '..', 'default-apps.json');
const DEFAULT_CATEGORIES_PATH = process.env.DEFAULT_CATEGORIES_PATH || path.join(__dirname, '..', 'config', 'default-categories.json');
const BUNDLED_DEFAULT_APPS_PATH = path.join(__dirname, '..', 'default-apps.json');
const SOURCE_DEFAULT_APPS_PATH = path.join(__dirname, '..', 'config', 'default-apps.json');
const BUNDLED_DEFAULT_CATEGORIES_PATH = path.join(__dirname, '..', 'default-categories.json');
const SOURCE_DEFAULT_CATEGORIES_PATH = path.join(__dirname, '..', 'config', 'default-categories.json');
const CONFIG_EXAMPLE_PATH = process.env.CONFIG_EXAMPLE_PATH || path.join(__dirname, '..', 'config', 'config.example.json');
const BUNDLED_CONFIG_EXAMPLE_PATH = path.join(__dirname, '..', 'config.example.json');
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const PUBLIC_DIR = path.join(__dirname, '..', 'public');
const LOCAL_MODULES_DIR = path.join(__dirname, '..', 'local-modules');
const ICONS_DIR = path.join(PUBLIC_DIR, 'icons');
const USER_AVATAR_DIR = path.join(ICONS_DIR, 'custom', 'avatars');
const USER_AVATAR_BASE = '/icons/custom/avatars';
const MAX_USER_AVATAR_BYTES = 2 * 1024 * 1024;
const USER_AVATAR_ALLOWED_MIME = new Set(['image/png', 'image/jpeg', 'image/webp']);
const HTTP_ACCESS_LOGS = parseEnvFlag(process.env.HTTP_ACCESS_LOGS, true);
const HTTP_ACCESS_LOGS_SKIP_STATIC = parseEnvFlag(process.env.HTTP_ACCESS_LOGS_SKIP_STATIC, false);
const LOG_MIRROR_STDERR_TO_STDOUT = parseEnvFlag(process.env.LOG_MIRROR_STDERR_TO_STDOUT, false);
const LOG_REDACT_HOSTS = parseEnvFlag(process.env.LOG_REDACT_HOSTS, false);
const LOG_REDACT_IPS = parseEnvFlag(process.env.LOG_REDACT_IPS, false);
const LOG_HOST_ALIAS_CACHE = new Map();
const LOG_IP_ALIAS_CACHE = new Map();

const ADMIN_USERS = parseCsv(process.env.ADMIN_USERS || '');
const ARR_APP_IDS = ['radarr', 'sonarr', 'lidarr', 'readarr'];
const DOWNLOADER_APP_IDS = ['transmission', 'nzbget', 'qbittorrent', 'sabnzbd'];
const MEDIA_APP_IDS = ['plex', 'jellyfin', 'emby'];
const LEGACY_MULTI_INSTANCE_APP_IDS = ['radarr', 'sonarr', 'bazarr', 'transmission'];
const DEFAULT_MAX_MULTI_INSTANCES_PER_APP = 5;
const DEFAULT_INSTANCE_NAME_PLACEHOLDER = 'Instance (e.g. Main)';
const DEFAULT_CATEGORY_ORDER = ['Admin', 'Media', 'Manager', 'Arr Suite', 'Indexers', 'Downloaders', 'Games', 'Photos', 'System', 'Documents', 'Tools', 'Finance', 'Requesters'];
const DEFAULT_CATEGORY_ICON = '/icons/category.svg';
const ARR_COMBINE_SECTIONS = [
  { key: 'downloadingSoon', elementId: 'downloading-soon' },
  { key: 'recentlyDownloaded', elementId: 'recently-downloaded' },
  { key: 'activityQueue', elementId: 'activity-queue' },
  { key: 'calendar', elementId: 'calendar' },
];
const ARR_COMBINED_SECTION_PREFIX = {
  downloadingSoon: 'arrcombinedsoon',
  recentlyDownloaded: 'arrcombinedrecent',
  activityQueue: 'arrcombinedqueue',
  calendar: 'arrcombinedcalendar',
};
const DOWNLOADER_COMBINE_SECTIONS = [
  { key: 'activityQueue', elementId: 'activity-queue' },
];
const DOWNLOADER_COMBINED_SECTION_PREFIX = {
  activityQueue: 'downloaderscombinedqueue',
};
const MEDIA_COMBINE_SECTIONS = [
  { key: 'active', elementId: 'active' },
  { key: 'recent', elementId: 'recent' },
];
const MEDIA_COMBINED_SECTION_PREFIX = {
  active: 'mediacombinedactive',
  recent: 'mediacombinedrecent',
};
const ENABLE_ARR_UNIFIED_CARDS = true;
const ENABLE_DOWNLOADER_UNIFIED_CARDS = true;

// Widget bar stat card types — one entry per integrated app family.
// typeId matches getAppBaseId(appId). metricFields define which stats are shown.
const WIDGET_STAT_TYPES = [
  { typeId: 'plex',         name: 'Plex',         icon: '/icons/plex.svg',         metricFields: [{ key: 'movies', label: 'Movies' }, { key: 'shows', label: 'TV Shows' }, { key: 'artists', label: 'Artists' }, { key: 'albums', label: 'Albums' }] },
  { typeId: 'tautulli',     name: 'Tautulli',     icon: '/icons/tautulli.svg',     metricFields: [{ key: 'streams', label: 'Active Streams' }, { key: 'plays', label: 'Plays Today' }] },
  { typeId: 'jellyfin',     name: 'Jellyfin',     icon: '/icons/jellyfin.png',     metricFields: [{ key: 'movies', label: 'Movies' }, { key: 'shows', label: 'Series' }, { key: 'episodes', label: 'Episodes' }] },
  { typeId: 'emby',         name: 'Emby',         icon: '/icons/emby.png',         metricFields: [{ key: 'movies', label: 'Movies' }, { key: 'shows', label: 'Series' }, { key: 'episodes', label: 'Episodes' }] },
  { typeId: 'radarr',       name: 'Radarr',       icon: '/icons/radarr.svg',       metricFields: [{ key: 'movies', label: 'Movies' }, { key: 'movie_files', label: 'Movie Files' }, { key: 'monitored', label: 'Monitored' }, { key: 'unmonitored', label: 'Unmonitored' }] },
  { typeId: 'sonarr',       name: 'Sonarr',       icon: '/icons/sonarr.svg',       metricFields: [{ key: 'series', label: 'Series' }, { key: 'ended', label: 'Ended' }, { key: 'continuing', label: 'Continuing' }, { key: 'monitored', label: 'Monitored' }, { key: 'unmonitored', label: 'Unmonitored' }, { key: 'episodes', label: 'Episodes' }] },
  { typeId: 'lidarr',       name: 'Lidarr',       icon: '/icons/lidarr.svg',       metricFields: [{ key: 'artists', label: 'Artists' }, { key: 'albums', label: 'Albums' }] },
  { typeId: 'readarr',      name: 'Readarr',      icon: '/icons/readarr.svg',      metricFields: [{ key: 'books', label: 'Books' }, { key: 'authors', label: 'Authors' }] },
  { typeId: 'prowlarr',     name: 'Prowlarr',     icon: '/icons/prowlarr.svg',     metricFields: [{ key: 'active_indexers', label: 'Active Indexers' }, { key: 'total_queries', label: 'Total Queries' }, { key: 'total_grabs', label: 'Total Grabs' }, { key: 'active_apps', label: 'Active Apps' }] },
  { typeId: 'jackett',      name: 'Jackett',      icon: '/icons/jackett.png',      metricFields: [{ key: 'indexers', label: 'Indexers' }] },
  { typeId: 'bazarr',       name: 'Bazarr',       icon: '/icons/bazarr.png',       metricFields: [{ key: 'episodes', label: 'Episode Subs' }, { key: 'movies', label: 'Movie Subs' }] },
  { typeId: 'autobrr',      name: 'Autobrr',      icon: '/icons/autobrr.png',      metricFields: [{ key: 'filtered', label: 'Filtered Releases' }, { key: 'push_approved', label: 'Approved Pushes' }, { key: 'push_rejected', label: 'Rejected Pushes' }, { key: 'push_error', label: 'Errored Pushes' }] },
  { typeId: 'qbittorrent',  name: 'qBittorrent',  icon: '/icons/qbittorrent.png',  metricFields: [{ key: 'downloading', label: 'Downloading' }, { key: 'seeding', label: 'Seeding' }] },
  { typeId: 'sabnzbd',      name: 'SABnzbd',      icon: '/icons/sabnzbd.png',      metricFields: [{ key: 'speed', label: 'Speed' }, { key: 'queue', label: 'Queue' }] },
  { typeId: 'nzbget',       name: 'NZBGet',       icon: '/icons/nzbget.svg',       metricFields: [{ key: 'downloading', label: 'Downloading' }, { key: 'queue', label: 'Queued' }, { key: 'speed', label: 'Download Speed' }, { key: 'remaining', label: 'Remaining' }] },
  { typeId: 'transmission', name: 'Transmission', icon: '/icons/transmission.svg', metricFields: [{ key: 'active', label: 'Active' }, { key: 'paused', label: 'Paused' }, { key: 'total', label: 'Total' }, { key: 'downloading', label: 'Downloading' }, { key: 'seeding', label: 'Seeding' }, { key: 'dlspeed', label: 'Download Speed' }, { key: 'upspeed', label: 'Upload Speed' }] },
  { typeId: 'maintainerr',  name: 'Maintainerr',  icon: '/icons/maintainerr.svg',  metricFields: [{ key: 'rules', label: 'Rules' }, { key: 'active', label: 'Active' }] },
  { typeId: 'cleanuparr',   name: 'Cleanuparr',   icon: '/icons/cleanuparr.svg',   metricFields: [{ key: 'tracked', label: 'Tracked' }, { key: 'removed', label: 'Removed' }] },
  { typeId: 'agregarr',     name: 'Agregarr',     icon: '/icons/agregarr.svg',     metricFields: [{ key: 'apps', label: 'Apps' }, { key: 'rules', label: 'Rules' }, { key: 'version', label: 'Version' }] },
  { typeId: 'profilarr',    name: 'Profilarr',    icon: '/icons/profilarr.svg',    metricFields: [{ key: 'sync', label: 'Sync' }, { key: 'backup', label: 'Backup' }] },
  { typeId: 'sortarr',      name: 'Sortarr',      icon: '/icons/sortarr.svg',      metricFields: [{ key: 'profiles', label: 'Profiles' }, { key: 'rules', label: 'Rules' }, { key: 'version', label: 'Version' }] },
  { typeId: 'romm',         name: 'Romm',         icon: '/icons/romm.svg',         metricFields: [{ key: 'games', label: 'Games' }, { key: 'consoles', label: 'Consoles' }, { key: 'collections', label: 'Collections' }, { key: 'virtual_collections', label: 'Virtual Collections' }, { key: 'smart_collections', label: 'Smart Collections' }, { key: 'bios', label: 'BIOS' }, { key: 'saves', label: 'Saves' }] },
  { typeId: 'ersatztv',         name: 'ErsatzTV',         icon: '/icons/ersatztv.png',         metricFields: [{ key: 'channels', label: 'Channels' }, { key: 'streams', label: 'Streams' }] },
  { typeId: 'seerr',            name: 'Overseerr',        icon: '/icons/seerr.png',            metricFields: [{ key: 'pending', label: 'Pending' }, { key: 'approved', label: 'Approved' }, { key: 'processing', label: 'Processing' }, { key: 'available', label: 'Available' }] },
  { typeId: 'pulsarr',          name: 'Pulsarr',          icon: '/icons/pulsarr.svg',          metricFields: [{ key: 'auto_approved', label: 'Auto Approved' }, { key: 'approved', label: 'Approved' }, { key: 'movies', label: 'Movies' }, { key: 'shows', label: 'TV Shows' }] },
  { typeId: 'immich',           name: 'Immich',           icon: '/icons/immich.svg',           metricFields: [{ key: 'photos', label: 'Photos' }, { key: 'videos', label: 'Videos' }, { key: 'users', label: 'Users' }, { key: 'storage', label: 'Storage' }] },
  { typeId: 'portainer',        name: 'Portainer',        icon: '/icons/portainer.svg',        metricFields: [{ key: 'running', label: 'Running' }, { key: 'stopped', label: 'Stopped' }, { key: 'total', label: 'Total' }] },
  { typeId: 'glances',          name: 'Glances',          icon: '/icons/glances.svg',          metricFields: [{ key: 'cpu', label: 'CPU' }, { key: 'memory', label: 'Memory' }, { key: 'load', label: 'Load Avg' }] },
  { typeId: 'uptime-kuma',      name: 'Uptime Kuma',      icon: '/icons/uptime-kuma.svg',      metricFields: [{ key: 'up', label: 'Up' }, { key: 'down', label: 'Down' }, { key: 'pending', label: 'Pending' }, { key: 'maintenance', label: 'Maintenance' }] },
  { typeId: 'speedtest-tracker', name: 'Speedtest Tracker', icon: '/icons/speedtest-tracker.png', metricFields: [{ key: 'download', label: 'Download' }, { key: 'upload', label: 'Upload' }, { key: 'ping', label: 'Ping' }] },
  { typeId: 'gluetun',          name: 'Gluetun',          icon: '/icons/gluetun.svg',          metricFields: [{ key: 'status', label: 'VPN Status' }, { key: 'ip', label: 'Public IP' }, { key: 'country', label: 'Country' }] },
  { typeId: 'guardian',         name: 'Guardian',         icon: '/icons/guardian.svg',         metricFields: [{ key: 'devices', label: 'Devices' }, { key: 'blocked', label: 'Blocked' }, { key: 'version', label: 'Version' }] },
  { typeId: 'paperless-ngx',    name: 'Paperless-ngx',    icon: '/icons/paperless-ngx.svg',    metricFields: [{ key: 'documents', label: 'Documents' }, { key: 'inbox', label: 'Inbox' }] },
  { typeId: 'metube',           name: 'MeTube',           icon: '/icons/metube.svg',           metricFields: [{ key: 'downloading', label: 'Downloading' }, { key: 'queued', label: 'Queued' }, { key: 'done', label: 'Done' }] },
  { typeId: 'audiobookshelf',  name: 'Audiobookshelf',  icon: '/icons/audiobookshelf.svg',  metricFields: [{ key: 'books', label: 'Books' }, { key: 'podcasts', label: 'Podcasts' }] },
  { typeId: 'tdarr',           name: 'Tdarr',           icon: '/icons/tdarr.png',           metricFields: [{ key: 'queue', label: 'Queue' }, { key: 'processed', label: 'Processed' }, { key: 'errored', label: 'Errored' }, { key: 'saved', label: 'Space Saved' }] },
  { typeId: 'apprise',         name: 'Apprise',         icon: '/icons/apprise.png',         metricFields: [{ key: 'state', label: 'State' }, { key: 'queued', label: 'Queued' }, { key: 'version', label: 'Version' }] },
  { typeId: 'termix',          name: 'Termix',          icon: '/icons/termix.svg',          metricFields: [{ key: 'sessions', label: 'Sessions' }, { key: 'clients', label: 'Clients' }, { key: 'uptime', label: 'Uptime' }] },
  { typeId: 'wizarr',          name: 'Wizarr',          icon: '/icons/wizarr.svg',          metricFields: [{ key: 'users', label: 'Users' }, { key: 'pending', label: 'Pending' }, { key: 'expired', label: 'Expired' }] },
  { typeId: 'guacamole',       name: 'Guacamole',       icon: '/icons/guacamole.svg',       metricFields: [{ key: 'active', label: 'Active' }, { key: 'connections', label: 'Connections' }, { key: 'users', label: 'Users' }] },
  { typeId: 'traefik',         name: 'Traefik',         icon: '/icons/traefik.svg',         metricFields: [{ key: 'routers', label: 'Routers' }, { key: 'services', label: 'Services' }, { key: 'middlewares', label: 'Middlewares' }] },
  { typeId: 'dozzle',          name: 'Dozzle',          icon: '/icons/dozzle.svg',          metricFields: [{ key: 'running', label: 'Running' }] },
  { typeId: 'qnap',            name: 'QNAP',            icon: '/icons/qnap.svg',            metricFields: [{ key: 'cpu', label: 'CPU' }, { key: 'memory', label: 'Memory' }, { key: 'volume', label: 'Volume' }] },
];
const WIDGET_STAT_TYPE_BY_ID = new Map(WIDGET_STAT_TYPES.map((t) => [t.typeId, t]));

const SYSTEM_WIDGET_TYPES = [
  { typeId: 'sys-resources', label: 'System Info', icon: '/icons/dashboard.svg', hasConfig: true, configFields: [] },
  { typeId: 'links', label: 'Links', icon: '/icons/system.svg', hasConfig: true, configFields: [] },
  { typeId: 'deployment-summary', label: 'Launcharr', icon: '/icons/launcharr-icon.png', hasConfig: true, configFields: [] },
];
const SYSTEM_WIDGET_TYPE_BY_ID = new Map(SYSTEM_WIDGET_TYPES.map((t) => [t.typeId, t]));
const SYSTEM_WIDGET_SEARCH_PROVIDERS = [
  { id: 'duckduckgo', label: 'DuckDuckGo', url: 'https://duckduckgo.com/?q={query}' },
  { id: 'google',     label: 'Google',     url: 'https://www.google.com/search?q={query}' },
  { id: 'bing',       label: 'Bing',       url: 'https://www.bing.com/search?q={query}' },
  { id: 'brave',      label: 'Brave',      url: 'https://search.brave.com/search?q={query}' },
  { id: 'searxng',    label: 'SearXNG',    url: '{baseUrl}?q={query}' },
];
const SYSTEM_WIDGET_TIMEZONES = [
  'UTC', 'Europe/London', 'Europe/Paris', 'Europe/Berlin', 'Europe/Rome', 'Europe/Madrid',
  'Europe/Amsterdam', 'Europe/Stockholm', 'Europe/Oslo', 'Europe/Helsinki', 'Europe/Athens',
  'Europe/Warsaw', 'Europe/Prague', 'Europe/Budapest', 'Europe/Bucharest', 'Europe/Lisbon',
  'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles',
  'America/Toronto', 'America/Vancouver', 'America/Phoenix', 'America/Anchorage',
  'America/Honolulu', 'America/Mexico_City', 'America/Sao_Paulo', 'America/Buenos_Aires',
  'America/Bogota', 'America/Lima',
  'Asia/Tokyo', 'Asia/Singapore', 'Asia/Shanghai', 'Asia/Hong_Kong', 'Asia/Seoul',
  'Asia/Taipei', 'Asia/Bangkok', 'Asia/Jakarta', 'Asia/Kolkata', 'Asia/Dubai',
  'Asia/Riyadh', 'Asia/Jerusalem', 'Asia/Karachi', 'Asia/Dhaka',
  'Australia/Sydney', 'Australia/Melbourne', 'Australia/Brisbane', 'Australia/Perth',
  'Pacific/Auckland', 'Pacific/Fiji',
  'Africa/Johannesburg', 'Africa/Cairo', 'Africa/Lagos', 'Africa/Nairobi',
];

function normalizeSystemWidget(entry) {
  if (!entry || typeof entry !== 'object') return null;
  const systemType = String(entry.systemType || entry.system_type || '').trim().toLowerCase();
  if (!SYSTEM_WIDGET_TYPE_BY_ID.has(systemType)) return null;
  const typeDef = SYSTEM_WIDGET_TYPE_BY_ID.get(systemType);
  const id = normalizeWidgetId(entry.id || '') || normalizeWidgetId(`wg-${systemType}-${Math.random().toString(36).slice(2, 6)}`);
  if (!id) return null;
  const rawConfig = (entry.systemConfig && typeof entry.systemConfig === 'object') ? entry.systemConfig : {};
  const normalizeLinksWidgetUrl = (rawValue) => {
    const input = String(rawValue || '').trim();
    if (!input) return '';
    let candidate = input;
    if (!/^[a-z][a-z0-9+.-]*:/i.test(candidate)) candidate = `https://${candidate}`;
    try {
      const parsed = new URL(candidate);
      if (!parsed.hostname) return '';
      if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return '';
      return parsed.toString();
    } catch (_err) {
      return '';
    }
  };
  if (systemType === 'links') {
    const rawLinks = Array.isArray(rawConfig.links) ? rawConfig.links : [];
    const links = rawLinks
      .map((row) => {
        const rawName = String((row && (row.name ?? row.title)) || '').trim();
        const rawUrl = String((row && (row.url ?? row.href)) || '').trim();
        const normalizedUrl = normalizeLinksWidgetUrl(rawUrl);
        if (!normalizedUrl) return null;
        let hostname = '';
        try { hostname = String(new URL(normalizedUrl).hostname || '').trim(); } catch (_err) { /* ignore */ }
        const name = rawName || hostname || normalizedUrl;
        return {
          name: String(name || '').trim().slice(0, 120),
          url: normalizedUrl,
        };
      })
      .filter(Boolean)
      .slice(0, 250);
    const title = String(rawConfig.title || rawConfig.name || 'Links').trim().slice(0, 80) || 'Links';
    const systemConfig = {
      title,
      showUrl: rawConfig.showUrl !== false,
      links,
    };
    return { id, systemType, systemConfig, icon: typeDef.icon, typeName: typeDef.label };
  }
  if (systemType === 'deployment-summary') {
    const rawStats = (rawConfig.stats && typeof rawConfig.stats === 'object') ? rawConfig.stats : {};
    const rawColumns = Number(rawConfig.columns ?? rawConfig.metricColumns ?? rawConfig.metric_columns);
    let showOnline = rawStats.online !== false;
    let showOffline = rawStats.offline !== false;
    let showTotal = rawStats.total !== false;
    if (!showOnline && !showOffline && !showTotal) showOnline = true;
    const systemConfig = {
      columns: Number.isFinite(rawColumns) ? Math.max(1, Math.min(4, Math.round(rawColumns))) : 3,
      stats: {
        online: showOnline,
        offline: showOffline,
        total: showTotal,
      },
    };
    return { id, systemType, systemConfig, icon: typeDef.icon, typeName: typeDef.label };
  }
  const validProviders = SYSTEM_WIDGET_SEARCH_PROVIDERS.map((p) => p.id);
  const rawDisks = Array.isArray(rawConfig.disks) ? rawConfig.disks : [];
  const rawSearch = (rawConfig.search && typeof rawConfig.search === 'object') ? rawConfig.search : {};
  const rawWeather = (rawConfig.weather && typeof rawConfig.weather === 'object') ? rawConfig.weather : {};
  const lat = Number(rawWeather.latitude);
  const lon = Number(rawWeather.longitude);
  const cache = Number(rawWeather.cache);
  const systemConfig = {
    cpu: rawConfig.cpu !== false,
    memory: rawConfig.memory !== false,
    showTotalSpace: rawConfig.showTotalSpace === true,
    disks: rawDisks
      .map((d) => ({ path: String((d && d.path) || '/').trim() || '/', label: String((d && d.label) || '').trim() }))
      .filter((d) => d.path),
    search: {
      enabled: rawSearch.enabled === true,
      provider: validProviders.includes(rawSearch.provider) ? rawSearch.provider : 'duckduckgo',
      target: rawSearch.target === '_self' ? '_self' : '_blank',
      baseUrl: String(rawSearch.baseUrl || '').trim(),
    },
    weather: {
      enabled: rawWeather.enabled === true,
      label: String(rawWeather.label || '').trim(),
      latitude: Number.isFinite(lat) ? Math.max(-90, Math.min(90, lat)) : 0,
      longitude: Number.isFinite(lon) ? Math.max(-180, Math.min(180, lon)) : 0,
      timezone: String(rawWeather.timezone || 'UTC').trim() || 'UTC',
      units: rawWeather.units === 'imperial' ? 'imperial' : 'metric',
      cache: Number.isFinite(cache) ? Math.max(1, Math.min(60, Math.round(cache))) : 5,
    },
  };
  return { id, systemType, systemConfig, icon: typeDef.icon, typeName: typeDef.label };
}

setupConsoleLogRedaction();
setupConsoleStderrMirrorToStdout();
const APP_OVERVIEW_ELEMENTS = {
  plex: [
    { id: 'active', name: 'Active Streams' },
    { id: 'recent', name: 'Recently Added' },
    { id: 'watchlisted', name: 'Most Watchlisted This Week' },
  ],
  jellyfin: [
    { id: 'active', name: 'Active Streams' },
    { id: 'recent', name: 'Recently Added' },
  ],
  emby: [
    { id: 'active', name: 'Active Streams' },
    { id: 'recent', name: 'Recently Added' },
  ],
  tautulli: [
    { id: 'watch-stats', name: 'Watch Statistics' },
  ],
  sonarr: [
    { id: 'downloading-soon', name: 'Downloading Soon' },
    { id: 'recently-downloaded', name: 'Recently Downloaded' },
    { id: 'activity-queue', name: 'Activity Queue' },
    { id: 'calendar', name: 'Calendar' },
  ],
  radarr: [
    { id: 'downloading-soon', name: 'Downloading Soon' },
    { id: 'recently-downloaded', name: 'Recently Downloaded' },
    { id: 'activity-queue', name: 'Activity Queue' },
    { id: 'calendar', name: 'Calendar' },
  ],
  lidarr: [
    { id: 'downloading-soon', name: 'Downloading Soon' },
    { id: 'recently-downloaded', name: 'Recently Downloaded' },
    { id: 'activity-queue', name: 'Activity Queue' },
    { id: 'calendar', name: 'Calendar' },
  ],
  readarr: [
    { id: 'downloading-soon', name: 'Downloading Soon' },
    { id: 'recently-downloaded', name: 'Recently Downloaded' },
    { id: 'activity-queue', name: 'Activity Queue' },
    { id: 'calendar', name: 'Calendar' },
  ],
  pulsarr: [
    { id: 'recent-requests', name: 'Recent Requests' },
    { id: 'most-watchlisted', name: 'Most Watchlisted' },
  ],
  seerr: [
    { id: 'recent-requests', name: 'Recent Requests' },
    { id: 'most-watchlisted', name: 'Most Watchlisted' },
  ],
  prowlarr: [
    { id: 'search', name: 'Indexer Search' },
  ],
  jackett: [
    { id: 'search', name: 'Indexer Search' },
  ],
  bazarr: [
    { id: 'subtitle-queue', name: 'Subtitle Queue' },
  ],
  autobrr: [
    { id: 'recent-matches', name: 'Releases' },
    { id: 'delivery-queue', name: 'Delivery Queue' },
  ],
  romm: [
    { id: 'recently-added', name: 'Recently Added' },
    { id: 'consoles', name: 'Consoles' },
  ],
  maintainerr: [
    { id: 'library-media', name: 'Library Media' },
    { id: 'rules', name: 'Rules' },
    { id: 'collections-media', name: 'Collections Media' },
  ],
  transmission: [
    { id: 'activity-queue', name: 'Download Queue' },
  ],
  nzbget: [
    { id: 'activity-queue', name: 'Download Queue' },
  ],
  qbittorrent: [
    { id: 'activity-queue', name: 'Download Queue' },
  ],
  sabnzbd: [
    { id: 'activity-queue', name: 'Download Queue' },
  ],
  immich: [
    { id: 'recent', name: 'Recently Added' },
  ],
  wizarr: [
    { id: 'users', name: 'Users' },
    { id: 'invitations', name: 'Invitations' },
  ],
  metube: [
    { id: 'queue', name: 'Download Queue' },
  ],
  audiobookshelf: [
    { id: 'recently-added', name: 'Recently Added' },
  ],
  tdarr: [
    { id: 'stats', name: 'Statistics' },
  ],
  'uptime-kuma': [
    { id: 'status', name: 'Status Page' },
  ],
  guacamole: [
    { id: 'active-sessions', name: 'Active Sessions' },
    { id: 'connections', name: 'Connections' },
  ],
  traefik: [
    { id: 'routers', name: 'Routers' },
    { id: 'services', name: 'Services' },
  ],
};
const PLEX_DISCOVERY_WATCHLISTED_URL = 'https://watch.plex.tv/discover/list/top_watchlisted';
const PLEX_DISCOVERY_CACHE_TTL_MS = 15 * 60 * 1000;
let plexDiscoveryWatchlistedCache = {
  expiresAt: 0,
  payload: null,
};
const TAUTULLI_WATCH_CARDS = [
  { id: 'top_movies', name: 'Most Watched Movies' },
  { id: 'popular_movies', name: 'Most Popular Movies' },
  { id: 'top_tv', name: 'Most Watched TV Shows' },
  { id: 'popular_tv', name: 'Most Popular TV Shows' },
  { id: 'top_music', name: 'Most Played Artists' },
  { id: 'popular_music', name: 'Most Popular Artists' },
  { id: 'top_libraries', name: 'Most Active Libraries' },
  { id: 'top_users', name: 'Most Active Users' },
  { id: 'top_platforms', name: 'Most Active Platforms' },
  { id: 'last_watched', name: 'Recently Watched' },
  { id: 'most_concurrent', name: 'Most Concurrent Streams' },
];
const LOG_BUFFER = [];
const LOG_PATH = process.env.LOG_PATH || path.join(DATA_DIR, 'logs.json');

const DEFAULT_LOG_SETTINGS = {
  maxEntries: 250,
  maxDays: 7,
  visibleRows: 10,
};
const DEFAULT_ONBOARDING_SETTINGS = {
  quickStartPending: false,
};

const VERSION_CACHE_TTL_MS = 10 * 60 * 1000;
let versionCache = { fetchedAt: 0, payload: null };

const DEFAULT_QUEUE_DISPLAY = {
  queueShowDetail: true,
  queueShowSubDetail: true,
  queueShowSize: true,
  queueShowProtocol: true,
  queueShowTimeLeft: true,
  queueShowProgress: true,
  queueVisibleRows: 10,
};

const DEFAULT_GENERAL_SETTINGS = {
  serverName: 'Launcharr',
  remoteUrl: '',
  localUrl: '',
  basePath: '',
  restrictGuests: false,
  autoOpenSingleAppMenuItem: false,
  sidebarButtonShortPressAction: 'default',
  sidebarButtonLongPressAction: 'default',
  sidebarButtonPressActions: {
    guest: { short: 'default', long: 'default' },
    user: { short: 'default', long: 'default' },
    'co-admin': { short: 'default', long: 'default' },
    admin: { short: 'default', long: 'default' },
  },
  hideSidebarAppSettingsLink: false,
  hideSidebarActivityLink: false,
};

const DEFAULT_NOTIFICATION_SETTINGS = {
  appriseEnabled: false,
  appriseApiUrl: '',
  appriseMode: 'targets',
  appriseConfigKey: '',
  appriseTargets: '',
  appriseTag: '',
  widgetStatusEnabled: false,
  widgetStatusDelaySeconds: 60,
  widgetStatusPollSeconds: 45,
  widgetStatusRequestTimeoutMs: 4000,
  widgetStatusMaxConcurrency: 4,
};
const ALLOWED_BRAND_THEMES = new Set(['custom', 'launcharr', 'rocketship', 'pulsarr', 'plex', 'radarr', 'sonarr', 'lidarr', 'readarr']);
const DEFAULT_THEME_SETTINGS = {
  mode: '',
  brandTheme: 'pulsarr',
  customColor: '#1cc6c2',
  sidebarInvert: false,
  squareCorners: false,
  bgMotion: true,
  carouselFreeScroll: false,
  hideScrollbars: false,
};

const APP_BASE_NAME_MAP = {
  radarr: 'Radarr',
  sonarr: 'Sonarr',
  bazarr: 'Bazarr',
  transmission: 'Transmission',
};
const APP_INSTANCE_PLACEHOLDER_MAP = {
  radarr: 'Movies (e.g. 4K)',
  sonarr: 'TV (e.g. Anime)',
  bazarr: 'Subtitles (e.g. Foreign)',
  transmission: 'Client (e.g. Main)',
};
let RUNTIME_MULTI_INSTANCE_BASE_IDS = [...LEGACY_MULTI_INSTANCE_APP_IDS];
const VISIBILITY_ROLE_ORDER = ['disabled', 'guest', 'user', 'co-admin', 'admin'];
const VISIBILITY_ROLE_RANK = {
  disabled: -1,
  guest: 0,
  user: 1,
  'co-admin': 2,
  admin: 3,
};
const CATEGORY_VISIBILITY_SELECTABLE_ROLES = ['guest', 'user', 'co-admin', 'admin'];
const SIDEBAR_APP_BUTTON_ACTIONS = new Set(['default', 'overview', 'launch', 'settings', 'activity']);

function normalizeSidebarAppButtonAction(value, fallback = 'default') {
  const raw = String(value || '').trim().toLowerCase();
  if (SIDEBAR_APP_BUTTON_ACTIONS.has(raw)) return raw;
  const fallbackRaw = String(fallback || '').trim().toLowerCase();
  if (SIDEBAR_APP_BUTTON_ACTIONS.has(fallbackRaw)) return fallbackRaw;
  return 'default';
}

function normalizeSidebarButtonPressActions(value, fallback = DEFAULT_GENERAL_SETTINGS.sidebarButtonPressActions) {
  const source = value && typeof value === 'object' ? value : {};
  const fallbackSource = fallback && typeof fallback === 'object'
    ? fallback
    : DEFAULT_GENERAL_SETTINGS.sidebarButtonPressActions;
  const resolveRoleActions = (roleKey) => {
    const roleSource = source[roleKey] && typeof source[roleKey] === 'object' ? source[roleKey] : {};
    const roleFallback = fallbackSource[roleKey] && typeof fallbackSource[roleKey] === 'object'
      ? fallbackSource[roleKey]
      : DEFAULT_GENERAL_SETTINGS.sidebarButtonPressActions[roleKey];
    return {
      short: normalizeSidebarAppButtonAction(roleSource.short, roleFallback.short),
      long: normalizeSidebarAppButtonAction(roleSource.long, roleFallback.long),
    };
  };
  return {
    guest: resolveRoleActions('guest'),
    user: resolveRoleActions('user'),
    'co-admin': resolveRoleActions('co-admin'),
    admin: resolveRoleActions('admin'),
  };
}

function resolveGeneralSettings(config) {
  const raw = config && typeof config.general === 'object' ? config.general : {};
  const restrictGuests = raw.restrictGuests === undefined
    ? DEFAULT_GENERAL_SETTINGS.restrictGuests
    : Boolean(raw.restrictGuests);
  const autoOpenSingleAppMenuItem = raw.autoOpenSingleAppMenuItem === undefined
    ? DEFAULT_GENERAL_SETTINGS.autoOpenSingleAppMenuItem
    : Boolean(raw.autoOpenSingleAppMenuItem);
  const sidebarButtonShortPressAction = normalizeSidebarAppButtonAction(
    raw.sidebarButtonShortPressAction,
    DEFAULT_GENERAL_SETTINGS.sidebarButtonShortPressAction
  );
  const sidebarButtonLongPressAction = normalizeSidebarAppButtonAction(
    raw.sidebarButtonLongPressAction,
    DEFAULT_GENERAL_SETTINGS.sidebarButtonLongPressAction
  );
  const legacyRolePressActions = {
    guest: { short: sidebarButtonShortPressAction, long: sidebarButtonLongPressAction },
    user: { short: sidebarButtonShortPressAction, long: sidebarButtonLongPressAction },
    'co-admin': { short: sidebarButtonShortPressAction, long: sidebarButtonLongPressAction },
    admin: { short: sidebarButtonShortPressAction, long: sidebarButtonLongPressAction },
  };
  const sidebarButtonPressActions = normalizeSidebarButtonPressActions(
    raw.sidebarButtonPressActions,
    legacyRolePressActions
  );
  const hideSidebarAppSettingsLink = raw.hideSidebarAppSettingsLink === undefined
    ? DEFAULT_GENERAL_SETTINGS.hideSidebarAppSettingsLink
    : Boolean(raw.hideSidebarAppSettingsLink);
  const hideSidebarActivityLink = raw.hideSidebarActivityLink === undefined
    ? DEFAULT_GENERAL_SETTINGS.hideSidebarActivityLink
    : Boolean(raw.hideSidebarActivityLink);
  return {
    serverName: String(raw.serverName || DEFAULT_GENERAL_SETTINGS.serverName || '').trim(),
    remoteUrl: String(raw.remoteUrl || DEFAULT_GENERAL_SETTINGS.remoteUrl || '').trim(),
    localUrl: String(raw.localUrl || DEFAULT_GENERAL_SETTINGS.localUrl || '').trim(),
    basePath: normalizeBasePath(raw.basePath || DEFAULT_GENERAL_SETTINGS.basePath || ''),
    restrictGuests,
    autoOpenSingleAppMenuItem,
    sidebarButtonShortPressAction: sidebarButtonPressActions.user.short,
    sidebarButtonLongPressAction: sidebarButtonPressActions.user.long,
    sidebarButtonPressActions,
    hideSidebarAppSettingsLink,
    hideSidebarActivityLink,
  };
}

function resolveNotificationSettings(config) {
  const raw = config && typeof config.notifications === 'object' ? config.notifications : {};
  const rawMode = String(raw.appriseMode || DEFAULT_NOTIFICATION_SETTINGS.appriseMode || '').trim().toLowerCase();
  const rawDelaySeconds = Number(raw.widgetStatusDelaySeconds);
  const rawPollSeconds = Number(raw.widgetStatusPollSeconds);
  const rawRequestTimeoutMs = Number(raw.widgetStatusRequestTimeoutMs);
  const rawMaxConcurrency = Number(raw.widgetStatusMaxConcurrency);
  const widgetStatusDelaySeconds = Number.isFinite(rawDelaySeconds)
    ? Math.max(5, Math.min(3600, Math.round(rawDelaySeconds)))
    : DEFAULT_NOTIFICATION_SETTINGS.widgetStatusDelaySeconds;
  const widgetStatusPollSeconds = Number.isFinite(rawPollSeconds)
    ? Math.max(15, Math.min(600, Math.round(rawPollSeconds)))
    : DEFAULT_NOTIFICATION_SETTINGS.widgetStatusPollSeconds;
  const widgetStatusRequestTimeoutMs = Number.isFinite(rawRequestTimeoutMs)
    ? Math.max(1000, Math.min(20000, Math.round(rawRequestTimeoutMs)))
    : DEFAULT_NOTIFICATION_SETTINGS.widgetStatusRequestTimeoutMs;
  const widgetStatusMaxConcurrency = Number.isFinite(rawMaxConcurrency)
    ? Math.max(1, Math.min(10, Math.round(rawMaxConcurrency)))
    : DEFAULT_NOTIFICATION_SETTINGS.widgetStatusMaxConcurrency;
  return {
    appriseEnabled: raw.appriseEnabled === undefined
      ? DEFAULT_NOTIFICATION_SETTINGS.appriseEnabled
      : Boolean(raw.appriseEnabled),
    appriseApiUrl: String(raw.appriseApiUrl || DEFAULT_NOTIFICATION_SETTINGS.appriseApiUrl || '').trim(),
    appriseMode: rawMode === 'config-key' ? 'config-key' : 'targets',
    appriseConfigKey: String(raw.appriseConfigKey || DEFAULT_NOTIFICATION_SETTINGS.appriseConfigKey || '').trim(),
    appriseTargets: String(raw.appriseTargets || DEFAULT_NOTIFICATION_SETTINGS.appriseTargets || '').trim(),
    appriseTag: String(raw.appriseTag || DEFAULT_NOTIFICATION_SETTINGS.appriseTag || '').trim(),
    widgetStatusEnabled: raw.widgetStatusEnabled === undefined
      ? DEFAULT_NOTIFICATION_SETTINGS.widgetStatusEnabled
      : Boolean(raw.widgetStatusEnabled),
    widgetStatusDelaySeconds,
    widgetStatusPollSeconds,
    widgetStatusRequestTimeoutMs,
    widgetStatusMaxConcurrency,
  };
}

function normalizeThemeMode(value, fallback = '') {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'day' || raw === 'night') return raw;
  return fallback;
}

function normalizeThemeBrandTheme(value, fallback = DEFAULT_THEME_SETTINGS.brandTheme) {
  const raw = String(value || '').trim().toLowerCase();
  if (ALLOWED_BRAND_THEMES.has(raw)) return raw;
  return ALLOWED_BRAND_THEMES.has(String(fallback || '').trim().toLowerCase())
    ? String(fallback || '').trim().toLowerCase()
    : DEFAULT_THEME_SETTINGS.brandTheme;
}

function normalizeThemeCustomColor(value, fallback = DEFAULT_THEME_SETTINGS.customColor) {
  const raw = String(value || '').trim();
  if (!raw) return String(fallback || DEFAULT_THEME_SETTINGS.customColor).toLowerCase();
  const normalized = raw.startsWith('#') ? raw : `#${raw}`;
  return /^#[0-9a-fA-F]{6}$/.test(normalized)
    ? normalized.toLowerCase()
    : String(fallback || DEFAULT_THEME_SETTINGS.customColor).toLowerCase();
}

function normalizeThemeFlag(value, fallback = false) {
  if (value === undefined || value === null || value === '') return Boolean(fallback);
  if (typeof value === 'boolean') return value;
  const raw = String(value).trim().toLowerCase();
  if (raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on') return true;
  if (raw === '0' || raw === 'false' || raw === 'no' || raw === 'off') return false;
  return Boolean(fallback);
}

function normalizeThemeSettings(settings, fallback = DEFAULT_THEME_SETTINGS) {
  const source = settings && typeof settings === 'object' ? settings : {};
  const base = fallback && typeof fallback === 'object'
    ? {
      ...DEFAULT_THEME_SETTINGS,
      ...fallback,
    }
    : { ...DEFAULT_THEME_SETTINGS };
  return {
    mode: normalizeThemeMode(source.mode, normalizeThemeMode(base.mode, '')),
    brandTheme: normalizeThemeBrandTheme(source.brandTheme, base.brandTheme),
    customColor: normalizeThemeCustomColor(source.customColor, base.customColor),
    sidebarInvert: normalizeThemeFlag(source.sidebarInvert, base.sidebarInvert),
    squareCorners: normalizeThemeFlag(source.squareCorners, base.squareCorners),
    bgMotion: normalizeThemeFlag(source.bgMotion, base.bgMotion),
    carouselFreeScroll: normalizeThemeFlag(source.carouselFreeScroll, base.carouselFreeScroll),
    hideScrollbars: normalizeThemeFlag(source.hideScrollbars, base.hideScrollbars),
  };
}

function resolveThemeDefaults(config) {
  const raw = config && typeof config.themeDefaults === 'object' ? config.themeDefaults : {};
  return normalizeThemeSettings(raw, DEFAULT_THEME_SETTINGS);
}

function resolveUserThemePreferences(config, fallback = DEFAULT_THEME_SETTINGS) {
  const raw = config && typeof config.userThemePreferences === 'object' ? config.userThemePreferences : {};
  const normalized = {};
  Object.entries(raw).forEach(([key, value]) => {
    const normalizedKey = String(key || '').trim().toLowerCase();
    if (!normalizedKey) return;
    normalized[normalizedKey] = normalizeThemeSettings(value, fallback);
  });
  return normalized;
}

function serializeUserThemePreferences(preferences, fallback = DEFAULT_THEME_SETTINGS) {
  if (!preferences || typeof preferences !== 'object') return {};
  const normalized = {};
  Object.entries(preferences).forEach(([key, value]) => {
    const normalizedKey = String(key || '').trim().toLowerCase();
    if (!normalizedKey) return;
    normalized[normalizedKey] = normalizeThemeSettings(value, fallback);
  });
  return normalized;
}

function resolveThemePreferenceKey(user) {
  const source = String(user?.source || '').trim().toLowerCase() === 'plex' ? 'plex' : 'local';
  const username = normalizeUserKey(user?.username || '');
  const email = normalizeUserKey(user?.email || '');
  const identity = source === 'plex'
    ? (email || username)
    : (username || email);
  if (!identity) return '';
  return `${source}:${identity}`;
}

function resolveThemeSettingsForUser(config, user) {
  const defaults = resolveThemeDefaults(config);
  const key = resolveThemePreferenceKey(user);
  if (!key) return defaults;
  const preferences = resolveUserThemePreferences(config, defaults);
  const match = preferences[key];
  return match ? normalizeThemeSettings(match, defaults) : defaults;
}

function parseAppriseTargets(value) {
  return String(value || '')
    .split(/[\n,]/)
    .map((entry) => String(entry || '').trim())
    .filter(Boolean);
}

function normalizeAppriseApiBaseUrl(value) {
  let raw = String(value || '').trim();
  if (!raw) return '';
  if (!/^https?:\/\//i.test(raw)) raw = `http://${raw}`;
  try {
    const parsed = new URL(raw);
    const pathname = String(parsed.pathname || '')
      .replace(/\/notify(?:\/.*)?$/i, '')
      .replace(/\/+$/, '');
    parsed.pathname = pathname || '';
    parsed.search = '';
    parsed.hash = '';
    return parsed.toString().replace(/\/$/, '');
  } catch (err) {
    return '';
  }
}

function normalizeAppriseNotifyUrl(value) {
  const base = normalizeAppriseApiBaseUrl(value);
  if (!base) return '';
  return `${base}/notify`;
}

function normalizeAppriseNotifyKeyUrl(value, key) {
  const base = normalizeAppriseApiBaseUrl(value);
  const token = String(key || '').trim();
  if (!base || !token) return '';
  return `${base}/notify/${encodeURIComponent(token)}`;
}

async function sendAppriseNotification(settings, payload = {}) {
  if (!settings?.appriseEnabled) throw new Error('Apprise notifications are disabled.');
  const mode = String(settings?.appriseMode || 'targets').trim().toLowerCase() === 'config-key'
    ? 'config-key'
    : 'targets';
  const title = String(payload.title || 'Launcharr Notification').trim();
  const body = String(payload.body || '').trim();
  const tag = String(payload.tag || settings?.appriseTag || '').trim();
  const requestBody = {
    title: title || 'Launcharr Notification',
    body: body || 'Launcharr test notification.',
    type: 'info',
    format: 'text',
  };

  let notifyUrl = '';
  if (mode === 'config-key') {
    const configKey = String(settings?.appriseConfigKey || '').trim();
    if (!configKey) throw new Error('Apprise config key is required when mode is Config Key.');
    notifyUrl = normalizeAppriseNotifyKeyUrl(settings?.appriseApiUrl, configKey);
  } else {
    notifyUrl = normalizeAppriseNotifyUrl(settings?.appriseApiUrl);
    const urls = parseAppriseTargets(settings?.appriseTargets);
    if (!urls.length) throw new Error('Add at least one Apprise target URL.');
    requestBody.urls = urls;
  }

  if (!notifyUrl) throw new Error('Apprise API URL is required.');
  if (tag) requestBody.tag = tag;

  let response = null;
  try {
    response = await fetch(notifyUrl, {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(requestBody),
    });
  } catch (err) {
    const cause = err && typeof err === 'object' ? err.cause : null;
    const details = [String(err?.message || 'fetch failed').trim()].filter(Boolean);
    if (cause && typeof cause === 'object') {
      if (cause.code) details.push(`code=${cause.code}`);
      if (cause.address) details.push(`address=${cause.address}`);
      if (cause.port) details.push(`port=${cause.port}`);
    }
    throw new Error(`Failed to reach Apprise API (${details.join(', ')})`);
  }

  if (!response.ok) {
    const message = String(await response.text() || '').trim();
    throw new Error(message || `Apprise request failed (${response.status}).`);
  }
}

function normalizeLocalUsers(items) {
  if (!Array.isArray(items)) return [];
  const normalized = items
    .map((entry) => {
      if (!entry || typeof entry !== 'object') return null;
      const username = String(entry.username || '').trim();
      const email = String(entry.email || '').trim();
      const role = String(entry.role || 'admin').trim().toLowerCase();
      const passwordHash = String(entry.passwordHash || '').trim();
      const salt = String(entry.salt || '').trim();
      if (!username || !passwordHash || !salt) return null;
      const rawAvatar = String(entry.avatar || '').trim();
      const avatar = normalizeStoredAvatarPath(rawAvatar);
      const createdByRaw = String(entry.createdBy || '').trim().toLowerCase();
      const createdBy = createdByRaw === 'setup' ? 'setup' : 'system';
      const setupAccount = entry.setupAccount === true || createdBy === 'setup';
      const systemCreated = entry.systemCreated !== false;
      return {
        username,
        email,
        role: normalizeLocalRole(role, 'admin'),
        passwordHash,
        salt,
        avatar,
        createdBy,
        setupAccount,
        systemCreated,
        createdAt: entry.createdAt ? String(entry.createdAt) : new Date().toISOString(),
      };
    })
    .filter(Boolean);

  const setupAdminKey = resolveSetupAdminUserKey(normalized);
  return normalized.map((entry) => {
    const usernameKey = normalizeUserKey(entry.username || '');
    const isSetupAdmin = Boolean(setupAdminKey && usernameKey && setupAdminKey === usernameKey);
    return {
      ...entry,
      isSetupAdmin,
      avatarFallback: resolveLocalAvatarFallback({ ...entry, isSetupAdmin }),
    };
  });
}

function normalizeLocalRole(value, fallback = 'user') {
  const role = String(value || '').trim().toLowerCase();
  if (role === 'admin' || role === 'co-admin' || role === 'user') return role;
  const fallbackRole = String(fallback || '').trim().toLowerCase();
  if (fallbackRole === 'admin' || fallbackRole === 'co-admin' || fallbackRole === 'user') return fallbackRole;
  return 'user';
}

function isValidEmail(value) {
  const email = String(value || '').trim();
  if (!email) return false;
  return email.includes('@');
}

function findLocalUserIndex(users, identity = {}) {
  const username = normalizeUserKey(identity.username || '');
  const email = normalizeUserKey(identity.email || '');
  if (!Array.isArray(users) || !users.length) return -1;
  return users.findIndex((entry) => {
    const entryUsername = normalizeUserKey(entry?.username || '');
    const entryEmail = normalizeUserKey(entry?.email || '');
    if (username && entryUsername === username) return true;
    if (email && entryEmail && entryEmail === email) return true;
    return false;
  });
}

function resolveLocalUsers(config) {
  return normalizeLocalUsers(config?.users);
}

function serializeLocalUsers(users) {
  if (!Array.isArray(users)) return [];
  return users
    .map((entry) => {
      if (!entry || typeof entry !== 'object') return null;
      const username = String(entry.username || '').trim();
      const email = String(entry.email || '').trim();
      const role = normalizeLocalRole(entry.role, 'user');
      const passwordHash = String(entry.passwordHash || '').trim();
      const salt = String(entry.salt || '').trim();
      if (!username || !passwordHash || !salt) return null;
      const createdByRaw = String(entry.createdBy || '').trim().toLowerCase();
      const createdBy = createdByRaw === 'setup' ? 'setup' : 'system';
      return {
        username,
        email,
        role,
        passwordHash,
        salt,
        avatar: normalizeStoredAvatarPath(entry.avatar || ''),
        createdBy,
        setupAccount: entry.setupAccount === true || createdBy === 'setup',
        systemCreated: entry.systemCreated !== false,
        createdAt: entry.createdAt ? String(entry.createdAt) : new Date().toISOString(),
      };
    })
    .filter(Boolean);
}

function hasLocalAdmin(config) {
  return resolveLocalUsers(config).some((user) => user.role === 'admin');
}

function normalizeUserKey(value) {
  return String(value || '').trim().toLowerCase();
}

function resolveSetupAdminUserKey(users) {
  if (!Array.isArray(users) || !users.length) return '';
  const explicit = users.find((entry) => entry?.setupAccount === true || entry?.createdBy === 'setup');
  if (explicit) return normalizeUserKey(explicit.username || explicit.email || '');

  const adminUsers = users.filter((entry) => entry?.role === 'admin');
  if (!adminUsers.length) return '';

  const sorted = adminUsers
    .slice()
    .sort((a, b) => {
      const aTime = Date.parse(String(a?.createdAt || '').trim());
      const bTime = Date.parse(String(b?.createdAt || '').trim());
      const safeATime = Number.isNaN(aTime) ? Number.MAX_SAFE_INTEGER : aTime;
      const safeBTime = Number.isNaN(bTime) ? Number.MAX_SAFE_INTEGER : bTime;
      if (safeATime !== safeBTime) return safeATime - safeBTime;
      return String(a?.username || '').localeCompare(String(b?.username || ''));
    });
  const firstAdmin = sorted[0];
  return normalizeUserKey(firstAdmin?.username || firstAdmin?.email || '');
}

function resolveLocalAvatarFallback(user) {
  const isSetupAdmin = Boolean(
    user?.isSetupAdmin
    || user?.setupAccount === true
    || String(user?.createdBy || '').trim().toLowerCase() === 'setup'
  );
  return isSetupAdmin ? '/icons/role.svg' : '/icons/user-profile.svg';
}

function normalizeStoredAvatarPath(value) {
  const avatar = String(value || '').trim();
  if (!avatar) return '';
  if (avatar.startsWith('/icons/')) return avatar;
  if (avatar.startsWith('http://') || avatar.startsWith('https://')) return avatar;
  return '';
}

function resolveUserLogins(config) {
  const raw = config && typeof config.userLogins === 'object' ? config.userLogins : {};
  const plex = raw && typeof raw.plex === 'object' ? raw.plex : {};
  const launcharr = raw && typeof raw.launcharr === 'object' ? raw.launcharr : {};
  return { plex, launcharr };
}

function updateUserLogins(config, { identifier, plex, launcharr }) {
  const key = normalizeUserKey(identifier);
  if (!key) return config;
  const store = resolveUserLogins(config);
  const now = new Date().toISOString();
  const next = {
    plex: { ...store.plex },
    launcharr: { ...store.launcharr },
  };
  if (plex) next.plex[key] = typeof plex === 'string' ? plex : now;
  if (launcharr) next.launcharr[key] = typeof launcharr === 'string' ? launcharr : now;
  return { ...config, userLogins: next };
}

function normalizePlexLastSeen(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const numeric = Number(raw);
  if (Number.isFinite(numeric) && numeric > 0) {
    const ms = numeric > 1e12 ? numeric : numeric * 1000;
    return new Date(ms).toISOString();
  }
  const parsed = Date.parse(raw);
  if (!Number.isNaN(parsed)) return new Date(parsed).toISOString();
  return '';
}

function resolvePlexHistoryLastSeen(xmlText) {
  const map = {};
  const tags = String(xmlText || '').match(/<[^>]+>/g) || [];
  tags.forEach((tag) => {
    if (!/(accountID|userID|userId|accountId|username|user)=/i.test(tag)) return;
    if (!/(viewedAt|lastViewedAt|viewed_at|last_viewed_at)=/i.test(tag)) return;
    const attrs = {};
    tag.replace(/(\w+)="([^"]*)"/g, (_m, key, value) => {
      attrs[key] = value;
      return '';
    });
    const rawSeen = attrs.viewedAt || attrs.lastViewedAt || attrs.viewed_at || attrs.last_viewed_at || '';
    const seenIso = normalizePlexLastSeen(rawSeen);
    if (!seenIso) return;
    const keys = [
      attrs.accountID,
      attrs.accountId,
      attrs.userID,
      attrs.userId,
      attrs.user,
      attrs.username,
    ]
      .map((value) => String(value || '').trim())
      .filter(Boolean);
    keys.forEach((key) => {
      const existing = map[key];
      if (!existing || new Date(existing) < new Date(seenIso)) {
        map[key] = seenIso;
      }
      const lower = key.toLowerCase();
      const existingLower = map[lower];
      if (!existingLower || new Date(existingLower) < new Date(seenIso)) {
        map[lower] = seenIso;
      }
    });
  });
  return map;
}

async function fetchPlexHistoryLastSeenMap(baseUrl, token) {
  if (!baseUrl || !token) return {};
  const paths = ['/status/sessions/history/all', '/status/sessions/history'];
  for (let index = 0; index < paths.length; index += 1) {
    const url = buildAppApiUrl(baseUrl, paths[index]);
    url.searchParams.set('X-Plex-Token', token);
    url.searchParams.set('sort', 'viewedAt:desc');
    url.searchParams.set('count', '2000');
    try {
      const res = await fetch(url.toString(), { headers: { Accept: 'application/xml' } });
      const xmlText = await res.text();
      if (!res.ok) continue;
      const map = resolvePlexHistoryLastSeen(xmlText);
      if (Object.keys(map).length) return map;
    } catch (err) {
      continue;
    }
  }
  return {};
}

function hashPassword(password, salt) {
  return crypto.scryptSync(String(password || ''), salt, 64).toString('hex');
}

function verifyPassword(password, user) {
  if (!user?.passwordHash || !user?.salt) return false;
  const candidate = hashPassword(password, user.salt);
  try {
    return crypto.timingSafeEqual(
      Buffer.from(candidate, 'hex'),
      Buffer.from(user.passwordHash, 'hex')
    );
  } catch (err) {
    return false;
  }
}

function setSessionUser(req, user, source = 'local') {
  const normalizedSource = String(source || '').trim().toLowerCase() || 'local';
  const avatar = normalizeStoredAvatarPath(user?.avatar || '');
  const avatarFallback = normalizedSource === 'local'
    ? resolveLocalAvatarFallback(user)
    : '/icons/user-profile.svg';
  req.session.user = {
    username: user.username,
    email: user.email || '',
    avatar,
    avatarFallback,
    role: user.role || 'admin',
    source: normalizedSource,
  };
  req.session.viewRole = null;
}

function resolveCombinedQueueDisplaySettings(config, key) {
  const raw = config && typeof config[key] === 'object' ? config[key] : {};
  const rowsValue = Number(raw.queueVisibleRows);
  const queueVisibleRows = Number.isFinite(rowsValue)
    ? Math.max(5, Math.min(50, rowsValue))
    : DEFAULT_QUEUE_DISPLAY.queueVisibleRows;
  const resolveBoolean = (value, fallback) => (value === undefined ? fallback : Boolean(value));
  return {
    queueShowDetail: resolveBoolean(raw.queueShowDetail, DEFAULT_QUEUE_DISPLAY.queueShowDetail),
    queueShowSubDetail: resolveBoolean(raw.queueShowSubDetail, DEFAULT_QUEUE_DISPLAY.queueShowSubDetail),
    queueShowSize: resolveBoolean(raw.queueShowSize, DEFAULT_QUEUE_DISPLAY.queueShowSize),
    queueShowProtocol: resolveBoolean(raw.queueShowProtocol, DEFAULT_QUEUE_DISPLAY.queueShowProtocol),
    queueShowTimeLeft: resolveBoolean(raw.queueShowTimeLeft, DEFAULT_QUEUE_DISPLAY.queueShowTimeLeft),
    queueShowProgress: resolveBoolean(raw.queueShowProgress, DEFAULT_QUEUE_DISPLAY.queueShowProgress),
    queueVisibleRows,
  };
}

function slugifyId(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 32);
}

function saveCustomIcon(iconDataUrl, targetDir, nameHint = '') {
  if (!iconDataUrl) return { ok: false, iconPath: '', error: 'missing-icon-data' };
  const match = String(iconDataUrl).match(/^data:(image\/[a-zA-Z0-9.+-]+);base64,([A-Za-z0-9+/=]+)$/);
  if (!match) return { ok: false, iconPath: '', error: 'invalid-data-url' };
  const mime = match[1].toLowerCase();
  const data = match[2];
  const extMap = {
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'image/jpg': 'jpg',
    'image/svg+xml': 'svg',
    'image/webp': 'webp',
  };
  const ext = extMap[mime];
  if (!ext) return { ok: false, iconPath: '', error: 'unsupported-mime' };
  const baseName = String(nameHint || '').replace(/\.[^/.]+$/, '').trim();
  const nameSlug = slugifyId(baseName);
  if (!nameSlug) return { ok: false, iconPath: '', error: 'invalid-icon-name' };
  let buffer = null;
  try {
    buffer = Buffer.from(data, 'base64');
  } catch (_err) {
    return { ok: false, iconPath: '', error: 'decode-failed' };
  }
  if (!buffer || !buffer.length) return { ok: false, iconPath: '', error: 'empty-image-data' };
  try {
    if (!fs.existsSync(targetDir)) fs.mkdirSync(targetDir, { recursive: true });
    const filename = `${nameSlug}.${ext}`;
    const fullPath = path.join(targetDir, filename);
    fs.writeFileSync(fullPath, buffer);
    return { ok: true, iconPath: filename, error: '' };
  } catch (err) {
    return { ok: false, iconPath: '', error: 'write-failed', detail: String(err?.message || '').trim() };
  }
}

function detectAvatarMimeFromBuffer(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 12) return '';
  if (
    buffer.length >= 8
    && buffer[0] === 0x89
    && buffer[1] === 0x50
    && buffer[2] === 0x4E
    && buffer[3] === 0x47
    && buffer[4] === 0x0D
    && buffer[5] === 0x0A
    && buffer[6] === 0x1A
    && buffer[7] === 0x0A
  ) {
    return 'image/png';
  }
  if (buffer[0] === 0xFF && buffer[1] === 0xD8 && buffer[2] === 0xFF) {
    return 'image/jpeg';
  }
  if (
    buffer[0] === 0x52
    && buffer[1] === 0x49
    && buffer[2] === 0x46
    && buffer[3] === 0x46
    && buffer[8] === 0x57
    && buffer[9] === 0x45
    && buffer[10] === 0x42
    && buffer[11] === 0x50
  ) {
    return 'image/webp';
  }
  return '';
}

function parseUserAvatarDataUrl(dataUrl) {
  const raw = String(dataUrl || '').trim();
  if (!raw) return { ok: false, error: 'Avatar image data is missing.' };
  const match = raw.match(/^data:(image\/[a-zA-Z0-9.+-]+);base64,([A-Za-z0-9+/=]+)$/);
  if (!match) return { ok: false, error: 'Avatar must be a valid PNG, JPG, or WEBP image.' };

  const requestedMime = String(match[1] || '').trim().toLowerCase();
  const mime = requestedMime === 'image/jpg' ? 'image/jpeg' : requestedMime;
  if (!USER_AVATAR_ALLOWED_MIME.has(mime)) {
    return { ok: false, error: 'Avatar type is not allowed. Use PNG, JPG, or WEBP.' };
  }

  const encoded = String(match[2] || '').trim();
  let buffer = null;
  try {
    buffer = Buffer.from(encoded, 'base64');
  } catch (err) {
    return { ok: false, error: 'Avatar image could not be decoded.' };
  }
  if (!buffer || !buffer.length) return { ok: false, error: 'Avatar image is empty.' };
  if (buffer.length > MAX_USER_AVATAR_BYTES) {
    return { ok: false, error: 'Avatar image is too large. Maximum size is 2 MB.' };
  }

  const decodedMime = detectAvatarMimeFromBuffer(buffer);
  if (!decodedMime || decodedMime !== mime) {
    return { ok: false, error: 'Avatar image content does not match the selected file type.' };
  }

  const ext = mime === 'image/png'
    ? 'png'
    : (mime === 'image/webp' ? 'webp' : 'jpg');
  return { ok: true, mime, ext, buffer };
}

function saveCustomUserAvatar(buffer, ext, nameHint = '') {
  if (!Buffer.isBuffer(buffer) || !buffer.length) return '';
  const safeExt = String(ext || '').trim().toLowerCase();
  if (!['png', 'jpg', 'webp'].includes(safeExt)) return '';
  try {
    if (!fs.existsSync(USER_AVATAR_DIR)) fs.mkdirSync(USER_AVATAR_DIR, { recursive: true });
    const baseName = slugifyId(nameHint) || 'avatar';
    const filename = `${baseName}-${Date.now()}.${safeExt}`;
    const fullPath = path.join(USER_AVATAR_DIR, filename);
    fs.writeFileSync(fullPath, buffer);
    return `${USER_AVATAR_BASE}/${filename}`;
  } catch (err) {
    return '';
  }
}

function deleteCustomIcon(iconPath, allowedBases) {
  const safePath = String(iconPath || '').trim();
  if (!safePath.startsWith('/icons/')) return false;
  const filename = path.basename(safePath);
  const baseMatch = allowedBases.find((base) => safePath.startsWith(base));
  if (!baseMatch) return false;
  const relativeBase = baseMatch.replace(/^\/+/, '');
  const absoluteDir = path.join(__dirname, '..', 'public', relativeBase);
  const fullPath = path.join(absoluteDir, filename);
  if (!fullPath.startsWith(absoluteDir)) return false;
  try {
    if (fs.existsSync(fullPath)) fs.unlinkSync(fullPath);
    return true;
  } catch (err) {
    return false;
  }
}

function saveCustomAppIcon(iconDataUrl, appId, nameHint = '') {
  if (!iconDataUrl || !appId) return { iconPath: '', iconData: '' };
  const match = String(iconDataUrl).match(/^data:(image\/[a-zA-Z0-9.+-]+);base64,(.+)$/);
  if (!match) return { iconPath: '', iconData: iconDataUrl };
  const mime = match[1].toLowerCase();
  const data = match[2];
  const extMap = {
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'image/jpg': 'jpg',
    'image/svg+xml': 'svg',
    'image/webp': 'webp',
  };
  const ext = extMap[mime];
  if (!ext) return { iconPath: '', iconData: iconDataUrl };
  try {
    const dir = path.join(__dirname, '..', 'public', 'icons', 'custom', 'apps');
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const nameSlug = slugifyId(nameHint) || 'custom-app';
    const filename = `${nameSlug}-${appId}.${ext}`;
    const fullPath = path.join(dir, filename);
    const buffer = Buffer.from(data, 'base64');
    fs.writeFileSync(fullPath, buffer);
    return { iconPath: `/icons/custom/${filename}`, iconData: '' };
  } catch (err) {
    return { iconPath: '', iconData: iconDataUrl };
  }
}

function resolveLogSettings(config) {
  const raw = config && typeof config.logs === 'object' ? config.logs : {};
  const maxEntries = Number(raw.maxEntries);
  const maxDays = Number(raw.maxDays);
  const visibleRows = Number(raw.visibleRows);
  return {
    maxEntries: Number.isFinite(maxEntries) && maxEntries > 0 ? Math.floor(maxEntries) : DEFAULT_LOG_SETTINGS.maxEntries,
    maxDays: Number.isFinite(maxDays) && maxDays > 0 ? Math.floor(maxDays) : DEFAULT_LOG_SETTINGS.maxDays,
    visibleRows: Number.isFinite(visibleRows) && visibleRows > 0 ? Math.floor(visibleRows) : DEFAULT_LOG_SETTINGS.visibleRows,
  };
}

function resolveOnboardingSettings(config) {
  const raw = config && typeof config.onboarding === 'object' ? config.onboarding : {};
  return {
    quickStartPending: raw.quickStartPending === undefined
      ? DEFAULT_ONBOARDING_SETTINGS.quickStartPending
      : Boolean(raw.quickStartPending),
  };
}

function hasActiveOnboardingApps(config) {
  const apps = Array.isArray(config?.apps) ? config.apps : [];
  return apps.some((appItem) => !appItem?.removed);
}

function shouldShowQuickStartOnboarding(config) {
  const onboarding = resolveOnboardingSettings(config);
  if (!onboarding.quickStartPending) return false;
  return !hasActiveOnboardingApps(config);
}

function applyLogRetention(entries, settings) {
  const maxEntries = settings?.maxEntries || DEFAULT_LOG_SETTINGS.maxEntries;
  const maxDays = settings?.maxDays || DEFAULT_LOG_SETTINGS.maxDays;
  const now = Date.now();
  const cutoff = Number.isFinite(maxDays) && maxDays > 0 ? now - (maxDays * 24 * 60 * 60 * 1000) : null;
  const filtered = Array.isArray(entries)
    ? entries.filter((entry) => {
        if (!cutoff) return true;
        const ts = entry && entry.ts ? Date.parse(entry.ts) : NaN;
        return Number.isFinite(ts) ? ts >= cutoff : true;
      })
    : [];
  if (!Number.isFinite(maxEntries) || maxEntries <= 0) return filtered;
  if (filtered.length <= maxEntries) return filtered;
  return filtered.slice(filtered.length - maxEntries);
}

function persistLogsToDisk(settings) {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    const pruned = applyLogRetention(LOG_BUFFER, settings);
    const tmpPath = LOG_PATH + '.tmp';
    fs.writeFileSync(tmpPath, JSON.stringify({ items: pruned }, null, 2));
    fs.renameSync(tmpPath, LOG_PATH);
  } catch (err) {
    // avoid crashing on disk errors
  }
}

function loadLogsFromDisk(settings) {
  try {
    if (!fs.existsSync(LOG_PATH)) return;
    const raw = fs.readFileSync(LOG_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    const items = Array.isArray(parsed?.items) ? parsed.items : [];
    const pruned = applyLogRetention(items, settings);
    LOG_BUFFER.splice(0, LOG_BUFFER.length, ...pruned);
  } catch (err) {
    // ignore invalid log file
  }
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
  if (pruned.length !== LOG_BUFFER.length) {
    LOG_BUFFER.splice(0, LOG_BUFFER.length, ...pruned);
  }
  persistLogsToDisk(settings);
}

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.set('trust proxy', TRUST_PROXY_SETTING);

app.use(httpAccessLogMiddleware);
app.use((req, res, next) => {
  // Baseline hardening headers that are compatible with Launcharr's iframe app pages.
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('X-Permitted-Cross-Domain-Policies', 'none');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=(), payment=(), usb=()');
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin-allow-popups');
  res.setHeader('Cross-Origin-Resource-Policy', 'same-origin');
  res.setHeader('Content-Security-Policy', "frame-ancestors 'self'; object-src 'none'; base-uri 'self'; form-action 'self'");
  next();
});
app.use(express.static(path.join(__dirname, '..', 'public')));
app.use(express.urlencoded({ extended: false, limit: URLENCODED_BODY_LIMIT }));
app.use(express.json({ limit: JSON_BODY_LIMIT }));
app.use((req, res, next) => {
  res.locals.assetVersion = ASSET_VERSION;
  const generalSettings = resolveGeneralSettings(loadConfig());
  res.locals.autoOpenSingleAppMenuItem = Boolean(generalSettings.autoOpenSingleAppMenuItem);
  res.locals.sidebarButtonPressActions = generalSettings.sidebarButtonPressActions;
  next();
});
app.use(
  cookieSession({
    name: 'launcharr_session',
    secret: SESSION_SECRET,
    httpOnly: true,
    sameSite: 'lax',
    secure: isSecureEnv(),
    maxAge: 7 * 24 * 60 * 60 * 1000,
  })
);
app.use((req, res, next) => {
  if (!req.session || typeof req.session !== 'object') return next();
  if (!req.session.csrfToken) req.session.csrfToken = crypto.randomBytes(24).toString('hex');
  res.locals.csrfToken = req.session.csrfToken;
  return next();
});
app.use(csrfProtectionMiddleware);
app.use('/login', createRequestBodySizeGuard(32 * 1024));
app.use('/setup', createRequestBodySizeGuard(32 * 1024));
app.use('/logout', createRequestBodySizeGuard(8 * 1024));
app.use('/switch-view', createRequestBodySizeGuard(8 * 1024));
app.use('/settings/local-users', createRequestBodySizeGuard(32 * 1024));
app.use('/api/plex/pin', createRequestBodySizeGuard(64 * 1024));
app.use('/api/logs/client', createRequestBodySizeGuard(64 * 1024));
app.use((req, res, next) => {
  const sessionUser = req.session?.user;
  if (!sessionUser || typeof sessionUser !== 'object') return next();

  const source = String(sessionUser.source || '').trim().toLowerCase();
  if (source === 'local') {
    const config = loadConfig();
    const users = resolveLocalUsers(config);
    const index = findLocalUserIndex(users, {
      username: sessionUser.username,
      email: sessionUser.email,
    });
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
      return next();
    }
  }

  req.session.user = {
    ...sessionUser,
    avatar: normalizeStoredAvatarPath(sessionUser.avatar || ''),
    avatarFallback: sessionUser.avatarFallback || '/icons/user-profile.svg',
    source: source || sessionUser.source || 'local',
  };
  return next();
});
app.use((req, res, next) => {
  const config = loadConfig();
  res.locals.themeDefaults = resolveThemeSettingsForUser(config, req.session?.user);
  const actualRole = getActualRole(req);
  res.locals.roleViewDashboardAvailability = actualRole === 'admin'
    ? buildRoleViewDashboardAvailability(config)
    : {};
  next();
});

loadLogsFromDisk(resolveLogSettings(loadConfig()));
// DEBUG: confirm Plex client id creation on startup.
console.log(`[plex] client id=${CLIENT_ID}`);

// GET / — moved to src/routes/auth.js

// GET /login — moved to src/routes/auth.js

// POST /login — moved to src/routes/auth.js

// GET /setup — moved to src/routes/auth.js

// POST /setup — moved to src/routes/auth.js

// GET /auth/plex — moved to src/routes/auth.js

// POST /api/plex/pin — moved to src/routes/auth.js

// GET /oauth/callback — moved to src/routes/auth.js

// GET /api/plex/pin/status — moved to src/routes/auth.js


// GET /dashboard — moved to src/routes/pages.js

// GET /apps/:id — moved to src/routes/pages.js

// GET /apps/:id/activity — moved to src/routes/pages.js

// GET /apps/:id/launch — moved to src/routes/pages.js

// GET /apps/:id/settings — moved to src/routes/pages.js

// GET /settings — moved to src/routes/pages.js

// POST /settings/* — moved to src/routes/settings.js
// POST /user-settings/* — moved to src/routes/settings.js
// POST /apps/:id/settings — moved to src/routes/settings.js
// GET /settings/plex-users — moved to src/routes/settings.js

// GET /api/plex/token — moved to src/routes/api-plex.js

// GET /api/plex/machine — moved to src/routes/api-plex.js
// /api/romm/viewer-session-test — moved to src/routes/api-specialty.js

// GET /api/onboarding/quick-start — moved to src/routes/api-util.js

// POST /api/onboarding/quick-start/dismiss — moved to src/routes/api-util.js

// GET /api/version — moved to src/routes/api-util.js

// GET /api/plex/discovery/watchlisted — moved to src/routes/api-plex.js

// GET /api/plex/discovery/details — moved to src/routes/api-plex.js

// POST /api/plex/discovery/watchlist — moved to src/routes/api-plex.js

function resolveJellyfinCandidates(appItem, req) {
  return uniqueList([
    normalizeBaseUrl(appItem?.remoteUrl || '', { stripWeb: true }),
    normalizeBaseUrl(resolveLaunchUrl(appItem, req), { stripWeb: true }),
    normalizeBaseUrl(appItem?.localUrl || '', { stripWeb: true }),
    normalizeBaseUrl(appItem?.url || '', { stripWeb: true }),
  ]).filter(Boolean);
}

function resolveEmbyCandidates(appItem, req) {
  return uniqueList([
    normalizeBaseUrl(appItem?.remoteUrl || '', { stripWeb: true }),
    normalizeBaseUrl(resolveLaunchUrl(appItem, req), { stripWeb: true }),
    normalizeBaseUrl(appItem?.localUrl || '', { stripWeb: true }),
    normalizeBaseUrl(appItem?.url || '', { stripWeb: true }),
  ]).filter(Boolean);
}

function buildJellyfinImageUrl({ baseUrl, itemId, type, apiKey, tag = '', index = '' }) {
  if (!baseUrl || !itemId || !type) return '';
  const safeType = String(type).trim();
  const safeId = encodeURIComponent(String(itemId).trim());
  const url = buildAppApiUrl(baseUrl, `Items/${safeId}/Images/${safeType}${index !== '' ? `/${encodeURIComponent(String(index))}` : ''}`);
  if (apiKey) url.searchParams.set('api_key', apiKey);
  if (tag) url.searchParams.set('tag', String(tag));
  url.searchParams.set('quality', '90');
  return url.toString();
}

function buildEmbyImageUrl({ baseUrl, itemId, type, apiKey, tag = '', index = '' }) {
  if (!baseUrl || !itemId || !type) return '';
  const safeType = String(type).trim();
  const safeId = encodeURIComponent(String(itemId).trim());
  const url = buildAppApiUrl(baseUrl, `emby/Items/${safeId}/Images/${safeType}${index !== '' ? `/${encodeURIComponent(String(index))}` : ''}`);
  if (apiKey) url.searchParams.set('api_key', apiKey);
  if (tag) url.searchParams.set('tag', String(tag));
  url.searchParams.set('quality', '90');
  return url.toString();
}

function formatDurationFromTicks(ticks) {
  const value = Number(ticks);
  if (!Number.isFinite(value) || value <= 0) return '';
  const totalMinutes = Math.round(value / 10000000 / 60);
  if (!Number.isFinite(totalMinutes) || totalMinutes <= 0) return '';
  if (totalMinutes >= 60) {
    const hours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;
    return minutes ? `${hours}h ${minutes}m` : `${hours}h`;
  }
  return `${totalMinutes}m`;
}

function formatRelativeTime(value) {
  const parsed = Date.parse(String(value || ''));
  if (!Number.isFinite(parsed)) return '';
  const diffMs = Date.now() - parsed;
  const diffMinutes = Math.max(1, Math.round(diffMs / 60000));
  if (diffMinutes < 60) return `${diffMinutes}m ago`;
  const diffHours = Math.round(diffMinutes / 60);
  if (diffHours < 48) return `${diffHours}h ago`;
  const diffDays = Math.round(diffHours / 24);
  return `${diffDays}d ago`;
}

function toPaddedEpisode(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return '';
  return String(Math.floor(numeric)).padStart(2, '0');
}

function mapJellyfinKind(typeValue) {
  const raw = String(typeValue || '').trim().toLowerCase();
  if (raw === 'movie' || raw === 'trailer') return 'movie';
  return 'tv';
}

async function fetchJellyfinJson({ candidates, apiKey, path, query }) {
  let lastError = '';
  for (let index = 0; index < candidates.length; index += 1) {
    const baseUrl = candidates[index];
    if (!baseUrl) continue;
    const upstreamUrl = buildAppApiUrl(baseUrl, path);
    Object.entries(query || {}).forEach(([key, value]) => {
      if (value === undefined || value === null || value === '') return;
      upstreamUrl.searchParams.set(key, String(value));
    });
    if (apiKey) upstreamUrl.searchParams.set('api_key', apiKey);

    try {
      const upstreamRes = await fetch(upstreamUrl.toString(), {
        headers: {
          Accept: 'application/json',
          'X-Emby-Token': apiKey,
        },
      });
      const text = await upstreamRes.text();
      if (!upstreamRes.ok) {
        const bodyMessage = String(text || '').trim();
        lastError = `Jellyfin request failed (${upstreamRes.status}) via ${baseUrl}${bodyMessage ? `: ${bodyMessage.slice(0, 220)}` : ''}`;
        continue;
      }
      try {
        return { baseUrl, payload: JSON.parse(text || '{}') };
      } catch (err) {
        lastError = `Invalid JSON response from Jellyfin via ${baseUrl}.`;
      }
    } catch (err) {
      const reason = safeMessage(err) || 'fetch failed';
      lastError = `${reason} via ${baseUrl}`;
    }
  }
  throw new Error(lastError || 'Failed to reach Jellyfin.');
}

async function fetchJellyfinRecentItems({ candidates, apiKey, limit, mediaType }) {
  const usersResponse = await fetchJellyfinJson({
    candidates,
    apiKey,
    path: '/Users',
    query: {},
  });
  const users = Array.isArray(usersResponse.payload) ? usersResponse.payload : [];
  const activeUser = users.find((user) => !user?.Policy?.IsDisabled) || users[0];
  const userId = String(activeUser?.Id || '').trim();
  if (!userId) throw new Error('No Jellyfin user available for latest items.');

  const includeItemTypes = mediaType === 'movie'
    ? 'Movie'
    : (mediaType === 'show' ? 'Series,Episode' : 'Movie,Series,Episode');

  const latestResponse = await fetchJellyfinJson({
    candidates,
    apiKey,
    path: `/Users/${encodeURIComponent(userId)}/Items/Latest`,
    query: {
      Limit: limit,
      IncludeItemTypes: includeItemTypes,
      Fields: 'Overview,ProviderIds',
      ImageTypeLimit: 1,
      EnableImageTypes: 'Primary,Backdrop',
    },
  });
  const items = Array.isArray(latestResponse.payload) ? latestResponse.payload : [];
  return { baseUrl: latestResponse.baseUrl, items };
}

async function fetchEmbyJson({ candidates, apiKey, path, query }) {
  let lastError = '';
  const pathCandidates = [path, `/emby${path}`];
  for (let index = 0; index < candidates.length; index += 1) {
    const baseUrl = candidates[index];
    if (!baseUrl) continue;
    for (let pathIndex = 0; pathIndex < pathCandidates.length; pathIndex += 1) {
      const attemptPath = pathCandidates[pathIndex];
      const upstreamUrl = buildAppApiUrl(baseUrl, attemptPath);
      Object.entries(query || {}).forEach(([key, value]) => {
        if (value === undefined || value === null || value === '') return;
        upstreamUrl.searchParams.set(key, String(value));
      });
      if (apiKey) upstreamUrl.searchParams.set('api_key', apiKey);

      try {
        const upstreamRes = await fetch(upstreamUrl.toString(), {
          headers: {
            Accept: 'application/json',
            'X-Emby-Token': apiKey,
          },
        });
        const text = await upstreamRes.text();
        if (!upstreamRes.ok) {
          const bodyMessage = String(text || '').trim();
          lastError = `Emby request failed (${upstreamRes.status}) via ${baseUrl}${bodyMessage ? `: ${bodyMessage.slice(0, 220)}` : ''}`;
          continue;
        }
        try {
          return { baseUrl, payload: JSON.parse(text || '{}') };
        } catch (err) {
          lastError = `Invalid JSON response from Emby via ${baseUrl}.`;
        }
      } catch (err) {
        const reason = safeMessage(err) || 'fetch failed';
        lastError = `${reason} via ${baseUrl}`;
      }
    }
  }
  throw new Error(lastError || 'Failed to reach Emby.');
}

async function fetchEmbyRecentItems({ candidates, apiKey, limit, mediaType }) {
  const usersResponse = await fetchEmbyJson({
    candidates,
    apiKey,
    path: '/Users',
    query: {},
  });
  const users = Array.isArray(usersResponse.payload) ? usersResponse.payload : [];
  const activeUser = users.find((user) => !user?.Policy?.IsDisabled) || users[0];
  const userId = String(activeUser?.Id || '').trim();
  if (!userId) throw new Error('No Emby user available for latest items.');

  const includeItemTypes = mediaType === 'movie'
    ? 'Movie'
    : (mediaType === 'show' ? 'Series,Episode' : 'Movie,Series,Episode');

  const latestResponse = await fetchEmbyJson({
    candidates,
    apiKey,
    path: `/Users/${encodeURIComponent(userId)}/Items/Latest`,
    query: {
      Limit: limit,
      IncludeItemTypes: includeItemTypes,
      Fields: 'Overview,ProviderIds',
      ImageTypeLimit: 1,
      EnableImageTypes: 'Primary,Backdrop',
    },
  });
  const items = Array.isArray(latestResponse.payload) ? latestResponse.payload : [];
  return { baseUrl: latestResponse.baseUrl, items };
}

// GET /api/jellyfin/active — moved to src/routes/api-media.js

// GET /api/jellyfin/recent — moved to src/routes/api-media.js

// GET /api/emby/active — moved to src/routes/api-media.js

// GET /api/emby/recent — moved to src/routes/api-media.js

function mapSeerrRequestStatus(statusValue) {
  const numeric = Number(statusValue);
  if (numeric === 5) return 'available';
  if (numeric === 3) return 'declined';
  return 'requested';
}

function mapSeerrFilter(statusValue) {
  const value = String(statusValue || '').trim().toLowerCase();
  if (value === 'available') return 'available';
  if (value === 'declined') return 'declined';
  if (value === 'requested') return 'pending';
  return 'all';
}

async function fetchSeerrJson({ candidates, apiKey, path, query }) {
  let lastError = '';
  const attempted = [];
  for (let index = 0; index < candidates.length; index += 1) {
    const baseUrl = candidates[index];
    if (!baseUrl) continue;
    attempted.push(baseUrl);
    const upstreamUrl = buildAppApiUrl(baseUrl, path);
    Object.entries(query || {}).forEach(([key, value]) => {
      if (value === undefined || value === null || value === '') return;
      upstreamUrl.searchParams.set(key, String(value));
    });

    try {
      const upstreamRes = await fetch(upstreamUrl.toString(), {
        headers: {
          Accept: 'application/json',
          'X-API-Key': apiKey,
        },
      });
      const text = await upstreamRes.text();
      if (!upstreamRes.ok) {
        const bodyMessage = String(text || '').trim();
        lastError = `Seerr request failed (${upstreamRes.status}) via ${baseUrl}${bodyMessage ? `: ${bodyMessage.slice(0, 220)}` : ''}`;
        continue;
      }
      try {
        return JSON.parse(text || '{}');
      } catch (err) {
        lastError = `Invalid JSON response from Seerr via ${baseUrl}.`;
      }
    } catch (err) {
      const reason = safeMessage(err) || 'fetch failed';
      lastError = `${reason} via ${baseUrl}`;
    }
  }
  const attemptsSummary = attempted.length ? ` (tried: ${attempted.join(', ')})` : '';
  throw new Error((lastError || 'Failed to reach Seerr.') + attemptsSummary);
}

// GET /api/pulsarr/stats/:kind — moved to src/routes/api-media.js

// GET /api/seerr/stats/:kind — moved to src/routes/api-media.js

// GET /api/pulsarr/tmdb/:kind/:id — moved to src/routes/api-media.js

// GET /api/seerr/tmdb/:kind/:id — moved to src/routes/api-media.js
// /api/prowlarr/search/filters — moved to src/routes/api-arr.js
// /api/prowlarr/search — moved to src/routes/api-arr.js
// /api/prowlarr/download — moved to src/routes/api-arr.js

function resolveAppApiCandidates(appItem, req) {
  return uniqueList([
    normalizeBaseUrl(appItem?.remoteUrl || ''),
    normalizeBaseUrl(resolveLaunchUrl(appItem, req)),
    normalizeBaseUrl(appItem?.localUrl || ''),
    normalizeBaseUrl(appItem?.url || ''),
  ]);
}

function isLoopbackHost(host) {
  const value = String(host || '').trim().toLowerCase().replace(/^\[|\]$/g, '');
  if (!value) return false;
  return value === 'localhost' || value === '127.0.0.1' || value === '::1';
}

function isLoopbackBaseUrl(value) {
  const normalized = normalizeBaseUrl(value || '');
  if (!normalized) return false;
  try {
    const parsed = new URL(normalized);
    return isLoopbackHost(parsed.hostname);
  } catch (err) {
    return false;
  }
}

function resolveRequestApiCandidates(appItem, req) {
  const remoteUrl = normalizeBaseUrl(appItem?.remoteUrl || '');
  const localUrl = normalizeBaseUrl(appItem?.localUrl || '');
  const launchUrl = normalizeBaseUrl(resolveLaunchUrl(appItem, req));
  const fallbackUrl = normalizeBaseUrl(appItem?.url || '');
  const explicitCandidates = uniqueList([remoteUrl, localUrl]).filter(Boolean);
  const explicitSet = new Set(explicitCandidates);
  const hasExplicitNonLoopback = explicitCandidates.some((candidate) => !isLoopbackBaseUrl(candidate));

  return uniqueList([...explicitCandidates, launchUrl, fallbackUrl])
    .filter(Boolean)
    .filter((candidate) => {
      if (!hasExplicitNonLoopback) return true;
      if (explicitSet.has(candidate)) return true;
      return !isLoopbackBaseUrl(candidate);
    });
}

function parseFiniteNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function formatBytesLabel(value) {
  const size = parseFiniteNumber(value, 0);
  if (!size || size <= 0) return '';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let amount = size;
  let index = 0;
  while (amount >= 1024 && index < units.length - 1) {
    amount /= 1024;
    index += 1;
  }
  const decimals = amount >= 10 || index === 0 ? 0 : 1;
  return `${amount.toFixed(decimals)} ${units[index]}`;
}

const TORZNAB_TOP_LEVEL_CATEGORY_LABELS = {
  1000: 'Console',
  2000: 'Movies',
  3000: 'Audio',
  4000: 'PC',
  5000: 'TV',
  6000: 'XXX',
  7000: 'Books',
  8000: 'Other',
};

function normalizeIndexerProtocol(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';
  if (raw.includes('usenet') || raw.includes('newznab') || raw === '1') return 'usenet';
  if (raw.includes('torrent') || raw.includes('torznab') || raw === '2') return 'torrent';
  return '';
}

function toTopLevelCategoryId(value) {
  const numeric = Number(String(value || '').trim());
  if (!Number.isFinite(numeric) || numeric <= 0) return 0;
  return Math.floor(numeric / 1000) * 1000;
}

function collectTopLevelCategoryIds(value, bucket = new Set()) {
  if (value === null || value === undefined) return bucket;
  if (Array.isArray(value)) {
    value.forEach((entry) => collectTopLevelCategoryIds(entry, bucket));
    return bucket;
  }
  if (typeof value === 'object') {
    const idCandidates = [
      value.id,
      value.Id,
      value.ID,
      value.categoryId,
      value.categoryID,
      value.category,
      value.Category,
      value.cat,
    ];
    idCandidates.forEach((entry) => {
      const topLevel = toTopLevelCategoryId(entry);
      if (topLevel) bucket.add(topLevel);
    });
    Object.values(value).forEach((entry) => collectTopLevelCategoryIds(entry, bucket));
    return bucket;
  }
  if (typeof value === 'string' && /[,;|\s]/.test(value)) {
    value
      .split(/[,;|\s]+/)
      .map((entry) => entry.trim())
      .filter(Boolean)
      .forEach((entry) => collectTopLevelCategoryIds(entry, bucket));
    return bucket;
  }
  const topLevel = toTopLevelCategoryId(value);
  if (topLevel) bucket.add(topLevel);
  return bucket;
}

function extractTopLevelCategoryIds(value) {
  return Array.from(collectTopLevelCategoryIds(value, new Set())).sort((a, b) => a - b);
}

function toTopLevelCategoryOptions(value) {
  const labels = new Map();
  const walk = (entry) => {
    if (entry === null || entry === undefined) return;
    if (Array.isArray(entry)) {
      entry.forEach((item) => walk(item));
      return;
    }
    if (typeof entry === 'object') {
      const id = toTopLevelCategoryId(entry.id ?? entry.Id ?? entry.ID ?? entry.categoryId ?? entry.category ?? entry.Category);
      const rawName = String(entry.name || entry.Name || entry.label || entry.Label || '').trim();
      if (id) {
        const fallback = TORZNAB_TOP_LEVEL_CATEGORY_LABELS[id] || `Category ${id}`;
        const label = rawName
          ? rawName.split(/[\\/]/)[0].trim() || fallback
          : fallback;
        if (!labels.has(id) || labels.get(id) === fallback) labels.set(id, label);
      }
      Object.values(entry).forEach((item) => walk(item));
      return;
    }
    const id = toTopLevelCategoryId(entry);
    if (id && !labels.has(id)) labels.set(id, TORZNAB_TOP_LEVEL_CATEGORY_LABELS[id] || `Category ${id}`);
  };

  walk(value);
  if (!labels.size) {
    Object.entries(TORZNAB_TOP_LEVEL_CATEGORY_LABELS).forEach(([id, label]) => labels.set(Number(id), label));
  }
  return Array.from(labels.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([id, name]) => ({ id: String(id), name }));
}

function escapeRegexLiteral(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function parseJackettTorznabItems(xmlText) {
  const text = String(xmlText || '');
  const itemMatches = text.match(/<item\b[\s\S]*?<\/item>/gi) || [];
  return itemMatches.map((rawItem) => {
    const readTag = (tagName) => {
      const match = rawItem.match(new RegExp(`<${tagName}[^>]*>([\\s\\S]*?)<\\/${tagName}>`, 'i'));
      return decodeXmlEntities(match?.[1] || '').trim();
    };
    const readAttr = (name) => {
      const match = rawItem.match(new RegExp(`<(?:torznab:)?attr[^>]*name=["']${escapeRegexLiteral(name)}["'][^>]*value=["']([^"']*)["'][^>]*\\/?>`, 'i'));
      return decodeXmlEntities(match?.[1] || '').trim();
    };
    const title = readTag('title');
    const guid = readTag('guid');
    const infoUrl = readTag('comments') || readTag('link');
    const downloadUrl = readTag('link') || readTag('enclosure');
    const size = parseFiniteNumber(readTag('size') || readAttr('size'));
    const seeders = parseFiniteNumber(readAttr('seeders'));
    const leechers = parseFiniteNumber(readAttr('peers') || readAttr('leechers'));
    const indexer = readAttr('indexer') || readTag('author');
    const indexerId = readAttr('indexerid') || readAttr('indexerId');
    const protocolRaw = readAttr('downloadvolumefactor') === '0'
      ? 'usenet'
      : (readAttr('protocol') || '');
    const protocol = normalizeIndexerProtocol(protocolRaw) || 'torrent';
    const categoryMatches = [...rawItem.matchAll(/<(?:torznab:)?attr[^>]*name=["']category["'][^>]*value=["']([^"']+)["'][^>]*\/?>/gi)];
    const categoryIds = extractTopLevelCategoryIds(categoryMatches.map((match) => decodeXmlEntities(match?.[1] || '').trim()));
    return {
      id: guid || infoUrl || downloadUrl || title,
      guid,
      title: title || 'Untitled',
      indexer: indexer || '',
      indexerId: String(indexerId || '').trim(),
      protocol,
      categoryIds,
      size,
      seeders,
      leechers,
      publishDate: readTag('pubDate'),
      infoUrl,
      downloadUrl,
    };
  }).filter((entry) => Boolean(entry.title));
}

function parseJackettJsonItems(payload) {
  const list = Array.isArray(payload?.Results)
    ? payload.Results
    : (Array.isArray(payload?.results)
      ? payload.results
      : (Array.isArray(payload) ? payload : []));
  return list.map((item) => {
    const protocolRaw = item?.Protocol || item?.protocol || item?.Type || item?.type || '';
    return {
      id: String(item?.Guid || item?.guid || item?.InfoHash || item?.link || item?.Title || item?.title || '').trim(),
      guid: String(item?.Guid || item?.guid || '').trim(),
      title: String(item?.Title || item?.title || 'Untitled').trim() || 'Untitled',
      indexer: String(item?.Tracker || item?.tracker || item?.Indexer || item?.indexer || item?.JackettIndexer || '').trim(),
      indexerId: String(item?.TrackerId || item?.trackerId || item?.IndexerId || item?.indexerId || item?.JackettIndexer || '').trim(),
      protocol: normalizeIndexerProtocol(protocolRaw) || 'torrent',
      categoryIds: extractTopLevelCategoryIds(item?.Category || item?.Categories || item?.category || item?.categories || item?.Cat || item?.cat),
      size: parseFiniteNumber(item?.Size || item?.size || item?.Length || item?.length || 0),
      seeders: parseFiniteNumber(item?.Seeders || item?.seeders || 0),
      leechers: parseFiniteNumber(item?.Peers || item?.peers || item?.Leechers || item?.leechers || 0),
      publishDate: String(item?.PublishDate || item?.publishDate || item?.Published || '').trim(),
      infoUrl: String(item?.Details || item?.details || item?.Comments || item?.comments || '').trim(),
      downloadUrl: String(item?.Link || item?.link || item?.MagnetUri || item?.magnetUri || '').trim(),
    };
  }).filter((entry) => Boolean(entry.title));
}

function mapBazarrStatusKey(value) {
  const status = String(value || '').trim().toLowerCase();
  if (!status) return 'queued';
  if (status.includes('error') || status.includes('fail')) return 'error';
  if (status.includes('done') || status.includes('complete') || status.includes('downloaded')) return 'completed';
  if (status.includes('active') || status.includes('downloading') || status.includes('processing')) return 'active';
  if (status.includes('pause')) return 'paused';
  return 'queued';
}

function mapAutobrrStatusKey(value) {
  const status = String(value || '').trim().toLowerCase();
  if (!status) return 'queued';
  if (status.includes('error') || status.includes('fail') || status.includes('reject')) return 'error';
  if (status.includes('push') || status.includes('deliver') || status.includes('approve') || status.includes('accept') || status.includes('complete')) return 'completed';
  if (status.includes('active') || status.includes('match') || status.includes('run')) return 'active';
  if (status.includes('pause')) return 'paused';
  return 'queued';
}

function pickFirstNonEmpty(values = []) {
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    const text = String(value || '').trim();
    if (text) return text;
  }
  return '';
}

function mapBazarrQueueItem(entry) {
  const mediaType = String(entry?.type || entry?.mediaType || (entry?.movieTitle ? 'movie' : 'tv')).trim().toLowerCase();
  const seriesTitle = pickFirstNonEmpty([entry?.seriesTitle, entry?.showTitle, entry?.series, entry?.title]);
  const movieTitle = pickFirstNonEmpty([entry?.movieTitle, entry?.movie, entry?.title]);
  const season = parseFiniteNumber(entry?.season, NaN);
  const episode = parseFiniteNumber(entry?.episode, NaN);
  const episodeLabel = Number.isFinite(season) && Number.isFinite(episode)
    ? `S${String(season).padStart(2, '0')}E${String(episode).padStart(2, '0')}`
    : '';
  const subtitleLanguages = Array.isArray(entry?.missing_subtitles)
    ? entry.missing_subtitles
    : (Array.isArray(entry?.missingSubtitles) ? entry.missingSubtitles : []);
  const languageLabel = subtitleLanguages
    .map((language) => String(language?.name || language?.code2 || language || '').trim())
    .filter(Boolean)
    .slice(0, 3)
    .join(', ');
  const statusText = pickFirstNonEmpty([entry?.status, entry?.state, entry?.subtitlesStatus, entry?.subtitleStatus, 'Wanted']);
  const statusKey = mapBazarrStatusKey(statusText);
  const detail = mediaType === 'movie'
    ? (pickFirstNonEmpty([entry?.year, entry?.releaseYear]) || 'Movie')
    : (episodeLabel || 'Episode');
  const subDetail = pickFirstNonEmpty([
    languageLabel,
    entry?.episodeTitle,
    entry?.title,
    statusText,
    '-',
  ]);
  return {
    kind: mediaType === 'movie' ? 'movie' : 'tv',
    title: pickFirstNonEmpty([movieTitle, seriesTitle, 'Unknown']),
    episode: detail,
    episodeTitle: subDetail,
    quality: languageLabel || '-',
    protocol: 'subtitle',
    timeLeft: '-',
    progress: statusKey === 'completed' ? 100 : 0,
    statusKey,
    statusKeys: [statusKey],
  };
}

function mapAutobrrQueueItem(entry, mode = 'recent-matches') {
  const maybeHttpUrl = (value) => {
    const raw = String(value || '').trim();
    if (!raw) return '';
    if (/^https?:\/\//i.test(raw)) return raw;
    return '';
  };
  const collectAutobrrLinks = (source) => {
    const add = (list, kind, label, href) => {
      const url = maybeHttpUrl(href);
      if (!url) return;
      if (list.some((entry) => entry.href === url)) return;
      list.push({ kind, label, href: url });
    };
    const links = [];
    add(links, 'info', 'Info', source?.indexer_url);
    add(links, 'info', 'Info', source?.indexerUrl);
    add(links, 'info', 'Info', source?.details_url);
    add(links, 'info', 'Info', source?.detailsUrl);
    add(links, 'info', 'Info', source?.info_url);
    add(links, 'info', 'Info', source?.infoUrl);
    add(links, 'info', 'Info', source?.url);
    add(links, 'info', 'Info', source?.link);
    add(links, 'download', 'Download', source?.download_url);
    add(links, 'download', 'Download', source?.downloadUrl);
    add(links, 'download', 'Download', source?.nzb_url);
    add(links, 'download', 'Download', source?.nzbUrl);
    add(links, 'download', 'Download', source?.torrent_url);
    add(links, 'download', 'Download', source?.torrentUrl);
    add(links, 'open', 'Open', source?.web_url);
    add(links, 'open', 'Open', source?.webUrl);
    if (source?.links && typeof source.links === 'object') {
      add(links, 'info', 'Info', source.links.info);
      add(links, 'download', 'Download', source.links.download);
      add(links, 'open', 'Open', source.links.web || source.links.open);
    }
    return links;
  };
  // filter_status is the primary field from the autobrr releases API
  const statusText = pickFirstNonEmpty([entry?.filter_status, entry?.status, entry?.state, entry?.result, entry?.action]);
  const statusKey = mapAutobrrStatusKey(statusText);
  const protocolRaw = pickFirstNonEmpty([entry?.protocol, entry?.kind, entry?.type]);
  const protocol = protocolRaw.toLowerCase().includes('usenet') ? 'usenet' : 'torrent';
  const sizeBytes = parseFiniteNumber(entry?.size || entry?.bytes || entry?.totalSize || 0);
  // Indexer name: handle nested indexer object from autobrr API
  const indexerName = pickFirstNonEmpty([
    entry?.indexer?.name,
    entry?.indexer?.identifier_external,
    typeof entry?.indexer === 'string' ? entry.indexer : '',
    entry?.indexerName,
    entry?.tracker,
    entry?.filter,
    entry?.filterName,
    mode === 'delivery-queue' ? 'Delivery' : 'Match',
  ]);
  // Build the releases sub-detail line: Category: X Size: Y Misc: Z
  const category = String(entry?.category || '').trim();
  const resolution = String(entry?.resolution || '').trim();
  const source = String(entry?.source || '').trim();
  const codec = Array.isArray(entry?.codec) ? entry.codec.join(' ') : String(entry?.codec || '').trim();
  const container = String(entry?.container || '').trim();
  const miscParts = [resolution, source, codec, container].filter(Boolean);
  const sizeLabel = sizeBytes > 0 ? formatBytesLabel(sizeBytes) : '0 byte';
  const subDetailLine = `Category: ${category} Size: ${sizeLabel} Misc: ${miscParts.join(' ')}`;
  const links = collectAutobrrLinks(entry);
  const actionType = pickFirstNonEmpty([
    entry?.action_type,
    entry?.actionType,
    entry?.action?.type,
    entry?.action,
  ]);
  const actionApp = pickFirstNonEmpty([
    entry?.action?.name,
    entry?.action_name,
    entry?.actionName,
    actionType,
  ]);
  const filterName = pickFirstNonEmpty([
    entry?.filter?.name,
    entry?.filter_name,
    entry?.filterName,
    entry?.filter,
  ]);
  const actionReason = pickFirstNonEmpty([
    entry?.reason,
    entry?.reject_reason,
    entry?.rejectReason,
    entry?.error,
    entry?.error_message,
    entry?.errorMessage,
    entry?.message,
  ]);
  return {
    kind: protocol,
    title: pickFirstNonEmpty([entry?.name, entry?.releaseName, entry?.releaseTitle, entry?.title, 'Unknown']),
    episode: indexerName,
    episodeTitle: indexerName,
    quality: sizeBytes > 0 ? formatBytesLabel(sizeBytes) : '-',
    protocol,
    timeLeft: '-',
    progress: statusKey === 'completed' ? 100 : (statusKey === 'active' ? 50 : 0),
    statusKey,
    statusKeys: [statusKey],
    timestamp: String(entry?.timestamp || '').trim(),
    subDetailLine,
    indexer: indexerName,
    links,
    actionMeta: {
      status: statusText,
      app: actionApp,
      type: actionType,
      filter: filterName,
      time: String(entry?.timestamp || '').trim(),
      reason: actionReason,
    },
    actions: [{ kind: statusKey === 'error' ? 'block' : 'status', label: statusText || statusKey || 'Status', disabled: true }],
  };
}

function parseMaintainerrTimestamp(value) {
  if (value === null || value === undefined || value === '') return 0;
  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric > 0) {
    if (numeric > 1e11) return Math.round(numeric); // milliseconds
    return Math.round(numeric * 1000); // seconds
  }
  const parsed = Date.parse(String(value || '').trim());
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatMaintainerrDateLabel(value) {
  const timestamp = parseMaintainerrTimestamp(value);
  if (!timestamp) return '';
  try {
    return new Date(timestamp).toLocaleDateString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  } catch (err) {
    return '';
  }
}

function formatMaintainerrRelativePill(value) {
  const timestamp = parseMaintainerrTimestamp(value);
  if (!timestamp) return '';
  const diffMs = Date.now() - timestamp;
  const diffMinutes = Math.max(1, Math.round(diffMs / 60000));
  if (diffMinutes < 60) return `${diffMinutes}m ago`;
  const diffHours = Math.round(diffMinutes / 60);
  if (diffHours < 48) return `${diffHours}h ago`;
  const diffDays = Math.round(diffHours / 24);
  return `${diffDays}d ago`;
}

function normalizeMaintainerrMediaKind(value, fallback = 'movie') {
  const text = String(value || '').trim().toLowerCase();
  if (!text) return fallback;
  if (text === 'movie' || text === 'movies') return 'movie';
  if (text === 'show' || text === 'shows' || text === 'tv' || text === 'series') return 'show';
  if (text === 'season' || text === 'episode') return 'show';
  if (text === '1') return 'movie';
  if (text === '2') return 'show';
  return fallback;
}

function normalizeMaintainerrInitial(value) {
  const text = String(value || '').trim();
  if (!text) return '#';
  const first = text[0].toUpperCase();
  return /[A-Z]/.test(first) ? first : '#';
}

function resolveMaintainerrAssetUrl(baseUrl, candidate = '') {
  const value = String(candidate || '').trim();
  if (!value) return '';
  if (/^https?:\/\//i.test(value)) return value;
  if (/^\/\//.test(value)) return `https:${value}`;
  try {
    const normalizedBase = normalizeBaseUrl(baseUrl || '');
    const relativeBase = normalizedBase || baseUrl;
    return new URL(value, relativeBase).toString();
  } catch (err) {
    return value;
  }
}

function isMaintainerrPlaceholderArtwork(value = '') {
  const text = String(value || '').trim();
  if (!text) return false;
  const normalized = decodeURIComponent(text).toLowerCase();
  if (normalized.includes('/icons/maintainerr.svg') || normalized.includes('/icons/app.svg')) return true;
  if (!normalized.includes('maintainerr')) {
    return /(placeholder|fallback|no[-_ ]?art(?:work)?|no[-_ ]?poster|default[-_ ]?(poster|art|image)|missing[-_ ]?(poster|art|image))/i.test(normalized);
  }
  return /(placeholder|fallback|no[-_ ]?art(?:work)?|no[-_ ]?poster|default[-_ ]?(poster|art|image)|missing[-_ ]?(poster|art|image))/i.test(normalized);
}

function isMaintainerrPlexLibraryAssetPath(value = '') {
  return /^\/library\/metadata\/\d+\/(?:thumb|art|clearLogo)(?:\/\d+)?$/i.test(String(value || '').trim());
}

function normalizeMaintainerrTmdbKind(value, fallback = 'movie') {
  const text = String(value || '').trim().toLowerCase();
  if (text === 'tv' || text === 'show' || text === 'shows' || text === 'series') return 'tv';
  if (text === 'movie' || text === 'movies') return 'movie';
  return fallback;
}

function extractMaintainerrTmdbId(source = {}) {
  const direct = parseFiniteNumber(
    source?.tmdbId
    ?? source?.tmdbID
    ?? source?.tmdb_id
    ?? source?.metadata?.tmdbId
    ?? source?.metadata?.tmdb_id
    ?? source?.parentTmdbId
    ?? source?.parentTmdbID,
    0
  );
  if (direct > 0) return direct;

  const guidCandidates = [];
  const pushGuid = (value) => {
    const text = String(value || '').trim();
    if (!text) return;
    guidCandidates.push(text);
  };
  pushGuid(source?.guid);
  if (Array.isArray(source?.Guid)) {
    source.Guid.forEach((entry) => {
      if (entry && typeof entry === 'object') {
        pushGuid(entry.id || entry.guid || entry.url || '');
      } else {
        pushGuid(entry);
      }
    });
  }
  if (Array.isArray(source?.guids)) {
    source.guids.forEach((entry) => {
      if (entry && typeof entry === 'object') {
        pushGuid(entry.id || entry.guid || entry.url || '');
      } else {
        pushGuid(entry);
      }
    });
  }
  for (let index = 0; index < guidCandidates.length; index += 1) {
    const match = guidCandidates[index].match(/tmdb:\/\/(\d+)/i);
    if (match && match[1]) {
      const parsed = parseFiniteNumber(match[1], 0);
      if (parsed > 0) return parsed;
    }
  }
  return 0;
}

function buildMaintainerrTmdbImageUrl(path = '', size = 'w500') {
  const value = String(path || '').trim();
  if (!value) return '';
  if (/^https?:\/\//i.test(value)) return value;
  if (/^\/\//.test(value)) return `https:${value}`;
  if (value.startsWith('/')) return `https://image.tmdb.org/t/p/${size}${value}`;
  if (/^[\w-]+\.(jpg|jpeg|png|webp)$/i.test(value)) return `https://image.tmdb.org/t/p/${size}/${value}`;
  return '';
}

function parseMaintainerrRuleJson(value) {
  if (!value && value !== 0) return {};
  if (typeof value === 'object') return value || {};
  const text = String(value || '').trim();
  if (!text) return {};
  try {
    const parsed = JSON.parse(text);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch (err) {
    return {};
  }
}

function formatMaintainerrRuleValue(value) {
  if (Array.isArray(value)) {
    const normalized = value
      .map((entry) => String(entry ?? '').trim())
      .filter(Boolean);
    return normalized.join(', ');
  }
  if (value && typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch (err) {
      return '';
    }
  }
  return String(value ?? '').trim();
}

function summarizeMaintainerrRuleCondition(condition = {}) {
  const sectionValue = String(condition.section ?? '').trim();
  const actionValue = String(condition.action ?? '').trim();
  const operatorValue = String(condition.operator ?? '').trim();
  const firstValue = formatMaintainerrRuleValue(condition.firstVal);
  const lastValue = formatMaintainerrRuleValue(condition.lastVal);
  const pieces = [];

  if (sectionValue) pieces.push(`Section ${sectionValue}`);
  if (actionValue) pieces.push(`Action ${actionValue}`);
  if (operatorValue === '0') pieces.push('AND');
  if (operatorValue === '1') pieces.push('OR');
  if (firstValue) pieces.push(`Value A: ${firstValue}`);
  if (lastValue) pieces.push(`Value B: ${lastValue}`);
  return pieces.join(' • ');
}

function mapMaintainerrLibraryItem(entry, options = {}) {
  const source = entry && typeof entry === 'object' ? entry : {};
  const imageList = Array.isArray(source.Image) ? source.Image : [];
  const posterFromImage = imageList.find((item) => String(item?.type || '').toLowerCase().includes('cover'))?.url || '';
  const artFromImage = imageList.find((item) => String(item?.type || '').toLowerCase().includes('background'))?.url || '';
  const kind = normalizeMaintainerrMediaKind(source.type || options.libraryType, 'movie');
  const tmdbKind = normalizeMaintainerrTmdbKind(kind, 'movie');
  const tmdbId = extractMaintainerrTmdbId(source);
  const rawThumbCandidate = pickFirstNonEmpty([source.thumb, source.poster, posterFromImage]);
  const rawArtCandidate = pickFirstNonEmpty([source.art, source.background, artFromImage]);
  const resolvedThumb = resolveMaintainerrAssetUrl(options.baseUrl, rawThumbCandidate);
  const resolvedArt = resolveMaintainerrAssetUrl(options.baseUrl, rawArtCandidate);
  const thumb = isMaintainerrPlexLibraryAssetPath(rawThumbCandidate)
    || isMaintainerrPlexLibraryAssetPath(resolvedThumb)
    || isMaintainerrPlaceholderArtwork(rawThumbCandidate)
    || isMaintainerrPlaceholderArtwork(resolvedThumb)
    ? ''
    : resolvedThumb;
  const art = isMaintainerrPlexLibraryAssetPath(rawArtCandidate)
    || isMaintainerrPlexLibraryAssetPath(resolvedArt)
    || isMaintainerrPlaceholderArtwork(rawArtCandidate)
    || isMaintainerrPlaceholderArtwork(resolvedArt)
    ? ''
    : resolvedArt;
  const posterProxy = tmdbId > 0 ? `/api/maintainerr-poster/${tmdbKind}/${tmdbId}` : '';
  const backdropProxy = tmdbId > 0 ? `/api/maintainerr-backdrop/${tmdbKind}/${tmdbId}` : '';
  const year = parseFiniteNumber(source.year, 0);
  const addedAt = source.addedAt || source.updatedAt || source.originallyAvailableAt || source.lastViewedAt;
  const addedLabel = formatMaintainerrDateLabel(addedAt);
  const subtitle = pickFirstNonEmpty([options.libraryTitle, kind === 'show' ? 'TV' : 'Movies']);
  const metaParts = [];
  if (year > 0) metaParts.push(String(year));
  if (subtitle) metaParts.push(subtitle);
  return {
    id: pickFirstNonEmpty([source.ratingKey, source.guid, source.key, source.title]),
    title: pickFirstNonEmpty([source.title, 'Untitled']),
    kind,
    subtitle,
    meta: metaParts.join(' • '),
    pill: formatMaintainerrRelativePill(addedAt),
    sortTs: parseMaintainerrTimestamp(addedAt),
    addedLabel: addedLabel ? `Added ${addedLabel}` : '',
    year: year || undefined,
    letter: normalizeMaintainerrInitial(source.title),
    thumb: pickFirstNonEmpty([posterProxy, thumb]),
    art: pickFirstNonEmpty([backdropProxy, art]),
    overview: pickFirstNonEmpty([source.summary, source.tagline]),
    libraryId: parseFiniteNumber(options.libraryId, 0),
    libraryTitle: subtitle,
    tmdbId: tmdbId || undefined,
  };
}

function mapMaintainerrRuleItem(entry) {
  const source = entry && typeof entry === 'object' ? entry : {};
  const dataType = parseFiniteNumber(source.dataType || source.collection?.type, 0);
  const kind = normalizeMaintainerrMediaKind(String(dataType || ''), 'movie');
  const statusLabel = source.isActive === false ? 'Paused' : 'Active';
  const rules = Array.isArray(source.rules) ? source.rules : [];
  const ruleConditions = rules
    .map((ruleEntry) => {
      const parsed = parseMaintainerrRuleJson(ruleEntry?.ruleJson);
      const summary = summarizeMaintainerrRuleCondition(parsed);
      if (!summary) return '';
      return summary;
    })
    .filter(Boolean);
  return {
    id: parseFiniteNumber(source.id, 0),
    name: pickFirstNonEmpty([source.name, source.collection?.title, `Rule ${source.id || ''}`]).trim(),
    description: pickFirstNonEmpty([source.description, source.collection?.description]),
    isActive: source.isActive !== false,
    status: statusLabel,
    kind,
    libraryId: parseFiniteNumber(source.libraryId || source.collection?.libraryId, 0),
    libraryLabel: kind === 'show' ? 'TV' : 'Movies',
    collectionId: parseFiniteNumber(source.collection?.id || source.collectionId, 0),
    collectionTitle: pickFirstNonEmpty([source.collection?.title, source.manualCollectionName]),
    rulesCount: rules.length,
    ruleConditions,
    useRules: source.useRules !== false,
    cronSchedule: pickFirstNonEmpty([source.ruleHandlerCronSchedule]),
    handledMediaAmount: parseFiniteNumber(source.collection?.handledMediaAmount, 0),
    deleteAfterDays: parseFiniteNumber(source.collection?.deleteAfterDays, 0),
    keepLogsForMonths: parseFiniteNumber(source.collection?.keepLogsForMonths, 0),
    arrAction: pickFirstNonEmpty([source.collection?.arrAction]),
    lastDurationSeconds: parseFiniteNumber(source.collection?.lastDurationInSeconds, 0),
  };
}

function mapMaintainerrCollectionMediaItem(entry, options = {}) {
  const source = entry && typeof entry === 'object' ? entry : {};
  const plexData = source.plexData && typeof source.plexData === 'object'
    ? source.plexData
    : {};
  const imageList = Array.isArray(plexData.Image) ? plexData.Image : [];
  const posterFromImage = imageList.find((item) => String(item?.type || '').toLowerCase().includes('cover'))?.url || '';
  const artFromImage = imageList.find((item) => String(item?.type || '').toLowerCase().includes('background'))?.url || '';
  const kind = normalizeMaintainerrMediaKind(plexData.type || options.type, 'movie');
  const tmdbKind = normalizeMaintainerrTmdbKind(kind, 'movie');
  const tmdbId = extractMaintainerrTmdbId({
    ...source,
    ...plexData,
    tmdbId: source.tmdbId ?? plexData.tmdbId,
  });
  const imagePathPoster = isMaintainerrPlaceholderArtwork(source.image_path)
    ? ''
    : buildMaintainerrTmdbImageUrl(source.image_path, 'w500');
  const rawThumbCandidate = pickFirstNonEmpty([plexData.thumb, source.thumb, posterFromImage]);
  const rawArtCandidate = pickFirstNonEmpty([plexData.art, plexData.background, artFromImage]);
  const resolvedThumb = resolveMaintainerrAssetUrl(options.baseUrl, rawThumbCandidate);
  const resolvedArt = resolveMaintainerrAssetUrl(options.baseUrl, rawArtCandidate);
  const thumb = isMaintainerrPlexLibraryAssetPath(rawThumbCandidate)
    || isMaintainerrPlexLibraryAssetPath(resolvedThumb)
    || isMaintainerrPlaceholderArtwork(rawThumbCandidate)
    || isMaintainerrPlaceholderArtwork(resolvedThumb)
    ? ''
    : resolvedThumb;
  const art = isMaintainerrPlexLibraryAssetPath(rawArtCandidate)
    || isMaintainerrPlexLibraryAssetPath(resolvedArt)
    || isMaintainerrPlaceholderArtwork(rawArtCandidate)
    || isMaintainerrPlaceholderArtwork(resolvedArt)
    ? ''
    : resolvedArt;
  const posterProxy = tmdbId > 0 ? `/api/maintainerr-poster/${tmdbKind}/${tmdbId}` : '';
  const backdropProxy = tmdbId > 0 ? `/api/maintainerr-backdrop/${tmdbKind}/${tmdbId}` : '';
  const title = pickFirstNonEmpty([plexData.title, source.title, plexData.parentTitle, 'Untitled']);
  const year = parseFiniteNumber(plexData.year, 0);
  const addedAt = source.addDate || source.addedAt || plexData.addedAt || plexData.updatedAt;
  const addedLabel = formatMaintainerrDateLabel(addedAt);
  return {
    id: pickFirstNonEmpty([source.id, plexData.ratingKey, title]),
    title,
    kind,
    subtitle: pickFirstNonEmpty([options.collectionTitle, source.collectionTitle]),
    meta: year > 0 ? String(year) : '',
    pill: formatMaintainerrRelativePill(addedAt),
    sortTs: parseMaintainerrTimestamp(addedAt),
    addedLabel: addedLabel ? `Added ${addedLabel}` : '',
    year: year || undefined,
    letter: normalizeMaintainerrInitial(title),
    thumb: pickFirstNonEmpty([imagePathPoster, posterProxy, thumb]),
    art: pickFirstNonEmpty([backdropProxy, art]),
    overview: pickFirstNonEmpty([plexData.summary, plexData.tagline]),
    collectionId: parseFiniteNumber(options.collectionId || source.collectionId, 0),
    collectionTitle: pickFirstNonEmpty([options.collectionTitle, source.collectionTitle]),
    tmdbId: tmdbId || undefined,
  };
}

function parseCleanuparrTimestamp(value) {
  if (value === null || value === undefined || value === '') return 0;
  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric > 0) {
    if (numeric > 1e11) return Math.round(numeric);
    return Math.round(numeric * 1000);
  }
  const parsed = Date.parse(String(value || '').trim());
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatCleanuparrDateLabel(value) {
  const timestamp = parseCleanuparrTimestamp(value);
  if (!timestamp) return '';
  try {
    return new Date(timestamp).toLocaleDateString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  } catch (_err) {
    return '';
  }
}

function formatCleanuparrRelativePill(value) {
  const timestamp = parseCleanuparrTimestamp(value);
  if (!timestamp) return '';
  const diffMs = Date.now() - timestamp;
  const diffMinutes = Math.max(1, Math.round(diffMs / 60000));
  if (diffMinutes < 60) return `${diffMinutes}m ago`;
  const diffHours = Math.round(diffMinutes / 60);
  if (diffHours < 48) return `${diffHours}h ago`;
  const diffDays = Math.round(diffHours / 24);
  return `${diffDays}d ago`;
}

function normalizeCleanuparrMediaKind(value, fallback = 'movie') {
  const text = String(value || '').trim().toLowerCase();
  if (!text) return fallback;
  if (text === 'movie' || text === 'movies' || text === 'film') return 'movie';
  if (text === 'show' || text === 'shows' || text === 'tv' || text === 'series') return 'show';
  if (text === 'season' || text === 'episode') return 'show';
  return fallback;
}

function normalizeCleanuparrStatus(value, fallback = 'Active') {
  const text = String(value || '').trim();
  if (!text) return fallback;
  if (/error|failed|blocked|denied|rejected/i.test(text)) return 'Error';
  if (/skip|ignored|ignore/i.test(text)) return 'Ignored';
  if (/remove|deleted|struck|strike/i.test(text)) return 'Removed';
  if (/warn|warning/i.test(text)) return 'Warning';
  if (/success|complete|handled|done|cleaned/i.test(text)) return 'Handled';
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function resolveCleanuparrAssetUrl(baseUrl, candidate = '') {
  const value = String(candidate || '').trim();
  if (!value) return '';
  if (/^https?:\/\//i.test(value)) return value;
  if (/^\/\//.test(value)) return `https:${value}`;
  try {
    const normalizedBase = normalizeBaseUrl(baseUrl || '');
    const relativeBase = normalizedBase || baseUrl;
    return new URL(value, relativeBase).toString();
  } catch (_err) {
    return value;
  }
}

function extractCleanuparrList(payload, kind = 'events') {
  const source = payload && typeof payload === 'object' ? payload : {};
  if (Array.isArray(payload)) return payload;
  const preferredKeys = kind === 'recent-strikes'
    ? ['strikes', 'recentStrikes', 'recent_strikes', 'items', 'results', 'data']
    : ['events', 'items', 'results', 'entries', 'data'];
  for (let index = 0; index < preferredKeys.length; index += 1) {
    const list = source?.[preferredKeys[index]];
    if (Array.isArray(list)) return list;
  }
  const nestedCandidates = [source.data, source.result, source.results, source.payload, source.response];
  for (let index = 0; index < nestedCandidates.length; index += 1) {
    const entry = nestedCandidates[index];
    if (Array.isArray(entry)) return entry;
    if (entry && typeof entry === 'object') {
      for (let inner = 0; inner < preferredKeys.length; inner += 1) {
        const list = entry?.[preferredKeys[inner]];
        if (Array.isArray(list)) return list;
      }
    }
  }
  return [];
}

function mapCleanuparrStrikeItem(entry, baseUrl = '') {
  const source = entry && typeof entry === 'object' ? entry : {};
  const title = pickFirstNonEmpty([
    source.mediaTitle,
    source.title,
    source.name,
    source.releaseTitle,
    source.media?.title,
    source.item?.title,
    source.fileName,
    source.filename,
    'Untitled',
  ]);
  const mediaKind = normalizeCleanuparrMediaKind(
    pickFirstNonEmpty([source.mediaType, source.type, source.kind, source.libraryType]),
    'movie'
  );
  const strikeDate = pickFirstNonEmpty([
    source.createdAt,
    source.created_at,
    source.timestamp,
    source.date,
    source.time,
    source.updatedAt,
    source.updated_at,
  ]);
  const year = parseFiniteNumber(source.year || source.releaseYear || source.media?.year, 0);
  const status = normalizeCleanuparrStatus(
    pickFirstNonEmpty([source.status, source.action, source.result, source.state, 'Strike']),
    'Strike'
  );
  const reason = pickFirstNonEmpty([
    source.reason,
    source.reasonText,
    source.rule,
    source.ruleName,
    source.description,
    source.message,
  ]);
  const poster = resolveCleanuparrAssetUrl(baseUrl, pickFirstNonEmpty([
    source.poster,
    source.posterUrl,
    source.poster_url,
    source.thumb,
    source.thumbnail,
    source.media?.poster,
    source.media?.thumb,
  ]));
  const art = resolveCleanuparrAssetUrl(baseUrl, pickFirstNonEmpty([
    source.art,
    source.backdrop,
    source.backdropUrl,
    source.backdrop_url,
    source.media?.art,
    source.media?.backdrop,
  ]));
  const dateLabel = formatCleanuparrDateLabel(strikeDate);
  const metaParts = [];
  if (year > 0) metaParts.push(String(year));
  if (reason) metaParts.push(reason);
  return {
    id: pickFirstNonEmpty([source.id, source.uuid, source.slug, title]),
    title,
    kind: mediaKind,
    subtitle: reason || (mediaKind === 'show' ? 'TV' : 'Movie'),
    meta: metaParts.join(' • '),
    pill: formatCleanuparrRelativePill(strikeDate),
    sortTs: parseCleanuparrTimestamp(strikeDate),
    status,
    statusKey: String(status || '').toLowerCase(),
    overview: pickFirstNonEmpty([source.description, source.message, reason]),
    thumb: poster,
    art,
    addedLabel: dateLabel ? `Updated ${dateLabel}` : '',
  };
}

function mapCleanuparrEventItem(entry, baseUrl = '') {
  const source = entry && typeof entry === 'object' ? entry : {};
  const title = pickFirstNonEmpty([
    source.title,
    source.name,
    source.event,
    source.eventName,
    source.action,
    source.message,
    source.description,
    'Event',
  ]);
  const mediaKind = normalizeCleanuparrMediaKind(
    pickFirstNonEmpty([source.mediaType, source.type, source.kind, source.category]),
    'movie'
  );
  const status = normalizeCleanuparrStatus(
    pickFirstNonEmpty([source.level, source.severity, source.status, source.state, source.action, 'Info']),
    'Info'
  );
  const eventDate = pickFirstNonEmpty([
    source.createdAt,
    source.created_at,
    source.timestamp,
    source.date,
    source.time,
    source.occurredAt,
    source.occurred_at,
    source.updatedAt,
    source.updated_at,
  ]);
  const message = pickFirstNonEmpty([source.message, source.description, source.details, source.detail]);
  const poster = resolveCleanuparrAssetUrl(baseUrl, pickFirstNonEmpty([
    source.poster,
    source.posterUrl,
    source.poster_url,
    source.thumb,
    source.thumbnail,
  ]));
  const art = resolveCleanuparrAssetUrl(baseUrl, pickFirstNonEmpty([
    source.art,
    source.backdrop,
    source.backdropUrl,
    source.backdrop_url,
  ]));
  const dateLabel = formatCleanuparrDateLabel(eventDate);
  return {
    id: pickFirstNonEmpty([source.id, source.uuid, source.slug, `${title}:${eventDate}`]),
    title,
    kind: mediaKind,
    subtitle: pickFirstNonEmpty([source.mediaTitle, source.target, source.ruleName, source.type]),
    meta: message,
    pill: formatCleanuparrRelativePill(eventDate),
    sortTs: parseCleanuparrTimestamp(eventDate),
    status,
    statusKey: String(status || '').toLowerCase(),
    overview: message,
    thumb: poster,
    art,
    addedLabel: dateLabel ? `Logged ${dateLabel}` : '',
  };
}

function normalizeRommMediaKind(value, fallback = 'game') {
  const text = String(value || '').trim().toLowerCase();
  if (!text) return fallback;
  if (text.includes('handheld') || text.includes('portable')) return 'handheld';
  if (text.includes('console') || text.includes('system') || text.includes('platform')) return 'console';
  if (text.includes('bios') || text.includes('firmware')) return 'bios';
  if (text.includes('homebrew')) return 'homebrew';
  if (text.includes('game') || text.includes('rom')) return 'game';
  return fallback;
}

function parseRommTimestamp(value) {
  if (value === null || value === undefined || value === '') return 0;
  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric > 0) {
    if (numeric > 1e17) return Math.round(numeric / 1e6); // nanoseconds
    if (numeric > 1e14) return Math.round(numeric / 1e3); // microseconds
    if (numeric > 1e11) return Math.round(numeric); // milliseconds
    return Math.round(numeric * 1000); // seconds
  }
  const parsed = Date.parse(String(value || '').trim());
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatRommDateLabel(value) {
  const timestamp = parseRommTimestamp(value);
  if (!timestamp) return '';
  try {
    return new Date(timestamp).toLocaleDateString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  } catch (err) {
    return '';
  }
}

function formatRommRelativePill(value) {
  const timestamp = parseRommTimestamp(value);
  if (!timestamp) return '';
  const diffMs = Date.now() - timestamp;
  const diffMinutes = Math.max(1, Math.round(diffMs / 60000));
  if (diffMinutes < 60) return `${diffMinutes}m ago`;
  const diffHours = Math.round(diffMinutes / 60);
  if (diffHours < 48) return `${diffHours}h ago`;
  const diffDays = Math.round(diffHours / 24);
  return `${diffDays}d ago`;
}

function resolveRommAssetUrl(baseUrl, candidates = []) {
  let relativeBase = '';
  let basePathPrefix = '';
  try {
    const normalizedBase = normalizeBaseUrl(baseUrl || '');
    if (normalizedBase) {
      const parsedBase = new URL(normalizedBase);
      if (!String(parsedBase.pathname || '/').endsWith('/')) {
        parsedBase.pathname = `${parsedBase.pathname}/`;
      }
      parsedBase.search = '';
      parsedBase.hash = '';
      relativeBase = parsedBase.toString();
      basePathPrefix = normalizeBasePath(parsedBase.pathname || '');
    }
  } catch (err) {
    relativeBase = '';
    basePathPrefix = '';
  }

  for (let index = 0; index < candidates.length; index += 1) {
    const candidate = String(candidates[index] || '').trim();
    if (!candidate) continue;
    if (candidate === '[object Object]') continue;
    if (/^data:image\//i.test(candidate)) return candidate;
    if (/^https?:\/\//i.test(candidate)) return candidate;
    if (/^\/\//.test(candidate)) return `https:${candidate}`;

    const attempts = [candidate];
    if (
      candidate.startsWith('/')
      && basePathPrefix
      && basePathPrefix !== '/'
      && candidate !== basePathPrefix
      && !candidate.startsWith(`${basePathPrefix}/`)
    ) {
      attempts.unshift(`${basePathPrefix}${candidate}`);
    }

    for (let attemptIndex = 0; attemptIndex < attempts.length; attemptIndex += 1) {
      try {
        return new URL(attempts[attemptIndex], relativeBase || baseUrl).toString();
      } catch (err) {
        continue;
      }
    }
  }
  return '';
}

function normalizeRommAssetKey(value) {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9]/g, '');
}

function collectRommAssetValues(value, bucket = []) {
  if (value === null || value === undefined) return bucket;
  if (typeof value === 'string') {
    const text = value.trim();
    if (text) bucket.push(text);
    return bucket;
  }
  if (Array.isArray(value)) {
    value.forEach((entry) => collectRommAssetValues(entry, bucket));
    return bucket;
  }
  if (typeof value === 'object') {
    const directKeys = ['url', 'href', 'src', 'path', 'image', 'thumb', 'cover', 'poster', 'icon', 'logo', 'background', 'backdrop', 'banner', 'fanart', 'art', 'artwork'];
    directKeys.forEach((key) => {
      if (value[key] !== undefined) collectRommAssetValues(value[key], bucket);
    });
  }
  return bucket;
}

function normalizeRommPlatformSlug(value) {
  if (value === null || value === undefined) return '';
  let raw = String(value).trim().toLowerCase();
  if (!raw) return '';
  try {
    raw = decodeURIComponent(raw);
  } catch (err) {
    // Keep original when the value isn't URI-encoded.
  }
  raw = raw.replace(/^.*[\\/]/, '').replace(/\.[a-z0-9]{2,6}$/i, '');
  return raw
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function buildRommPlatformAssetCandidates(entry, title = '') {
  const source = entry && typeof entry === 'object' ? entry : {};
  const slugValues = uniqueList([
    source.slug,
    source.platformSlug,
    source.platform_slug,
    source.consoleSlug,
    source.console_slug,
    source.systemSlug,
    source.system_slug,
    source.fsSlug,
    source.fs_slug,
    source.igdbSlug,
    source.igdb_slug,
    source.assets?.slug,
    source.assets?.platformSlug,
    source.metadata?.slug,
    source.metadata?.platformSlug,
    source.platform?.slug,
    source.console?.slug,
    source.system?.slug,
    title,
    source.name,
    source.platformName,
    source.consoleName,
    source.systemName,
  ]);
  const slugs = uniqueList(slugValues.map((value) => normalizeRommPlatformSlug(value)).filter(Boolean));
  const extensions = ['ico', 'png', 'svg', 'webp', 'jpg', 'jpeg'];
  const candidates = [];
  slugs.forEach((slug) => {
    extensions.forEach((ext) => {
      candidates.push(`assets/platforms/${slug}.${ext}`);
      candidates.push(`/assets/platforms/${slug}.${ext}`);
    });
  });
  return uniqueList(candidates);
}

function findRommAssetCandidates(entry, options = {}) {
  const exactKeySet = new Set((Array.isArray(options.exactKeys) ? options.exactKeys : []).map(normalizeRommAssetKey).filter(Boolean));
  const containsTokens = (Array.isArray(options.containsTokens) ? options.containsTokens : [])
    .map(normalizeRommAssetKey)
    .filter(Boolean);
  const maxDepth = Math.max(1, Math.min(6, parseFiniteNumber(options.maxDepth, 4)));
  const candidates = [];
  const seenValues = new Set();
  const seenObjects = new Set();
  const queue = [{ value: entry, depth: 0 }];

  const pushCandidate = (rawValue) => {
    const text = String(rawValue || '').trim();
    if (!text) return;
    if (seenValues.has(text)) return;
    seenValues.add(text);
    candidates.push(text);
  };

  while (queue.length) {
    const { value, depth } = queue.shift();
    if (!value || typeof value !== 'object') continue;
    if (seenObjects.has(value)) continue;
    seenObjects.add(value);
    if (depth > maxDepth) continue;

    if (Array.isArray(value)) {
      value.forEach((entryValue) => {
        if (entryValue && typeof entryValue === 'object') {
          queue.push({ value: entryValue, depth: depth + 1 });
        } else {
          collectRommAssetValues(entryValue).forEach(pushCandidate);
        }
      });
      continue;
    }

    Object.entries(value).forEach(([rawKey, rawValue]) => {
      const key = normalizeRommAssetKey(rawKey);
      const matchesExact = exactKeySet.has(key);
      const matchesToken = containsTokens.some((token) => key.includes(token));
      if (matchesExact || matchesToken) {
        collectRommAssetValues(rawValue).forEach(pushCandidate);
      }
      if (rawValue && typeof rawValue === 'object' && depth < maxDepth) {
        queue.push({ value: rawValue, depth: depth + 1 });
      }
    });
  }

  return candidates;
}

function isLikelyRommCollection(value) {
  if (!Array.isArray(value)) return false;
  if (!value.length) return true;
  return value.some((entry) => entry && typeof entry === 'object');
}

function extractRommList(payload, kind = 'recently-added') {
  const preferredKeys = kind === 'consoles'
    ? ['platforms', 'consoles', 'systems', 'items', 'results', 'records', 'data']
    : ['games', 'roms', 'items', 'results', 'records', 'data'];

  const fromContainer = (container) => {
    if (!container || typeof container !== 'object') return null;
    for (let index = 0; index < preferredKeys.length; index += 1) {
      const key = preferredKeys[index];
      if (isLikelyRommCollection(container[key])) return container[key];
    }
    const values = Object.values(container);
    for (let index = 0; index < values.length; index += 1) {
      if (isLikelyRommCollection(values[index])) return values[index];
    }
    return null;
  };

  if (isLikelyRommCollection(payload)) return payload;
  const direct = fromContainer(payload);
  if (direct) return direct;

  const nestedKeys = ['data', 'result', 'results', 'payload', 'response'];
  for (let index = 0; index < nestedKeys.length; index += 1) {
    const nested = payload?.[nestedKeys[index]];
    if (!nested || typeof nested !== 'object') continue;
    if (isLikelyRommCollection(nested)) return nested;
    const nestedCollection = fromContainer(nested);
    if (nestedCollection) return nestedCollection;
  }

  const visited = new Set();
  const queue = [{ value: payload, depth: 0 }];
  while (queue.length) {
    const { value, depth } = queue.shift();
    if (!value || typeof value !== 'object') continue;
    if (visited.has(value)) continue;
    visited.add(value);
    if (isLikelyRommCollection(value)) return value;
    if (depth >= 3) continue;
    Object.values(value).forEach((entry) => {
      if (entry && typeof entry === 'object') queue.push({ value: entry, depth: depth + 1 });
    });
  }

  return null;
}

function parseRommNumericValue(value) {
  if (value === null || value === undefined || value === '') return 0;
  const direct = Number(value);
  if (Number.isFinite(direct)) return Math.max(0, Math.round(direct));
  if (typeof value !== 'string') return 0;
  const text = value.trim();
  if (!text) return 0;
  const compact = text.replace(/[\s,]+/g, '');
  if (/^-?\d+(\.\d+)?$/.test(compact)) {
    const numeric = Number(compact);
    return Number.isFinite(numeric) ? Math.max(0, Math.round(numeric)) : 0;
  }
  const embedded = text.match(/(\d[\d,]*)/);
  if (!embedded || !embedded[1]) return 0;
  const parsed = Number(embedded[1].replace(/,/g, ''));
  return Number.isFinite(parsed) ? Math.max(0, Math.round(parsed)) : 0;
}

function parseRommCount(value, seen = new Set()) {
  const numeric = parseRommNumericValue(value);
  if (numeric > 0) return numeric;
  if (Array.isArray(value)) return value.length;
  if (!value || typeof value !== 'object') return 0;
  if (seen.has(value)) return 0;
  seen.add(value);

  const preferredCandidates = [
    value.romCount,
    value.romsCount,
    value.rom_count,
    value.roms_count,
    value.totalRoms,
    value.total_roms,
    value.gameCount,
    value.gamesCount,
    value.game_count,
    value.games_count,
    value.totalGames,
    value.total_games,
    value.count,
    value.total,
    value.entries,
    value.items,
    value.roms,
    value.games,
    value.stats,
    value.totals,
    value.summary,
  ];
  for (let index = 0; index < preferredCandidates.length; index += 1) {
    const parsed = parseRommCount(preferredCandidates[index], seen);
    if (parsed > 0) return parsed;
  }

  let best = 0;
  Object.entries(value).forEach(([rawKey, rawValue]) => {
    const key = normalizeRommAssetKey(rawKey);
    if (!key) return;
    const isCountish = key.includes('count')
      || key.includes('total')
      || key.startsWith('num')
      || key.includes('rom')
      || key.includes('game')
      || key.includes('entry')
      || key.includes('item');
    if (!isCountish) return;
    const parsed = parseRommCount(rawValue, seen);
    if (parsed > best) best = parsed;
  });
  if (best > 0) return best;

  const nestedContainers = [
    value.data,
    value.result,
    value.results,
    value.payload,
    value.response,
    value.meta,
    value.metadata,
    value.pagination,
    value.page,
    value.pageInfo,
  ];
  for (let index = 0; index < nestedContainers.length; index += 1) {
    const parsed = parseRommCount(nestedContainers[index], seen);
    if (parsed > 0) return parsed;
  }

  return 0;
}

function pickRommCount(candidates = []) {
  const list = Array.isArray(candidates) ? candidates : [candidates];
  let best = 0;
  for (let index = 0; index < list.length; index += 1) {
    const parsed = parseRommCount(list[index]);
    if (parsed > best) best = parsed;
    if (parsed > 0) return parsed;
  }
  return best;
}

function mapRommRecentlyAddedItem(entry, baseUrl = '') {
  const source = entry && typeof entry === 'object' ? entry : {};
  const game = source?.game || source?.gameInfo || source?.gameData || source?.games?.[0] || null;
  const rom = source?.rom || source?.romInfo || source?.romData || source?.roms?.[0] || null;
  const kind = normalizeRommMediaKind(
    pickFirstNonEmpty([
      source?.type,
      source?.category,
      source?.romType,
      source?.kind,
      game?.type,
      game?.category,
      game?.kind,
      rom?.type,
      rom?.category,
      rom?.kind,
    ]),
    'game'
  );
  const addedAt = pickFirstNonEmpty([
    source?.addedAt,
    source?.added_at,
    source?.createdAt,
    source?.created_at,
    source?.created,
    source?.createdOn,
    source?.created_on,
    source?.added,
    source?.addedOn,
    source?.added_on,
    source?.importedAt,
    source?.imported_at,
    source?.imported,
    source?.importedOn,
    source?.imported_on,
    source?.insertedAt,
    source?.inserted_at,
    source?.inserted,
    source?.insertedOn,
    source?.inserted_on,
    source?.timestamp,
    source?.timeAdded,
    source?.time_added,
    source?.sortTs,
    source?.sort_ts,
    source?.sortValue,
    source?.sort_value,
    game?.addedAt,
    game?.added_at,
    game?.createdAt,
    game?.created_at,
    game?.created,
    game?.createdOn,
    game?.created_on,
    game?.added,
    game?.addedOn,
    game?.added_on,
    game?.importedAt,
    game?.imported_at,
    game?.imported,
    game?.importedOn,
    game?.imported_on,
    game?.insertedAt,
    game?.inserted_at,
    game?.inserted,
    game?.insertedOn,
    game?.inserted_on,
    game?.timestamp,
    game?.timeAdded,
    game?.time_added,
    game?.sortTs,
    game?.sort_ts,
    game?.sortValue,
    game?.sort_value,
    rom?.addedAt,
    rom?.added_at,
    rom?.createdAt,
    rom?.created_at,
    rom?.created,
    rom?.createdOn,
    rom?.created_on,
    rom?.added,
    rom?.addedOn,
    rom?.added_on,
    rom?.importedAt,
    rom?.imported_at,
    rom?.imported,
    rom?.importedOn,
    rom?.imported_on,
    rom?.insertedAt,
    rom?.inserted_at,
    rom?.inserted,
    rom?.insertedOn,
    rom?.inserted_on,
    rom?.timestamp,
    rom?.timeAdded,
    rom?.time_added,
    rom?.sortTs,
    rom?.sort_ts,
    rom?.sortValue,
    rom?.sort_value,
    source?.updatedAt,
    source?.updated_at,
    source?.modifiedAt,
    source?.modified_at,
    source?.lastModified,
    source?.last_modified,
    game?.updatedAt,
    game?.updated_at,
    rom?.updatedAt,
    rom?.updated_at,
    source?.releasedAt,
    source?.releaseDate,
    source?.release_date,
    game?.releasedAt,
    game?.releaseDate,
    game?.release_date,
    rom?.releasedAt,
    rom?.releaseDate,
    rom?.release_date,
  ]);
  const addedTs = parseRommTimestamp(addedAt);
  const addedLabel = formatRommDateLabel(addedAt);
  const addedPill = formatRommRelativePill(addedAt);
  const region = pickFirstNonEmpty([
    source?.region,
    source?.releaseRegion,
    source?.country,
    source?.locale,
    game?.region,
    game?.releaseRegion,
    game?.country,
    game?.locale,
    rom?.region,
    rom?.releaseRegion,
    rom?.country,
    rom?.locale,
  ]);
  const platformName = pickFirstNonEmpty([
    source?.platformName,
    source?.platform_name,
    source?.consoleName,
    source?.systemName,
    source?.platform?.name,
    source?.console?.name,
    source?.system?.name,
    source?.platform,
    source?.console,
    source?.system,
    game?.platformName,
    game?.platform_name,
    game?.consoleName,
    game?.systemName,
    game?.platform?.name,
    game?.console?.name,
    game?.system?.name,
    game?.platform,
    game?.console,
    game?.system,
    rom?.platformName,
    rom?.platform_name,
    rom?.consoleName,
    rom?.systemName,
    rom?.platform?.name,
    rom?.console?.name,
    rom?.system?.name,
    rom?.platform,
    rom?.console,
    rom?.system,
  ]);
  const addedSummary = [addedLabel ? `Added ${addedLabel}` : '', region].filter(Boolean).join(' • ');
  const sizeBytes = parseFiniteNumber(
    source?.sizeBytes
      ?? source?.size_bytes
      ?? source?.fileSize
      ?? source?.file_size
      ?? source?.size
      ?? game?.sizeBytes
      ?? game?.size_bytes
      ?? game?.fileSize
      ?? game?.file_size
      ?? game?.size
      ?? rom?.sizeBytes
      ?? rom?.size_bytes
      ?? rom?.fileSize
      ?? rom?.file_size
      ?? rom?.size,
    0
  );
  const sizeLabel = sizeBytes > 0 ? formatBytesLabel(sizeBytes) : '';
  const extension = pickFirstNonEmpty([
    source?.extension,
    source?.fileExtension,
    source?.fileType,
    source?.format,
    game?.extension,
    game?.fileExtension,
    game?.fileType,
    game?.format,
    rom?.extension,
    rom?.fileExtension,
    rom?.fileType,
    rom?.format,
  ]).toUpperCase();
  const preferredThumbCandidates = [
    source?.url_cover,
    source?.path_cover_small,
    source?.path_cover_large,
    source?.path_cover,
    game?.coverUrl,
    game?.coverURL,
    game?.cover_url,
    game?.url_cover,
    game?.path_cover_small,
    game?.path_cover_large,
    game?.path_cover,
    game?.coverImage,
    game?.cover_image,
    game?.cover,
    game?.boxArt,
    game?.boxart,
    game?.posterUrl,
    game?.poster_url,
    game?.poster,
    game?.thumbnail,
    game?.thumb,
    rom?.coverUrl,
    rom?.coverURL,
    rom?.cover_url,
    rom?.url_cover,
    rom?.path_cover_small,
    rom?.path_cover_large,
    rom?.path_cover,
    rom?.coverImage,
    rom?.cover_image,
    rom?.cover,
    rom?.boxArt,
    rom?.boxart,
    rom?.posterUrl,
    rom?.poster_url,
    rom?.poster,
    rom?.thumbnail,
    rom?.thumb,
    source?.coverUrl,
    source?.coverURL,
    source?.cover_url,
    source?.coverImage,
    source?.cover_image,
    source?.cover,
    source?.boxArt,
    source?.boxart,
    source?.posterUrl,
    source?.poster_url,
    source?.poster,
    source?.thumbnail,
    source?.thumb,
    source?.metadatum?.url_cover,
    source?.metadatum?.path_cover_small,
    source?.metadatum?.path_cover_large,
    source?.metadatum?.path_cover,
  ];
  const thumbCandidates = [
    source?.url_cover,
    source?.path_cover_small,
    source?.path_cover_large,
    source?.path_cover,
    source?.coverUrl,
    source?.coverURL,
    source?.cover_url,
    source?.coverImage,
    source?.cover_image,
    source?.cover,
    source?.boxArt,
    source?.boxart,
    source?.posterUrl,
    source?.poster_url,
    source?.poster,
    source?.thumbnail,
    source?.thumb,
    source?.media?.cover,
    source?.assets?.cover,
    source?.assets?.boxArt,
    source?.assets?.box_art,
    source?.assets?.poster,
    source?.metadata?.cover,
    source?.metadata?.boxArt,
    source?.metadata?.box_art,
    source?.metadata?.poster,
    source?.metadatum?.url_cover,
    source?.metadatum?.path_cover_small,
    source?.metadatum?.path_cover_large,
    source?.metadatum?.path_cover,
    game?.coverUrl,
    game?.coverURL,
    game?.cover_url,
    game?.url_cover,
    game?.path_cover_small,
    game?.path_cover_large,
    game?.path_cover,
    game?.coverImage,
    game?.cover_image,
    game?.cover,
    game?.boxArt,
    game?.boxart,
    game?.posterUrl,
    game?.poster_url,
    game?.poster,
    game?.thumbnail,
    game?.thumb,
    game?.media?.cover,
    game?.assets?.cover,
    game?.assets?.boxArt,
    game?.assets?.box_art,
    game?.assets?.poster,
    game?.metadata?.cover,
    game?.metadata?.boxArt,
    game?.metadata?.box_art,
    game?.metadata?.poster,
    rom?.coverUrl,
    rom?.coverURL,
    rom?.cover_url,
    rom?.url_cover,
    rom?.path_cover_small,
    rom?.path_cover_large,
    rom?.path_cover,
    rom?.coverImage,
    rom?.cover_image,
    rom?.cover,
    rom?.boxArt,
    rom?.boxart,
    rom?.posterUrl,
    rom?.poster_url,
    rom?.poster,
    rom?.thumbnail,
    rom?.thumb,
    rom?.media?.cover,
    rom?.assets?.cover,
    rom?.assets?.boxArt,
    rom?.assets?.box_art,
    rom?.assets?.poster,
    rom?.metadata?.cover,
    rom?.metadata?.boxArt,
    rom?.metadata?.box_art,
    rom?.metadata?.poster,
    ...findRommAssetCandidates(source, {
      exactKeys: ['cover', 'cover_url', 'coverurl', 'cover_path', 'coverpath', 'path_cover', 'path_cover_small', 'path_cover_large', 'url_cover', 'box_art', 'boxart', 'poster', 'thumbnail', 'thumb'],
      containsTokens: ['cover', 'thumb', 'poster', 'boxart'],
      maxDepth: 5,
    }),
  ];
  const thumb = resolveRommAssetUrl(baseUrl, preferredThumbCandidates) || resolveRommAssetUrl(baseUrl, thumbCandidates);
  const preferredArtCandidates = [
    game?.background,
    game?.backgroundUrl,
    game?.background_url,
    game?.backdrop,
    game?.backdropUrl,
    game?.backdrop_url,
    game?.banner,
    game?.fanart,
    rom?.background,
    rom?.backgroundUrl,
    rom?.background_url,
    rom?.backdrop,
    rom?.backdropUrl,
    rom?.backdrop_url,
    rom?.banner,
    rom?.fanart,
    source?.background,
    source?.backgroundUrl,
    source?.background_url,
    source?.backdrop,
    source?.backdropUrl,
    source?.backdrop_url,
    source?.banner,
    source?.fanart,
  ];
  const artCandidates = [
    source?.background,
    source?.backgroundUrl,
    source?.background_url,
    source?.backdrop,
    source?.backdropUrl,
    source?.backdrop_url,
    source?.banner,
    source?.fanart,
    source?.media?.background,
    source?.media?.fanart,
    source?.assets?.background,
    source?.assets?.backdrop,
    source?.assets?.banner,
    source?.assets?.fanart,
    source?.metadata?.background,
    source?.metadata?.backdrop,
    source?.metadata?.banner,
    source?.metadata?.fanart,
    game?.background,
    game?.backgroundUrl,
    game?.background_url,
    game?.backdrop,
    game?.backdropUrl,
    game?.backdrop_url,
    game?.banner,
    game?.fanart,
    game?.media?.background,
    game?.media?.fanart,
    game?.assets?.background,
    game?.assets?.backdrop,
    game?.assets?.banner,
    game?.assets?.fanart,
    game?.metadata?.background,
    game?.metadata?.backdrop,
    game?.metadata?.banner,
    game?.metadata?.fanart,
    rom?.background,
    rom?.backgroundUrl,
    rom?.background_url,
    rom?.backdrop,
    rom?.backdropUrl,
    rom?.backdrop_url,
    rom?.banner,
    rom?.fanart,
    rom?.media?.background,
    rom?.media?.fanart,
    rom?.assets?.background,
    rom?.assets?.backdrop,
    rom?.assets?.banner,
    rom?.assets?.fanart,
    rom?.metadata?.background,
    rom?.metadata?.backdrop,
    rom?.metadata?.banner,
    rom?.metadata?.fanart,
    ...findRommAssetCandidates(source, {
      exactKeys: ['background', 'background_url', 'backgroundurl', 'backdrop', 'backdrop_url', 'backdropurl', 'banner', 'fanart', 'wallpaper', 'hero'],
      containsTokens: ['background', 'backdrop', 'banner', 'fanart', 'wallpaper', 'hero'],
      maxDepth: 5,
    }),
  ];
  const art = resolveRommAssetUrl(baseUrl, preferredArtCandidates) || resolveRommAssetUrl(baseUrl, artCandidates);
  const overview = pickFirstNonEmpty([
    source?.overview,
    source?.summary,
    source?.description,
    source?.synopsis,
    source?.plot,
    game?.overview,
    game?.summary,
    game?.description,
    game?.synopsis,
    game?.plot,
    rom?.overview,
    rom?.summary,
    rom?.description,
    rom?.synopsis,
    rom?.plot,
  ]);
  const subtitle = platformName || '-';
  const metaParts = [];
  if (platformName && addedSummary) metaParts.push(addedSummary);
  if (!platformName && addedLabel) metaParts.push(`Added ${addedLabel}`);
  if (!platformName && region) metaParts.push(region);
  if (sizeLabel) metaParts.push(sizeLabel);
  if (extension) metaParts.push(extension);
  return {
    id: pickFirstNonEmpty([
      source?.id,
      source?.uuid,
      source?.slug,
      source?.romId,
      source?.gameId,
      game?.id,
      game?.uuid,
      game?.slug,
      rom?.id,
      rom?.uuid,
      rom?.slug,
      source?.name,
      source?.title,
    ]),
    kind,
    title: pickFirstNonEmpty([
      game?.title,
      game?.name,
      game?.gameTitle,
      source?.title,
      source?.name,
      source?.gameTitle,
      source?.romName,
      rom?.title,
      rom?.name,
      rom?.romName,
      source?.filename,
      rom?.filename,
      game?.filename,
      'Unknown',
    ]),
    subtitle,
    meta: metaParts.join(' • '),
    pill: addedPill,
    user: '',
    overview,
    thumb,
    art,
    sortTs: addedTs,
  };
}

function mapRommConsoleItem(entry, baseUrl = '') {
  const kind = normalizeRommMediaKind(
    pickFirstNonEmpty([entry?.type, entry?.category, entry?.platformType, entry?.deviceType]),
    'console'
  );
  const title = pickFirstNonEmpty([entry?.name, entry?.title, entry?.platformName, entry?.consoleName, entry?.systemName, 'Unknown Console']);
  const romCount = pickRommCount([
    entry?.romCount,
    entry?.romsCount,
    entry?.rom_count,
    entry?.roms_count,
    entry?.totalRoms,
    entry?.total_roms,
    entry?.stats?.roms,
    entry?.stats?.romCount,
    entry?.stats?.rom_count,
    entry?.totals?.roms,
    entry?.totals?.romCount,
    entry?.totals?.rom_count,
    entry?.roms,
    entry?.count,
    entry?.totals,
    entry?.stats,
    entry?.gameCount,
    entry?.gamesCount,
    entry?.game_count,
    entry?.games_count,
    entry?.games,
  ]);
  const gameCount = pickRommCount([
    entry?.gameCount,
    entry?.gamesCount,
    entry?.game_count,
    entry?.games_count,
    entry?.totalGames,
    entry?.total_games,
    entry?.stats?.games,
    entry?.stats?.gameCount,
    entry?.stats?.game_count,
    entry?.totals?.games,
    entry?.totals?.gameCount,
    entry?.totals?.game_count,
    entry?.games,
  ]);
  const biosCount = pickRommCount([
    entry?.biosCount,
    entry?.bios_count,
    entry?.stats?.bios,
    entry?.stats?.biosCount,
    entry?.stats?.bios_count,
    entry?.totals?.bios,
    entry?.totals?.biosCount,
    entry?.totals?.bios_count,
    entry?.bios,
  ]);
  const saveCount = pickRommCount([
    entry?.saveCount,
    entry?.save_count,
    entry?.savesCount,
    entry?.saves_count,
    entry?.stats?.saves,
    entry?.stats?.saveCount,
    entry?.stats?.save_count,
    entry?.totals?.saves,
    entry?.totals?.saveCount,
    entry?.totals?.save_count,
    entry?.saves,
  ]);
  const screenshotCount = pickRommCount([
    entry?.screenshotCount,
    entry?.screenshot_count,
    entry?.screenshotsCount,
    entry?.screenshots_count,
    entry?.stats?.screenshots,
    entry?.stats?.screenshotCount,
    entry?.stats?.screenshot_count,
    entry?.totals?.screenshots,
    entry?.totals?.screenshotCount,
    entry?.totals?.screenshot_count,
    entry?.screenshots,
  ]);
  const generation = pickFirstNonEmpty([entry?.generation, entry?.era, entry?.family, entry?.releaseYear, entry?.year]);
  const manufacturer = pickFirstNonEmpty([entry?.manufacturer, entry?.vendor, entry?.company]);
  const statItems = [];
  const pushStat = (label, rawValue) => {
    const textLabel = String(label || '').trim();
    const textValue = String(rawValue || '').trim();
    if (!textLabel || !textValue) return;
    if (statItems.some((item) => item.label === textLabel)) return;
    statItems.push({ label: textLabel, value: textValue });
  };
  pushStat('ROMs', romCount > 0 ? romCount.toLocaleString() : '0');
  if (gameCount > 0 && gameCount !== romCount) pushStat('Games', gameCount.toLocaleString());
  if (biosCount > 0) pushStat('BIOS', biosCount.toLocaleString());
  if (saveCount > 0) pushStat('Saves', saveCount.toLocaleString());
  if (screenshotCount > 0) pushStat('Screenshots', screenshotCount.toLocaleString());
  if (manufacturer) pushStat('Manufacturer', manufacturer);
  if (generation) pushStat('Generation', generation);
  const subDetail = `${romCount.toLocaleString()} ROM${romCount === 1 ? '' : 's'}`;
  const fallbackPlatformAssets = buildRommPlatformAssetCandidates(entry, title);
  const thumbCandidates = [
    entry?.icon,
    entry?.iconUrl,
    entry?.icon_url,
    entry?.iconPath,
    entry?.icon_path,
    entry?.iconFile,
    entry?.icon_file,
    entry?.logo,
    entry?.logoUrl,
    entry?.logo_url,
    entry?.logoPath,
    entry?.logo_path,
    entry?.image,
    entry?.imageUrl,
    entry?.image_url,
    entry?.imagePath,
    entry?.image_path,
    entry?.cover,
    entry?.coverUrl,
    entry?.cover_url,
    entry?.coverPath,
    entry?.cover_path,
    entry?.coverImage,
    entry?.cover_image,
    entry?.boxArt,
    entry?.box_art,
    entry?.boxart,
    entry?.poster,
    entry?.posterUrl,
    entry?.poster_url,
    entry?.artwork,
    entry?.artworkUrl,
    entry?.artwork_url,
    entry?.thumbnail,
    entry?.thumb,
    entry?.banner,
    entry?.asset,
    entry?.assetUrl,
    entry?.asset_url,
    entry?.assets?.icon,
    entry?.assets?.iconUrl,
    entry?.assets?.icon_url,
    entry?.assets?.logo,
    entry?.assets?.logoUrl,
    entry?.assets?.logo_url,
    entry?.assets?.cover,
    entry?.assets?.coverUrl,
    entry?.assets?.cover_url,
    entry?.assets?.boxArt,
    entry?.assets?.box_art,
    entry?.assets?.poster,
    entry?.assets?.thumbnail,
    entry?.assets?.image,
    entry?.assets?.artwork,
    entry?.assets?.banner,
    entry?.metadata?.icon,
    entry?.metadata?.logo,
    entry?.metadata?.cover,
    entry?.metadata?.boxArt,
    entry?.metadata?.box_art,
    entry?.metadata?.poster,
    entry?.metadata?.thumbnail,
    entry?.metadata?.image,
    entry?.metadata?.artwork,
    entry?.metadata?.banner,
    entry?.metadata?.iconPath,
    entry?.metadata?.icon_path,
    entry?.metadata?.coverPath,
    entry?.metadata?.cover_path,
    entry?.roms?.[0]?.cover,
    entry?.roms?.[0]?.coverUrl,
    entry?.roms?.[0]?.cover_url,
    entry?.roms?.[0]?.thumb,
    entry?.roms?.[0]?.thumbnail,
    entry?.games?.[0]?.cover,
    entry?.games?.[0]?.coverUrl,
    entry?.games?.[0]?.cover_url,
    entry?.games?.[0]?.thumb,
    entry?.games?.[0]?.thumbnail,
    ...fallbackPlatformAssets,
    ...findRommAssetCandidates(entry, {
      exactKeys: ['icon', 'icon_path', 'iconpath', 'logo', 'image', 'image_path', 'imagepath', 'cover', 'cover_url', 'coverurl', 'cover_path', 'coverpath', 'box_art', 'boxart', 'poster', 'thumbnail', 'thumb', 'artwork', 'custom_cover', 'customcover', 'screenshot', 'screenshots'],
      containsTokens: ['cover', 'thumb', 'poster', 'logo', 'icon', 'image', 'boxart', 'artwork', 'screenshot', 'asset'],
      maxDepth: 5,
    }),
  ];
  const thumb = resolveRommAssetUrl(baseUrl, thumbCandidates);
  const artCandidates = [
    entry?.background,
    entry?.backgroundUrl,
    entry?.background_url,
    entry?.banner,
    entry?.fanart,
    entry?.backdrop,
    entry?.backdropUrl,
    entry?.backdrop_url,
    entry?.wallpaper,
    entry?.hero,
    entry?.assets?.background,
    entry?.assets?.backdrop,
    entry?.assets?.banner,
    entry?.assets?.fanart,
    entry?.assets?.wallpaper,
    entry?.metadata?.background,
    entry?.metadata?.backdrop,
    entry?.metadata?.banner,
    entry?.metadata?.fanart,
    entry?.metadata?.wallpaper,
    ...findRommAssetCandidates(entry, {
      exactKeys: ['background', 'background_url', 'backgroundurl', 'backdrop', 'backdrop_url', 'backdropurl', 'banner', 'fanart', 'wallpaper', 'hero'],
      containsTokens: ['background', 'backdrop', 'banner', 'fanart', 'wallpaper', 'hero'],
      maxDepth: 5,
    }),
  ];
  const art = resolveRommAssetUrl(baseUrl, artCandidates);
  const resolvedThumb = thumb || art;
  const resolvedArt = art || thumb;
  const existingOverview = pickFirstNonEmpty([
    entry?.overview,
    entry?.summary,
    entry?.description,
    entry?.notes,
  ]);
  const generatedOverview = statItems.length
    ? `Library summary: ${statItems.map((item) => `${item.label}: ${item.value}`).join(' • ')}.`
    : '';
  const overview = existingOverview || generatedOverview;
  return {
    id: pickFirstNonEmpty([entry?.id, entry?.uuid, entry?.slug, entry?.platformId, entry?.consoleId, entry?.name]),
    kind,
    title,
    subtitle: subDetail,
    meta: [manufacturer, generation].filter(Boolean).join(' • '),
    pill: generation || '',
    user: '',
    overview,
    romCount,
    stats: statItems,
    thumb: resolvedThumb,
    art: resolvedArt,
    sortTs: romCount,
  };
}
// /api/jackett/search/filters — moved to src/routes/api-arr.js
// /api/jackett/search — moved to src/routes/api-arr.js
// /api/bazarr/subtitle-queue — moved to src/routes/api-arr.js
// /api/autobrr/:kind — moved to src/routes/api-arr.js
// maintainerrTmdbAssetCache const + routes — moved to src/routes/api-arr.js
// /api/maintainerr/rules/:id/execute — moved to src/routes/api-arr.js
// /api/romm/:kind — moved to src/routes/api-specialty.js
// /api/cleanuparr/:kind — moved to src/routes/api-arr.js
// /api/widgets/cards (POST) — moved to src/routes/api-specialty.js
// /api/widgets/cards/:id (PUT) — moved to src/routes/api-specialty.js
// /api/widgets/cards/:id (DELETE) — moved to src/routes/api-specialty.js

function buildBasicAuthHeader(username, password) {
  const user = String(username || '');
  const pass = String(password || '');
  if (!user && !pass) return '';
  return `Basic ${Buffer.from(`${user}:${pass}`).toString('base64')}`;
}

function injectBasicAuthIntoUrl(rawUrl, username, password) {
  const input = String(rawUrl || '').trim();
  if (!input) return '';
  const user = String(username || '').trim();
  const pass = String(password || '');
  if (!user && !pass) return input;

  try {
    const parsed = new URL(input);
    if (!/^https?:$/i.test(parsed.protocol)) return input;
    parsed.username = user;
    parsed.password = pass;
    return parsed.toString();
  } catch (err) {
    return input;
  }
}

function hasEmbeddedUrlCredentials(rawUrl) {
  const input = String(rawUrl || '').trim();
  if (!input) return false;
  try {
    const parsed = new URL(input);
    return Boolean(parsed.username || parsed.password);
  } catch (err) {
    return false;
  }
}

function stripUrlEmbeddedCredentials(rawUrl) {
  const input = String(rawUrl || '').trim();
  if (!input) return '';
  try {
    const parsed = new URL(input);
    parsed.username = '';
    parsed.password = '';
    return parsed.toString();
  } catch (err) {
    return input;
  }
}

function splitSetCookieHeaderValue(value) {
  const header = String(value || '').trim();
  if (!header) return [];
  return header
    .split(/,(?=\s*[!#$%&'*+\-.^_`|~0-9A-Za-z]+=)/g)
    .map((item) => String(item || '').trim())
    .filter(Boolean);
}

function getFetchResponseSetCookies(response) {
  if (!response?.headers) return [];
  if (typeof response.headers.getSetCookie === 'function') {
    const values = response.headers.getSetCookie();
    return Array.isArray(values) ? values.map((item) => String(item || '').trim()).filter(Boolean) : [];
  }
  return splitSetCookieHeaderValue(response.headers.get('set-cookie'));
}

function getCookieValueFromSetCookie(setCookie, cookieName) {
  const raw = String(setCookie || '').trim();
  const target = String(cookieName || '').trim();
  if (!raw || !target) return '';
  const first = raw.split(';')[0] || '';
  const eqIndex = first.indexOf('=');
  if (eqIndex <= 0) return '';
  const name = first.slice(0, eqIndex).trim();
  if (name !== target) return '';
  return first.slice(eqIndex + 1);
}

function getCookieNameFromSetCookie(setCookie) {
  const raw = String(setCookie || '').trim();
  if (!raw) return '';
  const first = raw.split(';')[0] || '';
  const eqIndex = first.indexOf('=');
  if (eqIndex <= 0) return '';
  return first.slice(0, eqIndex).trim();
}

function findSetCookieValueByNames(setCookies = [], cookieNames = []) {
  const accepted = new Set(
    (Array.isArray(cookieNames) ? cookieNames : [])
      .map((name) => String(name || '').trim().toLowerCase())
      .filter(Boolean),
  );
  if (!accepted.size) return '';
  const list = Array.isArray(setCookies) ? setCookies : [];
  for (let index = 0; index < list.length; index += 1) {
    const cookie = list[index];
    const cookieName = getCookieNameFromSetCookie(cookie);
    if (!cookieName) continue;
    if (!accepted.has(cookieName.toLowerCase())) continue;
    const value = getCookieValueFromSetCookie(cookie, cookieName);
    if (value) return value;
  }
  return '';
}

function getRommCsrfTokenFromSetCookies(setCookies = []) {
  return findSetCookieValueByNames(setCookies, ['romm_csrftoken', 'csrftoken', 'csrf_token']);
}

function hasRommSessionCookie(setCookies = []) {
  const names = (Array.isArray(setCookies) ? setCookies : [])
    .map((cookie) => getCookieNameFromSetCookie(cookie).toLowerCase())
    .filter(Boolean);
  return names.some((name) => (
    name === 'romm_session'
    || name === 'session_id'
    || name === 'sessionid'
    || /^romm_.*session/.test(name)
    || name.endsWith('_session')
  ));
}

function hasHostPrefixedRommSessionCookie(setCookies = []) {
  const names = (Array.isArray(setCookies) ? setCookies : [])
    .map((cookie) => getCookieNameFromSetCookie(cookie).toLowerCase())
    .filter(Boolean);
  return names.some((name) => (
    name.startsWith('__host-')
    && (
      name === '__host-romm_session'
      || /^__host-romm_.*session/.test(name)
      || name.endsWith('_session')
    )
  ));
}

function buildCookieHeaderFromSetCookies(setCookies = []) {
  return (Array.isArray(setCookies) ? setCookies : [])
    .map((value) => String(value || '').split(';')[0].trim())
    .filter(Boolean)
    .join('; ');
}

function stripCookieDomainAttribute(setCookie) {
  return String(setCookie || '')
    .replace(/;\s*Domain=[^;]*/ig, '')
    .trim();
}

function rewriteSetCookieDomainAttribute(setCookie, cookieDomain) {
  const cleaned = stripCookieDomainAttribute(setCookie);
  const domain = normalizeHostnameForCompare(cookieDomain);
  if (!cleaned || !domain) return cleaned;
  return `${cleaned}; Domain=${domain}`;
}

function normalizeHostnameForCompare(value) {
  return String(value || '').trim().toLowerCase().replace(/^\[|\]$/g, '');
}

function extractHostnameFromUrl(value) {
  const input = String(value || '').trim();
  if (!input) return '';
  try {
    const parsed = new URL(input);
    return normalizeHostnameForCompare(parsed.hostname);
  } catch (err) {
    return '';
  }
}

function getRequestHostname(req) {
  const rawRequestHost = String(getRequestHost(req) || '').trim();
  return normalizeHostnameForCompare(rawRequestHost.split(':')[0] || rawRequestHost);
}

function getConfiguredLauncharrHostname(config, req) {
  const generalSettings = resolveGeneralSettings(config || {});
  const configuredCandidates = [
    generalSettings.remoteUrl,
    generalSettings.localUrl,
    resolvePublicBaseUrl(req),
  ];
  for (let index = 0; index < configuredCandidates.length; index += 1) {
    const host = extractHostnameFromUrl(configuredCandidates[index]);
    if (host) return host;
  }
  return '';
}

function buildRommCookiePrimingPlan({ config, req, browserUrl }) {
  const targetHost = extractHostnameFromUrl(browserUrl);
  const requestHost = getRequestHostname(req);
  const configuredLauncharrHost = getConfiguredLauncharrHostname(config, req);
  if (!targetHost) {
    return {
      canPrime: false,
      mode: 'none',
      cookieDomain: '',
      requestHost,
      targetHost,
      configuredLauncharrHost,
      reason: 'Missing Romm browser hostname.',
    };
  }

  if (requestHost && requestHost === targetHost) {
    return {
      canPrime: true,
      mode: 'host-only',
      cookieDomain: '',
      requestHost,
      targetHost,
      configuredLauncharrHost,
      reason: '',
    };
  }

  if (configuredLauncharrHost) {
    const candidate = configuredLauncharrHost;
    const targetMatchesShared = targetHost === candidate || targetHost.endsWith(`.${candidate}`);
    if (targetMatchesShared && candidate.includes('.')) {
      return {
        canPrime: true,
        mode: 'shared-domain',
        cookieDomain: candidate,
        requestHost,
        targetHost,
        configuredLauncharrHost,
        reason: '',
      };
    }
  }

  return {
    canPrime: false,
    mode: 'none',
    cookieDomain: '',
    requestHost,
    targetHost,
    configuredLauncharrHost,
    reason: 'Launcharr and Romm hosts do not share a cookie-priming domain.',
  };
}

function evaluateRommCookiePrimingCompatibility(setCookies, plan) {
  if (!plan?.canPrime) {
    return {
      ok: false,
      blocking: true,
      reason: String(plan?.reason || 'Browser cookie priming is not available.').trim(),
    };
  }

  if (plan.mode === 'shared-domain' && hasHostPrefixedRommSessionCookie(setCookies)) {
    // __Host- cookies must be host-only (no Domain attribute). Shared-domain priming
    // rewrites Domain=... so the browser will reject the cookie.
    return {
      ok: false,
      blocking: true,
      reason: 'Romm returned a __Host- prefixed session cookie, which browsers reject when Launcharr rewrites it to a shared Domain for subdomain cookie priming.',
    };
  }

  return { ok: true, blocking: false, reason: '' };
}

function prepareRommPrimedSetCookies(setCookies, plan) {
  const list = Array.isArray(setCookies) ? setCookies : [];
  if (!list.length) return [];
  const compatibility = evaluateRommCookiePrimingCompatibility(list, plan);
  if (!compatibility.ok) return [];
  if (plan.mode === 'shared-domain' && plan.cookieDomain) {
    return list.map((cookie) => rewriteSetCookieDomainAttribute(cookie, plan.cookieDomain)).filter(Boolean);
  }
  return list.map((cookie) => stripCookieDomainAttribute(cookie)).filter(Boolean);
}

async function bootstrapRommIframeSession({ req, launchUrl, authBaseCandidates = [] }) {
  const input = String(launchUrl || '').trim();
  if (!input) return { ok: false, error: 'Missing launch URL.' };
  let parsed;
  try {
    parsed = new URL(input);
  } catch (err) {
    return { ok: false, error: 'Invalid launch URL.' };
  }

  const username = String(parsed.username || '');
  const password = String(parsed.password || '');
  if (!username && !password) {
    return { ok: false, error: 'Missing embedded credentials.' };
  }

  const cleanLaunchUrl = stripUrlEmbeddedCredentials(input);
  const apiCandidates = uniqueList([
    ...(Array.isArray(authBaseCandidates) ? authBaseCandidates : []),
    cleanLaunchUrl,
  ])
    .map((candidate) => normalizeBaseUrl(candidate))
    .filter(Boolean);
  if (!apiCandidates.length) return { ok: false, error: 'Invalid Romm base URL.' };

  let lastError = '';
  const attemptedBases = [];
  for (let index = 0; index < apiCandidates.length; index += 1) {
    const apiBase = apiCandidates[index];
    if (!apiBase) continue;
    attemptedBases.push(apiBase);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    try {
      const heartbeatUrl = buildAppApiUrl(apiBase, 'api/heartbeat').toString();
      const heartbeatResponse = await fetch(heartbeatUrl, {
        headers: { Accept: 'application/json' },
        signal: controller.signal,
      });
      const heartbeatText = await heartbeatResponse.text().catch(() => '');
      if (!heartbeatResponse.ok) {
        lastError = `Romm heartbeat failed (${heartbeatResponse.status}) via ${apiBase}. ${heartbeatText || ''}`.trim();
        continue;
      }
      const heartbeatCookies = getFetchResponseSetCookies(heartbeatResponse);
      const csrfToken = getRommCsrfTokenFromSetCookies(heartbeatCookies);

      const loginUrl = buildAppApiUrl(apiBase, 'api/login').toString();
      const loginHeaders = {
        Accept: 'application/json',
        Authorization: buildBasicAuthHeader(username, password),
      };
      if (csrfToken) loginHeaders['x-csrftoken'] = csrfToken;
      const heartbeatCookieHeader = buildCookieHeaderFromSetCookies(heartbeatCookies);
      if (heartbeatCookieHeader) loginHeaders.Cookie = heartbeatCookieHeader;

      const loginResponse = await fetch(loginUrl, {
        method: 'POST',
        headers: loginHeaders,
        signal: controller.signal,
      });
      const loginText = await loginResponse.text().catch(() => '');
      if (!loginResponse.ok) {
        lastError = `Romm login failed (${loginResponse.status}) via ${apiBase}. ${loginText || ''}`.trim();
        continue;
      }

      const loginCookies = getFetchResponseSetCookies(loginResponse);
      const forwardedCookies = [...heartbeatCookies, ...loginCookies]
        .map(stripCookieDomainAttribute)
        .filter(Boolean);
      const hasSessionCookie = hasRommSessionCookie(forwardedCookies);
      if (!hasSessionCookie) {
        lastError = `Romm login via ${apiBase} did not return a supported session cookie.`;
        continue;
      }

      return {
        ok: true,
        launchUrl: cleanLaunchUrl,
        setCookies: forwardedCookies,
        authBaseUrl: apiBase,
        attemptedBases,
      };
    } catch (err) {
      lastError = (safeMessage(err) || 'Failed to bootstrap Romm iframe session.') + ` via ${apiBase}.`;
    } finally {
      clearTimeout(timeout);
    }
  }

  return {
    ok: false,
    error: lastError || 'Failed to bootstrap Romm iframe session.',
    attemptedBases,
  };
}

function resolveRoleAwareLaunchUrl(appItem, req, launchUrl, roleOverride = '') {
  const resolved = String(launchUrl || '').trim();
  if (!resolved) return '';
  const baseId = getAppBaseId(appItem?.id);
  if (baseId !== 'romm') return resolved;

  const role = String(roleOverride || getEffectiveRole(req) || '').trim().toLowerCase();
  if (role === 'user' || role === 'co-admin') {
    const viewerUsername = String(appItem?.viewerUsername || '').trim();
    const viewerPassword = String(appItem?.viewerPassword || '');
    return injectBasicAuthIntoUrl(resolved, viewerUsername, viewerPassword);
  }
  return resolved;
}

function buildAppApiUrl(baseUrl, suffixPath) {
  const url = new URL(baseUrl);
  const suffix = String(suffixPath || '').trim().replace(/^\/+/, '');
  const basePath = String(url.pathname || '/').replace(/\/+$/, '');
  const joined = `${basePath}/${suffix}`.replace(/\/{2,}/g, '/');
  url.pathname = joined.startsWith('/') ? joined : `/${joined}`;
  return url;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function sendClientLaunchRedirectPage(res, targetUrl, options = {}) {
  const href = String(targetUrl || '').trim();
  if (!href) return res.status(400).send('Launch URL not configured.');
  const title = escapeHtml(String(options?.title || 'Launch').trim() || 'Launch');
  const message = escapeHtml(String(options?.message || 'Redirecting...').trim() || 'Redirecting...');
  const escapedHref = escapeHtml(href);
  const scriptHref = JSON.stringify(href);
  const script = `
  <script>
    (function () {
      var href = ${scriptHref};
      if (!href) return;
      try { window.location.replace(href); return; } catch (e) {}
      window.location.href = href;
    }());
  </script>`;
  return res
    .status(200)
    .type('html')
    .send(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${title}</title>
  <meta http-equiv="refresh" content="0;url=${escapedHref}">
  <style>
    body { margin: 0; font-family: system-ui, -apple-system, Segoe UI, sans-serif; background: #0f172a; color: #e2e8f0; display: grid; place-items: center; min-height: 100vh; }
    .card { width: min(28rem, calc(100vw - 2rem)); background: rgba(15, 23, 42, 0.92); border: 1px solid rgba(148, 163, 184, 0.28); border-radius: 12px; padding: 1rem 1.1rem; box-shadow: 0 12px 30px rgba(2, 6, 23, 0.35); }
    .muted { color: #94a3b8; font-size: 0.92rem; margin-top: 0.35rem; word-break: break-word; }
    a { color: #93c5fd; }
  </style>
</head>
<body>
  <div class="card" role="status" aria-live="polite">
    <div>${message}</div>
    <div class="muted">If you are not redirected, <a href="${escapedHref}" rel="noreferrer">continue to the app</a>.</div>
  </div>
  ${script}
</body>
</html>`);
}

function buildRommLaunchDebugPayload({
  req,
  primingPlan,
  rommBootstrap,
  primedCookies = [],
  browserLaunchTarget = '',
  launchMode = '',
}) {
  const cookieNames = uniqueList(
    (Array.isArray(primedCookies) ? primedCookies : [])
      .map((cookie) => String(cookie || '').split(';')[0])
      .map((pair) => String(pair || '').split('=')[0].trim())
      .filter(Boolean)
  );
  const plan = primingPlan && typeof primingPlan === 'object' ? primingPlan : {};
  const bootstrap = rommBootstrap && typeof rommBootstrap === 'object' ? rommBootstrap : {};
  return {
    launchMode: String(launchMode || '').trim(),
    browserLaunchTarget: String(browserLaunchTarget || '').trim(),
    bootstrapOk: !!bootstrap.ok,
    bootstrapError: String(bootstrap.error || '').trim(),
    authBaseUrl: String(bootstrap.authBaseUrl || '').trim(),
    attemptedBases: Array.isArray(bootstrap.attemptedBases) ? bootstrap.attemptedBases : [],
    primedCookieCount: cookieNames.length,
    primedCookieNames: cookieNames,
    primingPlan: {
      canPrime: !!plan.canPrime,
      mode: String(plan.mode || '').trim(),
      cookieDomain: String(plan.cookieDomain || '').trim(),
      requestHost: String(plan.requestHost || '').trim(),
      targetHost: String(plan.targetHost || '').trim(),
      configuredLauncharrHost: String(plan.configuredLauncharrHost || '').trim(),
      reason: String(plan.reason || '').trim(),
    },
    userAgent: String(req?.headers?.['user-agent'] || '').trim(),
  };
}

function logRommLaunchServerDiagnostic(req, payload) {
  try {
    const details = buildRommLaunchDebugPayload({
      req,
      primingPlan: payload?.primingPlan,
      rommBootstrap: payload?.rommBootstrap,
      primedCookies: payload?.primedCookies,
      browserLaunchTarget: payload?.browserLaunchTarget,
      launchMode: payload?.launchMode,
    });
    const logPayload = redactRommLaunchLogPayloadHosts({
      ts: new Date().toISOString(),
      route: String(payload?.route || 'launch').trim() || 'launch',
      stage: String(payload?.stage || '').trim(),
      role: String(payload?.role || '').trim(),
      path: String(req?.originalUrl || req?.url || '').trim(),
      ...details,
    });
    console.log(`[romm-launch] ${JSON.stringify(logPayload)}`);
  } catch (err) {
    console.warn('[romm-launch] Failed to emit diagnostic log:', safeMessage(err));
  }
}

function redactRommLaunchLogPayloadHosts(payload) {
  if ((!LOG_REDACT_HOSTS && !LOG_REDACT_IPS) || !payload || typeof payload !== 'object') return payload;
  const next = { ...payload };
  next.browserLaunchTarget = redactLogHostOrUrl(next.browserLaunchTarget);
  next.authBaseUrl = redactLogHostOrUrl(next.authBaseUrl);
  next.attemptedBases = Array.isArray(next.attemptedBases)
    ? next.attemptedBases.map((value) => redactLogHostOrUrl(value))
    : next.attemptedBases;
  if (next.primingPlan && typeof next.primingPlan === 'object') {
    next.primingPlan = {
      ...next.primingPlan,
      requestHost: redactLogHostOrUrl(next.primingPlan.requestHost),
      targetHost: redactLogHostOrUrl(next.primingPlan.targetHost),
      configuredLauncharrHost: redactLogHostOrUrl(next.primingPlan.configuredLauncharrHost),
    };
  }
  return next;
}

function redactLogHostOrUrl(value) {
  const input = String(value ?? '').trim();
  if ((!LOG_REDACT_HOSTS && !LOG_REDACT_IPS) || !input) return input;

  if (/^https?:\/\//i.test(input)) {
    try {
      const parsed = new URL(input);
      if (parsed.hostname) parsed.hostname = redactHostnameToken(parsed.hostname);
      return parsed.toString();
    } catch (_err) {
      return input;
    }
  }

  if (/^[^\s/]+$/.test(input)) {
    try {
      const parsed = new URL(`http://${input}`);
      if (parsed.hostname) parsed.hostname = redactHostnameToken(parsed.hostname);
      return parsed.host;
    } catch (_err) {
      return input;
    }
  }

  return input;
}

function redactHostnameToken(hostname) {
  const raw = String(hostname ?? '').trim();
  if (!raw) return raw;
  const redactedIp = redactLogIp(raw);
  if (LOG_REDACT_IPS && redactedIp && redactedIp !== raw) return redactedIp;
  if (LOG_REDACT_HOSTS) return getRedactedHostAlias(raw);
  return raw;
}

function getRedactedHostAlias(hostname) {
  const raw = String(hostname ?? '').trim();
  if (!raw) return raw;
  const key = raw.toLowerCase();
  if (LOG_HOST_ALIAS_CACHE.has(key)) return LOG_HOST_ALIAS_CACHE.get(key);
  const alias = `host-${crypto.createHash('sha256').update(key).digest('hex').slice(0, 8)}`;
  LOG_HOST_ALIAS_CACHE.set(key, alias);
  return alias;
}

async function fetchTransmissionQueue(baseUrl, authHeader) {
  const rpcUrl = buildAppApiUrl(baseUrl, 'transmission/rpc');
  const payload = {
    method: 'torrent-get',
    arguments: {
      fields: [
        'id',
        'name',
        'status',
        'percentDone',
        'eta',
        'rateDownload',
        'rateUpload',
        'sizeWhenDone',
        'totalSize',
        'leftUntilDone',
        'addedDate',
        'isFinished',
        'isStalled',
        'error',
        'errorString',
      ],
    },
  };

  let sessionId = '';
  let lastError = '';

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const headers = {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    };
    if (authHeader) headers.Authorization = authHeader;
    if (sessionId) headers['X-Transmission-Session-Id'] = sessionId;

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    try {
      const response = await fetch(rpcUrl.toString(), {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
      if (response.status === 409) {
        sessionId = response.headers.get('x-transmission-session-id') || '';
        if (sessionId) continue;
        lastError = 'Transmission session negotiation failed.';
        break;
      }
      const text = await response.text();
      if (!response.ok) {
        lastError = `Transmission request failed (${response.status}).`;
        break;
      }
      const parsed = text ? JSON.parse(text) : {};
      const list = Array.isArray(parsed?.arguments?.torrents) ? parsed.arguments.torrents : [];
      return { items: list };
    } catch (err) {
      lastError = safeMessage(err) || 'Failed to reach Transmission.';
    } finally {
      clearTimeout(timeout);
    }
  }

  return { error: lastError || 'Failed to reach Transmission.' };
}

async function fetchNzbgetQueue(baseUrl, authHeader) {
  const rpcUrl = buildAppApiUrl(baseUrl, 'jsonrpc');
  const payload = {
    method: 'listgroups',
    params: [0],
    id: Date.now(),
  };
  const headers = {
    Accept: 'application/json',
    'Content-Type': 'application/json',
  };
  if (authHeader) headers.Authorization = authHeader;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12000);
  try {
    const response = await fetch(rpcUrl.toString(), {
      method: 'POST',
      headers,
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      return { error: `NZBGet request failed (${response.status}).` };
    }
    const parsed = text ? JSON.parse(text) : {};
    const list = Array.isArray(parsed?.result) ? parsed.result : [];
    return { items: list };
  } catch (err) {
    return { error: safeMessage(err) || 'Failed to reach NZBGet.' };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchQbittorrentQueue(baseUrl, username, password) {
  const user = String(username || '').trim();
  const pass = String(password || '').trim();
  let cookieHeader = '';

  if (user || pass) {
    const loginUrl = buildAppApiUrl(baseUrl, 'api/v2/auth/login');
    const loginPayload = new URLSearchParams({
      username: user,
      password: pass,
    });
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    try {
      const response = await fetch(loginUrl.toString(), {
        method: 'POST',
        headers: {
          Accept: 'text/plain',
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        },
        body: loginPayload.toString(),
        signal: controller.signal,
      });
      const text = await response.text();
      if (!response.ok || !/^ok\.?$/i.test(String(text || '').trim())) {
        return { error: `qBittorrent authentication failed (${response.status}).` };
      }
      const setCookie = String(response.headers.get('set-cookie') || '').trim();
      const firstCookie = setCookie.split(';')[0].trim();
      if (firstCookie) cookieHeader = firstCookie;
    } catch (err) {
      return { error: safeMessage(err) || 'Failed to authenticate with qBittorrent.' };
    } finally {
      clearTimeout(timeout);
    }
  }

  const infoUrl = buildAppApiUrl(baseUrl, 'api/v2/torrents/info');
  const headers = { Accept: 'application/json' };
  if (cookieHeader) headers.Cookie = cookieHeader;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12000);
  try {
    const response = await fetch(infoUrl.toString(), {
      headers,
      signal: controller.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      return { error: `qBittorrent request failed (${response.status}).` };
    }
    const parsed = text ? JSON.parse(text) : [];
    const list = Array.isArray(parsed) ? parsed : [];
    return { items: list };
  } catch (err) {
    return { error: safeMessage(err) || 'Failed to reach qBittorrent.' };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchSabnzbdQueue(baseUrl, apiKey, authHeader) {
  const queueUrl = buildAppApiUrl(baseUrl, 'api');
  queueUrl.searchParams.set('mode', 'queue');
  queueUrl.searchParams.set('output', 'json');
  if (apiKey) queueUrl.searchParams.set('apikey', apiKey);

  const headers = { Accept: 'application/json' };
  if (authHeader) headers.Authorization = authHeader;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12000);
  try {
    const response = await fetch(queueUrl.toString(), {
      headers,
      signal: controller.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      return { error: `SABnzbd request failed (${response.status}).` };
    }
    const parsed = text ? JSON.parse(text) : {};
    const slots = parsed?.queue?.slots;
    const list = Array.isArray(slots) ? slots : [];
    return { items: list };
  } catch (err) {
    return { error: safeMessage(err) || 'Failed to reach SABnzbd.' };
  } finally {
    clearTimeout(timeout);
  }
}
// /api/downloaders/:appId/queue — moved to src/routes/api-arr.js
// /api/arr/:appId/:version/* — moved to src/routes/api-arr.js
// /api/arr/* — moved to src/routes/api-arr.js

// GET /api/logs — moved to src/routes/api-util.js

// POST /api/logs/client — moved to src/routes/api-util.js

// GET /switch-view — moved to src/routes/api-util.js

// GET /logout — moved to src/routes/auth.js

// GET /healthz — moved to src/routes/api-util.js

app.use((err, req, res, next) => {
  pushLog({
    level: 'error',
    app: 'system',
    action: 'server.error',
    message: safeMessage(err) || 'Unhandled server error.',
    meta: { path: req.originalUrl || req.url || '' },
  });
  res.status(500).json({ error: 'Server error' });
});

if (process.argv[1] === __filename) {
  app.listen(PORT, () => {
    console.log(`Launcharr listening on port ${PORT}`);
  });
}

export { app };

function parseCsv(value) {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function readJsonFileSafe(filePath, fallback = null) {
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw);
  } catch (_err) {
    return fallback;
  }
}

function listLocalModuleDescriptors(modulesDir = LOCAL_MODULES_DIR) {
  try {
    return fs.readdirSync(modulesDir, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => {
        const moduleDir = path.join(modulesDir, entry.name);
        const manifestPath = path.join(moduleDir, 'module.json');
        const manifest = readJsonFileSafe(manifestPath, null);
        if (!manifest || typeof manifest !== 'object' || Array.isArray(manifest)) return null;
        const id = String(manifest.id || entry.name).trim().toLowerCase();
        const serverEntry = String(manifest.serverEntry || '').trim();
        if (!id || !serverEntry || manifest.enabled === false) return null;
        const serverPath = path.join(moduleDir, serverEntry);
        if (!fs.existsSync(serverPath)) return null;
        return {
          id,
          moduleDir,
          manifestPath,
          serverPath,
          manifest: {
            ...manifest,
            id,
          },
        };
      })
      .filter(Boolean);
  } catch (_err) {
    return [];
  }
}

function syncLocalModuleApps(moduleDescriptor, ctx) {
  if (!ctx || typeof ctx.loadConfig !== 'function' || typeof ctx.saveConfig !== 'function') return;
  const moduleApps = Array.isArray(moduleDescriptor?.manifest?.apps) ? moduleDescriptor.manifest.apps : [];
  if (!moduleApps.length) return;

  const config = ctx.loadConfig();
  if (!config || !Array.isArray(config.apps)) return;

  const existingIds = new Set(
    config.apps
      .map((appItem) => normalizeAppId(appItem?.id))
      .filter(Boolean)
  );
  const additions = moduleApps
    .filter((appItem) => appItem && typeof appItem === 'object' && !Array.isArray(appItem))
    .filter((appItem) => {
      const appId = normalizeAppId(appItem.id);
      return appId && !existingIds.has(appId);
    })
    .map((appItem) => JSON.parse(JSON.stringify(appItem)));

  if (!additions.length) return;

  ctx.saveConfig({
    ...config,
    apps: [...config.apps, ...additions],
  });
}

async function registerLocalModules(app, ctx) {
  const modules = listLocalModuleDescriptors();
  for (const moduleDescriptor of modules) {
    try {
      syncLocalModuleApps(moduleDescriptor, ctx);
      const imported = await import(pathToFileURL(moduleDescriptor.serverPath).href);
      const moduleContext = {
        ...moduleDescriptor.manifest,
        moduleDir: moduleDescriptor.moduleDir,
        manifestPath: moduleDescriptor.manifestPath,
      };
      if (typeof imported.register === 'function') {
        await imported.register(app, ctx, moduleContext);
      }
      if (typeof imported.start === 'function') {
        await imported.start(ctx, moduleContext);
      }
      if (typeof ctx.pushLog === 'function') {
        ctx.pushLog({
          level: 'info',
          app: 'system',
          action: 'local-module.loaded',
          message: `Loaded local module "${moduleDescriptor.id}".`,
        });
      }
    } catch (err) {
      if (typeof ctx.pushLog === 'function') {
        ctx.pushLog({
          level: 'error',
          app: 'system',
          action: 'local-module.load-failed',
          message: `Failed to load local module "${moduleDescriptor.id}".`,
          meta: {
            error: safeMessage(err) || 'Unknown error',
          },
        });
      }
    }
  }
}

function parseEnvFlag(value, fallback = false) {
  const raw = String(value ?? '').trim().toLowerCase();
  if (!raw) return Boolean(fallback);
  return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on';
}

function resolveProxyHopCount(value, fallback = 1) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return Math.max(1, Math.round(fallback || 1));
  return Math.max(1, Math.min(10, Math.round(parsed)));
}

function buildCurrentRequestOrigin(req) {
  const protocol = String(req.protocol || 'http').trim().toLowerCase() || 'http';
  const host = String(req.get('host') || '').trim().toLowerCase();
  if (!host) return '';
  return `${protocol}://${host}`;
}

function isSameOriginValue(value, req) {
  const raw = String(value || '').trim();
  if (!raw) return false;
  try {
    const parsed = new URL(raw);
    const expectedOrigin = buildCurrentRequestOrigin(req);
    if (!expectedOrigin) return false;
    const expected = new URL(expectedOrigin);
    return parsed.protocol.toLowerCase() === expected.protocol.toLowerCase()
      && parsed.host.toLowerCase() === expected.host.toLowerCase();
  } catch (_err) {
    return false;
  }
}

function safeTokenEquals(expectedValue, providedValue) {
  const expected = String(expectedValue || '');
  const provided = String(providedValue || '');
  if (!expected || !provided) return false;
  const expectedBuf = Buffer.from(expected);
  const providedBuf = Buffer.from(provided);
  if (expectedBuf.length !== providedBuf.length) return false;
  return crypto.timingSafeEqual(expectedBuf, providedBuf);
}

function isUnsafeHttpMethod(method) {
  const normalized = String(method || '').trim().toUpperCase();
  return normalized === 'POST' || normalized === 'PUT' || normalized === 'PATCH' || normalized === 'DELETE';
}

function rejectCsrfRequest(req, res) {
  const payload = { error: 'CSRF validation failed.' };
  const acceptsJson = String(req.get('accept') || '').toLowerCase().includes('application/json')
    || String(req.get('content-type') || '').toLowerCase().includes('application/json')
    || String(req.path || '').startsWith('/api/');
  if (acceptsJson) return res.status(403).json(payload);
  return res.status(403).send(payload.error);
}

function csrfProtectionMiddleware(req, res, next) {
  if (!isUnsafeHttpMethod(req.method)) return next();

  const sessionToken = String(req.session?.csrfToken || '').trim();
  const suppliedToken = String(
    req.get('x-csrf-token')
      || req.body?._csrf
      || req.query?._csrf
      || ''
  ).trim();
  if (sessionToken && safeTokenEquals(sessionToken, suppliedToken)) return next();

  const origin = String(req.get('origin') || '').trim();
  if (origin) {
    if (isSameOriginValue(origin, req)) return next();
    return rejectCsrfRequest(req, res);
  }

  const referer = String(req.get('referer') || '').trim();
  if (referer) {
    if (isSameOriginValue(referer, req)) return next();
    return rejectCsrfRequest(req, res);
  }

  // Requests without Origin/Referer (CLI tools, legacy clients) are allowed.
  return next();
}

function createRequestBodySizeGuard(maxBytes) {
  const limitBytes = Number(maxBytes);
  const effectiveLimit = Number.isFinite(limitBytes) ? Math.max(1, Math.round(limitBytes)) : 0;
  if (!effectiveLimit) return (_req, _res, next) => next();
  return (req, res, next) => {
    if (!isUnsafeHttpMethod(req.method)) return next();
    const contentLength = Number(req.get('content-length'));
    if (Number.isFinite(contentLength) && contentLength > effectiveLimit) {
      return res.status(413).send('Payload too large.');
    }
    if (req.body && typeof req.body === 'object') {
      try {
        const estimatedBytes = Buffer.byteLength(JSON.stringify(req.body), 'utf8');
        if (estimatedBytes > effectiveLimit) {
          return res.status(413).send('Payload too large.');
        }
      } catch (_err) {
        // ignore serialization errors
      }
    }
    return next();
  };
}

function validateLocalPasswordStrength(passwordValue) {
  const password = String(passwordValue || '');
  if (!password || password.length < LOCAL_AUTH_MIN_PASSWORD) {
    return `Password must be at least ${LOCAL_AUTH_MIN_PASSWORD} characters.`;
  }
  if (!/[a-z]/.test(password)) return 'Password must include at least one lowercase letter.';
  if (!/[A-Z]/.test(password)) return 'Password must include at least one uppercase letter.';
  if (!/[0-9]/.test(password)) return 'Password must include at least one number.';
  if (!/[^A-Za-z0-9]/.test(password)) return 'Password must include at least one symbol.';
  return '';
}

function setupConsoleLogRedaction() {
  if (!LOG_REDACT_HOSTS && !LOG_REDACT_IPS) return;
  if (globalThis.__launcharrConsoleRedactionInstalled) return;
  globalThis.__launcharrConsoleRedactionInstalled = true;

  const wrap = (fnName) => {
    const original = console[fnName];
    if (typeof original !== 'function') return;
    const bound = original.bind(console);
    console[fnName] = (...args) => bound(...redactConsoleArgs(args));
  };

  wrap('log');
  wrap('info');
  wrap('warn');
  wrap('error');

  console.log(`[logs] console redaction enabled hosts=${LOG_REDACT_HOSTS ? 'on' : 'off'} ips=${LOG_REDACT_IPS ? 'on' : 'off'}`);
}

function redactConsoleArgs(args) {
  if ((!LOG_REDACT_HOSTS && !LOG_REDACT_IPS) || !Array.isArray(args) || !args.length) return args;
  return args.map((arg) => redactConsoleArgValue(arg));
}

function redactConsoleArgValue(value, depth = 0) {
  if (value == null) return value;
  if (typeof value === 'string') return redactLogText(value);
  if (depth >= 2) return value;
  if (Array.isArray(value)) return value.map((item) => redactConsoleArgValue(item, depth + 1));
  if (value instanceof URL) return redactLogHostOrUrl(value.toString());
  if (typeof value === 'object') {
    const out = {};
    for (const [key, entry] of Object.entries(value)) out[key] = redactConsoleArgValue(entry, depth + 1);
    return out;
  }
  return value;
}

function redactLogText(inputValue) {
  let text = String(inputValue ?? '');
  if (!text) return text;

  if (LOG_REDACT_HOSTS) {
    text = text.replace(/\bhttps?:\/\/[^\s"'<>]+/gi, (match) => redactLogHostOrUrl(match));
    text = text.replace(/\bhost=([^\s]+)/gi, (_m, hostValue) => `host=${redactLogHostOrUrl(hostValue)}`);
  }
  if (LOG_REDACT_IPS) {
    text = text.replace(/\bip=([^\s]+)/gi, (_m, ipValue) => `ip=${redactLogIp(ipValue)}`);
    text = text.replace(/\bx-forwarded-for=([^\s]+)/gi, (_m, ipValue) => `x-forwarded-for=${redactLogIp(ipValue)}`);
  }
  return text;
}

function setupConsoleStderrMirrorToStdout() {
  if (!LOG_MIRROR_STDERR_TO_STDOUT) return;
  if (globalThis.__launcharrStderrStdoutMirrorInstalled) return;
  globalThis.__launcharrStderrStdoutMirrorInstalled = true;

  const originalWarn = console.warn.bind(console);
  const originalError = console.error.bind(console);

  const mirror = (level, args) => {
    try {
      const line = formatConsoleArgs(...(Array.isArray(args) ? args : []));
      process.stdout.write(`[mirror:${level}] ${line}${line.endsWith('\n') ? '' : '\n'}`);
    } catch (_err) {
      // Avoid recursive logging if formatting/write fails.
    }
  };

  console.warn = (...args) => {
    originalWarn(...args);
    mirror('warn', args);
  };
  console.error = (...args) => {
    originalError(...args);
    mirror('error', args);
  };

  console.log('[logs] LOG_MIRROR_STDERR_TO_STDOUT enabled');
}

function httpAccessLogMiddleware(req, res, next) {
  if (!HTTP_ACCESS_LOGS) return next();
  if (HTTP_ACCESS_LOGS_SKIP_STATIC && shouldSkipHttpAccessLog(req)) return next();
  const startedAt = process.hrtime.bigint();
  let logged = false;
  const logAccess = (eventName) => {
    if (logged) return;
    logged = true;
    const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
    const user = req.session?.user || null;
    const role = getEffectiveRole(req) || '';
    const forwardedFor = String(req.headers?.['x-forwarded-for'] || '').split(',')[0].trim();
    const ip = forwardedFor || req.ip || req.socket?.remoteAddress || '';
    const host = getRequestHost(req);
    const pathOnly = String(req.originalUrl || req.url || '').split('#')[0];
    const contentLength = res.getHeader('content-length');
    const bytes = contentLength === undefined ? '' : String(contentLength);
    console.log(`[http-access] ${req.method} ${pathOnly} status=${res.statusCode} durMs=${elapsedMs.toFixed(1)} host=${redactLogHostOrUrl(host) || '-'} ip=${redactLogIp(ip) || '-'} role=${role || '-'} user=${String(user?.username || user?.email || '').trim() || '-'} bytes=${bytes || '-'} event=${eventName}`);
  };
  res.on('finish', () => logAccess('finish'));
  res.on('close', () => logAccess('close'));
  next();
}

function shouldSkipHttpAccessLog(req) {
  const method = String(req?.method || '').trim().toUpperCase();
  if (method !== 'GET' && method !== 'HEAD') return false;
  const rawPath = String(req?.originalUrl || req?.url || '').trim();
  const pathOnly = rawPath.split('?')[0].split('#')[0];
  if (!pathOnly) return false;

  if (pathOnly.startsWith('/icons/')
    || pathOnly.startsWith('/fonts/')
    || pathOnly.startsWith('/images/')
    || pathOnly.startsWith('/public/')) {
    return true;
  }

  return /\.(?:css|js|mjs|map|png|jpe?g|gif|webp|svg|ico|woff2?|ttf|eot|webmanifest)$/i.test(pathOnly);
}

function redactLogIp(value) {
  const input = String(value ?? '').trim();
  if (!LOG_REDACT_IPS || !input) return input;
  const normalized = input.replace(/^\[|\]$/g, '');
  if (!normalized) return input;
  const ipv4Match = normalized.match(/^(?:::ffff:)?(\d{1,3}(?:\.\d{1,3}){3})$/i);
  if (ipv4Match) {
    const ip = ipv4Match[1];
    if (!isLikelyIpv4Address(ip)) return input;
    const alias = getRedactedIpAlias(ip);
    return normalized.toLowerCase().startsWith('::ffff:') ? `::ffff:${alias}` : alias;
  }
  if (isLikelyIpv6Address(normalized)) return getRedactedIpAlias(normalized.toLowerCase());
  return input;
}

function isLikelyIpv4Address(value) {
  const parts = String(value || '').split('.');
  if (parts.length !== 4) return false;
  return parts.every((part) => /^\d+$/.test(part) && Number(part) >= 0 && Number(part) <= 255);
}

function isLikelyIpv6Address(value) {
  const raw = String(value || '').trim();
  if (!raw || !raw.includes(':')) return false;
  return /^[0-9a-f:]+$/i.test(raw);
}

function getRedactedIpAlias(ipValue) {
  const raw = String(ipValue ?? '').trim();
  if (!raw) return raw;
  const key = raw.toLowerCase();
  if (LOG_IP_ALIAS_CACHE.has(key)) return LOG_IP_ALIAS_CACHE.get(key);
  const alias = `ip-${crypto.createHash('sha256').update(key).digest('hex').slice(0, 8)}`;
  LOG_IP_ALIAS_CACHE.set(key, alias);
  return alias;
}

function uniqueList(items) {
  const seen = new Set();
  const out = [];
  items.forEach((item) => {
    const value = String(item || '').trim();
    const key = value.toLowerCase();
    if (!value || seen.has(key)) return;
    seen.add(key);
    out.push(value);
  });
  return out;
}

function isSecureEnv() {
  return (process.env.COOKIE_SECURE || '').toLowerCase() === 'true';
}

function loadPackageVersion() {
  try {
    const pkgPath = path.join(__dirname, '..', 'package.json');
    const raw = fs.readFileSync(pkgPath, 'utf8');
    const parsed = JSON.parse(raw);
    return String(parsed?.version || '').trim();
  } catch (err) {
    return '';
  }
}

function normalizeVersionTag(value) {
  const tag = String(value || '').trim();
  if (!tag) return '';
  return tag.startsWith('v') ? tag : `v${tag}`;
}

function buildReleaseNotesUrl(versionTag) {
  const normalized = normalizeVersionTag(versionTag);
  if (!normalized) return '';
  return `${RELEASE_NOTES_BASE_URL}${encodeURIComponent(normalized)}`;
}

function stripMarkdownInline(value) {
  let text = String(value || '').trim();
  if (!text) return '';
  text = text.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1');
  text = text.replace(/`([^`]+)`/g, '$1');
  text = text.replace(/\*\*([^*]+)\*\*/g, '$1');
  text = text.replace(/\*([^*]+)\*/g, '$1');
  text = text.replace(/~~([^~]+)~~/g, '$1');
  return text.replace(/\s+/g, ' ').trim();
}

function loadReleaseHighlights(versionTag, options = {}) {
  const normalized = normalizeVersionTag(versionTag);
  if (!normalized) return [];
  const maxItemsRaw = Number(options.maxItems);
  const maxItems = Number.isFinite(maxItemsRaw) ? Math.max(1, Math.min(20, maxItemsRaw)) : RELEASE_HIGHLIGHT_LIMIT;
  const sectionSet = new Set(RELEASE_HIGHLIGHT_SECTIONS.map((section) => section.toLowerCase()));
  const releaseFilePath = path.join(RELEASE_NOTES_DIR, `${normalized}.md`);
  if (!fs.existsSync(releaseFilePath)) return [];
  try {
    const raw = fs.readFileSync(releaseFilePath, 'utf8');
    const lines = raw.split(/\r?\n/);
    let activeSection = '';
    const highlights = [];

    for (let index = 0; index < lines.length; index += 1) {
      const line = String(lines[index] || '');
      const sectionMatch = line.match(/^###\s+(.+?)\s*$/);
      if (sectionMatch) {
        const heading = String(sectionMatch[1] || '').trim();
        const key = heading.toLowerCase();
        activeSection = sectionSet.has(key) ? heading : '';
        continue;
      }
      if (/^##\s+/.test(line)) {
        activeSection = '';
        continue;
      }
      if (!activeSection) continue;
      const bulletMatch = line.match(/^\s*-\s+(.+?)\s*$/);
      if (!bulletMatch) continue;
      const bulletText = stripMarkdownInline(bulletMatch[1]);
      if (!bulletText) continue;
      highlights.push(`${activeSection}: ${bulletText}`);
      if (highlights.length >= maxItems) break;
    }
    return highlights;
  } catch (err) {
    return [];
  }
}

function parseSemver(value) {
  const raw = String(value || '').trim().replace(/^v/i, '');
  const match = raw.match(/^(\d+)\.(\d+)\.(\d+)$/);
  if (!match) return null;
  return {
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3]),
  };
}

function compareSemver(a, b) {
  if (a.major !== b.major) return a.major - b.major;
  if (a.minor !== b.minor) return a.minor - b.minor;
  return a.patch - b.patch;
}

function loadSettingsReleases(options = {}) {
  const limitRaw = Number(options.limit);
  const limit = Number.isFinite(limitRaw)
    ? Math.max(1, Math.min(30, Math.floor(limitRaw)))
    : 12;
  const currentVersion = normalizeVersionTag(options.currentVersion || '');
  let filenames = [];
  try {
    filenames = fs.readdirSync(RELEASE_NOTES_DIR);
  } catch (err) {
    return [];
  }
  const releases = filenames
    .filter((filename) => /^v\d+\.\d+\.\d+\.md$/i.test(String(filename || '').trim()))
    .map((filename) => {
      const tag = normalizeVersionTag(String(filename || '').replace(/\.md$/i, ''));
      const semver = parseSemver(tag);
      if (!semver) return null;
      const fullPath = path.join(RELEASE_NOTES_DIR, filename);
      let publishedAt = '';
      try {
        const stats = fs.statSync(fullPath);
        publishedAt = stats?.mtime ? stats.mtime.toISOString() : '';
      } catch (err) {
        publishedAt = '';
      }
      const highlights = loadReleaseHighlights(tag, { maxItems: 3 });
      const primaryHighlight = String(highlights[0] || '').trim();
      const summary = primaryHighlight
        ? primaryHighlight.replace(/^[A-Za-z][A-Za-z\s]+:\s*/, '').trim()
        : 'Release notes are available.';
      return {
        tag,
        semver,
        publishedAt,
        releaseNotesUrl: buildReleaseNotesUrl(tag),
        summary,
        highlights,
      };
    })
    .filter(Boolean)
    .sort((left, right) => compareSemver(right.semver, left.semver))
    .slice(0, limit)
    .map((entry, index) => ({
      tag: entry.tag,
      publishedAt: entry.publishedAt,
      releaseNotesUrl: entry.releaseNotesUrl,
      summary: entry.summary,
      highlights: entry.highlights,
      isLatest: index === 0,
      isCurrent: Boolean(currentVersion && entry.tag === currentVersion),
    }));
  return releases;
}

function getNavApps(apps, role, req, categoryOrder = DEFAULT_CATEGORY_ORDER, generalSettings = resolveGeneralSettings(loadConfig())) {
  const rankCategory = buildCategoryRank(categoryOrder);
  const isFavourite = (appItem) => Boolean(appItem?.favourite || appItem?.favorite);
  const orderValue = (value) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : Number.MAX_SAFE_INTEGER;
  };

  return apps
    .map((appItem) => {
      if (appItem?.removed) return null;
      const baseId = getAppBaseId(appItem?.id);
      const supportsInstances = supportsAppInstances(baseId);
      const instanceName = supportsInstances ? String(appItem?.instanceName || '').trim() : '';
      const resolvedName = supportsInstances
        ? (instanceName || String(appItem?.name || '').trim() || getDefaultInstanceName(baseId, appItem?.id))
        : (String(appItem?.name || '').trim() || getBaseAppTitle(baseId));
      const resolvedIcon = resolvePersistedAppIconPath(appItem);
      const access = getMenuAccess(appItem, role);
      const sidebarOverviewAccess = canAccessSidebarOverview(appItem, role);
      const sidebarSettingsAccess = canAccessSidebarSettings(appItem, role);
      const sidebarActivityAccess = canAccessSidebarActivity(appItem, role);
      const menuAccess = {
        ...access,
        overview: sidebarOverviewAccess && access.overview,
        settings: sidebarSettingsAccess && access.settings && !generalSettings.hideSidebarAppSettingsLink,
        activity: sidebarActivityAccess
          && sidebarOverviewAccess
          && access.overview
          && role === 'admin'
          && !generalSettings.hideSidebarActivityLink,
      };
      return {
        ...appItem,
        name: resolvedName,
        icon: resolvedIcon,
        launchMode: resolveAppLaunchMode(appItem, normalizeMenu(appItem)),
        effectiveLaunchMode: resolveEffectiveLaunchMode(appItem, req),
        menuAccess,
      };
    })
    .filter((appItem) => appItem && appItem.menuAccess.sidebar && hasAnyMenuAccess(appItem.menuAccess))
    .sort((a, b) => {
      const favouriteDelta = (isFavourite(b) ? 1 : 0) - (isFavourite(a) ? 1 : 0);
      if (favouriteDelta !== 0) return favouriteDelta;
      const categoryDelta = rankCategory(a.category) - rankCategory(b.category);
      if (categoryDelta !== 0) return categoryDelta;
      const orderDelta = orderValue(a.order) - orderValue(b.order);
      if (orderDelta !== 0) return orderDelta;
      return String(a.name || '').localeCompare(String(b.name || ''));
    });
}

function buildNavCategories(navApps, categoryEntries, role = 'user') {
  const defaultIcon = DEFAULT_CATEGORY_ICON;
  const entries = Array.isArray(categoryEntries) ? categoryEntries : [];
  const isFavourite = (appItem) => Boolean(appItem?.favourite || appItem?.favorite);
  const grouped = new Map();
  entries.forEach((entry) => {
    grouped.set(entry.name, []);
  });
  (Array.isArray(navApps) ? navApps : []).forEach((appItem) => {
    const rawCategory = String(appItem?.category || '').trim();
    const category = rawCategory || 'Apps';
    const key = category.toLowerCase();
    const existingName = Array.from(grouped.keys()).find((name) => name.toLowerCase() === key);
    const target = existingName || category;
    if (!grouped.has(target)) grouped.set(target, []);
    grouped.get(target).push(appItem);
  });

  const result = [];
  const seen = new Set();
  entries.forEach((entry) => {
    const apps = grouped.get(entry.name) || [];
    seen.add(entry.name.toLowerCase());
    if (!apps.length) return;
    const visibilityRoles = Array.isArray(entry?.visibilityRoles)
      ? entry.visibilityRoles
      : categoryVisibilityRolesFromLegacyMinRole(entry?.sidebarMinRole || (entry?.sidebarMenu ? 'user' : 'disabled'));
    const shouldGroup = visibilityRoles.includes(role);
    const filteredApps = shouldGroup ? apps : apps.filter((appItem) => !isFavourite(appItem));
    if (!filteredApps.length) return;
    result.push({
      name: entry.name,
      sidebarMenu: shouldGroup,
      visibilityRoles,
      sidebarMinRole: visibilityRoles.length > 0 ? (visibilityRoles[0] || 'user') : 'disabled',
      icon: entry.icon || defaultIcon,
      apps: filteredApps,
    });
  });
  grouped.forEach((apps, name) => {
    if (!apps.length) return;
    if (seen.has(name.toLowerCase())) return;
    result.push({ name, sidebarMenu: false, sidebarMinRole: 'disabled', icon: defaultIcon, apps });
  });
  const favourites = (Array.isArray(navApps) ? navApps : [])
    .filter(isFavourite)
    .map((appItem) => ({ ...appItem, navFavourite: true }));
  if (favourites.length) {
    result.unshift({
      name: 'Favourites',
      sidebarMenu: false,
      sidebarMinRole: 'disabled',
      icon: '/icons/favourite.svg',
      apps: favourites,
    });
  }
  return result;
}

function hasAnyMenuAccess(access) {
  if (!access) return false;
  return Boolean(access.overview || access.launch || access.settings || access.activity);
}

function parseVisibilityRole(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';
  if (raw === 'coadmin' || raw === 'co_admin') return 'co-admin';
  return VISIBILITY_ROLE_ORDER.includes(raw) ? raw : '';
}

function normalizeVisibilityRole(value, fallback = 'disabled') {
  const parsed = parseVisibilityRole(value);
  if (parsed) return parsed;
  const fallbackParsed = parseVisibilityRole(fallback);
  return fallbackParsed || 'disabled';
}

function roleMeetsMinRole(role, minRole) {
  const roleKey = parseVisibilityRole(role);
  const minRoleKey = normalizeVisibilityRole(minRole, 'disabled');
  if (!roleKey || minRoleKey === 'disabled') return false;
  const roleRank = VISIBILITY_ROLE_RANK[roleKey];
  const minRoleRank = VISIBILITY_ROLE_RANK[minRoleKey];
  if (!Number.isFinite(roleRank) || !Number.isFinite(minRoleRank) || minRoleRank < 0) return false;
  return roleRank >= minRoleRank;
}

const MENU_VISIBILITY_SELECTABLE_ROLES = VISIBILITY_ROLE_ORDER.filter((role) => role !== 'disabled');

function visibilityRolesFromLegacyMinRole(minRole = 'disabled') {
  const normalized = normalizeVisibilityRole(minRole, 'disabled');
  if (normalized === 'disabled') return [];
  const minRank = Number(VISIBILITY_ROLE_RANK[normalized]);
  return MENU_VISIBILITY_SELECTABLE_ROLES.filter((role) => {
    const rank = Number(VISIBILITY_ROLE_RANK[role]);
    return Number.isFinite(rank) && Number.isFinite(minRank) && rank >= minRank;
  });
}

function normalizeMenuVisibilityRoles(value, fallback = undefined) {
  const hasExplicitArray = Array.isArray(value);
  const inputList = hasExplicitArray
    ? value
    : (typeof value === 'string' && value.includes(','))
      ? value.split(',')
      : (value === undefined ? [] : [value]);
  const parsed = uniqueList(
    inputList
      .map((item) => parseVisibilityRole(item))
      .filter((role) => role && role !== 'disabled' && MENU_VISIBILITY_SELECTABLE_ROLES.includes(role))
  );
  if (parsed.length) {
    return MENU_VISIBILITY_SELECTABLE_ROLES.filter((role) => parsed.includes(role));
  }
  if (hasExplicitArray) return [];
  if (fallback === undefined) return [];
  if (Array.isArray(fallback)) {
    return normalizeMenuVisibilityRoles(fallback);
  }
  return visibilityRolesFromLegacyMinRole(fallback);
}

function normalizeMenuRoleSection(rawSection, fallbackRole = 'disabled') {
  const source = rawSection && typeof rawSection === 'object' ? rawSection : {};
  const fallbackMinRole = normalizeVisibilityRole(source.minRole, fallbackRole);
  const hasExplicitVisibilityRoles = Object.prototype.hasOwnProperty.call(source, 'visibilityRoles');
  const visibilityRoles = hasExplicitVisibilityRoles
    ? normalizeMenuVisibilityRoles(source.visibilityRoles)
    : visibilityRolesFromLegacyMinRole(fallbackMinRole);
  const minRole = visibilityRoles.length
    ? normalizeVisibilityRole(visibilityRoles[0], fallbackRole)
    : (hasExplicitVisibilityRoles ? 'disabled' : fallbackMinRole);
  return {
    ...source,
    minRole,
    visibilityRoles,
  };
}

function normalizeMenuSectionFromInput(input, fallbackRole = 'disabled') {
  if (input && typeof input === 'object' && !Array.isArray(input)) {
    return normalizeMenuRoleSection(input, fallbackRole);
  }
  if (Array.isArray(input)) {
    const visibilityRoles = normalizeMenuVisibilityRoles(input);
    return {
      minRole: visibilityRoles.length ? visibilityRoles[0] : 'disabled',
      visibilityRoles,
    };
  }
  const minRole = normalizeVisibilityRole(input, fallbackRole);
  return {
    minRole,
    visibilityRoles: visibilityRolesFromLegacyMinRole(minRole),
  };
}

function canRoleAccessMenuSection(section, role, fallbackRole = 'disabled') {
  const roleKey = parseVisibilityRole(role);
  if (!roleKey) return false;
  const source = section && typeof section === 'object' ? section : {};
  const visibilityRoles = normalizeMenuVisibilityRoles(source.visibilityRoles, source.minRole || fallbackRole);
  return visibilityRoles.includes(roleKey);
}

function deriveSectionMinRoleFromLegacy(sectionName, section) {
  const userAllowed = Boolean(section?.user);
  const adminAllowed = Boolean(section?.admin);
  if (userAllowed) return 'user';
  if (adminAllowed) return sectionName === 'settings' ? 'admin' : 'co-admin';
  return 'disabled';
}

function normalizeMenuSection(sectionName, rawSection, fallbackRole = 'disabled') {
  const hasSectionObject = Boolean(rawSection && typeof rawSection === 'object');
  const source = hasSectionObject ? rawSection : {};
  const explicitMinRole = parseVisibilityRole(source.minRole);
  const legacyMinRole = hasSectionObject ? deriveSectionMinRoleFromLegacy(sectionName, source) : '';
  const resolvedMinRole = explicitMinRole || legacyMinRole || normalizeVisibilityRole(fallbackRole, 'disabled');
  const normalizedSection = normalizeMenuRoleSection(source, resolvedMinRole);
  const visibilityRoles = normalizeMenuVisibilityRoles(
    normalizedSection.visibilityRoles,
    normalizedSection.minRole
  );
  return {
    ...normalizedSection,
    visibilityRoles,
    minRole: visibilityRoles.length ? visibilityRoles[0] : 'disabled',
    user: visibilityRoles.includes('user'),
    admin: visibilityRoles.includes('admin'),
  };
}

function deriveSidebarMinRole(rawSidebar, sectionRoles) {
  const source = rawSidebar && typeof rawSidebar === 'object' ? rawSidebar : {};
  const explicit = parseVisibilityRole(source.minRole);
  if (explicit) return explicit;
  const enabledRoles = (Array.isArray(sectionRoles) ? sectionRoles : [])
    .map((value) => normalizeVisibilityRole(value, 'disabled'))
    .filter((value) => value !== 'disabled');
  if (!enabledRoles.length) return 'disabled';
  return enabledRoles.reduce((lowest, nextRole) => {
    const lowestRank = VISIBILITY_ROLE_RANK[normalizeVisibilityRole(lowest, 'disabled')];
    const nextRank = VISIBILITY_ROLE_RANK[normalizeVisibilityRole(nextRole, 'disabled')];
    return nextRank < lowestRank ? nextRole : lowest;
  }, enabledRoles[0]);
}

function buildMenuAccessConfig({
  sidebar = 'disabled',
  sidebarOverview = 'disabled',
  sidebarSettings = '',
  sidebarActivity = '',
  overview = 'disabled',
  launch = 'disabled',
  settings = 'disabled',
} = {}) {
  const overviewSection = normalizeMenuSectionFromInput(overview, 'disabled');
  const launchSection = normalizeMenuSectionFromInput(launch, 'disabled');
  const settingsSection = normalizeMenuSectionFromInput(settings, 'disabled');
  const sidebarOverviewSection = normalizeMenuSectionFromInput(sidebarOverview, overviewSection.minRole);
  const sidebarSettingsSection = normalizeMenuSectionFromInput(sidebarSettings || settingsSection, settingsSection.minRole);
  const sidebarActivitySection = normalizeMenuSectionFromInput(sidebarActivity || 'admin', 'admin');
  const sidebarSection = normalizeMenuSectionFromInput(
    sidebar,
    deriveSidebarMinRole({}, [overviewSection.minRole, launchSection.minRole, settingsSection.minRole])
  );
  return {
    sidebar: sidebarSection,
    sidebarOverview: sidebarOverviewSection,
    sidebarSettings: sidebarSettingsSection,
    sidebarActivity: sidebarActivitySection,
    overview: {
      ...overviewSection,
      user: overviewSection.visibilityRoles.includes('user'),
      admin: overviewSection.visibilityRoles.includes('admin'),
    },
    launch: {
      ...launchSection,
      user: launchSection.visibilityRoles.includes('user'),
      admin: launchSection.visibilityRoles.includes('admin'),
    },
    settings: {
      ...settingsSection,
      user: settingsSection.visibilityRoles.includes('user'),
      admin: settingsSection.visibilityRoles.includes('admin'),
    },
  };
}

function canAccess(appItem, role, key) {
  const access = getMenuAccess(appItem, role);
  return Boolean(access && access[key]);
}

function canAccessSidebarOverview(appItem, role) {
  if (appItem?.removed) return false;
  const roleKey = parseVisibilityRole(role);
  if (!roleKey) return false;
  const menu = normalizeMenu(appItem);
  return canRoleAccessMenuSection(menu.sidebarOverview, roleKey, 'disabled');
}

function canAccessSidebarSettings(appItem, role) {
  if (appItem?.removed) return false;
  const roleKey = parseVisibilityRole(role);
  if (!roleKey) return false;
  const menu = normalizeMenu(appItem);
  return canRoleAccessMenuSection(menu.sidebarSettings, roleKey, 'disabled');
}

function canAccessSidebarActivity(appItem, role) {
  if (appItem?.removed) return false;
  const roleKey = parseVisibilityRole(role);
  if (!roleKey) return false;
  const menu = normalizeMenu(appItem);
  return canRoleAccessMenuSection(menu.sidebarActivity, roleKey, 'disabled');
}

function getMenuAccess(appItem, role) {
  if (appItem?.removed) {
    return {
      sidebar: false,
      overview: false,
      launch: false,
      settings: false,
    };
  }
  const menu = normalizeMenu(appItem);
  const launchMode = resolveAppLaunchMode(appItem, menu);
  const launchEnabled = launchMode !== 'disabled';
  const roleKey = parseVisibilityRole(role);
  if (!roleKey) {
    return {
      sidebar: false,
      overview: false,
      launch: false,
      settings: false,
    };
  }
  return {
    sidebar: canRoleAccessMenuSection(menu.sidebar, roleKey, 'disabled'),
    overview: canRoleAccessMenuSection(menu.overview, roleKey, 'disabled'),
    launch: canRoleAccessMenuSection(menu.launch, roleKey, 'disabled') && launchEnabled,
    settings: canRoleAccessMenuSection(menu.settings, roleKey, 'disabled'),
  };
}

function normalizeMenu(appItem) {
  if (appItem && appItem.menu) {
    const source = appItem.menu && typeof appItem.menu === 'object' ? appItem.menu : {};
    const overview = normalizeMenuSection('overview', source.overview, 'disabled');
    const launch = normalizeMenuSection('launch', source.launch, 'disabled');
    const settings = normalizeMenuSection('settings', source.settings, 'admin');
    const sidebarSource = source.sidebar && typeof source.sidebar === 'object' ? source.sidebar : {};
    const sidebarOverviewSource = source.sidebarOverview && typeof source.sidebarOverview === 'object'
      ? source.sidebarOverview
      : {};
    const sidebarSettingsSource = source.sidebarSettings && typeof source.sidebarSettings === 'object'
      ? source.sidebarSettings
      : {};
    const sidebarActivitySource = source.sidebarActivity && typeof source.sidebarActivity === 'object'
      ? source.sidebarActivity
      : {};
    let sidebar = normalizeMenuRoleSection(
      sidebarSource,
      deriveSidebarMinRole(sidebarSource, [overview.minRole, launch.minRole, settings.minRole])
    );
    let sidebarOverview = normalizeMenuRoleSection(sidebarOverviewSource, overview.minRole);
    let sidebarSettings = normalizeMenuRoleSection(sidebarSettingsSource, settings.minRole);
    let sidebarActivity = normalizeMenuRoleSection(sidebarActivitySource, 'admin');
    const enforceAdminSidebarSection = (section) => {
      const sourceSection = section && typeof section === 'object' ? section : {};
      const normalizedRoles = normalizeMenuVisibilityRoles(
        sourceSection.visibilityRoles,
        sourceSection.minRole || 'admin'
      );
      const visibilityRoles = normalizedRoles.length ? normalizedRoles : ['admin'];
      return {
        ...sourceSection,
        minRole: visibilityRoles[0] || 'admin',
        visibilityRoles,
      };
    };
    sidebar = enforceAdminSidebarSection(sidebar);
    sidebarOverview = enforceAdminSidebarSection(sidebarOverview);
    sidebarSettings = enforceAdminSidebarSection(sidebarSettings);
    sidebarActivity = enforceAdminSidebarSection(sidebarActivity);
    if (!Boolean(appItem?.custom)) {
      const mergedOverviewRoles = MENU_VISIBILITY_SELECTABLE_ROLES.filter((role) =>
        overview.visibilityRoles.includes(role) || sidebarOverview.visibilityRoles.includes(role)
      );
      if (mergedOverviewRoles.length !== overview.visibilityRoles.length
        || mergedOverviewRoles.some((role) => !overview.visibilityRoles.includes(role))) {
        overview.visibilityRoles = mergedOverviewRoles;
        overview.minRole = mergedOverviewRoles.length ? mergedOverviewRoles[0] : 'disabled';
        overview.user = mergedOverviewRoles.includes('user');
        overview.admin = mergedOverviewRoles.includes('admin');
      }
      // App settings route is admin-only; keep section access aligned for built-in apps.
      if (settings.minRole !== 'admin' || settings.visibilityRoles.length !== 1 || settings.visibilityRoles[0] !== 'admin') {
        settings.minRole = 'admin';
        settings.visibilityRoles = ['admin'];
        settings.user = false;
        settings.admin = true;
      }
    }
    return { overview, launch, settings, sidebar, sidebarOverview, sidebarSettings, sidebarActivity };
  }
  const roles = normalizeRoles(appItem);
  const allowUser = !roles.length || roles.includes('user') || roles.includes('both');
  const allowAdmin = !roles.length || roles.includes('admin') || roles.includes('both');
  const overviewRole = allowUser ? 'user' : (allowAdmin ? 'co-admin' : 'disabled');
  const launchRole = allowUser ? 'user' : (allowAdmin ? 'co-admin' : 'disabled');
  const settingsRole = allowAdmin ? 'admin' : 'disabled';
  return buildMenuAccessConfig({
    sidebar: deriveSidebarMinRole({}, [overviewRole, launchRole, settingsRole]),
    sidebarOverview: overviewRole,
    sidebarSettings: settingsRole,
    sidebarActivity: 'admin',
    overview: overviewRole,
    launch: launchRole,
    settings: settingsRole,
  });
}

function getOverviewElements(appItem) {
  if (!appItem) return [];
  return APP_OVERVIEW_ELEMENTS[getAppBaseId(appItem.id)] || [];
}

function buildDisabledMenuAccess() {
  return buildMenuAccessConfig({
    sidebar: 'disabled',
    sidebarOverview: 'disabled',
    sidebarSettings: 'disabled',
    sidebarActivity: 'disabled',
    overview: 'disabled',
    launch: 'disabled',
    settings: 'disabled',
  });
}

function buildDisabledOverviewElements(appItem) {
  const elements = getOverviewElements(appItem);
  return elements.map((element, index) => ({
    id: element.id,
    enable: false,
    dashboard: false,
    favourite: false,
    order: index + 1,
  }));
}

function normalizeCategoryName(value) {
  const name = String(value || '').trim();
  const key = name.toLowerCase();
  if (key === 'media manager') return 'Manager';
  if (key === 'utilities') return 'Tools';
  return name;
}

function categoryVisibilityRolesFromLegacyMinRole(minRole = 'user') {
  const normalized = normalizeVisibilityRole(minRole, 'user');
  if (normalized === 'disabled') return [];
  const minRank = Number(VISIBILITY_ROLE_RANK[normalized]);
  return CATEGORY_VISIBILITY_SELECTABLE_ROLES.filter((role) => {
    const rank = Number(VISIBILITY_ROLE_RANK[role]);
    return Number.isFinite(rank) && Number.isFinite(minRank) && rank >= minRank;
  });
}

function normalizeCategoryVisibilityRoles(value, fallback = undefined) {
  const inputList = Array.isArray(value)
    ? value
    : (typeof value === 'string' && value.includes(','))
      ? value.split(',')
      : (value == null ? [] : [value]);
  const parsed = uniqueList(
    inputList
      .map((item) => parseVisibilityRole(item))
      .filter((role) => role && role !== 'disabled' && CATEGORY_VISIBILITY_SELECTABLE_ROLES.includes(role))
  );
  if (parsed.length) {
    return CATEGORY_VISIBILITY_SELECTABLE_ROLES.filter((role) => parsed.includes(role));
  }
  if (fallback === undefined) return [];
  if (Array.isArray(fallback)) return normalizeCategoryVisibilityRoles(fallback);
  return categoryVisibilityRolesFromLegacyMinRole(fallback);
}

function normalizeCategoryEntries(items) {
  if (!Array.isArray(items)) return [];
  const seen = new Set();
  const entries = [];
  items.forEach((value) => {
    let label = '';
    let sidebarMenu = false;
    let sidebarMinRole = '';
    let visibilityRolesRaw = null;
    let icon = '';
    if (typeof value === 'string') {
      label = value;
    } else if (value && typeof value === 'object') {
      label = value.name || value.category || value.label || value.value || '';
      sidebarMenu = Boolean(
        value.sidebarMenu
        || value.sidebar_menu
        || value.submenu
        || value.sidebarSubmenu
        || value.grouped
      );
      sidebarMinRole = value.sidebarMinRole
        || value.sidebar_min_role
        || value.categorySidebarMinRole
        || value.visibilityRole
        || value.minRole
        || '';
      visibilityRolesRaw = value.visibilityRoles != null ? value.visibilityRoles : null;
      icon = value.icon || value.iconPath || value.iconUrl || value.icon_url || '';
    }
    const name = normalizeCategoryName(label);
    const key = name.toLowerCase();
    if (!name || seen.has(key)) return;
    seen.add(key);
    let visibilityRoles;
    if (visibilityRolesRaw != null) {
      visibilityRoles = normalizeCategoryVisibilityRoles(visibilityRolesRaw);
    } else {
      visibilityRoles = categoryVisibilityRolesFromLegacyMinRole(sidebarMinRole || (sidebarMenu ? 'user' : 'disabled'));
    }
    entries.push({
      name,
      sidebarMenu: visibilityRoles.length > 0,
      visibilityRoles,
      sidebarMinRole: visibilityRoles.length > 0 ? (visibilityRoles[0] || 'user') : 'disabled',
      icon: (() => {
        const iconValue = String(icon || '').trim();
        // Normalize legacy Requesters icon path to current default.
        if (key === 'requesters' && iconValue === '/icons/requesters.png') {
          return '/icons/requesters.svg';
        }
        return iconValue;
      })(),
    });
  });
  return entries;
}

function normalizeCategoryList(items) {
  return normalizeCategoryEntries(items).map((entry) => entry.name);
}

function resolveDefaultCategoryIcon(name) {
  const key = String(name || '').trim().toLowerCase();
  if (!key) return DEFAULT_CATEGORY_ICON;
  if (key === 'admin') return '/icons/admin.svg';
  if (key === 'media') return '/icons/media-play.svg';
  if (key === 'requesters') return '/icons/requesters.svg';
  if (key === 'manager') return '/icons/settings.svg';
  if (key === 'arr suite') return '/icons/app.svg';
  if (key === 'indexers') return '/icons/indexers.svg';
  if (key === 'downloaders') return '/icons/download.svg';
  if (key === 'tools') return '/icons/tools.svg';
  if (key === 'photos') return '/icons/photos.svg';
  if (key === 'system') return '/icons/system.svg';
  if (key === 'documents') return '/icons/documents.svg';
  if (key === 'finance') return '/icons/finance.svg';
  return DEFAULT_CATEGORY_ICON;
}

function resolveCategoryEntries(config, apps = [], options = {}) {
  const includeAppCategories = options.includeAppCategories !== false;
  const configured = normalizeCategoryEntries(config?.categories);
  const entries = configured.length
    ? [...configured]
    : DEFAULT_CATEGORY_ORDER.map((name) => ({ name, sidebarMenu: false, visibilityRoles: [], sidebarMinRole: 'disabled', icon: DEFAULT_CATEGORY_ICON }));
  if (!includeAppCategories) return entries;

  const seen = new Set(entries.map((entry) => entry.name.toLowerCase()));
  (Array.isArray(apps) ? apps : []).forEach((appItem) => {
    const category = normalizeCategoryName(appItem?.category);
    const key = category.toLowerCase();
    if (!category || seen.has(key)) return;
    seen.add(key);
    entries.push({ name: category, sidebarMenu: false, visibilityRoles: [], sidebarMinRole: 'disabled', icon: DEFAULT_CATEGORY_ICON });
  });
  return entries.map((entry) => {
    const iconValue = String(entry.icon || '').trim();
    if (!iconValue || iconValue === DEFAULT_CATEGORY_ICON) {
      return { ...entry, icon: resolveDefaultCategoryIcon(entry.name) };
    }
    return entry;
  });
}

function resolveCategoryOrder(config, apps = [], options = {}) {
  return resolveCategoryEntries(config, apps, options).map((entry) => entry.name);
}

function listIconFiles(dir, baseUrl) {
  try {
    return fs
      .readdirSync(dir)
      .filter((name) => /\.(svg|png|jpe?g|webp)$/i.test(name))
      .map((name) => `${baseUrl}/${name}`);
  } catch (err) {
    return [];
  }
}

function resolveKnownAppIconBaseNames(apps = []) {
  const additionalBases = [
    'dozzle',
    'minecraft-bedrock',
    'traefik',
    'plex-login',
    'plex-login-white',
  ];
  const combinedApps = dedupeApps([
    ...loadDefaultApps(),
    ...(Array.isArray(apps) ? apps : []),
  ]);
  const bases = new Set();
  additionalBases.forEach((name) => {
    const normalized = String(name || '').trim().toLowerCase();
    if (normalized) bases.add(normalized);
  });
  combinedApps.forEach((appItem) => {
    const id = normalizeAppId(appItem?.id);
    if (id) bases.add(id);
    const iconPath = String(appItem?.icon || '').trim();
    if (!iconPath.startsWith('/icons/')) return;
    const iconName = path.basename(iconPath).replace(/\.(svg|png|jpe?g|webp)$/i, '').toLowerCase();
    if (iconName) bases.add(iconName);
  });
  return bases;
}

function getDefaultSystemIconOptions(apps = []) {
  const iconsDir = path.join(__dirname, '..', 'public', 'icons');
  const excludedFiles = new Set([
    'launcharr-icon.png',
    'launcharr.svg',
    'appsa.png',
    'appsa.svg',
    'app-arr.svg',
  ]);
  const knownAppIconBases = resolveKnownAppIconBaseNames(apps);
  try {
    return fs
      .readdirSync(iconsDir)
      .filter((name) => /\.svg$/i.test(name))
      .filter((name) => !excludedFiles.has(name))
      .filter((name) => {
        const baseName = path.basename(name).replace(/\.(svg|png|jpe?g|webp)$/i, '').toLowerCase();
        return !knownAppIconBases.has(baseName);
      })
      .map((name) => `/icons/${name}`);
  } catch (err) {
    return [];
  }
}

function getCustomSystemIconOptions() {
  migrateMisplacedCustomIcons();
  const dir = path.join(__dirname, '..', 'public', 'icons', 'custom', 'system');
  return listIconFiles(dir, '/icons/custom/system');
}

function migrateMisplacedCustomIcons() {
  const misplacedRoot = path.join(__dirname, 'public', 'icons', 'custom');
  const correctRoot = path.join(__dirname, '..', 'public', 'icons', 'custom');
  const migrateBucket = (bucketName) => {
    const sourceDir = path.join(misplacedRoot, bucketName);
    const targetDir = path.join(correctRoot, bucketName);
    try {
      if (!fs.existsSync(sourceDir)) return;
      if (!fs.existsSync(targetDir)) fs.mkdirSync(targetDir, { recursive: true });
      fs.readdirSync(sourceDir).forEach((name) => {
        const sourcePath = path.join(sourceDir, name);
        const stat = fs.statSync(sourcePath);
        if (!stat.isFile()) return;
        if (!/\.(svg|png|jpe?g|webp)$/i.test(name)) return;
        const targetPath = path.join(targetDir, name);
        if (fs.existsSync(targetPath)) {
          fs.unlinkSync(sourcePath);
          return;
        }
        fs.renameSync(sourcePath, targetPath);
      });
    } catch (_err) {
      // ignore migration issues and continue with listing
    }
  };
  migrateBucket('apps');
  migrateBucket('system');
}

function migrateLegacyCustomAppIcons() {
  const legacyDir = path.join(__dirname, '..', 'public', 'icons', 'custom');
  const targetDir = path.join(__dirname, '..', 'public', 'icons', 'custom', 'apps');
  try {
    if (!fs.existsSync(legacyDir)) return;
    if (!fs.existsSync(targetDir)) fs.mkdirSync(targetDir, { recursive: true });
    fs.readdirSync(legacyDir).forEach((name) => {
      const legacyPath = path.join(legacyDir, name);
      const stat = fs.statSync(legacyPath);
      if (!stat.isFile()) return;
      if (!/\.(svg|png|jpe?g|webp)$/i.test(name)) return;
      const targetPath = path.join(targetDir, name);
      if (fs.existsSync(targetPath)) {
        fs.unlinkSync(legacyPath);
        return;
      }
      fs.renameSync(legacyPath, targetPath);
    });
  } catch (err) {
    return;
  }
}

function getCategoryIconOptions(apps = []) {
  return [...getDefaultSystemIconOptions(apps), ...getCustomSystemIconOptions()];
}

function getDefaultAppIconOptions(apps = []) {
  const iconsDir = path.join(__dirname, '..', 'public', 'icons');
  const appIds = Array.from(resolveKnownAppIconBaseNames(apps));
  const seen = new Set();
  const options = [];
  appIds.forEach((appId) => {
    const pngPath = path.join(iconsDir, `${appId}.png`);
    const svgPath = path.join(iconsDir, `${appId}.svg`);
    if (fs.existsSync(pngPath)) {
      options.push(`/icons/${appId}.png`);
      seen.add(`/icons/${appId}.png`);
    } else if (fs.existsSync(svgPath)) {
      options.push(`/icons/${appId}.svg`);
      seen.add(`/icons/${appId}.svg`);
    }
  });
  try {
    fs.readdirSync(iconsDir)
      .filter((name) => /\.png$/i.test(name))
      .forEach((name) => {
        const url = `/icons/${name}`;
        if (!seen.has(url)) options.push(url);
      });
  } catch (err) {
    return options;
  }
  return options;
}

function getCustomAppIconOptions() {
  migrateMisplacedCustomIcons();
  migrateLegacyCustomAppIcons();
  const appsDir = path.join(__dirname, '..', 'public', 'icons', 'custom', 'apps');
  return listIconFiles(appsDir, '/icons/custom/apps');
}

function getAppIconOptions(apps = []) {
  return [...getDefaultAppIconOptions(apps), ...getCustomAppIconOptions()];
}

function buildCategoryRank(categoryOrder) {
  const categories = normalizeCategoryList(categoryOrder);
  const rank = new Map(categories.map((value, index) => [value.toLowerCase(), index]));
  return (value) => {
    const key = String(value || '').trim().toLowerCase();
    if (!key) return categories.length;
    return rank.has(key) ? rank.get(key) : categories.length;
  };
}

function resolveOverviewElementVisibilityRole(appItem, elementSettings = {}, fallback = 'user') {
  const source = elementSettings && typeof elementSettings === 'object' ? elementSettings : {};
  const explicit = parseVisibilityRole(source.overviewVisibilityRole);
  if (explicit) return explicit;
  const legacyDashboardRole = parseVisibilityRole(source.dashboardVisibilityRole);
  const isEnabled = source.enable === undefined ? true : Boolean(source.enable);
  if (!isEnabled) return 'disabled';
  if (legacyDashboardRole) return legacyDashboardRole;
  const menu = normalizeMenu(appItem);
  return normalizeVisibilityRole(menu?.overview?.minRole || fallback, fallback);
}

function resolveDashboardElementVisibilityRole(appItem, elementSettings = {}, fallback = 'user') {
  const source = elementSettings && typeof elementSettings === 'object' ? elementSettings : {};
  const explicit = parseVisibilityRole(source.dashboardVisibilityRole);
  if (explicit) return explicit;
  const isOnDashboard = source.dashboard === undefined
    ? (source.enable === undefined ? true : Boolean(source.enable))
    : Boolean(source.dashboard);
  if (!isOnDashboard) return 'disabled';
  const menu = normalizeMenu(appItem);
  return normalizeVisibilityRole(menu?.overview?.minRole || fallback, fallback);
}

function canAccessOverviewElement(appItem, elementSettings, role) {
  const roleKey = parseVisibilityRole(role);
  if (!roleKey) return false;
  const minRole = resolveOverviewElementVisibilityRole(appItem, elementSettings, 'user');
  return roleMeetsMinRole(roleKey, minRole);
}

function canAccessDashboardElement(appItem, elementSettings, role) {
  const roleKey = parseVisibilityRole(role);
  if (!roleKey) return false;
  const minRole = resolveDashboardElementVisibilityRole(appItem, elementSettings, 'user');
  return roleMeetsMinRole(roleKey, minRole);
}

function canAccessAnyDashboardElement(appItem, role) {
  if (appItem?.removed) return false;
  return mergeOverviewElementSettings(appItem).some((element) =>
    canAccessDashboardElement(appItem, element, role)
  );
}

function resolveCombinedDashboardVisibilityRole(settings, fallback = 'user') {
  const source = settings && typeof settings === 'object' ? settings : {};
  const explicit = parseVisibilityRole(source.visibilityRole);
  if (explicit) return explicit;
  const isEnabled = source.enable === undefined ? true : Boolean(source.enable);
  const isOnDashboard = source.dashboard === undefined ? true : Boolean(source.dashboard);
  if (!isEnabled || !isOnDashboard) return 'disabled';
  return normalizeVisibilityRole(fallback, 'user');
}

function canAccessCombinedDashboardVisibility(settings, role, fallback = 'user') {
  const roleKey = parseVisibilityRole(role);
  if (!roleKey) return false;
  const minRole = resolveCombinedDashboardVisibilityRole(settings, fallback);
  return roleMeetsMinRole(roleKey, minRole);
}

const DASHBOARD_MAIN_ID = 'main';
const DASHBOARD_MAX_COUNT = 10;
const DEFAULT_DASHBOARD_ICON = '/icons/dashboard.svg';
const DASHBOARD_VISIBILITY_SELECTABLE_ROLES = ['guest', 'user', 'co-admin', 'admin'];

function dashboardVisibilityRolesFromLegacyMinRole(minRole = 'user') {
  const normalized = normalizeVisibilityRole(minRole, 'user');
  if (normalized === 'disabled') return [];
  const minRank = Number(VISIBILITY_ROLE_RANK[normalized]);
  return DASHBOARD_VISIBILITY_SELECTABLE_ROLES.filter((role) => {
    const rank = Number(VISIBILITY_ROLE_RANK[role]);
    return Number.isFinite(rank) && Number.isFinite(minRank) && rank >= minRank;
  });
}

function normalizeDashboardVisibilityRoles(value, fallback = undefined) {
  const inputList = Array.isArray(value)
    ? value
    : (typeof value === 'string' && value.includes(','))
      ? value.split(',')
      : (value === undefined ? [] : [value]);
  const parsed = uniqueList(
    inputList
      .map((item) => parseVisibilityRole(item))
      .filter((role) => role && role !== 'disabled' && DASHBOARD_VISIBILITY_SELECTABLE_ROLES.includes(role))
  );
  if (parsed.length) {
    return DASHBOARD_VISIBILITY_SELECTABLE_ROLES.filter((role) => parsed.includes(role));
  }
  if (fallback === undefined) return [];
  if (Array.isArray(fallback)) {
    return normalizeDashboardVisibilityRoles(fallback);
  }
  return dashboardVisibilityRolesFromLegacyMinRole(fallback);
}

function deriveDashboardLegacyVisibilityRole(roles = [], fallback = 'disabled') {
  const normalized = normalizeDashboardVisibilityRoles(roles);
  if (!normalized.length) return normalizeVisibilityRole(fallback, 'disabled');
  const first = DASHBOARD_VISIBILITY_SELECTABLE_ROLES.find((role) => normalized.includes(role));
  return normalizeVisibilityRole(first, fallback);
}

function resolveDashboardDefinitionVisibilityRoles(entry, fallback = 'user') {
  const source = entry && typeof entry === 'object' ? entry : {};
  if (Array.isArray(source.visibilityRoles)) {
    return normalizeDashboardVisibilityRoles(source.visibilityRoles);
  }
  return normalizeDashboardVisibilityRoles(source.visibilityRoles, source.visibilityRole || fallback);
}

function cloneJsonValue(value, fallback) {
  if (value === undefined) return fallback;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (err) {
    return fallback;
  }
}

function normalizeDashboardInstanceId(value) {
  const source = Array.isArray(value)
    ? value.find((item) => String(item || '').trim()) ?? value[0]
    : value;
  const normalized = String(source || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '');
  if (!normalized) return '';
  if (normalized === DASHBOARD_MAIN_ID) return DASHBOARD_MAIN_ID;
  return normalized.slice(0, 48);
}

function normalizeDashboardDisplayName(value, fallback = 'Dashboard') {
  const name = String(value || '').trim();
  if (!name) return String(fallback || 'Dashboard').trim() || 'Dashboard';
  return name.slice(0, 48);
}

function normalizeDashboardIcon(value, fallback = DEFAULT_DASHBOARD_ICON) {
  const raw = String(value || '').trim();
  if (!raw) return String(fallback || DEFAULT_DASHBOARD_ICON).trim() || DEFAULT_DASHBOARD_ICON;
  if (!raw.startsWith('/icons/')) {
    return String(fallback || DEFAULT_DASHBOARD_ICON).trim() || DEFAULT_DASHBOARD_ICON;
  }
  return raw;
}

function normalizeDashboardDefinition(entry, index = 0) {
  const source = entry && typeof entry === 'object' ? entry : {};
  const requestedId = normalizeDashboardInstanceId(source.id);
  const fallbackId = index === 0 ? DASHBOARD_MAIN_ID : '';
  const id = requestedId || fallbackId;
  if (!id) return null;
  const isMain = id === DASHBOARD_MAIN_ID;
  const visibilityRoles = resolveDashboardDefinitionVisibilityRoles(source, 'user');
  return {
    id,
    name: normalizeDashboardDisplayName(source.name, isMain ? 'Dashboard' : 'Dashboard'),
    icon: normalizeDashboardIcon(source.icon, DEFAULT_DASHBOARD_ICON),
    visibilityRole: deriveDashboardLegacyVisibilityRole(
      visibilityRoles,
      visibilityRoles.length ? normalizeVisibilityRole(source.visibilityRole, 'user') : 'disabled',
    ),
    visibilityRoles,
    state: source.state && typeof source.state === 'object'
      ? cloneJsonValue(source.state, {})
      : undefined,
  };
}

function resolveDashboardDefinitions(config) {
  const source = Array.isArray(config?.dashboards) ? config.dashboards : [];
  const seen = new Set();
  const entries = [];

  source.forEach((entry, index) => {
    const normalized = normalizeDashboardDefinition(entry, index);
    if (!normalized?.id) return;
    if (seen.has(normalized.id)) return;
    seen.add(normalized.id);
    entries.push(normalized);
  });

  if (!seen.has(DASHBOARD_MAIN_ID)) {
    entries.unshift({
      id: DASHBOARD_MAIN_ID,
      name: 'Dashboard',
      icon: DEFAULT_DASHBOARD_ICON,
      visibilityRole: 'user',
      visibilityRoles: dashboardVisibilityRolesFromLegacyMinRole('user'),
    });
  }

  const mainIndex = entries.findIndex((entry) => entry.id === DASHBOARD_MAIN_ID);
  if (mainIndex > 0) {
    const [mainEntry] = entries.splice(mainIndex, 1);
    entries.unshift(mainEntry);
  }

  return entries.map((entry, index) => normalizeDashboardDefinition(entry, index)).filter(Boolean);
}

function buildDashboardInstanceId(existingDashboards = []) {
  const taken = new Set(
    (Array.isArray(existingDashboards) ? existingDashboards : [])
      .map((entry) => normalizeDashboardInstanceId(entry?.id))
      .filter(Boolean)
  );
  for (let index = 2; index <= 999; index += 1) {
    const candidate = `dashboard-${index}`;
    if (!taken.has(candidate)) return candidate;
  }
  return `dashboard-${Date.now()}`;
}

function canAccessDashboardDefinition(entry, role) {
  if (!entry) return false;
  const roleKey = parseVisibilityRole(role);
  if (!roleKey || roleKey === 'disabled') return false;
  const allowedRoles = resolveDashboardDefinitionVisibilityRoles(entry, 'user');
  return allowedRoles.includes(roleKey);
}

function resolveRequestedDashboardId(req) {
  return normalizeDashboardInstanceId(
    req?.body?.dashboardId
    || req?.body?.dashboard
    || req?.query?.dashboard
    || ''
  );
}

function resolveDashboardSelection(config, requestedId, role, options = {}) {
  const includeHidden = options.includeHidden === true;
  const dashboards = resolveDashboardDefinitions(config);
  const requested = normalizeDashboardInstanceId(requestedId);
  const visibleDashboards = includeHidden
    ? dashboards
    : dashboards.filter((entry) => canAccessDashboardDefinition(entry, role));
  const visibleById = new Map(visibleDashboards.map((entry) => [entry.id, entry]));
  const allById = new Map(dashboards.map((entry) => [entry.id, entry]));
  const activeDashboard = (requested && (includeHidden ? allById.get(requested) : visibleById.get(requested)))
    || (includeHidden ? (allById.get(DASHBOARD_MAIN_ID) || dashboards[0]) : (visibleById.get(DASHBOARD_MAIN_ID) || visibleDashboards[0]))
    || null;
  return {
    dashboards,
    visibleDashboards,
    activeDashboard,
    activeDashboardId: String(activeDashboard?.id || '').trim() || DASHBOARD_MAIN_ID,
  };
}

function resolveDashboardVisibilityRolesFromRequest(body, fallbackEntry = null) {
  const source = body && typeof body === 'object' ? body : {};
  const fallbackRoles = resolveDashboardDefinitionVisibilityRoles(fallbackEntry || {}, 'user');
  const hasMultiSelectField = Object.prototype.hasOwnProperty.call(source, 'dashboard_visibility_roles')
    || Object.prototype.hasOwnProperty.call(source, 'dashboard_visibility_roles_present');
  if (hasMultiSelectField) {
    return normalizeDashboardVisibilityRoles(source.dashboard_visibility_roles, []);
  }
  return normalizeDashboardVisibilityRoles(
    source.dashboard_visibility_role,
    fallbackRoles.length ? fallbackRoles : 'user',
  );
}

function extractDashboardStateSnapshot(config) {
  const apps = Array.isArray(config?.apps) ? config.apps : [];
  const appStates = {};
  apps.forEach((appItem) => {
    const appId = normalizeAppId(appItem?.id) || String(appItem?.id || '').trim();
    if (!appId) return;
    const snapshot = {};
    if (Array.isArray(appItem?.overviewElements)) {
      snapshot.overviewElements = cloneJsonValue(appItem.overviewElements, []);
    }
    if (Array.isArray(appItem?.tautulliCards)) {
      snapshot.tautulliCards = cloneJsonValue(appItem.tautulliCards, []);
    }
    if (Object.keys(snapshot).length) {
      appStates[appId] = snapshot;
    }
  });
  return {
    dashboardRemovedElements: cloneJsonValue(config?.dashboardRemovedElements, {}),
    dashboardCombinedOrder: cloneJsonValue(config?.dashboardCombinedOrder, {}),
    dashboardCombinedSettings: cloneJsonValue(config?.dashboardCombinedSettings, {}),
    arrDashboardCombine: cloneJsonValue(config?.arrDashboardCombine, {}),
    mediaDashboardCombine: cloneJsonValue(config?.mediaDashboardCombine, {}),
    downloaderDashboardCombine: cloneJsonValue(config?.downloaderDashboardCombine, {}),
    arrCombinedQueueDisplay: cloneJsonValue(config?.arrCombinedQueueDisplay, {}),
    downloaderCombinedQueueDisplay: cloneJsonValue(config?.downloaderCombinedQueueDisplay, {}),
    arrDashboardCombinedCards: cloneJsonValue(config?.arrDashboardCombinedCards, []),
    downloaderDashboardCards: cloneJsonValue(config?.downloaderDashboardCards, []),
    mediaDashboardCards: cloneJsonValue(config?.mediaDashboardCards, []),
    apps: appStates,
  };
}

function buildEmptyDashboardStateSnapshot(config) {
  const apps = Array.isArray(config?.apps) ? config.apps : [];
  const appStates = {};
  const dashboardRemovedElements = {};
  apps.forEach((appItem) => {
    const appId = normalizeAppId(appItem?.id) || String(appItem?.id || '').trim();
    if (!appId) return;
    const rawAppId = String(appItem?.id || '').trim();
    const overviewElements = getOverviewElements(appItem).map((element, index) => {
      const elementId = String(element?.id || '').trim();
      if (rawAppId && elementId) {
        dashboardRemovedElements[`app:${rawAppId}:${elementId}`] = true;
      }
      return {
        id: elementId,
        enable: false,
        dashboard: false,
        overviewVisibilityRole: 'disabled',
        dashboardVisibilityRole: 'disabled',
        favourite: false,
        order: index + 1,
      };
    });
    const tautulliCards = getTautulliCards(appItem).map((card, index) => ({
      id: String(card?.id || '').trim(),
      enable: false,
      order: index + 1,
    })).filter((card) => card.id);
    const snapshot = {};
    if (overviewElements.length) snapshot.overviewElements = overviewElements;
    if (tautulliCards.length) snapshot.tautulliCards = tautulliCards;
    if (Object.keys(snapshot).length) appStates[appId] = snapshot;
  });
  ARR_COMBINE_SECTIONS.forEach((section) => {
    dashboardRemovedElements[`combined:arr:${section.key}`] = true;
  });
  DOWNLOADER_COMBINE_SECTIONS.forEach((section) => {
    dashboardRemovedElements[`combined:downloader:${section.key}`] = true;
  });
  MEDIA_COMBINE_SECTIONS.forEach((section) => {
    dashboardRemovedElements[`combined:media:${section.key}`] = true;
  });
  resolveArrDashboardCombinedCards(config, apps).forEach((card, index) => {
    const token = normalizeCombinedCardToken(card?.id || '') || `card-${index + 1}`;
    dashboardRemovedElements[`combined:arrcustom:${token}`] = true;
  });
  resolveDownloaderDashboardCards(config, apps).forEach((card, index) => {
    const token = normalizeCombinedCardToken(card?.id || '') || `card-${index + 1}`;
    dashboardRemovedElements[`combined:downloadercustom:${token}`] = true;
  });
  resolveMediaDashboardCards(config, apps).forEach((card, index) => {
    const token = normalizeCombinedCardToken(card?.id || '') || `card-${index + 1}`;
    dashboardRemovedElements[`combined:mediacustom:${token}`] = true;
  });
  return {
    dashboardRemovedElements,
    dashboardCombinedOrder: {},
    dashboardCombinedSettings: {},
    arrDashboardCombine: {},
    mediaDashboardCombine: {},
    downloaderDashboardCombine: {},
    arrCombinedQueueDisplay: {},
    downloaderCombinedQueueDisplay: {},
    arrDashboardCombinedCards: [],
    downloaderDashboardCards: [],
    mediaDashboardCards: [],
    apps: appStates,
  };
}

function applyDashboardStateSnapshot(config, dashboardEntry) {
  if (!dashboardEntry || dashboardEntry.id === DASHBOARD_MAIN_ID) return config;
  const snapshot = dashboardEntry.state && typeof dashboardEntry.state === 'object' ? dashboardEntry.state : null;
  if (!snapshot) return config;
  const nextConfig = { ...config };
  if (snapshot.dashboardRemovedElements && typeof snapshot.dashboardRemovedElements === 'object') {
    nextConfig.dashboardRemovedElements = cloneJsonValue(snapshot.dashboardRemovedElements, {});
  }
  if (snapshot.dashboardCombinedOrder && typeof snapshot.dashboardCombinedOrder === 'object') {
    nextConfig.dashboardCombinedOrder = cloneJsonValue(snapshot.dashboardCombinedOrder, {});
  }
  if (snapshot.dashboardCombinedSettings && typeof snapshot.dashboardCombinedSettings === 'object') {
    nextConfig.dashboardCombinedSettings = cloneJsonValue(snapshot.dashboardCombinedSettings, {});
  }
  if (snapshot.arrDashboardCombine && typeof snapshot.arrDashboardCombine === 'object') {
    nextConfig.arrDashboardCombine = cloneJsonValue(snapshot.arrDashboardCombine, {});
  }
  if (snapshot.mediaDashboardCombine && typeof snapshot.mediaDashboardCombine === 'object') {
    nextConfig.mediaDashboardCombine = cloneJsonValue(snapshot.mediaDashboardCombine, {});
  }
  if (snapshot.downloaderDashboardCombine && typeof snapshot.downloaderDashboardCombine === 'object') {
    nextConfig.downloaderDashboardCombine = cloneJsonValue(snapshot.downloaderDashboardCombine, {});
  }
  if (snapshot.arrCombinedQueueDisplay && typeof snapshot.arrCombinedQueueDisplay === 'object') {
    nextConfig.arrCombinedQueueDisplay = cloneJsonValue(snapshot.arrCombinedQueueDisplay, {});
  }
  if (snapshot.downloaderCombinedQueueDisplay && typeof snapshot.downloaderCombinedQueueDisplay === 'object') {
    nextConfig.downloaderCombinedQueueDisplay = cloneJsonValue(snapshot.downloaderCombinedQueueDisplay, {});
  }
  if (Array.isArray(snapshot.arrDashboardCombinedCards)) {
    nextConfig.arrDashboardCombinedCards = cloneJsonValue(snapshot.arrDashboardCombinedCards, []);
  }
  if (Array.isArray(snapshot.downloaderDashboardCards)) {
    nextConfig.downloaderDashboardCards = cloneJsonValue(snapshot.downloaderDashboardCards, []);
  }
  if (Array.isArray(snapshot.mediaDashboardCards)) {
    nextConfig.mediaDashboardCards = cloneJsonValue(snapshot.mediaDashboardCards, []);
  }
  const snapshotApps = snapshot.apps && typeof snapshot.apps === 'object' ? snapshot.apps : {};
  if (Array.isArray(config?.apps)) {
    nextConfig.apps = config.apps.map((appItem) => {
      const appId = normalizeAppId(appItem?.id) || String(appItem?.id || '').trim();
      const appSnapshot = (appId && snapshotApps[appId] && typeof snapshotApps[appId] === 'object')
        ? snapshotApps[appId]
        : null;
      if (!appSnapshot) return appItem;
      const nextApp = { ...appItem };
      if (Array.isArray(appSnapshot.overviewElements)) {
        nextApp.overviewElements = cloneJsonValue(appSnapshot.overviewElements, []);
      }
      if (Array.isArray(appSnapshot.tautulliCards)) {
        nextApp.tautulliCards = cloneJsonValue(appSnapshot.tautulliCards, []);
      }
      return nextApp;
    });
  }
  return nextConfig;
}

function buildDashboardScopedConfigForSave(baseConfig, nextEffectiveConfig, dashboardId, nextDashboards) {
  const selectedDashboardId = normalizeDashboardInstanceId(dashboardId) || DASHBOARD_MAIN_ID;
  if (selectedDashboardId === DASHBOARD_MAIN_ID) {
    const dashboards = Array.isArray(nextDashboards) ? nextDashboards : resolveDashboardDefinitions(nextEffectiveConfig);
    return {
      ...nextEffectiveConfig,
      dashboards,
    };
  }
  const baseDashboards = Array.isArray(nextDashboards) ? nextDashboards : resolveDashboardDefinitions(baseConfig);
  const snapshot = extractDashboardStateSnapshot(nextEffectiveConfig);
  const updatedDashboards = baseDashboards.map((entry) => (
    entry.id === selectedDashboardId
      ? { ...entry, state: snapshot }
      : entry
  ));
  return {
    ...baseConfig,
    dashboards: updatedDashboards,
  };
}

function saveDashboardScopedConfig(baseConfig, nextEffectiveConfig, dashboardId, nextDashboards) {
  saveConfig(buildDashboardScopedConfigForSave(baseConfig, nextEffectiveConfig, dashboardId, nextDashboards));
}

function buildDashboardSettingsRedirect(req, extraParams = {}) {
  const params = new URLSearchParams({
    tab: 'custom',
    settingsCustomTab: 'dashboard',
  });
  const dashboardId = normalizeDashboardInstanceId(extraParams.dashboardId || resolveRequestedDashboardId(req));
  if (dashboardId) params.set('dashboard', dashboardId);
  Object.entries(extraParams || {}).forEach(([key, value]) => {
    if (key === 'dashboardId') return;
    if (value === undefined || value === null || value === '') return;
    params.set(key, String(value));
  });
  return `/settings?${params.toString()}`;
}

function resolveCombinedSourceSelectionIds(appIds = [], sectionMap = {}) {
  const normalizedIds = [...new Set(
    (Array.isArray(appIds) ? appIds : [])
      .map((id) => normalizeAppId(id))
      .filter(Boolean)
  )];
  if (!normalizedIds.length) return [];
  const source = sectionMap && typeof sectionMap === 'object' ? sectionMap : {};
  const selectedIds = normalizedIds.filter((id) => Boolean(source[id]));
  return selectedIds.length ? selectedIds : normalizedIds;
}

function canAccessDashboardAppViaCombined(config, appItem, role) {
  if (appItem?.removed) return false;
  const appId = normalizeAppId(appItem?.id);
  if (!appId) return false;
  const apps = Array.isArray(config?.apps) ? config.apps : [];
  const dashboardRemovedElements = (config && typeof config.dashboardRemovedElements === 'object' && config.dashboardRemovedElements)
    ? config.dashboardRemovedElements
    : {};
  const dashboardCombinedSettings = (config && typeof config.dashboardCombinedSettings === 'object' && config.dashboardCombinedSettings)
    ? config.dashboardCombinedSettings
    : {};

  if (isAppInSet(appId, ARR_APP_IDS)) {
    const arrApps = apps.filter((entry) => !entry?.removed && isAppInSet(entry?.id, ARR_APP_IDS));
    const arrAppIds = arrApps.map((entry) => normalizeAppId(entry.id)).filter(Boolean);
    const arrCombineMap = resolveArrDashboardCombineSettings(config, apps);
    for (let index = 0; index < ARR_COMBINE_SECTIONS.length; index += 1) {
      const section = ARR_COMBINE_SECTIONS[index];
      const combinedKey = `combined:arr:${section.key}`;
      if (dashboardRemovedElements[combinedKey]) continue;
      if (!canAccessCombinedDashboardVisibility(dashboardCombinedSettings[combinedKey], role, 'user')) continue;
      const selectedIds = resolveCombinedSourceSelectionIds(arrAppIds, arrCombineMap?.[section.key]);
      if (selectedIds.includes(appId)) return true;
    }
    const arrCustomCards = resolveArrDashboardCombinedCards(config, apps);
    for (let index = 0; index < arrCustomCards.length; index += 1) {
      const card = arrCustomCards[index];
      const customToken = normalizeCombinedCardToken(card?.id || '') || `card-${index + 1}`;
      const combinedKey = `combined:arrcustom:${customToken}`;
      if (dashboardRemovedElements[combinedKey]) continue;
      if (!canAccessCombinedDashboardVisibility(dashboardCombinedSettings[combinedKey], role, 'user')) continue;
      const selectedIds = [...new Set(
        (Array.isArray(card?.appIds) ? card.appIds : [])
          .map((id) => normalizeAppId(id))
          .filter(Boolean)
      )];
      if (selectedIds.includes(appId)) return true;
    }
  }

  if (isAppInSet(appId, DOWNLOADER_APP_IDS)) {
    const downloaderApps = apps.filter((entry) => !entry?.removed && isAppInSet(entry?.id, DOWNLOADER_APP_IDS));
    const downloaderAppIds = downloaderApps.map((entry) => normalizeAppId(entry.id)).filter(Boolean);
    const downloaderCombineMap = resolveDownloaderDashboardCombineSettings(config, apps);
    for (let index = 0; index < DOWNLOADER_COMBINE_SECTIONS.length; index += 1) {
      const section = DOWNLOADER_COMBINE_SECTIONS[index];
      const combinedKey = `combined:downloader:${section.key}`;
      if (dashboardRemovedElements[combinedKey]) continue;
      if (!canAccessCombinedDashboardVisibility(dashboardCombinedSettings[combinedKey], role, 'user')) continue;
      const selectedIds = resolveCombinedSourceSelectionIds(downloaderAppIds, downloaderCombineMap?.[section.key]);
      if (selectedIds.includes(appId)) return true;
    }
    const downloaderCustomCards = resolveDownloaderDashboardCards(config, apps);
    for (let index = 0; index < downloaderCustomCards.length; index += 1) {
      const card = downloaderCustomCards[index];
      const customToken = normalizeCombinedCardToken(card?.id || '') || `card-${index + 1}`;
      const combinedKey = `combined:downloadercustom:${customToken}`;
      if (dashboardRemovedElements[combinedKey]) continue;
      if (!canAccessCombinedDashboardVisibility(dashboardCombinedSettings[combinedKey], role, 'user')) continue;
      const selectedIds = [...new Set(
        (Array.isArray(card?.appIds) ? card.appIds : [])
          .map((id) => normalizeAppId(id))
          .filter(Boolean)
      )];
      if (selectedIds.includes(appId)) return true;
    }
  }

  if (isAppInSet(appId, MEDIA_APP_IDS)) {
    const mediaApps = apps.filter((entry) => !entry?.removed && isAppInSet(entry?.id, MEDIA_APP_IDS));
    const mediaAppIds = mediaApps.map((entry) => normalizeAppId(entry.id)).filter(Boolean);
    const mediaCombineMap = resolveMediaDashboardCombineSettings(config, apps);
    for (let index = 0; index < MEDIA_COMBINE_SECTIONS.length; index += 1) {
      const section = MEDIA_COMBINE_SECTIONS[index];
      const combinedKey = `combined:media:${section.key}`;
      if (dashboardRemovedElements[combinedKey]) continue;
      if (!canAccessCombinedDashboardVisibility(dashboardCombinedSettings[combinedKey], role, 'user')) continue;
      const selectedIds = resolveCombinedSourceSelectionIds(mediaAppIds, mediaCombineMap?.[section.key]);
      if (selectedIds.includes(appId)) return true;
    }
    const mediaCustomCards = resolveMediaDashboardCards(config, apps);
    for (let index = 0; index < mediaCustomCards.length; index += 1) {
      const card = mediaCustomCards[index];
      const customToken = normalizeCombinedCardToken(card?.id || '') || `card-${index + 1}`;
      const combinedKey = `combined:mediacustom:${customToken}`;
      if (dashboardRemovedElements[combinedKey]) continue;
      if (!canAccessCombinedDashboardVisibility(dashboardCombinedSettings[combinedKey], role, 'user')) continue;
      const selectedIds = [...new Set(
        (Array.isArray(card?.appIds) ? card.appIds : [])
          .map((id) => normalizeAppId(id))
          .filter(Boolean)
      )];
      if (selectedIds.includes(appId)) return true;
    }
  }

  return false;
}

function canAccessDashboardApp(config, appItem, role) {
  if (!appItem || appItem?.removed) return false;
  if (canAccess(appItem, role, 'overview')) return true;
  if (canAccessAnyDashboardElement(appItem, role)) return true;
  return canAccessDashboardAppViaCombined(config, appItem, role);
}

function resolveTableVisibleRows(elementId, rawValue, fallbackValue = undefined) {
  const id = String(elementId || '').trim().toLowerCase();
  const isIndexerSearch = id === 'search';
  const minRows = 5;
  const maxRows = isIndexerSearch ? 100 : 50;
  const defaultRows = isIndexerSearch ? 25 : 10;
  const parsed = Number(rawValue);
  if (Number.isFinite(parsed)) {
    return Math.max(minRows, Math.min(maxRows, parsed));
  }
  const parsedFallback = Number(fallbackValue);
  if (Number.isFinite(parsedFallback)) {
    return Math.max(minRows, Math.min(maxRows, parsedFallback));
  }
  return defaultRows;
}

function normalizeTautulliStatsView(value, fallback = 'list') {
  const normalizedFallback = String(fallback || '').trim().toLowerCase() === 'wheel' ? 'wheel' : 'list';
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'wheel') return 'wheel';
  if (raw === 'list') return 'list';
  if (raw === 'watch-stats-wheel' || raw === 'wheel-view') return 'wheel';
  return normalizedFallback;
}

function resolveTautulliStatsView(appItem, elementSettings = {}, fallback = 'list') {
  if (normalizeAppId(appItem?.id) !== 'tautulli') return undefined;
  const elementId = String(elementSettings?.id || '').trim();
  if (elementId === 'watch-stats-wheel') return 'wheel';
  return normalizeTautulliStatsView(elementSettings?.tautulliStatsView, fallback);
}

function mergeOverviewElementSettings(appItem) {
  const elements = getOverviewElements(appItem);
  if (!elements.length) return [];
  const saved = Array.isArray(appItem.overviewElements) ? appItem.overviewElements : [];
  const savedMap = new Map(saved.map((item) => [item.id, item]));
  const merged = elements.map((element, index) => {
    const savedItem = savedMap.get(element.id) || {};
    const orderValue = Number(savedItem.order);
    const resolveBoolean = (value, fallback) => (value === undefined ? fallback : Boolean(value));
    const overviewVisibilityRole = resolveOverviewElementVisibilityRole(appItem, savedItem, 'user');
    const dashboardVisibilityRole = resolveDashboardElementVisibilityRole(appItem, savedItem, 'user');
    const overviewVisible = overviewVisibilityRole !== 'disabled';
    const dashboardVisible = dashboardVisibilityRole !== 'disabled';
    const queueVisibleRows = resolveTableVisibleRows(element.id, savedItem.queueVisibleRows);
    const queueLabels = resolveQueueColumnLabels(appItem);
    const legacyTautulliWheel = savedMap.get('watch-stats-wheel') || {};
    const tautulliViewFallback = (() => {
      if (normalizeAppId(appItem?.id) !== 'tautulli' || element.id !== 'watch-stats') return 'list';
      const wheelOverviewRole = resolveOverviewElementVisibilityRole(appItem, legacyTautulliWheel, 'disabled');
      const wheelDashboardRole = resolveDashboardElementVisibilityRole(appItem, legacyTautulliWheel, 'disabled');
      return (wheelOverviewRole !== 'disabled' || wheelDashboardRole !== 'disabled') ? 'wheel' : 'list';
    })();
    const tautulliStatsView = resolveTautulliStatsView(appItem, savedItem, tautulliViewFallback);
    return {
      id: element.id,
      name: element.name,
      enable: resolveBoolean(savedItem.enable, overviewVisible),
      dashboard: resolveBoolean(savedItem.dashboard, dashboardVisible),
      overviewVisibilityRole,
      dashboardVisibilityRole,
      favourite: resolveBoolean(savedItem.favourite, false),
      showSubtitle: resolveBoolean(savedItem.showSubtitle, true),
      showMeta: resolveBoolean(savedItem.showMeta, true),
      showPill: resolveBoolean(savedItem.showPill, true),
      showTypeIcon: resolveBoolean(savedItem.showTypeIcon, true),
      showViewIcon: resolveBoolean(savedItem.showViewIcon, true),
      showUsername: resolveBoolean(savedItem.showUsername, true),
      queueShowDetail: resolveBoolean(savedItem.queueShowDetail, true),
      queueShowSubDetail: resolveBoolean(savedItem.queueShowSubDetail, true),
      queueShowSize: resolveBoolean(savedItem.queueShowSize, true),
      queueShowProtocol: resolveBoolean(savedItem.queueShowProtocol, true),
      queueShowTimeLeft: resolveBoolean(savedItem.queueShowTimeLeft, true),
      queueShowProgress: resolveBoolean(savedItem.queueShowProgress, true),
      tautulliStatsView,
      queueDetailLabel: queueLabels.detailLabel,
      queueSubDetailLabel: queueLabels.subDetailLabel,
      queueVisibleRows,
      order: Number.isFinite(orderValue) ? orderValue : index + 1,
    };
  });
  return merged.sort((a, b) => {
    const favouriteDelta = (b.favourite ? 1 : 0) - (a.favourite ? 1 : 0);
    if (favouriteDelta !== 0) return favouriteDelta;
    const orderDelta = a.order - b.order;
    if (orderDelta !== 0) return orderDelta;
    return String(a.name || '').localeCompare(String(b.name || ''));
  });
}

function buildOverviewElementsFromRequest(appItem, body) {
  const elements = getOverviewElements(appItem);
  if (!elements.length) return appItem.overviewElements;
  const existingSettings = new Map(
    mergeOverviewElementSettings(appItem).map((item) => [item.id, item])
  );
  return elements.map((element, index) => {
    const existing = existingSettings.get(element.id) || {};
    const orderValue = body[`element_order_${element.id}`];
    const parsedOrder = Number(orderValue);
    const isQueue = element.id === 'activity-queue';
    const queueRowsRaw = body[`element_queue_visible_rows_${element.id}`];
    const queueVisibleRows = queueRowsRaw === undefined
      ? undefined
      : resolveTableVisibleRows(element.id, queueRowsRaw);
    const tautulliStatsView = resolveTautulliStatsView(appItem, existing, 'list');
    return {
      id: element.id,
      enable: Boolean(body[`element_enable_${element.id}`]),
      dashboard: Boolean(body[`element_dashboard_${element.id}`]),
      overviewVisibilityRole: resolveOverviewElementVisibilityRole(appItem, existing, 'user'),
      dashboardVisibilityRole: resolveDashboardElementVisibilityRole(appItem, existing, 'user'),
      favourite: Boolean(body[`element_favourite_${element.id}`]),
      showSubtitle: Boolean(body[`element_showSubtitle_${element.id}`]),
      showMeta: Boolean(body[`element_showMeta_${element.id}`]),
      showPill: Boolean(body[`element_showPill_${element.id}`]),
      showTypeIcon: Boolean(body[`element_showTypeIcon_${element.id}`]),
      showViewIcon: Boolean(body[`element_showViewIcon_${element.id}`]),
      showUsername: Boolean(body[`element_showUsername_${element.id}`]),
      queueShowDetail: isQueue ? Boolean(body[`element_queue_col_detail_${element.id}`]) : undefined,
      queueShowSubDetail: isQueue ? Boolean(body[`element_queue_col_subdetail_${element.id}`]) : undefined,
      queueShowSize: isQueue ? Boolean(body[`element_queue_col_size_${element.id}`]) : undefined,
      queueShowProtocol: isQueue ? Boolean(body[`element_queue_col_protocol_${element.id}`]) : undefined,
      queueShowTimeLeft: isQueue ? Boolean(body[`element_queue_col_timeLeft_${element.id}`]) : undefined,
      queueShowProgress: isQueue ? Boolean(body[`element_queue_col_progress_${element.id}`]) : undefined,
      tautulliStatsView,
      queueVisibleRows,
      order: Number.isFinite(parsedOrder) ? parsedOrder : index + 1,
    };
  });
}

function buildDashboardElementsFromRequest(appItem, body) {
  const elements = getOverviewElements(appItem);
  if (!elements.length) return appItem.overviewElements;
  const existingSettings = new Map(
    mergeOverviewElementSettings(appItem).map((item) => [item.id, item])
  );
  return elements.map((element, index) => {
    const prefix = `dashboard_${appItem.id}_${element.id}_`;
    const isPresent = Boolean(body[`${prefix}present`]);
    if (!isPresent) {
      const fallback = existingSettings.get(element.id);
      if (fallback) {
        const fallbackOverviewVisibilityRole = resolveOverviewElementVisibilityRole(appItem, fallback, 'user');
        const fallbackVisibilityRole = resolveDashboardElementVisibilityRole(appItem, fallback, 'user');
        const fallbackTautulliStatsView = resolveTautulliStatsView(appItem, fallback, 'list');
        return {
          id: element.id,
          enable: Boolean(fallback.enable),
          dashboard: Boolean(fallback.dashboard),
          overviewVisibilityRole: fallbackOverviewVisibilityRole,
          dashboardVisibilityRole: fallbackVisibilityRole,
          favourite: Boolean(fallback.favourite),
          showSubtitle: Boolean(fallback.showSubtitle),
          showMeta: Boolean(fallback.showMeta),
          showPill: Boolean(fallback.showPill),
          showTypeIcon: Boolean(fallback.showTypeIcon),
          showViewIcon: Boolean(fallback.showViewIcon),
          showUsername: Boolean(fallback.showUsername),
          queueShowDetail: fallback.queueShowDetail !== undefined ? Boolean(fallback.queueShowDetail) : undefined,
          queueShowSubDetail: fallback.queueShowSubDetail !== undefined ? Boolean(fallback.queueShowSubDetail) : undefined,
          queueShowSize: fallback.queueShowSize !== undefined ? Boolean(fallback.queueShowSize) : undefined,
          queueShowProtocol: fallback.queueShowProtocol !== undefined ? Boolean(fallback.queueShowProtocol) : undefined,
          queueShowTimeLeft: fallback.queueShowTimeLeft !== undefined ? Boolean(fallback.queueShowTimeLeft) : undefined,
          queueShowProgress: fallback.queueShowProgress !== undefined ? Boolean(fallback.queueShowProgress) : undefined,
          tautulliStatsView: fallbackTautulliStatsView,
          queueVisibleRows: fallback.queueVisibleRows,
          order: Number.isFinite(fallback.order) ? fallback.order : index + 1,
        };
      }
    }
    const orderValue = body[`${prefix}order`];
    const parsedOrder = Number(orderValue);
    const isQueue = element.id === 'activity-queue' || ['queue', 'users', 'invitations', 'status', 'active-sessions', 'connections', 'routers', 'services'].includes(element.id);
    const overviewVisibilityRole = normalizeVisibilityRole(
      body[`${prefix}overview_visibility_role`],
      resolveOverviewElementVisibilityRole(appItem, existingSettings.get(element.id) || {}, 'user')
    );
    const visibilityRole = normalizeVisibilityRole(
      body[`${prefix}visibility_role`],
      resolveDashboardElementVisibilityRole(appItem, existingSettings.get(element.id) || {}, 'user')
    );
    const overviewVisible = overviewVisibilityRole !== 'disabled';
    const dashboardVisible = visibilityRole !== 'disabled';
    const queueRowsRaw = body[`${prefix}queue_visible_rows`];
    const queueVisibleRows = queueRowsRaw === undefined
      ? undefined
      : resolveTableVisibleRows(element.id, queueRowsRaw);
    const tautulliStatsView = normalizeTautulliStatsView(
      body[`${prefix}tautulli_stats_view`],
      resolveTautulliStatsView(appItem, existingSettings.get(element.id) || {}, 'list')
    );
    return {
      id: element.id,
      enable: overviewVisible,
      dashboard: dashboardVisible,
      overviewVisibilityRole,
      dashboardVisibilityRole: visibilityRole,
      favourite: Boolean(body[`${prefix}favourite`]),
      showSubtitle: Boolean(body[`${prefix}showSubtitle`]),
      showMeta: Boolean(body[`${prefix}showMeta`]),
      showPill: Boolean(body[`${prefix}showPill`]),
      showTypeIcon: Boolean(body[`${prefix}showTypeIcon`]),
      showViewIcon: Boolean(body[`${prefix}showViewIcon`]),
      showUsername: Boolean(body[`${prefix}showUsername`]),
      queueShowDetail: isQueue ? Boolean(body[`${prefix}queue_col_detail`]) : undefined,
      queueShowSubDetail: isQueue ? Boolean(body[`${prefix}queue_col_subdetail`]) : undefined,
      queueShowSize: isQueue ? Boolean(body[`${prefix}queue_col_size`]) : undefined,
      queueShowProtocol: isQueue ? Boolean(body[`${prefix}queue_col_protocol`]) : undefined,
      queueShowTimeLeft: isQueue ? Boolean(body[`${prefix}queue_col_timeLeft`]) : undefined,
      queueShowProgress: isQueue ? Boolean(body[`${prefix}queue_col_progress`]) : undefined,
      tautulliStatsView: resolveTautulliStatsView(appItem, { id: element.id, tautulliStatsView }, 'list'),
      queueVisibleRows,
      order: Number.isFinite(parsedOrder) ? parsedOrder : index + 1,
    };
  });
}


function getTautulliCards(appItem) {
  if (!appItem || appItem.id !== 'tautulli') return [];
  return TAUTULLI_WATCH_CARDS;
}

function mergeTautulliCardSettings(appItem) {
  const cards = getTautulliCards(appItem);
  if (!cards.length) return [];
  const saved = Array.isArray(appItem.tautulliCards) ? appItem.tautulliCards : [];
  const savedMap = new Map(saved.map((item) => [item.id, item]));
  return cards
    .map((card, index) => {
      const savedItem = savedMap.get(card.id) || {};
      const orderValue = Number(savedItem.order);
      return {
        id: card.id,
        name: card.name,
        enable: savedItem.enable === undefined ? true : Boolean(savedItem.enable),
        order: Number.isFinite(orderValue) ? orderValue : index + 1,
      };
    })
    .sort((a, b) => {
      const orderDelta = a.order - b.order;
      if (orderDelta !== 0) return orderDelta;
      return String(a.name || '').localeCompare(String(b.name || ''));
    });
}

function buildTautulliCardsFromRequest(appItem, body) {
  const cards = getTautulliCards(appItem);
  if (!cards.length) return appItem.tautulliCards;
  return cards.map((card, index) => {
    const orderValue = body[`tautulli_card_order_${card.id}`];
    const parsedOrder = Number(orderValue);
    return {
      id: card.id,
      enable: Boolean(body[`tautulli_card_enable_${card.id}`]),
      order: Number.isFinite(parsedOrder) ? parsedOrder : index + 1,
    };
  });
}

function buildTautulliCardsFromDashboardRequest(appItem, body) {
  const cards = getTautulliCards(appItem);
  if (!cards.length) return appItem.tautulliCards;
  const candidates = ['watch-stats', 'watch-stats-wheel'];
  const activePrefix = candidates.find((prefix) =>
    cards.some((card) =>
      body[`dashboard_tautulli_card_enable_${prefix}_${card.id}`] !== undefined
      || body[`dashboard_tautulli_card_order_${prefix}_${card.id}`] !== undefined
    )
  );
  if (!activePrefix) return appItem.tautulliCards;
  return cards.map((card, index) => {
    const orderValue = body[`dashboard_tautulli_card_order_${activePrefix}_${card.id}`];
    const parsedOrder = Number(orderValue);
    return {
      id: card.id,
      enable: Boolean(body[`dashboard_tautulli_card_enable_${activePrefix}_${card.id}`]),
      order: Number.isFinite(parsedOrder) ? parsedOrder : index + 1,
    };
  });
}


function resolveLaunchUrl(appItem, req) {
  const localUrl = appItem.localUrl || appItem.url || '';
  const remoteUrl = appItem.remoteUrl || appItem.url || '';
  const host = getRequestHost(req);
  const clientIp = req.ip || '';
  const hostIsLocal = host ? isLocalHost(host) : false;
  const ipIsLocal = isPrivateIp(clientIp);
  const isLocal = host ? hostIsLocal : ipIsLocal;

  if (isLocal) return localUrl || remoteUrl;
  return remoteUrl || localUrl;
}

function resolveIframeLaunchUrl(req, launchUrl) {
  return String(launchUrl || '').trim();
}

async function resolveDeepLaunchUrl(appItem, req, options = {}) {
  const query = String(options.query || '').trim();
  const imdbId = String(options.imdbId || '').trim();
  const tmdbId = String(options.tmdbId || '').trim();
  const mediaType = String(options.mediaType || '').trim().toLowerCase();
  const plexToken = String(options.plexToken || '').trim();
  const effectiveQuery = query || imdbId || tmdbId;
  if (!effectiveQuery) return '';
  const launchUrl = String(resolveLaunchUrl(appItem, req) || '').trim();
  if (!launchUrl) return '';

  if (appItem?.id === 'tautulli') {
    const base = normalizeBaseUrl(launchUrl);
    const apiKey = String(appItem.apiKey || '').trim();
    const ratingKey = await resolveTautulliRatingKey({ base, apiKey, query: effectiveQuery, imdbId, tmdbId, mediaType });
    if (ratingKey) {
      return base.replace(/\/+$/, '') + '/info?rating_key=' + encodeURIComponent(ratingKey);
    }
    return base.replace(/\/+$/, '') + '/search?query=' + encodeURIComponent(effectiveQuery);
  }

  if (appItem?.id === 'plex') {
    let url = launchUrl;
    if (!/^https?:\/\//i.test(url)) url = 'http://' + url;
    try {
      const parsed = new URL(url);
      const origin = parsed.origin;
      const pathName = String(parsed.pathname || '').replace(/\/+$/, '');
      const apiBase = normalizeBaseUrl(url, { stripWeb: true }) || origin;
      const ratingKey = await resolvePlexRatingKey({
        baseUrl: apiBase,
        token: plexToken,
        query: effectiveQuery,
        imdbId,
        tmdbId,
      });
      const detailsKey = ratingKey ? '/library/metadata/' + ratingKey : '';
      const machineId = detailsKey
        ? await resolvePlexMachineIdentifier({ baseUrl: apiBase, token: plexToken })
        : '';
      const detailsHash = detailsKey
        ? (machineId
          ? ('#!/server/' + encodeURIComponent(machineId) + '/details?key=' + encodeURIComponent(detailsKey))
          : ('#!/details?key=' + encodeURIComponent(detailsKey)))
        : '';
      if (/\/web\/index\.html$/i.test(pathName)) {
        return detailsHash
          ? (origin + pathName + detailsHash)
          : (origin + pathName + '#!/search?query=' + encodeURIComponent(effectiveQuery));
      }
      if (/\/web$/i.test(pathName)) {
        return detailsHash
          ? (origin + pathName + '/index.html' + detailsHash)
          : (origin + pathName + '/index.html#!/search?query=' + encodeURIComponent(effectiveQuery));
      }
      const uiBase = apiBase.replace(/\/+$/, '');
      return detailsHash
        ? (uiBase + '/web/index.html' + detailsHash)
        : (uiBase + '/web/index.html#!/search?query=' + encodeURIComponent(effectiveQuery));
    } catch (err) {
      return launchUrl;
    }
  }

  return launchUrl;
}

async function resolveTautulliRatingKey({ base, apiKey, query, imdbId, tmdbId, mediaType }) {
  if (!base || !apiKey) return '';
  const hasIds = Boolean(imdbId || tmdbId);
  const searchTerms = [];
  if (imdbId) searchTerms.push(imdbId);
  if (tmdbId) searchTerms.push(tmdbId);
  if (!hasIds && query) searchTerms.push(query);
  if (!searchTerms.length) return '';
  try {
    for (let termIndex = 0; termIndex < searchTerms.length; termIndex += 1) {
      const term = String(searchTerms[termIndex] || '').trim();
      if (!term) continue;
      const url = buildAppApiUrl(base, 'api/v2');
      url.searchParams.set('apikey', apiKey);
      url.searchParams.set('cmd', 'search');
      url.searchParams.set('query', term);
      url.searchParams.set('limit', '20');
      const response = await fetch(url.toString(), { headers: { Accept: 'application/json' } });
      if (!response.ok) continue;
      const payload = await response.json().catch(() => ({}));
      const responseData = payload?.response?.data || {};
      const directRows = Array.isArray(responseData) ? responseData : [];
      const resultLists = responseData && typeof responseData.results_list === 'object'
        ? Object.values(responseData.results_list).flatMap((value) => (Array.isArray(value) ? value : []))
        : [];
      const data = directRows.concat(resultLists);
      if (!data.length) continue;
      const normalizedType = mediaType === 'show' ? 'tv' : mediaType;
      const direct = data.find((row) => {
        const guid = String(row?.guid || '').toLowerCase();
        const guids = Array.isArray(row?.guids) ? row.guids.map((g) => String(g || '').toLowerCase()) : [];
        const guidText = [guid].concat(guids).join(' ');
        if (imdbId && guid.includes(String(imdbId).toLowerCase())) return true;
        if (imdbId && guidText.includes(String(imdbId).toLowerCase())) return true;
        if (tmdbId && (guid.includes('tmdb://' + String(tmdbId).toLowerCase()) || guidText.includes(String(tmdbId).toLowerCase()))) return true;
        return false;
      }) || (!hasIds && data.find((row) => {
        if (!normalizedType) return true;
        const rowType = String(row?.media_type || row?.mediaType || '').toLowerCase();
        if (!rowType) return true;
        if (normalizedType === 'tv') return rowType.includes('show') || rowType.includes('episode');
        return rowType.includes(normalizedType);
      })) || (!hasIds ? data[0] : null);
      const ratingKey = String(direct?.rating_key || direct?.grandparent_rating_key || '').trim();
      if (ratingKey) return ratingKey;
    }
    return '';
  } catch (err) {
    return '';
  }
}

async function resolvePlexRatingKey({ baseUrl, token, query, imdbId, tmdbId }) {
  if (!baseUrl || !token) return '';
  const hasIds = Boolean(imdbId || tmdbId);
  const searchTerms = [];
  if (imdbId) searchTerms.push(imdbId);
  if (tmdbId) searchTerms.push(tmdbId);
  if (!hasIds && query) searchTerms.push(query);
  if (!searchTerms.length) return '';
  try {
    for (let termIndex = 0; termIndex < searchTerms.length; termIndex += 1) {
      const term = String(searchTerms[termIndex] || '').trim();
      if (!term) continue;
      const url = buildAppApiUrl(baseUrl, 'search');
      url.searchParams.set('query', term);
      url.searchParams.set('X-Plex-Token', token);
      const response = await fetch(url.toString(), { headers: { Accept: 'application/xml' } });
      if (!response.ok) continue;
      const xml = await response.text();
      const nodes = parsePlexSearchNodes(xml);
      if (!nodes.length) continue;
      const normalizedQuery = String(term || '').trim().toLowerCase();
      if (imdbId || tmdbId) {
        const normalizedImdb = String(imdbId || '').toLowerCase();
        const normalizedTmdb = String(tmdbId || '').toLowerCase();
        for (let index = 0; index < nodes.length; index += 1) {
          const node = nodes[index];
          const guidText = node.guids.join(' ');
          if (normalizedImdb && guidText.includes(normalizedImdb)) return node.ratingKey;
          if (normalizedTmdb && guidText.includes(normalizedTmdb)) return node.ratingKey;
        }
      }
      if (!hasIds) {
        const exactTitle = nodes.find((node) => String(node.title || '').toLowerCase() === normalizedQuery);
        if (exactTitle?.ratingKey) return exactTitle.ratingKey;
        if (nodes[0]?.ratingKey) return String(nodes[0].ratingKey).trim();
      }
    }
    return '';
  } catch (err) {
    return '';
  }
}

function parsePlexSearchNodes(xmlText) {
  const xml = String(xmlText || '');
  const nodes = [];
  const pattern = /<(Video|Directory|Track)\b([^>]*)>([\s\S]*?)<\/\1>/gi;
  let match = pattern.exec(xml);
  while (match) {
    const attrs = String(match[2] || '');
    const body = String(match[3] || '');
    const ratingKeyMatch = attrs.match(/\bratingKey="([^"]+)"/i);
    const titleMatch = attrs.match(/\btitle="([^"]+)"/i)
      || attrs.match(/\bgrandparentTitle="([^"]+)"/i)
      || attrs.match(/\bparentTitle="([^"]+)"/i);
    const ratingKey = String(ratingKeyMatch?.[1] || '').trim();
    if (ratingKey) {
      const guids = Array.from(body.matchAll(/<Guid\b[^>]*\bid="([^"]+)"/gi))
        .map((guidMatch) => String(guidMatch?.[1] || '').toLowerCase())
        .filter(Boolean);
      nodes.push({
        ratingKey,
        title: String(titleMatch?.[1] || '').trim(),
        guids,
      });
    }
    match = pattern.exec(xml);
  }
  return nodes;
}

async function resolvePlexMachineIdentifier({ baseUrl, token }) {
  if (!baseUrl || !token) return '';
  try {
    const url = buildAppApiUrl(baseUrl, 'identity');
    url.searchParams.set('X-Plex-Token', token);
    const response = await fetch(url.toString(), { headers: { Accept: 'application/xml' } });
    if (!response.ok) return '';
    const xml = await response.text();
    const match = xml.match(/\bmachineIdentifier="([^"]+)"/i);
    return String(match?.[1] || '').trim();
  } catch (err) {
    return '';
  }
}

function normalizeBasePath(value) {
  let raw = String(value || '').trim();
  if (!raw) return '';
  if (/^https?:\/\//i.test(raw)) {
    try {
      raw = String(new URL(raw).pathname || '').trim();
    } catch (err) {
      raw = '';
    }
  }
  if (!raw) return '';
  let pathValue = raw.replace(/[#?].*$/, '').replace(/\/{2,}/g, '/').trim();
  if (!pathValue) return '';
  if (!pathValue.startsWith('/')) pathValue = `/${pathValue}`;
  pathValue = pathValue.replace(/\/+$/, '');
  return pathValue === '/' ? '' : pathValue;
}

function normalizeBaseUrl(value, options = {}) {
  const stripWeb = Boolean(options?.stripWeb);
  let url = String(value || '').trim();
  if (!url) return '';
  if (!/^https?:\/\//i.test(url)) url = 'http://' + url;
  try {
    const parsed = new URL(url);
    let pathname = String(parsed.pathname || '');
    if (stripWeb) {
      pathname = pathname
        .replace(/\/web\/index\.html$/i, '')
        .replace(/\/web\/?$/i, '');
    }
    pathname = pathname.replace(/\/+$/, '');
    parsed.pathname = pathname || '/';
    parsed.search = '';
    parsed.hash = '';
    return parsed.toString().replace(/\/$/, '');
  } catch (err) {
    return url.replace(/\/+$/, '');
  }
}

function applyConfiguredBasePath(baseUrl, basePath) {
  const normalizedBase = normalizeBaseUrl(baseUrl || '');
  const normalizedPath = normalizeBasePath(basePath || '');
  if (!normalizedBase) return '';
  if (!normalizedPath) return normalizedBase;
  try {
    const parsed = new URL(normalizedBase);
    const currentPath = normalizeBasePath(parsed.pathname || '');
    if (currentPath && (currentPath === normalizedPath || currentPath.endsWith(normalizedPath))) {
      return normalizedBase;
    }
    const joined = currentPath ? `${currentPath}${normalizedPath}` : normalizedPath;
    parsed.pathname = joined;
    parsed.search = '';
    parsed.hash = '';
    return parsed.toString().replace(/\/$/, '');
  } catch (err) {
    return normalizedBase;
  }
}

function buildAppBaseUrls(apps, req) {
  return (apps || []).reduce((acc, appItem) => {
    if (!appItem?.id) return acc;
    acc[appItem.id] = resolveLaunchUrl(appItem, req);
    return acc;
  }, {});
}

function normalizeLaunchMode(value, fallback = 'new-tab') {
  const allowed = new Set(['disabled', 'new-tab', 'iframe']);
  const next = String(value || '').toLowerCase();
  if (allowed.has(next)) return next;
  return fallback;
}

function resolveAppLaunchMode(appItem, menu) {
  const configured = normalizeLaunchMode(appItem?.launchMode, '');
  if (configured) return configured;
  const accessMenu = menu || normalizeMenu(appItem);
  const launchRoles = normalizeMenuVisibilityRoles(
    accessMenu?.launch?.visibilityRoles,
    accessMenu?.launch?.minRole || 'disabled'
  );
  return launchRoles.length ? 'new-tab' : 'disabled';
}

function resolveEffectiveLaunchMode(appItem, req, menu) {
  const configured = resolveAppLaunchMode(appItem, menu);
  if (shouldForceLocalNewTab(appItem, configured, req)) return 'new-tab';
  return configured;
}

function shouldForceLocalNewTab(appItem, mode, req) {
  const normalizedMode = normalizeLaunchMode(mode, '');
  if (normalizedMode !== 'iframe') return false;
  const baseId = getAppBaseId(appItem?.id);
  if (baseId === 'agregarr' || baseId === 'sortarr') return true;
  if (baseId !== 'tautulli') return false;
  const requestHost = getRequestHost(req);
  let targetHost = '';
  try {
    targetHost = new URL(resolveLaunchUrl(appItem, req)).hostname || '';
  } catch (err) {
    targetHost = '';
  }
  if (isLocalHost(requestHost)) return true;
  if (isLocalHost(targetHost)) return true;
  return false;
}

function getRequestHost(req) {
  if (!req) return '';
  const forwardedHost = String(req.headers?.['x-forwarded-host'] || '').split(',')[0].trim();
  const host = forwardedHost || req.get('host') || req.hostname || '';
  return String(host || '').trim();
}

function getRequestProto(req) {
  if (!req) return '';
  const forwardedProto = String(req.headers?.['x-forwarded-proto'] || '').split(',')[0].trim();
  if (forwardedProto) return forwardedProto;
  const proto = String(req.protocol || '').trim();
  if (proto) return proto;
  return isSecureEnv() ? 'https' : 'http';
}

function resolvePublicBaseUrl(req) {
  const config = loadConfig();
  const generalSettings = resolveGeneralSettings(config);
  const configuredBasePath = normalizeBasePath(generalSettings.basePath || '');
  const host = getRequestHost(req);
  if (generalSettings.localUrl && isLocalHost(host)) {
    const configuredLocal = normalizeBaseUrl(generalSettings.localUrl || '');
    if (configuredLocal) return applyConfiguredBasePath(configuredLocal, configuredBasePath);
  }
  const configured = normalizeBaseUrl(generalSettings.remoteUrl || '');
  if (configured) return applyConfiguredBasePath(configured, configuredBasePath);
  const proto = getRequestProto(req);
  if (host) return applyConfiguredBasePath(normalizeBaseUrl(`${proto}://${host}`), configuredBasePath);
  return applyConfiguredBasePath(normalizeBaseUrl(BASE_URL) || BASE_URL, configuredBasePath);
}

function isLocalHost(host) {
  if (!host) return false;
  const raw = String(host).trim().toLowerCase();
  const unwrapped = raw.startsWith('[') ? raw.slice(1, raw.indexOf(']')) : raw;
  const withoutPort = unwrapped.includes(':') && !unwrapped.includes('::')
    ? unwrapped.split(':')[0]
    : unwrapped;
  if (!withoutPort) return false;
  if (withoutPort === 'localhost' || withoutPort === '::1' || withoutPort.endsWith('.local')) return true;
  if (isPrivateIp(withoutPort)) return true;
  if (!withoutPort.includes('.')) return true;
  return false;
}

function isPrivateIp(ip) {
  if (!ip) return false;
  const normalized = ip.replace(/^::ffff:/, '');
  if (normalized === '127.0.0.1' || normalized === '::1') return true;
  if (normalized.startsWith('10.')) return true;
  if (normalized.startsWith('192.168.')) return true;
  const parts = normalized.split('.').map((part) => Number(part));
  if (parts.length === 4 && parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) return true;
  return false;
}

function getArrCombineSection(sectionKey) {
  const key = String(sectionKey || '').trim();
  if (!key) return null;
  return ARR_COMBINE_SECTIONS.find((section) => section.key === key) || null;
}

function getArrCombineSectionByElementId(elementId) {
  const id = String(elementId || '').trim();
  if (!id) return null;
  return ARR_COMBINE_SECTIONS.find((section) => section.elementId === id) || null;
}

function getArrCombineSectionLabel(sectionKey) {
  const section = getArrCombineSection(sectionKey);
  if (!section) return 'Combined';
  if (section.key === 'downloadingSoon') return 'Downloading Soon';
  if (section.key === 'recentlyDownloaded') return 'Recently Downloaded';
  if (section.key === 'activityQueue') return 'Activity Queue';
  if (section.key === 'calendar') return 'Calendar';
  return String(section.elementId || section.key || 'Combined');
}

function getArrCombineSectionIconPath(sectionKey) {
  const section = getArrCombineSection(sectionKey);
  if (!section) return '/icons/arr-suite.svg';
  if (section.key === 'downloadingSoon') return '/icons/downloading-soon.svg';
  if (section.key === 'recentlyDownloaded') return '/icons/recently-added.svg';
  if (section.key === 'activityQueue') return '/icons/activity-queue.svg';
  if (section.key === 'calendar') return '/icons/calendar-white.svg';
  return '/icons/arr-suite.svg';
}

function getDownloaderCombineSection(sectionKey) {
  const key = String(sectionKey || '').trim();
  if (!key) return null;
  return DOWNLOADER_COMBINE_SECTIONS.find((section) => section.key === key) || null;
}

function getDownloaderCombineSectionByElementId(elementId) {
  const id = String(elementId || '').trim();
  if (!id) return null;
  return DOWNLOADER_COMBINE_SECTIONS.find((section) => section.elementId === id) || null;
}

function getDownloaderCombineSectionLabel(sectionKey) {
  const section = getDownloaderCombineSection(sectionKey);
  if (!section) return 'Download Queue';
  if (section.key === 'activityQueue') return 'Download Queue';
  return String(section.elementId || section.key || 'Download Queue');
}

function getDownloaderCombineSectionIconPath(sectionKey) {
  const section = getDownloaderCombineSection(sectionKey);
  if (!section) return '/icons/download.svg';
  if (section.key === 'activityQueue') return '/icons/download.svg';
  return '/icons/download.svg';
}

function getMediaCombineSection(sectionKey) {
  const key = String(sectionKey || '').trim();
  if (!key) return null;
  return MEDIA_COMBINE_SECTIONS.find((section) => section.key === key) || null;
}

function getMediaCombineSectionByElementId(elementId) {
  const id = String(elementId || '').trim();
  if (!id) return null;
  return MEDIA_COMBINE_SECTIONS.find((section) => section.elementId === id) || null;
}

function getMediaCombineSectionLabel(sectionKey) {
  const section = getMediaCombineSection(sectionKey);
  if (!section) return 'Media';
  if (section.key === 'recent') return 'Recently Added';
  return 'Active Streams';
}

function getMediaCombineSectionIconPath(sectionKey) {
  const section = getMediaCombineSection(sectionKey);
  if (!section) return '/icons/media-play.svg';
  if (section.key === 'recent') return '/icons/recently-added.svg';
  return '/icons/media-play.svg';
}

function resolveDeprecatedDashboardCardDescriptor(key) {
  const value = String(key || '').trim();
  if (!value) return null;
  const combinedMatch = value.match(/^combined:(arr|downloader|media):(.+)$/);
  if (combinedMatch) {
    const type = String(combinedMatch[1] || '').trim();
    const sectionKey = String(combinedMatch[2] || '').trim();
    if (type === 'arr' && getArrCombineSection(sectionKey)) {
      return { kind: 'combined', type, sectionKey };
    }
    if (type === 'downloader' && getDownloaderCombineSection(sectionKey)) {
      return { kind: 'combined', type, sectionKey };
    }
    if (type === 'media' && getMediaCombineSection(sectionKey)) {
      return { kind: 'combined', type, sectionKey };
    }
    return null;
  }

  const appMatch = value.match(/^app:([^:]+):(.+)$/);
  if (!appMatch) return null;
  const appId = normalizeAppId(appMatch[1]);
  const elementId = String(appMatch[2] || '').trim();
  if (!appId || !elementId) return null;

  const arrSection = getArrCombineSectionByElementId(elementId);
  if (arrSection && isAppInSet(appId, ARR_APP_IDS)) {
    return { kind: 'app', type: 'arr', sectionKey: arrSection.key, appId, elementId };
  }
  const downloaderSection = getDownloaderCombineSectionByElementId(elementId);
  if (downloaderSection && isAppInSet(appId, DOWNLOADER_APP_IDS)) {
    return { kind: 'app', type: 'downloader', sectionKey: downloaderSection.key, appId, elementId };
  }
  const mediaSection = getMediaCombineSectionByElementId(elementId);
  if (mediaSection && isAppInSet(appId, MEDIA_APP_IDS)) {
    return { kind: 'app', type: 'media', sectionKey: mediaSection.key, appId, elementId };
  }
  return null;
}

function resolveDeprecatedDashboardElementIdsForApp(appId) {
  const normalizedAppId = normalizeAppId(appId);
  if (!normalizedAppId) return [];
  const deprecatedIds = [];
  if (isAppInSet(normalizedAppId, ARR_APP_IDS)) {
    deprecatedIds.push(...ARR_COMBINE_SECTIONS.map((section) => String(section?.elementId || '').trim()).filter(Boolean));
  }
  if (isAppInSet(normalizedAppId, DOWNLOADER_APP_IDS)) {
    deprecatedIds.push(...DOWNLOADER_COMBINE_SECTIONS.map((section) => String(section?.elementId || '').trim()).filter(Boolean));
  }
  if (isAppInSet(normalizedAppId, MEDIA_APP_IDS)) {
    deprecatedIds.push(...MEDIA_COMBINE_SECTIONS.map((section) => String(section?.elementId || '').trim()).filter(Boolean));
  }
  return [...new Set(deprecatedIds)];
}

function normalizeCombinedCardToken(value) {
  const normalized = String(value || '').trim().toLowerCase().replace(/[^a-z0-9_-]+/g, '-').replace(/-+/g, '-').replace(/^[-_]+|[-_]+$/g, '');
  return normalized;
}

function buildCombinedCardId() {
  return `card-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function resolveArrDashboardCombinedCards(config, apps) {
  if (!ENABLE_ARR_UNIFIED_CARDS) return [];
  const configured = Array.isArray(config?.arrDashboardCombinedCards) ? config.arrDashboardCombinedCards : [];
  const allowedAppIds = [
    ...new Set(
      (Array.isArray(apps) ? apps : [])
        .filter((appItem) => !appItem?.removed)
        .map((appItem) => normalizeAppId(appItem?.id))
        .filter((id) => isAppInSet(id, ARR_APP_IDS))
    ),
  ];
  const allowedAppIdSet = new Set(allowedAppIds);
  const cards = [];
  const seen = new Set();
  configured.forEach((entry) => {
    const baseId = normalizeCombinedCardToken(entry?.id || '');
    if (!baseId) return;
    const sectionKey = String(entry?.sectionKey || '').trim();
    if (!getArrCombineSection(sectionKey)) return;
    const rawAppIds = Array.isArray(entry?.appIds) ? entry.appIds : [entry?.appIds];
    const selectedAppIds = [...new Set(
      rawAppIds
        .map((appId) => normalizeAppId(appId))
        .filter((appId) => allowedAppIdSet.has(appId))
    )];
    const appIds = selectedAppIds.length ? selectedAppIds : [...allowedAppIds];
    if (!appIds.length) return;
    let nextId = baseId;
    if (seen.has(nextId)) {
      let suffix = 2;
      while (seen.has(`${baseId}-${suffix}`)) suffix += 1;
      nextId = `${baseId}-${suffix}`;
    }
    seen.add(nextId);
    cards.push({
      id: nextId,
      sectionKey,
      appIds,
    });
  });
  return cards;
}

function resolveMediaDashboardCards(config, apps) {
  const configured = Array.isArray(config?.mediaDashboardCards) ? config.mediaDashboardCards : [];
  const allowedAppIds = [
    ...new Set(
      (Array.isArray(apps) ? apps : [])
        .filter((appItem) => !appItem?.removed)
        .map((appItem) => normalizeAppId(appItem?.id))
        .filter((id) => isAppInSet(id, MEDIA_APP_IDS))
    ),
  ];
  const allowedAppIdSet = new Set(allowedAppIds);
  const cards = [];
  const seen = new Set();
  configured.forEach((entry) => {
    const baseId = normalizeCombinedCardToken(entry?.id || '');
    if (!baseId) return;
    const sectionKey = String(entry?.sectionKey || '').trim();
    if (!getMediaCombineSection(sectionKey)) return;
    const hasExplicitAppIds = Object.prototype.hasOwnProperty.call(entry || {}, 'appIds');
    const rawAppIds = hasExplicitAppIds
      ? (Array.isArray(entry?.appIds) ? entry.appIds : [entry?.appIds])
      : [...allowedAppIds];
    const selectedAppIds = [...new Set(
      rawAppIds
        .map((appId) => normalizeAppId(appId))
        .filter((appId) => allowedAppIdSet.has(appId))
    )];
    const appIds = hasExplicitAppIds ? selectedAppIds : [...allowedAppIds];
    if (!hasExplicitAppIds && !appIds.length) return;
    let nextId = baseId;
    if (seen.has(nextId)) {
      let suffix = 2;
      while (seen.has(`${baseId}-${suffix}`)) suffix += 1;
      nextId = `${baseId}-${suffix}`;
    }
    seen.add(nextId);
    cards.push({
      id: nextId,
      sectionKey,
      appIds,
    });
  });
  return cards;
}

function resolveDownloaderDashboardCards(config, apps) {
  if (!ENABLE_DOWNLOADER_UNIFIED_CARDS) return [];
  const configured = Array.isArray(config?.downloaderDashboardCards) ? config.downloaderDashboardCards : [];
  const allowedAppIds = [
    ...new Set(
      (Array.isArray(apps) ? apps : [])
        .filter((appItem) => !appItem?.removed)
        .map((appItem) => normalizeAppId(appItem?.id))
        .filter((id) => isAppInSet(id, DOWNLOADER_APP_IDS))
    ),
  ];
  const allowedAppIdSet = new Set(allowedAppIds);
  const cards = [];
  const seen = new Set();
  configured.forEach((entry) => {
    const baseId = normalizeCombinedCardToken(entry?.id || '');
    if (!baseId) return;
    const sectionKey = String(entry?.sectionKey || '').trim();
    if (!getDownloaderCombineSection(sectionKey)) return;
    const hasExplicitAppIds = Object.prototype.hasOwnProperty.call(entry || {}, 'appIds');
    const rawAppIds = hasExplicitAppIds
      ? (Array.isArray(entry?.appIds) ? entry.appIds : [entry?.appIds])
      : [...allowedAppIds];
    const selectedAppIds = [...new Set(
      rawAppIds
        .map((appId) => normalizeAppId(appId))
        .filter((appId) => allowedAppIdSet.has(appId))
    )];
    const appIds = hasExplicitAppIds ? selectedAppIds : [...allowedAppIds];
    if (!hasExplicitAppIds && !appIds.length) return;
    let nextId = baseId;
    if (seen.has(nextId)) {
      let suffix = 2;
      while (seen.has(`${baseId}-${suffix}`)) suffix += 1;
      nextId = `${baseId}-${suffix}`;
    }
    seen.add(nextId);
    cards.push({
      id: nextId,
      sectionKey,
      appIds,
    });
  });
  return cards;
}


function migrateDeprecatedDashboardCards(config) {
  const source = config && typeof config === 'object' ? config : {};
  const sourceApps = Array.isArray(source.apps) ? source.apps : [];
  const apps = sourceApps.map((appItem) => ({
    ...(appItem || {}),
    overviewElements: Array.isArray(appItem?.overviewElements)
      ? appItem.overviewElements.map((entry) => ({ ...(entry || {}) }))
      : [],
  }));
  const dashboardRemovedElements = (source.dashboardRemovedElements && typeof source.dashboardRemovedElements === 'object')
    ? { ...source.dashboardRemovedElements }
    : {};
  const dashboardCombinedSettings = (source.dashboardCombinedSettings && typeof source.dashboardCombinedSettings === 'object')
    ? { ...source.dashboardCombinedSettings }
    : {};
  const dashboardCombinedOrder = (source.dashboardCombinedOrder && typeof source.dashboardCombinedOrder === 'object')
    ? { ...source.dashboardCombinedOrder }
    : {};
  const arrDashboardCombinedCards = resolveArrDashboardCombinedCards(source, apps).map((card) => ({ ...card, appIds: Array.isArray(card?.appIds) ? [...card.appIds] : [] }));
  const mediaDashboardCards = resolveMediaDashboardCards(source, apps).map((card) => ({ ...card, appIds: Array.isArray(card?.appIds) ? [...card.appIds] : [] }));
  const downloaderDashboardCards = resolveDownloaderDashboardCards(source, apps).map((card) => ({ ...card, appIds: Array.isArray(card?.appIds) ? [...card.appIds] : [] }));
  const arrLegacySelection = resolveArrDashboardCombineSettings(source, apps);
  const mediaLegacySelection = resolveMediaDashboardCombineSettings(source, apps);
  const downloaderLegacySelection = resolveDownloaderDashboardCombineSettings(source, apps);

  const allowedByType = {
    arr: [...new Set(
      apps
        .filter((appItem) => !appItem?.removed && isAppInSet(appItem?.id, ARR_APP_IDS))
        .map((appItem) => normalizeAppId(appItem?.id))
        .filter(Boolean)
    )],
    media: [...new Set(
      apps
        .filter((appItem) => !appItem?.removed && isAppInSet(appItem?.id, MEDIA_APP_IDS))
        .map((appItem) => normalizeAppId(appItem?.id))
        .filter(Boolean)
    )],
    downloader: [...new Set(
      apps
        .filter((appItem) => !appItem?.removed && isAppInSet(appItem?.id, DOWNLOADER_APP_IDS))
        .map((appItem) => normalizeAppId(appItem?.id))
        .filter(Boolean)
    )],
  };
  const allowedSetByType = {
    arr: new Set(allowedByType.arr),
    media: new Set(allowedByType.media),
    downloader: new Set(allowedByType.downloader),
  };
  const cardsByType = {
    arr: arrDashboardCombinedCards,
    media: mediaDashboardCards,
    downloader: downloaderDashboardCards,
  };
  const supportsType = {
    arr: ENABLE_ARR_UNIFIED_CARDS,
    media: true,
    downloader: ENABLE_DOWNLOADER_UNIFIED_CARDS,
  };
  let changed = false;
  const markChanged = () => {
    changed = true;
  };

  const makeSignature = (sectionKey, appIds) => {
    const uniqueSorted = [...new Set((Array.isArray(appIds) ? appIds : []).map((id) => normalizeAppId(id)).filter(Boolean))].sort();
    return `${String(sectionKey || '').trim()}|${uniqueSorted.join(',')}`;
  };

  const cardStateByType = {
    arr: { signatures: new Map(), ids: new Set() },
    media: { signatures: new Map(), ids: new Set() },
    downloader: { signatures: new Map(), ids: new Set() },
  };
  Object.entries(cardsByType).forEach(([type, cards]) => {
    cards.forEach((card, index) => {
      const cardId = normalizeCombinedCardToken(card?.id || '') || `card-${index + 1}`;
      cardStateByType[type].ids.add(cardId);
      cardStateByType[type].signatures.set(makeSignature(card?.sectionKey, card?.appIds), cardId);
    });
  });

  const ensureReplacement = ({ type, sectionKey, appIds, hasExplicitSelection, visibilityRole, order }) => {
    if (!supportsType[type]) return '';
    if (!sectionKey) return '';
    const allowedSet = allowedSetByType[type];
    const allowedList = allowedByType[type] || [];
    if (!allowedSet || !Array.isArray(allowedList)) return '';
    let selected = [...new Set(
      (Array.isArray(appIds) ? appIds : [])
        .map((id) => normalizeAppId(id))
        .filter((id) => allowedSet.has(id))
    )];
    if (type === 'arr' && !selected.length) selected = [...allowedList];
    if ((type === 'media' || type === 'downloader') && !hasExplicitSelection) {
      selected = [...allowedList];
    }
    if (type === 'arr' && !selected.length) return '';

    const signature = makeSignature(sectionKey, selected);
    let cardId = cardStateByType[type].signatures.get(signature) || '';
    if (!cardId) {
      cardId = normalizeCombinedCardToken(buildCombinedCardId()) || `card-${Date.now().toString(36)}`;
      while (cardStateByType[type].ids.has(cardId)) {
        cardId = `${cardId}-${Math.random().toString(36).slice(2, 6)}`;
      }
      cardStateByType[type].ids.add(cardId);
      cardStateByType[type].signatures.set(signature, cardId);
      cardsByType[type].push({
        id: cardId,
        sectionKey,
        appIds: selected,
      });
      markChanged();
    }

    const combinedKey = `combined:${type}custom:${cardId}`;
    const normalizedVisibilityRole = normalizeVisibilityRole(visibilityRole, 'user');
    const existingSettings = dashboardCombinedSettings[combinedKey] && typeof dashboardCombinedSettings[combinedKey] === 'object'
      ? dashboardCombinedSettings[combinedKey]
      : {};
    const nextSettings = {
      ...existingSettings,
      visibilityRole: normalizedVisibilityRole,
      enable: normalizedVisibilityRole !== 'disabled',
      dashboard: normalizedVisibilityRole !== 'disabled',
    };
    if (!deepEqual(existingSettings, nextSettings)) {
      dashboardCombinedSettings[combinedKey] = nextSettings;
      markChanged();
    }
    const parsedOrder = Number(order);
    if (Number.isFinite(parsedOrder) && dashboardCombinedOrder[combinedKey] !== parsedOrder) {
      dashboardCombinedOrder[combinedKey] = parsedOrder;
      markChanged();
    }
    if (dashboardRemovedElements[combinedKey]) {
      delete dashboardRemovedElements[combinedKey];
      markChanged();
    }
    return combinedKey;
  };

  apps.forEach((appItem) => {
    if (appItem?.removed) return;
    const normalizedAppId = normalizeAppId(appItem?.id);
    if (!normalizedAppId) return;
    let nextOverviewElements = Array.isArray(appItem?.overviewElements)
      ? appItem.overviewElements.map((entry) => ({ ...(entry || {}) }))
      : [];

    if (normalizedAppId === 'tautulli') {
      const watchStatsKey = `app:${appItem.id}:watch-stats`;
      const wheelKey = `app:${appItem.id}:watch-stats-wheel`;
      const watchIndex = nextOverviewElements.findIndex((element) => String(element?.id || '').trim() === 'watch-stats');
      const wheelIndex = nextOverviewElements.findIndex((element) => String(element?.id || '').trim() === 'watch-stats-wheel');
      const hadWatchStats = watchIndex >= 0;
      const wheelRemoved = Boolean(dashboardRemovedElements[wheelKey]);
      if (wheelIndex >= 0) {
        const watchStatsElement = watchIndex >= 0 ? nextOverviewElements[watchIndex] : null;
        const wheelElement = nextOverviewElements[wheelIndex] || {};
        const watchOverviewRole = resolveOverviewElementVisibilityRole(appItem, watchStatsElement || {}, 'disabled');
        const watchDashboardRole = resolveDashboardElementVisibilityRole(appItem, watchStatsElement || {}, 'disabled');
        const wheelOverviewRole = resolveOverviewElementVisibilityRole(appItem, wheelElement, 'disabled');
        const wheelDashboardRole = resolveDashboardElementVisibilityRole(appItem, wheelElement, 'disabled');
        const preferWheelView = !watchStatsElement
          || (
            !Object.prototype.hasOwnProperty.call(watchStatsElement, 'tautulliStatsView')
            && (
              (watchOverviewRole === 'disabled' && wheelOverviewRole !== 'disabled')
              || (watchDashboardRole === 'disabled' && wheelDashboardRole !== 'disabled')
            )
          );
        const mergedWatchStats = {
          ...(watchStatsElement || wheelElement || {}),
          id: 'watch-stats',
          tautulliStatsView: normalizeTautulliStatsView(
            watchStatsElement?.tautulliStatsView,
            preferWheelView ? 'wheel' : 'list'
          ),
        };
        if (watchOverviewRole === 'disabled' && wheelOverviewRole !== 'disabled') {
          mergedWatchStats.enable = true;
          mergedWatchStats.overviewVisibilityRole = wheelOverviewRole;
        }
        if (watchDashboardRole === 'disabled' && wheelDashboardRole !== 'disabled') {
          mergedWatchStats.dashboard = true;
          mergedWatchStats.dashboardVisibilityRole = wheelDashboardRole;
          const wheelOrder = Number(wheelElement?.order);
          if (Number.isFinite(wheelOrder)) mergedWatchStats.order = wheelOrder;
        }
        const displayFields = [
          'favourite',
          'showSubtitle',
          'showMeta',
          'showPill',
          'showTypeIcon',
          'showViewIcon',
          'showUsername',
          'queueShowDetail',
          'queueShowSubDetail',
          'queueShowSize',
          'queueShowProtocol',
          'queueShowTimeLeft',
          'queueShowProgress',
          'queueVisibleRows',
        ];
        displayFields.forEach((field) => {
          if (mergedWatchStats[field] === undefined && wheelElement[field] !== undefined) {
            mergedWatchStats[field] = wheelElement[field];
          }
        });
        if (watchIndex >= 0) {
          nextOverviewElements[watchIndex] = mergedWatchStats;
        } else {
          nextOverviewElements.push(mergedWatchStats);
        }
        nextOverviewElements = nextOverviewElements.filter((element, index) => (
          index !== wheelIndex
          && String(element?.id || '').trim() !== 'watch-stats-wheel'
        ));
        markChanged();
      }
      if (wheelRemoved) {
        if (!hadWatchStats && !dashboardRemovedElements[watchStatsKey]) {
          dashboardRemovedElements[watchStatsKey] = true;
          markChanged();
        }
        delete dashboardRemovedElements[wheelKey];
        markChanged();
      }
    }

    appItem.overviewElements = nextOverviewElements;
    nextOverviewElements.forEach((element) => {
      const elementId = String(element?.id || '').trim();
      if (!elementId) return;
      const legacyKey = `app:${appItem.id}:${elementId}`;
      if (dashboardRemovedElements[legacyKey]) return;
      let type = '';
      let sectionKey = '';
      if (isAppInSet(normalizedAppId, ARR_APP_IDS)) {
        const section = getArrCombineSectionByElementId(elementId);
        if (section) {
          type = 'arr';
          sectionKey = section.key;
        }
      } else if (isAppInSet(normalizedAppId, MEDIA_APP_IDS)) {
        const section = getMediaCombineSectionByElementId(elementId);
        if (section) {
          type = 'media';
          sectionKey = section.key;
        }
      } else if (isAppInSet(normalizedAppId, DOWNLOADER_APP_IDS)) {
        const section = getDownloaderCombineSectionByElementId(elementId);
        if (section) {
          type = 'downloader';
          sectionKey = section.key;
        }
      }
      if (!type || !sectionKey) return;
      const visibilityRole = resolveDashboardElementVisibilityRole(appItem, element, 'user');
      if (visibilityRole === 'disabled') return;
      const replacementKey = ensureReplacement({
        type,
        sectionKey,
        appIds: [normalizedAppId],
        hasExplicitSelection: true,
        visibilityRole,
        order: Number(element?.order),
      });
      if (!replacementKey) return;
      if (!dashboardRemovedElements[legacyKey]) {
        dashboardRemovedElements[legacyKey] = true;
        markChanged();
      }
      if (element.dashboard !== false || normalizeVisibilityRole(element.dashboardVisibilityRole, 'disabled') !== 'disabled') {
        element.dashboard = false;
        element.dashboardVisibilityRole = 'disabled';
        markChanged();
      }
    });
  });

  const migrateLegacyCombinedSection = (type, sectionKey, selectionMap) => {
    const legacyKey = `combined:${type}:${sectionKey}`;
    if (dashboardRemovedElements[legacyKey]) return;
    const legacySettings = dashboardCombinedSettings[legacyKey] && typeof dashboardCombinedSettings[legacyKey] === 'object'
      ? dashboardCombinedSettings[legacyKey]
      : {};
    const visibilityRole = resolveCombinedDashboardVisibilityRole(legacySettings, 'user');
    if (visibilityRole === 'disabled') return;
    const allowedIds = allowedByType[type] || [];
    const sourceSelection = selectionMap && typeof selectionMap === 'object'
      ? selectionMap
      : {};
    const hasExplicitSelection = Object.keys(sourceSelection).length > 0;
    const selectedIds = hasExplicitSelection
      ? allowedIds.filter((id) => Boolean(sourceSelection[id]))
      : [...allowedIds];
    const replacementKey = ensureReplacement({
      type,
      sectionKey,
      appIds: selectedIds,
      hasExplicitSelection,
      visibilityRole,
      order: Number(dashboardCombinedOrder[legacyKey]),
    });
    if (!replacementKey) return;
    if (!dashboardRemovedElements[legacyKey]) {
      dashboardRemovedElements[legacyKey] = true;
      markChanged();
    }
  };

  if (ENABLE_ARR_UNIFIED_CARDS) {
    ARR_COMBINE_SECTIONS.forEach((section) => {
      migrateLegacyCombinedSection('arr', section.key, arrLegacySelection[section.key]);
    });
  }
  if (ENABLE_DOWNLOADER_UNIFIED_CARDS) {
    DOWNLOADER_COMBINE_SECTIONS.forEach((section) => {
      migrateLegacyCombinedSection('downloader', section.key, downloaderLegacySelection[section.key]);
    });
  }
  MEDIA_COMBINE_SECTIONS.forEach((section) => {
    migrateLegacyCombinedSection('media', section.key, mediaLegacySelection[section.key]);
  });

  if (!changed) {
    return { changed: false, config: source };
  }

  return {
    changed: true,
    config: {
      ...source,
      apps,
      arrDashboardCombinedCards,
      mediaDashboardCards,
      downloaderDashboardCards,
      dashboardRemovedElements,
      dashboardCombinedSettings,
      dashboardCombinedOrder,
    },
  };
}

function buildArrCombinedDisplayMeta(appLookup, sectionKey, appIds) {
  const sectionLabel = getArrCombineSectionLabel(sectionKey);
  const combinedTitle = sectionLabel;
  const sectionIconPath = getArrCombineSectionIconPath(sectionKey);
  const selectedApps = (Array.isArray(appIds) ? appIds : [])
    .map((appId) => appLookup.get(normalizeAppId(appId)))
    .filter(Boolean);
  if (!selectedApps.length) {
    return {
      appIds: [],
      appNames: [],
      iconPath: sectionIconPath,
      displayName: combinedTitle,
    };
  }
  const selectedAppIds = selectedApps.map((appItem) => normalizeAppId(appItem.id)).filter(Boolean);
  const selectedAppNames = selectedApps.map((appItem) => String(appItem.name || '').trim()).filter(Boolean);
  const iconPath = selectedApps.length === 1
    ? resolvePersistedAppIconPath(selectedApps[0])
    : sectionIconPath;
  return {
    appIds: selectedAppIds,
    appNames: selectedAppNames,
    iconPath,
    displayName: combinedTitle,
  };
}

function buildMediaCombinedDisplayMeta(appLookup, sectionKey, appIds) {
  const sectionLabel = getMediaCombineSectionLabel(sectionKey);
  const combinedTitle = sectionLabel;
  const selectedApps = (Array.isArray(appIds) ? appIds : [])
    .map((appId) => appLookup.get(normalizeAppId(appId)))
    .filter(Boolean);
  if (!selectedApps.length) {
    return {
      appIds: [],
      appNames: [],
      iconPath: getMediaCombineSectionIconPath(sectionKey),
      displayName: combinedTitle,
    };
  }
  const selectedAppIds = selectedApps.map((appItem) => normalizeAppId(appItem.id)).filter(Boolean);
  const selectedAppNames = selectedApps.map((appItem) => String(appItem.name || '').trim()).filter(Boolean);
  const iconPath = selectedApps.length === 1
    ? resolvePersistedAppIconPath(selectedApps[0])
    : getMediaCombineSectionIconPath(sectionKey);
  return {
    appIds: selectedAppIds,
    appNames: selectedAppNames,
    iconPath,
    displayName: combinedTitle,
  };
}

function buildDownloaderCombinedDisplayMeta(appLookup, sectionKey, appIds) {
  const sectionLabel = getDownloaderCombineSectionLabel(sectionKey);
  const combinedTitle = sectionLabel;
  const selectedApps = (Array.isArray(appIds) ? appIds : [])
    .map((appId) => appLookup.get(normalizeAppId(appId)))
    .filter(Boolean);
  if (!selectedApps.length) {
    return {
      appIds: [],
      appNames: [],
      iconPath: getDownloaderCombineSectionIconPath(sectionKey),
      displayName: combinedTitle,
    };
  }
  const selectedAppIds = selectedApps.map((appItem) => normalizeAppId(appItem.id)).filter(Boolean);
  const selectedAppNames = selectedApps.map((appItem) => String(appItem.name || '').trim()).filter(Boolean);
  const iconPath = selectedApps.length === 1
    ? resolvePersistedAppIconPath(selectedApps[0])
    : getDownloaderCombineSectionIconPath(sectionKey);
  return {
    appIds: selectedAppIds,
    appNames: selectedAppNames,
    iconPath,
    displayName: combinedTitle,
  };
}

function getOrCreatePlexClientId() {
  try {
    if (!fs.existsSync(DATA_DIR)) {
      fs.mkdirSync(DATA_DIR, { recursive: true });
    }
    const idPath = path.join(DATA_DIR, 'plex_client_id.txt');
    if (fs.existsSync(idPath)) {
      const stored = fs.readFileSync(idPath, 'utf8').trim();
      if (stored) return stored;
    }
    const created = `launcharr-${crypto.randomBytes(12).toString('hex')}`;
    fs.writeFileSync(idPath, created);
    return created;
  } catch (err) {
    return `launcharr-${crypto.randomBytes(12).toString('hex')}`;
  }
}

function resolveArrDashboardCombineSettings(config, apps) {
  const configured = (config && typeof config.arrDashboardCombine === 'object' && config.arrDashboardCombine)
    ? config.arrDashboardCombine
    : {};
  const appIds = (Array.isArray(apps) ? apps : [])
    .map((appItem) => String(appItem?.id || '').trim().toLowerCase())
    .filter((id) => isAppInSet(id, ARR_APP_IDS));
  const hasConfigured = ARR_COMBINE_SECTIONS.some((section) => {
    const sectionValue = configured && typeof configured[section.key] === 'object' ? configured[section.key] : null;
    return sectionValue && Object.keys(sectionValue).length > 0;
  });

  return ARR_COMBINE_SECTIONS.reduce((acc, section) => {
    const sectionValue = (configured && typeof configured[section.key] === 'object' && configured[section.key])
      ? configured[section.key]
      : {};
    acc[section.key] = appIds.reduce((sectionAcc, appId) => {
      sectionAcc[appId] = hasConfigured ? Boolean(sectionValue[appId]) : true;
      return sectionAcc;
    }, {});
    return acc;
  }, {});
}

function resolveDownloaderDashboardCombineSettings(config, apps) {
  const configured = (config && typeof config.downloaderDashboardCombine === 'object' && config.downloaderDashboardCombine)
    ? config.downloaderDashboardCombine
    : {};
  const appIds = (Array.isArray(apps) ? apps : [])
    .map((appItem) => String(appItem?.id || '').trim().toLowerCase())
    .filter((id) => isAppInSet(id, DOWNLOADER_APP_IDS));
  const hasConfigured = DOWNLOADER_COMBINE_SECTIONS.some((section) => {
    const sectionValue = configured && typeof configured[section.key] === 'object' ? configured[section.key] : null;
    return sectionValue && Object.keys(sectionValue).length > 0;
  });

  return DOWNLOADER_COMBINE_SECTIONS.reduce((acc, section) => {
    const sectionValue = (configured && typeof configured[section.key] === 'object' && configured[section.key])
      ? configured[section.key]
      : {};
    acc[section.key] = appIds.reduce((sectionAcc, appId) => {
      sectionAcc[appId] = hasConfigured ? Boolean(sectionValue[appId]) : true;
      return sectionAcc;
    }, {});
    return acc;
  }, {});
}

function resolveMediaDashboardCombineSettings(config, apps) {
  const configured = (config && typeof config.mediaDashboardCombine === 'object' && config.mediaDashboardCombine)
    ? config.mediaDashboardCombine
    : {};
  const appIds = (Array.isArray(apps) ? apps : [])
    .map((appItem) => String(appItem?.id || '').trim().toLowerCase())
    .filter((id) => isAppInSet(id, MEDIA_APP_IDS));
  const hasConfigured = MEDIA_COMBINE_SECTIONS.some((section) => {
    const sectionValue = configured && typeof configured[section.key] === 'object' ? configured[section.key] : null;
    return sectionValue && Object.keys(sectionValue).length > 0;
  });

  return MEDIA_COMBINE_SECTIONS.reduce((acc, section) => {
    const sectionValue = (configured && typeof configured[section.key] === 'object' && configured[section.key])
      ? configured[section.key]
      : {};
    acc[section.key] = appIds.reduce((sectionAcc, appId) => {
      sectionAcc[appId] = hasConfigured ? Boolean(sectionValue[appId]) : true;
      return sectionAcc;
    }, {});
    return acc;
  }, {});
}

function normalizeAppId(value) {
  return String(value || '').trim().toLowerCase();
}

function isMultiInstanceEnabled(appItem) {
  const raw = appItem?.supportsInstances;
  return raw === true || String(raw || '').trim().toLowerCase() === 'true';
}

function getConfiguredInstanceBaseId(appItem) {
  const configured = normalizeAppId(appItem?.instanceBaseId || '');
  if (configured) return configured;
  const id = normalizeAppId(appItem?.id || '');
  if (!id) return '';
  const match = id.match(/^(.*)-(\d+)$/);
  if (match && Number(match[2]) > 1) return normalizeAppId(match[1] || '');
  return id;
}

function refreshRuntimeMultiInstanceBaseIds(apps = []) {
  const next = new Set(LEGACY_MULTI_INSTANCE_APP_IDS.map((id) => normalizeAppId(id)).filter(Boolean));
  (Array.isArray(apps) ? apps : []).forEach((appItem) => {
    if (!isMultiInstanceEnabled(appItem)) return;
    const baseId = getConfiguredInstanceBaseId(appItem);
    if (baseId) next.add(baseId);
  });
  RUNTIME_MULTI_INSTANCE_BASE_IDS = Array.from(next);
}

function getMultiInstanceBaseIds() {
  return Array.from(new Set(
    (Array.isArray(RUNTIME_MULTI_INSTANCE_BASE_IDS) ? RUNTIME_MULTI_INSTANCE_BASE_IDS : [])
      .map((id) => normalizeAppId(id))
      .filter(Boolean)
  ));
}

function supportsAppInstances(baseId) {
  const key = normalizeAppId(baseId);
  return Boolean(key) && getMultiInstanceBaseIds().includes(key);
}

function getAppBaseId(value) {
  const id = normalizeAppId(value);
  if (!id) return '';
  const matched = getMultiInstanceBaseIds()
    .slice()
    .sort((a, b) => b.length - a.length)
    .find((baseId) => {
      if (id === baseId) return true;
      if (!id.startsWith(`${baseId}-`)) return false;
      return /^\d+$/.test(id.slice(baseId.length + 1));
    });
  return matched || id;
}

function isAppInSet(appId, baseIds) {
  const baseId = getAppBaseId(appId);
  return Array.isArray(baseIds) && baseIds.includes(baseId);
}

function getInstanceSuffix(appId, baseId = '') {
  const normalized = normalizeAppId(appId);
  const resolvedBase = normalizeAppId(baseId) || getAppBaseId(normalized);
  if (!normalized || !resolvedBase) return NaN;
  const match = normalized.match(new RegExp(`^${resolvedBase}-(\\d+)$`));
  if (!match) return normalized === resolvedBase ? 1 : NaN;
  return Number(match[1]);
}

function canManageWithDefaultAppManager(appItem) {
  if (!appItem) return false;
  if (Boolean(appItem?.custom)) return false;
  const id = normalizeAppId(appItem?.id);
  if (!id) return false;
  const baseId = getAppBaseId(id);
  return getInstanceSuffix(id, baseId) === 1;
}

function getBaseAppTitle(baseId) {
  const key = normalizeAppId(baseId);
  if (!key) return 'App';
  if (APP_BASE_NAME_MAP[key]) return APP_BASE_NAME_MAP[key];
  return key.charAt(0).toUpperCase() + key.slice(1);
}

function getDefaultInstanceName(baseId, appId) {
  const title = getBaseAppTitle(baseId);
  const suffix = getInstanceSuffix(appId, baseId);
  if (Number.isFinite(suffix) && suffix > 1) return `${title} ${suffix}`;
  return title;
}

function coerceMultiInstanceLimit(value, fallback = DEFAULT_MAX_MULTI_INSTANCES_PER_APP) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(2, Math.min(20, Math.round(parsed)));
}

function getMaxMultiInstancesForBase(baseId, apps = []) {
  const key = normalizeAppId(baseId);
  if (!key) return DEFAULT_MAX_MULTI_INSTANCES_PER_APP;
  const sourceApps = Array.isArray(apps) && apps.length ? apps : loadDefaultApps();
  const match = sourceApps.find((appItem) => {
    if (!appItem) return false;
    const appId = normalizeAppId(appItem?.id);
    const configuredBase = getConfiguredInstanceBaseId(appItem);
    return configuredBase === key || appId === key;
  });
  return coerceMultiInstanceLimit(match?.maxInstances, DEFAULT_MAX_MULTI_INSTANCES_PER_APP);
}

function getInstanceNamePlaceholder(baseId, apps = []) {
  const key = normalizeAppId(baseId);
  if (!key) return DEFAULT_INSTANCE_NAME_PLACEHOLDER;
  const sourceApps = Array.isArray(apps) && apps.length ? apps : loadDefaultApps();
  const match = sourceApps.find((appItem) => {
    if (!appItem) return false;
    const appId = normalizeAppId(appItem?.id);
    const configuredBase = getConfiguredInstanceBaseId(appItem);
    return configuredBase === key || appId === key;
  });
  const configured = String(match?.instanceNamePlaceholder || '').trim();
  if (configured) return configured;
  return APP_INSTANCE_PLACEHOLDER_MAP[key] || DEFAULT_INSTANCE_NAME_PLACEHOLDER;
}

function getMultiInstanceTitleMap() {
  return getMultiInstanceBaseIds().reduce((acc, baseId) => {
    acc[baseId] = getBaseAppTitle(baseId);
    return acc;
  }, {});
}

function getMultiInstanceMaxMap(apps = []) {
  return getMultiInstanceBaseIds().reduce((acc, baseId) => {
    acc[baseId] = getMaxMultiInstancesForBase(baseId, apps);
    return acc;
  }, {});
}

function getMultiInstancePlaceholderMap(apps = []) {
  return getMultiInstanceBaseIds().reduce((acc, baseId) => {
    acc[baseId] = getInstanceNamePlaceholder(baseId, apps);
    return acc;
  }, {});
}

function buildNextInstanceId(baseId, apps = []) {
  const key = normalizeAppId(baseId);
  if (!key || !supportsAppInstances(key)) return '';
  const maxInstances = getMaxMultiInstancesForBase(key, apps);
  const used = new Set(
    (Array.isArray(apps) ? apps : [])
      .map((appItem) => normalizeAppId(appItem?.id))
      .filter((appId) => getAppBaseId(appId) === key)
  );
  for (let index = 2; index <= maxInstances; index += 1) {
    const candidate = `${key}-${index}`;
    if (!used.has(candidate)) return candidate;
  }
  return '';
}

function getDefaultIconPathForAppId(appId) {
  const baseId = getAppBaseId(appId);
  const normalizedBaseId = normalizeAppId(baseId);
  if (!normalizedBaseId) return '/icons/app.svg';
  const pngPath = path.join(ICONS_DIR, `${normalizedBaseId}.png`);
  if (fs.existsSync(pngPath)) {
    return `/icons/${normalizedBaseId}.png`;
  }
  const svgPath = path.join(ICONS_DIR, `${normalizedBaseId}.svg`);
  if (fs.existsSync(svgPath)) {
    return `/icons/${normalizedBaseId}.svg`;
  }
  return '/icons/app.svg';
}

function iconPathExists(iconPath) {
  const normalized = String(iconPath || '').trim();
  if (!normalized.startsWith('/icons/')) return false;
  const relativePath = normalized.replace(/^\/+/, '');
  const resolvedPath = path.normalize(path.join(PUBLIC_DIR, relativePath));
  if (!resolvedPath.startsWith(PUBLIC_DIR)) return false;
  return fs.existsSync(resolvedPath);
}

function resolvePersistedAppIconPath(appItem) {
  const configuredPath = String(appItem?.icon || '').trim();
  if (configuredPath && iconPathExists(configuredPath)) return configuredPath;
  return getDefaultIconPathForAppId(appItem?.id);
}

function resolveQueueColumnLabels(appItem) {
  const appId = getAppBaseId(appItem?.id);
  if (appId === 'nzbget' || appId === 'sabnzbd') {
    return { detailLabel: 'Category', subDetailLabel: 'Status' };
  }
  if (appId === 'transmission' || appId === 'qbittorrent') {
    return { detailLabel: 'Status', subDetailLabel: 'Rate' };
  }
  if (appId === 'radarr') {
    return { detailLabel: 'Year', subDetailLabel: 'Studio' };
  }
  if (appId === 'lidarr') {
    return { detailLabel: 'Album', subDetailLabel: 'Track' };
  }
  if (appId === 'readarr') {
    return { detailLabel: 'Author', subDetailLabel: 'Book' };
  }
  return { detailLabel: 'Episode', subDetailLabel: 'Episode Title' };
}

function stableStringify(value) {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  }
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).sort();
    const entries = keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`);
    return `{${entries.join(',')}}`;
  }
  return JSON.stringify(value);
}

function deepEqual(a, b) {
  return stableStringify(a) === stableStringify(b);
}

function readJsonFileCandidates(candidatePaths) {
  const uniquePaths = Array.from(new Set(
    (Array.isArray(candidatePaths) ? candidatePaths : [])
      .map((candidate) => String(candidate || '').trim())
      .filter(Boolean)
  ));
  for (const candidatePath of uniquePaths) {
    try {
      const raw = fs.readFileSync(candidatePath, 'utf8');
      return JSON.parse(raw);
    } catch (err) {
      continue;
    }
  }
  return null;
}

function loadDefaultApps() {
  const configDefaultPath = path.join(path.dirname(CONFIG_PATH), 'default-apps.json');
  const parsed = readJsonFileCandidates([
    DEFAULT_APPS_PATH,
    BUNDLED_DEFAULT_APPS_PATH,
    configDefaultPath,
    SOURCE_DEFAULT_APPS_PATH,
  ]);
  const list = Array.isArray(parsed)
    ? parsed
    : (parsed && Array.isArray(parsed.apps) ? parsed.apps : []);
  return dedupeApps(list);
}

function loadDefaultCategories() {
  const configDefaultPath = path.join(path.dirname(CONFIG_PATH), 'default-categories.json');
  const parsed = readJsonFileCandidates([
    DEFAULT_CATEGORIES_PATH,
    BUNDLED_DEFAULT_CATEGORIES_PATH,
    configDefaultPath,
    SOURCE_DEFAULT_CATEGORIES_PATH,
  ]);
  return normalizeCategoryEntries(parsed);
}

function dedupeApps(apps) {
  const seen = new Set();
  const out = [];
  (Array.isArray(apps) ? apps : []).forEach((app) => {
    const id = normalizeAppId(app?.id);
    if (!id || seen.has(id)) return;
    seen.add(id);
    out.push(app);
  });
  return out;
}

function mergeAppDefaults(defaultApps, overrideApps) {
  const defaults = dedupeApps(defaultApps);
  const overrides = dedupeApps(overrideApps);
  const overrideMap = new Map(
    overrides
      .filter((app) => normalizeAppId(app?.id))
      .map((app) => [normalizeAppId(app.id), app])
  );

  const merged = defaults
    .filter((app) => normalizeAppId(app?.id))
    .map((app) => {
      const override = overrideMap.get(normalizeAppId(app.id));
      if (!override) return app;
      const hasExplicitRemoved = Object.prototype.hasOwnProperty.call(override, 'removed');
      if (hasExplicitRemoved) {
        return { ...app, ...override };
      }
      return { ...app, ...override, removed: false };
    });

  const defaultIds = new Set(defaults.map((app) => normalizeAppId(app?.id)).filter(Boolean));
  const custom = overrides
    .filter((app) => {
      const id = normalizeAppId(app?.id);
      return id && !defaultIds.has(id);
    });

  return [...merged, ...custom];
}

function mergeCategoryDefaults(defaultCategories, overrideCategories) {
  const defaults = normalizeCategoryEntries(defaultCategories);
  const overrides = normalizeCategoryEntries(overrideCategories);
  if (!overrides.length) return defaults;
  const defaultMap = new Map(
    defaults
      .filter((entry) => entry?.name)
      .map((entry) => [String(entry.name).toLowerCase(), entry])
  );
  const seen = new Set();
  const ordered = overrides
    .filter((entry) => entry?.name)
    .map((entry) => {
      const key = String(entry.name).toLowerCase();
      const base = defaultMap.get(key);
      seen.add(key);
      if (!base) return entry;
      const mergedEntry = { ...base, ...entry };
      const iconValue = String(entry.icon || '').trim();
      if (!iconValue || iconValue === '/icons/category.svg') {
        mergedEntry.icon = base.icon;
      }
      return mergedEntry;
    });

  return ordered;
}

function buildCategoryOverrides(defaultCategories, mergedCategories) {
  return normalizeCategoryEntries(mergedCategories);
}

function buildAppOverrides(defaultApps, mergedApps) {
  const defaults = Array.isArray(defaultApps) ? defaultApps : [];
  const apps = Array.isArray(mergedApps) ? mergedApps : [];
  const defaultMap = new Map(
    defaults
      .filter((app) => normalizeAppId(app?.id))
      .map((app) => [normalizeAppId(app.id), app])
  );

  return apps
    .map((app) => {
      const id = normalizeAppId(app?.id);
      if (!id) return null;
      const base = defaultMap.get(id);
      if (!base) return app;
      const override = { id: app.id };
      Object.keys(app).forEach((key) => {
        if (key === 'id') return;
        // Preserve explicit removals even when the default catalog also marks the app as removed.
        // Without this, save/load drops `removed:true` for hidden-by-default apps (e.g. Emby),
        // and mergeAppDefaults later reactivates them because the override no longer has `removed`.
        if (key === 'removed' && app[key] === true) {
          override[key] = true;
          return;
        }
        if (!deepEqual(app[key], base[key])) {
          override[key] = app[key];
        }
      });
      return Object.keys(override).length > 1 ? override : null;
    })
    .filter(Boolean);
}

function loadConfig() {
  const defaults = loadDefaultApps();
  const defaultCategories = loadDefaultCategories();
  try {
    let seededNewConfig = false;
    if (!fs.existsSync(CONFIG_PATH)) {
      if (!fs.existsSync(path.dirname(CONFIG_PATH))) {
        fs.mkdirSync(path.dirname(CONFIG_PATH), { recursive: true });
      }
      const configExampleSource = [CONFIG_EXAMPLE_PATH, BUNDLED_CONFIG_EXAMPLE_PATH]
        .find((candidatePath) => fs.existsSync(candidatePath));
      if (configExampleSource) {
        fs.copyFileSync(configExampleSource, CONFIG_PATH);
      } else {
        fs.writeFileSync(CONFIG_PATH, JSON.stringify({ apps: [], categories: [] }, null, 2));
      }
      seededNewConfig = true;
    }
    const raw = fs.readFileSync(CONFIG_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    const overrideApps = parsed && Array.isArray(parsed.apps) ? parsed.apps : [];
    const overrideCategories = parsed && Array.isArray(parsed.categories) ? parsed.categories : [];
    const mergedApps = mergeAppDefaults(defaults, overrideApps);
    const mergedCategories = mergeCategoryDefaults(defaultCategories, overrideCategories);
    const nextConfig = { ...parsed, apps: mergedApps, categories: mergedCategories };
    if (seededNewConfig) {
      Object.assign(nextConfig, buildEmptyDashboardStateSnapshot(nextConfig));
      saveConfig(nextConfig);
    }
    const hasActiveApps = mergedApps.some((appItem) => !appItem?.removed);
    if (seededNewConfig || !hasActiveApps) {
      refreshRuntimeMultiInstanceBaseIds(mergedApps);
      return nextConfig;
    }
    const migration = migrateDeprecatedDashboardCards(nextConfig);
    if (migration.changed) {
      saveConfig(migration.config);
      refreshRuntimeMultiInstanceBaseIds(Array.isArray(migration.config?.apps) ? migration.config.apps : mergedApps);
      return migration.config;
    }
    refreshRuntimeMultiInstanceBaseIds(mergedApps);
    return nextConfig;
  } catch (err) {
    refreshRuntimeMultiInstanceBaseIds(defaults);
    return { apps: defaults, categories: defaultCategories };
  }
}

function saveConfig(config) {
  const defaults = loadDefaultApps();
  const defaultCategories = loadDefaultCategories();
  const nextConfig = { ...config };
  const runtimeApps = mergeAppDefaults(
    defaults,
    Array.isArray(nextConfig.apps) ? nextConfig.apps : []
  );
  if (defaults.length) {
    nextConfig.apps = buildAppOverrides(defaults, nextConfig.apps);
  }
  if (defaultCategories.length) {
    nextConfig.categories = buildCategoryOverrides(defaultCategories, nextConfig.categories);
  }
  refreshRuntimeMultiInstanceBaseIds(runtimeApps);
  if (!fs.existsSync(path.dirname(CONFIG_PATH))) {
    fs.mkdirSync(path.dirname(CONFIG_PATH), { recursive: true });
  }
  const tmpPath = CONFIG_PATH + '.tmp';
  fs.writeFileSync(tmpPath, JSON.stringify(nextConfig, null, 2));
  fs.renameSync(tmpPath, CONFIG_PATH);
}

function groupByCategory(apps) {
  const grouped = new Map();
  for (const appItem of apps) {
    const key = appItem.category || 'apps';
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(appItem);
  }
  return Array.from(grouped.entries()).map(([category, items]) => ({
    category,
    items,
  }));
}

function isVisible(appItem, role) {
  if (!role) return false;
  const access = getMenuAccess(appItem, role);
  return hasAnyMenuAccess(access);
}

function normalizeRoles(appItem) {
  if (!appItem) return [];
  if (Array.isArray(appItem.roles)) return appItem.roles.map((r) => r.toLowerCase());
  if (appItem.role) return [String(appItem.role).toLowerCase()];
  return [];
}

function resolveRole(plexUser) {
  const identifiers = [
    plexUser.username,
    plexUser.email,
    plexUser.title,
  ].filter(Boolean);

  const admins = loadAdmins();
  if (matches(admins, identifiers)) return 'admin';

  const coAdmins = loadCoAdmins();
  if (matches(coAdmins, identifiers)) return 'co-admin';

  if (admins.length === 0) {
    const adminKey = identifiers[0];
    if (adminKey) {
      saveAdmins([adminKey]);
      return 'admin';
    }
  }

  return 'user';
}

function matches(list, identifiers) {
  const normalized = list.map((value) => value.toLowerCase());
  return identifiers.some((value) => normalized.includes(String(value).toLowerCase()));
}

function loadAdmins() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }

  const adminPath = path.join(DATA_DIR, 'admins.json');
  if (fs.existsSync(adminPath)) {
    try {
      const raw = fs.readFileSync(adminPath, 'utf8');
      const data = JSON.parse(raw);
      if (Array.isArray(data.admins)) return data.admins;
    } catch (err) {
      return [];
    }
  }

  if (ADMIN_USERS.length) {
    saveAdmins(ADMIN_USERS);
    return ADMIN_USERS;
  }

  return [];
}

function saveAdmins(admins) {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
  const adminPath = path.join(DATA_DIR, 'admins.json');
  fs.writeFileSync(adminPath, JSON.stringify({ admins }, null, 2));
}

function loadCoAdmins() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }

  const coAdminPath = path.join(DATA_DIR, 'coadmins.json');
  if (fs.existsSync(coAdminPath)) {
    try {
      const raw = fs.readFileSync(coAdminPath, 'utf8');
      const data = JSON.parse(raw);
      if (Array.isArray(data.coAdmins)) return data.coAdmins;
    } catch (err) {
      return [];
    }
  }

  return [];
}

function saveCoAdmins(coAdmins) {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
  const coAdminPath = path.join(DATA_DIR, 'coadmins.json');
  fs.writeFileSync(coAdminPath, JSON.stringify({ coAdmins }, null, 2));
}

async function getPlexDiscoveryWatchlisted() {
  const now = Date.now();
  if (plexDiscoveryWatchlistedCache.payload && plexDiscoveryWatchlistedCache.expiresAt > now) {
    return { ...plexDiscoveryWatchlistedCache.payload, cached: true };
  }

  const response = await fetch(PLEX_DISCOVERY_WATCHLISTED_URL, {
    method: 'GET',
    headers: {
      Accept: 'text/html,application/xhtml+xml',
      'User-Agent': PRODUCT,
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Plex discovery request failed (${response.status}): ${text.slice(0, 180)}`);
  }

  const html = await response.text();
  const items = parsePlexDiscoveryTiles(html).slice(0, 80);
  const payload = {
    fetchedAt: new Date().toISOString(),
    source: PLEX_DISCOVERY_WATCHLISTED_URL,
    items,
  };
  const ttlMs = items.length ? PLEX_DISCOVERY_CACHE_TTL_MS : 30 * 1000;

  plexDiscoveryWatchlistedCache = {
    expiresAt: now + ttlMs,
    payload,
  };

  return { ...payload, cached: false };
}

function parsePlexDiscoveryTiles(html) {
  const source = String(html || '');
  const normalized = source
    .replace(/\\"/g, '"')
    .replace(/\\\//g, '/')
    .replace(/\\u0026/g, '&')
    .replace(/\\u003d/g, '=');
  const sources = [normalized, source];
  const dedupe = new Map();

  sources.forEach((blob) => {
    const chunks = String(blob || '').split('"_component":"ArtworkTile"');
    for (let i = 0; i < chunks.length - 1; i += 1) {
      const prefix = chunks[i].slice(-5000);
      const suffix = chunks[i + 1].slice(0, 800);

      const linkMatches = Array.from(prefix.matchAll(/"link":{"url":"([^"]+)","external":(?:true|false)}/g));
      const imageMatches = Array.from(prefix.matchAll(/"image":{"url":"([^"]+)"/g));
      const ratingKeyMatches = Array.from(prefix.matchAll(/"ratingKey":"([^"]+)"/g));
      const titleMatch = suffix.match(/,"title":"((?:\\.|[^"\\])+)"/);
      const subtitleMatch = suffix.match(/"subtitles":\[(.*?)\]/);

      const rawLink = decodePlexEscapes(linkMatches.length ? linkMatches[linkMatches.length - 1][1] : '');
      const title = decodePlexEscapes(titleMatch ? titleMatch[1] : '').trim();
      if (!rawLink || !title) continue;

      const link = rawLink.startsWith('http') ? rawLink : `https://watch.plex.tv${rawLink}`;
      if (dedupe.has(link)) continue;

      const thumbRaw = imageMatches.length ? imageMatches[imageMatches.length - 1][1] : '';
      const ratingKey = decodePlexEscapes(ratingKeyMatches.length ? ratingKeyMatches[ratingKeyMatches.length - 1][1] : '');
      const subtitleValues = parsePlexSubtitleList(subtitleMatch ? subtitleMatch[1] : '');
      const year = subtitleValues.find((value) => /^\d{4}$/.test(value)) || '';
      const watchlistedCountLabel = extractWatchlistedCountLabel(subtitleValues, prefix + suffix);
      const kind = rawLink.includes('/show/') ? 'tv' : (rawLink.includes('/movie/') ? 'movie' : 'movie');
      const slug = String(rawLink)
        .replace(/^https?:\/\/[^/]+/i, '')
        .split('?')[0]
        .split('/')
        .filter(Boolean)
        .slice(1)
        .join('/');

      dedupe.set(link, {
        kind,
        title,
        year,
        subtitle: subtitleValues.join(' • '),
        watchlistedCountLabel,
        thumb: decodePlexEscapes(thumbRaw),
        ratingKey,
        slug,
        link,
      });
    }
  });

  return Array.from(dedupe.values());
}

function parsePlexSubtitleList(value) {
  const source = decodePlexEscapes(value || '');
  const matches = source.match(/"([^"]+)"/g) || [];
  return matches.map((entry) => entry.replace(/^"|"$/g, '').trim()).filter(Boolean);
}

function extractWatchlistedCountLabel(values, rawChunk = '') {
  const list = Array.isArray(values) ? values.map((value) => String(value || '').trim()).filter(Boolean) : [];
  for (let index = 0; index < list.length; index += 1) {
    const value = list[index];
    const lower = value.toLowerCase();
    if (!lower.includes('watchlist')) continue;
    const countMatch = value.match(/(\d[\d.,]*\s*[kmb]?)/i);
    if (countMatch) {
      const raw = String(countMatch[1] || '').replace(/\s+/g, '').trim();
      if (raw) return raw + ' watchlists';
    }
    return value;
  }
  const raw = decodePlexEscapes(String(rawChunk || ''));
  const patterns = [
    /(\d[\d.,]*\s*[kmb]?)\s+(?:people\s+)?watchlist(?:ed|s)?/i,
    /watchlist(?:ed|s)?\s+by\s+(\d[\d.,]*\s*[kmb]?)/i,
  ];
  for (let index = 0; index < patterns.length; index += 1) {
    const match = raw.match(patterns[index]);
    if (!match) continue;
    const rawCount = String(match[1] || '').replace(/\s+/g, '').trim();
    if (rawCount) return rawCount + ' watchlists';
  }
  return '';
}

async function fetchPlexDiscoveryMetadata(ratingKey, token) {
  const url = `https://discover.provider.plex.tv/library/metadata/${encodeURIComponent(ratingKey)}?X-Plex-Token=${encodeURIComponent(token)}`;
  const response = await fetch(url, {
    method: 'GET',
    headers: {
      Accept: 'application/xml',
      ...plexHeaders(),
    },
  });
  const xmlText = await response.text();
  if (!response.ok) {
    throw new Error(`Plex metadata request failed (${response.status}): ${xmlText.slice(0, 180)}`);
  }

  const match = xmlText.match(/<(Video|Directory)\b[^>]*>/);
  const attrs = parseXmlAttributes(match ? match[0] : '');
  const guidCandidates = Array.from(xmlText.matchAll(/<Guid\b[^>]*\bid="([^"]+)"/gi))
    .map((entry) => String(entry?.[1] || '').trim())
    .concat([
      String(attrs.guid || '').trim(),
      String(attrs.parentGuid || '').trim(),
      String(attrs.grandparentGuid || '').trim(),
    ])
    .filter(Boolean);
  let imdbId = '';
  let tmdbId = '';
  guidCandidates.forEach((guidValue) => {
    const value = String(guidValue || '').trim();
    const lower = value.toLowerCase();
    if (!imdbId && lower.startsWith('imdb://')) {
      const id = value.slice('imdb://'.length).split('?')[0].trim();
      if (/^tt\d+$/i.test(id)) imdbId = id;
    }
    if (!tmdbId && lower.startsWith('tmdb://')) {
      const id = value.slice('tmdb://'.length).split('?')[0].trim();
      if (/^\d+$/.test(id)) tmdbId = id;
    }
  });
  return {
    summary: String(attrs.summary || '').trim(),
    studio: String(attrs.studio || '').trim(),
    contentRating: String(attrs.contentRating || '').trim(),
    tagline: String(attrs.tagline || '').trim(),
    year: String(attrs.year || '').trim(),
    imdbId,
    tmdbId,
  };
}

function buildWatchlistStateFromActions(actions) {
  const addAction = actions.find((action) => action && action.id === 'addToWatchlist');
  const removeAction = actions.find((action) => action && action.id === 'removeFromWatchlist');
  const upsell = actions.find((action) => action && action.id === 'upsellWatchlist');

  if (upsell && upsell.visible) {
    return {
      allowed: false,
      signedIn: false,
      isWatchlisted: false,
      nextAction: 'add',
      label: 'Sign in to Watchlist',
    };
  }

  if (!addAction && !removeAction) {
    return {
      allowed: false,
      signedIn: true,
      isWatchlisted: false,
      nextAction: 'add',
      label: 'Watchlist unavailable',
    };
  }

  const isWatchlisted = Boolean(removeAction && removeAction.visible);
  return {
    allowed: true,
    signedIn: true,
    isWatchlisted,
    nextAction: isWatchlisted ? 'remove' : 'add',
    label: isWatchlisted ? 'Remove from Watchlist' : 'Add to Watchlist',
  };
}

async function fetchPlexWatchlistState({ kind, slug, token }) {
  const actions = await fetchPlexDiscoveryActions({ kind, slug, token });
  return buildWatchlistStateFromActions(actions);
}

async function resolvePlexDiscoverRatingKey({ kind, slug, token }) {
  const actions = await fetchPlexDiscoveryActions({ kind, slug, token });
  for (const action of actions) {
    const key = action?.data?.ratingKey;
    if (key) return String(key);
  }
  return '';
}

async function updatePlexWatchlist({ kind, slug, action, token }) {
  const actions = await fetchPlexDiscoveryActions({ kind, slug, token });
  const actionId = action === 'remove' ? 'removeFromWatchlist' : 'addToWatchlist';
  const target = actions.find((entry) => entry && entry.id === actionId);
  if (!target || !target.data || !target.data.url) {
    throw new Error(`Watchlist action not available (${actionId}).`);
  }

  const method = String(target.data.method || 'PUT').toUpperCase();
  const response = await fetch(String(target.data.url), {
    method,
    headers: {
      Accept: 'application/json, text/plain, */*',
      'X-Plex-Token': token,
      ...plexHeaders(),
    },
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Watchlist update failed (${response.status}): ${text.slice(0, 180)}`);
  }
}

async function fetchPlexDiscoveryActions({ kind, slug, token }) {
  const sourceKind = kind === 'tv' ? 'show' : 'movie';
  const normalizedSlug = String(slug || '').replace(/^\/+/, '').replace(/^.*\//, '');
  const discoverSlug = `${sourceKind}:${normalizedSlug}`;
  const url = `https://luma.plex.tv/api/action/get-actions-for-metadata-item?slug=${encodeURIComponent(discoverSlug)}&detailsSource=discover&screen.type=List`;
  const response = await fetch(url, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      'X-Plex-Token': token,
      ...plexHeaders(),
    },
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`Plex action request failed (${response.status}).`);
  }
  return Array.isArray(payload.actions) ? payload.actions : [];
}

function parseXmlAttributes(tag) {
  const attrs = {};
  String(tag || '').replace(/(\w+)="([^"]*)"/g, (_m, key, value) => {
    attrs[key] = decodeXmlEntities(value);
    return '';
  });
  return attrs;
}

function decodeXmlEntities(value) {
  return String(value || '')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .replace(/&#(\d+);/g, (_m, code) => String.fromCharCode(Number(code) || 0))
    .replace(/&#x([0-9a-fA-F]+);/g, (_m, code) => String.fromCharCode(parseInt(code, 16) || 0));
}

function decodePlexEscapes(value) {
  return String(value || '')
    .replace(/\\u([0-9a-fA-F]{4})/g, (_m, hex) => String.fromCharCode(parseInt(hex, 16)))
    .replace(/\\"/g, '"')
    .replace(/\\\//g, '/')
    .replace(/\\\\/g, '\\');
}

function requireAdmin(req, res, next) {
  const role = getEffectiveRole(req);
  if (role === 'admin') return next();
  const requestPath = String(req.originalUrl || req.url || '').trim() || '/';
  const method = String(req.method || 'GET').toUpperCase();
  const isApiPath = requestPath.startsWith('/api/');
  if (!role && !isApiPath && (method === 'GET' || method === 'HEAD')) {
    try {
      if (req.session) req.session.postLoginRedirect = requestPath;
    } catch (err) {
      /* ignore session write errors and fall back to plain login redirect */
    }
    return res.redirect('/login');
  }
  pushLog({
    level: 'error',
    app: 'system',
    action: 'access.denied',
    message: 'Admin access required.',
    meta: { path: requestPath },
  });
  res.status(403).send('Admin access required.');
}

function requireActualAdmin(req, res, next) {
  const role = getActualRole(req);
  if (role === 'admin') return next();
  const requestPath = String(req.originalUrl || req.url || '').trim() || '/';
  const method = String(req.method || 'GET').toUpperCase();
  const isApiPath = requestPath.startsWith('/api/');
  if (!role && !isApiPath && (method === 'GET' || method === 'HEAD')) {
    try {
      if (req.session) req.session.postLoginRedirect = requestPath;
    } catch (err) {
      /* ignore session write errors and fall back to plain login redirect */
    }
    return res.redirect('/login');
  }
  pushLog({
    level: 'error',
    app: 'system',
    action: 'access.denied',
    message: 'Admin access required.',
    meta: { path: requestPath },
  });
  res.status(403).send('Admin access required.');
}

function requireSettingsAdmin(req, res, next) {
  const role = getActualRole(req);
  if (role === 'admin') return next();
  const requestPath = String(req.originalUrl || req.url || '').trim() || '/settings';
  const method = String(req.method || 'GET').toUpperCase();
  if (!role && (method === 'GET' || method === 'HEAD') && requestPath.startsWith('/settings')) {
    try {
      if (req.session) req.session.postLoginRedirect = requestPath;
    } catch (err) {
      /* ignore session write errors and fall back to plain login redirect */
    }
    return res.redirect('/login');
  }
  pushLog({
    level: 'error',
    app: 'system',
    action: 'access.denied',
    message: 'Settings access denied.',
    meta: { path: requestPath },
  });
  res.status(403).send('Admin access required.');
}

function requireUser(req, res, next) {
  const role = getActualRole(req);
  if (role) return next();
  pushLog({
    level: 'error',
    app: 'system',
    action: 'auth.required',
    message: 'User authentication required.',
    meta: { path: req.originalUrl || req.url || '' },
  });
  res.redirect('/login');
}

function getActualRole(req) {
  return req.session?.user?.role;
}

function getEffectiveRole(req) {
  const actualRole = getActualRole(req);
  const viewRole = req.session?.viewRole;
  if (actualRole === 'admin' && ['guest', 'user', 'co-admin'].includes(String(viewRole || '').trim().toLowerCase())) {
    return String(viewRole || '').trim().toLowerCase();
  }
  return actualRole;
}

function buildRoleViewDashboardAvailability(config) {
  const sourceConfig = config && typeof config === 'object' ? config : loadConfig();
  return ['guest', 'user', 'co-admin', 'admin'].reduce((acc, role) => {
    const selection = resolveDashboardSelection(sourceConfig, '', role);
    acc[role] = {
      hasDashboard: Boolean(selection?.visibleDashboards?.length),
      firstDashboardId: String(selection?.visibleDashboards?.[0]?.id || '').trim(),
    };
    return acc;
  }, {});
}

function resolveFirstAccessibleAppPath(config, req, role) {
  const apps = Array.isArray(config?.apps) ? config.apps : [];
  const categoryEntries = resolveCategoryEntries(config, apps);
  const categoryOrder = categoryEntries.map((entry) => entry.name);
  const navApps = getNavApps(apps, role, req, categoryOrder, resolveGeneralSettings(config));
  const appItem = Array.isArray(navApps) && navApps.length ? navApps[0] : null;
  if (!appItem?.id) return '';
  const appId = encodeURIComponent(String(appItem.id).trim());
  if (appItem.menuAccess?.overview) return `/apps/${appId}`;
  if (appItem.menuAccess?.launch) return `/apps/${appId}/launch`;
  if (appItem.menuAccess?.settings && role === 'admin') return `/apps/${appId}/settings`;
  if (appItem.menuAccess?.activity && role === 'admin') return `/apps/${appId}/activity`;
  return '';
}

function resolveBestRoleLandingPath(req, role, options = {}) {
  const config = options.config && typeof options.config === 'object' ? options.config : loadConfig();
  const excludeDashboard = options.excludeDashboard === true;
  if (!excludeDashboard) {
    const dashboardSelection = resolveDashboardSelection(config, resolveRequestedDashboardId(req), role);
    if (dashboardSelection.visibleDashboards.length) {
      const targetDashboard = dashboardSelection.activeDashboard || dashboardSelection.visibleDashboards[0];
      const dashboardId = normalizeDashboardInstanceId(targetDashboard?.id) || DASHBOARD_MAIN_ID;
      return `/dashboard?dashboard=${encodeURIComponent(dashboardId)}`;
    }
  }
  const firstAppPath = resolveFirstAccessibleAppPath(config, req, role);
  if (firstAppPath) return firstAppPath;
  if (getActualRole(req) === 'admin') return '/settings';
  return '/user-settings';
}

function resolveRoleSwitchRedirectPath(req, targetRole, options = {}) {
  const config = options.config && typeof options.config === 'object' ? options.config : loadConfig();
  const fallback = resolveBestRoleLandingPath(req, targetRole, { config });
  const referrer = resolveReturnPath(req, fallback);
  try {
    const host = req.headers.host || '';
    const url = new URL(referrer, `http://${host}`);
    const path = String(url.pathname || '').trim();
    if (!path) return fallback;

    if (path === '/dashboard') {
      const requestedDashboardId = normalizeDashboardInstanceId(url.searchParams.get('dashboard'));
      const dashboardSelection = resolveDashboardSelection(config, requestedDashboardId, targetRole);
      if (!dashboardSelection.visibleDashboards.length || !dashboardSelection.activeDashboard) {
        return resolveBestRoleLandingPath(req, targetRole, { config, excludeDashboard: true });
      }
      const activeDashboardId = normalizeDashboardInstanceId(dashboardSelection.activeDashboard.id) || DASHBOARD_MAIN_ID;
      const requestedIsVisible = requestedDashboardId
        ? dashboardSelection.visibleDashboards.some((entry) => entry.id === requestedDashboardId)
        : false;
      if (!requestedDashboardId || requestedIsVisible) return referrer;
      return `/dashboard?dashboard=${encodeURIComponent(activeDashboardId)}`;
    }

    const appOverviewMatch = path.match(/^\/apps\/([^/]+)$/);
    if (appOverviewMatch) {
      const appId = decodeURIComponent(appOverviewMatch[1] || '');
      const appItem = (Array.isArray(config?.apps) ? config.apps : []).find((entry) => String(entry?.id || '') === appId);
      return (appItem && canAccess(appItem, targetRole, 'overview'))
        ? referrer
        : resolveBestRoleLandingPath(req, targetRole, { config });
    }

    const appLaunchMatch = path.match(/^\/apps\/([^/]+)\/launch$/);
    if (appLaunchMatch) {
      const appId = decodeURIComponent(appLaunchMatch[1] || '');
      const appItem = (Array.isArray(config?.apps) ? config.apps : []).find((entry) => String(entry?.id || '') === appId);
      return (appItem && canAccess(appItem, targetRole, 'launch'))
        ? referrer
        : resolveBestRoleLandingPath(req, targetRole, { config });
    }

    if (/^\/apps\/[^/]+\/(?:settings|activity)$/.test(path) && targetRole !== 'admin') {
      return resolveBestRoleLandingPath(req, targetRole, { config });
    }
  } catch (err) {
    return fallback;
  }
  return referrer;
}

function resolveReturnPath(req, fallback = '/dashboard') {
  const referrer = req.get('referer');
  if (!referrer) return fallback;
  try {
    const host = req.headers.host || '';
    const url = new URL(referrer, `http://${host}`);
    if (url.host !== host) return fallback;
    return `${url.pathname}${url.search}${url.hash}`;
  } catch (err) {
    return fallback;
  }
}

async function completePlexLogin(req, authToken) {
  const plexUser = await fetchPlexUser(authToken);
  const role = resolveRole(plexUser);
  const config = loadConfig();
  const apps = config.apps || [];
  const generalSettings = resolveGeneralSettings(config);
  const plexApp = apps.find((appItem) => normalizeAppId(appItem?.id) === 'plex');
  let serverResource = null;
  let serverToken = '';
  let isServerOwner = false;

  if (!role) {
    pushLog({
      level: 'error',
      app: 'plex',
      action: 'login.callback',
      message: 'Access denied for Plex user.',
      meta: { user: plexUser?.username || plexUser?.title || plexUser?.email || '' },
    });
    throw new Error('Access denied for this Plex user.');
  }

  if (plexApp) {
    try {
      const resources = await fetchPlexResources(authToken);
      serverResource = resolvePlexServerResource(resources, {
        machineId: String(plexApp?.plexMachine || '').trim(),
        localUrl: plexApp?.localUrl,
        remoteUrl: plexApp?.remoteUrl,
        plexHost: plexApp?.plexHost,
      });
      serverToken = String(serverResource?.accessToken || '').trim();
      isServerOwner = isPlexServerOwner(serverResource);
      const debug = buildPlexResourceDebug(resources, {
        machineId: String(plexApp?.plexMachine || '').trim(),
        localUrl: plexApp?.localUrl,
        remoteUrl: plexApp?.remoteUrl,
        plexHost: plexApp?.plexHost,
      });
      pushLog({
        level: 'info',
        app: 'plex',
        action: 'login.resources',
        message: serverToken ? 'Plex server token resolved.' : 'Plex server token not resolved.',
        meta: debug,
      });
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'plex',
        action: 'login.resources',
        message: safeMessage(err) || 'Failed to resolve Plex server resources.',
      });
      if (generalSettings.restrictGuests) {
        const denied = new Error('Access restricted to Plex server users.');
        denied.status = 403;
        throw denied;
      }
    }
  }

  if (generalSettings.restrictGuests && plexApp) {
    const hasAccess = Boolean(serverResource && String(serverResource.accessToken || '').trim());
    if (!hasAccess) {
      pushLog({
        level: 'error',
        app: 'plex',
        action: 'login.denied',
        message: 'Blocked Plex guest user login.',
        meta: { user: plexUser?.username || plexUser?.title || plexUser?.email || '' },
      });
      const denied = new Error('Access restricted to Plex server users.');
      denied.status = 403;
      throw denied;
    }
  }

  const rawAvatar = plexUser.thumb || plexUser.avatar || plexUser.photo || null;
  const avatar = rawAvatar
    ? (rawAvatar.startsWith('http') ? rawAvatar : `https://plex.tv${rawAvatar}`)
    : null;

  req.session.user = {
    username: plexUser.username || plexUser.title || 'Plex User',
    email: plexUser.email || null,
    avatar,
    avatarFallback: '/icons/user-profile.svg',
    role,
    source: 'plex',
  };
  req.session.viewRole = null;
  req.session.authToken = authToken;
  req.session.plexServerToken = null;
  req.session.pinId = null;

  const loginIdentifier = plexUser.email || plexUser.username || plexUser.title || plexUser.id || '';
  let nextConfig = updateUserLogins(config, {
    identifier: loginIdentifier,
    launcharr: true,
  });
  let configUpdated = nextConfig !== config;

  if (serverToken) {
    req.session.plexServerToken = serverToken;
    if (role === 'admin' && isServerOwner && serverToken !== plexApp.plexToken) {
      const nextApps = apps.map((appItem) => (normalizeAppId(appItem?.id) === 'plex'
        ? { ...appItem, plexToken: serverToken }
        : appItem
      ));
      nextConfig = { ...nextConfig, apps: nextApps };
      configUpdated = true;
    } else if (role === 'admin' && !isServerOwner && serverToken !== plexApp.plexToken) {
      pushLog({
        level: 'info',
        app: 'plex',
        action: 'token.save',
        message: 'Skipped Plex token update; only server owner can update token.',
        meta: { user: req.session?.user?.username || '' },
      });
    }
  }

  if (configUpdated) {
    saveConfig(nextConfig);
  }

  pushLog({
    level: 'info',
    app: 'plex',
    action: 'login.success',
    message: 'Plex login successful.',
    meta: { user: req.session.user.username || '', role },
  });
}

function plexHeaders() {
  return {
    'X-Plex-Client-Identifier': CLIENT_ID,
    'X-Plex-Product': PRODUCT,
    'X-Plex-Platform': PLATFORM,
    'X-Plex-Device': PLATFORM,
    'X-Plex-Device-Name': DEVICE_NAME,
  };
}

async function fetchPlexResources(token) {
  const res = await fetch('https://plex.tv/api/v2/resources?includeHttps=1&includeRelay=1', {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      'X-Plex-Token': token,
      ...plexHeaders(),
    },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Plex resources request failed (${res.status}): ${text.slice(0, 180)}`);
  }
  return res.json();
}

function resolvePlexServerToken(resources, { machineId, localUrl, remoteUrl, plexHost }) {
  const match = resolvePlexServerResource(resources, { machineId, localUrl, remoteUrl, plexHost });
  return match?.accessToken || '';
}

function resolvePlexServerResource(resources, { machineId, localUrl, remoteUrl, plexHost }) {
  const list = Array.isArray(resources)
    ? resources
    : (resources?.MediaContainer?.Device || resources?.mediaContainer?.Device || []);
  const servers = (Array.isArray(list) ? list : [])
    .filter((item) => String(item?.provides || '').includes('server'));
  const normalizeId = (value) => String(value || '').trim();
  const machine = normalizeId(machineId);
  if (machine) {
    const match = servers.find((item) =>
      normalizeId(item?.clientIdentifier || item?.clientidentifier) === machine
    );
    if (match) return match;
  }

  const toHost = (value) => {
    if (!value) return '';
    try {
      return new URL(String(value)).hostname.toLowerCase();
    } catch (err) {
      return String(value).replace(/^https?:\/\//i, '').replace(/\/.*$/, '').toLowerCase();
    }
  };
  const hostCandidates = [localUrl, remoteUrl, plexHost]
    .map(toHost)
    .filter(Boolean);

  if (hostCandidates.length) {
    for (const server of servers) {
      const connections = Array.isArray(server?.connections)
        ? server.connections
        : (Array.isArray(server?.Connection) ? server.Connection : []);
      const connectionHosts = connections
        .map((conn) => toHost(conn?.uri || conn?.address || conn?.host))
        .filter(Boolean);
      if (connectionHosts.some((host) => hostCandidates.includes(host))) {
        return server;
      }
    }
  }

  if (servers.length === 1) return servers[0];
  return null;
}

function isPlexServerOwner(server) {
  if (!server) return false;
  const value = server?.owned ?? server?.owner ?? server?.isOwner;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value === 1;
  if (typeof value === 'string') return value.toLowerCase() === 'true' || value === '1';
  return false;
}

function buildPlexResourceDebug(resources, { machineId, localUrl, remoteUrl, plexHost }) {
  const list = Array.isArray(resources)
    ? resources
    : (resources?.MediaContainer?.Device || resources?.mediaContainer?.Device || []);
  const servers = (Array.isArray(list) ? list : [])
    .filter((item) => String(item?.provides || '').includes('server'));
  const normalizeId = (value) => String(value || '').trim();
  const machine = normalizeId(machineId);
  const toHost = (value) => {
    if (!value) return '';
    try {
      return new URL(String(value)).hostname.toLowerCase();
    } catch (err) {
      return String(value).replace(/^https?:\/\//i, '').replace(/\/.*$/, '').toLowerCase();
    }
  };
  const hostCandidates = [localUrl, remoteUrl, plexHost]
    .map(toHost)
    .filter(Boolean);
  const machineMatch = machine
    ? servers.some((item) => normalizeId(item?.clientIdentifier || item?.clientidentifier) === machine)
    : false;
  const hostMatch = hostCandidates.length
    ? servers.some((server) => {
      const connections = Array.isArray(server?.connections)
        ? server.connections
        : (Array.isArray(server?.Connection) ? server.Connection : []);
      const connectionHosts = connections
        .map((conn) => toHost(conn?.uri || conn?.address || conn?.host))
        .filter(Boolean);
      return connectionHosts.some((host) => hostCandidates.includes(host));
    })
    : false;
  const serverSummaries = servers.map((server) => {
    const connections = Array.isArray(server?.connections)
      ? server.connections
      : (Array.isArray(server?.Connection) ? server.Connection : []);
    const connectionHosts = connections
      .map((conn) => toHost(conn?.uri || conn?.address || conn?.host))
      .filter(Boolean);
    return {
      name: String(server?.name || ''),
      clientIdentifier: normalizeId(server?.clientIdentifier || server?.clientidentifier),
      connectionHosts,
    };
  });
  return {
    serverCount: servers.length,
    machineMatch,
    hostMatch,
    hasMachineId: Boolean(machine),
    hostCandidates,
    servers: serverSummaries,
  };
}

async function fetchLatestDockerTag() {
  const res = await fetch('https://registry.hub.docker.com/v2/repositories/mickygx/launcharr/tags?page_size=50', {
    method: 'GET',
    headers: { Accept: 'application/json' },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Docker Hub tag lookup failed (${res.status}): ${text.slice(0, 180)}`);
  }
  const payload = await res.json();
  const tags = Array.isArray(payload?.results) ? payload.results : [];
  const parsed = tags
    .map((tag) => {
      const name = String(tag?.name || '').trim();
      const semver = parseSemver(name);
      if (!semver) return null;
      return { name: normalizeVersionTag(name), semver };
    })
    .filter(Boolean);
  if (!parsed.length) return '';
  parsed.sort((a, b) => compareSemver(b.semver, a.semver));
  return parsed[0].name;
}

function parsePlexUsers(xmlText, options = {}) {
  const machineId = String(options.machineId || '').trim();
  const users = [];
  const blocks = String(xmlText || '').match(/<User\b[^>]*>[\s\S]*?<\/User>/g) || [];
  blocks.forEach((block) => {
    const userTagMatch = block.match(/<User\b[^>]*>/);
    if (!userTagMatch) return;
    const attrs = {};
    userTagMatch[0].replace(/(\w+)="([^"]*)"/g, (_m, key, value) => {
      attrs[key] = value;
      return '';
    });
    const serverTags = block.match(/<Server\b[^>]*>/g) || [];
    const servers = serverTags.map((tag) => {
      const serverAttrs = {};
      tag.replace(/(\w+)="([^"]*)"/g, (_m, key, value) => {
        serverAttrs[key] = value;
        return '';
      });
      return serverAttrs;
    });
    let serverMatch = null;
    if (machineId) {
      serverMatch = servers.find((server) => String(server.machineIdentifier || '') === machineId) || null;
    }
    if (!serverMatch) {
      serverMatch = servers.find((server) => String(server.owned || '') === '1') || null;
    }
    if (!serverMatch) {
      serverMatch = servers[0] || null;
    }
    users.push({
      id: attrs.id || attrs.uuid || '',
      uuid: attrs.uuid || '',
      username: attrs.username || '',
      email: attrs.email || '',
      title: attrs.title || '',
      lastSeenAt: serverMatch?.lastSeenAt || serverMatch?.last_seen_at || '',
    });
  });
  return users;
}

async function ensureKeypair() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }

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

function buildAuthUrl(code, pinId, baseUrl = BASE_URL) {
  const params = new URLSearchParams();
  params.set('clientID', CLIENT_ID);
  params.set('code', code);
  const callbackUrl = buildAppApiUrl(baseUrl, 'oauth/callback');
  if (pinId) callbackUrl.searchParams.set('pinId', String(pinId));
  params.set('forwardUrl', callbackUrl.toString());
  params.set('context[device][product]', PRODUCT);
  params.set('context[device][platform]', PLATFORM);

  return `https://app.plex.tv/auth#?${params.toString()}`;
}

async function exchangePin(pinId) {
  const url = `https://plex.tv/api/v2/pins/${pinId}`;
  const res = await fetch(url, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      ...plexHeaders(),
    },
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`PIN exchange failed (${res.status}): ${text}`);
  }

  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch (err) {
    throw new Error(`PIN exchange JSON parse failed: ${text.slice(0, 180)}`);
  }
  if (!data?.authToken) {
    pushLog({
      level: 'error',
      app: 'plex',
      action: 'login.pin',
      message: 'PIN exchange returned no authToken.',
      meta: { pinId: String(pinId || ''), payload: data || {} },
    });
  }
  return data.authToken || null;
}

async function exchangePinWithRetry(pinId, attempts = 20, delayMs = 1000) {
  let lastError = '';
  for (let i = 0; i < attempts; i += 1) {
    try {
      const token = await exchangePin(pinId);
      if (token) {
        return { token, attempts: i + 1, error: '' };
      }
    } catch (err) {
      lastError = safeMessage(err) || '';
    }
    await sleep(delayMs);
  }
  return { token: null, attempts, error: lastError };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchPlexUser(token) {
  const res = await fetch('https://plex.tv/api/v2/user', {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      'X-Plex-Token': token,
      ...plexHeaders(),
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Plex user lookup failed (${res.status}): ${text}`);
  }

  return res.json();
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

// ─── Widget Bar helpers ──────────────────────────────────────────────────────

function getWidgetStatType(typeId) {
  const normalizedTypeId = String(typeId || '').trim().toLowerCase();
  if (!normalizedTypeId) return null;
  const knownType = WIDGET_STAT_TYPE_BY_ID.get(normalizedTypeId);
  if (knownType) return knownType;
  return {
    typeId: normalizedTypeId,
    name: getBaseAppTitle(normalizedTypeId),
    icon: '/icons/app.svg',
    metricFields: [
      { key: 'http_status', label: 'HTTP' },
      { key: 'latency_ms', label: 'Latency' },
    ],
    fallback: true,
  };
}

function buildWidgetBarId() {
  return `wbar-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function normalizeWidgetBarId(raw) {
  return String(raw || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^[-_]+|[-_]+$/g, '');
}

function normalizeWidgetId(raw) {
  return String(raw || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^[-_]+|[-_]+$/g, '');
}

function buildWidgetRowId() {
  return `wrow-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function normalizeWidgetRowSettings(raw) {
  const s = (raw && typeof raw === 'object') ? raw : {};
  const maxCols = Number.isFinite(Number(s.maxCols)) ? Math.max(0, Math.min(20, Math.round(Number(s.maxCols)))) : 0;
  return { maxCols, fixedWidth: Boolean(s.fixedWidth), scroll: Boolean(s.scroll), fill: Boolean(s.fill) };
}

function normalizeWidgetRow(raw, defaults = {}) {
  if (!raw || typeof raw !== 'object') return null;
  const id = normalizeWidgetBarId(raw.id || defaults.id || '');
  if (!id) return null;
  const orderRaw = Number(raw.order ?? defaults.order);
  const order = Number.isFinite(orderRaw) ? orderRaw : 0;
  const settings = normalizeWidgetRowSettings(raw.settings);
  const seenWidgetIds = new Set();
  const widgets = (Array.isArray(raw.widgets) ? raw.widgets : []).map((w) => {
    const n = normalizeWidgetInBar(w);
    if (!n || seenWidgetIds.has(n.id)) return null;
    seenWidgetIds.add(n.id);
    return n;
  }).filter(Boolean);
  return { id, order, settings, widgets };
}

function normalizeWidgetInBar(entry) {
  if (!entry || typeof entry !== 'object') return null;
  // System widget path (no appId required)
  const systemType = String(entry.systemType || entry.system_type || '').trim().toLowerCase();
  if (systemType) {
    return normalizeSystemWidget(entry);
  }
  const appId = String(entry.appId || entry.app_id || '').trim();
  if (!appId) return null;
  const id = normalizeWidgetId(entry.id || '') || normalizeWidgetId(`wg-${appId}-${Math.random().toString(36).slice(2, 6)}`);
  if (!id) return null;
  const hasSelectedMetricKeys =
    Object.prototype.hasOwnProperty.call(entry, 'selectedMetricKeys') ||
    Object.prototype.hasOwnProperty.call(entry, 'selected_metric_keys') ||
    Object.prototype.hasOwnProperty.call(entry, 'metricKeys') ||
    Object.prototype.hasOwnProperty.call(entry, 'metric_keys');
  const hasMetricColumns =
    Object.prototype.hasOwnProperty.call(entry, 'metricColumns') ||
    Object.prototype.hasOwnProperty.call(entry, 'metric_columns') ||
    Object.prototype.hasOwnProperty.call(entry, 'metricCols') ||
    Object.prototype.hasOwnProperty.call(entry, 'metric_cols');
  let selectedMetricKeys;
  let metricColumns;
  if (hasSelectedMetricKeys) {
    const rawMetricKeys = entry.selectedMetricKeys ?? entry.selected_metric_keys ?? entry.metricKeys ?? entry.metric_keys;
    selectedMetricKeys = Array.from(new Set(
      (Array.isArray(rawMetricKeys) ? rawMetricKeys : [])
        .map((k) => String(k || '').trim().toLowerCase())
        .filter(Boolean)
    ));
  }
  if (hasMetricColumns) {
    const rawMetricCols = Number(entry.metricColumns ?? entry.metric_columns ?? entry.metricCols ?? entry.metric_cols);
    metricColumns = Number.isFinite(rawMetricCols) ? Math.max(1, Math.min(4, Math.round(rawMetricCols))) : 2;
  }
  const hasSelectedLibraryKeys = Object.prototype.hasOwnProperty.call(entry, 'selectedLibraryKeys');
  let selectedLibraryKeys;
  if (hasSelectedLibraryKeys) {
    const rawLibKeys = entry.selectedLibraryKeys;
    if (rawLibKeys && typeof rawLibKeys === 'object' && !Array.isArray(rawLibKeys)) {
      const norm = {};
      for (const [mk, libKeys] of Object.entries(rawLibKeys)) {
        const key = String(mk || '').trim().toLowerCase();
        if (!key) continue;
        const libs = Array.isArray(libKeys) ? libKeys.map((k) => String(k || '').trim()).filter(Boolean) : [];
        if (libs.length) norm[key] = libs;
      }
      if (Object.keys(norm).length) selectedLibraryKeys = norm;
    }
  }
  const normalized = { id, appId };
  if (selectedMetricKeys !== undefined) normalized.selectedMetricKeys = selectedMetricKeys;
  if (metricColumns !== undefined) normalized.metricColumns = metricColumns;
  if (selectedLibraryKeys !== undefined) normalized.selectedLibraryKeys = selectedLibraryKeys;
  return normalized;
}

function normalizeWidgetBar(raw, defaults = {}) {
  if (!raw || typeof raw !== 'object') return null;
  const id = normalizeWidgetBarId(raw.id || defaults.id || '');
  if (!id) return null;
  const name = String(raw.name ?? defaults.name ?? '').trim() || 'Widget Bar';
  const icon = normalizeDashboardIcon(raw.icon ?? defaults.icon, '/icons/dashboard.svg');
  const rawHasVisibilityRoles = Object.prototype.hasOwnProperty.call(raw, 'visibilityRoles');
  const defaultsHasVisibilityRoles = defaults && Object.prototype.hasOwnProperty.call(defaults, 'visibilityRoles');
  const visibilityRoles = rawHasVisibilityRoles
    ? normalizeDashboardVisibilityRoles(raw.visibilityRoles)
    : defaultsHasVisibilityRoles
      ? normalizeDashboardVisibilityRoles(defaults.visibilityRoles)
      : normalizeDashboardVisibilityRoles(raw.visibilityRoles, raw.visibilityRole ?? defaults.visibilityRole ?? 'user');
  const visibilityRole = deriveDashboardLegacyVisibilityRole(
    visibilityRoles,
    normalizeVisibilityRole(raw.visibilityRole ?? defaults.visibilityRole ?? 'user', 'user')
  );
  const refreshRaw = Number(raw.refreshSeconds ?? defaults.refreshSeconds);
  const refreshSeconds = Number.isFinite(refreshRaw)
    ? (Math.round(refreshRaw) <= 0 ? 0 : Math.max(15, Math.min(3600, Math.round(refreshRaw))))
    : 60;
  const orderRaw = Number(raw.order ?? defaults.order);
  const order = Number.isFinite(orderRaw) ? orderRaw : 0;
  // Multi-row: use rows array if present; otherwise migrate legacy flat widgets array
  let rawRows;
  if (Array.isArray(raw.rows) && raw.rows.length > 0) {
    rawRows = raw.rows;
  } else if (Array.isArray(raw.widgets) && raw.widgets.length > 0) {
    // Legacy migration: wrap flat widgets in a single row with default settings
    rawRows = [{ id: buildWidgetRowId(), order: 10, settings: {}, widgets: raw.widgets }];
  } else {
    rawRows = [];
  }
  const seenRowIds = new Set();
  const rows = rawRows.map((r, i) => {
    const normalized = normalizeWidgetRow(r, { order: (i + 1) * 10 });
    if (!normalized || seenRowIds.has(normalized.id)) return null;
    seenRowIds.add(normalized.id);
    return normalized;
  }).filter(Boolean);
  // Ensure at least one row
  const finalRows = rows.length > 0 ? rows : [{ id: buildWidgetRowId(), order: 10, settings: normalizeWidgetRowSettings({}), widgets: [] }];
  return { id, name, icon, visibilityRole, visibilityRoles, refreshSeconds, order, rows: finalRows };
}

function resolveWidgetBars(config, apps, role, opts = {}) {
  const { includeHidden = false } = opts;
  const roleKey = parseVisibilityRole(role) || 'user';
  const rawBars = Array.isArray(config?.widgetBars) ? config.widgetBars : [];
  const appList = Array.isArray(apps) ? apps : [];
  const appByNormalizedId = new Map(
    appList
      .filter((a) => !a?.removed)
      .map((a) => [normalizeAppId(a?.id), a])
      .filter(([id]) => Boolean(id))
  );
  const seenBarIds = new Set();
  return rawBars
    .map((raw, index) => {
      const normalized = normalizeWidgetBar(raw, { order: (index + 1) * 10 });
      if (!normalized) return null;
      if (seenBarIds.has(normalized.id)) return null;
      seenBarIds.add(normalized.id);
      if (!includeHidden) {
        const allowedRoles = Array.isArray(normalized.visibilityRoles) ? normalized.visibilityRoles : [];
        if (!allowedRoles.includes(roleKey)) return null;
      }
      const resolvedRows = normalized.rows.map((row) => {
        const resolvedWidgets = row.widgets.map((widget) => {
          // System widget — resolved directly (no app lookup needed)
          if (widget.systemType) {
            const sysTypeDef = SYSTEM_WIDGET_TYPE_BY_ID.get(widget.systemType);
            return {
              ...widget,
              icon: sysTypeDef ? sysTypeDef.icon : '/icons/app.svg',
              typeName: sysTypeDef ? sysTypeDef.label : widget.systemType,
              metricFields: [],
              available: true,
            };
          }
          const app = appByNormalizedId.get(normalizeAppId(widget.appId));
          const baseId = app ? getAppBaseId(app.id) : '';
          const typeDef = baseId ? getWidgetStatType(baseId) : null;
          const metricFields = Array.isArray(typeDef?.metricFields)
            ? typeDef.metricFields
                .map((f) => ({ key: String(f?.key || '').trim(), label: String(f?.label || f?.key || '').trim() }))
                .filter((f) => f.key)
            : [];
          const metricFieldKeys = new Set(metricFields.map((f) => f.key));
          const selectedMetricKeys = Array.isArray(widget.selectedMetricKeys)
            ? widget.selectedMetricKeys
                .map((k) => String(k || '').trim().toLowerCase())
                .filter((k) => !metricFieldKeys.size || metricFieldKeys.has(k))
            : undefined;
          return {
            ...widget,
            typeId: baseId || '',
            icon: resolvePersistedAppIconPath(app) || typeDef?.icon || '/icons/app.svg',
            typeName: app ? (app.name || typeDef?.name || widget.appId) : (typeDef?.name || widget.appId),
            metricFields,
            ...(Number.isFinite(Number(widget.metricColumns)) ? { metricColumns: Math.max(1, Math.min(4, Math.round(Number(widget.metricColumns)))) } : {}),
            ...(selectedMetricKeys !== undefined ? { selectedMetricKeys } : {}),
            ...(widget.selectedLibraryKeys && typeof widget.selectedLibraryKeys === 'object' && !Array.isArray(widget.selectedLibraryKeys) ? { selectedLibraryKeys: widget.selectedLibraryKeys } : {}),
            available: Boolean(app),
          };
        });
        return { ...row, widgets: resolvedWidgets };
      });
      return { ...normalized, rows: resolvedRows };
    })
    .filter(Boolean)
    .sort((a, b) => (Number(a.order) || 0) - (Number(b.order) || 0));
}

function serializeWidgetBars(bars) {
  return (Array.isArray(bars) ? bars : [])
    .map((bar) => normalizeWidgetBar(bar))
    .filter(Boolean)
    .map((bar) => ({
      id: bar.id,
      name: bar.name,
      icon: normalizeDashboardIcon(bar.icon, '/icons/dashboard.svg'),
      visibilityRole: bar.visibilityRole,
      visibilityRoles: Array.isArray(bar.visibilityRoles) ? bar.visibilityRoles : dashboardVisibilityRolesFromLegacyMinRole(bar.visibilityRole),
      refreshSeconds: bar.refreshSeconds,
      order: bar.order,
      rows: bar.rows.map((r) => ({
        id: r.id,
        order: r.order,
        settings: r.settings,
        widgets: r.widgets.map((w) => {
          if (w.systemType) {
            return { id: w.id, systemType: w.systemType, systemConfig: w.systemConfig || {} };
          }
          return {
            id: w.id,
            appId: w.appId,
            ...(Number.isFinite(Number(w.metricColumns)) ? { metricColumns: Math.max(1, Math.min(4, Math.round(Number(w.metricColumns)))) } : {}),
            ...(Array.isArray(w.selectedMetricKeys) ? { selectedMetricKeys: w.selectedMetricKeys } : {}),
            ...(w.selectedLibraryKeys && typeof w.selectedLibraryKeys === 'object' && !Array.isArray(w.selectedLibraryKeys) ? { selectedLibraryKeys: w.selectedLibraryKeys } : {}),
          };
        }),
      })),
    }));
}

function resolveNextWidgetBarOrder(config) {
  const bars = Array.isArray(config?.widgetBars) ? config.widgetBars : [];
  const maxOrder = bars.reduce((max, bar) => {
    const v = Number(bar?.order);
    return (Number.isFinite(v) && v > max) ? v : max;
  }, 0);
  return maxOrder + 10;
}

function resolveNextWidgetRowOrder(bar) {
  const rows = Array.isArray(bar?.rows) ? bar.rows : [];
  const maxOrder = rows.reduce((max, row) => {
    const v = Number(row?.order);
    return (Number.isFinite(v) && v > max) ? v : max;
  }, 0);
  return maxOrder + 10;
}

function resolveWidgetBarTypes(apps) {
  const appList = Array.isArray(apps) ? apps : [];
  const seenTypeIds = new Set();
  return appList
    .filter((a) => !a?.removed)
    .map((a) => getAppBaseId(a?.id))
    .filter((typeId) => {
      if (!typeId) return false;
      if (seenTypeIds.has(typeId)) return false;
      seenTypeIds.add(typeId);
      return true;
    })
    .map((typeId) => getWidgetStatType(typeId))
    .filter(Boolean);
}

// ─── Route registration via context injection ───────────────────────────────
// ctx is built here (after all const/let/function definitions) so every
// entry is guaranteed to be initialised before it is referenced.
const _routeCtx = {
  // middleware
  requireUser,
  requireAdmin,
  requireSettingsAdmin,
  requireActualAdmin,
  // config
  loadConfig,
  saveConfig,
  // auth / session helpers
  getActualRole,
  getEffectiveRole,
  canAccessDashboardApp,
  // logging
  pushLog,
  LOG_BUFFER,
  // version
  normalizeVersionTag,
  APP_VERSION,
  VERSION_CACHE_TTL_MS,
  buildReleaseNotesUrl,
  loadReleaseHighlights,
  fetchLatestDockerTag,
  // navigation helpers
  resolveRoleSwitchRedirectPath,
  // onboarding
  resolveOnboardingSettings,
  hasActiveOnboardingApps,
  shouldShowQuickStartOnboarding,
  // auth
  hasLocalAdmin,
  resolveLocalUsers,
  serializeLocalUsers,
  verifyPassword,
  hashPassword,
  setSessionUser,
  updateUserLogins,
  resolvePublicBaseUrl,
  buildAppApiUrl,
  exchangePinWithRetry,
  exchangePin,
  completePlexLogin,
  safeMessage,
  // plex
  fetchPlexResources,
  resolvePlexServerToken,
  resolvePlexMachineIdentifier,
  normalizeBaseUrl,
  resolveLaunchUrl,
  uniqueList,
  getPlexDiscoveryWatchlisted,
  resolvePlexDiscoverRatingKey,
  fetchPlexDiscoveryMetadata,
  fetchPlexDiscoveryActions,
  buildWatchlistStateFromActions,
  updatePlexWatchlist,
  fetchPlexWatchlistState,
  // media (jellyfin/emby/pulsarr/seerr)
  resolveJellyfinCandidates,
  resolveEmbyCandidates,
  buildJellyfinImageUrl,
  buildEmbyImageUrl,
  formatDurationFromTicks,
  formatRelativeTime,
  toPaddedEpisode,
  mapJellyfinKind,
  fetchJellyfinJson,
  fetchJellyfinRecentItems,
  fetchEmbyJson,
  fetchEmbyRecentItems,
  mapSeerrRequestStatus,
  mapSeerrFilter,
  fetchSeerrJson,
  resolveRequestApiCandidates,
  // pages — access control
  canAccess,
  canAccessOverviewElement,
  // pages — dashboard
  resolveDashboardSelection,
  resolveRequestedDashboardId,
  resolveBestRoleLandingPath,
  applyDashboardStateSnapshot,
  resolveCategoryEntries,
  buildAppBaseUrls,
  resolveArrDashboardCombineSettings,
  resolveMediaDashboardCombineSettings,
  resolveCombinedQueueDisplaySettings,
  resolveDownloaderDashboardCombineSettings,
  getNavApps,
  buildNavCategories,
  buildCategoryRank,
  canAccessDashboardElement,
  mergeOverviewElementSettings,
  resolveArrDashboardCombinedCards,
  resolveDownloaderDashboardCards,
  resolveMediaDashboardCards,
  isAppInSet,
  normalizeAppId,
  canAccessCombinedDashboardVisibility,
  getArrCombineSection,
  getDownloaderCombineSection,
  getMediaCombineSection,
  normalizeCombinedCardToken,
  buildArrCombinedDisplayMeta,
  buildDownloaderCombinedDisplayMeta,
  buildMediaCombinedDisplayMeta,
  getMultiInstanceBaseIds,
  getMultiInstancePlaceholderMap,
  mergeTautulliCardSettings,
  resolveDashboardDefinitions,
  resolvePersistedAppIconPath,
  // pages — launch
  resolveDeepLaunchUrl,
  resolveRoleAwareLaunchUrl,
  resolveEffectiveLaunchMode,
  resolveIframeLaunchUrl,
  hasEmbeddedUrlCredentials,
  stripUrlEmbeddedCredentials,
  buildRommCookiePrimingPlan,
  bootstrapRommIframeSession,
  resolveAppApiCandidates,
  evaluateRommCookiePrimingCompatibility,
  prepareRommPrimedSetCookies,
  logRommLaunchServerDiagnostic,
  sendClientLaunchRedirectPage,
  normalizeMenu,
  // pages — app-settings
  loadAdmins,
  // pages — user-settings
  resolveGeneralSettings,
  findLocalUserIndex,
  // pages — dashboard constants
  DASHBOARD_MAIN_ID,
  DEFAULT_DASHBOARD_ICON,
  ARR_APP_IDS,
  DOWNLOADER_APP_IDS,
  MEDIA_APP_IDS,
  ARR_COMBINE_SECTIONS,
  DOWNLOADER_COMBINE_SECTIONS,
  MEDIA_COMBINE_SECTIONS,
  ARR_COMBINED_SECTION_PREFIX,
  DOWNLOADER_COMBINED_SECTION_PREFIX,
  MEDIA_COMBINED_SECTION_PREFIX,
  // api-arr — indexer helpers
  normalizeIndexerProtocol,
  extractTopLevelCategoryIds,
  toTopLevelCategoryOptions,
  toTopLevelCategoryId,
  parseJackettJsonItems,
  parseJackettTorznabItems,
  // api-arr — mappers
  mapBazarrQueueItem,
  mapAutobrrQueueItem,
  normalizeMaintainerrTmdbKind,
  buildBasicAuthHeader,
  buildMaintainerrTmdbImageUrl,
  mapMaintainerrRuleItem,
  normalizeMaintainerrMediaKind,
  mapMaintainerrLibraryItem,
  pickFirstNonEmpty,
  mapMaintainerrCollectionMediaItem,
  parseFiniteNumber,
  extractCleanuparrList,
  mapCleanuparrStrikeItem,
  mapCleanuparrEventItem,
  // api-arr — downloader helpers
  getAppBaseId,
  fetchTransmissionQueue,
  fetchNzbgetQueue,
  fetchQbittorrentQueue,
  fetchSabnzbdQueue,
  // api-specialty — romm data
  injectBasicAuthIntoUrl,
  buildCookieHeaderFromSetCookies,
  getRommCsrfTokenFromSetCookies,
  extractRommList,
  mapRommConsoleItem,
  mapRommRecentlyAddedItem,
  // api-specialty — widget bars
  WIDGET_STAT_TYPES,
  WIDGET_STAT_TYPE_BY_ID,
  getWidgetStatType,
  SYSTEM_WIDGET_TYPES,
  SYSTEM_WIDGET_TYPE_BY_ID,
  SYSTEM_WIDGET_SEARCH_PROVIDERS,
  SYSTEM_WIDGET_TIMEZONES,
  normalizeSystemWidget,
  buildWidgetBarId,
  buildWidgetRowId,
  normalizeWidgetBarId,
  normalizeWidgetId,
  normalizeWidgetInBar,
  normalizeWidgetRowSettings,
  normalizeWidgetRow,
  normalizeWidgetBar,
  resolveWidgetBars,
  serializeWidgetBars,
  resolveNextWidgetBarOrder,
  resolveNextWidgetRowOrder,
  resolveWidgetBarTypes,
  // constants
  PRODUCT,
  PLATFORM,
  DEVICE_NAME,
  CLIENT_ID,
  LOCAL_AUTH_MIN_PASSWORD,
  validateLocalPasswordStrength,
  // settings — constants
  DASHBOARD_MAX_COUNT,
  DATA_DIR,
  DEFAULT_GENERAL_SETTINGS,
  DEFAULT_LOG_SETTINGS,
  ENABLE_ARR_UNIFIED_CARDS,
  ENABLE_DOWNLOADER_UNIFIED_CARDS,
  USER_AVATAR_BASE,
  // settings — helpers
  applyLogRetention,
  buildCombinedCardId,
  buildDashboardElementsFromRequest,
  buildDashboardInstanceId,
  buildDashboardSettingsRedirect,
  buildDisabledMenuAccess,
  buildDisabledOverviewElements,
  buildEmptyDashboardStateSnapshot,
  buildMenuAccessConfig,
  buildNextInstanceId,
  buildOverviewElementsFromRequest,
  buildTautulliCardsFromDashboardRequest,
  buildTautulliCardsFromRequest,
  canManageWithDefaultAppManager,
  deepEqual,
  deleteCustomIcon,
  deriveDashboardLegacyVisibilityRole,
  extractDashboardStateSnapshot,
  fetchPlexHistoryLastSeenMap,
  getAppIconOptions,
  getArrCombineSectionIconPath,
  getArrCombineSectionLabel,
  getBaseAppTitle,
  getCategoryIconOptions,
  getCustomAppIconOptions,
  getCustomSystemIconOptions,
  getDefaultAppIconOptions,
  getDefaultInstanceName,
  getDefaultSystemIconOptions,
  getDownloaderCombineSectionIconPath,
  getDownloaderCombineSectionLabel,
  getInstanceSuffix,
  getMaxMultiInstancesForBase,
  getMediaCombineSectionIconPath,
  getMediaCombineSectionLabel,
  getMultiInstanceMaxMap,
  getMultiInstanceTitleMap,
  isValidEmail,
  loadCoAdmins,
  loadDefaultApps,
  loadDefaultCategories,
  loadSettingsReleases,
  migrateDeprecatedDashboardCards,
  normalizeBasePath,
  normalizeCategoryEntries,
  normalizeCategoryName,
  normalizeDashboardDisplayName,
  normalizeDashboardIcon,
  normalizeDashboardInstanceId,
  normalizeLaunchMode,
  normalizeLocalRole,
  normalizePlexLastSeen,
  normalizeSidebarAppButtonAction,
  normalizeSidebarButtonPressActions,
  normalizeStoredAvatarPath,
  normalizeThemeSettings,
  normalizeUserKey,
  normalizeVisibilityRole,
  parseCsv,
  parsePlexUsers,
  parseUserAvatarDataUrl,
  persistLogsToDisk,
  plexHeaders,
  resolveAppLaunchMode,
  resolveCategoryOrder,
  resolveCombinedDashboardVisibilityRole,
  resolveDashboardDefinitionVisibilityRoles,
  resolveDashboardVisibilityRolesFromRequest,
  resolveDefaultCategoryIcon,
  resolveDeprecatedDashboardCardDescriptor,
  resolveDeprecatedDashboardElementIdsForApp,
  resolveLogSettings,
  resolveNotificationSettings,
  resolveThemeDefaults,
  resolveThemePreferenceKey,
  resolveUserLogins,
  resolveUserThemePreferences,
  saveAdmins,
  saveCoAdmins,
  saveCustomAppIcon,
  saveCustomIcon,
  saveCustomUserAvatar,
  saveDashboardScopedConfig,
  sendAppriseNotification,
  serializeUserThemePreferences,
  slugifyId,
  supportsAppInstances,
  widgetStatsInternalToken: WIDGET_STATUS_INTERNAL_TOKEN,
  // mutable let — accessed via getter/setter so route files always see current value
  get versionCache() { return versionCache; },
  set versionCache(v) { versionCache = v; },
};

registerApiUtil(app, _routeCtx);
registerAuth(app, _routeCtx);
registerApiPlex(app, _routeCtx);
registerApiMedia(app, _routeCtx);
registerPages(app, _routeCtx);
registerApiArr(app, _routeCtx);
registerApiSpecialty(app, _routeCtx);
registerSettings(app, _routeCtx);
await registerLocalModules(app, _routeCtx);
