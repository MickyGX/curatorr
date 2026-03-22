// Media server adapter factory.
// Returns the correct adapter module for the configured server type.
//
// All adapters expose:
//   verifyConnection(url, credential)         → { ok, serverName, serverId }
//   getLibraries(url, credential)             → [{ key, title }]
//   getLibraryTracks(url, credential, keys)   → normalised track objects
//   registerWebhook(url, credential, hookUrl, ctx?)
//     → { ok, created?, webhookUrl, manual, reason? }
//
// Jellyfin/Emby also export:
//   authenticate(url, username, password)     → { token, userId }

import * as plex     from './plex.js';
import * as jellyfin from './jellyfin.js';
import * as emby     from './emby.js';

const ADAPTERS = { plex, jellyfin, emby };

/**
 * Returns the adapter for the given server type string.
 * Defaults to 'plex' for backward compatibility with existing installs
 * that pre-date multi-server support.
 *
 * @param {string} type  'plex' | 'jellyfin' | 'emby'
 */
export function getAdapter(type) {
  const key = String(type || 'plex').toLowerCase();
  const adapter = ADAPTERS[key];
  if (!adapter) throw new Error(`Unknown media server type: "${type}"`);
  return adapter;
}

/**
 * Returns the adapter for the currently configured server by reading config.
 *
 * @param {object} config  Curatorr config object (from loadConfig())
 */
export function getConfiguredAdapter(config) {
  const type = String(config?.mediaServer?.type || 'plex').toLowerCase();
  return getAdapter(type);
}

/**
 * Returns the credential (token/apiKey) for the active server from config.
 */
export function getConfiguredCredential(config) {
  const type = String(config?.mediaServer?.type || 'plex').toLowerCase();
  if (type === 'plex')     return String(config?.plex?.token || '');
  if (type === 'jellyfin') return String(config?.jellyfin?.apiKey || '');
  if (type === 'emby')     return String(config?.emby?.apiKey || '');
  return '';
}

/**
 * Returns the server URL for the active server from config.
 */
export function getConfiguredUrl(config) {
  const type = String(config?.mediaServer?.type || 'plex').toLowerCase();
  if (type === 'plex')     return String(config?.plex?.url || '');
  if (type === 'jellyfin') return String(config?.jellyfin?.url || '');
  if (type === 'emby')     return String(config?.emby?.url || '');
  return '';
}

/**
 * Returns the selected library keys for the active server from config.
 */
export function getConfiguredLibraryKeys(config) {
  const type = String(config?.mediaServer?.type || 'plex').toLowerCase();
  if (type === 'plex')     return config?.plex?.libraries || [];
  if (type === 'jellyfin') return config?.jellyfin?.libraries || [];
  if (type === 'emby')     return config?.emby?.libraries || [];
  return [];
}

export { plex, jellyfin, emby };
