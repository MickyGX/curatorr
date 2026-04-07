// Jellyfin media server adapter.
// Implements the shared media-server interface.
//
// Auth model: admin username + password exchanged once during setup for an API key.
// The API key is stored in config.jellyfin.apiKey — the password is never persisted.
//
// Moods: Jellyfin has no native mood field. moods is always [] for now.
// Tags (item.Tags) could be mapped to moods in a future enhancement.

const PAGE_SIZE = 1000;

function buildUrl(baseUrl, path) {
  let normalized = String(baseUrl || '').trim().replace(/\/+$/, '');
  if (!normalized) return new URL('about:blank');
  if (!/^https?:\/\//i.test(normalized)) normalized = `http://${normalized}`;
  const suffix = String(path || '').replace(/^\/+/, '');
  return new URL(`${normalized}/${suffix}`);
}

function authHeaders(apiKey) {
  return {
    'X-Emby-Token': String(apiKey || ''),
    Accept: 'application/json',
  };
}

function extractMusicbrainzUuid(value) {
  const match = String(value || '').match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
  return match ? match[0].toLowerCase() : '';
}

function extractProviderRecordingMbid(item = {}) {
  const providerIds = item?.ProviderIds && typeof item.ProviderIds === 'object' ? item.ProviderIds : {};
  const candidates = [
    providerIds.MusicBrainzTrack,
    providerIds.MusicBrainzRecording,
  ];
  for (const candidate of candidates) {
    const mbid = extractMusicbrainzUuid(candidate);
    if (mbid) return mbid;
  }
  return '';
}

function extractItemReleaseYear(item = {}) {
  const direct = Number(item?.ProductionYear || 0);
  if (Number.isInteger(direct) && direct >= 1900 && direct <= 2099) return direct;
  const match = String(item?.PremiereDate || '').trim().match(/^(\d{4})/);
  return match ? Number(match[1]) : null;
}

function extractItemReleaseDate(item = {}) {
  const raw = String(item?.PremiereDate || '').trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) return raw.slice(0, 10);
  const year = extractItemReleaseYear(item);
  return year ? String(year) : '';
}

// ── Auth ──────────────────────────────────────────────────────────────────────
// Exchange username + password for an API key. Returns the API key string.

export async function authenticate(url, username, password) {
  const u = buildUrl(url, 'Users/AuthenticateByName');
  const res = await fetch(u.toString(), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      // Jellyfin requires a minimal authorization header to call this endpoint
      'X-Emby-Authorization': 'MediaBrowser Client="Curatorr", Device="Server", DeviceId="curatorr-setup", Version="1.0"',
    },
    body: JSON.stringify({ Username: username, Pw: password }),
  });
  if (!res.ok) throw new Error(`Jellyfin authentication failed (HTTP ${res.status})`);
  const json = await res.json();
  const token = String(json?.AccessToken || '').trim();
  if (!token) throw new Error('Jellyfin did not return an access token');
  const userId = String(json?.User?.Id || '').trim();
  return { token, userId };
}

// ── API key creation ──────────────────────────────────────────────────────────
// Exchanges a session token (from AuthenticateByName) for a permanent admin API
// key. Jellyfin session tokens may expire; API keys are persistent and always
// have admin scope for admin users.

export async function createApiKey(url, sessionToken, appName = 'Curatorr') {
  const u = buildUrl(url, 'Auth/Keys');
  u.searchParams.set('app', appName);
  const res = await fetch(u.toString(), {
    method: 'POST',
    headers: authHeaders(sessionToken),
  });
  if (!res.ok) {
    // Not fatal — fall back to using the session token directly
    return null;
  }
  // After creating, list keys to find the one just created
  const listRes = await fetch(buildUrl(url, 'Auth/Keys').toString(), { headers: authHeaders(sessionToken) });
  if (!listRes.ok) return null;
  const json = await listRes.json();
  const keys = Array.isArray(json) ? json : (json?.Items || []);
  const match = [...keys].reverse().find((k) => String(k.AppName || '') === appName);
  return match ? String(match.AccessToken || '') : null;
}

// ── Connection verification ───────────────────────────────────────────────────

export async function verifyConnection(url, apiKey) {
  const u = buildUrl(url, 'System/Info');
  const res = await fetch(u.toString(), { headers: authHeaders(apiKey) });
  if (!res.ok) throw new Error(`Jellyfin returned HTTP ${res.status}`);
  const json = await res.json();
  return {
    ok: true,
    serverName: String(json?.ServerName || ''),
    serverId:   String(json?.Id || ''),
  };
}

// ── Library enumeration ───────────────────────────────────────────────────────

export async function getLibraries(url, apiKey) {
  const u = buildUrl(url, 'Library/VirtualFolders');
  const res = await fetch(u.toString(), { headers: authHeaders(apiKey) });
  if (!res.ok) throw new Error(`Jellyfin library fetch failed (${res.status})`);
  const json = await res.json();
  const folders = Array.isArray(json) ? json : (json?.Items || []);
  return folders
    .filter((f) => String(f.CollectionType || '').toLowerCase() === 'music')
    .map((f) => ({
      key:   String(f.ItemId || f.Id || ''),
      title: String(f.Name || ''),
    }));
}

// ── Track cache population ────────────────────────────────────────────────────

export async function getLibraryTracks(url, apiKey, libraryKeys) {
  const tracks = [];

  for (const key of libraryKeys) {
    let startIndex = 0;
    let totalCount = null;

    while (totalCount === null || startIndex < totalCount) {
      const u = buildUrl(url, 'Items');
      u.searchParams.set('ParentId', key);
      u.searchParams.set('IncludeItemTypes', 'Audio');
      u.searchParams.set('Recursive', 'true');
      u.searchParams.set('Fields', 'Genres,Tags,Artists,AlbumArtist,Album,RunTimeTicks,ProviderIds,Path,ProductionYear,PremiereDate');
      u.searchParams.set('StartIndex', String(startIndex));
      u.searchParams.set('Limit', String(PAGE_SIZE));

      const res = await fetch(u.toString(), { headers: authHeaders(apiKey) });
      if (!res.ok) break;
      const json = await res.json();

      if (totalCount === null) totalCount = Number(json?.TotalRecordCount ?? 0);
      const items = json?.Items || [];
      if (!items.length) break;

      for (const item of items) {
        tracks.push({
          ratingKey:   String(item.Id || ''),
          artistName:  String(item.AlbumArtist || item.Artists?.[0] || ''),
          trackTitle:  String(item.Name || ''),
          albumName:   String(item.Album || ''),
          recordingMbid: extractProviderRecordingMbid(item),
          trackYear:   extractItemReleaseYear(item),
          originalReleaseDate: extractItemReleaseDate(item),
          genres:      (item.Genres || []).map(String),
          moods:       [], // no native mood field in Jellyfin; Tags could map here later
          libraryKey:  String(key),
          filePath:    String(item.Path || ''),
          durationMs:  Math.round((item.RunTimeTicks || 0) / 10_000),
          ratingCount: 0,
          viewCount:   Number(item.UserData?.PlayCount ?? 0),
        });
      }
      startIndex += items.length;
    }
  }

  return tracks;
}

// ── Active session polling ────────────────────────────────────────────────────
// Returns all currently active audio playback sessions. Used by the session
// poller as an alternative to webhooks — no plugin required.

export async function getActiveSessions(url, apiKey) {
  const u = buildUrl(url, 'Sessions');
  const res = await fetch(u.toString(), {
    headers: authHeaders(apiKey),
    signal: AbortSignal.timeout(10_000),
  });
  if (!res.ok) return [];
  const sessions = await res.json();
  return (Array.isArray(sessions) ? sessions : [])
    .filter((s) => s.NowPlayingItem?.Type === 'Audio')
    .map((s) => ({
      sessionId:   String(s.Id || ''),
      userId:      String(s.UserId || ''),
      username:    String(s.UserName || ''),
      itemId:      String(s.NowPlayingItem?.Id || ''),
      albumId:     String(s.NowPlayingItem?.AlbumId || s.NowPlayingItem?.Id || ''),
      trackTitle:  String(s.NowPlayingItem?.Name || ''),
      album:       String(s.NowPlayingItem?.Album || ''),
      artist:      String(s.NowPlayingItem?.AlbumArtist || s.NowPlayingItem?.Artists?.[0] || ''),
      durationMs:  Math.round((s.NowPlayingItem?.RunTimeTicks || 0) / 10_000),
      positionMs:  Math.round((s.PlayState?.PositionTicks || 0) / 10_000),
      isPaused:    Boolean(s.PlayState?.IsPaused),
      deviceName:  String(s.DeviceName || s.Client || ''),
    }));
}

// ── Playlist management ───────────────────────────────────────────────────────

// Return all Jellyfin users as { id, name } objects.
export async function getUsers(url, apiKey) {
  const u = buildUrl(url, 'Users');
  const res = await fetch(u.toString(), { headers: authHeaders(apiKey), signal: AbortSignal.timeout(10_000) });
  if (!res.ok) return [];
  const users = await res.json();
  const list = Array.isArray(users) ? users : (users?.Items || []);
  return list.map((u) => ({
    id:   String(u.Id || ''),
    name: String(u.Name || ''),
  }));
}

// Return the Jellyfin user ID for a given username (case-insensitive).
export async function getUserIdByName(url, apiKey, username) {
  const u = buildUrl(url, 'Users');
  const res = await fetch(u.toString(), { headers: authHeaders(apiKey), signal: AbortSignal.timeout(10_000) });
  if (!res.ok) throw new Error(`Could not list Jellyfin users (HTTP ${res.status})`);
  const users = await res.json();
  const list = Array.isArray(users) ? users : (users?.Items || []);
  const lower = String(username || '').toLowerCase();
  const match = list.find((u) => String(u.Name || '').toLowerCase() === lower);
  if (!match) throw new Error(`Jellyfin user "${username}" not found`);
  return String(match.Id || '');
}

// Ensure a playlist with the given title exists for userId.
// Returns { playlistId, created }.
export async function ensurePlaylist(url, apiKey, userId, title) {
  // Search existing playlists
  const searchU = buildUrl(url, `Users/${userId}/Items`);
  searchU.searchParams.set('IncludeItemTypes', 'Playlist');
  searchU.searchParams.set('Recursive', 'true');
  searchU.searchParams.set('Fields', 'Id,Name');
  const searchRes = await fetch(searchU.toString(), { headers: authHeaders(apiKey), signal: AbortSignal.timeout(10_000) });
  if (searchRes.ok) {
    const json = await searchRes.json();
    const items = json?.Items || [];
    const existing = items.find((p) => String(p.Name || '').trim() === title.trim());
    if (existing?.Id) {
      const playlistId = String(existing.Id);
      // Try to make existing playlist private (Jellyfin 10.9+); ignore errors for older versions
      await setPlaylistPrivate(url, apiKey, playlistId).catch(() => {});
      return { playlistId, created: false };
    }
  }

  // Create new playlist as private (IsPublic: false — Jellyfin 10.9+; ignored by older versions)
  const createRes = await fetch(buildUrl(url, 'Playlists').toString(), {
    method: 'POST',
    headers: { ...authHeaders(apiKey), 'Content-Type': 'application/json' },
    signal: AbortSignal.timeout(10_000),
    body: JSON.stringify({ Name: title, UserId: userId, MediaType: 'Audio', IsPublic: false }),
  });
  if (!createRes.ok) throw new Error(`Could not create Jellyfin playlist "${title}" (HTTP ${createRes.status})`);
  const json = await createRes.json();
  const playlistId = String(json?.Id || '');
  if (!playlistId) throw new Error('Jellyfin did not return a playlist ID after creation');
  // Also attempt the explicit privacy update in case IsPublic was ignored at creation
  await setPlaylistPrivate(url, apiKey, playlistId).catch(() => {});
  return { playlistId, created: true };
}

// Set a Jellyfin playlist to private (not public). Jellyfin 10.9+.
// Silently ignored on older versions — call with .catch(() => {}) to be safe.
async function setPlaylistPrivate(url, apiKey, playlistId) {
  const u = buildUrl(url, `Playlists/${playlistId}`);
  const res = await fetch(u.toString(), {
    method: 'POST',
    headers: { ...authHeaders(apiKey), 'Content-Type': 'application/json' },
    signal: AbortSignal.timeout(8_000),
    body: JSON.stringify({ IsPublic: false }),
  });
  return res.ok;
}

// Replace all items in a Jellyfin playlist with the given itemIds (Jellyfin ratingKeys).
export async function replacePlaylistItems(url, apiKey, playlistId, itemIds, userId = '') {
  // Clear existing items
  const items = await fetch(buildUrl(url, `Playlists/${playlistId}/Items`).toString(), {
    headers: authHeaders(apiKey),
    signal: AbortSignal.timeout(10_000),
  });
  if (items.ok) {
    const json = await items.json();
    const existingIds = (json?.Items || []).map((i) => String(i.PlaylistItemId || i.Id || '')).filter(Boolean);
    if (existingIds.length) {
      const delU = buildUrl(url, `Playlists/${playlistId}/Items`);
      delU.searchParams.set('EntryIds', existingIds.join(','));
      await fetch(delU.toString(), { method: 'DELETE', headers: authHeaders(apiKey), signal: AbortSignal.timeout(10_000) });
    }
  }

  if (!itemIds.length) return;

  // Add new items in batches of 100
  for (let i = 0; i < itemIds.length; i += 100) {
    const batch = itemIds.slice(i, i + 100);
    const addU = buildUrl(url, `Playlists/${playlistId}/Items`);
    addU.searchParams.set('Ids', batch.join(','));
    if (userId) addU.searchParams.set('UserId', userId);
    const addRes = await fetch(addU.toString(), {
      method: 'POST',
      headers: authHeaders(apiKey),
      signal: AbortSignal.timeout(10_000),
    });
    if (!addRes.ok) throw new Error(`Could not add items to Jellyfin playlist (HTTP ${addRes.status})`);
  }
}

// ── Webhook debug ─────────────────────────────────────────────────────────────
// Returns the raw plugin list and webhook plugin config for troubleshooting.

export async function debugWebhookPlugin(url, apiKey) {
  const pluginsUrl = buildUrl(url, 'Plugins');
  const pluginsRes = await fetch(pluginsUrl.toString(), { headers: authHeaders(apiKey) });
  if (!pluginsRes.ok) return { error: `Plugin list failed: HTTP ${pluginsRes.status}` };

  const plugins = await pluginsRes.json();
  const pluginList = Array.isArray(plugins) ? plugins : (plugins?.Items || []);
  const webhookPlugin = pluginList.find((p) => /webhook/i.test(String(p.Name || '')) || /webhook/i.test(String(p.Id || '')));

  if (!webhookPlugin) return { plugins: pluginList.map((p) => ({ name: p.Name, id: p.Id })), webhookPlugin: null };

  const pluginId = String(webhookPlugin.Id || '');
  const configUrl = buildUrl(url, `Plugins/${pluginId}/Configuration`);
  const configRes = await fetch(configUrl.toString(), { headers: authHeaders(apiKey) });
  const rawConfig = configRes.ok ? await configRes.json() : { error: `Config fetch HTTP ${configRes.status}` };

  return {
    webhookPlugin: { name: webhookPlugin.Name, id: pluginId, version: webhookPlugin.Version },
    configUrl: configUrl.toString(),
    rawConfig,
  };
}

// ── Webhook auto-registration ─────────────────────────────────────────────────
// Attempts to register a webhook via the Jellyfin webhook plugin API (10.9+).
// Falls back gracefully — returns { manual: true, webhookUrl } so the caller
// can present the URL to the user for manual configuration.

export async function registerWebhook(url, apiKey, webhookUrl) {
  // First check if the webhook plugin is installed
  try {
    const pluginsUrl = buildUrl(url, 'Plugins');
    const pluginsRes = await fetch(pluginsUrl.toString(), { headers: authHeaders(apiKey) });
    if (!pluginsRes.ok) return { ok: false, manual: true, webhookUrl, reason: `Could not list plugins (HTTP ${pluginsRes.status})` };

    const plugins = await pluginsRes.json();
    const pluginList = Array.isArray(plugins) ? plugins : (plugins?.Items || []);
    const webhookPlugin = pluginList.find((p) =>
      /webhook/i.test(String(p.Name || '')) || /webhook/i.test(String(p.Id || ''))
    );

    if (!webhookPlugin) {
      return {
        ok: false,
        manual: true,
        webhookUrl,
        reason: 'Jellyfin webhook plugin not installed. Configure the webhook URL manually in Jellyfin → Dashboard → Plugins → Webhook.',
      };
    }

    // Plugin is installed — use its configuration endpoint to add our webhook
    const pluginId = String(webhookPlugin.Id || '');
    const configUrl = buildUrl(url, `Plugins/${pluginId}/Configuration`);
    const configRes = await fetch(configUrl.toString(), { headers: authHeaders(apiKey) });

    if (!configRes.ok) {
      return { ok: false, manual: true, webhookUrl, reason: `Could not read webhook plugin config (HTTP ${configRes.status})` };
    }

    const config = await configRes.json();

    // Jellyfin webhook plugin v18+ uses 'genericServerOptions' (camelCase JSON).
    // Older versions used 'ServerOptions'. Try all variants.
    const fieldName = ['genericServerOptions', 'GenericServerOptions', 'serverOptions', 'ServerOptions']
      .find((k) => Array.isArray(config?.[k])) || 'genericServerOptions';
    const destinations = Array.isArray(config[fieldName]) ? [...config[fieldName]] : [];

    // Check if already registered (handle both 'url' and 'Url' field names)
    const alreadyExists = destinations.some((d) => String(d.url || d.Url || '') === webhookUrl);
    if (alreadyExists) return { ok: true, created: false, webhookUrl, manual: false };

    // Jellyfin expects camelCase field names on POST too (case-insensitive but camelCase is safest)
    destinations.push({
      url: webhookUrl,
      notificationTypes: ['PlaybackStart', 'PlaybackStop', 'PlaybackProgress'],
      itemType: ['Audio'],
      sendAll: false,
    });

    const saveRes = await fetch(configUrl.toString(), {
      method: 'POST',
      headers: { ...authHeaders(apiKey), 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...config, [fieldName]: destinations }),
    });

    if (!saveRes.ok) {
      const body = await saveRes.text().catch(() => '');
      return { ok: false, manual: true, webhookUrl, reason: `Could not save webhook plugin config (HTTP ${saveRes.status}): ${body}` };
    }

    return { ok: true, created: true, webhookUrl, manual: false };
  } catch (err) {
    return { ok: false, manual: true, webhookUrl, reason: String(err?.message || err) };
  }
}
