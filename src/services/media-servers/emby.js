// Emby media server adapter.
// Emby's REST API is a close fork of Jellyfin's — this adapter re-exports
// most Jellyfin functions directly and only overrides what differs:
//
//  - Authentication endpoint path is identical
//  - Webhook registration uses Emby's built-in advanced notifications API
//    rather than a plugin model (available in Emby Premiere)
//
// Config stored at config.emby = { url, apiKey, apiKeySet, userId, serverName }

export { authenticate, verifyConnection, getLibraries, getLibraryTracks, getActiveSessions, debugWebhookPlugin, createApiKey, getUsers, getUserIdByName, ensurePlaylist, replacePlaylistItems } from './jellyfin.js';

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

// ── Webhook auto-registration ─────────────────────────────────────────────────
// Uses Emby's Notifications API. Requires Emby Premiere for webhook delivery.
// Falls back to manual if the server returns an error.

export async function registerWebhook(url, apiKey, webhookUrl) {
  try {
    // List existing notification configurations
    const listUrl = buildUrl(url, 'Notifications/Admin');
    const listRes = await fetch(listUrl.toString(), { headers: authHeaders(apiKey) });

    if (!listRes.ok) {
      return {
        ok: false,
        manual: true,
        webhookUrl,
        reason: `Could not query Emby notifications (HTTP ${listRes.status}). Configure the webhook URL manually in Emby → Dashboard → Notifications.`,
      };
    }

    const existing = await listRes.json();
    const notificationList = Array.isArray(existing) ? existing : (existing?.Items || []);

    // Check if a Curatorr webhook already exists
    const alreadyExists = notificationList.some((n) =>
      String(n.Url || '').trim() === webhookUrl
    );
    if (alreadyExists) return { ok: true, created: false, webhookUrl, manual: false };

    // Create new webhook notification
    const createUrl = buildUrl(url, 'Notifications/Admin');
    const createRes = await fetch(createUrl.toString(), {
      method: 'POST',
      headers: { ...authHeaders(apiKey), 'Content-Type': 'application/json' },
      body: JSON.stringify({
        Name: 'Curatorr',
        Url: webhookUrl,
        NotificationTypes: ['PlaybackStart', 'PlaybackStop', 'PlaybackProgress'],
        Enabled: true,
        SendToUserMode: 'All',
      }),
    });

    if (!createRes.ok) {
      return {
        ok: false,
        manual: true,
        webhookUrl,
        reason: `Emby rejected webhook creation (HTTP ${createRes.status}). Emby Premiere may be required. Configure manually in Emby → Dashboard → Notifications.`,
      };
    }

    return { ok: true, created: true, webhookUrl, manual: false };
  } catch (err) {
    return { ok: false, manual: true, webhookUrl, reason: String(err?.message || err) };
  }
}
