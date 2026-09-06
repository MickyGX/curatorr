Implementation Plan: Music Assistant Integration
Architecture Decision
MA is fundamentally different from the existing sources. Instead of MA sending webhooks to Curatorr, Curatorr connects to MA as a WebSocket client and receives MEDIA_ITEM_PLAYED events. This is actually simpler than the session-based approach because MA sends a single completion event per track containing seconds_played and fully_played — no session open/close cycle is needed.

MA is also additive, not exclusive — plays can be tracked from both MA and the primary Plex/Jellyfin source simultaneously.

Files to Change
File	Change
package.json	Add ws dependency (Node 20 lacks native WebSocket client)
src/services/music-assistant.js	New — WebSocket client + event handler
src/index.js	Add musicAssistant block to DEFAULT_CONFIG
src/routes/webhooks.js	Start the MA client in the same init block as the polling loop
src/routes/settings.js	Add GET/POST /api/settings/music-assistant endpoints
src/services/media-servers/index.js	Add MA URL/credential helpers
Frontend settings view	Add MA configuration card
1. Config Schema (src/index.js)

musicAssistant: {
  enabled: false,
  url: '',         // e.g. http://192.168.1.100:8095
  apiKey: '',      // Bearer token from MA Settings → Core Configuration
  apiKeySet: false,
}
2. WebSocket Client (src/services/music-assistant.js, new file)

import WebSocket from 'ws';

export function startMusicAssistantClient(ctx) {
  let delay = 5_000;

  function connect() {
    const config = ctx.loadConfig();
    const { enabled, url, apiKey } = config?.musicAssistant || {};
    if (!enabled || !url || !apiKey) return;

    const ws = new WebSocket(url.replace(/^http/, 'ws') + '/api', {
      headers: { Authorization: `Bearer ${apiKey}` },
    });

    ws.on('open', () => {
      delay = 5_000; // reset backoff on success
    });

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }
      if (msg?.event === 'MEDIA_ITEM_PLAYED') handleMaPlayEvent(msg.data, ctx);
    });

    ws.on('close', () => setTimeout(connect, delay = Math.min(delay * 2, 60_000)));
    ws.on('error', () => {}); // 'close' always follows 'error'
  }

  setTimeout(connect, 5_000); // mirror the poller's startup delay
}
Event handler logic:

Filter: media_type !== 'track' → ignore
ratingKey: parse from Plex URI (plex://PROVIDER/track/12345 → 12345), fall back to full URI for non-Plex sources
durationMs = data.duration * 1000, playedMs = data.seconds_played * 1000
isSkip: compare playedMs against skipThresholdSeconds from resolveUserSmartConfig
Call recordPlayEvent() directly — no session table involvement
Call classifyTier() + scheduleRebuild() afterwards (same as the polling loop's finalise path)
eventSource: 'music_assistant'
sessionKey: ma-{userid}-{ratingKey}-{timestamp} (unique per play, no dedup needed)
3. Startup Hook (src/routes/webhooks.js)
In the same block where the Jellyfin/Emby polling timer is started (~line 1421), add:


import { startMusicAssistantClient } from '../services/music-assistant.js';

// after the poller setTimeout block:
startMusicAssistantClient({
  loadConfig, db, pushLog, resolveUserSmartConfig,
  classifyTier, scheduleRebuild,
  // recordPlayEvent imported from db.js
});
4. Settings API (src/routes/settings.js)
GET /api/settings/music-assistant — returns config with apiKey masked to apiKeySet: bool

POST /api/settings/music-assistant — saves and tests:


{ "enabled": true, "url": "http://192.168.1.100:8095", "apiKey": "abc123" }
Tests by POSTing { "command": "server_info" } to MA_URL/api with Authorization: Bearer {apiKey}. On success, saves and bounces the WebSocket client.

5. Frontend — Settings Card
A new card alongside the Plex/Jellyfin/Emby cards:

Enable toggle
URL field (with placeholder http://192.168.1.100:8095)
API Key field (password input — find it at MA Settings → Core → API key)
Connection status pill (Connected / Disconnected / Not configured)
Small note: "MA user IDs are matched to Curatorr users by username. Your MA username should match your Plex/primary source username."
6. User Mapping
No explicit mapping table for the MVP. MA's userid field contains the MA username, which in most setups matches the Plex username. The user_plex_id column in play_events / track_stats is already used as a string identifier for all server types — it just stores whatever username arrives. If a user hits a mismatch we can add a mapping table in a follow-up.

Effort Estimate
Phase	Work
Config + backend client + DB wiring	~3–4 hours
Settings API endpoints	~1 hour
Frontend settings card	~1–2 hours
Testing + edge cases (reconnect, Plex URI parsing, skip detection)	~1–2 hours
Total	~6–9 hours
