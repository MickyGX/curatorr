#!/usr/bin/env node
// Music Assistant probe — Phase 0 spike for "MA IMPLEMENTATION PLAN.md".
// Connects to an MA server, authenticates, dumps the facts the plan depends on,
// then logs every media_item_played / playlog_updated event until Ctrl+C.
//
// Usage:
//   MA_URL=http://192.168.0.x:8095 MA_TOKEN=<long-lived token> node scripts/ma-probe.mjs [--sample-provider <instance_id>]
//
// Output is newline-delimited JSON on stdout, so it can be tee'd into a fixture file:
//   node scripts/ma-probe.mjs | tee tmp/ma-probe.ndjson

const MA_URL = String(process.env.MA_URL || '').replace(/\/+$/, '');
const MA_TOKEN = String(process.env.MA_TOKEN || '');
if (!MA_URL || !MA_TOKEN) {
  console.error('Set MA_URL and MA_TOKEN');
  process.exit(1);
}
const sampleProviderArg = process.argv.indexOf('--sample-provider');
const sampleProvider = sampleProviderArg > -1 ? process.argv[sampleProviderArg + 1] : '';

const ws = new WebSocket(`${MA_URL.replace(/^http/, 'ws')}/ws`);
const pending = new Map();
let nextId = 1;

function out(kind, data) {
  process.stdout.write(`${JSON.stringify({ t: new Date().toISOString(), kind, data })}\n`);
}

function send(command, args = {}) {
  const messageId = String(nextId++);
  return new Promise((resolve, reject) => {
    const chunks = [];
    pending.set(messageId, { resolve, reject, chunks });
    ws.send(JSON.stringify({ message_id: messageId, command, args }));
  });
}

async function tryCommand(label, command, args) {
  try {
    const result = await send(command, args);
    out(label, result);
    return result;
  } catch (err) {
    out(`${label}:error`, String(err?.message || err));
    return null;
  }
}

ws.addEventListener('message', (evt) => {
  let msg;
  try { msg = JSON.parse(String(evt.data)); } catch { return; }
  if (msg.server_id && msg.schema_version !== undefined && !msg.message_id) {
    out('server_info', msg);
    return;
  }
  if (msg.event) {
    if (['media_item_played', 'playlog_updated', 'queue_updated', 'player_updated'].includes(msg.event)) {
      // queue/player updates are noisy; keep only the fields useful for correlating plays
      if (msg.event === 'queue_updated' || msg.event === 'player_updated') {
        out(msg.event, { object_id: msg.object_id, state: msg.data?.state, current_item: msg.data?.current_item?.uri || msg.data?.current_media?.uri });
      } else {
        out(msg.event, { object_id: msg.object_id, data: msg.data });
      }
    }
    return;
  }
  const entry = pending.get(String(msg.message_id));
  if (!entry) return;
  if (msg.error_code !== undefined) {
    pending.delete(String(msg.message_id));
    entry.reject(new Error(`${msg.error_code}: ${msg.details}`));
    return;
  }
  if (Array.isArray(msg.result)) entry.chunks.push(...msg.result);
  if (msg.partial) return;
  pending.delete(String(msg.message_id));
  entry.resolve(Array.isArray(msg.result) ? entry.chunks : msg.result);
});

ws.addEventListener('error', (evt) => out('ws:error', String(evt?.message || 'error')));
ws.addEventListener('close', (evt) => { out('ws:close', { code: evt.code, reason: evt.reason }); process.exit(0); });

ws.addEventListener('open', async () => {
  const auth = await tryCommand('auth', 'auth', { token: MA_TOKEN });
  if (!auth?.authenticated) { ws.close(); return; }
  await tryCommand('auth/me', 'auth/me');
  await tryCommand('auth/users', 'auth/users');
  const providers = await tryCommand('providers', 'providers');
  await tryCommand('players/all', 'players/all');

  // One full library track, to confirm provider_mappings item_id format and external_ids.
  const provider = sampleProvider
    || (Array.isArray(providers) ? providers.find((p) => ['plex', 'jellyfin', 'emby'].includes(p.domain))?.instance_id : '');
  await tryCommand('library_track_sample', 'music/tracks/library_items', { limit: 2, summary: false, ...(provider ? { provider } : {}) });
  await tryCommand('recently_played_items', 'music/recently_played_items', { limit: 5, media_types: ['track'] });
  await tryCommand('builtin_playlists', 'music/playlists/library_items', { limit: 5, provider: 'builtin' });
  out('ready', 'Listening for playback events — play, skip, pause/resume on an MA player now. Ctrl+C to stop.');
});
