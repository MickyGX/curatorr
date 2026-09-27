# Implementation Plan: Music Assistant Integration

Revised 2026-09-27 after the discussion in [#165](https://github.com/MickyGX/curatorr/discussions/165). The revision is based on the current code (v0.1.99) and on Music Assistant server source (2.10.x). Confirmed live against the test install: MA 2.10.4 as an HA add-on, `schema_version` 65, `/api` returns 401 without a token, and `server_info` is sent on `/ws` before auth.

## Implementation status (branch `feat/music-assistant`)

Phase 1 is implemented. It was verified end to end against the test MA install: a real Chromecast play was recorded as one `music_assistant` play on the matching rating key, with track and artist stats updated.

| Area | Where |
|---|---|
| Shared recorder, extracted with no behaviour change (217 existing tests unchanged and green) | `src/services/play-recorder.js`, `src/routes/webhooks.js` |
| MA client, tracker, identity, service | `src/services/music-assistant/{client,play-tracker,identity,index}.js` |
| Lifecycle | `start()` / `stop()` in `src/index.js` |
| Settings tab + `/settings/music-assistant`, `/api/music-assistant/{status,test}` | `src/routes/settings.js`, `src/views/settings.ejs` |
| Tests (25 new) | `src/test/ma-{play-tracker,identity,client,recorder}.test.js`, plus a settings case in `auth.test.js` |
| Docs | `docs/wiki/Integrations.md` → Music Assistant |
| Now Playing card falls back to MA queues (a Plex session still wins) | `getMusicAssistantNowPlaying` in the MA service, `/api/music/overview/now-playing` in `src/routes/api-music.js` |

**Deviations from the plan below:**
- **`ma_track_map` is created by the MA module** (`ensureMaTables`) instead of in `src/db.js`, because that file is root-owned in this checkout.
  - It has no `ma_library_id` column; the URI already carries the id.
  - There is no hook in `pruneStaleMasterTracks`. Instead, cached rating keys are checked against `master_tracks` on every lookup.
- **The recorder gained `allowMergeCandidate`**, which defaults to `allowRecentPlayReuse`. The existing loose/strict merge collapses a repeat of the same track within the last 10 plays. MA keeps `session_key` continuation (for pause/resume) but opts out of the merge, since the tracker already knows the play boundaries.
- **Unmatched plays credit the artist only** (`+0.05`, as Last.fm sync does). This path lives in the recorder.
- **Removed three dead `classifyTier(db, …)` calls** in the Jellyfin/Emby poller.
- **The index uses pages of 200 with a 180 s timeout, and runs at most daily** (on connect only if the map is older than 24 h, plus after MA's `music_sync_completed`). Pages took 27–55 s while MA was syncing about 24k Plex tracks, against about 3 s when idle. Plays resolve on demand anyway (a single `music/tracks/get` takes about 150 ms).
- **Lookups pass `allow_update_metadata:false`.** Without it, every `music/tracks/get` queues an "Update metadata" task in MA (seen live).
- **The settings tab is visible to actual admins only**, like Lidarr. The token is never rendered.

## What changed from the first draft

The first draft had the right overall shape: Curatorr connects to MA as a WebSocket client and ingests plays in addition to the primary source. Several details were wrong, though, and would have produced a client that never received an event or a play that never reached the stats.

| First draft | Reality | Fix in this plan |
|---|---|---|
| Add `ws` because Node 20 has no WebSocket client | The image is `node:24-alpine`, which has a global `WebSocket` | No new dependency |
| Connect to `<url>/api` with an `Authorization` header | The WebSocket is `/ws`. MA ≥ 2.7 requires an `auth` command as the first message. `/api` is HTTP JSON-RPC | §3 |
| `msg.event === 'MEDIA_ITEM_PLAYED'` | Event names are lowercase: `media_item_played` | §3 |
| One event per track | The report fires on every state change and every 30 s while playing. The finished play is the report with `is_playing: false`, and a pause/resume can produce several of them | §4 (play-instance tracker) |
| Parse the ratingKey from `plex://…/track/12345` | The event `uri` is usually `library://track/<MA db id>`. The Plex provider's `item_id` is `/library/metadata/<ratingKey>` | §5 (identity resolution) |
| `userid` is the MA username and matches the Plex username | MA ≥ 2.7 has real users. `userid` is an MA user id, and it can be null for automation or voice playback | §6 (explicit user map) |
| `recordPlayEvent()` + `classifyTier()` + `scheduleRebuild()` | `recordPlayEvent` only inserts into `play_events` and never updates `track_stats` or `artist_stats`. `classifyTier` is a pure `(listenedMs, durationMs, smartConfig)` function. The real finalise path is `recordOrUpdateSessionPlay`, a closure inside `registerWebhooks` | §4: extract the recorder and reuse it |
| JSON `GET/POST /api/settings/music-assistant` | Settings are EJS pages with form POSTs to `/settings/<source>` | §7 |
| MA helpers in `media-servers/index.js` | That module resolves the single *active* server. MA isn't a replacement server | New `src/services/music-assistant/` |

## Scope and the identity question (from #165)

@tayfuryldz pointed out that the hard part is identity: the same recording can exist through Plex, Spotify, local files and so on. This plan fixes that up front:

> **Curatorr's canonical track identity stays the primary server's native id** (Plex ratingKey or Jellyfin/Emby item Id in `master_tracks`). MA is an **adapter at the edge**. Every MA track is resolved to a `master_tracks` row before it touches play history or rules. Nothing MA-specific leaks into scoring or playlist logic.

Resolution order for an MA track (§5):
1. The provider mapping that points at Curatorr's own server.
2. MusicBrainz recording id.
3. Normalised artist + title + duration, using the existing import matcher.
4. Unmatched: record the play with an empty rating key. That still feeds artist stats, as Last.fm sync already does.

**Phasing:**

| Phase | What | Why this order |
|---|---|---|
| 0 | Live spike against a real MA install | Settles the handful of facts the source doesn't pin down |
| 1 | **MA as a play source.** Listening on any MA player updates Curatorr stats and triggers smart-playlist rebuilds | Most value and least surface area. Needs only read scopes |
| 2 | **MA as a playlist sink.** Curatorr smart playlists are mirrored to MA "builtin" playlists, so they're playable on any MA player | Reuses Phase 1's identity map. Only MA's builtin provider supports playlist create/edit (Plex/Jellyfin/Emby playlists are read-only through MA) |
| Later | Now-playing from MA queues, "play this playlist on player X", MA as a *library source* for tracks that exist only in streaming providers | MA as a library source needs a multi-source `master_tracks` model, which Curatorr doesn't have. Out of scope here |

Phases 1 and 2 require that the MA library includes the same Plex/Jellyfin/Emby server that Curatorr uses as its primary source. Tracks MA plays from Spotify/Tidal etc. are handled by fallbacks 2–4, and are never *added* to Curatorr's library.

---

## Phase 0 — Live spike (done 2026-09-27)

Run against the test install: MA 2.10.4 as an HA add-on at `http://192.168.0.4:8095`, with a Plex provider (`plex--D5gauxcg`) mid-sync at about 12k tracks, and a muted Chromecast as the player. Playback was driven through the MA WebSocket API with the same commands the MA web UI sends: `player_queues/play_media`, `/next`, `/pause` and `/play`.

- Probe script: [scripts/ma-probe.mjs](scripts/ma-probe.mjs).
  ```
  MA_URL=http://<ma-host>:8095 MA_TOKEN=<long-lived token> node scripts/ma-probe.mjs | tee tmp/ma-probe.ndjson
  ```
  The token is created in MA under **Settings → Profile → Long-lived access tokens**. Use an admin user.
- Captured events: [src/test/fixtures/ma-events.ndjson](src/test/fixtures/ma-events.ndjson). They are anonymised, with `scenario` marker lines for A (full play), B (skip at ~12 s), C (pause 70 s, resume, finish) and D (same track twice).

| # | Question | Answer |
|---|---|---|
| Q1 | Event `uri` form | **`library://track/<id>`**. The report also carries `name`, `artist`, `artists[]`, `album`, `duration`, `mbid` (recording), `artist_mbids`, `album_mbid`, `userid` and `player_id` (= queue id). No `item_by_uri` round-trip is needed for the MBID/text fallbacks |
| Q2 | Plex mapping `item_id` | **`/library/metadata/<ratingKey>`**. Checked 5 tracks: every stripped id is present in Curatorr's `master_tracks` with the same title and artist, and the MBIDs match `master_tracks.recording_mbid` |
| Q3 | `library_items` provider filter | **Works** with `provider: "plex--D5gauxcg"` and `summary:false` (500 full tracks per page, including `provider_mappings` and `external_ids`) |
| Q4 | Ordering on track change | The old item's `is_playing:false` report arrives ~3 s after `next`, before the new item's first report (which only comes at 30 s). Keying on `(queue, uri)` handles either order |
| Q5 | `userid` for HA-initiated playback | **Not tested** (the HA connection dropped during the HA reboot). Keep the "default user" fallback. Re-test before release |
| Q6 | Does Plex/Tautulli also see MA streams? | **No.** Plex `/status/sessions` was empty 30 s into MA playback of a Plex track, so there's no Plex/Tautulli double count. The `ignorePrimaryUser` idea is dropped |
| Q7 | Per-user playlists via impersonation | The impersonation arg is `user`, but in 2.10.4 it exists only on read and queue commands (`library_items`, `search`, `play_media` …). **Not on `create_playlist` or `add/remove_playlist_tracks`.** Created playlists have `owner: "Music Assistant"` and no per-user access field, so every MA user sees them |
| Q8 | Token lifetime | **365 days** (JWT `exp` − `iat`). `auth/me` has no expiry field, so decode `exp` from the JWT payload for the settings panel |

**Other findings that change the design:**

1. **Spurious duplicate final report.** After scenario A ended (`seconds_played 68, duration 73, fully_played true`), starting a new queue re-sent the same item as `is_playing:false, seconds_played 68, duration 11`, with `duration` wrong. The tracker must treat it as the same instance and keep the **max** duration seen, never the latest.
2. **A pause emits a final-looking report.** Pausing at 14 s sent `is_playing:false, fully_played:false, seconds_played 14`, which is identical to a skip. After resuming, `seconds_played` **continued cumulatively** (30, 60, 90, then a final 92). So a `fully_played:false` report is only provisional. See the debounce in §4.
3. **`seconds_played` runs ~3–5 s short** of wall time (start latency). A full play reported 68–73 s for a 73 s track. Use `fully_played` as the completion signal.
4. **`playlog_updated`** (`{uri, media_type, fully_played, seconds_played, userid}`) fires once per finish and on pause, but not for the spurious duplicate. It also fires for the artist (`library://artist/<id>`). It lacks names, duration and the player, so `media_item_played` stays the primary signal.
5. **Playlist edits are queued background tasks** (`BackgroundTask` → `tasks/get`). During a library sync they stayed `pending` for over 15 s, queued behind `music_sync`. Phase 2 must not block on them (§Phase 2).
6. **Builtin playlist provider `item_id` is the playlist name**, while the library id is numeric. Don't rename mirrored playlists; delete and recreate instead.

Still open: **Q5**. (**Q9** was later confirmed: positions are 1-indexed.) Originally also open: **Q9**, whether `remove_playlist_tracks` positions are 1-indexed on 2.10.4. The test was cancelled because the tasks were queued behind the sync. Re-run [scripts/ma-probe.mjs](scripts/ma-probe.mjs) and a playlist round-trip once the Plex sync has finished.

---

## Phase 1 — MA as a play source

### 1. Config (`config.musicAssistant`)

```js
musicAssistant: {
  enabled: false,
  url: '',                 // http://host:8095 (no trailing /ws)
  token: '',               // MA long-lived access token
  tokenSet: false,
  providerInstance: '',    // MA provider instance that fronts Curatorr's server, e.g. "plex--AbCdEfGh"
  userMap: {},             // { [maUserId]: curatorrUserPlexId }. '' = ignore that MA user
  defaultUser: '',         // user_plex_id for plays with null/unknown userid; '' = drop them
  mirrorPlaylists: false,  // Phase 2
}
```

- `loadConfig` shallow-merges defaults (`src/index.js` ~705), so a nested block added to `DEFAULT_CONFIG` won't reach existing installs. Read it through a `getMaConfig(config)` helper that applies defaults, the same way `config.jellyfin`/`config.emby` are handled.
- Don't add it to `DEFAULT_CONFIG`.

### 2. Module layout (new `src/services/music-assistant/`)

| File | Responsibility |
|---|---|
| `client.js` | Connection lifecycle: connect, `auth`, request/response correlation, `partial` chunk assembly, event fan-out, backoff reconnect, `status()` |
| `play-tracker.js` | Pure state machine that turns progress reports into finished plays (§4). No I/O. Unit tested with the fixture |
| `identity.js` | Maps an MA track to a `master_tracks` row (§5), and maintains `ma_track_map` |
| `index.js` | `startMusicAssistant(ctx)`, `restartMusicAssistant(ctx)`, `stopMusicAssistant()`, `getMusicAssistantStatus()`. Wires the client to the tracker, identity and recorder |

`client.js` takes an injectable `WebSocketImpl` (defaulting to the global `WebSocket`), so tests can drive it with a fake socket. That avoids adding `ws` as a devDependency just to run a test server.

### 3. Client protocol

- **Connect** to `url.replace(/^http/, 'ws') + '/ws'`.
- **Handshake:**
  - The server first sends `ServerInfoMessage`.
  - Check `server_version` (semver compare, not `schema_version`, which differs between stable 65 and dev 77). MA < 2.7 has no auth; reject it with "Music Assistant 2.7 or newer is required".
  - If `onboard_done` is false, report "Finish Music Assistant setup first" rather than an auth error.
  - Send `{message_id, command:'auth', args:{token}}`, expecting `{authenticated:true, user}`.
  - Events only flow after auth, and there's no subscribe command.
- **Requests:** `{message_id, command, args}`. A response carries `result`. When `partial:true`, keep accumulating array chunks until the final message. An error response carries `{error_code, details}`. Put a 30 s timeout on every pending request.
- **Events:** `{event, object_id, data}`. Phase 1 handles only `media_item_played` (and `media_item_updated`/`media_item_deleted` to invalidate identity cache entries).
- **Reconnect:** exponential backoff from 5 s, capped at 5 min. Reset it after a successful auth.
  - A 401/auth failure does *not* retry in a tight loop. Mark the status `auth_failed` and wait for a settings change.
  - All timers are `.unref()`'d.
- **Lifecycle:**
  - `startMusicAssistant(ctx)` is called from `start()` in `src/index.js`, next to `registerWebhooks` (~2274), and not from `webhooks.js`: MA doesn't depend on the webhook/poller gates.
  - `stopMusicAssistant()` is called from `stop()` (~2326), so tests don't hang.
  - Saving settings calls `restartMusicAssistant(ctx)`.
- **Status** (for the UI): `{state: disabled|connecting|connected|auth_failed|error, serverVersion, user, lastEventAt, lastError, counters:{plays, matched, unmatched, droppedNoUser}}`.

### 4. Turning MA reports into plays

**Report semantics** (from `player_queues/playback_tracker.py`):
- MA sends `media_item_played` on state changes and every 30 s while playing.
- On a track change it reports the previous item with `is_playing:false`.
- Plays under 5 s are never reported.

**Play-instance tracker** (`play-tracker.js`):
- It keeps a `Map` keyed by `${data.player_id}|${data.uri}` → `{instanceId, startedAt, maxSeconds, maxDuration, fullyPlayed, finalisedSeconds, pendingTimer, lastSeenAt}`.
- A report starts a **new instance** when there's no entry, or when `seconds_played < maxSeconds - 5`. That covers a restart or repeat: in scenario D, the second play's first report was 30 s after a 73 s finish.
- Otherwise the report updates the instance:
  - `maxSeconds` / `maxDuration` are updated with **max()**, because the spurious duplicate final report carried `duration 11` (Phase 0 finding 1).
  - `fullyPlayed` is OR-ed in.
  - `lastSeenAt` is updated.
  - This makes the tracker robust to ordering (Q4) and to pause/resume, since `seconds_played` is cumulative across a pause (finding 2).
- `startedAt` = `now - seconds_played*1000` at the first report.
- **Finishing:**
  - `is_playing:false` with `fully_played:true` → emit a finish **immediately**.
  - `is_playing:false` with `fully_played:false` is either a skip or a pause (they look identical). Start a **60 s debounce**. Any later report for the same instance cancels it; otherwise the finish is emitted when it fires.
  - That delays skip detection by a minute (the rebuild is debounced 30 s anyway), and avoids recording a provisional skip and triggering a rebuild on every pause.
  - A pause longer than 60 s still produces a provisional skip. On resume, the recorder's `session_key` continuation updates the same row, and the stats are rebuilt from events.
- **Duplicates:** an `is_playing:false` report whose `seconds_played <= finalisedSeconds` for an already-finished instance is ignored (finding 1).
- **Listened time:** when `fullyPlayed`, listened = `maxDuration` (`seconds_played` runs a few seconds short, finding 3). Otherwise it's `maxSeconds`.
- Entries are evicted after `max(duration, 10 min) + 30 min` of silence.

**Recorder: extract the existing finalise path instead of rewriting it.**
- Move `recordOrUpdateSessionPlay` and its helpers out of the `registerWebhooks` closure (`src/routes/webhooks.js:293-576`) into `src/services/play-recorder.js` as `createPlayRecorder(ctx)`. The helpers are `settleSessionProgress`, `normalizeHistoryText`, the strict/loose key builders, `findRecentPlayMergeCandidate` and `rebuildAffectedPlayStats`.
- Also move and export `scheduleRebuild` (`webhooks.js:134`).
- `webhooks.js` then uses the extracted recorder, with no behaviour change. The existing Tautulli/Plex webhook tests in `src/test/auth.test.js` are the regression guard.
- MA then gets, for free:
  - skip threshold and completion logic from `resolveUserSmartConfig`
  - `track_stats` + `artist_stats` updates
  - `session_key` continuation, which covers pause/resume
  - the recent-play merge, which covers the same user and track arriving from two sources
  - the debounced rebuild on skip or completion
- The MA finish becomes a synthetic session. It is **not** written to `open_sessions`, because `purgeInactiveSourceSessions` would delete any non-primary source's rows:
  ```js
  {
    session_key: `ma:${player_id}:${instanceId}`,
    user_plex_id, plex_rating_key: resolved.ratingKey || '',
    track_title: data.name, artist_name: resolved.artistName || data.artist, album_name: resolved.albumName || data.album || '',
    library_key: resolved.libraryKey || '',
    started_at: startedAt, track_duration_ms: Math.round((data.duration || 0) * 1000),
    accumulated_ms: 0, max_position_ms: data.seconds_played * 1000, playing_since: 0,
  }
  ```
  It is recorded with `recorder.record({session, endedAt: Date.now(), playbackPositionMs: data.seconds_played*1000, smartSettings, eventSource: 'music_assistant'})`. `closeSession` on a key that was never opened is a harmless delete.
- **Unmatched tracks** (`plex_rating_key: ''`): the recorder's `updateTrackStats` path must be skipped when there's no rating key. Mirror `lastfm-sync.js:100-185` and only update `artist_stats`. Add that guard inside the extracted recorder, because today no caller passes an empty key.

**Double counting:**
- **MA → Last.fm scrobbling + Curatorr Last.fm sync:** already deduped by `findExistingPlay` (user + artist + title + time window) in `lastfm-sync.js`, provided the user ids line up.
- **MA streaming from Plex also showing up in Plex/Tautulli:** it doesn't (Q6). The Plex session list stayed empty during MA playback, so nothing to do.

### 5. Identity resolution (`identity.js`)

**New table** (migration in `src/db.js` next to the other `CREATE TABLE IF NOT EXISTS` blocks):

```sql
CREATE TABLE IF NOT EXISTS ma_track_map (
  ma_uri        TEXT PRIMARY KEY,         -- library://track/123 (and provider URIs seen in events)
  ma_library_id TEXT,                     -- MA library item_id, used for Phase 2 playlist URIs
  rating_key    TEXT NOT NULL DEFAULT '', -- master_tracks.rating_key; '' = unmatched
  match_method  TEXT NOT NULL,            -- provider | mbid | text | none
  updated_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ma_track_map_rating_key ON ma_track_map(rating_key);
```

**Bulk index (Phase 1, not Phase 2):**
- Fill `ma_track_map` by paging `music/tracks/library_items {provider: providerInstance, summary:false, limit:500, offset}` (Q3 confirmed).
- For the test install that's about 25 pages for about 12k tracks.
- Run it on first connect, on the MA `music_sync_completed` event, and on a 360-min job alongside `masterTrackRefresh`. Incremental updates come from `media_item_added/updated/deleted`.
- This makes resolving a finished play a local lookup, and Phase 2 needs the same index for rating key → MA URI.

**`resolveMaTrack(uri, reportData)`:**
1. `ma_track_map` hit → return it.
2. On a miss (a new track since the last index, or a provider URI), fetch the full item with `music/tracks/get {item_id, provider_instance_id_or_domain:'library'}` for `library://track/<id>`, or `music/item_by_uri {uri}` otherwise. This returns `provider_mappings` and `external_ids`.
3. **Provider mapping:** find the mapping with `provider_instance === config.providerInstance`, or failing that, `provider_domain === mediaServer.type`.
   - Plex: `item_id.replace(/^\/library\/metadata\//, '')`.
   - Jellyfin/Emby: `item_id` as is.
   - Accept the result only if it exists in `master_tracks`.
4. **MBID:** `external_ids` entry `musicbrainz_recordingid` (or `reportData.mbid`) → `master_tracks.recording_mbid`, reusing `buildListenbrainzTrackLookups` (`src/services/playlists.js:349`).
5. **Text:** `pickSpotifyTrackMatch(buildSpotifyTrackLookups(masterTracks), {title, artists:[{name}], durationMs})` from `src/services/import-matching.js`. It already handles "The", `&`/`and`, joint credits and duration closeness, and requires a shared artist, which prevents the bug fixed in 949a227.
6. None of these match → `match_method:'none'`, `rating_key:''`.

If the item fetch fails, go straight to steps 4–5 using the report's `name`/`artist`/`artists`/`album`/`duration`/`mbid`, which are all present in the report (Q1).

Cache the lookups built from `master_tracks` and rebuild them when `refreshMasterTrackCache` runs. Clear `ma_track_map` rows with `rating_key` pointing at pruned tracks in `pruneStaleMasterTracks`.

**Provider instance picker:** the settings "Test connection" call lists `providers`. The UI offers the Plex/Jellyfin/Emby instances, preselecting the only one whose domain matches `mediaServer.type`.

### 6. User mapping

- MA plays carry `userid` (an MA user id), or null.
- Resolve names with `auth/users`. That needs `users.read`, so use an **admin** token, or a user with the `service` role.
  - If the command is forbidden, the settings panel shows only the token's own user (`auth/me`) plus "default user".
- `config.musicAssistant.userMap` maps MA user id → Curatorr `user_plex_id`.
  - The settings panel lists MA users, each with a dropdown of known Curatorr listeners: distinct `play_events.user_plex_id` ∪ `config.users` usernames.
  - The dropdown is pre-suggested by case-insensitive match on username or display name. Nothing is saved until the admin saves the form.
- **Unmapped or blank → the play is dropped**, and a counter is incremented plus a `pushLog` message is written once per MA user per process. Null userid → `defaultUser`, or dropped if that isn't set.
- No mapping table in the DB. This is small admin config, like `plex.userServerTokens`.

### 7. Settings UI and routes

Follows the existing Jellyfin/Emby pattern in `src/routes/settings.js` (~875) and `src/views/settings.ejs` (~449).

**Tab and panel:**
- New tab button "Music Assistant", shown for every `mediaServerType`, since MA is additive.
- New `settings-tab-panel` with a form posting to `/settings/music-assistant`, carrying `_csrf` and admin-only fieldsets. Fields:
  - enabled toggle
  - URL (placeholder `http://homeassistant.local:8095`)
  - token (password field; blank = keep the existing value, same as Jellyfin's `apiKey`)
  - provider instance (select)
  - user map table
  - default user
  - (Phase 2) mirror playlists toggle
- Status pill driven by `GET /api/music-assistant/status` (admin), polled while the tab is open. Includes server version, connected user, last event time, matched/unmatched counters and token expiry.
- "Test connection" button → `POST /api/music-assistant/test` (admin, `x-csrf-token` header):
  - Opens a short-lived connection with the form's URL/token (or the saved token if the field is blank).
  - Returns `{serverVersion, schemaVersion, user, users[], providers[]}` to populate the selects.
  - Errors: unreachable / MA too old / invalid token / missing `users.read`.

**`POST /settings/music-assistant`:**
- Validates the URL (http/https only, strips a trailing `/ws` or `/`).
- Keeps the token if the field is blank, and sets `tokenSet`.
- Saves, calls `restartMusicAssistant(ctx)`, then redirects back to the tab.

**Masking:** add a `musicAssistant` entry to `renderedConfig` in GET `/settings` (~586) with `token` blanked and `tokenSet` exposed. Never render the token, even for admins.

**Docs:** add a wiki page `docs/wiki/Music-Assistant.md` covering:
- where to create the token
- admin vs service role
- that MA must include the same Plex/Jellyfin/Emby server
- the HA add-on port: 8095 must be reachable from Curatorr; the ingress port 8094 is not usable

### 8. Tests (`node --test`)

| Test file | Covers |
|---|---|
| `src/test/ma-play-tracker.test.js` | Replays `src/test/fixtures/ma-events.ndjson` with fake timers. Expected finishes: **A** 1 full play (the spurious `duration 11` duplicate is ignored); **B** 1 skip at 11 s; **C** the fixture's 70 s pause exceeds the 60 s debounce, so it gives a provisional skip at 14 s, then the same `session_key` finishes as a full 96 s play (assert one `play_events` row, `is_skip=0`). Add a synthetic variant with a 30 s pause, which should give a single full play with no provisional skip; **D** 2 full plays. Also: track-change ordering either way, and eviction |
| `src/test/ma-identity.test.js` | Plex `/library/metadata/123` → `123`; Jellyfin GUID passthrough; MBID fallback; text fallback that rejects a same-title track by a different artist; unmatched |
| `src/test/ma-client.test.js` | Uses a fake `WebSocketImpl`. Checks the auth-first handshake; that partial chunks are assembled; that a server version below 2.7 is rejected, and that `onboard_done:false` gets its own error; that an auth failure stops reconnecting; and that `stop()` clears timers |
| Recorder extraction | The existing webhook tests in `src/test/auth.test.js` stay green unchanged. Add one integration test that pushes a synthetic MA finish through the recorder and asserts `play_events` (`event_source='music_assistant'`), `track_stats` and `artist_stats` |
| Settings | POST `/settings/music-assistant` keeps the token when the field is blank; GET `/settings` never renders the token |

---

## Phase 2 — MA as a playlist sink (shelved 2026-09-27)

> **Not needed.** Curatorr already pushes its playlists to the primary server, and MA's Plex provider imports them (read-only in MA), so they're already playable on MA players. This was confirmed on the test install.
>
> A working implementation was built and tested, then removed. It has 8 unit tests against a fake builtin provider, and Q9 was confirmed: positions are 1-indexed, and tasks take about 0.3 s each when MA is idle. It's saved as `tmp/ma-phase2-playlist-mirror.patch` (tmp/ is gitignored); apply it with `git apply`.
>
> Revisit it only for playlists that don't reach the primary server: local-only Curatorr users without a Plex token, or users whose Plex playlists MA can't see because MA's Plex provider is signed in as a different account. The design below is what the patch implements, except that the source is Curatorr's stored playlists, chosen per playlist in settings, and named `<playlist> · <listener>`.


Only MA's `builtin` provider can create or edit playlists, so Curatorr mirrors each smart playlist into a builtin MA playlist that references MA library URIs.

1. **Library index:** already built in Phase 1 (§5). Phase 2 reads it in the reverse direction: rating key → `ma_uri`.
2. **Mirror table:**
   ```sql
   CREATE TABLE IF NOT EXISTS ma_playlists (
     user_plex_id TEXT NOT NULL, playlist_key TEXT NOT NULL,
     ma_playlist_id TEXT NOT NULL, name TEXT NOT NULL, updated_at INTEGER NOT NULL,
     pending_task_ids TEXT NOT NULL DEFAULT '[]', dirty INTEGER NOT NULL DEFAULT 0,
     PRIMARY KEY (user_plex_id, playlist_key)
   );
   ```
3. **Sync:** `syncPlaylistToMusicAssistant(ctx, userId, playlistKey, name, ratingKeys)` runs after the primary-server push in `syncSmartPlaylistForUser` (`src/services/playlists.js:2866`), whatever the server type. It is best-effort, and a failure only logs.
   - Create the playlist with `create_playlist {name, provider_instance_or_domain:'builtin'}` if it doesn't exist. It returns `item_id` (numeric library id) and `uri` (`library://playlist/<id>`). If the stored id returns "media item could not be found", recreate it.
   - Diff against `music/playlists/playlist_tracks {item_id, provider_instance_id_or_domain:'library', force_refresh:true}`. When the playlist has changed:
     - Submit `remove_playlist_tracks {db_playlist_id, positions_to_remove}` for every current position (1-indexed per the source; **Q9**, confirm live).
     - Then submit `add_playlist_tracks {db_playlist_id, uris}`.
   - **Don't block on these calls.** Both return a `BackgroundTask` that MA queues, and during a library sync they sat `pending` behind `music_sync` (Phase 0 finding 5).
     - Store the returned task ids in `ma_playlists.pending_task_ids`.
     - On the next sync of that playlist: skip it if a stored task is still `pending`/`running` (`tasks/get`), and mark it dirty so the following cycle picks it up.
     - Submit the add only once the remove task is `success`. Chain it from a `tasks/get` poll with a 10-min cap, rather than submitting both at once, because their ordering in MA's queue isn't guaranteed.
   - Rating keys with no MA URI are skipped and counted, and the count is shown in the status panel.
   - Don't rename a mirrored playlist when its Curatorr name changes. The builtin provider's `item_id` is the name (finding 6), so delete the playlist (`music/playlists/remove`, admin only) and recreate it.
4. **Ownership:** MA 2.10.4 can't create playlists on behalf of another user (Q7), and builtin playlists are visible to every MA user (`owner: "Music Assistant"`).
   - So mirrored playlists are shared and named `Curatorr · <display name> · <playlist name>`.
   - Add an option to mirror only selected users' playlists, to avoid cluttering every MA user's library.
   - Revisit if a later MA release adds `user` to `create_playlist`.
5. **Settings:** `mirrorPlaylists` toggle, a "Sync now" button, and per-playlist counts in the status panel. Needs `library.write`: token role `user` or above.

---

## Known issues found during this review (fix separately)

- **Jellyfin/Emby webhooks are rejected by CSRF.** `CSRF_EXEMPT_PATHS` (`src/index.js:643`) exempts only `/webhook/tautulli` and `/webhook/plex`, so POSTs to `/webhook/jellyfin` and `/webhook/emby` get 403 and only the poller works.
- **The poller calls `classifyTier` with the wrong arguments.** It calls `classifyTier(db, config, user, itemId)` (`src/routes/webhooks.js` ~1280/1347/1413), but the function is `(listenedMs, durationMs, smartConfig)`. The result is discarded, so it's dead code, but it's misleading. Remove it while extracting the recorder.
- **`src/db.js` is owned by root** in this checkout, so it isn't writable by the dev user.
- **Secret masking is incomplete.** Jellyfin/Emby API keys aren't blanked in `renderedConfig`, and the Tautulli POST overwrites the API key when the field is blank.

## Effort estimate

| Work | Estimate |
|---|---|
| Phase 0 spike + fixture | done |
| Recorder extraction (no behaviour change) + unmatched-track guard | 2–3 h |
| MA client, tracker, identity + bulk index, wiring | 5–6 h |
| Settings tab, test/status endpoints, masking, docs | 3 h |
| Phase 1 tests | 2–3 h |
| **Phase 1 total** | **≈ 12–15 h** (the spike is done, and the bulk index has moved into Phase 1) |
| Phase 2 (mirror with async task handling, settings) | 5–7 h |

The first draft estimated 6–9 h. It is higher now mainly because of the recorder extraction, identity resolution, and the user map. Those are what make the numbers correct rather than merely present.
