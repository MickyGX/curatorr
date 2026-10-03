# Troubleshooting

## Plays are not being recorded

First check which live playback source is selected in `Settings -> General`.

### If playback source is `Plex`

- confirm the Plex webhook is registered
- confirm plays are arriving in Curatorr logs
- confirm the track is in a selected Plex music library

### If playback source is `Tautulli`

- confirm the Tautulli webhook is registered
- confirm the Tautulli URL and API key are valid
- confirm the track is in a selected Plex music library

If live playback source is `Plex`, Tautulli webhooks being absent is not the problem.

### If your media server is `Jellyfin` or `Emby`

- confirm the server URL and API key are valid
- confirm Curatorr can reach the media server from the container
- confirm the track is in a selected music library
- confirm recent playback/session activity is showing up in Curatorr logs

### If playback is through Music Assistant

For Music Assistant, check its separate enable switch, connection status, and user mappings in **Settings → Music Assistant**. An identified but unmapped MA user is ignored even if a default listener is selected. A stopped track can take about 60 seconds to settle. See the [MA troubleshooting table](Music-Assistant.md#status-and-troubleshooting).

## Restored playlists are missing tracks

- run **Master Track Cache Refresh** against the server you restored to, then use **Refresh import** on the playlist; found tracks return to their original positions
- check that the files are in a music library selected in Curatorr; matching uses file path, MusicBrainz ID, or artist and title, so retagged artist names may not match
- smart and system playlists rebuild from rules and settings, so they have no missing list; check their rules match your new library

See [Playlist Backup and Restore](Playlist-Backup.md).

## Tautulli gap-fill is not importing expected rows

Check:

- the Tautulli URL and API key
- that the target rows are inside the gap-fill lookback window
- that Tautulli is reporting the item as `media_type = track`
- that the library is currently selected in `Settings -> Plex`

Gap-fill does not require the Tautulli webhook.

## Excluded library plays are still visible

Curatorr only cleans its own derived data when a library is deselected. It does not modify Plex, Jellyfin, Emby, or Tautulli history.

If old plays remain after deselecting a library, they may have been written before library-key tracking was complete or may need direct cleanup from Curatorr's `play_events` table.

## Playlist changes are not appearing in the media server

Check:

- media-server connection settings
- selected music libraries
- Smart Playlist Sync job status
- whether the target account has playlist write access

For Plex-specific playlist problems, also verify the Plex token and machine ID.

## New music library is missing from settings

Use the relevant refresh/save flow in the media-server settings after the library exists server-side.

On Plex, this is `Settings -> Plex -> Refresh libraries`.

## ListenBrainz playlists are not appearing

Check:

- ListenBrainz username in User Profile
- optional token if needed for the selected feed
- the source chosen in Playlists → Import playlist → ListenBrainz
- Smart Playlist / playlist sync job status
- log entries under the `ListenBrainz` filter

## Lidarr automation is not available

Check:

- Lidarr connection details
- automation enabled state
- automation scope
- current role quota

## Imported playlist is incomplete or has wrong matches

- Refresh the master track cache after adding music, then use **Refresh import**.
- Review **Missing from source**; named artists no longer fall back to unrelated tracks with the same title.
- For incomplete Spotify URL previews, import an owned copy through the connected Spotify tab.
- If a TIDAL URL reports the playlist as not found, it is private to another account. Make it public in TIDAL, or import it from the owner's connected TIDAL tab.
- M3U auto-refresh rematches the stored upload. Very old imports without source content may need to be imported again.

See [Playlist Imports](Playlist-Imports.md).

## Audio profile is unavailable or produces few tracks

Check analysis coverage, active content filters, and output caps in the playlist wizard. Missing BPM/key/energy values are not treated as zero. Confirm the sidecar sees the same data files and music paths, then inspect **Track Analysis Pipeline** progress. Use throttling and the decode-duration cap for resource-heavy libraries: [Track Analysis](Track-Analysis.md).

## Artist suggestions are missing

Look in **Discover → Artist Pipeline**, then run or schedule **Artist Pipeline Rebuild**. Last.fm suggestions require the shared Discovery API key. Check personal artist filters and Lidarr configuration if acquisition actions are absent.

## Session/login checks

Check:

- `SESSION_SECRET`
- proxy headers and `TRUST_PROXY`
- `COOKIE_SECURE` for HTTPS
- local URL and base URL accuracy

## Logs

Use `Settings -> Logs` and filter by app/component:

- `plex`
- `webhook`
- `tautulli-sync`
- `music-assistant`
- `lidarr`
- `listenbrainz`
- `settings`

This is usually the fastest way to confirm whether Curatorr ignored, imported, or rejected an event.
