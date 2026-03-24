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

## Tautulli gap-fill is not importing expected rows

Check:

- the Tautulli URL and API key
- that the target rows are inside the gap-fill lookback window
- that Tautulli is reporting the item as `media_type = track`
- that the library is currently selected in `Settings -> Plex`

Gap-fill does not require the Tautulli webhook.

## Excluded Plex library plays are still visible

Curatorr only cleans its own derived data when a Plex library is deselected. It does not modify Plex or Tautulli history.

If old plays remain after deselecting a library, they may have been written before library-key tracking was complete or may need direct cleanup from Curatorr's `play_events` table.

## Smart playlist is not appearing in Plex

Check:

- Plex token and machine ID
- selected music libraries
- Smart Playlist Sync job status
- whether the Plex account used has playlist write access

## New Plex music library is missing from settings

Use `Settings -> Plex -> Refresh libraries`.

The checklist is not automatically refreshed just because a new Plex library was added server-side.

## ListenBrainz playlists are not appearing

Check:

- ListenBrainz username in User Profile
- optional token if needed for the selected feed
- selected playlist types
- Smart Playlist / playlist sync job status
- log entries under the `ListenBrainz` filter

## Lidarr automation is not available

Check:

- Lidarr connection details
- automation enabled state
- automation scope
- current role quota

## Session/login problems

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
- `lidarr`
- `listenbrainz`
- `settings`

This is usually the fastest way to confirm whether Curatorr ignored, imported, or rejected an event.
