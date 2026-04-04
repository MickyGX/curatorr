# Integrations

Curatorr supports `Plex`, `Jellyfin`, and `Emby` as its primary media server.

## Plex

Curatorr uses Plex for:

- sign-in and Plex Home user selection
- music library indexing
- smart playlist creation and updates
- live playback webhooks when playback source is set to `Plex`
- optional Tautulli live playback or gap-fill support
- Plex-only generated playlist integrations like Last.fm stations, ListenBrainz playlist suggestions, Daily Mix, Curatorr rotating playlists, sonic ordering, and loudness-aware sequencing

### Plex setup

In `Settings -> Plex`:

1. Set local and/or remote Plex URL
2. Retrieve the Plex token
3. Retrieve the machine ID
4. Select the music libraries Curatorr should monitor
5. Save

You can also use `Refresh libraries` later if you add or remove Plex music libraries after initial setup.

### Plex webhook behavior

Curatorr can register the Plex webhook automatically from the wizard or Settings.

Plex is the recommended live playback source for most installs.

## Jellyfin

Jellyfin is supported as a primary Curatorr media server.

Curatorr uses Jellyfin for:

- native sign-in
- music library indexing
- live playback tracking through the Curatorr Jellyfin webhook and native session polling
- smart playlist creation and updates
- personal, blended, and rule-based playlist sync

### Jellyfin setup

In `Settings -> Jellyfin`:

1. Set the Jellyfin URL
2. Add the API key
3. Select the music libraries Curatorr should monitor
4. Save

Jellyfin does not use Tautulli inside Curatorr.

## Emby

Emby is supported as a primary Curatorr media server.

Curatorr uses Emby for:

- native sign-in
- music library indexing
- live playback tracking through the Curatorr Emby webhook and native session polling
- smart playlist creation and updates
- personal, blended, and rule-based playlist sync

### Emby setup

In `Settings -> Emby`:

1. Set the Emby URL
2. Add the API key
3. Select the music libraries Curatorr should monitor
4. Save

Emby does not use Tautulli inside Curatorr.

## Tautulli

Tautulli is optional.

Curatorr uses Tautulli for either:

- optional live playback events, if playback source is set to `Tautulli`
- history backfill and repair through the Tautulli API

### Tautulli setup

In `Settings -> Tautulli`:

1. Set local and/or remote Tautulli URL
2. Add the API key
3. Save
4. Optionally click `Register webhook in Tautulli`

### Important distinctions

- Gap-fill/backfill only needs the URL and API key.
- The Tautulli webhook is only needed for live Tautulli playback ingestion.
- If playback source is still `Plex`, live Tautulli webhooks are ignored.

### Library filtering

Tautulli imports now respect the selected Plex music libraries:

- live webhooks from unselected libraries are ignored
- gap-fill rows from unselected libraries are ignored

Tautulli is Plex-only. It is not used on Jellyfin or Emby installs.

## Last.fm

Last.fm is optional and is used in two separate ways.

### Shared Last.fm API key

Configured in `Settings -> Discovery`.

This powers:

- Trending Artists
- Trending Tracks
- Because You Like

### Per-user Last.fm account features

Configured in `User Profile`.

Users can set:

- Last.fm username
- station playlist types
- Loved tracks playlist
- Top Tracks playlist and period
- full-history backfill controls

Last.fm history sync is not a replacement for webhook playback tracking. It is best used as historical backfill or as a supplement.

## Spotify

Spotify is optional and user-specific.

Current support is for:

- connecting a Spotify account from `User Profile`
- browsing owned Spotify playlists
- importing Spotify playlists into Curatorr as managed custom playlists
- refreshing imported Spotify playlists against the current library

Spotify requires app-level credentials on the Curatorr container:

- `SPOTIFY_CLIENT_ID`
- `SPOTIFY_CLIENT_SECRET`

### Spotify app setup

1. Go to the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard).
2. Sign in with a Spotify account.
3. Click `Create app`.
4. Give it a name and description, then select `Web API`.
5. Open the app settings.
6. Add a redirect URI that matches your Curatorr base URL.
7. Save the app settings.
8. Copy the `Client ID` into `SPOTIFY_CLIENT_ID`.
9. Click `View client secret` and copy that value into `SPOTIFY_CLIENT_SECRET`.

The Spotify developer app must also include a redirect URI that matches your Curatorr base
URL, for example:

- `http://localhost:7676/user-settings/spotify/callback`
- `https://curatorr.example.com/user-settings/spotify/callback`

These credentials identify the Curatorr app, not a single Spotify account. Each Curatorr
user still connects their own Spotify account separately through OAuth.

## ListenBrainz

ListenBrainz is optional and user-specific.

Configured in `User Profile`.

Current support is for playlist suggestions synced into Plex:

- Daily Jams
- Weekly Jams
- Weekly Exploration

Matching prefers MusicBrainz recording MBIDs when ListenBrainz provides them, and falls back to artist + track-title matching when it does not. This does not currently import ListenBrainz listening history.

Logging is available under the `ListenBrainz` filter in the Curatorr log view.

ListenBrainz playlist sync is currently Plex-only.

## Track analysis import

Track analysis is optional.

Curatorr can enrich tracks with:

- BPM
- musical key
- Camelot key
- energy
- danceability

Curatorr supports two analysis paths from `Settings -> General -> Track Analysis Import`:

- `Analyzer sidecar`
- `Custom command`

The recommended Docker deployment is the sidecar. It keeps Python audio-analysis dependencies out of the main app container while letting Curatorr run analysis automatically through `Track Analysis Pipeline`.

Minimal same-stack example:

```yaml
  curatorr_analyzer:
    image: mickygx/curatorr-analyzer:latest
    container_name: curatorr_analyzer
    depends_on:
      - curatorr
    environment:
      - PORT=8765
    volumes:
      - ./data:/app/data
      # Mount your music library at the same absolute path Plex reports for track files.
      # - /path/to/music:/media/music:ro
    network_mode: "service:curatorr"
    restart: unless-stopped
```

Sidecar mode should normally use:

- `Analyzer sidecar URL`: `http://127.0.0.1:8765`
- `Feature manifest path`: `/app/data/track-features.json`
- `Analyzer results path`: `/app/data/track-features.results.json`

Curatorr also ships with an export helper so external analyzers can work from Curatorr's library data:

```bash
npm run features:export-template
```

Important behavior:

- BPM/key-driven presets and numeric feature filters ignore tracks that do not have the required analysis data yet.
- `Track Analysis Pipeline` runs in chunks and imports results as chunks complete.
- If a run is interrupted, the next run starts again from chunk `1` of the remaining missing-track set, not from the whole library.

Full workflow details are documented in [Track Analysis](Track-Analysis.md).

## Lidarr

Lidarr is optional.

When enabled, Curatorr can:

- add artists to Lidarr
- choose a starter album
- queue search commands
- retry and fall back to release grabs when searches fail
- progressively unlock more albums as engagement improves

Weekly quotas can be configured per role, and the app also supports automatic-add quotas for eligible users.

## Feature gaps on Jellyfin and Emby

Core playback tracking, artist scoring, smart playlists, personal playlists, blended playlists, discovery, and Lidarr automation are supported.

The main Plex-only gaps at the moment are:

- Tautulli integration
- Daily Mix sync
- Last.fm station playlists
- ListenBrainz playlist suggestion sync
- Sonic ordering and loudness-aware sequencing
