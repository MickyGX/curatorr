# Configuration

Curatorr is configured through container environment variables and the Settings UI.

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `SESSION_SECRET` | Yes | Session encryption secret. Generate with `openssl rand -hex 32`. |
| `WEBHOOK_SECRET` | No | Shared secret used to protect Curatorr webhook endpoints. If omitted, Curatorr generates and stores one in config. |
| `BASE_URL` | Yes | Public or local URL Curatorr is served from. Used in redirects and webhook registration. |
| `CONFIG_PATH` | No | Config file path inside the container. Default: `/app/config/config.json` |
| `DATA_DIR` | No | Runtime data directory. Default: `/app/data` |
| `TRUST_PROXY` | No | Set `true` behind a reverse proxy. |
| `TRUST_PROXY_HOPS` | No | Trusted proxy hop count. Default: `1` |
| `COOKIE_SECURE` | No | Mark cookies as secure when serving over HTTPS only. |
| `EMBED_ALLOWED_ORIGINS` | No | Comma-separated list of origins allowed to embed Curatorr in an iframe. |
| `LOCAL_AUTH_MIN_PASSWORD` | No | Minimum password length for local Curatorr accounts. Default: `12` |
| `SPOTIFY_CLIENT_ID` | No | Spotify app client ID used for Spotify playlist import and refresh. |
| `SPOTIFY_CLIENT_SECRET` | No | Spotify app client secret used for Spotify playlist import and refresh. |
| `TIDAL_CLIENT_ID` | No | TIDAL app client ID used for TIDAL playlist import and refresh. |
| `TIDAL_CLIENT_SECRET` | No | TIDAL app client secret used for TIDAL playlist import and refresh. |
| `TIDAL_COUNTRY_CODE` | No | Two-letter catalog region for public TIDAL playlist links read without a connected account. Default: `US` |
| `YOUTUBE_API_KEY` | No | YouTube Data API v3 key used for public YouTube playlist URL import. |
| `PLAYLIST_BACKUP_BODY_LIMIT` | No | Largest playlist backup file accepted for restore. Default: `64mb` |

For Spotify:

1. Create an app in the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard).
2. Add a redirect URI that matches your Curatorr base URL, for example `http://localhost:7676/user-settings/spotify/callback`.
3. Put the app `Client ID` into `SPOTIFY_CLIENT_ID`.
4. Put the app `Client Secret` into `SPOTIFY_CLIENT_SECRET`.

For TIDAL:

1. Create an app in the [TIDAL Developer Dashboard](https://developer.tidal.com/dashboard) with the `playlists.read` and `user.read` scopes.
2. Add a redirect URI that matches your Curatorr base URL, for example `http://localhost:7676/user-settings/tidal/callback`.
3. Put the app `Client ID` into `TIDAL_CLIENT_ID`.
4. Put the app `Client Secret` into `TIDAL_CLIENT_SECRET`.

For YouTube:

1. Create or select a project in the [Google Cloud Console](https://console.cloud.google.com/).
2. Enable the `YouTube Data API v3` for that project.
3. Create an API key credential.
4. Put the API key into `YOUTUBE_API_KEY`.

## Settings UI

All runtime configuration lives in `Settings`.

The visible media-server settings depend on the server type selected in the setup wizard:

- `Plex`
- `Jellyfin`
- `Emby`

### General

- server name and URLs
- playback source (`Plex` or `Tautulli`) on Plex installs
- guest restriction behavior
- global theme defaults

### Plex

- local and remote Plex URLs
- Plex token and machine ID helpers
- selected music libraries
- `Refresh libraries` action to re-pull the available Plex music library list

Important behavior:

- Curatorr only ingests from the selected Plex libraries.
- If you deselect a library and save, Curatorr removes that library's derived Curatorr data from its own database.
- This cleanup affects Curatorr data only, not Plex or Tautulli itself.

### Jellyfin

- Jellyfin server URL
- API key
- selected music libraries
- webhook/session-based playback tracking

### Emby

- Emby server URL
- API key
- selected music libraries
- webhook/session-based playback tracking

### Tautulli

- URL and API key
- webhook registration helper

Tautulli can be used for:

- live playback, if selected as the active playback source
- manual or scheduled gap-fill/backfill

Tautulli is only relevant for Plex installs.

### Lidarr

- connection details
- automation enablement and scope
- fallback search and release-grab settings
- weekly quotas per role
- automatic add quotas

### Music Assistant

- optional additional play tracking, independent of the primary playback-source selector
- direct MA server URL and saved long-lived token
- connection test, library provider selection, and MA-user-to-Curatorr-listener mappings
- optional default listener for events with no MA user
- live connection status, play/match counters, and token expiry

This tab requires an actual admin account. No extra container variable is required. See [Music Assistant](Music-Assistant.md) for setup and troubleshooting.

### Playlists

- default preset for new users
- Curatorr tier thresholds and weights
- song skip limit
- Crescive and Curative starting-position rules
- addition and subtraction rules

The Playlists tab contains the shared defaults and system-playlist configuration. Personal, blended, and global rule-based playlists are built from the main [Playlists page](Smart-Playlists.md).

### Discovery

- shared Last.fm API key
- discovery panel controls

### Users

- view user roles and linked Plex, Jellyfin, or Emby identities
- change roles
- remove users

### Logs

- filter by app/component
- inspect playback, `music-assistant`, Lidarr, Last.fm, ListenBrainz, analyzer, and Settings activity

### Jobs

- enable or disable background jobs
- adjust intervals
- run jobs manually

Use **Artist Pipeline Rebuild** to refresh recommendations, **Master Track Cache Refresh** after library changes, and **Track Analysis Pipeline** for audio enrichment. These jobs have different purposes; playlist rebuild/refresh depends on the library cache already being current.

### Themes

- save the current theme as the global default

## User Profile

Each user also has `User Profile` settings for:

- Spotify account connection for playlist import
- TIDAL account connection for playlist import
- Last.fm username (playlist sources are selected from the import dialog)
- Last.fm full-history backfill controls
- ListenBrainz username and token (playlist suggestions are selected from the import dialog)
- personal theme selection
- artist include/exclude lists

## Data Storage

Curatorr stores runtime data in `DATA_DIR`, including:

- `curatorr.db`
- logs
- generated secrets and runtime metadata

Back up `DATA_DIR` and the file at `CONFIG_PATH` to preserve history, imported M3U source content, artwork, integration settings, and listener mappings. Keep configuration backups private because they contain credentials.

For playlists alone, **Playlists → Back up and restore playlists** downloads a portable backup file that can rebuild them on a reinstalled or new media server. See [Playlist Backup and Restore](Playlist-Backup.md).
