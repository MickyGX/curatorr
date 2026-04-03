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

### Smart Playlist Rules

- default preset for new users
- Curatorr tier thresholds and weights
- song skip limit
- Crescive and Curative starting-position rules
- addition and subtraction rules

### Discovery

- shared Last.fm API key
- discovery panel controls

### Users

- view user roles and linked Plex, Jellyfin, or Emby identities
- change roles
- remove users

### Logs

- filter by app/component
- inspect Recent, Plex, Tautulli, Lidarr, Last.fm, ListenBrainz, and Settings activity

### Jobs

- enable or disable background jobs
- adjust intervals
- run jobs manually

### Themes

- save the current theme as the global default

## User Profile

Each user also has `User Profile` settings for:

- Spotify account connection for playlist import
- Last.fm username and playlist options
- Last.fm full-history backfill controls
- ListenBrainz username, token, and playlist options
- personal theme selection
- artist include/exclude lists

## Data Storage

Curatorr stores runtime data in `DATA_DIR`, including:

- `curatorr.db`
- logs
- generated secrets and runtime metadata

Back up `DATA_DIR` regularly if you want to preserve history and stats.
