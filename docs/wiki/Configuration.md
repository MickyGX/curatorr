# Configuration

Curatorr is configured through environment variables (in your compose file) and through the Settings UI at `/settings`.

---

## Environment Variables

These are set in your `docker-compose.yml` and take effect on container start.

| Variable | Required | Description |
|---|---|---|
| `SESSION_SECRET` | **Yes** | Random secret for session encryption. Generate with `openssl rand -hex 32`. Never reuse this across apps. |
| `BASE_URL` | **Yes** | The URL Curatorr is served from, e.g. `http://192.168.1.x:7676` or `https://curatorr.example.com`. Used for redirects and webhook callback display. |
| `CONFIG_PATH` | No | Path to the config JSON file inside the container. Default: `/app/config/config.json` |
| `DATA_DIR` | No | Directory for the SQLite database and runtime data. Default: `/app/data` |
| `TRUST_PROXY` | No | Set to `true` if Curatorr is behind a reverse proxy (Traefik, nginx, etc.). |
| `TRUST_PROXY_HOPS` | No | Number of trusted proxy hops. Default: `1` |
| `COOKIE_SECURE` | No | Set to `true` to mark session cookies as `Secure`. Recommended when serving over HTTPS only. |
| `EMBED_ALLOWED_ORIGINS` | No | Comma-separated origins allowed to embed Curatorr in an iframe, e.g. `https://launcharr.example.com`. |

---

## Settings UI

All runtime settings are managed in the Curatorr Settings area (`/settings`). Changes take effect immediately unless otherwise noted.

### General

- **Server name** — display name shown in the UI header
- **Remote URL** — your externally accessible Curatorr URL; used in webhook callback hints
- **Local URL** — preferred for internal Plex SSO callbacks
- **Base URL Path** — set this if Curatorr is served at a subpath, e.g. `/curatorr`
- **Restrict guest users** — when enabled, guest accounts cannot view any dashboard content

### Plex

- **Local URL / Remote URL** — Curatorr prefers the local URL when set, falling back to remote
- **Plex Token** — your Plex server token (use the helper button to retrieve it via Plex.tv)
- **Machine ID** — your Plex server machine identifier (use the helper button to auto-detect)
- **Music Libraries** — select which Plex music libraries Curatorr monitors; appears after a successful test connection

### Tautulli

See [Integrations → Tautulli](Integrations#tautulli) for the full setup steps.

- **Local URL / Remote URL** — your Tautulli server address
- **API key** — found in Tautulli → Settings → Web Interface

### Lidarr (admin only)

See [Integrations → Lidarr](Integrations#lidarr) for the full setup steps.

- **Local URL / Remote URL** — your Lidarr server address
- **API key** — found in Lidarr → Settings → General
- **Automation enabled** — master switch for Curatorr's Lidarr automation features
- **Automation scope** — set to **Role based** to enforce per-role quotas
- **Auto trigger manual search** — when enabled, Curatorr immediately queues an `AlbumSearch` in Lidarr after a starter album is monitored
- **Fallback search attempts / delay** — how many times and how often Curatorr retries if Lidarr finds no files
- **Minimum peers for fallback grab** — minimum seeder count for a release to be eligible for automatic grab
- **Prefer approved releases** — prefer indexer-approved releases during fallback grab
- **Weekly quotas per role** — how many artists and albums each role can add per week; set to `-1` for unlimited

### Smart Playlists

See [Smart Playlists](Smart-Playlists) for a full explanation.

- **Default preset** — the tier preset applied to new users (Cautious, Measured, Aggressive)
- **Tier thresholds and weights** — controls when tracks are classified as Skip, Belter, etc.
- **Song skip limit** — consecutive skips before a track is auto-excluded from playlists

### Discovery

Per-user preferences for the Discover page and suggestion engine (liked/ignored genres and artists).

### Users

Admin view of all accounts, their roles, and Plex links.

### Logs

Real-time log stream from the server process.

### Jobs

Schedule and manually trigger background jobs. See [Background Jobs](Smart-Playlists#background-jobs).

### Themes / Appearance

Switch between available UI themes.
