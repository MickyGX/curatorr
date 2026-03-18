# Curatorr

Curatorr is a self-hosted Plex and Plexamp companion for music discovery, smart playlist generation, playback tracking, and optional Lidarr automation. It learns your listening habits over time and surfaces artists and tracks from your own library that you have not given enough attention to yet.

---

## Contents

- [What's New in v0.1.17](#whats-new-in-v0117)
- [What It Does](#what-it-does)
- [Quick Start](#quick-start)
- [Environment Variables](#environment-variables)
- [Playback Tracking](#playback-tracking)
- [Last.fm Integration](#lastfm-integration)
- [Lidarr Integration](#lidarr-integration)
- [How Artist Suggestions Work](#how-artist-suggestions-work)
- [How Lidarr Activity Works](#how-lidarr-activity-works)
- [Discover — External Artist Discovery](#discover--external-artist-discovery)
- [Smart Playlists and Track Tiers](#smart-playlists-and-track-tiers)
- [Background Jobs](#background-jobs)
- [Roles and Permissions](#roles-and-permissions)
- [Embed Mode](#embed-mode)
- [Traefik Setup](#traefik-setup)

---

## What's New in v0.1.17

- **Dashboard period toggle** — the Overview Stats panel has a `7d · 30d · All` toggle in the title bar. Each card shows the selected period's value with a comparison sub-line (e.g. "30d: 352"). All data is embedded at page load; switching periods is instant with no server round-trip.
- **Stat card icons** — all stat cards now display a large theme-coloured icon on the right side of each card with a subtle brand-colour glow. Icons adapt automatically to your Curatorr theme colour.
- **Library Artists glance card** — the "Curatorr at a glance" panel now shows a unique Library Artists count, including artists from Various Artists compilations but excluding duplicates and placeholder names.
- **Playlist delta badges** — playlist artwork on the dashboard and Playlists page now shows a ↑/↓/= pill overlay tracking how many tracks were added or removed in the last sync. Persisted per sync via two new database columns.
- **Last.fm dedup fix** — the scrobble backfill duplicate-detection window now uses the known track duration rather than a fixed 90 s window, handles Unicode hyphens in Tautulli-stored artist names, and matches Tautulli's `"Track - Artist"` title suffix pattern.

---

## What It Does

- **Smart Playlists** — builds and maintains per-user playlists in Plex based on your play history, skips, and listening trends
- **Personal Playlists** — lets users build their own rule-based playlists on top of Curatorr's scoring and filter system
- **Artist Suggestions** — scores every artist in your library against your taste profile and surfaces ones you have under-explored
- **Track Tiers** — classifies each track as Belter, Decent, Half Decent, Skip, or Curatorr (unclassified) based on real playback behaviour
- **Lidarr Automation** — when connected to Lidarr, suggested artists can be added with a starter album, monitored, and progressively expanded as you engage with them
- **History** — full per-user playback log driven by Plex webhooks, with optional Tautulli repair and backfill
- **Last.fm Sync** — optional backfill of scrobble history for users who have a Last.fm account, as a supplement to webhook-based tracking
- **Discover** — surfaces artists outside your library via Last.fm, includes manual artist search, queue handling, and per-user add history

---

## Screenshots

### Dashboard

![Curatorr dashboard overview](docs/media/curatorr-dashboard.png)

### Discover

![Curatorr discover page](docs/media/curatorr-discover.png)

### Playlists

![Curatorr playlists page](docs/media/curatorr-playlists.png)

### Login

![Curatorr login page](docs/media/curatorr-login.png)

---

## Quick Start

### 1. Create a docker-compose file

```yaml
services:
  curatorr:
    container_name: curatorr
    image: mickygx/curatorr:latest
    ports:
      - "7676:7676"
    environment:
      - CONFIG_PATH=/app/config/config.json
      - DATA_DIR=/app/data
      - BASE_URL=http://localhost:7676
      - TRUST_PROXY=true
      - TRUST_PROXY_HOPS=1
      - SESSION_SECRET=replace-this-with-a-random-secret   # openssl rand -hex 32
    volumes:
      - ./config:/app/config
      - ./data:/app/data
    restart: unless-stopped
```

### 2. Start the container

```bash
docker compose up -d
```

### 3. Complete the setup wizard

Open `http://localhost:7676/wizard` in your browser and follow the steps to connect Plex, optionally Tautulli, and optionally Lidarr.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `SESSION_SECRET` | Yes | Random secret for session encryption. Generate with `openssl rand -hex 32`. |
| `BASE_URL` | Yes | The URL Curatorr is served from. Used for redirects and webhook callbacks. |
| `CONFIG_PATH` | No | Path to the config file inside the container. Default: `/app/config/config.json` |
| `DATA_DIR` | No | Directory for the SQLite database and other runtime data. Default: `/app/data` |
| `TRUST_PROXY` | No | Set to `true` if Curatorr is behind a reverse proxy. |
| `TRUST_PROXY_HOPS` | No | Number of trusted proxy hops. Default: `1` |
| `COOKIE_SECURE` | No | Set to `true` to mark session cookies as Secure (HTTPS only). Recommended when behind HTTPS. |
| `EMBED_ALLOWED_ORIGINS` | No | Comma-separated list of origins permitted to embed Curatorr in an iframe, e.g. `https://launcharr.example.com` |
| `LOCAL_AUTH_MIN_PASSWORD` | No | Minimum password length for local Curatorr accounts. Default: `12`. Capped between `1` and `128`. Useful for local network installs where a strict policy is not required. Not recommended below `12` if the instance is internet-facing — Curatorr will log a warning at startup if this is set below `12` and `TRUST_PROXY` or `COOKIE_SECURE` is also enabled. |

---

## Playback Tracking

Curatorr is Plex-first for playback tracking.

- **Plex webhooks** provide the primary real-time play, pause, resume, stop, and scrobble events
- **Tautulli** is optional and is used for backfill and repair when you want an extra source of playback history
- **Split listens** within a 10-event lookback are consolidated into one play event, which reduces false skips caused by pause/resume or quick restart flows

### Plex webhook setup

Curatorr registers the Plex webhook automatically during the setup wizard and from **Settings → Plex → Register webhook in Plex**. When you click that button, Curatorr calls the Plex account API directly to add its own URL and also enables server webhooks on your Plex Media Server — no manual steps required.

The registered URL includes a shared secret key as a query parameter for security:

```text
http://your-curatorr-url:7676/webhook/plex?key=<your-webhook-secret>
```

The secret is either taken from the `WEBHOOK_SECRET` environment variable or generated automatically by Curatorr on first run and stored in config. If you need to register manually, copy the exact URL (including the `?key=` parameter) from **Settings → Plex** — the full URL is shown there for admins.

Use your public HTTPS URL if Curatorr is behind a reverse proxy.

### Optional Tautulli webhook setup

Tautulli is best used as a daily gap-fill backup rather than a live tracking source. Curatorr will fill in any plays missed by the Plex webhook without overwriting Plex-recorded listens.

Curatorr can register the Tautulli webhook automatically: go to **Settings → Tautulli**, enter your Tautulli URL and API key, save, then click **Register webhook in Tautulli**. Curatorr calls the Tautulli API to create the notification agent for you.

To configure manually instead:

1. Go to **Settings → Notification Agents → Add a new notification agent → Webhook**
2. Set the **Webhook URL** to:
   ```
   http://your-curatorr-url:7676/webhook/tautulli?key=<your-webhook-secret>
   ```
3. Set the **Method** to `POST`
4. Enable: **Playback Start**, **Playback Stop**, **Playback Pause**, **Playback Resume**, **Watched**

> The `?key=` parameter must match the `WEBHOOK_SECRET` environment variable, or the secret Curatorr generated automatically (visible in **Settings → Plex** for admins).

---

## Last.fm Integration

Curatorr uses Last.fm in two distinct ways: as a **discovery data source** for the Discover page, and as an **optional history backfill** for users who have a Last.fm account.

### Last.fm API key

Both features require a Last.fm API key. Create a free account at [last.fm](https://www.last.fm), generate an API key in your account settings, then enter it in **Settings → Discovery → Last.fm API key**.

### Discovery (Trending and Similar Artists)

The Discover page uses the Last.fm API to surface:

- **Trending Artists and Tracks** — pulled from Last.fm's global charts
- **Because You Like…** — artists similar to your most-played artists, personalised to your actual listening profile

This requires only the shared Last.fm API key in Settings → Discovery and works without any per-user Last.fm account.

### History Sync (optional, per-user)

Each user can optionally connect their own Last.fm account to backfill scrobble history into Curatorr. This is useful if you have years of Last.fm history predating your Plex setup, or as a safety net for plays that occurred when Curatorr was not running.

To enable, each user saves their Last.fm username in **User Profile → Last.fm username**. The background **Last.fm History Sync** job then periodically fetches recent scrobbles and inserts any plays not already recorded.

The job is **disabled by default**. Enable it in **Settings → Jobs** once at least one user has saved a Last.fm username and the shared API key is configured.

### Limitations of Last.fm sync vs. webhooks

Last.fm sync is a best-effort backfill and has real limitations compared to webhook-based tracking:

| | Plex webhook | Last.fm sync |
|---|---|---|
| Real-time | Yes — events arrive as you listen | No — batch job runs on a schedule |
| Play duration | Full ms-accurate duration per event | Not available — scrobbles carry no duration |
| Track tier accuracy | Full (Belter/Decent/Half Decent/Skip calculated from duration) | Assumed completed play — always treated as Belter |
| Skip detection | Yes | No — skips are never scrobbled |
| Artist-only fallback | N/A | Yes — unmatched tracks still contribute to artist stats |
| Per-track stats | Full | Only if a prior Plex play exists to supply the duration |

**Plex webhooks are the primary and most accurate tracking source.** Last.fm sync is a supplement — ideal for historical backfill and as a passive fallback, but it cannot replace real-time webhook tracking for accurate tier classification and skip detection.

---

## Lidarr Integration

Curatorr can connect to Lidarr to automate adding suggested artists to your music library. This is entirely optional.

**To connect:**

1. Go to **Settings → Lidarr** in Curatorr
2. Enter your Lidarr local and/or remote URL
3. Enter your Lidarr API key (found in Lidarr → Settings → General)
4. Enable **Lidarr automation** and set the scope to **Role based**

**How automation works:**

- When you click **Add to Lidarr** on a suggested artist, Curatorr adds the artist to Lidarr, picks a starter album based on your taste, and monitors it
- Curatorr applies its own management tags to artists it adds in Lidarr and can backfill those tags for existing Curatorr-managed artists
- If enabled, Curatorr immediately triggers a Lidarr `AlbumSearch` for the starter album
- If the search returns no files, Curatorr can re-monitor, retry the search, and fall back to manually grabbing the best available release
- As you engage with the artist, Curatorr progressively unlocks additional albums

> Note: some Lidarr builds expose artist tags but not album tags. Curatorr detects that case and logs album tagging as unsupported instead of pretending it succeeded.

**Weekly quotas** can be set per role to limit how many artists and albums each user tier can add each week (see [Roles and Permissions](#roles-and-permissions)).

---

## How Artist Suggestions Work

The **Suggested Artists** panel on the Artists page shows artists already in your Plex library that you have under-explored, ranked by how well they match your current taste profile. Only artists **not yet in Lidarr** appear here — artists already in Lidarr move to the Lidarr Activity panel instead.

### Step 1 — Build your taste profile

Curatorr reads your listening history and preferences:

- Your **top artists** by ranking score and play count
- Your **recently played tracks** (last 25) including their tiers (Belter, Decent, Half Decent, Skip)
- Your manually **liked and ignored** genres and artists (set in your user settings)

### Step 2 — Build genre affinity

A weighted score is calculated for every genre based on how much you engage with it:

| Signal | Weight |
|---|---|
| Manually liked genre | +4 |
| Manually ignored genre | −5 |
| Genre of a top artist (scales with rank + plays) | up to +8 |
| Genre of a recent Belter track | +3.5 |
| Genre of a recent Half Decent track | +2.25 |
| Genre of a recent Decent track | +1.25 |
| Genre of a recent Skip track | −2.5 |

### Step 3 — Score every library artist

Three components are combined into a `totalScore` for each artist:

**Genre Score** — how well the artist's genres match your affinity (top 3 genre weights summed).

**Behaviour Score** — based on how much you have actually listened to them:

| Condition | Points |
|---|---|
| Never played | +4 |
| 1–2 plays | +2.75 |
| 3–5 plays | +1.5 |
| 6+ plays | decreases gradually |
| Not played in 30+ days | +1.5 |
| Not played in 90+ days | additional +1 |
| High ranking score (above 3) | small bonus |
| Each skip on record | −0.5 |

**Editorial Score** — library signals:

| Condition | Points |
|---|---|
| Already a top artist | −3 |
| Has 2 or more albums in library | +0.75 |
| Has 8 or more tracks in library | +0.5 |
| Genre appears in your liked genres | +1 |

Artists with a `totalScore` of 0.5 or below are dropped entirely. Artists you have manually excluded, or those with 12+ plays and a ranking score of 7 or above (well-established favourites), are also excluded.

The **top 12 by score** are shown, each displaying their top 3 matching genres and album count as a subtitle. The score shown on each row is this combined total.

---

## How Lidarr Activity Works

The **Lidarr Activity** panel on the Artists page shows what Curatorr has done with Lidarr — artists added, search statuses, download progress, quota blocks, and progression stages.

### What appears here

An artist appears in Lidarr Activity when any of the following are true:

- It is already in your Lidarr library (detected via live Lidarr API)
- Curatorr has a local progress record for it (i.e. it was previously acted on)
- Its suggestion status is anything other than plain `Suggested` (e.g. added, queued, quota blocked)

Artists that are purely **Suggested** and not yet in Lidarr appear in the Suggested Artists panel above, not here.

### Status labels

Each entry shows one of the following status badges, determined in priority order:

| Status | Meaning |
|---|---|
| **Queued** | Waiting for quota to free up before being added |
| **Quota blocked** | Weekly artist or album limit has been reached |
| **Downloaded** | Track files exist in Lidarr for this artist |
| **Search queued** | Lidarr search command is queued |
| **Search running** | Lidarr is actively searching |
| **Search complete** | Search finished and files were found |
| **Search finished** | Search finished but no files found yet |
| **Search failed** | Lidarr search command failed |
| **Starter album added** | Artist added to Lidarr with an initial album monitored |
| **Next album added** | A subsequent album has been unlocked and added |
| **Catalog complete** | No further album unlocks are pending |
| **Awaiting belter** | Waiting for a stronger listening signal before expanding |
| **Artist added** | Base state after being added to Lidarr |

Items are sorted by most recently updated and capped at 8 entries. The count chips in the panel header (e.g. **3 Downloaded**) are a tally of each label across all items.

---

## Discover — External Artist Discovery

The Discover page goes beyond your Plex library and lets you find and add artists that do not yet exist in your collection.

### What it shows

**Trending Artists and Trending Tracks** — pulled from Last.fm's global charts. Requires a Last.fm API key configured in Settings → Discovery.

**Because You Like…** — similar artists to your top-played artists, sourced from Last.fm's similar-artist data. These are personalised based on your actual listening profile, not generic recommendations.

**Manual Discovery** — search for any artist by name. Results come from a Lidarr lookup (MusicBrainz-backed), so you can find and add any artist regardless of whether they are in your library.

### Adding artists from Discover

All Discover adds are manual — you pick the artist, optionally choose a specific album, and click to add. Curatorr then handles the Lidarr add, monitors a starter album, and searches for it. Weekly role quotas apply. If your quota is full, the request is queued automatically and processed when quota resets.

The Discover page also keeps a **Queue** panel for deferred requests and an **Added For You** panel so users can see what Curatorr has already added on their behalf.

> Automatic adding of external artists (without a manual add action) is not currently implemented. The Discover page is a manual curation tool. For Last.fm API key setup, see [Last.fm Integration](#lastfm-integration).

---

## Smart Playlists and Track Tiers

Curatorr classifies every track based on your real playback behaviour and uses these tiers to build and maintain smart playlists in Plex.

### Track tiers

| Tier | How it is assigned |
|---|---|
| **Belter** | Played through to or near completion (configurable threshold in seconds) |
| **Decent** | Played past 50% of the track |
| **Half Decent** | Played less than 50% of the track before moving on |
| **Skip** | Skipped within the configurable skip threshold (default: 30 seconds) |
| **Curatorr** | Not yet classified — not enough data |

### Playlist presets

Three presets are available and can be set as the default for new users:

- **Cautious** — larger playlist, wider range of artists
- **Measured** — medium playlist, balanced mix
- **Aggressive** — smaller playlist, focused on proven favourites

### Skip protection

Consecutive skips on the same track are tracked as a **skip streak**. Once a track reaches the configured song skip limit (default: 2 consecutive skips), it is automatically excluded from smart playlists. You can reset an artist's skip streak on the Artists page.

---

## Background Jobs

Curatorr runs several scheduled background jobs. Intervals can be adjusted in **Settings → Jobs** (admin only).

| Job | Default interval | Description |
|---|---|---|
| Master Track Cache Refresh | Every 6 hours | Fetches all tracks from Plex and updates genres, rating counts, and view counts in the local cache |
| Smart Playlist Sync | Every 30 minutes | Rebuilds each user's smart playlist and syncs it to Plex |
| Lidarr: Review Due Artists | Every 30 minutes | Reviews suggested artists and queues Lidarr searches for artists that are due |
| Lidarr: Process Queued Requests | Every 20 minutes | Processes pending Lidarr add and monitor requests |
| Daily Mix Sync | Daily | Builds each user's Daily Mix playlist from recent favourites, suggestions, and fresh library tracks |
| Tautulli History Sync | Daily | If Tautulli is configured, backfills or repairs any plays missed by Plex webhook ingestion |
| Last.fm History Sync | Every 6 hours (disabled by default) | Fetches recent scrobbles from Last.fm for users with a Last.fm username saved, and backfills plays not already in Curatorr |

---

## Roles and Permissions

Curatorr supports two account types:

- **Plex SSO accounts** — users sign in with their Plex account. Roles are assigned in **Settings → Users → Plex Users** and stored in Curatorr's config.
- **Local Curatorr accounts** — created in **Settings → Users → Curatorr Users**. Local accounts can sign in without Plex and use the same role system. Useful for admin access when Plex is unavailable or for users who do not have a Plex account.

| Role | Lidarr access | Weekly quota (default) |
|---|---|---|
| **Admin** | Full access, all settings | Unlimited artists and albums |
| **Co-admin** | Automation access | 3 artists / 6 albums per week |
| **Power user** | Automation access (when enabled) | 1 artist / 2 albums per week |
| **User** | No automation by default | 0 (configurable) |
| **Guest** | Read-only | None |

Weekly quota limits are configurable per role in **Settings → Lidarr → Automation**. Set any quota to `-1` for unlimited.

Admin accounts (both Plex and local) can view global listening data across users for the dashboard, history, artists, and tracks pages. User-specific discovery queues, preferences, and playlists remain scoped per account.

> **Plex server owner note:** the Plex API excludes the server owner from the managed users list. Curatorr fetches the owner separately and shows them at the top of the Plex Users table, locked to the Admin role. If the owner was previously appearing as a normal user, set the **Plex Admin User** field in **Settings → Plex** to their Plex username or email — on next login via Plex SSO they will be automatically promoted and their identifier saved to the admin list.

---

## Embed Mode

To render Curatorr inside a Launcharr iframe (or any other dashboard) without Curatorr's own sidebar, title bar, and background chrome, append `?embed=launcharr` to any page URL:

```
http://your-curatorr-url:7676/dashboard?embed=launcharr
```

If the embedding dashboard is on a different origin, add that origin to the `EMBED_ALLOWED_ORIGINS` environment variable:

```
EMBED_ALLOWED_ORIGINS=http://192.168.0.2:3333
```

Multiple origins can be comma-separated.

---

## Traefik Setup

For a Traefik reverse proxy setup, see the example below. Replace `curatorr.example.com` with your own domain and ensure a `proxy` external Docker network exists.

```yaml
services:
  curatorr:
    container_name: curatorr
    image: mickygx/curatorr:latest
    environment:
      - CONFIG_PATH=/app/config/config.json
      - DATA_DIR=/app/data
      - BASE_URL=https://curatorr.example.com
      - TRUST_PROXY=true
      - TRUST_PROXY_HOPS=1
      - SESSION_SECRET=replace-this-with-a-random-secret
      - COOKIE_SECURE=true
    labels:
      - traefik.enable=true
      - traefik.docker.network=proxy
      - traefik.http.routers.curatorr.rule=Host(`curatorr.example.com`)
      - traefik.http.routers.curatorr.entrypoints=websecure
      - traefik.http.routers.curatorr.tls.certresolver=letsencrypt
      - traefik.http.services.curatorr.loadbalancer.server.port=7676
    volumes:
      - ./config:/app/config
      - ./data:/app/data
    networks:
      - proxy
    restart: unless-stopped

networks:
  proxy:
    external: true
```
