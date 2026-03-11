# Curatorr

Curatorr is a self-hosted Plex and Plexamp companion for music discovery, smart playlist generation, playback tracking, and optional Lidarr automation. It learns your listening habits over time and surfaces artists and tracks from your own library that you have not given enough attention to yet.

---

## Contents

- [What It Does](#what-it-does)
- [Quick Start](#quick-start)
- [Environment Variables](#environment-variables)
- [Tautulli Webhook Setup](#tautulli-webhook-setup)
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

## What It Does

- **Smart Playlists** — builds and maintains per-user playlists in Plex based on your play history, skips, and listening trends
- **Artist Suggestions** — scores every artist in your library against your taste profile and surfaces ones you have under-explored
- **Track Tiers** — classifies each track as Belter, Decent, Half Decent, Skip, or Curatorr (unclassified) based on real playback behaviour
- **Lidarr Automation** — when connected to Lidarr, suggested artists can be added with a starter album, monitored, and progressively expanded as you engage with them
- **History** — full per-user playback log synced from Tautulli and real-time Plex webhooks
- **Discover** — surfaces artists outside your library via Last.fm (trending and similar-artist data), and lets you manually search for and add any artist to Lidarr directly

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

Open `http://localhost:7676/wizard` in your browser and follow the steps to connect Plex, Tautulli, and optionally Lidarr.

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

---

## Tautulli Webhook Setup

Curatorr uses Tautulli to receive real-time playback events and to backfill play history. Without it, no play or skip data will be recorded.

**In Tautulli:**

1. Go to **Settings → Notification Agents → Add a new notification agent → Webhook**
2. Set the **Webhook URL** to:
   ```
   http://your-curatorr-url:7676/webhook/tautulli
   ```
3. Set the **Method** to `POST`
4. Enable the following **Triggers**: Playback Start, Playback Stop, Playback Pause, Playback Resume, Watched

Then in Curatorr's Settings → Tautulli tab, enter your Tautulli URL and API key.

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
- If enabled, Curatorr immediately triggers a Lidarr `AlbumSearch` for the starter album
- If the search returns no files, Curatorr can re-monitor, retry the search, and fall back to manually grabbing the best available release
- As you engage with the artist, Curatorr progressively unlocks additional albums

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

### Last.fm setup

To enable Trending and Similar Artist panels:

1. Create a free account at last.fm and generate an API key in your account settings
2. In Curatorr go to **Settings → Discovery**
3. Enter your Last.fm API key and choose which panels to show

> Automatic adding of external artists (without a manual add action) is not currently implemented. The Discover page is a manual curation tool.

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
| Tautulli History Sync | Daily | Backfills any plays missed by real-time webhooks |

---

## Roles and Permissions

Curatorr uses role-based access for users connected via Plex authentication.

| Role | Lidarr access | Weekly quota (default) |
|---|---|---|
| **Admin** | Full access, all settings | Unlimited artists and albums |
| **Co-admin** | Automation access | 3 artists / 6 albums per week |
| **Power user** | Automation access (when enabled) | 1 artist / 2 albums per week |
| **User** | No automation by default | 0 (configurable) |
| **Guest** | Read-only | None |

Weekly quota limits are configurable per role in **Settings → Lidarr → Automation**. Set any quota to `-1` for unlimited.

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
