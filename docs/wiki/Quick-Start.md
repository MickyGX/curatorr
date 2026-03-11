# Quick Start

## 1. Create your compose file

Create a `docker-compose.yml` with the following content. Change `SESSION_SECRET` to a randomly generated value (`openssl rand -hex 32`).

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
      - SESSION_SECRET=replace-this-with-a-random-secret
    volumes:
      - ./config:/app/config
      - ./data:/app/data
    restart: unless-stopped
```

## 2. Start the container

```bash
docker compose up -d
```

## 3. Complete the setup wizard

Open `http://localhost:7676/wizard` in your browser.

The wizard walks you through:

1. **Plex connection** — enter your Plex server URL, token, and machine ID
2. **Admin account** — create a local admin account or sign in with Plex
3. **Tautulli connection** — enter your Tautulli URL and API key, then configure the webhook (see [Integrations](Integrations))
4. **Lidarr connection** — optional; enter your Lidarr URL and API key if you want automation

## 4. Let Curatorr build your library cache

After setup, Curatorr runs a **Master Track Cache Refresh** to index your Plex music library. This may take a few minutes for large libraries. Once complete, smart playlists and artist suggestions will begin appearing.

## What to expect

- **Smart Playlists** — a Curatorr playlist appears in Plex within the first sync cycle (default: 30 minutes)
- **Track tiers** — tracks are classified as plays come in via Tautulli webhooks; the first few plays set the baseline
- **Artist Suggestions** — the Artists page starts showing scored suggestions once you have enough play history to build a taste profile
- **Lidarr Activity** — appears as soon as you add a first artist through the suggestion panel

## Next steps

- [Configuration](Configuration) — environment variables and settings reference
- [Integrations](Integrations) — Tautulli webhook setup and Lidarr connection details
- [Authentication and Roles](Authentication-and-Roles) — managing users, roles, and access
