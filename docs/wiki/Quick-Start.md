# Quick Start

## 1. Create your compose file

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

Generate `SESSION_SECRET` with:

```bash
openssl rand -hex 32
```

## 2. Start the container

```bash
docker compose up -d
```

## 3. Complete the setup wizard

Open `http://localhost:7676/wizard`.

The wizard walks through:

1. Plex connection and library selection
2. Local admin creation or Plex admin sign-in
3. Optional Tautulli connection
4. Optional Lidarr connection

## 4. Choose your playback source

Curatorr can ingest live playback from either:

- `Plex` — recommended default
- `Tautulli` — useful if you prefer Tautulli as the live event source

Set this in `Settings -> General -> Playback source`.

Notes:

- Plex webhooks are the most direct live source.
- Tautulli gap-fill does not require the Tautulli webhook.
- If you want live playback from Tautulli, you must both set playback source to `Tautulli` and register the Tautulli webhook.

## 5. Let Curatorr build the library cache

After setup, run or wait for:

- `Master Track Cache Refresh`
- `Smart Playlist Sync`

Large libraries can take a while on first refresh. Curatorr pages tracks through Plex so it can handle very large libraries more safely than the older all-at-once approach.

## 6. Recommended first checks

- `Settings -> Plex`
  - verify token, machine ID, and selected music libraries
  - use `Refresh libraries` if you add a new Plex music library later
- `Settings -> Jobs`
  - confirm the core jobs you want are enabled
- `User Profile`
  - optional: set Last.fm username
  - optional: set ListenBrainz username/token
  - optional: adjust your theme

## What to expect

- Smart playlists appear in Plex after the next sync cycle.
- Track tiers begin to populate as plays come in.
- Suggested artists become useful once there is enough listening history.
- If Lidarr is configured, add/queue actions and progression appear on the Artists page.

## Next steps

- [Configuration](Configuration)
- [Integrations](Integrations)
- [Authentication and Roles](Authentication-and-Roles)
- [Smart Playlists](Smart-Playlists)
