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
      # Optional: required for Spotify playlist import and refresh.
      # Add the same callback URL in your Spotify app settings:
      # http://localhost:7676/user-settings/spotify/callback
      # - SPOTIFY_CLIENT_ID=your-spotify-client-id
      # - SPOTIFY_CLIENT_SECRET=your-spotify-client-secret
      - SESSION_SECRET=replace-this-with-a-random-secret
      - WEBHOOK_SECRET=replace-this-with-a-random-secret
    volumes:
      - ./config:/app/config
      - ./data:/app/data
    restart: unless-stopped
```

Generate `SESSION_SECRET` and `WEBHOOK_SECRET` with:

```bash
openssl rand -hex 32
```

If you want Spotify playlist import, also create a Spotify developer app and uncomment
`SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET`. The redirect URI must match your Curatorr
base URL, for example `http://localhost:7676/user-settings/spotify/callback`.

To create the Spotify app:

1. Open the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard).
2. Sign in and click `Create app`.
3. Create a `Web API` app.
4. In the app settings, add your Curatorr callback URL, for example `http://localhost:7676/user-settings/spotify/callback`.
5. Copy the `Client ID` into `SPOTIFY_CLIENT_ID`.
6. Use `View client secret` and copy that value into `SPOTIFY_CLIENT_SECRET`.

## 2. Start the container

```bash
docker compose up -d
```

## 3. Complete the setup wizard

Open `http://localhost:7676/wizard`.

The wizard walks through:

1. Media server selection (`Plex`, `Jellyfin`, or `Emby`)
2. Server connection and library selection
3. Local admin creation or media-server sign-in
4. Optional Tautulli connection on Plex installs
5. Optional Lidarr connection

## 4. Choose your playback source

Curatorr can ingest live playback from either:

- `Plex` — recommended default
- `Tautulli` — useful if you prefer Tautulli as the live event source

Set this in `Settings -> General -> Playback source`.

This setting only applies to Plex installs. Jellyfin and Emby use their own native live playback path.

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

- `Settings -> Plex / Jellyfin / Emby`
  - verify the configured server URL and selected music libraries
  - on Plex, verify the token and machine ID
  - on Plex, use `Refresh libraries` if you add a new Plex music library later
- `Settings -> Jobs`
  - confirm the core jobs you want are enabled
- `User Profile`
  - optional: set Last.fm username
  - optional: set ListenBrainz username/token
  - optional: adjust your theme

## What to expect

- Smart playlists appear in your connected media server after the next sync cycle.
- Track tiers begin to populate as plays come in.
- Suggested artists become useful once there is enough listening history.
- If Lidarr is configured, add/queue actions and progression appear on the Artists page.

## Next steps

- [Configuration](Configuration.md)
- [Integrations](Integrations.md)
- [Authentication and Roles](Authentication-and-Roles.md)
- [Smart Playlists](Smart-Playlists.md)
- [History](History.md)
- [Tracks](Tracks.md)
- [User Profile](User-Profile.md)
