<p align="center">
  <img src="public/icons/curatorr-icon.png" alt="" width="64" height="64" />
</p>
<h3 align="center">CURATORR</h3>
<p align="center"><sub>SMART PLAYLIST CURATOR</sub></p>
<p align="center">
  <a href="https://github.com/MickyGX/curatorr/releases/latest"><img src="https://img.shields.io/github/v/release/MickyGX/curatorr?display_name=release&label=latest%20release" alt="Latest release" /></a>
  <a href="https://discord.gg/TvrxJWD4PK"><img src="https://img.shields.io/badge/Discord-Join%20the%20server-5865F2?logo=discord&logoColor=white" alt="Discord" /></a>
  <a href="https://github.com/MickyGX/curatorr/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-AGPL--3.0-1677c5" alt="License AGPL-3.0" /></a>
  <a href="https://hub.docker.com/r/mickygx/curatorr"><img src="https://img.shields.io/badge/docker-mickygx%2Fcuratorr-1677c5?logo=docker&logoColor=white" alt="Docker Hub" /></a>
</p>
<p align="center">
  <a href="https://ko-fi.com/U7U61X81Z1" target="_blank"><img src="https://storage.ko-fi.com/cdn/kofi6.png?v=6" alt="Support me on Ko-fi" height="24" /></a>
  <a href="https://www.buymeacoffee.com/MickyGX"><img src="https://img.buymeacoffee.com/button-api/?text=Buy%20me%20a%20coffee&emoji=&slug=MickyGX&button_colour=FFDD00&font_colour=000000&font_family=Cookie&outline_colour=000000&coffee_colour=ffffff" alt="Buy Me a Coffee" height="24" /></a>
</p>
<hr />

Curatorr is a self-hosted Plex, Jellyfin, and Emby companion for playback tracking, smart playlist generation, artist discovery, and optional Lidarr automation.

## Preview

![Curatorr dashboard overview](docs/media/curatorr-dashboard.png)

## What It Does

- Tracks playback from Plex, Jellyfin, or Emby
- Builds per-user smart playlists from real listening behavior
- Supports personal playlists, blended playlists, Curatorr rotating playlists, and configurable Daily Mix
- Adds feature-driven playlist building with BPM, key, Camelot, energy, and danceability filters when analysis data is available
- Surfaces library-based suggestions and external discovery
- Integrates with Lidarr for optional add, queue, and progression workflows
- Supports Last.fm history/station features and ListenBrainz playlist suggestions on supported server paths

## Media Server Support

- **Plex**: broadest feature set, including Plex SSO, Plex Home users, native Plex webhooks, optional Tautulli support, Daily Mix, Last.fm station playlists, and ListenBrainz playlist suggestions
- **Jellyfin**: native server login, library indexing, live playback tracking, smart playlists, personal playlists, blended playlists, discovery, and Lidarr automation
- **Emby**: native server login, library indexing, live playback tracking, smart playlists, personal playlists, blended playlists, discovery, and Lidarr automation

Current Plex-only features:

- Tautulli integration
- Daily Mix sync
- Last.fm station playlists
- ListenBrainz playlist suggestion sync
- Sonic ordering and loudness-aware sequencing

## Quick Start

Minimal Docker Compose example:

```yaml
services:
  curatorr:
    image: mickygx/curatorr:latest
    container_name: curatorr
    ports:
      - "7676:7676"
    environment:
      - CONFIG_PATH=/app/config/config.json
      - DATA_DIR=/app/data
      - BASE_URL=http://localhost:7676
      - TRUST_PROXY=true
      - TRUST_PROXY_HOPS=1
      - SESSION_SECRET=replace-this-with-a-random-secret
      - WEBHOOK_SECRET=replace-this-with-a-random-secret
    volumes:
      - ./config:/app/config
      - ./data:/app/data
      - ./data/icons/custom:/app/public/icons/custom
    network_mode: bridge
    restart: unless-stopped
```

Generate `SESSION_SECRET` and `WEBHOOK_SECRET` with:

```bash
openssl rand -hex 32
```

Start Curatorr with:

```bash
docker compose up -d
```

Then open `http://localhost:7676/wizard` and complete the setup wizard for Plex, Jellyfin, or Emby.

## Optional Features

Curatorr supports an optional analyzer sidecar for `BPM`, `musical key`, `Camelot key`, `energy`, and `danceability` enrichment. For setup and workflow details, see [Track Analysis](docs/wiki/Track-Analysis.md).

Spotify support is optional and user-specific. Users can connect Spotify accounts to browse and import playlists after app-level credentials are configured on the Curatorr container. For setup details, see [Spotify in Integrations](docs/wiki/Integrations.md#spotify).

## Documentation

For installation, setup, configuration, and troubleshooting:

- [GitHub Wiki](https://github.com/MickyGX/curatorr/wiki)
- [Wiki Home](docs/wiki/Home.md)
- [Quick Start](docs/wiki/Quick-Start.md)
- [Configuration](docs/wiki/Configuration.md)
- [Authentication and Roles](docs/wiki/Authentication-and-Roles.md)
- [Integrations](docs/wiki/Integrations.md)
- [Track Analysis](docs/wiki/Track-Analysis.md)
- [Artist Suggestions and Lidarr Activity](docs/wiki/Artist-Suggestions-and-Lidarr-Activity.md)
- [Discover](docs/wiki/Discover.md)
- [Smart Playlists](docs/wiki/Smart-Playlists.md)
- [History](docs/wiki/History.md)
- [Tracks](docs/wiki/Tracks.md)
- [Blend](docs/wiki/Blend.md)
- [User Profile](docs/wiki/User-Profile.md)
- [Troubleshooting](docs/wiki/Troubleshooting.md)
- [FAQ](docs/wiki/FAQ.md)

## Support

- [Discord](https://discord.gg/TvrxJWD4PK)
- [GitHub Discussions](https://github.com/MickyGX/curatorr/discussions)
- [GitHub Wiki](https://github.com/MickyGX/curatorr/wiki)
