# Curatorr Wiki

Curatorr is a self-hosted Plex and Plexamp companion for playback tracking, smart playlists, artist discovery, and optional Lidarr automation.

Use this wiki as the operational source of truth for setup, playback sources, integrations, and day-to-day administration.

## Preview

### Dashboard

![Curatorr dashboard overview](../media/curatorr-dashboard.png)

### Discover

![Curatorr discover page](../media/curatorr-discover.png)

### Playlists

![Curatorr playlists page](../media/curatorr-playlists.png)

### Login

![Curatorr login page](../media/curatorr-login.png)

## Start Here

1. [Quick Start](Quick-Start)
2. [Configuration](Configuration)
3. [Authentication and Roles](Authentication-and-Roles)
4. [Integrations](Integrations)
5. [Artist Suggestions and Lidarr Activity](Artist-Suggestions-and-Lidarr-Activity)
6. [Discover](Discover)
7. [Smart Playlists](Smart-Playlists)
8. [Troubleshooting](Troubleshooting)
9. [FAQ](FAQ)

## What Curatorr Solves

- Tracks plays from Plex webhooks, with optional Tautulli repair and backfill.
- Builds per-user smart playlists directly in Plex using real listening behavior.
- Scores artists in your own library to surface under-explored suggestions.
- Supports personal playlists, blended playlists, Daily Mix, and external discovery.
- Optionally connects to Lidarr to queue artists, pick starter albums, and progressively expand catalogs.
- Supports Last.fm history sync and station playlists, plus ListenBrainz playlist suggestions.

## Key Product Capabilities

- Plex-first playback tracking with optional Tautulli live source or gap-fill support.
- Per-user track tiers: `Belter`, `Decent`, `Half Decent`, `Skip`, and `Curatorr`.
- Local admin account plus Plex SSO and Plex Home profile support.
- Admin preview mode so the local admin can inspect the app as another Plex user.
- Per-user Last.fm and ListenBrainz integrations from User Profile.
- Theme customization for both global defaults and per-user appearance.

## Operational Endpoints

- `GET /healthz`
- `GET /api/version`
- `GET /api/logs` (authenticated)
