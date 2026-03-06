# Integrations

Launcharr ships with a built-in app catalog from `config/default-apps.json`.

Current catalog status:

- `48` built-in app definitions.
- Built-ins are opt-in (`removed: true`) until enabled from settings.
- App support levels vary: full overview modules, widget stats, or launch/settings only.

## Built-in App Catalog (Current)

| App | ID | Category | Support Level | Default URL |
| --- | --- | --- | --- | --- |
| Agregarr | `agregarr` | `Arr Suite` | Widget stats | `http://localhost:5055` |
| Autobrr | `autobrr` | `Arr Suite` | Overview + Widget stats | `http://localhost:7474` |
| Bazarr | `bazarr` | `Arr Suite` | Overview + Widget stats | `http://localhost:6767` |
| Cleanuparr | `cleanuparr` | `Arr Suite` | Widget stats | `http://localhost:11011` |
| Huntarr | `huntarr` | `Arr Suite` | Launch/settings | `http://localhost:9705` |
| Lidarr | `lidarr` | `Arr Suite` | Overview + Widget stats | `http://localhost:8686` |
| Maintainerr | `maintainerr` | `Arr Suite` | Overview + Widget stats | `http://localhost:6246` |
| Profilarr | `profilarr` | `Arr Suite` | Widget stats | `http://localhost:6868` |
| Radarr | `radarr` | `Arr Suite` | Overview + Widget stats | `http://localhost:7878` |
| Readarr | `readarr` | `Arr Suite` | Overview + Widget stats | `http://localhost:8787` |
| Sonarr | `sonarr` | `Arr Suite` | Overview + Widget stats | `http://localhost:8989` |
| Sortarr | `sortarr` | `Arr Suite` | Widget stats | `http://localhost:8787` |
| Paperless-ngx | `paperless-ngx` | `Documents` | Widget stats | `http://localhost:8010` |
| Deemix | `deemix` | `Downloaders` | Launch/settings | `http://localhost:6595` |
| MeTube | `metube` | `Downloaders` | Overview + Widget stats | `http://localhost:8081` |
| NZBGet | `nzbget` | `Downloaders` | Overview + Widget stats | `http://localhost:6789` |
| SABnzbd | `sabnzbd` | `Downloaders` | Overview + Widget stats | `http://localhost:8085` |
| Transmission | `transmission` | `Downloaders` | Overview + Widget stats | `http://localhost:9091` |
| qBittorrent | `qbittorrent` | `Downloaders` | Overview + Widget stats | `http://localhost:8080` |
| Actual Budget | `actual-budget` | `Finance` | Launch/settings | `http://localhost:5006` |
| Romm | `romm` | `Games` | Overview + Widget stats | `http://localhost:8080` |
| Jackett | `jackett` | `Indexers` | Overview + Widget stats | `http://localhost:9117` |
| Prowlarr | `prowlarr` | `Indexers` | Overview + Widget stats | `http://localhost:9696` |
| Tautulli | `tautulli` | `Manager` | Overview + Widget stats | `http://localhost:8181` |
| Audiobookshelf | `audiobookshelf` | `Media` | Overview + Widget stats | `http://localhost:13378` |
| Emby | `emby` | `Media` | Overview + Widget stats | `http://localhost:8096/web/index.html` |
| ErsatzTV | `ersatztv` | `Media` | Widget stats | `http://localhost:8409` |
| Jellyfin | `jellyfin` | `Media` | Overview + Widget stats | `http://localhost:8096/web/index.html` |
| Kometa | `kometa` | `Media` | Launch/settings | `http://localhost:4242` |
| Pikaraoke | `pikaraoke` | `Media` | Launch/settings | `http://localhost:5555` |
| Plex | `plex` | `Media` | Overview + Widget stats | `http://localhost:32400/web` |
| Tdarr | `tdarr` | `Media` | Overview + Widget stats | `http://localhost:8265` |
| Immich | `immich` | `Photos` | Overview + Widget stats | `http://localhost:2283` |
| Pulsarr | `pulsarr` | `Requesters` | Overview + Widget stats | `http://localhost:3030` |
| Seerr | `seerr` | `Requesters` | Overview + Widget stats | `http://localhost:5055` |
| Glances | `glances` | `System` | Widget stats | `http://localhost:61208` |
| Gluetun | `gluetun` | `System` | Widget stats | `http://localhost:8000` |
| Guardian | `guardian` | `System` | Widget stats | `http://localhost:3005` |
| Portainer | `portainer` | `System` | Widget stats | `http://localhost:9000` |
| Speedtest Tracker | `speedtest-tracker` | `System` | Widget stats | `http://localhost:8765` |
| Uptime Kuma | `uptime-kuma` | `System` | Overview + Widget stats | `http://localhost:3001` |
| Apprise | `apprise` | `Tools` | Widget stats | `http://localhost:8000` |
| Code Server | `code-server` | `Tools` | Launch/settings | `http://localhost:8443` |
| Guacamole | `guacamole` | `Tools` | Overview + Widget stats | `http://localhost:8090` |
| Termix | `termix` | `Tools` | Widget stats | `http://localhost:9090` |
| The Lounge | `thelounge` | `Tools` | Launch/settings | `http://localhost:9001` |
| Wizarr | `wizarr` | `Tools` | Overview + Widget stats | `http://localhost:5690` |
| phpMyAdmin | `phpmyadmin` | `Tools` | Launch/settings | `http://localhost:1977` |

## Setup Pattern

For each enabled app:

1. Open `Settings -> [App]`.
2. Set local/remote URL values.
3. Add credentials or API keys if needed.
4. Save and validate from dashboard, overview, or widget cards.

## Credential Notes by Integration

- Plex: use `Get Plex Token` and `Get Plex Machine` in app settings.
- Immich: requires API key (`x-api-key`) for recent assets and thumbnail proxy.
- Audiobookshelf: requires bearer API key for recent items and cover proxy.
- Tdarr: supports `x-api-key`; stats can work without key depending on server config.
- Wizarr: supports optional `X-API-Key`.
- Uptime Kuma: set `uptimeKumaSlug` for status-page API reads.

## Overview Modules

Overview modules are currently implemented for:

- Media: Plex, Jellyfin, Emby, Audiobookshelf, Tdarr.
- Arr Suite: Radarr, Sonarr, Lidarr, Readarr, Bazarr, Autobrr, Maintainerr.
- Requesters: Pulsarr, Seerr.
- Indexers: Prowlarr, Jackett.
- Downloaders: Transmission, qBittorrent, SABnzbd, NZBGet, MeTube.
- Specialty: Romm, Immich, Wizarr, Uptime Kuma, Guacamole.
- Manager: Tautulli.

## Widget Stats

Widget stat cards are available for a broader set than overview modules, including:

- Core media/arr/downloader stack.
- Immich, Uptime Kuma, MeTube, Audiobookshelf, Tdarr, Wizarr.
- System services like Portainer, Glances, Speedtest Tracker, Gluetun, Paperless-ngx, Guardian.
- Additional tools and Arr utilities: Apprise, Termix, ErsatzTV, Sortarr, Profilarr, Agregarr.

## Custom Apps

Custom apps support:

- Name, URL, category, and icon upload.
- Launch mode (`iframe`, `new-tab`, `disabled`).
- Role-aware overview/launch/settings/sidebar visibility.

Customizations are persisted in `config/config.json`.
