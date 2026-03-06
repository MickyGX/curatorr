# Supported Apps

Source of truth:

- App inventory: `config/default-apps.json`
- Overview modules: `APP_OVERVIEW_ELEMENTS` in `src/index.js`
- Widget stat types: `WIDGET_STAT_TYPES` in `src/index.js`

Current built-in catalog totals:

- `48` app definitions.
- `27` with overview modules plus widget stats.
- `13` with widget stats only.
- `8` launch/settings only.

## Overview Module Matrix

| App | ID | Overview Modules |
| --- | --- | --- |
| Plex | `plex` | Active Streams, Recently Added, Most Watchlisted This Week |
| Jellyfin | `jellyfin` | Active Streams, Recently Added |
| Emby | `emby` | Active Streams, Recently Added |
| Tautulli | `tautulli` | Watch Statistics |
| Radarr | `radarr` | Downloading Soon, Recently Downloaded, Activity Queue, Calendar |
| Sonarr | `sonarr` | Downloading Soon, Recently Downloaded, Activity Queue, Calendar |
| Lidarr | `lidarr` | Downloading Soon, Recently Downloaded, Activity Queue, Calendar |
| Readarr | `readarr` | Downloading Soon, Recently Downloaded, Activity Queue, Calendar |
| Bazarr | `bazarr` | Subtitle Queue |
| Prowlarr | `prowlarr` | Indexer Search |
| Jackett | `jackett` | Indexer Search |
| Pulsarr | `pulsarr` | Recent Requests, Most Watchlisted |
| Seerr | `seerr` | Recent Requests, Most Watchlisted |
| Autobrr | `autobrr` | Releases, Delivery Queue |
| Maintainerr | `maintainerr` | Library Media, Rules, Collections Media |
| Transmission | `transmission` | Download Queue |
| qBittorrent | `qbittorrent` | Download Queue |
| SABnzbd | `sabnzbd` | Download Queue |
| NZBGet | `nzbget` | Download Queue |
| Romm | `romm` | Recently Added, Consoles |
| Immich | `immich` | Recently Added |
| MeTube | `metube` | Download Queue |
| Audiobookshelf | `audiobookshelf` | Recently Added |
| Tdarr | `tdarr` | Statistics |
| Guacamole | `guacamole` | Active Sessions, Connections |
| Wizarr | `wizarr` | Users, Invitations |
| Uptime Kuma | `uptime-kuma` | Status Page |

## Full Catalog and Support Levels

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

## Notes

- Built-ins are opt-in and can be enabled/disabled from app settings.
- Custom apps are also supported with custom categories/icons.
- Role visibility for apps/modules is configurable in settings.
