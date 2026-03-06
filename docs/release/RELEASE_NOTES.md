# Release Notes (v0.1.0 -> v0.2.45)

## v0.2.45 (2026-03-05)

- [Added]
- Added mobile launch-flow documentation media (`docs/media/launch-flow-mobile.gif`) and linked it from docs/wiki pages.
- [Changed]
- Updated overview carousel swipe handling so pointer capture is only used for mouse drags.
- Updated horizontal widget-row gesture behavior to `touch-action: pan-x pan-y` so vertical panning remains available.
- Updated mobile-width layout behavior for desktop touch devices to use `.dash-main` as the vertical scroll surface.
- [Fixed]
- Fixed Immich recently-added swipe reliability by adding touch fallback handlers and horizontal-intent gesture detection.
- Fixed vertical page scrolling being blocked in mobile-width view on desktop touchscreen devices while preserving horizontal carousel swipe behavior.

## v0.2.44 (2026-03-02)

- [Added]
- Added a new Launcharr deployment-summary system widget with online/offline/total counters and status-percentage badge.
- Added deployment-summary widget configuration controls for metric columns and per-stat visibility.
- Added `GET /api/widget-deployment-summary` for deployment-summary widget data.
- Added built-in `QNAP` app support (`config/default-apps.json`, `public/icons/qnap.svg`) with widget metrics for CPU, memory, and volume.
- Added API-key reveal/hide controls and wider API-key field layout in app settings forms.
- [Changed]
- Updated settings widget-canvas behavior so deployment-summary config opens on card click (no separate edit pencil).
- Updated app-settings navigation panel generation to include all non-removed apps directly.
- Enforced default sidebar visibility minimum of `admin` for app sidebar menu sections.
- [Fixed]
- Fixed Immich recently-added loading and thumbnail rendering by supporting multiple response shapes and thumbnail URL formats.
- Fixed qBittorrent widget false-offline behavior by adding WebUI login/session-cookie flow for authenticated instances.
- Fixed widget-stats access for widget-only apps (for example, QNAP) when app visibility is granted through widget bars.
- Fixed deployment-summary app counting to include stat types that use fallback metadata.
- Fixed custom icon uploads not appearing in settings by correcting upload/delete filesystem paths and CSRF form coverage.
- Added migration for previously misplaced custom icon files plus clearer icon-upload error diagnostics.
- [Security]
- Added rate limiting for failed setup submissions (`POST /setup`).
- Added rate limiting for Plex PIN status polling (`GET /api/plex/pin/status`).
- Tightened Plex PIN handling so callback/status checks use session-bound PIN values only.

## v0.2.43 (2026-03-01)

- [Added]
- Expanded built-in app catalog/category/icon coverage and aligned default categories/icons (including requesters.svg).
- Added multi-instance defaults metadata for built-in apps (supportsInstances, maxInstances, instanceNamePlaceholder).
- Added specialty overview/data modules for MeTube, Audiobookshelf, Tdarr, Immich, Wizarr, Uptime Kuma, Guacamole, and Traefik.
- Added app-level behavior controls under Settings -> Apps -> General for sidebar link visibility and per-role short/long press actions.
- [Changed]
- Moved Add default app and Add custom app rows to the top of Settings -> Apps -> General.
- Updated app settings/tab workflows and support docs to match expanded integrations and app-general behavior placement.
- [Fixed]
- Fixed custom app add regression in 0.2.42 (crypto.randomBytes is not a function, GitHub issue #22).
- Fixed default-app add/form-layout issues across settings surfaces.
- Fixed user-menu logout label typography inconsistencies.
- [Security]
- Added conditional/hop-limited trust proxy configuration, baseline headers, CSRF protections, stricter payload limits, and stronger local password rules.
- Documented recommended reverse-proxy env defaults in the example compose (`TRUST_PROXY=true`, `TRUST_PROXY_HOPS=1`, optional `COOKIE_SECURE=true`).


## v0.2.42 (2026-02-27)

- [Added]
- Added a major System Info widget expansion (`sys-resources`) with configurable CPU, RAM, multi-disk, search, and weather elements.
- Added `Show Total Space (RAM + Disks)` support for System Info cards so widgets can switch between free and total-space displays.
- Added widget status notifications via Apprise with explicit `Enable widget status alerts` and delay-seconds controls.
- Added a background widget-state monitor that polls widget apps and sends online/offline notifications after sustained state transitions.
- Added widget monitor debugging surfaces:
  - `GET /api/widget-status-monitor`
  - Live monitor summary line in `Settings -> Log`.
- Added widget-focused runtime/icon assets (`public/widgets-stat-cards.js`, CPU/RAM/disk/weather icon set).
- [Changed]
- Updated widget row editing UX with compact up/down row insertion strips and reduced row action footprint.
- Updated widget add-menu/popup layering so floating menus and config panels render above surrounding settings/topbar UI.
- Updated System Info widget theming/layout to better match dashboard styling, including improved mobile wrapping and search/weather placement behavior.
- Updated widget configuration behavior to allow multiple System Info widgets in the same widget bar.
- [Fixed]
- Fixed widget status transition tracking so transient `unknown` probe results do not reset pending notification timers.
- Fixed System Info disk output consistency for multi-disk path configurations.
- Improved settings-preview/dashboard-runtime parity for System Info widget rendering.

## v0.2.41 (2026-02-25)

- [Added]
- Added local login rate limiting for `POST /login` with temporary lockout (`429`) and retry messaging after repeated failed attempts.
- Added auth and route-guard automated tests (`npm test`) using `node:test` + `supertest`.
- Added modular route registration files under `src/routes/*` and shared EJS partials to reduce duplication.
- Added split page-specific CSS bundles (`styles-auth`, `styles-home`, `styles-layout`, `styles-overview`, `styles-settings`) alongside shared `styles.css`.
- [Changed]
- Refactored `src/index.js` route wiring to register route groups via `register*()` modules while preserving existing endpoints.
- Updated package scripts/dev tooling to include `npm test` and `supertest` for HTTP route coverage.
- [Fixed]
- Fixed Romm iframe viewer launch cookie priming for embedded Romm launches.
- Hardened log persistence writes with temp-file + rename behavior to reduce partial writes on interruption.
- [Security]
- Removed the insecure fallback session secret (`change-me`); Launcharr now generates a random secret and logs a warning when `SESSION_SECRET` is unset.

## v0.2.40 (2026-02-24)

- [Added]
- Added multi-dashboard support with dashboard tabs/definitions, per-dashboard name/icon/visibility, and admin add/duplicate/delete controls in `Settings -> Custom -> Dashboard`.
- Added per-dashboard saved state snapshots so dashboard card layouts/settings can differ by dashboard instead of sharing one global layout.
- Added admin role-view switching (`guest` / `user` / `co-admin` / `admin`) via `/switch-view`, including role picker UI and dashboard-availability indicators in the user menu.
- [Changed]
- Updated dashboard/app/settings navigation and routing to preserve a selected dashboard via `?dashboard=` and fall back to a visible dashboard when the requested one is not allowed for the current role.
- Updated dashboard settings saves to persist dashboard metadata (name/icon/visibility) plus dashboard-specific card/combined/widget state.
- [Fixed]
- Fixed version badge admin-view detection so status/version UI works correctly with the new role-view picker links.
- Reduced stale asset mismatches after upgrades by cache-busting rendered CSS/JS asset URLs (`styles.css`, `version-badge.js`, `pwa.js`) across updated views.

## v0.2.39 (2026-02-22)

- [Added]
- Added Romm `Recently Added Probe Limit` setting (50-200) in Romm settings (both settings pages via shared form).
- [Changed]
- Moved theme toggle into the Launcharr brand menu and replaced the top-right theme button with a Buy Me a Coffee heart link.
- Reduced default Romm recently-added API probe size from `200` to `50` (configurable) and updated recent ordering to prioritize parsed timestamps.
- [Fixed]
- Improved Romm recently-added ordering consistency between backend normalization and the Romm overview UI.

## v0.2.38 (2026-02-22)

- [Added]
- [Changed]
- Reverted Romm admin auto-launch credential injection/bootstrap so admin launches follow the normal launch path again (avoids unexpected fallback to new-window when admin Romm bootstrap fails).

## v0.2.37 (2026-02-22)

- [Added]
- [Changed]
- [Fixed]


## v0.2.36 (2026-02-22)

- Added a `Hide scrollbars` theme option for cleaner UI while preserving scroll behavior.
- Added persistence for `Hide scrollbars` in admin theme defaults and per-user theme preferences.
- Updated the About page Discord support link to the current invite URL.

## v0.2.35 (2026-02-21)

- Added default Maintainerr integration (`Arr Suite`) with Launcharr-native Dashboard/App Overview modules: `Library Media`, `Rules`, and `Collections Media`.
- Added Maintainerr API integration routes for media/rules/collections, rule execute action, and TMDB poster/backdrop proxying.
- Updated Maintainerr cards to Launcharr style, including fallback-art handling and rules-card metadata updates.
- Fixed touch fallback for sidebar short/long-press actions when Pointer Events are unavailable.
- Fixed stale asset behavior by versioning `styles.css`, `version-badge.js`, and `pwa.js` across app views plus runtime cache-busted `assetVersion`.
- Fixed local-network iframe host mismatch/login-loop scenario (including Romm) by normalizing iframe target host to the active Launcharr LAN host.

## v0.2.34 (2026-02-21)

- Fixed Romm `Recently Added` ordering mismatch between Dashboard and App Overview cards.
- Updated Romm `Recently Added` rendering to sort newest-first explicitly instead of relying on upstream response order.
- Improved consistency for recent Romm content visibility across Launcharr surfaces.


## v0.2.33 (2026-02-21)

- Added `Settings -> About` with support links, build/version context, and release history with changelog modal + full release links.
- Added role-based sidebar app-button `Short Press` / `Long Press` actions (`Default`, `Launch`, `App settings`, `Activity`) with compact filter-popover controls in `Settings -> General`.
- Fixed sidebar press-action routing in dashboard and app/info pages so launch-mode behavior is consistent, including `new-tab` handling.
- Fixed hidden-sidebar-link interaction so configured `App settings` and `Activity` press actions still open even when those submenu links are hidden.
- Updated settings tab labels/icons (`Logs`, `About`) and improved release modal centering/z-index behavior.

## v0.2.32 (2026-02-20)

- Fixed dashboard manager drag-order persistence for `Tautulli -> Watch Statistics` cards.
- Updated dashboard settings row-order assignment so dashboard row drag/save targets dashboard order fields only (avoiding nested watch-card order collisions).

## v0.2.31 (2026-02-20)

- Added first-install `Quick Start Guide` popup with guided setup steps and direct sidebar-settings navigation.
- Added release `Welcome` popup that surfaces highlights from `Added/Changed/Fixed` sections for the installed version.
- Added unified custom cards for ARR and Downloaders to match media unified-card workflows.
- Added unified-card source selection so a single unified card can target one app or multiple apps.
- Added default `Requesters` category with `Pulsarr` and `Seerr` assigned to Requesters.
- Updated fresh-install defaults so built-in apps start removed until explicitly restored in `Settings -> Custom -> Sidebar`.
- Unified Tautulli watch statistics into one card with selectable `list`/`wheel` mode.
- Fixed deprecated-card reintroduction during app add/restore flows and improved dashboard ordering consistency.

## v0.2.30 (2026-02-19)

- Added default `Indexers` category with a dedicated category icon and default ordering support.
- Updated default app catalog so `Prowlarr` and `Jackett` now default to the `Indexers` category.
- Updated settings dashboard customization so removed sidebar apps no longer continue to generate app-specific dashboard cards.
- Preserved combined dashboard cards unless no active source apps remain for that combined module.
- Fixed app-instance deletion flow so non-primary instances remain deletable even when the primary instance is absent.
- Fixed settings instance-tab UI to show a delete action for first visible non-primary instances (for example `qbittorrent-2`).

## v0.2.29 (2026-02-19)

- Added Romm built-in integration modules for `Recently Added` and `Consoles` with Plex-style carousel cards and detail popups.
- Added console sorting/limit controls (`Most ROMs`, `A to Z`, `Z to A`, `Least ROMs`, `All`) and expanded backend support for full console fetch limits.
- Improved Romm console asset and metadata mapping (artwork fallback, ROM/stat parsing, and popup summaries when overview text is missing).
- Added theme persistence controls for both admin defaults (`Settings -> Custom -> Themes`) and per-user profile preferences.
- Removed Romm API-key field from app settings UI for default Romm setup flow.

## v0.2.28 (2026-02-19)

- Restored Seerr API key field visibility in both `Settings -> App` and per-app settings pages.
- Updated API-key-required app detection to use explicit base app IDs so field visibility does not regress with category metadata changes.
- Fixed settings token-status evaluation for Seerr/Pulsarr and related API-key integrations when category labels are customized.

## v0.2.27 (2026-02-19)

- Updated Seerr/Pulsarr request API URL candidate resolution to prefer configured `remoteUrl`/`localUrl` values.
- Fixed request fallback behavior to avoid loopback-only candidates (`localhost`/`127.0.0.1`/`::1`) when explicit non-loopback URLs are configured.
- Improved Seerr error diagnostics to include all attempted upstream base URLs when requests fail.

## v0.2.26 (2026-02-19)

- Added separate `Overview` visibility control in `Settings -> Custom -> Dashboard` visibility popovers.
- Updated dashboard visibility popover ordering so `Overview` appears above `Dashboard`.
- Updated element visibility persistence to keep `overviewVisibilityRole` and `dashboardVisibilityRole` independent.
- Updated app overview rendering to enforce the dedicated overview visibility role by user role.

## v0.2.25 (2026-02-19)

- Added local account avatar upload in user profile with validation and preview.
- Added owner-aware user avatar fallback behavior (`role.svg` for setup owner admin, `user-profile.svg` for other local/system users).
- Added local Launcharr user delete actions in `Settings -> User` with confirmation plus owner/current-session protections.
- Updated profile role display to a top-right header pill in user profile.
- Improved mobile handling for local users table controls to reduce clipping/overflow regressions.

## v0.2.24 (2026-02-19)

- Added multi-instance support for Transmission end-to-end (settings forms, dashboard queue modules, and app overview queue modules).
- Added metadata-driven instance capabilities in default app definitions (`supportsInstances`, `maxInstances`, `instanceNamePlaceholder`) for Radarr, Sonarr, Bazarr, Transmission, and qBittorrent.
- Updated multi-instance runtime detection to use app metadata with legacy fallback compatibility.
- Updated downloader queue server/client handling to resolve instance IDs (for example `transmission-2`) to base downloader behavior.
- Fixed settings regressions where username/password fields could be missing for additional downloader instances.
- Fixed overview runtime error (`{"error":"Server error"}`) caused by undefined downloader base resolution for instance IDs.

## v0.2.23 (2026-02-19)

- Restored `Base URL Path` in `Settings -> General` with persisted `general.basePath` handling.
- Updated Launcharr public/callback URL resolution to apply configured base path (including Plex auth forward URLs).
- Updated integration/proxy URL joins to preserve app path prefixes for subpath deployments (for example `https://domain.com/app`).
- Updated quick-menu starfield/maximize UX (stateful labels, disabled starfield toggle in mobile/maximized, automatic starfield state restore).

## v0.2.22 (2026-02-18)

- Added dynamic Plex library section-key discovery for Recently Added, replacing hardcoded section IDs.
- Added ARR activity-queue per-page options (`5`, `10`, `25`, `50`) in app overview and dashboard.
- Updated indexer filter popover rendering/layering to prevent clipping over table regions.
- Fixed Plex Recently Added behavior on servers with non-default section keys.

## v0.2.21 (2026-02-18)

- Added shared queue-table pagination support and table-card overrides across queue modules.
- Added dedicated indexer table sorting and richer indexer search interactions.
- Expanded dashboard/app-overview module wiring for queue/indexer cards.
- Updated default-app bootstrap/startup handling for stronger first-run reliability.
- Fixed queue/indexer card behavior inconsistencies between dashboard and app overview.

## v0.2.20 (2026-02-18)

- Built-in app Settings access normalized to admin-only to align menu visibility with route permissions.
- Sidebar menu access checks now require both sidebar visibility and underlying section access.
- Fixed removed-app recovery so re-added built-in apps restore submenu role settings correctly.

## v0.2.19 (2026-02-18)

- Removed built-in apps now retain backup menu/overview/launch/favorite state for restoration.
- Re-adding a built-in app restores preserved configuration instead of forcing defaults.
- Fixed legacy removed-app recovery paths for older backup data.

## v0.2.18 (2026-02-18)

- Updated built-in app remove/re-add validation to use built-in eligibility rather than catalog membership.
- Fixed Settings fallback recovery so removed built-in apps missing from the catalog can be re-added.

## v0.2.17 (2026-02-18)

- Release bookkeeping update for `v0.2.17`.

## v0.2.16 (2026-02-18)

- Release bookkeeping update for `v0.2.16`.


## v0.2.14 (2026-02-18)

- Added queue-table pagination controls across ARR queue cards and downloader queue cards (including combined cards).
- Added sortable header direction indicators (`up/down`) across queue-based table cards.
- Added `Visible items in queue` configuration to all table-card settings dropdowns.
- Added `Jackett` and `Autobrr` as major default integrations, including dashboard/app-overview card support and icon coverage.
- Updated table-card styling to use consistent header/body treatment across ARR + downloader + combined queue modules.
- Updated Prowlarr/Jackett indexer search UX with numeric pagination controls and removed top-bar prev/next arrow paging.
- Improved Prowlarr filter behavior to support indexer/category selection workflows and persistent multi-select interaction.
- Fixed recurring mobile layout regressions in queue/table modules, app settings instance controls, and card formatting.
- Fixed app settings visibility regression where app submenu settings entries could disappear after remove/re-add flows.
- Fixed runtime startup regression from malformed `src/index.js` expression handling.

## v0.2.13 (2026-02-17)

- Release bookkeeping update for `v0.2.13`.


## v0.2.12 (2026-02-17)

- Added `Base URL Path` runtime handling across request routing, redirects, and rendered HTML output.
- Added base-path-aware client patching for fetch/XHR and URL-bearing DOM attributes to improve reverse-proxy subpath compatibility.
- Updated PWA path handling (`manifest`, `pwa.js`, `sw.js`) to work correctly when Launcharr is served from a subpath.
- Added app-overview filter popover behavior to align individual app cards with dashboard filter UX.
- Added `/api/jellyfin/image` proxy endpoint and stronger Jellyfin artwork fallback mapping for recent/active payloads.
- Fixed Jellyfin Recently Added card/modal artwork fallback (poster and background).
- Reduced Plex watchlisted card background hydration requests to improve responsiveness.
- Updated admin-view detection logic in version badge handling for switch-view links.

## v0.2.11 (2026-02-17)

- Added `v0.2.11` release artifact files:
  - `docs/release/releases/v0.2.11.md`
  - `docs/release/releases/v0.2.11-announcement.md`
- Updated package metadata/version release bookkeeping for `0.2.11`.
- Updated release timeline metadata from `v0.1.0 -> v0.2.10` to `v0.1.0 -> v0.2.11`.
- Corrected release documentation attribution so base URL/subpath support changes are tracked under `v0.2.10`, not `v0.2.9`.
- Finalized `v0.2.11` release-note population for changelog + announcement artifacts.


## v0.2.10 (2026-02-17)

- Added dedicated Launcharr `Base URL Path` setting in `Settings -> General`.
- Added config-level `general.basePath` handling with path normalization.
- Updated Launcharr callback/public URL generation to apply configured base path.
- Updated app integration URL joins to preserve configured path prefixes (for example `/radarr`, `/prowlarr`).
- Fixed path-stripping behavior that could break integrations when services were hosted under subpaths.
- Moved base URL feature documentation from `v0.2.9` to `v0.2.10` to reflect actual release ownership.


## v0.2.9 (2026-02-17)

- Added `v0.2.9` release artifact files and timeline entry.
- Updated package metadata/version release bookkeeping for `0.2.9`.
- Clarified that base URL/subpath support changes belong to `v0.2.10`.


## v0.2.8 (2026-02-17)

- Added role-based visibility filters in `Settings -> Custom -> Sidebar` so each app can set access for:
  - Sidebar
  - Overview
  - Launch
  - App settings
  - Activity
- Added role-based visibility filters in `Settings -> Custom -> Dashboard` for per-card access control.
- Added category visibility roles in `Settings -> Custom -> Categories`, plus default-category remove/re-add flow with icon dropdown selection.
- Added a sidebar header quick menu with starfield toggle and maximize/minimize-in-browser controls.
- Changed manager/filter UX to use consistent filter-button popovers across cards and settings tables.
- Changed maximize behavior to remain in browser viewport and persist cleanly during navigation.
- Fixed settings-page script regression that blocked manager actions after category updates.
- Fixed dashboard manager filter dropdown click-through where selecting an option could trigger underlying controls.
- Fixed combined downloader re-add labeling (`Combined Download Queue`) and missing `NZBGet` icon rendering.


## v0.2.7 (2026-02-16)

- Added `qBittorrent` and `SABnzbd` as default apps (with uploaded icons) in sidebar/default app management.
- Added downloader queue support for `qBittorrent` and `SABnzbd`, including combined download queue card compatibility.
- Added `qBittorrent` and `SABnzbd` to downloader combined-source selection and queue-card rendering across dashboard/app overview.
- Changed Combined Download Queue source settings UI to use the same source-pill formatting as other combined cards.
- Fixed Prowlarr API key field visibility in both settings locations (`Settings -> Apps` and per-app settings page).

## v0.2.6 (2026-02-16)

- Added Jellyfin and Emby dashboard/app-overview parity for media cards (active streams, recently added, and combined media support).
- Added multi-instance support for Radarr, Sonarr, and Bazarr with instance tabs and instance-specific naming/selection.
- Added dashboard manager improvements for add/remove card workflows, including combined card management and clearer source selection.
- Added default app add/remove controls in sidebar manager, with re-add flow for removed default apps.
- Improved downloader/media icon consistency across sidebar, dashboard manager, and combined card selectors.
- Fixed empty-state behavior for active stream modules to align card output across Plex/Jellyfin/Emby and combined views.


This document summarizes project releases from the initial `v0.1.0` baseline to current `v0.2.32`.

## v0.2.5 (2026-02-15)

- Added 3D starfield background support across dashboard, overview, startup, login, and setup pages.
- Set starfield defaults (density `165`, speed `45`, size `1.2`) and removed star sliders from settings.
- Kept animated space background + free carousel toggles and simplified theme option logic.
- Fixed horizontal page drift affecting dashboard/overview top bar alignment.
- Bumped package version to `0.2.5`.

## v0.2.4 (2026-02-15)

- Added custom theme toggles (sidebar invert, square corners, animated background, free carousel scroll).
- Added ARR Calendar as a configurable overview section, including combined ARR calendar support.
- Expanded default app/overview config (including Bazarr defaults).
- Improved free-scrolling carousel behavior across ARR/Plex/Pulsarr/Tautulli overviews.
- Improved mobile UX for sidebar/nav behavior, launch-page frame sizing/spacing, and calendar continuity.
- Hardened ARR proxy/fallback paths with timeout handling and structured client/server error logging.
- Updated service worker cache strategy to reduce stale assets.

## v0.2.3 (2026-02-13)

- Fixed Pulsarr regression and restored dedicated API flow stability.
- Added/fixed Seerr API integration (stats + TMDB details).
- Improved request-app fallback behavior and diagnostics.
- Included latest UI/settings refinements from prior v0.2.x work.

Note: this release is published as `v0.2.3` (version naming has been normalized in later release workflow/docs).

## v0.2.2 (2026-02-13)

- Broad frontend and UX pass across overview/activity/settings pages.
- Updated ARR/download queue and app overview client scripts.
- Updated PWA/service-worker assets and styles for consistency/performance.

## v0.2.1 (2026-02-13)

- Introduced theme system with brand presets and custom color wheel/contrast handling.
- Applied theme variables across major UI surfaces for consistent light/dark and brand rendering.
- Refreshed settings UI (tab/subtab icons, active states, readability/accessibility polish).
- Added inline per-app settings panels in `Settings -> Apps` with icon+name switcher.
- Updated app settings save flow to return to `/settings?tab=app&app=<id>`.
- Delivered major mobile UX fixes across settings grids/forms/tables and controls.
- Updated version badge behavior (hidden in user view, compact labels on mobile).

## v0.2.0 (2026-02-13)

- UI tidying pass.
- Added light/dark mode support.

## v0.1.9 (2026-02-11)

- Major settings UI overhaul to improve intuitiveness.
- Adjusted settings tab styling and layering.

## v0.1.8 (2026-02-10)

- Follow-up update to guest access control and user settings placement.

## v0.1.7 (2026-02-10)

- Added guest access control with setting placement in user settings tab.
- Included hardening for non-owner Plex token overwrite path.

## v0.1.6 (2026-02-10)

- Fixed non-owner Plex token overwrite.
- Included Plex watchlist flag + Plex SSO reliability updates from in-between commits.
- Updated README quick start/compose guidance.

## v0.1.5 (2026-02-09)

- Fixed Plex SSO flow.
- Cleaned debug logs.

## v0.1.4 (2026-02-09)

- Added watchlist flag to Plex “Most Watchlisted This Week” overview cards.
- Fixed Plex SSO login behavior.

## v0.1.3 (2026-02-09)

- Added version badge.
- Added Docker build argument support.

## v0.1.2 (2026-02-09)

- Fixed duplicate favourites in sidebar.

## v0.1.1 (2026-02-09)

- Fixed Plex token retrieval for server access.

## v0.1.0 (2026-02-08)

- Initial clean import / baseline project state.
- Initial public-release scaffolding (dashboard app, Docker/Compose setup, Plex SSO + Arr hub foundation).
