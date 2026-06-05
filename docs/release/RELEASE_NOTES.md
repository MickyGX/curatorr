# Release Notes

## v0.1.86 (2026-06-05)

- Fixed smart playlist wizard Step 5 so the summary name now updates live as you type instead of requiring a back/forward step to refresh.
- Fixed custom artwork state in the smart playlist wizard — Step 5 now shows a thumbnail of the current stored image (edit mode) or the pending upload (after back-navigation), making it clear the artwork will be applied on save.
- Improved Spotify integration docs with explicit Spotify Premium requirements for private/owned playlist imports and Developer App ownership restrictions.
- Added a "Source" link in the imported playlist edit modal so Spotify and YouTube imported playlists now show a direct link back to the original source.
- Moved `starfield-3d.js` to the shared `<head>` partial so the background effect loads consistently on all pages.
- Refactored per-page panel and header styles to use scoped `dash-content--<page>` CSS classes across Artists, Blend, Playlists, Tracks, History, Discover, Admin Users, and Settings pages, removing the dependency on body-level page classes.
- History table column header is no longer sticky — it now scrolls with the table content.
- Improved Listening Report profile hero layout with better sizing, padding, and mobile responsive behaviour at narrow viewports.
- Added error logging for playlist rebuild failures and Plex add-items HTTP errors to aid diagnostics.

## v0.1.85 (2026-06-03)

- Fixed Lidarr automation so albums that can never be obtained (no indexer release, or a grab that never imports) are now marked unobtainable and skipped during catalog expansion instead of triggering a daily re-search indefinitely.
- Fixed playlist sync failures caused by a stale Plex machineId — Curatorr now re-fetches the identifier on HTTP 400 and retries automatically, saving the refreshed value for future syncs.
- Tightened the setup-admin music cleanup so the startup data purge is now opt-in rather than always running, preventing accidental data loss when a media-server user ID matches the setup-admin identity.
- Improved getMasterTracks performance with an in-process WeakMap cache that avoids repeated full table scans during smart playlist builds.

## v0.1.84 (2026-05-16)

- Fixed Spotify OAuth callback handling so a successful Spotify authorization can still complete when the browser loses the Curatorr session during the redirect round trip.
- Improved large-library handling by streaming Plex, Jellyfin, and Emby master track refreshes into the database in batches instead of holding the full library in memory at once.
- Reduced expensive full-library reads on Settings and Playlists by moving playlist feature coverage checks into SQL and capping path segment discovery for very large libraries.

## v0.1.83 (2026-04-30)

- Fixed Plex playlist artwork reapplication so Curatorr now uploads stored playlist images directly to Plex instead of relying on Plex to fetch a Curatorr-hosted artwork URL later.
- This hardens imported and generated playlist behavior when Curatorr is temporarily offline, including the reported case where Plex could repeatedly destabilize when opening Spotify-imported playlists after a reboot.
- Added regression coverage for both the new direct artwork upload path and the compatibility fallback to the older URL-based Plex artwork update flow.

## v0.1.82 (2026-04-30)

- Fixed the setup/local admin account model so the recovery account can no longer enter the personal music wizard, build personal playlists, or participate in Lidarr automation as if it were a real listener profile.
- Added startup cleanup for stale setup-admin music data, automatically purging old listening stats, discovery rows, preferences, and other user-scoped records that had already been written to the fallback admin identity in existing installs.
- Tightened scheduler and page scoping behavior so background sync jobs, recommendation rebuilds, and personal overview/discovery state now consistently ignore the setup admin unless an administrator is explicitly previewing another user.

## v0.1.81 (2026-04-19)

- Overview and Report now replace Dashboard as the primary analytics destinations, with dedicated sidebar entries, Overview as the default landing page, the admin "Viewing" strip on both pages, and the old dashboard hidden from normal navigation.
- Refined the analytics UI substantially: Overview now adds period pickers, clickable artist/album/track cards, top playlists, and cleaner heatmap treatment, while Listening Report adds a featured top-track hero, a 52-week activity heatmap, improved chart layouts, and more consistent time-zone-aware date handling.
- Improved workflow reliability across the app: Plex Home user selection now keeps pending state server-side with better avatar handling and protected-user PIN routing, smart playlists gain last-played filters, Lidarr automation can choose default quality/metadata profiles plus monitoring mode for new artists, and Spotify owned-playlist imports handle larger paginated playlists more reliably while URL imports explicitly stay on the public-source path.

## v0.1.80 (2026-04-18)

- Added a new Listening Report experience with period-led analytics, weekly activity comparison, top artists/albums/tracks, tag and decade charts, listening clock, tier breakdown, and quick facts, while moving slower-burn stats to the Overview page.
- Added album-level metadata support across the master track cache and smart-playlist pipeline, so album genres, album styles, and album moods can be filtered independently from track-level genres and moods.
- Reworked smart-playlist content filters and imported-playlist prefills to separate media-server metadata, Curatorr-derived values, and Last.fm tags more clearly, and fixed a report rendering bug that could trigger a `/report` error.

## v0.1.79 (2026-04-18)

- Imported playlists are now much easier to manage from the Playlists page: you can rename them, change their auto-refresh period, and admins can promote supported imported playlists into global shared playlists.
- Plex playlist artwork is now first-class for smart playlists, with auto-generated, preserve-existing, and custom-upload modes that Curatorr can keep in sync after rebuilds.
- The Overview profile now shows a live Now Playing module sourced from active media-server sessions, including paused playback state and current album artwork when available.

## v0.1.78 (2026-04-15)

- Fixed Lidarr artist progression so entries that have genuinely reached `downloaded` no longer return early before the catalog expansion and completion checks run.
- Artists can now advance from intermediate acquisition states through to `catalog_complete` after Lidarr import finishes, instead of getting stuck indefinitely after recovery succeeds.
- Media-server-only downloaded states still short-circuit as before, so Curatorr avoids expanding catalog state before Lidarr has actually imported the files.

## v0.1.77 (2026-04-15)

- Broadened artist collaboration filtering again so recommendation cleanup now catches `duet` labels, credits joined with the word `and`, and more multi-word joint-artist patterns that should not surface as standalone discovery candidates.
- Fixed URL playlist imports on the Playlists page to call the real import endpoints instead of the preview routes, restoring one-click imports for Spotify, YouTube, Last.fm, and ListenBrainz links.
- Collaboration filtering now treats known joint names and multi-word credited artists more aggressively, reducing leftover recommendation noise from soundtrack, duet, and guest-credit naming variations.

## v0.1.76 (2026-04-14)

- Recommendation rebuilds now purge old `suggested` collaboration-credit rows that were written before the collaboration filter existed, so stale joint-credit artists do not linger in Discover after upgrading.
- Collaboration cleanup now runs against the retained suggestion pool during rebuilds, ensuring the newer artist-collaboration rules apply to previously persisted entries as well as newly generated suggestions.
- This removes leftover recommendation noise without requiring users to wait for retention expiry or manually clear stale collaboration suggestions.

## v0.1.75 (2026-04-14)

- Simplified artist collaboration filtering so joint credits are now suppressed whenever any credited artist is already present in the user’s library, reducing recommendation noise for common guest-collaboration patterns.
- Expanded collaboration detection to handle comma-separated credits and ampersand-separated credits with one shared path, improving coverage for suggestions like `Artist A, Guest B & Guest C`.
- Kept the named-band safety guard by only applying the combined-name fallback when at least one credited part looks like a multi-word artist name and none of the parts begin with an article like `The`.

## v0.1.74 (2026-04-14)

- Refined artist discovery collaboration filtering so joint credits already present in the library are suppressed more reliably, including comma-separated primary-artist-plus-guests patterns and person-name collaborations stored under a combined artist name.
- Improved the heuristic to avoid false positives for real band names while still catching temporary collaboration credits, so acts like `Earth, Wind & Fire` and `Southside Johnny & The Asbury Jukes` are less likely to be misclassified.
- The tighter collaboration-credit handling now applies consistently across both library-derived and Last.fm-derived artist suggestions, reducing noisy standalone recommendation candidates.

## v0.1.73 (2026-04-14)

- Discover now reconciles suggested artists against the live Lidarr artist list before review runs, so artists you already added manually in Lidarr are marked as `already_in_lidarr` instead of continuing to appear as fresh Curatorr suggestions.
- Fixed artist progression regressions when Lidarr album references go stale: if an artist was already marked `album_acquired` and Lidarr still reports downloaded files, Curatorr preserves the acquired state and reschedules review instead of dropping the artist back into expansion.
- Filtered collaboration credits out of artist discovery candidates, preventing MusicBrainz-style `feat.`, `featuring`, `ft.`, and known `Artist A & Artist B` joint credits from surfacing as standalone suggestions.

## v0.1.72 (2026-04-14)

- Rebuilt the Artist Pipeline scoring model so Last.fm similar artists compete fairly alongside library-affinity artists: genre affinity now expands compound catalog genres (e.g. Pop/Rock → pop + rock) so Last.fm tags match correctly, and similar artists inherit genre context from their seed artists' catalog genres plus their own Last.fm top tags.
- Fixed a perpetual stale-rebuild loop caused by the model version check comparing against an outdated constant; the pipeline now only rebuilds when the stored model version genuinely differs from the current one.
- Pipeline now refreshes scores for all artists in the full scored pool on each rebuild — not just the top-N — so stale suggested entries outside the display slot get updated scores without waiting for them to rank up first.
- Added "Browse albums" and "Curatorr pick" actions to the pipeline menu: Browse albums loads the artist directly into the Manual Discovery section with a pre-populated artist card; Curatorr pick selects the highest-ranked starter album and sends it to Lidarr automatically.
- Added configurable pipeline discovery settings: sensitivity slider (1–5) controls the minimum Last.fm similarity score required to surface an artist, and a minimum-role gate restricts the Last.fm pool to specific user roles.
- Added a scheduled Artist Pipeline Rebuild job (default every 6 hours) so suggestions stay current without requiring a page visit.
- Fixed inflated legacy scores: 152 rows with pre-model-versioning scores (200–330 range) were reset; actioned artists (in_progress, added_to_lidarr) now show a dash instead of a stale historical score.
- Spotify playlist URL import now works without an active Spotify account connection, falling back to the public-page scraper so users can import shared playlists without OAuth.

## v0.1.71 (2026-04-13)

- Reworked the Discover page around clearer acquisition flows: the top section now separates recent album activity from artist pipeline status, manual discovery aligns more cleanly with the media-card layout, and the artist pipeline menu behaves correctly for top-row entries.
- Added new recent album carousels and richer album popups, including Recently Added and Recently Requested sections, consistent album-card actions, better state-aware popup messaging, fallback artwork handling, and full track details when Lidarr album data is available.
- Fixed Lidarr request cleanup so failed or stale completed requests can be removed cleanly and no longer leave artists permanently stuck in queued pipeline states.

## v0.1.70 (2026-04-13)

- Added broader import and refresh workflows for external playlists: the Playlists page can now import YouTube playlist URLs, Last.fm generated playlists, and ListenBrainz generated playlists, while imported playlists now support configurable automatic refresh periods.
- Reworked the Playlists and User Profile flow so Spotify, Last.fm, and ListenBrainz playlist imports live in one place, imported playlists behave more like first-class editable Curatorr playlists, and profile pages keep only the account-level settings that still matter.
- Clarified discovery and overview behavior across the app with a cleaner Discover artist/request split, non-working queue drag controls removed, stronger overview popup rendering and fallback handling, and more reliable dashboard activity popups for admin sessions and non-library artist rows.

## v0.1.69 (2026-04-12)

- Fixed the animated Music Notes background so disabling it now stops the render loop entirely, tab visibility pauses the effect correctly, and Curatorr no longer burns CPU repainting hidden or disabled canvas frames across the app.
- Fixed stale Lidarr artist and album references so deleted upstream items no longer leave Discover/Lidarr progression stuck until `curatorr.db` is wiped; Curatorr now clears broken links, repairs stored state, and keeps review jobs moving.
- Improved track and playlist workflows: track overview popups now show richer metadata/audio details with pin or unpin actions, Spotify owned-playlist imports work again for larger playlists, and the Spotify import picker is simplified back to owned playlists only.

## v0.1.68 (2026-04-09)

- Added an optional guarded Tautulli repair mode for the daily gap-fill job, allowing Tautulli to upgrade obviously-short Plex listens only when it has a close timestamp match and a longer credible completed play.
- Fixed Plex webhook replay handling so quickly restarting the same track from the beginning no longer traps the earlier partial session as a skip when the combined rolled-up play should count as a proper listen.
- Fixed Lidarr artist progression so Curatorr stops expanding an artist once the tracked album is already present in the local library, including media-server matched cases.
- Fixed analyzer Docker publishing so changes to `scripts/analyze-track-features.py` now trigger a fresh `curatorr-analyzer` image release instead of reusing a stale sidecar image.

## v0.1.67 (2026-04-08)

- Fixed the Admin Users page so playlist totals no longer double-count synced personal playlists, now aggregate across user identity aliases, and show clearer system/user/linked breakdowns with draft counts separated out.
- Fixed Admin Users Lidarr stats so they come from recorded Curatorr usage instead of placeholder progress rows, preventing false `1 / 1 / —` totals for users who had not actually added new content through Curatorr.
- Fixed Lidarr album usage tracking so future album adds reliably record track totals even when the first Lidarr album payload omits `trackCount`, allowing the Admin Users Lidarr tracks column to populate over time.

## v0.1.66 (2026-04-08)

- Added stronger Lidarr monitoring verification and repair for Discover requests, so Manual Discovery no longer treats unmonitored Lidarr album metadata as already added and completed queue rows can re-monitor albums that lost monitoring.
- Added Force Search actions to Discover queue rows and monitored album cards, including a no-reload queue action so manual search state, scroll position, and focus are preserved while Lidarr searches are triggered.
- Added deeper smart-playlist dedupe controls and inspection tools: optional duration/variant/live-album guards now sit alongside the existing dedupe toggles, and the wizard can copy or download a CSV of skipped duplicate matches.
- Fixed release and Discover UI rough edges, including release-popup scrolling on long release notes and ISO album card dates showing only the date instead of the raw `T00:00:00Z` timestamp.

## v0.1.65 (2026-04-07)

- Added a Random Library Mix starting point for smart playlists, with random ordering plus per-artist, per-album, and total track limits so users can build daily library samplers without repeat-heavy results.
- Added Curatorr-generated Decades filters based on each track's year/release-date metadata, including 2020s, while keeping Last.fm tags separate so decade-looking Last.fm tags do not accidentally act like track-year filters.
- Fixed scheduled jobs and stats edge cases: jobs no longer all fire during container startup, the server wizard starts the scheduler and records the first master refresh through job status, Last.fm zero-duration scrobbles replay as plays during rebuilds, and the weekly Last.fm tag sync interval can be saved from Settings.

## v0.1.64 (2026-04-04)

- Discover now uses album-level Lidarr/library reconciliation instead of artist-only presence, including variant matching for standard vs deluxe editions and a Lidarr downloaded fallback when the local media cache has not caught up yet.
- Discover request management is now split cleanly between a live Queue and Added For You history, with row action menus for retry/delete/manual availability overrides and safer failed-request handling.
- Polished the Discover UI with scrollable request popups, a tighter centered action column, smarter queue menu positioning inside the scroller, and drag handles hidden when reordering is not actually available.

## v0.1.63 (2026-04-04)

- Imported Plex and Spotify playlists can now be converted into editable smart playlists, with inferred audio/profile defaults, recommended chip suggestions, a toggle to switch to all detected genres/moods/tags, and an option to keep or remove the original imported mirror.
- Smart-playlist saving is safer: personal and global saves now validate zero-match rule sets, personal playlists can be saved as Curatorr drafts when they currently match nothing, and those drafts now surface directly on the Playlists page for later editing.
- Refined playlist, blend, and dashboard UI behavior with tighter page spacing, clearer Step 2 chip controls in the smart-playlist wizard, better stat-card sizing/consistency, and cleaner blend/dashboard panel spacing.

## v0.1.62 (2026-04-04)

- Reworked Curatorr overview popups to use a Launcharr-style layout with better desktop/mobile sizing, square album artwork, fixed artist backdrops, internal text scrolling, cleaner release dates, and corrected multi-disc numbering.
- Fixed Discover request rows so artists and albums open the correct overview popups, album artwork now resolves from stored Lidarr/manual album data instead of falling back to the artist image, and failed requests get a proper retry button.
- Polished page spacing and scrolling across the app: removed the extra sidebar divider, tightened dashboard/discover/playlist panel gaps, kept the History header visible while scrolling, and refined playlist carousel collapse/expand behavior and page spacing.

## v0.1.61 (2026-04-03)

- Added playlist import workflows for both Plex and Spotify, including Spotify account connection, owned-playlist filtering, previewing matched tracks, and one-click import into Curatorr-managed custom playlists.
- Added imported-playlist review tools directly on the Playlists page: imported cards now show missing counts, imported playlists sort/filter separately, missing source tracks stay attached to the imported playlist, and selected missing rows can be queued for Lidarr review or added straight to the Lidarr queue.
- Improved the playlists page UX for larger imported lists by merging missing rows into the main table, adding imported-playlist refresh support, and auto-collapsing the playlist carousel while you browse the track list.

## v0.1.60 (2026-04-03)

- Added first-class smart-playlist popularity controls powered by Plex `ratingCount`, including album-level Top 3 filtering and absolute popularity slicing (Top 50/25/10/5 percent or custom percentile) directly in the wizard.
- Added popularity flame badges across playlist, tracks, dashboard, history, and blend views so high-performing album tracks are visible at a glance.
- Fixed the smart-playlist wizard flow around advanced rules and general usability: raw `Plex rating count` was removed from the admin-only cleanup step, dropdown headings/layout were corrected, and the modal now keeps a stable height between steps instead of resizing page by page.

## v0.1.59 (2026-04-03)

- Fixed the worst cold-start lag on NAS installs. Curatorr now opens the SQLite database after a sequential pre-read, starts listening before heavy background jobs begin, checkpoints the WAL more aggressively, and truncates it on shutdown. This avoids the long "app is up but unusable" period that happened when startup immediately kicked off expensive playlist/master-track work.
- Fixed smart-playlist template reuse so saved templates now behave like real branching starting points: they preserve advanced filters and the original starting point, can be selected from the first wizard step, and can be updated or deleted later instead of being save-only.
- Improved container startup behaviour by avoiding recursive ownership rewrites across the entire data/config tree and by shipping explicit analyzer CPU/memory caps in the sample compose file, reducing avoidable startup and runtime pressure on lower-spec NAS hardware.

## v0.1.58 (2026-04-02)

- Added analyzer throttling controls — chunk size, delay between chunks (ms), and delay between tracks (ms) are now configurable in Settings under Analyzer Throttling. Useful for low-spec or shared NAS hardware where the analyzer can monopolise CPU or memory.
- Added `--track-delay-ms` flag to the built-in analyzer script; the analyzer sidecar now forwards the configured per-track delay to the script.
- The built-in analyzer now calls `os.nice(10)` at startup to lower its CPU scheduling priority, reducing impact on other processes.
- Added commented-out `cpus` and `mem_limit` resource limit examples to `docker-compose.yml` for users who want hard caps on analyzer resource usage.

## v0.1.57 (2026-03-31)

- Fixed admin users table showing 0 plays and "No play history yet" for users whose Tautulli-stored identity differed from their Plex API username — the table now queries stats using the exact identifier stored in the database rather than deriving it from the Plex API response.
- Fixed admin users table Lidarr stats showing incorrect counts for users who have never used Lidarr automation — caused by the same identity mismatch above.
- Added Lidarr track count tracking — albums added via Lidarr now record the track count so the admin users table can display total tracks added (previously always shown as "—").
- Added retry button on failed "Added for you" items in the Discover page — clicking the inline icon re-queues the request and immediately triggers processing.

## v0.1.56 (2026-03-31)

- Fixed the root cause of slow startup and general app sluggishness on NAS hardware: every log entry was synchronously writing and renaming a 239 KB file to disk, blocking the Node.js event loop. Log writes are now debounced to at most one disk write per second.
- Added a server-side in-memory cache for the Plex/Jellyfin/Emby art proxy routes (`/api/plex/art`, `/api/ms/art`), eliminating redundant upstream image fetches on the playlist page and across the app.
- Fixed two unconditional full-table-scan UPDATE statements in the startup migration that ran on every container restart even when no rows needed updating.
- Added an 8-second timeout to the Docker Hub version check so a slow or unreachable connection does not hang the `/api/version` response.

## v0.1.55 (2026-03-31)

- Fixed track analysis crashing entire chunks when DSF/DFF (DSD) files are encountered — the analyzer now skips these unsupported formats rather than attempting ffmpeg conversion that can exhaust memory on low-spec NAS hardware.

## v0.1.54 (2026-03-31)

- Hardened database migration to re-verify column existence before creating partial indexes, preventing `SqliteError: no such column` crashes when upgrading from older installs.
- Fixed Electron desktop wrapper so fatal curatorr startup errors show a dialog instead of silently killing the process.

## v0.1.53 (2026-03-31)

- Fixed Windows desktop startup crash where upgrading from an older install caused a fatal `SqliteError: no such column` due to partial indexes being created before the columns they reference were migrated.
- Fixed dashboard stat counts (excluded tracks, skip-tier artists, belter tracks, heard tracks) showing incorrect values by using the correct user ID in queries.

## v0.1.52 (2026-03-30)

- Clarified regex-based playlist filtering so the UI now labels those rules as exclusions, reducing confusion around `does not match regex` behavior.
- Added full file-path matching support for regex exclusion rules so mixed compilation folders can be filtered without selecting each folder manually.
- Fixed global playlist filter editing so existing file-path rules load and round-trip correctly in Settings.

## v0.1.51 (2026-03-30)

- Reverted Docker runtime base from Debian slim back to `node:20-alpine`; `wget` is now explicitly included so compose healthchecks work without changes.
- Fixed CI hang: `server.close()` now calls `closeAllConnections()` first so keep-alive connections don't block test teardown.
- Fixed CI hang: media server session polling timer in the webhook handler now calls `.unref()` so the Node.js process exits after tests complete instead of waiting for the next poll interval.

## v0.1.50 (2026-03-30)

- Fixed the main Curatorr Docker image healthcheck regression by restoring `wget` in the runtime image after the move from Alpine to Debian slim.
- Fixed repeated restarts on installs that use the existing compose healthcheck `wget -q -O /dev/null http://127.0.0.1:7676/`, including the default `/share/Docker` deployment.

## v0.1.49 (2026-03-30)

- Fixed analyzer tag reuse in Docker publishes so release tags now fall back to the nearest existing analyzer image on Docker Hub instead of assuming the immediately previous git tag was published successfully.
- Restored complete aligned release tagging for `curatorr` and `curatorr-analyzer` after the `v0.1.48` analyzer retag step failed looking for a non-existent `v0.1.47` analyzer image.

## v0.1.48 (2026-03-30)

- Fixed Docker release packaging for the main Curatorr image by moving the app container off Alpine/QEMU for native module installation and using a Debian-based Node image instead.
- Fixed runtime user switching in the container entrypoint so the same image continues to support `PUID` and `PGID` overrides after the base-image change.
- Restored a clean path for tagged multi-arch publishes after the `v0.1.47` app image failed during the ARM64 build stage.

## v0.1.47 (2026-03-30)

- Added richer playlist management controls with per-card audience badges, Launcharr-style playlist filtering, status-aware ordering, and enable/disable actions for personal, external, blend, global, and system playlists.
- Added broader smart-playlist deduplication and ordering controls, including release-variant title dedupe plus shared base/final ordering support across personal, external, and system playlists.
- Fixed disabled and external playlists so they can be deleted cleanly, external toggles now stay in sync with user-profile source settings, and filtered playlist carousels no longer leave a shifted gap after narrowing the visible set.
- Improved analyzer-side release readiness with clearer unreadable-track error handling and release workflow support for optional shared announcements plus analyzer image reuse when the sidecar has not changed.

## v0.1.46 (2026-03-29)

- Replaced the old smart-playlist editors with the new multi-step wizard for create and edit flows, including personal, blend, and global playlists from the same UI.
- Added settings-derived starting points for `Curatorr`, `Crescive`, and `Curative`, plus a new `Wake Up` audio profile and broader audio/content preset seeding.
- Added smarter wizard previewing and filter controls, including eligible-pool vs final-playlist counts, tri-state tier filters, per-section `Any/All` include matching, accordion sections, and admin-only advanced folder/regex settings.
- Fixed analyzer batch processing so unreadable or broken tracks are skipped instead of aborting the whole chunk, and surfaced richer analyzer failure details in the app.

## v0.1.45 (2026-03-29)

- Fixed the npm lockfile so GitHub Actions `checks` can run `npm ci` successfully against the committed dependency manifest.
- Keeps the analyzer release packaging and multi-arch Docker publish fixes from `v0.1.44`, including tracked analyzer worker scripts and `linux/amd64` plus `linux/arm64` analyzer images.

## v0.1.44 (2026-03-29)

- Fixed release packaging so the analyzer worker scripts are tracked in Git and included in tagged source checkouts used by GitHub Actions.
- Fixed analyzer Docker builds failing with missing `scripts/analyze-track-features.py` and `scripts/analyzer-sidecar.py` during `COPY`.
- Keeps the `v0.1.43` Docker publish fix so `mickygx/curatorr-analyzer` continues to publish for both `linux/amd64` and `linux/arm64`.

## v0.1.43 (2026-03-29)

- Fixed Docker Hub publishing so `mickygx/curatorr-analyzer` is now released alongside the main app image instead of being skipped by the publish workflow.
- Fixed analyzer sidecar image publishing to target both `linux/amd64` and `linux/arm64`, restoring sidecar support on x86_64 Docker hosts.
- Split GitHub Actions build-cache scopes for the app and analyzer images so the two multi-arch builds do not overwrite each other's cached layers.

## v0.1.42 (2026-03-29)

- Added analysis-based smart-playlist sort modes including BPM, energy, danceability, Camelot wheel, and DJ-flow ordering for global, personal, and blended playlist builders.
- Fixed the smart-playlist builders and preset cards so they render cleanly in both light and dark themes, with improved modal sizing, readable helper text, and better field spacing.
- Fixed local-admin dashboard banners and builder notices in light theme so Plex sign-in guidance, preview labels, and other theme-sensitive text stay readable.
- Fixed duplicate personal playlist records pointing at the same Plex playlist by deduplicating stale rows and preventing duplicate-name saves for the same user.
- Fixed analyzer-sidecar workflows in production so the sidecar must see the exact media path Curatorr exports, preventing zero-row analysis runs caused by mismatched mounts.

## v0.1.41 (2026-03-28)

- Added an optional analyzer sidecar and custom analysis pipeline workflow for automatic BPM, musical key, Camelot key, energy, and danceability enrichment, with chunked processing, progress updates, and Docker-friendly defaults.
- Added track enrichment workflows around MusicBrainz release dates, feature manifest import/export, and Plex loudness sync so Curatorr can combine external metadata, local analysis, and Plex loudness data in one shared enrichment store.
- Added feature-driven playlist presets for both global and personal playlists, including visual preset cards, Camelot focus/spread controls, coverage-aware preset gating, and live feature-match counts while editing.
- Extended Camelot matching to support multiple focus keys plus exact, adjacent, relative major/minor, and full harmonic-set modes, with inline help explaining the DJ wheel notation.
- Fixed feature filters and preset previews so tracks without BPM/energy/danceability data are excluded instead of being treated like zero-valued matches, preventing massively inflated preset counts.

## v0.1.40 (2026-03-28)

- Added a new generated rotating playlist type called `Curatorr`, blending familiar favourites with discovery tracks using configurable target size, discovery ratio, artist caps, cooldowns, and track filters.
- Expanded Daily Mix into a fully configurable rotating playlist with controls for favourite, suggested, and fresh-track mix, total track count, per-artist caps, repeat cooldown, and filters.
- Unified Daily Mix and Curatorr rebuild/sync handling so jobs, playlist rebuilds, stored playlist membership, wizard creation, and playlists-page support all behave consistently.
- Fixed settings persistence so very small Daily Mix and Curatorr playlist sizes are saved correctly instead of being clamped back to older minimum values.
- Fixed missing strict-match defaults in user preferences, preventing first-save and ListenBrainz fallback-path issues.

## v0.1.39 (2026-03-28)

- Added max track duration setting in Settings → Smart Playlists — tracks longer than the configured limit (default 10 minutes) are excluded from Crescive, Curative, and personal smart playlists, preventing long live recordings from negatively skewing play-skip scoring.
- Improved cross-source playlist matching so Last.fm and ListenBrainz tracks resolve more reliably against Plex library items even when metadata differs slightly.
- Fixed featured-artist text in track titles and artist names causing otherwise identical songs to miss the library match step.
- Fixed parenthetical and bracketed title suffixes such as remaster, live, and remix labels causing playlist seed tracks to fail matching when Plex stores a cleaner title.

## v0.1.37 (2026-03-28)

- Added ARM64 Docker image — the published image is now multi-arch (`linux/amd64,linux/arm64`), so Raspberry Pi and Apple Silicon NAS devices can pull a native image without emulation.
- Fixed user-created smart playlists (personal playlists) not being rebuilt by the scheduled sync job — they are now included alongside Crescive, Curative, and Daily Mix in every automatic sync cycle.
- Fixed admin preview showing no playlists or play history when a Plex user's webhook identity differs from their OAuth login identity (e.g. numeric Plex account ID vs username) — the system now carries both identities separately and uses the correct one for each type of query.
- Fixed playlist artwork not loading in admin preview — the previewed user's own Plex token is now used when fetching playlist artwork, instead of the admin token which cannot see other users' playlists.
- Fixed duplicate and triplicate tracks appearing in Crescive and Curative playlists when the same song exists across multiple albums — tracks are now deduplicated by normalised artist and title, keeping the highest-rated copy.
- Fixed featured-artist variants (e.g. `Eminem f/ Eye-Kyu`) being treated as different artists from the primary during deduplication, causing both versions of the same song to appear in a playlist.
- Fixed dashboard playlists panel showing empty when viewing another user in admin preview.

## v0.1.34 (2026-03-26)

- Added playlist-style select, view, and add controls to manual discovery album cards and Curatorr picks, with richer album overview modals including track lists and ordered fallback albums.
- Added direct edit actions for global playlists from Settings and the Playlists page, and added independent enable toggles for Crescive and Curative generated playlists.
- Fixed recent play consolidation so repeated listens merge more reliably across pause/resume and metadata variation, and the dashboard recent-plays panel now reflects the rolled history view.
- Fixed Plex webhook re-registration so stale Curatorr `/webhook/plex` URLs are pruned automatically instead of accumulating in Plex.

## v0.1.32 (2026-03-23)

- Restored the Blend page header status area so the current version badge and the light/dark theme toggle appear again.
- Polished the Blend page layout by tightening panel spacing, reducing oversized helper text, and matching the Top Blended Artists and Top Blended Tracks panel heights more closely.
- Restored the leading left-edge gutter on the Playlists and Blend carousels so the first card no longer sits flush against the panel edge.

## v0.1.31 (2026-03-23)

- Fixed the Plex Home login flow again by preventing duplicate profile-selection submits from dropping users back to `/login` after choosing their home profile.
- Fixed Blend and blended smart-playlist user avatars for normal Plex users, so non-admin sessions now resolve live Plex/Home profile images instead of falling back to initials only.
- Simplified the smart-playlist tier filters by removing the redundant `All` option and tightening the tier-chip layout under the Artist Tier and Track Tier labels.

## v0.1.30 (2026-03-23)

- Fixed a Plex Home login loop where some browsers would drop the cookie-backed session before profile selection completed, sending users back to `/login` after successful Plex authentication.
- Fixed the Blend page shell so it now loads the shared dashboard scripts, restoring the animated music-note background and the user-pill pop-up menu.

## v0.1.29 (2026-03-23)

- Added a new Blend page with a compatibility-ranked user carousel, playlist-style artwork cards, and live shared artist/track breakdowns for building blends around the current listener.
- Added blended smart playlists, allowing playlists to be generated from two or more users instead of a single listener.
- Added a Plex owner sign-in token fetch flow to the server setup wizard, so Plex tokens can be stored without manual copy/paste.
- Fixed the user setup wizard so Plex-backed genre and artist selection bootstrap correctly even when the Plex token was skipped earlier in setup.
- Fixed Plex playback finalisation so repeated plays of the same track are tracked as separate completed events instead of collapsing together.
- Fixed Lidarr artist-list request error handling for abort/timeout cases and increased the request timeout for large libraries.

## v0.1.23 (2026-03-19)

- Added Last.fm Neighbours station playlist — toggle alongside Recommended, Mix, and Library in User Profile.
- Added Last.fm Full History Backfill controls to User Profile — users can now trigger and reset their own backfill without admin access.
- Fixed Last.fm History Backfill incorrectly marking history as complete after the first batch when more than 10,000 scrobbles exist; the cursor now advances correctly across all pages.
- Fixed playlist carousel overflowing the page horizontally instead of scrolling internally.
- Added Last.fm Tags, Last.fm Sync, and Last.fm Backfill as separate filter options in the Settings activity log.

## v0.1.22 (2026-03-19)

- Added Plex Home Users support — profile picker shown after Plex SSO when multiple home users exist, with PIN entry for PIN-protected profiles.
- Added setup wizard step 6 prompting the admin to sign in with Plex immediately after server setup completes.
- Added local admin banner on Dashboard, Discover, History, and Playlists directing local admin users to sign in with Plex to access music features.
- Fixed confirm password fields in `/setup` and wizard step 1 using `autocomplete="new-password"`, which triggered Firefox's password generation UI on the confirmation field.
- Fixed Plex Home PIN entry field using webkit-only `-webkit-text-security: disc` with `type="tel"`, causing PIN digits to display as plain text in Firefox. Now uses `type="password"` with `inputmode="numeric"`.

## v0.1.21 (2026-03-19)

- Unified all playlist naming to `Type (Username)` format — Crescive, Curative, Daily Mix, global, and personal playlists are renamed in Plex in-place on first sync after upgrading.
- Added automatic Plex playlist rename on sync whenever the stored title drifts from the desired title.
- Fixed Last.fm station playlists: resolveMachineId was called with userId instead of config (preventing Plex playlist creation), and master track lookup used snake_case keys against a camelCase result set (causing 0 track matches). Both bugs introduced in v0.1.18.

## v0.1.20 (2026-03-19)

- Fixed Last.fm station playlist track matching: internal lookup used snake_case column names against a camelCase result set, causing 0 tracks to match regardless of library content.
- Fixed Last.fm station playlists incorrectly adopting the Curative Playlist's Plex ID due to a legacy title fallback being applied to all playlist types. Each station now gets its own independent Plex playlist.

## v0.1.19 (2026-03-19)

- Fixed Last.fm Station Playlists (Recommended, Mix, Library) silently failing to sync to Plex due to an incorrect argument passed when resolving the Plex server identifier. Playlists now create and update correctly on every Smart Playlist Sync run.

## v0.1.18 (2026-03-19)

- Added per-user Last.fm History Sync: set a Last.fm username in User Profile to pull scrobble history into Curatorr on a configurable schedule, feeding artist and track scoring the same as Plex/Tautulli plays.
- Added Last.fm History Backfill: a manually triggered job that pages backwards through a user's full Last.fm history in batches, with a resumable cursor so it can work through years of history across multiple runs.
- Added Last.fm Station Playlists: Recommended, Mix, and Library stations can be enabled per user in User Profile and are kept in sync as Plex playlists (`Recommended (Last.fm)` etc.) on every playlist sync cycle.
- Fixed duplicate playlist creation: Curatorr now searches Plex by title before creating a playlist, preventing duplicates when the database is reset or migrated.
- Renamed Crescive and Curative playlists to `Crescive Playlist (username)` / `Curative Playlist (username)` so the playlist type is visible in Plexamp's truncated playlist list. Existing playlists are renamed in place on first sync.

## v0.1.14 (2026-03-16)

- Refined the day-theme styling across filter controls, panel chrome, discover titles, and Plex modals so the light theme now uses a consistent readable surface treatment instead of falling back to dark-theme colors in key UI areas.

## v0.1.13 (2026-03-16)

- Fixed the server wizard Tautulli skip path so choosing to skip Tautulli setup now renders the next wizard step correctly instead of calling the render helper with the wrong argument shape.

## v0.1.12 (2026-03-16)

- Fixed Crescive and Curative manual rebuilds so they now perform a true full rebuild from current user preferences instead of only evolving the existing playlist state.
- Fixed always-include artists so playlist rebuilds now reapply the configured favorite-artist track percentages correctly across both generated playlists.
- Refreshed the README and wiki with current product screenshots for dashboard, login, discovery, and playlists, and added a generated PNG app icon for docs and release assets.

## v0.1.11 (2026-03-16)

- Fixed Lidarr artist tagging so Curatorr now tags artists even when the artist already exists in Lidarr or the request path reaches an already-monitored item.
- Added a Lidarr tag backfill utility for existing Curatorr-managed artists and made album-tag failures explicit on Lidarr servers that do not expose album tag support.
- Fixed local Curatorr admin scoping so dashboard, artists, tracks, and history stay globally scoped without generating fake per-admin discovery suggestions.
- Fixed Settings so “Last Curatorr login” now surfaces existing Plex-auth login timestamps and records future Plex logins into the same Curatorr login store.
- Improved history-page responsiveness by caching repeated artist and album artwork proxy requests, reducing expensive repeated upstream lookups during table renders.
- Aligned history and artists table headers with their content columns for a cleaner media-table layout.

## v0.1.10 (2026-03-16)

- Added user-created personal smart playlists with inline previewing, build/sync support, and richer playlist management controls.
- Expanded playlist filtering with genres, Plex moods, and stored Last.fm artist tags across settings, playlist creation, and preview flows.
- Improved playback tracking by consolidating split listens within a 10-event lookback so pause/resume or quick restart sequences accumulate into one play instead of creating false skips.
- Added a scheduled Last.fm tag sync job plus mood ingestion in the master track refresh pipeline, so playlist filters have richer metadata to work with.

## v0.1.9 (2026-03-16)

- Reworked playlist management around a card carousel, inline rebuild actions, background full-playlist loading, and sortable track tables that keep the visible slice small while sorting against the full playlist.
- Unified the playlist, tracks, artists, and history tables around the same compact media-table pattern with artwork-aware columns, sortable headers, consistent filtering, and shared panel controls.
- Refreshed the dashboard and discovery surfaces with playlist and active-stream carousels, cleaner recommendation panels, and tighter mobile behavior for discovery ledgers, dashboard widgets, and playlist views.
- Improved discovery artwork loading and caching so recommendation art renders more reliably without blocking the page on repeated upstream lookups.

## v0.1.8 (2026-03-13)

- Fixed Launcharr embed scrolling so long Curatorr pages, including Ages, can scroll to the real bottom of the content instead of being clipped inside a fixed-height iframe shell.
- Updated the embedded layout to use document-level scrolling in Launcharr mode while keeping the standalone Curatorr layout unchanged.

## v0.1.7 (2026-03-13)

- Switched Curatorr to a Plex-first playback model with a selectable live playback source, so Plex webhooks can drive now-playing and play scoring while Tautulli remains available as an optional backup source.
- Reworked Plex play ingestion to accumulate session progress across play, pause, resume, scrobble, and next-track transitions, reducing false skips, duplicate recent-play rows, and bad track-length assumptions.
- Brought Tautulli webhook ingestion onto the same session-accumulator model and limited Tautulli daily sync to gap-fill and repair work instead of overwriting plays already recorded by Plex.
- Added automatic Plex webhook registration during setup and from Settings, including server-side webhook enablement and clearer Plex/Tautulli setup guidance in the UI.
- Expanded Lidarr recommendation handling with clearer recommendation wording, fixed stale queue states, separate automatic-add caps, manual vs automatic source labels, and Curatorr-applied Lidarr tags for artist and album adds.

## v0.1.6 (2026-03-12)

- Hardened inbound webhook handling with a shared-secret protected URL for Plex and Tautulli, and surfaced the secure webhook URL directly in Settings.
- Fixed Tautulli webhook auto-registration so the Settings button now saves the webhook URL, enabled triggers, and JSON payload instead of creating a blank notifier.
- Restricted the Lidarr image proxy to authenticated requests and approved media-cover paths only, preventing unauthenticated use of the stored Lidarr API key.
- Tightened admin-only utility APIs and stopped rendering plaintext Plex, Tautulli, and Lidarr credentials to `co-admin` users in Settings.
- Moved Plex and Tautulli credentialed API calls away from query-string authentication to reduce token leakage in logs and upstream requests.

## v0.1.5 (2026-03-11)

- Fixed Plex settings so the saved Plex token remains in the masked input after save, matching the machine ID field and avoiding the appearance that the token was cleared.
- Fixed the Settings route to expose stored-secret state consistently for Plex, Tautulli, and Lidarr, so saved credentials render correctly in the UI.
- Added Launcharr-style release loading in About settings, so Curatorr now lists recent releases from `docs/release/releases` and shows real changelog highlights in the release modal.

## v0.1.4 (2026-03-11)

- Fixed onboarding redirects so locally created Curatorr accounts, including the setup admin, are no longer forced into the server wizard automatically. Plex accounts still auto-run onboarding when required.

## v0.1.3 (2026-03-11)

- Changed onboarding so the personal wizard only auto-runs for Plex accounts. Locally created Curatorr users, including the setup admin, can still launch it manually when needed.

## v0.1.2 (2026-03-11)

- Fixed the first-run setup flow so the server wizard no longer shows a duplicate second admin account step.
- Fixed personal wizard handoff so both local and Plex logins are redirected into the favourite-genres flow when the user has not completed onboarding.
- Fixed the user wizard to warm the master track cache before rendering, so genre choices appear on fresh installs instead of showing an empty first step.
- Added local ignore rules for temp compose and temp data folders used during release testing.

## v0.1.1 (2026-03-11)

- Added Launcharr-friendly embed mode with iframe-safe chrome removal, theme syncing, and configurable allowed embed origins.
- Added a manual Tautulli webhook registration action in Settings, alongside clearer Tautulli setup guidance.
- Fixed Tautulli webhook registration to fall back to Curatorr local or remote URLs when a dedicated webhook URL is not set.
- Improved release scaffolding for GitHub releases and Docker Hub publishing.

## v0.1.0 (2026-03-11)

- Initial Curatorr release notes scaffold.
