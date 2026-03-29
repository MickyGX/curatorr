# Release Notes

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
