# Release Notes

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
