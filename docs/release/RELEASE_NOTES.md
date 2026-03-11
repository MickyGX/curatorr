# Release Notes

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
