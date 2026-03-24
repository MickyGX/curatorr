# Integrations

## Plex

Plex is required.

Curatorr uses Plex for:

- sign-in and Plex Home user selection
- music library indexing
- smart playlist creation and updates
- live playback webhooks when playback source is set to `Plex`

### Plex setup

In `Settings -> Plex`:

1. Set local and/or remote Plex URL
2. Retrieve the Plex token
3. Retrieve the machine ID
4. Select the music libraries Curatorr should monitor
5. Save

You can also use `Refresh libraries` later if you add or remove Plex music libraries after initial setup.

### Plex webhook behavior

Curatorr can register the Plex webhook automatically from the wizard or Settings.

Plex is the recommended live playback source for most installs.

## Tautulli

Tautulli is optional.

Curatorr uses Tautulli for either:

- optional live playback events, if playback source is set to `Tautulli`
- history backfill and repair through the Tautulli API

### Tautulli setup

In `Settings -> Tautulli`:

1. Set local and/or remote Tautulli URL
2. Add the API key
3. Save
4. Optionally click `Register webhook in Tautulli`

### Important distinctions

- Gap-fill/backfill only needs the URL and API key.
- The Tautulli webhook is only needed for live Tautulli playback ingestion.
- If playback source is still `Plex`, live Tautulli webhooks are ignored.

### Library filtering

Tautulli imports now respect the selected Plex music libraries:

- live webhooks from unselected libraries are ignored
- gap-fill rows from unselected libraries are ignored

## Last.fm

Last.fm is optional and is used in two separate ways.

### Shared Last.fm API key

Configured in `Settings -> Discovery`.

This powers:

- Trending Artists
- Trending Tracks
- Because You Like

### Per-user Last.fm account features

Configured in `User Profile`.

Users can set:

- Last.fm username
- station playlist types
- Loved tracks playlist
- Top Tracks playlist and period
- full-history backfill controls

Last.fm history sync is not a replacement for webhook playback tracking. It is best used as historical backfill or as a supplement.

## ListenBrainz

ListenBrainz is optional and user-specific.

Configured in `User Profile`.

Current support is for playlist suggestions synced into Plex:

- Daily Jams
- Weekly Jams
- Weekly Exploration

Current matching is artist + track-title based, matching the existing Last.fm station infrastructure. This does not currently import ListenBrainz listening history.

Logging is available under the `ListenBrainz` filter in the Curatorr log view.

## Lidarr

Lidarr is optional.

When enabled, Curatorr can:

- add artists to Lidarr
- choose a starter album
- queue search commands
- retry and fall back to release grabs when searches fail
- progressively unlock more albums as engagement improves

Weekly quotas can be configured per role, and the app also supports automatic-add quotas for eligible users.
