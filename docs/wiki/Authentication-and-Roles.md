# Authentication and Roles

## Authentication Modes

Curatorr supports two login methods side by side:

### Media server sign-in

Users authenticate with the selected media server.

This supports:

- Plex users
- Plex Home users and managed home-user selection after sign-in
- Jellyfin users
- Emby users

On Plex installs, the Plex server owner is treated as the main admin account when matched against the configured Plex admin user.

### Local admin account

Created during setup.

This account exists as an always-available fallback and keeps full admin access even if Plex SSO is unavailable.

## Roles

| Role | Description |
|---|---|
| `Admin` | Full access to settings, jobs, users, themes, logs, and automation. |
| `Co-admin` | Broad access with admin-only actions still restricted. |
| `Power user` | Standard user access plus optional Lidarr automation if enabled by role scope/quota. |
| `User` | Standard per-user history, playlists, discover, and profile access. |
| `Guest` | Read-only style access with restricted interaction. |

## Managing Users

Use `Settings -> Users` to:

- review known users
- change roles
- remove users
- inspect Plex-linked identities and activity context

## Local Admin Preview Mode

The local Curatorr admin can preview the app as another Plex user.

This is useful for:

- checking a user's dashboard, playlists, history, and discover state
- verifying role-based access
- troubleshooting user-specific playback or automation issues

## Lidarr Access by Role

Lidarr access is controlled by:

- whether Lidarr is configured
- whether automation is enabled
- automation scope
- per-role quotas

Users can still be effectively blocked by quota even if the UI surface is visible.
