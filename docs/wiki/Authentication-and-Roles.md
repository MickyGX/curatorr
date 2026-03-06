# Authentication and Roles

## Authentication Flows

Launcharr supports two sign-in paths:

1. Local fallback login (`/setup` then `/login`)
2. Plex SSO (PIN flow + callback)

## Local Fallback Admin

On a fresh install (no local admin yet), Launcharr redirects to `/setup`.

Setup requirements:

- Username required.
- Valid email required.
- Password minimum length: `6`.
- Password confirmation must match.

The first local setup account is created as role `admin`.

## Plex SSO

Launcharr uses Plex PIN login and stores session data in a cookie session.

For reliable callback behavior behind a reverse proxy:

- Configure `Settings -> General -> Remote URL`.
- Set `COOKIE_SECURE=true` when serving HTTPS.

If no Plex admin list exists yet, the first Plex user to authenticate becomes owner admin.

## Roles

| Role | Overview Access | Launch Access | App Settings | Global Settings |
| --- | --- | --- | --- | --- |
| `admin` | Yes (admin menu flags) | Yes (admin menu flags) | Yes | Yes |
| `co-admin` | Yes (admin menu flags) | Yes (admin menu flags) | No | No |
| `user` | Yes (user menu flags) | Yes (user menu flags) | No | No |

Notes:

- Role enforcement is route-level (`requireUser`, `requireAdmin`, `requireSettingsAdmin`).
- `co-admin` cannot access settings routes.
- Admin can switch view mode via `/switch-view?role=guest|user|co-admin|admin` to preview role experiences.
- `guest` is a visibility/view role for dashboards/modules, not a standalone authenticated account role.

## Role Storage

- Owner/admin list: `data/admins.json`
- Co-admin list: `data/coadmins.json`

`ADMIN_USERS` environment variable can bootstrap initial admins (comma-separated identifiers).

## Recovery and Safety

- Keep one known local admin credential for lockout recovery.
- Use Plex users + roles UI to manage day-to-day access.
- Back up `config/config.json` and `data/admins.json` before major changes.
