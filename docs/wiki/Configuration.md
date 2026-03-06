# Configuration

## Config Model

Launcharr merges shipped defaults with user overrides:

- `config/default-apps.json`: built-in app definitions.
- `config/default-categories.json`: built-in category definitions.
- `config/config.json`: only your overrides/customizations are saved.

This keeps upgrades cleaner because defaults can evolve while your override file stays compact.

## General Settings

`Settings -> General` writes to `config.general`.

Important fields:

- `serverName`
- `localUrl`
- `remoteUrl`
- `restrictGuests`
- `autoOpenSingleAppMenuItem`
- `hideSidebarAppSettingsLink`
- `hideSidebarActivityLink`

## App Display and Launch

`Settings -> Display` controls:

- App and module visibility by role (`guest`, `user`, `co-admin`, `admin`).
- Overview and launch permission toggles.
- Launch mode per app:
  - `iframe`
  - `new-tab`
  - `disabled`
- Category assignment and ordering.
- Combined Arr/downloader dashboard sections.
- Queue column visibility and row limits.
- Dashboard visibility roles and ordering.
- Widget bars and widget row layout.

## App Credentials and Integrations

Per-app `Settings` pages store credentials and connection info (for example API keys, tokens, local/remote URLs).

Plex-specific:

- `Get Plex Token`
- `Get Plex Machine`

These are used by Plex widgets and user management flows.

## Roles and User Access

Role mappings and ownership files:

- `data/admins.json`
- `data/coadmins.json`

Plex-based role assignment is managed in `Settings -> Users`.

## Notifications

`Settings -> Notifications` supports Apprise:

- `appriseEnabled`
- `appriseApiUrl`
- `appriseMode` (`targets` or `config-key`)
- `appriseTargets`
- `appriseConfigKey`
- `appriseTag`
- `widgetStatusEnabled`
- `widgetStatusDelaySeconds`
- `widgetStatusPollSeconds`
- `widgetStatusRequestTimeoutMs`
- `widgetStatusMaxConcurrency`

Use `Test Notification` to validate routing before rollout.

When enabled, widget status monitoring can trigger Apprise notifications after a delay threshold for state changes (online/offline).

## Logs

`Settings -> Logs` controls retention:

- `maxEntries`
- `maxDays`
- `visibleRows`

Persisted file:

- `data/logs.json` (or `LOG_PATH` if overridden)

## Security Baseline

Launcharr includes baseline hardening for reverse-proxy deployments and browser clients:

- Conditional proxy trust with limited hop count:
  - `TRUST_PROXY` (`true|false`)
  - `TRUST_PROXY_HOPS` (default `1`, clamped)
- Safer default request body limits:
  - `URLENCODED_BODY_LIMIT` (default `8mb`)
  - `JSON_BODY_LIMIT` (default `2mb`)
- Per-route payload guards on sensitive endpoints (for example login/setup/session-switch routes).
- CSRF protections on unsafe methods (`POST`, `PUT`, `PATCH`, `DELETE`), plus state-changing routes moved off `GET` (for example `/logout` and `/switch-view` now require `POST`).
- Security response headers suitable for Launcharr's iframe usage (`frame-ancestors 'self'`, `X-Frame-Options: SAMEORIGIN`, `nosniff`, referrer/policy headers).
- Local-auth password policy requires:
  - minimum length `12`
  - at least one lowercase letter
  - at least one uppercase letter
  - at least one number
  - at least one symbol

Recommended Docker env (especially when running behind a reverse proxy):

```yaml
environment:
  - TRUST_PROXY=true
  - TRUST_PROXY_HOPS=1
  # optional but recommended if you only use HTTPS on domain:
  # - COOKIE_SECURE=true
```

2FA note: local-auth 2FA is not yet built in. For production access, enforce MFA at your identity provider/reverse proxy (or use Plex/OIDC SSO) until native 2FA is added.

## Example Override (`config/config.json`)

```json
{
  "general": {
    "serverName": "Launcharr",
    "remoteUrl": "https://launcharr.example.com",
    "localUrl": "http://192.168.1.20:3333",
    "restrictGuests": false,
    "autoOpenSingleAppMenuItem": false,
    "hideSidebarAppSettingsLink": false,
    "hideSidebarActivityLink": false
  },
  "logs": {
    "maxEntries": 500,
    "maxDays": 14,
    "visibleRows": 15
  },
  "notifications": {
    "appriseEnabled": false,
    "appriseApiUrl": "",
    "appriseMode": "targets",
    "appriseTargets": "",
    "appriseConfigKey": "",
    "appriseTag": "",
    "widgetStatusEnabled": false,
    "widgetStatusDelaySeconds": 60,
    "widgetStatusPollSeconds": 45,
    "widgetStatusRequestTimeoutMs": 4000,
    "widgetStatusMaxConcurrency": 4
  }
}
```
