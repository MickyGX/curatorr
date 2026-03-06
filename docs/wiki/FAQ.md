# FAQ

## Does Launcharr require Plex login?

Plex SSO is the main authentication path, but Launcharr also supports a local fallback admin account for recovery and initial setup.

## What roles are available?

- `admin`
- `co-admin`
- `user`

`admin` is required for settings access.

`guest` is also supported as a dashboard/module visibility role for view-mode and access control tuning.

## Where are my settings saved?

- App/category overrides: `config/config.json`
- Admin/co-admin lists: `data/admins.json`, `data/coadmins.json`
- Logs: `data/logs.json`

## Can I hide apps from regular users?

Yes. Use `Settings -> Display` to control overview/launch visibility by role.

## Can I add custom apps?

Yes. Launcharr supports custom apps, categories, and custom icons.

## Why do some apps work better in new tab mode?

Some upstream apps block iframe embedding via security headers. Use `new-tab` launch mode for those apps.

## How do I make guest users read-only?

Use role permissions and `restrictGuests` in general settings to constrain access behavior.

## Is there a health endpoint for monitoring?

Yes: `GET /healthz`
