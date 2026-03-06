# Troubleshooting

## Login Loop or Session Loss

Checks:

- Set a non-default `SESSION_SECRET`.
- If served over HTTPS, set `COOKIE_SECURE=true`.
- Confirm reverse proxy forwards host/protocol headers.

## Plex Login Fails or Callback Errors

Checks:

- `Settings -> General -> Remote URL` is set to your public URL.
- Plex app has correct local/remote URL values.
- Re-run `Get Plex Token` and `Get Plex Machine`.

## Missing Widgets or Empty Dashboard Cards

Checks:

- User role can access app overview menu.
- Overview elements are enabled for that app.
- App credentials/API keys are configured.

## Access Denied for Settings

Expected behavior:

- Only `admin` can access settings routes.
- `co-admin` has overview/launch access but not settings access.

## App Not Loading in Iframe

Common causes:

- Target app denies framing (`X-Frame-Options` / CSP).
- Incorrect remote URL or mixed content issue.

Fix:

- Switch app launch mode to `new-tab`.

## Logs Are Not Persisting

Checks:

- Ensure `DATA_DIR` is writable.
- Confirm `LOG_PATH` points to a writable file path.
- Verify container UID/GID (`PUID`/`PGID`) matches host volume ownership.

## Reset Plex Device Identity

If Plex auth material is corrupted, stop Launcharr and remove:

- `data/plex_private.pem`
- `data/plex_public.json`
- `data/plex_client_id.txt`

Then restart and re-authenticate.
