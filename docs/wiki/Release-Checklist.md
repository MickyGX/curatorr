# Release Checklist

Use this checklist before announcing a Launcharr release.

## Product Readiness

- [ ] Version number updated (`package.json` / image tag).
- [ ] `README.md` reflects current setup and feature set.
- [ ] Wiki pages updated for any settings or auth changes.
- [ ] Deprecated routes/flags reviewed.

## Deployment Readiness

- [ ] Docker image built and pushed.
- [ ] Compose examples validated.
- [ ] Reverse proxy example updated if labels or ports changed.
- [ ] Migration notes prepared (if config/data behavior changed).

## Security and Access

- [ ] Default `SESSION_SECRET` overridden in deployment docs.
- [ ] Role behavior validated (`admin`, `co-admin`, `user`).
- [ ] Plex login/callback tested via local and remote URLs.
- [ ] Sensitive values not committed to repo.

## Functional Verification

- [ ] First-run flow (`/setup`) tested on clean data volume.
- [ ] Plex SSO login and logout tested.
- [ ] Dashboard renders widgets for enabled apps.
- [ ] App launch modes tested (`iframe`, `new-tab`, `disabled`).
- [ ] Logs and notifications settings validated.

## Final Communication Pack

- [ ] Release notes drafted.
- [ ] Breaking changes called out clearly.
- [ ] Upgrade steps included.
- [ ] Known issues and workarounds documented.
