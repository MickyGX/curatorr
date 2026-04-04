# Curatorr Wiki (In-Repo Source)

This folder contains the in-repo source for the Curatorr GitHub wiki.

These source pages use normal repo-relative `.md` links so they work when browsed directly on GitHub. The publish script rewrites those links to GitHub wiki-style page links during sync.

You can:

- Keep these pages in-repo as product documentation.
- Publish them to the GitHub wiki with the included sync script.
- Edit them alongside feature work so README and wiki stay aligned.

Recommended publish order:

1. `Home.md`
2. `Quick-Start.md`
3. `Configuration.md`
4. `Authentication-and-Roles.md`
5. `Integrations.md`
6. `Track-Analysis.md`
7. `Artist-Suggestions-and-Lidarr-Activity.md`
8. `Discover.md`
9. `Smart-Playlists.md`
10. `History.md`
11. `Tracks.md`
12. `Blend.md`
13. `User-Profile.md`
14. `Troubleshooting.md`
15. `FAQ.md`

## Publish To GitHub Wiki

From repository root:

```bash
chmod +x scripts/publish-wiki.sh
scripts/publish-wiki.sh
```

If your `origin` remote is not the target repo:

```bash
scripts/publish-wiki.sh --repo owner/repo
```

Preview without pushing:

```bash
scripts/publish-wiki.sh --dry-run
```

The publish script copies pages from `docs/wiki/`, syncs screenshot assets from `docs/media/` into `media/`, and rewrites image paths for the GitHub wiki repo.
