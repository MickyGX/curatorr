# Folder browser (wizard Step 4 + global-playlist settings) truncates at ~ABBA in per-artist-folder libraries

**Parent:** [#141](https://github.com/MickyGX/curatorr/issues/141)
**Type:** AFK
**Status:** Ready to implement

## What to build

The folder browser used to scope playlist eligibility ("Include folders" / "Exclude folders") only lists folders derived from the first 2000 file paths in alphabetical order. In a library organised as one folder per artist with ~100 tracks each, those 2000 paths are exhausted within the first ~20 artists, so the root-level folder list dies around "ABBA" and every later artist (Anouk, BLØF, De Dijk, Frank Boeijen, Ilse DeLange, Nits, Racoon…) is unreachable.

**Root cause:** `getDistinctPathSegments` ([src/db.js:2238](src/db.js#L2238)) runs `SELECT DISTINCT file_path FROM master_tracks ... ORDER BY file_path LIMIT 2000`, then derives folder segments from that truncated row set. The cap is applied to **file paths (tracks)**, not to **folders** — so a library with more than 2000 tracks silently loses every folder past the cutoff. Both callers pass `limit: 2000`:

- Playlist wizard Step 4 — [src/routes/pages.js:2699](src/routes/pages.js#L2699) → embedded as `allPathSegments` in [src/views/playlists.ejs:1138](src/views/playlists.ejs#L1138), consumed by `wizardFolderChildren` ([src/views/playlists.ejs:5286](src/views/playlists.ejs#L5286)).
- Global-playlist settings browser — [src/routes/settings.js:675](src/routes/settings.js#L675).

Both UIs share the same function, so both are broken and both must be verified.

Fix by deriving distinct **directory** segments in SQL so the number of rows scales with the number of folders, not the number of tracks, and apply any cap to folder segments rather than to raw file paths. Root-level folder listing must always be complete A–Z.

**Constraint — do not block the event loop.** A prior incident (#134, scheduler healthcheck kills) came from synchronous work stalling the request loop. Do **not** "fix" this by dropping the LIMIT and reading every distinct `file_path` (potentially hundreds of thousands of rows) into JS on each page render. Push the dirname extraction into SQL (`DISTINCT` on the directory portion of `file_path`) so the query returns at most one row per folder.

## Acceptance criteria

- [ ] In a library with >2000 tracks organised as one folder per artist, the Step 4 "Include folders" browser lists **all** top-level artist folders A–Z — ABBA is no longer the last visible entry.
- [ ] The same behaviour is verified in the global-playlist folder browser under Settings (same underlying function).
- [ ] A folder that was previously visible (e.g. "10,000 Maniacs") still selects correctly, and a folder that was previously cut off (e.g. "Racoon") can now be added.
- [ ] Directory extraction happens in SQL; the number of rows read scales with folder count, not track count.
- [ ] Nested folders (deeper than one level) still resolve correctly and `hasChildren` remains accurate.
- [ ] The common-root-prefix stripping behaviour of `getDistinctPathSegments` is preserved (or intentionally revised with the change documented).
- [ ] Regression test uses a per-artist-folder dataset larger than the old 2000-track cap and asserts that late-alphabet folders are present in the output.
- [ ] Page render does not perform an unbounded synchronous scan of every track path for large libraries.

## Notes / implementation sketch

- SQLite dirname idiom (no `path` functions available): the directory of `file_path` is
  `substr(file_path, 1, length(file_path) - length(replace_after_last_separator...))`. Simpler and portable: compute distinct directories by stripping the trailing `/<filename>` in SQL, e.g. `rtrim(file_path, replace(file_path, '/', ''))` gives the path up to and including the last `/`. Group/DISTINCT on that, then do the (cheap, folder-count-bounded) segment-splitting in JS.
- Keep the `\\` → `/` normalisation and the common-root-depth trimming that already exists.
- If a cap is still desired as a safety valve, apply it to the distinct-directory count and log when it truncates (silent truncation is what caused this bug).

## Blocked by

None — can start immediately.
