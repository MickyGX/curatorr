# "Lidarr: Retry Failed Requests" job permanently wedges when duplicate failed rows pile up

**Parent:** [#142](https://github.com/MickyGX/curatorr/issues/142)
**Type:** AFK
**Status:** Ready to implement

## Error

```
UNIQUE constraint failed: lidarr_requests.user_plex_id, lidarr_requests.artist_name,
lidarr_requests.album_title, lidarr_requests.status
```

## What to build

The `lidarrRetryFailed` scheduled job re-queues `failed` Lidarr requests. When a user accumulates **multiple `failed` rows for the same artist/album** (e.g. 28 duplicates built up during a ~36-hour Lidarr outage), the job promotes the first failed row to `queued` successfully, then throws `UNIQUE constraint failed` when it tries to promote the second — because the partial unique index `idx_lidarr_requests_user_active` allows only one `queued` (and one `processing`) row per `(user, artist, album)`. With no try/catch, the exception aborts the entire job, so it never runs to completion again until the DB is manually cleaned.

Three root causes, all addressed:

1. **Retry job has no error isolation and no dedup.** It blindly transitions every failed row to `queued`. → Make it dedup-aware and constraint-safe: group failed rows per artist/album, keep one survivor, drain the redundant duplicates, only promote when no active row already exists and the survivor is under the retry limit, and isolate each group so one failure can't abort the pass. The stuck-`processing` reset loop is likewise wrapped so a collision drains the dupe instead of throwing.

2. **Duplicates accumulate at the source.** `enqueueLidarrRequest` only dedups against `queued`/`processing` rows, never `failed` ones, so every re-request while an item is failed inserts another `failed` row. → On enqueue, when there is no active row, revive the most recent `failed` row for that artist/album (back to `queued`) and drain older failed dupes instead of inserting a new row.

3. **No safety valve when a collision does occur.** → Constraint errors during retry are caught, logged, and the offending duplicate is drained rather than propagated.

## Acceptance criteria

- [ ] With N (>1) duplicate `failed` rows for the same user/artist/album, the retry pass completes without throwing, promotes exactly **one** row to `queued`, and drains the rest.
- [ ] Failed rows at or above the retry limit (`MAX_RETRIES = 3`) are left as `failed` (not re-queued), and their duplicates are still drained.
- [ ] A failed row whose artist/album already has an active `queued`/`processing` row is drained, not promoted (no collision).
- [ ] `enqueueLidarrRequest` no longer creates a second row when a `failed` row already exists for the same artist/album — it revives one and drains extras.
- [ ] The stuck-`processing` reset path no longer throws when a `queued` row already exists for the same artist/album.
- [ ] Existing `lidarr_requests` behaviour (remove, reorder, pipeline reset) is unchanged; full test suite passes.
- [ ] Regression test manufactures the exact 28-duplicate stuck state and asserts the job drains and re-queues cleanly.

## Blocked by

None — can start immediately.
