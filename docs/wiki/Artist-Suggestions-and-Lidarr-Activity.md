# Artist Suggestions and Lidarr Activity

The Artists page contains two panels that work together to surface under-explored artists from your library and track what Curatorr has done with Lidarr.

---

## Suggested Artists

This panel shows artists **already in your Plex library** that you have not given much attention to, ranked by how well they match your current taste profile.

Only artists that are **not yet in Lidarr** appear here. If an artist is already in your Lidarr library, they move to the Lidarr Activity panel instead.

### How suggestions are built

**Step 1 — Build your taste profile**

Curatorr reads your listening history and preferences:

- Your **top artists** by ranking score and play count
- Your **recently played tracks** (last 25) and their tiers (Belter, Decent, Half Decent, Skip)
- Your manually **liked and ignored** genres and artists (set in your user settings)

**Step 2 — Build genre affinity**

A weighted score is calculated for every genre in your library:

| Signal | Weight |
|---|---|
| Manually liked genre | +4 |
| Manually ignored genre | −5 |
| Genre of a top artist (scales with rank + plays) | up to +8 |
| Genre of a recent Belter track | +3.5 |
| Genre of a recent Half Decent track | +2.25 |
| Genre of a recent Decent track | +1.25 |
| Genre of a recent Skip track | −2.5 |

**Step 3 — Score every library artist**

Three components combine into a `totalScore` for each candidate artist:

**Genre Score** — how well the artist's genres match your affinity. The top 3 matching genre weights are summed.

**Behaviour Score** — based on how much you have actually listened to them:

| Condition | Points |
|---|---|
| Never played | +4 |
| 1–2 plays | +2.75 |
| 3–5 plays | +1.5 |
| 6+ plays | gradually decreasing |
| Not played in 30+ days | +1.5 |
| Not played in 90+ days | additional +1 |
| High ranking score (above 3) | small positive bonus |
| Each skip on record | −0.5 |

**Editorial Score** — library signals about the artist:

| Condition | Points |
|---|---|
| Already a top artist for you | −3 |
| Has 2 or more albums in your library | +0.75 |
| Has 8 or more tracks in your library | +0.5 |
| Any genre appears in your liked genres list | +1 |

**Filtering:**

- Artists with a `totalScore` of 0.5 or below are dropped
- Artists you have manually excluded are skipped
- Artists already in your liked artists list are skipped
- Artists with 12+ plays **and** a ranking score of 7 or above are skipped (they are established favourites, not undiscovered gems)

**Result:** The top 12 by `totalScore` are displayed. Each entry shows:
- Artist thumbnail
- Artist name
- Top 3 matching genres and album count as a subtitle
- The combined `totalScore` (labelled "Score")
- The current Lidarr status badge
- An **Add to Lidarr** button (if Lidarr automation is enabled and the artist is not yet in Lidarr)

### Quota chips

If Lidarr is connected, the panel header shows your current weekly quota usage, for example **Artists 1/3** and **Albums 2/6**. When a quota is reached, the Add to Lidarr button is replaced with a **Quota blocked** message.

---

## Lidarr Activity

This panel shows what Curatorr has actually done with Lidarr — artists added, search progress, download status, quota blocks, and progression stages.

### What appears here

An artist appears in Lidarr Activity when any of the following are true:

- It is already in your Lidarr library (Curatorr checks this via a live Lidarr API call on each page load)
- Curatorr has a local progress record for it (i.e. it was previously acted on)
- Its suggestion status is anything other than plain `Suggested` — for example it is `Added`, `Queued`, or `Quota blocked`

Artists that are purely `Suggested` and not yet in Lidarr appear in the Suggested Artists panel above, not here.

The panel shows up to **8 entries**, sorted by most recently updated. The count chips in the header (e.g. **3 Downloaded**) tally how many entries carry each status label.

### Status labels

| Status | What it means |
|---|---|
| **Suggested** | Artist was scored and surfaced but no action has been taken |
| **Queued** | Artist is in the queue, waiting for quota to free up before being added |
| **Quota blocked** | Weekly artist or album limit has been reached; no further adds until quota resets |
| **Artist added** | Artist has been added to Lidarr |
| **Starter album added** | Artist is in Lidarr and a starter album has been monitored |
| **Starter album linked** | A starter album in Lidarr was identified but no new album was added |
| **Search queued** | Lidarr has queued an `AlbumSearch` command |
| **Search running** | Lidarr is actively executing the search |
| **Search complete** | Search finished and track files were found |
| **Search finished** | Search finished but no files were found yet |
| **Search failed** | Lidarr search command returned an error |
| **Downloaded** | Track files exist in Lidarr for this artist |
| **Next album added** | A subsequent album has been unlocked and added based on listening progress |
| **Awaiting belter** | Curatorr is waiting for a stronger listening signal (a Belter-tier play) before unlocking the next album |
| **Catalog complete** | No further album unlocks are pending for this artist |

### Review now button

If an artist is reviewable (e.g. enough time has passed since the last evaluation), a **Review now** button appears. Clicking it triggers an immediate re-evaluation of the artist's Lidarr progression, rather than waiting for the next scheduled job.
