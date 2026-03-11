# Smart Playlists

Curatorr builds and maintains personalised playlists directly in Plex, based on your real listening behaviour. These update automatically on a schedule and reflect your actual taste — not just your most-played tracks.

---

## Track Tiers

Every track in your library is classified into one of five tiers based on how you interact with it:

| Tier | How it is assigned |
|---|---|
| **Belter** | Played through to near completion (configurable threshold, default: last 30 seconds) |
| **Decent** | Played past 50% of the track length |
| **Half Decent** | Played less than 50% before moving on |
| **Skip** | Skipped within the skip threshold (default: 30 seconds from start) |
| **Curatorr** | Not yet classified — not enough data |

Tiers are updated in real time as Tautulli webhook events arrive. A track's tier can move up or down over time as you listen to it more.

---

## Track Tier Weights

Each tier carries a weight that feeds into the artist ranking score (out of 10, starting at 5):

| Tier | Default weight |
|---|---|
| Skip | −1 |
| Half Decent | −0.5 (derived: half of skip weight) |
| Decent | +0.5 (derived: half of belter weight) |
| Belter | +1 |

Half Decent and Decent weights are always derived from Skip and Belter — they cannot be set independently.

---

## Artist Ranking Score

Each artist has a ranking score between 0 and 10 (starting at 5). Every play adjusts the score based on the tier of the track played. The score reflects how well-regarded an artist is in your library overall:

- An artist trending towards **Belter** plays will score higher than 5
- An artist you frequently skip will score below 5
- When the ranking score falls below the **artist skip rank** threshold (default: 2), the artist is considered a weak signal
- When the ranking score exceeds the **artist belter rank** threshold (default: 8), the artist is considered a strong signal and is deprioritised in suggestions (already a proven favourite)

---

## Skip Streak

Curatorr tracks **consecutive skips** on individual tracks. This is separate from the general skip count.

- Once a track accumulates consecutive skips equal to the **song skip limit** (default: 2), it is automatically excluded from smart playlists
- The skip streak for each artist is visible in the Artists table
- You can reset the skip streak for an artist using the **Reset skips** button on the Artists page

---

## Playlist Presets

Three presets control the balance between playlist size and selectivity. A default preset can be set for new users in **Settings → Smart Playlists**.

| Preset | Character |
|---|---|
| **Cautious** | Larger playlist, wider range of artists — good for discovering new artists in your library |
| **Measured** | Balanced — a medium-sized playlist with a mix of proven and emerging artists |
| **Aggressive** | Smaller, tighter playlist — focused on artists you have clearly responded well to |

Existing users keep their chosen preset. Changing the default only affects new accounts.

---

## Daily Mix

Curatorr also builds a **Daily Mix** playlist once per day. This is separate from the main smart playlist and combines:

- Recent favourites (Belter and Decent tier tracks)
- Artist suggestions that match your taste profile
- Fresh tracks from your library that you have not played recently

The Daily Mix is designed for daily listening variety — it rotates more aggressively than the main smart playlist.

---

## Background Jobs

Smart playlists and the Daily Mix are maintained by background jobs. These run automatically and can also be triggered manually from **Settings → Jobs** (admin only).

| Job | Default interval | Description |
|---|---|---|
| Master Track Cache Refresh | Every 6 hours | Indexes all tracks from Plex, updating genres, ratings, and play counts in the local cache |
| Smart Playlist Sync | Every 30 minutes | Rebuilds each user's smart playlist and pushes it to Plex |
| Lidarr: Review Due Artists | Every 30 minutes | Reviews suggested artists and queues Lidarr actions for those that are due |
| Lidarr: Process Queued Requests | Every 20 minutes | Processes pending Lidarr add and monitor requests from the queue |
| Daily Mix Sync | Daily | Builds each user's Daily Mix playlist and syncs it to Plex |
| Tautulli History Sync | Daily | Fetches recent play history from Tautulli to backfill any missed webhook events |

Job intervals can be adjusted per job in **Settings → Jobs**.
