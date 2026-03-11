# Discover

The Discover page lets you find and add artists that do not yet exist in your Plex library. It is separate from the [Artist Suggestions](Artist-Suggestions-and-Lidarr-Activity) panel, which only surfaces artists already in your library.

---

## Panels

### Trending Artists

Globally trending artists from Last.fm. These are artists getting the most plays worldwide right now and may have no overlap with your current library. Requires a Last.fm API key — see [Setup](#setup) below.

### Trending Tracks

Globally trending tracks from Last.fm, displayed alongside their artist. Same requirement as Trending Artists.

### Because You Like…

Similar artists to your top-played artists, sourced from Last.fm's similar-artist data. The panel title updates to reflect which of your top artists is being used as the seed (e.g. "Because You Like Arctic Monkeys"). This is personalised to your listening profile.

These can be artists completely outside your library. Clicking one opens the Lidarr lookup and album picker so you can add them.

### Manual Discovery

Search for any artist by name. Results come from a Lidarr lookup (MusicBrainz-backed), so you can find any artist regardless of library status. Each result shows whether the artist is already added to Lidarr.

Once you select an artist, an **Album Choice** panel appears where you can either:
- **Pick a specific album** — Curatorr adds the artist and monitors that album as the starter
- **Let Curatorr choose for you** — Curatorr picks based on your taste profile (greatest hits / highest-rated)

---

## Adding Artists

All adds from the Discover page are manual — you choose the artist and initiate the add. Curatorr then:

1. Adds the artist to Lidarr
2. Monitors the chosen or automatically selected starter album
3. Triggers a Lidarr search if auto-trigger is enabled in Lidarr settings
4. Tracks progress in the **Lidarr Activity** panel on the Artists page

Weekly role quotas apply (see [Roles and Permissions](Authentication-and-Roles#roles)). If your quota is full when you try to add, the request moves into your **Queue** automatically and is processed once quota resets.

---

## Queue

The Queue panel shows pending add requests that are waiting for quota. Items can be:
- **Reordered** by dragging — higher items are processed first
- **Removed** — removes the request from Curatorr's queue only, not from Lidarr or your library

---

## Added For You

The Added For You panel shows a history of all artists added through the Discover page for your account, including both manual adds and any automatic adds. Each entry shows the artist name, the album chosen, whether it was a manual or automatic add, and the current status.

---

## Setup

### Last.fm API Key (required for Trending and Similar Artist panels)

1. Create a free account at [last.fm](https://www.last.fm) if you do not have one
2. Go to your Last.fm account settings and create an API application to get a key
3. In Curatorr, go to **Settings → Discovery**
4. Enter your Last.fm API key
5. Toggle which panels you want to show: Trending Artists, Trending Tracks, Similar Artists

If no Last.fm key is configured, only the Manual Discovery panel is shown.

---

## What Discover Does Not Do

- **Automatic external adds** — Curatorr does not automatically add artists from Last.fm or any external source without you clicking. All Discover adds require a manual action.
- **Suggestions from external data** — the [Suggested Artists](Artist-Suggestions-and-Lidarr-Activity) panel on the Artists page is entirely separate and uses only your Plex library. External discovery lives here on the Discover page.

> **Note:** Automatic adding of library-based suggestions *is* available via **Settings → Lidarr → Automatically add top suggested artists to Lidarr**. This only applies to artists already in your Plex library that score highly enough — not to external Last.fm artists.
