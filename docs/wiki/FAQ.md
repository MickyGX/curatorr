# FAQ

---

**Does Curatorr work without Tautulli?**

No. Tautulli is the source of real-time playback data. Without it, Curatorr has no way to know what you have played or skipped, so smart playlists and suggestions will not work. Tautulli is free and straightforward to set up — see [Integrations](Integrations#tautulli).

---

**Does Curatorr require Lidarr?**

No. Lidarr is entirely optional. All core features (smart playlists, track tiers, artist suggestions, play history) work without it. Lidarr is only needed if you want Curatorr to automatically add suggested artists to your music library.

---

**Does Curatorr pull in artists from outside my Plex library?**

It depends on which feature you are using:

- **Suggested Artists (Artists page)** — library-only. These are artists already in your Plex library that you have under-explored. No external data sources are involved.
- **Discover page** — yes, this goes beyond your library. If you have a Last.fm API key configured, the Discover page shows **Trending Artists** (globally trending on Last.fm) and **Because You Like…** (similar artists to your top artists). These can be artists that do not exist in your library at all. You can then manually add any of them to Lidarr from that page.
- **Manual Discovery (Discover page)** — you can also search for any artist by name and add them to Lidarr directly, regardless of whether they are in your library.

**Automatic** adding of library-based suggestions is available via **Settings → Lidarr → Automatically add top suggested artists to Lidarr**. When enabled, Curatorr will automatically queue the highest-scoring artist suggestion for each eligible user during the Lidarr review job (every 30 minutes). Artists must score 7.0 or above. Weekly role quotas apply — if quota is full, the artist moves into the queue for later.

Automatic adding from external sources (Last.fm trending/similar artists) is not currently implemented. External discovery still requires a manual add action from the Discover page.

---

**Why is an artist not appearing in suggestions?**

Several things can prevent an artist from appearing:

- They are already in your Lidarr library (they appear in Lidarr Activity instead)
- You have manually excluded them
- They are in your liked artists list
- They have more than 12 plays and a ranking score of 7 or above (considered an established favourite)
- Their genre affinity score is too low relative to your taste profile
- Their combined score is 0.5 or below after all factors are applied

---

**How often do suggestions update?**

Suggestions are rebuilt each time you load the Artists page. If your listening profile has changed significantly since the last load, the list will reflect that. Suggestions are also rebuilt periodically as part of the Smart Playlist Sync job (every 30 minutes by default).

---

**What happens to the weekly Lidarr quota?**

Quotas reset on a rolling 7-day window. Adding an artist consumes one artist quota slot; the starter album and any subsequent albums consume album quota slots. When a quota is reached, new suggestions show a **Quota blocked** status and are queued automatically for when the quota resets.

---

**Can multiple users use Curatorr?**

Yes. Each user who signs in with their Plex account gets their own taste profile, smart playlist, track stats, and suggestion list. Lidarr automation quotas are also tracked per user.

---

**Why does the smart playlist look the same as before?**

The Smart Playlist Sync runs every 30 minutes by default. If you want an immediate update, go to **Settings → Jobs** and manually trigger the **Smart Playlist Sync** job.

---

**Can I use Curatorr behind a reverse proxy?**

Yes. Set `TRUST_PROXY=true` and `TRUST_PROXY_HOPS=1` in your compose environment. For HTTPS, also set `COOKIE_SECURE=true`. See the [Traefik example in the README](../README.md#traefik-setup) for a working compose configuration.

---

**Can I embed Curatorr in Launcharr or another dashboard?**

Yes. Append `?embed=launcharr` to any Curatorr page URL to remove the sidebar and chrome, making it suitable for iframes. If the embedding dashboard is on a different origin, add it to `EMBED_ALLOWED_ORIGINS` in your compose environment. See [Embed Mode in the README](../README.md#embed-mode).

---

**How do I reset a track or artist's skip data?**

On the Artists page, any artist with a skip streak above 0 has a **Reset skips** button in the Actions column. This clears the consecutive skip counter for all of that artist's tracks, allowing them back into the smart playlist.

---

**Where is the database stored?**

In the `DATA_DIR` directory inside the container, which maps to `./data` in the default compose file. The file is `curatorr.db` (SQLite). Back this up regularly.
