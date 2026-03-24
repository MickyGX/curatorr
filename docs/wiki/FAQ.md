# FAQ

---

**Does Curatorr work without Tautulli?**

Yes.

Plex webhooks can be the live playback source on their own. Tautulli is optional and is mainly useful for:

- live playback, if you choose it as the playback source
- gap-fill / backfill repair

---

**Does Curatorr require Lidarr?**

No. Lidarr is optional.

Core features like playback history, smart playlists, personal playlists, blended playlists, and track tiers work without Lidarr.

---

**Does Curatorr only use my Plex library?**

For scoring and smart playlists, yes.

External discovery is separate:

- `Artists` suggestions are library-based
- `Discover` can show external Last.fm-driven results
- `ListenBrainz` currently contributes playlist suggestions, not listening history

---

**Can I exclude a Plex library?**

Yes. Select only the music libraries Curatorr should monitor in `Settings -> Plex`.

If you later remove a library and save, Curatorr cleans its own derived data for that library.

---

**If I add a removed library back later, does Curatorr resync it?**

Yes, for future playback and for whatever history is still inside the active backfill window.

That is not the same as a full historical re-import of everything ever seen by Tautulli.

---

**Does ListenBrainz use MBID matching?**

Not yet.

Current playlist syncing uses artist + track-title matching, consistent with the existing Last.fm station approach.

---

**Can multiple users use Curatorr?**

Yes. Each user gets separate:

- play history
- smart playlist state
- user profile settings
- Last.fm / ListenBrainz settings
- Lidarr quota tracking

---

**Where is the database stored?**

Inside `DATA_DIR` as `curatorr.db`.

Back up `DATA_DIR` if you want to preserve history, stats, and user state.
