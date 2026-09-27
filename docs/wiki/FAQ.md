# FAQ

---

**Does Curatorr work without Tautulli?**

Yes.

Plex webhooks can be the live playback source on their own. Tautulli is optional and is mainly useful for:

- live playback, if you choose it as the playback source
- gap-fill / backfill repair

On Jellyfin and Emby installs, Tautulli is not used.

---

**Does Curatorr only work with Plex?**

No.

Curatorr supports `Plex`, `Jellyfin`, and `Emby` as its primary media server. Plex currently has the broadest feature set, including Tautulli support, Daily Mix, Last.fm station playlists, and ListenBrainz playlist suggestion sync.

---

**Does Curatorr require Lidarr?**

No. Lidarr is optional.

Core features like playback history, smart playlists, personal playlists, blended playlists, and track tiers work without Lidarr.

---

**Does Curatorr only use my media-server library?**

Exported smart playlists use tracks matched to your media-server library. Optional Music Assistant listening can also credit an artist when a track is unmatched, without adding that recording to the library.

External discovery is separate:

- `Discover → Artist Pipeline` combines catalog candidates and Last.fm similar-artist suggestions
- `Tracks` surfaces track and album suggestions from your library
- `ListenBrainz` currently contributes playlist suggestions, not listening history

---

**Can I exclude a library?**

Yes. Select only the music libraries Curatorr should monitor in `Settings -> Plex`, `Settings -> Jellyfin`, or `Settings -> Emby`.

If you later remove a library and save, Curatorr cleans its own derived data for that library.

---

**If I add a removed library back later, does Curatorr resync it?**

Yes, for future playback and for whatever history is still inside the active backfill window.

That is not the same as a full historical re-import of everything ever seen by Tautulli on Plex installs.

---

**Does ListenBrainz use MBID matching?**

Yes, when ListenBrainz provides recording MBIDs.

Curatorr prefers recording MBID matches first and falls back to artist + track-title matching when MBIDs are unavailable.

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

Also back up `CONFIG_PATH` for integration settings and listener mappings.

---

**Does Music Assistant replace Plex, Jellyfin, or Emby?**

No. It is an additional live play source. Your primary server still provides the library, authentication, and exported playlists. See [Music Assistant](Music-Assistant.md).

---

**Can imported playlists pick up music added later?**

Yes. Refresh the library cache, then use Refresh import or a daily, weekly, or monthly import schedule. M3U refresh uses the upload stored in Curatorr. See [Playlist Imports](Playlist-Imports.md).

---

**Why is a refreshed import smaller?**

From v0.1.99, named artists must match. A track that previously matched a same-titled song by another artist will now remain missing instead. Review the missing-source list and artist credits.

---

**Where did Suggested Artists and Added For You go?**

Use Discover's Artist Pipeline, Recently Requested, and Recently Added Albums. Artists now focuses on your artist listening statistics. See [Discover](Discover.md).
