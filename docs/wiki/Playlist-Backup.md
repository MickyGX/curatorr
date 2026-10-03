# Playlist Backup and Restore

Curatorr can export every playlist it manages to a backup file and rebuild those playlists later, on the same server or a new one. It can also keep copies of Plex playlists that Curatorr does not manage, so they can be recreated if Plex loses them.

Open **Playlists → Back up and restore playlists** (the box icon next to **Import playlist**).

## What a backup contains

A backup is a `.curatorr.json` file. Each playlist type is saved the way Curatorr needs to rebuild it:

| Playlist | Saved | Restored as |
|---|---|---|
| Smart (personal) | Rules, track filters, rebuild schedule, artwork, and the current track list | A smart playlist that rebuilds from its rules against the current library |
| Global (admins) | Rules, track filters, blend users, and artwork | A global playlist, synced to every user |
| Imported | Source details (Plex, Spotify, TIDAL, M3U, and so on), refresh schedule, track list, missing tracks, and artwork | An imported playlist that keeps its source, so **Refresh import** still works where the source exists |
| Static | Track list and artwork | A playlist restored from the backup's tracks |
| System (Crescive, Curative, Curatorr, Daily Mix, Last.fm, ListenBrainz) | Custom name and artwork | The user's existing system playlist gets its name and artwork back; its tracks regenerate from the user's settings |

The **Include system playlist settings** option also saves each user's liked and ignored genres and artists, smart playlist tuning, and Last.fm and ListenBrainz playlist choices.

Each saved track records its artist, title, album, duration, file path, and MusicBrainz recording ID where known. This is how Curatorr finds the track again when the media server gives it a new ID.

Backups never contain passwords, tokens, or API keys. They do contain usernames, playlist names, and file paths, so store them privately.

## Export

- **Everything:** on the **Export** tab, select **Download backup**. Turn off **Include artwork** for a smaller file.
- **One playlist:** use **Export backup** in a playlist card's options menu.
- **Several playlists:** select cards with their checkboxes, then **Export Selected**.
- **Every user (admins):** set **Playlists to include** to **Every user's playlists and global playlists**. Admin exports always include global playlist definitions.
- **M3U:** use **Export M3U** in a card's options menu for a playlist file that other players can read. M3U files hold the track list only (paths with artist and title). Use a Curatorr backup to keep smart playlist rules and settings.

## Restore

1. Make sure the library is loaded: **Settings → Jobs → Master Track Cache Refresh** must have completed against the server you are restoring to.
2. Open the **Restore** tab and choose the backup file.
3. Review the preview. Imported and static playlists show how many tracks were found in your library. Smart playlists rebuild from their rules, so they show no match count.
4. Choose what happens when a playlist name is already taken: restore with **(restored)** added, or skip it.
5. Untick anything you do not want, then select **Restore**.

Curatorr writes the playlists straight away and creates them in your media server in the background.

- **Missing tracks:** tracks that are not in your library go to the playlist's **Missing from source** list. You can send them to Lidarr from there. Refreshing a restored static playlist rematches its tracks and missing list against the library and moves newly found tracks back into their original positions.
- **Disabled and backup-only playlists** are restored without being created in the media server.
- **Owners (admins):** a backup containing other users' playlists can go to **Each playlist's original user** or to **My account**. A playlist for a user who has not signed in to Curatorr yet appears when they do. Regular users always restore into their own account.
- **Global playlists:** these restore as global for admins. For regular users, they restore as a personal smart playlist with the same rules.
- **System playlists** need the user to have finished setup first, so the playlists exist. Restore again afterwards if they were skipped.
- **Versions:** backups record their format version. A backup made by a newer Curatorr version than the one installed is refused, with a prompt to update.

## Back up Plex playlists

The **Plex playlists** tab lists Plex playlists that Curatorr does not manage, such as playlists made in Plexamp. Select the ones to keep, then select **Back up**.

- **Backup only:** Curatorr stores each playlist's tracks without adding a second copy to Plex. The card says **Backup only, not in Plex**.
- **Kept current:** backups refresh weekly while the Plex playlist exists. Change the schedule in the card's **Edit** dialog.
- **Included in backup files:** Curatorr backup files include these playlists.
- **Recovering a lost playlist:** use **Restore to Plex** on its card to create it in Plex from Curatorr's copy.

Backing up a playlist that is already saved refreshes the saved copy.

## Rebuilding playlists after a new Plex install

Plex gives every track a new ID when its database is rebuilt or the server is reinstalled. Curatorr keeps each playlist track's identity beside the Plex ID, so it can find the same recordings again.

**If Curatorr's data survived** (the same `DATA_DIR`):

1. Point Curatorr at the new Plex server and select its music libraries in **Settings → Plex**.
2. Run **Master Track Cache Refresh**.
3. Curatorr re-matches playlist tracks to their new IDs and re-syncs affected imported and static playlists. Smart and system playlists rebuild on their next scheduled run, or use **Rebuild** on their cards. Use **Restore to Plex** on any backup-only playlist you want back.

**If Curatorr was reinstalled too:**

1. Complete Curatorr setup against the new server and let the master track cache refresh finish.
2. Have each user sign in and finish user setup, so their system playlists exist.
3. Restore your backup file.

Keep a recent backup file somewhere other than the Plex and Curatorr host. Back up `DATA_DIR` as well to keep listening history; see [Configuration](Configuration.md#data-storage).

## How tracks are matched

Curatorr looks for each track in this order:

1. The same file path.
2. The same MusicBrainz recording ID. If several copies share it, the one from the same album is preferred.
3. A unique file with the same name and a matching title.
4. Artist and title, preferring the same album and then the closest duration.

A named artist must match, so a same-titled song by a different artist is left unmatched.

When a track's ID disappears during a library refresh, Curatorr re-matches it with these steps. If it cannot, an imported or static playlist moves the track to its missing list, and a generated playlist drops it. A track Curatorr has never cached, such as one from a library not selected in Curatorr, is left as it is.

## Large backups

Restore requests accept backup files up to 64 MB. Embedded artwork is the usual reason for a large file. Export without artwork, or raise `PLAYLIST_BACKUP_BODY_LIMIT` on the container (for example `128mb`).

See [Smart Playlists](Smart-Playlists.md) and [Playlist Imports](Playlist-Imports.md) for day-to-day playlist management.
