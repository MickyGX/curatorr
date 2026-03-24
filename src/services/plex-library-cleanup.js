import {
  rebuildTrackStatsFromEvents,
  rebuildArtistStatsFromEvents,
  resolveUserSmartConfig,
} from '../db.js';

function uniqueStrings(values) {
  return [...new Set(
    values
      .map((value) => String(value || '').trim())
      .filter(Boolean),
  )];
}

export async function pruneDeselectedPlexLibraries(ctx, { removedLibraryKeys = [] } = {}) {
  const libraryKeys = uniqueStrings(removedLibraryKeys);
  if (!libraryKeys.length) {
    return {
      removedLibraryKeys: [],
      removedPlayEvents: 0,
      removedOpenSessions: 0,
      removedMasterTracks: 0,
      affectedUsers: [],
    };
  }

  const { db, loadConfig, pushLog, safeMessage } = ctx;
  const config = loadConfig();
  const placeholders = libraryKeys.map(() => '?').join(', ');

  try {
    const affectedTracks = db.prepare(`
      SELECT DISTINCT user_plex_id, plex_rating_key
      FROM play_events
      WHERE library_key IN (${placeholders})
    `).all(...libraryKeys);
    const affectedArtists = db.prepare(`
      SELECT DISTINCT user_plex_id, artist_name
      FROM play_events
      WHERE library_key IN (${placeholders})
        AND TRIM(COALESCE(artist_name, '')) != ''
    `).all(...libraryKeys);
    const affectedUsers = uniqueStrings(affectedTracks.map((row) => row.user_plex_id));

    const removedOpenSessions = db.prepare(`DELETE FROM open_sessions WHERE library_key IN (${placeholders})`).run(...libraryKeys).changes;
    const removedPlayEvents = db.prepare(`DELETE FROM play_events WHERE library_key IN (${placeholders})`).run(...libraryKeys).changes;
    const removedMasterTracks = db.prepare(`DELETE FROM master_tracks WHERE library_key IN (${placeholders})`).run(...libraryKeys).changes;

    for (const row of affectedTracks) {
      const userPlexId = String(row.user_plex_id || '').trim();
      const plexRatingKey = String(row.plex_rating_key || '').trim();
      if (!userPlexId || !plexRatingKey) continue;

      const remaining = Number(
        db.prepare('SELECT COUNT(*) AS n FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ?')
          .get(userPlexId, plexRatingKey)?.n || 0,
      );
      if (!remaining) {
        db.prepare('DELETE FROM track_stats WHERE user_plex_id = ? AND plex_rating_key = ?').run(userPlexId, plexRatingKey);
        continue;
      }

      const smartConfig = resolveUserSmartConfig(db, config, userPlexId);
      const songSkipLimit = Number(smartConfig.songSkipLimit) || 3;
      rebuildTrackStatsFromEvents(db, {
        userPlexId,
        plexRatingKey,
        songSkipLimit,
        smartConfig,
      });
    }

    for (const row of affectedArtists) {
      const userPlexId = String(row.user_plex_id || '').trim();
      const artistName = String(row.artist_name || '').trim();
      if (!userPlexId || !artistName) continue;

      const remaining = Number(
        db.prepare('SELECT COUNT(*) AS n FROM play_events WHERE user_plex_id = ? AND artist_name = ?')
          .get(userPlexId, artistName)?.n || 0,
      );
      if (!remaining) {
        db.prepare('DELETE FROM artist_stats WHERE user_plex_id = ? AND artist_name = ?').run(userPlexId, artistName);
        continue;
      }

      const smartConfig = resolveUserSmartConfig(db, config, userPlexId);
      rebuildArtistStatsFromEvents(db, { userPlexId, artistName, smartConfig });
    }

    if (affectedUsers.length) {
      const { rebuildSmartPlaylist } = await import('../routes/api-music.js');
      await Promise.allSettled(
        affectedUsers.map((userPlexId) => rebuildSmartPlaylist(ctx, userPlexId)),
      );
    }

    pushLog({
      level: 'info',
      app: 'settings',
      action: 'plex.libraries.cleaned',
      message: `Cleaned data for ${libraryKeys.length} removed Plex librar${libraryKeys.length === 1 ? 'y' : 'ies'}.`,
      meta: {
        removedLibraryKeys: libraryKeys,
        removedPlayEvents,
        removedOpenSessions,
        removedMasterTracks,
        affectedUsers,
      },
    });

    return {
      removedLibraryKeys: libraryKeys,
      removedPlayEvents,
      removedOpenSessions,
      removedMasterTracks,
      affectedUsers,
    };
  } catch (err) {
    pushLog({
      level: 'error',
      app: 'settings',
      action: 'plex.libraries.cleaned.error',
      message: safeMessage(err),
      meta: { removedLibraryKeys: libraryKeys },
    });
    throw err;
  }
}
