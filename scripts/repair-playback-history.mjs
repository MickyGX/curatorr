import fs from 'fs';
import path from 'path';
import Database from 'better-sqlite3';

const PRESET_VALUES = {
  cautious: { skipThresholdSeconds: 20, completionThresholdSeconds: 20, skipWeight: -0.5, belterWeight: 0.5, artistSkipRank: 1, artistBelterRank: 9, songSkipLimit: 3 },
  measured: { skipThresholdSeconds: 30, completionThresholdSeconds: 30, skipWeight: -1, belterWeight: 1, artistSkipRank: 2, artistBelterRank: 8, songSkipLimit: 2 },
  aggressive: { skipThresholdSeconds: 40, completionThresholdSeconds: 40, skipWeight: -1.5, belterWeight: 1.5, artistSkipRank: 3, artistBelterRank: 7, songSkipLimit: 1 },
};

function parseArgs(argv) {
  const args = {
    db: '/app/data/curatorr.db',
    config: '/app/config/config.json',
    users: [],
    lookbackHours: 24 * 365 * 3,
    skipTautulli: false,
    tautulliTimeoutMs: 10000,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--db') args.db = String(argv[++i] || args.db);
    else if (arg === '--config') args.config = String(argv[++i] || args.config);
    else if (arg === '--users') {
      args.users = String(argv[++i] || '')
        .split(',')
        .map((v) => v.trim())
        .filter(Boolean);
    } else if (arg === '--lookback-hours') {
      const n = Number(argv[++i] || args.lookbackHours);
      if (Number.isFinite(n) && n > 0) args.lookbackHours = Math.floor(n);
    } else if (arg === '--skip-tautulli') {
      args.skipTautulli = true;
    } else if (arg === '--tautulli-timeout-ms') {
      const n = Number(argv[++i] || args.tautulliTimeoutMs);
      if (Number.isFinite(n) && n > 0) args.tautulliTimeoutMs = Math.floor(n);
    }
  }
  if (!args.users.length) {
    throw new Error('Missing required --users argument');
  }
  return args;
}

function loadConfig(configPath) {
  return JSON.parse(fs.readFileSync(configPath, 'utf8'));
}

function parseSmartConfig(value) {
  try {
    return JSON.parse(String(value || 'null')) || null;
  } catch {
    return null;
  }
}

function resolveUserSmartConfig(db, config, userId) {
  const row = db.prepare('SELECT smart_config FROM user_preferences WHERE user_plex_id = ?').get(userId);
  const smartConfig = parseSmartConfig(row?.smart_config);
  const userPreset = smartConfig?.preset;
  if (userPreset && PRESET_VALUES[userPreset]) return { ...PRESET_VALUES[userPreset] };
  const adminPreset = config?.smartPlaylist?.defaultPreset;
  if (adminPreset && PRESET_VALUES[adminPreset]) return { ...PRESET_VALUES[adminPreset] };
  return { ...(config?.smartPlaylist || {}) };
}

function classifyTier(listenedMs, trackDurationMs, smartConfig) {
  const skipMs = (Number(smartConfig.skipThresholdSeconds) || 30) * 1000;
  const completionMs = (Number(smartConfig.completionThresholdSeconds) || 30) * 1000;
  if (listenedMs < skipMs) return 'skip';
  if (trackDurationMs > 0 && listenedMs >= trackDurationMs - completionMs) return 'belter';
  if (trackDurationMs > 0 && listenedMs >= trackDurationMs * 0.5) return 'decent';
  return 'half-decent';
}

function isSkipEvent(listenedMs, trackDurationMs, smartConfig) {
  const skipMs = (Number(smartConfig.skipThresholdSeconds) || 30) * 1000;
  return trackDurationMs > 0 && listenedMs < skipMs;
}

function deriveTierFromEvent(listenedMs, trackDurationMs, smartConfig, eventIsSkip) {
  if (eventIsSkip) return 'skip';
  if (trackDurationMs > 0) return classifyTier(listenedMs, trackDurationMs, smartConfig);
  return 'decent';
}

function tierWeight(tier, smartConfig) {
  const skipWeight = Number(smartConfig.skipWeight) || -1;
  const belterWeight = Number(smartConfig.belterWeight) || 1;
  switch (tier) {
    case 'skip': return skipWeight;
    case 'half-decent': return skipWeight / 2;
    case 'decent': return belterWeight / 2;
    case 'belter': return belterWeight;
    default: return 0;
  }
}

function clampScore(value) {
  return Math.min(10, Math.max(0, value));
}

function eventTime(row) {
  return Number(row?.ended_at || row?.started_at || Date.now());
}

function findExistingPlay(db, userPlexId, plexRatingKey, startedAtMs) {
  return db.prepare(`
    SELECT id, duration_ms, ended_at, track_duration_ms
    FROM play_events
    WHERE user_plex_id = ? AND plex_rating_key = ?
      AND started_at BETWEEN ? AND ?
    LIMIT 1
  `).get(userPlexId, plexRatingKey, startedAtMs - 90_000, startedAtMs + 90_000);
}

function findExistingPlayByStop(db, userPlexId, plexRatingKey, stoppedAtMs) {
  return db.prepare(`
    SELECT id, duration_ms, ended_at, track_duration_ms
    FROM play_events
    WHERE user_plex_id = ? AND plex_rating_key = ?
      AND ended_at BETWEEN ? AND ?
    ORDER BY ABS(ended_at - ?) ASC
    LIMIT 1
  `).get(userPlexId, plexRatingKey, stoppedAtMs - 120_000, stoppedAtMs + 120_000, stoppedAtMs);
}

function inferHistoryTrackDurationMs(listenedMs, row) {
  const fullDurationMs = Number(row.full_duration || 0) * 1000;
  if (fullDurationMs > 0) return fullDurationMs;
  const pct = Number(row.percent_complete || 0);
  if (listenedMs > 0 && pct > 0 && pct <= 100) {
    return Math.round(listenedMs / (pct / 100));
  }
  return 0;
}

function chooseTrackDurationMs(existingTrackDurationMs, inferredTrackDurationMs) {
  const current = Number(existingTrackDurationMs || 0);
  const inferred = Number(inferredTrackDurationMs || 0);
  if (current > 0 && inferred > 0) return Math.min(current, inferred);
  return inferred || current || 0;
}

function removeNearDuplicateUnknownDurationRows(db, users) {
  const placeholders = users.map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT id, user_plex_id, plex_rating_key, started_at, ended_at, duration_ms, track_duration_ms
    FROM play_events
    WHERE user_plex_id IN (${placeholders})
    ORDER BY user_plex_id, plex_rating_key, started_at ASC, id ASC
  `).all(...users);
  const toDelete = new Set();
  for (let i = 0; i < rows.length; i += 1) {
    const current = rows[i];
    if (toDelete.has(current.id)) continue;
    for (let j = i + 1; j < rows.length; j += 1) {
      const candidate = rows[j];
      if (candidate.user_plex_id !== current.user_plex_id || candidate.plex_rating_key !== current.plex_rating_key) break;
      if (Math.abs(Number(candidate.started_at || 0) - Number(current.started_at || 0)) > 120_000) break;
      const currentKnown = Number(current.track_duration_ms || 0) > 0;
      const candidateKnown = Number(candidate.track_duration_ms || 0) > 0;
      if (currentKnown === candidateKnown) continue;
      const keep = currentKnown ? current : candidate;
      const drop = currentKnown ? candidate : current;
      if (Math.abs(Number((keep.ended_at || keep.started_at) || 0) - Number((drop.ended_at || drop.started_at) || 0)) > 180_000) continue;
      toDelete.add(drop.id);
    }
  }
  for (const id of toDelete) {
    db.prepare('DELETE FROM play_events WHERE id = ?').run(id);
  }
  return toDelete.size;
}

function removeOverlappingShorterRows(db, users) {
  const placeholders = users.map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT id, user_plex_id, plex_rating_key, started_at, ended_at, duration_ms, track_duration_ms, event_source
    FROM play_events
    WHERE user_plex_id IN (${placeholders})
    ORDER BY user_plex_id, plex_rating_key, started_at ASC, id ASC
  `).all(...users);
  const toDelete = new Set();
  for (let i = 0; i < rows.length; i += 1) {
    const first = rows[i];
    if (toDelete.has(first.id)) continue;
    for (let j = i + 1; j < rows.length; j += 1) {
      const second = rows[j];
      if (second.user_plex_id !== first.user_plex_id || second.plex_rating_key !== first.plex_rating_key) break;
      if (Number(second.started_at || 0) - Number(first.started_at || 0) > 30 * 60_000) break;
      const overlapMs = Number(first.ended_at || 0) - Number(second.started_at || 0);
      if (overlapMs < 0 || overlapMs > 60_000) continue;
      const firstKnown = Number(first.track_duration_ms || 0) > 0;
      const secondKnown = Number(second.track_duration_ms || 0) > 0;
      if (!firstKnown && !secondKnown) continue;
      const shorter = Number(first.duration_ms || 0) <= Number(second.duration_ms || 0) ? first : second;
      const longer = shorter.id === first.id ? second : first;
      if (Number(longer.duration_ms || 0) <= Number(shorter.duration_ms || 0)) continue;
      if (Math.abs(Number(longer.ended_at || 0) - Number(shorter.ended_at || 0)) > 180_000) continue;
      toDelete.add(shorter.id);
    }
  }
  for (const id of toDelete) {
    db.prepare('DELETE FROM play_events WHERE id = ?').run(id);
  }
  return toDelete.size;
}

function findResumedPlay(db, userPlexId, plexRatingKey, startedAtMs) {
  return db.prepare(`
    SELECT id, duration_ms
    FROM play_events
    WHERE user_plex_id = ? AND plex_rating_key = ?
      AND ended_at BETWEEN ? AND ?
    ORDER BY ended_at DESC
    LIMIT 1
  `).get(userPlexId, plexRatingKey, startedAtMs - 300_000, startedAtMs + 30_000);
}

async function fetchTautulliRows(config, targetUsers, afterTs, timeoutMs) {
  const tautulliUrl = String(config?.tautulli?.url || '').trim();
  const apiKey = String(config?.tautulli?.apiKey || '').trim();
  if (!tautulliUrl || !apiKey) {
    throw new Error('Tautulli is not configured');
  }

  const api = `${tautulliUrl.replace(/\/$/, '')}/api/v2`;
  const users = new Set(targetUsers);
  const rows = [];
  let start = 0;
  const pageSize = 100;

  while (true) {
    const body = new URLSearchParams({
      apikey: apiKey,
      cmd: 'get_history',
      media_type: 'track',
      length: String(pageSize),
      start: String(start),
      order_column: 'date',
      order_dir: 'desc',
      after: String(afterTs),
    });
    const res = await fetch(api, {
      method: 'POST',
      headers: { Accept: 'application/json' },
      body,
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!res.ok) {
      throw new Error(`Tautulli request failed (${res.status})`);
    }
    const json = await res.json();
    const batch = Array.isArray(json?.response?.data?.data) ? json.response.data.data : [];
    for (const row of batch) {
      const userPlexId = String(row.user || row.user_id || '').trim();
      if (users.has(userPlexId)) rows.push(row);
    }
    start += pageSize;
    if (batch.length < pageSize) break;
  }

  rows.sort((a, b) => Number(a.started || 0) - Number(b.started || 0));
  return rows;
}

function collectManualTrackRows(db, users) {
  const placeholders = users.map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT user_plex_id, plex_rating_key, track_title, artist_name, album_name,
           manually_excluded, manually_included
    FROM track_stats
    WHERE user_plex_id IN (${placeholders})
  `).all(...users);
  return new Map(rows.map((row) => [`${row.user_plex_id}\u0000${row.plex_rating_key}`, row]));
}

function collectManualArtistRows(db, users) {
  const placeholders = users.map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT user_plex_id, artist_name, manually_excluded, manually_included
    FROM artist_stats
    WHERE user_plex_id IN (${placeholders})
  `).all(...users);
  return new Map(rows.map((row) => [`${row.user_plex_id}\u0000${row.artist_name}`, row]));
}

function rebuildStatsForUsers(db, config, users) {
  const manualTrackRows = collectManualTrackRows(db, users);
  const manualArtistRows = collectManualArtistRows(db, users);
  const placeholders = users.map(() => '?').join(',');
  const events = db.prepare(`
    SELECT id, user_plex_id, plex_rating_key, track_title, artist_name, album_name,
           started_at, ended_at, duration_ms, track_duration_ms
    FROM play_events
    WHERE user_plex_id IN (${placeholders})
    ORDER BY started_at ASC, id ASC
  `).all(...users);
  const smartConfigByUser = new Map(users.map((user) => [user, resolveUserSmartConfig(db, config, user)]));
  const trackMap = new Map();
  const artistMap = new Map();

  db.prepare(`DELETE FROM track_stats WHERE user_plex_id IN (${placeholders})`).run(...users);
  db.prepare(`DELETE FROM artist_stats WHERE user_plex_id IN (${placeholders})`).run(...users);

  for (const event of events) {
    const userPlexId = String(event.user_plex_id || '').trim();
    const artistName = String(event.artist_name || '').trim();
    const plexRatingKey = String(event.plex_rating_key || '').trim();
    if (!userPlexId || !artistName || !plexRatingKey) continue;

    const smartConfig = smartConfigByUser.get(userPlexId) || {};
    const songSkipLimit = Number(smartConfig.songSkipLimit) || 3;
    const eventIsSkip = Number(event.is_skip || 0) === 1;
    const tier = deriveTierFromEvent(
      Number(event.duration_ms || 0),
      Number(event.track_duration_ms || 0),
      smartConfig,
      eventIsSkip,
    );
    const weight = tierWeight(tier, smartConfig);
    const isSkip = eventIsSkip;
    const trackKey = `${userPlexId}\u0000${plexRatingKey}`;
    const manualTrack = manualTrackRows.get(trackKey);
    let track = trackMap.get(trackKey);
    if (!track) {
      track = {
        userPlexId,
        plexRatingKey,
        trackTitle: String(manualTrack?.track_title || event.track_title || '').trim(),
        artistName,
        albumName: String(manualTrack?.album_name || event.album_name || '').trim(),
        playCount: 0,
        skipCount: 0,
        consecutiveSkips: 0,
        excludedFromSmart: Number(manualTrack?.manually_excluded || 0) ? 1 : 0,
        manuallyExcluded: Number(manualTrack?.manually_excluded || 0),
        manuallyIncluded: Number(manualTrack?.manually_included || 0),
        tier: 'curatorr',
        tierWeight: 0,
        lastPlayedAt: null,
        lastSkippedAt: null,
      };
      trackMap.set(trackKey, track);
    }

    if (event.track_title) track.trackTitle = String(event.track_title).trim();
    if (event.album_name) track.albumName = String(event.album_name).trim();
    track.artistName = artistName;

    const scoreDelta = tier !== track.tier ? weight - track.tierWeight : 0;
    if (isSkip) {
      track.skipCount += 1;
      track.consecutiveSkips += 1;
      track.lastSkippedAt = eventTime(event);
    } else {
      track.playCount += 1;
      const alreadyExcluded = track.excludedFromSmart === 1 && track.manuallyIncluded !== 1;
      track.consecutiveSkips = alreadyExcluded ? track.consecutiveSkips : Math.max(0, track.consecutiveSkips - 1);
      track.lastPlayedAt = eventTime(event);
    }
    track.excludedFromSmart = track.manuallyIncluded
      ? 0
      : (track.consecutiveSkips >= songSkipLimit ? 1 : track.excludedFromSmart);
    track.tier = tier;
    track.tierWeight = weight;

    const artistKey = `${userPlexId}\u0000${artistName}`;
    const manualArtist = manualArtistRows.get(artistKey);
    let artist = artistMap.get(artistKey);
    if (!artist) {
      artist = {
        userPlexId,
        artistName,
        playCount: 0,
        skipCount: 0,
        consecutiveSkips: 0,
        excludedFromSmart: Number(manualArtist?.manually_excluded || 0) ? 1 : 0,
        manuallyExcluded: Number(manualArtist?.manually_excluded || 0),
        manuallyIncluded: Number(manualArtist?.manually_included || 0),
        rankingScore: 5.0,
        lastPlayedAt: null,
        lastSkippedAt: null,
      };
      artistMap.set(artistKey, artist);
    }

    if (isSkip) {
      artist.skipCount += 1;
      artist.consecutiveSkips += 1;
      artist.lastSkippedAt = eventTime(event);
    } else {
      artist.playCount += 1;
      artist.consecutiveSkips = 0;
      artist.lastPlayedAt = eventTime(event);
    }
    artist.rankingScore = clampScore(artist.rankingScore + scoreDelta);
  }

  for (const [trackKey, manualTrack] of manualTrackRows) {
    if (trackMap.has(trackKey)) continue;
    const [userPlexId, plexRatingKey] = trackKey.split('\u0000');
    trackMap.set(trackKey, {
      userPlexId,
      plexRatingKey,
      trackTitle: String(manualTrack.track_title || '').trim(),
      artistName: String(manualTrack.artist_name || '').trim(),
      albumName: String(manualTrack.album_name || '').trim(),
      playCount: 0,
      skipCount: 0,
      consecutiveSkips: 0,
      excludedFromSmart: Number(manualTrack.manually_excluded || 0) ? 1 : 0,
      manuallyExcluded: Number(manualTrack.manually_excluded || 0),
      manuallyIncluded: Number(manualTrack.manually_included || 0),
      tier: 'curatorr',
      tierWeight: 0,
      lastPlayedAt: null,
      lastSkippedAt: null,
    });
  }

  for (const [artistKey, manualArtist] of manualArtistRows) {
    if (artistMap.has(artistKey)) continue;
    const [userPlexId, artistName] = artistKey.split('\u0000');
    artistMap.set(artistKey, {
      userPlexId,
      artistName,
      playCount: 0,
      skipCount: 0,
      consecutiveSkips: 0,
      excludedFromSmart: Number(manualArtist.manually_excluded || 0) ? 1 : 0,
      manuallyExcluded: Number(manualArtist.manually_excluded || 0),
      manuallyIncluded: Number(manualArtist.manually_included || 0),
      rankingScore: 5.0,
      lastPlayedAt: null,
      lastSkippedAt: null,
    });
  }

  const insertTrack = db.prepare(`
    INSERT INTO track_stats
      (plex_rating_key, user_plex_id, track_title, artist_name, album_name,
       play_count, skip_count, consecutive_skips, excluded_from_smart,
       manually_excluded, manually_included, tier, tier_weight,
       last_played_at, last_skipped_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  for (const track of trackMap.values()) {
    insertTrack.run(
      track.plexRatingKey,
      track.userPlexId,
      track.trackTitle,
      track.artistName,
      track.albumName,
      track.playCount,
      track.skipCount,
      track.consecutiveSkips,
      track.excludedFromSmart,
      track.manuallyExcluded,
      track.manuallyIncluded,
      track.tier,
      track.tierWeight,
      track.lastPlayedAt,
      track.lastSkippedAt,
      Date.now(),
    );
  }

  const insertArtist = db.prepare(`
    INSERT INTO artist_stats
      (artist_name, user_plex_id, play_count, skip_count, consecutive_skips,
       excluded_from_smart, manually_excluded, manually_included,
       ranking_score, last_played_at, last_skipped_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  for (const artist of artistMap.values()) {
    insertArtist.run(
      artist.artistName,
      artist.userPlexId,
      artist.playCount,
      artist.skipCount,
      artist.consecutiveSkips,
      artist.excludedFromSmart,
      artist.manuallyExcluded,
      artist.manuallyIncluded,
      artist.rankingScore,
      artist.lastPlayedAt,
      artist.lastSkippedAt,
      Date.now(),
    );
  }

  return { trackRows: trackMap.size, artistRows: artistMap.size };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig(args.config);
  const db = new Database(args.db);
  const users = [...new Set(args.users)];
  const backupPath = path.join(path.dirname(args.db), `curatorr-repair-${Date.now()}.db`);
  const summary = {
    users,
    backupPath,
    tautulliUsed: !args.skipTautulli,
    tautulliRows: 0,
    updatedExisting: 0,
    mergedResumed: 0,
    insertedMissing: 0,
    skipFlagFixes: 0,
    duplicateRowsRemoved: 0,
    overlappingRowsRemoved: 0,
    trackRows: 0,
    artistRows: 0,
  };

  try {
    await db.backup(backupPath);

    const placeholders = users.map(() => '?').join(',');
    const earliest = db.prepare(`
      SELECT MIN(started_at) AS started_at
      FROM play_events
      WHERE user_plex_id IN (${placeholders})
    `).get(...users);
    const afterTs = Math.max(
      0,
      Math.floor(((Number(earliest?.started_at || 0) || (Date.now() - (args.lookbackHours * 60 * 60 * 1000))) - (6 * 60 * 60 * 1000)) / 1000),
    );

    let tautulliRows = [];
    if (!args.skipTautulli) {
      tautulliRows = await fetchTautulliRows(config, users, afterTs, args.tautulliTimeoutMs);
      summary.tautulliRows = tautulliRows.length;
    }

    const syncTxn = db.transaction((rows) => {
      const smartConfigByUser = new Map(users.map((user) => [user, resolveUserSmartConfig(db, config, user)]));

      for (const row of rows) {
        const userPlexId = String(row.user || row.user_id || '').trim();
        const plexRatingKey = String(row.rating_key || '').trim();
        if (!userPlexId || !plexRatingKey) continue;

        const startedAtMs = Number(row.started || 0) * 1000;
        if (!startedAtMs) continue;
        const stoppedAtMs = Number(row.stopped || 0) * 1000;
        const listenedMs = Number(row.play_duration || row.duration || 0) * 1000;
        const trackDurationMs = inferHistoryTrackDurationMs(listenedMs, row);
        const trackTitle = String(row.title || '').trim();
        const artistName = String(row.original_title || row.grandparent_title || '').trim();
        const albumName = String(row.parent_title || '').trim();
        const libraryKey = String(row.section_id || '').trim();
        const smartConfig = smartConfigByUser.get(userPlexId) || {};
        const isWatched = row.watched_status === 1 || row.watched_status === '1';
        const isSkip = Boolean(isSkipEvent(listenedMs, trackDurationMs, smartConfig) && !isWatched);

        const existing = findExistingPlay(db, userPlexId, plexRatingKey, startedAtMs)
          || (stoppedAtMs ? findExistingPlayByStop(db, userPlexId, plexRatingKey, stoppedAtMs) : null);
        if (existing) {
          const nextDurationMs = Math.max(listenedMs, Number(existing.duration_ms || 0));
          const nextTrackDurationMs = chooseTrackDurationMs(existing.track_duration_ms, trackDurationMs);
          if (nextDurationMs > Number(existing.duration_ms || 0) || nextTrackDurationMs !== Number(existing.track_duration_ms || 0)) {
            db.prepare(`
              UPDATE play_events
              SET duration_ms = ?, track_duration_ms = ?, ended_at = ?, is_skip = ?
              WHERE id = ?
            `).run(
              nextDurationMs,
              nextTrackDurationMs,
              stoppedAtMs || (startedAtMs + nextDurationMs),
              (isSkipEvent(nextDurationMs, nextTrackDurationMs, smartConfig) && !isWatched) ? 1 : 0,
              existing.id,
            );
            summary.updatedExisting += 1;
          }
          continue;
        }

        const resumed = findResumedPlay(db, userPlexId, plexRatingKey, startedAtMs);
        if (resumed) {
          const combined = Number(resumed.duration_ms || 0) + listenedMs;
          const cappedMs = trackDurationMs > 0 ? Math.min(combined, trackDurationMs) : combined;
          const combinedIsSkip = Boolean(isSkipEvent(cappedMs, trackDurationMs, smartConfig) && !isWatched);
          db.prepare(`
            UPDATE play_events
            SET duration_ms = ?, track_duration_ms = ?, ended_at = ?, is_skip = ?
            WHERE id = ?
          `).run(
            cappedMs,
            trackDurationMs,
            stoppedAtMs || (startedAtMs + listenedMs),
            combinedIsSkip ? 1 : 0,
            resumed.id,
          );
          summary.mergedResumed += 1;
          continue;
        }

        db.prepare(`
          INSERT INTO play_events
            (user_plex_id, plex_rating_key, track_title, artist_name, album_name, library_key,
             started_at, ended_at, duration_ms, track_duration_ms, is_skip, event_source, session_key)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
          userPlexId,
          plexRatingKey,
          trackTitle,
          artistName,
          albumName,
          libraryKey,
          startedAtMs,
          stoppedAtMs || (startedAtMs + listenedMs),
          listenedMs,
          trackDurationMs,
          isSkip ? 1 : 0,
          'tautulli_repair',
          `tautulli-repair-${userPlexId}-${plexRatingKey}-${startedAtMs}`,
        );
        summary.insertedMissing += 1;
      }

      summary.duplicateRowsRemoved = removeNearDuplicateUnknownDurationRows(db, users);
      summary.overlappingRowsRemoved = removeOverlappingShorterRows(db, users);

      const rowsToFix = db.prepare(`
        SELECT id, user_plex_id, duration_ms, track_duration_ms, is_skip
        FROM play_events
        WHERE user_plex_id IN (${placeholders})
      `).all(...users);
      for (const row of rowsToFix) {
        const smartConfig = smartConfigByUser.get(row.user_plex_id) || {};
        const fixedSkip = isSkipEvent(Number(row.duration_ms || 0), Number(row.track_duration_ms || 0), smartConfig) ? 1 : 0;
        if (fixedSkip !== Number(row.is_skip || 0)) {
          db.prepare('UPDATE play_events SET is_skip = ? WHERE id = ?').run(fixedSkip, row.id);
          summary.skipFlagFixes += 1;
        }
      }

      const rebuilt = rebuildStatsForUsers(db, config, users);
      summary.trackRows = rebuilt.trackRows;
      summary.artistRows = rebuilt.artistRows;
    });

    syncTxn(tautulliRows);
    console.log(JSON.stringify(summary, null, 2));
  } finally {
    db.close();
  }
}

main().catch((err) => {
  console.error(err?.stack || String(err));
  process.exitCode = 1;
});
