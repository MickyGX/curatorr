import { saveArtistTags, getLastfmTagSyncStats } from '../db.js';

const LASTFM_API_BASE = 'https://ws.audioscrobbler.com/2.0/';
const MIN_TAG_COUNT = 10;  // Last.fm tags have a count 0-100; ignore tags below this threshold
const MAX_TAGS_PER_ARTIST = 15;
const BATCH_SIZE = 5;      // concurrent API calls
const REQUEST_TIMEOUT_MS = 10000;

async function fetchArtistTags(artistName, apiKey) {
  const url = new URL(LASTFM_API_BASE);
  url.searchParams.set('method', 'artist.getTopTags');
  url.searchParams.set('artist', artistName);
  url.searchParams.set('autocorrect', '1');
  url.searchParams.set('api_key', apiKey);
  url.searchParams.set('format', 'json');
  const r = await fetch(url.toString(), {
    headers: { Accept: 'application/json', 'User-Agent': 'Curatorr/1.0' },
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!r.ok) return [];
  const json = await r.json();
  const tags = json?.toptags?.tag || [];
  return tags
    .filter((t) => Number(t.count || 0) >= MIN_TAG_COUNT)
    .slice(0, MAX_TAGS_PER_ARTIST)
    .map((t) => String(t.name || '').toLowerCase().trim())
    .filter(Boolean);
}

export async function syncLastfmTags(ctx) {
  const { db, loadConfig, pushLog, safeMessage } = ctx;
  const config = loadConfig();
  const apiKey = config.discovery?.lastfmApiKey;
  if (!apiKey) {
    pushLog({ level: 'warn', app: 'lastfm', action: 'tag.sync.skip', message: 'Last.fm Tag Sync skipped: no API key configured' });
    return;
  }

  const artists = db.prepare('SELECT DISTINCT artist_name FROM master_tracks ORDER BY artist_name').all().map((r) => r.artist_name).filter(Boolean);
  if (!artists.length) return;

  let synced = 0;
  let failed = 0;

  for (let i = 0; i < artists.length; i += BATCH_SIZE) {
    await Promise.all(artists.slice(i, i + BATCH_SIZE).map(async (artist) => {
      try {
        const tags = await fetchArtistTags(artist, apiKey);
        saveArtistTags(db, artist, tags);
        synced++;
      } catch {
        failed++;
      }
    }));
  }

  const stats = getLastfmTagSyncStats(db);
  pushLog({
    level: 'info', app: 'lastfm', action: 'tag.sync.complete',
    message: `Last.fm Tag Sync complete: ${synced} artists synced, ${failed} failed, ${stats.withTags}/${stats.total} have tags`,
  });
}
