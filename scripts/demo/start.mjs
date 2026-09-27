// Isolated documentation fixtures. Never imports an existing config or database.
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { randomBytes } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { initDb } from '../../src/db.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
process.chdir(root);
const data = fs.mkdtempSync(path.join(os.tmpdir(), 'curatorr-docs-demo-'));
const port = Number(process.env.DEMO_PORT || 7677);
Object.assign(process.env, {
  CURATORR_DISABLE_AUTOSTART: '1', CONFIG_PATH: path.join(data, 'config.json'),
  DATA_DIR: data, DB_PATH: path.join(data, 'curatorr.db'), PORT: String(port),
  BASE_URL: `http://127.0.0.1:${port}`, SESSION_SECRET: randomBytes(32).toString('hex'),
  SESSION_COOKIE_NAME: 'curatorr_docs_demo', HTTP_ACCESS_LOGS: 'false', COOKIE_SECURE: 'false',
});
const catalogDir = path.join(root, 'tmp/demo-catalog');
const catalog = JSON.parse(fs.readFileSync(path.join(catalogDir, 'catalog.json'), 'utf8'));
const albums = catalog.map(a => [a.artist, a.album, a.genre]);
const listeners = ['Alex', 'Sam', 'Jordan'];
const tracks = catalog.flatMap((album, a) => album.tracks.map((t, n) => ({
  key: String(1000 + a * 100 + n), artist: album.artist, album: album.album, genre: album.genre, title: t.title, duration: t.duration, a, n, year: album.year,
})));
const now = Date.now();
fs.writeFileSync(path.join(catalogDir, 'Demo Mix.m3u'), '#EXTM3U\n' + tracks.filter(t => t.n === 0).map(t => `#EXTINF:${Math.round(t.duration / 1000)},${t.artist} - ${t.title}\n/music/${t.artist}/${t.album}/${t.title}.flac`).join('\n'));
const day = 86400000;
const db = initDb(process.env.DB_PATH);
function insert(table, row) {
  const keys = Object.keys(row);
  db.prepare(`INSERT OR REPLACE INTO ${table} (${keys.join(',')}) VALUES (${keys.map(() => '?').join(',')})`).run(...Object.values(row));
}
db.transaction(() => {
  for (const t of tracks) {
    if (t.a < 8) insert('master_tracks', { rating_key: t.key, artist_name: t.artist, track_title: t.title, album_name: t.album, genres: JSON.stringify([t.genre]), album_genres: JSON.stringify([t.genre]), album_moods: JSON.stringify(['dreamy', 'uplifting']), artist_countries: '["United Kingdom"]', library_key: '1', duration_ms: t.duration, library_added_at: now - t.a * day, file_path: `/music/${t.artist}/${t.album}/${t.title}.flac`, view_count: 15 + t.n });
    insert('track_enrichment', { rating_key: t.key, track_year: t.year, bpm: 80 + t.a * 5 + t.n, musical_key: 'A minor', camelot_key: '8A', energy: .3 + t.a * .04, danceability: .4 + t.n * .04, loudness: -11 - t.n / 2, loudness_range: 6.4, peak: -.8, analysis_source: 'librosa', analysis_confidence: .96 });
  }
  listeners.forEach((user, u) => {
    insert('user_preferences', { user_plex_id: user, user_wizard_completed: 1, liked_genres: '["electronic","indie","ambient"]', liked_artists: JSON.stringify(albums.slice(u, u + 3).map(a => a[0])) });
    const stats = new Map();
    for (let d = 0; d < 365; d++) {
      for (let p = 0; p < 7 + (d * 7 + u) % 16; p++) {
        const t = tracks[(d * 13 + p * 3 + u * 19) % tracks.length];
        const skipped = (d + p + u) % 17 === 0;
        const started = now - d * day - p * 210000 - 600000;
        insert('play_events', { user_plex_id: user, plex_rating_key: t.key, track_title: t.title, artist_name: t.artist, album_name: t.album, library_key: '1', started_at: started, ended_at: started + t.duration, duration_ms: skipped ? 18000 : t.duration, track_duration_ms: t.duration, is_skip: Number(skipped), event_source: p % 3 ? 'plex_webhook' : 'music_assistant', session_key: `demo-${u}-${d}-${p}` });
        const s = stats.get(t.key) || { plays: 0, skips: 0, last: started }; s[skipped ? 'skips' : 'plays']++; stats.set(t.key, s);
      }
    }
    for (const t of tracks) {
      const s = stats.get(t.key);
      insert('track_stats', { user_plex_id: user, plex_rating_key: t.key, track_title: t.title, artist_name: t.artist, album_name: t.album, play_count: s.plays, skip_count: s.skips, tier: ['belter', 'decent', 'half-decent', 'curatorr'][t.n % 4], tier_weight: .85, last_played_at: s.last });
    }
    albums.forEach(([artist, album, genre], a) => {
      const ss = tracks.filter(t => t.artist === artist).map(t => stats.get(t.key));
      insert('artist_stats', { artist_name: artist, user_plex_id: user, play_count: ss.reduce((s, x) => s + x.plays, 0), skip_count: ss.reduce((s, x) => s + x.skips, 0), ranking_score: 9.4 - a * .25, last_played_at: Math.max(...ss.map(x => x.last)) });
      insert('artist_tags', { artist_name: artist, tags: JSON.stringify([genre, 'chill']) });
      insert('suggested_artists', { user_plex_id: user, artist_name: artist, source: 'library-affinity', similarity_score: .87, behavior_score: .91, total_score: 9.3 - a * .2, status: 'suggested', reason_json: JSON.stringify({ modelVersion: 'phase2h-lastfm-tokenized-tags', sharedGenres: [genre], inLibrary: true, summary: 'Matches your favourite genres and listening habits' }) });
      insert('lidarr_requests', { user_plex_id: user, source_kind: a % 2 ? 'manual' : 'automatic', artist_name: artist, album_title: album, status: a < 8 ? 'completed' : 'queued', lidarr_album_id: 200 + a, updated_at: now - a * day, detail_json: JSON.stringify({ albumImageUrl: `/demo/art/${a}`, albumType: 'Album', monitoredConfirmed: true }) });
    });
    ['Curatorred for You', 'Golden Hour', 'Night Drive', 'Sunday Slowdown', 'Focus Flow', 'Weekend Together'].forEach((name, p) => {
      const key = p === 0 ? 'curatorred' : `personal:demo-${u}-${p}`;
      if (p) insert('user_personal_playlists', { id: `demo-${u}-${p}`, user_plex_id: user, name, rules: JSON.stringify({ limit: 36, genres: [albums[p][2]], sort: 'random' }) });
      insert('user_generated_playlists', { user_plex_id: user, playlist_type: p === 0 ? 'curatorred' : 'personal', playlist_key: key, plex_playlist_id: String(500 + p), playlist_title: name, track_count: 36, last_built_at: now - p * 3600000, last_synced_at: now - p * 3600000 });
      tracks.slice(p * 12, p * 12 + 36).forEach((t, i) => insert('playlist_tracks', { user_plex_id: user, playlist_key: key, rating_key: t.key, artist_name: t.artist, source_position: i }));
    });
  });
})();
db.close();
const config = {
  theme: { hideScrollbars: true },
  wizard: { completed: false }, general: { serverName: 'Curatorr', playbackSource: 'plex', restrictGuests: false },
  plex: { url: 'http://plex.example:32400', token: 'demo-token', machineId: 'demo-server', adminUser: 'Alex', libraries: ['1'] },
  mediaServer: { type: 'plex' }, users: [{ username: 'DemoAdmin', role: 'admin', source: 'local', setupAccount: true, passwordHash: randomBytes(64).toString('hex'), salt: randomBytes(16).toString('hex') }, ...listeners.map((username, i) => ({ username, source: 'plex', role: i ? 'user' : 'admin', avatar: `/demo/avatar/${username}` }))],
  discovery: { lastfmApiKey: '', showTrendingArtists: false, showTrendingTracks: false, showSimilarArtists: false },
  musicAssistant: { enabled: true, url: 'http://music-assistant.example:8095', token: 'demo-token', providerInstance: 'plex_demo', userMap: { 'ma-alex': 'Alex', 'ma-sam': 'Sam', 'ma-jordan': 'Jordan' }, defaultUser: 'Alex' },
  analysis: { analyzerMode: 'sidecar', analyzerSidecarUrl: 'http://analyzer.example:8765', analyzerInputFormat: 'auto', analyzerChunkSize: 100, analyzerChunkDelayMs: 1000, analyzerTrackDelayMs: 200, featuresImportPath: '/data/track-features.json', analyzerResultsPath: '/data/track-features.results.json' },
};
fs.writeFileSync(process.env.CONFIG_PATH, JSON.stringify(config));
const json = value => new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json' } });
// Deliberately no native fetch fallback: all upstream traffic stays in this process.
globalThis.fetch = async input => {
  const url = new URL(String(input instanceof Request ? input.url : input));
  if (url.pathname === '/api/v2/user') return json({ id: 1, username: 'Alex', title: 'Alex', email: 'alex@example.com', thumb: '/demo/avatar/Alex' });
  if (url.pathname.includes('/api/users') || url.pathname.includes('/api/home/users')) return new Response(`<MediaContainer>${listeners.map((u, i) => `<User id="${i + 1}" title="${u}" username="${u}" thumb="/demo/avatar/${u}" restricted="0"><Server machineIdentifier="demo-server"/></User>`).join('')}</MediaContainer>`, { headers: { 'Content-Type': 'application/xml' } });
  if (url.pathname === '/playlists') return json({ MediaContainer: { Metadata: Array.from({ length: 6 }, (_, p) => ({ ratingKey: String(500 + p), composite: `/demo/art/${p}`, leafCount: 36 })) } });
  if (/^\/playlists\/\d+\/items$/.test(url.pathname)) {
    const p = Math.max(0, Number(url.pathname.split('/')[2]) - 500);
    const offset = Number(url.searchParams.get('X-Plex-Container-Start') || 0);
    const limit = Number(url.searchParams.get('X-Plex-Container-Size') || 100);
    return json({ MediaContainer: { totalSize: 36, Metadata: tracks.slice(p * 12, p * 12 + 36).slice(offset, offset + limit).map((t, i) => ({ ratingKey: t.key, title: t.title, grandparentTitle: t.artist, parentTitle: t.album, duration: t.duration, parentThumb: `/demo/art/${t.a}`, grandparentThumb: `/demo/art/${t.a}`, playlistItemID: String(i + 1) })) } });
  }
  return json({ MediaContainer: { Metadata: [], Directory: [], size: 0 } });
};
const { app, start } = await import('../../src/index.js');
// Avoid platform-specific scrollbar chrome in documentation captures, including dialogs.
app.get('/demo/screenshot.css', (_req, res) => res.type('css').send('html[data-hide-scrollbars="1"] * { scrollbar-width: none !important; } html[data-hide-scrollbars="1"] *::-webkit-scrollbar { display: none !important; width: 0 !important; height: 0 !important; }'));
app.use((_req, res, next) => {
  const render = res.render.bind(res);
  res.render = (view, options = {}, callback) => render(view, { ...options, extraCss: [...(options.extraCss || []), '/demo/screenshot.css'] }, callback);
  next();
});
const xml = s => String(s).replace(/[<>&"]/g, c => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;' }[c]));
app.get('/demo/sign-in', (req, res) => { req.session.user = { username: 'Alex', source: 'plex', role: 'admin', avatar: '/demo/avatar/Alex', thumb: '/demo/avatar/Alex', email: 'alex@example.com' }; res.redirect('/overview'); });
app.get('/demo/avatar/:name', (req, res) => res.type('svg').send(`<svg xmlns="http://www.w3.org/2000/svg" width="160" height="160"><rect width="160" height="160" rx="80" fill="#6350a0"/><text x="80" y="103" text-anchor="middle" fill="white" font-size="72" font-family="sans-serif">${xml(req.params.name.charAt(0))}</text></svg>`));
app.get(['/demo/art/:id', '/api/music/thumb/track/:key', '/api/music/thumb/artist/:name', '/api/music/thumb/album', '/api/plex/art'], (req, res) => {
  const t = tracks.find(t => t.key === req.params.key);
  const a = albums.findIndex(a => a[0] === (req.params.name || req.query.artist));
  const id = Number(req.params.id ?? String(req.query.path || '').split('/').pop());
  const index = t?.a ?? (a >= 0 ? a : (Number.isFinite(id) ? id : 0));
  res.sendFile(path.join(catalogDir, catalog[Math.abs(index) % catalog.length].image));
});
app.get('/api/music/overview/now-playing', (req, res) => res.json({ nowPlaying: { trackTitle: tracks[0].title, artistName: tracks[0].artist, albumName: tracks[0].album, albumThumbPath: '/demo/art/0', isPaused: false, source: 'music_assistant', playCount: 24 } }));
app.get('/api/music-assistant/status', (req, res) => res.json({ state: 'connected', serverVersion: '2.7.0', serverName: 'Demo Music Assistant', user: { username: 'Alex', userId: 'ma-alex', role: 'admin' }, lastPlayAt: now - 600000, tokenExpiresAt: now + 300 * day, index: { matched: 144, scanned: 144 }, counters: { plays: 128, matched: 124, unmatched: 4, droppedNoUser: 0, errors: 0 } }));
app.post('/api/music-assistant/test', (req, res) => res.json({ ok: true, serverName: 'Demo Music Assistant', serverVersion: '2.7.0', user: { username: 'Alex' }, canListUsers: true, users: listeners.map(u => ({ userId: `ma-${u.toLowerCase()}`, displayName: u, username: u })), providers: [{ instanceId: 'plex_demo', name: 'Demo Music Library', domain: 'plex', available: true }] }));
app.use((req, res, next) => req.method === 'GET' || req.method === 'HEAD' || (req.method === 'POST' && req.path === '/api/music/import/m3u/preview') ? next() : res.status(405).json({ error: 'The documentation demo is read-only.' }));
// Bind explicitly to loopback; the fixture sign-in must never be exposed on a LAN.
const listen = app.listen.bind(app);
app.listen = (port, callback) => listen(port, '127.0.0.1', callback);
await start(); // wizard incomplete prevents background jobs and MA from starting
config.wizard.completed = true;
fs.writeFileSync(process.env.CONFIG_PATH, JSON.stringify(config));
console.log(`Demo ready: http://127.0.0.1:${port}/demo/sign-in`);
console.log(`Disposable data: ${data}`);
