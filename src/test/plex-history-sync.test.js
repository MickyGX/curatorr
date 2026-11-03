import { after, afterEach, before, beforeEach, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { initDb } from '../db.js';
import { collapseDoubleScrobbles, runPlexHistorySync } from '../services/plex-history-sync.js';

const NOW = Date.UTC(2026, 10, 3, 18, 0, 0);
const MIN = 60_000;
const TRACK_MS = 4 * MIN;

const CONFIG = {
  mediaServer: { type: 'plex' },
  plex: { url: 'http://plex.local:32400', token: 'tok', libraries: ['3'] },
  smartPlaylist: {},
};

function historyRow(id, { ratingKey = 'rk-1', viewedAtMs, accountID = 1, section = '3', type = 'track', title = 'Song', artist = 'Artist' }) {
  return {
    historyKey: `/status/sessions/history/${id}`,
    ratingKey,
    librarySectionID: section,
    type,
    title,
    grandparentTitle: artist,
    parentTitle: 'Album',
    viewedAt: Math.floor(viewedAtMs / 1000),
    accountID,
  };
}

function mockPlex(rows, calls = []) {
  return async (url) => {
    const href = String(url);
    calls.push(href);
    const { pathname } = new URL(href);
    let body;
    if (pathname === '/accounts') {
      body = { MediaContainer: { Account: [{ id: 0, name: '' }, { id: 1, name: 'owner' }, { id: 2, name: 'friend' }] } };
    } else if (pathname === '/status/sessions/history/all') {
      body = { MediaContainer: { Metadata: rows } };
    } else if (pathname.startsWith('/library/metadata/')) {
      body = { MediaContainer: { Metadata: [{ duration: TRACK_MS }] } };
    } else {
      return new Response('not found', { status: 404 });
    }
    return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } });
  };
}

function insertPlay(db, { user = 'owner', ratingKey = 'rk-1', startedAt, source = 'plex_webhook' }) {
  db.prepare(`
    INSERT INTO play_events (user_plex_id, plex_rating_key, track_title, artist_name, album_name,
      started_at, ended_at, duration_ms, track_duration_ms, is_skip, event_source, session_key)
    VALUES (?, ?, 'Song', 'Artist', 'Album', ?, ?, ?, ?, 0, ?, '')
  `).run(user, ratingKey, startedAt, startedAt + TRACK_MS, TRACK_MS, TRACK_MS, source);
}

function historyEvents(db) {
  return db.prepare("SELECT * FROM play_events WHERE event_source = 'plex_history' ORDER BY started_at").all();
}

describe('Plex history sync', () => {
  let dir;
  let db;
  let originalFetch;
  const ctx = () => ({
    db,
    loadConfig: () => CONFIG,
    pushLog: () => {},
    safeMessage: (err) => String(err?.message || err),
  });

  before(() => { dir = mkdtempSync(join(tmpdir(), 'curatorr-plex-history-')); });
  after(() => rmSync(dir, { recursive: true, force: true }));
  beforeEach(() => {
    db = initDb(join(dir, `db-${Math.random().toString(36).slice(2)}.sqlite`));
    db.prepare("INSERT INTO master_tracks (rating_key, artist_name, track_title, duration_ms) VALUES ('rk-1', 'Artist', 'Song', ?)").run(TRACK_MS);
    originalFetch = global.fetch;
  });
  afterEach(() => {
    global.fetch = originalFetch;
    db.close();
  });

  it('adds an offline play and leaves plays webhooks already recorded alone', async () => {
    const onlineViewedAt = NOW - 3 * 60 * MIN;
    insertPlay(db, { startedAt: onlineViewedAt - 3.5 * MIN });
    const offlineViewedAt = NOW - 26 * 60 * MIN;

    const calls = [];
    global.fetch = mockPlex([
      historyRow(10, { viewedAtMs: onlineViewedAt }),
      historyRow(11, { viewedAtMs: offlineViewedAt }),
    ], calls);

    const result = await runPlexHistorySync(ctx(), { now: NOW });
    assert.equal(result.inserted, 1);
    assert.equal(result.recorded, 1);

    const [event] = historyEvents(db);
    assert.equal(event.user_plex_id, 'owner');
    assert.equal(event.session_key, 'plex-history-11');
    assert.equal(event.duration_ms, TRACK_MS);
    assert.equal(event.started_at, offlineViewedAt - Math.round(TRACK_MS * 0.9));
    assert.equal(event.is_skip, 0);

    const stats = db.prepare("SELECT play_count FROM track_stats WHERE user_plex_id = 'owner' AND plex_rating_key = 'rk-1'").get();
    assert.equal(stats?.play_count, 1);

    const historyCall = calls.find((href) => href.includes('/status/sessions/history/all'));
    assert.match(historyCall, /&viewedAt>=\d+$/);
  });

  it('is safe to run again', async () => {
    global.fetch = mockPlex([historyRow(20, { viewedAtMs: NOW - 5 * 60 * MIN })]);
    assert.equal((await runPlexHistorySync(ctx(), { now: NOW })).inserted, 1);
    assert.equal((await runPlexHistorySync(ctx(), { now: NOW })).inserted, 0);
    assert.equal(historyEvents(db).length, 1);
  });

  it('collapses a play Plex logged twice a few seconds apart', async () => {
    const viewedAt = NOW - 5 * 60 * MIN;
    global.fetch = mockPlex([
      historyRow(31, { viewedAtMs: viewedAt + 7000 }),
      historyRow(30, { viewedAtMs: viewedAt }),
    ]);
    const result = await runPlexHistorySync(ctx(), { now: NOW });
    assert.equal(result.inserted, 1);
    assert.equal(result.collapsed, 1);
  });

  it('keeps a track played twice in a row when only one play was recorded', async () => {
    const first = NOW - 5 * 60 * MIN;
    const second = first + TRACK_MS;
    insertPlay(db, { startedAt: first - 3.5 * MIN });
    global.fetch = mockPlex([
      historyRow(41, { viewedAtMs: second }),
      historyRow(40, { viewedAtMs: first }),
    ]);
    const result = await runPlexHistorySync(ctx(), { now: NOW });
    assert.equal(result.inserted, 1);
    assert.equal(result.recorded, 1);
    assert.equal(historyEvents(db)[0].session_key, 'plex-history-41');
  });

  it('leaves recent plays and plays with an open session for the next run', async () => {
    db.prepare(`
      INSERT INTO open_sessions (session_key, user_plex_id, plex_rating_key, started_at, event_source)
      VALUES ('plex-friend-x-rk-1', 'friend', 'rk-1', ?, 'plex_webhook')
    `).run(NOW - 70 * MIN);
    global.fetch = mockPlex([
      historyRow(50, { viewedAtMs: NOW - 5 * MIN }),
      historyRow(51, { viewedAtMs: NOW - 60 * MIN, accountID: 2 }),
    ]);
    const result = await runPlexHistorySync(ctx(), { now: NOW });
    assert.equal(result.inserted, 0);
    assert.equal(result.pending, 2);
  });

  it('ignores other libraries, non-track rows and unknown accounts', async () => {
    const viewedAt = NOW - 5 * 60 * MIN;
    global.fetch = mockPlex([
      historyRow(60, { viewedAtMs: viewedAt, section: '9' }),
      historyRow(61, { viewedAtMs: viewedAt, type: 'episode' }),
      historyRow(62, { viewedAtMs: viewedAt, accountID: 999 }),
    ]);
    const result = await runPlexHistorySync(ctx(), { now: NOW });
    assert.equal(result.inserted, 0);
    assert.equal(result.libraryFiltered, 1);
    assert.equal(result.unknownUser, 1);
  });

  it('looks up the track length from Plex when Curatorr does not know it', async () => {
    const calls = [];
    global.fetch = mockPlex([historyRow(70, { ratingKey: 'rk-new', viewedAtMs: NOW - 5 * 60 * MIN })], calls);
    const result = await runPlexHistorySync(ctx(), { now: NOW });
    assert.equal(result.inserted, 1);
    assert.ok(calls.some((href) => href.includes('/library/metadata/rk-new')));
    assert.equal(historyEvents(db)[0].track_duration_ms, TRACK_MS);
  });

  it('skips cleanly when the media server is not Plex', async () => {
    global.fetch = async () => { throw new Error('should not fetch'); };
    const result = await runPlexHistorySync({ ...ctx(), loadConfig: () => ({ mediaServer: { type: 'jellyfin' } }) }, { now: NOW });
    assert.equal(result.inserted, 0);
  });
});

describe('collapseDoubleScrobbles', () => {
  it('only collapses rows for the same user and track', () => {
    const rows = [
      { userPlexId: 'a', plexRatingKey: '1', viewedAtMs: 0 },
      { userPlexId: 'b', plexRatingKey: '1', viewedAtMs: 1000 },
      { userPlexId: 'a', plexRatingKey: '2', viewedAtMs: 2000 },
      { userPlexId: 'a', plexRatingKey: '1', viewedAtMs: 3000 },
    ];
    const { kept, collapsed } = collapseDoubleScrobbles(rows);
    assert.equal(kept.length, 3);
    assert.equal(collapsed, 1);
  });
});
