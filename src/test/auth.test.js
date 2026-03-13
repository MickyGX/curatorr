import { after, afterEach, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolveUserFilter } from '../routes/pages.js';

const testDir = join(tmpdir(), `curatorr-test-${process.pid}`);
process.env.CONFIG_PATH = join(testDir, 'config.json');
process.env.DATA_DIR = join(testDir, 'data');
process.env.SESSION_SECRET = 'test-secret-do-not-use-in-prod';
process.env.PLEX_CLIENT_ID = 'test-client-id';
process.env.CURATORR_DISABLE_AUTOSTART = '1';
process.env.PORT = String(37000 + (process.pid % 1000));

const baseUrl = `http://127.0.0.1:${process.env.PORT}`;

const { start, stop } = await import('../index.js');
const { initDb, listLidarrRequests } = await import('../db.js');
const { runTautulliDailySync } = await import('../services/tautulli-sync.js');
const {
  resetLoginAttempts,
  checkLoginRateLimit,
  recordLoginFailure,
  clearLoginFailures,
} = await import('../routes/auth.js');

function extractCsrfToken(html) {
  const match = String(html || '').match(/name="_csrf"\s+value="([^"]+)"/);
  return match ? match[1] : '';
}

function createClient() {
  const cookies = new Map();

  function updateCookies(response) {
    const setCookies = typeof response.headers.getSetCookie === 'function'
      ? response.headers.getSetCookie()
      : (response.headers.get('set-cookie') ? [response.headers.get('set-cookie')] : []);
    for (const entry of setCookies) {
      const [pair] = String(entry || '').split(';');
      const eq = pair.indexOf('=');
      if (eq <= 0) continue;
      cookies.set(pair.slice(0, eq), pair.slice(eq + 1));
    }
  }

  async function request(path, options = {}) {
    const headers = new Headers(options.headers || {});
    if (cookies.size) {
      headers.set('cookie', [...cookies.entries()].map(([key, value]) => `${key}=${value}`).join('; '));
    }
    const response = await fetch(`${baseUrl}${path}`, {
      method: options.method || 'GET',
      headers,
      body: options.body,
      redirect: options.redirect || 'manual',
    });
    updateCookies(response);
    const text = await response.text();
    let json = null;
    const contentType = response.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
      try { json = JSON.parse(text); } catch { json = null; }
    }
    return {
      status: response.status,
      headers: response.headers,
      text,
      json,
      location: response.headers.get('location') || '',
    };
  }

  async function postForm(path, fields, csrfPath = path) {
    const page = await request(csrfPath);
    const csrfToken = extractCsrfToken(page.text);
    assert.ok(csrfToken, `Expected CSRF token on ${csrfPath}`);
    return request(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ ...fields, _csrf: csrfToken }),
    });
  }

  async function postJson(path, payload, csrfPath = '/settings') {
    const page = await request(csrfPath);
    const csrfToken = extractCsrfToken(page.text);
    assert.ok(csrfToken, `Expected CSRF token on ${csrfPath}`);
    return request(path, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRF-Token': csrfToken,
      },
      body: JSON.stringify(payload || {}),
    });
  }

  return { request, postForm, postJson };
}

async function readConfig() {
  return JSON.parse(await readFile(process.env.CONFIG_PATH, 'utf8'));
}

async function writeConfig(config) {
  await mkdir(dirname(process.env.CONFIG_PATH), { recursive: true });
  await writeFile(process.env.CONFIG_PATH, JSON.stringify(config, null, 2));
}

async function setPlaybackSource(source) {
  const config = await readConfig();
  config.general = {
    ...(config.general || {}),
    playbackSource: source,
  };
  await writeConfig(config);
}

async function login(username, password) {
  const client = createClient();
  const response = await client.postForm('/login', { username, password }, '/login');
  return { client, response };
}

function readDbRow(sql, ...params) {
  const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
  const db = initDb(dbPath);
  try {
    return db.prepare(sql).get(...params);
  } finally {
    db.close();
  }
}

function runDbStatement(sql, ...params) {
  const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
  const db = initDb(dbPath);
  try {
    return db.prepare(sql).run(...params);
  } finally {
    db.close();
  }
}

function buildPlexWebhookForm(payload) {
  const form = new FormData();
  form.set('payload', JSON.stringify(payload));
  return form;
}

let webhookKey = '';

before(async () => {
  await start();

  const setupClient = createClient();
  const setupResponse = await setupClient.postForm('/setup', {
    username: 'testadmin',
    email: 'test@curatorr.test',
    password: 'TestPassword1!',
    confirmPassword: 'TestPassword1!',
  }, '/setup');
  assert.equal(setupResponse.status, 302);

  const config = await readConfig();
  const adminUser = Array.isArray(config.users) ? config.users.find((user) => user.username === 'testadmin') : null;
  assert.ok(adminUser, 'expected setup to create the admin user');
  config.users.push({
    ...adminUser,
    username: 'coadmin',
    email: 'coadmin@curatorr.test',
    role: 'co-admin',
    setupAccount: false,
    systemCreated: false,
    createdAt: new Date().toISOString(),
  });
  config.plex = {
    ...config.plex,
    token: 'plex-secret-token',
    machineId: 'machine-secret-id',
  };
  config.tautulli = {
    ...config.tautulli,
    url: 'http://tautulli.local',
    apiKey: 'tautulli-secret-key',
  };
  await writeConfig(config);
  webhookKey = String(config?.webhooks?.sharedSecret || '').trim();
  assert.ok(webhookKey, 'expected startup to generate a webhook key');
});

after(async () => {
  await stop();
});

describe('rate limiter helpers', () => {
  afterEach(() => resetLoginAttempts());

  it('returns null for an unknown IP', () => {
    assert.equal(checkLoginRateLimit('1.2.3.4'), null);
  });

  it('returns null while under the failure threshold', () => {
    for (let i = 0; i < 9; i += 1) recordLoginFailure('1.2.3.4');
    assert.equal(checkLoginRateLimit('1.2.3.4'), null);
  });

  it('returns minutes remaining once threshold is reached', () => {
    for (let i = 0; i < 10; i += 1) recordLoginFailure('1.2.3.4');
    const mins = checkLoginRateLimit('1.2.3.4');
    assert.ok(mins !== null);
    assert.ok(mins >= 1);
  });

  it('clearLoginFailures removes the block immediately', () => {
    for (let i = 0; i < 10; i += 1) recordLoginFailure('1.2.3.4');
    clearLoginFailures('1.2.3.4');
    assert.equal(checkLoginRateLimit('1.2.3.4'), null);
  });
});

describe('auth flows', () => {
  afterEach(() => resetLoginAttempts());

  it('GET /login returns 200 when an admin exists', async () => {
    const client = createClient();
    const res = await client.request('/login');
    assert.equal(res.status, 200);
  });

  it('POST /login with wrong password returns 401', async () => {
    const client = createClient();
    const res = await client.postForm('/login', {
      username: 'testadmin',
      password: 'wrongpassword',
    }, '/login');
    assert.equal(res.status, 401);
  });

  it('POST /login with correct credentials redirects to /dashboard', async () => {
    const { response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);
    assert.ok(response.location.includes('/dashboard'));
  });

  it('POST /login returns 429 after 10 failed attempts', async () => {
    for (let i = 0; i < 10; i += 1) {
      const client = createClient();
      await client.postForm('/login', { username: 'testadmin', password: 'wrong' }, '/login');
    }
    const client = createClient();
    const res = await client.postForm('/login', { username: 'testadmin', password: 'wrong' }, '/login');
    assert.equal(res.status, 429);
  });

  it('successful login clears the rate limit counter', async () => {
    for (let i = 0; i < 9; i += 1) {
      const client = createClient();
      await client.postForm('/login', { username: 'testadmin', password: 'wrong' }, '/login');
    }

    await login('testadmin', 'TestPassword1!');

    for (let i = 0; i < 9; i += 1) {
      const client = createClient();
      await client.postForm('/login', { username: 'testadmin', password: 'wrong' }, '/login');
    }
    const client = createClient();
    const res = await client.postForm('/login', { username: 'testadmin', password: 'wrong' }, '/login');
    assert.equal(res.status, 401);
  });
});

describe('page scoping', () => {
  it('keeps local admin accounts on the global activity view', () => {
    const filter = resolveUserFilter({ username: 'admin', source: 'local' }, 'admin');
    assert.equal(filter, '');
  });

  it('scopes Plex-backed admin accounts to their Plex username', () => {
    const filter = resolveUserFilter({ username: 'MickyGX', source: 'plex' }, 'admin');
    assert.equal(filter, 'MickyGX');
  });
});

describe('security guards', () => {
  it('redirects unauthenticated dashboard access to /login', async () => {
    const client = createClient();
    const res = await client.request('/dashboard');
    assert.equal(res.status, 302);
    assert.ok(res.location.includes('/login'));
  });

  it('blocks unauthenticated Lidarr image proxy access', async () => {
    const client = createClient();
    const res = await client.request('/api/music/lidarr/image?path=/MediaCover/1/poster.jpg');
    assert.equal(res.status, 401);
  });

  it('rejects webhook calls without the shared key', async () => {
    const client = createClient();
    const res = await client.request('/webhook/tautulli', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ media_type: 'track', user: 'u', rating_key: '1' }),
    });
    assert.equal(res.status, 401);
  });

  it('accepts webhook calls with the shared key', async () => {
    const client = createClient();
    const res = await client.request(`/webhook/tautulli?key=${encodeURIComponent(webhookKey)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ media_type: 'movie' }),
    });
    assert.equal(res.status, 200);
    assert.equal(res.json?.ok, true);
  });

  it('upgrades a short Tautulli stop to the later highest session progress', async () => {
    await setPlaybackSource('tautulli');
    try {
      const client = createClient();
      const user = `tautulli-user-${Date.now()}`;
      const ratingKey = `track-${Date.now()}`;
      const sessionKey = `session-${Date.now()}`;
      const baseBody = {
        media_type: 'track',
        user,
        rating_key: ratingKey,
        session_key: sessionKey,
        title: 'Regression Song',
        grandparent_title: 'Regression Artist',
        parent_title: 'Regression Album',
        duration: 4,
      };

      const playRes = await client.request(`/webhook/tautulli?key=${encodeURIComponent(webhookKey)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...baseBody, action: 'play', view_offset: 0 }),
      });
      assert.equal(playRes.status, 200);

      const stopRes = await client.request(`/webhook/tautulli?key=${encodeURIComponent(webhookKey)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...baseBody, action: 'stop', view_offset: 3000 }),
      });
      assert.equal(stopRes.status, 200);

      const watchedRes = await client.request(`/webhook/tautulli?key=${encodeURIComponent(webhookKey)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...baseBody, action: 'watched', view_offset: 238000 }),
      });
      assert.equal(watchedRes.status, 200);

      const eventRow = readDbRow(
        'SELECT duration_ms, is_skip FROM play_events WHERE session_key = ? AND plex_rating_key = ?',
        sessionKey,
        ratingKey,
      );
      assert.ok(eventRow);
      assert.equal(Number(eventRow.is_skip || 0), 0);
      assert.equal(Number(eventRow.duration_ms || 0), 238000);

      const trackRow = readDbRow(
        'SELECT play_count, skip_count, tier, last_played_at, last_skipped_at FROM track_stats WHERE user_plex_id = ? AND plex_rating_key = ?',
        user,
        ratingKey,
      );
      assert.ok(trackRow);
      assert.equal(Number(trackRow.play_count || 0), 1);
      assert.equal(Number(trackRow.skip_count || 0), 0);
      assert.equal(trackRow.tier, 'belter');
      assert.ok(Number(trackRow.last_played_at || 0) > 0);
      assert.equal(trackRow.last_skipped_at, null);
    } finally {
      await setPlaybackSource('plex');
    }
  });

  it('keeps a Tautulli session open through scrobble and finalizes on the next play', async () => {
    await setPlaybackSource('tautulli');
    try {
      const client = createClient();
      const user = `tautulli-user-${Date.now()}`;
      const player = `tautulli-player-${Date.now()}`;
      const firstRatingKey = `track-a-${Date.now()}`;
      const secondRatingKey = `track-b-${Date.now()}`;
      const firstSessionKey = `session-a-${Date.now()}`;
      const secondSessionKey = `session-b-${Date.now()}`;

      const firstBaseBody = {
        media_type: 'track',
        user,
        player,
        rating_key: firstRatingKey,
        session_key: firstSessionKey,
        title: 'Held Open Song',
        grandparent_title: 'Regression Artist',
        parent_title: 'Regression Album',
        duration: 4,
      };

      const secondBaseBody = {
        ...firstBaseBody,
        rating_key: secondRatingKey,
        session_key: secondSessionKey,
        title: 'Next Song',
      };

      const playRes = await client.request(`/webhook/tautulli?key=${encodeURIComponent(webhookKey)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...firstBaseBody, action: 'play', view_offset: 0 }),
      });
      assert.equal(playRes.status, 200);

      const scrobbleRes = await client.request(`/webhook/tautulli?key=${encodeURIComponent(webhookKey)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...firstBaseBody, action: 'scrobble', view_offset: 238000 }),
      });
      assert.equal(scrobbleRes.status, 200);

      let eventRow = readDbRow(
        'SELECT duration_ms FROM play_events WHERE session_key = ? AND plex_rating_key = ?',
        firstSessionKey,
        firstRatingKey,
      );
      assert.equal(eventRow, undefined);

      const nextPlayRes = await client.request(`/webhook/tautulli?key=${encodeURIComponent(webhookKey)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...secondBaseBody, action: 'play', view_offset: 0 }),
      });
      assert.equal(nextPlayRes.status, 200);

      eventRow = readDbRow(
        'SELECT duration_ms, is_skip, event_source FROM play_events WHERE session_key = ? AND plex_rating_key = ?',
        firstSessionKey,
        firstRatingKey,
      );
      assert.ok(eventRow);
      assert.equal(Number(eventRow.duration_ms || 0), 238000);
      assert.equal(Number(eventRow.is_skip || 0), 0);
      assert.equal(eventRow.event_source, 'tautulli');
    } finally {
      await setPlaybackSource('plex');
    }
  });

  it('accumulates Tautulli pause and resume segments before stop', async () => {
    await setPlaybackSource('tautulli');
    try {
      const client = createClient();
      const user = `tautulli-user-${Date.now()}`;
      const ratingKey = `track-${Date.now()}`;
      const sessionKey = `session-${Date.now()}`;
      const baseBody = {
        media_type: 'track',
        user,
        rating_key: ratingKey,
        session_key: sessionKey,
        title: 'Segmented Song',
        grandparent_title: 'Regression Artist',
        parent_title: 'Regression Album',
        duration: 4,
      };

      for (const payload of [
        { ...baseBody, action: 'play', view_offset: 0 },
        { ...baseBody, action: 'pause', view_offset: 120000 },
        { ...baseBody, action: 'resume', view_offset: 120000 },
        { ...baseBody, action: 'stop', view_offset: 220000 },
      ]) {
        const res = await client.request(`/webhook/tautulli?key=${encodeURIComponent(webhookKey)}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        assert.equal(res.status, 200);
      }

      const eventRow = readDbRow(
        'SELECT duration_ms, is_skip FROM play_events WHERE session_key = ? AND plex_rating_key = ?',
        sessionKey,
        ratingKey,
      );
      assert.ok(eventRow);
      assert.equal(Number(eventRow.duration_ms || 0), 220000);
      assert.equal(Number(eventRow.is_skip || 0), 0);
    } finally {
      await setPlaybackSource('plex');
    }
  });

  it('does not let Tautulli gap-fill overwrite a play already recorded by Plex', async () => {
    const user = `plex-user-${Date.now()}`;
    const ratingKey = `plex-track-${Date.now()}`;
    const startedAt = Date.now() - 10 * 60 * 1000;
    const endedAt = startedAt + 120000;

    runDbStatement(
      `INSERT INTO play_events (
        user_plex_id, plex_rating_key, track_title, artist_name, album_name,
        started_at, ended_at, duration_ms, track_duration_ms, is_skip, event_source, session_key
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      user,
      ratingKey,
      'Primary Plex Song',
      'Primary Plex Artist',
      'Primary Plex Album',
      startedAt,
      endedAt,
      120000,
      240000,
      0,
      'plex_webhook',
      `plex-${user}-${ratingKey}`,
    );

    const config = await readConfig();
    let fetchCalls = 0;
    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      fetchCalls += 1;
      assert.equal(String(url), 'http://tautulli.local/api/v2');
      const bodyText = options.body instanceof URLSearchParams ? options.body.toString() : String(options.body || '');
      assert.match(bodyText, /cmd=get_history/);
      const rows = fetchCalls === 1 ? [{
        media_type: 'track',
        user,
        rating_key: ratingKey,
        started: Math.floor(startedAt / 1000),
        stopped: Math.floor((startedAt + 220000) / 1000),
        play_duration: 220,
        full_duration: 240,
        title: 'Primary Plex Song',
        original_title: 'Primary Plex Artist',
        parent_title: 'Primary Plex Album',
        section_id: '1',
        watched_status: 1,
      }] : [];
      return new Response(JSON.stringify({
        response: {
          data: { data: rows },
        },
      }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    };

    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    try {
      const result = await runTautulliDailySync({
        db,
        loadConfig: () => config,
        pushLog: () => {},
        safeMessage: (err) => String(err?.message || err || ''),
      });
      assert.equal(result.inserted, 0);
      assert.equal(result.skipped, 1);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const eventRow = readDbRow(
      'SELECT duration_ms, is_skip, event_source FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ?',
      user,
      ratingKey,
    );
    assert.ok(eventRow);
    assert.equal(eventRow.event_source, 'plex_webhook');
    assert.equal(Number(eventRow.duration_ms || 0), 120000);
    assert.equal(Number(eventRow.is_skip || 0), 0);
  });

  it('records Plex multipart webhooks as play events', async () => {
    const client = createClient();
    const user = `plex-user-${Date.now()}`;
    const ratingKey = `plex-track-${Date.now()}`;
    const playerUuid = `player-${Date.now()}`;

    const playRes = await client.request(`/webhook/plex?key=${encodeURIComponent(webhookKey)}`, {
      method: 'POST',
      body: buildPlexWebhookForm({
        event: 'media.play',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Plex Multipart Song',
          grandparentTitle: 'Plex Multipart Artist',
          parentTitle: 'Plex Multipart Album',
          duration: 240000,
          viewOffset: 0,
        },
      }),
    });
    assert.equal(playRes.status, 200);
    assert.equal(playRes.json?.ok, true);

    const stopRes = await client.request(`/webhook/plex?key=${encodeURIComponent(webhookKey)}`, {
      method: 'POST',
      body: buildPlexWebhookForm({
        event: 'media.stop',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Plex Multipart Song',
          grandparentTitle: 'Plex Multipart Artist',
          parentTitle: 'Plex Multipart Album',
          duration: 240000,
          viewOffset: 3000,
        },
      }),
    });
    assert.equal(stopRes.status, 200);
    assert.equal(stopRes.json?.ok, true);

    const eventRow = readDbRow(
      'SELECT duration_ms, is_skip, event_source, session_key FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ?',
      user,
      ratingKey,
    );
    assert.ok(eventRow);
    assert.equal(eventRow.event_source, 'plex_webhook');
    assert.equal(Number(eventRow.is_skip || 0), 1);
    assert.equal(Number(eventRow.duration_ms || 0), 3000);
    assert.match(String(eventRow.session_key || ''), new RegExp(playerUuid));
  });

  it('keeps separate Plex plays of the same track as separate rows', async () => {
    const client = createClient();
    const user = `plex-repeat-${Date.now()}`;
    const ratingKey = `plex-repeat-track-${Date.now()}`;
    const playerUuid = `repeat-player-${Date.now()}`;

    const firstPlay = {
      event: 'media.play',
      Account: { title: user },
      Player: { uuid: playerUuid },
      Metadata: {
        type: 'track',
        ratingKey,
        title: 'Repeat Song',
        grandparentTitle: 'Repeat Artist',
        parentTitle: 'Repeat Album',
        duration: 240000,
        viewOffset: 0,
      },
    };

    const firstStop = {
      ...firstPlay,
      event: 'media.stop',
      Metadata: {
        ...firstPlay.Metadata,
        viewOffset: 3000,
      },
    };

    const secondPlay = {
      ...firstPlay,
      Metadata: {
        ...firstPlay.Metadata,
        viewOffset: 0,
      },
    };

    const secondStop = {
      ...firstPlay,
      event: 'media.stop',
      Metadata: {
        ...firstPlay.Metadata,
        viewOffset: 235000,
      },
    };

    for (const payload of [firstPlay, firstStop, secondPlay, secondStop]) {
      const res = await client.request(`/webhook/plex?key=${encodeURIComponent(webhookKey)}`, {
        method: 'POST',
        body: buildPlexWebhookForm(payload),
      });
      assert.equal(res.status, 200);
      assert.equal(res.json?.ok, true);
    }

    const rows = (() => {
      const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
      const db = initDb(dbPath);
      try {
        return db.prepare(
          'SELECT duration_ms, is_skip FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ? ORDER BY id ASC',
        ).all(user, ratingKey);
      } finally {
        db.close();
      }
    })();

    assert.equal(rows.length, 2);
    assert.equal(Number(rows[0].is_skip || 0), 1);
    assert.equal(Number(rows[0].duration_ms || 0), 3000);
    assert.equal(Number(rows[1].is_skip || 0), 0);
    assert.equal(Number(rows[1].duration_ms || 0), 235000);
  });

  it('waits until the next Plex play before finalizing a scrobbled track and hydrates missing duration', async () => {
    const client = createClient();
    const user = `plex-split-${Date.now()}`;
    const firstKey = `plex-first-${Date.now()}`;
    const secondKey = `plex-second-${Date.now()}`;
    const playerUuid = `player-${Date.now()}`;
    const config = await readConfig();
    await writeConfig({
      ...config,
      plex: {
        ...(config.plex || {}),
        url: 'http://plex.local',
        token: 'plex-secret-token',
      },
    });

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target.startsWith(baseUrl)) return originalFetch(url, options);
      if (target.startsWith('http://plex.local/library/metadata/')) {
        const ratingKey = decodeURIComponent(target.split('/').pop()?.split('?')[0] || '');
        const durations = {
          [firstKey]: 224940,
          [secondKey]: 180000,
        };
        return new Response(JSON.stringify({
          MediaContainer: {
            Metadata: [{ duration: durations[ratingKey] || 0 }],
          },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in test: ${target}`);
    };

    try {
      const firstPlay = {
        event: 'media.play',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey: firstKey,
          title: 'Sequence Start',
          grandparentTitle: 'Sequence Artist',
          parentTitle: 'Sequence Album',
          viewOffset: 0,
        },
      };

      const firstScrobble = {
        ...firstPlay,
        event: 'media.scrobble',
      };

      const secondPlay = {
        event: 'media.play',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey: secondKey,
          title: 'Sequence Next',
          grandparentTitle: 'Sequence Artist 2',
          parentTitle: 'Sequence Album 2',
          viewOffset: 0,
        },
      };

      let res = await client.request(`/webhook/plex?key=${encodeURIComponent(webhookKey)}`, {
        method: 'POST',
        body: buildPlexWebhookForm(firstPlay),
      });
      assert.equal(res.status, 200);

      await new Promise((resolve) => setTimeout(resolve, 20));

      res = await client.request(`/webhook/plex?key=${encodeURIComponent(webhookKey)}`, {
        method: 'POST',
        body: buildPlexWebhookForm(firstScrobble),
      });
      assert.equal(res.status, 200);

      const beforeRow = readDbRow(
        'SELECT id FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ?',
        user,
        firstKey,
      );
      assert.equal(beforeRow, undefined);

      await new Promise((resolve) => setTimeout(resolve, 20));

      res = await client.request(`/webhook/plex?key=${encodeURIComponent(webhookKey)}`, {
        method: 'POST',
        body: buildPlexWebhookForm(secondPlay),
      });
      assert.equal(res.status, 200);

      const eventRow = readDbRow(
        'SELECT duration_ms, track_duration_ms, event_source FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ?',
        user,
        firstKey,
      );
      assert.ok(eventRow);
      assert.equal(eventRow.event_source, 'plex_webhook');
      assert.equal(Number(eventRow.track_duration_ms || 0), 224940);
      assert.ok(Number(eventRow.duration_ms || 0) > 0);
    } finally {
      global.fetch = originalFetch;
    }
  });

  it('accumulates a Plex play across pause and resume events', async () => {
    const client = createClient();
    const user = `plex-resume-${Date.now()}`;
    const ratingKey = `plex-resume-track-${Date.now()}`;
    const playerUuid = `resume-player-${Date.now()}`;

    const events = [
      {
        event: 'media.play',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Resume Song',
          grandparentTitle: 'Resume Artist',
          parentTitle: 'Resume Album',
          duration: 240000,
          viewOffset: 0,
        },
      },
      {
        event: 'media.pause',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Resume Song',
          grandparentTitle: 'Resume Artist',
          parentTitle: 'Resume Album',
          duration: 240000,
          viewOffset: 120000,
        },
      },
      {
        event: 'media.resume',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Resume Song',
          grandparentTitle: 'Resume Artist',
          parentTitle: 'Resume Album',
          duration: 240000,
          viewOffset: 120000,
        },
      },
      {
        event: 'media.stop',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Resume Song',
          grandparentTitle: 'Resume Artist',
          parentTitle: 'Resume Album',
          duration: 240000,
          viewOffset: 220000,
        },
      },
    ];

    for (const payload of events) {
      const res = await client.request(`/webhook/plex?key=${encodeURIComponent(webhookKey)}`, {
        method: 'POST',
        body: buildPlexWebhookForm(payload),
      });
      assert.equal(res.status, 200);
      assert.equal(res.json?.ok, true);
    }

    const eventRow = readDbRow(
      'SELECT duration_ms, track_duration_ms, is_skip FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ?',
      user,
      ratingKey,
    );
    assert.ok(eventRow);
    assert.equal(Number(eventRow.duration_ms || 0), 220000);
    assert.equal(Number(eventRow.track_duration_ms || 0), 240000);
    assert.equal(Number(eventRow.is_skip || 0), 0);
  });

  it('lists queued Lidarr requests across all users when no user filter is supplied', async () => {
    runDbStatement(
      `INSERT INTO lidarr_requests (
        user_plex_id, source_kind, request_kind, artist_name, album_title,
        status, priority_order, detail_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      'MickyGX',
      'automatic',
      'artist_album',
      `Queue Artist A ${Date.now()}`,
      '',
      'queued',
      1,
      '{}',
      Date.now(),
      Date.now(),
    );
    runDbStatement(
      `INSERT INTO lidarr_requests (
        user_plex_id, source_kind, request_kind, artist_name, album_title,
        status, priority_order, detail_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      'emmal142',
      'automatic',
      'artist_album',
      `Queue Artist B ${Date.now()}`,
      '',
      'queued',
      2,
      '{}',
      Date.now(),
      Date.now(),
    );

    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    try {
      const rows = listLidarrRequests(db, '', { statuses: ['queued'], limit: 10 });
      const users = new Set(rows.map((row) => row.userPlexId));
      assert.ok(users.has('MickyGX'));
      assert.ok(users.has('emmal142'));
    } finally {
      db.close();
    }
  });

  it('persists separate weekly automatic Lidarr caps in settings', async () => {
    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const saveRes = await client.postForm('/settings/lidarr', {
      lidarrLocalUrl: 'http://lidarr.local',
      lidarrRemoteUrl: 'https://lidarr.example.com',
      apiKey: 'lidarr-api-key',
      automationEnabled: '1',
      autoAddArtists: '1',
      autoAddWeeklyArtists: '1',
      autoAddWeeklyAlbums: '1',
      autoTriggerManualSearch: '1',
      manualSearchFallbackAttempts: '2',
      manualSearchFallbackHours: '24',
      minimumReleasePeers: '2',
      preferApprovedReleases: '1',
      coAdminWeeklyArtists: '3',
      coAdminWeeklyAlbums: '6',
      powerUserWeeklyArtists: '1',
      powerUserWeeklyAlbums: '2',
      userWeeklyArtists: '0',
      userWeeklyAlbums: '0',
    }, '/settings?tab=lidarr');
    assert.equal(saveRes.status, 302);

    const config = await readConfig();
    assert.equal(Number(config?.lidarr?.autoAddQuotas?.weeklyArtists), 1);
    assert.equal(Number(config?.lidarr?.autoAddQuotas?.weeklyAlbums), 1);
  });

  it('blocks co-admin access to admin-only wizard actions', async () => {
    const { client, response } = await login('coadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const plexWebhookRes = await client.postJson('/api/wizard/configure-plex-webhook', {}, '/settings');
    assert.equal(plexWebhookRes.status, 403);

    const webhookRes = await client.postJson('/api/wizard/configure-tautulli-webhook', {}, '/settings');
    assert.equal(webhookRes.status, 403);

    const refreshRes = await client.postJson('/api/wizard/refresh-master', {}, '/settings');
    assert.equal(refreshRes.status, 403);
  });

  it('does not render plaintext service secrets to co-admin users', async () => {
    const { client, response } = await login('coadmin', 'TestPassword1!');
    assert.equal(response.status, 302);
    const settingsRes = await client.request('/settings');
    assert.equal(settingsRes.status, 200);
    assert.ok(!settingsRes.text.includes('plex-secret-token'));
    assert.ok(!settingsRes.text.includes('tautulli-secret-key'));
    assert.ok(!settingsRes.text.includes(webhookKey));
  });
});
