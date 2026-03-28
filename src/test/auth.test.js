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

const { start, stop, canUserAccessLidarrAutomation, completePlexLogin } = await import('../index.js');
const { initDb, listLidarrRequests, getUserPreferences, saveUserPreferences, listUserGeneratedPlaylists, saveUserGeneratedPlaylist } = await import('../db.js');
const { createLidarrService } = await import('../services/lidarr.js');
const { createPlaylistService } = await import('../services/playlists.js');
const { runTautulliDailySync } = await import('../services/tautulli-sync.js');
const {
  resetLoginAttempts,
  checkLoginRateLimit,
  recordLoginFailure,
  clearLoginFailures,
  completePlexWizardTokenFetch,
} = await import('../routes/auth.js');

function extractCsrfToken(html) {
  const match = String(html || '').match(/name="_csrf"\s+value="([^"]+)"/);
  return match ? match[1] : '';
}

function createClient(options = {}) {
  const cookies = new Map();
  const maxCookieSize = Number(options.maxCookieSize || 0);

  function getSetCookieEntries(response) {
    return typeof response.headers.getSetCookie === 'function'
      ? response.headers.getSetCookie()
      : (response.headers.get('set-cookie') ? [response.headers.get('set-cookie')] : []);
  }

  function updateCookies(response) {
    const setCookies = getSetCookieEntries(response);
    for (const entry of setCookies) {
      const [pair] = String(entry || '').split(';');
      const eq = pair.indexOf('=');
      if (eq <= 0) continue;
      if (maxCookieSize > 0 && pair.length > maxCookieSize) continue;
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

  function cookieHeader() {
    return [...cookies.entries()].map(([key, value]) => `${key}=${value}`).join('; ');
  }

  function ingestCookies(response) {
    updateCookies(response);
  }

  return { request, postForm, postJson, cookieHeader, ingestCookies };
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

  it('keeps the Plex Home selection session under browser cookie limits', async () => {
    const client = createClient({ maxCookieSize: 4096 });
    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target.startsWith(baseUrl)) return originalFetch(url, options);
      if (target === 'https://plex.tv/api/v2/pins/123456') {
        return new Response(JSON.stringify({ authToken: 'plex-main-token' }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'https://plex.tv/api/v2/home/users') {
        return new Response(JSON.stringify([
          {
            id: 'home-owner',
            title: 'Owner',
            protected: false,
            admin: true,
            thumb: `https://metadata.plex.tv/${'a'.repeat(1800)}`,
          },
          {
            id: 'home-kid-1',
            title: 'Kid One',
            protected: true,
            admin: false,
            thumb: `https://metadata.plex.tv/${'b'.repeat(1800)}`,
          },
          {
            id: 'home-kid-2',
            title: 'Kid Two',
            protected: false,
            admin: false,
            thumb: `https://metadata.plex.tv/${'c'.repeat(1800)}`,
          },
        ]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in test: ${target}`);
    };

    try {
      let res = await client.request('/auth/plex');
      assert.equal(res.status, 200);

      res = await client.postJson('/api/plex/pin', { pinId: '123456' }, '/login');
      assert.equal(res.status, 200);
      assert.equal(res.json?.ok, true);

      res = await client.request('/oauth/callback');
      assert.equal(res.status, 302);
      assert.equal(res.location, '/auth/plex/home-users');
      const setCookies = typeof res.headers.getSetCookie === 'function'
        ? res.headers.getSetCookie()
        : (res.headers.get('set-cookie') ? [res.headers.get('set-cookie')] : []);
      const sessionCookie = setCookies.find((entry) => String(entry || '').startsWith('curatorr_session='))
        || setCookies[0]
        || '';
      assert.ok(sessionCookie.length > 0);
      assert.ok(sessionCookie.length < 4096);

      res = await client.request('/auth/plex/home-users');
      assert.equal(res.status, 200);
      assert.match(res.text, /Who&#39;s watching\?/);
      assert.match(res.text, /Kid One/);
      assert.doesNotMatch(res.text, /metadata\.plex\.tv/);
    } finally {
      global.fetch = originalFetch;
    }
  });

  it('tolerates duplicate home-user selection submits', async () => {
    const client = createClient();
    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target.startsWith(baseUrl)) return originalFetch(url, options);
      if (target === 'https://plex.tv/api/v2/pins/123456') {
        return new Response(JSON.stringify({ authToken: 'plex-main-token' }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'https://plex.tv/api/v2/home/users') {
        return new Response(JSON.stringify([
          { id: 'home-owner', title: 'Owner', protected: false, admin: true },
          { id: 'home-kid-2', title: 'Kid Two', protected: false, admin: false },
        ]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'https://plex.tv/api/v2/home/users/home-kid-2/switch') {
        await new Promise((resolve) => setTimeout(resolve, 75));
        return new Response(JSON.stringify({ authToken: 'plex-home-token' }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'https://plex.tv/api/v2/user') {
        const authToken = String(options?.headers?.['X-Plex-Token'] || options?.headers?.get?.('X-Plex-Token') || '');
        if (authToken === 'plex-home-token') {
          return new Response(JSON.stringify({
            id: 'home-kid-2',
            username: 'KidTwo',
            title: 'Kid Two',
            email: 'kidtwo@example.com',
            thumb: '',
          }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          });
        }
      }
      if (target.startsWith('https://plex.tv/api/v2/resources')) {
        return new Response(JSON.stringify([]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in test: ${target}`);
    };

    try {
      let res = await client.request('/auth/plex');
      assert.equal(res.status, 200);

      res = await client.postJson('/api/plex/pin', { pinId: '123456' }, '/login');
      assert.equal(res.status, 200);
      assert.equal(res.json?.ok, true);

      res = await client.request('/oauth/callback');
      assert.equal(res.status, 302);
      assert.equal(res.location, '/auth/plex/home-users');

      const selectionPage = await client.request('/auth/plex/home-users');
      assert.equal(selectionPage.status, 200);
      const csrfToken = extractCsrfToken(selectionPage.text);
      assert.ok(csrfToken, 'Expected CSRF token on home user selector');
      const cookieHeader = client.cookieHeader();
      assert.ok(cookieHeader.includes('curatorr_session='), 'Expected session cookie before duplicate submit test');

      const doSelect = async () => {
        const response = await fetch(`${baseUrl}/auth/plex/home-users/select`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            cookie: cookieHeader,
          },
          body: new URLSearchParams({ _csrf: csrfToken, userId: 'home-kid-2' }),
          redirect: 'manual',
        });
        const text = await response.text();
        return {
          status: response.status,
          headers: response.headers,
          text,
          location: response.headers.get('location') || '',
        };
      };

      const [firstSelect, secondSelect] = await Promise.all([doSelect(), doSelect()]);
      client.ingestCookies(firstSelect);
      client.ingestCookies(secondSelect);

      assert.equal(firstSelect.status, 302);
      assert.equal(secondSelect.status, 302);
      assert.notEqual(firstSelect.location, '/login');
      assert.equal(secondSelect.location, firstSelect.location);
      assert.equal(secondSelect.headers.get('set-cookie'), null);

      const postLoginRes = await client.request(firstSelect.location);
      assert.notEqual(postLoginRes.location, '/login');
      assert.doesNotMatch(postLoginRes.text, /Incorrect username or password/i);
    } finally {
      global.fetch = originalFetch;
    }
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

describe('user settings integrations', () => {
  it('saves ListenBrainz playlist preferences', async () => {
    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const page = await client.request('/user-settings');
    const csrfToken = extractCsrfToken(page.text);
    assert.ok(csrfToken, 'Expected CSRF token on /user-settings');

    const form = new URLSearchParams();
    form.set('_csrf', csrfToken);
    form.set('listenbrainzUsername', 'lb-user');
    form.set('listenbrainzToken', 'lb-token');
    form.append('listenbrainzPlaylists', 'daily-jams');
    form.append('listenbrainzPlaylists', 'weekly-exploration');

    const res = await client.request('/user-settings/listenbrainz', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: form,
    });
    assert.equal(res.status, 302);
    assert.equal(res.location, '/user-settings?success=listenbrainz-updated');

    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    try {
      const prefs = getUserPreferences(db, 'testadmin');
      assert.equal(prefs.listenbrainzUsername, 'lb-user');
      assert.equal(prefs.listenbrainzToken, 'lb-token');
      assert.deepEqual(prefs.listenbrainzEnabledPlaylists, ['daily-jams', 'weekly-exploration']);
    } finally {
      db.close();
    }
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

  it('ignores Tautulli webhooks from Plex libraries that are not selected', async () => {
    const originalConfig = await readConfig();
    await writeConfig({
      ...originalConfig,
      plex: {
        ...originalConfig.plex,
        libraries: ['1'],
        availableLibraries: [
          { key: '1', title: 'Music' },
          { key: '2', title: 'Audiobooks' },
        ],
      },
      general: {
        ...(originalConfig.general || {}),
        playbackSource: 'tautulli',
      },
    });

    try {
      const client = createClient();
      const user = `tautulli-user-${Date.now()}`;
      const ratingKey = `track-${Date.now()}`;
      const res = await client.request(`/webhook/tautulli?key=${encodeURIComponent(webhookKey)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action: 'play',
          media_type: 'track',
          user,
          rating_key: ratingKey,
          session_key: `session-${Date.now()}`,
          title: 'Ignored Audiobook',
          grandparent_title: 'Audiobook Author',
          parent_title: 'Ignored Album',
          section_id: '2',
          library_name: 'Audiobooks',
          duration: 4,
          view_offset: 0,
        }),
      });
      assert.equal(res.status, 200);
      assert.equal(res.json?.ignored, 'library_not_selected');

      const eventRow = readDbRow(
        'SELECT id FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ?',
        user,
        ratingKey,
      );
      assert.equal(eventRow, undefined);
    } finally {
      await writeConfig(originalConfig);
    }
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

  it('ignores Tautulli gap-fill rows from Plex libraries that are not selected', async () => {
    const originalConfig = await readConfig();
    const updatedConfig = {
      ...originalConfig,
      plex: {
        ...originalConfig.plex,
        libraries: ['1'],
        availableLibraries: [
          { key: '1', title: 'Music' },
          { key: '2', title: 'Audiobooks' },
        ],
      },
    };
    await writeConfig(updatedConfig);

    const user = `plex-user-${Date.now()}`;
    const ratingKey = `plex-track-${Date.now()}`;
    const startedAt = Date.now() - 10 * 60 * 1000;

    const originalFetch = global.fetch;
    global.fetch = async () => new Response(JSON.stringify({
      response: {
        data: {
          data: [{
            media_type: 'track',
            user,
            rating_key: ratingKey,
            started: Math.floor(startedAt / 1000),
            stopped: Math.floor((startedAt + 220000) / 1000),
            play_duration: 220,
            full_duration: 240,
            title: 'Ignored Audiobook',
            original_title: 'Audiobook Author',
            parent_title: 'Ignored Album',
            section_id: '2',
            section_name: 'Audiobooks',
            watched_status: 1,
          }],
        },
      },
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });

    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    try {
      const result = await runTautulliDailySync({
        db,
        loadConfig: () => updatedConfig,
        pushLog: () => {},
        safeMessage: (err) => String(err?.message || err || ''),
      });
      assert.equal(result.inserted, 0);
      assert.equal(result.skipped, 1);
    } finally {
      global.fetch = originalFetch;
      db.close();
      await writeConfig(originalConfig);
    }

    const eventRow = readDbRow(
      'SELECT id FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ?',
      user,
      ratingKey,
    );
    assert.equal(eventRow, undefined);
  });

  it('removes stale Plex library data when a library is deselected in settings', async () => {
    const originalConfig = await readConfig();
    await writeConfig({
      ...originalConfig,
      plex: {
        ...originalConfig.plex,
        libraries: ['1', '2'],
        availableLibraries: [
          { key: '1', title: 'Music' },
          { key: '2', title: 'Audiobooks' },
        ],
      },
    });

    const user = `cleanup-user-${Date.now()}`;
    const keepRatingKey = `keep-track-${Date.now()}`;
    const removedRatingKey = `remove-track-${Date.now()}`;
    const artistName = `cleanup-artist-${Date.now()}`;
    const now = Date.now();

    runDbStatement(
      `INSERT INTO play_events (
        user_plex_id, plex_rating_key, track_title, artist_name, album_name,
        library_key, started_at, ended_at, duration_ms, track_duration_ms, is_skip, event_source, session_key
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      user,
      keepRatingKey,
      'Kept Song',
      artistName,
      'Kept Album',
      '1',
      now - 300000,
      now - 60000,
      240000,
      240000,
      0,
      'tautulli_sync',
      `keep-${user}`,
    );
    runDbStatement(
      `INSERT INTO play_events (
        user_plex_id, plex_rating_key, track_title, artist_name, album_name,
        library_key, started_at, ended_at, duration_ms, track_duration_ms, is_skip, event_source, session_key
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      user,
      removedRatingKey,
      'Removed Song',
      artistName,
      'Removed Album',
      '2',
      now - 200000,
      now - 100000,
      100000,
      240000,
      0,
      'tautulli_sync',
      `remove-${user}`,
    );
    runDbStatement(
      `INSERT INTO open_sessions (
        session_key, user_plex_id, plex_rating_key, track_title, artist_name, album_name,
        library_key, track_duration_ms, player_scope, playback_state, last_position_ms,
        max_position_ms, accumulated_ms, playing_since, last_event_at, started_at, event_source
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      `open-${user}`,
      user,
      removedRatingKey,
      'Removed Song',
      artistName,
      'Removed Album',
      '2',
      240000,
      '',
      'playing',
      0,
      0,
      0,
      now - 50000,
      now - 50000,
      now - 50000,
      'tautulli',
    );
    runDbStatement(
      `INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        manually_excluded, manually_included, tier, tier_weight,
        last_played_at, last_skipped_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      removedRatingKey,
      user,
      'Removed Song',
      artistName,
      'Removed Album',
      1,
      0,
      0,
      0,
      0,
      0,
      'belter',
      1,
      now - 100000,
      null,
      now,
    );
    runDbStatement(
      `INSERT INTO artist_stats (
        artist_name, user_plex_id, play_count, skip_count, consecutive_skips,
        excluded_from_smart, manually_excluded, manually_included,
        ranking_score, last_played_at, last_skipped_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      artistName,
      user,
      2,
      0,
      0,
      0,
      0,
      0,
      7,
      now - 60000,
      null,
      now,
    );
    runDbStatement(
      `INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, genres, library_key, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      keepRatingKey,
      artistName,
      'Kept Song',
      'Kept Album',
      '[]',
      '1',
      0,
      0,
      now,
    );
    runDbStatement(
      `INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, genres, library_key, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      removedRatingKey,
      artistName,
      'Removed Song',
      'Removed Album',
      '[]',
      '2',
      0,
      0,
      now,
    );

    try {
      const { client, response } = await login('testadmin', 'TestPassword1!');
      assert.equal(response.status, 302);

      const settingsPage = await client.request('/settings?tab=plex');
      const csrfToken = extractCsrfToken(settingsPage.text);
      assert.ok(csrfToken);

      const form = new URLSearchParams({
        _csrf: csrfToken,
        plexLocalUrl: '',
        plexRemoteUrl: '',
        plexToken: '',
        machineId: String(originalConfig?.plex?.machineId || ''),
        plexAdminUser: String(originalConfig?.plex?.adminUser || ''),
      });
      form.append('libraries', '1');

      const res = await client.request('/settings/plex', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: form,
      });
      assert.equal(res.status, 302);

      const removedEvent = readDbRow(
        'SELECT id FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ?',
        user,
        removedRatingKey,
      );
      const keptEvent = readDbRow(
        'SELECT id FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ?',
        user,
        keepRatingKey,
      );
      const removedSession = readDbRow('SELECT session_key FROM open_sessions WHERE session_key = ?', `open-${user}`);
      const removedTrack = readDbRow(
        'SELECT plex_rating_key FROM track_stats WHERE user_plex_id = ? AND plex_rating_key = ?',
        user,
        removedRatingKey,
      );
      const artistStats = readDbRow(
        'SELECT play_count, ranking_score FROM artist_stats WHERE user_plex_id = ? AND artist_name = ?',
        user,
        artistName,
      );
      const removedMasterTrack = readDbRow(
        'SELECT rating_key FROM master_tracks WHERE rating_key = ?',
        removedRatingKey,
      );
      const keptMasterTrack = readDbRow(
        'SELECT rating_key FROM master_tracks WHERE rating_key = ?',
        keepRatingKey,
      );

      assert.equal(removedEvent, undefined);
      assert.ok(keptEvent);
      assert.equal(removedSession, undefined);
      assert.equal(removedTrack, undefined);
      assert.ok(artistStats);
      assert.equal(Number(artistStats.play_count || 0), 1);
      assert.ok(Number(artistStats.ranking_score || 0) <= 10);
      assert.equal(removedMasterTrack, undefined);
      assert.ok(keptMasterTrack);
    } finally {
      await writeConfig(originalConfig);
    }
  });

  it('refreshes Plex libraries from settings without dropping valid selections', async () => {
    const originalConfig = await readConfig();
    await writeConfig({
      ...originalConfig,
      plex: {
        ...originalConfig.plex,
        url: 'http://plex.local:32400',
        localUrl: 'http://plex.local:32400',
        token: 'plex-refresh-token',
        libraries: ['1'],
        availableLibraries: [
          { key: '1', title: 'Music' },
        ],
      },
    });

    try {
      const { client, response } = await login('testadmin', 'TestPassword1!');
      assert.equal(response.status, 302);

      const originalFetch = global.fetch;
      global.fetch = async (url, options) => {
        if (String(url) === 'http://plex.local:32400/library/sections') {
          return new Response(JSON.stringify({
            MediaContainer: {
              Directory: [
                { key: '1', title: 'Music', type: 'artist' },
                { key: '2', title: 'Audiobooks', type: 'artist' },
                { key: '99', title: 'Films', type: 'movie' },
              ],
            },
          }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          });
        }
        return originalFetch(url, options);
      };

      const refreshRes = await client.postForm('/settings/plex/refresh-libraries', {}, '/settings?tab=plex');
      assert.equal(refreshRes.status, 302);

      const nextConfig = await readConfig();
      assert.deepEqual(nextConfig?.plex?.libraries || [], ['1']);
      assert.deepEqual(nextConfig?.plex?.availableLibraries || [], [
        { key: '1', title: 'Music', type: 'artist', agent: '' },
        { key: '2', title: 'Audiobooks', type: 'artist', agent: '' },
      ]);
      global.fetch = originalFetch;
    } finally {
      await writeConfig(originalConfig);
    }
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

  it('consolidates recent Plex plays of the same track into one row', async () => {
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

    assert.equal(rows.length, 1);
    assert.equal(Number(rows[0].is_skip || 0), 0);
    assert.equal(Number(rows[0].duration_ms || 0), 238000);
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

  it('consolidates a resumed Plex play using only the additional listened time', async () => {
    const client = createClient();
    const user = `plex-resume-merge-${Date.now()}`;
    const ratingKey = `plex-resume-merge-track-${Date.now()}`;
    const playerUuid = `resume-merge-player-${Date.now()}`;

    const firstPass = [
      {
        event: 'media.play',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Resume Merge Song',
          grandparentTitle: 'Resume Merge Artist',
          parentTitle: 'Resume Merge Album',
          duration: 240000,
          viewOffset: 0,
        },
      },
      {
        event: 'media.stop',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Resume Merge Song',
          grandparentTitle: 'Resume Merge Artist',
          parentTitle: 'Resume Merge Album',
          duration: 240000,
          viewOffset: 120000,
        },
      },
    ];

    const resumedPass = [
      {
        event: 'media.play',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Resume Merge Song',
          grandparentTitle: 'Resume Merge Artist',
          parentTitle: 'Resume Merge Album',
          duration: 240000,
          viewOffset: 120000,
        },
      },
      {
        event: 'media.pause',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Resume Merge Song',
          grandparentTitle: 'Resume Merge Artist',
          parentTitle: 'Resume Merge Album',
          duration: 240000,
          viewOffset: 180000,
        },
      },
      {
        event: 'media.resume',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Resume Merge Song',
          grandparentTitle: 'Resume Merge Artist',
          parentTitle: 'Resume Merge Album',
          duration: 240000,
          viewOffset: 180000,
        },
      },
      {
        event: 'media.stop',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Resume Merge Song',
          grandparentTitle: 'Resume Merge Artist',
          parentTitle: 'Resume Merge Album',
          duration: 240000,
          viewOffset: 220000,
        },
      },
    ];

    for (const payload of [...firstPass, ...resumedPass]) {
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

    assert.equal(rows.length, 1);
    assert.equal(Number(rows[0].is_skip || 0), 0);
    assert.equal(Number(rows[0].duration_ms || 0), 220000);
  });

  it('consolidates recent Plex plays when rating keys differ but track and artist match', async () => {
    const client = createClient();
    const user = `plex-loose-merge-${Date.now()}`;
    const firstRatingKey = `plex-loose-merge-a-${Date.now()}`;
    const secondRatingKey = `plex-loose-merge-b-${Date.now()}`;
    const playerUuid = `loose-merge-player-${Date.now()}`;

    const firstPlay = {
      event: 'media.play',
      Account: { title: user },
      Player: { uuid: playerUuid },
      Metadata: {
        type: 'track',
        ratingKey: firstRatingKey,
        title: 'If I Had A Gun...',
        grandparentTitle: 'Noel Gallagher’s High Flying Birds',
        parentTitle: 'Album One',
        duration: 240000,
        viewOffset: 0,
      },
    };

    const firstStop = {
      ...firstPlay,
      event: 'media.stop',
      Metadata: {
        ...firstPlay.Metadata,
        viewOffset: 0,
      },
    };

    const secondPlay = {
      event: 'media.play',
      Account: { title: user },
      Player: { uuid: playerUuid },
      Metadata: {
        type: 'track',
        ratingKey: secondRatingKey,
        title: 'If I Had A Gun...',
        grandparentTitle: 'Noel Gallagher’s High Flying Birds',
        parentTitle: 'Album Two',
        duration: 240000,
        viewOffset: 0,
      },
    };

    const secondStop = {
      ...secondPlay,
      event: 'media.stop',
      Metadata: {
        ...secondPlay.Metadata,
        viewOffset: 233000,
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
          'SELECT plex_rating_key, duration_ms, is_skip FROM play_events WHERE user_plex_id = ? ORDER BY id ASC',
        ).all(user);
      } finally {
        db.close();
      }
    })();

    assert.equal(rows.length, 1);
    assert.equal(Number(rows[0].is_skip || 0), 0);
    assert.ok(Number(rows[0].duration_ms || 0) >= 233000);
    assert.ok(Number(rows[0].duration_ms || 0) <= 233250);
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

  it('allows regular users to access Lidarr automation when user quotas are configured', () => {
    const allowed = canUserAccessLidarrAutomation({
      lidarr: {
        automationEnabled: true,
        automationScope: 'global',
        roleQuotas: {
          user: { weeklyArtists: 1, weeklyAlbums: 2 },
        },
      },
    }, {
      username: 'plain-user',
      role: 'user',
    });
    assert.equal(allowed, true);

    const blocked = canUserAccessLidarrAutomation({
      lidarr: {
        automationEnabled: true,
        automationScope: 'global',
        roleQuotas: {
          user: { weeklyArtists: 0, weeklyAlbums: 0 },
        },
      },
    }, {
      username: 'plain-user',
      role: 'user',
    });
    assert.equal(blocked, false);
  });

  it('resolves Lidarr automation roles across shared Plex token aliases', () => {
    const lidarrService = createLidarrService({
      db: null,
      loadConfig: () => ({
        plex: {
          userServerTokens: {
            emmal142: 'shared-token',
            'emmalesley@hotmail.com': 'shared-token',
          },
        },
      }),
      safeMessage: (err) => String(err?.message || err || ''),
      slugifyId: (value) => String(value || ''),
      pushLog: () => {},
      resolveRole: ({ username, email, title }) => {
        const ids = [username, email, title].map((value) => String(value || '').toLowerCase());
        return ids.includes('emmalesley@hotmail.com') ? 'co-admin' : 'user';
      },
      resolveLocalUsers: () => [],
    });

    assert.equal(lidarrService.resolveAutomationRoleForUserId('emmal142'), 'co-admin');
  });

  it('wraps Lidarr artist-list abort errors without mutating read-only error codes', async () => {
    const lidarrService = createLidarrService({
      db: null,
      loadConfig: () => ({
        lidarr: {
          url: 'http://lidarr.local',
          apiKey: 'lidarr-api-key',
        },
      }),
      safeMessage: (err) => String(err?.message || err || ''),
      slugifyId: (value) => String(value || ''),
      pushLog: () => {},
      resolveRole: () => 'user',
      resolveLocalUsers: () => [],
    });

    const originalFetch = global.fetch;
    global.fetch = async () => {
      const err = new Error('The operation was aborted.');
      err.name = 'AbortError';
      Object.defineProperty(err, 'code', {
        configurable: true,
        enumerable: true,
        get() { return 'UND_ERR_ABORTED'; },
      });
      throw err;
    };

    try {
      await assert.rejects(
        () => lidarrService.listArtists({ timeoutMs: 1 }),
        (err) => {
          assert.equal(err?.message, 'Lidarr timed out while processing the request.');
          assert.equal(err?.code, 'REQUEST_TIMEOUT');
          return true;
        },
      );
    } finally {
      global.fetch = originalFetch;
    }
  });

  it('syncs ListenBrainz generated playlists into Plex using artist and title matching', async () => {
    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const userId = `listenbrainz-user-${Date.now()}`;
    const playlistUuid = '12345678-1234-1234-1234-1234567890ab';
    const logs = [];
    db.prepare(`
      INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name, recording_mbid, genres, library_key, rating_count, view_count, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      'rk-1',
      'Artist Match',
      'Track Match',
      'Matched Album',
      '',
      '[]',
      '1',
      0,
      0,
      Date.now(),
    );
    saveUserPreferences(db, userId, {
      ...getUserPreferences(db, userId),
      listenbrainzUsername: 'lb-user',
      listenbrainzToken: 'lb-token',
      listenbrainzEnabledPlaylists: ['weekly-jams'],
      userWizardCompleted: true,
    });

    const service = createPlaylistService({
      db,
      loadConfig: () => ({
        mediaServer: { type: 'plex' },
        plex: {
          url: 'http://plex.local',
          token: 'plex-admin-token',
          machineId: 'machine-123',
        },
        smartPlaylist: {},
      }),
      saveConfig: () => {},
      buildAppApiUrl: (base, path = '') => new URL(path, `${String(base).replace(/\/?$/, '/')}`),
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      pushLog: (entry) => logs.push(entry),
    });

    const originalFetch = global.fetch;
    const calls = [];
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      calls.push({ url: target, method: String(options.method || 'GET').toUpperCase(), headers: options.headers || {} });
      if (target === 'https://api.listenbrainz.org/1/user/lb-user/playlists/createdfor') {
        assert.equal(options.headers?.Authorization, 'Token lb-token');
        return new Response(JSON.stringify({
          playlists: [
            {
              playlist: {
                identifier: `https://listenbrainz.org/playlist/${playlistUuid}`,
                title: 'Weekly Jams',
                extension: {
                  'https://musicbrainz.org/doc/jspf#playlist': {
                    additional_metadata: {
                      algorithm_metadata: {
                        source_patch: 'weekly-jams',
                      },
                    },
                  },
                },
              },
            },
          ],
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === `https://api.listenbrainz.org/1/playlist/${playlistUuid}`) {
        assert.equal(options.headers?.Authorization, 'Token lb-token');
        return new Response(JSON.stringify({
          playlist: {
            track: [
              {
                title: 'Track Match',
                creator: 'Artist Match',
                extension: {
                  'https://musicbrainz.org/doc/jspf#track': {
                    additional_metadata: {
                      artists: [
                        { artist_credit_name: 'Artist Match' },
                      ],
                    },
                  },
                },
              },
              {
                title: 'Track Miss',
                creator: 'Artist Miss',
              },
            ],
          },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists?playlistType=audio') {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=')) {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [{ ratingKey: 'plex-playlist-1' }] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists/plex-playlist-1/items' && String(options.method || 'GET').toUpperCase() === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-playlist-1/items?uri=')) {
        assert.match(decodeURIComponent(target), /library\/metadata\/rk-1/);
        return new Response('', { status: 200 });
      }
      throw new Error(`Unexpected fetch in ListenBrainz playlist test: ${target}`);
    };

    try {
      await service.syncListenbrainzPlaylists(userId);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const dbVerify = initDb(dbPath);
    try {
      const playlists = listUserGeneratedPlaylists(dbVerify, userId, { activeOnly: false });
      const synced = playlists.find((entry) => entry.playlistKey === 'listenbrainz:weekly-jams');
      assert.ok(synced);
      assert.equal(synced.playlistType, 'listenbrainz-playlist');
      assert.equal(synced.plexPlaylistId, 'plex-playlist-1');
      assert.equal(synced.trackCount, 1);
      assert.equal(synced.algorithmVersion, 'listenbrainz-playlist-v2');
      assert.match(synced.playlistTitle, /ListenBrainz Weekly Jams/);
    } finally {
      dbVerify.close();
    }

    assert.ok(logs.some((entry) => entry?.action === 'sync.discovered' && String(entry?.message || '').includes('matched enabled types')));
    assert.ok(logs.some((entry) => entry?.action === 'playlist.fetched' && String(entry?.message || '').includes('fetched 2 source tracks')));
    assert.ok(logs.some((entry) => String(entry?.message || '').includes('ListenBrainz Weekly Jams synced: 1/2 matched')));
    assert.equal(calls.filter((entry) => entry.url.startsWith('https://api.listenbrainz.org/')).length, 2);
  });

  it('prefers ListenBrainz recording MBIDs over artist and title text matching', async () => {
    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const userId = `listenbrainz-mbid-${Date.now()}`;
    const playlistUuid = '22345678-1234-1234-1234-1234567890ab';
    const logs = [];
    db.prepare(`
      INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name, recording_mbid, genres, library_key, rating_count, view_count, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      'lb-seed-batwam-1',
      'Eminem',
      'The Real Slim Shady',
      'Seed Album',
      '684ca01c-9775-42fe-9cdb-28fa37c27851',
      '[]',
      'listenbrainz-seed',
      0,
      0,
      Date.now(),
    );
    db.prepare(`
      INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name, recording_mbid, genres, library_key, rating_count, view_count, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      'rk-mbid-1',
      'Completely Different Artist Name',
      'Local Library Title Variation',
      'Matched Album',
      '684ca01c-9775-42fe-9cdb-28fa37c27851',
      '[]',
      '1',
      0,
      0,
      Date.now(),
    );
    saveUserPreferences(db, userId, {
      ...getUserPreferences(db, userId),
      listenbrainzUsername: 'lb-user',
      listenbrainzToken: 'lb-token',
      listenbrainzEnabledPlaylists: ['weekly-jams'],
      userWizardCompleted: true,
    });

    const service = createPlaylistService({
      db,
      loadConfig: () => ({
        mediaServer: { type: 'plex' },
        plex: {
          url: 'http://plex.local',
          token: 'plex-admin-token',
          machineId: 'machine-123',
        },
        smartPlaylist: {},
      }),
      saveConfig: () => {},
      buildAppApiUrl: (base, path = '') => new URL(path, `${String(base).replace(/\/?$/, '/')}`),
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      pushLog: (entry) => logs.push(entry),
    });

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target === 'https://api.listenbrainz.org/1/user/lb-user/playlists/createdfor') {
        return new Response(JSON.stringify({
          playlists: [
            {
              playlist: {
                identifier: `https://listenbrainz.org/playlist/${playlistUuid}`,
                title: 'Weekly Jams',
                extension: {
                  'https://musicbrainz.org/doc/jspf#playlist': {
                    additional_metadata: {
                      algorithm_metadata: {
                        source_patch: 'weekly-jams',
                      },
                    },
                  },
                },
              },
            },
          ],
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === `https://api.listenbrainz.org/1/playlist/${playlistUuid}`) {
        return new Response(JSON.stringify({
          playlist: {
            track: [
              {
                title: 'The Real Slim Shady',
                creator: 'Eminem',
                identifier: ['https://musicbrainz.org/recording/684ca01c-9775-42fe-9cdb-28fa37c27851'],
              },
            ],
          },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists?playlistType=audio') {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=')) {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [{ ratingKey: 'plex-playlist-mbid' }] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists/plex-playlist-mbid/items' && String(options.method || 'GET').toUpperCase() === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-playlist-mbid/items?uri=')) {
        assert.doesNotMatch(decodeURIComponent(target), /lb-seed-batwam-1/);
        assert.match(decodeURIComponent(target), /library\/metadata\/rk-mbid-1/);
        return new Response('', { status: 200 });
      }
      throw new Error(`Unexpected fetch in ListenBrainz MBID test: ${target}`);
    };

    try {
      await service.syncListenbrainzPlaylists(userId);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const dbVerify = initDb(dbPath);
    try {
      const playlists = listUserGeneratedPlaylists(dbVerify, userId, { activeOnly: false });
      const synced = playlists.find((entry) => entry.playlistKey === 'listenbrainz:weekly-jams');
      assert.ok(synced);
      assert.equal(synced.trackCount, 1);
      assert.equal(synced.algorithmVersion, 'listenbrainz-playlist-v2');
    } finally {
      dbVerify.close();
    }

    const syncLog = logs.find((entry) => entry?.action === 'playlist.synced');
    assert.equal(syncLog?.meta?.mbidMatched, 1);
    assert.equal(syncLog?.meta?.textMatched, 0);
  });

  it('falls back when ListenBrainz fetch fails with container network errors', async () => {
    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const userId = `listenbrainz-fallback-${Date.now()}`;
    const playlistUuid = '22345678-1234-1234-1234-1234567890ab';
    const logs = [];

    db.prepare(`
      INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name, recording_mbid, genres, library_key, rating_count, view_count, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      'rk-fallback-1',
      'Artist Match',
      'Track Match',
      'Matched Album',
      '',
      '[]',
      '1',
      0,
      0,
      Date.now(),
    );
    saveUserPreferences(db, userId, {
      ...getUserPreferences(db, userId),
      listenbrainzUsername: 'lb-user',
      listenbrainzEnabledPlaylists: ['weekly-jams'],
      userWizardCompleted: true,
    });

    const service = createPlaylistService({
      db,
      loadConfig: () => ({
        mediaServer: { type: 'plex' },
        plex: {
          url: 'http://plex.local',
          token: 'plex-admin-token',
          machineId: 'machine-123',
        },
        smartPlaylist: {},
      }),
      saveConfig: () => {},
      buildAppApiUrl: (base, path = '') => new URL(path, `${String(base).replace(/\/?$/, '/')}`),
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      listenbrainzFetchJsonFallback: async (url) => {
        const target = String(url || '');
        if (target === 'https://api.listenbrainz.org/1/user/lb-user/playlists/createdfor') {
          return {
            playlists: [
              {
                playlist: {
                  identifier: `https://listenbrainz.org/playlist/${playlistUuid}`,
                  title: 'Weekly Jams',
                  extension: {
                    'https://musicbrainz.org/doc/jspf#playlist': {
                      additional_metadata: {
                        algorithm_metadata: {
                          source_patch: 'weekly-jams',
                        },
                      },
                    },
                  },
                },
              },
            ],
          };
        }
        if (target === `https://api.listenbrainz.org/1/playlist/${playlistUuid}`) {
          return {
            playlist: {
              track: [
                {
                  title: 'Track Match',
                  creator: 'Artist Match',
                },
              ],
            },
          };
        }
        throw new Error(`Unexpected fallback request: ${target}`);
      },
      pushLog: (entry) => logs.push(entry),
    });

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target === 'http://plex.local/playlists?playlistType=audio') {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=')) {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [{ ratingKey: 'plex-playlist-fallback' }] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists/plex-playlist-fallback/items' && String(options.method || 'GET').toUpperCase() === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-playlist-fallback/items?uri=')) {
        assert.match(decodeURIComponent(target), /library\/metadata\/rk-fallback-1/);
        return new Response('', { status: 200 });
      }
      const err = new TypeError('fetch failed');
      err.cause = { code: 'ETIMEDOUT' };
      throw err;
    };

    try {
      await service.syncListenbrainzPlaylists(userId);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const dbVerify = initDb(dbPath);
    try {
      const playlists = listUserGeneratedPlaylists(dbVerify, userId, { activeOnly: false });
      const synced = playlists.find((entry) => entry.playlistKey === 'listenbrainz:weekly-jams');
      assert.ok(synced);
    } finally {
      dbVerify.close();
    }

    assert.ok(logs.some((entry) => entry?.action === 'fetch.fallback' && String(entry?.message || '').includes('created-for fetch')));
  });

  it('recreates a ListenBrainz playlist when the stored Plex playlist id is stale', async () => {
    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const userId = `listenbrainz-stale-${Date.now()}`;
    const stalePlaylistId = 'stale-plex-playlist';
    const replacementPlaylistId = 'plex-playlist-new';
    const playlistUuid = '32345678-1234-1234-1234-1234567890ab';
    const logs = [];

    db.prepare(`
      INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name, recording_mbid, genres, library_key, rating_count, view_count, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      'rk-stale-1',
      'Artist Match',
      'Track Match',
      'Matched Album',
      '',
      '[]',
      '1',
      0,
      0,
      Date.now(),
    );
    saveUserPreferences(db, userId, {
      ...getUserPreferences(db, userId),
      listenbrainzUsername: 'lb-user',
      listenbrainzToken: 'lb-token',
      listenbrainzEnabledPlaylists: ['weekly-jams'],
      userWizardCompleted: true,
    });
    saveUserGeneratedPlaylist(db, userId, {
      playlistType: 'listenbrainz-playlist',
      playlistKey: 'listenbrainz:weekly-jams',
      plexPlaylistId: stalePlaylistId,
      playlistTitle: 'ListenBrainz Weekly Jams (MickyGX)',
      algorithmVersion: 'listenbrainz-playlist-v1',
      active: true,
      updatedAt: Date.now(),
    });

    const service = createPlaylistService({
      db,
      loadConfig: () => ({
        mediaServer: { type: 'plex' },
        plex: {
          url: 'http://plex.local',
          token: 'plex-admin-token',
          machineId: 'machine-123',
        },
        smartPlaylist: {},
      }),
      saveConfig: () => {},
      buildAppApiUrl: (base, path = '') => new URL(path, `${String(base).replace(/\/?$/, '/')}`),
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      pushLog: (entry) => logs.push(entry),
    });

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target === 'https://api.listenbrainz.org/1/user/lb-user/playlists/createdfor') {
        return new Response(JSON.stringify({
          playlists: [
            {
              playlist: {
                identifier: `https://listenbrainz.org/playlist/${playlistUuid}`,
                title: 'Weekly Jams',
                extension: {
                  'https://musicbrainz.org/doc/jspf#playlist': {
                    additional_metadata: {
                      algorithm_metadata: {
                        source_patch: 'weekly-jams',
                      },
                    },
                  },
                },
              },
            },
          ],
        }), { status: 200, headers: { 'Content-Type': 'application/json' } });
      }
      if (target === `https://api.listenbrainz.org/1/playlist/${playlistUuid}`) {
        return new Response(JSON.stringify({
          playlist: {
            track: [
              {
                title: 'Track Match',
                creator: 'Artist Match',
              },
            ],
          },
        }), { status: 200, headers: { 'Content-Type': 'application/json' } });
      }
      if (target === `http://plex.local/playlists/${stalePlaylistId}?X-Plex-Container-Start=0&X-Plex-Container-Size=1`) {
        return new Response('', { status: 404 });
      }
      if (target === 'http://plex.local/playlists?playlistType=audio') {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [] },
        }), { status: 200, headers: { 'Content-Type': 'application/json' } });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=')) {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [{ ratingKey: replacementPlaylistId }] },
        }), { status: 200, headers: { 'Content-Type': 'application/json' } });
      }
      if (target === `http://plex.local/playlists/${replacementPlaylistId}/items` && String(options.method || 'GET').toUpperCase() === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith(`http://plex.local/playlists/${replacementPlaylistId}/items?uri=`)) {
        assert.match(decodeURIComponent(target), /library\/metadata\/rk-stale-1/);
        return new Response('', { status: 200 });
      }
      throw new Error(`Unexpected fetch in stale ListenBrainz playlist test: ${target}`);
    };

    try {
      await service.syncListenbrainzPlaylists(userId);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const dbVerify = initDb(dbPath);
    try {
      const playlists = listUserGeneratedPlaylists(dbVerify, userId, { activeOnly: false });
      const synced = playlists.find((entry) => entry.playlistKey === 'listenbrainz:weekly-jams');
      assert.ok(synced);
      assert.equal(synced.plexPlaylistId, replacementPlaylistId);
    } finally {
      dbVerify.close();
    }

    assert.ok(logs.some((entry) => entry?.action === 'generated.stale' && String(entry?.message || '').includes('stale Plex id')));
  });

  it('logs when ListenBrainz sync is skipped because the user has no personal Plex token', async () => {
    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const userId = `listenbrainz-tokenless-${Date.now()}`;
    const logs = [];

    saveUserPreferences(db, userId, {
      ...getUserPreferences(db, userId),
      listenbrainzUsername: 'batwam',
      listenbrainzEnabledPlaylists: ['weekly-jams'],
      userWizardCompleted: true,
    });

    const service = createPlaylistService({
      db,
      loadConfig: () => ({
        mediaServer: { type: 'plex' },
        plex: {
          url: 'http://plex.local',
          token: 'plex-admin-token',
          machineId: 'machine-123',
        },
        smartPlaylist: {},
      }),
      saveConfig: () => {},
      buildAppApiUrl: (base, path = '') => new URL(path, `${String(base).replace(/\/?$/, '/')}`),
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => '',
      userHasOwnPlexToken: () => false,
      pushLog: (entry) => logs.push(entry),
    });

    const originalFetch = global.fetch;
    global.fetch = async () => {
      throw new Error('ListenBrainz fetch should not run without a personal Plex token');
    };

    try {
      await service.syncListenbrainzPlaylists(userId);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    assert.ok(logs.some((entry) =>
      entry?.action === 'sync.skip'
      && String(entry?.message || '').includes('no personal Plex server token is configured'),
    ));
  });

  it('builds Daily Mix using stored settings including artist caps', () => {
    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const userId = `daily-mix-options-${Date.now()}`;

    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('dm-a1', userId, 'Artist A Track 1', 'Artist A', 'Album A', 12, 'belter', 1.5, 0, Date.now());
    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('dm-a2', userId, 'Artist A Track 2', 'Artist A', 'Album A', 10, 'decent', 0.5, 0, Date.now());
    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('dm-b1', userId, 'Artist B Track 1', 'Artist B', 'Album B', 8, 'decent', 0.5, 0, Date.now());

    const service = createPlaylistService({
      db,
      loadConfig: () => ({
        mediaServer: { type: 'plex' },
        smartPlaylist: {
          dailyMix: {
            favoriteLimit: 3,
            suggestedLimit: 0,
            freshLimit: 0,
            maxTracks: 3,
            maxTracksPerArtist: 2,
            repeatCooldownDays: 0,
          },
        },
      }),
    });

    try {
      const playlist = service.buildDailyMix(userId);
      assert.equal(playlist.trackCount, 3);
      assert.equal(playlist.trackKeys.length, 3);
      assert.deepEqual(
        playlist.tracks.map((track) => track.artistName),
        ['Artist A', 'Artist A', 'Artist B'],
      );
      assert.equal(playlist.options.maxTracksPerArtist, 2);
    } finally {
      db.close();
    }
  });

  it('syncs the Curatorr rotating playlist into Plex', async () => {
    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const userId = `curatorr-rotating-${Date.now()}`;

    const now = Date.now();
    const insertMaster = db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMaster.run('ct-a1', 'Artist A', 'Known Favorite', 'Album A', '', '["Rock"]', '1', 20, 0, now);
    insertMaster.run('ct-b1', 'Artist B', 'Steady Favorite', 'Album B', '', '["Rock"]', '1', 10, 0, now);
    insertMaster.run('ct-c1', 'Artist C', 'Fresh Discovery', 'Album C', '', '["Indie"]', '1', 3, 0, now);
    insertMaster.run('ct-d1', 'Artist D', 'Suggested Track', 'Album D', '', '["Synth"]', '1', 4, 0, now);

    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('ct-a1', userId, 'Known Favorite', 'Artist A', 'Album A', 14, 'belter', 1.5, now - 20 * 24 * 60 * 60 * 1000, now);
    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('ct-b1', userId, 'Steady Favorite', 'Artist B', 'Album B', 8, 'decent', 0.5, now - 10 * 24 * 60 * 60 * 1000, now);

    db.prepare(`
      INSERT INTO artist_stats (
        artist_name, user_plex_id, play_count, skip_count, consecutive_skips,
        excluded_from_smart, manually_excluded, manually_included, ranking_score,
        last_played_at, updated_at
      ) VALUES (?, ?, ?, 0, 0, 0, 0, 0, ?, ?, ?)
    `).run('Artist A', userId, 14, 9.2, now - 20 * 24 * 60 * 60 * 1000, now);
    db.prepare(`
      INSERT INTO artist_stats (
        artist_name, user_plex_id, play_count, skip_count, consecutive_skips,
        excluded_from_smart, manually_excluded, manually_included, ranking_score,
        last_played_at, updated_at
      ) VALUES (?, ?, ?, 0, 0, 0, 0, 0, ?, ?, ?)
    `).run('Artist B', userId, 8, 8.1, now - 10 * 24 * 60 * 60 * 1000, now);
    db.prepare(`
      INSERT INTO artist_stats (
        artist_name, user_plex_id, play_count, skip_count, consecutive_skips,
        excluded_from_smart, manually_excluded, manually_included, ranking_score,
        last_played_at, updated_at
      ) VALUES (?, ?, 0, 0, 0, 0, 0, 0, ?, 0, ?)
    `).run('Artist C', userId, 6.3, now);
    db.prepare(`
      INSERT INTO artist_stats (
        artist_name, user_plex_id, play_count, skip_count, consecutive_skips,
        excluded_from_smart, manually_excluded, manually_included, ranking_score,
        last_played_at, updated_at
      ) VALUES (?, ?, 0, 0, 0, 0, 0, 0, ?, 0, ?)
    `).run('Artist D', userId, 7.1, now);

    db.prepare(`
      INSERT INTO suggested_tracks (
        user_plex_id, suggestion_key, rating_key, artist_name, track_title,
        album_name, source, total_score, reason_json, created_at, expires_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(userId, 'curatorr-suggested-1', 'ct-d1', 'Artist D', 'Suggested Track', 'Album D', 'curatorr', 8.5, '{}', now, null);

    const service = createPlaylistService({
      db,
      loadConfig: () => ({
        mediaServer: { type: 'plex' },
        plex: {
          url: 'http://plex.local',
          token: 'plex-admin-token',
          machineId: 'machine-123',
        },
        smartPlaylist: {
          appendUsernameToPlaylistTitles: true,
          artistSkipRank: 2,
          curatorr: {
            targetTracks: 3,
            discoveryRatio: 0.34,
            maxTracksPerArtist: 1,
            repeatCooldownDays: 0,
          },
        },
      }),
      saveConfig: () => {},
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      pushLog: () => {},
    });

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target === 'http://plex.local/playlists?playlistType=audio') {
        return new Response(JSON.stringify({ MediaContainer: { Metadata: [] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=')) {
        return new Response(JSON.stringify({ MediaContainer: { Metadata: [{ ratingKey: 'plex-curatorr-1' }] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists/plex-curatorr-1/items' && String(options.method || 'GET').toUpperCase() === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-curatorr-1/items?uri=')) {
        const decoded = decodeURIComponent(target);
        assert.match(decoded, /library\/metadata\/ct-a1/);
        assert.match(decoded, /(ct-b1|ct-c1|ct-d1)/);
        return new Response('', { status: 200 });
      }
      throw new Error(`Unexpected fetch in Curatorr playlist test: ${target}`);
    };

    try {
      const built = service.buildCuratorr(userId);
      assert.equal(built.trackCount, 3);
      assert.ok(built.sourceBreakdown.discovery >= 1);

      const synced = await service.syncCuratorr(userId);
      assert.equal(synced.trackCount, 3);
      assert.equal(synced.plexPlaylistId, 'plex-curatorr-1');
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const dbVerify = initDb(dbPath);
    try {
      const playlists = listUserGeneratedPlaylists(dbVerify, userId, { activeOnly: false });
      const synced = playlists.find((entry) => entry.playlistKey === 'curatorr');
      assert.ok(synced);
      assert.equal(synced.playlistType, 'curatorr');
      assert.equal(synced.trackCount, 3);
      assert.equal(synced.plexPlaylistId, 'plex-curatorr-1');
    } finally {
      dbVerify.close();
    }
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

  it('prunes stale Curatorr Plex webhook URLs when configuring the Plex webhook', async () => {
    const originalConfig = await readConfig();
    await writeConfig({
      ...originalConfig,
      general: {
        ...(originalConfig.general || {}),
        remoteUrl: 'https://curatorr.example',
      },
      plex: {
        ...(originalConfig.plex || {}),
        url: 'http://plex.local',
        token: 'plex-secret-token',
      },
    });

    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const originalFetch = global.fetch;
    const staleUrl = 'https://old-curatorr.example/webhook/plex?key=stale-secret';
    const unrelatedUrl = 'https://example.com/unrelated-webhook';
    const expectedUrl = `https://curatorr.example/webhook/plex?key=${encodeURIComponent(webhookKey)}`;
    const savedBodies = [];

    global.fetch = async (url, options = {}) => {
      if (String(url) === 'https://plex.tv/api/v2/user/webhooks' && (!options.method || options.method === 'GET')) {
        return new Response(JSON.stringify([staleUrl, unrelatedUrl]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (String(url) === 'https://plex.tv/api/v2/user/webhooks' && options.method === 'POST') {
        savedBodies.push(JSON.parse(String(options.body || '{}')));
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (String(url).startsWith('http://plex.local') && String(url).includes(':/prefs')) {
        return new Response('', { status: 200 });
      }
      return originalFetch(url, options);
    };

    try {
      const webhookRes = await client.postJson('/api/wizard/configure-plex-webhook', {}, '/settings');
      assert.equal(webhookRes.status, 200);
      assert.equal(webhookRes.json?.ok, true);
      assert.equal(Number(webhookRes.json?.pruned || 0), 1);
      assert.equal(savedBodies.length, 1);
      assert.deepEqual(savedBodies[0]?.urls || [], [unrelatedUrl, expectedUrl]);
    } finally {
      global.fetch = originalFetch;
      await writeConfig(originalConfig);
    }
  });

  it('redirects the server wizard token fetch action into Plex auth', async () => {
    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const fetchRes = await client.postForm('/wizard/plex/fetch-token', {
      plexUrl: 'http://plex.local',
      mediaServerType: 'plex',
    }, '/wizard');
    assert.equal(fetchRes.status, 302);
    assert.ok(fetchRes.location.startsWith('/auth/plex?purpose=wizard-server-token&next='));

    const nextConfig = await readConfig();
    assert.equal(nextConfig?.mediaServer?.type, 'plex');
    assert.equal(nextConfig?.plex?.url, 'http://plex.local');
  });

  it('fetches and stores the Plex owner token for the server wizard without replacing the current session user', async () => {
    let currentConfig = {
      mediaServer: { type: 'plex' },
      plex: {
        url: 'http://plex.local',
        token: '',
        machineId: '',
        libraries: [],
        availableLibraries: [],
      },
    };
    const req = {
      session: {
        user: {
          username: 'testadmin',
          source: 'local',
          role: 'admin',
        },
      },
    };

    const result = await completePlexWizardTokenFetch({
      loadConfig: () => currentConfig,
      saveConfig: (nextConfig) => { currentConfig = nextConfig; },
      fetchPlexResources: async (token) => {
        assert.equal(token, 'plex-auth-token');
        return [{
          name: 'Plex Server',
          provides: 'server',
          owned: true,
          clientIdentifier: 'wizard-machine',
          accessToken: 'plex-owner-token',
          connections: [{ uri: 'http://plex.local' }],
        }];
      },
      resolvePlexServerToken: () => 'plex-owner-token',
      resolvePlexMachineIdentifier: () => 'wizard-machine',
      fetchPlexMusicLibraries: async (url, token) => {
        assert.equal(url, 'http://plex.local');
        assert.equal(token, 'plex-owner-token');
        return [{ key: '12', title: 'Music', type: 'artist' }];
      },
      pushLog: () => {},
      safeMessage: (err) => String(err?.message || err || ''),
    }, req, 'plex-auth-token');

    assert.equal(result.ok, true);
    assert.equal(result.step, 3);
    assert.equal(req.session.user?.username, 'testadmin');
    assert.equal(req.session.user?.source, 'local');
    assert.equal(req.session.plexServerToken, 'plex-owner-token');
    assert.equal(currentConfig?.plex?.token, 'plex-owner-token');
    assert.equal(currentConfig?.plex?.machineId, 'wizard-machine');
    assert.equal(currentConfig?.plex?.availableLibraries?.[0]?.key, '12');
  });

  it('stores the Plex owner token for the server wizard when Plex returns multiple servers and only one is owned', async () => {
    let currentConfig = {
      mediaServer: { type: 'plex' },
      plex: {
        url: 'http://192.168.0.20:32400',
        token: '',
        machineId: '',
        libraries: [],
        availableLibraries: [],
      },
    };
    const req = {
      session: {
        user: {
          username: 'testadmin',
          source: 'local',
          role: 'admin',
        },
      },
    };

    const result = await completePlexWizardTokenFetch({
      loadConfig: () => currentConfig,
      saveConfig: (nextConfig) => { currentConfig = nextConfig; },
      fetchPlexResources: async () => ([
        {
          name: 'Remote Shared Server',
          provides: 'server',
          owned: false,
          clientIdentifier: 'shared-machine',
          accessToken: 'shared-token',
          connections: [{ uri: 'http://remote-server:32400' }],
        },
        {
          name: 'Owner Server',
          provides: 'server',
          owned: true,
          clientIdentifier: 'owner-machine',
          accessToken: 'owner-token',
          connections: [{ uri: 'http://plexbox.local:32400' }],
        },
      ]),
      resolvePlexServerToken: (resources, opts) => {
        const match = resources.find((server) => String(server?.clientIdentifier || '') === 'owner-machine');
        return match?.accessToken || '';
      },
      resolvePlexMachineIdentifier: () => 'owner-machine',
      fetchPlexMusicLibraries: async (_url, token) => {
        assert.equal(token, 'owner-token');
        return [{ key: '22', title: 'Music', type: 'artist' }];
      },
      pushLog: () => {},
      safeMessage: (err) => String(err?.message || err || ''),
    }, req, 'plex-auth-token');

    assert.equal(result.ok, true);
    assert.equal(currentConfig?.plex?.token, 'owner-token');
    assert.equal(currentConfig?.plex?.machineId, 'owner-machine');
    assert.equal(currentConfig?.plex?.availableLibraries?.[0]?.key, '22');
  });

  it('stores the global Plex token during first owner login when Plex setup is still incomplete', async () => {
    await writeFile(join(process.env.DATA_DIR, 'admins.json'), JSON.stringify({ admins: ['existing-admin'] }, null, 2));

    const config = await readConfig();
    await writeConfig({
      ...config,
      wizard: { completed: true, completedAt: new Date().toISOString() },
      mediaServer: { ...(config.mediaServer || {}), type: 'plex' },
      plex: {
        ...(config.plex || {}),
        url: 'http://plex.local',
        token: '',
        machineId: '',
        libraries: [],
        availableLibraries: [],
        userServerTokens: {},
      },
    });

    const req = { session: {} };
    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      const token = options?.headers?.['X-Plex-Token'];

      if (target === 'https://plex.tv/api/v2/user') {
        assert.equal(token, 'plex-auth-token');
        return new Response(JSON.stringify({
          id: 'plex-user-1',
          username: 'MickyGX',
          email: 'micky@example.com',
          title: 'MickyGX',
          thumb: '/avatar.png',
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (target.startsWith('https://plex.tv/api/v2/resources')) {
        assert.equal(token, 'plex-auth-token');
        return new Response(JSON.stringify([{
          name: 'Plex Server',
          provides: 'server',
          owned: true,
          clientIdentifier: 'wizard-machine',
          accessToken: 'plex-owner-server-token',
          connections: [{ uri: 'http://plex.local' }],
        }]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      throw new Error('Unexpected fetch in test: ' + target);
    };

    try {
      await completePlexLogin(req, 'plex-auth-token');
    } finally {
      global.fetch = originalFetch;
    }

    assert.equal(req.session.user?.role, 'user');
    assert.equal(req.session.plexServerToken, 'plex-owner-server-token');

    const nextConfig = await readConfig();
    assert.equal(nextConfig?.plex?.token, 'plex-owner-server-token');
    assert.equal(nextConfig?.plex?.machineId, 'wizard-machine');
    assert.equal(nextConfig?.plex?.userServerTokens?.mickygx, 'plex-owner-server-token');
  });

  it('bootstraps Plex libraries and genres on first user wizard load after tokenless setup', async () => {
    runDbStatement('DELETE FROM master_tracks');

    const config = await readConfig();
    await writeConfig({
      ...config,
      wizard: { completed: true, completedAt: new Date().toISOString() },
      mediaServer: { ...(config.mediaServer || {}), type: 'plex' },
      plex: {
        ...(config.plex || {}),
        url: 'http://plex.local',
        token: '',
        machineId: '',
        libraries: [],
        availableLibraries: [],
        userServerTokens: {
          ...((config.plex && config.plex.userServerTokens) || {}),
          testadmin: 'plex-user-token',
        },
      },
    });

    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target.startsWith(baseUrl)) return originalFetch(url, options);

      const headers = options?.headers || {};
      const token = typeof headers.get === 'function' ? headers.get('X-Plex-Token') : headers['X-Plex-Token'];
      assert.equal(token, 'plex-user-token');

      if (target === 'http://plex.local' || target === 'http://plex.local/') {
        return new Response(JSON.stringify({
          MediaContainer: { machineIdentifier: 'wizard-machine' },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (target === 'http://plex.local/library/sections' || target.startsWith('http://plex.local/library/sections?')) {
        return new Response(JSON.stringify({
          MediaContainer: {
            Directory: [{ key: '12', title: 'Music', type: 'artist', agent: 'tv.plex.agents.music' }],
          },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (target.startsWith('http://plex.local/library/sections/12/all?')) {
        return new Response(JSON.stringify({
          MediaContainer: {
            totalSize: 1,
            Metadata: [{
              ratingKey: 'track-1',
              originalTitle: 'Bootstrap Artist',
              title: 'Bootstrap Song',
              parentTitle: 'Bootstrap Album',
              Genre: [{ tag: 'Rock' }],
              ratingCount: 0,
              viewCount: 0,
            }],
          },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (target.startsWith('http://plex.local/library/sections/12/mood?')) {
        return new Response(JSON.stringify({
          MediaContainer: { Directory: [] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      throw new Error('Unexpected fetch in test: ' + target);
    };

    try {
      const wizardRes = await client.request('/wizard/user');
      assert.equal(wizardRes.status, 200);
      assert.ok(!wizardRes.text.includes('No genres found yet'));
    } finally {
      global.fetch = originalFetch;
    }

    const nextConfig = await readConfig();
    assert.equal(nextConfig?.plex?.token, 'plex-user-token');
    assert.equal(nextConfig?.plex?.machineId, 'wizard-machine');
    assert.deepEqual(nextConfig?.plex?.libraries, ['12']);
    assert.equal(nextConfig?.plex?.availableLibraries?.[0]?.key, '12');

    const masterRow = readDbRow('SELECT COUNT(*) AS count FROM master_tracks');
    assert.ok(Number(masterRow?.count || 0) > 0);
  });
});
