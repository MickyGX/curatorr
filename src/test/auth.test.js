import { after, afterEach, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { deriveHistoryTier, resolveUserFilter, summarizeAdminLidarrCounts, summarizeAdminPlaylistCounts } from '../routes/pages.js';

const testDir = join(tmpdir(), `curatorr-test-${process.pid}`);
process.env.CONFIG_PATH = join(testDir, 'config.json');
process.env.DATA_DIR = join(testDir, 'data');
process.env.SESSION_SECRET = 'test-secret-do-not-use-in-prod';
process.env.PLEX_CLIENT_ID = 'test-client-id';
process.env.CURATORR_DISABLE_AUTOSTART = '1';
process.env.PORT = String(37000 + (process.pid % 1000));
process.env.SPOTIFY_CLIENT_ID = 'test-spotify-client-id';
process.env.SPOTIFY_CLIENT_SECRET = 'test-spotify-client-secret';

const baseUrl = `http://127.0.0.1:${process.env.PORT}`;

const { start, stop, canUserAccessLidarrAutomation, cleanupSetupAdminMusicState, completePlexLogin } = await import('../index.js');
const {
  countTracksMissingEnrichment,
  countTracksMissingPlexLoudness,
  createUserPersonalPlaylist,
  findUserPersonalPlaylistByName,
  getAllLastfmTags,
  getAllTrackDecadeTags,
  getAllUserIds,
  getLidarrArtistProgress,
  getSuggestedArtist,
  getMasterTracks,
  getPlaylistTracks,
  getTrackEnrichmentByRatingKeys,
  initDb,
  listLidarrRequests,
  listTracksMissingPlexLoudness,
  purgeUserScopedMusicData,
  getUserPreferences,
  rebuildArtistStatsFromEvents,
  rebuildTrackStatsFromEvents,
  refreshMasterTracks,
  saveLidarrArtistProgress,
  saveArtistTags,
  setPlaylistTracks,
  saveUserPreferences,
  upsertSuggestedArtist,
  listUserGeneratedPlaylists,
  previewGlobalPlaylist,
  saveUserGeneratedPlaylist,
} = await import('../db.js');
const { createJobService } = await import('../services/jobs.js');
const { createLidarrService } = await import('../services/lidarr.js');
const { applyFeaturePresetFilters, applyTrackFilters, applyTrackFiltersWithReport, createPlaylistService, sortPlaylistTracksByAnalysis } = await import('../services/playlists.js');
const { createTrackEnrichmentService } = await import('../services/track-enrichment.js');
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

async function waitForDbRow(sql, predicate, params = [], { timeoutMs = 1000, intervalMs = 20 } = {}) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const row = readDbRow(sql, ...params);
    if (predicate(row)) return row;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  return readDbRow(sql, ...params);
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

  it('POST /login with correct credentials redirects to /overview', async () => {
    const { response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);
    assert.ok(response.location.includes('/overview'));
  });

  it('derives history tier from listened duration instead of a stale skip flag', () => {
    const tier = deriveHistoryTier({
      duration_ms: 160000,
      track_duration_ms: 180000,
      is_skip: 1,
      current_tier: 'skip',
      current_excluded: 0,
      current_force_included: 0,
    }, {
      smartPlaylist: {
        skipThresholdSeconds: 30,
        completionThresholdSeconds: 30,
      },
    });

    assert.deepEqual(tier, {
      key: 'belter',
      label: 'Belter',
      tone: 'belter',
    });
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

      res = await client.request('/api/plex/pin', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pinId: '123456' }),
      });
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
      assert.match(res.text, /(metadata\.plex\.tv|\/auth\/plex\/home-users\/avatar\/(?:[^"'\/]+\/)?home-kid-1)/);
    } finally {
      global.fetch = originalFetch;
    }
  });

  it('routes protected Plex Home users with string flags to the PIN screen', async () => {
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
          {
            id: 'home-owner',
            title: 'Owner',
            protected: '0',
            admin: '1',
            thumb: 'https://metadata.plex.tv/home-owner',
          },
          {
            id: 'home-protected',
            title: 'Protected Kid',
            protected: '1',
            admin: '0',
            thumb: 'https://metadata.plex.tv/protected-kid',
          },
        ]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in protected home-user test: ${target}`);
    };

    try {
      let res = await client.request('/auth/plex');
      assert.equal(res.status, 200);

      res = await client.request('/api/plex/pin', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pinId: '123456' }),
      });
      assert.equal(res.status, 200);
      assert.equal(res.json?.ok, true);

      res = await client.request('/oauth/callback');
      assert.equal(res.status, 302);
      assert.equal(res.location, '/auth/plex/home-users');

      const selectionPage = await client.request('/auth/plex/home-users');
      assert.equal(selectionPage.status, 200);
      const csrfToken = extractCsrfToken(selectionPage.text);
      assert.ok(csrfToken, 'Expected CSRF token on home user selector');

      const selectRes = await client.request('/auth/plex/home-users/select', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ _csrf: csrfToken, userId: 'home-protected' }),
      });
      assert.equal(selectRes.status, 302);
      assert.equal(selectRes.location, '/auth/plex/home-users/pin');

      const pinPage = await client.request('/auth/plex/home-users/pin');
      assert.equal(pinPage.status, 200);
      assert.match(pinPage.text, /Enter your PIN to continue/);
      assert.match(pinPage.text, /(metadata\.plex\.tv|\/auth\/plex\/home-users\/avatar\/(?:[^"'\/]+\/)?home-protected)/);
    } finally {
      global.fetch = originalFetch;
    }
  });

  it('routes protected Plex Home admins to the PIN screen', async () => {
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
          {
            id: 'home-owner',
            title: 'Owner',
            protected: '1',
            admin: '1',
            thumb: 'https://metadata.plex.tv/home-owner',
          },
          {
            id: 'home-kid',
            title: 'Kid',
            protected: '0',
            admin: '0',
            thumb: 'https://metadata.plex.tv/home-kid',
          },
        ]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in protected home-admin test: ${target}`);
    };

    try {
      let res = await client.request('/auth/plex');
      assert.equal(res.status, 200);

      res = await client.request('/api/plex/pin', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pinId: '123456' }),
      });
      assert.equal(res.status, 200);
      assert.equal(res.json?.ok, true);

      res = await client.request('/oauth/callback');
      assert.equal(res.status, 302);
      assert.equal(res.location, '/auth/plex/home-users');

      const selectionPage = await client.request('/auth/plex/home-users');
      assert.equal(selectionPage.status, 200);
      const csrfToken = extractCsrfToken(selectionPage.text);
      assert.ok(csrfToken, 'Expected CSRF token on home user selector');

      const selectRes = await client.request('/auth/plex/home-users/select', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ _csrf: csrfToken, userId: 'home-owner' }),
      });
      assert.equal(selectRes.status, 302);
      assert.equal(selectRes.location, '/auth/plex/home-users/pin');
    } finally {
      global.fetch = originalFetch;
    }
  });

  it('renders Plex Home avatar proxy URLs even when thumbs need fallback lookup', async () => {
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
          {
            id: 'home-owner',
            title: 'Owner',
            protected: false,
            admin: true,
            avatarUrl: '',
          },
          {
            id: 'home-kid-1',
            title: 'Kid One',
            protected: false,
            admin: false,
            thumb: '',
          },
        ]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'https://plex.tv/api/users') {
        return new Response(
          '<MediaContainer>'
          + '<User id="home-owner" title="Owner" thumb="https://metadata.plex.tv/home-owner-avatar" />'
          + '<User id="home-kid-1" title="Kid One" thumb="https://metadata.plex.tv/home-kid-avatar" />'
          + '</MediaContainer>',
          {
            status: 200,
            headers: { 'Content-Type': 'application/xml' },
          }
        );
      }
      throw new Error(`Unexpected fetch in home avatar proxy test: ${target}`);
    };

    try {
      let res = await client.request('/auth/plex');
      assert.equal(res.status, 200);

      res = await client.request('/api/plex/pin', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pinId: '123456' }),
      });
      assert.equal(res.status, 200);
      assert.equal(res.json?.ok, true);

      res = await client.request('/oauth/callback');
      assert.equal(res.status, 302);
      assert.equal(res.location, '/auth/plex/home-users');

      const selectorRes = await client.request('/auth/plex/home-users');
      assert.equal(selectorRes.status, 200);
      assert.match(selectorRes.text, /(home-owner-avatar|\/auth\/plex\/home-users\/avatar\/(?:[^"'\/]+\/)?home-owner)/);
      assert.match(selectorRes.text, /(home-kid-avatar|\/auth\/plex\/home-users\/avatar\/(?:[^"'\/]+\/)?home-kid-1)/);
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

      res = await client.request('/api/plex/pin', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pinId: '123456' }),
      });
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

  it('lets a signed-in local admin complete Plex Home user selection and switch into Plex', async () => {
    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

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
      throw new Error(`Unexpected fetch in local-admin home-user switch test: ${target}`);
    };

    try {
      let res = await client.request('/auth/plex');
      assert.equal(res.status, 200);

      res = await client.request('/api/plex/pin', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pinId: '123456' }),
      });
      assert.equal(res.status, 200);
      assert.equal(res.json?.ok, true);

      res = await client.request('/oauth/callback');
      assert.equal(res.status, 302);
      assert.equal(res.location, '/auth/plex/home-users');

      const selectionPage = await client.request('/auth/plex/home-users');
      assert.equal(selectionPage.status, 200);
      const csrfToken = extractCsrfToken(selectionPage.text);
      assert.ok(csrfToken, 'Expected CSRF token on home user selector for local admin');

      const selectRes = await client.request('/auth/plex/home-users/select', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ _csrf: csrfToken, userId: 'home-kid-2' }),
      });
      assert.equal(selectRes.status, 302);
      assert.match(selectRes.location, /^\/wizard(?:\/user)?$/);

      const dashboardRes = await client.request('/dashboard');
      assert.equal(dashboardRes.status, 302);
      assert.match(dashboardRes.location, /^\/wizard(?:\/user)?$/);
    } finally {
      global.fetch = originalFetch;
    }
  });
});

describe('page scoping', () => {
  it('includes playlist-only owners in scheduled user discovery', () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-scheduler-users-${Date.now()}.db`);
    const db = initDb(dbPath);
    const personalOwner = 'playlist-only-personal';
    const generatedOwner = 'playlist-only-generated';

    try {
      createUserPersonalPlaylist(db, personalOwner, {
        id: 'pp_scheduler',
        name: 'Scheduler Personal',
        rules: { rebuildSchedule: 'daily' },
      });
      saveUserGeneratedPlaylist(db, generatedOwner, {
        playlistType: 'daily-mix',
        playlistKey: 'daily-mix',
        playlistTitle: 'Daily Mix',
        plexPlaylistId: 'plex-daily-mix',
        active: true,
      });

      const userIds = getAllUserIds(db);
      assert.ok(userIds.includes(personalOwner));
      assert.ok(userIds.includes(generatedOwner));
    } finally {
      db.close();
    }
  });

  it('keeps local admin accounts on the global activity view', () => {
    const filter = resolveUserFilter({ username: 'admin', source: 'local' }, 'admin');
    assert.equal(filter, '');
  });

  it('scopes Plex-backed admin accounts to their Plex username', () => {
    const filter = resolveUserFilter({ username: 'MickyGX', source: 'plex' }, 'admin');
    assert.equal(filter, 'MickyGX');
  });

  it('counts synced personal playlists once on the admin users page', () => {
    const counts = summarizeAdminPlaylistCounts([
      { playlistKey: 'curatorr', playlistType: 'curatorr', active: true },
      { playlistKey: 'crescive', playlistType: 'crescive', active: true },
      { playlistKey: 'curative', playlistType: 'curative', active: true },
      { playlistKey: 'daily-mix', playlistType: 'daily-mix', active: true },
      { playlistKey: 'personal:pp_britpop', playlistType: 'personal', active: true },
      { playlistKey: 'personal:pp_soul', playlistType: 'personal', active: true },
    ], [
      { id: 'pp_britpop', rules: {} },
      { id: 'pp_soul', rules: {} },
    ]);

    assert.equal(counts.playlistTotalCount, 6);
    assert.equal(counts.systemPlaylistCount, 4);
    assert.equal(counts.userPlaylistCount, 2);
    assert.equal(counts.draftPlaylistCount, 0);
  });

  it('includes unsynced personal drafts in admin users playlist counts', () => {
    const counts = summarizeAdminPlaylistCounts([
      { playlistKey: 'curatorr', playlistType: 'curatorr', active: true },
    ], [
      { id: 'pp_draft', rules: {} },
    ]);

    assert.equal(counts.playlistTotalCount, 2);
    assert.equal(counts.systemPlaylistCount, 1);
    assert.equal(counts.userPlaylistCount, 1);
    assert.equal(counts.draftPlaylistCount, 1);
  });

  it('aggregates Lidarr usage across identity aliases without counting progress placeholders', () => {
    const counts = summarizeAdminLidarrCounts([
      { usageKey: 'artists', total: 1 },
      { usageKey: 'albums', total: 1 },
      { usageKey: 'artists', total: 2 },
      { usageKey: 'albums', total: 1 },
      { usageKey: 'tracks', total: 24 },
      { usageKey: 'auto_artists', total: 1 },
    ]);

    assert.equal(counts.artistsAdded, 3);
    assert.equal(counts.albumsAdded, 2);
    assert.equal(counts.tracksAdded, 24);
  });

  it('keeps Lidarr tracks blank when no tracked usage rows exist yet', () => {
    const counts = summarizeAdminLidarrCounts([
      { usageKey: 'artists', total: 1 },
      { usageKey: 'albums', total: 1 },
    ]);

    assert.equal(counts.artistsAdded, 1);
    assert.equal(counts.albumsAdded, 1);
    assert.equal(counts.tracksAdded, null);
  });
});

describe('user settings integrations', () => {
  it('completes Spotify OAuth even if the browser loses the Curatorr session before callback', async () => {
    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const connect = await client.request('/user-settings/spotify/connect');
    assert.equal(connect.status, 302);
    const authorizeUrl = new URL(connect.location);
    assert.equal(authorizeUrl.origin, 'https://accounts.spotify.com');
    const state = authorizeUrl.searchParams.get('state');
    assert.ok(state, 'expected Spotify authorize URL to include state');

    const originalFetch = globalThis.fetch;
    globalThis.fetch = async (url, options) => {
      const target = String(url || '');
      if (target === 'https://accounts.spotify.com/api/token') {
        const body = new URLSearchParams(String(options?.body || ''));
        assert.equal(body.get('grant_type'), 'authorization_code');
        assert.equal(body.get('code'), 'spotify-code');
        assert.equal(body.get('redirect_uri'), `${baseUrl}/user-settings/spotify/callback`);
        return new Response(JSON.stringify({
          access_token: 'spotify-access-token',
          refresh_token: 'spotify-refresh-token',
          token_type: 'Bearer',
          expires_in: 3600,
        }), { status: 200, headers: { 'Content-Type': 'application/json' } });
      }
      if (target === 'https://api.spotify.com/v1/me') {
        return new Response(JSON.stringify({
          id: 'spotify-user-1',
          display_name: 'Spotify User',
        }), { status: 200, headers: { 'Content-Type': 'application/json' } });
      }
      return originalFetch(url, options);
    };

    try {
      const callbackClient = createClient();
      const callback = await callbackClient.request(`/user-settings/spotify/callback?code=spotify-code&state=${encodeURIComponent(state)}`);
      assert.equal(callback.status, 302);
      assert.equal(callback.location, '/user-settings?success=spotify-connected');
    } finally {
      globalThis.fetch = originalFetch;
    }

    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    try {
      const prefs = getUserPreferences(db, 'testadmin');
      assert.equal(prefs.spotifyUserId, 'spotify-user-1');
      assert.equal(prefs.spotifyDisplayName, 'Spotify User');
      assert.equal(prefs.spotifyAccessToken, 'spotify-access-token');
      assert.equal(prefs.spotifyRefreshToken, 'spotify-refresh-token');
      assert.ok(prefs.spotifyTokenExpiresAt > Date.now());
    } finally {
      db.close();
    }
  });

  it('saves ListenBrainz account settings and clears legacy playlist toggles', async () => {
    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const page = await client.request('/user-settings');
    const csrfToken = extractCsrfToken(page.text);
    assert.ok(csrfToken, 'Expected CSRF token on /user-settings');

    const form = new URLSearchParams();
    form.set('_csrf', csrfToken);
    form.set('listenbrainzUsername', 'lb-user');
    form.set('listenbrainzToken', 'lb-token');
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
      assert.deepEqual(prefs.listenbrainzEnabledPlaylists, []);
    } finally {
      db.close();
    }
  });

  it('saves Last.fm username settings and clears legacy station toggles', async () => {
    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const page = await client.request('/user-settings');
    const csrfToken = extractCsrfToken(page.text);
    assert.ok(csrfToken, 'Expected CSRF token on /user-settings');

    const form = new URLSearchParams();
    form.set('_csrf', csrfToken);
    form.set('lastfmUsername', 'last-user');
    const res = await client.request('/user-settings/lastfm', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: form,
    });
    assert.equal(res.status, 302);
    assert.equal(res.location, '/user-settings?success=lastfm-updated');

    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    try {
      const prefs = getUserPreferences(db, 'testadmin');
      assert.equal(prefs.lastfmUsername, 'last-user');
      assert.deepEqual(prefs.lastfmEnabledStations, []);
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

  it('does not let Tautulli gap-fill overwrite a play already recorded by Plex when guarded repair is disabled', async () => {
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

  it('does not let nearby Tautulli refinement overwrite a Plex play when guarded repair is disabled', async () => {
    const user = `plex-user-${Date.now()}`;
    const ratingKey = `plex-track-${Date.now()}`;
    const startedAt = Date.now() - 20 * 60 * 1000;
    const endedAt = startedAt + 20000;
    const tautulliStartedAt = startedAt + 6 * 60 * 1000;

    runDbStatement(
      `INSERT INTO play_events (
        user_plex_id, plex_rating_key, track_title, artist_name, album_name,
        started_at, ended_at, duration_ms, track_duration_ms, is_skip, event_source, session_key
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      user,
      ratingKey,
      'Nearby Plex Song',
      'Nearby Plex Artist',
      'Nearby Plex Album',
      startedAt,
      endedAt,
      20000,
      240000,
      1,
      'plex_webhook',
      `plex-nearby-${user}-${ratingKey}`,
    );

    const config = await readConfig();
    const originalFetch = global.fetch;
    global.fetch = async () => new Response(JSON.stringify({
      response: {
        data: {
          data: [{
            media_type: 'track',
            user,
            rating_key: ratingKey,
            started: Math.floor(tautulliStartedAt / 1000),
            stopped: Math.floor((tautulliStartedAt + 220000) / 1000),
            play_duration: 220,
            full_duration: 240,
            title: 'Nearby Plex Song',
            original_title: 'Nearby Plex Artist',
            parent_title: 'Nearby Plex Album',
            section_id: '1',
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
        loadConfig: () => ({ ...config, tautulli: { ...config.tautulli, enableHistoryRepair: false } }),
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
      'SELECT duration_ms, is_skip, event_source FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ? ORDER BY id DESC LIMIT 1',
      user,
      ratingKey,
    );
    assert.ok(eventRow);
    assert.equal(eventRow.event_source, 'plex_webhook');
    assert.equal(Number(eventRow.duration_ms || 0), 20000);
    assert.equal(Number(eventRow.is_skip || 0), 1);
  });

  it('guardedly repairs a shorter Plex play when Tautulli shows a longer completed listen', async () => {
    const user = `plex-user-${Date.now()}`;
    const ratingKey = `plex-track-${Date.now()}`;
    const startedAt = Date.now() - 10 * 60 * 1000;
    const endedAt = startedAt + 20000;

    runDbStatement(
      `INSERT INTO play_events (
        user_plex_id, plex_rating_key, track_title, artist_name, album_name,
        started_at, ended_at, duration_ms, track_duration_ms, is_skip, event_source, session_key
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      user,
      ratingKey,
      'Repair Plex Song',
      'Repair Plex Artist',
      'Repair Plex Album',
      startedAt,
      endedAt,
      20000,
      240000,
      1,
      'plex_webhook',
      `plex-repair-${user}-${ratingKey}`,
    );

    const config = await readConfig();
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
            title: 'Repair Plex Song',
            original_title: 'Repair Plex Artist',
            parent_title: 'Repair Plex Album',
            section_id: '1',
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
        loadConfig: () => ({ ...config, tautulli: { ...config.tautulli, enableHistoryRepair: true } }),
        pushLog: () => {},
        safeMessage: (err) => String(err?.message || err || ''),
      });
      assert.equal(result.inserted, 1);
      assert.equal(result.skipped, 0);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const eventRow = readDbRow(
      'SELECT duration_ms, is_skip, event_source, track_duration_ms FROM play_events WHERE user_plex_id = ? AND plex_rating_key = ? ORDER BY id DESC LIMIT 1',
      user,
      ratingKey,
    );
    assert.ok(eventRow);
    assert.equal(eventRow.event_source, 'plex_webhook');
    assert.equal(Number(eventRow.duration_ms || 0), 220000);
    assert.equal(Number(eventRow.track_duration_ms || 0), 240000);
    assert.equal(Number(eventRow.is_skip || 0), 0);

    const trackRow = readDbRow(
      'SELECT tier, play_count, skip_count FROM track_stats WHERE user_plex_id = ? AND plex_rating_key = ?',
      user,
      ratingKey,
    );
    assert.ok(trackRow);
    assert.equal(trackRow.tier, 'belter');
    assert.equal(Number(trackRow.play_count || 0), 1);
    assert.equal(Number(trackRow.skip_count || 0), 0);
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

  it('treats a same-track Plex play from the start as a fresh replay instead of resuming old progress', async () => {
    const client = createClient();
    const user = `plex-restart-${Date.now()}`;
    const ratingKey = `plex-restart-track-${Date.now()}`;
    const playerUuid = `restart-player-${Date.now()}`;

    const events = [
      {
        event: 'media.play',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Restart Song',
          grandparentTitle: 'Restart Artist',
          parentTitle: 'Restart Album',
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
          title: 'Restart Song',
          grandparentTitle: 'Restart Artist',
          parentTitle: 'Restart Album',
          duration: 240000,
          viewOffset: 180000,
        },
      },
      {
        event: 'media.play',
        Account: { title: user },
        Player: { uuid: playerUuid },
        Metadata: {
          type: 'track',
          ratingKey,
          title: 'Restart Song',
          grandparentTitle: 'Restart Artist',
          parentTitle: 'Restart Album',
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
          title: 'Restart Song',
          grandparentTitle: 'Restart Artist',
          parentTitle: 'Restart Album',
          duration: 240000,
          viewOffset: 60000,
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
    assert.equal(Number(rows[0].duration_ms || 0), 240000);
    assert.equal(Number(rows[0].is_skip || 0), 0);
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
      defaultMetadataProfileId: '4',
      defaultQualityProfileId: '7',
      newArtistMonitoringMode: 'latest',
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
    assert.equal(Number(config?.lidarr?.defaultMetadataProfileId || 0), 4);
    assert.equal(Number(config?.lidarr?.defaultQualityProfileId || 0), 7);
    assert.equal(Number(config?.lidarr?.autoAddQuotas?.weeklyArtists), 1);
    assert.equal(Number(config?.lidarr?.autoAddQuotas?.weeklyAlbums), 1);
    assert.equal(config?.lidarr?.newArtistMonitoringMode, 'latest');
  });

  it('uses the configured new-artist monitoring mode in the Lidarr add payload', async () => {
    const lidarrService = createLidarrService({
      db: null,
      loadConfig: () => ({
        lidarr: {
          url: 'http://lidarr.local',
          apiKey: 'lidarr-api-key',
          defaultMetadataProfileId: 4,
          defaultQualityProfileId: 7,
          newArtistMonitoringMode: 'latest',
        },
      }),
      safeMessage: (err) => String(err?.message || err || ''),
      slugifyId: (value) => String(value || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, ''),
      pushLog: () => {},
      resolveRole: () => 'user',
      resolveLocalUsers: () => [],
    });

    const originalFetch = global.fetch;
    let createdArtistPayload = null;
    global.fetch = async (url, init = {}) => {
      const target = String(url || '');
      if (target === 'http://lidarr.local/api/v1/rootfolder') {
        return new Response(JSON.stringify([
          { path: '/music', accessible: true, defaultQualityProfileId: 7, defaultMetadataProfileId: 4 },
        ]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/qualityprofile') {
        return new Response(JSON.stringify([
          { id: 7, name: 'HQ' },
        ]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/metadataprofile') {
        return new Response(JSON.stringify([
          { id: 4, name: 'Albums and EP' },
        ]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/artist' && String(init.method || 'GET').toUpperCase() === 'POST') {
        createdArtistPayload = JSON.parse(String(init.body || '{}'));
        return new Response(JSON.stringify({
          id: 42,
          artistName: 'The National',
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/artist/42') {
        return new Response(JSON.stringify({
          id: 42,
          artistName: 'The National',
          monitored: true,
          monitorNewItems: 'latest',
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in new-artist monitoring mode test: ${target}`);
    };

    try {
      const result = await lidarrService.addArtistFromSuggestion('The National', {
        lookupArtistResult: {
          artistName: 'The National',
          foreignArtistId: 'mbid-the-national',
          folder: 'the-national',
        },
      });
      assert.equal(result.created, true);
      assert.equal(createdArtistPayload?.metadataProfileId, 4);
      assert.equal(createdArtistPayload?.qualityProfileId, 7);
      assert.equal(createdArtistPayload?.monitorNewItems, 'latest');
      assert.equal(createdArtistPayload?.addOptions?.monitor, 'latest');
      assert.equal(createdArtistPayload?.monitored, true);
    } finally {
      global.fetch = originalFetch;
    }
  });

  it('allows the default weekly Last.fm tag sync interval on the jobs page', async () => {
    const originalConfig = await readConfig();
    await writeConfig({
      ...originalConfig,
      tautulli: {
        ...(originalConfig.tautulli || {}),
        enableHistoryRepair: false,
      },
      jobs: {
        ...(originalConfig.jobs || {}),
        lastfmTagSync: { intervalMinutes: 10080, enabled: true },
      },
    });

    try {
      const { client, response } = await login('testadmin', 'TestPassword1!');
      assert.equal(response.status, 302);

      const page = await client.request('/settings?tab=jobs');
      assert.equal(page.status, 200);
      assert.match(page.text, /name="lastfmTagSync_interval" value="10080" min="1" max="10080"/);
      assert.match(page.text, /name="tautulli_enableHistoryRepair" value="1"/);

      const saveRes = await client.postForm('/settings/jobs', {
        lastfmTagSync_interval: '10080',
        lastfmTagSync_enabled: '1',
        tautulli_enableHistoryRepair: '1',
      }, '/settings?tab=jobs');
      assert.equal(saveRes.status, 302);

      const nextConfig = await readConfig();
      assert.equal(Number(nextConfig.jobs?.lastfmTagSync?.intervalMinutes || 0), 10080);
      assert.equal(Boolean(nextConfig.tautulli?.enableHistoryRepair), true);
    } finally {
      await writeConfig(originalConfig);
    }
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

  it('blocks the local setup admin from Lidarr automation even when admin quotas are enabled', () => {
    const blocked = canUserAccessLidarrAutomation({
      users: [{
        username: 'testadmin',
        email: 'test@curatorr.test',
        role: 'admin',
        passwordHash: 'hash',
        salt: 'salt',
        createdBy: 'setup',
        setupAccount: true,
      }],
      lidarr: {
        automationEnabled: true,
        automationScope: 'global',
        roleQuotas: {
          admin: { weeklyArtists: -1, weeklyAlbums: -1 },
        },
      },
    }, {
      username: 'testadmin',
      email: 'test@curatorr.test',
      role: 'admin',
      source: 'local',
      setupAccount: true,
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

  it('treats the setup admin identity as disabled for background Lidarr automation', () => {
    const lidarrService = createLidarrService({
      db: null,
      loadConfig: () => ({
        users: [{
          username: 'testadmin',
          email: 'test@curatorr.test',
          role: 'admin',
          passwordHash: 'hash',
          salt: 'salt',
          createdBy: 'setup',
          setupAccount: true,
        }],
      }),
      safeMessage: (err) => String(err?.message || err || ''),
      slugifyId: (value) => String(value || ''),
      pushLog: () => {},
      resolveRole: () => 'admin',
      resolveLocalUsers: (config) => config.users.map((user) => ({
        ...user,
        isSetupAdmin: true,
      })),
    });

    assert.equal(lidarrService.resolveAutomationRoleForUserId('testadmin'), 'disabled');
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

  it('falls back to track listing when Lidarr album payload omits track counts', async () => {
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
    global.fetch = async (url) => {
      const target = String(url || '');
      if (target === 'http://lidarr.local/api/v1/album/42') {
        return new Response(JSON.stringify({
          id: 42,
          title: 'No Count Album',
          statistics: { trackFileCount: 0 },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/track?albumId=42') {
        return new Response(JSON.stringify([
          { title: 'One', trackNumber: 1, mediumNumber: 1 },
          { title: 'Two', trackNumber: 2, mediumNumber: 1 },
          { title: 'Three', trackNumber: 3, mediumNumber: 1 },
        ]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in test: ${target}`);
    };

    try {
      const trackCount = await lidarrService.getAlbumTrackCount({ id: 42 });
      assert.equal(trackCount, 3);
    } finally {
      global.fetch = originalFetch;
    }
  });

  it('stops artist progression when the tracked album is already present in the local library', async () => {
    const dbPath = join(process.env.DATA_DIR, `lidarr-progress-${Date.now()}.db`);
    const db = initDb(dbPath);
    const userId = `lidarr-progress-${Date.now()}`;
    const artistName = 'Passion Pit';
    const now = Date.now();

    db.prepare(`
      INSERT INTO artist_stats (artist_name, user_plex_id, ranking_score, updated_at)
      VALUES (?, ?, ?, ?)
    `).run(artistName, userId, 10, now);
    db.prepare(`
      INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name, recording_mbid, genres, library_key, rating_count, view_count, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      'rk-lidarr-progress-1',
      artistName,
      'Moth\'s Wings',
      'Manners',
      '',
      '[]',
      '1',
      0,
      0,
      now,
    );
    upsertSuggestedArtist(db, userId, {
      artistName,
      status: 'added_to_lidarr',
      lidarrArtistId: 358,
      reason: {
        latestAlbum: {
          albumId: 42,
          albumTitle: 'Manners',
          commandId: 99,
          sourceKind: 'manual',
          addedByCuratorr: true,
        },
      },
    });
    saveLidarrArtistProgress(db, userId, {
      artistName,
      lidarrArtistId: 358,
      currentStage: 'catalog_expanded',
      albumsAddedCount: 1,
      lastAlbumAddedAt: now - (2 * 24 * 60 * 60 * 1000),
      nextReviewAt: now - 1000,
      highestObservedRank: 10,
      lastManualSearchAt: now - (60 * 60 * 1000),
      lastManualSearchStatus: 'started',
      updatedAt: now - 1000,
    });

    const lidarrService = createLidarrService({
      db,
      loadConfig: () => ({
        lidarr: {
          url: 'http://lidarr.local',
          apiKey: 'lidarr-api-key',
          automationEnabled: true,
          automationScope: 'global',
          roleQuotas: {
            user: { weeklyArtists: 10, weeklyAlbums: 10 },
          },
        },
      }),
      safeMessage: (err) => String(err?.message || err || ''),
      slugifyId: (value) => String(value || ''),
      pushLog: () => {},
      resolveRole: () => 'user',
      resolveLocalUsers: () => [],
    });

    const originalFetch = global.fetch;
    global.fetch = async (url) => {
      const target = String(url || '');
      if (target === 'http://lidarr.local/api/v1/album/42') {
        return new Response(JSON.stringify({
          id: 42,
          title: 'Manners',
          artistId: 358,
          monitored: true,
          statistics: { trackFileCount: 0 },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/artist/358') {
        return new Response(JSON.stringify({
          id: 358,
          artistName,
          monitored: true,
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in Lidarr progression test: ${target}`);
    };

    try {
      const result = await lidarrService.reviewArtistProgression({
        userPlexId: userId,
        artistName,
        role: 'user',
      });

      assert.equal(result.status, 'downloaded_media_server');
      assert.equal(result.progress?.currentStage, 'album_acquired');
      assert.equal(result.progress?.lastManualSearchStatus, 'completed');

      const progress = getLidarrArtistProgress(db, userId, artistName);
      assert.equal(progress?.currentStage, 'album_acquired');
      assert.equal(progress?.lastManualSearchStatus, 'completed');
      assert.ok(Number(progress?.nextReviewAt || 0) > now);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }
  });

  it('repairs a stale tracked Lidarr album without requiring a database reset', async () => {
    const dbPath = join(process.env.DATA_DIR, `lidarr-stale-album-${Date.now()}.db`);
    const db = initDb(dbPath);
    const userId = `lidarr-stale-album-${Date.now()}`;
    const artistName = 'Passion Pit';
    const now = Date.now();

    db.prepare(`
      INSERT INTO artist_stats (artist_name, user_plex_id, ranking_score, updated_at)
      VALUES (?, ?, ?, ?)
    `).run(artistName, userId, 10, now);
    upsertSuggestedArtist(db, userId, {
      artistName,
      status: 'added_to_lidarr',
      lidarrArtistId: 358,
      reason: {
        latestAlbum: {
          albumId: 42,
          albumTitle: 'Manners',
          commandId: 99,
          sourceKind: 'manual',
          addedByCuratorr: true,
        },
      },
    });
    saveLidarrArtistProgress(db, userId, {
      artistName,
      lidarrArtistId: 358,
      currentStage: 'catalog_expanded',
      albumsAddedCount: 1,
      lastAlbumAddedAt: now - (2 * 24 * 60 * 60 * 1000),
      nextReviewAt: now - 1000,
      highestObservedRank: 10,
      lastManualSearchAt: now - (60 * 60 * 1000),
      lastManualSearchStatus: 'started',
      updatedAt: now - 1000,
    });

    const lidarrService = createLidarrService({
      db,
      loadConfig: () => ({
        lidarr: {
          url: 'http://lidarr.local',
          apiKey: 'lidarr-api-key',
          automationEnabled: true,
          automationScope: 'global',
          autoTriggerManualSearch: false,
          roleQuotas: {
            user: { weeklyArtists: 10, weeklyAlbums: 10 },
          },
        },
      }),
      safeMessage: (err) => String(err?.message || err || ''),
      slugifyId: (value) => String(value || ''),
      pushLog: () => {},
      resolveRole: () => 'user',
      resolveLocalUsers: () => [],
    });

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      const method = String(options?.method || 'GET').toUpperCase();
      if (target === 'http://lidarr.local/api/v1/album/42') {
        return new Response(JSON.stringify({
          message: 'Album with ID 42 does not exist',
        }), {
          status: 404,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/artist/358') {
        return new Response(JSON.stringify({
          id: 358,
          artistName,
          monitored: true,
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/album?artistId=358&page=1&pageSize=200') {
        return new Response(JSON.stringify([
          {
            id: 77,
            title: 'Chunk of Change',
            artistId: 358,
            albumType: 'EP',
            monitored: false,
            releaseDate: '2008-09-16',
            statistics: { trackCount: 5, trackFileCount: 0 },
            ratings: { value: 8.1, votes: 40 },
            tags: [],
          },
        ]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/album/monitor' && method === 'PUT') {
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/album/77') {
        return new Response(JSON.stringify({
          id: 77,
          title: 'Chunk of Change',
          artistId: 358,
          monitored: true,
          albumType: 'EP',
          releaseDate: '2008-09-16',
          statistics: { trackCount: 5, trackFileCount: 0 },
          ratings: { value: 8.1, votes: 40 },
          tags: [],
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/tag') {
        return new Response(JSON.stringify([
          { id: 11, label: 'curatorr-auto-album' },
        ]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/album/editor' && method === 'PUT') {
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in stale Lidarr album test: ${method} ${target}`);
    };

    try {
      const result = await lidarrService.reviewArtistProgression({
        userPlexId: userId,
        artistName,
        role: 'user',
      });

      assert.equal(result.status, 'album_added');
      assert.equal(result.progress?.currentStage, 'catalog_expanded');

      const progress = getLidarrArtistProgress(db, userId, artistName);
      assert.equal(progress?.lidarrArtistId, 358);
      assert.equal(progress?.currentStage, 'catalog_expanded');
      assert.equal(progress?.lastManualSearchStatus, '');

      const suggestion = getSuggestedArtist(db, userId, artistName);
      assert.equal(suggestion?.status, 'added_to_lidarr');
      assert.equal(suggestion?.lidarrArtistId, 358);
      assert.equal(suggestion?.reason?.latestAlbum?.albumId, 77);
      assert.equal(suggestion?.reason?.latestAlbum?.albumTitle, 'Chunk of Change');
      assert.equal(suggestion?.reason?.latestAlbum?.commandId ?? null, null);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }
  });

  it('stops re-searching an unobtainable album and expands the next one (issue #124)', async () => {
    const dbPath = join(process.env.DATA_DIR, `lidarr-unobtainable-${Date.now()}.db`);
    const db = initDb(dbPath);
    const userId = `lidarr-unobtainable-${Date.now()}`;
    const artistName = 'James McMurtry';
    const now = Date.now();

    db.prepare(`
      INSERT INTO artist_stats (artist_name, user_plex_id, ranking_score, updated_at)
      VALUES (?, ?, ?, ?)
    `).run(artistName, userId, 10, now);
    upsertSuggestedArtist(db, userId, {
      artistName,
      status: 'added_to_lidarr',
      lidarrArtistId: 358,
      reason: {
        // The manual search retries are already exhausted, so reconcile goes straight to the
        // release-grab fallback. With no obtainable release this would previously loop forever.
        acquisition: { searchAttempts: 2 },
        latestAlbum: {
          albumId: 42,
          albumTitle: 'Live in Aught-Three',
          commandId: null,
          sourceKind: 'manual',
          addedByCuratorr: true,
        },
      },
    });
    saveLidarrArtistProgress(db, userId, {
      artistName,
      lidarrArtistId: 358,
      currentStage: 'catalog_expanded',
      albumsAddedCount: 1,
      // Added well over the 14-day "stuck too long" guard so the unobtainable album is given up on.
      lastAlbumAddedAt: now - (20 * 24 * 60 * 60 * 1000),
      nextReviewAt: now - 1000,
      highestObservedRank: 10,
      lastManualSearchAt: now - (8 * 60 * 60 * 1000),
      lastManualSearchStatus: 'queued',
      updatedAt: now - 1000,
    });

    const lidarrService = createLidarrService({
      db,
      loadConfig: () => ({
        lidarr: {
          url: 'http://lidarr.local',
          apiKey: 'lidarr-api-key',
          automationEnabled: true,
          automationScope: 'global',
          autoTriggerManualSearch: false,
          roleQuotas: {
            user: { weeklyArtists: 10, weeklyAlbums: 10 },
          },
        },
      }),
      safeMessage: (err) => String(err?.message || err || ''),
      slugifyId: (value) => String(value || ''),
      pushLog: () => {},
      resolveRole: () => 'user',
      resolveLocalUsers: () => [],
    });

    let releaseLookups = 0;
    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      const method = String(options?.method || 'GET').toUpperCase();
      if (target === 'http://lidarr.local/api/v1/album/42') {
        return new Response(JSON.stringify({
          id: 42,
          title: 'Live in Aught-Three',
          artistId: 358,
          monitored: true,
          albumType: 'Album',
          releaseDate: '2004-03-09',
          statistics: { trackCount: 12, trackFileCount: 0 },
        }), { status: 200, headers: { 'Content-Type': 'application/json' } });
      }
      if (target === 'http://lidarr.local/api/v1/artist/358') {
        return new Response(JSON.stringify({ id: 358, artistName, monitored: true }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/release?albumId=42') {
        releaseLookups += 1;
        return new Response(JSON.stringify([]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/album?artistId=358&page=1&pageSize=200') {
        return new Response(JSON.stringify([
          {
            id: 42,
            title: 'Live in Aught-Three',
            artistId: 358,
            albumType: 'Album',
            monitored: true,
            releaseDate: '2004-03-09',
            statistics: { trackCount: 12, trackFileCount: 0 },
            ratings: { value: 7.5, votes: 30 },
            tags: [],
          },
          {
            id: 77,
            title: 'Childish Things',
            artistId: 358,
            albumType: 'Album',
            monitored: false,
            releaseDate: '2005-09-13',
            statistics: { trackCount: 11, trackFileCount: 0 },
            ratings: { value: 8.4, votes: 55 },
            tags: [],
          },
        ]), { status: 200, headers: { 'Content-Type': 'application/json' } });
      }
      if (target === 'http://lidarr.local/api/v1/album/monitor' && method === 'PUT') {
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/album/77') {
        return new Response(JSON.stringify({
          id: 77,
          title: 'Childish Things',
          artistId: 358,
          monitored: true,
          albumType: 'Album',
          releaseDate: '2005-09-13',
          statistics: { trackCount: 11, trackFileCount: 0 },
          ratings: { value: 8.4, votes: 55 },
          tags: [],
        }), { status: 200, headers: { 'Content-Type': 'application/json' } });
      }
      if (target === 'http://lidarr.local/api/v1/tag') {
        return new Response(JSON.stringify([{ id: 11, label: 'curatorr-auto-album' }]), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://lidarr.local/api/v1/album/editor' && method === 'PUT') {
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in unobtainable album test: ${method} ${target}`);
    };

    try {
      const result = await lidarrService.reviewArtistProgression({
        userPlexId: userId,
        artistName,
        role: 'user',
      });

      // The un-gettable album was skipped and the next catalog album was expanded instead of
      // looping on another daily search.
      assert.equal(releaseLookups, 1);
      assert.equal(result.status, 'album_added');
      assert.equal(result.progress?.currentStage, 'catalog_expanded');

      const progress = getLidarrArtistProgress(db, userId, artistName);
      assert.equal(progress?.currentStage, 'catalog_expanded');
      assert.equal(progress?.albumsAddedCount, 2);
      // Fresh album → no stale 'queued'/'started' status carried over.
      assert.equal(progress?.lastManualSearchStatus, '');

      const suggestion = getSuggestedArtist(db, userId, artistName);
      assert.equal(suggestion?.reason?.latestAlbum?.albumId, 77);
      assert.equal(suggestion?.reason?.acquisition?.searchAttempts, 0);
      assert.equal(suggestion?.reason?.acquisition?.manualGrabAttempts, 0);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }
  });

  it('clears stale Lidarr artist links instead of leaving due reviews stuck forever', async () => {
    const dbPath = join(process.env.DATA_DIR, `lidarr-stale-artist-${Date.now()}.db`);
    const db = initDb(dbPath);
    const userId = `lidarr-stale-artist-${Date.now()}`;
    const artistName = 'Passion Pit';
    const now = Date.now();

    db.prepare(`
      INSERT INTO artist_stats (artist_name, user_plex_id, ranking_score, updated_at)
      VALUES (?, ?, ?, ?)
    `).run(artistName, userId, 10, now);
    upsertSuggestedArtist(db, userId, {
      artistName,
      status: 'added_to_lidarr',
      lidarrArtistId: 358,
      reason: {
        latestAlbum: {
          albumId: null,
          albumTitle: 'Manners',
          commandId: null,
          sourceKind: 'manual',
          addedByCuratorr: true,
        },
      },
    });
    saveLidarrArtistProgress(db, userId, {
      artistName,
      lidarrArtistId: 358,
      currentStage: 'added',
      albumsAddedCount: 1,
      lastAlbumAddedAt: now - (2 * 24 * 60 * 60 * 1000),
      nextReviewAt: now - 1000,
      highestObservedRank: 10,
      lastManualSearchAt: now - (60 * 60 * 1000),
      lastManualSearchStatus: '',
      updatedAt: now - 1000,
    });

    const lidarrService = createLidarrService({
      db,
      loadConfig: () => ({
        lidarr: {
          url: 'http://lidarr.local',
          apiKey: 'lidarr-api-key',
          automationEnabled: true,
          automationScope: 'global',
          roleQuotas: {
            user: { weeklyArtists: 10, weeklyAlbums: 10 },
          },
        },
      }),
      safeMessage: (err) => String(err?.message || err || ''),
      slugifyId: (value) => String(value || ''),
      pushLog: () => {},
      resolveRole: () => 'user',
      resolveLocalUsers: () => [],
    });

    const originalFetch = global.fetch;
    global.fetch = async (url) => {
      const target = String(url || '');
      if (target === 'http://lidarr.local/api/v1/album?artistId=358&page=1&pageSize=200') {
        return new Response(JSON.stringify({
          message: 'Artist with ID 358 does not exist',
        }), {
          status: 404,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in stale Lidarr artist test: ${target}`);
    };

    try {
      const result = await lidarrService.reviewArtistProgression({
        userPlexId: userId,
        artistName,
        role: 'user',
      });

      assert.equal(result.status, 'artist_not_linked');
      assert.equal(getLidarrArtistProgress(db, userId, artistName), null);

      const suggestion = getSuggestedArtist(db, userId, artistName);
      assert.equal(suggestion?.status, 'suggested');
      assert.equal(suggestion?.lidarrArtistId, null);
      assert.equal(suggestion?.reason?.acquisition?.lastRecoveryStatus, 'artist_missing');
    } finally {
      global.fetch = originalFetch;
      db.close();
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
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
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
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
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
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
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
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
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
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
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

  it('disables and re-enables custom playlists by removing and restoring the Plex playlist', async () => {
    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const userId = `custom-toggle-${Date.now()}`;
    const playlistKey = 'custom-plex-custom-1';

    saveUserGeneratedPlaylist(db, userId, {
      playlistType: 'custom',
      playlistKey,
      plexPlaylistId: 'plex-custom-1',
      playlistTitle: 'Road Test',
      trackCount: 2,
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
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      pushLog: () => {},
    });

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      const method = String(options.method || 'GET').toUpperCase();
      if (target === 'http://plex.local/playlists/plex-custom-1/items?X-Plex-Container-Start=0&X-Plex-Container-Size=1000') {
        return new Response(JSON.stringify({
          MediaContainer: {
            Metadata: [
              { ratingKey: 'rk-custom-1', originalTitle: 'Artist A' },
              { ratingKey: 'rk-custom-2', originalTitle: 'Artist B' },
            ],
          },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists/plex-custom-1' && method === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
        return new Response(JSON.stringify({ MediaContainer: { Metadata: [] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=Road+Test')) {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [{ ratingKey: 'plex-custom-2' }] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists/plex-custom-2/items' && method === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-custom-2/items?uri=')) {
        const decoded = decodeURIComponent(target);
        assert.match(decoded, /library\/metadata\/rk-custom-1,rk-custom-2/);
        return new Response('', { status: 200 });
      }
      throw new Error(`Unexpected fetch in custom playlist toggle test: ${target}`);
    };

    try {
      const disabled = await service.setGeneratedActive(userId, playlistKey, false);
      assert.equal(disabled.active, false);
      assert.equal(disabled.plexPlaylistId, '');
      assert.deepEqual(getPlaylistTracks(db, userId, playlistKey).map((track) => track.ratingKey), ['rk-custom-1', 'rk-custom-2']);

      const enabled = await service.setGeneratedActive(userId, playlistKey, true);
      assert.equal(enabled.active, true);
      assert.equal(enabled.plexPlaylistId, 'plex-custom-2');
      assert.equal(enabled.trackCount, 2);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const dbVerify = initDb(dbPath);
    try {
      const playlist = listUserGeneratedPlaylists(dbVerify, userId, { activeOnly: false }).find((entry) => entry.playlistKey === playlistKey);
      assert.ok(playlist);
      assert.equal(playlist.active, true);
      assert.equal(playlist.plexPlaylistId, 'plex-custom-2');
      assert.equal(playlist.trackCount, 2);
    } finally {
      dbVerify.close();
    }
  });

  it('does not create a duplicate custom playlist when Plex playlist lookup fails transiently', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-custom-transient-${Date.now()}.db`);
    const db = initDb(dbPath);
    const userId = `custom-transient-${Date.now()}`;
    const playlistKey = 'custom-transient-road-test';

    saveUserGeneratedPlaylist(db, userId, {
      playlistType: 'custom',
      playlistKey,
      plexPlaylistId: '',
      playlistTitle: 'Road Test',
      trackCount: 1,
      active: true,
      updatedAt: Date.now(),
    });
    setPlaylistTracks(db, userId, playlistKey, [
      { ratingKey: 'rk-custom-transient-1', artistName: 'Artist A' },
    ]);

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
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      pushLog: () => {},
    });

    const playlist = listUserGeneratedPlaylists(db, userId, { activeOnly: false })
      .find((entry) => entry.playlistKey === playlistKey);
    assert.ok(playlist);

    let createCalls = 0;
    const originalFetch = global.fetch;
    global.fetch = async (url) => {
      const target = String(url || '');
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
        throw new Error('temporary Plex lookup failure');
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=Road+Test')) {
        createCalls += 1;
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [{ ratingKey: 'plex-custom-transient-created' }] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in transient custom playlist test: ${target}`);
    };

    try {
      await assert.rejects(
        service.syncCustomPlaylist(userId, playlist),
        /temporary Plex lookup failure/,
      );
      assert.equal(createCalls, 0);
      const persisted = listUserGeneratedPlaylists(db, userId, { activeOnly: false })
        .find((entry) => entry.playlistKey === playlistKey);
      assert.equal(persisted?.plexPlaylistId, '');
    } finally {
      global.fetch = originalFetch;
      db.close();
    }
  });

  it('serializes concurrent custom playlist creation to avoid duplicate Plex playlists', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-custom-concurrent-${Date.now()}.db`);
    const db = initDb(dbPath);
    const userId = `custom-concurrent-${Date.now()}`;
    const playlistKey = 'custom-concurrent-road-test';

    saveUserGeneratedPlaylist(db, userId, {
      playlistType: 'custom',
      playlistKey,
      plexPlaylistId: '',
      playlistTitle: 'Road Test',
      trackCount: 1,
      active: true,
      updatedAt: Date.now(),
    });
    setPlaylistTracks(db, userId, playlistKey, [
      { ratingKey: 'rk-custom-concurrent-1', artistName: 'Artist A' },
    ]);

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
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      pushLog: () => {},
    });

    const playlist = listUserGeneratedPlaylists(db, userId, { activeOnly: false })
      .find((entry) => entry.playlistKey === playlistKey);
    assert.ok(playlist);

    let searchCalls = 0;
    let createCalls = 0;
    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      const method = String(options.method || 'GET').toUpperCase();
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
        searchCalls += 1;
        await new Promise((resolve) => setTimeout(resolve, 25));
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [], totalSize: 0, size: 0 },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=Road+Test')) {
        createCalls += 1;
        await new Promise((resolve) => setTimeout(resolve, 25));
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [{ ratingKey: 'plex-custom-concurrent-1' }] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists/plex-custom-concurrent-1/items' && method === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-custom-concurrent-1/items?uri=')) {
        assert.match(decodeURIComponent(target), /library\/metadata\/rk-custom-concurrent-1/);
        return new Response('', { status: 200 });
      }
      throw new Error(`Unexpected fetch in concurrent custom playlist test: ${target}`);
    };

    try {
      const [first, second] = await Promise.all([
        service.syncCustomPlaylist(userId, playlist),
        service.syncCustomPlaylist(userId, playlist),
      ]);
      assert.equal(first?.plexPlaylistId, 'plex-custom-concurrent-1');
      assert.equal(second?.plexPlaylistId, 'plex-custom-concurrent-1');
      assert.equal(searchCalls, 1);
      assert.equal(createCalls, 1);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const dbVerify = initDb(dbPath);
    try {
      const persisted = listUserGeneratedPlaylists(dbVerify, userId, { activeOnly: false })
        .find((entry) => entry.playlistKey === playlistKey);
      assert.ok(persisted);
      assert.equal(persisted.plexPlaylistId, 'plex-custom-concurrent-1');
    } finally {
      dbVerify.close();
    }
  });

  it('keeps external playlist toggles in sync with user preference settings', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-external-toggle-${Date.now()}.db`);
    const db = initDb(dbPath);
    const userId = `external-toggle-${Date.now()}`;
    const now = Date.now();

    db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      'rk-external-1',
      'Artist Match',
      'Track Match',
      'Matched Album',
      'mbid-external-1',
      '[]',
      '1',
      180000,
      0,
      0,
      now,
    );

    saveUserPreferences(db, userId, {
      ...getUserPreferences(db, userId),
      lastfmUsername: 'last-user',
      lastfmEnabledStations: [],
      listenbrainzUsername: 'lb-user',
      listenbrainzToken: 'lb-token',
      listenbrainzEnabledPlaylists: [],
      userWizardCompleted: true,
    });

    saveUserGeneratedPlaylist(db, userId, {
      playlistType: 'lastfm-station',
      playlistKey: 'lastfm:mix',
      plexPlaylistId: '',
      playlistTitle: 'Last.fm Mix',
      algorithmVersion: 'lastfm-station-v1',
      trackCount: 0,
      active: false,
      updatedAt: now,
    });
    saveUserGeneratedPlaylist(db, userId, {
      playlistType: 'listenbrainz-playlist',
      playlistKey: 'listenbrainz:weekly-jams',
      plexPlaylistId: '',
      playlistTitle: 'ListenBrainz Weekly Jams',
      algorithmVersion: 'listenbrainz-playlist-v2',
      trackCount: 0,
      active: false,
      updatedAt: now,
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
        discovery: {},
        smartPlaylist: {},
      }),
      saveConfig: () => {},
      buildAppApiUrl: (base, path = '') => new URL(path, `${String(base).replace(/\/?$/, '/')}`),
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      pushLog: () => {},
    });

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      const method = String(options.method || 'GET').toUpperCase();

      if (target === 'https://www.last.fm/player/station/user/last-user/mix') {
        return new Response(JSON.stringify({
          playlist: [
            {
              artists: [{ _name: 'Artist Match' }],
              _name: 'Track Match',
            },
          ],
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'https://api.listenbrainz.org/1/user/lb-user/playlists/createdfor') {
        return new Response(JSON.stringify({
          playlists: [
            {
              playlist: {
                identifier: 'https://listenbrainz.org/playlist/12345678-1234-1234-1234-1234567890ab',
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
      if (target === 'https://api.listenbrainz.org/1/playlist/12345678-1234-1234-1234-1234567890ab') {
        return new Response(JSON.stringify({
          playlist: {
            track: [
              {
                title: 'Track Match',
                creator: 'Artist Match',
              },
            ],
          },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
        return new Response(JSON.stringify({ MediaContainer: { Metadata: [] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=Last.fm+Mix')) {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [{ ratingKey: 'plex-lastfm-1' }] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=ListenBrainz+Weekly+Jams')) {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [{ ratingKey: 'plex-lb-1' }] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if ((target === 'http://plex.local/playlists/plex-lastfm-1/items' || target === 'http://plex.local/playlists/plex-lb-1/items') && method === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if ((target.startsWith('http://plex.local/playlists/plex-lastfm-1/items?uri=') || target.startsWith('http://plex.local/playlists/plex-lb-1/items?uri=')) && method === 'PUT') {
        assert.match(decodeURIComponent(target), /library\/metadata\/rk-external-1/);
        return new Response('', { status: 200 });
      }
      if ((target === 'http://plex.local/playlists/plex-lastfm-1' || target === 'http://plex.local/playlists/plex-lb-1') && method === 'DELETE') {
        return new Response('', { status: 200 });
      }
      throw new Error(`Unexpected fetch in external playlist toggle test: ${target}`);
    };

    try {
      const enabledLastfm = await service.setGeneratedActive(userId, 'lastfm:mix', true);
      assert.equal(enabledLastfm.active, true);
      assert.equal(enabledLastfm.plexPlaylistId, 'plex-lastfm-1');
      assert.deepEqual(getUserPreferences(db, userId).lastfmEnabledStations, ['mix']);

      const enabledListenbrainz = await service.setGeneratedActive(userId, 'listenbrainz:weekly-jams', true);
      assert.equal(enabledListenbrainz.active, true);
      assert.equal(enabledListenbrainz.plexPlaylistId, 'plex-lb-1');
      assert.deepEqual(getUserPreferences(db, userId).listenbrainzEnabledPlaylists, ['weekly-jams']);

      const disabledLastfm = await service.setGeneratedActive(userId, 'lastfm:mix', false);
      assert.equal(disabledLastfm.active, false);
      assert.deepEqual(getUserPreferences(db, userId).lastfmEnabledStations, []);

      const disabledListenbrainz = await service.setGeneratedActive(userId, 'listenbrainz:weekly-jams', false);
      assert.equal(disabledListenbrainz.active, false);
      assert.deepEqual(getUserPreferences(db, userId).listenbrainzEnabledPlaylists, []);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }
  });

  it('applies Plex sonic ordering to Daily Mix during sync when enabled', async () => {
    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const userId = `daily-mix-sonic-${Date.now()}`;
    const logs = [];

    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('dm-s1', userId, 'Track 1', 'Artist A', 'Album A', 12, 'belter', 1.5, 0, Date.now());
    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('dm-s2', userId, 'Track 2', 'Artist A', 'Album A', 10, 'decent', 0.5, 0, Date.now());
    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('dm-s3', userId, 'Track 3', 'Artist B', 'Album B', 8, 'decent', 0.5, 0, Date.now());

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
          dailyMix: {
            favoriteLimit: 3,
            suggestedLimit: 0,
            freshLimit: 0,
            maxTracks: 3,
            maxTracksPerArtist: 2,
            repeatCooldownDays: 0,
            useSonicOrdering: true,
            sonicSeedCount: 2,
            sonicExpansionLimit: 4,
            sonicMaxDistance: 0.35,
            sonicStrategy: 'favor-favorites',
          },
        },
      }),
      saveConfig: () => {},
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      pushLog: (entry) => logs.push(entry),
    });

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target.includes('/library/metadata/dm-s1/nearest?')) {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [{ ratingKey: 'dm-s3' }, { ratingKey: 'dm-s2' }] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.includes('/library/metadata/dm-s2/nearest?')) {
        return new Response(JSON.stringify({
          MediaContainer: { Metadata: [{ ratingKey: 'dm-s1' }] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
        return new Response(JSON.stringify({ MediaContainer: { Metadata: [] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=')) {
        return new Response(JSON.stringify({ MediaContainer: { Metadata: [{ ratingKey: 'plex-daily-mix-sonic' }] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists/plex-daily-mix-sonic/items' && String(options.method || 'GET').toUpperCase() === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-daily-mix-sonic/items?uri=')) {
        const decoded = decodeURIComponent(target);
        assert.match(decoded, /library\/metadata\/dm-s1,dm-s3,dm-s2/);
        return new Response('', { status: 200 });
      }
      throw new Error(`Unexpected fetch in Daily Mix sonic test: ${target}`);
    };

    try {
      const built = service.buildDailyMix(userId);
      assert.deepEqual(built.trackKeys, ['dm-s1', 'dm-s2', 'dm-s3']);

      const synced = await service.syncDailyMix(userId);
      assert.deepEqual(synced.trackKeys, ['dm-s1', 'dm-s3', 'dm-s2']);
      assert.equal(synced.sonicApplied, true);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    assert.ok(logs.some((entry) => entry?.action === 'sonic.ordering'));
  });

  it('uses Plex sonic path sequencing when selected tracks share a library section', async () => {
    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const userId = `daily-mix-sonic-path-${Date.now()}`;
    const logs = [];
    const now = Date.now();

    const insertMaster = db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMaster.run('dm-p1', 'Artist A', 'Track 1', 'Album A', '', '[]', '1', 10, 0, now);
    insertMaster.run('dm-p2', 'Artist B', 'Track 2', 'Album B', '', '[]', '1', 8, 0, now);
    insertMaster.run('dm-p3', 'Artist C', 'Track 3', 'Album C', '', '[]', '1', 6, 0, now);

    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('dm-p1', userId, 'Track 1', 'Artist A', 'Album A', 12, 'belter', 1.5, 0, now);
    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('dm-p2', userId, 'Track 2', 'Artist B', 'Album B', 10, 'decent', 0.5, 0, now);
    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('dm-p3', userId, 'Track 3', 'Artist C', 'Album C', 8, 'decent', 0.5, 0, now);

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
          dailyMix: {
            favoriteLimit: 3,
            suggestedLimit: 0,
            freshLimit: 0,
            maxTracks: 3,
            maxTracksPerArtist: 1,
            repeatCooldownDays: 0,
            useSonicOrdering: true,
            sonicSeedCount: 2,
            sonicExpansionLimit: 4,
            sonicMaxDistance: 0.35,
            sonicStrategy: 'favor-favorites',
          },
        },
      }),
      saveConfig: () => {},
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      pushLog: (entry) => logs.push(entry),
    });

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target.includes('/library/sections/1/computePath?')) {
        return new Response(JSON.stringify({
          MediaContainer: { Path: [{ ratingKey: 'dm-p1' }, { ratingKey: 'dm-p3' }, { ratingKey: 'dm-p2' }] },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.includes('/library/metadata/') && target.includes('/nearest?')) {
        throw new Error(`Nearest fallback should not run when path sequencing succeeds: ${target}`);
      }
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
        return new Response(JSON.stringify({ MediaContainer: { Metadata: [] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=')) {
        return new Response(JSON.stringify({ MediaContainer: { Metadata: [{ ratingKey: 'plex-daily-mix-path' }] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists/plex-daily-mix-path/items' && String(options.method || 'GET').toUpperCase() === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-daily-mix-path/items?uri=')) {
        const decoded = decodeURIComponent(target);
        assert.match(decoded, /library\/metadata\/dm-p1,dm-p3,dm-p2/);
        return new Response('', { status: 200 });
      }
      throw new Error(`Unexpected fetch in Daily Mix sonic path test: ${target}`);
    };

    try {
      const synced = await service.syncDailyMix(userId);
      assert.deepEqual(synced.trackKeys, ['dm-p1', 'dm-p3', 'dm-p2']);
      assert.equal(synced.sonicApplied, true);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const pathLog = logs.find((entry) => entry?.action === 'sonic.ordering');
    assert.equal(pathLog?.meta?.method, 'path');
  });

  it('falls back to standard Curatorr ordering when Plex sonic data is unavailable', async () => {
    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const userId = `curatorr-sonic-fallback-${Date.now()}`;
    const logs = [];
    const now = Date.now();

    const insertMaster = db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMaster.run('ct-s1', 'Artist A', 'Known Favorite', 'Album A', '', '["Rock"]', '1', 20, 0, now);
    insertMaster.run('ct-s2', 'Artist B', 'Steady Favorite', 'Album B', '', '["Rock"]', '1', 10, 0, now);
    insertMaster.run('ct-s3', 'Artist C', 'Suggested Track', 'Album C', '', '["Synth"]', '1', 4, 0, now);

    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('ct-s1', userId, 'Known Favorite', 'Artist A', 'Album A', 14, 'belter', 1.5, now, now);
    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `).run('ct-s2', userId, 'Steady Favorite', 'Artist B', 'Album B', 8, 'decent', 0.5, now, now);

    db.prepare(`
      INSERT INTO artist_stats (
        artist_name, user_plex_id, play_count, skip_count, consecutive_skips,
        excluded_from_smart, manually_excluded, manually_included, ranking_score,
        last_played_at, updated_at
      ) VALUES (?, ?, ?, 0, 0, 0, 0, 0, ?, ?, ?)
    `).run('Artist A', userId, 14, 9.2, now, now);
    db.prepare(`
      INSERT INTO artist_stats (
        artist_name, user_plex_id, play_count, skip_count, consecutive_skips,
        excluded_from_smart, manually_excluded, manually_included, ranking_score,
        last_played_at, updated_at
      ) VALUES (?, ?, ?, 0, 0, 0, 0, 0, ?, ?, ?)
    `).run('Artist B', userId, 8, 8.1, now, now);
    db.prepare(`
      INSERT INTO artist_stats (
        artist_name, user_plex_id, play_count, skip_count, consecutive_skips,
        excluded_from_smart, manually_excluded, manually_included, ranking_score,
        last_played_at, updated_at
      ) VALUES (?, ?, 0, 0, 0, 0, 0, 0, ?, 0, ?)
    `).run('Artist C', userId, 7.1, now);

    db.prepare(`
      INSERT INTO suggested_tracks (
        user_plex_id, suggestion_key, rating_key, artist_name, track_title,
        album_name, source, total_score, reason_json, created_at, expires_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(userId, 'curatorr-sonic-suggested-1', 'ct-s3', 'Artist C', 'Suggested Track', 'Album C', 'curatorr', 8.5, '{}', now, null);

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
            useSonicOrdering: true,
            sonicSeedCount: 2,
            sonicExpansionLimit: 4,
            sonicMaxDistance: 0.35,
            sonicStrategy: 'balanced',
          },
        },
      }),
      saveConfig: () => {},
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      pushLog: (entry) => logs.push(entry),
    });

    const built = service.buildCuratorr(userId);
    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target.includes('/library/sections/') && target.includes('/computePath?')) {
        return new Response('', { status: 404 });
      }
      if (target.includes('/library/metadata/') && target.includes('/nearest?')) {
        return new Response('', { status: 404 });
      }
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
        return new Response(JSON.stringify({ MediaContainer: { Metadata: [] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=')) {
        return new Response(JSON.stringify({ MediaContainer: { Metadata: [{ ratingKey: 'plex-curatorr-sonic-fallback' }] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists/plex-curatorr-sonic-fallback/items' && String(options.method || 'GET').toUpperCase() === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-curatorr-sonic-fallback/items?uri=')) {
        const decoded = decodeURIComponent(target);
        assert.match(decoded, new RegExp(`library/metadata/${built.trackKeys.join(',')}`));
        return new Response('', { status: 200 });
      }
      throw new Error(`Unexpected fetch in Curatorr sonic fallback test: ${target}`);
    };

    try {
      const synced = await service.syncCuratorr(userId);
      assert.deepEqual(synced.trackKeys, built.trackKeys);
      assert.equal(synced.sonicApplied, false);
      assert.equal(synced.sonicFallbackReason, 'no-sonic-data');
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    assert.ok(logs.some((entry) => entry?.action === 'sonic.fallback'));
  });

  it('backfills track enrichment from MusicBrainz release dates', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-enrichment-${Date.now()}-1.db`);
    const db = initDb(dbPath);
    const logs = [];
    const now = Date.now();

    db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      'enrich-1',
      'Boards of Canada',
      'Roygbiv',
      'Music Has the Right to Children',
      'mbid-track-1',
      '[]',
      '1',
      '/music/Boards of Canada/Roygbiv.flac',
      180000,
      0,
      0,
      now,
    );

    const service = createTrackEnrichmentService({
      db,
      pushLog: (entry) => logs.push(entry),
      safeMessage: (err) => String(err?.message || err || 'Unknown error'),
    });

    const originalFetch = global.fetch;
    global.fetch = async (url) => {
      const target = String(url || '');
      if (!target.includes('/ws/2/recording/mbid-track-1?')) {
        throw new Error(`Unexpected enrichment fetch URL: ${target}`);
      }
      return new Response(JSON.stringify({
        id: 'mbid-track-1',
        releases: [
          { id: 'release-late', status: 'Official', date: '2000-04-10' },
          { id: 'release-early', status: 'Official', date: '1998-04-20' },
        ],
        'release-groups': [
          { id: 'rg-1', 'first-release-date': '1998-04' },
        ],
      }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    };

    try {
      const result = await service.runSync({ limit: 5, requestDelayMs: 0, timeoutMs: 5000 });
      assert.equal(result.processed, 1);
      assert.equal(result.enriched, 1);
      assert.equal(result.failed, 0);
      assert.equal(result.remaining, 0);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const dbVerify = initDb(dbPath);
    try {
      const [row] = getTrackEnrichmentByRatingKeys(dbVerify, ['enrich-1']);
      assert.ok(row);
      assert.equal(row.recordingMbid, 'mbid-track-1');
      assert.equal(row.trackYear, 1998);
      assert.equal(row.originalReleaseDate, '1998-04-20');
      assert.equal(row.analysisSource, 'musicbrainz');
      assert.equal(countTracksMissingEnrichment(dbVerify, { requireRecordingMbid: true }), 0);
    } finally {
      dbVerify.close();
    }

    assert.ok(logs.some((entry) => entry?.action === 'track-enrichment.finish'));
  });

  it('stores a completed enrichment row when only release-group year data is available', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-enrichment-${Date.now()}-2.db`);
    const db = initDb(dbPath);
    const now = Date.now();

    db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      'enrich-2',
      'Artist',
      'Unclear Date Track',
      'Album',
      'mbid-track-2',
      '[]',
      '1',
      '/music/Artist/Unclear Date Track.flac',
      200000,
      0,
      0,
      now,
    );

    const service = createTrackEnrichmentService({
      db,
      pushLog: () => {},
      safeMessage: (err) => String(err?.message || err || 'Unknown error'),
    });

    const originalFetch = global.fetch;
    global.fetch = async (url) => {
      const target = String(url || '');
      if (!target.includes('/ws/2/recording/mbid-track-2?')) {
        throw new Error(`Unexpected enrichment fetch URL: ${target}`);
      }
      return new Response(JSON.stringify({
        id: 'mbid-track-2',
        releases: [],
        'release-groups': [
          { id: 'rg-only', 'first-release-date': '1987' },
        ],
      }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    };

    try {
      const result = await service.runSync({ limit: 5, requestDelayMs: 0, timeoutMs: 5000 });
      assert.equal(result.processed, 1);
      assert.equal(result.enriched, 1);
      assert.equal(result.failed, 0);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const dbVerify = initDb(dbPath);
    try {
      const [row] = getTrackEnrichmentByRatingKeys(dbVerify, ['enrich-2']);
      assert.ok(row);
      assert.equal(row.trackYear, 1987);
      assert.equal(row.originalReleaseDate, '1987');
      assert.equal(row.payload.matchedKind, 'release-group');
    } finally {
      dbVerify.close();
    }
  });

  it('applies trackYear and originalReleaseDate exclusion filters using enrichment data', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-enrichment-${Date.now()}-3.db`);
    const db = initDb(dbPath);
    const now = Date.now();

    const insertMaster = db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMaster.run('filter-1', 'Artist A', 'Track 1', 'Album 1', 'mbid-1', '[]', '1', '/music/a.flac', 180000, 0, 0, now);
    insertMaster.run('filter-2', 'Artist B', 'Track 2', 'Album 2', 'mbid-2', '[]', '1', '/music/b.flac', 180000, 0, 0, now);
    insertMaster.run('filter-3', 'Artist C', 'Track 3', 'Album 3', 'mbid-3', '[]', '1', '/music/c.flac', 180000, 0, 0, now);

    db.prepare(`
      INSERT INTO track_enrichment (
        rating_key, recording_mbid, track_year, original_release_date,
        analysis_source, analysis_confidence, payload_json, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run('filter-1', 'mbid-1', 1998, '1998-04-20', 'musicbrainz', 0.95, '{}', now);
    db.prepare(`
      INSERT INTO track_enrichment (
        rating_key, recording_mbid, track_year, original_release_date,
        analysis_source, analysis_confidence, payload_json, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run('filter-2', 'mbid-2', 2005, '2005-11-04', 'musicbrainz', 0.95, '{}', now);

    try {
      const masterTracks = getMasterTracks(db);
      assert.equal(masterTracks.find((track) => track.ratingKey === 'filter-1')?.trackYear, 1998);
      assert.equal(masterTracks.find((track) => track.ratingKey === 'filter-2')?.originalReleaseDate, '2005-11-04');

      const yearFiltered = applyTrackFilters(masterTracks, {
        rules: [{ field: 'trackYear', operator: 'gt', value: '2000' }],
      });
      assert.deepEqual(yearFiltered.map((track) => track.ratingKey).sort(), ['filter-1', 'filter-3']);

      const dateFiltered = applyTrackFilters(masterTracks, {
        rules: [{ field: 'originalReleaseDate', operator: 'between', value: '1998-01-01..1999-12-31' }],
      });
      assert.deepEqual(dateFiltered.map((track) => track.ratingKey).sort(), ['filter-2', 'filter-3']);
    } finally {
      db.close();
    }
  });

  it('keeps generated track decades separate from Last.fm tags', () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-decade-tags-${Date.now()}.db`);
    const db = initDb(dbPath);
    refreshMasterTracks(db, [
      {
        ratingKey: 'decade-1998',
        artistName: 'Decade Artist',
        trackTitle: '1998 Song',
        albumName: '1998 Album',
        libraryKey: '1',
        filePath: '/music/1998.flac',
        durationMs: 180000,
        ratingCount: 10,
        viewCount: 0,
        trackYear: 1998,
        originalReleaseDate: '1998-04-20',
      },
      {
        ratingKey: 'decade-2001',
        artistName: 'Decade Artist',
        trackTitle: '2001 Song',
        albumName: '2000s Album',
        libraryKey: '1',
        filePath: '/music/2001.flac',
        durationMs: 180000,
        ratingCount: 10,
        viewCount: 0,
        trackYear: 2001,
        originalReleaseDate: '2001-03-01',
      },
      {
        ratingKey: 'decade-2009',
        artistName: 'Decade Artist',
        trackTitle: '2009 Song',
        albumName: '2000s Album',
        libraryKey: '1',
        filePath: '/music/2009.flac',
        durationMs: 180000,
        ratingCount: 10,
        viewCount: 0,
        trackYear: 2009,
        originalReleaseDate: '2009-11-05',
      },
      {
        ratingKey: 'decade-2021',
        artistName: 'Decade Artist',
        trackTitle: '2021 Song',
        albumName: '2021 Album',
        libraryKey: '1',
        filePath: '/music/2021.flac',
        durationMs: 180000,
        ratingCount: 10,
        viewCount: 0,
        trackYear: 2021,
        originalReleaseDate: '2021-07-10',
      },
    ]);
    saveArtistTags(db, 'Decade Artist', ['indie']);

    try {
      const tags = getAllLastfmTags(db);
      assert.deepEqual(tags, ['indie']);

      const decades = getAllTrackDecadeTags(db);
      assert.deepEqual(decades, ['1990s', '2000s', '2020s']);

      const preview = previewGlobalPlaylist(
        db,
        { decades: { include: ['2000s'], exclude: [], includeMode: 'any' } },
        'decade-user',
        {},
      );
      assert.equal(preview.forUser?.eligibleTrackCount, 2);
      assert.equal(preview.forUser?.trackCount, 2);

      const preview2020s = previewGlobalPlaylist(
        db,
        { decades: { include: ['2020s'], exclude: [], includeMode: 'any' } },
        'decade-user',
        {},
      );
      assert.equal(preview2020s.forUser?.eligibleTrackCount, 1);
      assert.equal(preview2020s.forUser?.trackCount, 1);
    } finally {
      db.close();
    }
  });

  it('applies random playlist artist and album caps in previews', () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-random-playlist-${Date.now()}.db`);
    const db = initDb(dbPath);
    const now = Date.now();
    const insertMaster = db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMaster.run('random-shared-a', 'Random Artist A', 'Shared A', 'Shared Album', '', '[]', '1', '/music/random-a.flac', 180000, 10, 0, now);
    insertMaster.run('random-shared-b', 'Random Artist B', 'Shared B', 'Shared Album', '', '[]', '1', '/music/random-b.flac', 180000, 9, 0, now);
    insertMaster.run('random-unique-c', 'Random Artist C', 'Unique C', 'Unique Album C', '', '[]', '1', '/music/random-c.flac', 180000, 8, 0, now);
    insertMaster.run('random-unique-d', 'Random Artist D', 'Unique D', 'Unique Album D', '', '[]', '1', '/music/random-d.flac', 180000, 7, 0, now);

    try {
      const preview = previewGlobalPlaylist(
        db,
        { sortBy: 'random', topNPerArtist: 1, maxTracksPerAlbum: 1, maxTracks: 50 },
        'random-user',
        {},
      );
      assert.equal(preview.forUser?.eligibleTrackCount, 4);
      assert.equal(preview.forUser?.trackCount, 3);
      assert.equal(preview.forUser?.artistCount, 3);

      const cappedPreview = previewGlobalPlaylist(
        db,
        { sortBy: 'random', topNPerArtist: 1, maxTracksPerAlbum: 1, maxTracks: 2 },
        'random-user',
        {},
      );
      assert.equal(cappedPreview.forUser?.trackCount, 2);
    } finally {
      db.close();
    }
  });

  it('filters smart playlist previews by last played recency', () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-last-played-preview-${Date.now()}.db`);
    const db = initDb(dbPath);
    const userId = `last-played-user-${Date.now()}`;
    const now = Date.now();

    refreshMasterTracks(db, [
      {
        ratingKey: 'last-played-recent',
        artistName: 'Recency Artist',
        trackTitle: 'Played Recently',
        albumName: 'Recency Album',
        libraryKey: '1',
        filePath: '/music/recent.flac',
        durationMs: 180000,
        ratingCount: 10,
        viewCount: 0,
      },
      {
        ratingKey: 'last-played-stale',
        artistName: 'Recency Artist',
        trackTitle: 'Played Ages Ago',
        albumName: 'Recency Album',
        libraryKey: '1',
        filePath: '/music/stale.flac',
        durationMs: 180000,
        ratingCount: 9,
        viewCount: 0,
      },
      {
        ratingKey: 'last-played-never',
        artistName: 'Recency Artist',
        trackTitle: 'Never Played',
        albumName: 'Recency Album',
        libraryKey: '1',
        filePath: '/music/never.flac',
        durationMs: 180000,
        ratingCount: 8,
        viewCount: 0,
      },
    ]);

    const insertTrackStats = db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `);
    insertTrackStats.run('last-played-recent', userId, 'Played Recently', 'Recency Artist', 'Recency Album', 12, 'belter', 1.5, now - 3 * 24 * 60 * 60 * 1000, now);
    insertTrackStats.run('last-played-stale', userId, 'Played Ages Ago', 'Recency Artist', 'Recency Album', 7, 'decent', 0.5, now - 45 * 24 * 60 * 60 * 1000, now);

    try {
      const withinPreview = previewGlobalPlaylist(
        db,
        { lastPlayedMode: 'within', lastPlayedDays: 7 },
        userId,
        {},
      );
      assert.equal(withinPreview.forUser?.eligibleTrackCount, 1);
      assert.equal(withinPreview.forUser?.trackCount, 1);

      const notWithinPreview = previewGlobalPlaylist(
        db,
        { lastPlayedMode: 'notWithin', lastPlayedDays: 30 },
        userId,
        {},
      );
      assert.equal(notWithinPreview.forUser?.eligibleTrackCount, 2);
      assert.equal(notWithinPreview.forUser?.trackCount, 2);

      const neverPreview = previewGlobalPlaylist(
        db,
        { lastPlayedMode: 'never' },
        userId,
        {},
      );
      assert.equal(neverPreview.forUser?.eligibleTrackCount, 1);
      assert.equal(neverPreview.forUser?.trackCount, 1);
    } finally {
      db.close();
    }
  });

  it('imports BPM and key features from a manifest and applies typed filters', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-enrichment-${Date.now()}-4.db`);
    const manifestPath = join(testDir, `track-features-${Date.now()}.json`);
    const db = initDb(dbPath);
    const now = Date.now();

    const insertMaster = db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMaster.run('feat-1', 'Artist A', 'Track 1', 'Album 1', 'mbid-feat-1', '[]', '1', '/music/feat-1.flac', 180000, 0, 0, now);
    insertMaster.run('feat-2', 'Artist B', 'Track 2', 'Album 2', 'mbid-feat-2', '[]', '1', '/music/feat-2.flac', 180000, 0, 0, now);

    await writeFile(manifestPath, JSON.stringify({
      tracks: [
        {
          recordingMbid: 'mbid-feat-1',
          bpm: 122,
          musicalKey: 'G minor',
          camelotKey: '6A',
          energy: 0.72,
          danceability: 0.61,
        },
        {
          filePath: '/music/feat-2.flac',
          bpm: 98,
          musicalKey: 'C major',
          camelotKey: '8B',
          energy: 0.31,
          danceability: 0.42,
        },
      ],
    }), 'utf8');

    try {
      const service = createTrackEnrichmentService({
        db,
        pushLog: () => {},
        safeMessage: (err) => String(err?.message || err || 'Unknown error'),
      });
      const result = await service.importFeatureManifest({ manifestPath });
      assert.equal(result.imported, 2);

      const masterTracks = getMasterTracks(db);
      const feat1 = masterTracks.find((track) => track.ratingKey === 'feat-1');
      const feat2 = masterTracks.find((track) => track.ratingKey === 'feat-2');
      assert.equal(feat1?.bpm, 122);
      assert.equal(feat1?.musicalKey, 'G minor');
      assert.equal(feat2?.camelotKey, '8B');
      assert.equal(feat2?.energy, 0.31);

      const bpmFiltered = applyTrackFilters(masterTracks, {
        rules: [{ field: 'bpm', operator: 'gte', value: '120' }],
      });
      assert.deepEqual(bpmFiltered.map((track) => track.ratingKey).sort(), ['feat-2']);

      const keyFiltered = applyTrackFilters(masterTracks, {
        rules: [{ field: 'camelotKey', operator: 'equals', value: '8B' }],
      });
      assert.deepEqual(keyFiltered.map((track) => track.ratingKey).sort(), ['feat-1']);
    } finally {
      db.close();
    }
  });

  it('applies file path regex exclusion filters against the full track path', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-path-filters-${Date.now()}.db`);
    const db = initDb(dbPath);
    const now = Date.now();

    const insertMaster = db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMaster.run('path-1', 'Artist Halloween', 'Track Halloween', 'Halloween', 'mbid-path-1', '[]', '1', '/music/Compilations/Halloween Party/track-1.flac', 180000, 0, 0, now);
    insertMaster.run('path-2', 'Artist Normal', 'Track Normal', 'Regular Album', 'mbid-path-2', '[]', '1', '/music/Compilations/Regular Party/track-2.flac', 180000, 0, 0, now);

    try {
      const masterTracks = getMasterTracks(db);

      const regexExcluded = applyTrackFilters(masterTracks, {
        rules: [{ field: 'filePath', operator: 'regex', value: 'halloween party', caseSensitive: false }],
      });
      assert.deepEqual(regexExcluded.map((track) => track.ratingKey).sort(), ['path-2']);

      const inverseRegexExcluded = applyTrackFilters(masterTracks, {
        rules: [{ field: 'filePath', operator: 'not_regex', value: 'halloween party', caseSensitive: false }],
      });
      assert.deepEqual(inverseRegexExcluded.map((track) => track.ratingKey).sort(), ['path-1']);
    } finally {
      db.close();
    }
  });

  it('deduplicates release variants by artist and fuzzy song title when requested', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-release-dedupe-${Date.now()}.db`);
    const db = initDb(dbPath);
    const now = Date.now();

    const insertMaster = db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMaster.run('rel-1', 'The Beatles', 'Penny Lane', 'Anthology 2', 'mbid-anthology', '[]', '1', '/music/anthology/penny-lane.flac', 193000, 120, 40, now);
    insertMaster.run('rel-2', 'The Beatles', 'Penny Lane - Radio Edit', 'Magical Mystery Tour', 'mbid-mmt', '[]', '1', '/music/mmt/penny-lane.flac', 198000, 180, 55, now);
    insertMaster.run('rel-3', 'The Beatles', 'Penny Lane (Live)', 'Live Bootleg', 'mbid-live', '[]', '1', '/music/live/penny-lane.flac', 254000, 250, 80, now);
    insertMaster.run('rel-4', 'The Beatles', 'Hello Goodbye', '1', 'mbid-hello', '[]', '1', '/music/1/hello-goodbye.flac', 207000, 140, 60, now);
    insertMaster.run('rel-5', 'The Beatles', 'Penny Lane', 'Duplicate MBID Release', 'mbid-anthology', '[]', '1', '/music/duplicate/penny-lane.flac', 193500, 90, 20, now);

    try {
      const masterTracks = getMasterTracks(db);

      const unfiltered = applyTrackFilters(masterTracks, { deduplicateByMbid: false });
      assert.deepEqual(unfiltered.map((track) => track.ratingKey).sort(), ['rel-1', 'rel-2', 'rel-3', 'rel-4', 'rel-5']);

      const mbidOnly = applyTrackFilters(masterTracks, { deduplicateByMbid: true });
      assert.deepEqual(mbidOnly.map((track) => track.ratingKey).sort(), ['rel-1', 'rel-2', 'rel-3', 'rel-4']);

      const dedupedByTitle = applyTrackFilters(masterTracks, { deduplicateByArtistTitle: true });
      assert.deepEqual(dedupedByTitle.map((track) => track.ratingKey).sort(), ['rel-3', 'rel-4']);

      const titleReport = applyTrackFiltersWithReport(masterTracks, {
        deduplicateByArtistTitle: true,
      }, { duplicateLimit: 10 });
      assert.equal(titleReport.duplicateCount, 3);
      assert.deepEqual(titleReport.tracks.map((track) => track.ratingKey).sort(), ['rel-3', 'rel-4']);
      assert.equal(titleReport.duplicateMatches[0].method, 'artist_title');
      assert.equal(titleReport.duplicateMatches[0].reason, 'Artist/title fuzzy match');
      assert.equal(titleReport.duplicateMatches[0].kept.ratingKey, 'rel-3');
      assert.equal(titleReport.duplicateMatches[0].duplicate.ratingKey, 'rel-2');

      const durationGuarded = applyTrackFilters(masterTracks, {
        deduplicateByArtistTitle: true,
        deduplicateByDuration: true,
      });
      assert.deepEqual(durationGuarded.map((track) => track.ratingKey).sort(), ['rel-2', 'rel-3', 'rel-4']);

      const durationReport = applyTrackFiltersWithReport(masterTracks, {
        deduplicateByArtistTitle: true,
        deduplicateByDuration: true,
      }, { duplicateLimit: 10 });
      assert.equal(durationReport.duplicateCount, 2);
      assert.equal(durationReport.duplicateMatches[0].reason, 'Artist/title fuzzy match within 5 seconds');

      const variantGuarded = applyTrackFilters(masterTracks, {
        deduplicateByArtistTitle: true,
        deduplicateIgnoreLikelyVariants: true,
      });
      assert.deepEqual(variantGuarded.map((track) => track.ratingKey).sort(), ['rel-1', 'rel-2', 'rel-3', 'rel-4']);

      const liveAlbumGuarded = applyTrackFilters([
        { ratingKey: 'live-album-1', artistName: 'The Beatles', trackTitle: 'Penny Lane', albumName: 'Concert', albumType: 'Live', durationMs: 193000, ratingCount: 260, viewCount: 0 },
        { ratingKey: 'studio-1', artistName: 'The Beatles', trackTitle: 'Penny Lane', albumName: 'Magical Mystery Tour', albumType: 'Album', durationMs: 193000, ratingCount: 120, viewCount: 0 },
      ], {
        deduplicateByArtistTitle: true,
        deduplicateIgnoreLiveAlbums: true,
      });
      assert.deepEqual(liveAlbumGuarded.map((track) => track.ratingKey).sort(), ['live-album-1', 'studio-1']);
    } finally {
      db.close();
    }
  });

  it('prefers artist-folder copies over compilations independently of the dedupe toggles', () => {
    const tracks = [
      // Same recording (shared MBID) in both a compilation folder and an artist folder.
      { ratingKey: 'comp-mbid', artistName: 'Artist A', trackTitle: 'Song One', albumName: 'Now Thats Music 50', recordingMbid: 'mbid-1', filePath: '/music/Compilations/Now 50/song.mp3', durationMs: 180000, ratingCount: 300, viewCount: 50 },
      { ratingKey: 'art-mbid', artistName: 'Artist A', trackTitle: 'Song One', albumName: 'Debut', recordingMbid: 'mbid-1', filePath: '/music/Artist A/Debut/song.flac', durationMs: 180500, ratingCount: 10, viewCount: 1 },
      // Compilation with no artist-folder sibling — must be kept.
      { ratingKey: 'comp-lonely', artistName: 'Artist Z', trackTitle: 'Solo Track', albumName: 'Various Artists', recordingMbid: 'mbid-2', filePath: '/music/Compilations/VA/solo.mp3', durationMs: 200000, ratingCount: 20, viewCount: 0 },
      // No MBID — matched by artist/title + close duration. Compilation detected via folder name.
      { ratingKey: 'comp-path', artistName: 'Artist B', trackTitle: 'Track Two', albumName: 'Mixtape', recordingMbid: '', filePath: '/music/Various Artists/Mix/t.mp3', durationMs: 200000, ratingCount: 100, viewCount: 0 },
      { ratingKey: 'art-path', artistName: 'Artist B', trackTitle: 'Track Two', albumName: 'Album B', recordingMbid: '', filePath: '/music/Artist B/Album B/t.flac', durationMs: 201000, ratingCount: 5, viewCount: 0 },
      // Genuinely distinct recording (live) on a compilation — must survive even with an artist-folder studio copy.
      { ratingKey: 'comp-live', artistName: 'Artist C', trackTitle: 'Track Three (Live)', albumName: 'Live Compilation', recordingMbid: '', filePath: '/music/Compilations/Live/t3.mp3', durationMs: 240000, ratingCount: 80, viewCount: 0 },
      { ratingKey: 'art-studio', artistName: 'Artist C', trackTitle: 'Track Three', albumName: 'Album C', recordingMbid: '', filePath: '/music/Artist C/Album C/t3.flac', durationMs: 235000, ratingCount: 5, viewCount: 0 },
    ];

    // Dedupe toggles all off — compilation suppression still applies.
    const preferOnly = applyTrackFiltersWithReport(tracks, { preferArtistFolderOverCompilation: true }, { duplicateLimit: 10 });
    assert.deepEqual(preferOnly.tracks.map((t) => t.ratingKey).sort(), ['art-mbid', 'art-path', 'art-studio', 'comp-live', 'comp-lonely']);
    assert.equal(preferOnly.duplicateCount, 2);
    assert.ok(preferOnly.duplicateMatches.every((m) => m.method === 'compilation'));

    // Without the flag, nothing is removed when dedupe is off.
    const off = applyTrackFilters(tracks, { preferArtistFolderOverCompilation: false });
    assert.equal(off.length, tracks.length);

    // With dedupe by MBID and the flag off, the higher-rated compilation copy wins.
    const mbidNoPrefer = applyTrackFilters(tracks, { deduplicateByMbid: true });
    assert.ok(mbidNoPrefer.some((t) => t.ratingKey === 'comp-mbid'));
    assert.ok(!mbidNoPrefer.some((t) => t.ratingKey === 'art-mbid'));

    // With dedupe by MBID and the flag on, the artist-folder copy is kept instead.
    const mbidPrefer = applyTrackFilters(tracks, { deduplicateByMbid: true, preferArtistFolderOverCompilation: true });
    assert.ok(mbidPrefer.some((t) => t.ratingKey === 'art-mbid'));
    assert.ok(!mbidPrefer.some((t) => t.ratingKey === 'comp-mbid'));
  });

  it('finds duplicate personal playlist names for the same user only', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-personal-dupes-${Date.now()}.db`);
    const db = initDb(dbPath);
    try {
      createUserPersonalPlaylist(db, 'MickyGX', {
        id: 'pp_existing',
        name: 'Britpop',
        rules: { tags: ['britpop'] },
      });

      const duplicate = findUserPersonalPlaylistByName(db, 'MickyGX', 'britpop');
      assert.equal(duplicate?.id, 'pp_existing');

      const excluded = findUserPersonalPlaylistByName(db, 'MickyGX', 'Britpop', { excludeId: 'pp_existing' });
      assert.equal(excluded, null);

      const otherUser = findUserPersonalPlaylistByName(db, 'Emma', 'Britpop');
      assert.equal(otherUser, null);
    } finally {
      db.close();
    }
  });

  it('rejects zero-track personal playlists unless they are explicitly saved as drafts', async () => {
    const now = Date.now();
    runDbStatement(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, `draft-track-${now}`, 'Draft Artist', 'Draft Track', 'Draft Album', `draft-mbid-${now}`, '["Rock"]', '1', `/music/draft-${now}.flac`, 180000, 0, 0, now);

    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const rejected = await client.postJson('/api/music/playlists/personal', {
      name: `Zero Draft ${now}`,
      genres: { include: ['Jazz'], exclude: [], includeMode: 'any' },
    }, '/settings');
    assert.equal(rejected.status, 422);
    assert.equal(rejected.json?.code, 'ZERO_TRACK_PLAYLIST');
    assert.equal(rejected.json?.canSaveDraft, true);

    const created = await client.postJson('/api/music/playlists/personal', {
      name: `Zero Draft ${now}`,
      genres: { include: ['Jazz'], exclude: [], includeMode: 'any' },
      allowEmptyDraft: true,
    }, '/settings');
    assert.equal(created.status, 200);
    assert.equal(created.json?.ok, true);
    assert.equal(created.json?.draft, true);

    const playlistId = String(created.json?.playlist?.id || '');
    assert.ok(playlistId);

    const draftRow = readDbRow(
      'SELECT id, name FROM user_personal_playlists WHERE user_plex_id = ? AND id = ?',
      'testadmin',
      playlistId,
    );
    assert.equal(draftRow?.name, `Zero Draft ${now}`);

    const db = initDb(join(process.env.DATA_DIR, 'curatorr.db'));
    try {
      const generated = listUserGeneratedPlaylists(db, 'testadmin', { activeOnly: false });
      const matchingGenerated = generated.find((entry) => entry.playlistKey === `personal:${playlistId}`);
      assert.equal(matchingGenerated, undefined);
    } finally {
      db.close();
    }

  });

  it('infers a smart-playlist wizard prefill from imported playlists and can remove the original import on create', async () => {
    const now = Date.now();
    runDbStatement(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, moods, library_key, file_path, duration_ms, rating_count, view_count,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, `import-track-1-${now}`, 'Import Artist', 'Import Track 1', 'Import Album', `import-mbid-1-${now}`, '["Electronic","Synthwave"]', '["Driving","Athletic"]', '1', `/music/import-1-${now}.flac`, 180000, 10, 3, now);
    runDbStatement(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, moods, library_key, file_path, duration_ms, rating_count, view_count,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, `import-track-2-${now}`, 'Import Artist', 'Import Track 2', 'Import Album', `import-mbid-2-${now}`, '["Electronic","Indie"]', '["Driving"]', '1', `/music/import-2-${now}.flac`, 182000, 8, 2, now);
    runDbStatement(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, moods, library_key, file_path, duration_ms, rating_count, view_count,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, `import-track-3-${now}`, 'Import Artist', 'Import Track 3', 'Import Album', `import-mbid-3-${now}`, '["Electronic","Rock"]', '["Athletic"]', '1', `/music/import-3-${now}.flac`, 184000, 7, 2, now);
    runDbStatement(`
      INSERT INTO track_enrichment (
        rating_key, recording_mbid, bpm, camelot_key, energy, danceability,
        analysis_source, analysis_confidence, payload_json, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, `import-track-1-${now}`, `import-mbid-1-${now}`, 132, '8A', 0.82, 0.71, 'test', 1, '{}', now);
    runDbStatement(`
      INSERT INTO track_enrichment (
        rating_key, recording_mbid, bpm, camelot_key, energy, danceability,
        analysis_source, analysis_confidence, payload_json, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, `import-track-2-${now}`, `import-mbid-2-${now}`, 128, '9A', 0.78, 0.68, 'test', 1, '{}', now);
    runDbStatement(`
      INSERT INTO track_enrichment (
        rating_key, recording_mbid, bpm, camelot_key, energy, danceability,
        analysis_source, analysis_confidence, payload_json, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, `import-track-3-${now}`, `import-mbid-3-${now}`, 136, '8B', 0.8, 0.73, 'test', 1, '{}', now);

    const db = initDb(join(process.env.DATA_DIR, 'curatorr.db'));
    try {
      saveUserGeneratedPlaylist(db, 'testadmin', {
        playlistKey: `custom-import-${now}`,
        playlistTitle: 'Imported Gym',
        playlistType: 'custom',
        plexPlaylistId: '',
        sourceType: 'spotify-playlist',
        sourceRef: `spotify-import-${now}`,
        sourceTitle: 'Imported Gym',
        sourceOwner: 'Import Owner',
        trackCount: 3,
        missingCount: 1,
        active: true,
        updatedAt: now,
      });
      saveArtistTags(db, 'Import Artist', ['road-trip', 'summer', 'night-drive', 'uplifting']);
    } finally {
      db.close();
    }
    runDbStatement(
      'INSERT INTO playlist_tracks (playlist_key, user_plex_id, rating_key, artist_name, added_at) VALUES (?, ?, ?, ?, ?)',
      `custom-import-${now}`, 'testadmin', `import-track-1-${now}`, 'Import Artist', now,
    );
    runDbStatement(
      'INSERT INTO playlist_tracks (playlist_key, user_plex_id, rating_key, artist_name, added_at) VALUES (?, ?, ?, ?, ?)',
      `custom-import-${now}`, 'testadmin', `import-track-2-${now}`, 'Import Artist', now,
    );
    runDbStatement(
      'INSERT INTO playlist_tracks (playlist_key, user_plex_id, rating_key, artist_name, added_at) VALUES (?, ?, ?, ?, ?)',
      `custom-import-${now}`, 'testadmin', `import-track-3-${now}`, 'Import Artist', now,
    );

    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const prefillResponse = await client.request(`/api/music/playlists/imported-convert?playlistKey=${encodeURIComponent(`custom-import-${now}`)}`);
    assert.equal(prefillResponse.status, 200);
    assert.equal(prefillResponse.json?.ok, true);
    assert.equal(prefillResponse.json?.prefill?.importSource?.playlistKey, `custom-import-${now}`);
    assert.equal(prefillResponse.json?.prefill?.keepImportedSource, true);
    assert.equal(prefillResponse.json?.prefill?.featurePreset, 'driving');
    assert.deepEqual(prefillResponse.json?.prefill?.genres?.include, ['Electronic']);
    assert.deepEqual(prefillResponse.json?.prefill?.tags?.include, []);
    assert.deepEqual(prefillResponse.json?.prefill?.importSuggestedContent?.genres?.include, ['Electronic']);
    assert.deepEqual(prefillResponse.json?.prefill?.importDetectedContent?.genres?.include, ['Electronic', 'Indie', 'Rock', 'Synthwave']);
    assert.deepEqual(prefillResponse.json?.prefill?.importDetectedContent?.tags?.include, ['night-drive', 'road-trip', 'summer', 'uplifting']);

    const created = await client.postJson('/api/music/playlists/personal', {
      name: `Imported Gym Smart ${now}`,
      genres: prefillResponse.json?.prefill?.genres,
      moods: prefillResponse.json?.prefill?.moods,
      tags: prefillResponse.json?.prefill?.tags,
      importSuggestedContent: prefillResponse.json?.prefill?.importSuggestedContent,
      importDetectedContent: prefillResponse.json?.prefill?.importDetectedContent,
      featurePreset: prefillResponse.json?.prefill?.featurePreset,
      bpmMin: prefillResponse.json?.prefill?.bpmMin,
      bpmMax: prefillResponse.json?.prefill?.bpmMax,
      energyMin: prefillResponse.json?.prefill?.energyMin,
      energyMax: prefillResponse.json?.prefill?.energyMax,
      danceabilityMin: prefillResponse.json?.prefill?.danceabilityMin,
      danceabilityMax: prefillResponse.json?.prefill?.danceabilityMax,
      importSource: prefillResponse.json?.prefill?.importSource,
      removeImportedSourcePlaylistKey: `custom-import-${now}`,
    }, '/settings');
    assert.equal(created.status, 200);
    assert.equal(created.json?.ok, true);
    assert.equal(created.json?.removedSourcePlaylistKey, `custom-import-${now}`);

    const personalRow = readDbRow(
      'SELECT id, name FROM user_personal_playlists WHERE user_plex_id = ? AND name = ?',
      'testadmin',
      `Imported Gym Smart ${now}`,
    );
    assert.ok(personalRow?.id);

    const verifyDb = initDb(join(process.env.DATA_DIR, 'curatorr.db'));
    try {
      const savedPersonal = findUserPersonalPlaylistByName(verifyDb, 'testadmin', `Imported Gym Smart ${now}`);
      assert.deepEqual(savedPersonal?.rules?.importSuggestedContent?.genres?.include, ['Electronic']);
      assert.deepEqual(savedPersonal?.rules?.importDetectedContent?.tags?.include, ['night-drive', 'road-trip', 'summer', 'uplifting']);
      const generated = listUserGeneratedPlaylists(verifyDb, 'testadmin', { activeOnly: false });
      assert.equal(generated.find((entry) => entry.playlistKey === `custom-import-${now}`), undefined);
    } finally {
      verifyDb.close();
    }
  });

  it('applies playlist feature presets and harmonic Camelot focus to master tracks', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-enrichment-${Date.now()}-feature-presets.db`);
    const manifestPath = join(testDir, `track-features-${Date.now()}-feature-presets.json`);
    const db = initDb(dbPath);
    const now = Date.now();

    const insertMaster = db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMaster.run('preset-1', 'Artist Club', 'Track Club', 'Album 1', 'mbid-preset-1', '[]', '1', '/music/preset-1.flac', 180000, 0, 0, now);
    insertMaster.run('preset-2', 'Artist Chill', 'Track Chill', 'Album 2', 'mbid-preset-2', '[]', '1', '/music/preset-2.flac', 180000, 0, 0, now);
    insertMaster.run('preset-3', 'Artist Harmonic', 'Track Harmonic', 'Album 3', 'mbid-preset-3', '[]', '1', '/music/preset-3.flac', 180000, 0, 0, now);

    await writeFile(manifestPath, JSON.stringify({
      tracks: [
        { recordingMbid: 'mbid-preset-1', bpm: 126, energy: 0.82, danceability: 0.61, camelotKey: '6A' },
        { recordingMbid: 'mbid-preset-2', bpm: 96, energy: 0.31, danceability: 0.42, camelotKey: '8B' },
        { recordingMbid: 'mbid-preset-3', bpm: 122, energy: 0.62, danceability: 0.58, camelotKey: '9A' },
      ],
    }), 'utf8');

    try {
      const service = createTrackEnrichmentService({
        db,
        pushLog: () => {},
        safeMessage: (err) => String(err?.message || err || 'Unknown error'),
      });
      const result = await service.importFeatureManifest({ manifestPath });
      assert.equal(result.imported, 3);

      const masterTracks = getMasterTracks(db);
      const clubTracks = applyFeaturePresetFilters(masterTracks, { featurePreset: 'club' });
      assert.deepEqual(clubTracks.map((track) => track.ratingKey), ['preset-1']);

      const harmonicTracks = applyFeaturePresetFilters(masterTracks, {
        featurePreset: 'harmonic',
        camelotFocus: '8A',
        camelotMode: 'adjacent',
      });
      assert.deepEqual(harmonicTracks.map((track) => track.ratingKey).sort(), ['preset-3']);

      const harmonicRelativeTracks = applyFeaturePresetFilters(masterTracks, {
        featurePreset: 'harmonic',
        camelotFocus: '8A',
        camelotMode: 'relative',
      });
      assert.deepEqual(harmonicRelativeTracks.map((track) => track.ratingKey).sort(), ['preset-2']);

      const harmonicFullTracks = applyFeaturePresetFilters(masterTracks, {
        featurePreset: 'harmonic',
        camelotFocus: '8A, 9A',
        camelotMode: 'harmonic',
      });
      assert.deepEqual(harmonicFullTracks.map((track) => track.ratingKey).sort(), ['preset-2', 'preset-3']);

      const customDrivingTracks = applyFeaturePresetFilters(masterTracks, {
        featurePreset: 'driving',
        bpmMax: 100,
        energyMin: 0.25,
        energyMax: 0.40,
      });
      assert.deepEqual(customDrivingTracks.map((track) => track.ratingKey), ['preset-2']);

      const missingFeatureTracks = applyFeaturePresetFilters([
        { ratingKey: 'missing-all', bpm: null, energy: null, danceability: null, camelotKey: '' },
        { ratingKey: 'missing-bpm', bpm: null, energy: 0.20, danceability: 0.20, camelotKey: '8A' },
        { ratingKey: 'valid', bpm: 92, energy: 0.22, danceability: 0.31, camelotKey: '8A' },
      ], {
        featurePreset: 'chill',
        bpmMax: 100,
        energyMax: 0.30,
      });
      assert.deepEqual(missingFeatureTracks.map((track) => track.ratingKey), ['valid']);
    } finally {
      db.close();
    }
  });

  it('sorts playlist candidates by analysis data and pushes missing feature data to the end', async () => {
    const bpmOrdered = sortPlaylistTracksByAnalysis([
      { ratingKey: 'missing', bpm: null, camelotKey: '', energy: null, danceability: null, rc: 99, tw: 0, pc: 0, artistName: 'Missing', trackTitle: 'Missing' },
      { ratingKey: 'mid', bpm: 118, camelotKey: '7A', energy: 0.58, danceability: 0.54, rc: 3, tw: 0, pc: 0, artistName: 'Mid', trackTitle: 'Mid' },
      { ratingKey: 'low', bpm: 92, camelotKey: '8A', energy: 0.20, danceability: 0.30, rc: 2, tw: 0, pc: 0, artistName: 'Low', trackTitle: 'Low' },
      { ratingKey: 'high', bpm: 128, camelotKey: '8B', energy: 0.82, danceability: 0.71, rc: 1, tw: 0, pc: 0, artistName: 'High', trackTitle: 'High' },
    ], 'bpmAsc');
    assert.deepEqual(bpmOrdered.map((track) => track.ratingKey), ['low', 'mid', 'high', 'missing']);

    const djOrdered = sortPlaylistTracksByAnalysis([
      { ratingKey: 'a', bpm: 120, camelotKey: '8A', energy: 0.60, rc: 3, tw: 0, pc: 0, artistName: 'A', trackTitle: 'A' },
      { ratingKey: 'b', bpm: 123, camelotKey: '8B', energy: 0.63, rc: 2, tw: 0, pc: 0, artistName: 'B', trackTitle: 'B' },
      { ratingKey: 'c', bpm: 126, camelotKey: '9A', energy: 0.66, rc: 1, tw: 0, pc: 0, artistName: 'C', trackTitle: 'C' },
      { ratingKey: 'missing', bpm: null, camelotKey: '', energy: null, rc: 99, tw: 0, pc: 0, artistName: 'Missing', trackTitle: 'Missing' },
    ], 'djFlow');
    assert.deepEqual(djOrdered.map((track) => track.ratingKey), ['a', 'b', 'c', 'missing']);
  });

  it('treats legacy builtin analyzer mode as sidecar mode', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-enrichment-${Date.now()}-builtin-alias.db`);
    const manifestPath = join(testDir, `track-features-builtin-alias-${Date.now()}.json`);
    const resultsPath = join(testDir, `track-features-builtin-alias-${Date.now()}.results.json`);
    const db = initDb(dbPath);
    const now = Date.now();

    db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run('builtin-alias-1', 'Artist Builtin Alias', 'Track Builtin Alias', 'Album Builtin Alias', 'mbid-builtin-alias-1', '[]', '1', '/media/music/builtin-alias-1.flac', 180000, 0, 0, now);

    const service = createTrackEnrichmentService({
      db,
      DB_PATH: dbPath,
      pushLog: () => {},
      safeMessage: (err) => String(err?.message || err || 'Unknown error'),
      execCommand: async () => {
        throw new Error('legacy builtin mode should not execute a local analyzer command');
      },
    });

    const sidecarServer = createServer(async (req, res) => {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const payload = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}');
      const chunkManifest = JSON.parse(await readFile(payload.inputPath, 'utf8'));
      const chunkTracks = Array.isArray(chunkManifest?.tracks) ? chunkManifest.tracks : [];
      await writeFile(payload.outputPath, JSON.stringify({
        tracks: chunkTracks.map((track) => ({
          recordingMbid: track.recordingMbid,
          bpm: 128,
          musicalKey: 'F minor',
          camelotKey: '4A',
          energy: 0.55,
          danceability: 0.66,
          analysisSource: 'curatorr-sidecar',
          analysisConfidence: 0.7,
        })),
      }), 'utf8');
      const body = JSON.stringify({ ok: true, outputPath: payload.outputPath });
      res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) });
      res.end(body);
    });
    await new Promise((resolve) => sidecarServer.listen(0, '127.0.0.1', resolve));
    const sidecarAddress = sidecarServer.address();
    const sidecarUrl = `http://127.0.0.1:${sidecarAddress.port}`;

    try {
      const result = await service.runAutomatedAnalysis({
        manifestPath,
        resultsPath,
        analyzerMode: 'builtin',
        analyzerSidecarUrl: sidecarUrl,
      });
      assert.equal(result.ok, true);
      assert.equal(result.analyzerMode, 'sidecar');
      assert.equal(result.importResult.imported, 1);

      const rows = getTrackEnrichmentByRatingKeys(db, ['builtin-alias-1']);
      assert.equal(rows[0]?.bpm, 128);
      assert.equal(rows[0]?.musicalKey, 'F minor');
      assert.equal(rows[0]?.camelotKey, '4A');
      assert.equal(rows[0]?.analysisSource, 'curatorr-sidecar');
    } finally {
      await new Promise((resolve, reject) => sidecarServer.close((err) => (err ? reject(err) : resolve())));
      db.close();
    }
  });

  it('runs the analyzer sidecar pipeline and imports analyzer output', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-enrichment-${Date.now()}-sidecar.db`);
    const manifestPath = join(testDir, `track-features-sidecar-${Date.now()}.json`);
    const resultsPath = join(testDir, `track-features-sidecar-${Date.now()}.results.json`);
    const db = initDb(dbPath);
    const now = Date.now();

    db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run('sidecar-1', 'Artist Sidecar', 'Track Sidecar', 'Album Sidecar', 'mbid-sidecar-1', '[]', '1', '/media/music/sidecar-1.flac', 180000, 0, 0, now);
    db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run('sidecar-2', 'Artist Sidecar', 'Track Sidecar 2', 'Album Sidecar', 'mbid-sidecar-2', '[]', '1', '/media/music/sidecar-2.flac', 181000, 0, 0, now);

    const service = createTrackEnrichmentService({
      db,
      DB_PATH: dbPath,
      pushLog: () => {},
      safeMessage: (err) => String(err?.message || err || 'Unknown error'),
      execCommand: async () => {
        throw new Error('sidecar mode should not execute a local analyzer command');
      },
    });

    let requestCount = 0;
    const sidecarServer = createServer(async (req, res) => {
      requestCount += 1;
      assert.equal(req.method, 'POST');
      assert.equal(req.url, '/analyze');
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const payload = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}');
      assert.match(payload.inputPath, /track-features-sidecar-.*\.json$/);
      assert.match(payload.outputPath, /track-features-sidecar-.*\.results(?:\.chunk-\d+)?\.json$/);
      const chunkManifest = JSON.parse(await readFile(payload.inputPath, 'utf8'));
      const chunkTracks = Array.isArray(chunkManifest?.tracks) ? chunkManifest.tracks : [];
      await writeFile(payload.outputPath, JSON.stringify({
        tracks: chunkTracks.map((track, index) => ({
          recordingMbid: track.recordingMbid,
          bpm: index === 0 && track.recordingMbid === 'mbid-sidecar-1' ? 124 : 126,
          musicalKey: track.recordingMbid === 'mbid-sidecar-1' ? 'A minor' : 'C major',
          camelotKey: track.recordingMbid === 'mbid-sidecar-1' ? '8A' : '8B',
          energy: track.recordingMbid === 'mbid-sidecar-1' ? 0.63 : 0.61,
          danceability: track.recordingMbid === 'mbid-sidecar-1' ? 0.58 : 0.57,
          analysisSource: 'curatorr-sidecar',
          analysisConfidence: 0.74,
        })),
      }), 'utf8');
      const body = JSON.stringify({ ok: true, outputPath: payload.outputPath });
      res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) });
      res.end(body);
    });
    await new Promise((resolve) => sidecarServer.listen(0, '127.0.0.1', resolve));
    const sidecarAddress = sidecarServer.address();
    const sidecarUrl = `http://127.0.0.1:${sidecarAddress.port}`;

    try {
      const result = await service.runAutomatedAnalysis({
        manifestPath,
        resultsPath,
        analyzerMode: 'sidecar',
        analyzerSidecarUrl: sidecarUrl,
        chunkSize: 1,
      });
      assert.equal(result.ok, true);
      assert.equal(result.analyzerMode, 'sidecar');
      assert.equal(result.importResult.imported, 2);
      assert.equal(result.sidecarResult?.ok, true);
      assert.equal(result.chunkCount, 2);
      assert.equal(requestCount, 2);

      const rows = getTrackEnrichmentByRatingKeys(db, ['sidecar-1', 'sidecar-2']);
      assert.equal(rows[0]?.bpm, 124);
      assert.equal(rows[0]?.musicalKey, 'A minor');
      assert.equal(rows[0]?.camelotKey, '8A');
      assert.equal(rows[0]?.analysisSource, 'curatorr-sidecar');
      assert.equal(rows[1]?.bpm, 126);
      assert.equal(rows[1]?.musicalKey, 'C major');
      assert.equal(rows[1]?.camelotKey, '8B');
    } finally {
      await new Promise((resolve, reject) => sidecarServer.close((err) => (err ? reject(err) : resolve())));
      db.close();
    }
  });

  it('includes sidecar error details when automated analysis fails', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-enrichment-${Date.now()}-sidecar-error.db`);
    const manifestPath = join(testDir, `track-features-sidecar-error-${Date.now()}.json`);
    const resultsPath = join(testDir, `track-features-sidecar-error-${Date.now()}.results.json`);
    const db = initDb(dbPath);
    const now = Date.now();

    db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run('sidecar-error-1', 'Artist Error', 'Track Error', 'Album Error', 'mbid-sidecar-error-1', '[]', '1', '/media/music/sidecar-error-1.flac', 180000, 0, 0, now);

    const service = createTrackEnrichmentService({
      db,
      DB_PATH: dbPath,
      pushLog: () => {},
      safeMessage: (err) => String(err?.message || err || 'Unknown error'),
    });

    const sidecarServer = createServer(async (req, res) => {
      assert.equal(req.method, 'POST');
      assert.equal(req.url, '/analyze');
      for await (const _chunk of req) {}
      const body = JSON.stringify({
        ok: false,
        error: 'analysis-failed',
        message: 'decoder crashed',
        stderr: 'Traceback: boom',
      });
      res.writeHead(500, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) });
      res.end(body);
    });
    await new Promise((resolve) => sidecarServer.listen(0, '127.0.0.1', resolve));
    const sidecarAddress = sidecarServer.address();
    const sidecarUrl = `http://127.0.0.1:${sidecarAddress.port}`;

    try {
      await assert.rejects(async () => {
        await service.runAutomatedAnalysis({
          manifestPath,
          resultsPath,
          analyzerMode: 'sidecar',
          analyzerSidecarUrl: sidecarUrl,
          chunkSize: 100,
        });
      }, /Chunk 1\/1 failed: External request failed \(500\): analysis-failed \| decoder crashed \| Traceback: boom/);
    } finally {
      await new Promise((resolve, reject) => sidecarServer.close((err) => (err ? reject(err) : resolve())));
      db.close();
    }
  });

  it('imports Plex loudness metrics into track enrichment via the public metadata API', async () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-enrichment-${Date.now()}-plex-loudness.db`);
    const db = initDb(dbPath);
    const logs = [];
    const fetchUrls = [];
    const now = Date.now();

    const insertMaster = db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMaster.run('loud-1', 'Artist A', 'Track 1', 'Album 1', 'mbid-loud-1', '[]', '1', '/music/loud-1.flac', 180000, 0, 0, now);
    insertMaster.run('loud-2', 'Artist B', 'Track 2', 'Album 2', 'mbid-loud-2', '[]', '1', '/music/loud-2.m4a', 190000, 0, 0, now);

    db.prepare(`
      INSERT INTO track_enrichment (
        rating_key, recording_mbid, track_year, original_release_date,
        analysis_source, analysis_confidence, payload_json, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run('loud-1', 'mbid-loud-1', 2001, '2001-01-02', 'musicbrainz', 0.95, '{}', now);

    const service = createTrackEnrichmentService({
      db,
      loadConfig: () => ({
        mediaServer: { type: 'plex' },
        plex: { url: 'http://plex.local', token: 'plex-admin-token' },
      }),
      pushLog: (entry) => logs.push(entry),
      safeMessage: (err) => String(err?.message || err || 'Unknown error'),
    });

    const originalFetch = global.fetch;
    global.fetch = async (url) => {
      const target = String(url || '');
      fetchUrls.push(target);
      if (target !== 'http://plex.local/library/metadata/loud-1'
        && target !== 'http://plex.local/library/metadata/loud-2') {
        throw new Error(`Unexpected Plex loudness URL: ${target}`);
      }
      return new Response(JSON.stringify({
        MediaContainer: {
          Metadata: [
            {
              ratingKey: 'loud-1',
              Media: [{
                id: 101,
                Part: [{
                  id: 201,
                  file: '/music/loud-1.flac',
                  Stream: [{
                    id: 301,
                    streamType: 2,
                    selected: true,
                    loudness: '-9.4',
                    lra: '4.1',
                    peak: '0.97',
                    gain: '-10.2',
                    albumGain: '-9.9',
                    albumPeak: '0.99',
                    albumRange: '5.3',
                  }],
                }],
              }],
            },
            {
              ratingKey: 'loud-2',
              Media: [{
                id: 102,
                Part: [{
                  id: 202,
                  file: '/music/loud-2.m4a',
                  Stream: [{
                    id: 302,
                    streamType: 2,
                    selected: true,
                    loudness: '-12.8',
                    lra: '6.2',
                    peak: '0.93',
                    gain: '-11.4',
                    albumGain: '-11.1',
                    albumPeak: '0.95',
                    albumRange: '6.8',
                  }],
                }],
              }],
            },
          ],
        },
      }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    };

    try {
      const result = await service.runPlexLoudnessSync({ limit: 1, batchSize: 1 });
      assert.equal(result.processed, 2);
      assert.equal(result.synced, 2);
      assert.equal(result.failed, 0);
      assert.equal(fetchUrls.length, 2);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    const dbVerify = initDb(dbPath);
    try {
      const rows = getTrackEnrichmentByRatingKeys(dbVerify, ['loud-1', 'loud-2']);
      const loud1 = rows.find((row) => row.ratingKey === 'loud-1');
      const loud2 = rows.find((row) => row.ratingKey === 'loud-2');
      assert.equal(loud1?.trackYear, 2001);
      assert.equal(loud1?.loudness, -9.4);
      assert.equal(loud1?.loudnessRange, 4.1);
      assert.equal(loud1?.trackGain, -10.2);
      assert.equal(loud1?.albumGain, -9.9);
      assert.equal(loud1?.payload?.plexLoudness?.streamId, 301);
      assert.equal(loud2?.loudness, -12.8);
      assert.equal(loud2?.albumRange, 6.8);
    } finally {
      dbVerify.close();
    }

    assert.ok(logs.some((entry) => entry?.action === 'plex-loudness.finish'));
  });

  it('excludes obvious silence tracks from the Plex loudness queue', () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-enrichment-${Date.now()}-plex-loudness-silence.db`);
    const db = initDb(dbPath);
    const now = Date.now();

    const insertMaster = db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMaster.run('silence-1', 'Blur', '[silence]', 'Think Tank', '', '[]', '1', '/music/blur-silence.mp3', 1000, 0, 0, now);
    insertMaster.run('song-1', 'Blur', 'Out of Time', 'Think Tank', '', '[]', '1', '/music/out-of-time.mp3', 240000, 0, 0, now);

    try {
      assert.equal(countTracksMissingPlexLoudness(db), 1);
      const rows = listTracksMissingPlexLoudness(db, { limit: 10 });
      assert.equal(rows.length, 1);
      assert.equal(rows[0]?.ratingKey, 'song-1');
    } finally {
      db.close();
    }
  });

  it('applies loudness smoothing to Daily Mix during sync when enabled', async () => {
    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const userId = `daily-mix-loudness-${Date.now()}`;
    const logs = [];
    const now = Date.now();

    const insertMaster = db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertMaster.run('dm-l1', 'Artist A', 'Track 1', 'Album A', '', '[]', '1', '/music/dm-l1.flac', 180000, 0, 0, now);
    insertMaster.run('dm-l2', 'Artist B', 'Track 2', 'Album B', '', '[]', '1', '/music/dm-l2.flac', 180000, 0, 0, now);
    insertMaster.run('dm-l3', 'Artist C', 'Track 3', 'Album C', '', '[]', '1', '/music/dm-l3.flac', 180000, 0, 0, now);
    insertMaster.run('dm-l4', 'Artist D', 'Track 4', 'Album D', '', '[]', '1', '/music/dm-l4.flac', 180000, 0, 0, now);

    const insertEnrichment = db.prepare(`
      INSERT INTO track_enrichment (
        rating_key, recording_mbid, loudness, loudness_range, peak,
        analysis_source, analysis_confidence, payload_json, updated_at
      ) VALUES (?, '', ?, ?, ?, ?, ?, '{}', ?)
    `);
    insertEnrichment.run('dm-l1', -8.0, 4.0, 0.98, 'plex-loudness', 1, now);
    insertEnrichment.run('dm-l2', -20.0, 5.0, 0.92, 'plex-loudness', 1, now);
    insertEnrichment.run('dm-l3', -9.0, 3.5, 0.97, 'plex-loudness', 1, now);
    insertEnrichment.run('dm-l4', -10.0, 3.0, 0.96, 'plex-loudness', 1, now);

    const insertStats = db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        tier, tier_weight, last_played_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, ?)
    `);
    insertStats.run('dm-l1', userId, 'Track 1', 'Artist A', 'Album A', 15, 'belter', 1.5, now, now);
    insertStats.run('dm-l2', userId, 'Track 2', 'Artist B', 'Album B', 14, 'belter', 1.5, now, now);
    insertStats.run('dm-l3', userId, 'Track 3', 'Artist C', 'Album C', 13, 'decent', 0.5, now, now);
    insertStats.run('dm-l4', userId, 'Track 4', 'Artist D', 'Album D', 12, 'decent', 0.5, now, now);

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
          dailyMix: {
            favoriteLimit: 4,
            suggestedLimit: 0,
            freshLimit: 0,
            maxTracks: 4,
            maxTracksPerArtist: 1,
            repeatCooldownDays: 0,
            useSonicOrdering: false,
            useLoudnessOrdering: true,
            loudnessLookahead: 3,
            maxLoudnessStepDb: 20,
          },
        },
      }),
      saveConfig: () => {},
      buildPlexAuthHeaders: (token, extraHeaders = {}) => ({ ...extraHeaders, 'X-Plex-Token': token }),
      resolveUserPlexServerToken: () => 'plex-user-token',
      userHasOwnPlexToken: () => true,
      pushLog: (entry) => logs.push(entry),
    });

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target.startsWith('http://plex.local/playlists?playlistType=audio')) {
        return new Response(JSON.stringify({ MediaContainer: { Metadata: [] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target.startsWith('http://plex.local/playlists?type=audio&title=')) {
        return new Response(JSON.stringify({ MediaContainer: { Metadata: [{ ratingKey: 'plex-daily-mix-loudness' }] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      if (target === 'http://plex.local/playlists/plex-daily-mix-loudness/items' && String(options.method || 'GET').toUpperCase() === 'DELETE') {
        return new Response('', { status: 200 });
      }
      if (target.startsWith('http://plex.local/playlists/plex-daily-mix-loudness/items?uri=')) {
        const decoded = decodeURIComponent(target);
        assert.match(decoded, /library\/metadata\/dm-l1,dm-l3,dm-l4,dm-l2/);
        return new Response('', { status: 200 });
      }
      throw new Error(`Unexpected fetch in Daily Mix loudness test: ${target}`);
    };

    try {
      const built = service.buildDailyMix(userId);
      assert.deepEqual(built.trackKeys, ['dm-l1', 'dm-l2', 'dm-l3', 'dm-l4']);

      const synced = await service.syncDailyMix(userId);
      assert.deepEqual(synced.trackKeys, ['dm-l1', 'dm-l3', 'dm-l4', 'dm-l2']);
      assert.equal(synced.loudnessApplied, true);
    } finally {
      global.fetch = originalFetch;
      db.close();
    }

    assert.ok(logs.some((entry) => entry?.action === 'loudness.ordering'));
  });

  it('replays zero-duration Last.fm scrobbles as plays during stats rebuilds', () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-lastfm-rebuild-${Date.now()}.db`);
    const db = initDb(dbPath);
    const userPlexId = 'issue-78-user';
    const plexRatingKey = 'issue-78-track';
    const artistName = 'Issue 78 Artist';
    const smartConfig = {
      skipThresholdSeconds: 30,
      completionThresholdSeconds: 30,
      skipWeight: -1,
      belterWeight: 1,
    };
    const startedAt = Date.now() - 60_000;

    const insertEvent = db.prepare(`
      INSERT INTO play_events (
        user_plex_id, plex_rating_key, track_title, artist_name, album_name, library_key,
        started_at, ended_at, duration_ms, track_duration_ms, is_skip, event_source, session_key
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertEvent.run(
      userPlexId,
      plexRatingKey,
      'Issue 78 Track',
      artistName,
      'Issue 78 Album',
      '1',
      startedAt,
      startedAt,
      0,
      240000,
      0,
      'lastfm_backfill',
      'issue-78-backfill',
    );
    insertEvent.run(
      userPlexId,
      plexRatingKey,
      'Issue 78 Track',
      artistName,
      'Issue 78 Album',
      '1',
      startedAt + 1000,
      startedAt + 1000,
      0,
      0,
      0,
      'lastfm_sync',
      'issue-78-sync',
    );

    try {
      const trackSnapshot = rebuildTrackStatsFromEvents(db, {
        userPlexId,
        plexRatingKey,
        songSkipLimit: 1,
        smartConfig,
      });
      const artistSnapshot = rebuildArtistStatsFromEvents(db, {
        userPlexId,
        artistName,
        smartConfig,
      });

      assert.equal(trackSnapshot?.playCount, 2);
      assert.equal(trackSnapshot?.skipCount, 0);
      assert.equal(trackSnapshot?.consecutiveSkips, 0);
      assert.equal(trackSnapshot?.excludedFromSmart, 0);
      assert.equal(trackSnapshot?.tier, 'belter');
      assert.equal(artistSnapshot?.playCount, 2);
      assert.equal(artistSnapshot?.skipCount, 0);
      assert.equal(artistSnapshot?.consecutiveSkips, 0);
    } finally {
      db.close();
    }
  });

  it('marks stale running jobs as interrupted when the scheduler starts', () => {
    const dbPath = join(process.env.DATA_DIR, `curatorr-jobs-${Date.now()}-interrupted.db`);
    const db = initDb(dbPath);
    const logs = [];
    const startedAt = Date.now() - 60_000;

    db.prepare(`
      INSERT INTO system_job_runs (job_id, status, last_run_at, message, updated_at)
      VALUES (?, ?, ?, ?, ?)
    `).run('trackAnalysisPipeline', 'running', startedAt, 'Analyzing chunk 2/162…', startedAt);

    const service = createJobService({
      db,
      loadConfig: () => ({ jobs: {} }),
      pushLog: (entry) => logs.push(entry),
      safeMessage: (err) => String(err?.message || err || 'Unknown error'),
    }, {});

    service.startAll(false);

    try {
      const row = db.prepare('SELECT status, message, last_run_at FROM system_job_runs WHERE job_id = ?').get('trackAnalysisPipeline');
      assert.equal(row?.status, 'error');
      assert.match(String(row?.message || ''), /Interrupted by app restart/);
      assert.equal(row?.last_run_at, startedAt);
      assert.ok(logs.some((entry) => entry?.action === 'job.interrupted'));
    } finally {
      db.close();
    }
  });

  it('returns summary-first track overview data with metadata and audio sections', async () => {
    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const ratingKey = `overview-track-${Date.now()}`;
    const originalConfig = await readConfig();
    await writeConfig({
      ...originalConfig,
      plex: {
        ...(originalConfig.plex || {}),
        url: 'http://plex.local',
        token: 'plex-secret-token',
      },
    });

    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const now = Date.now();
    db.prepare(`
      INSERT INTO master_tracks (
        rating_key, artist_name, track_title, album_name, recording_mbid,
        genres, moods, library_key, file_path, duration_ms, rating_count, view_count, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      ratingKey,
      'Bruce Springsteen',
      'Darlington County',
      'Born in the U.S.A.',
      '',
      JSON.stringify(['Heartland Rock', 'Pop/Rock']),
      JSON.stringify(['Earnest', 'Driving']),
      '1',
      '/music/Bruce Springsteen/Born in the U.S.A./Darlington County.flac',
      304000,
      12438,
      0,
      now,
    );
    db.prepare(`
      INSERT INTO track_enrichment (
        rating_key, recording_mbid, track_year, original_release_date, bpm,
        musical_key, camelot_key, energy, danceability, loudness, loudness_range,
        peak, track_gain, album_gain, album_peak, album_range,
        analysis_source, analysis_confidence, payload_json, updated_at
      ) VALUES (?, '', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)
    `).run(
      ratingKey,
      1984,
      '1984-06-04',
      128,
      'D major',
      '10B',
      0.78,
      0.54,
      -8.9,
      7.2,
      -0.3,
      -6.1,
      -5.8,
      -0.2,
      8.0,
      'curatorr_analyzer',
      1,
      now,
    );
    db.prepare(`
      INSERT INTO track_stats (
        plex_rating_key, user_plex_id, track_title, artist_name, album_name,
        play_count, skip_count, consecutive_skips, excluded_from_smart,
        manually_included, tier, tier_weight, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?)
    `).run(
      ratingKey,
      'testadmin',
      'Darlington County',
      'Bruce Springsteen',
      'Born in the U.S.A.',
      4,
      1,
      0,
      'belter',
      1.5,
      now,
    );

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target.startsWith(baseUrl)) return originalFetch(url, options);
      if (target === `http://plex.local/library/metadata/${encodeURIComponent(ratingKey)}`) {
        return new Response(JSON.stringify({
          MediaContainer: {
            Metadata: [{
              ratingKey,
              title: 'Darlington County',
              summary: 'A widescreen highway song with a weary, reflective pull.',
              originalTitle: 'Bruce Springsteen',
              grandparentTitle: 'Bruce Springsteen',
              parentTitle: 'Born in the U.S.A.',
              duration: 304000,
            }],
          },
        }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      throw new Error(`Unexpected fetch in track overview test: ${target}`);
    };

    try {
      const overviewRes = await client.request(`/api/music/overview/track/${encodeURIComponent(ratingKey)}`);
      assert.equal(overviewRes.status, 200);
      assert.equal(overviewRes.json?.item?.overview, 'A widescreen highway song with a weary, reflective pull.');
      assert.ok((overviewRes.json?.item?.pills || []).includes('🔥 Top 1 on album'));
      assert.deepEqual(
        overviewRes.json?.item?.actions,
        [{ kind: 'track-pin-toggle', label: 'Pin track', payload: { ratingKey, included: true } }],
      );
      assert.deepEqual(
        (overviewRes.json?.item?.detailSections || []).map((section) => section.title),
        ['Metadata', 'Audio Profile', 'Library'],
      );
      const metadataSection = overviewRes.json?.item?.detailSections?.find((section) => section.title === 'Metadata');
      assert.ok(metadataSection);
      assert.ok(metadataSection.rows.some((row) => row.label === 'Genre' && row.value.includes('Heartland Rock')));
      assert.ok(metadataSection.rows.some((row) => row.label === 'Mood' && row.value.includes('Driving')));
      assert.ok(metadataSection.rows.some((row) => row.label === 'Plex rating count' && row.value === '12,438'));
      const audioSection = overviewRes.json?.item?.detailSections?.find((section) => section.title === 'Audio Profile');
      assert.ok(audioSection);
      assert.ok(audioSection.rows.some((row) => row.label === 'BPM' && row.value === '128'));
      assert.ok(audioSection.rows.some((row) => row.label === 'Analysis source' && row.value === 'curatorr_analyzer'));
      const librarySection = overviewRes.json?.item?.detailSections?.find((section) => section.title === 'Library');
      assert.ok(librarySection.rows.some((row) => row.label === 'File' && row.value.includes('Darlington County.flac')));
    } finally {
      global.fetch = originalFetch;
      db.close();
      await writeConfig(originalConfig);
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

  it('rejects loopback-only Curatorr URLs when configuring the Plex webhook', async () => {
    const originalConfig = await readConfig();
    await writeConfig({
      ...originalConfig,
      general: {
        ...(originalConfig.general || {}),
        localUrl: 'http://localhost:7676',
        remoteUrl: '',
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
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target.startsWith('https://plex.tv/api/v2/user/webhooks') || target.startsWith('http://plex.local')) {
        throw new Error(`Unexpected upstream fetch during loopback webhook validation: ${target}`);
      }
      return originalFetch(url, options);
    };

    try {
      const webhookRes = await client.postJson('/api/wizard/configure-plex-webhook', {}, '/settings');
      assert.equal(webhookRes.status, 400);
      assert.equal(
        webhookRes.json?.reason,
        'Curatorr local or remote URL points to localhost/127.0.0.1. Set a reachable LAN or remote URL before registering the Plex webhook.',
      );
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

  it('starts scheduled jobs and records master refresh status when the server wizard completes', async () => {
    const originalConfig = await readConfig();
    runDbStatement('DELETE FROM system_job_runs WHERE job_id = ?', 'masterTrackRefresh');
    runDbStatement('DELETE FROM master_tracks WHERE rating_key = ?', 'wizard-job-track-1');

    await writeConfig({
      ...originalConfig,
      wizard: { completed: false },
      mediaServer: { ...(originalConfig.mediaServer || {}), type: 'plex' },
      plex: {
        ...(originalConfig.plex || {}),
        url: 'http://plex.local',
        token: 'plex-secret-token',
        machineId: 'wizard-machine',
        libraries: ['12'],
      },
      jobs: {
        ...(originalConfig.jobs || {}),
        masterTrackRefresh: { intervalMinutes: 360, enabled: true },
      },
    });

    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

    const originalFetch = global.fetch;
    global.fetch = async (url, options = {}) => {
      const target = String(url || '');
      if (target.startsWith(baseUrl)) return originalFetch(url, options);

      if (target.startsWith('http://plex.local/library/sections/12/all?')) {
        return new Response(JSON.stringify({
          MediaContainer: {
            totalSize: 1,
            Metadata: [{
              ratingKey: 'wizard-job-track-1',
              originalTitle: 'Wizard Job Artist',
              title: 'Wizard Job Track',
              parentTitle: 'Wizard Job Album',
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
        return new Response(JSON.stringify({ MediaContainer: { Directory: [] } }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      throw new Error('Unexpected fetch in server wizard jobs test: ' + target);
    };

    try {
      const completeRes = await client.postForm('/wizard/lidarr', { skip: '1' }, '/wizard?step=5');
      assert.equal(completeRes.status, 200);

      const row = await waitForDbRow(
        'SELECT status, last_run_at, message FROM system_job_runs WHERE job_id = ?',
        (candidate) => candidate?.status === 'success',
        ['masterTrackRefresh'],
        { timeoutMs: 1500 },
      );
      assert.equal(row?.status, 'success');
      assert.ok(Number(row?.last_run_at || 0) > 0);
    } finally {
      global.fetch = originalFetch;
      await writeConfig(originalConfig);
      runDbStatement('DELETE FROM system_job_runs WHERE job_id = ?', 'masterTrackRefresh');
      runDbStatement('DELETE FROM master_tracks WHERE rating_key = ?', 'wizard-job-track-1');
    }
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

  it('redirects the local setup admin away from the personal user wizard', async () => {
    const config = await readConfig();
    await writeConfig({
      ...config,
      wizard: { completed: true, completedAt: new Date().toISOString() },
    });

    const { client, response } = await login('testadmin', 'TestPassword1!');
    assert.equal(response.status, 302);
    const wizardRes = await client.request('/wizard/user');
    assert.equal(wizardRes.status, 302);
    assert.equal(wizardRes.location, '/overview');
  });

  it('keeps overlapping media-server music rows during default setup-admin startup cleanup', () => {
    runDbStatement("DELETE FROM user_preferences WHERE user_plex_id IN ('testadmin', 'test@curatorr.test')");
    runDbStatement("DELETE FROM play_events WHERE user_plex_id IN ('testadmin', 'test@curatorr.test')");
    runDbStatement("DELETE FROM track_stats WHERE user_plex_id IN ('testadmin', 'test@curatorr.test')");
    runDbStatement("DELETE FROM artist_stats WHERE user_plex_id IN ('testadmin', 'test@curatorr.test')");

    const now = Date.now();
    runDbStatement("INSERT INTO user_preferences (user_plex_id, user_wizard_completed) VALUES ('testadmin', 1)");
    runDbStatement("INSERT INTO user_preferences (user_plex_id, user_wizard_completed) VALUES ('test@curatorr.test', 1)");
    runDbStatement(`
      INSERT INTO play_events
        (user_plex_id, plex_rating_key, track_title, artist_name, album_name, started_at, ended_at, duration_ms, event_source)
      VALUES
        ('testadmin', 'overlap-track-1', 'Overlap Track', 'Overlap Artist', 'Overlap Album', ${now}, ${now + 1000}, 1000, 'plex_webhook')
    `);
    runDbStatement(`
      INSERT INTO track_stats
        (plex_rating_key, user_plex_id, track_title, artist_name, album_name, play_count)
      VALUES
        ('overlap-track-1', 'testadmin', 'Overlap Track', 'Overlap Artist', 'Overlap Album', 1)
    `);
    runDbStatement("INSERT INTO artist_stats (artist_name, user_plex_id, play_count) VALUES ('Overlap Artist', 'testadmin', 1)");

    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    const previousFlag = process.env.CURATORR_ENABLE_SETUP_ADMIN_MUSIC_CLEANUP;
    try {
      delete process.env.CURATORR_ENABLE_SETUP_ADMIN_MUSIC_CLEANUP;
      const result = cleanupSetupAdminMusicState(db);
      assert.equal(result.skipped, true);
      assert.equal(Number(result.dbCleanup?.totalChanges || 0), 0);
    } finally {
      if (previousFlag === undefined) delete process.env.CURATORR_ENABLE_SETUP_ADMIN_MUSIC_CLEANUP;
      else process.env.CURATORR_ENABLE_SETUP_ADMIN_MUSIC_CLEANUP = previousFlag;
      db.close();
    }

    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM user_preferences WHERE user_plex_id = 'testadmin'")?.count, 1);
    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM user_preferences WHERE user_plex_id = 'test@curatorr.test'")?.count, 1);
    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM play_events WHERE user_plex_id = 'testadmin'")?.count, 1);
    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM track_stats WHERE user_plex_id = 'testadmin'")?.count, 1);
    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM artist_stats WHERE user_plex_id = 'testadmin'")?.count, 1);
  });

  it('explicitly purges setup-admin music rows without touching other users', () => {
    runDbStatement("DELETE FROM user_preferences WHERE user_plex_id IN ('testadmin', 'test@curatorr.test', 'listener1')");
    runDbStatement("DELETE FROM play_events WHERE user_plex_id IN ('testadmin', 'test@curatorr.test', 'listener1')");
    runDbStatement("DELETE FROM track_stats WHERE user_plex_id IN ('testadmin', 'test@curatorr.test', 'listener1')");
    runDbStatement("DELETE FROM artist_stats WHERE user_plex_id IN ('testadmin', 'test@curatorr.test', 'listener1')");
    runDbStatement("DELETE FROM suggested_artists WHERE user_plex_id IN ('testadmin', 'test@curatorr.test', 'listener1')");
    runDbStatement("DELETE FROM lidarr_requests WHERE user_plex_id IN ('testadmin', 'test@curatorr.test', 'listener1')");
    runDbStatement("DELETE FROM playlist_rule_templates WHERE user_plex_id IN ('testadmin', 'test@curatorr.test', 'listener1')");

    runDbStatement("INSERT INTO user_preferences (user_plex_id, user_wizard_completed) VALUES ('testadmin', 1)");
    runDbStatement("INSERT INTO user_preferences (user_plex_id, user_wizard_completed) VALUES ('listener1', 1)");
    runDbStatement("INSERT INTO suggested_artists (user_plex_id, artist_name) VALUES ('testadmin', 'Cleanup Artist')");
    runDbStatement("INSERT INTO suggested_artists (user_plex_id, artist_name) VALUES ('listener1', 'Keep Artist')");
    runDbStatement("INSERT INTO lidarr_requests (user_plex_id, artist_name) VALUES ('testadmin', 'Cleanup Artist')");
    runDbStatement("INSERT INTO lidarr_requests (user_plex_id, artist_name) VALUES ('listener1', 'Keep Artist')");
    runDbStatement("INSERT INTO playlist_rule_templates (id, user_plex_id, name, rules) VALUES ('setup-template', 'testadmin', 'Setup Admin', '{}')");
    runDbStatement("INSERT INTO playlist_rule_templates (id, user_plex_id, name, rules) VALUES ('listener-template', 'listener1', 'Listener', '{}')");

    const dbPath = join(process.env.DATA_DIR, 'curatorr.db');
    const db = initDb(dbPath);
    try {
      const result = purgeUserScopedMusicData(db, ['testadmin', 'test@curatorr.test']);
      assert.ok(Number(result.totalChanges || 0) >= 4);
      assert.equal(Number(result.tableCounts.user_preferences || 0), 1);
      assert.equal(Number(result.tableCounts.suggested_artists || 0), 1);
      assert.equal(Number(result.tableCounts.lidarr_requests || 0), 1);
      assert.equal(Number(result.tableCounts.playlist_rule_templates || 0), 1);
    } finally {
      db.close();
    }

    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM user_preferences WHERE user_plex_id = 'testadmin'")?.count, 0);
    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM suggested_artists WHERE user_plex_id = 'testadmin'")?.count, 0);
    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM lidarr_requests WHERE user_plex_id = 'testadmin'")?.count, 0);
    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM playlist_rule_templates WHERE user_plex_id = 'testadmin'")?.count, 0);

    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM user_preferences WHERE user_plex_id = 'listener1'")?.count, 1);
    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM suggested_artists WHERE user_plex_id = 'listener1'")?.count, 1);
    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM lidarr_requests WHERE user_plex_id = 'listener1'")?.count, 1);
    assert.equal(readDbRow("SELECT COUNT(*) AS count FROM playlist_rule_templates WHERE user_plex_id = 'listener1'")?.count, 1);
  });
});
