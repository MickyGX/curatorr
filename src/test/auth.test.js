import { after, afterEach, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { mkdir, readFile, writeFile } from 'node:fs/promises';

const testDir = join(tmpdir(), `curatorr-test-${process.pid}`);
process.env.CONFIG_PATH = join(testDir, 'config.json');
process.env.DATA_DIR = join(testDir, 'data');
process.env.SESSION_SECRET = 'test-secret-do-not-use-in-prod';
process.env.PLEX_CLIENT_ID = 'test-client-id';
process.env.CURATORR_DISABLE_AUTOSTART = '1';
process.env.PORT = String(37000 + (process.pid % 1000));

const baseUrl = `http://127.0.0.1:${process.env.PORT}`;

const { start, stop } = await import('../index.js');
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

async function login(username, password) {
  const client = createClient();
  const response = await client.postForm('/login', { username, password }, '/login');
  return { client, response };
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

  it('blocks co-admin access to admin-only wizard actions', async () => {
    const { client, response } = await login('coadmin', 'TestPassword1!');
    assert.equal(response.status, 302);

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
