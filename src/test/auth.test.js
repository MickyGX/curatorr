import { describe, it, before, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// Set env vars BEFORE importing the app — must happen before any dynamic import
const testDir = join(tmpdir(), `launcharr-test-${process.pid}`);
process.env.CONFIG_PATH = join(testDir, 'config.json');
process.env.DATA_DIR = join(testDir, 'data');
process.env.SESSION_SECRET = 'test-secret-do-not-use-in-prod';
process.env.PLEX_CLIENT_ID = 'test-client-id'; // skip file I/O for plex client id

// Dynamic imports so env vars are set first
const { default: supertest } = await import('supertest');
const { app } = await import('../index.js');
const {
  resetLoginAttempts,
  checkLoginRateLimit,
  recordLoginFailure,
  clearLoginFailures,
} = await import('../routes/auth.js');

const request = supertest(app);

// ---------------------------------------------------------------------------
// Unit tests — rate limiter pure helpers
// ---------------------------------------------------------------------------

describe('rate limiter helpers', () => {
  afterEach(() => resetLoginAttempts());

  it('returns null for an unknown IP', () => {
    assert.equal(checkLoginRateLimit('1.2.3.4'), null);
  });

  it('returns null while under the failure threshold', () => {
    for (let i = 0; i < 9; i++) recordLoginFailure('1.2.3.4');
    assert.equal(checkLoginRateLimit('1.2.3.4'), null);
  });

  it('returns minutes remaining once threshold is reached', () => {
    for (let i = 0; i < 10; i++) recordLoginFailure('1.2.3.4');
    const mins = checkLoginRateLimit('1.2.3.4');
    assert.ok(mins !== null, 'should be blocked');
    assert.ok(mins >= 1, 'should report at least 1 minute remaining');
  });

  it('clearLoginFailures removes the block immediately', () => {
    for (let i = 0; i < 10; i++) recordLoginFailure('1.2.3.4');
    clearLoginFailures('1.2.3.4');
    assert.equal(checkLoginRateLimit('1.2.3.4'), null);
  });

  it('treats each IP independently', () => {
    for (let i = 0; i < 10; i++) recordLoginFailure('1.1.1.1');
    assert.ok(checkLoginRateLimit('1.1.1.1') !== null, '1.1.1.1 should be blocked');
    assert.equal(checkLoginRateLimit('2.2.2.2'), null, '2.2.2.2 should be unaffected');
  });
});

// ---------------------------------------------------------------------------
// Integration tests — auth routes
// ---------------------------------------------------------------------------

describe('auth routes', () => {
  // Create the admin user once before any login tests run
  before(async () => {
    await request.post('/setup').type('form').send({
      username: 'testadmin',
      email: 'test@launcharr.test',
      password: 'TestPassword1!',
      confirmPassword: 'TestPassword1!',
    });
  });

  afterEach(() => resetLoginAttempts());

  it('GET /login returns 200 when an admin exists', async () => {
    const res = await request.get('/login');
    assert.equal(res.status, 200);
  });

  it('POST /login with wrong password returns 401', async () => {
    const res = await request.post('/login').type('form').send({
      username: 'testadmin',
      password: 'wrongpassword',
    });
    assert.equal(res.status, 401);
  });

  it('POST /login with correct credentials redirects to /dashboard', async () => {
    const res = await request.post('/login').type('form').send({
      username: 'testadmin',
      password: 'TestPassword1!',
    });
    assert.equal(res.status, 302);
    assert.ok(res.headers.location?.includes('/dashboard'), 'should redirect to /dashboard');
  });

  it('POST /login returns 429 after 10 failed attempts', async () => {
    for (let i = 0; i < 10; i++) {
      await request.post('/login').type('form').send({ username: 'testadmin', password: 'wrong' });
    }
    const res = await request.post('/login').type('form').send({ username: 'testadmin', password: 'wrong' });
    assert.equal(res.status, 429);
  });

  it('successful login clears the rate limit counter', async () => {
    // 9 failures — one short of the limit
    for (let i = 0; i < 9; i++) {
      await request.post('/login').type('form').send({ username: 'testadmin', password: 'wrong' });
    }
    // Successful login should clear the counter
    await request.post('/login').type('form').send({ username: 'testadmin', password: 'TestPassword1!' });
    // Now 9 more failures should NOT trigger rate limiting (counter was reset)
    for (let i = 0; i < 9; i++) {
      await request.post('/login').type('form').send({ username: 'testadmin', password: 'wrong' });
    }
    const res = await request.post('/login').type('form').send({ username: 'testadmin', password: 'wrong' });
    assert.equal(res.status, 401, 'should be 401 not 429 — counter was reset by successful login');
  });
});

// ---------------------------------------------------------------------------
// Integration tests — route guards
// ---------------------------------------------------------------------------

describe('route guards (unauthenticated)', () => {
  it('GET / redirects to /login', async () => {
    const res = await request.get('/');
    assert.equal(res.status, 302);
    assert.ok(res.headers.location?.includes('/login') || res.headers.location?.includes('/dashboard'));
  });

  it('GET /dashboard redirects to /login', async () => {
    const res = await request.get('/dashboard');
    assert.equal(res.status, 302);
    assert.ok(res.headers.location?.includes('/login'), 'should redirect to /login');
  });

  it('GET /settings redirects to /login', async () => {
    const res = await request.get('/settings');
    assert.equal(res.status, 302);
    assert.ok(res.headers.location?.includes('/login'), 'should redirect to /login');
  });

  it('GET /apps/plex returns 302 to /login', async () => {
    const res = await request.get('/apps/plex');
    assert.equal(res.status, 302);
    assert.ok(res.headers.location?.includes('/login'), 'should redirect to /login');
  });
});
