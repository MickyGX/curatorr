import crypto from 'crypto';

// Rate limiting for POST /login — in-memory, resets on restart (acceptable for self-hosted)
const loginAttempts = new Map(); // ip -> { failures: number, windowStart: ms }

const PIN_MAX_AGE_MS = 15 * 60 * 1000; // Plex PIN valid for 15 minutes

function isValidPlexPinId(v) {
  const s = String(v || '').trim();
  return /^\d+$/.test(s) && s.length > 0 && s.length <= 20;
}

const RATE_LIMIT_MAX = 10;
const RATE_LIMIT_WINDOW_MS = 15 * 60 * 1000; // 15 minutes

setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of loginAttempts) {
    if (now - entry.windowStart > RATE_LIMIT_WINDOW_MS) loginAttempts.delete(ip);
  }
}, 30 * 60 * 1000).unref();

function getClientIp(req) {
  return req.ip || req.socket?.remoteAddress || 'unknown';
}

// Returns minutes remaining if blocked, null if not blocked.
function checkLoginRateLimit(ip) {
  const now = Date.now();
  const entry = loginAttempts.get(ip);
  if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW_MS) return null;
  if (entry.failures >= RATE_LIMIT_MAX) {
    return Math.max(1, Math.ceil((RATE_LIMIT_WINDOW_MS - (now - entry.windowStart)) / 60000));
  }
  return null;
}

function recordLoginFailure(ip) {
  const now = Date.now();
  const entry = loginAttempts.get(ip);
  if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW_MS) {
    loginAttempts.set(ip, { failures: 1, windowStart: now });
  } else {
    entry.failures += 1;
  }
}

function clearLoginFailures(ip) {
  loginAttempts.delete(ip);
}

// Rate limiting for POST /setup — 5 failures per 15 min per IP
const setupAttempts = new Map();
const SETUP_RATE_LIMIT_MAX = 5;

setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of setupAttempts) {
    if (now - entry.windowStart > RATE_LIMIT_WINDOW_MS) setupAttempts.delete(ip);
  }
}, 30 * 60 * 1000).unref();

function checkSetupRateLimit(ip) {
  const now = Date.now();
  const entry = setupAttempts.get(ip);
  if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW_MS) return null;
  if (entry.failures >= SETUP_RATE_LIMIT_MAX) {
    return Math.max(1, Math.ceil((RATE_LIMIT_WINDOW_MS - (now - entry.windowStart)) / 60000));
  }
  return null;
}

function recordSetupFailure(ip) {
  const now = Date.now();
  const entry = setupAttempts.get(ip);
  if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW_MS) {
    setupAttempts.set(ip, { failures: 1, windowStart: now });
  } else {
    entry.failures += 1;
  }
}

// Rate limiting for GET /api/plex/pin/status — 120 requests per 5 min per IP
const plexPinStatusAttempts = new Map();
const PLEX_PIN_STATUS_RATE_MAX = 120;
const PLEX_PIN_STATUS_WINDOW_MS = 5 * 60 * 1000;

setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of plexPinStatusAttempts) {
    if (now - entry.windowStart > PLEX_PIN_STATUS_WINDOW_MS) plexPinStatusAttempts.delete(ip);
  }
}, 10 * 60 * 1000).unref();

function checkPlexPinStatusRateLimit(ip) {
  const now = Date.now();
  const entry = plexPinStatusAttempts.get(ip);
  if (!entry || now - entry.windowStart > PLEX_PIN_STATUS_WINDOW_MS) return null;
  if (entry.count >= PLEX_PIN_STATUS_RATE_MAX) {
    return Math.max(1, Math.ceil((PLEX_PIN_STATUS_WINDOW_MS - (now - entry.windowStart)) / 60000));
  }
  return null;
}

function recordPlexPinStatusRequest(ip) {
  const now = Date.now();
  const entry = plexPinStatusAttempts.get(ip);
  if (!entry || now - entry.windowStart > PLEX_PIN_STATUS_WINDOW_MS) {
    plexPinStatusAttempts.set(ip, { count: 1, windowStart: now });
  } else {
    entry.count += 1;
  }
}

// Exported for testing only
export function resetLoginAttempts() { loginAttempts.clear(); }
export { checkLoginRateLimit, recordLoginFailure, clearLoginFailures };

function normalizePostLoginRedirectPath(value, fallback = '/dashboard') {
  const raw = String(value || '').trim();
  if (!raw) return fallback;
  if (!raw.startsWith('/') || raw.startsWith('//')) return fallback;
  const normalized = raw;
  const lowerPath = normalized.split('?')[0].toLowerCase();
  if (['/login', '/logout', '/setup', '/auth/plex'].includes(lowerPath)) return fallback;
  return normalized;
}

function setPostLoginRedirect(req, value) {
  try {
    if (!req?.session) return;
    req.session.postLoginRedirect = normalizePostLoginRedirectPath(value, '/dashboard');
  } catch (err) {
    /* ignore session write failures */
  }
}

function consumePostLoginRedirect(req, fallback = '/dashboard') {
  try {
    const next = normalizePostLoginRedirectPath(req?.session?.postLoginRedirect, fallback);
    if (req?.session) delete req.session.postLoginRedirect;
    return next;
  } catch (err) {
    return fallback;
  }
}

export function registerAuth(app, ctx) {
  const {
    loadConfig,
    saveConfig,
    hasLocalAdmin,
    resolveLocalUsers,
    serializeLocalUsers,
    verifyPassword,
    hashPassword,
    setSessionUser,
    updateUserLogins,
    resolvePublicBaseUrl,
    pushLog,
    buildAppApiUrl,
    exchangePinWithRetry,
    exchangePin,
    completePlexLogin,
    safeMessage,
    PRODUCT,
    PLATFORM,
    DEVICE_NAME,
    CLIENT_ID,
    LOCAL_AUTH_MIN_PASSWORD,
    validateLocalPasswordStrength,
  } = ctx;

  app.get('/', (req, res) => {
    const user = req.session?.user || null;
    if (!user) return res.redirect('/login');
    return res.redirect('/dashboard');
  });

  app.get('/login', (req, res) => {
    const user = req.session?.user || null;
    if (user) return res.redirect(consumePostLoginRedirect(req, '/dashboard'));
    if (req.query?.next) setPostLoginRedirect(req, req.query.next);
    const config = loadConfig();
    if (!hasLocalAdmin(config)) return res.redirect('/setup');
    res.render('login', {
      title: 'Launcharr',
      product: PRODUCT,
      allowLocalLogin: true,
      error: null,
      info: null,
    });
  });

  app.post('/login', (req, res) => {
    const user = req.session?.user || null;
    if (user) return res.redirect(consumePostLoginRedirect(req, '/dashboard'));
    if (req.body?.next) setPostLoginRedirect(req, req.body.next);

    const ip = getClientIp(req);
    const blockedMinutes = checkLoginRateLimit(ip);
    if (blockedMinutes !== null) {
      pushLog({ level: 'warn', app: 'system', action: 'login.ratelimit', message: `Rate limit reached from ${ip}.` });
      return res.status(429).render('login', {
        title: 'Launcharr',
        product: PRODUCT,
        allowLocalLogin: true,
        error: `Too many failed login attempts. Try again in ${blockedMinutes} minute${blockedMinutes === 1 ? '' : 's'}.`,
        info: null,
      });
    }

    const config = loadConfig();
    const users = resolveLocalUsers(config);
    if (!users.length) return res.redirect('/setup');
    const identifier = String(req.body?.username || '').trim();
    const password = String(req.body?.password || '');
    const match = users.find((entry) => {
      const username = String(entry.username || '').trim().toLowerCase();
      const email = String(entry.email || '').trim().toLowerCase();
      const candidate = identifier.toLowerCase();
      return candidate && (candidate === username || candidate === email);
    });

    if (!match || !verifyPassword(password, match)) {
      recordLoginFailure(ip);
      const nowBlocked = checkLoginRateLimit(ip);
      const suffix = nowBlocked !== null
        ? ` Too many failed attempts — try again in ${nowBlocked} minute${nowBlocked === 1 ? '' : 's'}.`
        : '';
      return res.status(401).render('login', {
        title: 'Launcharr',
        product: PRODUCT,
        allowLocalLogin: true,
        error: `Invalid username/email or password.${suffix}`,
        info: null,
      });
    }

    clearLoginFailures(ip);
    setSessionUser(req, match, 'local');
    const loginConfig = updateUserLogins(config, {
      identifier: match.email || match.username,
      launcharr: true,
    });
    if (loginConfig !== config) saveConfig(loginConfig);
    return res.redirect(consumePostLoginRedirect(req, '/dashboard'));
  });

  app.get('/setup', (req, res) => {
    const user = req.session?.user || null;
    if (user) return res.redirect('/dashboard');
    const config = loadConfig();
    if (hasLocalAdmin(config)) return res.redirect('/login');
    res.render('setup', {
      title: 'Launcharr Setup',
      minPassword: LOCAL_AUTH_MIN_PASSWORD,
      error: null,
      values: {
        username: '',
        email: '',
      },
    });
  });

  app.post('/setup', (req, res) => {
    const user = req.session?.user || null;
    if (user) return res.redirect('/dashboard');
    const config = loadConfig();
    if (hasLocalAdmin(config)) return res.redirect('/login');

    const ip = getClientIp(req);
    const setupBlocked = checkSetupRateLimit(ip);
    if (setupBlocked !== null) {
      return res.status(429).render('setup', {
        title: 'Launcharr Setup',
        minPassword: LOCAL_AUTH_MIN_PASSWORD,
        error: `Too many failed attempts. Try again in ${setupBlocked} minute${setupBlocked === 1 ? '' : 's'}.`,
        values: { username: '', email: '' },
      });
    }

    const username = String(req.body?.username || '').trim();
    const email = String(req.body?.email || '').trim();
    const password = String(req.body?.password || '');
    const confirm = String(req.body?.confirmPassword || '');
    const values = { username, email };

    if (!username) {
      recordSetupFailure(ip);
      return res.status(400).render('setup', {
        title: 'Launcharr Setup',
        minPassword: LOCAL_AUTH_MIN_PASSWORD,
        error: 'Username is required.',
        values,
      });
    }
    if (!email || !email.includes('@')) {
      recordSetupFailure(ip);
      return res.status(400).render('setup', {
        title: 'Launcharr Setup',
        minPassword: LOCAL_AUTH_MIN_PASSWORD,
        error: 'A valid email is required.',
        values,
      });
    }
    const passwordStrengthError = validateLocalPasswordStrength(password);
    if (passwordStrengthError) {
      recordSetupFailure(ip);
      return res.status(400).render('setup', {
        title: 'Launcharr Setup',
        minPassword: LOCAL_AUTH_MIN_PASSWORD,
        error: passwordStrengthError,
        values,
      });
    }
    if (password !== confirm) {
      recordSetupFailure(ip);
      return res.status(400).render('setup', {
        title: 'Launcharr Setup',
        minPassword: LOCAL_AUTH_MIN_PASSWORD,
        error: 'Passwords do not match.',
        values,
      });
    }

    const users = resolveLocalUsers(config);
    const exists = users.find((entry) => String(entry.username || '').toLowerCase() === username.toLowerCase());
    if (exists) {
      recordSetupFailure(ip);
      return res.status(400).render('setup', {
        title: 'Launcharr Setup',
        minPassword: LOCAL_AUTH_MIN_PASSWORD,
        error: 'Username already exists.',
        values,
      });
    }

    const salt = crypto.randomBytes(16).toString('hex');
    const passwordHash = hashPassword(password, salt);
    const newUser = {
      username,
      email,
      role: 'admin',
      passwordHash,
      salt,
      avatar: '',
      createdBy: 'setup',
      setupAccount: true,
      systemCreated: true,
      createdAt: new Date().toISOString(),
    };

    saveConfig({ ...config, users: serializeLocalUsers([...users, newUser]) });
    setSessionUser(req, newUser, 'local');
    return res.redirect('/dashboard');
  });

  app.get('/auth/plex', async (req, res) => {
    try {
      // Always use the request's actual Host header for the callback URL so the
      // forwardUrl matches the origin the browser is accessing from.  This keeps
      // the session cookie in scope when Plex redirects back.  Fall back to the
      // configured public base URL only when no Host header is present.
      const reqProto = (String(req.headers?.['x-forwarded-proto'] || '').split(',')[0].trim()) || req.protocol || 'http';
      const reqHost  = (String(req.headers?.['x-forwarded-host'] || '').split(',')[0].trim()) || req.get('host') || '';
      const authBaseUrl = reqHost ? `${reqProto}://${reqHost}` : resolvePublicBaseUrl(req);
      // Generate a per-flow nonce to protect the callback against login CSRF.
      // Stored in session; client embeds it in forwardUrl; we verify on callback.
      const plexState = crypto.randomBytes(20).toString('hex');
      if (req.session) {
        req.session.plexState = plexState;
        req.session.pinIssuedAt = null; // reset any stale pin from a previous flow
        req.session.pinId = null;
      }
      pushLog({
        level: 'info',
        app: 'plex',
        action: 'login.start',
        message: 'Plex login started.',
        meta: null,
      });
      return res.render('plex-auth', {
        title: 'Plex Login',
        callbackUrl: buildAppApiUrl(authBaseUrl, 'oauth/callback').toString(),
        plexState,
        client: {
          id: CLIENT_ID,
          product: PRODUCT,
          platform: PLATFORM,
          deviceName: DEVICE_NAME,
        },
      });
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'plex',
        action: 'login.start',
        message: safeMessage(err) || 'Plex login failed.',
      });
      return res.status(500).send(`Login failed: ${safeMessage(err)}`);
    }
  });

  app.post('/api/plex/pin', (req, res) => {
    try {
      const pinId = String(req.body?.pinId || '').trim();
      if (!isValidPlexPinId(pinId)) return res.status(400).json({ error: 'Invalid pinId.' });
      req.session.pinId = pinId;
      req.session.pinIssuedAt = Date.now();
      return res.json({ ok: true });
    } catch (err) {
      return res.status(500).json({ error: safeMessage(err) || 'Failed to store PIN.' });
    }
  });

  app.get('/oauth/callback', async (req, res) => {
    try {
      // ── Session / CSRF check ───────────────────────────────────────────────
      // Plex's auth SPA does not forward arbitrary query params through forwardUrl,
      // so we cannot use a state-in-URL round-trip. Instead we verify that this
      // session legitimately started a Plex login (plexState was set by /auth/plex).
      // Primary CSRF protection: sessionPin must match queryPin (below).
      const sessionState = String(req.session?.plexState || '').trim();
      if (!sessionState) {
        pushLog({
          level: 'warn',
          app: 'plex',
          action: 'login.callback',
          message: 'Plex callback rejected: no active login session.',
        });
        return res.status(400).send('No active login session. Please start the login again.');
      }

      // ── PIN validation ─────────────────────────────────────────────────────
      // Session is authoritative; query-string pinId is not accepted.
      const pinId = String(req.session?.pinId || '').trim();

      if (!isValidPlexPinId(pinId)) {
        pushLog({ level: 'error', app: 'plex', action: 'login.callback', message: 'Missing or invalid PIN.' });
        return res.status(400).send('Missing PIN session. Start login again.');
      }

      // ── PIN expiry check ───────────────────────────────────────────────────
      const issuedAt = Number(req.session?.pinIssuedAt || 0);
      if (!issuedAt || Date.now() - issuedAt > PIN_MAX_AGE_MS) {
        pushLog({ level: 'warn', app: 'plex', action: 'login.callback', message: 'Plex PIN expired.' });
        return res.status(400).send('Login session expired. Please start the login again.');
      }

      // ── Exchange PIN for token ─────────────────────────────────────────────
      const pinResult = await exchangePinWithRetry(pinId);
      const authToken = pinResult?.token || null;
      if (!authToken) {
        pushLog({
          level: 'error',
          app: 'plex',
          action: 'login.callback',
          message: 'Plex login not completed.',
          meta: {
            pinId: String(pinId || ''),
            attempts: pinResult?.attempts || 0,
            lastError: pinResult?.error || '',
          },
        });
        return res.status(401).send('Plex login not completed. Try again.');
      }

      await completePlexLogin(req, authToken);
      res.redirect(consumePostLoginRedirect(req, '/dashboard'));
    } catch (err) {
      pushLog({
        level: 'error',
        app: 'plex',
        action: 'login.callback',
        message: safeMessage(err) || 'Plex login callback failed.',
      });
      const status = err?.status || 500;
      res.status(status).send(`Login failed: ${safeMessage(err)}`);
    }
  });

  app.get('/api/plex/pin/status', async (req, res) => {
    try {
      const ip = getClientIp(req);
      const plexPinBlocked = checkPlexPinStatusRateLimit(ip);
      if (plexPinBlocked !== null) {
        return res.status(429).json({ error: `Too many requests. Try again in ${plexPinBlocked} minute${plexPinBlocked === 1 ? '' : 's'}.` });
      }
      recordPlexPinStatusRequest(ip);

      const pinId = String(req.session?.pinId || '').trim();
      if (!isValidPlexPinId(pinId)) return res.status(400).json({ error: 'Missing pinId.' });

      const issuedAt = Number(req.session?.pinIssuedAt || 0);
      if (issuedAt && Date.now() - issuedAt > PIN_MAX_AGE_MS) {
        return res.status(400).json({ error: 'PIN expired.' });
      }

      const authToken = await exchangePin(pinId);
      if (!authToken) return res.json({ ok: false });
      await completePlexLogin(req, authToken);
      return res.json({ ok: true, redirect: consumePostLoginRedirect(req, '/dashboard') });
    } catch (err) {
      const status = err?.status || 500;
      return res.status(status).json({ error: safeMessage(err) || 'PIN status check failed.' });
    }
  });

  const logoutHandler = (req, res) => {
    const user = req.session?.user || {};
    pushLog({
      level: 'info',
      app: 'system',
      action: 'logout',
      message: 'User logged out.',
      meta: { user: user.username || user.email || '' },
    });
    req.session = null;
    return res.redirect('/');
  };

  app.post('/logout', logoutHandler);
  app.get('/logout', (_req, res) => {
    return res.status(405).send('Method Not Allowed. Use POST /logout.');
  });
}
