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

function serializePendingPlexHomeUsers(homeUsers) {
  if (!Array.isArray(homeUsers)) return [];
  return homeUsers
    .map((user) => {
      const id = String(user?.uuid || user?.id || '').trim();
      if (!id) return null;
      return {
        id,
        title: String(user?.title || user?.username || 'User').trim() || 'User',
        protected: Boolean(user?.protected),
        admin: Boolean(user?.admin),
      };
    })
    .filter(Boolean);
}

export async function completePlexWizardTokenFetch(ctx, req, authToken) {
  const {
    loadConfig,
    saveConfig,
    fetchPlexResources,
    resolvePlexServerToken,
    resolvePlexMachineIdentifier,
    fetchPlexMusicLibraries,
    pushLog,
    safeMessage,
  } = ctx;

  const config = loadConfig();
  const plexCfg = (config?.plex && typeof config.plex === 'object') ? config.plex : {};
  const plexUrl = String(plexCfg.url || plexCfg.localUrl || plexCfg.remoteUrl || '').trim();
  if (!plexUrl) {
    return { ok: false, step: 2, message: 'Set your Plex server URL first, then fetch the token again.' };
  }

  try {
    const lookup = { machineId: plexCfg.machineId || '', plexUrl };
    const resources = await fetchPlexResources(authToken);
    const machineId = resolvePlexMachineIdentifier(resources, lookup) || String(plexCfg.machineId || '').trim();
    const serverToken = String(
      resolvePlexServerToken(resources, { machineId, plexUrl })
      || plexCfg.token
      || ''
    ).trim();

    if (!serverToken) {
      return {
        ok: false,
        step: 2,
        message: 'Could not resolve a Plex server token for this server. Sign in with the Plex server owner account.',
      };
    }

    let libraries = [];
    try {
      libraries = await fetchPlexMusicLibraries(plexUrl, serverToken);
    } catch (err) {
      saveConfig({
        ...config,
        mediaServer: { ...config.mediaServer, type: 'plex' },
        plex: {
          ...plexCfg,
          url: plexUrl,
          token: serverToken,
          machineId,
        },
      });
      pushLog({
        level: 'warn',
        app: 'wizard',
        action: 'plex.token-fetch.libraries.error',
        message: 'Plex token saved, but libraries could not be fetched: ' + safeMessage(err),
      });
      return {
        ok: false,
        step: 2,
        message: 'Plex token saved, but Curatorr could not fetch your music libraries yet. Check the server URL and try again.',
      };
    }

    saveConfig({
      ...config,
      mediaServer: { ...config.mediaServer, type: 'plex' },
      plex: {
        ...plexCfg,
        url: plexUrl,
        token: serverToken,
        machineId,
        availableLibraries: libraries,
      },
    });

    if (!libraries.length) {
      return {
        ok: false,
        step: 2,
        message: 'Plex token saved, but no music libraries were found. Add a music library in Plex first.',
      };
    }

    req.session.plexServerToken = serverToken;
    pushLog({
      level: 'info',
      app: 'wizard',
      action: 'plex.token-fetch.success',
      message: 'Plex token fetched from owner login and saved for the setup wizard.',
    });
    return {
      ok: true,
      step: 3,
      message: 'Plex connected. Token saved. Select the music libraries Curatorr should use.',
    };
  } catch (err) {
    pushLog({
      level: 'error',
      app: 'wizard',
      action: 'plex.token-fetch.error',
      message: safeMessage(err),
    });
    return {
      ok: false,
      step: 2,
      message: 'Could not fetch a Plex token: ' + safeMessage(err),
    };
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
    fetchPlexHomeUsers,
    fetchPlexResources,
    fetchPlexMusicLibraries,
    switchPlexHomeUser,
    resolvePlexServerToken,
    resolvePlexMachineIdentifier,
    safeMessage,
    PRODUCT,
    PLATFORM,
    DEVICE_NAME,
    CLIENT_ID,
    LOCAL_AUTH_MIN_PASSWORD,
    validateLocalPasswordStrength,
    db,
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
      title: 'Curatorr',
      product: PRODUCT,
      allowLocalLogin: true,
      mediaServerType: String(config?.mediaServer?.type || 'plex'),
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
        title: 'Curatorr',
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
        title: 'Curatorr',
        product: PRODUCT,
        allowLocalLogin: true,
        error: `Invalid username/email or password.${suffix}`,
        info: null,
      });
    }

    if (String(match.role || '').trim().toLowerCase() === 'disabled') {
      return res.status(403).render('login', {
        title: 'Curatorr',
        product: PRODUCT,
        allowLocalLogin: true,
        error: 'This account is disabled.',
        info: null,
      });
    }

    clearLoginFailures(ip);
    setSessionUser(req, match, 'local');
    const loginConfig = updateUserLogins(config, {
      identifier: match.email || match.username,
      curatorr: true,
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
      title: 'Curatorr Setup',
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
        title: 'Curatorr Setup',
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
        title: 'Curatorr Setup',
        minPassword: LOCAL_AUTH_MIN_PASSWORD,
        error: 'Username is required.',
        values,
      });
    }
    if (!email || !email.includes('@')) {
      recordSetupFailure(ip);
      return res.status(400).render('setup', {
        title: 'Curatorr Setup',
        minPassword: LOCAL_AUTH_MIN_PASSWORD,
        error: 'A valid email is required.',
        values,
      });
    }
    const passwordStrengthError = validateLocalPasswordStrength(password);
    if (passwordStrengthError) {
      recordSetupFailure(ip);
      return res.status(400).render('setup', {
        title: 'Curatorr Setup',
        minPassword: LOCAL_AUTH_MIN_PASSWORD,
        error: passwordStrengthError,
        values,
      });
    }
    if (password !== confirm) {
      recordSetupFailure(ip);
      return res.status(400).render('setup', {
        title: 'Curatorr Setup',
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
        title: 'Curatorr Setup',
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

    const nextConfig = { ...config, users: serializeLocalUsers([...users, newUser]) };
    saveConfig(nextConfig);
    setSessionUser(req, newUser, 'local');
    if (!nextConfig.wizard?.completed) return res.redirect('/wizard');
    return res.redirect('/dashboard');
  });

  app.get('/auth/plex', async (req, res) => {
    try {
      const popupRequested = String(req.query?.popup || '').trim() === '1';
      const authPurpose = String(req.query?.purpose || '').trim().toLowerCase();
      if (req.query?.next) setPostLoginRedirect(req, req.query.next);
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
        req.session.plexAuthMode = popupRequested ? 'popup' : 'redirect';
        req.session.plexAuthPurpose = authPurpose;
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

      const authPurpose = String(req.session?.plexAuthPurpose || '').trim().toLowerCase();
      if (authPurpose === 'wizard-server-token') {
        const result = await completePlexWizardTokenFetch({
          loadConfig,
          saveConfig,
          fetchPlexResources,
          resolvePlexServerToken,
          resolvePlexMachineIdentifier,
          fetchPlexMusicLibraries,
          pushLog,
          safeMessage,
        }, req, authToken);
        if (req.session) {
          req.session.serverWizardNotice = {
            type: result.ok ? 'info' : 'error',
            message: result.message,
          };
          req.session.plexState = null;
          req.session.pinId = null;
          req.session.pinIssuedAt = null;
          delete req.session.plexAuthMode;
          delete req.session.plexAuthPurpose;
        }
        return res.redirect(consumePostLoginRedirect(req, `/wizard?step=${result.step}`));
      }

      // ── Check for Plex Home users ──────────────────────────────────────────
      let homeUsers = [];
      try {
        homeUsers = await fetchPlexHomeUsers(authToken);
      } catch (_err) {
        // Non-fatal: if home users can't be fetched, fall through to normal login
      }

      if (homeUsers.length > 1) {
        // Store main token and home users for the selection step
        req.session.plexMainToken = authToken;
        req.session.pendingHomeUsers = serializePendingPlexHomeUsers(homeUsers);
        req.session.pendingHomeUsersAt = Date.now();
        // Clean up PIN/state (preserve plexAuthMode and postLoginRedirect)
        req.session.plexState = null;
        req.session.pinId = null;
        req.session.pinIssuedAt = null;
        pushLog({ level: 'info', app: 'plex', action: 'login.homeusers', message: `Plex home: ${homeUsers.length} users found, showing selection.` });
        return res.redirect('/auth/plex/home-users');
      }

      await completePlexLogin(req, authToken);
      const authMode = String(req.session?.plexAuthMode || 'redirect').trim().toLowerCase();
      if (req.session) delete req.session.plexAuthMode;
      const redirectTarget = consumePostLoginRedirect(req, '/dashboard');
      if (authMode === 'popup') {
        return res.render('plex-auth-complete', {
          title: 'Plex Login Complete',
          redirectTarget,
        });
      }
      return res.redirect(redirectTarget);
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
      const redirectFallback = normalizePostLoginRedirectPath(req.query?.next, '/dashboard');
      const ip = getClientIp(req);
      const plexPinBlocked = checkPlexPinStatusRateLimit(ip);
      if (plexPinBlocked !== null) {
        return res.status(429).json({ error: `Too many requests. Try again in ${plexPinBlocked} minute${plexPinBlocked === 1 ? '' : 's'}.` });
      }
      recordPlexPinStatusRequest(ip);

      if (req.session?.user) {
        return res.json({ ok: true, redirect: redirectFallback });
      }

      const pinId = String(req.session?.pinId || '').trim();
      if (!isValidPlexPinId(pinId)) return res.status(400).json({ error: 'Missing pinId.' });

      const issuedAt = Number(req.session?.pinIssuedAt || 0);
      if (issuedAt && Date.now() - issuedAt > PIN_MAX_AGE_MS) {
        return res.status(400).json({ error: 'PIN expired.' });
      }

      // If the home-users selection flow is already in progress (set by /oauth/callback),
      // don't touch the session — let the popup window complete it.
      if (req.session?.plexMainToken) return res.json({ ok: false });

      const authToken = await exchangePin(pinId);
      if (!authToken) return res.json({ ok: false });

      // Check for Plex Home users — same logic as /oauth/callback — so the poll
      // doesn't bypass home-user selection and log the user in as the main admin.
      let homeUsers = [];
      try { homeUsers = await fetchPlexHomeUsers(authToken); } catch (_err) { /* non-fatal */ }
      if (homeUsers.length > 1) {
        // Home-user selection is required; the popup will handle it.
        // Return ok:false so the login page keeps waiting.
        return res.json({ ok: false });
      }

      await completePlexLogin(req, authToken);
      return res.json({ ok: true, redirect: consumePostLoginRedirect(req, redirectFallback) });
    } catch (err) {
      const status = err?.status || 500;
      return res.status(status).json({ error: safeMessage(err) || 'PIN status check failed.' });
    }
  });

  // ─── Plex Home User flow ─────────────────────────────────────────────────────

  const HOME_USER_PENDING_MAX_AGE_MS = 5 * 60 * 1000; // 5 minutes to complete selection

  // Rate limiting for home user PIN attempts — 5 failures per 15 min per IP
  const homeUserPinAttempts = new Map();
  const HOME_USER_PIN_RATE_MAX = 5;

  setInterval(() => {
    const now = Date.now();
    for (const [ip, entry] of homeUserPinAttempts) {
      if (now - entry.windowStart > RATE_LIMIT_WINDOW_MS) homeUserPinAttempts.delete(ip);
    }
  }, 30 * 60 * 1000).unref();

  function checkHomeUserPinRateLimit(ip) {
    const now = Date.now();
    const entry = homeUserPinAttempts.get(ip);
    if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW_MS) return null;
    if (entry.failures >= HOME_USER_PIN_RATE_MAX) {
      return Math.max(1, Math.ceil((RATE_LIMIT_WINDOW_MS - (now - entry.windowStart)) / 60000));
    }
    return null;
  }

  function recordHomeUserPinFailure(ip) {
    const now = Date.now();
    const entry = homeUserPinAttempts.get(ip);
    if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW_MS) {
      homeUserPinAttempts.set(ip, { failures: 1, windowStart: now });
    } else {
      entry.failures += 1;
    }
  }

  function validatePendingHomeSession(req) {
    const mainToken = String(req.session?.plexMainToken || '').trim();
    const homeUsers = req.session?.pendingHomeUsers;
    const issuedAt = Number(req.session?.pendingHomeUsersAt || 0);
    if (!mainToken || !Array.isArray(homeUsers) || !homeUsers.length) return null;
    if (issuedAt && Date.now() - issuedAt > HOME_USER_PENDING_MAX_AGE_MS) return null;
    return { mainToken, homeUsers };
  }

  app.get('/auth/plex/home-users', (req, res) => {
    const pending = validatePendingHomeSession(req);
    if (!pending) {
      req.session = null;
      return res.redirect('/login');
    }
    return res.render('plex-home-users', {
      title: "Who's watching?",
      homeUsers: pending.homeUsers,
    });
  });

  app.post('/auth/plex/home-users/select', async (req, res) => {
    try {
      const pending = validatePendingHomeSession(req);
      if (!pending) {
        req.session = null;
        return res.redirect('/login');
      }
      const { mainToken, homeUsers } = pending;

      const userId = String(req.body?.userId || '').trim();
      const selectedUser = userId ? homeUsers.find((u) => String(u.uuid || u.id || '') === userId) : null;
      if (!selectedUser) return res.redirect('/auth/plex/home-users');

      if (selectedUser.protected && !selectedUser.admin) {
        req.session.pendingHomeUserId = userId;
        return res.redirect('/auth/plex/home-users/pin');
      }

      // Admin (home owner) already authenticated — use main token directly.
      // Non-admin without PIN: switch using uuid.
      const homeUserToken = selectedUser.admin
        ? mainToken
        : await switchPlexHomeUser(mainToken, userId, null);
      const authMode = String(req.session?.plexAuthMode || 'redirect').trim().toLowerCase();
      if (req.session) {
        delete req.session.pendingHomeUsers;
        delete req.session.pendingHomeUserId;
        delete req.session.pendingHomeUsersAt;
        delete req.session.plexAuthMode;
      }
      await completePlexLogin(req, homeUserToken);
      req.session.plexMainToken = mainToken;

      const redirectTarget = consumePostLoginRedirect(req, '/dashboard');
      if (authMode === 'popup') return res.render('plex-auth-complete', { title: 'Plex Login Complete', redirectTarget });
      return res.redirect(redirectTarget);
    } catch (err) {
      pushLog({ level: 'error', app: 'plex', action: 'login.homeuser.select', message: safeMessage(err) || 'Home user selection failed.' });
      return res.status(err?.status || 500).send(`Login failed: ${safeMessage(err)}`);
    }
  });

  app.get('/auth/plex/home-users/pin', (req, res) => {
    const pending = validatePendingHomeSession(req);
    if (!pending) {
      req.session = null;
      return res.redirect('/login');
    }
    const pendingHomeUserId = String(req.session?.pendingHomeUserId || '').trim();
    const selectedUser = pendingHomeUserId
      ? pending.homeUsers.find((u) => String(u.uuid || u.id || '') === pendingHomeUserId)
      : null;
    if (!selectedUser) return res.redirect('/auth/plex/home-users');

    return res.render('plex-home-pin', {
      title: 'Enter PIN',
      user: { id: pendingHomeUserId, title: selectedUser.title, thumb: selectedUser.thumb || null },
      error: null,
    });
  });

  app.post('/auth/plex/home-users/pin', async (req, res) => {
    try {
      const ip = getClientIp(req);
      const blockedMinutes = checkHomeUserPinRateLimit(ip);
      if (blockedMinutes !== null) {
        return res.status(429).send(`Too many PIN attempts. Try again in ${blockedMinutes} minute${blockedMinutes === 1 ? '' : 's'}.`);
      }

      const pending = validatePendingHomeSession(req);
      if (!pending) {
        req.session = null;
        return res.redirect('/login');
      }
      const { mainToken, homeUsers } = pending;

      const pendingHomeUserId = String(req.session?.pendingHomeUserId || '').trim();
      const selectedUser = pendingHomeUserId
        ? homeUsers.find((u) => String(u.uuid || u.id || '') === pendingHomeUserId)
        : null;
      if (!selectedUser) return res.redirect('/auth/plex/home-users');

      const pin = String(req.body?.pin || '').trim();
      const userRenderData = { id: pendingHomeUserId, title: selectedUser.title, thumb: selectedUser.thumb || null };

      if (!pin) {
        return res.render('plex-home-pin', { title: 'Enter PIN', user: userRenderData, error: 'PIN is required.' });
      }

      let homeUserToken;
      try {
        homeUserToken = await switchPlexHomeUser(mainToken, pendingHomeUserId, pin);
      } catch (err) {
        if (err?.status === 401) {
          recordHomeUserPinFailure(ip);
          const nowBlocked = checkHomeUserPinRateLimit(ip);
          const suffix = nowBlocked !== null
            ? ` Too many failed attempts — try again in ${nowBlocked} minute${nowBlocked === 1 ? '' : 's'}.`
            : '';
          return res.status(401).render('plex-home-pin', {
            title: 'Enter PIN',
            user: userRenderData,
            error: `Incorrect PIN.${suffix}`,
          });
        }
        throw err;
      }

      const authMode = String(req.session?.plexAuthMode || 'redirect').trim().toLowerCase();
      if (req.session) {
        delete req.session.pendingHomeUsers;
        delete req.session.pendingHomeUserId;
        delete req.session.pendingHomeUsersAt;
        delete req.session.plexAuthMode;
      }
      await completePlexLogin(req, homeUserToken);
      req.session.plexMainToken = mainToken;

      const redirectTarget = consumePostLoginRedirect(req, '/dashboard');
      if (authMode === 'popup') return res.render('plex-auth-complete', { title: 'Plex Login Complete', redirectTarget });
      return res.redirect(redirectTarget);
    } catch (err) {
      pushLog({ level: 'error', app: 'plex', action: 'login.homeuser.pin', message: safeMessage(err) || 'Home user PIN login failed.' });
      return res.status(err?.status || 500).send(`Login failed: ${safeMessage(err)}`);
    }
  });

  // ─── Jellyfin / Emby auth ────────────────────────────────────────────────────
  //
  // Unlike Plex, Jellyfin and Emby use a direct username+password form rather than
  // an OAuth popup. The password is validated against the media server API and is
  // never stored — only the returned API key is kept in config (already done in wizard).
  // Here we just authenticate the user for this session.

  async function handleMediaServerLogin(req, res, serverType) {
    const serverLabel = serverType === 'jellyfin' ? 'Jellyfin' : 'Emby';
    const ip = getClientIp(req);
    const blockedMinutes = checkLoginRateLimit(ip);
    if (blockedMinutes !== null) {
      return res.status(429).render(`${serverType}-login`, {
        title: `${serverLabel} Sign In`,
        error: `Too many failed login attempts. Try again in ${blockedMinutes} minute${blockedMinutes === 1 ? '' : 's'}.`,
      });
    }

    const config = loadConfig();
    const serverCfg = config[serverType] || {};
    const { url: serverUrl, apiKey } = serverCfg;
    if (!serverUrl || !apiKey) {
      return res.status(400).render(`${serverType}-login`, {
        title: `${serverLabel} Sign In`,
        error: `${serverLabel} is not configured. Contact your administrator.`,
      });
    }

    const username = String(req.body?.username || '').trim();
    const password = String(req.body?.password || '');
    if (!username || !password) {
      return res.status(400).render(`${serverType}-login`, {
        title: `${serverLabel} Sign In`,
        error: 'Username and password are required.',
      });
    }

    let authResult;
    try {
      const { getAdapter } = await import('../services/media-servers/index.js');
      const adapter = getAdapter(serverType);
      authResult = await adapter.authenticate(serverUrl, username, password);
    } catch (err) {
      recordLoginFailure(ip);
      const nowBlocked = checkLoginRateLimit(ip);
      const suffix = nowBlocked !== null ? ` Too many failed attempts — try again in ${nowBlocked} minute${nowBlocked === 1 ? '' : 's'}.` : '';
      return res.status(401).render(`${serverType}-login`, {
        title: `${serverLabel} Sign In`,
        error: `Invalid username or password.${suffix}`,
      });
    }

    clearLoginFailures(ip);

    const { userId } = authResult;
    req.session.user = {
      username,
      email: '',
      avatar: '',
      avatarFallback: '/icons/user-profile.svg',
      role: 'member',
      source: serverType,
      serverId: userId,
    };
    req.session.viewRole = null;
    req.session.previewUserId = null;

    // Route first-time users to the personal wizard
    try {
      const { getUserPreferences } = await import('../db.js');
      const prefs = getUserPreferences(db, username);
      if (!prefs.userWizardCompleted) req.session.postLoginRedirect = '/wizard/user';
    } catch (_err) { /* non-fatal */ }

    const loginConfig = updateUserLogins(config, { identifier: username, curatorr: true });
    if (loginConfig !== config) saveConfig(loginConfig);

    pushLog({ level: 'info', app: serverType, action: 'login.success', message: `${serverLabel} login successful.`, meta: { user: username } });
    return res.redirect(consumePostLoginRedirect(req, '/dashboard'));
  }

  app.get('/auth/jellyfin', (req, res) => {
    if (req.session?.user) return res.redirect(consumePostLoginRedirect(req, '/dashboard'));
    const config = loadConfig();
    if (String(config?.mediaServer?.type || 'plex') !== 'jellyfin') return res.redirect('/login');
    return res.render('jellyfin-login', { title: 'Jellyfin Sign In', error: null });
  });

  app.post('/auth/jellyfin', async (req, res) => {
    if (req.session?.user) return res.redirect(consumePostLoginRedirect(req, '/dashboard'));
    const config = loadConfig();
    if (String(config?.mediaServer?.type || 'plex') !== 'jellyfin') return res.redirect('/login');
    return handleMediaServerLogin(req, res, 'jellyfin');
  });

  app.get('/auth/emby', (req, res) => {
    if (req.session?.user) return res.redirect(consumePostLoginRedirect(req, '/dashboard'));
    const config = loadConfig();
    if (String(config?.mediaServer?.type || 'plex') !== 'emby') return res.redirect('/login');
    return res.render('emby-login', { title: 'Emby Sign In', error: null });
  });

  app.post('/auth/emby', async (req, res) => {
    if (req.session?.user) return res.redirect(consumePostLoginRedirect(req, '/dashboard'));
    const config = loadConfig();
    if (String(config?.mediaServer?.type || 'plex') !== 'emby') return res.redirect('/login');
    return handleMediaServerLogin(req, res, 'emby');
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
