// Music Assistant WebSocket client.
//
// Protocol (MA >= 2.7): connect to <url>/ws, the server sends a ServerInfoMessage,
// the client must send an `auth` command first, then events flow to the socket
// with no subscription step. Commands are {message_id, command, args}; results
// arrive as {message_id, result, partial?} (list results may be chunked with
// partial: true), errors as {message_id, error_code, details}, and events as
// {event, object_id, data}.

export const MIN_MA_SERVER_VERSION = '2.7.0';
const DEFAULT_CONNECT_TIMEOUT_MS = 15_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

export class MusicAssistantError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'MusicAssistantError';
    this.code = code;
  }
}

// Accepts http(s)://host:port with optional trailing slash or /ws; returns the
// canonical http(s) base URL, or '' when the value isn't a usable URL.
export function normalizeMusicAssistantUrl(raw) {
  const trimmed = String(raw || '').trim().replace(/\/+$/, '').replace(/\/ws$/i, '');
  if (!trimmed) return '';
  let parsed;
  try { parsed = new URL(trimmed); } catch { return ''; }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return '';
  return `${parsed.protocol}//${parsed.host}${parsed.pathname.replace(/\/+$/, '')}`;
}

export function compareVersions(left, right) {
  const parse = (value) => String(value || '').split(/[.\-+]/).slice(0, 3).map((part) => Number.parseInt(part, 10) || 0);
  const a = parse(left);
  const b = parse(right);
  for (let i = 0; i < 3; i += 1) {
    if ((a[i] || 0) !== (b[i] || 0)) return (a[i] || 0) < (b[i] || 0) ? -1 : 1;
  }
  return 0;
}

// Long-lived MA tokens are JWTs; the expiry is only available from the payload.
export function decodeTokenExpiry(token) {
  const payload = String(token || '').split('.')[1];
  if (!payload) return null;
  try {
    const json = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
    return Number(json?.exp) > 0 ? Number(json.exp) * 1000 : null;
  } catch {
    return null;
  }
}

function isServerInfoMessage(msg) {
  return Boolean(msg && typeof msg === 'object' && msg.server_id && msg.server_version && msg.message_id === undefined);
}

// Opens one authenticated connection. `ready` resolves with {serverInfo, user}
// or rejects with a MusicAssistantError whose code is one of:
// unreachable | timeout | unsupported_version | not_onboarded | auth_failed | closed.
export function connectMusicAssistant({
  url,
  token,
  WebSocketImpl = globalThis.WebSocket,
  connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS,
  requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
  onEvent = () => {},
  onClose = () => {},
}) {
  const baseUrl = normalizeMusicAssistantUrl(url);
  if (!baseUrl) {
    return {
      ready: Promise.reject(new MusicAssistantError('unreachable', 'Music Assistant URL is not a valid http(s) URL.')),
      send: () => Promise.reject(new MusicAssistantError('closed', 'Not connected.')),
      close: () => {},
    };
  }

  const pending = new Map();
  let nextMessageId = 1;
  let closed = false;
  let authenticated = false;
  let settleReady;
  const ready = new Promise((resolve, reject) => { settleReady = { resolve, reject }; });
  ready.catch(() => {});

  const ws = new WebSocketImpl(`${baseUrl.replace(/^http/i, 'ws')}/ws`);

  const connectTimer = setTimeout(() => {
    fail(new MusicAssistantError('timeout', 'Timed out connecting to Music Assistant.'));
  }, connectTimeoutMs);
  connectTimer.unref?.();

  function rejectPending(err) {
    for (const entry of pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(err);
    }
    pending.clear();
  }

  function fail(err) {
    if (settleReady) {
      settleReady.reject(err);
      settleReady = null;
    }
    close(err);
  }

  function close(reason = null) {
    if (closed) return;
    closed = true;
    clearTimeout(connectTimer);
    rejectPending(reason instanceof MusicAssistantError ? reason : new MusicAssistantError('closed', 'Connection closed.'));
    try { ws.close(); } catch { /* already closed */ }
    onClose(reason);
  }

  function sendRaw(command, args, { timeoutMs = requestTimeoutMs } = {}) {
    if (closed) return Promise.reject(new MusicAssistantError('closed', 'Not connected.'));
    const messageId = String(nextMessageId);
    nextMessageId += 1;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        pending.delete(messageId);
        reject(new MusicAssistantError('timeout', `Music Assistant command "${command}" timed out.`));
      }, timeoutMs);
      timer.unref?.();
      pending.set(messageId, { resolve, reject, timer, chunks: [] });
      try {
        ws.send(JSON.stringify({ message_id: messageId, command, args: args || {} }));
      } catch (err) {
        clearTimeout(timer);
        pending.delete(messageId);
        reject(new MusicAssistantError('closed', err?.message || 'Send failed.'));
      }
    });
  }

  async function authenticate(serverInfo) {
    if (compareVersions(serverInfo.server_version, MIN_MA_SERVER_VERSION) < 0) {
      throw new MusicAssistantError('unsupported_version', `Music Assistant ${MIN_MA_SERVER_VERSION} or newer is required (found ${serverInfo.server_version}).`);
    }
    if (serverInfo.onboard_done === false) {
      throw new MusicAssistantError('not_onboarded', 'Finish Music Assistant setup first (onboarding is not complete).');
    }
    let result;
    try {
      result = await sendRaw('auth', { token });
    } catch (err) {
      if (err instanceof MusicAssistantError && err.code === 'command_failed') {
        throw new MusicAssistantError('auth_failed', `Music Assistant rejected the token: ${err.message}`);
      }
      throw err;
    }
    if (!result?.authenticated) throw new MusicAssistantError('auth_failed', 'Music Assistant rejected the token.');
    authenticated = true;
    return result.user || null;
  }

  ws.addEventListener('message', (evt) => {
    let msg;
    try { msg = JSON.parse(String(evt.data)); } catch { return; }
    if (!msg || typeof msg !== 'object') return;

    if (isServerInfoMessage(msg)) {
      if (!settleReady) return;
      authenticate(msg).then((user) => {
        clearTimeout(connectTimer);
        if (settleReady) {
          settleReady.resolve({ serverInfo: msg, user });
          settleReady = null;
        }
      }, fail);
      return;
    }

    if (msg.event) {
      if (authenticated) {
        try { onEvent(msg); } catch { /* event handlers must not break the socket */ }
      }
      return;
    }

    const entry = pending.get(String(msg.message_id ?? ''));
    if (!entry) return;
    if (msg.error_code !== undefined) {
      pending.delete(String(msg.message_id));
      clearTimeout(entry.timer);
      entry.reject(new MusicAssistantError('command_failed', String(msg.details || `Error ${msg.error_code}`)));
      return;
    }
    if (Array.isArray(msg.result)) entry.chunks.push(...msg.result);
    if (msg.partial) return;
    pending.delete(String(msg.message_id));
    clearTimeout(entry.timer);
    entry.resolve(Array.isArray(msg.result) ? entry.chunks : msg.result);
  });

  ws.addEventListener('error', () => {
    if (settleReady) fail(new MusicAssistantError('unreachable', `Could not connect to Music Assistant at ${baseUrl}.`));
  });

  ws.addEventListener('close', () => {
    if (settleReady) fail(new MusicAssistantError('unreachable', `Music Assistant at ${baseUrl} closed the connection.`));
    else close(new MusicAssistantError('closed', 'Connection closed.'));
  });

  return {
    ready,
    send: (command, args, options) => (authenticated ? sendRaw(command, args, options) : Promise.reject(new MusicAssistantError('closed', 'Not authenticated.'))),
    close: () => close(null),
  };
}
