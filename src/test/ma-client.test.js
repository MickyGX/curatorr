import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  compareVersions,
  connectMusicAssistant,
  decodeTokenExpiry,
  normalizeMusicAssistantUrl,
} from '../services/music-assistant/client.js';

const SERVER_INFO = {
  server_id: 'abc', server_version: '2.10.4', schema_version: 65, min_supported_schema_version: 28, onboard_done: true,
};

// Minimal stand-in for the WHATWG WebSocket used by the client. `script` receives each
// parsed outbound command and returns the replies to deliver.
function fakeSocketFactory({ serverInfo = SERVER_INFO, script = () => [], failOpen = false } = {}) {
  const sockets = [];
  class FakeSocket {
    constructor(url) {
      this.url = url;
      this.listeners = {};
      this.sent = [];
      this.closed = false;
      sockets.push(this);
      queueMicrotask(() => {
        if (failOpen) { this.emit('error', {}); this.emit('close', {}); return; }
        this.emit('open', {});
        this.deliver(serverInfo);
      });
    }
    addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
    emit(type, evt) { (this.listeners[type] || []).forEach((fn) => fn(evt)); }
    deliver(msg) { this.emit('message', { data: JSON.stringify(msg) }); }
    send(raw) {
      const msg = JSON.parse(raw);
      this.sent.push(msg);
      for (const reply of script(msg, this)) queueMicrotask(() => this.deliver(reply));
    }
    close() { if (!this.closed) { this.closed = true; this.emit('close', {}); } }
  }
  return { FakeSocket, sockets };
}

const okAuth = (msg) => (msg.command === 'auth'
  ? [{ message_id: msg.message_id, result: { authenticated: true, user: { user_id: 'u1', username: 'michael', role: 'admin' } } }]
  : []);

describe('Music Assistant client', () => {
  it('normalises URLs and rejects non-http values', () => {
    assert.equal(normalizeMusicAssistantUrl('http://192.168.0.4:8095/'), 'http://192.168.0.4:8095');
    assert.equal(normalizeMusicAssistantUrl('http://192.168.0.4:8095/ws'), 'http://192.168.0.4:8095');
    assert.equal(normalizeMusicAssistantUrl('https://ma.example.com/sub/'), 'https://ma.example.com/sub');
    assert.equal(normalizeMusicAssistantUrl('ftp://x'), '');
    assert.equal(normalizeMusicAssistantUrl('not a url'), '');
  });

  it('compares versions and decodes JWT expiry', () => {
    assert.equal(compareVersions('2.10.4', '2.7.0'), 1);
    assert.equal(compareVersions('2.6.9', '2.7.0'), -1);
    assert.equal(compareVersions('2.7.0b3', '2.7.0'), 0);
    const payload = Buffer.from(JSON.stringify({ exp: 1822051267 })).toString('base64url');
    assert.equal(decodeTokenExpiry(`h.${payload}.s`), 1822051267000);
    assert.equal(decodeTokenExpiry('opaque'), null);
  });

  it('authenticates first, then assembles partial list results', async () => {
    const { FakeSocket, sockets } = fakeSocketFactory({
      script: (msg) => {
        if (msg.command === 'auth') return okAuth(msg);
        if (msg.command === 'music/tracks/library_items') {
          return [
            { message_id: msg.message_id, result: [1, 2], partial: true },
            { message_id: msg.message_id, result: [3] },
          ];
        }
        return [];
      },
    });
    const conn = connectMusicAssistant({ url: 'http://ma:8095', token: 't0k', WebSocketImpl: FakeSocket });
    const { serverInfo, user } = await conn.ready;
    assert.equal(sockets[0].url, 'ws://ma:8095/ws');
    assert.deepEqual(sockets[0].sent[0], { message_id: '1', command: 'auth', args: { token: 't0k' } });
    assert.equal(serverInfo.server_version, '2.10.4');
    assert.equal(user.username, 'michael');
    assert.deepEqual(await conn.send('music/tracks/library_items', { limit: 3 }), [1, 2, 3]);
    conn.close();
  });

  it('only delivers events after authentication', async () => {
    const events = [];
    const { FakeSocket, sockets } = fakeSocketFactory({ script: okAuth });
    const conn = connectMusicAssistant({ url: 'http://ma:8095', token: 't', WebSocketImpl: FakeSocket, onEvent: (e) => events.push(e.event) });
    sockets[0].deliver({ event: 'media_item_played', data: {} });
    await conn.ready;
    sockets[0].deliver({ event: 'media_item_played', data: {} });
    assert.deepEqual(events, ['media_item_played']);
    conn.close();
  });

  it('maps command errors to rejected promises', async () => {
    const { FakeSocket } = fakeSocketFactory({
      script: (msg) => (msg.command === 'auth' ? okAuth(msg) : [{ message_id: msg.message_id, error_code: 12, details: 'Invalid or unsupported command.' }]),
    });
    const conn = connectMusicAssistant({ url: 'http://ma:8095', token: 't', WebSocketImpl: FakeSocket });
    await conn.ready;
    await assert.rejects(conn.send('nope'), { code: 'command_failed', message: 'Invalid or unsupported command.' });
    conn.close();
  });

  it('rejects an invalid token as auth_failed', async () => {
    const { FakeSocket } = fakeSocketFactory({
      script: (msg) => [{ message_id: msg.message_id, error_code: 20, details: 'Invalid token' }],
    });
    const conn = connectMusicAssistant({ url: 'http://ma:8095', token: 'bad', WebSocketImpl: FakeSocket });
    await assert.rejects(conn.ready, { code: 'auth_failed' });
  });

  it('refuses servers older than 2.7 and servers that are not onboarded', async () => {
    const old = fakeSocketFactory({ serverInfo: { ...SERVER_INFO, server_version: '2.6.3' }, script: okAuth });
    await assert.rejects(connectMusicAssistant({ url: 'http://ma:8095', token: 't', WebSocketImpl: old.FakeSocket }).ready, { code: 'unsupported_version' });
    assert.equal(old.sockets[0].sent.length, 0, 'no token is sent to an unsupported server');
    const fresh = fakeSocketFactory({ serverInfo: { ...SERVER_INFO, onboard_done: false }, script: okAuth });
    await assert.rejects(connectMusicAssistant({ url: 'http://ma:8095', token: 't', WebSocketImpl: fresh.FakeSocket }).ready, { code: 'not_onboarded' });
  });

  it('reports an unreachable server and rejects pending commands on close', async () => {
    const down = fakeSocketFactory({ failOpen: true });
    await assert.rejects(connectMusicAssistant({ url: 'http://ma:8095', token: 't', WebSocketImpl: down.FakeSocket }).ready, { code: 'unreachable' });

    let closedWith = null;
    const up = fakeSocketFactory({ script: okAuth });
    const conn = connectMusicAssistant({ url: 'http://ma:8095', token: 't', WebSocketImpl: up.FakeSocket, onClose: (r) => { closedWith = r; } });
    await conn.ready;
    const pending = conn.send('players/all');
    up.sockets[0].close();
    await assert.rejects(pending, { code: 'closed' });
    assert.equal(closedWith?.code, 'closed');
  });
});
