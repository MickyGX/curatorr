// Worker thread for services/playlist-builder-options.js. Opens its own read-only
// connection (the main db runs in WAL mode, so this never blocks writers) and
// posts the computed option lists back to the main thread.

import { parentPort, workerData } from 'node:worker_threads';
import Database from 'better-sqlite3';
import { computePlaylistBuilderOptions } from '../db.js';

const db = new Database(workerData.dbPath, { readonly: true, fileMustExist: true });
try {
  db.pragma('busy_timeout = 5000');
  parentPort.postMessage(computePlaylistBuilderOptions(db));
} finally {
  db.close();
}
