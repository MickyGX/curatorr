import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';

import { promoteCompletedRequestsFromLidarr, resolveLibraryAlbumMatch } from '../services/album-reconciliation.js';
import { resolveManualPreviewAlbumStatus } from '../services/lidarr.js';

function createDb() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE master_tracks (
      rating_key TEXT NOT NULL DEFAULT '',
      artist_name TEXT NOT NULL DEFAULT '',
      track_title TEXT NOT NULL DEFAULT '',
      album_name TEXT NOT NULL DEFAULT ''
    );
  `);
  return db;
}

describe('resolveLibraryAlbumMatch', () => {
  it('returns exact for an exact album title match', () => {
    const db = createDb();
    db.prepare(`
      INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name)
      VALUES (?, ?, ?, ?)
    `).run('rk-1', 'Selena Gomez', 'Same Old Love', 'Revival');

    const result = resolveLibraryAlbumMatch(db, {
      artistName: 'Selena Gomez',
      albumTitle: 'Revival',
    });

    assert.equal(result.inLibrary, true);
    assert.equal(result.kind, 'exact');
    assert.equal(result.matchedAlbumTitle, 'Revival');
    db.close();
  });

  it('returns variant for deluxe-style album title differences', () => {
    const db = createDb();
    db.prepare(`
      INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name)
      VALUES (?, ?, ?, ?)
    `).run('rk-1', 'Selena Gomez', 'Same Old Love', 'Revival');

    const result = resolveLibraryAlbumMatch(db, {
      artistName: 'Selena Gomez',
      albumTitle: 'Revival (Deluxe)',
    });

    assert.equal(result.inLibrary, true);
    assert.equal(result.kind, 'variant');
    assert.equal(result.matchedAlbumTitle, 'Revival');
    db.close();
  });

  it('does not promote artist-only presence into an album match', () => {
    const db = createDb();
    db.prepare(`
      INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name)
      VALUES (?, ?, ?, ?)
    `).run('rk-1', 'Selena Gomez', 'Hands to Myself', 'Revival');

    const result = resolveLibraryAlbumMatch(db, {
      artistName: 'Selena Gomez',
      albumTitle: 'Stars Dance',
    });

    assert.equal(result.inLibrary, false);
    assert.equal(result.kind, 'artist_only');
    assert.equal(result.matchedAlbumTitle, '');
    db.close();
  });
});

describe('promoteCompletedRequestsFromLidarr', () => {
  it('promotes completed unresolved requests when Lidarr shows downloaded files', async () => {
    const lidarrService = {
      isConfigured() { return true; },
      async getAlbum(albumId) {
        return {
          id: albumId,
          title: 'Revival',
          statistics: { trackFileCount: 14 },
        };
      },
    };

    const [result] = await promoteCompletedRequestsFromLidarr([
      {
        id: 53,
        status: 'completed',
        lidarrAlbumId: 65151,
        inLibrary: false,
      },
    ], lidarrService);

    assert.equal(result.inLibrary, true);
    assert.equal(result.inLibraryKind, 'lidarr_downloaded');
    assert.equal(result.matchedAlbumTitle, 'Revival');
  });

  it('repairs completed requests when Lidarr shows the album is no longer monitored', async () => {
    const lidarrService = {
      isConfigured() { return true; },
      async getAlbum(albumId) {
        return {
          id: albumId,
          title: 'Power to the People',
          monitored: false,
          statistics: { trackFileCount: 0 },
        };
      },
      async setAlbumMonitoredAndVerify(albumId) {
        return {
          id: albumId,
          title: 'Power to the People',
          monitored: true,
          statistics: { trackFileCount: 0 },
        };
      },
    };

    const [result] = await promoteCompletedRequestsFromLidarr([
      {
        id: 54,
        status: 'completed',
        lidarrAlbumId: 5150,
        inLibrary: false,
      },
    ], lidarrService);

    assert.equal(result.inLibrary, false);
    assert.equal(result.lidarrMonitored, true);
    assert.equal(result.monitoringLost, false);
    assert.equal(result.monitoringRepairFailed, false);
  });
});

describe('resolveManualPreviewAlbumStatus', () => {
  it('does not treat unmonitored Lidarr catalog metadata as added', () => {
    const status = resolveManualPreviewAlbumStatus({
      id: 5150,
      title: 'Power to the People',
      monitored: false,
      statistics: { trackFileCount: 0 },
    });

    assert.equal(status, 'missing');
  });

  it('treats monitored albums without files as pending', () => {
    const status = resolveManualPreviewAlbumStatus({
      id: 5150,
      title: 'Power to the People',
      monitored: true,
      statistics: { trackFileCount: 0 },
    });

    assert.equal(status, 'pending');
  });
});
