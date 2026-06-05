import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  getAlbumGenresFromMaster,
  getAlbumMoodsFromMaster,
  getAlbumStylesFromMaster,
  getMasterTracks,
  initDb,
  previewGlobalPlaylist,
  refreshMasterTracks,
} from '../db.js';
import { getLibraryTracks } from '../services/media-servers/plex.js';

describe('Plex album metadata import', () => {
  it('hydrates album genres, styles, and moods onto tracks during library refresh', async () => {
    const originalFetch = global.fetch;
    global.fetch = async (input) => {
      const url = new URL(String(input));

      if (url.pathname === '/library/sections/1/all' && !url.searchParams.has('mood')) {
        return Response.json({
          MediaContainer: {
            totalSize: 2,
            Metadata: [
              {
                ratingKey: 'track-1',
                grandparentTitle: 'Artist 1',
                title: 'Track 1',
                parentTitle: 'Album 1',
                parentRatingKey: '100',
                addedAt: 1760000000,
                Genre: [{ tag: 'Indie Rock' }],
              },
              {
                ratingKey: 'track-2',
                grandparentTitle: 'Artist 2',
                title: 'Track 2',
                parentTitle: 'Album 2',
                parentRatingKey: '200',
                addedAt: 1760000100,
                Genre: [{ tag: 'Dream Pop' }],
              },
            ],
          },
        });
      }

      if (url.pathname === '/library/sections/1/mood') {
        return Response.json({ MediaContainer: { Directory: [] } });
      }

      if (url.pathname === '/library/metadata/100,200') {
        return Response.json({
          MediaContainer: {
            Metadata: [
              {
                ratingKey: '100',
                Genre: [{ tag: 'Rock' }],
                Style: [{ tag: 'Shoegaze' }],
                Mood: [{ tag: 'Dreamy' }],
              },
              {
                ratingKey: '200',
                Genre: [{ tag: 'Pop' }],
                Style: [{ tag: 'Synthpop' }],
                Mood: [{ tag: 'Bright' }],
              },
            ],
          },
        });
      }

      throw new Error(`Unexpected Plex fetch: ${url.toString()}`);
    };

    try {
      const tracks = await getLibraryTracks('http://plex.local', 'token-1', ['1']);
      assert.equal(tracks.length, 2);
      assert.deepEqual(
        tracks.map((track) => ({
          ratingKey: track.ratingKey,
          libraryAddedAt: track.libraryAddedAt,
          albumGenres: track.albumGenres,
          albumStyles: track.albumStyles,
          albumMoods: track.albumMoods,
        })),
        [
          {
            ratingKey: 'track-1',
            libraryAddedAt: 1760000000000,
            albumGenres: ['Rock'],
            albumStyles: ['Shoegaze'],
            albumMoods: ['Dreamy'],
          },
          {
            ratingKey: 'track-2',
            libraryAddedAt: 1760000100000,
            albumGenres: ['Pop'],
            albumStyles: ['Synthpop'],
            albumMoods: ['Bright'],
          },
        ],
      );
    } finally {
      global.fetch = originalFetch;
    }
  });
});

describe('Album metadata persistence and filtering', () => {
  it('stores album metadata in master_tracks and exposes distinct values', () => {
    const dbPath = join(tmpdir(), `curatorr-album-metadata-${Date.now()}.db`);
    const db = initDb(dbPath);

    try {
      refreshMasterTracks(db, [
        {
          ratingKey: 'album-meta-1',
          artistName: 'Artist A',
          trackTitle: 'Track A',
          albumName: 'Album A',
          genres: ['Indie Rock'],
          moods: ['Melancholy'],
          albumGenres: ['Rock'],
          albumStyles: ['Shoegaze'],
          albumMoods: ['Dreamy'],
          libraryKey: '1',
          filePath: '/music/a.flac',
          durationMs: 180000,
          ratingCount: 10,
          viewCount: 0,
        },
        {
          ratingKey: 'album-meta-2',
          artistName: 'Artist B',
          trackTitle: 'Track B',
          albumName: 'Album B',
          genres: ['Britpop'],
          moods: ['Bright'],
          albumGenres: ['Rock', 'Pop'],
          albumStyles: ['Britpop'],
          albumMoods: ['Uplifting'],
          libraryKey: '1',
          filePath: '/music/b.flac',
          durationMs: 200000,
          ratingCount: 8,
          viewCount: 0,
        },
      ]);

      const masterTracks = getMasterTracks(db);
      assert.deepEqual(
        masterTracks.find((track) => track.ratingKey === 'album-meta-1')?.albumGenres,
        ['Rock'],
      );
      assert.deepEqual(
        masterTracks.find((track) => track.ratingKey === 'album-meta-2')?.albumStyles,
        ['Britpop'],
      );
      assert.deepEqual(getAlbumGenresFromMaster(db), ['Pop', 'Rock']);
      assert.deepEqual(getAlbumStylesFromMaster(db), ['Britpop', 'Shoegaze']);
      assert.deepEqual(getAlbumMoodsFromMaster(db), ['Dreamy', 'Uplifting']);
    } finally {
      db.close();
    }
  });

  it('applies album genre, style, and mood rules in playlist previews', () => {
    const dbPath = join(tmpdir(), `curatorr-album-preview-${Date.now()}.db`);
    const db = initDb(dbPath);

    try {
      refreshMasterTracks(db, [
        {
          ratingKey: 'preview-album-1',
          artistName: 'Artist A',
          trackTitle: 'Track A',
          albumName: 'Album A',
          genres: ['Indie Rock'],
          moods: ['Melancholy'],
          albumGenres: ['Rock'],
          albumStyles: ['Shoegaze'],
          albumMoods: ['Dreamy'],
          libraryKey: '1',
          filePath: '/music/preview-a.flac',
          durationMs: 180000,
          ratingCount: 10,
          viewCount: 0,
        },
        {
          ratingKey: 'preview-album-2',
          artistName: 'Artist B',
          trackTitle: 'Track B',
          albumName: 'Album B',
          genres: ['Britpop'],
          moods: ['Bright'],
          albumGenres: ['Rock'],
          albumStyles: ['Britpop'],
          albumMoods: ['Uplifting'],
          libraryKey: '1',
          filePath: '/music/preview-b.flac',
          durationMs: 200000,
          ratingCount: 8,
          viewCount: 0,
        },
        {
          ratingKey: 'preview-album-3',
          artistName: 'Artist C',
          trackTitle: 'Track C',
          albumName: 'Album C',
          genres: ['Jazz'],
          moods: ['Calm'],
          albumGenres: ['Jazz'],
          albumStyles: ['Modal Jazz'],
          albumMoods: ['Reflective'],
          libraryKey: '1',
          filePath: '/music/preview-c.flac',
          durationMs: 220000,
          ratingCount: 6,
          viewCount: 0,
        },
      ]);

      const preview = previewGlobalPlaylist(
        db,
        {
          albumGenres: { include: ['Rock'], exclude: [], includeMode: 'any' },
          albumStyles: { include: ['Britpop'], exclude: [], includeMode: 'any' },
          albumMoods: { include: [], exclude: ['Reflective'], includeMode: 'any' },
        },
        'album-preview-user',
        {},
      );

      assert.equal(preview.forUser?.eligibleTrackCount, 1);
      assert.equal(preview.forUser?.trackCount, 1);
    } finally {
      db.close();
    }
  });

  it('filters previews by library addition and release metadata', () => {
    const dbPath = join(tmpdir(), `curatorr-release-preview-${Date.now()}.db`);
    const db = initDb(dbPath);
    const now = Date.now();
    const currentYear = new Date(now).getUTCFullYear();
    const today = new Date(now).toISOString().slice(0, 10);

    try {
      refreshMasterTracks(db, [
        {
          ratingKey: 'recent-old-release',
          artistName: 'Elvis Presley',
          trackTitle: 'Historic Track',
          albumName: 'Historic Album',
          genres: ['Rock'],
          libraryKey: '1',
          filePath: '/music/elvis.flac',
          durationMs: 180000,
          libraryAddedAt: now - (2 * 24 * 60 * 60 * 1000),
          trackYear: 1956,
          originalReleaseDate: '1956-03-23',
          ratingCount: 30,
          viewCount: 0,
        },
        {
          ratingKey: 'recent-new-release-1',
          artistName: 'Modern Artist',
          trackTitle: 'Fresh Track A',
          albumName: 'Fresh Album',
          genres: ['Pop'],
          libraryKey: '1',
          filePath: '/music/fresh-a.flac',
          durationMs: 180000,
          libraryAddedAt: now - (3 * 24 * 60 * 60 * 1000),
          trackYear: currentYear,
          originalReleaseDate: today,
          ratingCount: 20,
          viewCount: 0,
        },
        {
          ratingKey: 'recent-new-release-2',
          artistName: 'Modern Artist',
          trackTitle: 'Fresh Track B',
          albumName: 'Fresh Album',
          genres: ['Pop'],
          libraryKey: '1',
          filePath: '/music/fresh-b.flac',
          durationMs: 180000,
          libraryAddedAt: now - (4 * 24 * 60 * 60 * 1000),
          trackYear: currentYear,
          originalReleaseDate: today,
          ratingCount: 10,
          viewCount: 0,
        },
        {
          ratingKey: 'old-add-new-release',
          artistName: 'Slow Import',
          trackTitle: 'Fresh But Old Add',
          albumName: 'Fresh But Old Add Album',
          genres: ['Pop'],
          libraryKey: '1',
          filePath: '/music/old-add.flac',
          durationMs: 180000,
          libraryAddedAt: now - (180 * 24 * 60 * 60 * 1000),
          trackYear: currentYear,
          originalReleaseDate: today,
          ratingCount: 40,
          viewCount: 0,
        },
      ]);

      const masterTracks = getMasterTracks(db);
      assert.equal(masterTracks.find((track) => track.ratingKey === 'recent-old-release')?.libraryAddedAt, now - (2 * 24 * 60 * 60 * 1000));

      const recentlyAdded = previewGlobalPlaylist(
        db,
        {
          libraryAddedMode: 'within',
          libraryAddedDays: 30,
          maxTracksPerAlbum: 1,
        },
        'release-preview-user',
        {},
      );
      assert.equal(recentlyAdded.forUser?.eligibleTrackCount, 3);
      assert.equal(recentlyAdded.forUser?.trackCount, 2);

      const recentlyReleased = previewGlobalPlaylist(
        db,
        {
          libraryAddedMode: 'within',
          libraryAddedDays: 30,
          releaseYearMin: currentYear,
        },
        'release-preview-user',
        {},
      );
      assert.equal(recentlyReleased.forUser?.eligibleTrackCount, 2);
      assert.equal(recentlyReleased.forUser?.trackCount, 2);

      const releasedWithin = previewGlobalPlaylist(
        db,
        {
          releaseDateMode: 'within',
          releaseDateDays: 30,
        },
        'release-preview-user',
        {},
      );
      assert.equal(releasedWithin.forUser?.eligibleTrackCount, 3);
    } finally {
      db.close();
    }
  });
});
