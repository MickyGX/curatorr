import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  buildTrackFeatureManifest,
  mergeAnalyzerResultsIntoManifest,
  parseAnalyzerFeatureInput,
  trackNeedsFeatureImport,
} from '../services/track-feature-manifest.js';

describe('track feature manifest', () => {
  it('detects when a track still needs imported features', () => {
    assert.equal(trackNeedsFeatureImport({
      ratingKey: '1',
      bpm: 122,
      musicalKey: 'G minor',
      camelotKey: '6A',
      energy: 0.72,
      danceability: 0.61,
    }), false);

    assert.equal(trackNeedsFeatureImport({
      ratingKey: '2',
      bpm: 122,
      musicalKey: '',
      camelotKey: '6A',
      energy: 0.72,
      danceability: 0.61,
    }), true);
  });

  it('builds a missing-only manifest by default', () => {
    const manifest = buildTrackFeatureManifest([
      {
        ratingKey: 'a',
        artistName: 'Artist A',
        trackTitle: 'Track A',
        albumName: 'Album A',
        recordingMbid: 'mbid-a',
        filePath: '/music/a.flac',
        bpm: 122,
        musicalKey: 'G minor',
        camelotKey: '6A',
        energy: 0.72,
        danceability: 0.61,
      },
      {
        ratingKey: 'b',
        artistName: 'Artist B',
        trackTitle: 'Track B',
        albumName: 'Album B',
        recordingMbid: 'mbid-b',
        filePath: '/music/b.flac',
        bpm: null,
        musicalKey: '',
        camelotKey: '',
        energy: null,
        danceability: null,
      },
    ], { generatedAt: '2026-03-28T00:00:00.000Z' });

    assert.equal(manifest.trackCount, 1);
    assert.equal(manifest.missingOnly, true);
    assert.equal(manifest.generatedAt, '2026-03-28T00:00:00.000Z');
    assert.deepEqual(manifest.tracks.map((track) => track.ratingKey), ['b']);
    assert.equal('bpm' in manifest.tracks[0], false);
  });

  it('can include existing feature values when requested', () => {
    const manifest = buildTrackFeatureManifest([
      {
        ratingKey: 'b',
        artistName: 'Artist B',
        trackTitle: 'Track B',
        albumName: 'Album B',
        recordingMbid: 'mbid-b',
        filePath: '/music/b.flac',
        bpm: 98,
        musicalKey: 'C major',
        camelotKey: '8B',
        energy: 0.31,
        danceability: 0.42,
      },
    ], { missingOnly: false, includeExisting: true });

    assert.equal(manifest.trackCount, 1);
    assert.equal(manifest.tracks[0].bpm, 98);
    assert.equal(manifest.tracks[0].camelotKey, '8B');
  });

  it('parses analyzer JSON rows with common aliases', () => {
    const rows = parseAnalyzerFeatureInput(JSON.stringify([
      {
        rating_key: '123',
        tempo: 124,
        key: 'A minor',
        camelot: '8A',
        energy: 0.7,
        danceability: 0.6,
      },
    ]), 'json');

    assert.equal(rows.length, 1);
    assert.equal(rows[0].ratingKey, '123');
    assert.equal(rows[0].bpm, 124);
    assert.equal(rows[0].musicalKey, 'A minor');
    assert.equal(rows[0].camelotKey, '8A');
  });

  it('parses analyzer CSV rows with common aliases', () => {
    const rows = parseAnalyzerFeatureInput([
      'recording_mbid,tempo,key,camelot_key,energy,danceability',
      'mbid-1,110,D major,10B,0.55,0.44',
    ].join('\n'), 'csv');

    assert.equal(rows.length, 1);
    assert.equal(rows[0].recordingMbid, 'mbid-1');
    assert.equal(rows[0].bpm, 110);
    assert.equal(rows[0].musicalKey, 'D major');
    assert.equal(rows[0].camelotKey, '10B');
  });

  it('merges analyzer results into an exported manifest', () => {
    const manifest = buildTrackFeatureManifest([
      {
        ratingKey: 'a',
        artistName: 'Artist A',
        trackTitle: 'Track A',
        albumName: 'Album A',
        recordingMbid: 'mbid-a',
        filePath: '/music/a.flac',
        bpm: null,
        musicalKey: '',
        camelotKey: '',
        energy: null,
        danceability: null,
      },
      {
        ratingKey: 'b',
        artistName: 'Artist B',
        trackTitle: 'Track B',
        albumName: 'Album B',
        recordingMbid: 'mbid-b',
        filePath: '/music/b.flac',
        bpm: null,
        musicalKey: '',
        camelotKey: '',
        energy: null,
        danceability: null,
      },
    ], { missingOnly: false });

    const merged = mergeAnalyzerResultsIntoManifest(manifest, [
      { recordingMbid: 'mbid-a', tempo: 128, key: 'F minor', camelot: '4A' },
      { filePath: '/music/b.flac', bpm: 98, musicalKey: 'C major', camelotKey: '8B', energy: 0.31 },
    ]);

    assert.equal(merged.mergeSummary.matched, 2);
    assert.equal(merged.tracks.find((track) => track.ratingKey === 'a')?.bpm, 128);
    assert.equal(merged.tracks.find((track) => track.ratingKey === 'a')?.musicalKey, 'F minor');
    assert.equal(merged.tracks.find((track) => track.ratingKey === 'b')?.camelotKey, '8B');
    assert.equal(merged.tracks.find((track) => track.ratingKey === 'b')?.energy, 0.31);
  });
});
