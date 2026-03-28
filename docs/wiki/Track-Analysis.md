# Track Analysis

Curatorr can import track-level audio features such as:

- `BPM`
- `musical key`
- `Camelot key`
- `energy`
- `danceability`

Long-term, Curatorr uses this split:

- Plex API and sonic analysis for ordering and loudness where available
- Curatorr-owned analysis for explicit `BPM`, key, Camelot, `energy`, and `danceability`

Curatorr now supports two analysis modes:

1. `Built-in Curatorr worker`:
   Curatorr runs its bundled Python analyzer script automatically from the `Track Analysis Pipeline` job.
2. `Analyzer sidecar`:
   Curatorr posts the manifest and output paths to a separate `curatorr_analyzer` service running beside the main app.
3. `Custom command`:
   Curatorr exports a manifest, runs your preferred analyzer, then imports the results.

The built-in worker currently uses `librosa` and `numpy` in the analysis environment. If those packages are not available, switch to custom mode or install them where Curatorr runs the analysis job.

## Sidecar analysis mode

This is the recommended long-term deployment model because it keeps Python and audio-analysis dependencies out of the main Curatorr container.

Curatorr includes an optional same-repo sidecar image:

```bash
docker compose --profile analysis up -d curatorr curatorr_analyzer
```

The sidecar:

- reuses Curatorr's bundled analyzer worker
- shares `/app/data` with the main app for manifests and results
- should mount your music library at the same absolute path Plex reports in track file paths
- can share the main container's network namespace so `http://127.0.0.1:8765` works from Curatorr

In `Settings -> General -> Track Analysis Import`:

1. set `Analyzer mode` to `Analyzer sidecar`
2. set `Analyzer sidecar URL` to `http://127.0.0.1:8765`
3. set `Feature manifest path` and `Analyzer results path` to files inside `/app/data`
4. enable or run `Track Analysis Pipeline`

## Built-in analysis mode

In `Settings -> General -> Track Analysis Import`:

1. set `Analyzer mode` to `Built-in Curatorr worker`
2. set `Feature manifest path`
3. set `Analyzer results path`
4. optionally override `Python executable`
5. enable or run the `Track Analysis Pipeline` job

Curatorr will:

1. export a manifest of tracks that still need features
2. run `scripts/analyze-track-features.py`
3. write the analyzer output to the configured results path
4. merge the results
5. import them into `track_enrichment`

Useful manual command:

```bash
npm run features:analyze -- --input /data/track-features.json --output /data/track-features.results.json
```

## Export a manifest template

Curatorr includes a helper script that exports a ready-to-fill JSON file from the local database:

```bash
npm run features:export-template
```

Default output:

```text
data/track-features.template.json
```

Useful options:

```bash
node scripts/export-track-feature-template.mjs --all
node scripts/export-track-feature-template.mjs --limit 500
node scripts/export-track-feature-template.mjs --out /data/track-features.json
node scripts/export-track-feature-template.mjs --include-existing
```

By default, the export only includes tracks that are still missing one or more imported feature fields.

## Manifest format

Curatorr accepts:

- an array of track objects
- an object with a top-level `tracks` array
- an object keyed by `ratingKey`

Each track can be matched by:

- `ratingKey`
- `recordingMbid`
- `filePath`

Example:

```json
{
  "tracks": [
    {
      "ratingKey": "12345",
      "recordingMbid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
      "filePath": "/music/Artist/Album/Track.flac",
      "bpm": 122,
      "musicalKey": "G minor",
      "camelotKey": "6A",
      "energy": 0.72,
      "danceability": 0.61
    }
  ]
}
```

## Configure Curatorr for custom analyzers

In `Settings -> General`:

1. set `Feature manifest path`
2. set `Analyzer mode` to `Custom command`
3. set `Analyzer command`
4. save settings
5. go to `Settings -> Jobs`
6. run `Track Analysis Pipeline` or `Track Feature Import`

If you only want to import a finished manifest without running an analyzer, you can still use the dedicated import job:

1. set `Feature manifest path`
2. save settings
3. go to `Settings -> Jobs`
4. run `Track Feature Import` manually, or enable it on a schedule

## Use in filters

Once imported, these fields are available in track exclusion filters:

- `BPM`
- `musical key`
- `Camelot key`
- `energy`
- `danceability`

Numeric fields support:

- `is`
- `is not`
- `is greater than`
- `is at least`
- `is less than`
- `is at most`
- `is between`

String fields such as `musical key` and `Camelot key` use the existing text operators.

## Notes

- `energy` and `danceability` are expected on a `0` to `1` scale.
- Curatorr keeps year/date enrichment and feature enrichment in the same `track_enrichment` store.
- If a track already has year metadata from MusicBrainz, feature imports add BPM/key-style data without overwriting the year fields.
- The built-in worker is Curatorr-owned, but it still depends on Python audio-analysis packages being available in the analysis environment.
