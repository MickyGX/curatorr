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

Curatorr now supports two shipped analysis modes:

1. `Analyzer sidecar`:
   Curatorr posts the manifest and output paths to a separate `curatorr_analyzer` service running beside the main app.
2. `Custom command`:
   Curatorr exports a manifest, runs your preferred analyzer, then imports the results.

## Sidecar analysis mode

This is the recommended long-term deployment model because it keeps Python and audio-analysis dependencies out of the main Curatorr container.

Curatorr includes an optional same-repo sidecar image.

Full Docker Compose example:

```yaml
services:
  curatorr:
    image: mickygx/curatorr:latest
    container_name: curatorr
    ports:
      - "7676:7676"
    environment:
      - CONFIG_PATH=/app/config/config.json
      - DATA_DIR=/app/data
      - BASE_URL=http://localhost:7676
      - TRUST_PROXY=true
      - TRUST_PROXY_HOPS=1
      - SESSION_SECRET=replace-this-with-a-random-secret
      - WEBHOOK_SECRET=replace-this-with-a-random-secret
    volumes:
      - ./config:/app/config
      - ./data:/app/data
      - ./data/icons/custom:/app/public/icons/custom
    network_mode: bridge
    restart: unless-stopped

  curatorr_analyzer:
    image: mickygx/curatorr-analyzer:latest
    container_name: curatorr_analyzer
    depends_on:
      - curatorr
    environment:
      - PORT=8765
    volumes:
      - ./data:/app/data
      # Mount your music library at the same absolute path Plex reports in track file paths.
      # Example:
      # - /path/to/music:/media/music:ro
    network_mode: "service:curatorr"
    restart: unless-stopped
```

Repository example:

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
3. set `Feature manifest path` to `/app/data/track-features.json`
4. set `Analyzer results path` to `/app/data/track-features.results.json`
5. leave `Analyzer command` and `Analyzer working directory` unused in sidecar mode
6. enable or run `Track Analysis Pipeline`

The sidecar pipeline:

1. exports only tracks still missing feature data
2. processes them in chunks
3. writes intermediate output into `/app/data`
4. imports results chunk by chunk into `track_enrichment`
5. shows chunk progress in `Settings -> Jobs`

If a run is interrupted, the next run starts again from chunk `1` of the remaining missing-track set rather than from the entire library.

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

Feature-dependent filters only match tracks that actually have the required data. Missing values are ignored rather than being treated as zero.

Curatorr also uses this data in visual playlist presets for both personal and global playlist builders:

- `Club`
- `Driving`
- `Workout`
- `Chill`
- `Harmonic`

Those presets can prefill BPM, energy, danceability, and Camelot controls, then be tweaked before saving.

`Camelot key` uses DJ wheel notation:

- `8A` = `A minor`
- `8B` = `C major`

`Camelot focus` accepts one or more focus keys like `8A` or `8A, 9A, 10A`.

## Notes

- `energy` and `danceability` are expected on a `0` to `1` scale.
- Curatorr keeps year/date enrichment and feature enrichment in the same `track_enrichment` store.
- If a track already has year metadata from MusicBrainz, feature imports add BPM/key-style data without overwriting the year fields.
