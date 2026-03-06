# Sweeparr (Local Module)

`sweeparr` is a local-only, optional cleanup worker intended as a lightweight alternative to Cleanuparr.

It uses Launcharr's existing app integrations and reads clients from Launcharr config (`config/config.json`) via the module runtime context.

## Current V1 Features

- Polls configured downloader clients: `transmission`, `qbittorrent`, `nzbget`, `sabnzbd`.
- Evaluates simple rules per queue item:
  - `slowSpeed`
  - `slowTime`
  - `stalled`
- Tracks strikes and emits actions (`warn`, `pause`, `remove`) when max strikes are hit.
- Persists module-only config/state/events under:
  - `local-modules/sweeparr/data/sweeparr-config.json`
  - `local-modules/sweeparr/data/sweeparr-state.json`
  - `local-modules/sweeparr/data/sweeparr-events.json`
- Exposes admin API endpoints:
  - `GET /sweeparr` (basic test UI)
  - `GET /api/sweeparr/health`
  - `GET /api/sweeparr/clients`
  - `GET /api/sweeparr/config`
  - `GET /api/sweeparr/aggression-levels`
  - `POST /api/sweeparr/config`
  - `GET /api/sweeparr/events?limit=100`
  - `GET /api/sweeparr/status`
  - `POST /api/sweeparr/run-once`

## Safety Defaults

- `dryRun` defaults to `true`.
- `runOnStartup` defaults to `true` but only evaluates and logs in dry-run mode.
- Action defaults are `warn` on all rules.

## Example Config Patch

```json
{
  "dryRun": true,
  "pollIntervalSeconds": 120,
  "rules": {
    "slowSpeed": {
      "enabled": true,
      "minBps": 131072,
      "graceMinutes": 15,
      "maxStrikes": 3,
      "cooldownMinutes": 30,
      "action": "warn"
    },
    "slowTime": {
      "enabled": false,
      "minEtaMinutes": 120,
      "graceMinutes": 20,
      "maxStrikes": 3,
      "cooldownMinutes": 30,
      "action": "warn"
    },
    "stalled": {
      "enabled": true,
      "noProgressMinutes": 30,
      "graceMinutes": 20,
      "maxStrikes": 3,
      "cooldownMinutes": 30,
      "action": "warn"
    }
  }
}
```

## Suggested Rollout

1. Keep `dryRun=true`.
2. Run `POST /api/sweeparr/run-once`.
3. Inspect `GET /api/sweeparr/events`.
4. Tune thresholds.
5. Change actions to `pause` per rule before considering `remove`.
6. Only disable dry-run after multiple clean runs.

## Run-Once Modes

`POST /api/sweeparr/run-once` supports optional mode overrides for one-off tests:

- `{"mode":"dry"}` forces dry-run for that run only.
- `{"mode":"live"}` forces live actions for that run only.
- `{"dryRun":true}` / `{"dryRun":false}` also supported.
- No body uses persisted module config (`config.dryRun`).

## Aggression Levels

Sweeparr supports 3 presets:

- `low`: conservative checks and slower triggers
- `medium`: balanced defaults
- `high`: faster checks and more aggressive pause behavior

Apply a preset by posting config with `aggressionLevel`:

```json
{
  "aggressionLevel": "high",
  "applyAggressionDefaults": true
}
```

You can then manually override any field using `POST /api/sweeparr/config`.

## Removal Reason Logging

When a download is flagged for `remove`, Sweeparr records:

- `action` event with `reason` text (rule + thresholds)
- `removal_flagged` event with the same reason and action result metadata

This makes it clear why the item was flagged (for example `stalled`, `slowSpeed`, `slowTime`).
