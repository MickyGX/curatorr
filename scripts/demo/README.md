# Documentation demo

Run from the repository root after `npm ci`:

```sh
node scripts/demo/fetch-catalog.mjs
node scripts/demo/start.mjs
```

Open <http://127.0.0.1:7677/demo/sign-in>. `DEMO_PORT` optionally changes the port.

The first command caches public release metadata from MusicBrainz and album artwork from the Cover Art Archive in the ignored `tmp/demo-catalog/` directory. Each cached album records its source URLs. Downloads resume from existing entries. Artwork belongs to its respective rights holders; it is not Curatorr artwork.

The second command runs the current application with a fresh disposable SQLite database and config under the OS temporary directory. It binds to loopback only. It never reads production configuration, credentials, or listening history. Alex, Sam, and Jordan are fictional listeners; their history, playlists, preferences, analysis values, requests, and integration status are fixtures. Album and track names and covers are real catalogue data.

Artwork is served locally. Upstream HTTP requests are replaced by fixture responses with no network fallback. Startup jobs and Music Assistant connections are not started. The simulated Music Assistant connection test supplies demo listeners and a Plex provider. M3U previews use the real matching code: choose `tmp/demo-catalog/Demo Mix.m3u` to see eight library matches and four missing tracks. Other write requests are rejected; this is a screenshot environment, not an integration test or an audio player.

Capture through the browser, keeping browser chrome out of the image. Enable **Hide scrollbars** in User Profile; the demo also extends that preference to dialog and table scrollbars with a local capture stylesheet. Scrolling still works. Check that artwork and asynchronous content have loaded. Present the complete screenshot gallery for review before publishing. The normal application entry point does not import any demo code.

Stop with Ctrl+C. Every new launch creates fresh data and a new session key; revisit `/demo/sign-in` after restarting. The console prints the disposable data path if cleanup is needed later.
