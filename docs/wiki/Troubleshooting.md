# Troubleshooting

---

## Plays are not being recorded

**Check that the Tautulli webhook is configured correctly.**

1. In Tautulli, go to **Settings → Notification Agents** and confirm the webhook URL is set to `http://your-curatorr-url:7676/webhook/tautulli`
2. Confirm the triggers include Playback Start, Playback Stop, Playback Pause, Playback Resume, and Watched
3. In Curatorr, go to **Settings → Logs** and look for `webhook.tautulli` entries — if none appear when you play a track, the webhook is not reaching Curatorr
4. Check that `BASE_URL` in your compose file matches the URL Curatorr is actually served from — this affects how the webhook URL hint is displayed in settings, but the webhook itself just needs to be reachable at `/webhook/tautulli`

**If Curatorr is behind a reverse proxy**, ensure the proxy is forwarding requests correctly. Set `TRUST_PROXY=true` and `TRUST_PROXY_HOPS=1` in your compose environment.

---

## Smart playlist is not appearing in Plex

1. Confirm the Plex token and machine ID are correctly set in **Settings → Plex**
2. Check that the relevant music library is selected in **Settings → Plex → Music Libraries**
3. In **Settings → Jobs**, check the last run status of the **Smart Playlist Sync** job — if it shows an error, the error message will indicate the cause
4. Manually trigger the **Smart Playlist Sync** job and check the Logs tab for any errors
5. Confirm the Plex token has write access to the Plex server (it needs to belong to the account that owns the server)

---

## Artist suggestions are empty

Suggestions require enough play history to build a meaningful taste profile. If you have just set up Curatorr:

- Play some tracks and let the Tautulli webhook record them
- Wait for the next Smart Playlist Sync to run, which also rebuilds the suggestion cache
- Check **Settings → Logs** for any errors from the recommendation engine

If you have significant history and suggestions are still empty, check that none of your artists are all excluded. Tracks marked Skip and artists manually excluded are filtered out.

---

## Lidarr automation is not available

The **Add to Lidarr** button will not appear if:

- Lidarr is not connected — check **Settings → Lidarr** and confirm the connection is working
- Lidarr automation is not enabled — go to **Settings → Lidarr → Automation** and enable it
- Your role is not eligible — Users and Guests have no automation access by default; Power Users require the scope to be set to **Role based**; check with your admin

---

## Lidarr search is not running after adding an artist

If the status shows **Starter album added** but not **Search queued**:

- Check that **Auto trigger manual search** is enabled in **Settings → Lidarr → Automation**
- The **Lidarr: Process Queued Requests** job runs every 20 minutes; check its last run status in Settings → Jobs
- Manually trigger the job and check the Logs tab for errors

---

## Session expires immediately or login loop

- Ensure `SESSION_SECRET` is set to a non-empty, consistent value in your compose file. If this variable changes, all existing sessions are invalidated.
- If behind a reverse proxy, confirm `TRUST_PROXY=true` is set and the proxy is forwarding the correct headers (`X-Forwarded-For`, `X-Forwarded-Proto`)
- If serving over HTTPS, set `COOKIE_SECURE=true` to prevent cookie issues

---

## Plex SSO is not working

- Confirm the **Local URL** in Settings → Plex is reachable from within your Docker network
- Confirm the **Remote URL** in Settings → Plex matches your externally accessible Plex URL
- Check that the Plex token is valid — use the **Get Plex Token** helper to retrieve a fresh one
- Check Logs for any `auth.plex` errors

---

## Logs

The live log stream is available at **Settings → Logs**. Log entries include an `app` field indicating the component (e.g. `jobs`, `webhook.tautulli`, `lidarr`, `auth.plex`) and an `action` field for the specific event. Filter by these to narrow down issues.
