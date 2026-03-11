# Integrations

---

## Tautulli

Tautulli is the primary source of playback data for Curatorr. Without it, plays and skips are not recorded and smart playlists will not function.

### Setup

**In Curatorr:**

1. Go to **Settings → Tautulli**
2. Enter your Tautulli local and/or remote URL
3. Enter your Tautulli API key (found in Tautulli → Settings → Web Interface)
4. Save

That's it. If you set up Tautulli during the **setup wizard**, the webhook was registered in Tautulli automatically. Curatorr uses the Tautulli API to create a webhook notification agent pointing back to itself — it will not create a duplicate if one already exists.

### Manual webhook setup (fallback)

If auto-registration did not run (e.g. you skipped the wizard Tautulli step, or the registration silently failed), you can add the webhook in Tautulli manually:

1. In Tautulli go to **Settings → Notification Agents**
2. Click **Add a new notification agent** and choose **Webhook**
3. Set the **Webhook URL** to:
   ```
   http://your-curatorr-url:7676/webhook/tautulli
   ```
   If Curatorr is behind a reverse proxy, use your public URL instead.
4. Set the **Method** to `POST`
5. Under **Triggers**, enable: **Playback Start**, **Playback Stop**, **Playback Pause**, **Playback Resume**, **Watched**
6. Save the notification agent

The Tautulli settings tab in Curatorr shows the exact webhook URL to use for your installation.

### What Tautulli provides

- **Real-time webhook events** — Curatorr receives play start/stop/pause/resume events and records them immediately
- **Daily history backfill** — once a day, Curatorr fetches recent Tautulli history to catch any plays missed by the webhook (e.g. if Curatorr was offline)

### User mapping

Tautulli sends a Plex user ID with each event. Curatorr uses this to match events to the correct local user. If a user's Plex ID does not match an existing Curatorr account, the event is still recorded against that Plex ID and will associate correctly once the user signs in.

---

## Lidarr

Lidarr integration is optional. When connected, Curatorr can automatically add suggested artists to your Lidarr library with progressive album discovery.

### Setup

**In Lidarr:**

1. Go to **Settings → General**
2. Copy your **API key**

**In Curatorr:**

1. Go to **Settings → Lidarr** (admin only)
2. Enter your Lidarr local and/or remote URL
3. Paste your Lidarr API key
4. Enable **Lidarr automation**
5. Set **Automation scope** to **Role based** to enforce per-role weekly quotas
6. Save

### How automation works

When a user clicks **Add to Lidarr** on a suggested artist:

1. Curatorr adds the artist to Lidarr
2. A starter album is selected based on the user's taste profile and monitored in Lidarr
3. If **Auto trigger manual search** is enabled, Curatorr immediately queues a Lidarr `AlbumSearch` for that album
4. If the search returns no files, Curatorr can re-monitor, retry, and eventually fall back to grabbing the best available release based on peer count and score
5. As the user engages with the artist over time, Curatorr progressively unlocks additional albums

Progress and status for all acted-upon artists appears in the **Lidarr Activity** panel on the Artists page. See [Artist Suggestions and Lidarr Activity](Artist-Suggestions-and-Lidarr-Activity) for a full explanation of status labels.

### Weekly quotas

Quotas limit how many artists and albums each role can add per week. They reset on a rolling 7-day window.

Default quotas:

| Role | Artists per week | Albums per week |
|---|---|---|
| Admin | Unlimited | Unlimited |
| Co-admin | 3 | 6 |
| Power user | 1 | 2 |
| User | 0 | 0 |

Quotas are configurable in **Settings → Lidarr → Automation**. Set to `-1` for unlimited. The current quota usage for the signed-in user is shown as chips in the **Suggested Artists** panel header.

---

## Plex

Plex is required. Curatorr uses Plex for:

- **SSO authentication** — users sign in with their Plex account
- **Library indexing** — Curatorr reads your music library tracks, albums, genres, and metadata
- **Smart playlist sync** — Curatorr creates and updates playlists directly in Plex

### Plex Token

You need a Plex token to allow Curatorr to access your server. Use the **Get Plex Token** helper button in Settings → Plex, or retrieve it manually from a Plex web request header.

### Machine ID

The machine identifier uniquely identifies your Plex Media Server. Use the **Get Plex Machine** helper button to auto-detect it, or find it in Plex → Settings → Troubleshooting → Your Server Machine Identifier.

### Music Libraries

After a successful Plex connection, Curatorr will display your available music libraries. Select the ones you want it to monitor.
