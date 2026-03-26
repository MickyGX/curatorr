// Plex media server adapter.
// Implements the shared media-server interface for track cache population
// and webhook auto-registration.

function buildUrl(baseUrl, suffixPath) {
  let normalized = String(baseUrl || '').trim().replace(/\/+$/, '');
  if (!normalized) return new URL('about:blank');
  if (!/^https?:\/\//i.test(normalized)) normalized = `http://${normalized}`;
  const suffix = String(suffixPath || '').replace(/^\/+/, '');
  return new URL(`${normalized}/${suffix}`);
}

function plexHeaders(token, extraHeaders = {}) {
  return {
    ...(token ? { 'X-Plex-Token': String(token).trim() } : {}),
    ...extraHeaders,
  };
}

function extractMusicbrainzUuid(value) {
  const match = String(value || '').match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
  return match ? match[0].toLowerCase() : '';
}

function extractPlexRecordingMbid(metadata = {}) {
  const guidEntries = Array.isArray(metadata?.Guid) ? metadata.Guid : (metadata?.Guid ? [metadata.Guid] : []);
  for (const entry of guidEntries) {
    const raw = String(entry?.id || entry || '').trim();
    if (!/musicbrainz|mbid/i.test(raw)) continue;
    const mbid = extractMusicbrainzUuid(raw);
    if (mbid) return mbid;
  }
  return '';
}

// ── Connection verification ───────────────────────────────────────────────────

export async function verifyConnection(url, token) {
  const u = buildUrl(url, '');
  const res = await fetch(u.toString(), {
    headers: plexHeaders(token, { Accept: 'application/json' }),
  });
  if (!res.ok) throw new Error(`Plex returned HTTP ${res.status}`);
  const json = await res.json();
  return {
    ok: true,
    serverName: String(json?.MediaContainer?.friendlyName || ''),
    serverId:   String(json?.MediaContainer?.machineIdentifier || ''),
  };
}

// ── Library enumeration ───────────────────────────────────────────────────────

export async function getLibraries(url, token) {
  const u = buildUrl(url, 'library/sections');
  const res = await fetch(u.toString(), {
    headers: plexHeaders(token, { Accept: 'application/json' }),
  });
  if (!res.ok) throw new Error(`Plex library fetch failed (${res.status})`);
  const json = await res.json();
  return (json?.MediaContainer?.Directory || [])
    .filter((d) => d.type === 'artist')
    .map((d) => ({
      key:   String(d.key || ''),
      title: String(d.title || ''),
    }));
}

// ── Track cache population ────────────────────────────────────────────────────
// Returns a flat array of normalised track objects ready for master_tracks upsert.
// Extracted from refreshMasterTrackCache in wizard.js — no logic changes.

export async function getLibraryTracks(url, token, libraryKeys) {
  const PAGE_SIZE = 10000;
  const tracks = [];

  for (const key of libraryKeys) {
    let start = 0;
    let totalSize = null;
    while (totalSize === null || start < totalSize) {
      const u = buildUrl(url, `library/sections/${key}/all`);
      u.searchParams.set('type', '10');
      u.searchParams.set('includeGuids', '1');
      u.searchParams.set('X-Plex-Container-Start', String(start));
      u.searchParams.set('X-Plex-Container-Size', String(PAGE_SIZE));
      const res = await fetch(u.toString(), {
        headers: plexHeaders(token, { Accept: 'application/json' }),
      });
      if (!res.ok) break;
      const json = await res.json();
      if (totalSize === null) totalSize = Number(json?.MediaContainer?.totalSize ?? json?.MediaContainer?.size ?? 0);
      const metadata = json?.MediaContainer?.Metadata || [];
      if (!metadata.length) break;
      for (const t of metadata) {
        tracks.push({
          ratingKey:   String(t.ratingKey || ''),
          artistName:  String(t.originalTitle || t.grandparentTitle || ''),
          trackTitle:  String(t.title || ''),
          albumName:   String(t.parentTitle || ''),
          recordingMbid: extractPlexRecordingMbid(t),
          genres:      (t.Genre || []).map((g) => g.tag),
          moods:       [],
          libraryKey:  String(key),
          ratingCount: Number(t.ratingCount || 0),
          viewCount:   Number(t.viewCount || 0),
        });
      }
      start += metadata.length;
    }
  }

  // Plex omits moods from bulk listings — build ratingKey→moods map via mood directory
  const moodMap = new Map();
  for (const key of libraryKeys) {
    try {
      const moodDirUrl = buildUrl(url, `library/sections/${key}/mood`);
      moodDirUrl.searchParams.set('type', '10');
      const mr = await fetch(moodDirUrl.toString(), {
        headers: plexHeaders(token, { Accept: 'application/json' }),
      });
      if (!mr.ok) continue;
      const moodJson = await mr.json();
      const moodTags = moodJson?.MediaContainer?.Directory || [];

      for (let i = 0; i < moodTags.length; i += 10) {
        await Promise.all(moodTags.slice(i, i + 10).map(async (mood) => {
          try {
            let moodStart = 0;
            let moodTotal = null;
            while (moodTotal === null || moodStart < moodTotal) {
              const tracksUrl = buildUrl(url, `library/sections/${key}/all`);
              tracksUrl.searchParams.set('type', '10');
              tracksUrl.searchParams.set('mood', mood.key);
              tracksUrl.searchParams.set('X-Plex-Container-Start', String(moodStart));
              tracksUrl.searchParams.set('X-Plex-Container-Size', String(PAGE_SIZE));
              const tr = await fetch(tracksUrl.toString(), {
                headers: plexHeaders(token, { Accept: 'application/json' }),
              });
              if (!tr.ok) break;
              const tj = await tr.json();
              if (moodTotal === null) moodTotal = Number(tj?.MediaContainer?.totalSize ?? tj?.MediaContainer?.size ?? 0);
              const moodMeta = tj?.MediaContainer?.Metadata || [];
              if (!moodMeta.length) break;
              for (const t of moodMeta) {
                const rk = String(t.ratingKey || '');
                if (!rk) continue;
                if (!moodMap.has(rk)) moodMap.set(rk, new Set());
                moodMap.get(rk).add(mood.title);
              }
              moodStart += moodMeta.length;
            }
          } catch { /* non-fatal */ }
        }));
      }
    } catch { /* non-fatal */ }
  }

  for (const t of tracks) {
    const moods = moodMap.get(t.ratingKey);
    if (moods) t.moods = [...moods];
  }

  return tracks;
}

// ── Webhook auto-registration ─────────────────────────────────────────────────
// Registers the Curatorr /webhook/plex URL with plex.tv and enables server-side
// webhooks. Returns { ok, created, webhookUrl } or { ok: false, reason }.

function normalizeWebhookPathname(url) {
  try {
    const pathname = String(new URL(String(url || '')).pathname || '').trim();
    if (!pathname) return '';
    return pathname.replace(/\/+$/, '') || '/';
  } catch (_err) {
    return '';
  }
}

function isManagedCuratorrPlexWebhookUrl(candidateUrl, targetUrl) {
  const candidatePath = normalizeWebhookPathname(candidateUrl);
  const targetPath = normalizeWebhookPathname(targetUrl);
  if (!candidatePath || !targetPath) return false;
  return candidatePath === targetPath
    || (candidatePath.endsWith('/webhook/plex') && targetPath.endsWith('/webhook/plex'));
}

export async function registerWebhook(plexUrl, token, webhookUrl, ctx) {
  const { plexHeaders: ctxPlexHeaders, buildPlexAuthHeaders } = ctx;

  if (!plexUrl || !token || !webhookUrl) {
    return { ok: false, reason: 'missing config', manual: false };
  }

  const accountUrl = 'https://plex.tv/api/v2/user/webhooks';
  const headers = {
    ...(ctxPlexHeaders ? ctxPlexHeaders() : {}),
    ...buildPlexAuthHeaders(token, {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    }),
  };

  const listRes = await fetch(accountUrl, { headers });
  if (!listRes.ok) throw new Error(`Plex account API returned HTTP ${listRes.status}`);

  const existingRaw = await listRes.json();
  const existingUrls = normalizePlexWebhookUrls(existingRaw);
  const hasWebhook = existingUrls.includes(webhookUrl);
  const staleUrls = existingUrls.filter((url) => url !== webhookUrl && isManagedCuratorrPlexWebhookUrl(url, webhookUrl));
  const nextUrls = existingUrls.filter((url) => !staleUrls.includes(url));
  if (!nextUrls.includes(webhookUrl)) nextUrls.push(webhookUrl);

  if (staleUrls.length || !hasWebhook) {
    const saveRes = await fetch(accountUrl, {
      method: 'POST',
      headers,
      body: JSON.stringify({ urls: nextUrls }),
    });
    if (!saveRes.ok) throw new Error(`Plex account API returned HTTP ${saveRes.status}`);
    await saveRes.json();
  }

  // Enable server-side webhooks on the local Plex server
  const prefsUrl = buildUrl(plexUrl, ':/prefs');
  prefsUrl.searchParams.set('WebHooksEnabled', '1');
  const prefsRes = await fetch(prefsUrl.toString(), {
    method: 'PUT',
    headers: buildPlexAuthHeaders(token, { Accept: 'application/json' }),
  });
  if (!prefsRes.ok) throw new Error(`Plex returned HTTP ${prefsRes.status} while enabling webhooks`);

  return { ok: true, created: !hasWebhook, pruned: staleUrls.length, webhookUrl, manual: false };
}

function normalizePlexWebhookUrls(payload) {
  const items = Array.isArray(payload)
    ? payload
    : (Array.isArray(payload?.urls) ? payload.urls : []);
  return [...new Set(
    items.map((e) => (typeof e === 'string' ? e.trim() : String(e?.url || '').trim())).filter(Boolean),
  )];
}
