# Music Assistant Issue

A user as the following setup for playing music through plex/plexamp but plays are not being registered in curatorr. He has confirmed that plays direct through plexamp are registering as expected. Please review in depth and report back potential way forward:

https://www.music-assistant.io/music-providers/plex/
https://www.music-assistant.io/plugins/plex-connect/

Findings

Direct Plexamp plays registering in Curatorr strongly suggests the existing Curatorr Plex webhook path is already healthy. Curatorr’s live Plex ingest is centered on /webhook/plex and only accepts standard Plex playback events like media.play, media.resume, media.stop, and media.scrobble in webhooks.js (line 852).

Curatorr does not currently have a direct Plex history importer. On Plex installs it has two real ingest paths:

live Plex webhooks when playback source is Plex in webhooks.js (line 852)
Tautulli live/history ingestion when playback source is Tautulli or when the Tautulli sync job runs in webhooks.js (line 588) and tautulli-sync.js (line 156)
This also matches the product docs in Quick-Start.md (line 50).
Music Assistant’s current Plex Connect design is not the same thing as a native Plex player emitting standard PMS webhooks. The current MA docs say Plex Connect reports playback activity to Plex and makes listening history/scrobbles visible in the Plex dashboard, but the current MA source does that by pushing /:/timeline updates to Plex, not by posting Plex webhook events to third parties. Best inference: this is the likely gap. Plex may be updating its own dashboard/history from those timeline updates without generating the same webhook flow Curatorr relies on.

There is also a likely account-attribution risk. Music Assistant’s Plex source is bound to one Plex account and one library, and Plex Connect reports through that provider session. So even if Plex records the play, it may be attributed to the Plex account used by the MA Plex provider, not necessarily the human controlling Plexamp. Curatorr keys Plex plays off the Plex account in the webhook payload in webhooks.js (line 922).

There is a second MA-side edge case: Plex Connect only reports a timeline when the currently playing MA track has a mapping back to the exact Plex provider instance selected for the plugin. If the user is really playing a non-Plex item, a differently-linked item, or the wrong Plex provider instance/library, MA can return early and never report the play upstream.

Version matters. On April 1, 2025, MA support said Plex dashboard activity from MA playback was not implemented. Plex Connect was then added later and merged for the 2.7.0 milestone on November 25, 2025. So if the user is on an older MA build, this may simply not exist there yet.

Way Forward

First confirm the user is on Music Assistant 2.7.0 or newer, and specifically using the Plex Connect plugin, not just the Plex music provider.

Reproduce one track and check four places in order:

Plex Dashboard / Plex play history
Tautulli current activity / history
Curatorr logs filtered to webhook
Curatorr logs filtered to tautulli-sync
If the play appears in Tautulli, the most practical fix is to move Curatorr’s playback source to Tautulli, register the Tautulli webhook, and keep Tautulli gap-fill enabled. That is the best short-term path because Curatorr already supports that path cleanly.

If the play appears in Plex Dashboard/history but not in Tautulli and not in Curatorr, then this is a true integration gap: Curatorr currently has no direct Plex-history fallback for these MA-driven plays. The clean Curatorr-side enhancement would be a direct Plex history/session importer, analogous to the existing Tautulli gap-fill job.

If the play does not appear in Plex Dashboard/history at all, the issue is upstream in Music Assistant Plex Connect or its provider mapping/account selection, not in Curatorr.

Independently, verify whether the play is landing under the Plex account used by the MA Plex provider rather than the expected end-user account. If so, Curatorr may be recording it under a different user than the person being checked.

Sources

Curatorr local ingest paths: webhooks.js (line 588), webhooks.js (line 852), tautulli-sync.js (line 156), Quick-Start.md (line 50)
Music Assistant Plex provider docs: https://www.music-assistant.io/music-providers/plex/
Music Assistant Plex Connect docs: https://www.music-assistant.io/plugins/plex-connect/
Music Assistant support issue from April 1, 2025: https://github.com/music-assistant/support/issues/3757
Music Assistant Plex Connect PR merged November 25, 2025: https://github.com/music-assistant/server/pull/2608
Plex webhook event model: https://support.plex.tv/articles/115002267687-webhooks/
If you want, I can turn this into a concrete validation checklist for the user or outline what a Curatorr-side Plex-history fallback would need to do.