# Authentication and Roles

---

## Authentication Modes

Curatorr supports two login methods that can be used side by side:

### Plex SSO

Users sign in with their Plex account. Curatorr validates the token with Plex and maps the user to a role based on their relationship to the admin Plex account.

Role mapping happens automatically:
- The account that owns the Plex server (matching `Plex Admin User` in settings) is assigned the **Admin** role
- Home users and managed accounts on the same Plex account are assigned **User** by default
- Roles can be manually adjusted in **Settings → Users**

### Local Admin Account

Created during the setup wizard. This account bypasses Plex authentication and is intended as a fallback if Plex SSO is unavailable. It always has full admin access.

---

## Roles

| Role | Description |
|---|---|
| **Admin** | Full access to all settings, jobs, users, and Lidarr automation. Weekly Lidarr quotas are unlimited. |
| **Co-admin** | Access to Lidarr automation and most features. Subject to configurable weekly quotas (default: 3 artists / 6 albums per week). |
| **Power user** | Can use Lidarr automation when the admin has enabled it for this scope. Subject to lower quotas (default: 1 artist / 2 albums per week). |
| **User** | Standard access to their own play history, smart playlists, and artist/track views. Lidarr automation is off by default. |
| **Guest** | Read-only access. Cannot interact with suggestions or automation. Guest access can be restricted entirely in General settings. |

---

## Managing Users

Go to **Settings → Users** to view all accounts, their current roles, and their linked Plex identities.

From this view you can:
- Change a user's role
- Remove a user account

Role changes take effect on the user's next page load.

---

## Lidarr Automation Access

Lidarr automation eligibility is determined by role:

- **Admin** and **Co-admin** — always eligible when Lidarr is configured and automation is enabled
- **Power user** — eligible only when the admin has set automation scope to **Role based**
- **User** and **Guest** — not eligible by default; quota can be set to allow limited access

Weekly quota limits are configured in **Settings → Lidarr → Automation**. Setting a quota to `-1` makes it unlimited for that role.
