# AirportApp

## Overview

AirportApp is a local Windows portable desktop web app for airport sales, reports, notifications, and variable rewards.

The app is packaged as `AirportApp.exe`. When launched, it starts a local Flask server and opens the browser at:

```text
http://127.0.0.1:<port>
```

The server is bound to `127.0.0.1`, so it is not exposed to other computers on the company network by default.

## Current Release

Latest release folder:

```text
release_2026-06-17_security
```

Release contents:

```text
AirportApp.exe
install_update.exe
RELEASE_NOTES.txt
```

This release folder intentionally does not include `airport_app.db`. Existing customer data must not be overwritten by a release or test database.

## Data Storage

Main customer data is stored in:

```text
airport_app.db
```

For the portable/frozen app, this database must be in the same folder as:

```text
AirportApp.exe
```

Runtime files:

- `airport_app.db`: main SQLite database
- `airport_app.secret`: per-install Flask session signing secret
- `backups/`: automatic database backups
- `logs/app.log`: application log
- `app_runtime.json`: current local server port while the app is running
- `crash.log`: startup crash details, only created after a fatal startup error

Automatic DB backups are created on app startup in `backups/`. Backup retention keeps up to 30 automatic DB backups.

Transfer and recovery guide: see `TROUBLESHOOTING.md`.

## Customer Data Rule

`airport_app.db` is customer data. Build, release, update, troubleshooting, and support steps must not overwrite, delete, replace, or regenerate a customer's database.

Required behavior:

- updater may back up `airport_app.db`, but must replace only `AirportApp.exe`;
- release folders must not include a customer/test `airport_app.db`;
- build scripts must preserve any existing local `dist/airport_app.db`;
- before any manual DB operation, copy or rename the current DB first;
- never use a fresh/demo DB to "fix" an existing customer installation.

## Security Behavior

- The app no longer uses a fixed Flask `SECRET_KEY` fallback.
- On first run, the app creates `airport_app.secret` next to `AirportApp.exe`.
- Failed login attempts are temporarily rate-limited: 5 failed attempts per user/IP scope in 15 minutes.
- Failed password reset attempts are also temporarily rate-limited for 15 minutes.
- Security question answers are stored as bcrypt hashes.
- Existing plaintext security answers are migrated to bcrypt hashes on app startup.
- In the frozen portable app, the app applies best-effort Windows ACL hardening to local runtime files and folders.

Important: `airport_app.secret` is not customer business data, but it is security-sensitive. When moving an existing installation, copy the whole app folder so sessions and local security state remain consistent.

## Run Development Version

From the project root:

```powershell
.\.venv\Scripts\python.exe web\app.py
```

Environment variables:

- `AIRPORTAPP_DEBUG=1`: enables Flask debug mode.
- `AIRPORTAPP_HTTPS=1`: sets secure cookies for HTTPS deployments.
- `AIRPORTAPP_DB_PATH`: overrides the SQLite DB path for testing.
- `SECRET_KEY`: overrides the local generated secret, mainly for tests.
- `AIRPORTAPP_SKIP_ACL_HARDEN=1`: skips ACL hardening during tests.
- `AIRPORTAPP_FORCE_ACL_HARDEN=1`: forces ACL hardening in non-frozen development runs.

## SMTP And Notifications

If SMTP is not configured, emails are not sent, but in-app popup notifications still work.

SMTP can be configured in `Account settings` and is stored in the app database:

- SMTP host
- SMTP port
- SMTP user
- SMTP password
- sender
- TLS on/off

SMTP can also be provided through environment variables:

- `SMTP_HOST`
- `SMTP_PORT` (default 587)
- `SMTP_USER`
- `SMTP_PASSWORD`
- `SMTP_SENDER`

DB settings override environment values where both are present.

Notification triggers include:

- new user created and waiting for approval
- daily report created
- monthly report created
- daily report missing after the check time
- monthly report missing after the check time
- user deleted

## Roles

- `Admin`: full access
- `Deputy`: user approvals
- `User`: sales workflow and reports available through user menus

## Reports

- Daily, Monthly, and Custom reports are generated from live sales data.
- Ticket sales can use `Custom airline` and `Custom destination` when the requested route is not in the airline/destination master data. These values are saved only on that sale and are not added to master Airlines or Destinations.
- `Custom airline` automatically uses the custom destination flow because airline and destination are linked.
- Custom airline/destination is valid only for plane ticket sales and existing Airport Service Fees. It is not valid for standalone Airline Fees.
- Custom Report includes a `Custom Destinations` statistics table with airline, airline code, destination/country, city, airport code, ticket totals, Airport Service Fee totals, cash, and card totals.
- Report creation is logged in `report_snapshots` for notifications.
- PDF/CSV downloads support Slovak diacritics and other Unicode characters through safe ASCII fallback plus UTF-8 `filename*` support.

## Variable Rewards

- Rewards are based on monthly airport service fees.
- Manual overrides per user are supported.
- Current rewards screens and PDF exports are calculated from live database values.
- Saved snapshots remain available as audit/history data in `variable_rewards_snapshots`.
- Per-user and full-list PDF exports are available.
- Yearly rewards summary supports month ranges.

## Build And Release

Portable executable:

```powershell
installer\build_portable.bat
```

Customer updater:

```powershell
installer\build_update.bat
```

Expected build outputs:

```text
dist/AirportApp.exe
dist/install_update.exe
```

For customer updates, send `install_update.exe`. The updater:

1. asks for the folder containing the customer's `AirportApp.exe`,
2. stops the running target app,
3. backs up `airport_app.db`,
4. replaces only `AirportApp.exe`,
5. preserves customer data.

For a release package, create a folder like:

```text
release_YYYY-MM-DD_name/
  AirportApp.exe
  install_update.exe
  RELEASE_NOTES.txt
```

Do not include a customer or test `airport_app.db` in release folders unless the release is explicitly a fresh demo/test package.

## Moving To Another PC

For an existing customer installation, copy the whole app folder, not only the `.exe`.

Minimum files/folders to preserve customer data:

```text
AirportApp.exe
airport_app.db
airport_app.secret
backups/
```

Recommended: copy the whole app folder from the old PC to the new PC.

## Notes

- Keep `AIRPORTAPP_DEBUG` unset in production/customer builds.
- Do not run the production app directly from USB; copy it to a local disk first.
- Keep regular off-device backups of `airport_app.db` and `backups/`.
