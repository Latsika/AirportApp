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
release_2026-10-06_sales_list_theme_airline_colors
```

Release contents:

```text
AirportApp.exe
install_update.exe
RELEASE_INFO.md
app_release.json
RELEASE_NOTES.txt
README.md
TROUBLESHOOTING.md
Ticketing _AirportApp.md
```

Release ID is generated during build and is stored in `RELEASE_INFO.md` and `app_release.json`.

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
- `RELEASE_INFO.md`: installed release identifier, copied during fresh install/update
- `app_release.json`: machine-readable release identifier, copied during fresh install/update
- `backup_settings.json`: external backup configuration, created after Admin saves a backup folder
- `backups/`: automatic database backups
- `logs/app.log`: application log
- `app_runtime.json`: current local server port while the app is running
- `crash.log`: startup crash details, only created after a fatal startup error

## User Interface Updates

The current release adds a Light/Dark mode switch in the user menu. Light mode is the default. The selected mode is stored in the browser's local storage and stays selected after restarting the app.

`Sales List` now includes these visual updates:

- `Sold At` is shown near the start of the table.
- date is shown as `day.month.year`, with time below the date.
- item descriptions are hidden; item codes are shown only.
- `CASH` is green and bold, `CARD` is red and bold.
- `Item Count` is hidden from the visible table only; backend data and calculations remain unchanged.
- the table scrolls vertically in the visible window, and horizontal scrolling is available at the bottom of the table when the window is narrow.

Airline names can be visually highlighted in `Sales List`. Admin can set a custom airline text color in:

```text
Manage Airlines -> EDIT
```

Only `Sales List` uses the configured airline text color. The airline name remains bold there. Existing airlines keep default styling until Admin enables and saves a custom color.

Automatic DB backups are created on app startup in `backups/`. Backup retention keeps up to 30 automatic DB backups.

Transfer and recovery guide: see `TROUBLESHOOTING.md`.

## External Backups

Admin can configure a permanent external backup folder in:

```text
Account settings -> External backups
```

Use `Choose folder` to select the target folder through the Windows folder picker. The folder must be outside the AirportApp application folder. The setting is stored next to `AirportApp.exe` in:

```text
backup_settings.json
```

When automatic backups are enabled, the app creates ZIP backups in the selected external folder:

```text
daily/
weekly/
monthly/
manual/
```

Each ZIP contains all `.db` files from the runtime app folder, plus release metadata and `backup_manifest.json`.

Retention:

```text
daily:   10 backups
weekly:  10 backups
monthly: 10 backups
manual:  100 backups
```

Admin can also click `Create backup now` to create a manual ZIP backup immediately.

To restore data, Admin can click `Restore from backup`, choose an AirportApp backup ZIP, and let the app restore the database files. Before replacing current databases, the app saves the current `.db` files into:

```text
backups/pre_restore_YYYY-MM-DD_HHMMSS/
```

After restore, restart `AirportApp.exe` before continuing work.

## Customer Data Rule

`airport_app.db` is customer data. Build, release, update, troubleshooting, and support steps must not overwrite, delete, replace, or regenerate a customer's database.

Required behavior:

- updater may back up `airport_app.db`, but must replace only `AirportApp.exe`;
- release folders must not include a customer/test `airport_app.db`;
- build scripts must preserve any existing local `dist/airport_app.db`;
- before any manual DB operation, copy or rename the current DB first;
- never use a fresh/demo DB to "fix" an existing customer installation.

## First Install On A New PC

Use this only when the customer has no existing AirportApp data on that PC.

1. Create a local folder on the new PC, for example:

```text
Desktop\AirportApp
```

2. Copy these files from the latest release folder into that new folder:

```text
AirportApp.exe
RELEASE_INFO.md
app_release.json
```

Do not use `install_update.exe` for a clean first install. The updater is for an existing installation that already has `AirportApp.exe`.

3. Start:

```text
AirportApp.exe
```

4. The app opens the browser at a local address like:

```text
http://127.0.0.1:<port>
```

5. On first run, the app creates:

```text
airport_app.db
airport_app.secret
backups/
logs/
```

6. Log in with the default first-run admin:

```text
Nickname: Admin
Password: 12345
```

7. Change the Admin password immediately when prompted.

8. Verify the app folder now contains:

```text
AirportApp.exe
airport_app.db
airport_app.secret
RELEASE_INFO.md
backups/
logs/
```

After setup, configure users, roles, fees, airlines, destinations, SMTP, and notification recipients as needed.

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

Customer localhost setup:

1. Start the customer's existing `AirportApp.exe` and log in as Admin.
2. Open `Account settings` and fill SMTP host, port, user, password, sender, and TLS.
3. Open `Create notifications` and add at least one recipient email address.
4. Save both screens. The settings are stored in the customer's `airport_app.db`.
5. Use `Reports` -> `Daily Report` -> `SAVE` as a quick SMTP smoke test. This sends a report-created notification to the configured recipients.

Important SMTP notes:

- `localhost` is only the local browser/app address. Email still needs outbound access from the customer PC to the SMTP server.
- `Use TLS` means STARTTLS, normally port `587`. Plain local/test SMTP should have TLS off. Implicit SSL on port `465` is not supported by the current sender code.
- Many providers require an app password or authenticated SMTP to be enabled.
- Some providers reject mail if `sender` is not the same account as `SMTP user` or an approved alias.
- Automatic daily/monthly report emails are sent as PDF attachments after `00:05` local time, during app activity. If the app was closed, catch-up runs after the next start/use.

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
- `Reward active this month` is month-specific. If a user is disabled for June, previous months are not changed.
- Empty manual amount field means automatic calculation.
- Manual amount `0.00` or any higher value is an explicit manual override.
- Clearing the manual amount field returns that user to automatic calculation.
- Current rewards screens and PDF exports are calculated from live database values.
- Saved snapshots remain available as audit/history data in `variable_rewards_snapshots`.
- Per-user and full-list PDF exports are available.
- Yearly rewards summary supports month ranges.
- Yearly rewards summary has `View` for on-screen preview and `Print PDF` for saving/downloading the PDF.

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
dist/RELEASE_INFO.md
dist/app_release.json
```

For customer updates, send `install_update.exe`. The updater:

1. asks for the folder containing the customer's `AirportApp.exe`,
2. stops the running target app,
3. backs up `airport_app.db`,
4. replaces `AirportApp.exe`,
5. copies `RELEASE_INFO.md` and `app_release.json`,
6. preserves customer data.

For a release package, create a folder like:

```text
release_CURRENT_YYYY-MM-DD_name/
  AirportApp.exe
  install_update.exe
  RELEASE_INFO.md
  app_release.json
  RELEASE_NOTES.txt
  README.md
  TROUBLESHOOTING.md
  Ticketing _AirportApp.md
```

Do not include a customer or test `airport_app.db` in release folders unless the release is explicitly a fresh demo/test package.

The customer file for an existing installation is `install_update.exe`. Use `AirportApp.exe` directly only for a clean first install or a manually copied portable app folder.

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
