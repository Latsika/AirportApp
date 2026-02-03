# AirportApp

## Overview
AirportApp is a Flask-based desktop web app for airport sales, reports, and variable rewards.
Data is stored in a local SQLite database (airport_app.db).

## Run (Development)
From the project root:

```powershell
python web\app.py
```

The app runs locally and is accessed in a browser.

## Data Storage
- Main database: `airport_app.db`
- Automatic backups: created on every app start in `backups/`
- Backup retention: max 30 files (oldest deleted)

## Environment Variables

### App
- `AIRPORTAPP_DEBUG=1` enables Flask debug mode.
- `AIRPORTAPP_HTTPS=1` sets secure cookies (for HTTPS).
- `AIRPORTAPP_DB_PATH` overrides the SQLite DB path (useful for tests).

### SMTP (email notifications)
If not set, emails are not sent (popup still works).

- `SMTP_HOST`
- `SMTP_PORT` (default 587)
- `SMTP_USER`
- `SMTP_PASSWORD`
- `SMTP_SENDER` (optional)

Note: SMTP can also be configured in **Account settings** (stored in DB).
DB settings override ENV.

## Notifications
- Email recipients: configurable in **Account settings -> Create notifications** (max 10 addresses).
- Notification templates: editable and extendable in the same section.
- Pop-up notifications: shown to Admin on first app open after the check time.
- Email summary: sent together with the popup.

### Triggers
- New user created (waiting approval)
- Daily report created (on export)
- Monthly report created (on export)
- Daily report NOT created (checked after 08:00, Europe/Bratislava)
- Monthly report NOT created (first day of month, after 08:00)
- User deleted

## Reports
- Daily, Monthly, and Custom reports are generated from sales data.
- Report creation is logged in `report_snapshots` for notifications.

## Variable Rewards
- Rewards are based on monthly airport fees.
- Manual overrides per user are supported.
- Snapshots are stored in `variable_rewards_snapshots`.
- Per-user and full-list PDF exports are available.

## Roles
- Admin: full access
- Deputy: user approvals
- User: sales only

## Notes
- For production use, keep `AIRPORTAPP_DEBUG` unset.
- Ensure backups are copied if the app folder is moved.
