# Maintenance scripts

One-off backfills, checks and migrations. They are not used by the running app.
Run them from the project root, for example `python scripts/check_db.py`.

Careful: `reset_alerts.py` and the backfill scripts change production data.
