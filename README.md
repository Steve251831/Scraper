# UK Horse Racing Predictor - Stage 6

Stage 6 adds persistent database support.

## New in Stage 6

- SQLite fallback still works
- Supabase/PostgreSQL support
- Database connection helper
- SQL setup script
- Streamlit secrets support
- Data no longer needs to reset if connected to Supabase
- Keeps daily workflow, data quality, odds movement, staking, settlement and exports

## Streamlit Cloud

Main file path:

```text
app.py
```

## Quick start without Supabase

The app still works locally/Streamlit with SQLite if you do nothing.

## Supabase setup

1. Create a Supabase project.
2. Go to Project Settings > Database.
3. Copy your connection string.
4. In Streamlit Cloud, go to App > Settings > Secrets.
5. Add:

```toml
DATABASE_URL = "postgresql://postgres.xxxxxx:YOUR_PASSWORD@aws-0-eu-west-2.pooler.supabase.com:6543/postgres"
```

6. Reboot the app.
7. Open the app.
8. Go to Dashboard and check database mode.

## Important

Use the pooler connection string from Supabase if available.
