# UK Horse Racing Predictor - Stage 7 Daily Import

Stage 7 adds a daily one-button import workflow.

## What it can do

- Save source URLs for racecards, odds and results
- Press one button to fetch today’s source pages
- Extract HTML tables from those pages
- Auto-map common racing columns
- Review/edit before import
- Import racecards, odds and results into SQLite
- Keep previous scoring, picks, odds movement and exports

## Important

This does not bypass blocked sites, paywalls or Cloudflare.

If a site does not provide normal HTML tables, use the paste cleaner or a proper API.

## Streamlit Cloud

Main file path:

```text
app.py
```

## Best daily workflow

1. Go to **Source Manager**.
2. Add public URLs that contain racecard/odds/results tables.
3. Use `{date}` inside URLs where needed.
4. Go to **Daily Auto Import**.
5. Press **Fetch Today’s Sources**.
6. Review extracted tables.
7. Import the correct table.
