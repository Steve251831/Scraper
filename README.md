# UK Horse Racing Predictor - Ultra Safe Streamlit Version

This is the stripped-back deployment version designed to install cleanly on Streamlit Cloud.

## Streamlit Cloud settings

Main file path:

```text
app.py
```

## Requirements

This version only installs:

```text
streamlit
pandas
numpy
```

This avoids common Streamlit Cloud install failures from scraper/API/ML packages.

## First test

1. Deploy on Streamlit Cloud.
2. Open the app.
3. Go to **Import Data**.
4. Click **Load sample racecard data**.
5. Go to **Daily Picks**.
6. Select date **2026-05-06**.

## Later upgrades

Once this runs, add scraping/API packages one at a time.
