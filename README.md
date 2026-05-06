# UK Horse Racing Predictor - Stage 2

This is the next working Streamlit version.

## New in Stage 2

- Racecard CSV import
- Odds CSV import
- Results CSV import
- Improved daily selections
- NAP of the day
- Each-way NAP
- Win Yankee
- Each-way Yankee
- Value selections
- No Bet flags
- Results tracker
- Profit/loss dashboard
- CSV templates included

## Streamlit Cloud

Main file path:

```text
app.py
```

## Install

```bash
pip install -r requirements.txt
streamlit run app.py
```

## First test

1. Deploy app.
2. Go to **Import Data**.
3. Click **Load sample racecard data**.
4. Click **Load sample odds data**.
5. Go to **Daily Picks**.
6. Select **2026-05-06**.

## Import templates

Use the files inside:

```text
templates/
```

- `racecard_template.csv`
- `odds_template.csv`
- `results_template.csv`

## Important

This app does not invent missing horse racing data. If odds or results are missing, it leaves them blank.
