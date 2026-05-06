# UK Horse Racing Predictor - Stage 3

Streamlit-safe version with:

- Racecard CSV import
- Odds CSV import
- Multiple odds snapshots
- Steamer/drifter detection
- Results CSV import
- Auto-match results to logged selections
- Bet-type performance stats
- Database export page
- NAP, each-way NAP, Win Yankee, Each-Way Yankee
- Value singles and No Bet / Watch flags

## Streamlit Cloud

Main file path:

```text
app.py
```

## First test

1. Upload to GitHub.
2. Reboot Streamlit app.
3. Go to **Import Data**.
4. Load sample racecards.
5. Load sample early odds.
6. Load sample later odds.
7. Go to **Odds Movement**.
8. Go to **Daily Picks** and select `2026-05-06`.
9. Log a selection.
10. Load sample results.
11. Click **Auto-match results to selections** on Results Tracker.

## Dependencies

Only:

```text
streamlit
pandas
numpy
```
