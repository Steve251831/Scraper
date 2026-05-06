# UK Horse Racing Predictor - Stage 5

Stage 5 turns the app into a more usable daily system.

## New in Stage 5

- Daily setup checklist
- Data quality checker
- Missing odds/results checks
- Duplicate runner checks
- Odds snapshot workflow
- Staking calculator
- Bankroll page
- Auto-settlement for selections
- Win and each-way return calculations
- Semi-automatic racecard collection helper
- Keeps Stage 4 paste cleaner, manual entry, scoring controls, horse matcher and backtesting

## Streamlit Cloud

Main file path:

```text
app.py
```

## Dependencies

Only:

```text
streamlit
pandas
numpy
```

## Suggested daily workflow

1. Add today's racecards using CSV, paste cleaner, or manual entry.
2. Add early odds snapshot.
3. Add later odds snapshot.
4. Check data quality.
5. Review daily picks.
6. Log selections.
7. Add results.
8. Auto-match results.
9. Auto-settle returns.
10. Review bankroll/performance.
