# UK Horse Racing Predictor - Stage 8 CSV Converter

Adds a converter for Racing Post-style racecard CSVs and Betfair mapping CSVs.

## New pages

- CSV Converter
- Converts Racing Post-style files:
  - `name` → `horse`
  - `off_time` → `race_time`
  - `ofr` → `official_rating`
  - `field_size` → `runners_count`
  - `last_run` → `days_since_run`
- Converts Betfair mapping files:
  - `date` → `race_date`
  - `off` → `race_time`
  - `horse` → `horse`
  - `bsp/pre_min/pre_max/pre_vol` → odds fields

## Streamlit Cloud

Main file path:

```text
app.py
```
