import pandas as pd
import numpy as np

RACECARD_COLUMNS = [
    "race_date","course","country","going","race_time","race_name","race_type","distance",
    "class","runners_count","ew_terms","horse","trainer","jockey","draw","age","sex",
    "weight","official_rating","form","course_winner","distance_winner","cd_winner",
    "days_since_run","headgear","non_runner","source"
]

ODDS_COLUMNS = [
    "race_date","course","race_time","horse","bookmaker","decimal_odds","exchange_back",
    "exchange_lay","traded_volume","odds_time","source"
]

def clean_bool_to_int(series):
    return series.fillna(False).astype(str).str.lower().isin(["true", "1", "yes", "y"]).astype(int)

def convert_racingpost_csv(df):
    out = pd.DataFrame()
    out["race_date"] = df.get("date", "")
    out["course"] = df.get("course", "")
    out["country"] = df.get("region", "UK")
    out["going"] = df.get("going", "")
    out["race_time"] = df.get("off_time", "")
    out["race_name"] = df.get("race_name", "")
    out["race_type"] = df.get("race_type", "")
    out["distance"] = df.get("distance", df.get("distance_round", ""))
    out["class"] = df.get("race_class", "")
    out["runners_count"] = df.get("field_size", "")
    out["ew_terms"] = ""
    out["horse"] = df.get("name", "")
    out["trainer"] = df.get("trainer", "")
    out["jockey"] = df.get("jockey", "")
    out["draw"] = df.get("draw", "")
    out["age"] = df.get("age", "")
    out["sex"] = df.get("sex", df.get("sex_code", ""))
    out["weight"] = df.get("lbs", "")
    out["official_rating"] = df.get("ofr", "")
    out["form"] = df.get("form", "")
    out["course_winner"] = 0
    out["distance_winner"] = 0
    out["cd_winner"] = 0
    out["days_since_run"] = df.get("last_run", "")
    out["headgear"] = df.get("headgear", "")
    out["non_runner"] = clean_bool_to_int(df.get("non_runner", pd.Series([False] * len(df))))
    out["source"] = "racingpost_csv"

    # Use stats JSON-like strings to flag course/distance where possible.
    if "stats_horse_course" in df.columns:
        out["course_winner"] = df["stats_horse_course"].astype(str).str.contains("'wins': '1|'wins': '2|'wins': '3|'wins': '4|'wins': '5", regex=True).astype(int)
    if "stats_horse_distance" in df.columns:
        out["distance_winner"] = df["stats_horse_distance"].astype(str).str.contains("'wins': '1|'wins': '2|'wins': '3|'wins': '4|'wins': '5", regex=True).astype(int)
    out["cd_winner"] = ((out["course_winner"] == 1) & (out["distance_winner"] == 1)).astype(int)

    return out[RACECARD_COLUMNS]

def convert_betfair_mapping_csv(df):
    out = pd.DataFrame()
    out["race_date"] = df.get("date", "")
    out["course"] = df.get("course", "")
    out["race_time"] = df.get("off", "")
    out["horse"] = df.get("horse", "")
    out["bookmaker"] = "Betfair"
    # BSP is the settled Betfair Starting Price; use pre_min for available pre-race if present, else bsp.
    out["decimal_odds"] = df.get("bsp", df.get("pre_min", ""))
    out["exchange_back"] = df.get("pre_min", df.get("bsp", ""))
    out["exchange_lay"] = df.get("pre_max", "")
    out["traded_volume"] = df.get("pre_vol", df.get("morning_vol", ""))
    out["odds_time"] = df.get("off", "")
    out["source"] = "betfair_mapping_csv"
    return out[ODDS_COLUMNS]

def detect_file_type(df):
    cols = set(df.columns)
    if {"name", "off_time", "ofr"}.issubset(cols) or {"name", "off_time", "field_size"}.issubset(cols):
        return "Racing Post Racecard CSV"
    if {"horse", "bsp", "pre_min", "pre_max"}.issubset(cols) or {"horse", "bsp", "pre_vol"}.issubset(cols):
        return "Betfair Mapping Odds CSV"
    return "Unknown"

def convert_auto(df):
    detected = detect_file_type(df)
    if detected == "Racing Post Racecard CSV":
        return detected, convert_racingpost_csv(df)
    if detected == "Betfair Mapping Odds CSV":
        return detected, convert_betfair_mapping_csv(df)
    return detected, df
