import re
import pandas as pd
import numpy as np

def parse_form_score(form):
    if not isinstance(form, str) or not form.strip():
        return 0.0
    chars = re.findall(r"[0-9PUFROBD]", form.upper())[-6:]
    if not chars:
        return 0.0
    score = 0
    for c in chars:
        if c == "1": score += 10
        elif c == "2": score += 8
        elif c == "3": score += 6
        elif c == "4": score += 4
        elif c.isdigit(): score += max(1, 6 - int(c))
    return score / len(chars)

def implied_probability(odds):
    try:
        odds = float(odds)
        if odds <= 1: return np.nan
        return 1 / odds
    except Exception:
        return np.nan

def normalise(s):
    s = pd.to_numeric(s, errors="coerce")
    if len(s) == 0 or s.max() == s.min():
        return pd.Series([0] * len(s), index=s.index)
    return ((s - s.min()) / (s.max() - s.min())).fillna(0)

def score_runners(df):
    if df.empty:
        return df
    d = df.copy()
    for c in ["official_rating","cd_winner","course_winner","distance_winner","days_since_run","decimal_odds","exchange_back","latest_odds"]:
        if c not in d.columns:
            d[c] = np.nan
    d["best_odds"] = d["exchange_back"].fillna(d["decimal_odds"]).fillna(d["latest_odds"])
    d["implied_probability"] = d["best_odds"].apply(implied_probability)
    d["form_score"] = d["form"].apply(parse_form_score).fillna(0)
    d["rating_score"] = normalise(d["official_rating"]) * 10
    d["cd_score"] = np.where(pd.to_numeric(d["cd_winner"], errors="coerce").fillna(0) == 1, 10, 0)
    d["course_score"] = np.where(pd.to_numeric(d["course_winner"], errors="coerce").fillna(0) == 1, 5, 0)
    d["distance_score"] = np.where(pd.to_numeric(d["distance_winner"], errors="coerce").fillna(0) == 1, 5, 0)
    days = pd.to_numeric(d["days_since_run"], errors="coerce")
    d["freshness_score"] = np.where(days.between(10,60), 8, np.where(days.between(61,120), 4, 0))
    d["raw_score"] = d["form_score"] + d["rating_score"] + d["cd_score"] + d["course_score"] + d["distance_score"] + d["freshness_score"]
    d["score_positive"] = d["raw_score"].clip(lower=0.01)
    total = d.groupby("race_id")["score_positive"].transform("sum")
    d["model_win_probability"] = (d["score_positive"] / total).round(4)
    d["model_place_probability"] = (d["model_win_probability"] * 2.35).clip(upper=0.9).round(4)
    d["value_score"] = (d["model_win_probability"] - d["implied_probability"].fillna(0)).round(4)
    d["confidence_score"] = (d["model_win_probability"] * 100).round(1)
    d["risk_rating"] = np.where(d["confidence_score"] >= 25, "Low", np.where(d["confidence_score"] >= 14, "Medium", "High"))
    return d.sort_values(["race_id","model_win_probability"], ascending=[True,False])
