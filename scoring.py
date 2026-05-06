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
        if c == "1":
            score += 10
        elif c == "2":
            score += 8
        elif c == "3":
            score += 6
        elif c == "4":
            score += 4
        elif c.isdigit():
            score += max(1, 6 - int(c))
    return round(score / len(chars), 2)

def normalise(series):
    series = pd.to_numeric(series, errors="coerce")
    if len(series) == 0 or series.max() == series.min():
        return pd.Series([0] * len(series), index=series.index)
    return ((series - series.min()) / (series.max() - series.min())).fillna(0)

def implied_probability(decimal_odds):
    try:
        odds = float(decimal_odds)
        if odds <= 1:
            return np.nan
        return 1 / odds
    except Exception:
        return np.nan

def score_runners(df, weights=None):
    if df.empty:
        return df

    weights = weights or {
        "form": 20,
        "rating": 15,
        "cd": 15,
        "course": 7,
        "distance": 8,
        "freshness": 10,
        "draw": 5,
        "market": 10,
    }

    data = df.copy()

    for col in [
        "official_rating", "course_winner", "distance_winner", "cd_winner",
        "days_since_run", "draw", "decimal_odds", "exchange_back",
        "opening_odds", "latest_odds", "odds_change_pct", "traded_volume"
    ]:
        if col not in data.columns:
            data[col] = np.nan

    data["best_odds"] = data["exchange_back"].fillna(data["decimal_odds"]).fillna(data["latest_odds"])
    data["implied_probability"] = data["best_odds"].apply(implied_probability)

    data["form_score"] = data["form"].apply(parse_form_score) / 10
    data["rating_score"] = normalise(data["official_rating"])

    data["cd_score"] = np.where(pd.to_numeric(data["cd_winner"], errors="coerce").fillna(0) == 1, 1, 0)
    data["course_score"] = np.where(pd.to_numeric(data["course_winner"], errors="coerce").fillna(0) == 1, 1, 0)
    data["distance_score"] = np.where(pd.to_numeric(data["distance_winner"], errors="coerce").fillna(0) == 1, 1, 0)

    days = pd.to_numeric(data["days_since_run"], errors="coerce")
    data["freshness_score"] = np.where(days.between(10, 60), 1, np.where(days.between(61, 120), 0.5, 0))

    draw = pd.to_numeric(data["draw"], errors="coerce")
    runners = pd.to_numeric(data["runners_count"], errors="coerce")
    data["draw_score"] = np.where(draw.notna() & runners.notna(), 1 - ((draw - 1) / runners).clip(0, 1), 0.5)

    data["market_score"] = data["implied_probability"].fillna(0)
    if data["market_score"].max() > 0:
        data["market_score"] = normalise(data["market_score"])

    # Steamers get a small confidence lift, drifters get a small penalty.
    change = pd.to_numeric(data["odds_change_pct"], errors="coerce").fillna(0)
    data["movement_adjustment"] = np.where(change <= -10, 3, np.where(change >= 15, -3, 0))

    data["raw_score"] = (
        data["form_score"] * weights["form"] +
        data["rating_score"] * weights["rating"] +
        data["cd_score"] * weights["cd"] +
        data["course_score"] * weights["course"] +
        data["distance_score"] * weights["distance"] +
        data["freshness_score"] * weights["freshness"] +
        data["draw_score"] * weights["draw"] +
        data["market_score"] * weights["market"] +
        data["movement_adjustment"]
    )

    data["score_positive"] = data["raw_score"].clip(lower=0.01)
    race_total = data.groupby("race_id")["score_positive"].transform("sum")
    data["model_win_probability"] = (data["score_positive"] / race_total).round(4)
    data["model_place_probability"] = (data["model_win_probability"] * 2.35).clip(upper=0.90).round(4)

    data["value_score"] = (data["model_win_probability"] - data["implied_probability"].fillna(0)).round(4)
    data["confidence_score"] = (data["model_win_probability"] * 100).round(1)

    data["risk_rating"] = np.where(
        data["confidence_score"] >= 25, "Low",
        np.where(data["confidence_score"] >= 14, "Medium", "High")
    )

    data["each_way_score"] = (
        (data["model_place_probability"] * 100) +
        (data["value_score"].clip(lower=0) * 100)
    ).round(2)

    data["bet_flag"] = np.where(
        data["value_score"] > 0.02, "Value",
        np.where(data["confidence_score"] >= 25, "Strong chance", "No Bet / Watch")
    )

    return data.sort_values(["race_id", "model_win_probability"], ascending=[True, False])
