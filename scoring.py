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
    if series.empty or series.max() == series.min():
        return pd.Series([0] * len(series))
    return ((series - series.min()) / (series.max() - series.min())).fillna(0)

def score_runners(df):
    if df.empty:
        return df

    data = df.copy()
    data["form_score"] = data["form"].apply(parse_form_score)
    data["rating_score"] = normalise(data["official_rating"]) * 15

    data["cd_score"] = np.where(pd.to_numeric(data["cd_winner"], errors="coerce").fillna(0) == 1, 10, 0)
    data["course_score"] = np.where(pd.to_numeric(data["course_winner"], errors="coerce").fillna(0) == 1, 5, 0)
    data["distance_score"] = np.where(pd.to_numeric(data["distance_winner"], errors="coerce").fillna(0) == 1, 5, 0)

    days = pd.to_numeric(data["days_since_run"], errors="coerce")
    data["freshness_score"] = np.where(days.between(10, 60), 8, np.where(days.between(61, 120), 4, 0))

    data["base_score"] = (
        data["form_score"] * 2
        + data["rating_score"]
        + data["cd_score"]
        + data["course_score"]
        + data["distance_score"]
        + data["freshness_score"]
    )

    data["score_positive"] = data["base_score"].clip(lower=0.01)
    race_total = data.groupby("race_id")["score_positive"].transform("sum")
    data["model_win_probability"] = (data["score_positive"] / race_total).round(4)
    data["model_place_probability"] = (data["model_win_probability"] * 2.25).clip(upper=0.85).round(4)
    data["value_score"] = 0.0
    data["confidence_score"] = (data["model_win_probability"] * 100).round(1)
    data["risk_rating"] = np.where(data["confidence_score"] >= 25, "Low", np.where(data["confidence_score"] >= 14, "Medium", "High"))
    data["each_way_score"] = (data["model_place_probability"] * 100).round(2)

    return data.sort_values(["race_id", "model_win_probability"], ascending=[True, False])
