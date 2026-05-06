import difflib
import pandas as pd
import numpy as np
from db import connect
from scoring import score_runners

def odds_movement(selected_date=None):
    query = """
    SELECT race_date, course, race_time, horse, MIN(imported_at) AS first_imported,
           MAX(imported_at) AS last_imported, COUNT(*) AS snapshots
    FROM odds
    WHERE (? IS NULL OR race_date = ?)
    GROUP BY race_date, course, race_time, horse
    """
    con = connect()
    base = pd.read_sql_query(query, con, params=(selected_date, selected_date))
    if base.empty:
        return base

    odds = pd.read_sql_query("SELECT * FROM odds WHERE (? IS NULL OR race_date = ?)", con, params=(selected_date, selected_date))
    odds["price"] = pd.to_numeric(odds["exchange_back"], errors="coerce").fillna(pd.to_numeric(odds["decimal_odds"], errors="coerce"))
    odds = odds.dropna(subset=["price"])
    if odds.empty:
        return base

    first_rows = odds.sort_values(["imported_at", "odds_time"]).groupby(["race_date", "course", "race_time", "horse"]).first().reset_index()
    last_rows = odds.sort_values(["imported_at", "odds_time"]).groupby(["race_date", "course", "race_time", "horse"]).last().reset_index()

    first = first_rows[["race_date", "course", "race_time", "horse", "price", "traded_volume"]].rename(columns={"price": "opening_odds", "traded_volume": "opening_volume"})
    last = last_rows[["race_date", "course", "race_time", "horse", "price", "traded_volume", "bookmaker", "odds_time"]].rename(columns={"price": "latest_odds", "traded_volume": "latest_volume"})

    out = base.merge(first, on=["race_date", "course", "race_time", "horse"], how="left")
    out = out.merge(last, on=["race_date", "course", "race_time", "horse"], how="left")
    out["odds_change"] = out["latest_odds"] - out["opening_odds"]
    out["odds_change_pct"] = (out["odds_change"] / out["opening_odds"] * 100).round(2)
    out["movement_flag"] = np.where(out["odds_change_pct"] <= -10, "Steamer", np.where(out["odds_change_pct"] >= 15, "Drifter", "Stable"))
    out["volume_change"] = pd.to_numeric(out["latest_volume"], errors="coerce").fillna(0) - pd.to_numeric(out["opening_volume"], errors="coerce").fillna(0)
    return out.sort_values(["race_date", "course", "race_time", "odds_change_pct"])

def auto_match_results_to_selections():
    con = connect()
    selections = pd.read_sql_query("SELECT * FROM selections", con)
    results = pd.read_sql_query("SELECT * FROM results", con)
    if selections.empty or results.empty:
        return 0

    updates = 0
    with con:
        for _, sel in selections.iterrows():
            match = results[
                (results["race_date"].astype(str) == str(sel["selection_date"])) &
                (results["course"].astype(str).str.lower() == str(sel["meeting"]).lower()) &
                (results["race_time"].astype(str) == str(sel["race_time"])) &
                (results["horse"].astype(str).str.lower() == str(sel["horse"]).lower())
            ]
            if match.empty:
                continue
            res = match.iloc[0]
            position = str(res.get("finishing_position", ""))
            status = res.get("result_status")
            if not status or pd.isna(status):
                if position == "1":
                    status = "Won"
                elif position in ["2", "3", "4"]:
                    status = "Placed"
                else:
                    status = "Lost"
            con.execute("UPDATE selections SET result_position=?, result_status=? WHERE id=?", (position, status, int(sel["id"])))
            updates += 1
    return updates

def calculate_return(bet_type, status, stake, odds, ew_place_fraction=0.2):
    try:
        stake = float(stake or 0)
        odds = float(odds or 0)
    except Exception:
        return 0.0, -float(stake or 0)

    if stake <= 0 or odds <= 1:
        return 0.0, -stake

    status = str(status).lower()
    bet_type_lower = str(bet_type).lower()

    is_each_way = "each-way" in bet_type_lower or "each way" in bet_type_lower or "ew" in bet_type_lower

    if not is_each_way:
        if status == "won":
            ret = stake * odds
        else:
            ret = 0.0
        return round(ret, 2), round(ret - stake, 2)

    # Each-way stake is treated as total stake split 50/50 win/place.
    win_stake = stake / 2
    place_stake = stake / 2
    place_odds = 1 + ((odds - 1) * ew_place_fraction)

    if status == "won":
        ret = (win_stake * odds) + (place_stake * place_odds)
    elif status == "placed":
        ret = place_stake * place_odds
    else:
        ret = 0.0

    return round(ret, 2), round(ret - stake, 2)

def auto_settle_selections(ew_place_fraction=0.2):
    con = connect()
    selections = pd.read_sql_query("SELECT * FROM selections", con)
    if selections.empty:
        return 0

    updates = 0
    with con:
        for _, sel in selections.iterrows():
            status = sel.get("result_status")
            if not status or pd.isna(status):
                continue
            ret, pl = calculate_return(sel.get("bet_type"), status, sel.get("stake"), sel.get("odds_taken"), ew_place_fraction)
            con.execute("UPDATE selections SET return_amount=?, profit_loss=? WHERE id=?", (ret, pl, int(sel["id"])))
            updates += 1
    return updates

def performance_by_bet_type():
    df = pd.read_sql_query("SELECT * FROM selections", connect())
    if df.empty:
        return df
    df["stake"] = pd.to_numeric(df["stake"], errors="coerce").fillna(0)
    df["profit_loss"] = pd.to_numeric(df["profit_loss"], errors="coerce").fillna(0)
    df["is_won"] = df["result_status"].astype(str).str.lower().eq("won")
    df["is_placed"] = df["result_status"].astype(str).str.lower().isin(["won", "placed"])
    grouped = df.groupby("bet_type").agg(
        selections=("id", "count"),
        total_stake=("stake", "sum"),
        total_profit_loss=("profit_loss", "sum"),
        winners=("is_won", "sum"),
        placed=("is_placed", "sum"),
    ).reset_index()
    grouped["win_strike_rate_%"] = (grouped["winners"] / grouped["selections"] * 100).round(2)
    grouped["place_strike_rate_%"] = (grouped["placed"] / grouped["selections"] * 100).round(2)
    grouped["roi_%"] = (grouped["total_profit_loss"] / grouped["total_stake"].replace(0, np.nan) * 100).fillna(0).round(2)
    return grouped

def horse_name_matches(input_name, limit=10):
    con = connect()
    runners = pd.read_sql_query("SELECT DISTINCT horse FROM runners ORDER BY horse", con)
    if runners.empty or not input_name:
        return pd.DataFrame()
    names = runners["horse"].dropna().astype(str).tolist()
    matches = difflib.get_close_matches(input_name, names, n=limit, cutoff=0.35)
    rows = []
    for m in matches:
        score = round(difflib.SequenceMatcher(None, input_name.lower(), m.lower()).ratio() * 100, 1)
        rows.append({"input": input_name, "matched_horse": m, "match_score_%": score})
    return pd.DataFrame(rows)

def backtest_from_stored_results(load_runners_func, selected_date=None, weights=None):
    df = load_runners_func(selected_date)
    results = pd.read_sql_query("SELECT * FROM results WHERE (? IS NULL OR race_date = ?)", connect(), params=(selected_date, selected_date))
    if df.empty or results.empty:
        return pd.DataFrame(), pd.DataFrame()

    scored = score_runners(df, weights)
    top = scored.sort_values("model_win_probability", ascending=False).groupby("race_id").head(1).copy()
    ew = scored.sort_values(["each_way_score", "model_place_probability"], ascending=False).groupby("race_id").head(1).copy()

    tests = []
    for label, picks in [("Top Win Pick Per Race", top), ("Top Each-Way Pick Per Race", ew)]:
        merged = picks.merge(results, on=["race_date", "course", "race_time", "horse"], how="left", suffixes=("", "_result"))
        merged["is_won"] = merged["finishing_position"].astype(str).eq("1")
        merged["is_placed"] = merged["finishing_position"].astype(str).isin(["1", "2", "3", "4"])
        tests.append({
            "strategy": label,
            "picks": len(merged),
            "winners": int(merged["is_won"].sum()),
            "placed": int(merged["is_placed"].sum()),
            "win_strike_rate_%": round(merged["is_won"].mean() * 100, 2) if len(merged) else 0,
            "place_strike_rate_%": round(merged["is_placed"].mean() * 100, 2) if len(merged) else 0,
        })
    return pd.DataFrame(tests), scored

def data_quality_report(selected_date=None):
    con = connect()
    runners = pd.read_sql_query("""
        SELECT m.race_date, m.course, r.race_time, r.race_name, ru.horse, ru.trainer, ru.jockey,
               ru.official_rating, ru.form, ru.non_runner
        FROM runners ru
        JOIN races r ON ru.race_id = r.id
        JOIN meetings m ON r.meeting_id = m.id
        WHERE (? IS NULL OR m.race_date = ?)
    """, con, params=(selected_date, selected_date))

    odds = pd.read_sql_query("SELECT * FROM odds WHERE (? IS NULL OR race_date = ?)", con, params=(selected_date, selected_date))
    results = pd.read_sql_query("SELECT * FROM results WHERE (? IS NULL OR race_date = ?)", con, params=(selected_date, selected_date))

    issues = []

    if runners.empty:
        issues.append({"severity": "High", "issue": "No racecards loaded", "count": 1, "detail": "Import racecards first."})
        return pd.DataFrame(issues)

    dupes = runners.groupby(["race_date", "course", "race_time", "horse"]).size().reset_index(name="count")
    dupes = dupes[dupes["count"] > 1]
    if not dupes.empty:
        issues.append({"severity": "Medium", "issue": "Duplicate runners", "count": len(dupes), "detail": "Same horse appears more than once in a race."})

    missing_trainer = runners["trainer"].isna().sum() + runners["trainer"].astype(str).str.strip().eq("").sum()
    if missing_trainer:
        issues.append({"severity": "Low", "issue": "Missing trainer data", "count": int(missing_trainer), "detail": "Trainer form cannot be scored yet."})

    missing_jockey = runners["jockey"].isna().sum() + runners["jockey"].astype(str).str.strip().eq("").sum()
    if missing_jockey:
        issues.append({"severity": "Low", "issue": "Missing jockey data", "count": int(missing_jockey), "detail": "Jockey form cannot be scored yet."})

    if odds.empty:
        issues.append({"severity": "Medium", "issue": "No odds loaded", "count": len(runners), "detail": "Value scoring and market movement will be limited."})
    else:
        key_cols = ["race_date", "course", "race_time", "horse"]
        runners_key = runners[key_cols].drop_duplicates()
        odds_key = odds[key_cols].drop_duplicates()
        missing_odds = runners_key.merge(odds_key, on=key_cols, how="left", indicator=True)
        missing_odds = missing_odds[missing_odds["_merge"] == "left_only"]
        if not missing_odds.empty:
            issues.append({"severity": "Medium", "issue": "Missing odds for runners", "count": len(missing_odds), "detail": "Some runners do not have odds rows."})

    if results.empty:
        issues.append({"severity": "Low", "issue": "No results loaded", "count": 1, "detail": "Selections cannot be settled until results are loaded."})

    if not issues:
        issues.append({"severity": "OK", "issue": "No major data issues found", "count": 0, "detail": "Data looks usable for this stage."})

    return pd.DataFrame(issues)
