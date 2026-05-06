import pandas as pd
import numpy as np
from db import connect

def odds_movement(selected_date=None):
    query = """
    SELECT
        race_date, course, race_time, horse,
        MIN(imported_at) AS first_imported,
        MAX(imported_at) AS last_imported,
        COUNT(*) AS snapshots
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

    out["movement_flag"] = np.where(
        out["odds_change_pct"] <= -10, "Steamer",
        np.where(out["odds_change_pct"] >= 15, "Drifter", "Stable")
    )

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

            con.execute("""
                UPDATE selections
                SET result_position=?, result_status=?
                WHERE id=?
            """, (position, status, int(sel["id"])))
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
