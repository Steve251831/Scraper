import difflib
import pandas as pd
import numpy as np
from sqlalchemy import text
from db import get_connection, using_postgres
from scoring import score_runners

def read_sql(sql, params=None):
    conn = get_connection()
    try:
        return pd.read_sql_query(text(sql) if using_postgres() else sql, conn, params=params or {})
    finally:
        conn.close()

def odds_movement(selected_date=None):
    where = "WHERE race_date = :d" if using_postgres() and selected_date else "WHERE race_date = ?" if selected_date else ""
    params = {"d": selected_date} if using_postgres() and selected_date else (selected_date,) if selected_date else None
    base = read_sql(f"""
    SELECT race_date, course, race_time, horse, MIN(imported_at) AS first_imported,
           MAX(imported_at) AS last_imported, COUNT(*) AS snapshots
    FROM odds {where}
    GROUP BY race_date, course, race_time, horse
    """, params)
    if base.empty: return base
    odds = read_sql(f"SELECT * FROM odds {where}", params)
    odds["price"] = pd.to_numeric(odds["exchange_back"], errors="coerce").fillna(pd.to_numeric(odds["decimal_odds"], errors="coerce"))
    odds = odds.dropna(subset=["price"])
    if odds.empty: return base
    first_rows = odds.sort_values(["imported_at","odds_time"]).groupby(["race_date","course","race_time","horse"]).first().reset_index()
    last_rows = odds.sort_values(["imported_at","odds_time"]).groupby(["race_date","course","race_time","horse"]).last().reset_index()
    first = first_rows[["race_date","course","race_time","horse","price","traded_volume"]].rename(columns={"price":"opening_odds","traded_volume":"opening_volume"})
    last = last_rows[["race_date","course","race_time","horse","price","traded_volume","bookmaker","odds_time"]].rename(columns={"price":"latest_odds","traded_volume":"latest_volume"})
    out = base.merge(first,on=["race_date","course","race_time","horse"],how="left").merge(last,on=["race_date","course","race_time","horse"],how="left")
    out["odds_change"] = out["latest_odds"] - out["opening_odds"]
    out["odds_change_pct"] = (out["odds_change"] / out["opening_odds"] * 100).round(2)
    out["movement_flag"] = np.where(out["odds_change_pct"] <= -10, "Steamer", np.where(out["odds_change_pct"] >= 15, "Drifter", "Stable"))
    out["volume_change"] = pd.to_numeric(out["latest_volume"], errors="coerce").fillna(0) - pd.to_numeric(out["opening_volume"], errors="coerce").fillna(0)
    return out.sort_values(["race_date","course","race_time","odds_change_pct"])

def calculate_return(bet_type, status, stake, odds, ew_place_fraction=0.2):
    try:
        stake = float(stake or 0); odds = float(odds or 0)
    except Exception:
        return 0.0, -float(stake or 0)
    if stake <= 0 or odds <= 1: return 0.0, -stake
    status = str(status).lower()
    is_each_way = any(x in str(bet_type).lower() for x in ["each-way","each way","ew"])
    if not is_each_way:
        ret = stake * odds if status == "won" else 0.0
        return round(ret,2), round(ret-stake,2)
    win_stake = stake / 2; place_stake = stake / 2
    place_odds = 1 + ((odds - 1) * ew_place_fraction)
    if status == "won": ret = (win_stake * odds) + (place_stake * place_odds)
    elif status == "placed": ret = place_stake * place_odds
    else: ret = 0.0
    return round(ret,2), round(ret-stake,2)

def auto_match_results_to_selections():
    selections = read_sql("SELECT * FROM selections")
    results = read_sql("SELECT * FROM results")
    if selections.empty or results.empty: return 0
    conn = get_connection(); updates = 0
    try:
        trans = conn.begin() if using_postgres() else None
        for _, sel in selections.iterrows():
            match = results[(results["race_date"].astype(str)==str(sel["selection_date"])) & (results["course"].astype(str).str.lower()==str(sel["meeting"]).lower()) & (results["race_time"].astype(str)==str(sel["race_time"])) & (results["horse"].astype(str).str.lower()==str(sel["horse"]).lower())]
            if match.empty: continue
            res = match.iloc[0]; position = str(res.get("finishing_position","")); status = res.get("result_status")
            if not status or pd.isna(status): status = "Won" if position=="1" else "Placed" if position in ["2","3","4"] else "Lost"
            if using_postgres(): conn.execute(text("UPDATE selections SET result_position=:p, result_status=:s WHERE id=:id"), {"p": position, "s": status, "id": int(sel["id"])})
            else: conn.execute("UPDATE selections SET result_position=?, result_status=? WHERE id=?", (position, status, int(sel["id"])))
            updates += 1
        if using_postgres(): trans.commit()
        else: conn.commit()
    finally:
        conn.close()
    return updates

def auto_settle_selections(ew_place_fraction=0.2):
    selections = read_sql("SELECT * FROM selections")
    if selections.empty: return 0
    conn = get_connection(); updates = 0
    try:
        trans = conn.begin() if using_postgres() else None
        for _, sel in selections.iterrows():
            status = sel.get("result_status")
            if not status or pd.isna(status): continue
            ret, pl = calculate_return(sel.get("bet_type"), status, sel.get("stake"), sel.get("odds_taken"), ew_place_fraction)
            if using_postgres(): conn.execute(text("UPDATE selections SET return_amount=:r, profit_loss=:pl WHERE id=:id"), {"r": ret, "pl": pl, "id": int(sel["id"])})
            else: conn.execute("UPDATE selections SET return_amount=?, profit_loss=? WHERE id=?", (ret, pl, int(sel["id"])))
            updates += 1
        if using_postgres(): trans.commit()
        else: conn.commit()
    finally:
        conn.close()
    return updates

def performance_by_bet_type():
    df = read_sql("SELECT * FROM selections")
    if df.empty: return df
    df["stake"] = pd.to_numeric(df["stake"], errors="coerce").fillna(0)
    df["profit_loss"] = pd.to_numeric(df["profit_loss"], errors="coerce").fillna(0)
    df["is_won"] = df["result_status"].astype(str).str.lower().eq("won")
    df["is_placed"] = df["result_status"].astype(str).str.lower().isin(["won","placed"])
    grouped = df.groupby("bet_type").agg(selections=("id","count"), total_stake=("stake","sum"), total_profit_loss=("profit_loss","sum"), winners=("is_won","sum"), placed=("is_placed","sum")).reset_index()
    grouped["win_strike_rate_%"] = (grouped["winners"]/grouped["selections"]*100).round(2)
    grouped["place_strike_rate_%"] = (grouped["placed"]/grouped["selections"]*100).round(2)
    grouped["roi_%"] = (grouped["total_profit_loss"]/grouped["total_stake"].replace(0,np.nan)*100).fillna(0).round(2)
    return grouped

def horse_name_matches(input_name, limit=10):
    runners = read_sql("SELECT DISTINCT horse FROM runners ORDER BY horse")
    if runners.empty or not input_name: return pd.DataFrame()
    names = runners["horse"].dropna().astype(str).tolist()
    matches = difflib.get_close_matches(input_name, names, n=limit, cutoff=0.35)
    return pd.DataFrame([{"input": input_name, "matched_horse": m, "match_score_%": round(difflib.SequenceMatcher(None,input_name.lower(),m.lower()).ratio()*100,1)} for m in matches])

def data_quality_report(selected_date=None):
    param = {"d": selected_date} if using_postgres() and selected_date else (selected_date,) if selected_date else None
    where_m = "WHERE m.race_date = :d" if using_postgres() and selected_date else "WHERE m.race_date = ?" if selected_date else ""
    runners = read_sql(f"""SELECT m.race_date, m.course, r.race_time, r.race_name, ru.horse, ru.trainer, ru.jockey, ru.official_rating, ru.form, ru.non_runner
    FROM runners ru JOIN races r ON ru.race_id=r.id JOIN meetings m ON r.meeting_id=m.id {where_m}""", param)
    where = "WHERE race_date = :d" if using_postgres() and selected_date else "WHERE race_date = ?" if selected_date else ""
    odds = read_sql(f"SELECT * FROM odds {where}", param)
    results = read_sql(f"SELECT * FROM results {where}", param)
    issues=[]
    if runners.empty:
        return pd.DataFrame([{"severity":"High","issue":"No racecards loaded","count":1,"detail":"Import racecards first."}])
    missing_trainer = runners["trainer"].isna().sum() + runners["trainer"].astype(str).str.strip().eq("").sum()
    if missing_trainer: issues.append({"severity":"Low","issue":"Missing trainer data","count":int(missing_trainer),"detail":"Trainer form cannot be scored yet."})
    if odds.empty: issues.append({"severity":"Medium","issue":"No odds loaded","count":len(runners),"detail":"Value scoring and movement limited."})
    if results.empty: issues.append({"severity":"Low","issue":"No results loaded","count":1,"detail":"Selections cannot be settled."})
    if not issues: issues.append({"severity":"OK","issue":"No major data issues found","count":0,"detail":"Data looks usable."})
    return pd.DataFrame(issues)

def backtest_from_stored_results(load_runners_func, selected_date=None, weights=None):
    df = load_runners_func(selected_date)
    param = {"d": selected_date} if using_postgres() and selected_date else (selected_date,) if selected_date else None
    where = "WHERE race_date = :d" if using_postgres() and selected_date else "WHERE race_date = ?" if selected_date else ""
    results = read_sql(f"SELECT * FROM results {where}", param)
    if df.empty or results.empty: return pd.DataFrame(), pd.DataFrame()
    scored = score_runners(df, weights)
    top = scored.sort_values("model_win_probability", ascending=False).groupby("race_id").head(1).copy()
    ew = scored.sort_values(["each_way_score","model_place_probability"], ascending=False).groupby("race_id").head(1).copy()
    tests=[]
    for label,picks in [("Top Win Pick Per Race",top),("Top Each-Way Pick Per Race",ew)]:
        merged=picks.merge(results,on=["race_date","course","race_time","horse"],how="left",suffixes=("","_result"))
        merged["is_won"]=merged["finishing_position"].astype(str).eq("1")
        merged["is_placed"]=merged["finishing_position"].astype(str).isin(["1","2","3","4"])
        tests.append({"strategy":label,"picks":len(merged),"winners":int(merged["is_won"].sum()),"placed":int(merged["is_placed"].sum()),"win_strike_rate_%":round(merged["is_won"].mean()*100,2) if len(merged) else 0,"place_strike_rate_%":round(merged["is_placed"].mean()*100,2) if len(merged) else 0})
    return pd.DataFrame(tests), scored
