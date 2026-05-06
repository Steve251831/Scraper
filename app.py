from datetime import date
import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import text

from db import get_connection, init_db, database_mode, using_postgres
from scoring import score_runners, DEFAULT_WEIGHTS
from data_import import import_racecards, import_odds, import_results, import_racecard_dataframe, import_odds_dataframe, manual_add_runner
from analytics import odds_movement, auto_match_results_to_selections, auto_settle_selections, performance_by_bet_type, horse_name_matches, backtest_from_stored_results, data_quality_report, read_sql
from paste_cleaner import parse_pasted_table

st.set_page_config(page_title="UK Horse Racing Predictor", layout="wide")
init_db()
if "weights" not in st.session_state: st.session_state.weights = DEFAULT_WEIGHTS.copy()

st.title("UK Horse Racing Predictor — Stage 6")
st.caption("Persistent database support: Supabase/PostgreSQL with SQLite fallback.")

page = st.sidebar.radio("Page", ["Daily Setup","Dashboard","Import Data","Paste Racecard Cleaner","Manual Entry","Racecards","Odds Snapshot Builder","Odds Movement","Data Quality","Daily Picks","Staking & Bankroll","Scoring Settings","Horse Matcher","Results Tracker","Performance","Backtest","Export Database","CSV Templates","Supabase Setup"])

def sql_param(selected_date):
    if using_postgres(): return {"d": selected_date}, ":d"
    return (selected_date,), "?"

def load_runners(selected_date=None):
    movement = odds_movement(selected_date)
    movement_cols = movement[["race_date","course","race_time","horse","opening_odds","latest_odds","odds_change_pct","movement_flag","latest_volume"]] if not movement.empty else pd.DataFrame(columns=["race_date","course","race_time","horse","opening_odds","latest_odds","odds_change_pct","movement_flag","latest_volume"])
    where = ""
    params = None
    if selected_date:
        params, marker = sql_param(selected_date)
        where = f"WHERE m.race_date = {marker}"
    sql = f"""
    SELECT m.race_date, m.course, m.country, m.going,
        r.id as race_id, r.race_time, r.race_name, r.race_type, r.distance, r.class, r.runners_count, r.ew_terms,
        ru.id as runner_id, ru.horse, ru.trainer, ru.jockey, ru.draw, ru.age, ru.sex, ru.weight,
        ru.official_rating, ru.form, ru.course_winner, ru.distance_winner, ru.cd_winner,
        ru.days_since_run, ru.headgear, ru.non_runner
    FROM runners ru JOIN races r ON ru.race_id=r.id JOIN meetings m ON r.meeting_id=m.id {where}
    """
    df = read_sql(sql, params)
    if df.empty: return df
    # latest odds
    where_o = ""
    if selected_date: where_o = f"WHERE race_date = {marker}"
    odds = read_sql(f"SELECT * FROM odds {where_o}", params)
    if not odds.empty:
        odds = odds.sort_values("imported_at").groupby(["race_date","course","race_time","horse"]).last().reset_index()
        df = df.merge(odds[["race_date","course","race_time","horse","bookmaker","decimal_odds","exchange_back","exchange_lay","traded_volume","odds_time"]], on=["race_date","course","race_time","horse"], how="left")
    else:
        for c in ["bookmaker","decimal_odds","exchange_back","exchange_lay","traded_volume","odds_time"]: df[c]=np.nan
    return df.merge(movement_cols, on=["race_date","course","race_time","horse"], how="left")

def build_reason(row):
    reasons=[]
    if row.get("cd_winner")==1: reasons.append("course-and-distance winner")
    elif row.get("course_winner")==1: reasons.append("course winner")
    if row.get("distance_winner")==1: reasons.append("distance winner")
    if pd.notna(row.get("official_rating")): reasons.append(f"official rating {row.get('official_rating')}")
    if row.get("form"): reasons.append(f"recent form {row.get('form')}")
    if row.get("movement_flag")=="Steamer": reasons.append("market steamer")
    if row.get("movement_flag")=="Drifter": reasons.append("market drifter")
    if pd.notna(row.get("best_odds")): reasons.append(f"available odds {row.get('best_odds')}")
    if pd.notna(row.get("value_score")) and row.get("value_score")>0: reasons.append("model probability is above implied odds probability")
    return "; ".join(reasons) if reasons else "Selected by current model score."

def exec_insert_selection(row, bet_type, stake):
    conn = get_connection()
    try:
        if using_postgres():
            with conn.begin():
                conn.execute(text("""INSERT INTO selections (selection_date, bet_type, meeting, race_time, horse, odds_taken, model_win_probability, model_place_probability, implied_probability, value_score, confidence_score, risk_rating, reasoning, stake)
                VALUES (:selection_date,:bet_type,:meeting,:race_time,:horse,:odds_taken,:model_win_probability,:model_place_probability,:implied_probability,:value_score,:confidence_score,:risk_rating,:reasoning,:stake)"""), {
                    "selection_date": row["race_date"], "bet_type": bet_type, "meeting": row["course"], "race_time": row["race_time"], "horse": row["horse"],
                    "odds_taken": row.get("best_odds"), "model_win_probability": row.get("model_win_probability"), "model_place_probability": row.get("model_place_probability"), "implied_probability": row.get("implied_probability"), "value_score": row.get("value_score"), "confidence_score": row.get("confidence_score"), "risk_rating": row.get("risk_rating"), "reasoning": build_reason(row), "stake": stake})
        else:
            conn.execute("""INSERT INTO selections (selection_date, bet_type, meeting, race_time, horse, odds_taken, model_win_probability, model_place_probability, implied_probability, value_score, confidence_score, risk_rating, reasoning, stake)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (row["race_date"], bet_type, row["course"], row["race_time"], row["horse"], row.get("best_odds"), row.get("model_win_probability"), row.get("model_place_probability"), row.get("implied_probability"), row.get("value_score"), row.get("confidence_score"), row.get("risk_rating"), build_reason(row), stake))
            conn.commit()
    finally:
        conn.close()

def counts():
    return {t: read_sql(f"SELECT COUNT(*) total FROM {t}").iloc[0,0] for t in ["runners","races","odds","results","selections"]}

def staking_suggestion(bankroll, risk_rating, confidence, method):
    bankroll=float(bankroll or 0); confidence=float(confidence or 0)
    if bankroll<=0: return 0.0
    if method=="Level Stakes": return 1.0
    if method=="1% Bank": return round(bankroll*0.01,2)
    if method=="2% Bank": return round(bankroll*0.02,2)
    base=0.02 if risk_rating=="Low" else 0.01 if risk_rating=="Medium" else 0.005
    if confidence>=30: base += 0.005
    return round(min(bankroll*base, bankroll*0.03),2)

if page == "Daily Setup":
    selected_date = st.date_input("Working date", value=date.today()).isoformat()
    c = counts()
    st.subheader("Daily Checklist")
    for item, ok in [("Racecards loaded", c["runners"]>0),("Odds snapshots loaded",c["odds"]>0),("Selections logged",c["selections"]>0),("Results loaded",c["results"]>0)]:
        st.write(("✅ " if ok else "⬜ ")+item)
    st.subheader("Data Quality Snapshot")
    st.dataframe(data_quality_report(selected_date), use_container_width=True)

elif page == "Dashboard":
    st.subheader("Database Summary")
    st.success(f"Database mode: {database_mode()}")
    c=counts(); cols=st.columns(5)
    for i,t in enumerate(["runners","races","odds","results","selections"]): cols[i].metric(t.title(), int(c[t]))

elif page == "Import Data":
    st.subheader("Import Data")
    c1,c2,c3,c4=st.columns(4)
    with c1:
        if st.button("Load sample racecards"): st.success(f"Loaded {import_racecards('sample_data/racecards_sample.csv')} rows.")
    with c2:
        if st.button("Load sample early odds"): st.success(f"Loaded {import_odds('sample_data/odds_sample_early.csv')} rows.")
    with c3:
        if st.button("Load sample later odds"): st.success(f"Loaded {import_odds('sample_data/odds_sample_later.csv')} rows.")
    with c4:
        if st.button("Load sample results"): st.success(f"Loaded {import_results('sample_data/results_sample.csv')} rows.")
    for label, func, key in [("racecards", import_racecards, "r"), ("odds", import_odds, "o"), ("results", import_results, "res")]:
        f=st.file_uploader(f"Upload {label} CSV", type=["csv"], key=key)
        if f:
            tmp=pd.read_csv(f); st.dataframe(tmp.head(), use_container_width=True); path=f"_uploaded_{label}.csv"; tmp.to_csv(path,index=False)
            if st.button(f"Import {label}"): st.success(f"Imported {func(path)} rows.")

elif page == "Paste Racecard Cleaner":
    st.subheader("Paste Racecard Cleaner")
    race_date=st.date_input("Race date", value=date.today()).isoformat(); course=st.text_input("Course","Kempton"); race_time=st.text_input("Race time","18:00")
    race_name=st.text_input("Race name",""); going=st.text_input("Going",""); distance=st.text_input("Distance",""); runners_count=st.number_input("Runners count",0,50,0)
    pasted=st.text_area("Paste racecard table",height=240)
    if st.button("Clean pasted table"):
        st.session_state.cleaned=parse_pasted_table(pasted, {"race_date":race_date,"course":course,"country":"UK","going":going,"race_time":race_time,"race_name":race_name,"race_type":"","distance":distance,"class":"","runners_count":runners_count,"source":"pasted_table"})
    if "cleaned" in st.session_state:
        edited=st.data_editor(st.session_state.cleaned,use_container_width=True,num_rows="dynamic")
        st.download_button("Download cleaned CSV", edited.to_csv(index=False), "cleaned_racecard.csv", "text/csv")
        if st.button("Import cleaned racecard"): st.success(f"Imported {import_racecard_dataframe(edited)} rows.")

elif page == "Manual Entry":
    st.subheader("Manual Runner Entry")
    with st.form("manual"):
        race_date=st.date_input("Race date", value=date.today()).isoformat(); course=st.text_input("Course"); race_time=st.text_input("Race time","14:00"); horse=st.text_input("Horse")
        trainer=st.text_input("Trainer"); jockey=st.text_input("Jockey"); official_rating=st.number_input("Official rating",0.0,200.0,0.0); form=st.text_input("Form")
        submitted=st.form_submit_button("Add runner")
        if submitted:
            row={"race_date":race_date,"course":course,"country":"UK","going":"","race_time":race_time,"race_name":"","race_type":"","distance":"","class":"","runners_count":0,"ew_terms":"","horse":horse,"trainer":trainer,"jockey":jockey,"draw":0,"age":0,"sex":"","weight":"","official_rating":official_rating,"form":form,"course_winner":0,"distance_winner":0,"cd_winner":0,"days_since_run":0,"headgear":"","non_runner":0,"source":"manual"}
            st.success(f"Added {manual_add_runner(row)} runner.")

elif page == "Racecards":
    d=st.date_input("Race date", value=date.today()).isoformat(); df=load_runners(d)
    st.dataframe(score_runners(df, st.session_state.weights) if not df.empty else df, use_container_width=True)

elif page == "Odds Snapshot Builder":
    d=st.date_input("Odds date", value=date.today()).isoformat(); runners=load_runners(d)
    if runners.empty: st.warning("Load racecards first.")
    else:
        base=runners[["race_date","course","race_time","horse"]].drop_duplicates().copy()
        for c,v in [("bookmaker","Manual"),("decimal_odds",np.nan),("exchange_back",np.nan),("exchange_lay",np.nan),("traded_volume",np.nan),("odds_time",""),("source","manual_snapshot")]: base[c]=v
        edited=st.data_editor(base,use_container_width=True,num_rows="fixed")
        if st.button("Import odds snapshot"): st.success(f"Imported {import_odds_dataframe(edited)} rows.")

elif page == "Odds Movement":
    d=st.date_input("Odds date", value=date.today()).isoformat(); m=odds_movement(d)
    if m.empty: st.warning("No odds snapshots.")
    else: st.dataframe(m,use_container_width=True)

elif page == "Data Quality":
    d=st.date_input("Quality date", value=date.today()).isoformat()
    st.dataframe(data_quality_report(d),use_container_width=True)

elif page == "Daily Picks":
    d=st.date_input("Selection date", value=date.today()).isoformat(); stake=st.number_input("Stake per logged selection",0.0,10000.0,1.0)
    df=load_runners(d)
    if df.empty: st.warning("No racecards loaded.")
    else:
        scored=score_runners(df, st.session_state.weights); scored=scored[scored["non_runner"].fillna(0).astype(int)==0]
        top=scored.sort_values("model_win_probability",ascending=False).groupby("race_id").head(1)
        st.subheader("Top Runner Per Race"); st.dataframe(top,use_container_width=True)
        nap=scored.sort_values("model_win_probability",ascending=False).iloc[0]; ew=scored.sort_values(["each_way_score","model_place_probability"],ascending=False).iloc[0]
        c1,c2=st.columns(2)
        with c1:
            st.subheader("NAP"); st.write(f"**{nap['horse']}** — {nap['course']} {nap['race_time']}"); st.write(build_reason(nap))
            if st.button("Log NAP"): exec_insert_selection(nap,"NAP",stake); st.success("Logged.")
        with c2:
            st.subheader("Each-Way NAP"); st.write(f"**{ew['horse']}** — {ew['course']} {ew['race_time']}"); st.write(build_reason(ew))
            if st.button("Log Each-Way NAP"): exec_insert_selection(ew,"Each-Way NAP",stake); st.success("Logged.")
        st.subheader("Value Singles"); st.dataframe(scored[scored["value_score"]>0.02].head(10),use_container_width=True)

elif page == "Staking & Bankroll":
    bankroll=st.number_input("Bankroll",0.0,1000000.0,100.0); method=st.selectbox("Method",["Level Stakes","1% Bank","2% Bank","Kelly-lite"])
    d=st.date_input("Date", value=date.today()).isoformat(); df=load_runners(d)
    if not df.empty:
        scored=score_runners(df,st.session_state.weights).head(20); scored["suggested_stake"]=scored.apply(lambda r: staking_suggestion(bankroll,r.get("risk_rating"),r.get("confidence_score"),method),axis=1); st.dataframe(scored,use_container_width=True)

elif page == "Scoring Settings":
    st.subheader("Scoring Settings")
    for k,v in st.session_state.weights.items(): st.session_state.weights[k]=st.slider(k.title(),-10,40,int(v))
    st.json(st.session_state.weights)

elif page == "Horse Matcher":
    name=st.text_input("Horse name")
    if name: st.dataframe(horse_name_matches(name),use_container_width=True)

elif page == "Results Tracker":
    c1,c2=st.columns(2)
    with c1:
        if st.button("Auto-match results"): st.success(f"Updated {auto_match_results_to_selections()} rows.")
    with c2:
        frac=st.selectbox("Each-way place fraction",[0.2,0.25],format_func=lambda x:"1/5" if x==0.2 else "1/4")
        if st.button("Auto-settle returns/P&L"): st.success(f"Settled {auto_settle_selections(frac)} rows.")
    df=read_sql("SELECT * FROM selections ORDER BY created_at DESC")
    if not df.empty: st.dataframe(df,use_container_width=True)
    else: st.warning("No selections.")

elif page == "Performance":
    st.dataframe(performance_by_bet_type(),use_container_width=True)

elif page == "Backtest":
    d=st.date_input("Backtest date", value=date.today()).isoformat()
    summary, scored = backtest_from_stored_results(load_runners,d,st.session_state.weights)
    st.dataframe(summary,use_container_width=True)
    with st.expander("Scored data"): st.dataframe(scored,use_container_width=True)

elif page == "Export Database":
    for t in ["meetings","races","runners","odds","results","selections"]:
        df=read_sql(f"SELECT * FROM {t}"); st.markdown(f"### {t}"); st.download_button(f"Download {t}.csv", df.to_csv(index=False), f"{t}.csv","text/csv"); st.dataframe(df.head(20),use_container_width=True)

elif page == "CSV Templates":
    for name in ["racecard_template.csv","odds_template.csv","results_template.csv"]:
        content=open(f"templates/{name}","r",encoding="utf-8").read(); st.markdown(f"### {name}"); st.code(content); st.download_button(f"Download {name}",content,name,"text/csv")

elif page == "Supabase Setup":
    st.subheader("Supabase/PostgreSQL Setup")
    st.write(f"Current database mode: **{database_mode()}**")
    st.markdown("""
    1. Create a Supabase project.
    2. Copy the PostgreSQL connection string.
    3. Add it to Streamlit Cloud secrets as `DATABASE_URL`.
    4. Reboot the app.
    5. Check Dashboard for `Supabase/PostgreSQL` mode.
    """)
    st.code('DATABASE_URL = "postgresql://postgres.xxxxxx:YOUR_PASSWORD@aws-0-eu-west-2.pooler.supabase.com:6543/postgres"', language="toml")
