from datetime import date
import pandas as pd
import numpy as np
import streamlit as st

from db import connect, init_db
from data_import import import_racecard_dataframe, import_odds_dataframe
from converter import convert_auto
from scoring import score_runners

st.set_page_config(page_title="UK Horse Racing Predictor", layout="wide")
init_db()

st.title("UK Horse Racing Predictor — Stage 8 CSV Converter")
st.caption("Upload Racing Post-style CSVs or Betfair mapping CSVs, convert, review and import.")

page = st.sidebar.radio("Page", [
    "Dashboard",
    "CSV Converter",
    "Racecards",
    "Daily Picks",
    "Export Database",
    "CSV Templates"
])

def counts():
    return {t: pd.read_sql_query(f"SELECT COUNT(*) total FROM {t}", connect()).iloc[0,0] for t in ["runners","races","odds","results","selections"]}

def load_runners(d=None):
    df = pd.read_sql_query("""
    SELECT m.race_date, m.course, m.country, m.going,
           r.id race_id, r.race_time, r.race_name, r.race_type, r.distance, r.class, r.runners_count, r.ew_terms,
           ru.id runner_id, ru.horse, ru.trainer, ru.jockey, ru.draw, ru.age, ru.sex, ru.weight,
           ru.official_rating, ru.form, ru.course_winner, ru.distance_winner, ru.cd_winner,
           ru.days_since_run, ru.headgear, ru.non_runner
    FROM runners ru
    JOIN races r ON ru.race_id = r.id
    JOIN meetings m ON r.meeting_id = m.id
    WHERE (? IS NULL OR m.race_date = ?)
    """, connect(), params=(d,d))

    if df.empty:
        return df

    odds = pd.read_sql_query("SELECT * FROM odds WHERE (? IS NULL OR race_date = ?)", connect(), params=(d,d))
    if not odds.empty:
        odds = odds.sort_values("imported_at").groupby(["race_date","course","race_time","horse"]).last().reset_index()
        df = df.merge(odds[["race_date","course","race_time","horse","decimal_odds","exchange_back","traded_volume"]], on=["race_date","course","race_time","horse"], how="left")
    else:
        df["decimal_odds"] = np.nan
        df["exchange_back"] = np.nan
        df["traded_volume"] = np.nan
    return df

if page == "Dashboard":
    st.subheader("Database Summary")
    c = counts()
    cols = st.columns(5)
    for i,t in enumerate(["runners","races","odds","results","selections"]):
        cols[i].metric(t.title(), int(c[t]))

    st.info("Use CSV Converter to import your uploaded Racing Post racecards and Betfair odds mapping files.")

elif page == "CSV Converter":
    st.subheader("CSV Converter")
    uploaded = st.file_uploader("Upload Racing Post racecard CSV or Betfair mapping CSV", type=["csv"])

    if uploaded:
        raw = pd.read_csv(uploaded)
        detected, converted = convert_auto(raw)

        st.write(f"Detected file type: **{detected}**")
        st.markdown("### Original file preview")
        st.dataframe(raw.head(20), use_container_width=True)

        st.markdown("### Converted app-format file")
        edited = st.data_editor(converted, use_container_width=True, num_rows="dynamic")

        st.download_button(
            "Download converted CSV",
            edited.to_csv(index=False),
            file_name="converted_for_predictor.csv",
            mime="text/csv"
        )

        if detected == "Racing Post Racecard CSV":
            if st.button("Import converted racecards"):
                rows = import_racecard_dataframe(edited)
                st.success(f"Imported {rows} racecard row(s).")
        elif detected == "Betfair Mapping Odds CSV":
            if st.button("Import converted Betfair odds"):
                rows = import_odds_dataframe(edited)
                st.success(f"Imported {rows} odds row(s).")
        else:
            st.warning("Unknown file type. It has not been converted. Send me the headers and I can add a mapping.")

elif page == "Racecards":
    d = st.date_input("Race date", value=date.today()).isoformat()
    df = load_runners(d)
    if df.empty:
        st.warning("No racecards loaded for this date.")
    else:
        st.dataframe(score_runners(df), use_container_width=True)

elif page == "Daily Picks":
    d = st.date_input("Selection date", value=date.today()).isoformat()
    df = load_runners(d)
    if df.empty:
        st.warning("No racecards loaded.")
    else:
        scored = score_runners(df)
        st.dataframe(scored, use_container_width=True)

        nap = scored.iloc[0]
        st.subheader("NAP")
        st.write(f"**{nap['horse']}** — {nap['course']} {nap['race_time']}")
        st.write(f"Win probability: {nap['model_win_probability']:.2%}")
        st.write(f"Best odds: {nap.get('best_odds')}")

        if st.button("Log NAP"):
            with connect() as con:
                con.execute("""
                    INSERT INTO selections
                    (selection_date, bet_type, meeting, race_time, horse, odds_taken,
                     model_win_probability, model_place_probability, implied_probability,
                     value_score, confidence_score, risk_rating, reasoning, stake)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    nap["race_date"], "NAP", nap["course"], nap["race_time"], nap["horse"],
                    nap.get("best_odds"), nap.get("model_win_probability"), nap.get("model_place_probability"),
                    nap.get("implied_probability"), nap.get("value_score"), nap.get("confidence_score"),
                    nap.get("risk_rating"), "Logged by model", 1
                ))
            st.success("Logged NAP.")

elif page == "Export Database":
    for t in ["meetings","races","runners","odds","results","selections"]:
        df = pd.read_sql_query(f"SELECT * FROM {t}", connect())
        st.markdown(f"### {t}")
        st.download_button(f"Download {t}.csv", df.to_csv(index=False), f"{t}.csv", "text/csv")
        st.dataframe(df.head(20), use_container_width=True)

elif page == "CSV Templates":
    for name in ["racecard_template.csv", "odds_template.csv", "results_template.csv"]:
        content = open(f"templates/{name}", "r", encoding="utf-8").read()
        st.markdown(f"### {name}")
        st.code(content)
        st.download_button(f"Download {name}", content, name, "text/csv")
