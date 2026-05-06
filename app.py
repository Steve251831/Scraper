from datetime import date
import pandas as pd
import streamlit as st

from db import connect, init_db
from scoring import score_runners
from data_import import import_racecards, import_odds, import_results

st.set_page_config(page_title="UK Horse Racing Predictor", layout="wide")
init_db()

st.title("UK Horse Racing Predictor — Stage 2")
st.caption("Import racecards, odds and results. Score NAPs, each-way picks, Yankees and value selections.")

page = st.sidebar.radio(
    "Page",
    ["Dashboard", "Import Data", "Racecards", "Daily Picks", "Results Tracker", "Profit/Loss", "CSV Templates"]
)

def load_runners(selected_date=None):
    sql = """
    WITH latest_odds AS (
        SELECT o.*
        FROM odds o
        INNER JOIN (
            SELECT race_date, course, race_time, horse, MAX(imported_at) AS max_imported
            FROM odds
            GROUP BY race_date, course, race_time, horse
        ) x
        ON o.race_date = x.race_date
        AND o.course = x.course
        AND o.race_time = x.race_time
        AND o.horse = x.horse
        AND o.imported_at = x.max_imported
    )
    SELECT
        m.race_date, m.course, m.country, m.going,
        r.id as race_id, r.race_time, r.race_name, r.race_type, r.distance, r.class, r.runners_count, r.ew_terms,
        ru.id as runner_id, ru.horse, ru.trainer, ru.jockey, ru.draw, ru.age, ru.sex, ru.weight,
        ru.official_rating, ru.form, ru.course_winner, ru.distance_winner, ru.cd_winner,
        ru.days_since_run, ru.headgear, ru.non_runner,
        lo.bookmaker, lo.decimal_odds, lo.exchange_back, lo.exchange_lay, lo.traded_volume, lo.odds_time
    FROM runners ru
    JOIN races r ON ru.race_id = r.id
    JOIN meetings m ON r.meeting_id = m.id
    LEFT JOIN latest_odds lo
        ON lo.race_date = m.race_date
        AND lo.course = m.course
        AND lo.race_time = r.race_time
        AND lo.horse = ru.horse
    WHERE (? IS NULL OR m.race_date = ?)
    """
    return pd.read_sql_query(sql, connect(), params=(selected_date, selected_date))

def build_reason(row):
    reasons = []
    if row.get("cd_winner") == 1:
        reasons.append("course-and-distance winner")
    elif row.get("course_winner") == 1:
        reasons.append("course winner")
    if row.get("distance_winner") == 1:
        reasons.append("distance winner")
    if pd.notna(row.get("official_rating")):
        reasons.append(f"official rating {row.get('official_rating')}")
    if row.get("form"):
        reasons.append(f"recent form {row.get('form')}")
    if pd.notna(row.get("best_odds")):
        reasons.append(f"available odds {row.get('best_odds')}")
    if pd.notna(row.get("value_score")) and row.get("value_score") > 0:
        reasons.append("model probability is above implied odds probability")
    if row.get("bet_flag") == "No Bet / Watch":
        reasons.append("watch only unless price improves")
    return "; ".join(reasons) if reasons else "Selected by current model score."

def save_selection(row, bet_type):
    with connect() as con:
        con.execute("""
            INSERT INTO selections
            (selection_date, bet_type, meeting, race_time, horse, odds_taken,
             model_win_probability, model_place_probability, implied_probability,
             value_score, confidence_score, risk_rating, reasoning)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["race_date"], bet_type, row["course"], row["race_time"], row["horse"],
            row.get("best_odds"), row.get("model_win_probability"), row.get("model_place_probability"),
            row.get("implied_probability"), row.get("value_score"), row.get("confidence_score"),
            row.get("risk_rating"), build_reason(row)
        ))

def database_counts():
    con = connect()
    return {
        "runners": pd.read_sql_query("SELECT COUNT(*) total FROM runners", con).iloc[0, 0],
        "races": pd.read_sql_query("SELECT COUNT(*) total FROM races", con).iloc[0, 0],
        "odds": pd.read_sql_query("SELECT COUNT(*) total FROM odds", con).iloc[0, 0],
        "results": pd.read_sql_query("SELECT COUNT(*) total FROM results", con).iloc[0, 0],
        "selections": pd.read_sql_query("SELECT COUNT(*) total FROM selections", con).iloc[0, 0],
    }

if page == "Dashboard":
    st.subheader("Database Summary")
    counts = database_counts()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Runners", int(counts["runners"]))
    c2.metric("Races", int(counts["races"]))
    c3.metric("Odds rows", int(counts["odds"]))
    c4.metric("Results", int(counts["results"]))
    c5.metric("Selections", int(counts["selections"]))

    st.markdown("""
    ### Stage 2 workflow

    1. Import racecards.
    2. Import odds if you have them.
    3. Check **Daily Picks**.
    4. Log selections.
    5. Import results.
    6. Update profit/loss in **Results Tracker**.
    """)

elif page == "Import Data":
    st.subheader("Import Data")

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Load sample racecard data"):
            import_racecards("sample_data/racecards_sample.csv")
            st.success("Sample racecards loaded.")
    with c2:
        if st.button("Load sample odds data"):
            import_odds("sample_data/odds_sample.csv")
            st.success("Sample odds loaded.")
    with c3:
        if st.button("Load sample results data"):
            import_results("sample_data/results_sample.csv")
            st.success("Sample results loaded.")

    st.divider()

    racecard_file = st.file_uploader("Upload racecards CSV", type=["csv"], key="racecard")
    if racecard_file:
        tmp = pd.read_csv(racecard_file)
        st.dataframe(tmp.head(), use_container_width=True)
        if st.button("Import uploaded racecards"):
            tmp.to_csv("_uploaded_racecards.csv", index=False)
            import_racecards("_uploaded_racecards.csv")
            st.success("Racecards imported.")

    odds_file = st.file_uploader("Upload odds CSV", type=["csv"], key="odds")
    if odds_file:
        tmp = pd.read_csv(odds_file)
        st.dataframe(tmp.head(), use_container_width=True)
        if st.button("Import uploaded odds"):
            tmp.to_csv("_uploaded_odds.csv", index=False)
            import_odds("_uploaded_odds.csv")
            st.success("Odds imported.")

    results_file = st.file_uploader("Upload results CSV", type=["csv"], key="results")
    if results_file:
        tmp = pd.read_csv(results_file)
        st.dataframe(tmp.head(), use_container_width=True)
        if st.button("Import uploaded results"):
            tmp.to_csv("_uploaded_results.csv", index=False)
            import_results("_uploaded_results.csv")
            st.success("Results imported.")

elif page == "Racecards":
    selected_date = st.date_input("Race date", value=date.today()).isoformat()
    df = load_runners(selected_date)
    if df.empty:
        st.warning("No racecards loaded for this date.")
    else:
        scored = score_runners(df)
        st.dataframe(scored, use_container_width=True)

elif page == "Daily Picks":
    selected_date = st.date_input("Selection date", value=date.today()).isoformat()
    df = load_runners(selected_date)

    if df.empty:
        st.warning("No racecards loaded for this date.")
    else:
        scored = score_runners(df)
        scored = scored[scored["non_runner"].fillna(0).astype(int) == 0]

        st.subheader("Top Runner Per Race")
        top_per_race = scored.sort_values("model_win_probability", ascending=False).groupby("race_id").head(1)
        st.dataframe(
            top_per_race[[
                "race_date", "course", "race_time", "race_name", "horse", "trainer", "jockey",
                "best_odds", "implied_probability", "model_win_probability", "model_place_probability",
                "value_score", "confidence_score", "risk_rating", "bet_flag"
            ]],
            use_container_width=True,
        )

        nap = scored.sort_values("model_win_probability", ascending=False).iloc[0]
        ew_nap = scored.sort_values(["each_way_score", "model_place_probability"], ascending=False).iloc[0]
        yankee = top_per_race.sort_values("model_win_probability", ascending=False).head(4)
        ew_yankee = scored.sort_values(["each_way_score", "model_place_probability"], ascending=False).drop_duplicates("race_id").head(4)
        value_singles = scored[scored["value_score"] > 0.02].sort_values("value_score", ascending=False).head(10)

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("NAP of the Day")
            st.write(f"**{nap['horse']}** — {nap['course']} {nap['race_time']}")
            st.write(build_reason(nap))
            if st.button("Log NAP"):
                save_selection(nap, "NAP")
                st.success("NAP logged.")

        with c2:
            st.subheader("Each-Way NAP")
            st.write(f"**{ew_nap['horse']}** — {ew_nap['course']} {ew_nap['race_time']}")
            st.write(build_reason(ew_nap))
            if st.button("Log Each-Way NAP"):
                save_selection(ew_nap, "Each-Way NAP")
                st.success("Each-Way NAP logged.")

        st.subheader("Win Yankee")
        st.dataframe(yankee[["course", "race_time", "horse", "best_odds", "model_win_probability", "risk_rating", "bet_flag"]], use_container_width=True)
        if st.button("Log Win Yankee"):
            for _, row in yankee.iterrows():
                save_selection(row, "Win Yankee")
            st.success("Win Yankee logged.")

        st.subheader("Each-Way Yankee")
        st.dataframe(ew_yankee[["course", "race_time", "horse", "best_odds", "model_place_probability", "each_way_score", "risk_rating", "bet_flag"]], use_container_width=True)
        if st.button("Log Each-Way Yankee"):
            for _, row in ew_yankee.iterrows():
                save_selection(row, "Each-Way Yankee")
            st.success("Each-Way Yankee logged.")

        st.subheader("Value Singles")
        if value_singles.empty:
            st.info("No clear value singles found using current odds/model.")
        else:
            st.dataframe(value_singles[["course", "race_time", "horse", "best_odds", "implied_probability", "model_win_probability", "value_score", "risk_rating"]], use_container_width=True)

elif page == "Results Tracker":
    st.subheader("Selections Log")
    df = pd.read_sql_query("SELECT * FROM selections ORDER BY created_at DESC", connect())

    if df.empty:
        st.warning("No selections logged yet.")
    else:
        edited = st.data_editor(df, use_container_width=True, num_rows="dynamic")
        if st.button("Save edited selections"):
            with connect() as con:
                con.execute("DELETE FROM selections")
                edited.to_sql("selections", con, if_exists="append", index=False)
            st.success("Saved.")

elif page == "Profit/Loss":
    df = pd.read_sql_query("SELECT * FROM selections ORDER BY created_at", connect())

    if df.empty:
        st.warning("No selections logged yet.")
    else:
        df["profit_loss"] = pd.to_numeric(df["profit_loss"], errors="coerce").fillna(0)
        df["stake"] = pd.to_numeric(df["stake"], errors="coerce").fillna(0)
        df["running_pl"] = df["profit_loss"].cumsum()

        c1, c2, c3 = st.columns(3)
        c1.metric("Total P/L", f"£{df['profit_loss'].sum():.2f}")
        c2.metric("Total Stake", f"£{df['stake'].sum():.2f}")
        c3.metric("ROI", f"{(df['profit_loss'].sum() / max(df['stake'].sum(), 1) * 100):.2f}%")

        st.line_chart(df.set_index("created_at")["running_pl"])
        st.dataframe(df, use_container_width=True)

elif page == "CSV Templates":
    st.subheader("CSV Templates")
    st.write("Use these headings when building real racecard, odds and results files.")

    st.markdown("### Racecard template")
    st.code(open("templates/racecard_template.csv", "r", encoding="utf-8").read())

    st.markdown("### Odds template")
    st.code(open("templates/odds_template.csv", "r", encoding="utf-8").read())

    st.markdown("### Results template")
    st.code(open("templates/results_template.csv", "r", encoding="utf-8").read())
