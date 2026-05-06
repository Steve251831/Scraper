from datetime import date
import pandas as pd
import streamlit as st

from db import connect, init_db
from scoring import score_runners
from data_import import import_racecards, import_odds, import_results
from analytics import odds_movement, auto_match_results_to_selections, performance_by_bet_type

st.set_page_config(page_title="UK Horse Racing Predictor", layout="wide")
init_db()

st.title("UK Horse Racing Predictor — Stage 3")
st.caption("Odds movement, results auto-matching, performance tracking and database exports.")

page = st.sidebar.radio(
    "Page",
    [
        "Dashboard", "Import Data", "Racecards", "Odds Movement", "Daily Picks",
        "Results Tracker", "Performance", "Export Database", "CSV Templates"
    ]
)

def load_runners(selected_date=None):
    movement = odds_movement(selected_date)
    if not movement.empty:
        movement_cols = movement[[
            "race_date", "course", "race_time", "horse", "opening_odds", "latest_odds",
            "odds_change_pct", "movement_flag", "latest_volume"
        ]]
    else:
        movement_cols = pd.DataFrame(columns=[
            "race_date", "course", "race_time", "horse", "opening_odds", "latest_odds",
            "odds_change_pct", "movement_flag", "latest_volume"
        ])

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
    df = pd.read_sql_query(sql, connect(), params=(selected_date, selected_date))
    if df.empty:
        return df

    df = df.merge(movement_cols, on=["race_date", "course", "race_time", "horse"], how="left")
    return df

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
    if row.get("movement_flag") == "Steamer":
        reasons.append("market steamer")
    if row.get("movement_flag") == "Drifter":
        reasons.append("market drifter")
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
    tables = ["runners", "races", "odds", "results", "selections"]
    return {t: pd.read_sql_query(f"SELECT COUNT(*) total FROM {t}", con).iloc[0, 0] for t in tables}

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
    ### Stage 3 workflow

    1. Import racecards.
    2. Import early odds.
    3. Import later odds.
    4. Check **Odds Movement**.
    5. Check **Daily Picks**.
    6. Log selections.
    7. Import results.
    8. Auto-match results to selections.
    9. Review **Performance**.
    """)

elif page == "Import Data":
    st.subheader("Import Data")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if st.button("Load sample racecards"):
            import_racecards("sample_data/racecards_sample.csv")
            st.success("Sample racecards loaded.")
    with c2:
        if st.button("Load sample early odds"):
            import_odds("sample_data/odds_sample_early.csv")
            st.success("Sample early odds loaded.")
    with c3:
        if st.button("Load sample later odds"):
            import_odds("sample_data/odds_sample_later.csv")
            st.success("Sample later odds loaded.")
    with c4:
        if st.button("Load sample results"):
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
        st.dataframe(score_runners(df), use_container_width=True)

elif page == "Odds Movement":
    selected_date = st.date_input("Odds date", value=date.today()).isoformat()
    movement = odds_movement(selected_date)
    if movement.empty:
        st.warning("No odds snapshots loaded for this date.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Steamers", int((movement["movement_flag"] == "Steamer").sum()))
        c2.metric("Drifters", int((movement["movement_flag"] == "Drifter").sum()))
        c3.metric("Odds snapshots", int(movement["snapshots"].sum()))

        st.dataframe(movement, use_container_width=True)

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
                "opening_odds", "latest_odds", "odds_change_pct", "movement_flag",
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
        st.dataframe(yankee[["course", "race_time", "horse", "best_odds", "model_win_probability", "movement_flag", "risk_rating", "bet_flag"]], use_container_width=True)
        if st.button("Log Win Yankee"):
            for _, row in yankee.iterrows():
                save_selection(row, "Win Yankee")
            st.success("Win Yankee logged.")

        st.subheader("Each-Way Yankee")
        st.dataframe(ew_yankee[["course", "race_time", "horse", "best_odds", "model_place_probability", "each_way_score", "movement_flag", "risk_rating", "bet_flag"]], use_container_width=True)
        if st.button("Log Each-Way Yankee"):
            for _, row in ew_yankee.iterrows():
                save_selection(row, "Each-Way Yankee")
            st.success("Each-Way Yankee logged.")

        st.subheader("Value Singles")
        if value_singles.empty:
            st.info("No clear value singles found using current odds/model.")
        else:
            st.dataframe(value_singles[["course", "race_time", "horse", "best_odds", "implied_probability", "model_win_probability", "value_score", "movement_flag", "risk_rating"]], use_container_width=True)

elif page == "Results Tracker":
    st.subheader("Selections Log")

    if st.button("Auto-match results to selections"):
        updated = auto_match_results_to_selections()
        st.success(f"Updated {updated} selection result rows.")

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

elif page == "Performance":
    st.subheader("Performance by Bet Type")
    perf = performance_by_bet_type()
    if perf.empty:
        st.warning("No selections logged yet.")
    else:
        st.dataframe(perf, use_container_width=True)

    st.subheader("Selection History")
    df = pd.read_sql_query("SELECT * FROM selections ORDER BY created_at DESC", connect())
    if not df.empty:
        st.dataframe(df, use_container_width=True)

elif page == "Export Database":
    st.subheader("Export Database Tables")
    con = connect()
    for table in ["meetings", "races", "runners", "odds", "results", "selections"]:
        df = pd.read_sql_query(f"SELECT * FROM {table}", con)
        st.markdown(f"### {table}")
        st.write(f"{len(df)} rows")
        st.download_button(
            label=f"Download {table}.csv",
            data=df.to_csv(index=False),
            file_name=f"{table}.csv",
            mime="text/csv"
        )
        with st.expander(f"Preview {table}"):
            st.dataframe(df.head(20), use_container_width=True)

elif page == "CSV Templates":
    st.subheader("CSV Templates")
    for name in ["racecard_template.csv", "odds_template.csv", "results_template.csv"]:
        st.markdown(f"### {name}")
        content = open(f"templates/{name}", "r", encoding="utf-8").read()
        st.code(content)
        st.download_button(
            label=f"Download {name}",
            data=content,
            file_name=name,
            mime="text/csv"
        )
