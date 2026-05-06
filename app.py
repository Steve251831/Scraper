from datetime import date
import json
import pandas as pd
import streamlit as st

from db import connect, init_db
from scoring import score_runners, DEFAULT_WEIGHTS
from data_import import import_racecards, import_odds, import_results, import_racecard_dataframe, manual_add_runner
from analytics import odds_movement, auto_match_results_to_selections, performance_by_bet_type, horse_name_matches, backtest_from_stored_results
from paste_cleaner import parse_pasted_table, RACECARD_COLUMNS

st.set_page_config(page_title="UK Horse Racing Predictor", layout="wide")
init_db()

if "weights" not in st.session_state:
    st.session_state.weights = DEFAULT_WEIGHTS.copy()

st.title("UK Horse Racing Predictor — Stage 4")
st.caption("Paste cleaner, manual entry, adjustable scoring, horse-name matching and backtesting.")

page = st.sidebar.radio(
    "Page",
    [
        "Dashboard", "Import Data", "Paste Racecard Cleaner", "Manual Entry",
        "Racecards", "Odds Movement", "Daily Picks", "Scoring Settings",
        "Horse Matcher", "Results Tracker", "Performance", "Backtest",
        "Export Database", "CSV Templates"
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
        movement_cols = pd.DataFrame(columns=["race_date", "course", "race_time", "horse", "opening_odds", "latest_odds", "odds_change_pct", "movement_flag", "latest_volume"])

    sql = """
    WITH latest_odds AS (
        SELECT o.*
        FROM odds o
        INNER JOIN (
            SELECT race_date, course, race_time, horse, MAX(imported_at) AS max_imported
            FROM odds
            GROUP BY race_date, course, race_time, horse
        ) x
        ON o.race_date = x.race_date AND o.course = x.course
        AND o.race_time = x.race_time AND o.horse = x.horse
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
    return df.merge(movement_cols, on=["race_date", "course", "race_time", "horse"], how="left")

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
    ### Stage 4 workflow

    1. Use **Paste Racecard Cleaner** or **Manual Entry** to add real race data.
    2. Import odds snapshots.
    3. Check **Odds Movement**.
    4. Adjust **Scoring Settings**.
    5. Review **Daily Picks**.
    6. Log selections.
    7. Import results.
    8. Auto-match results.
    9. Use **Backtest** to check strategy performance.
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
            rows = import_racecards("_uploaded_racecards.csv")
            st.success(f"Racecards imported: {rows} rows.")

    odds_file = st.file_uploader("Upload odds CSV", type=["csv"], key="odds")
    if odds_file:
        tmp = pd.read_csv(odds_file)
        st.dataframe(tmp.head(), use_container_width=True)
        if st.button("Import uploaded odds"):
            tmp.to_csv("_uploaded_odds.csv", index=False)
            rows = import_odds("_uploaded_odds.csv")
            st.success(f"Odds imported: {rows} rows.")

    results_file = st.file_uploader("Upload results CSV", type=["csv"], key="results")
    if results_file:
        tmp = pd.read_csv(results_file)
        st.dataframe(tmp.head(), use_container_width=True)
        if st.button("Import uploaded results"):
            tmp.to_csv("_uploaded_results.csv", index=False)
            rows = import_results("_uploaded_results.csv")
            st.success(f"Results imported: {rows} rows.")

elif page == "Paste Racecard Cleaner":
    st.subheader("Paste Racecard Cleaner")
    st.write("Paste a copied table. The app will try to convert it into the racecard import format.")

    c1, c2, c3 = st.columns(3)
    with c1:
        race_date = st.date_input("Race date", value=date.today()).isoformat()
        course = st.text_input("Course", "Kempton")
        country = st.text_input("Country", "UK")
    with c2:
        going = st.text_input("Going", "")
        race_time = st.text_input("Race time", "18:00")
        race_name = st.text_input("Race name", "")
    with c3:
        race_type = st.text_input("Race type", "Flat")
        distance = st.text_input("Distance", "")
        race_class = st.text_input("Class", "")

    pasted = st.text_area("Paste racecard table here", height=240, placeholder="Paste columns like Horse, Trainer, Jockey, Draw, Age, Weight, OR, Form...")

    default_meta = {
        "race_date": race_date,
        "course": course,
        "country": country,
        "going": going,
        "race_time": race_time,
        "race_name": race_name,
        "race_type": race_type,
        "distance": distance,
        "class": race_class,
        "source": "pasted_table",
    }

    if st.button("Clean pasted table"):
        cleaned = parse_pasted_table(pasted, default_meta)
        st.session_state.cleaned_racecard = cleaned

    if "cleaned_racecard" in st.session_state:
        cleaned = st.session_state.cleaned_racecard
        edited = st.data_editor(cleaned, use_container_width=True, num_rows="dynamic")
        st.download_button("Download cleaned racecard CSV", edited.to_csv(index=False), file_name="cleaned_racecard.csv", mime="text/csv")
        if st.button("Import cleaned racecard"):
            rows = import_racecard_dataframe(edited)
            st.success(f"Imported {rows} runner rows.")

elif page == "Manual Entry":
    st.subheader("Manual Runner Entry")
    with st.form("manual_runner"):
        c1, c2, c3 = st.columns(3)
        with c1:
            race_date = st.date_input("Race date", value=date.today()).isoformat()
            course = st.text_input("Course")
            race_time = st.text_input("Race time", "14:00")
            race_name = st.text_input("Race name")
        with c2:
            horse = st.text_input("Horse")
            trainer = st.text_input("Trainer")
            jockey = st.text_input("Jockey")
            going = st.text_input("Going")
        with c3:
            draw = st.number_input("Draw", min_value=0, step=1)
            age = st.number_input("Age", min_value=0, step=1)
            official_rating = st.number_input("Official rating", min_value=0.0, step=1.0)
            form = st.text_input("Form")

        c4, c5, c6 = st.columns(3)
        with c4:
            distance = st.text_input("Distance")
            runners_count = st.number_input("Runners count", min_value=0, step=1)
            weight = st.text_input("Weight")
        with c5:
            course_winner = st.checkbox("Course winner")
            distance_winner = st.checkbox("Distance winner")
            cd_winner = st.checkbox("CD winner")
        with c6:
            days_since_run = st.number_input("Days since run", min_value=0, step=1)
            headgear = st.text_input("Headgear")
            non_runner = st.checkbox("Non-runner")

        submitted = st.form_submit_button("Add runner")
        if submitted:
            row = {
                "race_date": race_date, "course": course, "country": "UK", "going": going,
                "race_time": race_time, "race_name": race_name, "race_type": "", "distance": distance,
                "class": "", "runners_count": runners_count, "ew_terms": "", "horse": horse,
                "trainer": trainer, "jockey": jockey, "draw": draw, "age": age, "sex": "",
                "weight": weight, "official_rating": official_rating, "form": form,
                "course_winner": int(course_winner), "distance_winner": int(distance_winner),
                "cd_winner": int(cd_winner), "days_since_run": days_since_run,
                "headgear": headgear, "non_runner": int(non_runner), "source": "manual_entry"
            }
            rows = manual_add_runner(row)
            st.success(f"Added {rows} runner row.")

elif page == "Racecards":
    selected_date = st.date_input("Race date", value=date.today()).isoformat()
    df = load_runners(selected_date)
    if df.empty:
        st.warning("No racecards loaded for this date.")
    else:
        st.dataframe(score_runners(df, st.session_state.weights), use_container_width=True)

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
        scored = score_runners(df, st.session_state.weights)
        scored = scored[scored["non_runner"].fillna(0).astype(int) == 0]

        st.subheader("Top Runner Per Race")
        top_per_race = scored.sort_values("model_win_probability", ascending=False).groupby("race_id").head(1)
        show_cols = [c for c in ["race_date","course","race_time","race_name","horse","trainer","jockey","opening_odds","latest_odds","odds_change_pct","movement_flag","best_odds","implied_probability","model_win_probability","model_place_probability","value_score","confidence_score","risk_rating","bet_flag"] if c in top_per_race.columns]
        st.dataframe(top_per_race[show_cols], use_container_width=True)

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
        st.dataframe(yankee[[c for c in ["course","race_time","horse","best_odds","model_win_probability","movement_flag","risk_rating","bet_flag"] if c in yankee.columns]], use_container_width=True)
        if st.button("Log Win Yankee"):
            for _, row in yankee.iterrows():
                save_selection(row, "Win Yankee")
            st.success("Win Yankee logged.")

        st.subheader("Each-Way Yankee")
        st.dataframe(ew_yankee[[c for c in ["course","race_time","horse","best_odds","model_place_probability","each_way_score","movement_flag","risk_rating","bet_flag"] if c in ew_yankee.columns]], use_container_width=True)
        if st.button("Log Each-Way Yankee"):
            for _, row in ew_yankee.iterrows():
                save_selection(row, "Each-Way Yankee")
            st.success("Each-Way Yankee logged.")

        st.subheader("Value Singles")
        if value_singles.empty:
            st.info("No clear value singles found using current odds/model.")
        else:
            st.dataframe(value_singles[[c for c in ["course","race_time","horse","best_odds","implied_probability","model_win_probability","value_score","movement_flag","risk_rating"] if c in value_singles.columns]], use_container_width=True)

elif page == "Scoring Settings":
    st.subheader("Scoring Settings")
    st.write("Adjust the weights used by the scoring model. These settings apply during the current app session.")
    new_weights = {}
    for key, value in st.session_state.weights.items():
        new_weights[key] = st.slider(key.title(), min_value=-10, max_value=40, value=int(value), step=1)
    st.session_state.weights = new_weights
    st.json(st.session_state.weights)

elif page == "Horse Matcher":
    st.subheader("Horse Name Matcher")
    name = st.text_input("Type a horse name to find closest saved matches")
    if name:
        matches = horse_name_matches(name)
        if matches.empty:
            st.warning("No close matches found.")
        else:
            st.dataframe(matches, use_container_width=True)

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

elif page == "Backtest":
    st.subheader("Backtest Stored Race Results")
    selected_date = st.date_input("Backtest date", value=date.today()).isoformat()
    summary, scored = backtest_from_stored_results(load_runners, selected_date, st.session_state.weights)
    if summary.empty:
        st.warning("Need racecards and results for this date before backtesting.")
    else:
        st.dataframe(summary, use_container_width=True)
        with st.expander("Scored runner data"):
            st.dataframe(scored, use_container_width=True)

elif page == "Export Database":
    st.subheader("Export Database Tables")
    con = connect()
    for table in ["meetings", "races", "runners", "odds", "results", "selections"]:
        df = pd.read_sql_query(f"SELECT * FROM {table}", con)
        st.markdown(f"### {table}")
        st.write(f"{len(df)} rows")
        st.download_button(f"Download {table}.csv", df.to_csv(index=False), file_name=f"{table}.csv", mime="text/csv")
        with st.expander(f"Preview {table}"):
            st.dataframe(df.head(20), use_container_width=True)

elif page == "CSV Templates":
    st.subheader("CSV Templates")
    for name in ["racecard_template.csv", "odds_template.csv", "results_template.csv"]:
        st.markdown(f"### {name}")
        content = open(f"templates/{name}", "r", encoding="utf-8").read()
        st.code(content)
        st.download_button(f"Download {name}", content, file_name=name, mime="text/csv")
