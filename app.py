from datetime import date
import pandas as pd
import streamlit as st

from db import connect, init_db
from scoring import score_runners
from data_import import import_racecards

st.set_page_config(page_title="UK Horse Racing Predictor", layout="wide")
init_db()

st.title("UK Horse Racing Predictor")
st.caption("Ultra-safe Streamlit version. Import racecards, score runners, log picks and track results.")

page = st.sidebar.radio(
    "Page",
    ["Dashboard", "Racecards", "Daily Picks", "Results Tracker", "Profit/Loss", "Import Data"]
)

def load_runners(selected_date=None):
    sql = """
    SELECT
        m.race_date, m.course, m.country, m.going,
        r.id as race_id, r.race_time, r.race_name, r.race_type, r.distance, r.class, r.ew_terms,
        ru.id as runner_id, ru.horse, ru.trainer, ru.jockey, ru.draw, ru.age, ru.sex, ru.weight,
        ru.official_rating, ru.form, ru.course_winner, ru.distance_winner, ru.cd_winner,
        ru.days_since_run, ru.headgear, ru.non_runner
    FROM runners ru
    JOIN races r ON ru.race_id = r.id
    JOIN meetings m ON r.meeting_id = m.id
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
    return "; ".join(reasons) if reasons else "Selected by current model score."

def save_selection(row, bet_type):
    with connect() as con:
        con.execute("""
            INSERT INTO selections
            (selection_date, bet_type, meeting, race_time, horse,
             model_win_probability, model_place_probability, value_score,
             confidence_score, risk_rating, reasoning)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["race_date"], bet_type, row["course"], row["race_time"], row["horse"],
            row.get("model_win_probability"), row.get("model_place_probability"),
            row.get("value_score"), row.get("confidence_score"), row.get("risk_rating"),
            build_reason(row)
        ))

if page == "Dashboard":
    st.subheader("Database Summary")
    con = connect()
    runners = pd.read_sql_query("SELECT COUNT(*) total FROM runners", con).iloc[0, 0]
    races = pd.read_sql_query("SELECT COUNT(*) total FROM races", con).iloc[0, 0]
    selections = pd.read_sql_query("SELECT COUNT(*) total FROM selections", con).iloc[0, 0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Runners stored", int(runners))
    c2.metric("Races stored", int(races))
    c3.metric("Selections logged", int(selections))

    st.info("Start on Import Data. Load sample data, then go to Daily Picks and choose 2026-05-06.")

elif page == "Racecards":
    selected_date = st.date_input("Race date", value=date.today()).isoformat()
    df = load_runners(selected_date)
    if df.empty:
        st.warning("No racecards loaded for this date.")
    else:
        st.dataframe(score_runners(df), use_container_width=True)

elif page == "Daily Picks":
    selected_date = st.date_input("Selection date", value=date.today()).isoformat()
    df = load_runners(selected_date)

    if df.empty:
        st.warning("No racecards loaded for this date.")
    else:
        scored = score_runners(df)
        scored = scored[scored["non_runner"].fillna(0).astype(int) == 0]

        top_per_race = scored.sort_values("model_win_probability", ascending=False).groupby("race_id").head(1)
        st.subheader("Top runner per race")
        st.dataframe(top_per_race, use_container_width=True)

        nap = scored.sort_values("model_win_probability", ascending=False).iloc[0]
        ew_nap = scored.sort_values(["each_way_score", "model_place_probability"], ascending=False).iloc[0]
        yankee = top_per_race.sort_values("model_win_probability", ascending=False).head(4)
        ew_yankee = scored.sort_values(["each_way_score", "model_place_probability"], ascending=False).drop_duplicates("race_id").head(4)

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
        st.dataframe(yankee[["course", "race_time", "horse", "model_win_probability", "risk_rating"]], use_container_width=True)

        st.subheader("Each-Way Yankee")
        st.dataframe(ew_yankee[["course", "race_time", "horse", "model_place_probability", "each_way_score", "risk_rating"]], use_container_width=True)

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

        st.metric("Total P/L", f"£{df['profit_loss'].sum():.2f}")
        st.metric("ROI", f"{(df['profit_loss'].sum() / max(df['stake'].sum(), 1) * 100):.2f}%")
        st.line_chart(df.set_index("created_at")["running_pl"])
        st.dataframe(df, use_container_width=True)

elif page == "Import Data":
    st.subheader("Import CSV Data")

    if st.button("Load sample racecard data"):
        import_racecards("sample_data/racecards_sample.csv")
        st.success("Sample racecard data loaded. Go to Daily Picks and choose 2026-05-06.")

    racecard_file = st.file_uploader("Upload racecards CSV", type=["csv"])
    if racecard_file:
        tmp = pd.read_csv(racecard_file)
        st.dataframe(tmp.head(), use_container_width=True)
        if st.button("Import uploaded racecards"):
            tmp.to_csv("_uploaded_racecards.csv", index=False)
            import_racecards("_uploaded_racecards.csv")
            st.success("Racecards imported.")
