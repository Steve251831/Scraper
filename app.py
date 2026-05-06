from datetime import date
import pandas as pd
import numpy as np
import streamlit as st

from db import connect, init_db
from data_import import import_racecard_dataframe, import_odds_dataframe, import_results_dataframe, import_csv
from scoring import score_runners
from daily_scraper import fetch_tables, map_to_racecard, map_to_odds, map_to_results

st.set_page_config(page_title="UK Horse Racing Predictor", layout="wide")
init_db()

st.title("UK Horse Racing Predictor — Stage 7 Daily Import")
st.caption("Press-button daily table fetcher + review/import workflow.")

page = st.sidebar.radio("Page", [
    "Dashboard",
    "Source Manager",
    "Daily Auto Import",
    "Racecards",
    "Odds Snapshot Builder",
    "Daily Picks",
    "Export Database",
    "CSV Templates"
])

def counts():
    return {t: pd.read_sql_query(f"SELECT COUNT(*) total FROM {t}", connect()).iloc[0,0] for t in ["sources","runners","races","odds","results","selections"]}

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
        df = df.merge(odds[["race_date","course","race_time","horse","decimal_odds","exchange_back"]], on=["race_date","course","race_time","horse"], how="left")
    else:
        df["decimal_odds"] = np.nan
        df["exchange_back"] = np.nan
    return df

if page == "Dashboard":
    st.subheader("Database Summary")
    c = counts()
    cols = st.columns(6)
    for i,t in enumerate(["sources","runners","races","odds","results","selections"]):
        cols[i].metric(t.title(), int(c[t]))
    st.info("Set up source URLs once, then use Daily Auto Import each day.")

elif page == "Source Manager":
    st.subheader("Source Manager")
    st.write("Add public pages that contain normal HTML tables. Use `{date}` in the URL if the site uses dates.")

    with st.form("add_source"):
        source_type = st.selectbox("Source type", ["racecard", "odds", "results"])
        source_name = st.text_input("Source name", "My racing source")
        url_template = st.text_input("URL template", "https://example.com/racecards/{date}")
        submitted = st.form_submit_button("Add source")
        if submitted:
            with connect() as con:
                con.execute("INSERT INTO sources (source_type, source_name, url_template, enabled) VALUES (?, ?, ?, 1)", (source_type, source_name, url_template))
            st.success("Source added.")

    sources = pd.read_sql_query("SELECT * FROM sources ORDER BY id DESC", connect())
    if not sources.empty:
        edited = st.data_editor(sources, use_container_width=True, num_rows="dynamic")
        if st.button("Save source changes"):
            with connect() as con:
                con.execute("DELETE FROM sources")
                edited.to_sql("sources", con, if_exists="append", index=False)
            st.success("Saved.")

elif page == "Daily Auto Import":
    st.subheader("Daily Auto Import")
    work_date = st.date_input("Date", value=date.today()).isoformat()
    race_time = st.text_input("Default race time if missing", "14:00")
    course = st.text_input("Default course if missing", "")
    race_name = st.text_input("Default race name if missing", "")
    going = st.text_input("Default going", "")

    sources = pd.read_sql_query("SELECT * FROM sources WHERE enabled=1 ORDER BY id", connect())

    if sources.empty:
        st.warning("No enabled sources. Go to Source Manager first.")
    else:
        st.dataframe(sources, use_container_width=True)

        if st.button("Fetch Today’s Sources"):
            st.session_state.fetched_tables = []
            for _, src in sources.iterrows():
                url = str(src["url_template"]).replace("{date}", work_date)
                try:
                    tables, preview = fetch_tables(url)
                    for idx, table in enumerate(tables):
                        st.session_state.fetched_tables.append({
                            "source_id": src["id"],
                            "source_type": src["source_type"],
                            "source_name": src["source_name"],
                            "url": url,
                            "table_index": idx,
                            "table": table
                        })
                    st.success(f"{src['source_name']}: fetched {len(tables)} table(s).")
                except Exception as e:
                    st.error(f"{src['source_name']} failed: {e}")

    if "fetched_tables" in st.session_state and st.session_state.fetched_tables:
        st.subheader("Review fetched tables")
        labels = [
            f"{i}: {x['source_name']} | {x['source_type']} | table {x['table_index']} | {len(x['table'])} rows"
            for i, x in enumerate(st.session_state.fetched_tables)
        ]
        selected_label = st.selectbox("Select table", labels)
        selected_index = int(selected_label.split(":")[0])
        item = st.session_state.fetched_tables[selected_index]
        raw_table = item["table"]

        st.markdown("### Raw table")
        st.dataframe(raw_table.head(50), use_container_width=True)

        defaults = {
            "race_date": work_date,
            "course": course,
            "country": "UK",
            "going": going,
            "race_time": race_time,
            "race_name": race_name,
            "race_type": "",
            "distance": "",
            "class": "",
            "runners_count": len(raw_table),
            "ew_terms": "",
            "bookmaker": item["source_name"],
            "odds_time": "",
            "source": item["source_name"]
        }

        source_type = item["source_type"]
        if source_type == "racecard":
            mapped = map_to_racecard(raw_table, defaults)
        elif source_type == "odds":
            mapped = map_to_odds(raw_table, defaults)
        else:
            mapped = map_to_results(raw_table, defaults)

        st.markdown("### Auto-mapped table")
        edited = st.data_editor(mapped, use_container_width=True, num_rows="dynamic")
        st.download_button("Download mapped CSV", edited.to_csv(index=False), file_name=f"mapped_{source_type}.csv", mime="text/csv")

        if st.button(f"Import this {source_type} table"):
            if source_type == "racecard":
                rows = import_racecard_dataframe(edited)
            elif source_type == "odds":
                rows = import_odds_dataframe(edited)
            else:
                rows = import_results_dataframe(edited)
            st.success(f"Imported {rows} row(s).")

elif page == "Racecards":
    d = st.date_input("Race date", value=date.today()).isoformat()
    df = load_runners(d)
    if df.empty:
        st.warning("No racecards loaded.")
    else:
        st.dataframe(score_runners(df), use_container_width=True)

elif page == "Odds Snapshot Builder":
    d = st.date_input("Date", value=date.today()).isoformat()
    runners = load_runners(d)
    if runners.empty:
        st.warning("Load racecards first.")
    else:
        base = runners[["race_date","course","race_time","horse"]].drop_duplicates().copy()
        base["bookmaker"] = "Manual"
        base["decimal_odds"] = np.nan
        base["exchange_back"] = np.nan
        base["exchange_lay"] = np.nan
        base["traded_volume"] = np.nan
        base["odds_time"] = ""
        base["source"] = "manual_snapshot"
        edited = st.data_editor(base, use_container_width=True)
        if st.button("Import odds snapshot"):
            st.success(f"Imported {import_odds_dataframe(edited)} odds rows.")

elif page == "Daily Picks":
    d = st.date_input("Selection date", value=date.today()).isoformat()
    df = load_runners(d)
    if df.empty:
        st.warning("No racecards.")
    else:
        scored = score_runners(df)
        st.dataframe(scored, use_container_width=True)
        nap = scored.iloc[0]
        st.subheader("NAP")
        st.write(f"**{nap['horse']}** — {nap['course']} {nap['race_time']}")
        if st.button("Log NAP"):
            with connect() as con:
                con.execute("""
                    INSERT INTO selections
                    (selection_date, bet_type, meeting, race_time, horse, odds_taken, model_win_probability, model_place_probability,
                     implied_probability, value_score, confidence_score, risk_rating, reasoning, stake)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    nap["race_date"], "NAP", nap["course"], nap["race_time"], nap["horse"],
                    nap.get("best_odds"), nap.get("model_win_probability"), nap.get("model_place_probability"),
                    nap.get("implied_probability"), nap.get("value_score"), nap.get("confidence_score"),
                    nap.get("risk_rating"), "Logged by model", 1
                ))
            st.success("Logged NAP.")

elif page == "Export Database":
    for t in ["sources","meetings","races","runners","odds","results","selections"]:
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
