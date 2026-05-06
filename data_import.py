import pandas as pd
from sqlalchemy import text
from db import get_connection, using_postgres, init_db

def safe_int(v):
    try:
        if v in ("", None): return None
        return int(float(v))
    except Exception:
        return None

def safe_float(v):
    try:
        if v in ("", None): return None
        return float(v)
    except Exception:
        return None

def clean_row(row):
    return {k: (None if pd.isna(v) else v) for k, v in row.items()}

def exec_stmt(conn, sql, params):
    if using_postgres():
        return conn.execute(text(sql), params)
    return conn.execute(sql, tuple(params.values()))

def scalar(conn, sql, params):
    if using_postgres():
        return conn.execute(text(sql), params).fetchone()[0]
    return conn.execute(sql, tuple(params.values())).fetchone()[0]

def import_racecards(path):
    init_db()
    return import_racecard_dataframe(pd.read_csv(path))

def import_racecard_dataframe(df):
    required = {"race_date", "course", "race_time", "horse"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required racecard CSV columns: {missing}")
    rows_added = 0
    conn = get_connection()
    try:
        trans = conn.begin() if using_postgres() else None
        for _, r in df.iterrows():
            row = clean_row(r.to_dict())
            p = {"race_date": str(row["race_date"]), "course": str(row["course"]), "country": row.get("country"), "going": row.get("going"), "source": row.get("source")}
            exec_stmt(conn, "INSERT OR IGNORE INTO meetings (race_date, course, country, going, source) VALUES (?, ?, ?, ?, ?)" if not using_postgres() else "INSERT INTO meetings (race_date, course, country, going, source) VALUES (:race_date, :course, :country, :going, :source) ON CONFLICT (race_date, course) DO NOTHING", p)
            meeting_id = scalar(conn, "SELECT id FROM meetings WHERE race_date=? AND course=?" if not using_postgres() else "SELECT id FROM meetings WHERE race_date=:race_date AND course=:course", {"race_date": p["race_date"], "course": p["course"]})
            race_name = row.get("race_name") or ""
            rp = {"meeting_id": meeting_id, "race_time": str(row["race_time"]), "race_name": race_name, "race_type": row.get("race_type"), "distance": row.get("distance"), "class": row.get("class"), "runners_count": safe_int(row.get("runners_count")), "ew_terms": row.get("ew_terms")}
            exec_stmt(conn, "INSERT OR IGNORE INTO races (meeting_id, race_time, race_name, race_type, distance, class, runners_count, ew_terms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" if not using_postgres() else "INSERT INTO races (meeting_id, race_time, race_name, race_type, distance, class, runners_count, ew_terms) VALUES (:meeting_id, :race_time, :race_name, :race_type, :distance, :class, :runners_count, :ew_terms) ON CONFLICT (meeting_id, race_time, race_name) DO NOTHING", rp)
            race_id = scalar(conn, "SELECT id FROM races WHERE meeting_id=? AND race_time=? AND race_name=?" if not using_postgres() else "SELECT id FROM races WHERE meeting_id=:meeting_id AND race_time=:race_time AND race_name=:race_name", {"meeting_id": meeting_id, "race_time": rp["race_time"], "race_name": race_name})
            runp = {"race_id": race_id, "horse": row.get("horse"), "trainer": row.get("trainer"), "jockey": row.get("jockey"), "draw": safe_int(row.get("draw")), "age": safe_int(row.get("age")), "sex": row.get("sex"), "weight": row.get("weight"), "official_rating": safe_float(row.get("official_rating")), "form": row.get("form"), "course_winner": safe_int(row.get("course_winner")) or 0, "distance_winner": safe_int(row.get("distance_winner")) or 0, "cd_winner": safe_int(row.get("cd_winner")) or 0, "days_since_run": safe_int(row.get("days_since_run")), "headgear": row.get("headgear"), "non_runner": safe_int(row.get("non_runner")) or 0}
            exec_stmt(conn, "INSERT OR IGNORE INTO runners (race_id, horse, trainer, jockey, draw, age, sex, weight, official_rating, form, course_winner, distance_winner, cd_winner, days_since_run, headgear, non_runner) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" if not using_postgres() else "INSERT INTO runners (race_id, horse, trainer, jockey, draw, age, sex, weight, official_rating, form, course_winner, distance_winner, cd_winner, days_since_run, headgear, non_runner) VALUES (:race_id, :horse, :trainer, :jockey, :draw, :age, :sex, :weight, :official_rating, :form, :course_winner, :distance_winner, :cd_winner, :days_since_run, :headgear, :non_runner) ON CONFLICT (race_id, horse) DO NOTHING", runp)
            rows_added += 1
        if using_postgres(): trans.commit()
        else: conn.commit()
    except Exception:
        if using_postgres() and trans: trans.rollback()
        raise
    finally:
        conn.close()
    return rows_added

def import_odds(path):
    init_db()
    return import_odds_dataframe(pd.read_csv(path))

def import_odds_dataframe(df):
    required = {"race_date", "course", "race_time", "horse"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required odds CSV columns: {missing}")
    rows_added = 0
    conn = get_connection()
    try:
        trans = conn.begin() if using_postgres() else None
        for _, r in df.iterrows():
            row = clean_row(r.to_dict())
            p = {"race_date": str(row["race_date"]), "course": str(row["course"]), "race_time": str(row["race_time"]), "horse": str(row["horse"]), "bookmaker": row.get("bookmaker"), "decimal_odds": safe_float(row.get("decimal_odds")), "exchange_back": safe_float(row.get("exchange_back")), "exchange_lay": safe_float(row.get("exchange_lay")), "traded_volume": safe_float(row.get("traded_volume")), "odds_time": row.get("odds_time"), "source": row.get("source")}
            exec_stmt(conn, "INSERT INTO odds (race_date, course, race_time, horse, bookmaker, decimal_odds, exchange_back, exchange_lay, traded_volume, odds_time, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" if not using_postgres() else "INSERT INTO odds (race_date, course, race_time, horse, bookmaker, decimal_odds, exchange_back, exchange_lay, traded_volume, odds_time, source) VALUES (:race_date, :course, :race_time, :horse, :bookmaker, :decimal_odds, :exchange_back, :exchange_lay, :traded_volume, :odds_time, :source)", p)
            rows_added += 1
        if using_postgres(): trans.commit()
        else: conn.commit()
    except Exception:
        if using_postgres() and trans: trans.rollback()
        raise
    finally:
        conn.close()
    return rows_added

def import_results(path):
    init_db()
    return import_results_dataframe(pd.read_csv(path))

def import_results_dataframe(df):
    required = {"race_date", "course", "race_time", "horse", "finishing_position"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required results CSV columns: {missing}")
    rows_added = 0
    conn = get_connection()
    try:
        trans = conn.begin() if using_postgres() else None
        for _, r in df.iterrows():
            row = clean_row(r.to_dict())
            p = {"race_date": str(row["race_date"]), "course": str(row["course"]), "race_time": str(row["race_time"]), "horse": str(row["horse"]), "finishing_position": str(row["finishing_position"]), "sp": safe_float(row.get("sp")), "result_status": row.get("result_status"), "source": row.get("source")}
            exec_stmt(conn, "INSERT OR REPLACE INTO results (race_date, course, race_time, horse, finishing_position, sp, result_status, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" if not using_postgres() else "INSERT INTO results (race_date, course, race_time, horse, finishing_position, sp, result_status, source) VALUES (:race_date, :course, :race_time, :horse, :finishing_position, :sp, :result_status, :source) ON CONFLICT (race_date, course, race_time, horse) DO UPDATE SET finishing_position=EXCLUDED.finishing_position, sp=EXCLUDED.sp, result_status=EXCLUDED.result_status, source=EXCLUDED.source", p)
            rows_added += 1
        if using_postgres(): trans.commit()
        else: conn.commit()
    except Exception:
        if using_postgres() and trans: trans.rollback()
        raise
    finally:
        conn.close()
    return rows_added

def manual_add_runner(row):
    return import_racecard_dataframe(pd.DataFrame([row]))
