import pandas as pd
from db import connect, init_db

def safe_int(v):
    try:
        if v in ("", None):
            return None
        return int(float(v))
    except Exception:
        return None

def safe_float(v):
    try:
        if v in ("", None):
            return None
        return float(v)
    except Exception:
        return None

def import_racecards(path):
    init_db()
    df = pd.read_csv(path)
    required = {"race_date", "course", "race_time", "horse"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required CSV columns: {missing}")

    with connect() as con:
        for _, r in df.iterrows():
            row = {k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}

            con.execute("""
                INSERT OR IGNORE INTO meetings (race_date, course, country, going, source)
                VALUES (?, ?, ?, ?, ?)
            """, (str(row["race_date"]), str(row["course"]), row.get("country"), row.get("going"), row.get("source")))

            meeting_id = con.execute(
                "SELECT id FROM meetings WHERE race_date=? AND course=?",
                (str(row["race_date"]), str(row["course"]))
            ).fetchone()[0]

            race_name = row.get("race_name") or ""

            con.execute("""
                INSERT OR IGNORE INTO races
                (meeting_id, race_time, race_name, race_type, distance, class, runners_count, ew_terms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                meeting_id,
                str(row["race_time"]),
                race_name,
                row.get("race_type"),
                row.get("distance"),
                row.get("class"),
                safe_int(row.get("runners_count")),
                row.get("ew_terms"),
            ))

            race_id = con.execute("""
                SELECT id FROM races WHERE meeting_id=? AND race_time=? AND race_name=?
            """, (meeting_id, str(row["race_time"]), race_name)).fetchone()[0]

            con.execute("""
                INSERT OR IGNORE INTO runners
                (race_id, horse, trainer, jockey, draw, age, sex, weight, official_rating, form,
                 course_winner, distance_winner, cd_winner, days_since_run, headgear, non_runner)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                race_id,
                row.get("horse"),
                row.get("trainer"),
                row.get("jockey"),
                safe_int(row.get("draw")),
                safe_int(row.get("age")),
                row.get("sex"),
                row.get("weight"),
                safe_float(row.get("official_rating")),
                row.get("form"),
                safe_int(row.get("course_winner")) or 0,
                safe_int(row.get("distance_winner")) or 0,
                safe_int(row.get("cd_winner")) or 0,
                safe_int(row.get("days_since_run")),
                row.get("headgear"),
                safe_int(row.get("non_runner")) or 0,
            ))
