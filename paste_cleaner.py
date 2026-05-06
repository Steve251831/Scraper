from io import StringIO
import pandas as pd

RACECARD_COLUMNS = [
    "race_date", "course", "country", "going", "race_time", "race_name", "race_type",
    "distance", "class", "runners_count", "ew_terms", "horse", "trainer", "jockey",
    "draw", "age", "sex", "weight", "official_rating", "form", "course_winner",
    "distance_winner", "cd_winner", "days_since_run", "headgear", "non_runner", "source"
]

COLUMN_GUESSES = {
    "horse": ["horse", "runner", "selection", "name"],
    "trainer": ["trainer", "trn"],
    "jockey": ["jockey", "jky", "rider"],
    "draw": ["draw", "stall", "stall no", "stall_no"],
    "age": ["age"],
    "weight": ["weight", "wgt", "wt"],
    "official_rating": ["or", "official rating", "official_rating", "rating"],
    "form": ["form", "recent form"],
}

def parse_pasted_table(text, default_meta):
    if not text or not text.strip():
        return pd.DataFrame(columns=RACECARD_COLUMNS)

    try:
        df = pd.read_csv(StringIO(text), sep="\t")
        if df.shape[1] <= 1:
            raise ValueError
    except Exception:
        try:
            df = pd.read_csv(StringIO(text))
        except Exception:
            lines = [line.split() for line in text.splitlines() if line.strip()]
            df = pd.DataFrame(lines)

    df.columns = [str(c).strip().lower() for c in df.columns]
    out = pd.DataFrame()
    for target in RACECARD_COLUMNS:
        out[target] = ""

    for target, guesses in COLUMN_GUESSES.items():
        for g in guesses:
            if g in df.columns:
                out[target] = df[g]
                break

    if out["horse"].astype(str).str.strip().eq("").all() and len(df.columns) > 0:
        out["horse"] = df.iloc[:, 0]

    for key, value in default_meta.items():
        if key in out.columns:
            out[key] = value

    for c in ["course_winner", "distance_winner", "cd_winner", "non_runner"]:
        out[c] = out[c].replace("", 0).fillna(0)

    out["source"] = out["source"].replace("", "pasted_table")
    return out[RACECARD_COLUMNS]
