from datetime import datetime
from io import StringIO
import pandas as pd
import requests
from bs4 import BeautifulSoup

RACECARD_COLS = [
    "race_date","course","country","going","race_time","race_name","race_type","distance",
    "class","runners_count","ew_terms","horse","trainer","jockey","draw","age","sex",
    "weight","official_rating","form","course_winner","distance_winner","cd_winner",
    "days_since_run","headgear","non_runner","source"
]

ODDS_COLS = [
    "race_date","course","race_time","horse","bookmaker","decimal_odds","exchange_back",
    "exchange_lay","traded_volume","odds_time","source"
]

RESULTS_COLS = [
    "race_date","course","race_time","horse","finishing_position","sp","result_status","source"
]

def fetch_tables(url):
    headers = {
        "User-Agent": "Mozilla/5.0 racing personal research app"
    }
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    html = r.text

    tables = []
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        tables = []

    # Fallback: try bs4 simple table extraction
    if not tables:
        soup = BeautifulSoup(html, "lxml")
        for table in soup.find_all("table"):
            rows = []
            for tr in table.find_all("tr"):
                cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th","td"])]
                if cells:
                    rows.append(cells)
            if rows:
                tables.append(pd.DataFrame(rows[1:], columns=rows[0] if len(rows) > 1 else None))

    return tables, html[:1000]

def normalise_columns(df):
    out = df.copy()
    out.columns = [str(c).strip().lower().replace("\n", " ") for c in out.columns]
    return out

def find_col(df, candidates):
    cols = list(df.columns)
    for c in candidates:
        c = c.lower()
        for col in cols:
            if col == c or c in col:
                return col
    return None

def map_to_racecard(df, defaults):
    src = normalise_columns(df)
    out = pd.DataFrame(index=src.index)
    for c in RACECARD_COLS:
        out[c] = ""

    mapping = {
        "horse": ["horse", "runner", "selection", "name"],
        "trainer": ["trainer", "trn"],
        "jockey": ["jockey", "rider", "jky"],
        "draw": ["draw", "stall"],
        "age": ["age"],
        "sex": ["sex"],
        "weight": ["weight", "wgt", "wt"],
        "official_rating": ["or", "official rating", "rating"],
        "form": ["form"],
        "headgear": ["headgear"],
        "days_since_run": ["days since", "dsr"]
    }

    for target, candidates in mapping.items():
        col = find_col(src, candidates)
        if col:
            out[target] = src[col]

    if out["horse"].astype(str).str.strip().eq("").all() and len(src.columns):
        out["horse"] = src.iloc[:, 0]

    for k, v in defaults.items():
        if k in out.columns:
            out[k] = v

    for c in ["course_winner", "distance_winner", "cd_winner", "non_runner"]:
        out[c] = 0

    out["source"] = defaults.get("source", "daily_import")
    return out[RACECARD_COLS]

def map_to_odds(df, defaults):
    src = normalise_columns(df)
    out = pd.DataFrame(index=src.index)
    for c in ODDS_COLS:
        out[c] = ""

    mapping = {
        "horse": ["horse", "runner", "selection", "name"],
        "decimal_odds": ["odds", "price", "decimal"],
        "exchange_back": ["back"],
        "exchange_lay": ["lay"],
        "traded_volume": ["volume", "traded"]
    }

    for target, candidates in mapping.items():
        col = find_col(src, candidates)
        if col:
            out[target] = src[col]

    if out["horse"].astype(str).str.strip().eq("").all() and len(src.columns):
        out["horse"] = src.iloc[:, 0]

    for k, v in defaults.items():
        if k in out.columns:
            out[k] = v

    out["source"] = defaults.get("source", "daily_import")
    return out[ODDS_COLS]

def map_to_results(df, defaults):
    src = normalise_columns(df)
    out = pd.DataFrame(index=src.index)
    for c in RESULTS_COLS:
        out[c] = ""

    mapping = {
        "horse": ["horse", "runner", "selection", "name"],
        "finishing_position": ["position", "pos", "finish", "result"],
        "sp": ["sp", "starting price"],
        "result_status": ["status"]
    }

    for target, candidates in mapping.items():
        col = find_col(src, candidates)
        if col:
            out[target] = src[col]

    if out["horse"].astype(str).str.strip().eq("").all() and len(src.columns):
        out["horse"] = src.iloc[:, 0]

    for k, v in defaults.items():
        if k in out.columns:
            out[k] = v

    out["source"] = defaults.get("source", "daily_import")
    return out[RESULTS_COLS]
