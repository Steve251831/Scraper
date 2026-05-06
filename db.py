import os
import sqlite3
import streamlit as st
from pathlib import Path
from sqlalchemy import create_engine, text

DB_PATH = Path("racing.db")

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS meetings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_date TEXT NOT NULL,
    course TEXT NOT NULL,
    country TEXT,
    going TEXT,
    source TEXT,
    UNIQUE(race_date, course)
);

CREATE TABLE IF NOT EXISTS races (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    meeting_id INTEGER NOT NULL,
    race_time TEXT NOT NULL,
    race_name TEXT,
    race_type TEXT,
    distance TEXT,
    class TEXT,
    runners_count INTEGER,
    ew_terms TEXT,
    UNIQUE(meeting_id, race_time, race_name)
);

CREATE TABLE IF NOT EXISTS runners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id INTEGER NOT NULL,
    horse TEXT NOT NULL,
    trainer TEXT,
    jockey TEXT,
    draw INTEGER,
    age INTEGER,
    sex TEXT,
    weight TEXT,
    official_rating REAL,
    form TEXT,
    course_winner INTEGER DEFAULT 0,
    distance_winner INTEGER DEFAULT 0,
    cd_winner INTEGER DEFAULT 0,
    days_since_run INTEGER,
    headgear TEXT,
    non_runner INTEGER DEFAULT 0,
    UNIQUE(race_id, horse)
);

CREATE TABLE IF NOT EXISTS odds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_date TEXT NOT NULL,
    course TEXT NOT NULL,
    race_time TEXT NOT NULL,
    horse TEXT NOT NULL,
    bookmaker TEXT,
    decimal_odds REAL,
    exchange_back REAL,
    exchange_lay REAL,
    traded_volume REAL,
    odds_time TEXT,
    source TEXT,
    imported_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_date TEXT NOT NULL,
    course TEXT NOT NULL,
    race_time TEXT NOT NULL,
    horse TEXT NOT NULL,
    finishing_position TEXT,
    sp REAL,
    result_status TEXT,
    source TEXT,
    imported_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(race_date, course, race_time, horse)
);

CREATE TABLE IF NOT EXISTS selections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    selection_date TEXT NOT NULL,
    bet_type TEXT NOT NULL,
    meeting TEXT,
    race_time TEXT,
    horse TEXT NOT NULL,
    odds_taken REAL,
    model_win_probability REAL,
    model_place_probability REAL,
    implied_probability REAL,
    value_score REAL,
    confidence_score REAL,
    risk_rating TEXT,
    reasoning TEXT,
    result_position TEXT,
    result_status TEXT,
    stake REAL DEFAULT 1,
    return_amount REAL DEFAULT 0,
    profit_loss REAL DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS meetings (
    id SERIAL PRIMARY KEY,
    race_date TEXT NOT NULL,
    course TEXT NOT NULL,
    country TEXT,
    going TEXT,
    source TEXT,
    UNIQUE(race_date, course)
);

CREATE TABLE IF NOT EXISTS races (
    id SERIAL PRIMARY KEY,
    meeting_id INTEGER NOT NULL REFERENCES meetings(id),
    race_time TEXT NOT NULL,
    race_name TEXT,
    race_type TEXT,
    distance TEXT,
    class TEXT,
    runners_count INTEGER,
    ew_terms TEXT,
    UNIQUE(meeting_id, race_time, race_name)
);

CREATE TABLE IF NOT EXISTS runners (
    id SERIAL PRIMARY KEY,
    race_id INTEGER NOT NULL REFERENCES races(id),
    horse TEXT NOT NULL,
    trainer TEXT,
    jockey TEXT,
    draw INTEGER,
    age INTEGER,
    sex TEXT,
    weight TEXT,
    official_rating REAL,
    form TEXT,
    course_winner INTEGER DEFAULT 0,
    distance_winner INTEGER DEFAULT 0,
    cd_winner INTEGER DEFAULT 0,
    days_since_run INTEGER,
    headgear TEXT,
    non_runner INTEGER DEFAULT 0,
    UNIQUE(race_id, horse)
);

CREATE TABLE IF NOT EXISTS odds (
    id SERIAL PRIMARY KEY,
    race_date TEXT NOT NULL,
    course TEXT NOT NULL,
    race_time TEXT NOT NULL,
    horse TEXT NOT NULL,
    bookmaker TEXT,
    decimal_odds REAL,
    exchange_back REAL,
    exchange_lay REAL,
    traded_volume REAL,
    odds_time TEXT,
    source TEXT,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS results (
    id SERIAL PRIMARY KEY,
    race_date TEXT NOT NULL,
    course TEXT NOT NULL,
    race_time TEXT NOT NULL,
    horse TEXT NOT NULL,
    finishing_position TEXT,
    sp REAL,
    result_status TEXT,
    source TEXT,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(race_date, course, race_time, horse)
);

CREATE TABLE IF NOT EXISTS selections (
    id SERIAL PRIMARY KEY,
    selection_date TEXT NOT NULL,
    bet_type TEXT NOT NULL,
    meeting TEXT,
    race_time TEXT,
    horse TEXT NOT NULL,
    odds_taken REAL,
    model_win_probability REAL,
    model_place_probability REAL,
    implied_probability REAL,
    value_score REAL,
    confidence_score REAL,
    risk_rating TEXT,
    reasoning TEXT,
    result_position TEXT,
    result_status TEXT,
    stake REAL DEFAULT 1,
    return_amount REAL DEFAULT 0,
    profit_loss REAL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def get_database_url():
    try:
        if "DATABASE_URL" in st.secrets:
            return st.secrets["DATABASE_URL"]
    except Exception:
        pass
    return os.getenv("DATABASE_URL")

def using_postgres():
    url = get_database_url()
    return bool(url and url.startswith(("postgresql://", "postgres://")))

@st.cache_resource
def get_engine():
    url = get_database_url()
    if using_postgres():
        return create_engine(url, pool_pre_ping=True)
    return None

def database_mode():
    return "Supabase/PostgreSQL" if using_postgres() else "SQLite local fallback"

def init_db():
    if using_postgres():
        engine = get_engine()
        with engine.begin() as conn:
            for statement in POSTGRES_SCHEMA.split(";"):
                if statement.strip():
                    conn.execute(text(statement))
    else:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as con:
            con.executescript(SQLITE_SCHEMA)

def get_connection():
    if using_postgres():
        return get_engine().connect()
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def execute_sql(sql, params=None):
    params = params or {}
    if using_postgres():
        with get_engine().begin() as conn:
            return conn.execute(text(sql), params)
    else:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as con:
            cur = con.execute(sql, params if isinstance(params, tuple) else tuple(params.values()) if params else ())
            con.commit()
            return cur
