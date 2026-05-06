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
