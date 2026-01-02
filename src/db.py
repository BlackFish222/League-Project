import sqlite3
import json
from pathlib import Path

DEFAULT_DB_PATH = Path("Data") / "Raw" / "riot.db"


def connect(db_path: str | Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    db_path = Path(db_path)  
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    print("Connected DB:", db_path.resolve())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def init_db(conn: sqlite3.Connection, schema_path: Path = Path("src/schema.sql"))-> None:
    if schema_path.exists():
        conn.executescript(schema_path.read_text(encoding="utf-8"))
    else:
        conn.executescript("""
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS matches (
        match_id TEXT PRIMARY KEY,
        game_creation INTEGER,
        game_duration INTEGER,
        game_end_timestamp INTEGER,
        game_mode TEXT,
        game_type TEXT,
        game_version TEXT,
        platform_id TEXT,
        queue_id INTEGER,
        map_id INTEGER,

        -- common useful fields
        game_name TEXT,
        game_start_timestamp INTEGER,
        ingestion_ts INTEGER DEFAULT (strftime('%s','now'))
        );

        CREATE TABLE IF NOT EXISTS participants (
        match_id TEXT NOT NULL,
        participant_id INTEGER NOT NULL,
        puuid TEXT NOT NULL,
        summoner_name TEXT,
        riot_id_game_name TEXT,
        riot_id_tagline TEXT,

        team_id INTEGER,
        champion_id INTEGER,
        champion_name TEXT,
        champion_transform INTEGER,

        win INTEGER,
        kills INTEGER,
        deaths INTEGER,
        assists INTEGER,

        total_damage_dealt_to_champions INTEGER,
        total_minions_killed INTEGER,
        neutral_minions_killed INTEGER,
        vision_score INTEGER,
        gold_earned INTEGER,
        champ_level INTEGER,
        role TEXT,
        lane TEXT,

        PRIMARY KEY (match_id, puuid),
        FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_participants_puuid ON participants(puuid);
        CREATE INDEX IF NOT EXISTS idx_matches_queue ON matches(queue_id);
        CREATE INDEX IF NOT EXISTS idx_matches_game_start ON matches(game_start_timestamp);
                           
        CREATE VIEW IF NOT EXISTS player_match_stats AS
        SELECT 
            p.puuid,
            p.match_id,
            m.game_creation AS game_creation,
            m.game_duration / 60.0 AS game_minutes,
            CASE 
                WHEN m.game_duration = 0 THEN NULL
                ELSE p.gold_earned / (m.game_duration / 60.0)
            END AS gold_per_min,
            CASE 
                WHEN m.game_duration = 0 THEN NULL
                ELSE p.deaths / (m.game_duration / 600.0)
            END AS deaths_per_10,
            p.kills,
            p.deaths,
            p.assists,
            p.role,
            p.lane,
            p.team_id,
            m.queue_id AS queue_id
        FROM participants p
        JOIN matches m ON p.match_id = m.match_id;
                           
        CREATE TABLE IF NOT EXISTS match_cache (
        match_id TEXT PRIMARY KEY,
        fetched_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
        json TEXT NOT NULL);
        """)

def match_exists(conn: sqlite3.Connection, match_id: str) -> bool:
    row = conn.execute("SELECT 1 FROM matches WHERE match_id = ? LIMIT 1", (match_id,)).fetchone()
    return row is not None

