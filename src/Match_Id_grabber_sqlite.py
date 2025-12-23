import sqlite3
import os
import time
import requests
from Config import api_key

DB_PATH = os.path.join("Data", "Raw", "riot_queue.sqlite")
HEADERS = {"X-Riot-Token": api_key}
REGION = "americas"

def getMatchIds(puuid: str, start: int = 0, count: int = 100) -> list[str]:
    api_url = f"https://{REGION}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    r = requests.get(api_url, headers=HEADERS, params={"start": start, "count": count}, timeout=20)

    if r.status_code == 429:
        retry_after = int(r.headers.get("Retry-After", "2"))
        time.sleep(retry_after)
        return getMatchIds(puuid, start, count)

    r.raise_for_status()
    return r.json()

def connect_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS match_ids (
        match_id TEXT PRIMARY KEY,
        added_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS puuids (
        puuid TEXT PRIMARY KEY,
        fetched INTEGER DEFAULT 0,
        last_error TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()

def add_seed_puuids(conn: sqlite3.Connection, puuids: list[str]) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO puuids(puuid) VALUES(?)",
        [(p,) for p in puuids]
    )
    conn.commit()

def get_next_puuid(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT puuid FROM puuids WHERE fetched=0 ORDER BY updated_at ASC LIMIT 1"
    ).fetchone()
    return row[0] if row else None

def mark_puuid_done(conn: sqlite3.Connection, puuid: str) -> None:
    conn.execute(
        "UPDATE puuids SET fetched=1, last_error=NULL, updated_at=CURRENT_TIMESTAMP WHERE puuid=?",
        (puuid,)
    )
    conn.commit()

def mark_puuid_error(conn: sqlite3.Connection, puuid: str, err: str) -> None:
    conn.execute(
        "UPDATE puuids SET last_error=?, updated_at=CURRENT_TIMESTAMP WHERE puuid=?",
        (err[:500], puuid)
    )
    conn.commit()

def insert_match_ids(conn: sqlite3.Connection, match_ids: list[str]) -> int:
    conn.executemany(
        "INSERT OR IGNORE INTO match_ids(match_id) VALUES(?)",
        [(m,) for m in match_ids]
    )
    conn.commit()
    return conn.execute("SELECT changes()").fetchone()[0]

def count_match_ids(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM match_ids").fetchone()[0]

def main():
    if not api_key:
        raise RuntimeError("Missing api key")

    seed_puuids = [
        "ZDeF5_l5PcdFrBAGZJc3FXH_rMVej7iZ_snsQl6yIZPuBZOPy2JTELg9fTtspAHE7tJzS5wy7460rQ",
        "MQXYnF9l3o09tQMyvCjN0v_PrKbFcu7uihOCC6_QaGF1njmoXqG4FxvxSn4ezDTgVS2BnWNUmQspdw"
    ]

    conn = connect_db(DB_PATH)
    init_db(conn)
    add_seed_puuids(conn, seed_puuids)

    target_total = 2000
    print("Starting. Current queued match IDs:", count_match_ids(conn))

    while count_match_ids(conn) < target_total:
        puuid = get_next_puuid(conn)
        if not puuid:
            print("No more unfetched PUUIDs.")
            break

        try:
            ids = getMatchIds(puuid, start=0, count=100)
            added = insert_match_ids(conn, ids)
            mark_puuid_done(conn, puuid)
            total = count_match_ids(conn)
            print(f"PUUID {puuid[:8]}… got {len(ids)} ids, added {added}, total={total}")

        except Exception as e:
            mark_puuid_error(conn, puuid, str(e))
            print(f"Error for PUUID {puuid[:8]}…: {e}")

        time.sleep(0.1)

    print("Done. Total queued match IDs:", count_match_ids(conn))
    conn.close()

if __name__ == "__main__":
    main()

import sqlite3
conn = sqlite3.connect("data/raw/riot_queue.sqlite")
print("match_ids:", conn.execute("SELECT COUNT(*) FROM match_ids").fetchone()[0])
print("puuids:", conn.execute("SELECT COUNT(*) FROM puuids").fetchone()[0])
print("unfetched puuids:", conn.execute("SELECT COUNT(*) FROM puuids WHERE fetched=0").fetchone()[0])
conn.close()
