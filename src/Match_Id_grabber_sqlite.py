import sqlite3
import os
import time
import requests
from Config import api_key

DB_PATH = os.path.join("Data", "Raw", "riot_queue.sqlite")
HEADERS = {"X-Riot-Token": api_key}
REGION = "americas"

PAGE_SIZE = 100
MAX_PAGES_PER_PUUID = 50
TARGET_TOTAL = 15000
SAMPLE_MATCHES = 20

def getMatchIds(puuid: str, start: int = 0, count: int = 100) -> list[str]:
    api_url = f"https://{REGION}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    r = requests.get(api_url, headers=HEADERS, params={"start": start, "count": count}, timeout=20)

    if r.status_code == 429:
        retry_after = int(r.headers.get("Retry-After", "2"))
        time.sleep(retry_after)
        return getMatchIds(puuid, start, count)

    r.raise_for_status()
    return r.json()

def fetch_match(match_id: str) -> dict:
    """Fetch full match detail JSON for a given match ID."""
    url = f"https://{REGION}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    while True:
        r = requests.get(url, headers=HEADERS, timeout=20)

        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "2"))
            print(f"[429] match detail {match_id} rate limit. Sleeping {retry_after}s")
            time.sleep(retry_after)
            continue

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

def add_puuids(conn: sqlite3.Connection, puuids: list[str])->int:
    conn.executemany(
        "INSERT OR IGNORE INTO puuids(puuid) VALUES(?)",
        [(p,) for p in puuids]
    )
    conn.commit()
    return conn.execute ("SELECT changes()").fetchone()[0]

def puuid_puller(conn: sqlite3.Connection, match_ids: list[str])->int:
    pulled = 0
    sample_ids = match_ids[:SAMPLE_MATCHES]

    for mid in sample_ids:
        try:
            match_json = fetch_match(mid)
            participants = match_json.get("info", {}).get("participants",[])
            puuids = [p.get("puuid") for p in participants if p.get("puuid")]
            added = add_puuids(conn, puuids)
            pulled += added
            if added > 0:
                print(f"added {added} puuids")
            time.sleep(.05)
        except Exception as e:
            print(e)
    return pulled
def main():
    if not api_key:
        raise RuntimeError("Missing api key")

    seed_puuids = [
        "ZDeF5_l5PcdFrBAGZJc3FXH_rMVej7iZ_snsQl6yIZPuBZOPy2JTELg9fTtspAHE7tJzS5wy7460rQ",
        "MQXYnF9l3o09tQMyvCjN0v_PrKbFcu7uihOCC6_QaGF1njmoXqG4FxvxSn4ezDTgVS2BnWNUmQspdw",
        "hYfvdISfgd1KIwX6EZXM4h6vvKEG-gOb7p8a4GNn5dnR6UhrG1KcVcnYVfNKIcF9tZiZ-iepgiTldg",
        "86L4pE0sAUM7g_9siCXOL4utfK_Y2HKpHnl8Q2k0TkDlAAs3fiC-sB-NuMwSo5OxI7uU4DLla8URCQ",
        "APpfP4las_yrmU8DAy1Gp878ITIk1VUzTqgORnHgtYRe9q12dLqXw1kbRgn8bwlAMwqg5hGycfNs1Q",
        "s18-zSEvvrFuXzkvSptrjjpeQV7y5BPtq2vpt7b---jl8O67lPMiVjCEpUZwiILK15m6lI7YAENBiw",
        "MQuzRDeGH3UpCdiCgG9HDq2hFSvP3S9H_0pn48sxBYZrQm2ntVTVpXM6lOLtqIoUTOa7YQMmGXlwpQ",
        "CC6srW-i03Q2CMlRkZ2P1e3T-GJV3oglXJAxnSRB438lQb9q26ipWjaCSnblAbp4uDPV7-KPQU8Egw",
        "J6KXeXfpdDQJE5KWZXp8W1VdS1fqybMJyUp15XmUma1N-BxDhy-3LCdgsgdTCbX2gqPmj4fCxDpKfg",
        "d9el1JZDXpI47SfRzwvJ_TSghao8ToiVkCkhrhOGK-6I43T3FbqjhJyuuBWzFKX62gohMBgXEPdsew",
    ]

    conn = connect_db(DB_PATH)
    init_db(conn)
    add_seed_puuids(conn, seed_puuids)

    print("Current queued match IDs:", count_match_ids(conn))

    while count_match_ids(conn) < TARGET_TOTAL:
        puuid = get_next_puuid(conn)
        if not puuid:
            print("No more unfetched PUUIDs.")
            break

        try:
            start = 0 
            pages = 0
            while count_match_ids(conn) < TARGET_TOTAL:
                ids = getMatchIds(puuid, start=start, count=PAGE_SIZE)
                if not ids:
                    print("End of match history")
                    break
                added = insert_match_ids(conn, ids)
                total = count_match_ids(conn)
                print(f"total matches = {total}/{TARGET_TOTAL}")
                #print(f"PUUID {puuid[:8]}… got {len(ids)} ids, added {added}, total={total}")
                new_puuids = puuid_puller(conn, ids)
                if new_puuids:
                    unfetched = conn.execute(
                        "SELECT COUNT(*) FROM puuids WHERE fetched=0"
                    ).fetchone()[0]
                    print(f"new puuids = {new_puuids}")
                pages += 1
                if pages >= MAX_PAGES_PER_PUUID:
                    print(f"Max pages reached")
                    break
                start += PAGE_SIZE
                time.sleep(.01)

            mark_puuid_done(conn, puuid)

        except Exception as e:
            mark_puuid_error(conn, puuid, str(e))
            print(f"Error for PUUID {puuid[:8]}…: {e}")

        time.sleep(0.1)

    print("Done. Total queued match IDs:", count_match_ids(conn))

    total_puuids = conn.execute("SELECT COUNT(*) FROM puuids").fetchone()[0]
    unfetched_puuids = conn.execute("SELECT COUNT(*) FROM puuids WHERE fetched=0").fetchone()[0]
    print(f"Total PUUIDs in pool: {total_puuids}, unfetched: {unfetched_puuids}")

    conn.close()

if __name__ == "__main__":
    main()

"""import sqlite3
conn = sqlite3.connect("data/raw/riot_queue.sqlite")
print("match_ids:", conn.execute("SELECT COUNT(*) FROM match_ids").fetchone()[0])
print("puuids:", conn.execute("SELECT COUNT(*) FROM puuids").fetchone()[0])
print("unfetched puuids:", conn.execute("SELECT COUNT(*) FROM puuids WHERE fetched=0").fetchone()[0])
conn.close()"""
