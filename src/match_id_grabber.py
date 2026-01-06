from __future__ import annotations

import os
import time
import sqlite3
from dataclasses import dataclass
from typing import Optional, Iterable

import requests

DB_PATH = "Data/Raw/riot.db"
REGION = "americas"            
QUEUE_ID = 420                 
PAGE_SIZE = 100
MAX_PAGES_PER_PUUID = 50
TARGET_TOTAL_MATCH_IDS = 25_000

EXPAND_EVERY_N_PAGES = 3       
EXPAND_MATCHES_PER_EXPAND = 2  

TIMEOUT_S = 20
MAX_RETRIES = 6
BACKOFF_BASE = 1.6

from Config import api_key 

def connect_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS match_ids (
        match_id TEXT PRIMARY KEY,
        added_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS puuids (
        puuid TEXT PRIMARY KEY,
        fetched INTEGER DEFAULT 0,
        last_error TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_puuids_fetched_updated
    ON puuids(fetched, updated_at);

    CREATE TABLE IF NOT EXISTS match_detail_cache (
        match_id TEXT PRIMARY KEY,
        fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
        json TEXT NOT NULL
    );
    """)
    conn.commit()


def count_match_ids(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) AS n FROM match_ids").fetchone()["n"]


def get_next_puuid(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute(
        "SELECT puuid FROM puuids WHERE fetched=0 ORDER BY updated_at ASC LIMIT 1"
    ).fetchone()
    return row["puuid"] if row else None


def add_seed_puuids(conn: sqlite3.Connection, puuids: Iterable[str]) -> int:
    rows = [(p,) for p in puuids]
    conn.executemany("INSERT OR IGNORE INTO puuids(puuid) VALUES(?)", rows)
    return conn.execute("SELECT changes() AS c").fetchone()["c"]


def add_puuids(conn: sqlite3.Connection, puuids: Iterable[str]) -> int:
    rows = [(p,) for p in puuids]
    conn.executemany("INSERT OR IGNORE INTO puuids(puuid) VALUES(?)", rows)
    return conn.execute("SELECT changes() AS c").fetchone()["c"]


def insert_match_ids(conn: sqlite3.Connection, match_ids: list[str]) -> int:
    rows = [(m,) for m in match_ids]
    conn.executemany("INSERT OR IGNORE INTO match_ids(match_id) VALUES(?)", rows)
    return conn.execute("SELECT changes() AS c").fetchone()["c"]


def mark_puuid_done(conn: sqlite3.Connection, puuid: str) -> None:
    conn.execute(
        "UPDATE puuids SET fetched=1, last_error=NULL, updated_at=CURRENT_TIMESTAMP WHERE puuid=?",
        (puuid,)
    )


def mark_puuid_error(conn: sqlite3.Connection, puuid: str, err: str) -> None:
    conn.execute(
        "UPDATE puuids SET last_error=?, updated_at=CURRENT_TIMESTAMP WHERE puuid=?",
        (err[:500], puuid)
    )

@dataclass
class RiotClient:
    session: requests.Session
    region: str = REGION

    def _request_json(self, url: str, params: Optional[dict] = None) -> dict | list:
        for attempt in range(1, MAX_RETRIES + 1):
            r = self.session.get(url, params=params, timeout=TIMEOUT_S)

            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", "2"))
                sleep_s = max(retry_after, int(BACKOFF_BASE ** attempt))
                print(f"[429] Sleeping {sleep_s}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(sleep_s)
                continue

            if 500 <= r.status_code < 600:
                sleep_s = BACKOFF_BASE ** attempt
                print(f"[{r.status_code}] Server error. Sleeping {sleep_s:.2f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(sleep_s)
                continue

            r.raise_for_status()
            return r.json()

        raise RuntimeError(f"Failed after {MAX_RETRIES} retries: {url}")

    def match_ids_by_puuid(
        self,
        puuid: str,
        start: int,
        count: int,
        queue: int = QUEUE_ID,
    ) -> list[str]:
        url = f"https://{self.region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params = {"start": start, "count": count, "queue": queue}
        data = self._request_json(url, params=params)
        return data if isinstance(data, list) else []

    def match_detail(self, match_id: str) -> dict:
        url = f"https://{self.region}.api.riotgames.com/lol/match/v5/matches/{match_id}"
        data = self._request_json(url, params=None)
        if not isinstance(data, dict):
            raise RuntimeError("Unexpected match detail response type")
        return data


def cache_get_match_detail(conn: sqlite3.Connection, match_id: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT json FROM match_detail_cache WHERE match_id=?",
        (match_id,)
    ).fetchone()
    if not row:
        return None
    import json
    return json.loads(row["json"])


def cache_put_match_detail(conn: sqlite3.Connection, match_id: str, match_json: dict) -> None:
    import json
    conn.execute(
        "INSERT OR REPLACE INTO match_detail_cache(match_id, json) VALUES(?, ?)",
        (match_id, json.dumps(match_json))
    )

def expand_puuids_from_matches(
    conn: sqlite3.Connection,
    client: RiotClient,
    match_ids: list[str],
    max_matches: int,
) -> int:

    added_total = 0
    used = 0

    for mid in match_ids:
        if used >= max_matches:
            break

        try:
            match_json = cache_get_match_detail(conn, mid)
            if match_json is None:
                match_json = client.match_detail(mid)
                cache_put_match_detail(conn, mid, match_json)

            info = match_json.get("info", {})
            participants = info.get("participants", [])
            puuids = [p.get("puuid") for p in participants if p.get("puuid")]

            if puuids:
                added_total += add_puuids(conn, puuids)

            used += 1

        except Exception as e:
            print(f"expand error on match {mid}: {e}")

    return added_total

def main(limit: Optional[int] = None) -> None:
    if not api_key:
        raise RuntimeError("Missing api_key")

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

    with conn:
        added_seeds = add_seed_puuids(conn, seed_puuids)

    total = count_match_ids(conn)
    print(f"DB: {DB_PATH}")
    print(f"Seed PUUIDs added this run: {added_seeds}")
    print(f"Current queued match IDs: {total}")

    session = requests.Session()
    session.headers.update({"X-Riot-Token": api_key})
    client = RiotClient(session=session, region=REGION)

    while total < TARGET_TOTAL_MATCH_IDS:
        puuid = get_next_puuid(conn)
        if not puuid:
            print("No more unfetched PUUIDs.")
            break

        try:
            start = 0
            pages = 0

            while total < TARGET_TOTAL_MATCH_IDS:
                if limit is not None and pages >= limit:
                    break

                ids = client.match_ids_by_puuid(puuid, start=start, count=PAGE_SIZE, queue=QUEUE_ID)

                if not ids:
                    break
                with conn:
                    added = insert_match_ids(conn, ids)
                    total += added
                    if (pages % EXPAND_EVERY_N_PAGES) == 0:
                        new_puuids = expand_puuids_from_matches(
                            conn=conn,
                            client=client,
                            match_ids=ids,
                            max_matches=EXPAND_MATCHES_PER_EXPAND,
                        )
                    else:
                        new_puuids = 0

                pages += 1
                start += PAGE_SIZE

                if pages % 5 == 0:
                    unfetched = conn.execute("SELECT COUNT(*) AS n FROM puuids WHERE fetched=0").fetchone()["n"]
                    print(f"[{puuid[:8]}…] pages={pages} total={total}/{TARGET_TOTAL_MATCH_IDS} "
                          f"added_last_page={added} new_puuids={new_puuids} unfetched={unfetched}")

                if pages >= MAX_PAGES_PER_PUUID:
                    break

            with conn:
                mark_puuid_done(conn, puuid)

        except Exception as e:
            with conn:
                mark_puuid_error(conn, puuid, str(e))
            print(f"Error for PUUID {puuid[:8]}…: {e}")

        time.sleep(0.02)

    print(f"Done. Total queued match IDs: {total}")
    total_puuids = conn.execute("SELECT COUNT(*) AS n FROM puuids").fetchone()["n"]
    unfetched_puuids = conn.execute("SELECT COUNT(*) AS n FROM puuids WHERE fetched=0").fetchone()["n"]
    print(f"Total PUUIDs in pool: {total_puuids}, unfetched: {unfetched_puuids}")

    conn.close()
    session.close()


if __name__ == "__main__":
    main()
