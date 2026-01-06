import sqlite3
import json
from typing import Optional
import time

from db import connect, init_db
from riot_api import fetch_match

BATCH = 100

def cache_get_db(conn: sqlite3.Connection, match_id: str) -> dict | None:
    row = conn.execute("SELECT json FROM match_cache WHERE match_id = ?", (match_id,)).fetchone()
    return None if row is None else json.loads(row["json"])

def cache_put_db(conn: sqlite3.Connection, match_id: str, match_json: dict) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO match_cache (match_id, json) VALUES (?, ?)",
        (match_id, json.dumps(match_json))
    )

def insert_match(conn: sqlite3.Connection, match_id: str, match_json: dict) -> None:
    info = match_json.get("info", {})

    conn.execute("""
      INSERT OR IGNORE INTO matches (
        match_id, game_creation, game_duration, game_end_timestamp,
        game_mode, game_type, game_version, platform_id,
        queue_id, map_id, game_name, game_start_timestamp
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
      match_id,
      info.get("gameCreation"),
      info.get("gameDuration"),
      info.get("gameEndTimestamp"),
      info.get("gameMode"),
      info.get("gameType"),
      info.get("gameVersion"),
      info.get("platformId"),
      info.get("queueId"),
      info.get("mapId"),
      info.get("gameName"),
      info.get("gameStartTimestamp"),
    ))

    participants = info.get("participants", [])
    rows = []
    for p in participants:
        rows.append((
            match_id,
            p.get("participantId"),
            p.get("puuid"),
            p.get("summonerName"),
            p.get("riotIdGameName"),
            p.get("riotIdTagline"),
            p.get("teamId"),
            p.get("championId"),
            p.get("championName"),
            p.get("championTransform"),
            int(bool(p.get("win"))),
            p.get("kills"),
            p.get("deaths"),
            p.get("assists"),
            p.get("totalDamageDealtToChampions"),
            p.get("totalMinionsKilled"),
            p.get("neutralMinionsKilled"),
            p.get("visionScore"),
            p.get("goldEarned"),
            p.get("champLevel"),
            p.get("role"),
            p.get("lane"),
        ))

    conn.executemany("""
      INSERT OR IGNORE INTO participants (
        match_id, participant_id, puuid, summoner_name,
        riot_id_game_name, riot_id_tagline,
        team_id, champion_id, champion_name, champion_transform,
        win, kills, deaths, assists,
        total_damage_dealt_to_champions,
        total_minions_killed, neutral_minions_killed,
        vision_score, gold_earned, champ_level,
        role, lane
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

def main(limit: Optional[int] = None) -> None:
    conn = connect()
    init_db(conn)

    # speed pragmas
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")

    q = """
    SELECT match_id FROM match_ids ORDER BY added_at ASC
    """ + (" LIMIT ?" if limit is not None else "")

    cur = conn.execute(q, (() if limit is None else (limit,)))

    inserted = skipped = failed = 0
    conn.execute("BEGIN")

    for i, row in enumerate(cur, start=1):
        match_id = row["match_id"]
        try:
            match_json = cache_get_db(conn, match_id)
            if match_json is None:
                match_json = fetch_match(match_id)
                cache_put_db(conn, match_id, match_json)

            insert_match(conn, match_id, match_json)
            inserted += 1

            if i % BATCH == 0:
                conn.commit()
                conn.execute("BEGIN")
                print(f"[{i}] inserted={inserted}, skipped={skipped}, failed={failed}")

        except Exception as e:
            failed += 1
            # keep going; rollback just this statement batch if desired
            print(f"FAILED match_id={match_id}: {e}")

    conn.commit()
    print(f"Finished: inserted={inserted}, failed={failed}")

if __name__ == "__main__":
    main()
