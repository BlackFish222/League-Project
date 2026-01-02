import sqlite3
import json
import time
from typing import Optional

from db import connect, init_db, match_exists
from riot_api import fetch_match

def cache_get_db(conn: sqlite3.Connection, match_id: str) -> dict | None:
    row = conn.execute(
        "SELECT json FROM match_cache WHERE match_id = ?",
        (match_id,)
    ).fetchone()
    if row is None:
        return None
    # row is sqlite3.Row because db.connect sets row_factory
    return json.loads(row["json"])

def cache_put_db(conn: sqlite3.Connection, match_id: str, match_json: dict) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO match_cache (match_id, json) VALUES (?, ?)",
        (match_id, json.dumps(match_json))
    )


def insert_match(conn: sqlite3.Connection, match_id: str, match_json: dict) -> None:
    info = match_json.get("info", {})
    meta = match_json.get("metadata", {})  # not used yet, but fine

    conn.execute("""
      INSERT INTO matches (
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
    for p in participants:
        conn.execute("""
          INSERT INTO participants (
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
        """, (
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

def main(limit: Optional[int] = None) -> None:
    conn = connect()
    init_db(conn)

    tables = [r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    if "match_ids" not in tables:
        raise RuntimeError(
            "match_ids table not found in this DB. "
            "You're connected to the wrong database file."
        )

    rows = conn.execute(
        "SELECT match_id FROM match_ids ORDER BY added_at ASC"
        + (" LIMIT ?" if limit is not None else ""),
        (() if limit is None else (limit,))
    ).fetchall()
    match_ids = [r["match_id"] for r in rows]

    print(f"Loaded {len(match_ids)} match ids from DB")

    if not match_ids:
        print("No match ids found in database.")
        return

    inserted = 0
    skipped = 0
    failed = 0

    for q, match_id in enumerate(match_ids, start=1):
        try:
            if match_exists(conn, match_id):
                skipped += 1
                continue

            match_json = cache_get_db(conn, match_id)
            if match_json is None:
                match_json = fetch_match(match_id)
                with conn:
                    cache_put_db(conn, match_id, match_json)

            with conn:
                insert_match(conn, match_id, match_json)

            inserted += 1

            if q % 25 == 0:
                print(f"[{q}/{len(match_ids)}] inserted={inserted}, skipped={skipped}, failed={failed}")

            if q % 50 == 0:
                time.sleep(0.25)

        except Exception as e:
            failed += 1
            print(f"FAILED match_id={match_id}: {e}")

    print(f"Process Finished: inserted={inserted}, skipped={skipped}, failed={failed}")

if __name__ == "__main__":
    main(limit=None)
