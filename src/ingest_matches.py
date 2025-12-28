import sqlite3
from pathlib import Path
from db import connect, init_db, match_exists
from riot_api import fetch_match, cache_read, cache_write


MATCH_CACHE_DIR = Path("Data/Cache/Matches")
MATCH_IDS_PATH = Path("Data/Raw/match_ids.txt")

def insert_match(conn: sqlite3.Connection, match_id: str, match_json: dict)-> None:
    info = match_json.get("info", {})
    meta = match_json.get("metadata", {})
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

def load_match_ids(path: Path)-> list[str]:
    raw = path.read_text(encoding = "utf-8").splitlines()
    return [line.strip() for line in raw if line.strip()]

def load_match_ids_from_queue_db(queue_db_path: Path, limit: int | None = None) -> list[str]:
    conn = sqlite3.connect(queue_db_path)
    rows = conn.execute(
        "SELECT match_id FROM match_ids ORDER BY added_at ASC"
        + (" LIMIT ?" if limit is not None else ""),
        (() if limit is None else (limit,))
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]

def main(limit: int | None = None) -> None:
    conn = connect()
    init_db(conn)

    ROOT = Path(__file__).resolve().parents[1]
    QUEUE_DB = ROOT / "data" / "raw" / "riot_queue.sqlite"

    match_ids = load_match_ids_from_queue_db(QUEUE_DB, limit=limit)
    print(f"Loaded {len(match_ids)} match ids from {QUEUE_DB}")

    if not match_ids:
        print("No match ids found in queue database.")
        return

    inserted = 0
    skipped = 0
    failed = 0

    for q, match_id in enumerate(match_ids, start=1):
        try:
            if match_exists(conn, match_id):
                skipped += 1
                continue
                
            cache_path = MATCH_CACHE_DIR / f"{match_id}.json"
            match_json = cache_read(cache_path)
            if match_json is None:
                match_json = fetch_match(match_id)
                cache_write(match_json, cache_path)

            with conn:
                insert_match(conn, match_id, match_json)
            
            inserted += 1

            if q % 25 == 0:
                print(f"[{q}/{len(match_ids)}] inserted = {inserted}, skipped = {skipped}, failed = {failed}")
        
        except Exception as e:
            failed += 1
            print(f"FAILED match_id={match_id}: {e}")

    print(f"Process Finished: inserted = {inserted}, skipped = {skipped}, failed = {failed}")

if __name__ == "__main__":
    main(limit=15000)