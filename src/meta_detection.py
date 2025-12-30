import os
import csv
import sqlite3
from days_since_patch import ms_since_patch
from pathlib import Path
from typing import Dict, Tuple
from db import connect

PATCH = ms_since_patch
MIN_GAMES = 5
META_CHARACTERS = 5
CSV_PATH = Path("Data/Processed/meta_champs.csv")
RANKED_SOLO_QUEUE = 420

def get_window_stats(conn: sqlite3.Connection, clause: str, params: Tuple) -> Tuple[Dict[str, Dict[str, float]], int]:
    cur = conn.cursor()
    cur.execute(
        f"""SELECT COUNT(*) FROM participants p JOIN MATCHES M on m.match_id = p.match_id WHERE {clause}""", params
    )
    total_picks = cur.fetchone()[0]
    
    cur.execute(
        f"""SELECT p.champion_name, Count(*) AS games, SUM(p.win) AS wins FROM participants p JOIN matches m on m.match_id = p.match_id 
        WHERE {clause} Group BY p.champion_name""", params
    )

    stats: Dict[str, Dict[str, float]] = {}
    for champ, games, wins in cur.fetchall():
        stats[champ] = {"games": games, "wins": wins}
    return stats, total_picks

def compute_rates(stats: Dict[str, Dict[str, float]], total_picks: int,)->Dict[str, Dict[str, float]]:
    result: Dict[str, Dict[str, float]] = {}
    for champ, s in stats.items():
        games = s["games"]
        wins = s["wins"]
        if games == 0:
            continue

        pick_rate = games/total_picks if total_picks > 0 else 0.0
        win_rate = wins/games

        result[champ] = {
            "games": games,
            "wins": wins,
            "pick_rate": pick_rate,
            "win_rate": win_rate
        }
    return result

def detect_meta_champs(conn: sqlite3.Connection):
    pre_where = "m.game_start_timestamp < ? AND m.queue_id =?"
    pre_params = (PATCH, RANKED_SOLO_QUEUE)

    post_where = "m.game_start_timestamp >= ? AND m.queue_id =?"
    post_params = (PATCH, RANKED_SOLO_QUEUE)

    pre_raw, pre_total_picks = get_window_stats(conn, pre_where, pre_params)
    post_raw, post_total_picks = get_window_stats(conn, post_where, post_params)

    pre_stats = compute_rates(pre_raw, pre_total_picks)
    post_stats = compute_rates(post_raw, post_total_picks)

    champs = set(pre_stats.keys()) | set(post_stats.keys())

    rows = []
    for champ in champs:
        pre = pre_stats.get(champ, {"games": 0, "wins": 0, "pick_rate": 0.0, "win_rate": 0.0})
        post = post_stats.get(champ, {"games": 0, "wins": 0, "pick_rate": 0.0, "win_rate": 0.0})

        if post["games"] < MIN_GAMES:
            continue

        pick_delta = post["pick_rate"] - pre["pick_rate"]
        win_delta = post["win_rate"] - pre["win_rate"]

        rows.append({
            "champion_name": champ,
            "pre_games": pre["games"],
            "post_games": post["games"],
            "pre_pick_rate": pre["pick_rate"],
            "post_pick_rate": post["pick_rate"],
            "pick_rate_delta": pick_delta,
            "pre_win_rate": pre["win_rate"],
            "post_win_rate": post["win_rate"],
            "win_rate_delta": win_delta
        })
    
    rows.sort(key=lambda r: r["pick_rate_delta"], reverse=True)

    return rows[:META_CHARACTERS]

def save_meta_champs_csv(rows):
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    field_names = [
        "champion_name",
        "pre_games",
        "post_games",
        "pre_pick_rate", "post_pick_rate", "pick_rate_delta",
        "pre_win_rate", "post_win_rate", "win_rate_delta"
    ]

    with CSV_PATH.open("w", newline = "", encoding = "utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=field_names)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"Saved meta champs to {CSV_PATH}")

def main():
    conn = connect()
    rows = detect_meta_champs(conn)
    save_meta_champs_csv(rows)

    print("Meta Champions:")
    for r in rows:
        print(
            f"{r['champion_name']}: "
            f"pick {r['pre_pick_rate']:.3%} -> {r['post_pick_rate']:.3%}"
            f"(delta {r['pick_rate_delta']:.3%}), "
            f"win {r['pre_win_rate']:.1%} -> {r['post_win_rate']:.1%}"
            f"(delta {r['win_rate_delta']:.1%})"
        )
    conn.close()

if __name__ == "__main__":
    main()