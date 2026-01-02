import os
import csv
import sqlite3
from days_since_patch import ms_since_patch, ms_since_pre_patch, ms_since_post_patch
from pathlib import Path
from typing import Dict, Tuple
from db import connect

PRE_PATCH = ms_since_pre_patch()
PATCH = ms_since_patch()
PATCH_END = ms_since_post_patch()

print(f"Patch window:{PRE_PATCH} to {PATCH_END}")

MIN_GAMES = 5
META_CHARACTERS = 8
CSV_PATH = Path("Data/Processed/meta_champs.csv")
RANKED_SOLO_QUEUE = 420

MIN_PRE_GAMES = 10   
MIN_POST_GAMES = 10    
MIN_PICK_DELTA = 0.0

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

def get_window_stats_range(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
) -> Tuple[Dict[str, Dict[str, float]], int]:
    """
    Stats for a specific patch window [start_ts, end_ts) in ranked solo.
    """
    where = """
        m.game_start_timestamp >= ?
        AND m.game_start_timestamp < ?
        AND m.queue_id = ?
    """
    params = (start_ts, end_ts, RANKED_SOLO_QUEUE)
    return get_window_stats(conn, where, params)
 
def detect_meta_champs(conn: sqlite3.Connection):
    # --- Stage 1: previous patch stats ---
    prev_raw, prev_total_picks = get_window_stats_range(conn, PRE_PATCH, PATCH)
    prev_stats = compute_rates(prev_raw, prev_total_picks)

    # --- Stage 2: current patch stats ---
    curr_raw, curr_total_picks = get_window_stats_range(conn, PATCH, PATCH_END)
    curr_stats = compute_rates(curr_raw, curr_total_picks)

    champs = set(prev_stats.keys()) | set(curr_stats.keys())

    rows = []
    for champ in champs:
        prev = prev_stats.get(champ, {"games": 0, "wins": 0, "pick_rate": 0.0, "win_rate": 0.0})
        curr = curr_stats.get(champ, {"games": 0, "wins": 0, "pick_rate": 0.0, "win_rate": 0.0})

        # Require enough volume in BOTH patches so we aren't reacting to noise
        if prev["games"] < MIN_PRE_GAMES:
            continue
        if curr["games"] < MIN_POST_GAMES:
            continue

        pick_delta = curr["pick_rate"] - prev["pick_rate"]
        win_delta = curr["win_rate"] - prev["win_rate"]

        # Only care about champs whose pick rate actually went up by a meaningful amount
        if pick_delta < MIN_PICK_DELTA:
            continue

        rows.append({
            "champion_name": champ,

            # previous patch window stats
            "pre_games": prev["games"],
            "pre_pick_rate": prev["pick_rate"],
            "pre_win_rate": prev["win_rate"],

            # current patch window stats
            "post_games": curr["games"],
            "post_pick_rate": curr["pick_rate"],
            "post_win_rate": curr["win_rate"],

            # deltas
            "pick_rate_delta": pick_delta,
            "win_rate_delta": win_delta,
        })

    # Sort by biggest increase in pick rate this patch
    rows.sort(key=lambda r: r["pick_rate_delta"], reverse=True)

    # Top N meta champs
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