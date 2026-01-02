from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pandas as pd

from days_since_patch import ms_since_patch
from db import connect  
from meta_character_ids import meta_ids 

# Output
OUT_PATH = Path("Data/Processed/player_performance.csv")

# Windows
BASELINE_N = 20
POST_N_MAX = 20
POST_N_MIN = 10
MIN_BASELINE = 10

# Patch cutoff (ms since epoch)
META_PATCH_TS = ms_since_patch()

# Meta champ IDs (set[int])
META_CHAMPS = set(meta_ids())
print("META_PATCH_TS =", META_PATCH_TS)
print("META_CHAMPS size:", len(META_CHAMPS), "sample:", list(sorted(META_CHAMPS))[:10])


def load_player_games() -> pd.DataFrame:
    """
    One row = one player in one match, with match timestamp attached.
    """
    conn: sqlite3.Connection = connect()
    print("Connected to DB via db.connect()")

    query = """
        SELECT
            p.match_id,
            p.team_id,
            p.puuid,
            m.game_start_timestamp AS game_start_time,
            p.champion_id,
            p.win,
            p.kills,
            p.deaths,
            p.assists,
            (p.total_minions_killed + p.neutral_minions_killed) AS cs,
            p.gold_earned AS gold,
            p.total_damage_dealt_to_champions AS damage
        FROM participants AS p
        JOIN matches AS m
          ON p.match_id = m.match_id
        -- Optional: restrict to ranked solo
        -- WHERE m.queue_id = 420
        ORDER BY p.puuid, m.game_start_timestamp
    """
    df = pd.read_sql(query, conn)
    conn.close()

    print(f"Loaded {len(df)} player-game rows")
    if df.empty:
        return df

    # Ensure integer ms timestamps
    df["game_start_time"] = df["game_start_time"].astype("int64")
    df["champion_id"] = df["champion_id"].astype("int64")
    df["team_id"] = df["team_id"].astype("int64")

    print("game_start_time sample:", df["game_start_time"].head().tolist())
    return df


def add_faced_meta_flag(df: pd.DataFrame, meta_ids_set: set[int]) -> pd.DataFrame:
    """
    faced_meta = 1 if the opponent team has ANY meta champ in that match.
    Fast: no merges, just groupby + lookup.
    """
    df = df.copy()

    if not meta_ids_set:
        df["faced_meta"] = 0
        return df

    df["is_meta"] = df["champion_id"].isin(meta_ids_set).astype("int64")

    # For each (match, team), did that team pick a meta champ?
    team_has_meta = df.groupby(["match_id", "team_id"], sort=False)["is_meta"].max()

    opp_team = 300 - df["team_id"].values
    mids = df["match_id"].values

    faced = [team_has_meta.get((mid, ot), 0) for mid, ot in zip(mids, opp_team)]
    df["faced_meta"] = pd.Series(faced, index=df.index).astype("int64")

    df.drop(columns=["is_meta"], inplace=True)
    return df


def compute_window_stats(df: pd.DataFrame, prefix: str) -> dict:
    n_games = len(df)
    if n_games == 0:
        return {}

    wins = df["win"].mean()
    kills = df["kills"].mean()
    deaths = df["deaths"].replace(0, 0.5).mean()  # avoid div by zero
    assists = df["assists"].mean()
    kda = (kills + assists) / deaths

    out = {
        f"{prefix}_ngames": n_games,
        f"{prefix}_winrate": wins,
        f"{prefix}_kills": kills,
        f"{prefix}_deaths": deaths,
        f"{prefix}_assists": assists,
        f"{prefix}_kda": kda,
        f"{prefix}_cs": df["cs"].mean(),
        f"{prefix}_gold": df["gold"].mean(),
        f"{prefix}_damage": df["damage"].mean(),
        f"{prefix}_faced_meta": df["faced_meta"].mean(),  # fraction in this window
    }
    return out


def build_player_features() -> None:
    t0 = time.time()
    df = load_player_games()
    print("load_player_games:", round(time.time() - t0, 3), "s")
    if df.empty:
        print("No rows loaded from DB; nothing to write.")
        return

    t1 = time.time()
    df = add_faced_meta_flag(df, META_CHAMPS)
    print("add_faced_meta_flag:", round(time.time() - t1, 3), "s")
    print("faced_meta counts:", df["faced_meta"].value_counts().to_dict())

    # Split pre/post
    pre = df[df["game_start_time"] < META_PATCH_TS]
    post = df[df["game_start_time"] >= META_PATCH_TS]
    print(f"Pre-patch rows: {len(pre)}, post-patch rows: {len(post)}")

    # Group once (FAST)
    post_groups = post.groupby("puuid", sort=False)

    features_rows: list[dict] = []
    t2 = time.time()

    for puuid, g_pre in pre.groupby("puuid", sort=False):
        if puuid not in post_groups.groups:
            continue
        g_post = post_groups.get_group(puuid)

        baseline_window = g_pre.tail(BASELINE_N)
        post_window = g_post.head(POST_N_MAX)

        if len(baseline_window) < MIN_BASELINE or len(post_window) < POST_N_MIN:
            continue

        baseline_stats = compute_window_stats(baseline_window, "baseline")
        post_stats = compute_window_stats(post_window, "post")

        # exposure_meta = fraction of post games where they faced meta
        exposure = post_stats["post_faced_meta"]

        row = {"puuid": puuid, "exposure_meta": exposure}
        row.update(baseline_stats)
        row.update(post_stats)

        # deltas: post - baseline
        for key, val in post_stats.items():
            if key.endswith("_ngames"):
                continue
            base_key = "baseline" + key[len("post"):]  # post_x -> baseline_x
            if base_key in baseline_stats:
                row["delta" + key[len("post"):]] = val - baseline_stats[base_key]

        features_rows.append(row)

    print("build loop:", round(time.time() - t2, 3), "s")
    print(f"Built features for {len(features_rows)} players")

    features_df = pd.DataFrame(features_rows)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    features_df.to_csv(OUT_PATH, index=False)

    print("Saved player features to:", OUT_PATH.resolve())
    print("Total time:", round(time.time() - t0, 3), "s")


if __name__ == "__main__":
    build_player_features()
