import time
from pathlib import Path
from typing import Tuple
import pandas as pd
import numpy as np

from db import connect
from days_since_patch import ms_since_patch, ms_since_post_patch, ms_since_pre_patch
from riot_api import fetch_json

BASELINE_GAMES = 20
BASELINE_GPM = float
BASELINE_DPT = float

DATABASE = "player_match_stats"
PLAYER_CSV = "Data/Processed/player_performance.csv"

def load_games(conn) -> pd.DataFrame:
    patch_start = ms_since_patch
    patch_end = ms_since_post_patch

    query = f"""SELECT puuid, game_creation, gold_per_min, deaths_per_10 FROM {DATABASE} WHERE game_creation >= ? AND game_creation < ?"""
    df = pd.read_sql_query(query, conn, params=(patch_start, patch_end))
    df = df.dropna(subset=["gold_per_min", "deaths_per_10"])
    return df

def label_player_games(group: pd.DataFrame) -> pd.DataFrame:
    group = group.sort_values("game_creation").reset_index(drop=True)

    if len(group) <= BASELINE_GAMES:
        group["give_up_game"] = np.nan
        return group
    
    baseline = group.iloc[:BASELINE_GAMES].copy()
    rest = group.iloc[BASELINE_GAMES:].copy()

    gpm_mean = baseline["gold_per_min"].mean()
    gpm_std = baseline["gold_per_min"].std(ddof=0)
    dpt_mean = baseline["deaths_per_10"].mean
    dpt_std = baseline["deaths_per_10"].std(ddof=0)

    if gpm_std == 0 or np.isnan(gpm_std):
        gpm_std = .000001
    if dpt_std == 0 or np.isnan(dpt_std):
        dpt_std = .000001

    rest["z_gpm"] = (rest["gold_per_min"] - gpm_mean) / gpm_std
    rest["z_dpt"] = (rest["deaths_per_10"] - dpt_mean) / dpt_std

    rest["give_up_game"] = (rest["z_gpm"] <= -BASELINE_GPM) & (rest["z_dpt"] >= BASELINE_DPT).astype(int)

    baseline["give_up_game"] = 0

    labeled = pd.concat([baseline, rest], ignore_index=True)
    return labeled

def compute_player_labels(games: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    labeled_games = (
        games.groupby("puuid", group_keys=False)
        .apply(label_player_games)
        .reset_index(drop=True)
    )
    agg = (
        labeled_games.groupby("puuid")["give_up_games"]
        .agg(["count", "sum"])
        .reset_index()
        .rename(columns={"count": "total_games", "sum": "give_up_count"})
        )
    
    agg["give_up"] = (agg["give_up_count"] > 0).astype(int)
    agg["player_give_up_rate"] = agg["give_up_count"] / agg["total_games"]

    return labeled_games, agg

def get_summoner_level(puuid: str) -> int | None:
    url = f"https://na1.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}"
    data = fetch_json(url)

    level = data.get("summonerLevel")
    if level is None:
        return None
    
    return int(level)

def attach_level(df: pd.DataFrame) -> pd.DataFrame:
    if "summoner_level" not in df.columns:
        df["summoner_level"] = np.nan

    mask_missing = df["summoner_level"].isna()
    missing_puuid = df.loc[mask_missing, "puuid"].unique()

    if len(missing_puuid) > 0:
        print(f"fetching summoner level for {len(missing_puuid)} players")
    
    for q, puuid in enumerate(missing_puuid, start=1):
        try:
            lvl = get_summoner_level(puuid)
        except Exception as e: 
            print(e)
            lvl = None

        if lvl is not None: 
            df.loc[df["puuid"] == puuid, "summoner_level"] = lvl
        
        if q % 20 == 0:
            time.sleep(1)

    def experiance_level(lvl):
        if pd.isna(lvl):
            return "unknown"
        lvl = int(lvl)
        if lvl < 50:
            return "new"
        elif lvl < 150:
            return "intermediate"
        else:
            return "veteran"
        
    df["experiance_from_level"] = df["summoner_level"].apply(experiance_level)
    return df 

def update_player_performance(player_labels: pd.DataFrame) -> None:
    if not PLAYER_CSV.exists():
        raise FileNotFoundError("player csv not fount")
    
    features = pd.read_csv(PLAYER_CSV)

    if "puuid" not in features.columns:
        raise RuntimeError("player features must have puuid column")
    
    merged = features.merge(player_labels, on="puuid", how="left")
    merged["total_games"] = merged["total_games"].fillna(0).astype(int)
    merged["give_up_count"] = merged["give_up_count"].fillna(0).astype(int)
    merged["gave_up"] = merged["gave_up"].fillna(0).astype(int)
    merged["player_give_up_rate"] = merged["player_give_up_rate"].fillna(0.0)

    merged = attach_level(merged)

    merged.to_csv(PLAYER_CSV, index=False)
    print(f"{PLAYER_CSV} Updated")

def main():
    conn = connect()
    games = load_games(conn)

    player_labels = compute_player_labels(games)
    update_player_performance(player_labels)

if __name__ == "__main__":
    main()