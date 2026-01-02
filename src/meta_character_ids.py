from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, Set, Tuple
import requests

META_CSV = Path("Data/Processed/meta_champs.csv")
CACHE_DIR = Path("Data/Cache")
DDRAGON_CHAMP_JSON = CACHE_DIR / "ddragon_champion_full.json"

DDRAGON_VERSION = "latest"


def _download_ddragon_champs() -> dict:

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    versions = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=20).json()
    version = versions[0]

    url = f"https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/championFull.json"
    data = requests.get(url, timeout=20).json()

    DDRAGON_CHAMP_JSON.write_text(json.dumps(data), encoding="utf-8")
    return data


def _load_ddragon_champs() -> dict:
    if DDRAGON_CHAMP_JSON.exists():
        return json.loads(DDRAGON_CHAMP_JSON.read_text(encoding="utf-8"))
    return _download_ddragon_champs()


def build_name_to_id_map() -> Dict[str, int]:
    data = _load_ddragon_champs()
    champs = data["data"]

    name_to_id: Dict[str, int] = {}
    for champ_key, champ_obj in champs.items():
        champ_id = int(champ_obj["key"])

        # DDragon variants:
        # champ_key is like "Aatrox"
        # champ_obj["id"] is like "Aatrox"
        # champ_obj["name"] is like "Aatrox"
        name_to_id[champ_key] = champ_id
        name_to_id[champ_obj["id"]] = champ_id
        name_to_id[champ_obj["name"]] = champ_id

    return name_to_id



def load_meta_champ_ids(meta_csv_path: Path = META_CSV) -> Tuple[Set[int], Dict[str, int], list[str]]:
    name_to_id = build_name_to_id_map()

    resolved: Dict[str, int] = {}
    missing: list[str] = []
    meta_ids: Set[int] = set()

    with meta_csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            champ_name = (row.get("champion_name") or "").strip()
            if not champ_name:
                continue

            champ_id = name_to_id.get(champ_name)
            if champ_id is None:
                missing.append(champ_name)
                continue

            resolved[champ_name] = champ_id
            meta_ids.add(champ_id)

    return meta_ids, resolved, missing


def meta_ids(meta_csv_path: Path = META_CSV) -> set[int]:
    ids, _, missing = load_meta_champ_ids(meta_csv_path)
    if missing:
        print(f"[meta_character_ids] WARNING: {len(missing)} champ names missing from DDragon mapping.")
        # print(missing[:20])  # uncomment if you want samples
    return ids

if __name__ == "__main__":
    meta_ids, resolved, missing = load_meta_champ_ids()
    print("Meta champ IDs:", meta_ids)
    if missing:
        print("WARNING: Missing champion mappings:", missing)
