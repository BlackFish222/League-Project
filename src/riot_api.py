import time
import json
import requests
from pathlib import Path
from Config import api_key

AMERICAS = "https://americas.api.riotgames.com"
PLATFORM  = "https://na1.api.riotgames.com" 

def platform_url(path: str) -> str:
    return f"{PLATFORM}{path}"

def fetch_platform(path: str) -> dict:
    return fetch_json(platform_url(path))

def riot_headers() -> dict:
    if not api_key:
        raise RuntimeError("No API key - check Config.py")
    return {"X-Riot-Token": api_key}

def fetch_json(url: str, retries: int = 5, backoff: float = 1.5) -> dict:
    h = riot_headers()
    for attempt in range(retries):
        r = requests.get(url, headers=h, timeout=20)

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "1"))
            time.sleep(wait)
            continue
        if r.status_code in (500, 502, 503, 504):
            time.sleep(backoff ** attempt)
            continue
        r.raise_for_status()
        return r.json()
    
    raise RuntimeError(f"Failed to fetch after {retries} tries:{url}")

def match_url(match_id: str) -> str:
    return f"{AMERICAS}/lol/match/v5/matches/{match_id}"

def fetch_match(match_id: str) -> dict:
    return fetch_json(match_url(match_id))

def cache_write(obj: dict, path: Path)-> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")

def cache_read(path: Path) -> dict | None:
    if path.exists():
        return json.loads(path.read_text(encoding = "utf-8"))
    return None
