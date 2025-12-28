#Take a small sample of players ~2
#set upper limit on Id's
#grab their most recent match
#grab the ids of every player from that game 
#Loop and clean the Id's already grabbed
import time
import requests
import os
from Config import api_key

DataBase = "Data/Raw"
outPath = os.path.join(DataBase,"match_ids.txt")
Header = {"X-Riot-Token": api_key}

def getMatchIds(puuid: str, start: int = 0, count: int = 100):
    api_url = f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    response = requests.get(api_url, headers = Header, params = {"start":start, "count":count}, timeout = 20)

    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", "2"))
        time.sleep(retry_after)
        return getMatchIds(puuid, start, count)
    
    response.raise_for_status()
    return response.json()

def puuidSeeds(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())
    
def appendNewIds(path: str, new_ids: list[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a', encoding='utf-8') as f:
        for matchid in new_ids:
            f.write(matchid + "\n")

def main():
    if not api_key:
        raise RuntimeError("missing api key")
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
    matchIds = puuidSeeds(outPath)
    print(f'Loaded {len(matchIds)} existing IDs')

    addedTotal = 0
    for puuid in seed_puuids:
        ids = getMatchIds(puuid, start=0, count=100)
        new = [mid for mid in ids if mid not in matchIds]

        if new:
            appendNewIds(outPath, new)
            matchIds.update(new)
            addedTotal += len(new)
        time.sleep(0.1)

    print(f'Added {addedTotal}. total queued: {len(matchIds)}')

if __name__ == "__main__":
    main()