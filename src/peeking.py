import random
import sqlite3
from riot_api import fetch_match

conn = sqlite3.connect("Data/Raw/riot.db")
ids = [r[0] for r in conn.execute("SELECT match_id FROM match_ids ORDER BY RANDOM() LIMIT 200;").fetchall()]
conn.close()

bad = 0
for mid in ids:
    qid = fetch_match(mid)["info"].get("queueId")
    if qid != 420:
        bad += 1
        print("NOT ranked:", mid, "queueId=", qid)

print("bad:", bad, "out of", len(ids))
