import sqlite3
conn = sqlite3.connect("data/raw/riot_queue.sqlite")
print("match_ids:", conn.execute("SELECT COUNT(*) FROM match_ids").fetchone()[0])
print("puuids:", conn.execute("SELECT COUNT(*) FROM puuids").fetchone()[0])
print("unfetched puuids:", conn.execute("SELECT COUNT(*) FROM puuids WHERE fetched=0").fetchone()[0])
conn.close()
