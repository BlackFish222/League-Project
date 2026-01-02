import sqlite3
import pandas as pd
conn = sqlite3.connect("Data/Raw/riot_queue.sqlite")
pd.read_sql_query(
    "SELECT name FROM sqlite_master WHERE type='Views';",
    conn
)

conn.close()
