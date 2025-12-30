from datetime import datetime, timezone 

PATCH_DATE = "2025-03-18 10:00:00"

dt = datetime.strptime(PATCH_DATE, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

ms_since_patch = int(dt.timestamp() * 1000)
print(ms_since_patch)