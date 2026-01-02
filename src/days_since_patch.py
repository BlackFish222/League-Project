from datetime import datetime, timezone 

def ms_since_pre_patch():
    PATCH_DATE = "2025-02-19 10:00:00"
    dt = datetime.strptime(PATCH_DATE, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def ms_since_patch():
    PATCH_DATE = "2025-03-18 10:00:00"
    dt = datetime.strptime(PATCH_DATE, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def ms_since_post_patch():
    PATCH_DATE = "2025-04-01 10:00:00"
    dt = datetime.strptime(PATCH_DATE, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

if __name__ == "__main__":
    print(ms_since_pre_patch())
    print(ms_since_patch())
    print(ms_since_post_patch())