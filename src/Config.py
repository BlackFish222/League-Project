import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("RIOT_API_KEY")

if not api_key:
    raise RuntimeError(f"RIOT_API_KEY not found." )

print(api_key)