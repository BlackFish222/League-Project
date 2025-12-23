import os
from dotenv import load_dotenv

load_dotenv()  # reads .env into environment variables

api_key = os.getenv("RIOT_API_KEY")

if api_key is None:
    raise RuntimeError("RIOT_API_KEY not found. Check your .env file.")